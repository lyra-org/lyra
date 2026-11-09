// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! The accept loop over real sockets.

use std::{
    net::{
        Ipv4Addr,
        SocketAddr,
    },
    sync::{
        Arc,
        Mutex,
        atomic::{
            AtomicUsize,
            Ordering,
        },
    },
    time::Duration,
};

use axum::{
    Router,
    body::{
        Body,
        Bytes,
    },
    extract::{
        ConnectInfo,
        WebSocketUpgrade,
        ws::Message,
    },
    routing::{
        get,
        post,
    },
};
use tokio::{
    io::{
        AsyncReadExt,
        AsyncWriteExt,
    },
    net::{
        TcpListener,
        TcpStream,
    },
    sync::oneshot,
    task::JoinHandle,
    time::{
        Instant,
        timeout,
    },
};
use tokio_util::sync::CancellationToken;

use super::*;

/// How long anything a test waits for may take. Reached only when a test is failing.
const BOUND: Duration = Duration::from_secs(10);

const SHORT: Duration = Duration::from_millis(150);

pub(in crate::routes) fn limits() -> ConnectionLimits {
    ConnectionLimits {
        header_read: BOUND * 6,
        body_stall: BOUND * 6,
        write_stall: BOUND * 6,
        shutdown_drain: BOUND * 6,
        per_peer: 64,
        global: 64,
    }
}

pub(in crate::routes) struct Server {
    pub(in crate::routes) addr: SocketAddr,
    shutdown: CancellationToken,
    task: JoinHandle<()>,
}

impl Server {
    /// Shuts the server down and returns how long that took.
    pub(in crate::routes) async fn stop(self) -> Duration {
        let started = Instant::now();
        self.shutdown.cancel();
        timeout(BOUND, self.task)
            .await
            .expect("the server stops")
            .expect("the server does not panic");
        started.elapsed()
    }
}

async fn start_trusting(app: Router, limits: ConnectionLimits, trusted: Vec<IpAddr>) -> Server {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(serve(listener, app, limits, trusted, shutdown.clone()));
    Server {
        addr,
        shutdown,
        task,
    }
}

pub(in crate::routes) async fn start(app: Router, limits: ConnectionLimits) -> Server {
    start_trusting(app, limits, Vec::new()).await
}

async fn echo_socket(ws: WebSocketUpgrade) -> axum::response::Response {
    ws.on_upgrade(|mut socket| async move {
        while let Some(Ok(message)) = socket.recv().await {
            if let Message::Text(_) = message
                && socket.send(message).await.is_err()
            {
                break;
            }
        }
    })
}

fn app() -> Router {
    Router::new()
        .route("/ok", get(|| async { "ok" }))
        .route("/ws", get(echo_socket))
        .route(
            "/peer",
            get(|ConnectInfo(peer): ConnectInfo<SocketAddr>| async move { peer.to_string() }),
        )
}

pub(in crate::routes) async fn connect(addr: SocketAddr) -> TcpStream {
    TcpStream::connect(addr).await.unwrap()
}

/// Everything the server still sends on `stream`, until it closes it.
async fn read_until_closed(stream: &mut TcpStream) -> String {
    let mut received = Vec::new();
    let mut chunk = [0; 4096];
    timeout(BOUND, async {
        loop {
            match stream.read(&mut chunk).await {
                Ok(0) | Err(_) => break,
                Ok(read) => received.extend_from_slice(&chunk[..read]),
            }
        }
    })
    .await
    .expect("the server closes the connection");
    String::from_utf8_lossy(&received).into_owned()
}

pub(in crate::routes) async fn read_until(stream: &mut TcpStream, needle: &str) -> String {
    let mut received = Vec::new();
    let mut chunk = [0; 4096];
    timeout(BOUND, async {
        while !String::from_utf8_lossy(&received).contains(needle) {
            let read = stream.read(&mut chunk).await.unwrap();
            assert_ne!(read, 0, "closed before {needle:?}");
            received.extend_from_slice(&chunk[..read]);
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{needle:?} never arrived"));
    String::from_utf8_lossy(&received).into_owned()
}

fn get_request(path: &str) -> String {
    format!("GET {path} HTTP/1.1\r\nHost: lyra.test\r\n\r\n")
}

/// A connection that has been served a request and stays open.
async fn held_connection(addr: SocketAddr) -> TcpStream {
    let mut stream = connect(addr).await;
    stream
        .write_all(get_request("/ok").as_bytes())
        .await
        .unwrap();
    read_until(&mut stream, "\r\n\r\nok").await;
    stream
}

async fn is_served(addr: SocketAddr) -> bool {
    let Ok(mut stream) = TcpStream::connect(addr).await else {
        return false;
    };
    let request = "GET /ok HTTP/1.1\r\nHost: lyra.test\r\nConnection: close\r\n\r\n";
    if stream.write_all(request.as_bytes()).await.is_err() {
        return false;
    }
    read_until_closed(&mut stream)
        .await
        .starts_with("HTTP/1.1 200")
}

async fn becomes_served(addr: SocketAddr) -> bool {
    timeout(BOUND, async {
        while !is_served(addr).await {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .is_ok()
}

/// A socket upgraded to a WebSocket, proven by a message echoed over it.
async fn upgraded_socket(addr: SocketAddr) -> TcpStream {
    let mut stream = connect(addr).await;
    let handshake = "GET /ws HTTP/1.1\r\nHost: lyra.test\r\nConnection: Upgrade\r\n\
                     Upgrade: websocket\r\nSec-WebSocket-Version: 13\r\n\
                     Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n";
    stream.write_all(handshake.as_bytes()).await.unwrap();
    let response = read_until(&mut stream, "\r\n\r\n").await;
    assert!(response.starts_with("HTTP/1.1 101"), "{response}");
    assert_echoes(&mut stream).await;
    stream
}

async fn assert_echoes(stream: &mut TcpStream) {
    stream
        .write_all(&[0x81, 0x82, 0, 0, 0, 0, b'h', b'i'])
        .await
        .unwrap();
    let mut echoed = [0; 4];
    timeout(BOUND, stream.read_exact(&mut echoed))
        .await
        .expect("the message comes back")
        .unwrap();
    assert_eq!(echoed, [0x81, 2, b'h', b'i']);
}

#[tokio::test]
async fn a_partial_head_is_closed_at_the_deadline() {
    let server = start(
        app(),
        ConnectionLimits {
            header_read: SHORT,
            ..limits()
        },
    )
    .await;
    let mut stream = connect(server.addr).await;
    stream.write_all(b"GET /ok HT").await.unwrap();

    let started = Instant::now();
    read_until_closed(&mut stream).await;
    assert!(started.elapsed() >= SHORT - Duration::from_millis(50));
}

#[tokio::test]
async fn trickling_a_head_does_not_extend_its_deadline() {
    let server = start(
        app(),
        ConnectionLimits {
            header_read: SHORT,
            ..limits()
        },
    )
    .await;
    let stream = connect(server.addr).await;
    let (mut reader, mut writer) = stream.into_split();
    let trickle = tokio::spawn(async move {
        for _ in 0..(BOUND.as_millis() / 20) {
            if writer.write_all(b"x").await.is_err() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    });

    let mut chunk = [0; 4096];
    timeout(BOUND / 2, async {
        while matches!(reader.read(&mut chunk).await, Ok(read) if read > 0) {}
    })
    .await
    .expect("the deadline covers the whole head");
    trickle.abort();
}

#[tokio::test]
async fn an_idle_connection_is_closed() {
    let server = start(
        app(),
        ConnectionLimits {
            header_read: SHORT,
            ..limits()
        },
    )
    .await;

    let mut silent = connect(server.addr).await;
    read_until_closed(&mut silent).await;

    let mut idle = held_connection(server.addr).await;
    read_until_closed(&mut idle).await;
}

fn buffering_app() -> Router {
    Router::new().route(
        "/upload",
        post(|body: Bytes| async move { body.len().to_string() }),
    )
}

fn upload_head(length: usize) -> String {
    format!("POST /upload HTTP/1.1\r\nHost: lyra.test\r\nContent-Length: {length}\r\n\r\n")
}

#[tokio::test]
async fn a_body_that_stops_arriving_ends_the_request() {
    let server = start(
        buffering_app(),
        ConnectionLimits {
            body_stall: SHORT,
            ..limits()
        },
    )
    .await;
    let mut stream = connect(server.addr).await;
    stream.write_all(upload_head(16).as_bytes()).await.unwrap();
    stream.write_all(b"partial").await.unwrap();

    let response = read_until_closed(&mut stream).await;
    assert!(response.starts_with("HTTP/1.1 400"), "{response}");
}

#[tokio::test]
async fn time_before_the_body_is_read_is_not_charged_to_it() {
    let stall = SHORT * 3;
    let (ready_tx, ready_rx) = oneshot::channel::<()>();
    let ready_tx = Arc::new(Mutex::new(Some(ready_tx)));
    let handler = post(move |request: axum::extract::Request| async move {
        tokio::time::sleep(stall * 2).await;
        let ready = ready_tx.lock().unwrap().take();
        ready.expect("one request").send(()).unwrap();
        axum::body::to_bytes(request.into_body(), 1024)
            .await
            .unwrap()
            .len()
            .to_string()
    });
    let server = start(
        Router::new().route("/upload", handler),
        ConnectionLimits {
            body_stall: stall,
            ..limits()
        },
    )
    .await;

    let mut stream = connect(server.addr).await;
    stream.write_all(upload_head(4).as_bytes()).await.unwrap();
    timeout(BOUND, ready_rx).await.unwrap().unwrap();
    stream.write_all(b"body").await.unwrap();

    let response = read_until(&mut stream, "\r\n\r\n4").await;
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
}

struct NotifyOnDrop(Option<oneshot::Sender<()>>);

impl Drop for NotifyOnDrop {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

#[tokio::test]
async fn a_socket_that_stops_taking_writes_is_closed() {
    let (dropped_tx, dropped_rx) = oneshot::channel();
    let guard = Arc::new(Mutex::new(Some(NotifyOnDrop(Some(dropped_tx)))));
    let flood = get(move || {
        let guard = guard.lock().unwrap().take().expect("one request");
        let chunks = futures::stream::repeat_with(move || {
            let _ = &guard;
            Ok::<_, std::io::Error>(Bytes::from_static(&[0; 64 * 1024]))
        });
        async move { Body::from_stream(chunks) }
    });
    let server = start(
        Router::new().route("/flood", flood),
        ConnectionLimits {
            write_stall: SHORT,
            ..limits()
        },
    )
    .await;

    let mut stream = connect(server.addr).await;
    stream
        .write_all(get_request("/flood").as_bytes())
        .await
        .unwrap();
    timeout(BOUND * 3, dropped_rx)
        .await
        .expect("the response is abandoned")
        .unwrap();
    drop(stream);
}

#[tokio::test]
async fn a_peer_over_its_cap_is_closed_and_gets_its_place_back() {
    let server = start(
        app(),
        ConnectionLimits {
            per_peer: 2,
            ..limits()
        },
    )
    .await;
    let first = held_connection(server.addr).await;
    let _second = held_connection(server.addr).await;
    assert!(!is_served(server.addr).await);

    drop(first);
    assert!(becomes_served(server.addr).await);
}

#[tokio::test]
async fn connections_over_the_global_cap_are_closed() {
    let server = start(
        app(),
        ConnectionLimits {
            global: 1,
            ..limits()
        },
    )
    .await;
    let only = held_connection(server.addr).await;
    assert!(!is_served(server.addr).await);

    drop(only);
    assert!(becomes_served(server.addr).await);
}

/// Answers once the server has served a request on `stream`, proving it stayed open.
async fn assert_still_served(stream: &mut TcpStream) {
    stream
        .write_all(get_request("/ok").as_bytes())
        .await
        .unwrap();
    read_until(stream, "\r\n\r\nok").await;
}

fn counting_service() -> (
    impl Service<Request, Response = Response, Error = std::convert::Infallible>,
    Arc<AtomicUsize>,
) {
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&calls);
    let service = hyper::service::service_fn(move |_: Request| {
        counted.fetch_add(1, Ordering::SeqCst);
        std::future::ready(Ok(Response::default()))
    });
    (service, calls)
}

fn loopback() -> IpAddr {
    IpAddr::V4(Ipv4Addr::LOCALHOST)
}

#[tokio::test]
async fn a_request_on_an_evicted_connection_is_not_dispatched() {
    let gate = ConnectionGate::new(8, 1, []);
    let evicted = gate.admit(loopback()).unwrap();
    let _newcomer = gate.admit(loopback()).unwrap();
    let (inner, calls) = counting_service();
    let service = ProtectingService {
        standing: evicted.standing(),
        inner,
    };

    let response = service.call(Request::default()).await.unwrap();

    assert_eq!(
        response.status(),
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn a_connection_is_protected_when_hyper_dispatches_not_when_polled() {
    let gate = ConnectionGate::new(8, 1, []);
    let permit = gate.admit(loopback()).unwrap();
    let (inner, calls) = counting_service();
    let service = ProtectingService {
        standing: permit.standing(),
        inner,
    };

    let unpolled = service.call(Request::default());

    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(gate.admit(loopback()).is_none(), "nothing is left to evict");
    drop(unpolled);
}

#[tokio::test]
async fn a_full_cap_closes_the_oldest_silent_connection_for_a_newcomer() {
    let server = start(
        app(),
        ConnectionLimits {
            global: 2,
            ..limits()
        },
    )
    .await;
    let mut oldest = connect(server.addr).await;
    let mut next = connect(server.addr).await;

    let mut newcomer = connect(server.addr).await;

    read_until_closed(&mut oldest).await;
    assert_still_served(&mut newcomer).await;
    assert_still_served(&mut next).await;
}

#[tokio::test]
async fn a_stream_of_silent_connections_keeps_only_the_newest() {
    const CAP: usize = 4;
    let server = start(
        app(),
        ConnectionLimits {
            global: CAP,
            ..limits()
        },
    )
    .await;
    let mut connections = Vec::new();
    for _ in 0..3 * CAP {
        connections.push(connect(server.addr).await);
    }

    let (evicted, kept) = connections.split_at_mut(2 * CAP);
    for connection in evicted {
        read_until_closed(connection).await;
    }
    for connection in kept {
        assert_still_served(connection).await;
    }
}

#[tokio::test]
async fn a_connection_that_delivered_a_request_is_not_evicted() {
    let server = start(
        app(),
        ConnectionLimits {
            global: 2,
            ..limits()
        },
    )
    .await;
    let mut protected = held_connection(server.addr).await;
    let mut silent = connect(server.addr).await;

    let mut newcomer = connect(server.addr).await;
    read_until_closed(&mut silent).await;
    assert_still_served(&mut newcomer).await;
    assert_still_served(&mut protected).await;

    assert!(!is_served(server.addr).await, "nothing is left to evict");
    assert_still_served(&mut protected).await;
    assert_still_served(&mut newcomer).await;
}

#[tokio::test]
async fn trusted_peers_are_held_only_to_the_global_cap() {
    let server = start_trusting(
        app(),
        ConnectionLimits {
            per_peer: 1,
            global: 2,
            ..limits()
        },
        vec![IpAddr::V4(Ipv4Addr::LOCALHOST)],
    )
    .await;
    let _first = held_connection(server.addr).await;
    let _second = held_connection(server.addr).await;
    assert!(!is_served(server.addr).await);
}

#[tokio::test]
async fn an_upgraded_socket_keeps_its_place_until_it_closes() {
    let server = start(
        app(),
        ConnectionLimits {
            per_peer: 1,
            ..limits()
        },
    )
    .await;
    let socket = upgraded_socket(server.addr).await;
    assert!(!is_served(server.addr).await);

    drop(socket);
    assert!(becomes_served(server.addr).await);
}

#[tokio::test]
async fn shutdown_does_not_wait_on_idle_or_upgraded_sockets() {
    let server = start(app(), limits()).await;
    let mut idle = held_connection(server.addr).await;
    let mut upgraded = upgraded_socket(server.addr).await;

    let took = server.stop().await;
    assert!(took < BOUND / 2, "shutdown took {took:?}");

    read_until_closed(&mut idle).await;
    assert_echoes(&mut upgraded).await;
}

#[tokio::test]
async fn shutdown_aborts_what_outlasts_the_drain() {
    let (entered_tx, entered_rx) = oneshot::channel();
    let (dropped_tx, mut dropped_rx) = oneshot::channel();
    let held = Arc::new(Mutex::new(Some((
        entered_tx,
        NotifyOnDrop(Some(dropped_tx)),
    ))));
    let hang = get(move || {
        let (entered, guard) = held.lock().unwrap().take().expect("one request");
        async move {
            let _guard = guard;
            entered.send(()).unwrap();
            std::future::pending::<&str>().await
        }
    });
    let server = start(
        app().route("/hang", hang),
        ConnectionLimits {
            shutdown_drain: SHORT,
            ..limits()
        },
    )
    .await;
    let mut stalled = connect(server.addr).await;
    stalled.write_all(b"GET /ok HT").await.unwrap();
    let mut in_flight = connect(server.addr).await;
    in_flight
        .write_all(get_request("/hang").as_bytes())
        .await
        .unwrap();
    timeout(BOUND, entered_rx).await.unwrap().unwrap();

    let took = server.stop().await;
    assert!(
        took >= SHORT - Duration::from_millis(50),
        "shutdown took {took:?}"
    );
    assert!(took < BOUND / 2, "shutdown took {took:?}");
    assert!(
        dropped_rx.try_recv().is_ok(),
        "nothing outlives the server but upgraded sockets"
    );
    read_until_closed(&mut stalled).await;
    read_until_closed(&mut in_flight).await;
}

#[tokio::test]
async fn handlers_see_the_peer_address() {
    let server = start(app(), limits()).await;
    let mut stream = connect(server.addr).await;
    let local = stream.local_addr().unwrap().to_string();
    stream
        .write_all(get_request("/peer").as_bytes())
        .await
        .unwrap();
    read_until(&mut stream, &format!("\r\n\r\n{local}")).await;
}

#[tokio::test]
async fn responses_stream_as_their_chunks_arrive() {
    let (chunk_tx, chunk_rx) = tokio::sync::mpsc::channel::<Bytes>(1);
    let chunk_rx = Arc::new(Mutex::new(Some(chunk_rx)));
    let chunks = get(move || {
        let chunk_rx = chunk_rx.lock().unwrap().take().expect("one request");
        let chunks = futures::stream::unfold(chunk_rx, |mut chunk_rx| async move {
            let chunk = chunk_rx.recv().await?;
            Some((Ok::<_, std::io::Error>(chunk), chunk_rx))
        });
        async move { Body::from_stream(chunks) }
    });
    let server = start(Router::new().route("/chunks", chunks), limits()).await;

    let mut stream = connect(server.addr).await;
    stream
        .write_all(get_request("/chunks").as_bytes())
        .await
        .unwrap();
    chunk_tx.send(Bytes::from_static(b"one")).await.unwrap();
    read_until(&mut stream, "one").await;
    chunk_tx.send(Bytes::from_static(b"two")).await.unwrap();
    read_until(&mut stream, "two").await;
}
