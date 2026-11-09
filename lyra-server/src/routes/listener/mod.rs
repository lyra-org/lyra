// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! The server's accept loop and the limits it holds each connection to.

mod gate;
mod limits;
mod socket;
#[cfg(test)]
pub(super) mod tests;
mod tls;
#[cfg(test)]
mod tls_tests;

use std::{
    io,
    net::SocketAddr,
    pin::pin,
    sync::Arc,
    time::Duration,
};

use axum::{
    Router,
    body::Body,
    extract::{
        ConnectInfo,
        Request,
    },
    http::StatusCode,
    response::{
        IntoResponse,
        Response,
    },
    serve::Listener,
};
use futures::future::{
    Either,
    Ready,
    ready,
};
use hyper::{
    body::Incoming,
    server::conn::http1,
    service::Service,
};
use hyper_util::{
    rt::{
        TokioIo,
        TokioTimer,
    },
    service::TowerToHyperService,
};
use tokio::{
    io::{
        AsyncRead,
        AsyncWrite,
        AsyncWriteExt,
    },
    net::{
        TcpListener,
        TcpStream,
    },
    task::JoinSet,
    time::timeout,
};
use tokio_rustls::{
    TlsAcceptor,
    server::TlsStream,
};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;
use tower_http::timeout::RequestBodyTimeoutLayer;

pub(super) use self::limits::ConnectionLimits;
pub(crate) use self::tls::ServerTls;
use self::{
    gate::{
        ConnectionGate,
        ConnectionStanding,
    },
    socket::AdmittedSocket,
};
use crate::{
    routes::request::ConnectionScheme,
    services::settings::server::TrustedProxies,
};

/// Serves `app` on `listener` until `shutdown`, over TLS alone when `tls` is given, then waits for
/// its connections up to the drain limit and aborts what remains. Sockets upgraded away from HTTP
/// are no longer connections here: they outlive this, and keep their place against the connection
/// caps until they close.
pub(super) async fn serve(
    mut listener: TcpListener,
    app: Router,
    limits: ConnectionLimits,
    trusted_peers: TrustedProxies,
    tls: Option<ServerTls>,
    shutdown: CancellationToken,
) {
    let handshake = tls.as_ref().map(|tls| Handshake {
        acceptor: tls.acceptor(),
        deadline: limits.tls_handshake,
    });
    let certificate_reloads =
        tls.map(|tls| tokio::spawn(tls.follow_files(limits.certificate_poll)));
    let app = app.layer(RequestBodyTimeoutLayer::new(limits.body_stall));
    let gate = ConnectionGate::new(limits.per_peer, limits.global, trusted_peers);
    let mut http = http1::Builder::new();
    http.timer(TokioTimer::new())
        .header_read_timeout(limits.header_read);

    let mut connections = JoinSet::new();
    loop {
        let (stream, peer) = tokio::select! {
            biased;
            () = shutdown.cancelled() => break,
            accepted = Listener::accept(&mut listener) => accepted,
        };
        while connections.try_join_next().is_some() {}
        let Some(permit) = gate.admit(peer.ip()) else {
            continue;
        };
        let standing = permit.standing();
        let socket = AdmittedSocket::new(stream, limits.write_stall, permit);
        connections.spawn(serve_connection(
            http.clone(),
            socket,
            standing,
            peer,
            app.clone(),
            handshake.clone(),
            shutdown.clone(),
        ));
    }
    drop(listener);
    if let Some(certificate_reloads) = certificate_reloads {
        certificate_reloads.abort();
    }

    let drained = timeout(limits.shutdown_drain, async {
        while connections.join_next().await.is_some() {}
    })
    .await;
    if drained.is_err() {
        tracing::warn!(
            timeout_ms = limits.shutdown_drain.as_millis() as u64,
            "server graceful drain deadline elapsed; forcing shutdown"
        );
        connections.shutdown().await;
    }
}

/// Settles a connection's standing as hyper dispatches each request, before anything is polled:
/// a connection evicted by then must not reach the router, where an upgrade could outlive it.
struct ProtectingService<S> {
    standing: Arc<ConnectionStanding>,
    inner: S,
}

impl<R, S> Service<Request<R>> for ProtectingService<S>
where
    S: Service<Request<R>, Response = Response>,
{
    type Response = Response;
    type Error = S::Error;
    type Future = Either<Ready<Result<Response, S::Error>>, S::Future>;

    fn call(&self, request: Request<R>) -> Self::Future {
        if self.standing.protect() {
            Either::Right(self.inner.call(request))
        } else {
            Either::Left(ready(Ok(StatusCode::SERVICE_UNAVAILABLE.into_response())))
        }
    }
}

/// The TLS handshake a connection must finish before it is read as HTTP.
#[derive(Clone)]
struct Handshake {
    acceptor: TlsAcceptor,
    deadline: Duration,
}

impl Handshake {
    /// The socket once the peer has finished the handshake, or `None` when it failed, outlasted
    /// the deadline, spoke plain HTTP, or the server began shutting down first.
    async fn accept(
        &self,
        socket: AdmittedSocket<TcpStream>,
        peer: SocketAddr,
        shutdown: &CancellationToken,
    ) -> Option<TlsStream<AdmittedSocket<TcpStream>>> {
        let accepting = async {
            // A TLS record never opens with a letter, and every HTTP method does.
            if socket
                .first_byte()
                .await?
                .is_some_and(|first| first.is_ascii_uppercase())
            {
                return refuse_plain_http(socket).await.map(|()| None);
            }
            self.acceptor.accept(socket).await.map(Some)
        };
        tokio::select! {
            biased;
            () = shutdown.cancelled() => None,
            accepted = timeout(self.deadline, accepting) => match accepted {
                Ok(Ok(Some(stream))) => Some(stream),
                Ok(Ok(None)) => {
                    tracing::debug!(%peer, "refused a plain HTTP request to the HTTPS port");
                    None
                }
                Ok(Err(err)) => {
                    tracing::debug!(%peer, error = %err, "TLS handshake failed");
                    None
                }
                Err(_) => {
                    tracing::debug!(%peer, "TLS handshake outlasted its deadline");
                    None
                }
            },
        }
    }
}

/// Tells a peer that spoke plain HTTP why it is being closed, then reads until it hangs up:
/// closing over its unread request would reset the connection and could lose the answer.
async fn refuse_plain_http(mut socket: AdmittedSocket<TcpStream>) -> io::Result<()> {
    const ANSWER: &[u8] =
        b"HTTP/1.1 400 Bad Request\r\nContent-Type: text/plain; charset=utf-8\r\n\
                            Content-Length: 49\r\nConnection: close\r\n\r\n\
                            This port serves HTTPS, and was sent plain HTTP.\n";
    socket.write_all(ANSWER).await?;
    socket.shutdown().await?;
    tokio::io::copy(&mut socket, &mut tokio::io::sink()).await?;
    Ok(())
}

/// Serves one admitted connection to its end. A handshake is part of the connection: it holds the
/// connection's place against the caps and is evicted like any connection with no request.
async fn serve_connection(
    http: http1::Builder,
    socket: AdmittedSocket<TcpStream>,
    standing: Arc<ConnectionStanding>,
    peer: SocketAddr,
    app: Router,
    handshake: Option<Handshake>,
    shutdown: CancellationToken,
) {
    let served = async {
        let Some(handshake) = handshake else {
            let scheme = ConnectionScheme::Http;
            return serve_http(http, socket, scheme, &standing, peer, app, shutdown).await;
        };
        if let Some(stream) = handshake.accept(socket, peer, &shutdown).await {
            let scheme = ConnectionScheme::Https;
            serve_http(http, stream, scheme, &standing, peer, app, shutdown).await;
        }
    };

    tokio::select! {
        biased;
        () = standing.evicted() => tracing::trace!(%peer, "evicted a connection with no request"),
        () = served => {}
    }
}

async fn serve_http<S>(
    http: http1::Builder,
    socket: S,
    scheme: ConnectionScheme,
    standing: &Arc<ConnectionStanding>,
    peer: SocketAddr,
    app: Router,
    shutdown: CancellationToken,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let service = app.map_request(move |request: Request<Incoming>| {
        let mut request = request.map(Body::new);
        request.extensions_mut().insert(ConnectInfo(peer));
        request.extensions_mut().insert(scheme);
        request
    });
    let connection = http
        .serve_connection(
            TokioIo::new(socket),
            ProtectingService {
                standing: Arc::clone(standing),
                inner: TowerToHyperService::new(service),
            },
        )
        .with_upgrades();
    let mut connection = pin!(connection);
    let result = tokio::select! {
        result = connection.as_mut() => result,
        () = shutdown.cancelled() => {
            connection.as_mut().graceful_shutdown();
            connection.await
        }
    };
    if let Err(err) = result {
        tracing::trace!(%peer, error = %err, "connection ended with an error");
    }
}
