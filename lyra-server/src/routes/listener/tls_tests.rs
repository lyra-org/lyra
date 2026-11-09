// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! The accept loop serving HTTPS, and the certificate files it follows.

use std::{
    net::SocketAddr,
    path::Path,
    sync::Arc,
    time::Duration,
};

use axum::{
    Extension,
    Router,
    extract::ConnectInfo,
    http::{
        HeaderMap,
        Uri,
    },
    routing::get,
};
use rustls::{
    ClientConfig,
    RootCertStore,
    crypto::ring::default_provider,
    pki_types::{
        CertificateDer,
        ServerName,
    },
};
use tokio::{
    io::AsyncWriteExt,
    net::TcpStream,
    time::{
        Instant,
        timeout,
    },
};
use tokio_rustls::{
    TlsConnector,
    client::TlsStream,
};

use super::{
    tests::{
        BOUND,
        SHORT,
        Server,
        app,
        assert_echoes,
        assert_still_served,
        connect,
        flooding_app,
        get_request,
        limits,
        read_until,
        read_until_closed,
        start_with,
        upgraded,
    },
    *,
};
use crate::{
    routes::request::base_url,
    services::boot::TlsFiles,
};

const HOST: &str = "lyra.test";

/// A self-signed certificate for [`HOST`] and its key.
struct Identity {
    cert: String,
    key: String,
    der: CertificateDer<'static>,
}

fn identity() -> Identity {
    let generated = rcgen::generate_simple_self_signed([HOST.to_string()]).unwrap();
    Identity {
        cert: generated.cert.pem(),
        key: generated.signing_key.serialize_pem(),
        der: generated.cert.der().clone(),
    }
}

/// The files a server is pointed at, in a directory of their own.
struct Pair {
    files: TlsFiles,
    _dir: tempfile::TempDir,
}

impl Pair {
    fn holding(cert: &str, key: &str) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let pair = Self {
            files: TlsFiles {
                cert: dir.path().join("lyra.crt"),
                key: dir.path().join("lyra.key"),
            },
            _dir: dir,
        };
        pair.write_cert(cert);
        pair.write_key(key);
        pair
    }

    fn of(identity: &Identity) -> Self {
        Self::holding(&identity.cert, &identity.key)
    }

    fn write_cert(&self, cert: &str) {
        std::fs::write(&self.files.cert, cert).unwrap();
    }

    fn write_key(&self, key: &str) {
        std::fs::write(&self.files.key, key).unwrap();
    }

    async fn refusal(&self) -> String {
        let error = ServerTls::load(self.files.clone())
            .await
            .err()
            .expect("the pair is refused");
        format!("{error:#}")
    }
}

async fn start_tls(app: Router, limits: ConnectionLimits, pair: &Pair) -> Server {
    let tls = ServerTls::load(pair.files.clone()).await.unwrap();
    start_with(app, limits, TrustedProxies::none(), Some(tls)).await
}

/// A client that trusts exactly `identities`.
fn client(identities: &[&Identity]) -> TlsConnector {
    let mut roots = RootCertStore::empty();
    for identity in identities {
        roots.add(identity.der.clone()).unwrap();
    }
    let mut config = ClientConfig::builder_with_provider(Arc::new(default_provider()))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    TlsConnector::from(Arc::new(config))
}

async fn handshake(
    addr: SocketAddr,
    client: &TlsConnector,
) -> std::io::Result<TlsStream<TcpStream>> {
    let stream = TcpStream::connect(addr).await?;
    let name = ServerName::try_from(HOST).unwrap();
    timeout(BOUND, client.connect(name, stream))
        .await
        .expect("the handshake settles")
}

fn presented(stream: &TlsStream<TcpStream>) -> &CertificateDer<'static> {
    &stream.get_ref().1.peer_certificates().unwrap()[0]
}

async fn is_served(addr: SocketAddr, client: &TlsConnector) -> bool {
    let Ok(mut stream) = handshake(addr, client).await else {
        return false;
    };
    let request = format!("GET /ok HTTP/1.1\r\nHost: {HOST}\r\nConnection: close\r\n\r\n");
    if stream.write_all(request.as_bytes()).await.is_err() {
        return false;
    }
    read_until_closed(&mut stream)
        .await
        .starts_with("HTTP/1.1 200")
}

async fn becomes_served(addr: SocketAddr, client: &TlsConnector) -> bool {
    timeout(BOUND, async {
        while !is_served(addr, client).await {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .is_ok()
}

/// Waits until new connections are presented `identity`'s certificate.
async fn becomes_presented(addr: SocketAddr, client: &TlsConnector, identity: &Identity) {
    timeout(BOUND, async {
        loop {
            let stream = handshake(addr, client).await.unwrap();
            if presented(&stream) == &identity.der {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the certificate is replaced");
}

fn base_url_app() -> Router {
    Router::new().route(
        "/base-url",
        get(
            |ConnectInfo(peer): ConnectInfo<SocketAddr>,
             Extension(connection): Extension<ConnectionScheme>,
             headers: HeaderMap,
             uri: Uri| async move {
                base_url(Some(peer), connection, &headers, &uri).unwrap_or_default()
            },
        ),
    )
}

#[tokio::test]
async fn a_request_is_served_over_tls() {
    let identity = identity();
    let server = start_tls(app(), limits(), &Pair::of(&identity)).await;

    let mut stream = handshake(server.addr, &client(&[&identity])).await.unwrap();

    assert_eq!(presented(&stream), &identity.der);
    assert_eq!(stream.get_ref().1.alpn_protocol(), Some(&b"http/1.1"[..]));
    assert_still_served(&mut stream).await;
    assert_still_served(&mut stream).await;
}

#[tokio::test]
async fn a_client_that_does_not_trust_the_certificate_is_not_served() {
    let server = start_tls(app(), limits(), &Pair::of(&identity())).await;

    assert!(!is_served(server.addr, &client(&[&identity()])).await);
}

#[tokio::test]
async fn the_base_url_follows_the_connection_unless_a_trusted_peer_forwards_one() {
    let identity = identity();
    let tls = start_tls(base_url_app(), limits(), &Pair::of(&identity)).await;
    let plain = start_with(base_url_app(), limits(), TrustedProxies::none(), None).await;
    let direct = format!("GET /base-url HTTP/1.1\r\nHost: {HOST}\r\n\r\n");
    let forwarded = format!(
        "GET /base-url HTTP/1.1\r\nHost: {HOST}\r\nX-Forwarded-Proto: http\r\n\
         X-Forwarded-Host: lyra.example\r\n\r\n"
    );

    let mut stream = handshake(tls.addr, &client(&[&identity])).await.unwrap();
    stream.write_all(direct.as_bytes()).await.unwrap();
    read_until(&mut stream, "\r\n\r\nhttps://lyra.test").await;
    stream.write_all(forwarded.as_bytes()).await.unwrap();
    read_until(&mut stream, "\r\n\r\nhttp://lyra.example").await;

    let mut stream = connect(plain.addr).await;
    stream.write_all(direct.as_bytes()).await.unwrap();
    read_until(&mut stream, "\r\n\r\nhttp://lyra.test").await;
}

#[tokio::test]
async fn a_stalled_handshake_is_closed_at_its_deadline() {
    let server = start_tls(
        app(),
        ConnectionLimits {
            tls_handshake: SHORT,
            ..limits()
        },
        &Pair::of(&identity()),
    )
    .await;

    let mut silent = connect(server.addr).await;
    let started = Instant::now();
    read_until_closed(&mut silent).await;
    assert!(started.elapsed() >= SHORT - Duration::from_millis(50));

    let mut partial = connect(server.addr).await;
    partial.write_all(&[0x16, 0x03, 0x01]).await.unwrap();
    let started = Instant::now();
    read_until_closed(&mut partial).await;
    assert!(started.elapsed() >= SHORT - Duration::from_millis(50));
}

#[tokio::test]
async fn a_handshake_in_progress_holds_its_peers_place() {
    let identity = identity();
    let client = client(&[&identity]);
    let server = start_tls(
        app(),
        ConnectionLimits {
            per_peer: 1,
            ..limits()
        },
        &Pair::of(&identity),
    )
    .await;
    let stalled = connect(server.addr).await;
    assert!(!is_served(server.addr, &client).await);

    drop(stalled);
    assert!(becomes_served(server.addr, &client).await);
}

#[tokio::test]
async fn a_handshake_that_times_out_gives_its_place_back() {
    let identity = identity();
    let client = client(&[&identity]);
    let server = start_tls(
        app(),
        ConnectionLimits {
            tls_handshake: SHORT,
            per_peer: 1,
            ..limits()
        },
        &Pair::of(&identity),
    )
    .await;
    let mut stalled = connect(server.addr).await;
    read_until_closed(&mut stalled).await;

    assert!(becomes_served(server.addr, &client).await);
}

#[tokio::test]
async fn a_full_cap_closes_the_oldest_handshake_for_a_newcomer() {
    let identity = identity();
    let client = client(&[&identity]);
    let server = start_tls(
        app(),
        ConnectionLimits {
            global: 2,
            ..limits()
        },
        &Pair::of(&identity),
    )
    .await;
    let mut oldest = connect(server.addr).await;
    let mut served = handshake(server.addr, &client).await.unwrap();
    assert_still_served(&mut served).await;

    let mut newcomer = handshake(server.addr, &client).await.unwrap();

    read_until_closed(&mut oldest).await;
    assert_still_served(&mut newcomer).await;
    assert_still_served(&mut served).await;
}

#[tokio::test]
async fn a_plain_http_request_is_told_the_port_serves_https() {
    let identity = identity();
    let client = client(&[&identity]);
    let server = start_tls(
        app(),
        ConnectionLimits {
            per_peer: 1,
            ..limits()
        },
        &Pair::of(&identity),
    )
    .await;

    let mut plaintext = connect(server.addr).await;
    plaintext
        .write_all(get_request("/ok").as_bytes())
        .await
        .unwrap();
    let answer = read_until(&mut plaintext, "plain HTTP.\n").await;
    assert!(
        answer.starts_with("HTTP/1.1 400 Bad Request\r\n"),
        "{answer}"
    );
    let body = answer.split_once("\r\n\r\n").unwrap().1;
    assert!(answer.contains(&format!("Content-Length: {}\r\n", body.len())));

    plaintext.shutdown().await.unwrap();
    read_until_closed(&mut plaintext).await;
    assert!(becomes_served(server.addr, &client).await);
}

#[tokio::test]
async fn a_plain_http_peer_that_stays_is_closed_at_the_handshake_deadline() {
    let identity = identity();
    let server = start_tls(
        app(),
        ConnectionLimits {
            tls_handshake: SHORT,
            ..limits()
        },
        &Pair::of(&identity),
    )
    .await;

    let mut plaintext = connect(server.addr).await;
    plaintext
        .write_all(get_request("/ok").as_bytes())
        .await
        .unwrap();
    let answer = read_until_closed(&mut plaintext).await;
    assert!(answer.starts_with("HTTP/1.1 400"), "{answer}");
}

#[tokio::test]
async fn a_socket_upgraded_over_tls_keeps_its_place_until_it_closes() {
    let identity = identity();
    let client = client(&[&identity]);
    let server = start_tls(
        app(),
        ConnectionLimits {
            per_peer: 1,
            ..limits()
        },
        &Pair::of(&identity),
    )
    .await;
    let mut socket = upgraded(handshake(server.addr, &client).await.unwrap()).await;
    assert!(!is_served(server.addr, &client).await);
    assert_echoes(&mut socket).await;

    drop(socket);
    assert!(becomes_served(server.addr, &client).await);
}

#[tokio::test]
async fn a_tls_socket_that_stops_taking_writes_is_closed() {
    let identity = identity();
    let (app, dropped) = flooding_app();
    let server = start_tls(
        app,
        ConnectionLimits {
            write_stall: SHORT,
            ..limits()
        },
        &Pair::of(&identity),
    )
    .await;

    let mut stream = handshake(server.addr, &client(&[&identity])).await.unwrap();
    stream
        .write_all(get_request("/flood").as_bytes())
        .await
        .unwrap();
    timeout(BOUND * 3, dropped)
        .await
        .expect("the response is abandoned")
        .unwrap();
    drop(stream);
}

#[tokio::test]
async fn shutdown_does_not_wait_on_a_handshake() {
    let identity = identity();
    let server = start_tls(app(), limits(), &Pair::of(&identity)).await;
    let mut stalled = connect(server.addr).await;
    let mut idle = handshake(server.addr, &client(&[&identity])).await.unwrap();
    assert_still_served(&mut idle).await;

    let took = server.stop().await;
    assert!(took < BOUND / 2, "shutdown took {took:?}");

    read_until_closed(&mut stalled).await;
    read_until_closed(&mut idle).await;
}

#[tokio::test]
async fn a_replaced_pair_is_served_to_new_connections() {
    let (first, second) = (identity(), identity());
    let client = client(&[&first, &second]);
    let pair = Pair::of(&first);
    let server = start_tls(
        app(),
        ConnectionLimits {
            certificate_poll: SHORT / 10,
            ..limits()
        },
        &pair,
    )
    .await;
    let mut earlier = handshake(server.addr, &client).await.unwrap();
    assert_eq!(presented(&earlier), &first.der);

    pair.write_cert(&second.cert);
    pair.write_key(&second.key);

    becomes_presented(server.addr, &client, &second).await;
    assert_still_served(&mut earlier).await;
}

#[cfg(unix)]
#[tokio::test]
async fn a_pair_behind_links_is_replaced_when_the_links_are_repointed() {
    use std::os::unix::fs::symlink;

    let (first, second) = (identity(), identity());
    let client = client(&[&first, &second]);
    let (issued, renewed) = (Pair::of(&first), Pair::of(&second));
    let dir = tempfile::tempdir().unwrap();
    let live = TlsFiles {
        cert: dir.path().join("cert.pem"),
        key: dir.path().join("key.pem"),
    };
    symlink(&issued.files.cert, &live.cert).unwrap();
    symlink(&issued.files.key, &live.key).unwrap();
    let tls = ServerTls::load(live.clone()).await.unwrap();
    let limits = ConnectionLimits {
        certificate_poll: SHORT / 10,
        ..limits()
    };
    let server = start_with(app(), limits, TrustedProxies::none(), Some(tls)).await;
    let stream = handshake(server.addr, &client).await.unwrap();
    assert_eq!(presented(&stream), &first.der);

    for (target, link) in [
        (&renewed.files.cert, &live.cert),
        (&renewed.files.key, &live.key),
    ] {
        std::fs::remove_file(link).unwrap();
        symlink(target, link).unwrap();
    }

    becomes_presented(server.addr, &client, &second).await;
}

#[tokio::test]
async fn a_broken_replacement_keeps_the_previous_certificate() {
    let (first, second) = (identity(), identity());
    let client = client(&[&first, &second]);
    let pair = Pair::of(&first);
    let server = start_tls(
        app(),
        ConnectionLimits {
            certificate_poll: SHORT / 10,
            ..limits()
        },
        &pair,
    )
    .await;
    let still_first = || async {
        tokio::time::sleep(SHORT).await;
        let stream = handshake(server.addr, &client).await.unwrap();
        assert_eq!(presented(&stream), &first.der);
    };

    pair.write_cert("not a certificate");
    still_first().await;

    // A working certificate and key ahead of a chain certificate that is not one.
    pair.write_key(&second.key);
    pair.write_cert(&format!(
        "{}-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n",
        second.cert
    ));
    still_first().await;

    std::fs::remove_file(&pair.files.key).unwrap();
    still_first().await;

    // A renewal caught between its two writes: the new certificate beside the old key.
    pair.write_cert(&second.cert);
    pair.write_key(&first.key);
    still_first().await;

    pair.write_key(&second.key);
    becomes_presented(server.addr, &client, &second).await;
}

#[tokio::test]
async fn a_pair_that_cannot_be_served_is_refused() {
    let (first, second) = (identity(), identity());
    let named = |path: &Path| path.display().to_string();

    let pair = Pair::of(&first);
    std::fs::remove_file(&pair.files.cert).unwrap();
    let refusal = pair.refusal().await;
    assert!(
        refusal.starts_with("failed to read the TLS certificate at "),
        "{refusal}"
    );
    assert!(refusal.contains(&named(&pair.files.cert)), "{refusal}");

    let pair = Pair::of(&first);
    std::fs::remove_file(&pair.files.key).unwrap();
    let refusal = pair.refusal().await;
    assert!(
        refusal.starts_with("failed to read the TLS key at "),
        "{refusal}"
    );

    let pair = Pair::holding("not a certificate", &first.key);
    assert_eq!(
        pair.refusal().await,
        format!(
            "TLS certificate at {} holds no PEM certificate",
            named(&pair.files.cert)
        )
    );

    let pair = Pair::holding(&first.cert, &first.cert);
    assert_eq!(
        pair.refusal().await,
        format!(
            "TLS key at {} holds no PEM private key",
            named(&pair.files.key)
        )
    );

    let pair = Pair::holding(&first.cert, &first.key.replace("MI", "!!"));
    assert_eq!(
        pair.refusal().await,
        format!("TLS key at {} is not valid PEM", named(&pair.files.key))
    );

    for encrypted in [
        "-----BEGIN ENCRYPTED PRIVATE KEY-----\nMIIB\n-----END ENCRYPTED PRIVATE KEY-----\n",
        "-----BEGIN RSA PRIVATE KEY-----\nProc-Type: 4,ENCRYPTED\nDEK-Info: AES-256-CBC,00\n\n\
         MIIB\n-----END RSA PRIVATE KEY-----\n",
    ] {
        let pair = Pair::holding(&first.cert, encrypted);
        assert_eq!(
            pair.refusal().await,
            format!("TLS key at {} is encrypted", named(&pair.files.key))
        );
    }

    let pair = Pair::holding(&first.cert, &second.key);
    let refusal = pair.refusal().await;
    assert!(refusal.contains("can't be served"), "{refusal}");
    assert!(refusal.contains(&named(&pair.files.cert)), "{refusal}");
    assert!(refusal.contains(&named(&pair.files.key)), "{refusal}");

    let garbage = "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n";
    for unreadable in [garbage.to_string(), format!("{}{garbage}", first.cert)] {
        let pair = Pair::holding(&unreadable, &first.key);
        assert_eq!(
            pair.refusal().await,
            format!(
                "TLS certificate at {} holds a certificate that can't be read",
                named(&pair.files.cert)
            )
        );
    }
}
