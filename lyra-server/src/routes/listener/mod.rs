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

use std::{
    net::{
        IpAddr,
        SocketAddr,
    },
    pin::pin,
    sync::Arc,
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
    net::{
        TcpListener,
        TcpStream,
    },
    task::JoinSet,
    time::timeout,
};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;
use tower_http::timeout::RequestBodyTimeoutLayer;

pub(super) use self::limits::ConnectionLimits;
use self::{
    gate::{
        ConnectionGate,
        ConnectionStanding,
    },
    socket::AdmittedSocket,
};

/// Serves `app` on `listener` until `shutdown`, then waits for its connections up to the drain
/// limit and aborts what remains. Sockets upgraded away from HTTP are no longer connections here:
/// they outlive this, and keep their place against the connection caps until they close.
pub(super) async fn serve(
    mut listener: TcpListener,
    app: Router,
    limits: ConnectionLimits,
    trusted_peers: impl IntoIterator<Item = IpAddr>,
    shutdown: CancellationToken,
) {
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
            shutdown.clone(),
        ));
    }
    drop(listener);

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

async fn serve_connection(
    http: http1::Builder,
    socket: AdmittedSocket<TcpStream>,
    standing: Arc<ConnectionStanding>,
    peer: SocketAddr,
    app: Router,
    shutdown: CancellationToken,
) {
    let service = app.map_request(move |request: Request<Incoming>| {
        let mut request = request.map(Body::new);
        request.extensions_mut().insert(ConnectInfo(peer));
        request
    });
    let connection = http
        .serve_connection(
            TokioIo::new(socket),
            ProtectingService {
                standing: Arc::clone(&standing),
                inner: TowerToHyperService::new(service),
            },
        )
        .with_upgrades();
    let served = async move {
        let mut connection = pin!(connection);
        tokio::select! {
            result = connection.as_mut() => result,
            () = shutdown.cancelled() => {
                connection.as_mut().graceful_shutdown();
                connection.await
            }
        }
    };

    tokio::select! {
        biased;
        () = standing.evicted() => tracing::trace!(%peer, "evicted a connection with no request"),
        result = served => {
            if let Err(err) = result {
                tracing::trace!(%peer, error = %err, "connection ended with an error");
            }
        }
    }
}
