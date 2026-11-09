// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! A request body a handler reads only once it has decided the request may go on.

use std::convert::Infallible;

use axum::{
    body::Bytes,
    extract::{
        FromRequest,
        Request,
        rejection::BytesRejection,
    },
};

/// The body of a request, not yet read, so a caller who is refused never gets to send it.
pub(crate) struct UnreadBody(Request);

impl UnreadBody {
    /// Reads the whole body, under the limit `DefaultBodyLimit` set for the route. A body past it
    /// rejects with 413.
    pub(crate) async fn read(self) -> Result<Bytes, BytesRejection> {
        Bytes::from_request(self.0, &()).await
    }
}

impl<S: Send + Sync> FromRequest<S> for UnreadBody {
    type Rejection = Infallible;

    async fn from_request(request: Request, _: &S) -> Result<Self, Infallible> {
        Ok(Self(request))
    }
}

#[cfg(feature = "docgen")]
impl aide::operation::OperationInput for UnreadBody {
    fn operation_input(
        ctx: &mut aide::generate::GenContext,
        operation: &mut aide::openapi::Operation,
    ) {
        Bytes::operation_input(ctx, operation);
    }
}

/// A body that records being read, for tests of what a handler refuses first.
#[cfg(test)]
pub(crate) fn watched_body() -> (
    axum::body::Body,
    std::sync::Arc<std::sync::atomic::AtomicBool>,
) {
    use std::sync::{
        Arc,
        atomic::{
            AtomicBool,
            Ordering,
        },
    };

    let read = Arc::new(AtomicBool::new(false));
    let seen = Arc::clone(&read);
    let body = axum::body::Body::from_stream(futures::stream::poll_fn(move |_| {
        seen.store(true, Ordering::SeqCst);
        std::task::Poll::<Option<Result<Bytes, std::io::Error>>>::Ready(None)
    }));
    (body, read)
}
