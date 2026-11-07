// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    io,
    pin::Pin,
    task::{
        Context,
        Poll,
    },
};

use axum::body::Body;
use bytes::Bytes;
use futures::Stream;
use tokio::task::coop;

/// A response body of `stream`, whose failure aborts the response without taking the chunks
/// before it along.
///
/// hyper drops what it holds unwritten when a body fails, and writes only once the body has
/// nothing ready, so a failure ready right behind a chunk would lose that chunk. The failure
/// waits out a pending poll instead, one with coop budget left, as a task out of budget writes
/// nothing. Bytes a full socket still refuses at that point are lost, as nothing tells a body
/// how far hyper has written.
pub(crate) fn streamed_body(
    stream: impl Stream<Item = io::Result<Bytes>> + Send + 'static,
) -> Body {
    Body::from_stream(FlushedBeforeFailure {
        stream: Box::pin(stream),
        unflushed: false,
        failure: None,
    })
}

struct FlushedBeforeFailure<S> {
    stream: Pin<Box<S>>,
    /// A chunk went out with no pending poll since that could flush it.
    unflushed: bool,
    failure: Option<io::Error>,
}

impl<S> FlushedBeforeFailure<S> {
    fn note_pending(&mut self) {
        if coop::has_budget_remaining() {
            self.unflushed = false;
        }
    }

    fn wait_for_flush<T>(&mut self, cx: &Context<'_>) -> Poll<T> {
        self.note_pending();
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

impl<S: Stream<Item = io::Result<Bytes>>> Stream for FlushedBeforeFailure<S> {
    type Item = io::Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.failure.is_some() {
            return if this.unflushed {
                this.wait_for_flush(cx)
            } else {
                Poll::Ready(this.failure.take().map(Err))
            };
        }
        let polled = this.stream.as_mut().poll_next(cx);
        match polled {
            Poll::Ready(Some(Ok(_))) => this.unflushed = true,
            Poll::Ready(Some(Err(failure))) if this.unflushed => {
                this.failure = Some(failure);
                return this.wait_for_flush(cx);
            }
            Poll::Pending => this.note_pending(),
            Poll::Ready(_) => {}
        }
        polled
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use tokio::io::{
        AsyncReadExt,
        AsyncWriteExt,
    };

    use super::*;

    async fn response_of(
        body: impl Fn() -> Body + Clone + Send + Sync + 'static,
    ) -> Result<String> {
        let app = axum::Router::new().fallback(move || {
            let body = body.clone();
            async move { body() }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        tokio::spawn(async move { axum::serve(listener, app).await });

        let mut stream = tokio::net::TcpStream::connect(address).await?;
        stream
            .write_all(b"GET / HTTP/1.1\r\nhost: test\r\nconnection: close\r\n\r\n")
            .await?;
        let mut raw = Vec::new();
        stream.read_to_end(&mut raw).await?;
        Ok(String::from_utf8_lossy(&raw).into_owned())
    }

    #[tokio::test]
    async fn a_failure_ready_behind_chunks_leaves_them_on_the_wire() -> Result<()> {
        let raw = response_of(|| {
            streamed_body(futures::stream::iter([
                Ok(Bytes::from_static(b"first")),
                Ok(Bytes::from_static(b"second")),
                Err(io::Error::other("the body failed")),
            ]))
        })
        .await?;

        assert!(raw.starts_with("HTTP/1.1 200"), "{raw}");
        assert!(raw.contains("first") && raw.contains("second"), "{raw}");
        assert!(!raw.contains("\r\n0\r\n"), "{raw}");
        Ok(())
    }

    #[tokio::test]
    async fn a_failure_that_spends_the_coop_budget_leaves_chunks_on_the_wire() -> Result<()> {
        let raw = response_of(|| {
            let mut sent = false;
            streamed_body(futures::stream::poll_fn(move |cx| {
                if !std::mem::replace(&mut sent, true) {
                    return Poll::Ready(Some(Ok(Bytes::from_static(b"first"))));
                }
                while coop::has_budget_remaining() {
                    if let Poll::Ready(restore) = coop::poll_proceed(cx) {
                        restore.made_progress();
                    }
                }
                Poll::Ready(Some(Err(io::Error::other("the body failed"))))
            }))
        })
        .await?;

        assert!(raw.starts_with("HTTP/1.1 200"), "{raw}");
        assert!(raw.contains("first"), "{raw}");
        assert!(!raw.contains("\r\n0\r\n"), "{raw}");
        Ok(())
    }
}
