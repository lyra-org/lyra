// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! An accepted socket, for as long as it stays open.

use std::{
    future::Future,
    io,
    pin::Pin,
    task::{
        Context,
        Poll,
    },
    time::Duration,
};

use tokio::{
    io::{
        AsyncRead,
        AsyncWrite,
        ReadBuf,
    },
    time::{
        Instant,
        Sleep,
        sleep,
    },
};

use super::gate::ConnectionPermit;

/// A socket that holds its place against the connection caps until it closes, through an upgrade
/// away from HTTP as well, and fails a write it can make no progress on for `write_stall`.
///
/// Reads have no deadline: a connection going quiet is ordinary, as while waiting on a transcode.
pub(super) struct AdmittedSocket<S> {
    inner: S,
    write_stall: Duration,
    write_deadline: Pin<Box<Sleep>>,
    write_blocked: bool,
    _permit: ConnectionPermit,
}

impl<S> AdmittedSocket<S> {
    pub(super) fn new(inner: S, write_stall: Duration, permit: ConnectionPermit) -> Self {
        Self {
            inner,
            write_stall,
            write_deadline: Box::pin(sleep(write_stall)),
            write_blocked: false,
            _permit: permit,
        }
    }

    /// Passes on the outcome of polling a write. A pending one polls the deadline itself, as a
    /// socket that never turns writable would otherwise never wake the write again.
    fn watch_write<T>(
        &mut self,
        cx: &mut Context<'_>,
        polled: Poll<io::Result<T>>,
    ) -> Poll<io::Result<T>> {
        if polled.is_ready() {
            self.write_blocked = false;
            return polled;
        }
        if !self.write_blocked {
            self.write_blocked = true;
            self.write_deadline
                .as_mut()
                .reset(Instant::now() + self.write_stall);
        }
        match self.write_deadline.as_mut().poll(cx) {
            Poll::Ready(()) => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "the socket stopped taking writes",
            ))),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for AdmittedSocket<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for AdmittedSocket<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let polled = Pin::new(&mut this.inner).poll_write(cx, buf);
        this.watch_write(cx, polled)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let polled = Pin::new(&mut this.inner).poll_write_vectored(cx, bufs);
        this.watch_write(cx, polled)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let polled = Pin::new(&mut this.inner).poll_flush(cx);
        this.watch_write(cx, polled)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use std::io::IoSlice;

    use tokio::io::{
        AsyncReadExt,
        AsyncWriteExt,
        DuplexStream,
        duplex,
    };

    use super::super::gate::ConnectionGate;
    use super::*;

    const STALL: Duration = Duration::from_secs(60);

    fn admitted<S>(inner: S) -> AdmittedSocket<S> {
        let permit = ConnectionGate::new(1, 1, [])
            .admit("203.0.113.1".parse().expect("valid ip"))
            .expect("an empty gate admits");
        AdmittedSocket::new(inner, STALL, permit)
    }

    /// A socket no write ever finishes on, and which writes vectors.
    struct Stuck;

    impl AsyncRead for Stuck {
        fn poll_read(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            _: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Pending
        }
    }

    impl AsyncWrite for Stuck {
        fn poll_write(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            _: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Pending
        }

        fn poll_write_vectored(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            _: &[IoSlice<'_>],
        ) -> Poll<io::Result<usize>> {
            Poll::Pending
        }

        fn is_write_vectored(&self) -> bool {
            true
        }

        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Pending
        }

        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test(start_paused = true)]
    async fn every_kind_of_write_fails_at_the_stall_deadline() {
        let started = Instant::now();
        let error = admitted(Stuck).write(b"x").await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert_eq!(started.elapsed(), STALL);

        let started = Instant::now();
        let error = admitted(Stuck)
            .write_vectored(&[IoSlice::new(b"x")])
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert_eq!(started.elapsed(), STALL);

        let started = Instant::now();
        let error = admitted(Stuck).flush().await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert_eq!(started.elapsed(), STALL);
    }

    #[tokio::test(start_paused = true)]
    async fn a_socket_nobody_reads_is_cut_off_once_full() {
        let (_peer, socket): (DuplexStream, _) = duplex(4);
        let mut socket = admitted(socket);
        socket.write_all(b"full").await.unwrap();

        let started = Instant::now();
        let error = socket.write_all(b"more").await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert_eq!(started.elapsed(), STALL);
    }

    #[tokio::test(start_paused = true)]
    async fn progress_resets_the_deadline() {
        let (mut peer, socket) = duplex(1);
        let mut socket = admitted(socket);
        let beat = STALL * 2 / 3;
        let reader = tokio::spawn(async move {
            let mut received = [0; 6];
            for byte in &mut received {
                tokio::time::sleep(beat).await;
                *byte = peer.read_u8().await.unwrap();
            }
            received
        });

        let started = Instant::now();
        socket.write_all(b"abcdef").await.unwrap();
        assert!(
            started.elapsed() > STALL,
            "the whole write outlasts one stall"
        );
        assert_eq!(&reader.await.unwrap(), b"abcdef");
    }

    #[tokio::test(start_paused = true)]
    async fn idle_time_between_writes_is_not_a_stall() {
        let (_peer, socket) = duplex(64);
        let mut socket = admitted(socket);
        socket.write_all(b"early").await.unwrap();
        tokio::time::sleep(STALL * 3).await;
        socket.write_all(b"late").await.unwrap();
    }

    #[test]
    fn vectored_writes_stay_available() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        let _entered = runtime.enter();
        assert!(admitted(Stuck).is_write_vectored());
    }
}
