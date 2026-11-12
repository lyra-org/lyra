// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! The blocking filesystem work of library scans and syncs, such as hashing files, listing
//! folders and looking for cover files, which reads a bounded number of items at once across the
//! server. Requests check their few files on their own threads, never waiting behind it.

use std::sync::{
    Arc,
    LazyLock,
};

use futures::{
    StreamExt,
    TryStreamExt,
};
use tokio::{
    sync::Semaphore,
    task::JoinError,
};

/// How many files or folders scans and syncs read at once. A few readers making large sequential
/// reads keep storage busy without spreading a spinning disk's head across many streams, or tying
/// up blocking threads beside streaming and image work.
const FILE_WORK_WIDTH: usize = 4;

static FILE_WORK: LazyLock<FileWork> = LazyLock::new(|| FileWork::with_width(FILE_WORK_WIDTH));

/// `work` of each item, in the items' order, each on a blocking thread once one of the server's
/// file work slots is free.
pub(crate) async fn map<T, U, F>(items: Vec<T>, work: F) -> Result<Vec<U>, JoinError>
where
    T: Send + 'static,
    U: Send + 'static,
    F: Fn(T) -> U + Send + Sync + 'static,
{
    FILE_WORK.map(items, work).await
}

/// Each item takes one of `width` slots on a blocking thread. Dropping a caller's future starts no
/// more of its items; those already running finish on their own, holding their slots.
struct FileWork {
    slots: Arc<Semaphore>,
    width: usize,
}

impl FileWork {
    fn with_width(width: usize) -> Self {
        Self {
            slots: Arc::new(Semaphore::new(width)),
            width,
        }
    }

    async fn run<U: Send + 'static>(
        &self,
        work: impl FnOnce() -> U + Send + 'static,
    ) -> Result<U, JoinError> {
        let slot = Arc::clone(&self.slots)
            .acquire_owned()
            .await
            .expect("the file work semaphore is never closed");
        tokio::task::spawn_blocking(move || {
            let _slot = slot;
            work()
        })
        .await
    }

    async fn map<T, U, F>(&self, items: Vec<T>, work: F) -> Result<Vec<U>, JoinError>
    where
        T: Send + 'static,
        U: Send + 'static,
        F: Fn(T) -> U + Send + Sync + 'static,
    {
        let work = Arc::new(work);
        // Unordered, so a slow item doesn't hold back the items after it; the order is restored
        // after.
        let mut done = futures::stream::iter(items.into_iter().enumerate())
            .map(|(index, item)| {
                let work = Arc::clone(&work);
                async move { Ok::<_, JoinError>((index, self.run(move || work(item)).await?)) }
            })
            .buffer_unordered(self.width)
            .try_collect::<Vec<_>>()
            .await?;
        done.sort_unstable_by_key(|(index, _)| *index);
        Ok(done.into_iter().map(|(_, value)| value).collect())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Mutex,
        atomic::{
            AtomicUsize,
            Ordering,
        },
    };

    use super::*;

    #[tokio::test]
    async fn concurrent_maps_share_one_width_and_keep_their_order() -> anyhow::Result<()> {
        let files = FileWork::with_width(3);
        let running = Arc::new(AtomicUsize::new(0));
        let most = Arc::new(AtomicUsize::new(0));
        let (started, mut starts) = tokio::sync::mpsc::unbounded_channel();
        let (release, released) = std::sync::mpsc::channel::<()>();
        let released = Arc::new(Mutex::new(released));
        let work = || {
            let (running, most) = (Arc::clone(&running), Arc::clone(&most));
            let (started, released) = (started.clone(), Arc::clone(&released));
            move |item: usize| {
                let now = running.fetch_add(1, Ordering::SeqCst) + 1;
                most.fetch_max(now, Ordering::SeqCst);
                started.send(()).expect("test is listening");
                released.lock().expect("gate").recv().expect("released");
                running.fetch_sub(1, Ordering::SeqCst);
                item * 2
            }
        };
        let items = (0..20).collect::<Vec<_>>();
        let maps = futures::future::join(
            files.map(items.clone(), work()),
            files.map(items.clone(), work()),
        );
        let driver = async {
            for _ in 0..3 {
                starts.recv().await.expect("an item starts");
            }
            assert_eq!(
                running.load(Ordering::SeqCst),
                3,
                "the width holds across maps"
            );
            for _ in 0..40 {
                release.send(()).expect("an item waits");
            }
        };
        let ((first, second), ()) = futures::future::join(maps, driver).await;

        let doubled = items.iter().map(|item| item * 2).collect::<Vec<_>>();
        assert_eq!(first?, doubled);
        assert_eq!(second?, doubled);
        assert_eq!(most.load(Ordering::SeqCst), 3);
        Ok(())
    }
}
