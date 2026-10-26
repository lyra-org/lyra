// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Progressive transcodes: the encodes behind streamed and downloaded audio, and the short-lived
//! on-disk cache they write into.

pub(crate) mod cache;

/// Empties the transcode cache directory left by a previous run and starts the sweeper for the
/// current generation's cache.
pub(crate) async fn initialize() {
    let dir = crate::STATE.boot.get().transcode_cache_dir();
    match tokio::fs::remove_dir_all(&dir).await {
        Ok(()) => tracing::info!(path = %dir.display(), "emptied the transcode cache"),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => tracing::warn!(
            path = %dir.display(),
            error = %err,
            "failed to empty the transcode cache"
        ),
    }
    cache::spawn_sweeper(&crate::STATE.generation().transcode_cache);
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn startup_empties_the_cache_directory() -> anyhow::Result<()> {
        let _guard = crate::testing::runtime_test_lock().await;
        crate::testing::init_default_test_state()?;
        let dir = crate::STATE.boot.get().transcode_cache_dir();
        std::fs::create_dir_all(&dir)?;
        std::fs::write(dir.join("leftover-1.part"), b"partial")?;
        std::fs::write(dir.join("leftover.mp3"), b"finished")?;

        super::initialize().await;

        assert!(!dir.exists());
        Ok(())
    }
}
