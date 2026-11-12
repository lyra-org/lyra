// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Discarding the files of removed covers. The write that removes a cover record lists its file
//! in the database, and the file leaves the list only once it is handled here, so a server that
//! stops first finds it listed on its next start.

use std::{
    io::ErrorKind,
    path::Path,
    sync::atomic::{
        AtomicBool,
        Ordering,
    },
};

use anyhow::{
    Context,
    Result,
};
use lyra_db::{
    DbAccess,
    DbAsync,
    covers::discards::DiscardedCover,
};

use crate::services::STATE;

/// The listed files handled under one read of the database.
const BATCH: usize = 256;

/// The passes over a generation's listed cover files, run one at a time.
#[derive(Default)]
pub(crate) struct CoverDiscards {
    /// Whether a pass is waiting to start. It reads the list once it starts, so it also handles
    /// whatever is listed meanwhile.
    queued: AtomicBool,
    running: tokio::sync::Mutex<()>,
}

/// Discards the listed cover files in the background, after the write `db` belongs to: called
/// with the database a write that may have removed cover records used, and at startup.
pub(crate) fn discard_listed_cover_files(db: &impl DbAccess) {
    match lyra_db::covers::discards::any(db) {
        Ok(false) => return,
        Ok(true) => {}
        Err(err) => {
            tracing::warn!(error = %format!("{err:#}"), "failed to read listed cover files")
        }
    }
    let (Some(state), Ok(runtime)) = (STATE.get(), tokio::runtime::Handle::try_current()) else {
        return;
    };
    let generation = state.generation();
    if generation
        .cover_discards
        .queued
        .swap(true, Ordering::AcqRel)
    {
        return;
    }
    let db = state.db.get();
    let queued = Queued(generation.clone());
    runtime.spawn(async move {
        let discards = &generation.cover_discards;
        let _running = discards.running.lock().await;
        drop(queued);
        if let Err(err) = discard_listed(&db, &super::configured_covers_root()).await {
            tracing::warn!(error = %format!("{err:#}"), "failed to discard listed cover files");
        }
    });
}

/// Deletes each listed file no cover record holds, with its transformed variants, and takes the
/// files it handled off the list: those deleted, gone already, still held, or not under
/// `covers_root`. One that fails to resolve, inspect or delete stays listed for the next pass.
pub(crate) async fn discard_listed(db: &DbAsync, covers_root: &Path) -> Result<()> {
    let listed = lyra_db::covers::discards::listed(&*db.read().await)?;
    for batch in listed.chunks(BATCH) {
        let db_read = db.clone().read_owned().await;
        let batch = batch.to_vec();
        let covers_root = covers_root.to_path_buf();
        // Under the read, so no write records a cover at a file while it is deleted.
        let handled = tokio::task::spawn_blocking(move || -> Result<Vec<DiscardedCover>> {
            let mut handled = Vec::new();
            for file in batch {
                if !lyra_db::covers::holds_file(&*db_read, &file.path)? {
                    if !discard_cover_file(&covers_root, &file.path) {
                        continue;
                    }
                    super::forget_cover_transforms(&file.hash);
                }
                handled.push(file);
            }
            Ok(handled)
        })
        .await
        .context("cover discard task panicked")??;
        lyra_db::covers::discards::forget(&mut *db.write_outside_catalog().await, &handled)?;
    }
    Ok(())
}

/// Deletes a cover file Lyra wrote, and returns whether that is done with: false when it could
/// not be resolved, inspected or deleted, to try again. Only a regular file stored under the
/// covers root, whose directory resolved on disk still lies inside it, is deleted, so library
/// files and anything a symlink leads to are never touched. A file under a covers root that does
/// not resolve stays listed, since its storage may only be away.
fn discard_cover_file(covers_root: &Path, path: &str) -> bool {
    let path = Path::new(path);
    if !path.starts_with(covers_root) {
        return true;
    }
    let (Some(dir), Some(name)) = (path.parent(), path.file_name()) else {
        return true;
    };
    let Ok(root) = std::fs::canonicalize(covers_root) else {
        return false;
    };
    let dir = match std::fs::canonicalize(dir) {
        Ok(dir) => dir,
        Err(err) => return err.kind() == ErrorKind::NotFound,
    };
    if !dir.starts_with(&root) {
        return true;
    }
    let target = dir.join(name);
    match std::fs::symlink_metadata(&target) {
        Ok(meta) if meta.file_type().is_file() => {}
        Ok(_) => return true,
        Err(err) if err.kind() == ErrorKind::NotFound => return true,
        Err(err) => {
            tracing::warn!(path = %target.display(), error = %err, "failed to inspect discarded cover file");
            return false;
        }
    }
    match std::fs::remove_file(&target) {
        Ok(()) => true,
        Err(err) if err.kind() == ErrorKind::NotFound => true,
        Err(err) => {
            tracing::warn!(path = %target.display(), error = %err, "failed to remove discarded cover file");
            false
        }
    }
}

/// Marks a pass as queued until it starts, or until it is dropped unstarted with its runtime, so
/// a pass that never ran does not hold back the next.
struct Queued(std::sync::Arc<crate::services::state::GenerationState>);

impl Drop for Queued {
    fn drop(&mut self) {
        self.0.cover_discards.queued.store(false, Ordering::Release);
    }
}

/// Waits until no pass of the current generation is queued or running.
#[cfg(test)]
pub(crate) async fn settled() {
    let generation = STATE.generation();
    let discards = &generation.cover_discards;
    loop {
        let running = discards.running.lock().await;
        if !discards.queued.load(Ordering::Acquire) {
            return;
        }
        drop(running);
        tokio::task::yield_now().await;
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::PathBuf,
    };

    use agdb::{
        DbAny,
        DbId,
    };
    use lyra_db::{
        Cover,
        covers::CoverSource,
        fixtures::insert_release,
        test_db::new_test_db,
    };

    use super::*;

    fn record(db: &mut DbAny, owner: DbId, source: CoverSource, path: &Path) -> anyhow::Result<()> {
        db.transaction_mut(|t| {
            lyra_db::covers::upsert(
                t,
                owner,
                source,
                Cover {
                    db_id: None,
                    id: lyra_db::nanoid(),
                    path: path.to_string_lossy().into_owned(),
                    mime_type: "image/png".to_string(),
                    hash: lyra_db::nanoid(),
                    blurhash: None,
                    provider_id: None,
                },
            )
        })?;
        Ok(())
    }

    fn written(path: PathBuf) -> anyhow::Result<PathBuf> {
        fs::create_dir_all(path.parent().expect("a cover file has a directory"))?;
        fs::write(&path, b"cover")?;
        Ok(path)
    }

    #[tokio::test]
    async fn the_listed_files_go_but_for_those_a_record_still_holds() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let covers_root = root.path().join("covers");
        let discarded = written(covers_root.join("1/provider-a.png"))?;
        let shared = written(covers_root.join("2/provider-b.png"))?;
        let library_cover = written(root.path().join("library/Album/cover.png"))?;
        let gone = covers_root.join("4/provider-c.png");

        let mut db = new_test_db()?;
        let mut removed = Vec::new();
        for (path, source) in [
            (&discarded, CoverSource::Provider),
            (&shared, CoverSource::Provider),
            (&library_cover, CoverSource::Local),
            (&gone, CoverSource::Provider),
        ] {
            let release = insert_release(&mut db, "Removed")?;
            record(&mut db, release, source, path)?;
            removed.push(release);
        }
        let keeper = insert_release(&mut db, "Kept")?;
        record(&mut db, keeper, CoverSource::Provider, &shared)?;
        lyra_db::metadata::cascade_remove_entities(&mut db, &removed)?;
        assert_eq!(lyra_db::covers::discards::listed(&db)?.len(), 4);

        let db = DbAsync::new(db);
        discard_listed(&db, &covers_root).await?;

        assert!(!discarded.exists());
        assert!(shared.is_file(), "another record still holds it");
        assert!(library_cover.is_file(), "a library file is never deleted");
        assert!(lyra_db::covers::discards::listed(&*db.read().await)?.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn a_file_that_fails_to_delete_stays_listed() -> anyhow::Result<()> {
        use std::os::unix::fs::PermissionsExt;

        let root = tempfile::tempdir()?;
        let covers_root = root.path().join("covers");
        let locked = written(covers_root.join("1/provider-a.png"))?;
        let dir = locked.parent().expect("a cover file has a directory");
        let mut db = new_test_db()?;
        let release = insert_release(&mut db, "Removed")?;
        record(&mut db, release, CoverSource::Provider, &locked)?;
        lyra_db::metadata::cascade_remove_entities(&mut db, &[release])?;
        let db = DbAsync::new(db);

        fs::set_permissions(dir, fs::Permissions::from_mode(0o555))?;
        let deletable = fs::File::create(dir.join("probe")).is_ok();
        let first = discard_listed(&db, &covers_root).await;
        let still_listed = lyra_db::covers::discards::any(&*db.read().await)?;
        fs::set_permissions(dir, fs::Permissions::from_mode(0o755))?;
        first?;
        if deletable {
            // Running as root, every directory is writable.
            return Ok(());
        }
        assert!(locked.is_file());
        assert!(still_listed);

        discard_listed(&db, &covers_root).await?;
        assert!(!locked.exists());
        assert!(!lyra_db::covers::discards::any(&*db.read().await)?);
        Ok(())
    }

    #[tokio::test]
    async fn a_file_that_cannot_be_inspected_stays_listed() -> anyhow::Result<()> {
        use std::os::unix::fs::PermissionsExt;

        let root = tempfile::tempdir()?;
        let covers_root = root.path().join("covers");
        let hidden = written(covers_root.join("1/provider-a.png"))?;
        let dir = hidden.parent().expect("a cover file has a directory");
        let mut db = new_test_db()?;
        let release = insert_release(&mut db, "Removed")?;
        record(&mut db, release, CoverSource::Provider, &hidden)?;
        lyra_db::metadata::cascade_remove_entities(&mut db, &[release])?;
        let db = DbAsync::new(db);

        fs::set_permissions(dir, fs::Permissions::from_mode(0o644))?;
        let searchable = hidden.symlink_metadata().is_ok();
        let first = discard_listed(&db, &covers_root).await;
        let still_listed = lyra_db::covers::discards::any(&*db.read().await)?;
        fs::set_permissions(dir, fs::Permissions::from_mode(0o755))?;
        first?;
        if searchable {
            // Running as root, every directory is searchable.
            return Ok(());
        }
        assert!(hidden.is_file());
        assert!(still_listed);

        discard_listed(&db, &covers_root).await?;
        assert!(!hidden.exists());
        assert!(!lyra_db::covers::discards::any(&*db.read().await)?);
        Ok(())
    }

    #[tokio::test]
    async fn files_stay_listed_while_the_covers_root_is_missing() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let covers_root = root.path().join("covers");
        let away = covers_root.join("1/provider-a.png");
        let mut db = new_test_db()?;
        let release = insert_release(&mut db, "Removed")?;
        record(&mut db, release, CoverSource::Provider, &away)?;
        lyra_db::metadata::cascade_remove_entities(&mut db, &[release])?;
        let db = DbAsync::new(db);

        discard_listed(&db, &covers_root).await?;
        assert!(lyra_db::covers::discards::any(&*db.read().await)?);

        let returned = written(away)?;
        discard_listed(&db, &covers_root).await?;
        assert!(!returned.exists());
        assert!(!lyra_db::covers::discards::any(&*db.read().await)?);
        Ok(())
    }

    #[tokio::test]
    async fn a_write_that_removes_covers_has_their_files_discarded_after_it() -> anyhow::Result<()>
    {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let cover = written(super::super::configured_covers_root().join("1/provider-a.png"))?;
        let release = {
            let mut db = STATE.db.write().await;
            let release = insert_release(&mut db, "Removed")?;
            record(&mut db, release, CoverSource::Provider, &cover)?;
            release
        };

        {
            let mut db = STATE.db.write().await;
            lyra_db::metadata::cascade_remove_entities(&mut db, &[release])?;
            discard_listed_cover_files(&*db);
            assert!(
                cover.is_file(),
                "nothing is deleted while the write holds the database"
            );
        }
        settled().await;

        assert!(!cover.exists());
        assert!(!lyra_db::covers::discards::any(&*STATE.db.read().await)?);
        Ok(())
    }

    #[test]
    fn a_pass_dropped_before_it_starts_does_not_hold_back_the_next() -> anyhow::Result<()> {
        let (mut db, _guard) = crate::services::testing::publishing_test_db()?;
        let release = insert_release(&mut db, "Removed")?;
        record(
            &mut db,
            release,
            CoverSource::Provider,
            Path::new("/nonexistent/covers/1/provider-a.png"),
        )?;
        lyra_db::metadata::cascade_remove_entities(&mut db, &[release])?;

        let runtime = tokio::runtime::Builder::new_current_thread().build()?;
        {
            let _entered = runtime.enter();
            discard_listed_cover_files(&db);
        }
        drop(runtime);

        assert!(
            !STATE
                .generation()
                .cover_discards
                .queued
                .load(Ordering::Acquire)
        );
        Ok(())
    }

    #[test]
    fn discarding_never_touches_files_outside_the_covers_root() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let covers_root = root.path().join("covers");
        fs::create_dir_all(&covers_root)?;
        let outside = root.path().join("library");
        fs::create_dir_all(&outside)?;
        let library_cover = outside.join("cover.png");
        fs::write(&library_cover, b"cover")?;

        let escaping = covers_root.join("..").join("library").join("cover.png");
        assert!(discard_cover_file(
            &covers_root,
            &library_cover.to_string_lossy()
        ));
        assert!(discard_cover_file(
            &covers_root,
            &escaping.to_string_lossy()
        ));

        // A directory symlink under the covers root that leads outside it.
        std::os::unix::fs::symlink(&outside, covers_root.join("linked"))?;
        assert!(discard_cover_file(
            &covers_root,
            &covers_root
                .join("linked")
                .join("cover.png")
                .to_string_lossy(),
        ));
        // A file symlink under the covers root.
        std::os::unix::fs::symlink(&library_cover, covers_root.join("manual-link.png"))?;
        assert!(discard_cover_file(
            &covers_root,
            &covers_root.join("manual-link.png").to_string_lossy(),
        ));

        assert!(library_cover.is_file());
        assert!(
            covers_root
                .join("manual-link.png")
                .symlink_metadata()
                .is_ok()
        );
        Ok(())
    }

    #[test]
    fn discarding_works_under_a_covers_root_with_dot_dot() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        fs::create_dir_all(root.path().join("data"))?;
        let covers_root = root.path().join("data").join("..").join("covers");
        let dir = covers_root.join("1");
        fs::create_dir_all(&dir)?;
        let stored = dir.join("manual-x.png");
        fs::write(&stored, b"cover")?;

        assert!(discard_cover_file(&covers_root, &stored.to_string_lossy()));
        assert!(!stored.exists());
        assert!(discard_cover_file(
            &covers_root,
            &dir.join("missing.png").to_string_lossy()
        ));
        Ok(())
    }
}
