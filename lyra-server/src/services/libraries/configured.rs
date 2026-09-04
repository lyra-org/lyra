// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::path::PathBuf;

use agdb::DbAny;
use nanoid::nanoid;

use crate::{
    STATE,
    config::{
        Config,
        LibraryConfig,
    },
    db::{
        self,
        Library,
    },
};

use super::{
    start_library_sync,
    sync_library,
};

/// Override via `library.name` in config.json on collision.
const DEFAULT_BOOTSTRAP_LIBRARY_NAME: &str = "Music";

/// Which entry point asked for the bootstrap library; selects the recovery
/// hint when the default name collides with another library.
#[derive(Clone, Copy, Debug)]
enum BootstrapSource {
    /// The dev-only config `library` block.
    Config,
    /// `serve --capture --library <dir>`.
    Capture,
}

/// Dev bootstrap: creates the config `library` block's library if it does not
/// exist and starts a background sync. No-op without the block.
pub(crate) async fn prepare_configured_library(config: &Config) -> anyhow::Result<()> {
    let Some(library) = config.library.as_ref().filter(|l| l.path.is_some()) else {
        return Ok(());
    };

    let library = find_or_create_library(library, BootstrapSource::Config).await?;
    let library_db_id = library
        .db_id
        .ok_or_else(|| anyhow::anyhow!("configured library missing db_id"))?;
    let response = start_library_sync(STATE.db.get(), library).await?;
    tracing::info!(
        library_id = library_db_id.0,
        run_id = response.run.run.id.as_deref().unwrap_or(""),
        started = response.started,
        "background library sync requested"
    );
    Ok(())
}

/// Finds or creates the library at `dir` and syncs it to completion so a
/// capture run sees its tracks. `dir` must already exist so a typo never
/// persists a library at a path that cannot be scanned.
pub(crate) async fn prepare_capture_library(dir: PathBuf) -> anyhow::Result<Library> {
    let is_dir = tokio::fs::metadata(&dir)
        .await
        .map(|meta| meta.is_dir())
        .unwrap_or(false);
    if !is_dir {
        anyhow::bail!("--library {} is not an existing directory", dir.display());
    }

    let config = LibraryConfig {
        path: Some(dir),
        ..Default::default()
    };
    let library = find_or_create_library(&config, BootstrapSource::Capture).await?;
    sync_library(&STATE.db.get(), &library).await?;
    Ok(library)
}

/// Finds the library stored for `config.path` or creates it as a system
/// library (admin-bypass-only until access is granted).
async fn find_or_create_library(
    config: &LibraryConfig,
    source: BootstrapSource,
) -> anyhow::Result<Library> {
    let path = config
        .path
        .clone()
        .ok_or_else(|| anyhow::anyhow!("bootstrap library has no path"))?;

    // Raw path preserved for symlink retargeting; canonicalize off the lock.
    let path_key = {
        let candidate = path.clone();
        tokio::task::spawn_blocking(move || db::libraries::path_key_for(&candidate))
            .await
            .map_err(|e| anyhow::anyhow!("path canonicalize task panicked: {e}"))?
    };

    let db = STATE.db.get();
    let mut db_write = db.write().await;
    find_or_create_library_locked(&mut db_write, config, path, path_key, source)
}

/// Runs under the held DB write lock. `path_key` is
/// [`db::libraries::path_key_for`] of `path`, computed off the lock.
fn find_or_create_library_locked(
    db: &mut DbAny,
    config: &LibraryConfig,
    path: PathBuf,
    path_key: String,
    source: BootstrapSource,
) -> anyhow::Result<Library> {
    let display_name = match config.name.as_deref() {
        Some(raw) => db::libraries::normalize_library_name_display(raw)
            .map_err(|e| anyhow::anyhow!("invalid library name '{raw}': {e}"))?,
        None => DEFAULT_BOOTSTRAP_LIBRARY_NAME.to_string(),
    };

    // One txn so a crash between node and edge can't orphan a Library.
    let lookup_path = path.clone();
    let lookup_key = path_key.clone();
    let outcome = db.transaction_mut(|t| -> Result<Library, db::libraries::LibraryCreateError> {
        if let Some(existing) = db::libraries::find_by_path_key(t, &lookup_key)? {
            return Ok(existing);
        }
        db::libraries::create_system(
            t,
            db::libraries::LibraryInsert {
                id: nanoid!(),
                name: display_name,
                path: lookup_path,
                path_key: lookup_key,
                language: config.language.clone(),
                country: config.country.clone(),
            },
        )
    });

    match outcome {
        Ok(library) => Ok(library),
        Err(db::libraries::LibraryCreateError::NameInUse(name)) => Err(match source {
            BootstrapSource::Config => anyhow::anyhow!(
                "library name '{name}' is already in use by a library at another path; \
                 set `library.name` in config.json to a unique value"
            ),
            BootstrapSource::Capture => anyhow::anyhow!(
                "library name '{name}' is already used by another library; \
                 pass --library with that library's directory or rename it via the API"
            ),
        }),
        Err(db::libraries::LibraryCreateError::PathInUse(conflicting_path)) => {
            // Unreachable unless `find_by_path_key`/`create` normalization diverges.
            tracing::error!(
                conflicting = %conflicting_path.display(),
                requested = %path.display(),
                path_key = %path_key,
                "bootstrap library path_key divergence"
            );
            Err(anyhow::anyhow!(
                "library path {} is already in use by another library; \
                 schema invariant violated",
                conflicting_path.display()
            ))
        }
        Err(db::libraries::LibraryCreateError::InvalidName(e)) => {
            Err(anyhow::anyhow!("invalid library name: {e}"))
        }
        Err(db::libraries::LibraryCreateError::Db(e)) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use agdb::DbAny;
    use nanoid::nanoid;

    use super::{
        BootstrapSource,
        find_or_create_library_locked,
    };
    use crate::{
        config::LibraryConfig,
        db::{
            Library,
            libraries::path_key_for,
            test_db::new_test_db,
        },
    };

    fn find_or_create_at(
        db: &mut DbAny,
        dir: &str,
        source: BootstrapSource,
    ) -> anyhow::Result<Library> {
        let path = PathBuf::from(dir);
        let path_key = path_key_for(&path);
        let config = LibraryConfig {
            path: Some(path.clone()),
            ..Default::default()
        };
        find_or_create_library_locked(db, &config, path, path_key, source)
    }

    fn unique_dir() -> String {
        format!("/tmp/lyra-bootstrap-{}", nanoid!())
    }

    #[test]
    fn find_or_create_returns_the_stored_library_for_a_known_path() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let dir = unique_dir();

        let created = find_or_create_at(&mut db, &dir, BootstrapSource::Config)?;
        assert_eq!(created.name, "Music");
        assert!(created.db_id.is_some());

        let found = find_or_create_at(&mut db, &dir, BootstrapSource::Capture)?;
        assert_eq!(found.db_id, created.db_id);
        assert_eq!(found.id, created.id);
        Ok(())
    }

    #[test]
    fn name_collision_hint_depends_on_bootstrap_source() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        find_or_create_at(&mut db, &unique_dir(), BootstrapSource::Config)?;

        let config_err = find_or_create_at(&mut db, &unique_dir(), BootstrapSource::Config)
            .expect_err("config name collision");
        let config_msg = config_err.to_string();
        assert!(config_msg.contains("'Music'"), "{config_msg}");
        assert!(config_msg.contains("config.json"), "{config_msg}");

        let capture_err = find_or_create_at(&mut db, &unique_dir(), BootstrapSource::Capture)
            .expect_err("capture name collision");
        let capture_msg = capture_err.to_string();
        assert!(capture_msg.contains("'Music'"), "{capture_msg}");
        assert!(capture_msg.contains("--library"), "{capture_msg}");
        assert!(!capture_msg.contains("config.json"), "{capture_msg}");
        Ok(())
    }
}
