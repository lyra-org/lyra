// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    fs::{
        File,
        OpenOptions,
        TryLockError,
    },
    path::{
        Path,
        PathBuf,
    },
};

use anyhow::{
    Context,
    Result,
    anyhow,
};

use crate::config::{
    DbConfig,
    DbKind,
};

pub(crate) struct DbProcessLock {
    _file: File,
}

#[derive(Clone, Copy)]
pub(crate) enum LockMode {
    Blocking,
    NonBlocking,
}

pub(crate) fn acquire(config: &DbConfig, mode: LockMode) -> Result<Option<DbProcessLock>> {
    if matches!(config.kind, DbKind::Memory) {
        return Ok(None);
    }

    let path = lockfile_path_for(&config.path);
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)
        .with_context(|| format!("failed to open db lockfile at {}", path.display()))?;

    match mode {
        LockMode::Blocking => file
            .lock()
            .with_context(|| format!("failed to lock db lockfile at {}", path.display()))?,
        LockMode::NonBlocking => match file.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => {
                return Err(anyhow!(
                    "database is already in use; stop the server before running db optimize"
                ));
            }
            Err(TryLockError::Error(err)) => {
                return Err(err)
                    .with_context(|| format!("failed to lock db lockfile at {}", path.display()));
            }
        },
    }

    Ok(Some(DbProcessLock { _file: file }))
}

fn lockfile_path_for(db_path: &Path) -> PathBuf {
    let parent = db_path.parent().unwrap_or_else(|| Path::new("."));
    let file_name = db_path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "lyra.db".to_string());
    parent.join(format!(".{file_name}.lock"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DbKind;

    #[test]
    fn memory_databases_do_not_lock() -> Result<()> {
        let config = DbConfig {
            kind: DbKind::Memory,
            path: "ignored".into(),
        };

        assert!(acquire(&config, LockMode::NonBlocking)?.is_none());
        Ok(())
    }

    #[test]
    fn lockfile_lives_next_to_database() {
        let path = lockfile_path_for(Path::new("/tmp/lyra/data.agdb"));
        assert_eq!(path, PathBuf::from("/tmp/lyra/.data.agdb.lock"));
    }
}
