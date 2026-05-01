// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Cross-process exclusion for the on-disk agdb file. agdb does not enforce
//! its own file lock; we hold an OS-level advisory lock on a sidecar dotfile
//! `<db_dir>/.<db_name>.lock`. `Memory` databases skip locking.

use std::{
    fs::File,
    io,
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
use fs4::fs_std::FileExt;

use crate::config::{
    DbConfig,
    DbKind,
};

/// RAII guard. Drop releases the OS advisory lock — including on `kill -9`.
pub(crate) struct DbProcessLock {
    /// Load-bearing: `flock` is bound to this fd; drop releases the lock.
    #[allow(dead_code)]
    file: File,
    #[cfg(test)]
    path: PathBuf,
}

impl DbProcessLock {
    #[cfg(test)]
    pub(crate) fn lockfile_path(&self) -> &Path {
        &self.path
    }
}

#[derive(Clone, Copy)]
pub(crate) enum LockMode {
    /// Block until acquired (server).
    Blocking,
    /// Fail-fast if held (CLI).
    NonBlocking,
}

/// Acquire the lock per `config.kind`; `Ok(None)` for in-memory.
pub(crate) fn acquire(config: &DbConfig, mode: LockMode) -> Result<Option<DbProcessLock>> {
    match config.kind {
        DbKind::Memory => Ok(None),
        DbKind::File | DbKind::Mmap => {
            let path = lockfile_path_for(&config.path);
            warn_if_network_filesystem(path.parent().unwrap_or_else(|| Path::new(".")));
            let file = open_lockfile(&path)?;
            acquire_or_probe(&file, mode, &path)?;
            #[cfg(test)]
            {
                Ok(Some(DbProcessLock { file, path }))
            }
            #[cfg(not(test))]
            {
                let _ = path;
                Ok(Some(DbProcessLock { file }))
            }
        }
    }
}

/// `<db_dir>/.<db_filename>.lock` — dotfile, co-located with agdb's `.{filename}` WAL.
fn lockfile_path_for(db_path: &Path) -> PathBuf {
    let parent = db_path.parent().unwrap_or_else(|| Path::new("."));
    let file_name = db_path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "lyra.db".to_string());
    parent.join(format!(".{file_name}.lock"))
}

#[cfg(unix)]
fn open_lockfile(path: &Path) -> Result<File> {
    use std::os::unix::fs::OpenOptionsExt;

    use rustix::fs::OFlags;

    // `O_NOFOLLOW`: refuse if the final component is a symlink, so a co-tenant
    // can't redirect the lockfile inode by pre-creating the dotfile.
    let flags = OFlags::NOFOLLOW.bits() as i32;
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .mode(0o600)
        .custom_flags(flags)
        .open(path)
        .with_context(|| format!("failed to open db lockfile at {}", path.display()))
}

#[cfg(not(unix))]
fn open_lockfile(path: &Path) -> Result<File> {
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(path)
        .with_context(|| format!("failed to open db lockfile at {}", path.display()))
}

fn held_error(path: &Path) -> anyhow::Error {
    anyhow!(
        "db lockfile {} is held by another process; refusing to proceed",
        path.display()
    )
}

fn acquire_or_probe(file: &File, mode: LockMode, path: &Path) -> Result<()> {
    match mode {
        LockMode::Blocking => FileExt::lock_exclusive(file)
            .with_context(|| format!("failed to acquire blocking db lock at {}", path.display())),
        LockMode::NonBlocking => match FileExt::try_lock_exclusive(file) {
            Ok(true) => Ok(()),
            Ok(false) => Err(held_error(path)),
            Err(err) if would_block(&err) => Err(held_error(path)),
            Err(err) => Err(anyhow::Error::from(err)
                .context(format!("failed to probe db lock at {}", path.display()))),
        },
    }
}

fn would_block(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::WouldBlock | io::ErrorKind::ResourceBusy
    )
}

/// `flock`/`LockFileEx` on network filesystems can silently no-op. We warn
/// rather than refuse — some self-hosters knowingly run on a NAS — so
/// corruption reports are triageable.
#[cfg(target_os = "linux")]
fn warn_if_network_filesystem(parent: &Path) {
    use rustix::fs::statfs;

    // Magic numbers from `man 2 statfs` / linux/magic.h.
    const NFS_SUPER_MAGIC: i64 = 0x6969;
    const SMB_SUPER_MAGIC: i64 = 0x517B;
    const CIFS_MAGIC_NUMBER: i64 = 0xFF53_4D42;
    const FUSE_SUPER_MAGIC: i64 = 0x6573_5546;
    const SMB2_MAGIC_NUMBER: i64 = 0xFE53_4D42;

    let stat = match statfs(parent) {
        Ok(stat) => stat,
        Err(err) => {
            // Same probe-fail-silently anti-pattern as the disk-full guard;
            // log so "not on NFS" stays distinguishable from "couldn't tell."
            tracing::debug!(
                error = %err,
                path = %parent.display(),
                "fs-type probe failed; cannot determine if db lives on a network filesystem"
            );
            return;
        }
    };
    let raw = stat.f_type as i64;
    let label = match raw {
        NFS_SUPER_MAGIC => Some("nfs"),
        SMB_SUPER_MAGIC | CIFS_MAGIC_NUMBER | SMB2_MAGIC_NUMBER => Some("cifs/smb"),
        FUSE_SUPER_MAGIC => Some("fuse"),
        _ => None,
    };
    if let Some(label) = label {
        tracing::warn!(
            filesystem = label,
            path = %parent.display(),
            "db lives on a network filesystem; advisory locks may not be honoured across hosts"
        );
    }
}

#[cfg(not(target_os = "linux"))]
fn warn_if_network_filesystem(_parent: &Path) {
    // statfs(2) magic-number sniffing is Linux-specific.
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    fn temp_db_path(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "lyra-process-lock-{}-{}-{}.agdb",
            label,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock drift")
                .as_nanos()
        ))
    }

    #[test]
    fn lockfile_path_is_dotfile_sibling() {
        let db_path = PathBuf::from("/tmp/dir/example.agdb");
        let lock_path = lockfile_path_for(&db_path);
        assert_eq!(lock_path, PathBuf::from("/tmp/dir/.example.agdb.lock"));
    }

    #[test]
    fn memory_kind_skips_lock() -> Result<()> {
        let config = DbConfig {
            kind: DbKind::Memory,
            path: temp_db_path("memory"),
        };
        let lock = acquire(&config, LockMode::NonBlocking)?;
        assert!(lock.is_none());
        Ok(())
    }

    #[test]
    fn second_acquire_fails_nonblocking() -> Result<()> {
        let path = temp_db_path("contention");
        let config = DbConfig {
            kind: DbKind::File,
            path,
        };
        let first =
            acquire(&config, LockMode::NonBlocking)?.expect("file kind must yield a lock guard");
        let second = acquire(&config, LockMode::NonBlocking);
        assert!(
            second.is_err(),
            "second non-blocking acquire must fail while first is held"
        );

        let lockfile = first.lockfile_path().to_path_buf();
        drop(first);
        let _ = std::fs::remove_file(&lockfile);
        let _ = std::fs::remove_file(&config.path);
        Ok(())
    }

    #[test]
    fn release_on_drop_allows_reacquire() -> Result<()> {
        let path = temp_db_path("reacquire");
        let config = DbConfig {
            kind: DbKind::File,
            path,
        };
        let first =
            acquire(&config, LockMode::NonBlocking)?.expect("file kind must yield a lock guard");
        let lockfile = first.lockfile_path().to_path_buf();
        drop(first);

        let second =
            acquire(&config, LockMode::NonBlocking)?.expect("re-acquire after drop should succeed");
        drop(second);

        let _ = std::fs::remove_file(&lockfile);
        let _ = std::fs::remove_file(&config.path);
        Ok(())
    }
}
