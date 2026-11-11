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

use super::bootstrap::{
    DbConfig,
    DbKind,
};

/// The database's lockfile could not be opened; callers explain storage
/// permissions to the operator.
#[derive(Debug, thiserror::Error)]
#[error("failed to open db lockfile at '{}'", path.display())]
pub struct LockfileOpenError {
    pub path: PathBuf,
    #[source]
    pub source: std::io::Error,
}

pub struct DbProcessLock {
    _file: File,
}

#[derive(Clone, Copy)]
pub enum LockMode {
    Blocking,
    /// Fails instead of waiting; `command` names the CLI command in the
    /// contention message.
    NonBlocking {
        command: &'static str,
    },
}

pub fn acquire(config: &DbConfig, mode: LockMode) -> Result<Option<DbProcessLock>> {
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
        .map_err(|source| LockfileOpenError {
            path: path.clone(),
            source,
        })?;

    match mode {
        LockMode::Blocking => match file.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => {
                tracing::warn!(
                    path = %path.display(),
                    "database is in use by another Lyra process; waiting for it to exit"
                );
                file.lock()
                    .with_context(|| format!("failed to lock db lockfile at {}", path.display()))?;
            }
            Err(TryLockError::Error(err)) => {
                return Err(err)
                    .with_context(|| format!("failed to lock db lockfile at {}", path.display()));
            }
        },
        LockMode::NonBlocking { command } => match file.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => {
                return Err(anyhow!(
                    "database is already in use; stop the server before running `lyra {command}`"
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
    use std::{
        sync::{
            Arc,
            atomic::{
                AtomicBool,
                Ordering,
            },
            mpsc::{
                self,
                Receiver,
                Sender,
            },
        },
        time::Duration,
    };

    use super::*;
    use crate::bootstrap::DbKind;

    #[test]
    fn memory_databases_do_not_lock() -> Result<()> {
        let config = DbConfig {
            kind: DbKind::Memory,
            path: "ignored".into(),
        };

        assert!(
            acquire(
                &config,
                LockMode::NonBlocking {
                    command: "db optimize"
                }
            )?
            .is_none()
        );
        Ok(())
    }

    /// How long a test waits for another thread before failing.
    const DEADLINE: Duration = Duration::from_secs(10);

    fn file_config(dir: &Path) -> DbConfig {
        DbConfig {
            kind: DbKind::File,
            path: dir.join("lyra.db"),
        }
    }

    struct LogLines(Sender<String>);

    impl std::io::Write for LogLines {
        fn write(&mut self, line: &[u8]) -> std::io::Result<usize> {
            let _ = self.0.send(String::from_utf8_lossy(line).into_owned());
            Ok(line.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogLines {
        type Writer = Self;

        fn make_writer(&'a self) -> Self::Writer {
            Self(self.0.clone())
        }
    }

    struct Waiter {
        logs: Receiver<String>,
        /// Whether `released` was set by the time the lock was taken.
        acquired: Receiver<Result<bool>>,
    }

    /// Takes the blocking lock on another thread, as a second server would.
    fn wait_for_lock(config: &DbConfig, released: &Arc<AtomicBool>) -> Waiter {
        let (lines, logs) = mpsc::channel();
        let (done, acquired) = mpsc::channel();
        let (config, released) = (config.clone(), released.clone());
        std::thread::spawn(move || {
            let _logs = tracing::subscriber::set_default(
                tracing_subscriber::fmt()
                    .with_ansi(false)
                    .with_writer(LogLines(lines))
                    .finish(),
            );
            let lock = acquire(&config, LockMode::Blocking);
            let _ = done.send(lock.map(|_lock| released.load(Ordering::SeqCst)));
        });
        Waiter { logs, acquired }
    }

    #[test]
    fn blocking_lock_waits_for_the_holder() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let config = file_config(dir.path());
        let holder = acquire(&config, LockMode::Blocking)?;
        let released = Arc::new(AtomicBool::new(false));
        let waiter = wait_for_lock(&config, &released);
        waiter.logs.recv_timeout(DEADLINE)?;

        released.store(true, Ordering::SeqCst);
        drop(holder);
        assert!(
            waiter.acquired.recv_timeout(DEADLINE)??,
            "the lock was taken while it was held"
        );
        Ok(())
    }

    #[test]
    fn waiting_for_the_lock_is_logged() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let config = file_config(dir.path());
        let holder = acquire(&config, LockMode::Blocking)?;
        let waiter = wait_for_lock(&config, &Arc::default());

        let line = waiter.logs.recv_timeout(DEADLINE)?;
        assert!(line.contains("WARN"), "{line}");
        assert!(
            line.contains("database is in use by another Lyra process"),
            "{line}"
        );
        let lockfile = lockfile_path_for(&config.path);
        assert!(line.contains(&lockfile.display().to_string()), "{line}");

        drop(holder);
        waiter.acquired.recv_timeout(DEADLINE)??;
        Ok(())
    }

    #[test]
    fn free_lock_is_taken_without_a_warning() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let waiter = wait_for_lock(&file_config(dir.path()), &Arc::default());
        waiter.acquired.recv_timeout(DEADLINE)??;
        // The thread, and with it the log sender, is gone once `acquired` closes.
        assert!(waiter.acquired.recv_timeout(DEADLINE).is_err());
        assert_eq!(waiter.logs.try_iter().collect::<Vec<_>>(), [""; 0]);
        Ok(())
    }

    #[test]
    fn non_blocking_lock_fails_only_while_held() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let config = file_config(dir.path());
        let command = LockMode::NonBlocking {
            command: "db optimize",
        };
        let holder = acquire(&config, LockMode::Blocking)?;
        let error = acquire(&config, command)
            .err()
            .expect("a held lock should refuse the command");
        assert_eq!(
            error.to_string(),
            "database is already in use; stop the server before running `lyra db optimize`"
        );

        drop(holder);
        assert!(acquire(&config, command)?.is_some());
        Ok(())
    }

    #[test]
    fn lockfile_lives_next_to_database() {
        let path = lockfile_path_for(Path::new("/tmp/lyra/data.agdb"));
        assert_eq!(path, PathBuf::from("/tmp/lyra/.data.agdb.lock"));
    }
}
