// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Values needed before the database opens: the listening port, the data
//! directory, and the database location. Resolved env > file > default.

use anyhow::{
    Context,
    Result,
    anyhow,
};
use serde::Deserialize;
use std::{
    env,
    ffi::OsString,
    path::{
        Path,
        PathBuf,
    },
};

use super::{
    ConfigFile,
    non_empty_path,
};

pub(crate) const DEFAULT_PORT: u16 = 4746;
const LYRA_DATA_DIR_ENV: &str = "LYRA_DATA_DIR";
const LYRA_DB_DIR_ENV: &str = "LYRA_DB_DIR";
const LYRA_PORT_ENV: &str = "LYRA_PORT";
const DEFAULT_DATA_DIR: &str = "data";
const DEFAULT_DB_FILE_NAME: &str = "lyra.db";
const DEFAULT_COVERS_DIR_NAME: &str = "covers";

/// Boot-class environment overrides. Read once from the process environment;
/// tests construct it directly so they never touch real variables.
#[derive(Default)]
pub(crate) struct BootEnv {
    pub(crate) data_dir: Option<OsString>,
    pub(crate) db_dir: Option<OsString>,
    pub(crate) port: Option<OsString>,
}

impl BootEnv {
    pub(crate) fn from_process() -> Self {
        Self {
            data_dir: env::var_os(LYRA_DATA_DIR_ENV),
            db_dir: env::var_os(LYRA_DB_DIR_ENV),
            port: env::var_os(LYRA_PORT_ENV),
        }
    }
}

#[derive(Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DbKind {
    Memory,
    File,
    #[default]
    Mmap,
}

impl DbKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::File => "file",
            Self::Mmap => "mmap",
        }
    }
}

#[derive(Clone)]
pub(crate) struct DbConfig {
    pub(crate) kind: DbKind,
    pub(crate) path: PathBuf,
}

impl Default for DbConfig {
    fn default() -> Self {
        Self {
            kind: DbKind::default(),
            path: PathBuf::from(DEFAULT_DB_FILE_NAME),
        }
    }
}

#[derive(Clone)]
pub(crate) struct BootConfig {
    pub(crate) port: u16,
    pub(crate) data_dir: PathBuf,
    pub(crate) db: DbConfig,
}

impl Default for BootConfig {
    fn default() -> Self {
        Self {
            port: DEFAULT_PORT,
            data_dir: PathBuf::from(DEFAULT_DATA_DIR),
            db: DbConfig::default(),
        }
    }
}

impl BootConfig {
    /// Resolves boot values without touching the filesystem, so subcommands
    /// that only inspect paths leave no directories behind.
    pub(crate) fn resolve(file: &ConfigFile, env: BootEnv) -> Result<Self> {
        let mut boot = Self::default();
        if let Some(port) = file.port {
            boot.port = port;
        }
        apply_port_override(&mut boot, env.port)?;
        boot.data_dir = data_dir_path(env.data_dir)?;
        if let Some(db) = &file.db {
            if let Some(kind) = db.kind {
                boot.db.kind = kind;
            }
            if let Some(path) = &db.path {
                boot.db.path = path.clone();
            }
        }
        resolve_db_path(&mut boot.db, &boot.data_dir, env.db_dir);
        Ok(boot)
    }

    /// Creates the data directory and the database directory. Only the
    /// serving path calls this; fail fast when either cannot be a directory.
    pub(crate) fn ensure_directories(&self) -> Result<()> {
        std::fs::create_dir_all(&self.data_dir).with_context(|| {
            format!(
                "{LYRA_DATA_DIR_ENV} points to '{}' but it could not be created as a directory",
                self.data_dir.display()
            )
        })?;

        if matches!(self.db.kind, DbKind::Memory) {
            return Ok(());
        }
        if let Some(db_dir) = self.db.path.parent() {
            std::fs::create_dir_all(db_dir).with_context(|| {
                format!(
                    "database directory '{}' ({LYRA_DB_DIR_ENV} or db.path) could not be created as a directory",
                    db_dir.display()
                )
            })?;
        }
        Ok(())
    }

    pub(crate) fn default_covers_path(&self) -> PathBuf {
        self.data_dir.join(DEFAULT_COVERS_DIR_NAME)
    }
}

fn apply_port_override(boot: &mut BootConfig, raw: Option<OsString>) -> Result<()> {
    let Some(raw) = raw else {
        return Ok(());
    };
    let value = raw.to_string_lossy();
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Ok(());
    }

    boot.port = trimmed
        .parse::<u16>()
        .map_err(|err| anyhow!("invalid {LYRA_PORT_ENV} value '{trimmed}': {err}"))?;
    Ok(())
}

fn data_dir_path(raw: Option<OsString>) -> Result<PathBuf> {
    let path = non_empty_path(raw).unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIR));
    if path.is_absolute() {
        return Ok(path);
    }

    let cwd = env::current_dir().context("failed to resolve the current working directory")?;
    Ok(cwd.join(path))
}

/// Memory-kind paths are pure identifiers and stay as written: agdb's
/// `DbMemory::new` would otherwise load a stale on-disk file of that name.
fn resolve_db_path(db: &mut DbConfig, data_dir: &Path, db_dir: Option<OsString>) {
    if matches!(db.kind, DbKind::Memory) {
        return;
    }

    let db_dir = non_empty_path(db_dir).unwrap_or_else(|| data_dir.to_path_buf());
    if db.path.is_relative() {
        db.path = db_dir.join(&db.path);
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BootConfig,
        BootEnv,
        DEFAULT_PORT,
        DbKind,
        apply_port_override,
        data_dir_path,
        resolve_db_path,
    };
    use crate::config::{
        ConfigFile,
        temp_dir,
    };
    use std::{
        ffi::OsString,
        path::PathBuf,
    };

    #[test]
    fn db_dir_env_resolves_relative_db_paths() -> anyhow::Result<()> {
        let db_dir = temp_dir("db-dir")?;
        let mut boot = BootConfig::default();
        boot.db.kind = DbKind::Mmap;
        boot.db.path = PathBuf::from("custom.db");

        resolve_db_path(
            &mut boot.db,
            &boot.data_dir,
            Some(db_dir.path().to_path_buf().into_os_string()),
        );

        assert_eq!(boot.db.path, db_dir.path().join("custom.db"));
        Ok(())
    }

    #[test]
    fn db_dir_env_preserves_absolute_db_paths() -> anyhow::Result<()> {
        let db_dir = temp_dir("db-dir")?;
        let mut boot = BootConfig::default();
        boot.db.kind = DbKind::Mmap;
        boot.db.path = PathBuf::from("/var/lib/lyra/custom.db");

        resolve_db_path(
            &mut boot.db,
            &boot.data_dir,
            Some(db_dir.path().to_path_buf().into_os_string()),
        );

        assert_eq!(boot.db.path, PathBuf::from("/var/lib/lyra/custom.db"));
        Ok(())
    }

    #[test]
    fn db_dir_env_ignores_empty_values() {
        let data_dir = PathBuf::from("/data");
        let mut boot = BootConfig::default();

        resolve_db_path(&mut boot.db, &data_dir, Some(OsString::new()));

        assert_eq!(boot.db.path, PathBuf::from("/data/lyra.db"));
    }

    #[test]
    fn db_dir_defaults_to_data_dir() -> anyhow::Result<()> {
        let data_dir = temp_dir("data-dir")?;
        let mut boot = BootConfig::default();

        resolve_db_path(&mut boot.db, data_dir.path(), None);

        assert_eq!(boot.db.path, data_dir.path().join("lyra.db"));
        Ok(())
    }

    #[test]
    fn db_dir_env_takes_precedence_over_data_dir() -> anyhow::Result<()> {
        let data_dir = temp_dir("data-dir")?;
        let db_dir = temp_dir("db-dir")?;
        let mut boot = BootConfig::default();

        resolve_db_path(
            &mut boot.db,
            data_dir.path(),
            Some(db_dir.path().to_path_buf().into_os_string()),
        );

        assert_eq!(boot.db.path, db_dir.path().join("lyra.db"));
        Ok(())
    }

    #[test]
    fn memory_db_paths_are_not_joined_with_data_dir() -> anyhow::Result<()> {
        let data_dir = temp_dir("data-dir")?;
        let mut boot = BootConfig::default();
        boot.db.kind = DbKind::Memory;
        boot.db.path = PathBuf::from("scratch");

        resolve_db_path(&mut boot.db, data_dir.path(), None);

        assert_eq!(boot.db.path, PathBuf::from("scratch"));
        Ok(())
    }

    #[test]
    fn port_env_overrides_file_value() -> anyhow::Result<()> {
        let mut boot = BootConfig {
            port: 5000,
            ..BootConfig::default()
        };

        apply_port_override(&mut boot, Some(OsString::from("6000")))?;
        assert_eq!(boot.port, 6000);

        apply_port_override(&mut boot, None)?;
        assert_eq!(boot.port, 6000);

        apply_port_override(&mut boot, Some(OsString::new()))?;
        assert_eq!(boot.port, 6000);
        Ok(())
    }

    #[test]
    fn port_env_rejects_invalid_values() {
        let mut boot = BootConfig::default();

        let error = apply_port_override(&mut boot, Some(OsString::from("not-a-port")))
            .expect_err("invalid port should be rejected");
        assert!(error.to_string().contains("LYRA_PORT"));

        let error = apply_port_override(&mut boot, Some(OsString::from("70000")))
            .expect_err("out-of-range port should be rejected");
        assert!(error.to_string().contains("LYRA_PORT"));
    }

    #[test]
    fn data_dir_defaults_to_data_under_cwd() -> anyhow::Result<()> {
        let resolved = data_dir_path(None)?;

        assert_eq!(resolved, std::env::current_dir()?.join("data"));
        assert_eq!(data_dir_path(Some(OsString::new()))?, resolved);
        Ok(())
    }

    #[test]
    fn data_dir_env_relative_values_resolve_under_cwd() -> anyhow::Result<()> {
        let resolved = data_dir_path(Some(OsString::from("state/lyra")))?;

        assert_eq!(resolved, std::env::current_dir()?.join("state/lyra"));
        Ok(())
    }

    #[test]
    fn boot_values_resolve_env_over_file_over_default() -> anyhow::Result<()> {
        let data_dir = temp_dir("data-dir")?;
        let file = ConfigFile::parse(r#"{"port": 5000, "db": {"kind": "file", "path": "x.db"}}"#)?;

        let defaults = BootConfig::resolve(
            &ConfigFile::default(),
            BootEnv {
                data_dir: Some(data_dir.path().as_os_str().to_owned()),
                ..BootEnv::default()
            },
        )?;
        assert_eq!(defaults.port, DEFAULT_PORT);
        assert!(matches!(defaults.db.kind, DbKind::Mmap));
        assert_eq!(defaults.db.path, data_dir.path().join("lyra.db"));

        let from_file = BootConfig::resolve(
            &file,
            BootEnv {
                data_dir: Some(data_dir.path().as_os_str().to_owned()),
                ..BootEnv::default()
            },
        )?;
        assert_eq!(from_file.port, 5000);
        assert!(matches!(from_file.db.kind, DbKind::File));
        assert_eq!(from_file.db.path, data_dir.path().join("x.db"));

        let from_env = BootConfig::resolve(
            &file,
            BootEnv {
                data_dir: Some(data_dir.path().as_os_str().to_owned()),
                db_dir: Some(data_dir.path().join("db").into_os_string()),
                port: Some(OsString::from("6000")),
            },
        )?;
        assert_eq!(from_env.port, 6000);
        assert_eq!(from_env.db.path, data_dir.path().join("db").join("x.db"));
        Ok(())
    }

    #[test]
    fn resolve_does_not_create_directories() -> anyhow::Result<()> {
        let parent = temp_dir("untouched")?;
        let data_dir = parent.path().join("data");
        let db_dir = parent.path().join("db");

        let boot = BootConfig::resolve(
            &ConfigFile::default(),
            BootEnv {
                data_dir: Some(data_dir.clone().into_os_string()),
                db_dir: Some(db_dir.clone().into_os_string()),
                port: None,
            },
        )?;

        assert_eq!(boot.data_dir, data_dir);
        assert_eq!(boot.db.path, db_dir.join("lyra.db"));
        assert!(!data_dir.exists());
        assert!(!db_dir.exists());
        Ok(())
    }

    #[test]
    fn ensure_directories_creates_data_and_db_dirs() -> anyhow::Result<()> {
        let parent = temp_dir("data-dir")?;
        let data_dir = parent.path().join("nested").join("data");
        let db_dir = parent.path().join("db");
        let mut boot = BootConfig {
            data_dir: data_dir.clone(),
            ..BootConfig::default()
        };
        boot.db.path = db_dir.join("lyra.db");

        boot.ensure_directories()?;

        assert!(data_dir.is_dir());
        assert!(db_dir.is_dir());
        Ok(())
    }

    #[test]
    fn ensure_directories_rejects_file_at_data_dir() -> anyhow::Result<()> {
        let parent = temp_dir("data-dir")?;
        let path = parent.path().join("data");
        std::fs::write(&path, "")?;
        let boot = BootConfig {
            data_dir: path,
            ..BootConfig::default()
        };

        let error = boot
            .ensure_directories()
            .expect_err("file at data dir should be rejected");

        assert!(error.to_string().contains("LYRA_DATA_DIR"));
        Ok(())
    }

    #[test]
    fn ensure_directories_rejects_file_at_db_dir() -> anyhow::Result<()> {
        let parent = temp_dir("db-dir")?;
        let file = parent.path().join("file");
        std::fs::write(&file, "")?;
        let mut boot = BootConfig {
            data_dir: parent.path().join("data"),
            ..BootConfig::default()
        };
        boot.db.path = file.join("lyra.db");

        let error = boot
            .ensure_directories()
            .expect_err("file at db directory should be rejected");

        assert!(error.to_string().contains("database directory"));
        Ok(())
    }
}
