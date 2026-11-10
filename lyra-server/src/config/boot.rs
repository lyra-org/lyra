// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Values needed before the database opens: the listening port, its TLS
//! files, the data directory, the database location, and whether claiming the
//! server takes a setup code. Resolved env > file > default.

use anyhow::{
    Context,
    Result,
    anyhow,
    bail,
};
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
use lyra_db::bootstrap::{
    DbConfig,
    DbKind,
};

pub(crate) const DEFAULT_PORT: u16 = 4746;
const LYRA_DATA_DIR_ENV: &str = "LYRA_DATA_DIR";
const LYRA_DB_DIR_ENV: &str = "LYRA_DB_DIR";
const LYRA_PORT_ENV: &str = "LYRA_PORT";
const LYRA_REQUIRE_SETUP_CODE_ENV: &str = "LYRA_REQUIRE_SETUP_CODE";
const LYRA_TLS_CERT_ENV: &str = "LYRA_TLS_CERT";
const LYRA_TLS_KEY_ENV: &str = "LYRA_TLS_KEY";
const DEFAULT_DATA_DIR: &str = "data";
const TRANSCODE_CACHE_DIR_NAME: &str = "transcodes";
const HLS_DIR_NAME: &str = "hls";
const COVER_CACHE_DIR_NAME: &str = "cover-transforms";

/// Boot-class environment overrides. Read once from the process environment;
/// tests construct it directly so they never touch real variables.
#[derive(Default)]
pub(crate) struct BootEnv {
    pub(crate) data_dir: Option<OsString>,
    pub(crate) db_dir: Option<OsString>,
    pub(crate) port: Option<OsString>,
    pub(crate) require_setup_code: Option<OsString>,
    pub(crate) tls_cert: Option<OsString>,
    pub(crate) tls_key: Option<OsString>,
}

impl BootEnv {
    pub(crate) fn from_process() -> Self {
        Self {
            data_dir: env::var_os(LYRA_DATA_DIR_ENV),
            db_dir: env::var_os(LYRA_DB_DIR_ENV),
            port: env::var_os(LYRA_PORT_ENV),
            require_setup_code: env::var_os(LYRA_REQUIRE_SETUP_CODE_ENV),
            tls_cert: env::var_os(LYRA_TLS_CERT_ENV),
            tls_key: env::var_os(LYRA_TLS_KEY_ENV),
        }
    }
}

#[derive(Clone)]
pub(crate) struct BootConfig {
    pub(crate) port: u16,
    /// Set when the port serves HTTPS.
    pub(crate) tls: Option<TlsPaths>,
    pub(crate) data_dir: PathBuf,
    pub(crate) db: DbConfig,
    /// Whether creating the first admin takes the setup code the server logs at startup.
    pub(crate) require_setup_code: bool,
}

/// The PEM certificate chain and private key the port serves HTTPS with.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TlsPaths {
    pub(crate) cert: PathBuf,
    pub(crate) key: PathBuf,
}

impl Default for BootConfig {
    fn default() -> Self {
        Self {
            port: DEFAULT_PORT,
            tls: None,
            data_dir: PathBuf::from(DEFAULT_DATA_DIR),
            db: DbConfig::default(),
            require_setup_code: true,
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
        apply_require_setup_code_override(&mut boot, env.require_setup_code)?;
        boot.tls = resolve_tls(file, env.tls_cert, env.tls_key)?;
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

    /// Creates and checks writable storage before opening the database.
    pub(crate) fn ensure_directories(&self) -> Result<()> {
        super::storage::ensure_writable_directory(
            &self.data_dir,
            "data directory (LYRA_DATA_DIR)",
        )?;
        if !matches!(self.db.kind, DbKind::Memory)
            && let Some(db_dir) = self
                .db
                .path
                .parent()
                .filter(|path| !path.as_os_str().is_empty())
            && db_dir != self.data_dir
        {
            super::storage::ensure_writable_directory(
                db_dir,
                "database directory (LYRA_DB_DIR or db.path)",
            )?;
        }
        super::storage::ensure_writable_directory(&self.hls_dir(), "HLS directory")?;
        Ok(())
    }

    pub(crate) fn transcode_cache_dir(&self) -> PathBuf {
        self.data_dir.join(TRANSCODE_CACHE_DIR_NAME)
    }

    pub(crate) fn cover_cache_dir(&self) -> PathBuf {
        self.data_dir.join(COVER_CACHE_DIR_NAME)
    }

    pub(crate) fn hls_dir(&self) -> PathBuf {
        self.data_dir.join(HLS_DIR_NAME)
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

fn apply_require_setup_code_override(boot: &mut BootConfig, raw: Option<OsString>) -> Result<()> {
    let Some(raw) = raw else {
        return Ok(());
    };
    let value = raw.to_string_lossy();
    boot.require_setup_code = match value.trim() {
        "" => return Ok(()),
        "true" => true,
        "false" => false,
        other => bail!(
            "invalid {LYRA_REQUIRE_SETUP_CODE_ENV} value '{other}': expected 'true' or 'false'"
        ),
    };
    Ok(())
}

/// Each path resolves on its own, so the environment may replace one and leave the other to the
/// file. Relative paths stay relative to the working directory.
fn resolve_tls(
    file: &ConfigFile,
    cert: Option<OsString>,
    key: Option<OsString>,
) -> Result<Option<TlsPaths>> {
    let file = file.tls.as_ref();
    let cert = non_empty_path(cert).or_else(|| file.and_then(|tls| tls.cert.clone()));
    let key = non_empty_path(key).or_else(|| file.and_then(|tls| tls.key.clone()));
    match (cert, key) {
        (Some(cert), Some(key)) => Ok(Some(TlsPaths { cert, key })),
        (None, None) => Ok(None),
        (Some(_), None) => {
            bail!("tls.cert is set without tls.key ({LYRA_TLS_KEY_ENV}); TLS needs both")
        }
        (None, Some(_)) => {
            bail!("tls.key is set without tls.cert ({LYRA_TLS_CERT_ENV}); TLS needs both")
        }
    }
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
        TlsPaths,
        apply_port_override,
        apply_require_setup_code_override,
        data_dir_path,
        resolve_db_path,
    };
    use crate::config::{
        ConfigFile,
        temp_dir,
    };
    use lyra_db::bootstrap::DbKind;
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
    fn setup_code_is_required_unless_the_env_says_false() -> anyhow::Result<()> {
        let mut boot = BootConfig::default();
        assert!(boot.require_setup_code);

        apply_require_setup_code_override(&mut boot, Some(OsString::new()))?;
        assert!(boot.require_setup_code);

        apply_require_setup_code_override(&mut boot, Some(OsString::from("false")))?;
        assert!(!boot.require_setup_code);

        apply_require_setup_code_override(&mut boot, None)?;
        assert!(!boot.require_setup_code);

        apply_require_setup_code_override(&mut boot, Some(OsString::from(" true ")))?;
        assert!(boot.require_setup_code);
        Ok(())
    }

    #[test]
    fn setup_code_env_rejects_anything_but_true_and_false() {
        for value in ["1", "no", "TRUE", "False", "yes"] {
            let mut boot = BootConfig::default();
            let error = apply_require_setup_code_override(&mut boot, Some(OsString::from(value)))
                .expect_err("only true and false are accepted");
            assert!(error.to_string().contains("LYRA_REQUIRE_SETUP_CODE"));
        }
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
                ..BootEnv::default()
            },
        )?;
        assert_eq!(from_env.port, 6000);
        assert_eq!(from_env.db.path, data_dir.path().join("db").join("x.db"));
        Ok(())
    }

    fn tls_paths(cert: &str, key: &str) -> Option<TlsPaths> {
        Some(TlsPaths {
            cert: PathBuf::from(cert),
            key: PathBuf::from(key),
        })
    }

    #[test]
    fn tls_paths_resolve_env_over_file() -> anyhow::Result<()> {
        let file = ConfigFile::parse(r#"{"tls": {"cert": "file.crt", "key": "file.key"}}"#)?;

        assert!(
            BootConfig::resolve(&ConfigFile::default(), BootEnv::default())?
                .tls
                .is_none()
        );
        assert_eq!(
            BootConfig::resolve(&file, BootEnv::default())?.tls,
            tls_paths("file.crt", "file.key")
        );
        assert_eq!(
            BootConfig::resolve(
                &file,
                BootEnv {
                    tls_cert: Some(OsString::from("env.crt")),
                    tls_key: Some(OsString::new()),
                    ..BootEnv::default()
                },
            )?
            .tls,
            tls_paths("env.crt", "file.key")
        );
        assert_eq!(
            BootConfig::resolve(
                &ConfigFile::default(),
                BootEnv {
                    tls_cert: Some(OsString::from("env.crt")),
                    tls_key: Some(OsString::from("env.key")),
                    ..BootEnv::default()
                },
            )?
            .tls,
            tls_paths("env.crt", "env.key")
        );
        Ok(())
    }

    #[test]
    fn half_a_tls_pair_is_rejected() -> anyhow::Result<()> {
        let error = BootConfig::resolve(
            &ConfigFile::parse(r#"{"tls": {"cert": "file.crt"}}"#)?,
            BootEnv::default(),
        )
        .err()
        .expect("a certificate alone should be rejected");
        assert!(error.to_string().contains("without tls.key"), "{error}");

        let error = BootConfig::resolve(
            &ConfigFile::default(),
            BootEnv {
                tls_key: Some(OsString::from("env.key")),
                ..BootEnv::default()
            },
        )
        .err()
        .expect("a key alone should be rejected");
        assert!(error.to_string().contains("without tls.cert"), "{error}");
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
                ..BootEnv::default()
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
        assert!(boot.hls_dir().is_dir());
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
