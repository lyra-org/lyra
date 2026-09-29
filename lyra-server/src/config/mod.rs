// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use anyhow::Result;
use std::{
    ffi::OsString,
    path::PathBuf,
};

mod boot;
mod file;
pub(crate) mod storage;

pub(crate) use boot::{
    BootConfig,
    BootEnv,
    DbConfig,
    DbKind,
};
pub(crate) use file::{
    ConfigFile,
    FileSettings,
    LibraryFile,
};

fn non_empty_path(raw: Option<OsString>) -> Option<PathBuf> {
    raw.map(PathBuf::from)
        .filter(|path| !path.as_os_str().is_empty())
}

/// Everything read at startup: the boot values plus the file they came from.
/// The runtime config is resolved later, once the database is open.
pub(crate) struct LoadedConfig {
    pub(crate) boot: BootConfig,
    pub(crate) file: ConfigFile,
}

/// Locates and parses the config file and resolves boot values. Touches no
/// directories; the serving path creates them via
/// [`BootConfig::ensure_directories`].
pub(crate) fn load() -> Result<LoadedConfig> {
    let file = ConfigFile::load()?.unwrap_or_default();
    let boot = BootConfig::resolve(&file, BootEnv::from_process())?;
    Ok(LoadedConfig { boot, file })
}

#[cfg(test)]
pub(super) fn temp_dir(label: &str) -> std::io::Result<tempfile::TempDir> {
    tempfile::Builder::new()
        .prefix(&format!("lyra-{label}-"))
        .tempdir()
}
