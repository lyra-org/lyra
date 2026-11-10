// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! What the server was started with from outside its database.

use std::path::PathBuf;

use lyra_db::bootstrap::DbConfig;

/// The values the server was started with, resolved before the database opened.
#[derive(Clone)]
pub(crate) struct Boot {
    pub(crate) port: u16,
    /// Set when the port serves HTTPS itself.
    pub(crate) tls: Option<TlsFiles>,
    pub(crate) data_dir: PathBuf,
    pub(crate) db: DbConfig,
    /// Where plugins are installed and discovered.
    pub(crate) plugins_dir: PathBuf,
    /// Where finished and in-progress transcodes are kept. Server-owned scratch space: emptied at
    /// startup and created on first use.
    pub(crate) transcode_cache_dir: PathBuf,
    /// Where transformed covers are kept between runs. Server-owned: created on first use, and
    /// nothing outside it is touched.
    pub(crate) cover_cache_dir: PathBuf,
    /// Where HLS jobs write their segments. Server-owned: its startup purge and cleanup touch
    /// nothing outside it.
    pub(crate) hls_dir: PathBuf,
    /// Whether creating the first admin takes the setup code.
    pub(crate) require_setup_code: bool,
    pub(crate) build: BuildInfo,
}

/// The PEM certificate chain and private key the server serves HTTPS with.
#[derive(Clone, Debug)]
pub(crate) struct TlsFiles {
    pub(crate) cert: PathBuf,
    pub(crate) key: PathBuf,
}

/// Identity of the build running the server, supplied by the binary so that
/// a new commit relinks the binary instead of recompiling this crate.
#[derive(Clone, Debug, Default)]
pub struct BuildInfo {
    /// Short commit hash, or `unknown` when the build could not resolve one.
    pub commit_hash: String,
}

impl BuildInfo {
    /// The `User-Agent` for every outbound request the server makes.
    pub(crate) fn user_agent(&self) -> String {
        let version = env!("CARGO_PKG_VERSION");
        if self.commit_hash.is_empty() {
            format!("Lyra/{version} (blue@spook.rip)")
        } else {
            format!("Lyra/{version}-{} (blue@spook.rip)", self.commit_hash)
        }
    }
}
