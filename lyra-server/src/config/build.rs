// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

/// Identity of the build running the server, supplied by the binary so that
/// a new commit relinks the binary instead of recompiling this crate.
#[derive(Clone, Debug, Default)]
pub struct BuildInfo {
    /// Short commit hash, or `unknown` when the build could not resolve one.
    pub commit_hash: String,
    /// The release this build was cut from; pins the default plugin
    /// repository to the matching ref.
    pub release_tag: Option<String>,
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
