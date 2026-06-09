// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Plugin distribution from Git forges, shared across Harmony host
//! applications. Hosts bring their own persistence and runtime lifecycle;
//! this crate owns URL parsing, forge APIs, archive fetching, and the
//! `repository.json` format.

pub mod fetch;
pub mod manifest;

pub use fetch::{
    FetchError,
    FetchedRepo,
    Forge,
    RepoSpec,
    fetch_repo,
};
pub use manifest::{
    ManifestError,
    RepositoryEntry,
    RepositoryManifest,
    SourceRecord,
};

#[cfg(test)]
pub(crate) mod testutil;
