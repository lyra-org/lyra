// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Plugin distribution from Git forges, shared across Harmony host
//! applications. Hosts bring their own persistence and runtime lifecycle;
//! this crate owns URL parsing, forge APIs, archive fetching, and the
//! `repository.json` format.

pub mod fetch;
pub mod forge;
pub mod install;
pub mod manifest;
pub mod resolve;

pub use fetch::{
    FetchError,
    FetchedRepo,
    RefPlan,
    fetch_file,
    fetch_planned,
    fetch_repo,
    plan_refs,
};
pub use forge::{
    Forge,
    RepoSpec,
    RepoUrlError,
};
pub use install::{
    InstallError,
    install_candidate,
    uninstall_plugin,
};
pub use manifest::{
    ManifestError,
    RepositoryEntry,
    RepositoryManifest,
    Revision,
    SourceRecord,
};
pub use resolve::{
    KnownTreeHashes,
    ListedPlugin,
    PluginCandidate,
    RepositoryListing,
    ResolveError,
    ResolvedRepository,
    list_plugins,
    resolve_planned,
    resolve_plugins,
};

#[cfg(test)]
pub(crate) mod testutil;
