// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Plugin distribution from Git forges, shared across Harmony host
//! applications. Hosts bring their own persistence and runtime lifecycle;
//! this crate owns URL parsing, forge APIs, archive fetching, and the
//! `repository.json` and `versions.json` formats. Hosts pass in their own
//! HTTP client, which must send a descriptive User-Agent: forge APIs such as
//! GitHub's reject requests without one.

pub mod fetch;
pub mod forge;
pub mod install;
pub mod manifest;
pub mod releases;
pub mod resolve;

pub use fetch::{
    FetchError,
    FetchedRepo,
    RefPlan,
    UnpackBudget,
    fetch_file,
    plan_refs,
};
pub use forge::{
    Forge,
    RateLimited,
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
    SourceRecord,
};
pub use releases::ReleaseError;
pub use resolve::{
    ListedPlugin,
    Offer,
    PluginCandidate,
    PluginError,
    PluginSource,
    RejectedPlugin,
    RepositoryListing,
    ResolveError,
    ResolvedRepository,
    list_plugins,
    resolve_listed,
    resolve_plugins,
};

#[cfg(test)]
pub(crate) mod testutil;
