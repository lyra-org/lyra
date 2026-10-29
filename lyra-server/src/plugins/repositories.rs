// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::{
        BTreeSet,
        HashMap,
    },
    path::Path,
    sync::LazyLock,
    time::{
        Duration,
        SystemTime,
        UNIX_EPOCH,
    },
};

use agdb::DbAny;
use harmony_core::plugin::{
    PLUGIN_CONFIG_FILENAME,
    PluginManifest,
};
use harmony_repository::{
    FetchError,
    InstallError,
    ListedPlugin,
    PluginCandidate,
    RateLimited,
    RejectedPlugin,
    RepoSpec,
    RepositoryListing,
    ResolveError,
    ResolvedRepository,
    SourceRecord,
    UnpackBudget,
    install_candidate,
    list_plugins,
    manifest::SOURCE_RECORD_FILENAME,
    resolve_listed,
    uninstall_plugin,
};
use nanoid::nanoid;
use semver::Version;
use tokio::sync::{
    Mutex,
    MutexGuard,
};

use crate::services::STATE;
use crate::services::boot::BuildInfo;
use lyra_db::plugin_repositories as repo_db;

const DEFAULT_ORIGIN: &str = "https://git.lyra.pub/lyra/lyra?forge=gitlab";

/// The client for one repository operation, identified as `build`.
fn http_client(build: &BuildInfo) -> Result<reqwest::Client, PluginRepoError> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    reqwest::Client::builder()
        .timeout(Duration::from_secs(120))
        .user_agent(build.user_agent())
        .build()
        .map_err(|error| PluginRepoError::Internal(error.into()))
}

/// The running server's build, for operations reached through the API.
fn server_build() -> BuildInfo {
    STATE.boot.get().build.clone()
}

/// Installs and uninstalls mutate `plugins_dir`; serialize them so staged
/// trees and runtime reloads never interleave. Every mutating path holds
/// the guard from its first filesystem write through the end of
/// `reload_runtime`, and takes it exactly once: the tokio mutex is not
/// reentrant, so helpers that write to disk expect the caller to hold it.
/// Repository resolution (network) always happens before the guard.
static MUTATION_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

/// Subscribes the official repository once per database, on its default
/// branch.
pub(crate) fn seed_default(db: &mut DbAny) -> anyhow::Result<()> {
    let manifest = harmony_repository::RepositoryManifest::parse(
        include_str!("../../../repository.json"),
        None,
    )?;
    repo_db::seed_default(
        db,
        &repo_db::PluginRepository {
            db_id: None,
            id: nanoid!(),
            origin: DEFAULT_ORIGIN.to_string(),
            name: manifest.name,
            description: manifest.description,
            git_ref: None,
            commit: None,
            refreshed_at_ms: None,
        },
    )
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum PluginRepoError {
    #[error("{0}")]
    BadRequest(String),
    #[error("{0}")]
    NotFound(String),
    #[error("{0}")]
    Conflict(String),
    #[error("{0}")]
    BadGateway(String),
    #[error("{message}")]
    Unavailable {
        message: String,
        retry_after: Option<Duration>,
    },
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

fn map_resolve_error(error: ResolveError) -> PluginRepoError {
    match &error {
        ResolveError::Fetch(FetchError::Network { .. } | FetchError::Status { .. }) => {
            tracing::warn!(error = %error, "plugin repository download failed");
            PluginRepoError::BadGateway(
                "The plugin repository could not be downloaded. Try again shortly.".into(),
            )
        }
        ResolveError::Fetch(FetchError::RateLimited(RateLimited { retry_after, .. })) => {
            tracing::warn!(error = %error, "plugin repository host rate limited the request");
            let wait = match retry_after.map(|wait| wait.as_secs()) {
                Some(seconds) if seconds > 1 => format!("Try again in {seconds} seconds."),
                Some(_) => "Try again in a moment.".to_string(),
                None => "Try again later.".to_string(),
            };
            PluginRepoError::Unavailable {
                message: format!("The plugin repository's host is rate limiting requests. {wait}"),
                retry_after: *retry_after,
            }
        }
        ResolveError::Io(_) | ResolveError::Fetch(FetchError::Io(_)) => {
            PluginRepoError::Internal(anyhow::Error::new(error))
        }
        ResolveError::Fetch(
            FetchError::InvalidArchive { .. }
            | FetchError::DirectoryMissing { .. }
            | FetchError::UnsafeArchiveEntry { .. },
        ) => {
            tracing::warn!(error = %error, "invalid plugin repository archive");
            PluginRepoError::BadRequest(
                "The plugin repository contains an invalid or unsupported archive.".into(),
            )
        }
        ResolveError::Fetch(FetchError::ArchiveTooLarge { .. }) => PluginRepoError::BadRequest(
            "The plugin repository archive exceeds the download size limit.".into(),
        ),
        ResolveError::Fetch(
            FetchError::UnpackedBytesExceeded { .. } | FetchError::UnpackedEntriesExceeded { .. },
        ) => PluginRepoError::BadRequest(
            "The plugin repository archives exceed the unpacked size limit.".into(),
        ),
        ResolveError::Fetch(FetchError::FileTooLarge { .. }) => PluginRepoError::BadRequest(
            "A plugin repository manifest exceeds the download size limit.".into(),
        ),
        ResolveError::Fetch(FetchError::RefNotFound { .. }) => PluginRepoError::BadRequest(
            "The plugin repository or requested branch could not be found.".into(),
        ),
        ResolveError::Manifest(_)
        | ResolveError::AmbiguousRoot { .. }
        | ResolveError::NotAPluginRepository { .. }
        | ResolveError::PathEntryMissing { .. }
        | ResolveError::NestedRepository { .. }
        | ResolveError::Plugin { .. } => PluginRepoError::BadRequest(error.to_string()),
    }
}

fn map_install_error(error: InstallError) -> PluginRepoError {
    match &error {
        InstallError::InvalidId { .. } => PluginRepoError::BadRequest(error.to_string()),
        InstallError::NotInstalled { .. } => PluginRepoError::NotFound(error.to_string()),
        InstallError::NotManaged { .. }
        | InstallError::OriginMismatch { .. }
        | InstallError::CaseCollision { .. } => PluginRepoError::Conflict(error.to_string()),
        InstallError::Io { .. } => PluginRepoError::Internal(anyhow::Error::new(error)),
    }
}

fn ensure_plugin_path_id(plugin_id: &str) -> Result<(), PluginRepoError> {
    let valid = !plugin_id.is_empty()
        && plugin_id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-');
    if !valid {
        return Err(PluginRepoError::BadRequest(format!(
            "invalid plugin id: {plugin_id}"
        )));
    }
    Ok(())
}

/// Whether a repository-managed plugin has a newer release to install.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum UpdateStatus {
    /// No release newer than the installed one runs on this server, or
    /// pinned.
    UpToDate,
    /// A newer release runs on this server.
    UpdateAvailable,
    /// No release the repository offers the plugin runs on this server;
    /// `requires` is the server requirement that excludes it.
    Incompatible { requires: String },
    /// The releases on offer are unknown.
    Unknown,
}

impl UpdateStatus {
    fn compare(installed: &Version, offered: &Offered) -> Self {
        match offered {
            Ok(offered) if offered.version > *installed => Self::UpdateAvailable,
            Ok(_) => Self::UpToDate,
            Err(Some(requires)) => Self::Incompatible {
                requires: requires.clone(),
            },
            Err(None) => Self::Unknown,
        }
    }
}

/// The release a repository offers one plugin on this server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OfferedRelease {
    /// The plugin's version at the release.
    pub(crate) version: Version,
    pub(crate) commit: String,
    /// The scopes the plugin declares at the release.
    pub(crate) scopes: Vec<String>,
}

/// What a repository offers one plugin on this server: the release an
/// install would choose, or, without one, the server requirement that
/// excludes this server when that is the reason.
type Offered = Result<OfferedRelease, Option<String>>;

fn listed_offer(plugin: &ListedPlugin) -> Offered {
    match &plugin.offer {
        Ok(offer) => Ok(OfferedRelease {
            version: offer.manifest.version.clone(),
            commit: offer.commit.clone(),
            scopes: offer.manifest.scopes.clone(),
        }),
        Err(error) => Err(error.required_host().map(ToString::to_string)),
    }
}

fn stored_offer(entry: &repo_db::PluginRepositoryEntry) -> Offered {
    match (&entry.version, &entry.commit, &entry.requires) {
        (Some(version), Some(commit), _) => Version::parse(version)
            .map(|version| OfferedRelease {
                version,
                commit: commit.clone(),
                scopes: entry.scopes.clone().unwrap_or_default(),
            })
            .map_err(|_| None),
        (None, _, requires) => Err(requires.clone()),
        (Some(_), None, _) => Err(None),
    }
}

/// Why an update to a newer release waits for an admin.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Hold {
    /// The release declares scopes the installed plugin does not; an
    /// update that names the release grants them.
    AddsScopes { scopes: Vec<String> },
    /// Installed plugins depend on this one at versions outside the
    /// release's; no update applies until they change.
    BreaksDependents { dependents: Vec<Dependent> },
}

/// An installed plugin whose dependency requirement excludes a release.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Dependent {
    pub(crate) id: String,
    pub(crate) requires: String,
}

impl Hold {
    /// Why updating `plugin_id` to `offered` waits, given the `installed`
    /// plugins' manifests.
    fn of(plugin_id: &str, offered: &OfferedRelease, installed: &[PluginManifest]) -> Option<Self> {
        let dependents: Vec<Dependent> = installed
            .iter()
            .filter(|manifest| manifest.id != plugin_id)
            .filter_map(|manifest| {
                let dependency = manifest.dependencies.get(plugin_id)?;
                (!dependency.version.matches(&offered.version)).then(|| Dependent {
                    id: manifest.id.clone(),
                    requires: dependency.version.to_string(),
                })
            })
            .collect();
        if !dependents.is_empty() {
            return Some(Self::BreaksDependents { dependents });
        }
        let current = installed.iter().find(|manifest| manifest.id == plugin_id);
        let scopes: Vec<String> = offered
            .scopes
            .iter()
            .filter(|scope| current.is_none_or(|manifest| !manifest.scopes.contains(scope)))
            .cloned()
            .collect();
        (!scopes.is_empty()).then_some(Self::AddsScopes { scopes })
    }
}

/// The manifests of the plugins in `plugins_dir`, as installed on disk;
/// unreadable ones are left out.
pub(crate) fn installed_manifests(plugins_dir: &Path) -> Vec<PluginManifest> {
    let Ok(entries) = std::fs::read_dir(plugins_dir) else {
        return Vec::new();
    };
    entries
        .filter_map(Result::ok)
        .filter(|entry| !entry.file_name().to_string_lossy().starts_with('.'))
        .filter_map(|entry| {
            let config = std::fs::read(entry.path().join(PLUGIN_CONFIG_FILENAME)).ok()?;
            serde_json::from_slice(&config).ok()
        })
        .collect()
}

/// Where a catalogue plugin stands relative to the plugins directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CatalogStatus {
    /// Not installed.
    Available,
    /// Installed from a repository; compared against the offered release.
    Installed(UpdateStatus),
    /// Installed without a source record, so repository tooling leaves it alone.
    Local,
}

#[derive(Debug, Clone)]
pub(crate) struct PluginPreview {
    pub(crate) id: String,
    pub(crate) name: String,
    /// The release an install would choose.
    pub(crate) version: String,
    pub(crate) description: String,
    pub(crate) scopes: Vec<String>,
    /// The commit of the release an install would choose.
    pub(crate) commit: Option<String>,
    pub(crate) status: CatalogStatus,
    /// Origin of the plugin when a multi-plugin index points at another
    /// repository; `None` when it lives in the resolved repository itself.
    pub(crate) source_origin: Option<String>,
    /// Why the plugin has no installable release, such as all of them
    /// requiring another server version. The other fields are then empty.
    pub(crate) error: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct RepositoryPreview {
    pub(crate) origin: String,
    /// The requested ref, when one was given.
    pub(crate) git_ref: Option<String>,
    pub(crate) resolved_ref: String,
    pub(crate) commit: Option<String>,
    /// From the `repository.json` index when present, else the repository
    /// name.
    pub(crate) name: String,
    pub(crate) description: String,
    pub(crate) plugins: Vec<PluginPreview>,
}

/// Lists a repository URL's plugins from their manifests, for previews.
async fn list(
    url: &str,
    git_ref: Option<&str>,
    build: &BuildInfo,
) -> Result<RepositoryListing, PluginRepoError> {
    let spec =
        RepoSpec::parse(url, git_ref).map_err(|e| PluginRepoError::BadRequest(e.to_string()))?;
    list_plugins(&http_client(build)?, &spec, &crate::plugins::plugin_host())
        .await
        .map_err(map_resolve_error)
}

fn catalog_status(installed_dir: &Path, offered: &Offered) -> CatalogStatus {
    if !installed_dir.is_dir() {
        return CatalogStatus::Available;
    }
    let record = match SourceRecord::load(installed_dir) {
        Ok(None) => return CatalogStatus::Local,
        Ok(Some(record)) => record,
        // Managed, but the release is unknowable until it is reinstalled.
        Err(_) => return CatalogStatus::Installed(UpdateStatus::Unknown),
    };
    CatalogStatus::Installed(UpdateStatus::compare(&record.version, offered))
}

/// A subscribed repository with the releases it last offered.
#[derive(Debug, Clone)]
pub(crate) struct Subscription {
    pub(crate) repository: repo_db::PluginRepository,
    pub(crate) entries: Vec<repo_db::PluginRepositoryEntry>,
}

fn load_subscriptions(db: &impl lyra_db::DbAccess) -> anyhow::Result<Vec<Subscription>> {
    repo_db::list(db)?
        .into_iter()
        .map(|repository| {
            let db_id = repository
                .db_id
                .ok_or_else(|| anyhow::anyhow!("listed repository has no db_id"))?;
            Ok(Subscription {
                entries: repo_db::entries(db, db_id)?,
                repository,
            })
        })
        .collect()
}

pub(crate) async fn list_subscriptions() -> Result<Vec<Subscription>, PluginRepoError> {
    let db = STATE.db.read().await;
    load_subscriptions(&*db).map_err(PluginRepoError::Internal)
}

/// Where an installed plugin stands against its subscribed repository.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InstalledStatus {
    pub(crate) status: UpdateStatus,
    /// The release the repository offers, when it offers one.
    pub(crate) offered: Option<OfferedRelease>,
    /// Why an available update waits for an admin.
    pub(crate) hold: Option<Hold>,
}

/// Where the installed plugin `plugin_id` stands relative to the
/// subscribed repository it was installed from, using only the releases it
/// stored and the `installed` manifests: no forge call is made. Pinned
/// plugins are always up to date. Without a subscription for the same
/// origin and ref that lists the plugin, the answer is `Unknown`.
pub(crate) fn installed_status(
    plugin_id: &str,
    record: &SourceRecord,
    subscriptions: &[Subscription],
    installed: &[PluginManifest],
) -> InstalledStatus {
    let unknown = InstalledStatus {
        status: UpdateStatus::Unknown,
        offered: None,
        hold: None,
    };
    if record.pinned {
        return InstalledStatus {
            status: UpdateStatus::UpToDate,
            ..unknown
        };
    }
    let Some(entry) = find_subscription(subscriptions, &record.origin, record.git_ref.as_deref())
        .and_then(|subscription| {
            subscription
                .entries
                .iter()
                .find(|entry| entry.plugin_id == plugin_id && entry.subpath == record.subpath)
        })
    else {
        return unknown;
    };
    let offered = stored_offer(entry);
    let status = UpdateStatus::compare(&record.version, &offered);
    let offered = offered.ok();
    let hold = offered
        .as_ref()
        .filter(|_| status == UpdateStatus::UpdateAvailable)
        .and_then(|offered| Hold::of(plugin_id, offered, installed));
    InstalledStatus {
        status,
        offered,
        hold,
    }
}

/// The entries a listing stores on its subscription: the repository's own
/// plugins, not those its index references elsewhere.
fn listing_entries(listing: &RepositoryListing) -> Vec<repo_db::PluginRepositoryEntry> {
    listing
        .plugins
        .iter()
        .filter(|plugin| plugin.source.via_repository.is_none())
        .map(|plugin| {
            let (offered, requires) = match listed_offer(plugin) {
                Ok(offered) => (Some(offered), None),
                Err(requires) => (None, requires),
            };
            repo_db::PluginRepositoryEntry {
                db_id: None,
                plugin_id: plugin.id.clone(),
                subpath: plugin.source.subpath.clone(),
                version: offered.as_ref().map(|offered| offered.version.to_string()),
                commit: offered.as_ref().map(|offered| offered.commit.clone()),
                scopes: offered.map(|offered| offered.scopes),
                requires,
            }
        })
        .collect()
}

/// The subscription plugins installed from `origin` at `git_ref` belong
/// to: the same repository as [`RepoSpec::is_same_repo`] sees it, so
/// owner and name compare case-insensitively, and the same ref.
fn find_subscription<'a>(
    subscriptions: &'a [Subscription],
    origin: &str,
    git_ref: Option<&str>,
) -> Option<&'a Subscription> {
    let spec = RepoSpec::parse(origin, None).ok();
    subscriptions.iter().find(|subscription| {
        let repository = &subscription.repository;
        repository.git_ref.as_deref() == git_ref
            && match (&spec, RepoSpec::parse(&repository.origin, None)) {
                (Some(spec), Ok(subscribed)) => spec.is_same_repo(&subscribed),
                _ => repository.origin == origin,
            }
    })
}

/// Stores a fresh listing's commit and entries on the matching
/// subscription, so update status reflects the forge without a separate
/// refresh.
fn record_listed(
    db: &mut DbAny,
    origin: &str,
    git_ref: Option<&str>,
    commit: Option<String>,
    entries: &[repo_db::PluginRepositoryEntry],
) -> Result<(), PluginRepoError> {
    let subscriptions = load_subscriptions(&*db)?;
    let Some(subscription) = find_subscription(&subscriptions, origin, git_ref) else {
        return Ok(());
    };
    let mut repository = subscription.repository.clone();
    repository.commit = commit;
    repository.refreshed_at_ms = Some(now_ms());
    repo_db::update(db, &repository, entries)?;
    Ok(())
}

fn build_preview(listing: &RepositoryListing, plugins_dir: &Path) -> RepositoryPreview {
    let plugins = listing
        .plugins
        .iter()
        .map(|plugin| {
            let source = &plugin.source;
            let offer = plugin.offer.as_ref().ok();
            let mut preview = PluginPreview {
                id: plugin.id.clone(),
                name: String::new(),
                version: String::new(),
                description: String::new(),
                scopes: Vec::new(),
                commit: offer.map(|offer| offer.commit.clone()),
                status: catalog_status(&plugins_dir.join(&plugin.id), &listed_offer(plugin)),
                source_origin: source
                    .via_repository
                    .is_some()
                    .then(|| source.origin.clone()),
                error: None,
            };
            match &plugin.offer {
                Ok(offer) => {
                    let manifest = &offer.manifest;
                    preview.name = manifest.name.clone();
                    preview.version = offer.manifest.version.to_string();
                    preview.description = manifest.description.clone();
                    preview.scopes = manifest.scopes.clone();
                }
                Err(error) => preview.error = Some(error.to_string()),
            }
            preview
        })
        .collect();

    let index = listing.index.as_ref();
    RepositoryPreview {
        origin: listing.spec.canonical_url(),
        git_ref: listing.spec.explicit_ref.clone(),
        resolved_ref: listing.git_ref.clone(),
        commit: listing.commit.clone(),
        name: index.map_or_else(
            || listing.spec.repo_name.clone(),
            |index| index.name.clone(),
        ),
        description: index
            .map(|index| index.description.clone())
            .unwrap_or_default(),
        plugins,
    }
}

/// Previews a repository URL from its manifests, without touching disk or
/// database state.
pub(crate) async fn resolve_preview(
    url: &str,
    git_ref: Option<&str>,
) -> Result<RepositoryPreview, PluginRepoError> {
    let listing = list(url, git_ref, &server_build()).await?;
    Ok(build_preview(&listing, &STATE.boot.get().plugins_dir))
}

#[derive(Debug, Clone)]
pub(crate) struct InstalledPlugin {
    pub(crate) id: String,
    pub(crate) version: String,
    pub(crate) commit: String,
    pub(crate) scopes: Vec<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct FailedInstall {
    pub(crate) id: String,
    pub(crate) error: String,
}

#[derive(Debug, Clone)]
pub(crate) struct InstallReport {
    pub(crate) installed: Vec<InstalledPlugin>,
    pub(crate) failed: Vec<FailedInstall>,
}

/// A plugin chosen from a preview, with the commit of the release it
/// showed.
#[derive(Debug, Clone)]
pub(crate) struct PreviewedPlugin {
    pub(crate) id: String,
    pub(crate) commit: String,
}

/// Which plugins an install takes from a repository.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Selection<'a> {
    /// Every plugin, at the releases offered now.
    All,
    /// The previewed plugins, each only at the release its preview showed.
    Previewed(&'a [PreviewedPlugin]),
}

/// Lists a repository URL and downloads the selected plugins' releases. A
/// previewed plugin whose offered release changed since its preview, or
/// that is no longer listed, fails on its own, returned with the resolved
/// repository. An empty selection is rejected, as is one the repository
/// lists none of, and a repository that offers nothing to install.
async fn resolve_selection(
    url: &str,
    git_ref: Option<&str>,
    selection: Selection<'_>,
    build: &BuildInfo,
) -> Result<(ResolvedRepository, Vec<FailedInstall>), PluginRepoError> {
    if matches!(selection, Selection::Previewed(plugins) if plugins.is_empty()) {
        return Err(PluginRepoError::BadRequest(
            "no plugins selected to install".to_string(),
        ));
    }
    let spec =
        RepoSpec::parse(url, git_ref).map_err(|e| PluginRepoError::BadRequest(e.to_string()))?;
    let client = http_client(build)?;
    let host = crate::plugins::plugin_host();
    let listing = list_plugins(&client, &spec, &host)
        .await
        .map_err(map_resolve_error)?;

    let mut changed = Vec::new();
    let ids = match selection {
        Selection::All => None,
        Selection::Previewed(plugins) => {
            if !plugins
                .iter()
                .any(|previewed| listing.plugins.iter().any(|p| p.id == previewed.id))
            {
                return Err(PluginRepoError::BadRequest(
                    "none of the selected plugins is provided by this repository".to_string(),
                ));
            }
            let mut ids = Vec::new();
            for previewed in plugins {
                let Some(listed) = listing.plugins.iter().find(|p| p.id == previewed.id) else {
                    changed.push(FailedInstall {
                        id: previewed.id.clone(),
                        error: format!(
                            "plugin '{}' is no longer offered by this repository since the \
                             preview",
                            previewed.id
                        ),
                    });
                    continue;
                };
                match &listed.offer {
                    Ok(offer) if offer.commit != previewed.commit => changed.push(FailedInstall {
                        id: previewed.id.clone(),
                        error: format!(
                            "plugin '{}' changed since the preview: it showed the release at \
                             commit {}, but {} at commit {} is offered now",
                            previewed.id, previewed.commit, offer.manifest.version, offer.commit
                        ),
                    }),
                    _ => ids.push(previewed.id.clone()),
                }
            }
            Some(ids)
        }
    };
    let resolved = resolve_listed(
        &client,
        listing,
        ids.as_deref(),
        &host,
        &UnpackBudget::new(),
    )
    .await
    .map_err(map_resolve_error)?;
    if resolved.candidates.is_empty() && resolved.rejected.is_empty() && changed.is_empty() {
        return Err(PluginRepoError::BadRequest(
            "repository provides no plugins to install".to_string(),
        ));
    }
    Ok((resolved, changed))
}

/// Writes the resolved candidates into `plugins_dir`, after the plugins
/// that already `failed`. Per-plugin failures don't abort siblings. The
/// caller holds `MUTATION_LOCK`.
fn install_selected(
    resolved: &ResolvedRepository,
    failed: Vec<FailedInstall>,
    plugins_dir: &Path,
) -> InstallReport {
    let mut report = InstallReport {
        installed: Vec::new(),
        failed,
    };
    report.failed.extend(rejected_failures(resolved));
    for candidate in &resolved.candidates {
        let manifest = &candidate.plugin.manifest;
        match install_candidate(candidate, plugins_dir, Some(now_rfc3339())) {
            Ok(record) => report.installed.push(InstalledPlugin {
                id: candidate.id().to_string(),
                version: manifest.version.to_string(),
                commit: record.commit,
                scopes: manifest.scopes.clone(),
            }),
            Err(error) => report.failed.push(FailedInstall {
                id: candidate.id().to_string(),
                error: error.to_string(),
            }),
        }
    }
    report
}

/// Selected plugins without an installable release, each failing on its
/// own.
fn rejected_failures(resolved: &ResolvedRepository) -> Vec<FailedInstall> {
    resolved
        .rejected
        .iter()
        .map(|plugin| FailedInstall {
            id: plugin.id.clone(),
            error: rejected_message(plugin),
        })
        .collect()
}

fn rejected_message(plugin: &RejectedPlugin) -> String {
    format!(
        "plugin at {} cannot be installed: {}",
        plugin.location, plugin.error
    )
}

/// Resolves a repository URL and installs the selected plugins into
/// `plugins_dir`. Does not reload the runtime; callers decide when the new
/// set goes live.
pub(crate) async fn install_to_disk(
    url: &str,
    git_ref: Option<&str>,
    selection: Selection<'_>,
    build: &BuildInfo,
    plugins_dir: &Path,
) -> Result<InstallReport, PluginRepoError> {
    let (resolved, changed) = resolve_selection(url, git_ref, selection, build).await?;
    let _guard = MUTATION_LOCK.lock().await;
    Ok(install_selected(&resolved, changed, plugins_dir))
}

/// Installs previewed plugins and then reloads the plugin runtime so the new
/// set goes live.
pub(crate) async fn install(
    url: &str,
    git_ref: Option<&str>,
    plugins: &[PreviewedPlugin],
) -> Result<InstallReport, PluginRepoError> {
    let (resolved, changed) =
        resolve_selection(url, git_ref, Selection::Previewed(plugins), &server_build()).await?;
    let guard = MUTATION_LOCK.lock().await;
    let report = install_selected(&resolved, changed, &STATE.boot.get().plugins_dir);
    if !report.installed.is_empty() {
        reload_runtime(guard).await?;
    }
    Ok(report)
}

/// Installs previewed plugins from a subscribed repository's origin and ref.
pub(crate) async fn install_from_repository(
    repository_id: &str,
    plugins: &[PreviewedPlugin],
) -> Result<InstallReport, PluginRepoError> {
    let record = load_repository(repository_id).await?;
    install(&record.origin, record.git_ref.as_deref(), plugins).await
}

#[derive(Debug, Clone)]
pub(crate) struct UpdatedPlugin {
    pub(crate) id: String,
    pub(crate) version: String,
    pub(crate) commit: String,
}

/// An installed plugin no release of its repository runs on this server.
#[derive(Debug, Clone)]
pub(crate) struct IncompatiblePlugin {
    pub(crate) id: String,
    pub(crate) requires: String,
}

/// An installed plugin whose newer release waits for an admin.
#[derive(Debug, Clone)]
pub(crate) struct HeldPlugin {
    pub(crate) id: String,
    pub(crate) release: OfferedRelease,
    pub(crate) hold: Hold,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct UpdateReport {
    pub(crate) updated: Vec<UpdatedPlugin>,
    pub(crate) up_to_date: Vec<String>,
    pub(crate) incompatible: Vec<IncompatiblePlugin>,
    pub(crate) held: Vec<HeldPlugin>,
    pub(crate) failed: Vec<FailedInstall>,
}

/// Which installed plugins an update takes up.
#[derive(Debug, Clone, Copy)]
pub(crate) enum UpdateSelection<'a> {
    /// Every repository-managed plugin, to newer releases that need no
    /// approval: one adding scopes is held.
    All,
    /// The named plugins, each only to the release its status showed,
    /// granting the scopes that release adds.
    Approved(&'a [PreviewedPlugin]),
}

/// Loads the source record of an installed, repository-managed plugin.
fn load_managed_record(
    plugins_dir: &Path,
    plugin_id: &str,
) -> Result<SourceRecord, PluginRepoError> {
    ensure_plugin_path_id(plugin_id)?;
    let installed_dir = plugins_dir.join(plugin_id);
    if !installed_dir.is_dir() {
        return Err(PluginRepoError::NotFound(format!(
            "plugin not found: {plugin_id}"
        )));
    }
    SourceRecord::load(&installed_dir)
        .map_err(anyhow::Error::new)?
        .ok_or_else(|| {
            PluginRepoError::Conflict(format!(
                "plugin '{plugin_id}' is local and not managed by a repository"
            ))
        })
}

/// Finds the candidate a plugin with a newer release is reinstalled from.
fn find_update<'a>(
    resolved: &'a ResolvedRepository,
    plugin_id: &str,
    record: &SourceRecord,
) -> Result<&'a PluginCandidate, PluginRepoError> {
    if let Some(rejected) = resolved
        .rejected
        .iter()
        .find(|plugin| plugin.id == plugin_id && plugin.source.subpath == record.subpath)
    {
        return Err(PluginRepoError::BadRequest(rejected_message(rejected)));
    }
    resolved
        .candidates
        .iter()
        .find(|c| c.id() == plugin_id && c.source.subpath == record.subpath)
        .ok_or_else(|| {
            PluginRepoError::Conflict(format!(
                "plugin '{plugin_id}' is no longer available at {}",
                record.location()
            ))
        })
}

/// What updating one installed plugin comes to.
enum UpdateOutcome {
    UpToDate,
    Incompatible {
        requires: String,
    },
    Held {
        release: OfferedRelease,
        hold: Hold,
    },
    Failed(String),
    /// Downloaded at its newer release, or attempted so that it fails with
    /// the reason.
    Changed,
}

/// What one origin and ref offered the plugins installed from it.
struct UpdateSource {
    /// The listing's commit and entries, for its subscription.
    commit: Option<String>,
    entries: Vec<repo_db::PluginRepositoryEntry>,
    /// By installed plugin id.
    outcomes: HashMap<String, UpdateOutcome>,
    /// The changed plugins, downloaded at their releases.
    repository: ResolvedRepository,
}

/// What updating the plugin `id`, installed at `record`, comes to when its
/// repository offers `offered`, under `selection`, by
/// [`UpdateStatus::compare`] and [`Hold::of`] over the `manifests` on disk.
fn update_outcome(
    id: &str,
    record: &SourceRecord,
    offered: Offered,
    selection: UpdateSelection<'_>,
    manifests: &[PluginManifest],
) -> UpdateOutcome {
    let release = match (UpdateStatus::compare(&record.version, &offered), offered) {
        (UpdateStatus::UpToDate, _) => return UpdateOutcome::UpToDate,
        (UpdateStatus::Incompatible { requires }, _) => {
            return UpdateOutcome::Incompatible { requires };
        }
        (UpdateStatus::UpdateAvailable, Ok(release)) => release,
        _ => return UpdateOutcome::Changed,
    };
    let approved = match selection {
        UpdateSelection::All => None,
        UpdateSelection::Approved(plugins) => plugins.iter().find(|plugin| plugin.id == id),
    };
    if let Some(approved) = approved
        && approved.commit != release.commit
    {
        return UpdateOutcome::Failed(format!(
            "plugin '{id}' changed since its status: it showed the release at commit {}, but {} \
             at commit {} is offered now",
            approved.commit, release.version, release.commit
        ));
    }
    match Hold::of(id, &release, manifests) {
        Some(Hold::AddsScopes { .. }) if approved.is_some() => UpdateOutcome::Changed,
        Some(hold) => UpdateOutcome::Held { release, hold },
        None => UpdateOutcome::Changed,
    }
}

/// Lists the repository and downloads the plugins in `installed` that it
/// offers a newer release of, by [`update_outcome`]. Plugins it does not
/// list, or whose offer is unknown, are attempted too, so their update
/// fails with the reason.
async fn resolve_update_source(
    client: &reqwest::Client,
    spec: &RepoSpec,
    installed: &[&(String, SourceRecord)],
    selection: UpdateSelection<'_>,
    manifests: &[PluginManifest],
    budget: &UnpackBudget,
) -> Result<UpdateSource, ResolveError> {
    let host = crate::plugins::plugin_host();
    let listing = list_plugins(client, spec, &host).await?;
    let outcomes: HashMap<String, UpdateOutcome> = installed
        .iter()
        .map(|(id, record)| {
            let outcome = match listing
                .plugins
                .iter()
                .find(|plugin| plugin.id == *id && plugin.source.subpath == record.subpath)
            {
                Some(plugin) => {
                    update_outcome(id, record, listed_offer(plugin), selection, manifests)
                }
                None => UpdateOutcome::Changed,
            };
            (id.clone(), outcome)
        })
        .collect();
    let changed: Vec<String> = outcomes
        .iter()
        .filter(|(_, outcome)| matches!(outcome, UpdateOutcome::Changed))
        .map(|(id, _)| id.clone())
        .collect();
    let commit = listing.commit.clone();
    let entries = listing_entries(&listing);
    let repository = resolve_listed(client, listing, Some(&changed), &host, budget).await?;
    Ok(UpdateSource {
        commit,
        entries,
        outcomes,
        repository,
    })
}

/// Ids of installed plugins that carry a source record file. Unreadable
/// records are included so they surface as per-plugin failures.
fn managed_plugin_ids(plugins_dir: &Path) -> Result<BTreeSet<String>, PluginRepoError> {
    let entries = match std::fs::read_dir(plugins_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(BTreeSet::new());
        }
        Err(error) => return Err(PluginRepoError::Internal(error.into())),
    };
    let mut ids = BTreeSet::new();
    for entry in entries {
        let entry = entry.map_err(|error| PluginRepoError::Internal(error.into()))?;
        let Ok(id) = entry.file_name().into_string() else {
            continue;
        };
        let dir = entry.path();
        if dir.is_dir() && dir.join(SOURCE_RECORD_FILENAME).is_file() {
            ids.insert(id);
        }
    }
    Ok(ids)
}

/// Updates the `selection` of installed plugins. An explicitly empty
/// selection is rejected. Pinned plugins are up to date by construction and
/// are not resolved. Each distinct origin and ref is listed once, and only
/// the plugins it offers a newer release of are downloaded and reinstalled,
/// unless that release is held; per-plugin failures don't abort siblings.
/// The runtime reloads once, and only when something was reinstalled.
pub(crate) async fn update_plugins(
    selection: UpdateSelection<'_>,
) -> Result<UpdateReport, PluginRepoError> {
    update_plugins_in(&STATE.boot.get().plugins_dir, selection).await
}

/// The origin and ref a group of installed plugins updates from.
type UpdateKey = (String, Option<String>);

async fn update_plugins_in(
    plugins_dir: &Path,
    selection: UpdateSelection<'_>,
) -> Result<UpdateReport, PluginRepoError> {
    let ids: BTreeSet<String> = match selection {
        UpdateSelection::All => managed_plugin_ids(plugins_dir)?,
        UpdateSelection::Approved([]) => {
            return Err(PluginRepoError::BadRequest(
                "no plugins selected to update".to_string(),
            ));
        }
        UpdateSelection::Approved(plugins) => {
            plugins.iter().map(|plugin| plugin.id.clone()).collect()
        }
    };

    let mut report = UpdateReport::default();
    let mut records = Vec::new();
    for id in ids {
        match load_managed_record(plugins_dir, &id) {
            Ok(record) if record.pinned => report.up_to_date.push(id),
            Ok(record) => records.push((id, record)),
            Err(error) => report.failed.push(FailedInstall {
                id,
                error: error.to_string(),
            }),
        }
    }

    let client = http_client(&server_build())?;
    let mut installed: HashMap<UpdateKey, Vec<&(String, SourceRecord)>> = HashMap::new();
    for plugin in &records {
        let (_, record) = plugin;
        installed
            .entry((record.origin.clone(), record.git_ref.clone()))
            .or_default()
            .push(plugin);
    }
    let manifests = installed_manifests(plugins_dir);
    // Every downloaded release stays on disk until the installs finish.
    let budget = UnpackBudget::new();
    let mut resolved: HashMap<UpdateKey, Result<UpdateSource, String>> = HashMap::new();
    // Hosts that rate-limited a request, with the error their later origins
    // fail with unasked.
    let mut limited_hosts: HashMap<String, String> = HashMap::new();
    for ((origin, git_ref), group) in &installed {
        let key = (origin.clone(), git_ref.clone());
        let spec = match RepoSpec::parse(origin, git_ref.as_deref()) {
            Ok(spec) => spec,
            Err(error) => {
                resolved.insert(key, Err(error.to_string()));
                continue;
            }
        };
        if let Some(error) = limited_hosts.get(&spec.host) {
            resolved.insert(key, Err(error.clone()));
            continue;
        }
        let result = resolve_update_source(&client, &spec, group, selection, &manifests, &budget)
            .await
            .map_err(|error| {
                let limited_host = match &error {
                    ResolveError::Fetch(FetchError::RateLimited(limited)) => {
                        Some(limited.host.clone())
                    }
                    _ => None,
                };
                let message = map_resolve_error(error).to_string();
                if let Some(host) = limited_host {
                    limited_hosts.insert(host, message.clone());
                }
                message
            });
        resolved.insert(key, result);
    }
    if resolved.values().any(Result::is_ok) {
        let mut db = STATE.db.write().await;
        for ((origin, git_ref), source) in &resolved {
            if let Ok(source) = source {
                record_listed(
                    &mut db,
                    origin,
                    git_ref.as_deref(),
                    source.commit.clone(),
                    &source.entries,
                )?;
            }
        }
    }

    let mut pending = Vec::new();
    for (id, record) in &records {
        let source = match &resolved[&(record.origin.clone(), record.git_ref.clone())] {
            Ok(source) => source,
            Err(error) => {
                report.failed.push(FailedInstall {
                    id: id.clone(),
                    error: error.clone(),
                });
                continue;
            }
        };
        match &source.outcomes[id] {
            UpdateOutcome::UpToDate => report.up_to_date.push(id.clone()),
            UpdateOutcome::Incompatible { requires } => {
                report.incompatible.push(IncompatiblePlugin {
                    id: id.clone(),
                    requires: requires.clone(),
                })
            }
            UpdateOutcome::Held { release, hold } => report.held.push(HeldPlugin {
                id: id.clone(),
                release: release.clone(),
                hold: hold.clone(),
            }),
            UpdateOutcome::Failed(error) => report.failed.push(FailedInstall {
                id: id.clone(),
                error: error.clone(),
            }),
            UpdateOutcome::Changed => match find_update(&source.repository, id, record) {
                Ok(candidate) => pending.push((id, candidate)),
                Err(error) => report.failed.push(FailedInstall {
                    id: id.clone(),
                    error: error.to_string(),
                }),
            },
        }
    }

    let guard = MUTATION_LOCK.lock().await;
    for (id, candidate) in pending {
        if load_managed_record(plugins_dir, id).is_err() {
            report.failed.push(FailedInstall {
                id: id.clone(),
                error: format!("plugin '{id}' was removed during update"),
            });
            continue;
        }
        match install_candidate(candidate, plugins_dir, Some(now_rfc3339())) {
            Ok(record) => report.updated.push(UpdatedPlugin {
                id: id.clone(),
                version: candidate.plugin.manifest.version.to_string(),
                commit: record.commit,
            }),
            Err(error) => report.failed.push(FailedInstall {
                id: id.clone(),
                error: error.to_string(),
            }),
        }
    }
    if !report.updated.is_empty() {
        reload_runtime(guard).await?;
    }

    Ok(report)
}

/// Uninstalls a repository-managed plugin and reloads the runtime.
pub(crate) async fn uninstall(plugin_id: &str) -> Result<(), PluginRepoError> {
    let guard = MUTATION_LOCK.lock().await;
    uninstall_plugin(&STATE.boot.get().plugins_dir, plugin_id).map_err(map_install_error)?;
    reload_runtime(guard).await
}

/// Reloads the plugin runtime from disk; the recovery step after a failed
/// reload and the way CLI installs go live without a restart.
pub(crate) async fn reload_plugins() -> Result<(), PluginRepoError> {
    reload_runtime(MUTATION_LOCK.lock().await).await
}

/// Reloads the runtime from `plugins_dir`. The reload holds `guard` to its
/// end, even when the caller is dropped, so no other mutation can change
/// `plugins_dir` while it reads it.
async fn reload_runtime(guard: MutexGuard<'static, ()>) -> Result<(), PluginRepoError> {
    tokio::spawn(async move {
        let _guard = guard;
        crate::plugins::STATE
            .generation()
            .registries
            .reload_all_plugins()
            .await
    })
    .await
    .map_err(anyhow::Error::new)
    .and_then(|result| result)
    .map_err(|error| {
        PluginRepoError::Conflict(format!(
            "plugins changed on disk but the runtime reload failed: {error:#}"
        ))
    })
}

pub(crate) async fn add_repository(
    url: &str,
    git_ref: Option<&str>,
) -> Result<(repo_db::PluginRepository, RepositoryPreview), PluginRepoError> {
    let listing = list(url, git_ref, &server_build()).await?;
    let preview = build_preview(&listing, &STATE.boot.get().plugins_dir);

    let origin = listing.spec.canonical_url();
    let mut record = repo_db::PluginRepository {
        db_id: None,
        id: nanoid!(),
        origin: origin.clone(),
        name: preview.name.clone(),
        description: preview.description.clone(),
        git_ref: listing.spec.explicit_ref.clone(),
        commit: listing.commit.clone(),
        refreshed_at_ms: Some(now_ms()),
    };

    let mut db = STATE.db.write().await;
    if repo_db::get_by_origin(&*db, &origin)
        .map_err(PluginRepoError::Internal)?
        .is_some()
    {
        return Err(PluginRepoError::Conflict(format!(
            "repository already added: {origin}"
        )));
    }
    let entries = listing_entries(&listing);
    let db_id = db
        .transaction_mut(|t| repo_db::create(t, &record, &entries))
        .map_err(PluginRepoError::Internal)?;
    record.db_id = Some(db_id);

    Ok((record, preview))
}

pub(crate) async fn list_repositories() -> Result<Vec<repo_db::PluginRepository>, PluginRepoError> {
    let db = STATE.db.read().await;
    repo_db::list(&*db).map_err(PluginRepoError::Internal)
}

async fn load_repository(
    repository_id: &str,
) -> Result<repo_db::PluginRepository, PluginRepoError> {
    let db = STATE.db.read().await;
    repo_db::get_by_id(&*db, repository_id)
        .map_err(PluginRepoError::Internal)?
        .ok_or_else(|| PluginRepoError::NotFound(format!("repository not found: {repository_id}")))
}

pub(crate) async fn refresh_repository(
    repository_id: &str,
) -> Result<(repo_db::PluginRepository, RepositoryPreview), PluginRepoError> {
    let mut record = load_repository(repository_id).await?;

    let listing = list(&record.origin, record.git_ref.as_deref(), &server_build()).await?;
    let preview = build_preview(&listing, &STATE.boot.get().plugins_dir);

    record.name = preview.name.clone();
    record.description = preview.description.clone();
    record.commit = listing.commit.clone();
    record.refreshed_at_ms = Some(now_ms());
    {
        let mut db = STATE.db.write().await;
        repo_db::update(&mut db, &record, &listing_entries(&listing))
            .map_err(PluginRepoError::Internal)?;
    }

    Ok((record, preview))
}

/// Forgets a subscription. Plugins installed from it stay on disk and
/// remain individually updatable through their source records.
pub(crate) async fn remove_repository(repository_id: &str) -> Result<(), PluginRepoError> {
    let mut db = STATE.db.write().await;
    let record = repo_db::get_by_id(&*db, repository_id)
        .map_err(PluginRepoError::Internal)?
        .ok_or_else(|| {
            PluginRepoError::NotFound(format!("repository not found: {repository_id}"))
        })?;
    let db_id = record.db_id.expect("loaded repository has db_id");
    repo_db::remove(&mut db, db_id).map_err(PluginRepoError::Internal)?;
    Ok(())
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or_default()
}

fn now_rfc3339() -> String {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        Mutex,
    };

    use harmony_repository::{
        Forge,
        manifest::SOURCE_RECORD_SCHEMA_VERSION,
    };

    use super::*;
    use crate::plugins::testing::HOST_REQUIREMENT;

    #[test]
    fn rate_limited_downloads_map_to_unavailable_with_the_forge_wait() {
        let rate_limited = |retry_after| {
            map_resolve_error(ResolveError::Fetch(FetchError::RateLimited(RateLimited {
                host: "forge.example".to_string(),
                url: "https://forge.example/archive.tar.gz".to_string(),
                retry_after,
            })))
        };

        let error = rate_limited(Some(Duration::from_secs(42)));
        let PluginRepoError::Unavailable {
            message,
            retry_after,
        } = &error
        else {
            panic!("expected unavailable: {error:?}");
        };
        assert_eq!(*retry_after, Some(Duration::from_secs(42)));
        assert!(message.ends_with("Try again in 42 seconds."), "{message}");

        let error = rate_limited(Some(Duration::from_secs(1)));
        assert!(
            error.to_string().ends_with("Try again in a moment."),
            "{error}"
        );

        let error = rate_limited(None);
        assert!(
            matches!(
                &error,
                PluginRepoError::Unavailable { message, retry_after: None }
                    if message.ends_with("Try again later.")
            ),
            "{error:?}"
        );
    }

    #[test]
    fn seeds_official_repository_on_its_default_branch() -> anyhow::Result<()> {
        let mut db = lyra_db::test_db::new_test_db()?;
        seed_default(&mut db)?;
        let repositories = repo_db::list(&db)?;
        assert_eq!(repositories.len(), 1);
        let record = &repositories[0];
        assert_eq!(record.origin, DEFAULT_ORIGIN);
        assert_eq!(record.git_ref, None);
        assert_eq!(record.commit, None);
        assert_eq!(record.refreshed_at_ms, None);
        Ok(())
    }

    #[test]
    fn default_catalog_lists_all_shipped_plugin_manifests() -> anyhow::Result<()> {
        use std::collections::BTreeSet;
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap();
        let catalog = harmony_repository::RepositoryManifest::parse(
            include_str!("../../../repository.json"),
            None,
        )?;
        let listed = catalog
            .entries
            .into_iter()
            .map(|entry| {
                let harmony_repository::RepositoryEntry::Path { path } = entry else {
                    panic!("official plugins should live in this repository");
                };
                assert!(root.join(&path).join("plugin.json").is_file());
                path
            })
            .collect::<BTreeSet<_>>();
        let shipped = std::fs::read_dir(root.join("plugins"))?
            .map(|entry| entry.map(|entry| entry.path()))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .filter(|path| path.join("plugin.json").is_file())
            .map(|path| {
                path.strip_prefix(root)
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_string()
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(listed, shipped);
        Ok(())
    }

    fn record(version: &str) -> SourceRecord {
        SourceRecord {
            schema_version: SOURCE_RECORD_SCHEMA_VERSION,
            origin: "https://github.com/o/r".into(),
            forge: Forge::GitHub,
            git_ref: None,
            version: Version::parse(version).expect("test version"),
            commit: "a".repeat(40),
            pinned: false,
            subpath: None,
            via_repository: None,
            installed_at: None,
        }
    }

    fn store_record(dir: &Path, version: &str) -> anyhow::Result<()> {
        std::fs::create_dir_all(dir)?;
        record(version).store(dir)?;
        Ok(())
    }

    #[test]
    fn load_managed_record_distinguishes_missing_local_and_managed() -> anyhow::Result<()> {
        let plugins_dir = tempfile::TempDir::new()?;
        std::fs::create_dir(plugins_dir.path().join("local"))?;
        store_record(&plugins_dir.path().join("managed"), "1.0.0")?;

        assert!(matches!(
            load_managed_record(plugins_dir.path(), "bad id"),
            Err(PluginRepoError::BadRequest(_))
        ));
        assert!(matches!(
            load_managed_record(plugins_dir.path(), "missing"),
            Err(PluginRepoError::NotFound(_))
        ));
        assert!(matches!(
            load_managed_record(plugins_dir.path(), "local"),
            Err(PluginRepoError::Conflict(_))
        ));
        let record = load_managed_record(plugins_dir.path(), "managed")?;
        assert_eq!(record.origin, "https://github.com/o/r");
        Ok(())
    }

    #[tokio::test]
    async fn updates_hold_releases_adding_scopes_until_their_commit_is_approved()
    -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::plugins::testing::init_default_test_state()?;
        lyra_db::server::ensure(&mut *STATE.db.write().await)?;
        let mut scoped: serde_json::Value = serde_json::from_str(&changed_at(HOST_REQUIREMENT))?;
        scoped["scopes"] = serde_json::json!(["lyra.metadata"]);
        let forge = catalog_forge(HOST_REQUIREMENT, HOST_REQUIREMENT, scoped.to_string()).await?;
        let plugins_dir = tempfile::TempDir::new()?;
        install_records(plugins_dir.path(), &forge.origin, &[("changed", "1.0.0")])?;
        subscribe(&forge.origin, &[]).await?;
        let r2 = "2".repeat(40);
        let adds = Hold::AddsScopes {
            scopes: vec!["lyra.metadata".into()],
        };

        let report = update_plugins_in(plugins_dir.path(), UpdateSelection::All).await?;
        let [held] = &report.held[..] else {
            panic!("expected one held plugin: {report:?}");
        };
        assert_eq!((held.id.as_str(), &held.hold), ("changed", &adds));
        assert_eq!(held.release.commit, r2);
        assert!(report.updated.is_empty() && forge.downloads().is_empty());
        let record = load_managed_record(plugins_dir.path(), "changed")?;
        let status = installed_status(
            "changed",
            &record,
            &list_subscriptions().await?,
            &installed_manifests(plugins_dir.path()),
        );
        assert_eq!(status.status, UpdateStatus::UpdateAvailable);
        assert_eq!(status.hold, Some(adds));
        assert_eq!(
            status.offered.map(|offered| offered.commit),
            Some(r2.clone())
        );

        let stale = [previewed("changed", "1")];
        let report =
            update_plugins_in(plugins_dir.path(), UpdateSelection::Approved(&stale)).await?;
        let [failed] = &report.failed[..] else {
            panic!("expected one failure: {report:?}");
        };
        assert!(
            failed.error.contains("changed since its status"),
            "{failed:?}"
        );

        let approved = [previewed("changed", "2")];
        let report =
            update_plugins_in(plugins_dir.path(), UpdateSelection::Approved(&approved)).await?;
        assert!(report.failed.is_empty(), "{report:?}");
        let updated: Vec<_> = report.updated.iter().map(|u| u.version.as_str()).collect();
        assert_eq!(updated, ["2.0.0"]);
        Ok(())
    }

    #[tokio::test]
    async fn updates_breaking_an_installed_dependent_are_held_even_when_approved()
    -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::plugins::testing::init_default_test_state()?;
        lyra_db::server::ensure(&mut *STATE.db.write().await)?;
        let forge = catalog_forge(
            HOST_REQUIREMENT,
            HOST_REQUIREMENT,
            changed_at(HOST_REQUIREMENT),
        )
        .await?;
        let plugins_dir = tempfile::TempDir::new()?;
        install_records(plugins_dir.path(), &forge.origin, &[("changed", "1.0.0")])?;
        let dependent = plugins_dir.path().join("dependent");
        std::fs::create_dir_all(&dependent)?;
        let mut manifest: serde_json::Value =
            serde_json::from_str(&manifest_json("dependent", "1.0.0", HOST_REQUIREMENT))?;
        manifest["dependencies"] = serde_json::json!({ "changed": "1" });
        std::fs::write(dependent.join(PLUGIN_CONFIG_FILENAME), manifest.to_string())?;
        let breaks = Hold::BreaksDependents {
            dependents: vec![Dependent {
                id: "dependent".into(),
                requires: "^1".into(),
            }],
        };

        let approved = [previewed("changed", "2")];
        for selection in [UpdateSelection::All, UpdateSelection::Approved(&approved)] {
            let report = update_plugins_in(plugins_dir.path(), selection).await?;
            let held: Vec<_> = report
                .held
                .iter()
                .map(|plugin| (plugin.id.as_str(), &plugin.hold))
                .collect();
            assert_eq!(held, [("changed", &breaks)], "{selection:?}");
            assert!(report.updated.is_empty(), "{report:?}");
        }
        assert!(forge.downloads().is_empty(), "{:?}", forge.downloads());
        Ok(())
    }

    #[tokio::test]
    async fn update_plugins_rejects_empty_selection() {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::plugins::testing::init_default_test_state().expect("test state");
        assert!(matches!(
            update_plugins(UpdateSelection::Approved(&[])).await,
            Err(PluginRepoError::BadRequest(_))
        ));
    }

    #[tokio::test]
    async fn update_plugins_reports_pinned_plugins_up_to_date_without_resolving()
    -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::plugins::testing::init_default_test_state()?;
        let plugins_dir = tempfile::TempDir::new()?;
        let dir = plugins_dir.path().join("pinned");
        std::fs::create_dir_all(&dir)?;
        let mut pinned = record("1.0.0");
        pinned.origin = "http://127.0.0.1:9/o/r".into();
        pinned.git_ref = Some("v1.0.0".into());
        pinned.pinned = true;
        pinned.store(&dir)?;

        let report = update_plugins_in(plugins_dir.path(), UpdateSelection::All).await?;

        assert!(report.updated.is_empty());
        assert!(report.failed.is_empty());
        assert_eq!(report.up_to_date, vec!["pinned".to_string()]);
        Ok(())
    }

    /// The commit the test catalog's `main` branch is at.
    fn tip() -> String {
        "f".repeat(40)
    }

    fn manifest_json(id: &str, version: &str, lyra: &str) -> String {
        serde_json::json!({
            "schema_version": 1,
            "id": id,
            "name": id,
            "version": version,
            "lyra": lyra,
            "description": "",
            "entrypoint": "init.luau",
            "scopes": [],
        })
        .to_string()
    }

    /// A `versions.json` listing each `(commit, lyra)` release, newest
    /// first.
    fn versions_json(releases: &[(&str, &str)]) -> String {
        let releases: Vec<_> = releases
            .iter()
            .map(|(commit, lyra)| serde_json::json!({ "commit": commit, "lyra": lyra }))
            .collect();
        serde_json::Value::Array(releases).to_string()
    }

    fn targz(files: &[(String, String)]) -> anyhow::Result<Vec<u8>> {
        let mut archive = tar::Builder::new(flate2::write::GzEncoder::new(
            Vec::new(),
            flate2::Compression::default(),
        ));
        for (path, content) in files {
            let mut header = tar::Header::new_gnu();
            header.set_size(content.len() as u64);
            header.set_mode(0o644);
            archive.append_data(&mut header, format!("repo/{path}"), content.as_bytes())?;
        }
        Ok(archive.into_inner()?.finish()?)
    }

    /// A Gitea-style forge whose `o/r` default branch `main` is at [`tip`],
    /// serving `files` there as raw files and each `(commit, files)` release
    /// as raw files and an archive. Every request is recorded.
    struct TestForge {
        origin: String,
        requests: Arc<Mutex<Vec<String>>>,
    }

    impl TestForge {
        async fn start(
            files: Vec<(String, String)>,
            releases: Vec<(String, Vec<(String, String)>)>,
        ) -> anyhow::Result<Self> {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let origin = format!("http://{}/o/r", listener.local_addr()?);
            let requests = Arc::new(Mutex::new(Vec::new()));
            let mut router = axum::Router::new()
                .route(
                    "/api/v1/repos/o/r",
                    axum::routing::get(|| async {
                        axum::Json(serde_json::json!({ "default_branch": "main" }))
                    }),
                )
                .route(
                    "/api/v1/repos/o/r/git/commits/main",
                    axum::routing::get(|| async {
                        axum::Json(serde_json::json!({ "sha": tip() }))
                    }),
                );
            for (path, content) in files {
                router = router.route(
                    &format!("/o/r/raw/commit/{}/{path}", tip()),
                    axum::routing::get(move || async move { content }),
                );
            }
            for (commit, files) in releases {
                let archive = targz(&files)?;
                for (path, content) in files {
                    router = router.route(
                        &format!("/o/r/raw/commit/{commit}/{path}"),
                        axum::routing::get(move || async move { content }),
                    );
                }
                router = router.route(
                    &format!("/o/r/archive/{commit}.tar.gz"),
                    axum::routing::get(move || async move { archive }),
                );
            }
            let router = router.layer(axum::middleware::from_fn_with_state(
                requests.clone(),
                async |axum::extract::State(requests): axum::extract::State<
                    Arc<Mutex<Vec<String>>>,
                >,
                       request: axum::extract::Request,
                       next: axum::middleware::Next| {
                    requests
                        .lock()
                        .unwrap()
                        .push(request.uri().path().to_string());
                    next.run(request).await
                },
            ));
            tokio::spawn(async move { axum::serve(listener, router).await });
            Ok(Self { origin, requests })
        }

        /// Every archive requested.
        fn downloads(&self) -> Vec<String> {
            self.requests
                .lock()
                .unwrap()
                .iter()
                .filter(|path| path.contains("archive"))
                .cloned()
                .collect()
        }
    }

    /// The `o/r` catalog of `plugins/same` and `plugins/changed`, released
    /// at `r2`, which `main` matches, to servers matching `newest_to`, then
    /// at `r1` to those matching `older_to`. `same` is at 1.0.0 in both; `changed` is at
    /// 1.0.0 in `r1`, and in `r2` has the `plugin.json` `changed`.
    async fn catalog_forge(
        newest_to: &str,
        older_to: &str,
        changed: String,
    ) -> anyhow::Result<TestForge> {
        let (r1, r2) = ("1".repeat(40), "2".repeat(40));
        let release = |changed: String| {
            let plugin = |dir: &str, manifest: String| {
                [
                    (format!("plugins/{dir}/plugin.json"), manifest),
                    (format!("plugins/{dir}/init.luau"), "return {}".to_string()),
                ]
            };
            let mut files = vec![(
                "repository.json".to_string(),
                r#"{"schema_version":1,"name":"Catalog","plugins":[{"path":"plugins/same"},{"path":"plugins/changed"}]}"#
                    .to_string(),
            )];
            files.extend(plugin(
                "same",
                manifest_json("same", "1.0.0", HOST_REQUIREMENT),
            ));
            files.extend(plugin("changed", changed));
            files
        };
        let newest = release(changed);
        let mut tip = newest.clone();
        tip.push((
            "versions.json".into(),
            versions_json(&[(&r2, newest_to), (&r1, older_to)]),
        ));
        TestForge::start(
            tip,
            vec![
                (
                    r1.clone(),
                    release(manifest_json("changed", "1.0.0", HOST_REQUIREMENT)),
                ),
                (r2.clone(), newest),
            ],
        )
        .await
    }

    /// The `plugin.json` of `changed` 2.0.0, requiring `lyra`.
    fn changed_at(lyra: &str) -> String {
        manifest_json("changed", "2.0.0", lyra)
    }

    /// Installs `(id, version)` records from `origin` as path entries.
    fn install_records(
        plugins_dir: &Path,
        origin: &str,
        installed: &[(&str, &str)],
    ) -> anyhow::Result<()> {
        for (id, version) in installed {
            let dir = plugins_dir.join(id);
            std::fs::create_dir_all(&dir)?;
            let mut managed = record(version);
            managed.origin = origin.to_string();
            managed.forge = Forge::Gitea;
            managed.subpath = Some(format!("plugins/{id}"));
            managed.store(&dir)?;
            std::fs::write(
                dir.join(PLUGIN_CONFIG_FILENAME),
                manifest_json(id, version, HOST_REQUIREMENT),
            )?;
        }
        Ok(())
    }

    async fn subscribe(origin: &str, entries: &[(&str, &str)]) -> anyhow::Result<agdb::DbId> {
        let entries: Vec<_> = entries
            .iter()
            .map(|(id, version)| repo_db::PluginRepositoryEntry {
                db_id: None,
                plugin_id: id.to_string(),
                subpath: Some(format!("plugins/{id}")),
                version: Some(version.to_string()),
                commit: Some("1".repeat(40)),
                scopes: Some(Vec::new()),
                requires: None,
            })
            .collect();
        repo_db::create(
            &mut *STATE.db.write().await,
            &repo_db::PluginRepository {
                db_id: None,
                id: "catalog".into(),
                origin: origin.to_string(),
                name: "Catalog".into(),
                description: String::new(),
                git_ref: None,
                commit: None,
                refreshed_at_ms: None,
            },
            &entries,
        )
    }

    async fn stored_versions(
        repository_db_id: agdb::DbId,
    ) -> anyhow::Result<Vec<(String, Option<String>)>> {
        let mut entries: Vec<_> = repo_db::entries(&*STATE.db.read().await, repository_db_id)?
            .into_iter()
            .map(|entry| (entry.plugin_id, entry.version))
            .collect();
        entries.sort();
        Ok(entries)
    }

    #[tokio::test]
    async fn update_plugins_reinstalls_newer_releases_and_stores_every_offered_one()
    -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::plugins::testing::init_default_test_state()?;
        lyra_db::server::ensure(&mut *STATE.db.write().await)?;
        let forge = catalog_forge(
            HOST_REQUIREMENT,
            HOST_REQUIREMENT,
            changed_at(HOST_REQUIREMENT),
        )
        .await?;
        let plugins_dir = tempfile::TempDir::new()?;
        install_records(
            plugins_dir.path(),
            &forge.origin,
            &[("same", "1.0.0"), ("changed", "1.0.0")],
        )?;
        let repository_db_id =
            subscribe(&forge.origin, &[("same", "0.1.0"), ("changed", "0.1.0")]).await?;

        let report = update_plugins_in(plugins_dir.path(), UpdateSelection::All).await?;

        assert!(report.failed.is_empty(), "{:?}", report.failed);
        assert_eq!(report.up_to_date, ["same"]);
        let updated: Vec<_> = report
            .updated
            .iter()
            .map(|u| (u.id.as_str(), u.version.as_str(), u.commit.as_str()))
            .collect();
        assert_eq!(updated, [("changed", "2.0.0", "2".repeat(40).as_str())]);
        let installed = SourceRecord::load(&plugins_dir.path().join("changed"))?.expect("record");
        assert_eq!(installed.version, Version::new(2, 0, 0));
        assert_eq!(installed.commit, "2".repeat(40));
        assert_eq!(
            forge.downloads(),
            [format!("/o/r/archive/{}.tar.gz", "2".repeat(40))],
            "one archive of the release, for the one newer plugin"
        );
        assert!(
            !plugins_dir.path().join("same").join("init.luau").exists(),
            "a plugin without a newer release is not reinstalled"
        );

        assert_eq!(
            stored_versions(repository_db_id).await?,
            [
                ("changed".into(), Some("2.0.0".into())),
                ("same".into(), Some("1.0.0".into())),
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn installs_of_releases_requiring_another_server_fail_alone() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::plugins::testing::init_default_test_state()?;
        lyra_db::server::ensure(&mut *STATE.db.write().await)?;
        let forge = catalog_forge(HOST_REQUIREMENT, HOST_REQUIREMENT, changed_at("0.1")).await?;

        let plugins_dir = tempfile::TempDir::new()?;
        let report = install_to_disk(
            &forge.origin,
            None,
            Selection::Previewed(&[previewed("same", "2"), previewed("changed", "2")]),
            &BuildInfo::default(),
            plugins_dir.path(),
        )
        .await?;
        let installed: Vec<_> = report
            .installed
            .iter()
            .map(|i| (i.id.as_str(), i.version.as_str()))
            .collect();
        assert_eq!(installed, [("same", "1.0.0")]);
        let [failed] = &report.failed[..] else {
            panic!("expected one failure: {report:?}");
        };
        assert_eq!(failed.id, "changed");
        assert!(failed.error.contains("requires lyra ^0.1"), "{failed:?}");
        Ok(())
    }

    #[tokio::test]
    async fn release_problems_read_the_same_in_previews_status_and_updates() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::plugins::testing::init_default_test_state()?;
        lyra_db::server::ensure(&mut *STATE.db.write().await)?;
        let incompatible = |requires: &str| UpdateStatus::Incompatible {
            requires: requires.to_string(),
        };
        let unknown_scope =
            changed_at(HOST_REQUIREMENT).replace(r#""scopes":[]"#, r#""scopes":["no.such.scope"]"#);
        for (newest_to, older_to, changed, expected) in [
            // No release runs here: the newest entry's requirement blocks.
            (
                ">=999.0.0",
                ">=998.0.0",
                changed_at(HOST_REQUIREMENT),
                incompatible(">=999.0.0"),
            ),
            // The chosen release's plugin.json rejects this server.
            (
                HOST_REQUIREMENT,
                HOST_REQUIREMENT,
                changed_at("0.1"),
                incompatible("^0.1"),
            ),
            // A newer release for another server is not offered at all.
            (
                ">=999.0.0",
                HOST_REQUIREMENT,
                changed_at(HOST_REQUIREMENT),
                UpdateStatus::UpToDate,
            ),
            // The release fails for another reason than this server.
            (
                HOST_REQUIREMENT,
                HOST_REQUIREMENT,
                unknown_scope.clone(),
                UpdateStatus::Unknown,
            ),
        ] {
            let forge = catalog_forge(newest_to, older_to, changed).await?;
            let plugins_dir = tempfile::TempDir::new()?;
            install_records(plugins_dir.path(), &forge.origin, &[("changed", "1.0.0")])?;
            let repository_db_id = subscribe(&forge.origin, &[]).await?;

            let preview = build_preview(
                &list(&forge.origin, None, &BuildInfo::default()).await?,
                plugins_dir.path(),
            );
            let changed = preview
                .plugins
                .iter()
                .find(|plugin| plugin.id == "changed")
                .expect("listed");
            assert_eq!(
                changed.status,
                CatalogStatus::Installed(expected.clone()),
                "{changed:?}"
            );

            let report = update_plugins_in(plugins_dir.path(), UpdateSelection::All).await?;
            assert!(report.updated.is_empty(), "{:?}", report.updated);
            let reported = match (
                &report.up_to_date[..],
                &report.incompatible[..],
                &report.failed[..],
            ) {
                ([id], [], []) if id == "changed" => UpdateStatus::UpToDate,
                ([], [plugin], []) if plugin.id == "changed" => incompatible(&plugin.requires),
                ([], [], [failed]) if failed.id == "changed" => {
                    assert!(failed.error.contains("unknown scope"), "{failed:?}");
                    UpdateStatus::Unknown
                }
                _ => panic!("{report:?}"),
            };
            assert_eq!(reported, expected);
            assert!(forge.downloads().is_empty(), "{:?}", forge.downloads());

            let record = load_managed_record(plugins_dir.path(), "changed")?;
            assert_eq!(
                installed_status("changed", &record, &list_subscriptions().await?, &[]).status,
                expected
            );
            repo_db::remove(&mut *STATE.db.write().await, repository_db_id)?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn previews_describe_the_release_rather_than_the_tip() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::plugins::testing::init_default_test_state()?;
        let release = "1".repeat(40);
        let mut tip = serde_json::from_str::<serde_json::Value>(&manifest_json(
            "foo",
            "2.0.0",
            HOST_REQUIREMENT,
        ))?;
        tip["scopes"] = serde_json::json!(["lyra.metadata"]);
        tip["description"] = serde_json::json!("the tip");
        let mut released = serde_json::from_str::<serde_json::Value>(&manifest_json(
            "foo",
            "1.0.0",
            HOST_REQUIREMENT,
        ))?;
        released["description"] = serde_json::json!("the release");
        let forge = TestForge::start(
            vec![
                ("plugin.json".into(), tip.to_string()),
                (
                    "versions.json".into(),
                    versions_json(&[(&release, HOST_REQUIREMENT)]),
                ),
            ],
            vec![(
                release.clone(),
                vec![
                    ("plugin.json".into(), released.to_string()),
                    ("init.luau".into(), "return {}".into()),
                ],
            )],
        )
        .await?;

        let preview = build_preview(
            &list(&forge.origin, None, &BuildInfo::default()).await?,
            Path::new("plugins"),
        );

        let [plugin] = &preview.plugins[..] else {
            panic!("expected one plugin: {:?}", preview.plugins);
        };
        assert_eq!(plugin.error, None);
        assert_eq!(plugin.version, "1.0.0");
        assert_eq!(plugin.commit.as_deref(), Some(release.as_str()));
        assert_eq!(plugin.description, "the release");
        assert!(
            plugin.scopes.is_empty(),
            "the scopes an install grants are the release's: {:?}",
            plugin.scopes
        );
        Ok(())
    }

    #[tokio::test]
    async fn plugins_without_releases_are_listed_with_the_reason_and_fail_to_update()
    -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::plugins::testing::init_default_test_state()?;
        let forge = TestForge::start(
            vec![
                (
                    "repository.json".into(),
                    r#"{"schema_version":1,"name":"Catalog","plugins":[{"path":"plugins/foo"}]}"#
                        .into(),
                ),
                (
                    "plugins/foo/plugin.json".into(),
                    manifest_json("foo", "1.0.0", HOST_REQUIREMENT),
                ),
            ],
            Vec::new(),
        )
        .await?;
        let plugins_dir = tempfile::TempDir::new()?;

        let preview = build_preview(
            &list(&forge.origin, None, &BuildInfo::default()).await?,
            plugins_dir.path(),
        );
        let [plugin] = &preview.plugins[..] else {
            panic!("expected one plugin: {:?}", preview.plugins);
        };
        assert_eq!(plugin.id, "foo");
        assert!(plugin.name.is_empty() && plugin.version.is_empty() && plugin.commit.is_none());
        assert_eq!(
            plugin.error.as_deref(),
            Some("no releases: the repository has no versions.json")
        );

        install_records(plugins_dir.path(), &forge.origin, &[("foo", "1.0.0")])?;
        let report = update_plugins_in(plugins_dir.path(), UpdateSelection::All).await?;
        let [failed] = &report.failed[..] else {
            panic!("expected one failure: {report:?}");
        };
        assert!(
            failed
                .error
                .ends_with("no releases: the repository has no versions.json"),
            "{failed:?}"
        );
        assert!(forge.downloads().is_empty(), "{:?}", forge.downloads());
        Ok(())
    }

    #[tokio::test]
    async fn update_checks_fail_rate_limited_release_reads_without_downloading()
    -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::plugins::testing::init_default_test_state()?;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let origin = format!("http://{}/o/r", listener.local_addr()?);
        let unrouted = Arc::new(Mutex::new(Vec::<String>::new()));
        let reset = (SystemTime::now().duration_since(UNIX_EPOCH)? + Duration::from_secs(120))
            .as_secs()
            .to_string();
        let router = axum::Router::new()
            .route(
                "/api/v1/repos/o/r",
                axum::routing::get(|| async {
                    axum::Json(serde_json::json!({ "default_branch": "main" }))
                }),
            )
            .route(
                "/api/v1/repos/o/r/git/commits/main",
                axum::routing::get(|| async { axum::Json(serde_json::json!({ "sha": tip() })) }),
            )
            .route(
                &format!("/o/r/raw/commit/{}/plugin.json", tip()),
                axum::routing::get(|| async { manifest_json("foo", "1.0.0", HOST_REQUIREMENT) }),
            )
            .route(
                &format!("/o/r/raw/commit/{}/versions.json", tip()),
                axum::routing::get(move || async move {
                    (
                        axum::http::StatusCode::FORBIDDEN,
                        [
                            ("x-ratelimit-remaining", "0".to_string()),
                            ("x-ratelimit-reset", reset),
                        ],
                    )
                }),
            )
            .fallback({
                let unrouted = unrouted.clone();
                move |uri: axum::http::Uri| async move {
                    unrouted.lock().unwrap().push(uri.path().to_string());
                    axum::http::StatusCode::NOT_FOUND
                }
            });
        tokio::spawn(async move { axum::serve(listener, router).await });

        let plugins_dir = tempfile::TempDir::new()?;
        let dir = plugins_dir.path().join("foo");
        std::fs::create_dir_all(&dir)?;
        let mut managed = record("0.9.0");
        managed.origin = origin;
        managed.forge = Forge::Gitea;
        managed.store(&dir)?;

        let report = update_plugins_in(plugins_dir.path(), UpdateSelection::All).await?;

        assert!(report.up_to_date.is_empty());
        assert!(report.updated.is_empty());
        let [failed] = &report.failed[..] else {
            panic!("expected one failure: {:?}", report.failed);
        };
        assert_eq!(failed.id, "foo");
        assert!(
            failed.error.contains("rate limiting") && failed.error.contains("Try again in"),
            "{}",
            failed.error
        );
        let unrouted = unrouted.lock().unwrap().clone();
        assert!(
            !unrouted.iter().any(|path| path.contains("archive")),
            "{unrouted:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn update_checks_send_nothing_more_to_a_rate_limited_host() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::plugins::testing::init_default_test_state()?;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let base = format!("http://{}", listener.local_addr()?);
        let requests = Arc::new(Mutex::new(Vec::<String>::new()));
        let router = axum::Router::new().fallback({
            let requests = requests.clone();
            move |uri: axum::http::Uri| async move {
                requests.lock().unwrap().push(uri.path().to_string());
                (
                    axum::http::StatusCode::TOO_MANY_REQUESTS,
                    [("retry-after", "30")],
                )
            }
        });
        tokio::spawn(async move { axum::serve(listener, router).await });

        let plugins_dir = tempfile::TempDir::new()?;
        for (id, repo) in [("first", "o/r"), ("second", "o/s")] {
            let dir = plugins_dir.path().join(id);
            std::fs::create_dir_all(&dir)?;
            let mut managed = record("1.0.0");
            managed.origin = format!("{base}/{repo}");
            managed.forge = Forge::Gitea;
            managed.store(&dir)?;
        }

        let report = update_plugins_in(plugins_dir.path(), UpdateSelection::All).await?;

        let mut failed: Vec<_> = report
            .failed
            .iter()
            .map(|failed| (failed.id.as_str(), failed.error.as_str()))
            .collect();
        failed.sort();
        let wait = "rate limiting requests. Try again in 30 seconds.";
        assert!(
            matches!(
                &failed[..],
                [("first", first), ("second", second)]
                    if first.ends_with(wait) && second.ends_with(wait)
            ),
            "{failed:?}"
        );
        assert_eq!(
            requests.lock().unwrap().len(),
            1,
            "only the first origin's ref listing is sent: {:?}",
            requests.lock().unwrap()
        );
        Ok(())
    }

    fn previewed(id: &str, commit: &str) -> PreviewedPlugin {
        PreviewedPlugin {
            id: id.to_string(),
            commit: commit.repeat(40),
        }
    }

    #[tokio::test]
    async fn installs_fail_plugins_whose_release_changed_or_went_since_the_preview()
    -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::plugins::testing::init_default_test_state()?;
        let forge = catalog_forge(
            HOST_REQUIREMENT,
            HOST_REQUIREMENT,
            changed_at(HOST_REQUIREMENT),
        )
        .await?;
        let install = async |selection| {
            let plugins_dir = tempfile::TempDir::new()?;
            install_to_disk(
                &forge.origin,
                None,
                selection,
                &BuildInfo::default(),
                plugins_dir.path(),
            )
            .await
            .map_err(anyhow::Error::from)
        };
        let installed = |report: &InstallReport| {
            report
                .installed
                .iter()
                .map(|plugin| (plugin.id.clone(), plugin.version.clone()))
                .collect::<Vec<_>>()
        };

        let stale = [
            previewed("same", "2"),
            previewed("changed", "1"),
            previewed("gone", "2"),
        ];
        let report = install(Selection::Previewed(&stale)).await?;
        assert_eq!(installed(&report), [("same".into(), "1.0.0".into())]);
        let failed: Vec<_> = report
            .failed
            .iter()
            .map(|failed| (failed.id.as_str(), failed.error.as_str()))
            .collect();
        assert!(
            matches!(
                &failed[..],
                [("changed", changed), ("gone", gone)]
                    if changed.contains("changed since the preview")
                        && gone.contains("no longer offered by this repository since the preview")
            ),
            "{failed:?}"
        );

        let unlisted = [previewed("gone", "2")];
        let error = install(Selection::Previewed(&unlisted)).await.unwrap_err();
        assert!(
            matches!(
                error.downcast_ref::<PluginRepoError>(),
                Some(PluginRepoError::BadRequest(_))
            ),
            "{error}"
        );

        let report = install(Selection::All).await?;
        assert!(report.failed.is_empty(), "{report:?}");
        assert_eq!(
            installed(&report),
            [
                ("same".into(), "1.0.0".into()),
                ("changed".into(), "2.0.0".into())
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn install_to_disk_rejects_empty_selection() {
        assert!(matches!(
            install_to_disk(
                "https://github.com/o/r",
                None,
                Selection::Previewed(&[]),
                &BuildInfo::default(),
                Path::new("plugins"),
            )
            .await,
            Err(PluginRepoError::BadRequest(_))
        ));
    }

    #[test]
    fn catalog_status_compares_installed_and_offered_versions() -> anyhow::Result<()> {
        let plugins_dir = tempfile::TempDir::new()?;
        std::fs::create_dir(plugins_dir.path().join("local"))?;
        store_record(&plugins_dir.path().join("managed"), "1.0.0")?;
        std::fs::create_dir(plugins_dir.path().join("broken"))?;
        std::fs::write(
            plugins_dir
                .path()
                .join("broken")
                .join(SOURCE_RECORD_FILENAME),
            "{",
        )?;

        let version = |version: &str| {
            Ok(OfferedRelease {
                version: Version::parse(version).unwrap(),
                commit: "1".repeat(40),
                scopes: Vec::new(),
            })
        };
        let status =
            |id: &str, offered: Offered| catalog_status(&plugins_dir.path().join(id), &offered);
        assert_eq!(status("absent", version("1.0.0")), CatalogStatus::Available);
        assert_eq!(status("local", version("1.0.0")), CatalogStatus::Local);
        for (offered, expected) in [
            (version("1.0.0"), UpdateStatus::UpToDate),
            (version("1.0.1"), UpdateStatus::UpdateAvailable),
            (version("1.0.1-rc.1"), UpdateStatus::UpdateAvailable),
            (version("0.9.0"), UpdateStatus::UpToDate),
            (
                Err(Some("^0.0.7".into())),
                UpdateStatus::Incompatible {
                    requires: "^0.0.7".into(),
                },
            ),
            (Err(None), UpdateStatus::Unknown),
        ] {
            assert_eq!(
                status("managed", offered.clone()),
                CatalogStatus::Installed(expected),
                "{offered:?}"
            );
        }
        assert_eq!(
            status("broken", version("1.0.0")),
            CatalogStatus::Installed(UpdateStatus::Unknown)
        );
        Ok(())
    }

    #[test]
    fn update_status_compares_stored_releases_of_the_matching_subscription() -> anyhow::Result<()> {
        let plugins_dir = tempfile::TempDir::new()?;
        let mut db = lyra_db::test_db::new_test_db()?;

        let listed = |id: &str, version: Option<&str>| repo_db::PluginRepositoryEntry {
            db_id: None,
            plugin_id: id.to_string(),
            subpath: None,
            version: version.map(str::to_string),
            commit: version.map(|_| "1".repeat(40)),
            scopes: version.map(|_| Vec::new()),
            requires: None,
        };
        repo_db::create(
            &mut db,
            &repo_db::PluginRepository {
                db_id: None,
                id: "main".into(),
                origin: "https://github.com/o/r".into(),
                name: "Plugins".into(),
                description: String::new(),
                git_ref: None,
                commit: Some("b".repeat(40)),
                refreshed_at_ms: Some(1),
            },
            &[
                listed("current", Some("1.0.0")),
                listed("stale", Some("1.1.0")),
                listed("ahead", Some("0.9.0")),
                listed("unreleased", None),
                listed("pinned", Some("2.0.0")),
                listed("other-ref", Some("2.0.0")),
                listed("unsubscribed", Some("2.0.0")),
                listed("mixed-case", Some("2.0.0")),
                repo_db::PluginRepositoryEntry {
                    requires: Some(">=0.1".into()),
                    ..listed("blocked", None)
                },
            ],
        )?;
        let subscriptions = load_subscriptions(&db)?;

        let mut plugins: Vec<(&str, SourceRecord)> = [
            "current",
            "stale",
            "ahead",
            "unreleased",
            "pinned",
            "other-ref",
            "unsubscribed",
            "mixed-case",
            "unlisted",
            "blocked",
        ]
        .into_iter()
        .map(|id| (id, record("1.0.0")))
        .collect();
        plugins[4].1.pinned = true;
        plugins[5].1.git_ref = Some("stable".into());
        plugins[6].1.origin = "https://github.com/o/elsewhere".into();
        plugins[7].1.origin = "https://github.com/O/R".into();
        for (id, plugin) in &plugins {
            let dir = plugins_dir.path().join(id);
            std::fs::create_dir_all(&dir)?;
            plugin.store(&dir)?;
        }

        let status = |id: &str| -> anyhow::Result<UpdateStatus> {
            let record = load_managed_record(plugins_dir.path(), id)?;
            Ok(installed_status(id, &record, &subscriptions, &[]).status)
        };
        assert_eq!(status("current")?, UpdateStatus::UpToDate);
        assert_eq!(status("stale")?, UpdateStatus::UpdateAvailable);
        assert_eq!(status("ahead")?, UpdateStatus::UpToDate);
        assert_eq!(status("unreleased")?, UpdateStatus::Unknown);
        assert_eq!(status("pinned")?, UpdateStatus::UpToDate);
        assert_eq!(status("other-ref")?, UpdateStatus::Unknown);
        assert_eq!(status("unsubscribed")?, UpdateStatus::Unknown);
        assert_eq!(status("mixed-case")?, UpdateStatus::UpdateAvailable);
        assert_eq!(status("unlisted")?, UpdateStatus::Unknown);
        assert_eq!(
            status("blocked")?,
            UpdateStatus::Incompatible {
                requires: ">=0.1".into()
            }
        );
        Ok(())
    }

    #[test]
    fn listed_releases_are_stored_on_the_matching_subscription() -> anyhow::Result<()> {
        let mut db = lyra_db::test_db::new_test_db()?;
        let at = |id: &str, version: &str| repo_db::PluginRepositoryEntry {
            db_id: None,
            plugin_id: id.to_string(),
            subpath: Some(format!("plugins/{id}")),
            version: Some(version.to_string()),
            commit: Some("1".repeat(40)),
            scopes: Some(Vec::new()),
            requires: None,
        };
        repo_db::create(
            &mut db,
            &repo_db::PluginRepository {
                db_id: None,
                id: "main".into(),
                origin: "https://github.com/O/R".into(),
                name: "Plugins".into(),
                description: String::new(),
                git_ref: None,
                commit: Some("a".repeat(40)),
                refreshed_at_ms: None,
            },
            &[at("dropped", "1.0.0"), at("moved", "1.0.0")],
        )?;
        let listed = [at("moved", "1.1.0")];

        record_listed(
            &mut db,
            "https://github.com/o/r",
            Some("v2"),
            Some("b".repeat(40)),
            &listed,
        )?;
        let subscription = repo_db::get_by_id(&db, "main")?.expect("subscription exists");
        assert_eq!(subscription.commit.as_deref(), Some(&*"a".repeat(40)));
        assert_eq!(subscription.refreshed_at_ms, None);

        record_listed(
            &mut db,
            "https://github.com/o/r",
            None,
            Some("b".repeat(40)),
            &listed,
        )?;
        let subscription = repo_db::get_by_id(&db, "main")?.expect("subscription exists");
        assert_eq!(subscription.commit.as_deref(), Some(&*"b".repeat(40)));
        assert!(subscription.refreshed_at_ms.is_some());
        let entries: Vec<_> = repo_db::entries(&db, subscription.db_id.unwrap())?
            .into_iter()
            .map(|entry| (entry.plugin_id, entry.version))
            .collect();
        assert_eq!(entries, [("moved".to_string(), Some("1.1.0".to_string()))]);
        Ok(())
    }

    #[test]
    fn managed_plugin_ids_keeps_broken_records_and_skips_local_plugins() -> anyhow::Result<()> {
        let plugins_dir = tempfile::TempDir::new()?;
        std::fs::create_dir(plugins_dir.path().join("local"))?;
        std::fs::write(plugins_dir.path().join("stray.txt"), "")?;
        store_record(&plugins_dir.path().join("zeta"), "1.0.0")?;
        store_record(&plugins_dir.path().join("alpha"), "1.0.0")?;

        std::fs::create_dir(plugins_dir.path().join("broken"))?;
        std::fs::write(
            plugins_dir
                .path()
                .join("broken")
                .join(SOURCE_RECORD_FILENAME),
            "{",
        )?;

        assert_eq!(
            managed_plugin_ids(plugins_dir.path())?
                .into_iter()
                .collect::<Vec<_>>(),
            vec![
                "alpha".to_string(),
                "broken".to_string(),
                "zeta".to_string()
            ]
        );
        assert!(matches!(
            load_managed_record(plugins_dir.path(), "broken"),
            Err(PluginRepoError::Internal(_))
        ));
        assert!(managed_plugin_ids(&plugins_dir.path().join("absent"))?.is_empty());
        Ok(())
    }
}
