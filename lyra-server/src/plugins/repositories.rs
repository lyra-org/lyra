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
use harmony_repository::{
    FetchError,
    InstallError,
    KnownTreeHashes,
    PluginCandidate,
    RepoSpec,
    RepositoryListing,
    ResolveError,
    ResolvedRepository,
    Revision,
    SourceRecord,
    forge::tree_hashes,
    install_candidate,
    list_plugins,
    manifest::SOURCE_RECORD_FILENAME,
    plan_refs,
    resolve_planned,
    resolve_plugins,
    uninstall_plugin,
};
use nanoid::nanoid;
use tokio::sync::Mutex;

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

/// Subscribes the official repository once per database, pinned to
/// `release_tag` when the build has one.
pub(crate) fn seed_default(db: &mut DbAny, release_tag: Option<&str>) -> anyhow::Result<()> {
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
            git_ref: release_tag.map(str::to_string),
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
        ResolveError::Io(_) | ResolveError::Fetch(FetchError::Io(_)) => {
            PluginRepoError::Internal(anyhow::Error::new(error))
        }
        ResolveError::Fetch(
            FetchError::InvalidArchive { .. } | FetchError::UnsafeArchiveEntry { .. },
        ) => {
            tracing::warn!(error = %error, "invalid plugin repository archive");
            PluginRepoError::BadRequest(
                "The plugin repository contains an invalid or unsupported archive.".into(),
            )
        }
        ResolveError::Fetch(FetchError::ArchiveTooLarge { .. }) => PluginRepoError::BadRequest(
            "The plugin repository archive exceeds the download size limit.".into(),
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
        | ResolveError::Plugin { .. }
        | ResolveError::DuplicatePluginId { .. } => PluginRepoError::BadRequest(error.to_string()),
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

/// Whether a repository-managed plugin has the latest known content.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UpdateStatus {
    /// Installed with the latest known content, or pinned.
    UpToDate,
    /// Installed with different content.
    UpdateAvailable,
    /// Neither a tree hash nor a commit is known on both sides.
    Unknown,
}

impl UpdateStatus {
    fn compare(installed: Revision<'_>, latest: Revision<'_>) -> Self {
        match installed.same_content(latest) {
            Some(true) => Self::UpToDate,
            Some(false) => Self::UpdateAvailable,
            None => Self::Unknown,
        }
    }
}

/// Where a catalogue plugin stands relative to the plugins directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CatalogStatus {
    /// Not installed.
    Available,
    /// Installed from a repository; compared against the offered revision.
    Installed(UpdateStatus),
    /// Installed without a source record, so repository tooling leaves it alone.
    Local,
}

#[derive(Debug, Clone)]
pub(crate) struct PluginPreview {
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) version: String,
    pub(crate) description: String,
    pub(crate) scopes: Vec<String>,
    pub(crate) commit: Option<String>,
    pub(crate) status: CatalogStatus,
    /// Origin of the plugin when a multi-plugin index points at another
    /// repository; `None` when it lives in the resolved repository itself.
    pub(crate) source_origin: Option<String>,
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

async fn resolve(
    url: &str,
    git_ref: Option<&str>,
    build: &BuildInfo,
) -> Result<ResolvedRepository, PluginRepoError> {
    let spec =
        RepoSpec::parse(url, git_ref).map_err(|e| PluginRepoError::BadRequest(e.to_string()))?;
    resolve_plugins(
        &http_client(build)?,
        &spec,
        &crate::plugins::module_scope_ids(),
    )
    .await
    .map_err(map_resolve_error)
}

/// Lists a repository URL's plugins from their manifests, for previews.
async fn list(
    url: &str,
    git_ref: Option<&str>,
    build: &BuildInfo,
) -> Result<RepositoryListing, PluginRepoError> {
    let spec =
        RepoSpec::parse(url, git_ref).map_err(|e| PluginRepoError::BadRequest(e.to_string()))?;
    list_plugins(
        &http_client(build)?,
        &spec,
        &crate::plugins::module_scope_ids(),
    )
    .await
    .map_err(map_resolve_error)
}

fn catalog_status(installed_dir: &Path, available: Revision<'_>) -> CatalogStatus {
    if !installed_dir.is_dir() {
        return CatalogStatus::Available;
    }
    let record = match SourceRecord::load(installed_dir) {
        Ok(None) => return CatalogStatus::Local,
        Ok(Some(record)) => record,
        // Managed, but the commit is unknowable until it is reinstalled.
        Err(_) => return CatalogStatus::Installed(UpdateStatus::Unknown),
    };
    CatalogStatus::Installed(UpdateStatus::compare(record.revision(), available))
}

/// A subscribed repository with the latest plugin revisions it offered.
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

/// Where the installed plugin `plugin_id` stands relative to the
/// subscribed repository it was installed from, using only stored
/// revisions: no forge call is made. Pinned plugins are always up to date.
/// Without a subscription for the same origin and ref that lists the
/// plugin, the answer is `Unknown`.
pub(crate) fn update_status(
    plugin_id: &str,
    record: &SourceRecord,
    subscriptions: &[Subscription],
) -> UpdateStatus {
    if record.pinned {
        return UpdateStatus::UpToDate;
    }
    let latest = find_subscription(subscriptions, &record.origin, record.git_ref.as_deref())
        .and_then(|subscription| {
            subscription
                .entries
                .iter()
                .find(|entry| entry.plugin_id == plugin_id && entry.subpath == record.subpath)
        });
    UpdateStatus::compare(
        record.revision(),
        latest.map(entry_revision).unwrap_or_default(),
    )
}

fn entry_revision(entry: &repo_db::PluginRepositoryEntry) -> Revision<'_> {
    Revision {
        commit: entry.commit.as_deref(),
        tree_hash: entry.tree_hash.as_deref(),
    }
}

fn subscription_entry(
    plugin_id: &str,
    subpath: Option<&String>,
    revision: Revision<'_>,
) -> repo_db::PluginRepositoryEntry {
    repo_db::PluginRepositoryEntry {
        db_id: None,
        plugin_id: plugin_id.to_string(),
        subpath: subpath.cloned(),
        commit: revision.commit.map(str::to_string),
        tree_hash: revision.tree_hash.map(str::to_string),
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
            subscription_entry(
                plugin.id(),
                plugin.source.subpath.as_ref(),
                plugin.source.revision(),
            )
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

/// Stores a freshly resolved commit and plugin revisions on the matching
/// subscription, so update status reflects the forge without a separate
/// refresh. Entries for plugins not in `resolved` are kept.
fn record_resolved(
    db: &mut DbAny,
    origin: &str,
    git_ref: Option<&str>,
    commit: Option<String>,
    resolved: Vec<repo_db::PluginRepositoryEntry>,
) -> Result<(), PluginRepoError> {
    let subscriptions = load_subscriptions(&*db)?;
    let Some(subscription) = find_subscription(&subscriptions, origin, git_ref) else {
        return Ok(());
    };
    let mut repository = subscription.repository.clone();
    repository.commit = commit;
    repository.refreshed_at_ms = Some(now_ms());
    let mut entries: Vec<_> = subscription
        .entries
        .iter()
        .filter(|kept| {
            !resolved
                .iter()
                .any(|entry| entry.plugin_id == kept.plugin_id && entry.subpath == kept.subpath)
        })
        .cloned()
        .collect();
    entries.extend(resolved);
    repo_db::update(db, &repository, &entries)?;
    Ok(())
}

fn build_preview(listing: &RepositoryListing, plugins_dir: &Path) -> RepositoryPreview {
    let plugins = listing
        .plugins
        .iter()
        .map(|plugin| {
            let manifest = &plugin.loaded.manifest;
            let source = &plugin.source;
            PluginPreview {
                id: manifest.id.clone(),
                name: manifest.name.clone(),
                version: manifest.version.clone(),
                description: manifest.description.clone(),
                scopes: manifest.scopes.clone(),
                commit: source.commit.clone(),
                status: catalog_status(&plugins_dir.join(plugin.id()), source.revision()),
                source_origin: source
                    .via_repository
                    .is_some()
                    .then(|| source.origin.clone()),
            }
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
    pub(crate) commit: Option<String>,
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

/// Resolves a repository URL and validates the requested selection. An
/// explicitly empty selection is rejected, as is a repository that offers
/// nothing to install.
async fn resolve_selection(
    url: &str,
    git_ref: Option<&str>,
    plugin_ids: Option<&[String]>,
    build: &BuildInfo,
) -> Result<ResolvedRepository, PluginRepoError> {
    if plugin_ids.is_some_and(<[String]>::is_empty) {
        return Err(PluginRepoError::BadRequest(
            "no plugins selected to install".to_string(),
        ));
    }
    let resolved = resolve(url, git_ref, build).await?;

    if let Some(ids) = plugin_ids {
        for id in ids {
            if !resolved.candidates.iter().any(|c| c.id() == id) {
                return Err(PluginRepoError::BadRequest(format!(
                    "plugin '{id}' is not provided by this repository"
                )));
            }
        }
    }
    if selected_candidates(&resolved, plugin_ids).is_empty() {
        return Err(PluginRepoError::BadRequest(
            "repository provides no plugins to install".to_string(),
        ));
    }
    Ok(resolved)
}

fn selected_candidates<'a>(
    resolved: &'a ResolvedRepository,
    plugin_ids: Option<&[String]>,
) -> Vec<&'a PluginCandidate> {
    resolved
        .candidates
        .iter()
        .filter(|candidate| plugin_ids.is_none_or(|ids| ids.iter().any(|id| id == candidate.id())))
        .collect()
}

/// Writes the selected candidates into `plugins_dir`. Per-plugin failures
/// don't abort siblings. The caller holds `MUTATION_LOCK`.
fn install_selected(
    resolved: &ResolvedRepository,
    plugin_ids: Option<&[String]>,
    plugins_dir: &Path,
) -> InstallReport {
    let mut report = InstallReport {
        installed: Vec::new(),
        failed: Vec::new(),
    };
    for candidate in selected_candidates(resolved, plugin_ids) {
        match install_candidate(candidate, plugins_dir, Some(now_rfc3339())) {
            Ok(record) => report.installed.push(InstalledPlugin {
                id: candidate.id().to_string(),
                version: candidate.plugin.manifest.version.clone(),
                commit: record.commit,
            }),
            Err(error) => report.failed.push(FailedInstall {
                id: candidate.id().to_string(),
                error: error.to_string(),
            }),
        }
    }
    report
}

/// Resolves a repository URL and installs its plugins (optionally a
/// subset) into `plugins_dir`. Does not reload the runtime; callers decide
/// when the new set goes live.
pub(crate) async fn install_to_disk(
    url: &str,
    git_ref: Option<&str>,
    plugin_ids: Option<&[String]>,
    build: &BuildInfo,
    plugins_dir: &Path,
) -> Result<InstallReport, PluginRepoError> {
    let resolved = resolve_selection(url, git_ref, plugin_ids, build).await?;
    let _guard = MUTATION_LOCK.lock().await;
    Ok(install_selected(&resolved, plugin_ids, plugins_dir))
}

/// Installs and then reloads the plugin runtime so the new set goes live.
pub(crate) async fn install(
    url: &str,
    git_ref: Option<&str>,
    plugin_ids: Option<&[String]>,
) -> Result<InstallReport, PluginRepoError> {
    let resolved = resolve_selection(url, git_ref, plugin_ids, &server_build()).await?;
    let _guard = MUTATION_LOCK.lock().await;
    let report = install_selected(&resolved, plugin_ids, &STATE.boot.get().plugins_dir);
    if !report.installed.is_empty() {
        reload_runtime().await?;
    }
    Ok(report)
}

/// Installs from a subscribed repository's origin and ref.
pub(crate) async fn install_from_repository(
    repository_id: &str,
    plugin_ids: Option<&[String]>,
) -> Result<InstallReport, PluginRepoError> {
    let record = load_repository(repository_id).await?;
    install(&record.origin, record.git_ref.as_deref(), plugin_ids).await
}

#[derive(Debug, Clone)]
pub(crate) struct UpdatedPlugin {
    pub(crate) id: String,
    pub(crate) version: String,
    pub(crate) commit: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct UpdateReport {
    pub(crate) updated: Vec<UpdatedPlugin>,
    pub(crate) up_to_date: Vec<String>,
    pub(crate) failed: Vec<FailedInstall>,
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

/// Finds the candidate a plugin should be reinstalled from, or `None`
/// when the installed content already matches the resolved one.
fn find_update<'a>(
    resolved: &'a ResolvedRepository,
    plugin_id: &str,
    record: &SourceRecord,
) -> Result<Option<&'a PluginCandidate>, PluginRepoError> {
    let candidate = resolved
        .candidates
        .iter()
        .find(|c| c.id() == plugin_id && c.source.subpath == record.subpath)
        .ok_or_else(|| {
            PluginRepoError::Conflict(format!(
                "plugin '{plugin_id}' is no longer available at {}",
                record.location()
            ))
        })?;
    if record.revision().same_content(candidate.source.revision()) == Some(true) {
        return Ok(None);
    }
    Ok(Some(candidate))
}

/// Where the plugins installed from one origin and ref update from.
enum UpdateSource {
    /// Every plugin's content is unchanged at the commit the ref resolves
    /// to, so nothing was downloaded.
    Current {
        commit: String,
        tree_hashes: HashMap<String, String>,
    },
    Changed(Box<ResolvedRepository>),
}

impl UpdateSource {
    fn commit(&self) -> Option<String> {
        match self {
            Self::Current { commit, .. } => Some(commit.clone()),
            Self::Changed(repository) => repository.commit.clone(),
        }
    }

    /// The revisions this source resolved for the `installed` plugins it
    /// was resolved for, as subscription entries.
    fn entries(
        &self,
        installed: &[&(String, SourceRecord)],
    ) -> Vec<repo_db::PluginRepositoryEntry> {
        match self {
            Self::Current {
                commit,
                tree_hashes,
            } => installed
                .iter()
                .map(|(id, record)| {
                    subscription_entry(
                        id,
                        record.subpath.as_ref(),
                        latest_revision(record, commit, tree_hashes),
                    )
                })
                .collect(),
            Self::Changed(repository) => repository
                .candidates
                .iter()
                .filter(|candidate| candidate.source.via_repository.is_none())
                .map(|candidate| {
                    subscription_entry(
                        candidate.id(),
                        candidate.source.subpath.as_ref(),
                        candidate.source.revision(),
                    )
                })
                .collect(),
        }
    }
}

/// Resolves the ref first and downloads only when the content of some
/// plugin in `installed` changed at the resolved commit.
async fn resolve_update_source(
    client: &reqwest::Client,
    origin: &str,
    git_ref: Option<&str>,
    installed: &[&SourceRecord],
) -> Result<UpdateSource, PluginRepoError> {
    let spec =
        RepoSpec::parse(origin, git_ref).map_err(|e| PluginRepoError::BadRequest(e.to_string()))?;
    let plan = plan_refs(client, &spec).await;
    let tree_hashes = match plan.commit() {
        Some(commit) => latest_tree_hashes(client, &spec, commit, installed).await,
        None => HashMap::new(),
    };
    if let Some(commit) = plan.commit()
        && all_current(installed, commit, &tree_hashes)
    {
        return Ok(UpdateSource::Current {
            commit: commit.to_string(),
            tree_hashes,
        });
    }
    resolve_planned(
        client,
        &spec,
        &plan,
        plan.commit().map(|commit| KnownTreeHashes {
            commit,
            hashes: &tree_hashes,
        }),
        &crate::plugins::module_scope_ids(),
    )
    .await
    .map(|repository| UpdateSource::Changed(Box::new(repository)))
    .map_err(map_resolve_error)
}

/// Tree hashes of the plugin directories `installed` holds at `commit`,
/// keyed by subpath. Skipped when every plugin is already at `commit`.
async fn latest_tree_hashes(
    client: &reqwest::Client,
    spec: &RepoSpec,
    commit: &str,
    installed: &[&SourceRecord],
) -> HashMap<String, String> {
    if installed
        .iter()
        .all(|record| record.commit.as_deref() == Some(commit))
    {
        return HashMap::new();
    }
    let dirs: Vec<&str> = installed.iter().map(|record| plugin_dir(record)).collect();
    tree_hashes(client, spec, commit, &dirs).await
}

fn all_current(
    installed: &[&SourceRecord],
    commit: &str,
    tree_hashes: &HashMap<String, String>,
) -> bool {
    installed.iter().all(|record| {
        record
            .revision()
            .same_content(latest_revision(record, commit, tree_hashes))
            == Some(true)
    })
}

/// The revision `record`'s plugin has at `commit`, given the tree hashes
/// [`latest_tree_hashes`] found there. A plugin already at `commit` keeps
/// its recorded tree hash, since none were looked up for it.
fn latest_revision<'a>(
    record: &'a SourceRecord,
    commit: &'a str,
    tree_hashes: &'a HashMap<String, String>,
) -> Revision<'a> {
    let tree_hash = match tree_hashes.get(plugin_dir(record)) {
        Some(tree_hash) => Some(tree_hash.as_str()),
        None if record.commit.as_deref() == Some(commit) => record.tree_hash.as_deref(),
        None => None,
    };
    Revision {
        commit: Some(commit),
        tree_hash,
    }
}

/// The plugin's directory within its origin; empty for the root.
fn plugin_dir(record: &SourceRecord) -> &str {
    record.subpath.as_deref().unwrap_or("")
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

/// Updates the named plugins, or every repository-managed plugin when
/// `plugin_ids` is `None`. An explicitly empty selection is rejected.
/// Pinned plugins are up to date by construction and are not resolved.
/// Each distinct origin and ref is resolved once and downloaded only when
/// a plugin installed from it is behind its commit; per-plugin failures
/// don't abort siblings. The runtime reloads once, and only when
/// something was reinstalled.
pub(crate) async fn update_plugins(
    plugin_ids: Option<&[String]>,
) -> Result<UpdateReport, PluginRepoError> {
    update_plugins_in(&STATE.boot.get().plugins_dir, plugin_ids).await
}

/// The origin and ref a group of installed plugins updates from.
type UpdateKey = (String, Option<String>);

async fn update_plugins_in(
    plugins_dir: &Path,
    plugin_ids: Option<&[String]>,
) -> Result<UpdateReport, PluginRepoError> {
    if plugin_ids.is_some_and(<[String]>::is_empty) {
        return Err(PluginRepoError::BadRequest(
            "no plugins selected to update".to_string(),
        ));
    }
    let ids: BTreeSet<String> = match plugin_ids {
        Some(ids) => ids.iter().cloned().collect(),
        None => managed_plugin_ids(plugins_dir)?,
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
    let mut resolved: HashMap<UpdateKey, Result<UpdateSource, String>> = HashMap::new();
    for ((origin, git_ref), group) in &installed {
        let group: Vec<&SourceRecord> = group.iter().map(|(_, record)| record).collect();
        let result = resolve_update_source(&client, origin, git_ref.as_deref(), &group)
            .await
            .map_err(|error| error.to_string());
        resolved.insert((origin.clone(), git_ref.clone()), result);
    }
    if resolved.values().any(Result::is_ok) {
        let mut db = STATE.db.write().await;
        for (key, source) in &resolved {
            if let Ok(source) = source {
                let (origin, git_ref) = key;
                let entries = source.entries(&installed[key]);
                record_resolved(
                    &mut db,
                    origin,
                    git_ref.as_deref(),
                    source.commit(),
                    entries,
                )?;
            }
        }
    }

    let mut pending = Vec::new();
    for (id, record) in &records {
        let repository = match &resolved[&(record.origin.clone(), record.git_ref.clone())] {
            Ok(UpdateSource::Current { .. }) => {
                report.up_to_date.push(id.clone());
                continue;
            }
            Ok(UpdateSource::Changed(repository)) => repository,
            Err(error) => {
                report.failed.push(FailedInstall {
                    id: id.clone(),
                    error: error.clone(),
                });
                continue;
            }
        };
        match find_update(repository, id, record) {
            Ok(Some(candidate)) => pending.push((id, candidate)),
            Ok(None) => report.up_to_date.push(id.clone()),
            Err(error) => report.failed.push(FailedInstall {
                id: id.clone(),
                error: error.to_string(),
            }),
        }
    }

    let _guard = MUTATION_LOCK.lock().await;
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
                version: candidate.plugin.manifest.version.clone(),
                commit: record.commit,
            }),
            Err(error) => report.failed.push(FailedInstall {
                id: id.clone(),
                error: error.to_string(),
            }),
        }
    }
    if !report.updated.is_empty() {
        reload_runtime().await?;
    }

    Ok(report)
}

/// Uninstalls a repository-managed plugin and reloads the runtime.
pub(crate) async fn uninstall(plugin_id: &str) -> Result<(), PluginRepoError> {
    let _guard = MUTATION_LOCK.lock().await;
    uninstall_plugin(&STATE.boot.get().plugins_dir, plugin_id).map_err(map_install_error)?;
    reload_runtime().await
}

/// Reloads the plugin runtime from disk; the recovery step after a failed
/// reload and the way CLI installs go live without a restart.
pub(crate) async fn reload_plugins() -> Result<(), PluginRepoError> {
    let _guard = MUTATION_LOCK.lock().await;
    reload_runtime().await
}

/// Reloads the runtime from `plugins_dir`. The caller holds `MUTATION_LOCK`.
async fn reload_runtime() -> Result<(), PluginRepoError> {
    crate::plugins::STATE
        .generation()
        .registries
        .reload_all_plugins()
        .await
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
    use harmony_repository::{
        Forge,
        manifest::SOURCE_RECORD_SCHEMA_VERSION,
    };

    use super::*;

    #[test]
    fn seeds_official_repository_at_release_tag() -> anyhow::Result<()> {
        let mut db = lyra_db::test_db::new_test_db()?;
        seed_default(&mut db, Some("v1.2.3"))?;
        let repositories = repo_db::list(&db)?;
        assert_eq!(repositories.len(), 1);
        let record = &repositories[0];
        assert_eq!(record.origin, DEFAULT_ORIGIN);
        assert_eq!(record.git_ref.as_deref(), Some("v1.2.3"));
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

    fn record(commit: Option<String>) -> SourceRecord {
        SourceRecord {
            schema_version: SOURCE_RECORD_SCHEMA_VERSION,
            origin: "https://github.com/o/r".into(),
            forge: Forge::GitHub,
            git_ref: None,
            commit,
            tree_hash: None,
            pinned: false,
            subpath: None,
            via_repository: None,
            installed_at: None,
        }
    }

    fn store_record(dir: &Path, commit: Option<String>) -> anyhow::Result<()> {
        std::fs::create_dir_all(dir)?;
        record(commit).store(dir)?;
        Ok(())
    }

    #[test]
    fn load_managed_record_distinguishes_missing_local_and_managed() -> anyhow::Result<()> {
        let plugins_dir = tempfile::TempDir::new()?;
        std::fs::create_dir(plugins_dir.path().join("local"))?;
        store_record(&plugins_dir.path().join("managed"), Some("a".repeat(40)))?;

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
    async fn update_plugins_rejects_empty_selection() {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::plugins::testing::init_default_test_state().expect("test state");
        assert!(matches!(
            update_plugins(Some(&[])).await,
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
        let mut pinned = record(Some("a".repeat(40)));
        pinned.origin = "http://127.0.0.1:9/o/r".into();
        pinned.git_ref = Some("v1.0.0".into());
        pinned.pinned = true;
        pinned.store(&dir)?;

        let report = update_plugins_in(plugins_dir.path(), None).await?;

        assert!(report.updated.is_empty());
        assert!(report.failed.is_empty());
        assert_eq!(report.up_to_date, vec!["pinned".to_string()]);
        Ok(())
    }

    /// A Gitea-style forge that answers API calls for `o/r` at `commit`,
    /// whose root tree is `root_tree`, and serves no archives, so any
    /// download fails.
    async fn api_only_forge(commit: &str, root_tree: &str) -> anyhow::Result<String> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let base = format!("http://{}", listener.local_addr()?);
        let root_tree = root_tree.to_string();
        let router = axum::Router::new()
            .route(
                &format!("/api/v1/repos/o/r/git/trees/{commit}"),
                axum::routing::get(move || async move {
                    axum::Json(serde_json::json!({ "sha": root_tree, "tree": [] }))
                }),
            )
            .route(
                "/api/v1/repos/o/r",
                axum::routing::get(|| async {
                    axum::Json(serde_json::json!({ "default_branch": "main" }))
                }),
            )
            .route(
                "/api/v1/repos/o/r/git/commits/main",
                axum::routing::get({
                    let commit = commit.to_string();
                    move || async move { axum::Json(serde_json::json!({ "sha": commit })) }
                }),
            );
        tokio::spawn(async move { axum::serve(listener, router).await });
        Ok(format!("{base}/o/r"))
    }

    #[tokio::test]
    async fn update_plugins_skips_the_download_when_installed_commits_match() -> anyhow::Result<()>
    {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::plugins::testing::init_default_test_state()?;
        let commit = "c".repeat(40);
        let origin = api_only_forge(&commit, "root-tree").await?;
        let plugins_dir = tempfile::TempDir::new()?;
        for (id, installed) in [("current", commit.clone()), ("behind", "d".repeat(40))] {
            let dir = plugins_dir.path().join(id);
            std::fs::create_dir_all(&dir)?;
            let mut managed = record(Some(installed));
            managed.origin = origin.clone();
            managed.forge = Forge::Gitea;
            managed.store(&dir)?;
        }

        let report = update_plugins_in(plugins_dir.path(), Some(&["current".into()])).await?;
        assert!(report.failed.is_empty(), "{:?}", report.failed);
        assert_eq!(report.up_to_date, vec!["current".to_string()]);

        let report = update_plugins_in(plugins_dir.path(), None).await?;
        assert!(report.up_to_date.is_empty());
        let failed = report
            .failed
            .iter()
            .map(|f| f.id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(failed, ["behind", "current"]);
        Ok(())
    }

    #[tokio::test]
    async fn update_plugins_skips_the_download_when_plugin_trees_match() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::plugins::testing::init_default_test_state()?;
        let origin = api_only_forge(&"c".repeat(40), "root-tree").await?;
        let plugins_dir = tempfile::TempDir::new()?;
        for (id, tree_hash) in [("same", "root-tree"), ("changed", "old-tree")] {
            let dir = plugins_dir.path().join(id);
            std::fs::create_dir_all(&dir)?;
            let mut managed = record(Some("d".repeat(40)));
            managed.origin = origin.clone();
            managed.forge = Forge::Gitea;
            managed.tree_hash = Some(tree_hash.into());
            managed.store(&dir)?;
        }

        let report = update_plugins_in(plugins_dir.path(), Some(&["same".into()])).await?;
        assert!(report.failed.is_empty(), "{:?}", report.failed);
        assert_eq!(report.up_to_date, vec!["same".to_string()]);

        let report = update_plugins_in(plugins_dir.path(), Some(&["changed".into()])).await?;
        assert!(report.up_to_date.is_empty());
        let [failed] = &report.failed[..] else {
            panic!("expected one failure: {:?}", report.failed);
        };
        assert!(
            failed.error.contains("could not be downloaded"),
            "the changed plugin should fail at its archive download: {}",
            failed.error
        );
        Ok(())
    }

    #[test]
    fn plugins_are_current_when_their_content_is_unchanged() {
        let commit = "a".repeat(40);
        let moved = "b".repeat(40);
        let at = |commit: &str, tree_hash: Option<&str>, subpath: Option<&str>| {
            let mut record = record(Some(commit.to_string()));
            record.tree_hash = tree_hash.map(str::to_string);
            record.subpath = subpath.map(str::to_string);
            record
        };
        let hashes = HashMap::from([("plugins/a".to_string(), "tree-a".to_string())]);

        let unchanged = at(&commit, Some("tree-a"), Some("plugins/a"));
        assert!(all_current(&[&unchanged], &moved, &hashes));

        let changed = at(&commit, Some("tree-old"), Some("plugins/a"));
        assert!(!all_current(&[&unchanged, &changed], &moved, &hashes));

        let unhashed = at(&commit, Some("tree-b"), Some("plugins/b"));
        assert!(!all_current(&[&unhashed], &moved, &hashes));
        assert!(all_current(&[&unhashed], &commit, &hashes));

        let legacy = at(&commit, None, Some("plugins/a"));
        assert!(!all_current(&[&legacy], &moved, &hashes));
        assert!(all_current(&[&legacy], &commit, &HashMap::new()));
    }

    #[tokio::test]
    async fn install_to_disk_rejects_empty_selection() {
        assert!(matches!(
            install_to_disk(
                "https://github.com/o/r",
                None,
                Some(&[]),
                &BuildInfo::default(),
                Path::new("plugins"),
            )
            .await,
            Err(PluginRepoError::BadRequest(_))
        ));
    }

    #[test]
    fn catalog_status_reflects_install_state_and_commits() -> anyhow::Result<()> {
        let plugins_dir = tempfile::TempDir::new()?;
        let commit = "a".repeat(40);
        std::fs::create_dir(plugins_dir.path().join("local"))?;
        store_record(&plugins_dir.path().join("pinned"), Some(commit.clone()))?;
        store_record(&plugins_dir.path().join("unpinned"), None)?;
        std::fs::create_dir(plugins_dir.path().join("broken"))?;
        std::fs::write(
            plugins_dir
                .path()
                .join("broken")
                .join(SOURCE_RECORD_FILENAME),
            "{",
        )?;

        let status = |id: &str, commit: Option<&str>| {
            catalog_status(
                &plugins_dir.path().join(id),
                Revision {
                    commit,
                    tree_hash: None,
                },
            )
        };
        assert_eq!(status("absent", Some(&commit)), CatalogStatus::Available);
        assert_eq!(status("local", Some(&commit)), CatalogStatus::Local);
        assert_eq!(
            status("pinned", Some(&commit)),
            CatalogStatus::Installed(UpdateStatus::UpToDate)
        );
        assert_eq!(
            status("pinned", Some(&"b".repeat(40))),
            CatalogStatus::Installed(UpdateStatus::UpdateAvailable)
        );
        assert_eq!(
            status("pinned", None),
            CatalogStatus::Installed(UpdateStatus::Unknown)
        );
        assert_eq!(
            status("unpinned", Some(&commit)),
            CatalogStatus::Installed(UpdateStatus::Unknown)
        );
        assert_eq!(
            status("broken", Some(&commit)),
            CatalogStatus::Installed(UpdateStatus::Unknown)
        );
        Ok(())
    }

    #[test]
    fn update_status_compares_stored_revisions_of_the_matching_subscription() -> anyhow::Result<()>
    {
        let plugins_dir = tempfile::TempDir::new()?;
        let mut db = lyra_db::test_db::new_test_db()?;
        let commit = "a".repeat(40);
        let moved = "b".repeat(40);

        let listed = |id: &str, commit: &str, tree_hash: Option<&str>| {
            subscription_entry(
                id,
                None,
                Revision {
                    commit: Some(commit),
                    tree_hash,
                },
            )
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
                commit: Some(moved.clone()),
                refreshed_at_ms: Some(1),
            },
            &[
                listed("current", &commit, None),
                listed("stale", &commit, None),
                listed("unresolved", &commit, None),
                listed("pinned", &commit, None),
                listed("other-ref", &commit, None),
                listed("unsubscribed", &commit, None),
                listed("mixed-case", &commit, None),
                listed("unchanged-tree", &moved, Some("tree")),
                listed("changed-tree", &moved, Some("new-tree")),
            ],
        )?;
        let subscriptions = load_subscriptions(&db)?;

        let mut plugins: Vec<(&str, SourceRecord)> = vec![
            ("current", record(Some(commit.clone()))),
            ("stale", record(Some("c".repeat(40)))),
            ("unresolved", record(None)),
            ("pinned", record(Some("c".repeat(40)))),
            ("other-ref", record(Some(commit.clone()))),
            ("unsubscribed", record(Some(commit.clone()))),
            ("mixed-case", record(Some("c".repeat(40)))),
            ("unchanged-tree", record(Some(commit.clone()))),
            ("changed-tree", record(Some(commit.clone()))),
            ("unlisted", record(Some(commit.clone()))),
        ];
        plugins[3].1.pinned = true;
        plugins[4].1.git_ref = Some("stable".into());
        plugins[5].1.origin = "https://github.com/o/elsewhere".into();
        plugins[6].1.origin = "https://github.com/O/R".into();
        plugins[7].1.tree_hash = Some("tree".into());
        plugins[8].1.tree_hash = Some("tree".into());
        for (id, plugin) in &plugins {
            let dir = plugins_dir.path().join(id);
            std::fs::create_dir_all(&dir)?;
            plugin.store(&dir)?;
        }

        let status = |id: &str| -> anyhow::Result<UpdateStatus> {
            let record = load_managed_record(plugins_dir.path(), id)?;
            Ok(update_status(id, &record, &subscriptions))
        };
        assert_eq!(status("current")?, UpdateStatus::UpToDate);
        assert_eq!(status("stale")?, UpdateStatus::UpdateAvailable);
        assert_eq!(status("unresolved")?, UpdateStatus::Unknown);
        assert_eq!(status("pinned")?, UpdateStatus::UpToDate);
        assert_eq!(status("other-ref")?, UpdateStatus::Unknown);
        assert_eq!(status("unsubscribed")?, UpdateStatus::Unknown);
        assert_eq!(status("mixed-case")?, UpdateStatus::UpdateAvailable);
        assert_eq!(status("unchanged-tree")?, UpdateStatus::UpToDate);
        assert_eq!(status("changed-tree")?, UpdateStatus::UpdateAvailable);
        assert_eq!(status("unlisted")?, UpdateStatus::Unknown);
        Ok(())
    }

    #[test]
    fn resolved_revisions_are_stored_on_the_matching_subscription() -> anyhow::Result<()> {
        let mut db = lyra_db::test_db::new_test_db()?;
        let revision =
            |commit: &str, tree_hash: &str| (Some(commit.to_string()), Some(tree_hash.to_string()));
        let at = |id: &str, (commit, tree_hash): (Option<String>, Option<String>)| {
            repo_db::PluginRepositoryEntry {
                db_id: None,
                plugin_id: id.to_string(),
                subpath: Some(format!("plugins/{id}")),
                commit,
                tree_hash,
            }
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
            &[
                at("kept", revision("a", "kept-tree")),
                at("moved", revision("a", "old-tree")),
            ],
        )?;
        let resolved = || vec![at("moved", revision("b", "new-tree"))];

        record_resolved(
            &mut db,
            "https://github.com/o/r",
            Some("v2"),
            Some("b".repeat(40)),
            resolved(),
        )?;
        let subscription = repo_db::get_by_id(&db, "main")?.expect("subscription exists");
        assert_eq!(subscription.commit.as_deref(), Some(&*"a".repeat(40)));
        assert_eq!(subscription.refreshed_at_ms, None);

        record_resolved(
            &mut db,
            "https://github.com/o/r",
            None,
            Some("b".repeat(40)),
            resolved(),
        )?;
        let subscription = repo_db::get_by_id(&db, "main")?.expect("subscription exists");
        assert_eq!(subscription.commit.as_deref(), Some(&*"b".repeat(40)));
        assert!(subscription.refreshed_at_ms.is_some());
        let mut entries: Vec<_> = repo_db::entries(&db, subscription.db_id.unwrap())?
            .into_iter()
            .map(|entry| (entry.plugin_id, entry.tree_hash))
            .collect();
        entries.sort();
        assert_eq!(
            entries,
            [
                ("kept".to_string(), Some("kept-tree".to_string())),
                ("moved".to_string(), Some("new-tree".to_string())),
            ]
        );
        Ok(())
    }

    #[test]
    fn managed_plugin_ids_keeps_broken_records_and_skips_local_plugins() -> anyhow::Result<()> {
        let plugins_dir = tempfile::TempDir::new()?;
        std::fs::create_dir(plugins_dir.path().join("local"))?;
        std::fs::write(plugins_dir.path().join("stray.txt"), "")?;
        store_record(&plugins_dir.path().join("zeta"), None)?;
        store_record(&plugins_dir.path().join("alpha"), Some("a".repeat(40)))?;

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
