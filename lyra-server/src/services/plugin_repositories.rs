// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    path::Path,
    sync::LazyLock,
    time::{
        Duration,
        SystemTime,
        UNIX_EPOCH,
    },
};

use harmony_repository::{
    FetchError,
    InstallError,
    RepoSpec,
    ResolveError,
    ResolvedRepository,
    SourceRecord,
    install_candidate,
    resolve_plugins,
    uninstall_plugin,
};
use nanoid::nanoid;
use tokio::sync::Mutex;

use crate::{
    STATE,
    db::plugin_repositories as repo_db,
};

static HTTP_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| {
    let _ = rustls::crypto::ring::default_provider().install_default();
    reqwest::Client::builder()
        .timeout(Duration::from_secs(120))
        .user_agent(crate::outbound_user_agent())
        .build()
        .expect("failed to create plugin repository HTTP client")
});

/// Installs and uninstalls mutate `plugins_dir`; serialize them so staged
/// trees and runtime reloads never interleave.
static MUTATION_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[derive(Debug, thiserror::Error)]
pub(crate) enum PluginRepoError {
    #[error("{0}")]
    BadRequest(String),
    #[error("{0}")]
    NotFound(String),
    #[error("{0}")]
    Conflict(String),
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

fn map_resolve_error(error: ResolveError) -> PluginRepoError {
    match &error {
        ResolveError::Fetch(FetchError::InvalidUrl { .. }) => {
            PluginRepoError::BadRequest(error.to_string())
        }
        _ => PluginRepoError::Conflict(error.to_string()),
    }
}

fn map_install_error(error: InstallError) -> PluginRepoError {
    match &error {
        InstallError::InvalidId { .. } => PluginRepoError::BadRequest(error.to_string()),
        InstallError::NotInstalled { .. } => PluginRepoError::NotFound(error.to_string()),
        InstallError::NotManaged { .. }
        | InstallError::OriginMismatch { .. }
        | InstallError::CaseCollision { .. } => PluginRepoError::Conflict(error.to_string()),
        InstallError::Io(_) => PluginRepoError::Internal(anyhow::Error::new(error)),
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PluginPreview {
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) version: String,
    pub(crate) description: String,
    pub(crate) scopes: Vec<String>,
    pub(crate) origin: String,
    pub(crate) subpath: Option<String>,
    pub(crate) commit: Option<String>,
    pub(crate) installed: bool,
    /// Whether the installed copy carries a source record (was installed
    /// through repository tooling rather than placed locally).
    pub(crate) managed: bool,
    /// `None` when either side's commit is unknown.
    pub(crate) update_available: Option<bool>,
}

#[derive(Debug, Clone)]
pub(crate) struct RepositoryPreview {
    pub(crate) origin: String,
    pub(crate) git_ref: String,
    pub(crate) commit: Option<String>,
    /// Set when the repository carries a `repository.json` index.
    pub(crate) name: Option<String>,
    pub(crate) description: Option<String>,
    pub(crate) plugins: Vec<PluginPreview>,
}

async fn resolve(url: &str, git_ref: Option<&str>) -> Result<ResolvedRepository, PluginRepoError> {
    let spec =
        RepoSpec::parse(url, git_ref).map_err(|e| PluginRepoError::BadRequest(e.to_string()))?;
    resolve_plugins(&HTTP_CLIENT, &spec, &crate::plugins::module_scope_ids())
        .await
        .map_err(map_resolve_error)
}

fn build_preview(resolved: &ResolvedRepository, plugins_dir: &Path) -> RepositoryPreview {
    let plugins = resolved
        .candidates
        .iter()
        .map(|candidate| {
            let manifest = &candidate.plugin.manifest;
            let installed_dir = plugins_dir.join(candidate.id());
            let installed = installed_dir.is_dir();
            let record = SourceRecord::load(&installed_dir).ok().flatten();
            let update_available = match (&record, &candidate.source.commit) {
                (Some(record), Some(available)) => record
                    .commit
                    .as_ref()
                    .map(|installed_commit| installed_commit != available),
                _ => None,
            };

            PluginPreview {
                id: manifest.id.clone(),
                name: manifest.name.clone(),
                version: manifest.version.clone(),
                description: manifest.description.clone(),
                scopes: manifest.scopes.clone(),
                origin: candidate.source.origin.clone(),
                subpath: candidate.source.subpath.clone(),
                commit: candidate.source.commit.clone(),
                installed,
                managed: record.is_some(),
                update_available,
            }
        })
        .collect();

    RepositoryPreview {
        origin: resolved.spec.canonical_url(),
        git_ref: resolved.git_ref.clone(),
        commit: resolved.commit.clone(),
        name: resolved.index.as_ref().map(|index| index.name.clone()),
        description: resolved
            .index
            .as_ref()
            .map(|index| index.description.clone()),
        plugins,
    }
}

/// Resolves a repository URL without touching disk or database state.
pub(crate) async fn resolve_preview(
    url: &str,
    git_ref: Option<&str>,
) -> Result<RepositoryPreview, PluginRepoError> {
    let resolved = resolve(url, git_ref).await?;
    Ok(build_preview(
        &resolved,
        &crate::plugins::bootstrap::plugins_dir(),
    ))
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

/// Resolves a repository URL and installs its plugins (optionally a
/// subset) into `plugins_dir`. Per-plugin failures don't abort siblings.
/// Does not reload the runtime; callers decide when the new set goes live.
pub(crate) async fn install_to_disk(
    url: &str,
    git_ref: Option<&str>,
    plugin_ids: Option<&[String]>,
) -> Result<InstallReport, PluginRepoError> {
    let _guard = MUTATION_LOCK.lock().await;
    let resolved = resolve(url, git_ref).await?;

    if let Some(ids) = plugin_ids {
        for id in ids {
            if !resolved.candidates.iter().any(|c| c.id() == id) {
                return Err(PluginRepoError::BadRequest(format!(
                    "plugin '{id}' is not provided by this repository"
                )));
            }
        }
    }
    let selected: Vec<_> = resolved
        .candidates
        .iter()
        .filter(|candidate| plugin_ids.is_none_or(|ids| ids.iter().any(|id| id == candidate.id())))
        .collect();
    if selected.is_empty() {
        return Err(PluginRepoError::BadRequest(
            "repository provides no plugins to install".to_string(),
        ));
    }

    let plugins_dir = crate::plugins::bootstrap::plugins_dir();
    let mut report = InstallReport {
        installed: Vec::new(),
        failed: Vec::new(),
    };
    for candidate in selected {
        match install_candidate(candidate, &plugins_dir, Some(now_rfc3339())) {
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

    Ok(report)
}

/// Installs and then reloads the plugin runtime so the new set goes live.
pub(crate) async fn install(
    url: &str,
    git_ref: Option<&str>,
    plugin_ids: Option<&[String]>,
) -> Result<InstallReport, PluginRepoError> {
    let report = install_to_disk(url, git_ref, plugin_ids).await?;
    if !report.installed.is_empty() {
        reload_runtime().await?;
    }
    Ok(report)
}

#[derive(Debug, Clone)]
pub(crate) enum UpdateOutcome {
    UpToDate,
    Updated { commit: Option<String> },
}

/// Re-resolves an installed plugin's recorded origin and reinstalls it
/// when the resolved commit differs. Branch refs track new commits; tag
/// and SHA refs stay pinned by construction.
pub(crate) async fn update_plugin(plugin_id: &str) -> Result<UpdateOutcome, PluginRepoError> {
    let plugins_dir = crate::plugins::bootstrap::plugins_dir();
    let installed_dir = plugins_dir.join(plugin_id);
    if plugin_id.is_empty() || !installed_dir.is_dir() {
        return Err(PluginRepoError::NotFound(format!(
            "plugin not found: {plugin_id}"
        )));
    }

    let record = SourceRecord::load(&installed_dir)
        .map_err(anyhow::Error::new)?
        .ok_or_else(|| {
            PluginRepoError::Conflict(format!(
                "plugin '{plugin_id}' is local and not managed by a repository"
            ))
        })?;

    let resolved = resolve(&record.origin, record.git_ref.as_deref()).await?;
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

    if record.commit.is_some() && record.commit == candidate.source.commit {
        return Ok(UpdateOutcome::UpToDate);
    }

    {
        let _guard = MUTATION_LOCK.lock().await;
        install_candidate(candidate, &plugins_dir, Some(now_rfc3339()))
            .map_err(map_install_error)?;
    }
    reload_runtime().await?;

    Ok(UpdateOutcome::Updated {
        commit: candidate.source.commit.clone(),
    })
}

/// Uninstalls a repository-managed plugin and reloads the runtime.
pub(crate) async fn uninstall(plugin_id: &str) -> Result<(), PluginRepoError> {
    {
        let _guard = MUTATION_LOCK.lock().await;
        uninstall_plugin(&crate::plugins::bootstrap::plugins_dir(), plugin_id)
            .map_err(map_install_error)?;
    }
    reload_runtime().await
}

async fn reload_runtime() -> Result<(), PluginRepoError> {
    STATE
        .generation()
        .plugin_registries
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
    let resolved = resolve(url, git_ref).await?;
    let preview = build_preview(&resolved, &crate::plugins::bootstrap::plugins_dir());

    let origin = resolved.spec.canonical_url();
    let mut record = repo_db::PluginRepository {
        db_id: None,
        id: nanoid!(),
        origin: origin.clone(),
        name: preview
            .name
            .clone()
            .unwrap_or_else(|| resolved.spec.repo_name.clone()),
        description: preview.description.clone().unwrap_or_default(),
        git_ref: resolved.spec.explicit_ref.clone(),
        last_commit: resolved.commit.clone(),
        refreshed_at_ms: now_ms(),
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
    let db_id = repo_db::create(&mut *db, &record).map_err(PluginRepoError::Internal)?;
    record.db_id = Some(db_id);

    Ok((record, preview))
}

pub(crate) async fn list_repositories() -> Result<Vec<repo_db::PluginRepository>, PluginRepoError> {
    let db = STATE.db.read().await;
    repo_db::list(&*db).map_err(PluginRepoError::Internal)
}

pub(crate) async fn refresh_repository(
    repository_id: &str,
) -> Result<(repo_db::PluginRepository, RepositoryPreview), PluginRepoError> {
    let mut record = {
        let db = STATE.db.read().await;
        repo_db::get_by_id(&*db, repository_id).map_err(PluginRepoError::Internal)?
    }
    .ok_or_else(|| PluginRepoError::NotFound(format!("repository not found: {repository_id}")))?;

    let resolved = resolve(&record.origin, record.git_ref.as_deref()).await?;
    let preview = build_preview(&resolved, &crate::plugins::bootstrap::plugins_dir());

    if let Some(name) = &preview.name {
        record.name = name.clone();
    }
    if let Some(description) = &preview.description {
        record.description = description.clone();
    }
    record.last_commit = resolved.commit.clone();
    record.refreshed_at_ms = now_ms();
    {
        let mut db = STATE.db.write().await;
        repo_db::update(&mut *db, &record).map_err(PluginRepoError::Internal)?;
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
    repo_db::remove(&mut *db, db_id).map_err(PluginRepoError::Internal)?;
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
