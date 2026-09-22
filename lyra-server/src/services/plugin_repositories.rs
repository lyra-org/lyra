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

use harmony_repository::{
    FetchError,
    InstallError,
    PluginCandidate,
    RepoSpec,
    ResolveError,
    ResolvedRepository,
    SourceRecord,
    install_candidate,
    manifest::SOURCE_RECORD_FILENAME,
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
        ResolveError::Fetch(FetchError::RefNotFound { .. }) => PluginRepoError::BadRequest(
            "The plugin repository or requested branch could not be found.".into(),
        ),
        ResolveError::Fetch(FetchError::InvalidUrl { .. })
        | ResolveError::Manifest(_)
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
/// when the installed commit already matches the resolved one.
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
    if record.commit.is_some() && record.commit == candidate.source.commit {
        return Ok(None);
    }
    Ok(Some(candidate))
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
/// `plugin_ids` is `None`. An explicitly empty selection is rejected. Each distinct origin and ref is resolved once;
/// per-plugin failures don't abort siblings. The runtime reloads once,
/// and only when something was reinstalled.
pub(crate) async fn update_plugins(
    plugin_ids: Option<&[String]>,
) -> Result<UpdateReport, PluginRepoError> {
    let plugins_dir = crate::plugins::bootstrap::plugins_dir();
    if plugin_ids.is_some_and(<[String]>::is_empty) {
        return Err(PluginRepoError::BadRequest(
            "no plugins selected to update".to_string(),
        ));
    }
    let ids: BTreeSet<String> = match plugin_ids {
        Some(ids) => ids.iter().cloned().collect(),
        None => managed_plugin_ids(&plugins_dir)?,
    };

    let mut report = UpdateReport::default();
    let mut records = Vec::new();
    for id in ids {
        match load_managed_record(&plugins_dir, &id) {
            Ok(record) => records.push((id, record)),
            Err(error) => report.failed.push(FailedInstall {
                id,
                error: error.to_string(),
            }),
        }
    }

    let mut resolved: HashMap<(String, Option<String>), Result<ResolvedRepository, String>> =
        HashMap::new();
    for (_, record) in &records {
        let key = (record.origin.clone(), record.git_ref.clone());
        if let std::collections::hash_map::Entry::Vacant(entry) = resolved.entry(key) {
            let result = resolve(&record.origin, record.git_ref.as_deref())
                .await
                .map_err(|error| error.to_string());
            entry.insert(result);
        }
    }

    let mut pending = Vec::new();
    for (id, record) in &records {
        let repository = match &resolved[&(record.origin.clone(), record.git_ref.clone())] {
            Ok(repository) => repository,
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

    {
        let _guard = MUTATION_LOCK.lock().await;
        for (id, candidate) in pending {
            if load_managed_record(&plugins_dir, id).is_err() {
                report.failed.push(FailedInstall {
                    id: id.clone(),
                    error: format!("plugin '{id}' was removed during update"),
                });
                continue;
            }
            match install_candidate(candidate, &plugins_dir, Some(now_rfc3339())) {
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
    }
    if !report.updated.is_empty() {
        reload_runtime().await?;
    }

    Ok(report)
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

/// Reloads the plugin runtime from disk; the recovery step after a failed
/// reload and the way CLI installs go live without a restart.
pub(crate) async fn reload_plugins() -> Result<(), PluginRepoError> {
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
        repo_db::update(&mut db, &record).map_err(PluginRepoError::Internal)?;
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

#[cfg(test)]
mod tests {
    use harmony_repository::{
        Forge,
        manifest::SOURCE_RECORD_SCHEMA_VERSION,
    };

    use super::*;

    fn store_record(dir: &Path, commit: Option<String>) -> anyhow::Result<()> {
        std::fs::create_dir_all(dir)?;
        SourceRecord {
            schema_version: SOURCE_RECORD_SCHEMA_VERSION,
            origin: "https://github.com/o/r".into(),
            forge: Forge::GitHub,
            git_ref: None,
            commit,
            subpath: None,
            via_repository: None,
            installed_at: None,
        }
        .store(dir)?;
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
        assert!(matches!(
            update_plugins(Some(&[])).await,
            Err(PluginRepoError::BadRequest(_))
        ));
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
