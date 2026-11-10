// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! The daily update check of subscribed repositories. Each tick checks at
//! most one due subscription, so a restart never checks them in a burst,
//! and reloads the runtime for installed updates once nothing is playing.
//! A subscription is checked ahead of its day, at startup or at the next
//! tick after a reload, while the runtime refuses a plugin installed from
//! it for the server version: a server upgrade leaves its plugins unloaded
//! until their releases for the new version are installed.

use std::{
    collections::{
        HashMap,
        HashSet,
    },
    hash::{
        DefaultHasher,
        Hash,
        Hasher,
    },
    path::Path,
    sync::atomic::{
        AtomicBool,
        Ordering,
    },
    time::Duration,
};

use harmony_core::plugin::{
    PluginDiscoveryError,
    PluginLoadError,
};
use harmony_repository::{
    InstalledBy,
    RepoSpec,
    UnpackBudget,
};
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

use super::{
    AvailableUpdate,
    CHECK_LOCK,
    HeldPlugin,
    MUTATION_LOCK,
    OfferedRelease,
    PluginRepoError,
    StoredListing,
    Subscription,
    UpdateOutcome,
    UpdateReport,
    UpdateSelection,
    find_subscription,
    http_client,
    install_updates,
    installed_manifests,
    list_update_source,
    load_managed_record,
    managed_plugin_ids,
    map_resolve_error,
    now_ms,
    reload_runtime,
    resolve_update_source,
    server_build,
    settle_updates,
    store_listed,
    take_outcome,
};
use crate::services::STATE;
use lyra_db::plugin_repositories as repo_db;

/// How often the loop wakes: to retry a deferred reload, and to check the
/// next due subscription.
const TICK: Duration = Duration::from_secs(5 * 60);
/// How long a subscription's check holds before it is due again.
const CHECK_INTERVAL_MS: u64 = 24 * 60 * 60 * 1000;
/// The most a subscription's check is spread past the interval.
const JITTER_MS: u64 = 2 * 60 * 60 * 1000;
/// The wait after a failed check or reload, doubled on each further
/// failure.
const RETRY_MIN_MS: u64 = 60 * 60 * 1000;
/// The same for an early check, which mostly fails for a network that is
/// not up yet.
const EARLY_RETRY_MIN_MS: u64 = 5 * 60 * 1000;

/// Runs the daily update checks until `shutdown`, which also abandons a
/// check in flight. A reload that has started always finishes.
pub(crate) async fn run_update_checks(shutdown: CancellationToken) {
    let mut retries = Retries::default();
    let mut early = EarlyChecks::default();
    let mut reload_wait: Option<Backoff> = None;
    let mut ticks = tokio::time::interval_at(tokio::time::Instant::now() + TICK, TICK);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let boot = STATE.boot.get();
    tokio::select! {
        () = check_refused(&boot.plugins_dir, &mut retries, &mut early) => {}
        () = shutdown.cancelled() => {}
    }
    if !shutdown.is_cancelled() {
        reload_when_idle(&mut reload_wait, now_ms()).await;
    }
    loop {
        tokio::select! {
            _ = ticks.tick() => {}
            () = shutdown.cancelled() => break,
        }
        if shutdown.is_cancelled() {
            break;
        }
        reload_when_idle(&mut reload_wait, now_ms()).await;
        let boot = STATE.boot.get();
        tokio::select! {
            _ = check_next(&boot.plugins_dir, &mut retries, &mut early, Due::Any, now_ms()) => {}
            () = shutdown.cancelled() => break,
        }
    }
    tracing::info!("plugin update checks shutting down");
}

/// Applies automatic updates waiting for a reload, unless a failed reload
/// is still waiting out its backoff.
async fn reload_when_idle(wait: &mut Option<Backoff>, now_ms: u64) {
    if wait.as_ref().is_some_and(|wait| !wait.allows(now_ms)) {
        return;
    }
    match apply_pending_reload().await {
        Ok(true) => {
            *wait = None;
            tracing::info!("reloaded plugins for automatic updates");
        }
        Ok(false) => {}
        Err(error) => {
            *wait = Some(Backoff::after(wait.as_ref(), now_ms, None, RETRY_MIN_MS));
            tracing::warn!(%error, "reloading plugins for automatic updates failed");
        }
    }
}

/// Checks every subscription due early, without waiting for the ticks.
/// Each is checked once: a check that succeeds ends its early checks, and
/// one that fails waits out its backoff.
async fn check_refused(plugins_dir: &Path, retries: &mut Retries, early: &mut EarlyChecks) {
    while check_next(plugins_dir, retries, early, Due::Early, now_ms()).await {}
}

/// Which subscriptions a pass checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Due {
    /// Those due early or by their day.
    Any,
    /// Only those due early.
    Early,
}

/// Checks the most overdue subscription `due`, if any is. Returns whether
/// it checked one.
async fn check_next(
    plugins_dir: &Path,
    retries: &mut Retries,
    early: &mut EarlyChecks,
    due: Due,
    now_ms: u64,
) -> bool {
    let repositories = {
        let db = STATE.db.read().await;
        match repo_db::list(&*db) {
            Ok(repositories) => repositories,
            Err(error) => {
                tracing::warn!(%error, "listing plugin repositories for update checks failed");
                return false;
            }
        }
    };
    let refused = || {
        let errors = crate::plugins::STATE.discovery_errors();
        tokio::task::block_in_place(|| refused_installs(plugins_dir, &repositories, &errors))
    };
    let waiting = early.waiting(&refused());
    let candidates = repositories
        .iter()
        .filter(|repository| due == Due::Any || waiting.contains(&repository.id));
    let Some(repository) = next_due(candidates, retries, &waiting, now_ms) else {
        return false;
    };
    let first_retry_ms = if waiting.contains(&repository.id) {
        EARLY_RETRY_MIN_MS
    } else {
        RETRY_MIN_MS
    };
    let install = STATE.config().plugins.auto_update;
    match check_subscription(repository, plugins_dir, install).await {
        Ok(report) => {
            // Read again: the check replaced the installs it updated.
            let retry = early.checked(&repository.id, refused(), &report);
            if retry {
                retries.delayed(repository, None, first_retry_ms, now_ms);
            } else {
                retries.succeeded(repository);
            }
            for plugin in &report.updated {
                tracing::info!(plugin = %plugin.id, version = %plugin.version, "installed plugin update automatically");
            }
            for plugin in &report.held {
                tracing::info!(plugin = %plugin.id, version = %plugin.release.version, hold = %plugin.hold, "plugin update held for approval");
            }
            for plugin in &report.available {
                tracing::info!(plugin = %plugin.id, version = %plugin.release.version, "plugin update available");
            }
            for failed in &report.failed {
                tracing::warn!(plugin = %failed.id, error = %failed.error, "automatic plugin update failed");
            }
        }
        Err(error) => {
            tracing::warn!(origin = %repository.origin, %error, "plugin update check failed");
            retries.failed(repository, &error, first_retry_ms, now_ms);
        }
    }
    true
}

/// An unpinned plugin the runtime refused for the server version, as
/// installed from a subscription whose check may replace it.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RefusedInstall {
    subscription: String,
    plugin: String,
    commit: String,
}

/// The installs in `plugins_dir` that `errors` refused for the server
/// version and that an update check of one of `repositories` takes up.
fn refused_installs(
    plugins_dir: &Path,
    repositories: &[repo_db::PluginRepository],
    errors: &[PluginDiscoveryError],
) -> Vec<RefusedInstall> {
    let subscriptions: Vec<Subscription> = repositories
        .iter()
        .map(|repository| Subscription {
            repository: repository.clone(),
            entries: Vec::new(),
        })
        .collect();
    errors
        .iter()
        .filter(|error| matches!(error.error, PluginLoadError::IncompatibleHost { .. }))
        .flat_map(|error| &error.directories)
        .filter_map(|plugin| {
            let record = load_managed_record(plugins_dir, plugin).ok()?;
            if record.pinned {
                return None;
            }
            let subscription =
                find_subscription(&subscriptions, &record.origin, record.git_ref.as_deref())?;
            Some(RefusedInstall {
                subscription: subscription.repository.id.clone(),
                plugin: plugin.clone(),
                commit: record.commit,
            })
        })
        .collect()
}

/// The refused installs whose subscription has been checked since they
/// were found refused, which wait for its day like any other. Kept in
/// memory: a restart checks early again.
#[derive(Debug, Default)]
struct EarlyChecks(HashSet<RefusedInstall>);

impl EarlyChecks {
    /// The ids of the subscriptions due early for one of `refused`.
    fn waiting(&self, refused: &[RefusedInstall]) -> HashSet<String> {
        refused
            .iter()
            .filter(|install| !self.0.contains(install))
            .map(|install| install.subscription.clone())
            .collect()
    }

    /// Records a check of `subscription` that gave `report`, for the
    /// installs still `refused` after it. Returns whether any of them
    /// failed to update, which leaves the subscription due early.
    fn checked(
        &mut self,
        subscription: &str,
        refused: Vec<RefusedInstall>,
        report: &UpdateReport,
    ) -> bool {
        let mut retry = false;
        for install in refused {
            if install.subscription != subscription {
                continue;
            }
            if report
                .failed
                .iter()
                .any(|failed| failed.id == install.plugin)
            {
                retry = true;
            } else {
                self.0.insert(install);
            }
        }
        retry
    }
}

/// Set when an automatic update installed files the runtime has not loaded
/// yet; cleared by any reload.
pub(super) static RELOAD_PENDING: AtomicBool = AtomicBool::new(false);

/// Lists one subscribed `repository`, stores what it offers on it, and
/// judges the newer releases of its installed plugins. When `install` is
/// set and the network cannot alter what the repository serves, those that
/// need no approval are reinstalled, recorded as automatic; otherwise they
/// are only reported as available. The runtime is not reloaded; that waits
/// for [`apply_pending_reload`]. A subscription removed in the meantime is
/// not checked.
pub(super) async fn check_subscription(
    repository: &repo_db::PluginRepository,
    plugins_dir: &Path,
    install: bool,
) -> Result<UpdateReport, PluginRepoError> {
    let _check = CHECK_LOCK.lock().await;
    let Some(repository) = repo_db::get_by_id(&*STATE.db.read().await, &repository.id)? else {
        return Ok(UpdateReport::default());
    };
    let spec = RepoSpec::parse(&repository.origin, repository.git_ref.as_deref())
        .map_err(|error| PluginRepoError::BadRequest(error.to_string()))?;
    let subscribed = [Subscription {
        repository,
        entries: Vec::new(),
    }];
    let (records, manifests) = tokio::task::block_in_place(|| {
        let mut records = Vec::new();
        for id in managed_plugin_ids(plugins_dir)? {
            if let Ok(record) = load_managed_record(plugins_dir, &id)
                && !record.pinned
                && find_subscription(&subscribed, &record.origin, record.git_ref.as_deref())
                    .is_some()
            {
                records.push((id, record));
            }
        }
        Ok::<_, PluginRepoError>((records, installed_manifests(plugins_dir)))
    })?;
    let group: Vec<_> = records.iter().collect();
    let client = http_client(&server_build())?;
    let mut listed = list_update_source(&client, &spec, &group, UpdateSelection::All)
        .await
        .map_err(map_resolve_error)?;
    let [Subscription { repository, .. }] = subscribed;
    if store_listed(
        &mut *STATE.db.write().await,
        repository,
        &StoredListing::of(&listed.listing),
    )?
    .is_none()
    {
        return Ok(UpdateReport::default());
    }
    let offered: HashMap<String, OfferedRelease> = listed
        .outcomes
        .iter()
        .filter_map(|(id, outcome)| match outcome {
            UpdateOutcome::Newer { release, .. } => Some((id.clone(), release.clone())),
            _ => None,
        })
        .collect();
    settle_updates([&mut listed], &manifests, false);
    if !install || !spec.is_protected_in_transit() {
        return Ok(available_updates(listed.outcomes, offered));
    }
    let source = resolve_update_source(&client, listed, &UnpackBudget::new())
        .await
        .map_err(map_resolve_error)?;

    let mut report = UpdateReport::default();
    let mut pending = Vec::new();
    for (id, record) in &records {
        take_outcome(&source, id, record, &mut report, &mut pending);
    }
    let _guard = MUTATION_LOCK.lock().await;
    tokio::task::block_in_place(|| {
        install_updates(
            pending,
            plugins_dir,
            InstalledBy::AutomaticUpdate,
            &mut report,
        );
    });
    if !report.updated.is_empty() {
        RELOAD_PENDING.store(true, Ordering::Release);
    }
    Ok(report)
}

/// What a check that installs nothing reports from its settled `outcomes`:
/// the held releases, and the rest of the newer ones it was `offered` as
/// available.
fn available_updates(
    outcomes: HashMap<String, UpdateOutcome>,
    mut offered: HashMap<String, OfferedRelease>,
) -> UpdateReport {
    let mut report = UpdateReport::default();
    for (id, outcome) in outcomes {
        match outcome {
            UpdateOutcome::Held { release, hold } => {
                report.held.push(HeldPlugin { id, release, hold });
            }
            UpdateOutcome::Changed { .. } => {
                if let Some(release) = offered.remove(&id) {
                    report.available.push(AvailableUpdate { id, release });
                }
            }
            _ => {}
        }
    }
    report
}

/// Reloads the runtime when installed updates wait for it and nothing is
/// playing. Returns whether it reloaded.
async fn apply_pending_reload() -> Result<bool, PluginRepoError> {
    if !RELOAD_PENDING.load(Ordering::Acquire)
        || crate::services::delivery::activity::is_playing().await?
    {
        return Ok(false);
    }
    reload_runtime(MUTATION_LOCK.lock().await).await?;
    Ok(true)
}

/// When `repository` is next due: right away until its first check, then
/// a day after the last one, spread by a jitter fixed per subscription. A
/// last check in the future, as after the clock moved back, is due now.
fn due_at(repository: &repo_db::PluginRepository, now_ms: u64) -> u64 {
    let Some(refreshed_at_ms) = repository.refreshed_at_ms.filter(|at| *at <= now_ms) else {
        return 0;
    };
    let mut hasher = DefaultHasher::new();
    repository.id.hash(&mut hasher);
    refreshed_at_ms + CHECK_INTERVAL_MS + hasher.finish() % JITTER_MS
}

/// The most overdue subscription not waiting out a failure. Those in
/// `early` are due right away.
fn next_due<'a>(
    repositories: impl IntoIterator<Item = &'a repo_db::PluginRepository>,
    retries: &Retries,
    early: &HashSet<String>,
    now_ms: u64,
) -> Option<&'a repo_db::PluginRepository> {
    let due_at = |repository: &repo_db::PluginRepository| {
        if early.contains(&repository.id) {
            0
        } else {
            due_at(repository, now_ms)
        }
    };
    repositories
        .into_iter()
        .filter(|repository| due_at(repository) <= now_ms && retries.allows(repository, now_ms))
        .min_by_key(|repository| due_at(repository))
}

/// The wait after a failure, doubled on each further one up to the check
/// interval.
#[derive(Debug, Clone, Copy)]
struct Backoff {
    until_ms: u64,
    delay_ms: u64,
}

impl Backoff {
    /// The wait after failing at `now_ms`: `first_ms`, or twice the
    /// `previous` one, and at least as long as the forge asked for in
    /// `retry_after`.
    fn after(
        previous: Option<&Self>,
        now_ms: u64,
        retry_after: Option<Duration>,
        first_ms: u64,
    ) -> Self {
        let delay_ms = previous.map_or(first_ms, |previous| {
            (previous.delay_ms * 2).min(CHECK_INTERVAL_MS)
        });
        let asked_ms = retry_after.map_or(0, |wait| wait.as_millis() as u64);
        Self {
            until_ms: now_ms + delay_ms.max(asked_ms),
            delay_ms,
        }
    }

    fn allows(&self, now_ms: u64) -> bool {
        self.until_ms <= now_ms
    }
}

/// When failed checks may run again, by subscription id. Kept in memory: a
/// restart retries at its first ticks.
#[derive(Debug, Default)]
struct Retries(HashMap<String, Backoff>);

impl Retries {
    fn allows(&self, repository: &repo_db::PluginRepository, now_ms: u64) -> bool {
        self.0
            .get(&repository.id)
            .is_none_or(|backoff| backoff.allows(now_ms))
    }

    fn succeeded(&mut self, repository: &repo_db::PluginRepository) {
        self.0.remove(&repository.id);
    }

    fn failed(
        &mut self,
        repository: &repo_db::PluginRepository,
        error: &PluginRepoError,
        first_ms: u64,
        now_ms: u64,
    ) {
        let retry_after = match error {
            PluginRepoError::Unavailable { retry_after, .. } => *retry_after,
            _ => None,
        };
        self.delayed(repository, retry_after, first_ms, now_ms);
    }

    /// Holds the next check of `repository` back as a failed one does.
    fn delayed(
        &mut self,
        repository: &repo_db::PluginRepository,
        retry_after: Option<Duration>,
        first_ms: u64,
        now_ms: u64,
    ) {
        let backoff = Backoff::after(self.0.get(&repository.id), now_ms, retry_after, first_ms);
        self.0.insert(repository.id.clone(), backoff);
    }
}

#[cfg(test)]
mod tests {
    use harmony_repository::SourceRecord;
    use semver::Version;

    use super::{
        super::{
            LoadState,
            UpdateStatus,
            installations,
            list_subscriptions,
            tests::{
                catalog_forge,
                changed_at,
                install_records,
                installed_status,
                manifest_json,
                subscribe,
            },
        },
        *,
    };
    use crate::plugins::testing::HOST_REQUIREMENT;

    fn repository(
        id: &str,
        origin: &str,
        refreshed_at_ms: Option<u64>,
    ) -> repo_db::PluginRepository {
        repo_db::PluginRepository {
            db_id: None,
            id: id.to_string(),
            origin: origin.to_string(),
            name: id.to_string(),
            description: String::new(),
            git_ref: None,
            resolved_ref: None,
            commit: None,
            refreshed_at_ms,
        }
    }

    async fn subscribed(origin: &str) -> anyhow::Result<repo_db::PluginRepository> {
        subscribe(origin, &[]).await?;
        Ok(repo_db::get_by_id(&*STATE.db.read().await, "catalog")?.expect("subscribed"))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn automatic_checks_install_plain_updates_and_reload_once_nothing_plays()
    -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::plugins::testing::init_default_test_state()?;
        lyra_db::server::ensure(&mut *STATE.db.write().await)?;
        RELOAD_PENDING.store(false, Ordering::Release);
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
        let repository = subscribed(&forge.origin).await?;

        let report = check_subscription(&repository, plugins_dir.path(), true).await?;

        assert!(
            report.failed.is_empty() && report.held.is_empty(),
            "{report:?}"
        );
        let updated: Vec<_> = report
            .updated
            .iter()
            .map(|u| (u.id.as_str(), u.version.as_str()))
            .collect();
        assert_eq!(updated, [("changed", "2.0.0")]);
        let record = load_managed_record(plugins_dir.path(), "changed")?;
        assert!(record.installed_automatically && record.installed_at.is_some());
        assert!(
            repo_db::get_by_id(&*STATE.db.read().await, "catalog")?
                .and_then(|repository| repository.refreshed_at_ms)
                .is_some(),
            "the check counts as the subscription's refresh"
        );

        assert!(RELOAD_PENDING.load(Ordering::Acquire));
        let playing = crate::services::delivery::activity::begin_stream();
        assert!(
            !apply_pending_reload().await?,
            "nothing reloads while playing"
        );
        assert!(RELOAD_PENDING.load(Ordering::Acquire));
        drop(playing);
        assert!(apply_pending_reload().await?);
        assert!(!RELOAD_PENDING.load(Ordering::Acquire));
        assert!(!apply_pending_reload().await?);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn automatic_checks_skip_pinned_held_and_unknown_plugins_quietly() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::plugins::testing::init_default_test_state()?;
        lyra_db::server::ensure(&mut *STATE.db.write().await)?;
        let mut scoped: serde_json::Value = serde_json::from_str(&changed_at(HOST_REQUIREMENT))?;
        scoped["scopes"] = serde_json::json!(["lyra.metadata"]);
        let unknown =
            changed_at(HOST_REQUIREMENT).replace(r#""scopes":[]"#, r#""scopes":["no.such.scope"]"#);
        for (case, changed, pinned, status) in [
            (
                "pinned",
                changed_at(HOST_REQUIREMENT),
                true,
                UpdateStatus::UpToDate,
            ),
            (
                "adds scopes",
                scoped.to_string(),
                false,
                UpdateStatus::UpdateAvailable,
            ),
            (
                "unknown",
                unknown,
                false,
                UpdateStatus::Unknown {
                    reason: String::new(),
                },
            ),
        ] {
            let forge = catalog_forge(HOST_REQUIREMENT, HOST_REQUIREMENT, changed).await?;
            let plugins_dir = tempfile::TempDir::new()?;
            install_records(plugins_dir.path(), &forge.origin, &[("changed", "1.0.0")])?;
            if pinned {
                let mut record = load_managed_record(plugins_dir.path(), "changed")?;
                record.pinned = true;
                record.store(&plugins_dir.path().join("changed"))?;
            }
            let repository = subscribed(&forge.origin).await?;

            let report = check_subscription(&repository, plugins_dir.path(), true).await?;

            assert!(
                report.updated.is_empty() && report.failed.is_empty(),
                "{case}: {report:?}"
            );
            assert_eq!(
                report.held.len(),
                usize::from(case == "adds scopes"),
                "{case}"
            );
            assert!(
                forge.downloads().is_empty(),
                "{case}: {:?}",
                forge.downloads()
            );
            let record = load_managed_record(plugins_dir.path(), "changed")?;
            assert_eq!(record.version, Version::new(1, 0, 0), "{case}");
            let installed = installed_status(
                "changed",
                &record,
                &list_subscriptions().await?,
                &installed_manifests(plugins_dir.path()),
            );
            match (&installed.status, &status) {
                (UpdateStatus::Unknown { reason }, UpdateStatus::Unknown { .. }) => {
                    assert!(reason.contains("unknown scope"), "{case}: {reason}");
                }
                _ => assert_eq!(installed.status, status, "{case}"),
            }
            repo_db::remove(
                &mut *STATE.db.write().await,
                repository.db_id.expect("stored"),
            )?;
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn automatic_checks_only_report_updates_while_the_setting_is_off() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::plugins::testing::init_test_state_with_file_settings(
            [("plugins.auto_update".to_string(), serde_json::json!(false))]
                .into_iter()
                .collect(),
        )?;
        lyra_db::server::ensure(&mut *STATE.db.write().await)?;
        let forge = catalog_forge(
            HOST_REQUIREMENT,
            HOST_REQUIREMENT,
            changed_at(HOST_REQUIREMENT),
        )
        .await?;
        let plugins_dir = tempfile::TempDir::new()?;
        install_records(plugins_dir.path(), &forge.origin, &[("changed", "1.0.0")])?;
        subscribed(&forge.origin).await?;

        check_next(
            plugins_dir.path(),
            &mut Retries::default(),
            &mut EarlyChecks::default(),
            Due::Any,
            now_ms(),
        )
        .await;
        let repository = repo_db::get_by_id(&*STATE.db.read().await, "catalog")?.expect("stored");
        let report = check_subscription(&repository, plugins_dir.path(), false).await?;

        let available: Vec<_> = report
            .available
            .iter()
            .map(|plugin| (plugin.id.as_str(), plugin.release.version.to_string()))
            .collect();
        assert_eq!(available, [("changed", "2.0.0".to_string())]);
        assert!(forge.downloads().is_empty(), "{:?}", forge.downloads());
        let record = load_managed_record(plugins_dir.path(), "changed")?;
        assert_eq!(record.version, Version::new(1, 0, 0));
        let status = installed_status(
            "changed",
            &record,
            &list_subscriptions().await?,
            &installed_manifests(plugins_dir.path()),
        );
        assert_eq!(status.status, UpdateStatus::UpdateAvailable);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn checks_store_what_they_listed_on_the_subscription_checked() -> anyhow::Result<()> {
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
        let variant = repo_db::PluginRepository {
            db_id: None,
            id: "variant".into(),
            origin: forge.origin.replace("/o/r", "/O/R"),
            name: "Catalog".into(),
            description: String::new(),
            git_ref: None,
            resolved_ref: None,
            commit: None,
            refreshed_at_ms: None,
        };
        repo_db::create(&mut *STATE.db.write().await, &variant, &[])?;
        let repository = subscribed(&forge.origin).await?;

        check_subscription(&repository, plugins_dir.path(), true).await?;

        let db = STATE.db.read().await;
        let refreshed = |id| -> anyhow::Result<Option<u64>> {
            Ok(repo_db::get_by_id(&*db, id)?.and_then(|repository| repository.refreshed_at_ms))
        };
        assert!(refreshed("catalog")?.is_some());
        assert!(refreshed("variant")?.is_none());
        Ok(())
    }

    /// A server requirement no server meets.
    const ANOTHER_SERVER: &str = "=0.0.0";

    /// Installs `changed` 1.0.0 from `origin` into `plugins_dir`, has `edit`
    /// change what was installed, and loads the plugins from there.
    async fn load_changed(
        plugins_dir: &Path,
        origin: &str,
        edit: impl FnOnce(&Path) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        install_records(plugins_dir, origin, &[("changed", "1.0.0")])?;
        edit(&plugins_dir.join("changed"))?;
        crate::plugins::testing::use_plugins_dir(plugins_dir);
        crate::plugins::STATE
            .generation()
            .registries
            .reload_all_plugins()
            .await
    }

    /// Makes the installed plugin in `dir` require another server.
    fn require_another_server(dir: &Path) -> anyhow::Result<()> {
        std::fs::write(
            dir.join(harmony_core::plugin::PLUGIN_CONFIG_FILENAME),
            manifest_json("changed", "1.0.0", ANOTHER_SERVER),
        )?;
        Ok(())
    }

    /// A subscription to `origin` checked just now, so not due for a day.
    async fn checked_just_now(origin: &str) -> anyhow::Result<repo_db::PluginRepository> {
        let mut repository = subscribed(origin).await?;
        repository.refreshed_at_ms = Some(now_ms());
        repo_db::update(&mut *STATE.db.write().await, &repository, &[])?;
        assert!(
            next_due(
                [&repository],
                &Retries::default(),
                &HashSet::new(),
                now_ms()
            )
            .is_none(),
            "not due by its day"
        );
        Ok(repository)
    }

    async fn check_early(plugins_dir: &Path, retries: &mut Retries, now_ms: u64) -> bool {
        check_next(
            plugins_dir,
            retries,
            &mut EarlyChecks::default(),
            Due::Early,
            now_ms,
        )
        .await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn plugins_refused_for_the_server_version_are_updated_ahead_of_the_daily_check()
    -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::plugins::testing::init_default_test_state()?;
        lyra_db::server::ensure(&mut *STATE.db.write().await)?;
        RELOAD_PENDING.store(false, Ordering::Release);
        let forge = catalog_forge(
            HOST_REQUIREMENT,
            HOST_REQUIREMENT,
            changed_at(HOST_REQUIREMENT),
        )
        .await?;
        let plugins_dir = tempfile::TempDir::new()?;
        load_changed(plugins_dir.path(), &forge.origin, require_another_server).await?;
        checked_just_now(&forge.origin).await?;
        assert!(crate::plugins::STATE.manifests().is_empty());

        let (mut retries, mut early) = (Retries::default(), EarlyChecks::default());
        check_refused(plugins_dir.path(), &mut retries, &mut early).await;

        let record = load_managed_record(plugins_dir.path(), "changed")?;
        assert_eq!(record.version, Version::new(2, 0, 0));
        assert!(record.installed_automatically);
        assert_eq!(forge.downloads().len(), 1);
        assert!(
            !check_next(
                plugins_dir.path(),
                &mut retries,
                &mut early,
                Due::Early,
                now_ms()
            )
            .await,
            "the update waiting for its reload is not checked again"
        );

        reload_when_idle(&mut None, now_ms()).await;

        let loaded: Vec<_> = crate::plugins::STATE
            .manifests()
            .iter()
            .map(|manifest| (manifest.id.clone(), manifest.version.to_string()))
            .collect();
        assert_eq!(loaded, [("changed".to_string(), "2.0.0".to_string())]);
        assert!(crate::plugins::STATE.discovery_errors().is_empty());
        assert!(
            !check_next(
                plugins_dir.path(),
                &mut retries,
                &mut early,
                Due::Early,
                now_ms()
            )
            .await
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn early_checks_only_report_updates_while_the_setting_is_off() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::plugins::testing::init_test_state_with_file_settings(
            [("plugins.auto_update".to_string(), serde_json::json!(false))]
                .into_iter()
                .collect(),
        )?;
        lyra_db::server::ensure(&mut *STATE.db.write().await)?;
        RELOAD_PENDING.store(false, Ordering::Release);
        let forge = catalog_forge(
            HOST_REQUIREMENT,
            HOST_REQUIREMENT,
            changed_at(HOST_REQUIREMENT),
        )
        .await?;
        let plugins_dir = tempfile::TempDir::new()?;
        load_changed(plugins_dir.path(), &forge.origin, require_another_server).await?;
        checked_just_now(&forge.origin).await?;

        let (mut retries, mut early) = (Retries::default(), EarlyChecks::default());
        check_refused(plugins_dir.path(), &mut retries, &mut early).await;

        assert!(forge.downloads().is_empty(), "{:?}", forge.downloads());
        assert!(!RELOAD_PENDING.load(Ordering::Acquire));
        let (manifests, errors) = crate::plugins::STATE.discovered();
        let listed = installations(
            &manifests,
            &errors,
            plugins_dir.path(),
            &list_subscriptions().await?,
        )?;
        let [changed] = &listed[..] else {
            panic!("{listed:?}");
        };
        assert!(matches!(changed.load, LoadState::NotLoaded { .. }));
        let crate::plugins::repositories::InstallSource::Repository(install) = &changed.source
        else {
            panic!("{changed:?}");
        };
        let (record, status) = (&install.record, &install.status);
        assert_eq!(record.version, Version::new(1, 0, 0));
        assert_eq!(status.status, UpdateStatus::UpdateAvailable);
        assert!(
            !check_next(
                plugins_dir.path(),
                &mut retries,
                &mut early,
                Due::Early,
                now_ms()
            )
            .await,
            "the refused install is checked early once"
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn early_checks_leave_pinned_local_and_otherwise_unloaded_plugins_to_their_day()
    -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        type Edit = fn(&Path) -> anyhow::Result<()>;
        let cases: [(&str, Edit); 3] = [
            ("pinned", |dir| {
                require_another_server(dir)?;
                let mut record = SourceRecord::load(dir)?.expect("installed");
                record.pinned = true;
                Ok(record.store(dir)?)
            }),
            ("local", |dir| {
                require_another_server(dir)?;
                Ok(std::fs::remove_file(dir.join(
                    harmony_repository::manifest::SOURCE_RECORD_FILENAME,
                ))?)
            }),
            ("unreadable", |dir| {
                Ok(std::fs::write(
                    dir.join(harmony_core::plugin::PLUGIN_CONFIG_FILENAME),
                    "{",
                )?)
            }),
        ];
        for (case, edit) in cases {
            crate::plugins::testing::init_default_test_state()?;
            lyra_db::server::ensure(&mut *STATE.db.write().await)?;
            let forge = catalog_forge(
                HOST_REQUIREMENT,
                HOST_REQUIREMENT,
                changed_at(HOST_REQUIREMENT),
            )
            .await?;
            let plugins_dir = tempfile::TempDir::new()?;
            load_changed(plugins_dir.path(), &forge.origin, edit).await?;
            let repository = checked_just_now(&forge.origin).await?;
            assert_eq!(
                crate::plugins::STATE.discovery_errors().len(),
                1,
                "{case}: not loaded"
            );

            assert!(
                !check_early(plugins_dir.path(), &mut Retries::default(), now_ms()).await,
                "{case}"
            );
            let stored = repo_db::get_by_id(&*STATE.db.read().await, "catalog")?.expect("stored");
            assert_eq!(stored.refreshed_at_ms, repository.refreshed_at_ms, "{case}");
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn failed_early_checks_wait_out_their_backoff() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::plugins::testing::init_default_test_state()?;
        lyra_db::server::ensure(&mut *STATE.db.write().await)?;
        let unreachable = {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            format!("http://{}/o/r", listener.local_addr()?)
        };
        let plugins_dir = tempfile::TempDir::new()?;
        load_changed(plugins_dir.path(), &unreachable, require_another_server).await?;
        checked_just_now(&unreachable).await?;

        let (mut retries, mut early) = (Retries::default(), EarlyChecks::default());
        check_refused(plugins_dir.path(), &mut retries, &mut early).await;

        let now = now_ms();
        let mut check = async |now_ms| {
            check_next(
                plugins_dir.path(),
                &mut retries,
                &mut early,
                Due::Early,
                now_ms,
            )
            .await
        };
        assert!(!check(now).await, "the failed check is not repeated");
        assert!(!check(now + EARLY_RETRY_MIN_MS / 2).await);
        assert!(check(now + EARLY_RETRY_MIN_MS).await, "it is tried again");
        assert!(
            !check(now + 2 * EARLY_RETRY_MIN_MS).await,
            "a further failure waits twice as long"
        );
        let record = load_managed_record(plugins_dir.path(), "changed")?;
        assert_eq!(record.version, Version::new(1, 0, 0));
        Ok(())
    }

    #[test]
    fn installs_that_fail_to_update_stay_due_early() {
        let refused = |plugin: &str| RefusedInstall {
            subscription: "catalog".into(),
            plugin: plugin.into(),
            commit: "a".repeat(40),
        };
        let installs = vec![refused("failed"), refused("held")];
        let report = UpdateReport {
            failed: vec![super::super::FailedInstall {
                id: "failed".into(),
                error: "download failed".into(),
            }],
            ..UpdateReport::default()
        };
        let mut early = EarlyChecks::default();
        assert_eq!(early.waiting(&installs), HashSet::from(["catalog".into()]));

        assert!(!early.checked("other", installs.clone(), &report));
        assert!(early.checked("catalog", installs.clone(), &report));

        assert_eq!(
            early.waiting(&installs[..1]),
            HashSet::from(["catalog".into()])
        );
        assert!(early.waiting(&installs[1..]).is_empty());
        let reinstalled = RefusedInstall {
            commit: "b".repeat(40),
            ..refused("held")
        };
        assert_eq!(
            early.waiting(&[reinstalled]),
            HashSet::from(["catalog".into()]),
            "another install of the plugin is checked early again"
        );
    }

    #[test]
    fn subscriptions_are_due_a_jittered_day_after_their_last_check() {
        let day = CHECK_INTERVAL_MS;
        let checked = repository("checked", "https://github.com/o/a", Some(1_000));
        let jitter = due_at(&checked, 1_000) - 1_000 - day;
        assert!(jitter < JITTER_MS);
        assert_eq!(
            due_at(&checked, 1_000),
            due_at(&checked, 5_000),
            "the jitter is fixed"
        );

        let never = repository("never", "https://github.com/o/b", None);
        let repositories = [checked.clone(), never];
        let retries = Retries::default();
        assert_eq!(
            next_due(&repositories, &retries, &HashSet::new(), 2_000).map(|r| r.id.as_str()),
            Some("never")
        );
        let repositories = [checked.clone()];
        assert!(
            next_due(
                &repositories,
                &retries,
                &HashSet::new(),
                1_000 + day + jitter - 1
            )
            .is_none()
        );
        assert_eq!(
            next_due(
                &repositories,
                &retries,
                &HashSet::new(),
                1_000 + day + jitter
            )
            .map(|r| r.id.as_str()),
            Some("checked")
        );
    }

    #[test]
    fn subscriptions_last_checked_in_the_future_are_due() {
        let ahead = repository("ahead", "https://github.com/o/a", Some(10_000));
        let repositories = [ahead];
        assert_eq!(
            next_due(&repositories, &Retries::default(), &HashSet::new(), 5_000)
                .map(|r| r.id.as_str()),
            Some("ahead")
        );
    }

    #[test]
    fn failed_checks_back_off_for_at_least_as_long_as_the_forge_asked() {
        let a = repository("a", "https://github.com/o/a", None);
        let b = repository("b", "https://github.com/o/b", None);
        let mut retries = Retries::default();
        let down = PluginRepoError::BadGateway("down".into());

        retries.failed(&a, &down, RETRY_MIN_MS, 0);
        assert!(!retries.allows(&a, RETRY_MIN_MS - 1));
        assert!(retries.allows(&a, RETRY_MIN_MS));
        assert!(retries.allows(&b, 0));
        retries.failed(&a, &down, RETRY_MIN_MS, RETRY_MIN_MS);
        assert!(!retries.allows(&a, 3 * RETRY_MIN_MS - 1));
        retries.succeeded(&a);
        assert!(retries.allows(&a, 0));

        let limited = |seconds| PluginRepoError::Unavailable {
            message: "rate limited".into(),
            retry_after: Some(Duration::from_secs(seconds)),
        };
        retries.failed(&a, &limited(30), RETRY_MIN_MS, 0);
        assert!(
            !retries.allows(&a, RETRY_MIN_MS - 1),
            "the backoff is the least wait"
        );
        retries.succeeded(&a);
        retries.failed(&a, &limited(3 * 60 * 60), RETRY_MIN_MS, 0);
        assert!(!retries.allows(&a, 3 * 60 * 60 * 1000 - 1));
        assert!(retries.allows(&a, 3 * 60 * 60 * 1000));
    }

    #[test]
    fn failed_reloads_back_off_up_to_the_check_interval() {
        let mut backoff = Backoff::after(None, 0, None, RETRY_MIN_MS);
        assert_eq!(backoff.until_ms, RETRY_MIN_MS);
        for _ in 0..10 {
            backoff = Backoff::after(Some(&backoff), 0, None, RETRY_MIN_MS);
        }
        assert_eq!(backoff.until_ms, CHECK_INTERVAL_MS);
    }
}
