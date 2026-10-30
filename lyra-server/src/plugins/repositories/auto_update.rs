// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! The daily update check of subscribed repositories. Each tick checks at
//! most one due subscription, so a restart never checks them in a burst,
//! and reloads the runtime for installed updates once nothing is playing.

use std::{
    collections::HashMap,
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

/// Runs the daily update checks until `shutdown`, which also abandons a
/// check in flight. A reload that has started always finishes.
pub(crate) async fn run_update_checks(shutdown: CancellationToken) {
    let mut retries = Retries::default();
    let mut reload_wait: Option<Backoff> = None;
    let mut ticks = tokio::time::interval_at(tokio::time::Instant::now() + TICK, TICK);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
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
            () = check_next(&boot.plugins_dir, &mut retries, now_ms()) => {}
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
            *wait = Some(Backoff::after(wait.as_ref(), now_ms, None));
            tracing::warn!(%error, "reloading plugins for automatic updates failed");
        }
    }
}

/// Checks the most overdue subscription, if any is due.
async fn check_next(plugins_dir: &Path, retries: &mut Retries, now_ms: u64) {
    let repositories = {
        let db = STATE.db.read().await;
        match repo_db::list(&*db) {
            Ok(repositories) => repositories,
            Err(error) => {
                tracing::warn!(%error, "listing plugin repositories for update checks failed");
                return;
            }
        }
    };
    let Some(repository) = next_due(&repositories, retries, now_ms) else {
        return;
    };
    let install = STATE.config().plugins.auto_update;
    match check_subscription(repository, plugins_dir, install).await {
        Ok(report) => {
            retries.succeeded(repository);
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
            retries.failed(repository, &error, now_ms);
        }
    }
}

/// Set when an automatic update installed files the runtime has not loaded
/// yet; cleared by any reload.
pub(super) static RELOAD_PENDING: AtomicBool = AtomicBool::new(false);

/// Lists one subscribed `repository`, stores what it offers on it, and
/// judges the newer releases of its installed plugins. When `install` is
/// set, those that need no approval are reinstalled, recorded as
/// automatic; otherwise they are only reported as available. The runtime
/// is not reloaded; that waits for [`apply_pending_reload`]. A
/// subscription removed in the meantime is not checked.
async fn check_subscription(
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
    let mut records = Vec::new();
    for id in managed_plugin_ids(plugins_dir)? {
        if let Ok(record) = load_managed_record(plugins_dir, &id)
            && !record.pinned
            && find_subscription(&subscribed, &record.origin, record.git_ref.as_deref()).is_some()
        {
            records.push((id, record));
        }
    }
    let group: Vec<_> = records.iter().collect();
    let manifests = installed_manifests(plugins_dir);
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
    if !install {
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
    install_updates(
        pending,
        plugins_dir,
        InstalledBy::AutomaticUpdate,
        &mut report,
    );
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

/// The most overdue subscription not waiting out a failure.
fn next_due<'a>(
    repositories: &'a [repo_db::PluginRepository],
    retries: &Retries,
    now_ms: u64,
) -> Option<&'a repo_db::PluginRepository> {
    repositories
        .iter()
        .filter(|repository| {
            due_at(repository, now_ms) <= now_ms && retries.allows(repository, now_ms)
        })
        .min_by_key(|repository| due_at(repository, now_ms))
}

/// The wait after a failure, doubled on each further one up to the check
/// interval.
#[derive(Debug, Clone, Copy)]
struct Backoff {
    until_ms: u64,
    delay_ms: u64,
}

impl Backoff {
    /// The wait after failing at `now_ms`, following the `previous` one,
    /// and at least as long as the forge asked for in `retry_after`.
    fn after(previous: Option<&Self>, now_ms: u64, retry_after: Option<Duration>) -> Self {
        let delay_ms = previous.map_or(RETRY_MIN_MS, |previous| {
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
        now_ms: u64,
    ) {
        let retry_after = match error {
            PluginRepoError::Unavailable { retry_after, .. } => *retry_after,
            _ => None,
        };
        let backoff = Backoff::after(self.0.get(&repository.id), now_ms, retry_after);
        self.0.insert(repository.id.clone(), backoff);
    }
}

#[cfg(test)]
mod tests {
    use semver::Version;

    use super::{
        super::{
            UpdateStatus,
            list_subscriptions,
            tests::{
                catalog_forge,
                changed_at,
                install_records,
                installed_status,
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

    #[tokio::test]
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

    #[tokio::test]
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

    #[tokio::test]
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

        check_next(plugins_dir.path(), &mut Retries::default(), now_ms()).await;
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

    #[tokio::test]
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
            next_due(&repositories, &retries, 2_000).map(|r| r.id.as_str()),
            Some("never")
        );
        let repositories = [checked.clone()];
        assert!(next_due(&repositories, &retries, 1_000 + day + jitter - 1).is_none());
        assert_eq!(
            next_due(&repositories, &retries, 1_000 + day + jitter).map(|r| r.id.as_str()),
            Some("checked")
        );
    }

    #[test]
    fn subscriptions_last_checked_in_the_future_are_due() {
        let ahead = repository("ahead", "https://github.com/o/a", Some(10_000));
        let repositories = [ahead];
        assert_eq!(
            next_due(&repositories, &Retries::default(), 5_000).map(|r| r.id.as_str()),
            Some("ahead")
        );
    }

    #[test]
    fn failed_checks_back_off_for_at_least_as_long_as_the_forge_asked() {
        let a = repository("a", "https://github.com/o/a", None);
        let b = repository("b", "https://github.com/o/b", None);
        let mut retries = Retries::default();
        let down = PluginRepoError::BadGateway("down".into());

        retries.failed(&a, &down, 0);
        assert!(!retries.allows(&a, RETRY_MIN_MS - 1));
        assert!(retries.allows(&a, RETRY_MIN_MS));
        assert!(retries.allows(&b, 0));
        retries.failed(&a, &down, RETRY_MIN_MS);
        assert!(!retries.allows(&a, 3 * RETRY_MIN_MS - 1));
        retries.succeeded(&a);
        assert!(retries.allows(&a, 0));

        let limited = |seconds| PluginRepoError::Unavailable {
            message: "rate limited".into(),
            retry_after: Some(Duration::from_secs(seconds)),
        };
        retries.failed(&a, &limited(30), 0);
        assert!(
            !retries.allows(&a, RETRY_MIN_MS - 1),
            "the backoff is the least wait"
        );
        retries.succeeded(&a);
        retries.failed(&a, &limited(3 * 60 * 60), 0);
        assert!(!retries.allows(&a, 3 * 60 * 60 * 1000 - 1));
        assert!(retries.allows(&a, 3 * 60 * 60 * 1000));
    }

    #[test]
    fn failed_reloads_back_off_up_to_the_check_interval() {
        let mut backoff = Backoff::after(None, 0, None);
        assert_eq!(backoff.until_ms, RETRY_MIN_MS);
        for _ in 0..10 {
            backoff = Backoff::after(Some(&backoff), 0, None);
        }
        assert_eq!(backoff.until_ms, CHECK_INTERVAL_MS);
    }
}
