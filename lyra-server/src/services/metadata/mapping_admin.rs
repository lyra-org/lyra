// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    sync::atomic::{
        AtomicBool,
        Ordering,
    },
    time::Duration,
};

use anyhow::Context;

use crate::{
    services::STATE,
    services::{
        libraries::{
            SyncRunStatus,
            start_library_sync,
            wait_for_sync_runs,
        },
        metadata::{
            SkipReason,
            classify_entry_file_kind,
            ingestion::metadata_groups,
            mapping::{
                MissingRequiredField,
                ensure_config,
                update_config,
            },
            parse_metadata,
        },
    },
};
use lyra_db::metadata::mapping_config::MetadataMappingConfig;

/// Caps response size on libraries with widespread breakage.
const DRY_RUN_SAMPLE_LIMIT: usize = 10;

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Default, serde::Serialize)]
pub(crate) struct DryRunReport {
    pub total_files: usize,
    pub would_reject: usize,
    pub read_failures: usize,
    pub rejected_samples: Vec<RejectedSample>,
    pub read_failure_samples: Vec<ReadFailureSample>,
}

impl DryRunReport {
    fn push_rejected(&mut self, path: String, missing: Vec<MissingRequiredField>) {
        self.would_reject += 1;
        if self.rejected_samples.len() < DRY_RUN_SAMPLE_LIMIT {
            self.rejected_samples.push(RejectedSample {
                path,
                missing: missing
                    .into_iter()
                    .map(|m| missing_name(m).to_string())
                    .collect(),
            });
        }
    }

    fn push_read_failure(&mut self, path: String, error: String) {
        self.read_failures += 1;
        if self.read_failure_samples.len() < DRY_RUN_SAMPLE_LIMIT {
            self.read_failure_samples
                .push(ReadFailureSample { path, error });
        }
    }
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct RejectedSample {
    pub path: String,
    pub missing: Vec<String>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct ReadFailureSample {
    pub path: String,
    pub error: String,
}

fn missing_name(m: MissingRequiredField) -> &'static str {
    match m {
        MissingRequiredField::Title => "title",
        MissingRequiredField::Album => "album",
        MissingRequiredField::ArtistKey => "artist_key",
    }
}

/// Full scan; expect multi-second latency on large libraries.
pub(crate) async fn dry_run_config(
    candidate: &MetadataMappingConfig,
) -> anyhow::Result<DryRunReport> {
    let libraries = {
        let db = STATE.db.read().await;
        let libraries = lyra_db::libraries::get(&db)?;
        let mut all = Vec::new();
        for library in libraries {
            let Some(library_db_id) = library.db_id else {
                continue;
            };
            all.push((
                library.path.clone(),
                lyra_db::entries::get(&db, library_db_id)?,
            ));
        }
        all
    };

    let mut groups = Vec::new();
    for (library_root, entries) in libraries {
        groups.extend(metadata_groups(&library_root, entries, &Default::default()).await);
    }

    let mut report = DryRunReport::default();

    for group in groups {
        let entries = group.entries;
        report.total_files += entries
            .iter()
            .filter(|entry| classify_entry_file_kind(entry).as_deref() == Some("audio"))
            .count();

        for record in parse_metadata(candidate, entries, &Default::default())
            .await?
            .skipped
        {
            let path = record.path.to_string_lossy().to_string();
            match record.reason {
                SkipReason::RequiredFieldEmpty(missing) => report.push_rejected(path, missing),
                SkipReason::ReadFailed(error) => report.push_read_failure(path, error.message),
                SkipReason::TaskFailed(error) => {
                    report.push_read_failure(path, format!("task failed: {error}"));
                }
                SkipReason::CueAlreadyClaimed
                | SkipReason::CueParseFailed(_)
                | SkipReason::CueUnreadable => {}
            }
        }
    }

    Ok(report)
}

/// Read by the reingest request gate to fence concurrent writes.
static REINGEST_ACTIVE: AtomicBool = AtomicBool::new(false);

pub(crate) fn reingest_in_progress() -> bool {
    REINGEST_ACTIVE.load(Ordering::Acquire)
}

/// Obtained via [`ReingestGuard::try_acquire`] at the route boundary
/// and consumed by [`commit_and_reingest`]. Routing the flag through
/// an RAII token forces the CAS to happen before the 202 response,
/// so two concurrent PUTs can't both see "not in progress" and both
/// get an accepted response with only one actually doing the work.
pub(crate) struct ReingestGuard(());

impl ReingestGuard {
    pub(crate) fn try_acquire() -> Option<Self> {
        if REINGEST_ACTIVE
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            Some(Self(()))
        } else {
            None
        }
    }
}

impl Drop for ReingestGuard {
    fn drop(&mut self) {
        REINGEST_ACTIVE.store(false, Ordering::Release);
    }
}

#[derive(Debug, Default, Clone)]
pub(crate) struct ReingestSummary {
    pub(crate) libraries_reingested: usize,
    pub(crate) libraries_failed: Vec<(String, String)>,
}

/// Stores `config` and reingests every library, restoring the previous
/// rules if any library fails. Existing tracks keep their identities.
pub(crate) async fn commit_and_reingest(
    config: MetadataMappingConfig,
    _guard: ReingestGuard,
) -> anyhow::Result<ReingestSummary> {
    let previous = {
        let mut db = STATE.db.write().await;
        let current = ensure_config(&mut db)?;
        update_config(&mut db, &config)?;
        current
    };

    let libraries = {
        let db = STATE.db.read().await;
        lyra_db::libraries::get(&db)?
    };

    let shutdown = crate::services::shutdown::token();
    let finished = tokio::select! {
        summary = reingest_libraries(libraries) => Some(summary),
        () = shutdown.cancelled() => None,
    };
    let Some(summary) = finished else {
        restore_rules(&previous).await;
        anyhow::bail!(
            "server shut down before the reingest finished; rolled back the mapping rules"
        );
    };

    if summary.libraries_failed.is_empty() {
        Ok(summary)
    } else {
        restore_rules(&previous).await;
        Err(anyhow::anyhow!(
            "reingest completed with {} library failure(s); rolled back the mapping rules",
            summary.libraries_failed.len(),
        ))
        .with_context(|| format!("reingested {} libraries", summary.libraries_reingested))
    }
}

async fn restore_rules(previous: &MetadataMappingConfig) {
    let rollback_result = {
        let mut db = STATE.db.write().await;
        update_config(&mut db, previous)
    };
    if let Err(rollback_err) = rollback_result {
        tracing::error!(error = %rollback_err, "failed to roll back metadata mapping config");
    }
}

/// Syncs every library and waits for each sync to end.
async fn reingest_libraries(libraries: Vec<lyra_db::Library>) -> ReingestSummary {
    let mut summary = ReingestSummary::default();
    let db_async = STATE.db.get();
    let mut started = Vec::new();
    for library in libraries {
        let name = library.name.clone();
        match start_reingest(db_async.clone(), library).await {
            Ok(run_id) => started.push((name, run_id)),
            Err(err) => {
                tracing::error!(library = %name, error = %err, "reingest kickoff failed");
                summary.libraries_failed.push((name, err.to_string()));
            }
        }
    }

    let run_ids = started
        .iter()
        .map(|(_, run_id)| run_id.clone())
        .collect::<Vec<_>>();
    match wait_for_sync_runs(&run_ids).await {
        Ok(ended) => {
            for ((name, _), run) in started.into_iter().zip(ended) {
                let status = run.run.status;
                if status == SyncRunStatus::Succeeded {
                    summary.libraries_reingested += 1;
                    continue;
                }
                let reason = run.run.error.unwrap_or_else(|| {
                    match status {
                        SyncRunStatus::Cancelled => "sync run cancelled",
                        _ => "sync run failed",
                    }
                    .to_string()
                });
                tracing::error!(library = %name, ?status, error = %reason, "reingest run did not succeed");
                summary.libraries_failed.push((name, reason));
            }
        }
        Err(err) => {
            tracing::error!(error = %format!("{err:#}"), "reingest runs could not be read");
            for (name, _) in started {
                summary.libraries_failed.push((name, format!("{err:#}")));
            }
        }
    }
    summary
}

/// How long shutdown waits for a reingest to finish, or to restore the rules it replaced.
const REINGEST_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// Waits, up to [`REINGEST_SHUTDOWN_TIMEOUT`], for a running reingest to end. Once shutdown
/// begins a reingest stops waiting for its runs and restores the rules it replaced.
pub(crate) async fn wait_for_reingest() {
    let ended = tokio::time::timeout(REINGEST_SHUTDOWN_TIMEOUT, async {
        while REINGEST_ACTIVE.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    if ended.is_err() {
        tracing::warn!("metadata mapping reingest still running at shutdown");
    }
}

/// Starts a sync of `library` under the stored rules and returns its run id. A run already going
/// there may have started under the earlier rules, so it is waited out and a sync started after it.
async fn start_reingest(db: lyra_db::DbAsync, library: lyra_db::Library) -> anyhow::Result<String> {
    let mut response = start_library_sync(db.clone(), library.clone()).await?;
    if !response.started
        && let Some(existing) = response.run.run.id.clone()
    {
        wait_for_sync_runs(&[existing]).await?;
        response = start_library_sync(db, library).await?;
    }
    anyhow::ensure!(response.started, "another run kept the library busy");
    response
        .run
        .run
        .id
        .ok_or_else(|| anyhow::anyhow!("started sync run has no id"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::{
        libraries::scanning::prepare_entry_scan_plan,
        metadata::{
            mapping::default_config,
            tests::fixture_with_comments,
        },
    };

    #[tokio::test]
    async fn a_failed_reingest_run_restores_the_previous_rules() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let dir = tempfile::tempdir()?;
        let previous = {
            let mut db = STATE.db.write().await;
            lyra_db::test_db::insert_library(
                &mut db,
                "Gone",
                dir.path().join("missing").to_str().expect("utf-8 path"),
            )?;
            ensure_config(&mut db)?
        };
        assert!(!previous.rules.is_empty());

        let reingest = commit_and_reingest(
            MetadataMappingConfig { rules: Vec::new() },
            ReingestGuard::try_acquire().expect("no reingest running"),
        );
        let result = tokio::time::timeout(Duration::from_secs(10), reingest).await?;

        assert!(result.is_err());
        let restored = ensure_config(&mut *STATE.db.write().await)?;
        assert_eq!(
            serde_json::to_value(&restored)?,
            serde_json::to_value(&previous)?
        );
        Ok(())
    }

    struct ShutdownResetGuard;

    impl Drop for ShutdownResetGuard {
        fn drop(&mut self) {
            crate::services::shutdown::reset();
        }
    }

    #[tokio::test]
    async fn shutdown_restores_the_rules_of_an_unfinished_reingest() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        let _shutdown = ShutdownResetGuard;
        crate::services::testing::init_default_test_state()?;
        crate::services::shutdown::reset();
        let (library, previous) = {
            let mut db = STATE.db.write().await;
            let library = lyra_db::test_db::insert_library(&mut db, "Busy", "/busy")?;
            let library = lyra_db::libraries::get_by_id(&db, library)?.expect("library exists");
            (library, ensure_config(&mut db)?)
        };
        // A run that never ends keeps the reingest waiting to start its own.
        let running = crate::services::libraries::begin_test_run(&library).await;
        let reingest = tokio::spawn(commit_and_reingest(
            MetadataMappingConfig { rules: Vec::new() },
            ReingestGuard::try_acquire().expect("no reingest running"),
        ));
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            ensure_config(&mut *STATE.db.write().await)?
                .rules
                .is_empty()
        );

        crate::services::shutdown::cancel();
        tokio::time::timeout(Duration::from_secs(1), wait_for_reingest()).await?;

        assert!(reingest.await?.is_err());
        let restored = ensure_config(&mut *STATE.db.write().await)?;
        assert_eq!(
            serde_json::to_value(&restored)?,
            serde_json::to_value(&previous)?
        );
        crate::services::libraries::end_test_run(&running).await;
        Ok(())
    }

    #[tokio::test]
    async fn dry_run_rejects_only_tracks_ingestion_skips() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let dir = tempfile::tempdir()?;
        let inferred_title = dir.path().join("Folder Album/01 - Path Title.flac");
        let untagged_album = dir.path().join("Folder Album/02 - Two.flac");
        fixture_with_comments(&inferred_title, "integration_track.flac", &["TITLE"], &[])?;
        fixture_with_comments(&untagged_album, "integration_track.flac", &["ALBUM"], &[])?;
        {
            let mut db = STATE.db.write().await;
            let library = db.transaction_mut(|t| {
                lyra_db::libraries::create_system(
                    t,
                    lyra_db::libraries::LibraryInsert {
                        id: nanoid::nanoid!(),
                        name: "Music".to_string(),
                        path: dir.path().to_path_buf(),
                        path_key: lyra_db::libraries::path_key_for(dir.path()),
                        language: None,
                        country: None,
                    },
                )
            })?;
            for group in prepare_entry_scan_plan(&library, Vec::new())?.groups {
                lyra_db::entries::sync_entry_group(&mut db, &library, group.entries)?;
            }
        }

        let report = dry_run_config(&default_config()).await?;

        assert_eq!(report.total_files, 2);
        assert_eq!(report.read_failures, 0);
        assert_eq!(report.would_reject, 1);
        assert_eq!(
            report.rejected_samples[0].path,
            untagged_album.to_string_lossy()
        );
        assert_eq!(report.rejected_samples[0].missing, ["album"]);
        Ok(())
    }
}
