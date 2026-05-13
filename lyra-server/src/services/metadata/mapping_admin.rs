// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::sync::atomic::{
    AtomicBool,
    Ordering,
};

use anyhow::Context;
use axum::response::IntoResponse;

use crate::{
    STATE,
    db,
    services::{
        libraries::start_library_sync,
        metadata::{
            mapping::{
                MetadataMappingConfig,
                MissingRequiredField,
                apply_mapping,
                check_required_fields,
            },
            read_audio_tags,
        },
    },
};

/// Caps response size on libraries with widespread breakage.
const DRY_RUN_SAMPLE_LIMIT: usize = 10;

#[derive(Debug, Clone, Default, serde::Serialize, schemars::JsonSchema)]
pub(crate) struct DryRunReport {
    pub total_files: usize,
    pub would_reject: usize,
    pub read_failures: usize,
    pub rejected_samples: Vec<RejectedSample>,
    pub read_failure_samples: Vec<ReadFailureSample>,
}

#[derive(Debug, Clone, serde::Serialize, schemars::JsonSchema)]
pub(crate) struct RejectedSample {
    pub path: String,
    pub missing: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize, schemars::JsonSchema)]
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
    let entries: Vec<crate::db::Entry> = {
        let db = STATE.db.read().await;
        let libraries = db::libraries::get(&db)?;
        let mut all = Vec::new();
        for library in libraries {
            let Some(library_db_id) = library.db_id else {
                continue;
            };
            let mut library_entries = db::entries::get(&db, library_db_id)?;
            all.append(&mut library_entries);
        }
        all
    };

    let mut report = DryRunReport::default();

    for entry in entries {
        if entry.kind != crate::db::entries::EntryKind::File {
            continue;
        }
        if !matches!(entry.file_kind.as_deref(), Some("audio")) {
            continue;
        }
        report.total_files += 1;

        let path = entry.full_path.clone();
        let path_for_task = path.clone();
        let task_result = tokio::task::spawn_blocking(move || read_audio_tags(path_for_task)).await;
        match task_result {
            Ok(Ok((tag, tagged_file))) => {
                let file_path = path.to_string_lossy().to_string();
                let raw = apply_mapping(&tag, &tagged_file, &file_path, candidate);
                if let Err(missing) = check_required_fields(&raw) {
                    report.would_reject += 1;
                    if report.rejected_samples.len() < DRY_RUN_SAMPLE_LIMIT {
                        report.rejected_samples.push(RejectedSample {
                            path: file_path,
                            missing: missing
                                .into_iter()
                                .map(|m| missing_name(m).to_string())
                                .collect(),
                        });
                    }
                }
            }
            Ok(Err(err)) => {
                report.read_failures += 1;
                if report.read_failure_samples.len() < DRY_RUN_SAMPLE_LIMIT {
                    report.read_failure_samples.push(ReadFailureSample {
                        path: path.to_string_lossy().to_string(),
                        error: err.to_string(),
                    });
                }
            }
            Err(err) => {
                report.read_failures += 1;
                if report.read_failure_samples.len() < DRY_RUN_SAMPLE_LIMIT {
                    report.read_failure_samples.push(ReadFailureSample {
                        path: path.to_string_lossy().to_string(),
                        error: format!("task failed: {err}"),
                    });
                }
            }
        }
    }

    Ok(report)
}

/// Read by [`reingest_request_gate`] to fence concurrent writes.
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

/// Returns 503 for non-read methods while a reingest runs; GET/HEAD/
/// OPTIONS pass through so the UI can still poll progress.
pub(crate) async fn reingest_request_gate(
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    use axum::http::{
        Method,
        StatusCode,
    };
    if reingest_in_progress()
        && !matches!(
            *request.method(),
            Method::GET | Method::HEAD | Method::OPTIONS
        )
    {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            [("retry-after", "30")],
            "metadata mapping reingest in progress; writes are temporarily unavailable",
        )
            .into_response();
    }
    next.run(request).await
}

#[derive(Debug, Default, Clone)]
pub(crate) struct ReingestSummary {
    pub(crate) libraries_reingested: usize,
    pub(crate) libraries_failed: Vec<(String, String)>,
    pub(crate) new_version: u64,
}

/// Track and Entry node identities are preserved via
/// `apply_metadata`'s `existing_tracks` reuse, so listens, playlists,
/// and playback sessions keep pointing at the same tracks across a
/// mapping change.
///
/// Not transactional across libraries. On any library failure the
/// stored config is rolled back to the pre-commit snapshot so the
/// server's advertised `mapping_version` never claims a state the
/// data never reached. Tracks that were reingested before the
/// failure remain in the graph — a subsequent scan or re-commit
/// converges because ingestion is idempotent over existing tracks.
///
/// During the reingest window the persisted `mapping_version` is
/// already N+1. `reingest_request_gate` fences HTTP writes, but
/// internal readers (scheduled provider sync, future code stamping
/// the version, etc.) see the bumped value transiently. On rollback
/// the stamp returns to N, but any action taken by such a reader
/// in the window remains. Single-transaction reingest across
/// libraries (v1 spec R4) would close this; it's v2 scope.
pub(crate) async fn commit_and_reingest(
    mut config: MetadataMappingConfig,
    _guard: ReingestGuard,
) -> anyhow::Result<ReingestSummary> {
    let (previous, bumped_version) = {
        let mut db = STATE.db.write().await;
        let current = db::metadata::mapping_config::ensure(&mut db)?;
        config.version = current.version.saturating_add(1);
        db::metadata::mapping_config::update(&mut db, &config)?;
        (current, config.version)
    };

    let libraries = {
        let db = STATE.db.read().await;
        db::libraries::get(&db)?
    };

    let mut summary = ReingestSummary {
        new_version: bumped_version,
        ..Default::default()
    };
    let db_async = STATE.db.get();
    for library in libraries {
        let name = library.name.clone();
        match start_library_sync(db_async.clone(), library).await {
            Ok(_) => summary.libraries_reingested += 1,
            Err(err) => {
                tracing::error!(library = %name, error = %err, "reingest kickoff failed");
                summary.libraries_failed.push((name, err.to_string()));
            }
        }
    }

    crate::services::wait_for_running_library_syncs().await;

    if summary.libraries_failed.is_empty() {
        Ok(summary)
    } else {
        let rollback_result = {
            let mut db = STATE.db.write().await;
            db::metadata::mapping_config::rollback_to(&mut db, &previous)
        };
        if let Err(rollback_err) = rollback_result {
            tracing::error!(error = %rollback_err, "failed to roll back metadata mapping config");
        } else {
            summary.new_version = previous.version;
        }
        Err(anyhow::anyhow!(
            "reingest completed with {} library failure(s); rolled back to version {}",
            summary.libraries_failed.len(),
            previous.version,
        ))
        .with_context(|| format!("reingested {} libraries", summary.libraries_reingested))
    }
}
