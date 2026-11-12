// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Purging what a library's missing files leave: their tracks, with everything attached to them,
//! and the missing entries themselves.

use super::{
    SyncRunProgress,
    SyncStageKey,
    SyncTotalState,
    SyncWorkDetails,
};
use lyra_db::{
    DbAsync,
    Library,
    entries::{
        PurgePlan,
        PurgePlanError,
        PurgeScope,
    },
};

/// How many tracks one write removes; agdb's edge removal grows with what a hub gained since, so
/// a purge holds the write lock a batch at a time.
const BATCH: usize = 256;

/// Removes what `plan` names: its tracks a batch at a time, each batch under a write of its own,
/// with everything attached to them and the releases they empty, then its entries. The artists
/// that leaves unreferenced wait for the caller's collection. Returns how many tracks it removed.
pub(crate) async fn apply(
    db: &DbAsync,
    library: &Library,
    plan: PurgePlan,
    progress: &SyncRunProgress,
) -> anyhow::Result<usize> {
    progress
        .add_stage_total(
            SyncStageKey::Purge,
            plan.tracks.len() as u64,
            SyncTotalState::Final,
        )
        .await;
    for batch in plan.tracks.chunks(BATCH) {
        progress.check_cancelled()?;
        {
            let mut db_write = db.write().await;
            let present = batch
                .iter()
                .copied()
                .filter(|track| {
                    lyra_db::tracks::get_by_id(&*db_write, *track).is_ok_and(|t| t.is_some())
                })
                .collect::<Vec<_>>();
            lyra_db::metadata::cascade_remove_entities(&mut db_write, &present)?;
            crate::services::covers::discard_listed_cover_files(&*db_write);
        }
        for _ in batch {
            let work_id = progress
                .start_work(SyncStageKey::Purge, SyncWorkDetails::new("purge_track"))
                .await;
            progress.complete_work(work_id, SyncStageKey::Purge).await;
        }
    }
    let removed = {
        let mut db_write = db.write().await;
        let removed = lyra_db::entries::remove_missing(&mut db_write, library, &plan.entries)?;
        crate::services::covers::discard_listed_cover_files(&*db_write);
        removed
    };
    tracing::info!(
        library_db_id = ?library.db_id,
        purged_tracks = plan.tracks.len(),
        purged_entries = removed,
        "missing files purged"
    );
    Ok(plan.tracks.len())
}

/// Purges `scope` from `library` as a run of its own does: applies its plan, then collects
/// the artists that left unreferenced.
pub(crate) async fn purge(
    db: &DbAsync,
    library: &Library,
    scope: &PurgeScope,
    progress: &SyncRunProgress,
) -> anyhow::Result<usize> {
    let plan = {
        let db_read = db.read().await;
        lyra_db::entries::missing_purge_plan(&db_read, library, scope).map_err(|err| match err {
            PurgePlanError::Db(err) => err,
            other => anyhow::anyhow!(other),
        })?
    };
    let purged = apply(db, library, plan, progress).await?;
    let collected = crate::services::metadata::cleanup::collect_unreferenced_artists(db).await?;
    #[cfg(test)]
    if !collected.held_back {
        lyra_db::orphans::testing::assert_drained(&*db.read().await)?;
    }
    #[cfg(not(test))]
    let _ = collected;
    Ok(purged)
}
