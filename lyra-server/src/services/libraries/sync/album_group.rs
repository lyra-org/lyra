// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Reading an album group's files and applying what they yield.

use std::{
    collections::{
        BTreeMap,
        HashMap,
        HashSet,
    },
    path::PathBuf,
    sync::Arc,
    time::Instant,
};

use agdb::{
    DbAnyTransactionMut,
    DbId,
};
use lyra_db::{
    DbAsync,
    Entry,
    entries::EntryKind,
    metadata::mapping_config::MetadataMappingConfig,
};

use super::{
    super::{
        MediaRoots,
        orchestrator::{
            SyncRunControlError,
            SyncRunProgress,
            SyncStageKey,
            SyncTotalState,
            SyncWorkDetails,
        },
    },
    mark_entries_synced,
};
use crate::services::metadata::{
    CueLinks,
    ParseMetadataOutput,
    TrackMetadata,
    ingestion::{
        MetadataGroup,
        SourceYields,
        apply_read_batch,
        coalesce_group_tracks,
    },
    log_skip_summary,
    parse_metadata,
    skip_reason_summary,
};

/// Parses an album group, coalesces its discs and applies its batches, each set of linked ones
/// in one write. Returns the release each batch applied to, with the entries marked synced once
/// its artifacts finish. A failed parse or write is a failed unit, so only a cancelled run fails
/// this.
pub(super) async fn process_album_group(
    db: &DbAsync,
    library_db_id: DbId,
    group: MetadataGroup,
    mapping_config: &MetadataMappingConfig,
    cue_links: &CueLinks,
    roots: &Arc<MediaRoots>,
    progress: SyncRunProgress,
) -> anyhow::Result<Vec<(DbId, Vec<DbId>)>> {
    progress.check_cancelled()?;
    let source_dir = group.source_dir.clone();
    let details = SyncWorkDetails::new("metadata_parse_group")
        .source_dir(Some(source_dir.to_string_lossy().into_owned()));
    let work_id = progress
        .start_work(SyncStageKey::MetadataParse, details.clone())
        .await;
    let candidate_dirs = group.candidate_dirs();
    let files = GroupFiles::of(&group.entries);
    let parse = async {
        #[cfg(test)]
        super::tests::before_parse(&source_dir).await;
        parse_metadata(mapping_config, group.entries, cue_links, roots).await
    };
    let parsed = tokio::select! {
        parsed = parse => parsed,
        err = progress.cancelled() => return Err(err.into()),
    };
    let parsed = match parsed {
        Ok(parsed) => parsed,
        Err(err) => {
            progress
                .fail_work(
                    work_id,
                    SyncStageKey::MetadataParse,
                    details,
                    format!("{err:#}"),
                )
                .await;
            return Ok(Vec::new());
        }
    };
    if !parsed.skipped.is_empty() {
        log_skip_summary(&parsed.skipped);
    }
    let plan = GroupPlan::new(&files, &parsed);
    match parsed
        .skipped
        .iter()
        .filter(|record| record.reason.is_transient())
        .collect::<Vec<_>>()
        .as_slice()
    {
        [] => {
            progress
                .complete_work(work_id, SyncStageKey::MetadataParse)
                .await;
        }
        unreadable @ [first, ..] => {
            let err = anyhow::anyhow!(
                "{} files could not be read; {}: {}",
                unreadable.len(),
                first.path.display(),
                skip_reason_summary(&first.reason)
            );
            progress
                .fail_work(
                    work_id,
                    SyncStageKey::MetadataParse,
                    details,
                    format!("{err:#}"),
                )
                .await;
        }
    }

    let batches = coalesce_group_tracks(&source_dir, &candidate_dirs, parsed.metadata)
        .into_iter()
        .filter(|batch| plan.selects(batch))
        .collect::<Vec<_>>();
    progress
        .add_stage_total(
            SyncStageKey::MetadataApply,
            batches.len() as u64,
            SyncTotalState::Estimated,
        )
        .await;
    let batch_entries = batches
        .iter()
        .map(|batch| metadata_entry_ids(batch).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    let sets = linked_sets(&batch_entries);
    let mut batches = batches.into_iter().map(Some).collect::<Vec<_>>();
    let mut applied = Vec::new();
    for set in &sets {
        let commit = SetCommit {
            settled: set
                .iter()
                .map(|&batch| plan.settled(&batch_entries[batch]))
                .collect(),
        };
        let set_batches = set
            .iter()
            .filter_map(|&batch| batches[batch].take())
            .collect();
        let committed = apply_linked_batches(
            db,
            library_db_id,
            &candidate_dirs,
            set_batches,
            &plan.yields,
            &commit,
            &progress,
        )
        .await?;
        if let Some(releases) = committed {
            applied.extend(
                releases
                    .into_iter()
                    .zip(commit.settled)
                    .filter_map(|(release, settled)| Some((release?, settled))),
            );
        }
    }
    mark_entries_synced(db, library_db_id, &plan.unyielding).await;
    Ok(applied)
}

/// The group's files as the scan found them, taken before the parse consumes them.
struct GroupFiles {
    by_path: HashMap<PathBuf, DbId>,
    unsynced: HashSet<DbId>,
}

impl GroupFiles {
    fn of(entries: &[Entry]) -> Self {
        let files = entries
            .iter()
            .filter(|entry| entry.kind == EntryKind::File)
            .filter_map(|entry| Some((entry, entry.db_id?)));
        let mut by_path = HashMap::new();
        let mut unsynced = HashSet::new();
        for (entry, entry_id) in files {
            by_path.insert(entry.full_path.clone(), entry_id);
            if entry.synced_at_ms.is_none() {
                unsynced.insert(entry_id);
            }
        }
        Self { by_path, unsynced }
    }
}

/// What a group's read means for its writes, decided before any of them.
struct GroupPlan {
    /// The files changed since a sync last settled them.
    unsynced: HashSet<DbId>,
    /// The files whose read failed for now, which no write settles.
    unreadable: HashSet<DbId>,
    /// The source keys the read yielded.
    yields: SourceYields,
    /// The changed files the read yielded nothing of.
    unyielding: Vec<DbId>,
}

impl GroupPlan {
    fn new(files: &GroupFiles, parsed: &ParseMetadataOutput) -> Self {
        let unreadable = parsed
            .skipped
            .iter()
            .filter(|record| record.reason.is_transient())
            .filter_map(|record| files.by_path.get(&record.path).copied())
            .collect::<HashSet<_>>();
        let read = metadata_entry_ids(&parsed.metadata).collect::<HashSet<_>>();
        let unyielding = files
            .unsynced
            .iter()
            .copied()
            .filter(|entry_id| !read.contains(entry_id) && !unreadable.contains(entry_id))
            .collect();
        Self {
            unsynced: files.unsynced.clone(),
            unreadable,
            yields: SourceYields::of(&parsed.metadata),
            unyielding,
        }
    }

    /// Whether this pass applies `batch`: whether any of its files changed.
    fn selects(&self, batch: &[TrackMetadata]) -> bool {
        metadata_entry_ids(batch).any(|entry_id| self.unsynced.contains(&entry_id))
    }

    /// The entries among `entries` a committed write settles: those read in full.
    fn settled(&self, entries: &[DbId]) -> Vec<DbId> {
        entries
            .iter()
            .copied()
            .filter(|entry_id| !self.unreadable.contains(entry_id))
            .collect()
    }
}

/// Entries joined into connected components.
#[derive(Default)]
struct Components(HashMap<DbId, DbId>);

impl Components {
    fn root(&mut self, entry: DbId) -> DbId {
        let mut top = entry;
        loop {
            let parent = *self.0.entry(top).or_insert(top);
            if parent == top {
                break;
            }
            top = parent;
        }
        self.0.insert(entry, top);
        top
    }

    fn join(&mut self, a: DbId, b: DbId) {
        let (a, b) = (self.root(a), self.root(b));
        if a != b {
            self.0.insert(a, b);
        }
    }
}

/// Links a group's batches, given by their entries, that share an entry, as a cue sheet split
/// across releases does: they commit together. Returns each set's batches, by index, in the order
/// of their first batch.
fn linked_sets(batch_entries: &[Vec<DbId>]) -> Vec<Vec<usize>> {
    let mut components = Components::default();
    for entries in batch_entries {
        for pair in entries.windows(2) {
            components.join(pair[0], pair[1]);
        }
    }
    let mut set_of_root = HashMap::new();
    let mut sets = Vec::<Vec<usize>>::new();
    for (batch, entries) in batch_entries.iter().enumerate() {
        let root = components.root(entries[0]);
        let set = *set_of_root.entry(root).or_insert_with(|| {
            sets.push(Vec::new());
            sets.len() - 1
        });
        sets[set].push(batch);
    }
    sets
}

/// What a set's write settles once its batches applied.
struct SetCommit {
    /// Each batch's entries the write settles.
    settled: Vec<Vec<DbId>>,
}

impl SetCommit {
    /// Stamps the entries of the batches that applied to no release, which have no artifacts
    /// to wait for.
    fn write(
        &self,
        t: &mut DbAnyTransactionMut<'_>,
        releases: &[Option<DbId>],
    ) -> anyhow::Result<()> {
        #[cfg(test)]
        super::tests::before_set_commit()?;
        let unreleased = releases
            .iter()
            .zip(&self.settled)
            .filter(|(release, _)| release.is_none())
            .flat_map(|(_, settled)| settled.iter().copied())
            .collect::<Vec<_>>();
        lyra_db::entries::mark_synced(t, &unreleased)
    }
}

fn metadata_entry_ids(metadata: &[TrackMetadata]) -> impl Iterator<Item = DbId> + '_ {
    metadata
        .iter()
        .flat_map(|track| std::iter::once(track.entry_db_id).chain(track.cue_sheet_entry_id))
}

/// Applies a set of linked batches and then `commit` in one write, so a failed or cancelled
/// apply changes none of them and leaves their files for the next sync. Returns the first release
/// each batch applied to, or `None` when the write failed, which is a failed unit; only a
/// cancelled run fails this.
async fn apply_linked_batches(
    db: &DbAsync,
    library_db_id: DbId,
    candidate_dirs: &BTreeMap<DbId, PathBuf>,
    batches: Vec<Vec<TrackMetadata>>,
    yields: &SourceYields,
    commit: &SetCommit,
    progress: &SyncRunProgress,
) -> anyhow::Result<Option<Vec<Option<DbId>>>> {
    progress.check_cancelled()?;
    let mut units = Vec::with_capacity(batches.len());
    for batch in &batches {
        let details = SyncWorkDetails::new("metadata_apply_group").source_dir(
            batch
                .first()
                .and_then(|track| candidate_dirs.get(&track.entry_db_id))
                .map(|path| path.to_string_lossy().into_owned()),
        );
        let work_id = progress
            .start_work(SyncStageKey::MetadataApply, details.clone())
            .await;
        units.push((work_id, details));
    }

    let track_count = batches.iter().map(Vec::len).sum::<usize>();
    let mut failed_batch = None;
    let mut db_write = db.write().await;
    let started = Instant::now();
    let result = db_write.transaction_mut(|t| -> anyhow::Result<Vec<Option<DbId>>> {
        let mut releases = Vec::with_capacity(batches.len());
        for (index, batch) in batches.into_iter().enumerate() {
            progress.check_cancelled()?;
            #[cfg(test)]
            let album = batch[0].album.clone().unwrap_or_default();
            let applied = apply_read_batch(t, library_db_id, batch, yields);
            #[cfg(test)]
            let applied =
                applied.and_then(|applied| super::tests::after_apply(&album).map(|()| applied));
            match applied {
                Ok(applied) => releases.push(applied.releases.first().copied()),
                Err(err) => {
                    failed_batch = Some(index);
                    return Err(err);
                }
            }
        }
        commit.write(t, &releases)?;
        progress.check_cancelled()?;
        Ok(releases)
    });
    if result.is_ok() {
        crate::services::covers::discard_listed_cover_files(&*db_write);
    }
    drop(db_write);
    tracing::info!(
        library_db_id = library_db_id.0,
        run_id = %progress.run_id(),
        stage = "metadata_apply",
        batch_count = units.len(),
        track_count,
        committed = result.is_ok(),
        write_ms = started.elapsed().as_millis() as u64,
        "scan linked batches applied"
    );

    let err = match result {
        Ok(releases) => {
            for (work_id, _) in units {
                progress
                    .complete_work(work_id, SyncStageKey::MetadataApply)
                    .await;
            }
            return Ok(Some(releases));
        }
        Err(err) => err,
    };
    let cancelled = err.downcast_ref::<SyncRunControlError>().is_some();
    // A failure past the batches, in what the write settles, is the first batch's.
    let failed = (!cancelled).then(|| failed_batch.unwrap_or(0));
    for (index, (work_id, details)) in units.into_iter().enumerate() {
        if failed == Some(index) {
            progress
                .fail_work(
                    work_id,
                    SyncStageKey::MetadataApply,
                    details,
                    format!("{err:#}"),
                )
                .await;
        } else {
            let reason = if cancelled {
                "cancelled"
            } else {
                "linked_batch_failed"
            };
            progress
                .skip_work(work_id, SyncStageKey::MetadataApply, reason)
                .await;
        }
    }
    if cancelled { Err(err) } else { Ok(None) }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids(ids: &[i64]) -> Vec<DbId> {
        ids.iter().copied().map(DbId).collect()
    }

    #[test]
    fn batches_sharing_an_entry_commit_together() {
        let sets = linked_sets(&[
            ids(&[1, 9]),
            ids(&[2]),
            ids(&[3, 9]),
            ids(&[3, 4]),
            ids(&[5]),
        ]);
        assert_eq!(sets, vec![vec![0, 2, 3], vec![1], vec![4]]);
    }
}
