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

use super::super::{
    MediaRoots,
    orchestrator::{
        SyncRunControlError,
        SyncRunProgress,
        SyncStageKey,
        SyncTotalState,
        SyncWorkDetails,
    },
};
use crate::services::metadata::{
    CueLinks,
    ParseMetadataOutput,
    TrackMetadata,
    classify_entry_file_kind,
    ingestion::{
        MetadataGroup,
        SourceYields,
        apply_read_batch,
        coalesce_group_tracks,
        prune_sources,
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
    let plan = match GroupPlan::read(db, &files, &parsed).await {
        Ok(plan) => plan,
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
    let linked = linked_sets(&batch_entries, &plan.sheet_links);
    // Each set's write takes its sheets' drops and settles the unyielding files that wait on it;
    // the last write settles those of no set.
    let mut drops = linked.split(&plan.drops, |segment| segment.sheet);
    let mut unyielding = linked.split(&plan.unyielding, |&entry_id| entry_id);
    let mut batches = batches.into_iter().map(Some).collect::<Vec<_>>();
    let mut applied = Vec::new();
    for (set, set_batches) in linked.sets.iter().enumerate() {
        let commit = SetCommit {
            drops: source_keys(std::mem::take(&mut drops[set])),
            settled: set_batches
                .iter()
                .map(|&batch| plan.settled(&batch_entries[batch]))
                .collect(),
            unyielding: std::mem::take(&mut unyielding[set])
                .into_iter()
                .copied()
                .collect(),
            media: &plan.media,
        };
        let set_batches = set_batches
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
        if let Some((releases, _)) = committed {
            applied.extend(
                releases
                    .into_iter()
                    .zip(commit.settled)
                    .filter_map(|(release, settled)| Some((release?, settled))),
            );
        }
    }

    // A failed settlement leaves its files unsynced for the next sync.
    let setless = linked.sets.len();
    let unyielding = std::mem::take(&mut unyielding[setless])
        .into_iter()
        .copied()
        .collect::<Vec<_>>();
    let drops = source_keys(std::mem::take(&mut drops[setless]));
    match settle_unyielding(db, &unyielding, &drops, &plan.media, &progress).await {
        Ok(_) => {}
        Err(err) if err.downcast_ref::<SyncRunControlError>().is_some() => return Err(err),
        Err(err) => report_failed_settlement(&progress, &source_dir, &err).await,
    }
    Ok(applied)
}

fn source_keys(segments: Vec<&StoredSegment>) -> Vec<String> {
    segments
        .into_iter()
        .map(|segment| segment.source_key.clone())
        .collect()
}

/// Reports the failed write settling a group's unyielding files as a failed unit.
async fn report_failed_settlement(
    progress: &SyncRunProgress,
    source_dir: &std::path::Path,
    err: &anyhow::Error,
) {
    progress
        .add_stage_total(SyncStageKey::MetadataApply, 1, SyncTotalState::Estimated)
        .await;
    let details = SyncWorkDetails::new("metadata_settle_group")
        .source_dir(Some(source_dir.to_string_lossy().into_owned()));
    let work_id = progress
        .start_work(SyncStageKey::MetadataApply, details.clone())
        .await;
    progress
        .fail_work(
            work_id,
            SyncStageKey::MetadataApply,
            details,
            format!("{err:#}"),
        )
        .await;
}

/// The group's files as the scan found them, taken before the parse consumes them.
struct GroupFiles {
    by_path: HashMap<PathBuf, DbId>,
    unsynced: HashSet<DbId>,
    audio: HashSet<DbId>,
    sheets: HashSet<DbId>,
}

impl GroupFiles {
    fn of(entries: &[Entry]) -> Self {
        let mut files = Self {
            by_path: HashMap::new(),
            unsynced: HashSet::new(),
            audio: HashSet::new(),
            sheets: HashSet::new(),
        };
        for entry in entries.iter().filter(|entry| entry.kind == EntryKind::File) {
            let Some(entry_id) = entry.db_id else {
                continue;
            };
            files.by_path.insert(entry.full_path.clone(), entry_id);
            if entry.synced_at_ms.is_none() {
                files.unsynced.insert(entry_id);
            }
            match classify_entry_file_kind(entry).as_deref() {
                Some("audio") => files.audio.insert(entry_id),
                Some("cue") => files.sheets.insert(entry_id),
                _ => false,
            };
        }
        files
    }
}

/// A segment as stored: the source a cue sheet's track gave an audio file.
#[derive(Clone, Debug)]
struct StoredSegment {
    sheet: DbId,
    audio: DbId,
    source_key: String,
}

/// The stored segments of `sheets`.
fn stored_cue_segments(
    db: &impl lyra_db::DbAccess,
    sheets: &HashSet<DbId>,
) -> anyhow::Result<Vec<StoredSegment>> {
    let mut segments = Vec::new();
    for &sheet in sheets {
        for source_db_id in lyra_db::track_sources::depending_on(db, sheet)? {
            let Some(source) = lyra_db::track_sources::get_by_id(db, source_db_id)? else {
                continue;
            };
            for audio in lyra_db::graph::neighbor_ids(db, source_db_id, "Entry")? {
                segments.push(StoredSegment {
                    sheet,
                    audio,
                    source_key: source.identity.clone(),
                });
            }
        }
    }
    Ok(segments)
}

/// What a group's read means for its writes, decided before any of them.
struct GroupPlan {
    /// The files changed since a sync last settled them.
    unsynced: HashSet<DbId>,
    /// The files the read missed for now, whole or, for a sheet, in part: no write settles them
    /// or changes their hold.
    unread: HashSet<DbId>,
    /// The source keys the read yielded.
    yields: SourceYields,
    /// The changed files the read yielded nothing of, and the sheets another one outbid.
    unyielding: Vec<DbId>,
    /// The audio files and cue sheets among the group's files, which can hold tracks.
    media: HashSet<DbId>,
    /// The audio applied in this pass whatever its sync, as a sheet's claim on it moved.
    forced: HashSet<DbId>,
    /// The files each changed or outbid sheet plays, played or references, whose batches commit
    /// together.
    sheet_links: HashMap<DbId, HashSet<DbId>>,
    /// The stored segments a changed sheet no longer references, of audio yielding nothing else.
    drops: Vec<StoredSegment>,
}

impl GroupPlan {
    async fn read(
        db: &DbAsync,
        files: &GroupFiles,
        parsed: &ParseMetadataOutput,
    ) -> anyhow::Result<Self> {
        let unread = parsed
            .skipped
            .iter()
            .filter(|record| record.reason.is_transient())
            .filter_map(|record| files.by_path.get(&record.path).copied())
            .chain(parsed.partial.keys().copied())
            .collect::<HashSet<_>>();
        let read = metadata_entry_ids(&parsed.metadata).collect::<HashSet<_>>();
        let yields = SourceYields::of(&parsed.metadata);
        let mut unyielding = files
            .unsynced
            .iter()
            .copied()
            .filter(|entry_id| !read.contains(entry_id) && !unread.contains(entry_id))
            .collect::<Vec<_>>();
        let outbid_synced = parsed
            .outbid
            .keys()
            .filter(|sheet| !files.unsynced.contains(sheet))
            .copied()
            .collect::<Vec<_>>();
        unyielding.extend(outbid_synced);

        // What each sheet references now: what it yielded, what it withheld of audio the read
        // couldn't read for now, or what it would have yielded had another not outbid it.
        let mut references = HashMap::<DbId, Vec<(DbId, &str)>>::new();
        for track in &parsed.metadata {
            if let Some(sheet) = track.cue_sheet_entry_id {
                let reference = (track.entry_db_id, track.source_key.as_str());
                references.entry(sheet).or_default().push(reference);
            }
        }
        for (&sheet, segments) in parsed.outbid.iter().chain(&parsed.partial) {
            let segments = segments.iter().map(|(audio, key)| (*audio, key.as_str()));
            references.entry(sheet).or_default().extend(segments);
        }
        let changed = files
            .sheets
            .intersection(&files.unsynced)
            .copied()
            .collect::<HashSet<_>>();
        let claiming = changed
            .iter()
            .chain(parsed.outbid.keys())
            .copied()
            .collect::<HashSet<_>>();
        let stored = stored_cue_segments(&*db.read().await, &claiming)?;

        let claimed = parsed
            .metadata
            .iter()
            .filter(|track| track.cue_sheet_entry_id.is_some())
            .map(|track| track.entry_db_id)
            .collect::<HashSet<_>>();
        // A changed sheet that references anything was reread: a segment it no longer references
        // for its audio moved, and the audio it left is applied; one it no longer references at
        // all goes, unless another file now yields it or its audio can't be read as audio, which
        // the sheet couldn't have yielded.
        let moved = stored
            .iter()
            .filter(|segment| changed.contains(&segment.sheet))
            .filter_map(|segment| Some((segment, references.get(&segment.sheet)?)))
            .filter(|(segment, references)| {
                !references.contains(&(segment.audio, segment.source_key.as_str()))
            })
            .collect::<Vec<_>>();
        let stale = moved
            .iter()
            .filter(|(segment, references)| {
                !references.iter().any(|(_, key)| *key == segment.source_key)
                    && !yields.yielded(&segment.source_key)
                    && files.audio.contains(&segment.audio)
                    && !parsed.unplayable.contains(&segment.audio)
            })
            .map(|(segment, _)| *segment)
            .collect::<Vec<_>>();
        // An outbid sheet claims none of its audio this read, so a file it stored a segment on that
        // the winner doesn't take reads on its own, as a fresh scan reads it: the pair moved too.
        let freed = stored
            .iter()
            .filter(|segment| parsed.outbid.contains_key(&segment.sheet))
            .filter(|segment| !claimed.contains(&segment.audio));
        let forced = moved
            .iter()
            .map(|(segment, _)| segment)
            .copied()
            .chain(freed)
            .map(|segment| segment.audio)
            .chain(
                stored
                    .iter()
                    .map(|segment| segment.audio)
                    .filter(|audio| claimed.contains(audio)),
            )
            .collect();
        let drops = stale
            .iter()
            .filter(|segment| !read.contains(&segment.audio))
            .map(|&segment| segment.clone())
            .collect();
        let mut sheet_links = HashMap::<DbId, HashSet<DbId>>::new();
        for segment in &stored {
            sheet_links
                .entry(segment.sheet)
                .or_default()
                .insert(segment.audio);
        }
        for sheet in &claiming {
            let referenced = references.get(sheet).into_iter().flatten();
            sheet_links
                .entry(*sheet)
                .or_default()
                .extend(referenced.map(|(audio, _)| *audio));
        }

        Ok(Self {
            unsynced: files.unsynced.clone(),
            unread,
            yields,
            unyielding,
            media: files.audio.union(&files.sheets).copied().collect(),
            forced,
            sheet_links,
            drops,
        })
    }

    /// Whether this pass applies `batch`: whether any of its files changed or had a sheet's
    /// claim on it move.
    fn selects(&self, batch: &[TrackMetadata]) -> bool {
        metadata_entry_ids(batch)
            .any(|entry_id| self.unsynced.contains(&entry_id) || self.forced.contains(&entry_id))
    }

    /// The entries among `entries` a committed write settles: those read in full.
    fn settled(&self, entries: &[DbId]) -> Vec<DbId> {
        entries
            .iter()
            .copied()
            .filter(|entry_id| !self.unread.contains(entry_id))
            .collect()
    }
}

/// A group's batches that commit together, and the set each entry waits on.
#[derive(Debug, PartialEq)]
struct LinkedSets {
    /// Each set's batches, by index.
    sets: Vec<Vec<usize>>,
    set_of: HashMap<DbId, usize>,
}

impl LinkedSets {
    /// Splits `items` by the set the entry `entry_of` names for each waits on, the last list
    /// holding those that wait on none.
    fn split<'a, T>(&self, items: &'a [T], entry_of: impl Fn(&T) -> DbId) -> Vec<Vec<&'a T>> {
        let mut split = vec![Vec::new(); self.sets.len() + 1];
        for item in items {
            let set = self.set_of.get(&entry_of(item)).copied();
            split[set.unwrap_or(self.sets.len())].push(item);
        }
        split
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

/// Links a group's batches, given by their entries, into the sets that commit together: the
/// connected components of their entries joined with each sheet and the files `sheet_links` lists
/// for it, which it plays, played or references, batched or not. Batches sharing an entry link,
/// as a sheet split across releases does, and so do those holding files one sheet's claims move
/// between. Every entry of a component waits on its set; sets are in the order of their first
/// batch.
fn linked_sets(
    batch_entries: &[Vec<DbId>],
    sheet_links: &HashMap<DbId, HashSet<DbId>>,
) -> LinkedSets {
    let mut components = Components::default();
    for entries in batch_entries {
        for pair in entries.windows(2) {
            components.join(pair[0], pair[1]);
        }
    }
    for (&sheet, files) in sheet_links {
        for &file in files {
            components.join(sheet, file);
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
    let entries = components.0.keys().copied().collect::<Vec<_>>();
    let set_of = entries
        .into_iter()
        .filter_map(|entry| Some((entry, *set_of_root.get(&components.root(entry))?)))
        .collect();
    LinkedSets { sets, set_of }
}

/// What a set's write settles once its batches applied.
struct SetCommit<'a> {
    /// The keys of the segments its sheets dropped.
    drops: Vec<String>,
    /// Each batch's entries the write settles.
    settled: Vec<Vec<DbId>>,
    /// The unyielding files that wait on the set.
    unyielding: Vec<DbId>,
    media: &'a HashSet<DbId>,
}

impl SetCommit<'_> {
    /// Drops the segments the set's sheets no longer reference and settles its unyielding files,
    /// then stamps the settled entries of the batches that applied to no release, which have no
    /// artifacts to wait for, and ends the holds of the others. Returns the files it held.
    fn write(
        &self,
        t: &mut DbAnyTransactionMut<'_>,
        releases: &[Option<DbId>],
    ) -> anyhow::Result<Vec<DbId>> {
        #[cfg(test)]
        super::tests::before_set_commit()?;
        let held = settle(t, &self.drops, &self.unyielding, self.media)?;
        let (mut released, mut unreleased) = (Vec::new(), Vec::new());
        for (release, entries) in releases.iter().zip(&self.settled) {
            match release {
                Some(_) => released.extend(entries),
                None => unreleased.extend(entries),
            }
        }
        lyra_db::entries::mark_synced(t, &unreleased, &[])?;
        // The released files' sources just applied, so they play now, though their stamp waits for
        // their release's artifacts.
        lyra_db::entries::reconcile_rejected(t, &released, &[])?;
        Ok(held)
    }
}

/// Drops the segments `drops` keys, then stamps `unyielding` synced with their holds: the audio
/// files and cue sheets among them that still have sources from an earlier read keep their
/// tracks, unable to play, rather than lose them to a read that offers nothing in their place.
/// Returns those held.
fn settle(
    t: &mut DbAnyTransactionMut<'_>,
    drops: &[String],
    unyielding: &[DbId],
    media: &HashSet<DbId>,
) -> anyhow::Result<Vec<DbId>> {
    prune_sources(t, drops)?;
    let mut held = Vec::new();
    for &entry_id in unyielding
        .iter()
        .filter(|entry_id| media.contains(entry_id))
    {
        if !lyra_db::track_sources::depending_on(t, entry_id)?.is_empty() {
            held.push(entry_id);
        }
    }
    lyra_db::entries::mark_synced(t, unyielding, &held)?;
    Ok(held)
}

/// Settles, as [`settle`] does, the unyielding files and drops that wait on no set, in a write of
/// their own. A cancelled run commits none of it.
async fn settle_unyielding(
    db: &DbAsync,
    unyielding: &[DbId],
    drops: &[String],
    media: &HashSet<DbId>,
    progress: &SyncRunProgress,
) -> anyhow::Result<Vec<DbId>> {
    if unyielding.is_empty() && drops.is_empty() {
        return Ok(Vec::new());
    }
    let mut db_write = db.write().await;
    let held = db_write.transaction_mut(|t| -> anyhow::Result<Vec<DbId>> {
        progress.check_cancelled()?;
        #[cfg(test)]
        super::tests::before_settlement_commit()?;
        let held = settle(t, drops, unyielding, media)?;
        #[cfg(test)]
        super::tests::in_settlement();
        progress.check_cancelled()?;
        Ok(held)
    })?;
    crate::services::covers::discard_listed_cover_files(&*db_write);
    Ok(held)
}

fn metadata_entry_ids(metadata: &[TrackMetadata]) -> impl Iterator<Item = DbId> + '_ {
    metadata
        .iter()
        .flat_map(|track| std::iter::once(track.entry_db_id).chain(track.cue_sheet_entry_id))
}

/// Applies a set of linked batches and then `commit` in one write, so a failed or cancelled
/// apply changes none of them and leaves their files for the next sync. Returns the first release
/// each batch applied to, with the files the commit held, or `None` when the write failed, which
/// is a failed unit; only a cancelled run fails this.
async fn apply_linked_batches(
    db: &DbAsync,
    library_db_id: DbId,
    candidate_dirs: &BTreeMap<DbId, PathBuf>,
    batches: Vec<Vec<TrackMetadata>>,
    yields: &SourceYields,
    commit: &SetCommit<'_>,
    progress: &SyncRunProgress,
) -> anyhow::Result<Option<(Vec<Option<DbId>>, Vec<DbId>)>> {
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
    let result = db_write.transaction_mut(|t| -> anyhow::Result<(Vec<Option<DbId>>, Vec<DbId>)> {
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
        let held = commit.write(t, &releases)?;
        progress.check_cancelled()?;
        Ok((releases, held))
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
        Ok(committed) => {
            for (work_id, _) in units {
                progress
                    .complete_work(work_id, SyncStageKey::MetadataApply)
                    .await;
            }
            return Ok(Some(committed));
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
    fn batches_link_through_shared_entries_and_a_sheet_s_files() {
        let sheet_links = HashMap::from([(DbId(10), HashSet::from([DbId(1), DbId(3), DbId(9)]))]);
        let linked = linked_sets(
            &[ids(&[1]), ids(&[2]), ids(&[3, 4]), ids(&[4, 5]), ids(&[6])],
            &sheet_links,
        );
        assert_eq!(linked.sets, vec![vec![0, 2, 3], vec![1], vec![4]]);
        assert_eq!(
            linked.set_of[&DbId(5)],
            0,
            "a shared entry links its batches"
        );
        assert_eq!(
            linked.set_of[&DbId(10)],
            0,
            "a sheet waits on its files' batches"
        );
        assert_eq!(
            linked.set_of[&DbId(9)],
            0,
            "a file in no batch waits on its sheet's set"
        );
        assert_eq!(linked.set_of[&DbId(6)], 2);
    }

    #[test]
    fn a_file_in_no_batch_joins_the_sets_of_the_sheets_naming_it() {
        // Two sheets name x.flac, in no batch; one's other file is in the first batch, the
        // other's in the second.
        let sheet_links = HashMap::from([
            (DbId(10), HashSet::from([DbId(1), DbId(3)])),
            (DbId(11), HashSet::from([DbId(3), DbId(2)])),
        ]);
        let linked = linked_sets(&[ids(&[1, 10]), ids(&[2])], &sheet_links);
        assert_eq!(linked.sets, vec![vec![0, 1]]);
        for entry in [1, 2, 3, 10, 11] {
            assert_eq!(linked.set_of[&DbId(entry)], 0);
        }
    }

    #[test]
    fn a_sheet_whose_files_are_in_no_batch_waits_on_nothing() {
        let sheet_links = HashMap::from([(DbId(10), HashSet::from([DbId(9)]))]);
        let linked = linked_sets(&[ids(&[1]), ids(&[2])], &sheet_links);
        assert_eq!(linked.sets, vec![vec![0], vec![1]]);
        assert!(!linked.set_of.contains_key(&DbId(10)));
        assert!(!linked.set_of.contains_key(&DbId(9)));
    }
}
