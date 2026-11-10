// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::{
        HashMap,
        HashSet,
    },
    fmt,
    path::{
        Path,
        PathBuf,
    },
};

use agdb::{
    CountComparison,
    DbAny,
    DbElement,
    DbError,
    DbId,
    DbTypeMarker,
    DbValue,
    QueryBuilder,
};

use crate::Library;

#[derive(Clone, Copy, Debug, PartialEq, Eq, DbTypeMarker)]
pub enum EntryKind {
    File,
    Dir,
}

impl EntryKind {
    fn as_db_str(self) -> &'static str {
        match self {
            Self::File => "file",
            Self::Dir => "dir",
        }
    }

    fn from_db_str(value: &str) -> Result<Self, DbError> {
        match value {
            "file" => Ok(Self::File),
            "dir" => Ok(Self::Dir),
            _ => Err(DbError::serialization(
                agdb::DbErrorType::TypeError,
                format!("invalid EntryKind value '{value}'"),
            )),
        }
    }
}

impl fmt::Display for EntryKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_db_str())
    }
}

impl From<EntryKind> for DbValue {
    fn from(value: EntryKind) -> Self {
        Self::from(value.as_db_str())
    }
}

impl From<&EntryKind> for DbValue {
    fn from(value: &EntryKind) -> Self {
        (*value).into()
    }
}

impl TryFrom<DbValue> for EntryKind {
    type Error = DbError;

    fn try_from(value: DbValue) -> Result<Self, Self::Error> {
        Self::from_db_str(value.string()?)
    }
}

#[derive(DbElement, Clone, Debug)]
pub struct Entry {
    pub db_id: Option<DbId>,
    pub id: String,
    pub full_path: PathBuf,
    pub kind: EntryKind,
    pub file_kind: Option<String>,
    pub name: String,
    pub hash: Option<String>,
    pub size: u64,
    pub file_modified_at_ms: u64,
    pub file_created_at_ms: u64,
    /// When a file's metadata and release artifacts last finished; absent until they do after it
    /// changes.
    pub synced_at_ms: Option<u64>,
    /// When a sync first failed to find the entry; absent while it is there.
    pub missing_at_ms: Option<u64>,
}

impl Entry {
    pub fn is_missing(&self) -> bool {
        self.missing_at_ms.is_some()
    }
}

/// Marks `entry_ids` missing since `at_ms`, each once: the time on the entry and its membership
/// of the missing root go together.
pub fn set_missing(
    db: &mut impl super::DbAccess,
    entry_ids: &[DbId],
    at_ms: u64,
) -> anyhow::Result<()> {
    if entry_ids.is_empty() {
        return Ok(());
    }
    db.exec_mut(
        QueryBuilder::insert()
            .values_uniform([("missing_at_ms", at_ms).into()])
            .ids(entry_ids)
            .query(),
    )?;
    crate::roots::link_all(db, crate::roots::Root::MissingEntries, entry_ids)
}

/// Ends the missing state of `entry_ids`, missing ones, in one pass over the missing root.
pub fn clear_missing(db: &mut impl super::DbAccess, entry_ids: &[DbId]) -> anyhow::Result<()> {
    if entry_ids.is_empty() {
        return Ok(());
    }
    db.exec_mut(
        QueryBuilder::remove()
            .values(["missing_at_ms"])
            .ids(entry_ids)
            .query(),
    )?;
    crate::roots::unlink(db, crate::roots::Root::MissingEntries, entry_ids)
}

/// Every missing entry, in any library.
pub fn missing_ids(db: &impl super::DbAccess) -> anyhow::Result<Vec<DbId>> {
    crate::roots::member_ids(db, crate::roots::Root::MissingEntries)
}

pub(crate) const SUPPORTED_AUDIO_EXTENSIONS: &[&str] = lofty::file::EXTENSIONS;
pub(crate) const CUE_EXTENSION: &str = "cue";

pub fn classify_file_kind(path: &Path) -> Option<&'static str> {
    let ext = path.extension()?.to_str()?.to_ascii_lowercase();
    if ext == CUE_EXTENSION {
        return Some("cue");
    }
    if SUPPORTED_AUDIO_EXTENSIONS.contains(&ext.as_str()) {
        return Some("audio");
    }
    None
}

pub fn get_by_id(db: &impl super::DbAccess, entry_db_id: DbId) -> anyhow::Result<Option<Entry>> {
    super::graph::fetch_typed_by_id(db, entry_db_id, "Entry")
}

/// The entries directly under `parent`, a library or a directory entry.
pub fn children(db: &impl super::DbAccess, parent: DbId) -> anyhow::Result<Vec<Entry>> {
    let entries: Vec<Entry> = db
        .exec(
            QueryBuilder::select()
                .elements::<Entry>()
                .search()
                .from(parent)
                .where_()
                .neighbor()
                .query(),
        )?
        .try_into()?;

    Ok(entries)
}

/// The library or directory entry directly holding `entry_db_id`.
pub fn parent_id(db: &impl super::DbAccess, entry_db_id: DbId) -> anyhow::Result<Option<DbId>> {
    let mut parents = super::graph::inbound_neighbor_ids(db, entry_db_id, "Entry")?;
    if parents.is_empty() {
        parents = super::graph::inbound_neighbor_ids(db, entry_db_id, "Library")?;
    }
    Ok(parents.into_iter().next())
}

pub fn get_by_track(db: &DbAny, track_db_id: DbId) -> anyhow::Result<Vec<Entry>> {
    let entries: Vec<Entry> = db
        .exec(
            QueryBuilder::select()
                .elements::<Entry>()
                .search()
                .from(track_db_id)
                .where_()
                .beyond()
                .where_()
                .not()
                .key("db_element_id")
                .value("Entry")
                .end_where()
                .query(),
        )?
        .try_into()?;

    Ok(entries)
}

/// The releases holding the tracks that play the audio file `entry_db_id`.
pub fn release_ids(db: &impl super::DbAccess, entry_db_id: DbId) -> anyhow::Result<HashSet<DbId>> {
    let mut releases = HashSet::new();
    for source in super::graph::inbound_neighbor_ids(db, entry_db_id, "TrackSource")? {
        for track in super::graph::inbound_neighbor_ids(db, source, "Track")? {
            releases.extend(super::graph::inbound_neighbor_ids(db, track, "Release")?);
        }
    }
    Ok(releases)
}

/// The library's own entries: the tree of directories and files under it. The walk goes through
/// entries alone, as other edges lead to releases, whose tracks can play another library's files
/// once a release is shared.
pub fn load_existing(db: &impl super::DbAccess, library_root: DbId) -> anyhow::Result<Vec<Entry>> {
    let qr = db.exec(
        QueryBuilder::select()
            .elements::<Entry>()
            .search()
            .from(library_root)
            .where_()
            .not_beyond()
            .where_()
            .node()
            .and()
            .distance(CountComparison::GreaterThan(0))
            .and()
            .not()
            .key("db_element_id")
            .value("Entry")
            .end_where()
            .query(),
    )?;
    Ok(qr.try_into()?)
}

/// A library's entries as read before a scan, which each group of the scan compares against and
/// finds its parents in, so a group costs its own size and not the library's. The ids stay good
/// for the sync: only a run of the library removes its entries, and one runs at a time, so any
/// other writer must keep to that or check them, since agdb reuses a removed id at once.
#[derive(Debug)]
pub struct StoredEntries {
    library_path: PathBuf,
    library_db_id: DbId,
    by_id: HashMap<DbId, Entry>,
    ids_by_path: HashMap<PathBuf, DbId>,
}

impl StoredEntries {
    pub fn new(library: &Library, entries: Vec<Entry>) -> anyhow::Result<Self> {
        let library_db_id = library
            .db_id
            .ok_or_else(|| anyhow::anyhow!("library missing db_id"))?;
        let mut stored = Self {
            library_path: library.path.clone(),
            library_db_id,
            by_id: HashMap::with_capacity(entries.len()),
            ids_by_path: HashMap::with_capacity(entries.len()),
        };
        stored.record(entries);
        Ok(stored)
    }

    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }

    pub fn entries(&self) -> impl Iterator<Item = &Entry> {
        self.by_id.values()
    }

    pub fn at_path(&self, path: &Path) -> Option<&Entry> {
        self.by_id.get(self.ids_by_path.get(path)?)
    }

    /// Adds entries stored since the read, as directories a sync added for the files under them.
    /// An entry already held at another path leaves it.
    pub fn record(&mut self, entries: Vec<Entry>) {
        for entry in entries {
            let Some(db_id) = entry.db_id else {
                continue;
            };
            let path = entry.full_path.clone();
            if let Some(old) = self.by_id.insert(db_id, entry)
                && old.full_path != path
                && self.ids_by_path.get(&old.full_path) == Some(&db_id)
            {
                self.ids_by_path.remove(&old.full_path);
            }
            self.ids_by_path.insert(path, db_id);
        }
    }

    /// What applying the scanned `entries` changes: the new ones, the changed ones, and those
    /// whose stored id the scan matched at another path.
    pub fn changes(&self, entries: Vec<Entry>) -> EntryChanges {
        let mut changes = EntryChanges::default();
        for entry in entries {
            if entry.full_path == self.library_path {
                continue;
            }

            let entry = Entry {
                synced_at_ms: None,
                ..entry
            };
            if let Some(stored) = entry.db_id.and_then(|db_id| self.by_id.get(&db_id))
                && stored.full_path != entry.full_path
            {
                changes.to_move.push(entry);
                continue;
            }
            let Some(existing) = self.at_path(&entry.full_path) else {
                changes.to_add.push(entry);
                continue;
            };

            if entry.kind != existing.kind
                || entry.file_kind != existing.file_kind
                || entry.name != existing.name
                || entry.size != existing.size
                || entry.file_modified_at_ms != existing.file_modified_at_ms
                || entry.hash != existing.hash
            {
                let mut updated = entry;
                updated.db_id = existing.db_id;
                updated.id = existing.id.clone();
                changes.to_update.push(updated);
            }
        }
        changes
    }

    /// Adds the new entries, updates the changed ones, and moves under their new parent those whose
    /// stored id the scan matched at another path. A changed or moved entry loses its sync. Each
    /// parent is the library, a stored entry, or an entry of `changes`; one that is none of them
    /// fails, which fails the whole sync after the groups it attempted, so nothing is marked
    /// missing until the next sync reads the library anew.
    pub fn apply_entry_changes(
        &self,
        db: &mut DbAny,
        changes: EntryChanges,
    ) -> anyhow::Result<EntrySyncResult> {
        db.transaction_mut(|t| -> anyhow::Result<EntrySyncResult> {
            let EntryChanges {
                to_add,
                to_update,
                to_move,
            } = &changes;
            let changed = to_update
                .iter()
                .chain(to_move)
                .filter_map(|entry| entry.db_id)
                .collect::<Vec<_>>();
            for entries in [to_update, to_move] {
                if !entries.is_empty() {
                    t.exec_mut(QueryBuilder::insert().elements(entries).query())?;
                }
            }
            if !changed.is_empty() {
                t.exec_mut(
                    QueryBuilder::remove()
                        .values(["synced_at_ms"])
                        .ids(&changed)
                        .query(),
                )?;
            }

            let mut path_to_id = HashMap::new();
            let mut added: Vec<Entry> = Vec::new();
            if !to_add.is_empty() {
                let qr_ids = t.exec_mut(QueryBuilder::insert().elements(to_add).query())?;
                added = t
                    .exec(
                        QueryBuilder::select()
                            .elements::<Entry>()
                            .ids(&qr_ids)
                            .query(),
                    )?
                    .try_into()?;
                for entry in &added {
                    let entry_id = entry
                        .db_id
                        .ok_or_else(|| anyhow::anyhow!("newly inserted entry missing db_id"))?;
                    path_to_id.insert(entry.full_path.clone(), entry_id);
                }
            }
            let parent_of = |path: &Path| -> anyhow::Result<DbId> {
                let parent = path
                    .parent()
                    .ok_or_else(|| anyhow::anyhow!("{} has no parent", path.display()))?;
                if parent == self.library_path {
                    return Ok(self.library_db_id);
                }
                path_to_id
                    .get(parent)
                    .copied()
                    .or_else(|| self.at_path(parent).and_then(|entry| entry.db_id))
                    .ok_or_else(|| anyhow::anyhow!("no entry holds {}", path.display()))
            };
            for entry in &added {
                let entry_id = entry
                    .db_id
                    .ok_or_else(|| anyhow::anyhow!("entry missing db_id during edge insert"))?;
                insert_owned_edge(t, parent_of(&entry.full_path)?, entry_id)?;
            }
            for entry in to_move {
                let entry_id = entry
                    .db_id
                    .ok_or_else(|| anyhow::anyhow!("moved entry missing db_id"))?;
                let new_parent = parent_of(&entry.full_path)?;
                if let Some(old_parent) = parent_id(t, entry_id)? {
                    if old_parent == new_parent {
                        continue;
                    }
                    super::graph::remove_edges_between(t, old_parent, entry_id)?;
                }
                insert_owned_edge(t, new_parent, entry_id)?;
            }

            Ok(EntrySyncResult {
                added,
                updated: to_update.len(),
                moved: to_move.len(),
            })
        })
    }
}

/// What [`StoredEntries::apply_entry_changes`] applies.
#[derive(Debug, Default)]
pub struct EntryChanges {
    to_add: Vec<Entry>,
    to_update: Vec<Entry>,
    to_move: Vec<Entry>,
}

impl EntryChanges {
    pub fn is_empty(&self) -> bool {
        self.to_add.is_empty() && self.to_update.is_empty() && self.to_move.is_empty()
    }
}

#[derive(Debug, Default)]
pub struct EntrySyncResult {
    pub added: Vec<Entry>,
    pub updated: usize,
    pub moved: usize,
}

fn insert_owned_edge(
    db: &mut impl super::DbAccess,
    parent: DbId,
    entry: DbId,
) -> anyhow::Result<()> {
    db.exec_mut(
        QueryBuilder::insert()
            .edges()
            .from(parent)
            .to(entry)
            .values_uniform([("owned", 1).into()])
            .query(),
    )?;
    Ok(())
}

/// The missing files a purge removes.
#[derive(Clone, Debug)]
pub enum PurgeScope {
    /// Every missing file of the library, and the tracks only they hold.
    All,
    /// These tracks, which must be the library's and unable to play, and the missing files no
    /// other track needs.
    Tracks(Vec<DbId>),
    /// The files missing since before this time, outside paths a sync couldn't read, and the
    /// tracks only they hold.
    Expired {
        missing_before_ms: u64,
        protected: Vec<PathBuf>,
    },
}

/// The tracks and missing entries a purge removes.
#[derive(Debug, Default)]
pub struct PurgePlan {
    pub tracks: Vec<DbId>,
    pub entries: Vec<DbId>,
}

#[derive(Debug, thiserror::Error)]
pub enum PurgePlanError {
    /// A chosen track isn't one of the library's tracks, or can still play; by its public id.
    #[error("track {0} is not an unavailable track of this library")]
    NotMissing(String),
    #[error(transparent)]
    Db(#[from] anyhow::Error),
}

/// What purging `scope` from `library` removes, as `db` has it now. A track is held by missing
/// files when every one of its sources depends on one of them.
pub fn missing_purge_plan(
    db: &DbAny,
    library: &Library,
    scope: &PurgeScope,
) -> Result<PurgePlan, PurgePlanError> {
    let library_db_id = library
        .db_id
        .ok_or_else(|| anyhow::anyhow!("library missing db_id"))?;
    let (missing, present): (Vec<_>, Vec<_>) = load_existing(db, library_db_id)?
        .into_iter()
        .partition(Entry::is_missing);
    let ids_of = |entries: &mut dyn Iterator<Item = &Entry>| -> HashSet<DbId> {
        entries.filter_map(|entry| entry.db_id).collect()
    };
    match scope {
        PurgeScope::All => Ok(entries_plan(db, ids_of(&mut missing.iter()))?),
        PurgeScope::Expired {
            missing_before_ms,
            protected,
        } => Ok(entries_plan(
            db,
            ids_of(&mut missing.iter().filter(|entry| {
                entry
                    .missing_at_ms
                    .is_some_and(|at| at <= *missing_before_ms)
                    && !is_protected(&entry.full_path, protected)
            })),
        )?),
        PurgeScope::Tracks(tracks) => tracks_plan(
            db,
            library_db_id,
            tracks,
            &ids_of(&mut missing.iter()),
            &ids_of(&mut present.iter()),
        ),
    }
}

fn entries_plan(db: &impl super::DbAccess, entries: HashSet<DbId>) -> anyhow::Result<PurgePlan> {
    use super::graph::{
        inbound_neighbor_ids,
        neighbor_ids,
    };

    let mut candidates = HashSet::new();
    for &entry in &entries {
        for source in crate::track_sources::depending_on(db, entry)? {
            candidates.extend(inbound_neighbor_ids(db, source, "Track")?);
        }
    }
    let mut tracks = Vec::new();
    'tracks: for track in candidates {
        for source in neighbor_ids(db, track, "TrackSource")? {
            let depended = crate::track_sources::depended_entries(db, source)?;
            if !depended.iter().any(|entry| entries.contains(entry)) {
                continue 'tracks;
            }
        }
        tracks.push(track);
    }
    Ok(PurgePlan {
        tracks,
        entries: entries.into_iter().collect(),
    })
}

fn tracks_plan(
    db: &impl super::DbAccess,
    library_db_id: DbId,
    tracks: &[DbId],
    missing: &HashSet<DbId>,
    present: &HashSet<DbId>,
) -> Result<PurgePlan, PurgePlanError> {
    use super::graph::{
        inbound_neighbor_ids,
        neighbor_ids,
    };

    let selected = tracks.iter().copied().collect::<HashSet<_>>();
    let mut candidates = HashSet::new();
    for &track in tracks {
        let mut in_library = false;
        for release in inbound_neighbor_ids(db, track, "Release")? {
            in_library |= inbound_neighbor_ids(db, release, "Library")?.contains(&library_db_id);
        }
        let mut held = Vec::new();
        let mut foreign = false;
        for source in neighbor_ids(db, track, "TrackSource")? {
            for entry in crate::track_sources::depended_entries(db, source)? {
                if missing.contains(&entry) {
                    held.push(entry);
                } else {
                    foreign |= !present.contains(&entry);
                }
            }
        }
        // A track of a shared release whose missing files are another library's is that one's, and
        // one held in another library too would leave that library's file without a track.
        if !in_library
            || held.is_empty()
            || foreign
            || crate::track_sources::track_available(db, track)?
        {
            let public_id = crate::lookup::find_id_by_db_id(db, track)?.unwrap_or_default();
            return Err(PurgePlanError::NotMissing(public_id));
        }
        candidates.extend(held);
    }
    let mut entries = Vec::new();
    'entries: for entry in candidates {
        for source in crate::track_sources::depending_on(db, entry)? {
            if inbound_neighbor_ids(db, source, "Track")?
                .iter()
                .any(|track| !selected.contains(track))
            {
                continue 'entries;
            }
        }
        entries.push(entry);
    }
    Ok(PurgePlan {
        tracks: tracks.to_vec(),
        entries,
    })
}

/// Removes the missing entries among `entry_ids`, with the sources and cue sheets depending on
/// them, then the library's missing directories left without a file beneath. Tracks the sources
/// leave without any are left for the orphan cleanup.
pub fn remove_missing(
    db: &mut DbAny,
    library: &Library,
    entry_ids: &[DbId],
) -> anyhow::Result<usize> {
    let library_db_id = library
        .db_id
        .ok_or_else(|| anyhow::anyhow!("library missing db_id"))?;
    let wanted = entry_ids.iter().copied().collect::<HashSet<_>>();
    let existing = load_existing(db, library_db_id)?;
    let removed = existing
        .iter()
        .filter(|entry| entry.is_missing())
        .filter_map(|entry| entry.db_id)
        .filter(|id| wanted.contains(id))
        .collect::<HashSet<_>>();
    if removed.is_empty() {
        return Ok(0);
    }
    let emptied_dirs = existing
        .iter()
        .filter(|dir| dir.kind == EntryKind::Dir && dir.is_missing())
        .filter(|dir| {
            !existing.iter().any(|entry| {
                entry.kind == EntryKind::File
                    && entry.full_path.starts_with(&dir.full_path)
                    && !entry.db_id.is_some_and(|id| removed.contains(&id))
            })
        })
        .filter_map(|dir| dir.db_id);
    // The missing root lists its newest members first; removing in that order keeps each
    // edge removal short.
    let mut doomed = removed.clone();
    doomed.extend(emptied_dirs);
    let mut order = missing_ids(db)?;
    order.retain(|id| doomed.remove(id));
    order.extend(doomed);
    db.transaction_mut(|t| -> anyhow::Result<()> {
        let mut dependents = Vec::new();
        for &entry_id in &order {
            dependents.extend(crate::track_sources::depending_on(t, entry_id)?);
            for sheet in super::graph::inbound_neighbor_ids(t, entry_id, "CueSheet")? {
                dependents.extend(super::graph::neighbor_ids(t, sheet, "CueTrack")?);
                dependents.push(sheet);
            }
        }
        dependents.sort_unstable();
        dependents.dedup();
        if !dependents.is_empty() {
            t.exec_mut(QueryBuilder::remove().ids(&dependents).query())?;
        }
        t.exec_mut(QueryBuilder::remove().ids(&order).query())?;
        Ok(())
    })?;
    Ok(removed.len())
}

pub fn mark_synced(db: &mut DbAny, entry_ids: &[DbId]) -> anyhow::Result<()> {
    if entry_ids.is_empty() {
        return Ok(());
    }
    db.exec_mut(
        QueryBuilder::insert()
            .values_uniform([("synced_at_ms", super::users::now_ms().unsigned_abs()).into()])
            .ids(entry_ids)
            .query(),
    )?;
    Ok(())
}

/// Whether a scan that couldn't read `protected` paths can tell anything about `path`: not when
/// it lies under one of them, nor when one lies under it.
pub fn is_protected(path: &Path, protected: &[PathBuf]) -> bool {
    protected
        .iter()
        .any(|protected| path.starts_with(protected) || protected.starts_with(path))
}

/// The album policy [`mark_missing`] defers to: given the entries a sync no longer finds where
/// they were and those it does find, picks the found ones that shared an album with the former,
/// which lose their sync so the next one regroups them. The cue sheet rule needs no policy and
/// stays in [`crate::cue::linked_entries`].
pub trait LeftBehind: FnOnce(&[Entry], &[Entry]) -> Vec<DbId> {}

impl<F: FnOnce(&[Entry], &[Entry]) -> Vec<DbId>> LeftBehind for F {}

/// What [`mark_missing`] changed.
#[derive(Debug, Default)]
pub struct MarkResult {
    pub marked: usize,
    pub revived: usize,
    pub removed_dirs: usize,
}

/// Marks the entries missing from `observed_paths` as missing, except where `protected` paths
/// leave the scan without evidence, and revives the missing entries it observed. Missing
/// directories with no file left beneath them are removed. The present entries `left_behind`
/// picks, given the newly missing ones and `moved_from`, the moved entries at their old paths,
/// lose their sync so the next one reads them again, as do those a newly missing, revived or
/// moved one shares a cue sheet with.
pub fn mark_missing(
    db: &mut DbAny,
    library: &Library,
    observed_paths: &HashSet<PathBuf>,
    protected: &[PathBuf],
    moved_from: &[Entry],
    left_behind: impl LeftBehind,
) -> anyhow::Result<MarkResult> {
    let library_db_id = library
        .db_id
        .ok_or_else(|| anyhow::anyhow!("library missing db_id"))?;
    let existing = load_existing(db, library_db_id)?;
    let mut newly_missing = Vec::new();
    let mut revived = Vec::new();
    let mut present = Vec::new();
    for entry in &existing {
        if observed_paths.contains(&entry.full_path) {
            if entry.is_missing() {
                revived.push(entry.clone());
            }
            present.push(entry.clone());
        } else if !entry.is_missing() && !is_protected(&entry.full_path, protected) {
            newly_missing.push(entry.clone());
        }
    }
    let missing_after = |entry: &Entry| {
        !observed_paths.contains(&entry.full_path)
            && (entry.is_missing() || !is_protected(&entry.full_path, protected))
    };
    let removed_dirs = existing
        .iter()
        .filter(|dir| dir.kind == EntryKind::Dir && missing_after(dir))
        .filter(|dir| {
            !existing.iter().any(|entry| {
                entry.kind == EntryKind::File && entry.full_path.starts_with(&dir.full_path)
            })
        })
        .filter_map(|dir| dir.db_id)
        .collect::<Vec<_>>();

    let gone = newly_missing
        .iter()
        .chain(moved_from)
        .cloned()
        .collect::<Vec<_>>();
    let mut unsynced = left_behind(&gone, &present)
        .into_iter()
        .collect::<HashSet<_>>();
    let ids = |entries: &[Entry]| {
        entries
            .iter()
            .map(|entry| {
                entry
                    .db_id
                    .ok_or_else(|| anyhow::anyhow!("entry missing db_id"))
            })
            .collect::<anyhow::Result<Vec<_>>>()
    };
    let missing_ids = ids(&newly_missing)?
        .into_iter()
        .filter(|entry_id| !removed_dirs.contains(entry_id))
        .collect::<Vec<_>>();
    let revived_ids = ids(&revived)?;
    let moved_ids = ids(moved_from)?;
    let now = super::users::now_ms().unsigned_abs();

    db.transaction_mut(|t| -> anyhow::Result<MarkResult> {
        for &entry_id in missing_ids.iter().chain(&revived_ids).chain(&moved_ids) {
            unsynced.extend(crate::cue::linked_entries(t, entry_id)?);
        }
        for entry_id in &missing_ids {
            unsynced.remove(entry_id);
        }
        for entry_id in &removed_dirs {
            unsynced.remove(entry_id);
        }
        if !unsynced.is_empty() {
            let unsynced = unsynced.into_iter().collect::<Vec<_>>();
            t.exec_mut(
                QueryBuilder::remove()
                    .values(["synced_at_ms"])
                    .ids(&unsynced)
                    .query(),
            )?;
        }
        set_missing(t, &missing_ids, now)?;
        clear_missing(t, &revived_ids)?;
        if !removed_dirs.is_empty() {
            t.exec_mut(QueryBuilder::remove().ids(&removed_dirs).query())?;
        }
        Ok(MarkResult {
            marked: missing_ids.len(),
            revived: revived_ids.len(),
            removed_dirs: removed_dirs.len(),
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nanoid;
    use crate::roots::{
        self,
        Root,
    };
    use crate::test_db::TestDb;
    use agdb::DbValue;

    fn new_db() -> anyhow::Result<DbAny> {
        Ok(TestDb::new()?.into_inner())
    }

    fn element_value(element: &agdb::DbElement, key: &str) -> Option<DbValue> {
        element.values.iter().find_map(|kv| {
            let Ok(found_key) = kv.key.string() else {
                return None;
            };
            if found_key == key {
                Some(kv.value.clone())
            } else {
                None
            }
        })
    }

    #[test]
    fn entry_kind_uses_stable_string_db_values() -> anyhow::Result<()> {
        assert_eq!(DbValue::from(EntryKind::File), DbValue::from("file"));
        assert_eq!(DbValue::from(EntryKind::Dir), DbValue::from("dir"));
        assert_eq!(EntryKind::try_from(DbValue::from("file"))?, EntryKind::File);
        assert!(EntryKind::try_from(DbValue::from("folder")).is_err());
        Ok(())
    }

    #[test]
    fn entry_persists_kind_as_string() -> anyhow::Result<()> {
        let mut db = new_db()?;
        let entry = Entry {
            db_id: None,
            id: nanoid(),
            full_path: PathBuf::from("/music/test.flac"),
            kind: EntryKind::File,
            file_kind: Some("audio".to_string()),
            name: "test.flac".to_string(),
            hash: None,
            size: 42,
            file_modified_at_ms: 1,
            file_created_at_ms: 1,
            synced_at_ms: None,
            missing_at_ms: None,
        };
        let entry_id = db
            .exec_mut(QueryBuilder::insert().element(&entry).query())?
            .ids()[0];

        let element = db
            .exec(QueryBuilder::select().ids(entry_id).query())?
            .elements
            .into_iter()
            .next()
            .expect("entry element");

        assert_eq!(element_value(&element, "kind"), Some(DbValue::from("file")));
        assert_eq!(
            get_by_id(&db, entry_id)?
                .expect("entry should round-trip")
                .kind,
            EntryKind::File
        );

        Ok(())
    }

    /// Applies `entries` as a scan reading the library just now would.
    fn sync_scanned(
        db: &mut DbAny,
        library: &Library,
        entries: Vec<Entry>,
    ) -> anyhow::Result<EntrySyncResult> {
        let existing = load_existing(db, library.db_id.expect("library"))?;
        let stored = StoredEntries::new(library, existing)?;
        let changes = stored.changes(entries);
        stored.apply_entry_changes(db, changes)
    }

    fn scanned(full_path: &str, kind: EntryKind) -> Entry {
        let full_path = PathBuf::from(full_path);
        Entry {
            db_id: None,
            id: nanoid(),
            name: full_path
                .file_name()
                .map(|name| name.to_string_lossy().into_owned())
                .unwrap_or_default(),
            full_path,
            kind,
            file_kind: (kind == EntryKind::File).then(|| "audio".to_string()),
            hash: None,
            size: 1,
            file_modified_at_ms: 1,
            file_created_at_ms: 1,
            synced_at_ms: None,
            missing_at_ms: None,
        }
    }

    #[test]
    fn children_and_parents_follow_the_scanned_tree() -> anyhow::Result<()> {
        let mut db = TestDb::initialized()?.into_inner();
        let library_db_id = crate::test_db::insert_library(&mut db, "Music", "/music")?;
        let library = crate::libraries::get_by_id(&db, library_db_id)?.expect("library");
        sync_scanned(
            &mut db,
            &library,
            vec![
                scanned("/music/Artist", EntryKind::Dir),
                scanned("/music/Artist/Album", EntryKind::Dir),
                scanned("/music/Artist/Album/01.flac", EntryKind::File),
                scanned("/music/Artist/Album/02.flac", EntryKind::File),
            ],
        )?;

        let names = |entries: Vec<Entry>| {
            let mut names = entries
                .into_iter()
                .map(|entry| entry.name)
                .collect::<Vec<_>>();
            names.sort();
            names
        };
        let top = children(&db, library_db_id)?;
        assert_eq!(names(top.clone()), ["Artist"]);
        let artist = top[0].db_id.expect("artist db id");
        let album = children(&db, artist)?;
        assert_eq!(names(album.clone()), ["Album"]);
        let album = album[0].db_id.expect("album db id");
        let files = children(&db, album)?;
        assert_eq!(names(files.clone()), ["01.flac", "02.flac"]);

        assert_eq!(parent_id(&db, artist)?, Some(library_db_id));
        assert_eq!(parent_id(&db, album)?, Some(artist));
        assert_eq!(
            parent_id(&db, files[0].db_id.expect("file db id"))?,
            Some(album)
        );
        assert!(children(&db, files[0].db_id.expect("file db id"))?.is_empty());
        Ok(())
    }

    #[test]
    fn a_group_finds_parents_only_in_what_the_scan_read() -> anyhow::Result<()> {
        let mut db = TestDb::initialized()?.into_inner();
        let library_db_id = crate::test_db::insert_library(&mut db, "Music", "/music")?;
        let library = crate::libraries::get_by_id(&db, library_db_id)?.expect("library");
        sync_scanned(
            &mut db,
            &library,
            vec![
                scanned("/music/A", EntryKind::Dir),
                scanned("/music/B", EntryKind::Dir),
            ],
        )?;
        let album = load_existing(&db, library_db_id)?
            .into_iter()
            .find(|entry| entry.name == "A")
            .expect("directory");
        let album_id = album.db_id.expect("directory db_id");

        let stored = StoredEntries::new(&library, vec![album])?;
        let changes = stored.changes(vec![scanned("/music/A/1.flac", EntryKind::File)]);
        stored.apply_entry_changes(&mut db, changes)?;
        assert_eq!(children(&db, album_id)?.len(), 1);

        let changes = stored.changes(vec![scanned("/music/B/1.flac", EntryKind::File)]);
        let err = stored
            .apply_entry_changes(&mut db, changes)
            .expect_err("the database holds this parent, but the scan didn't read it");
        assert!(err.to_string().contains("no entry holds"), "{err:#}");
        Ok(())
    }

    #[test]
    fn a_recorded_entry_leaves_its_old_path() -> anyhow::Result<()> {
        let mut db = TestDb::initialized()?.into_inner();
        let library_db_id = crate::test_db::insert_library(&mut db, "Music", "/music")?;
        let library = crate::libraries::get_by_id(&db, library_db_id)?.expect("library");
        sync_scanned(&mut db, &library, vec![scanned("/music/A", EntryKind::Dir)])?;
        let dir = load_existing(&db, library_db_id)?.remove(0);
        let (old, new) = (Path::new("/music/A"), Path::new("/music/B"));
        let mut stored = StoredEntries::new(&library, vec![dir.clone()])?;

        stored.record(vec![Entry {
            full_path: new.to_path_buf(),
            ..dir.clone()
        }]);

        assert!(stored.at_path(old).is_none());
        assert_eq!(stored.at_path(new).map(|entry| &entry.id), Some(&dir.id));
        Ok(())
    }

    #[test]
    fn a_library_reads_its_own_tree_and_not_what_shared_media_reaches() -> anyhow::Result<()> {
        let mut db = TestDb::initialized()?.into_inner();
        let own_id = crate::test_db::insert_library(&mut db, "Own", "/own")?;
        let other_id = crate::test_db::insert_library(&mut db, "Other", "/other")?;
        let own = crate::libraries::get_by_id(&db, own_id)?.expect("library");
        let other = crate::libraries::get_by_id(&db, other_id)?.expect("library");
        for (library, root) in [(&own, "/own"), (&other, "/other")] {
            sync_scanned(
                &mut db,
                library,
                vec![
                    scanned(&format!("{root}/Album"), EntryKind::Dir),
                    scanned(&format!("{root}/Album/Disc"), EntryKind::Dir),
                    scanned(&format!("{root}/Album/Disc/1.flac"), EntryKind::File),
                    scanned(&format!("{root}/2.flac"), EntryKind::File),
                ],
            )?;
        }
        let file = load_existing(&db, other_id)?
            .into_iter()
            .find(|entry| entry.name == "2.flac")
            .and_then(|entry| entry.db_id)
            .expect("entry");
        let release = crate::fixtures::insert_release(&mut db, "Shared")?;
        let track = crate::fixtures::insert_track(&mut db, "Shared")?;
        crate::graph::ensure_owned_edge(&mut db, own_id, release)?;
        crate::graph::ensure_owned_edge(&mut db, other_id, release)?;
        crate::fixtures::connect(&mut db, release, track)?;
        crate::fixtures::connect(&mut db, track, file)?;

        let entries = load_existing(&db, own_id)?;
        assert_eq!(entries.len(), 4);
        assert!(
            entries
                .iter()
                .all(|entry| entry.full_path.starts_with("/own"))
        );
        Ok(())
    }

    #[test]
    fn a_scan_changes_only_what_differs_from_what_it_read() -> anyhow::Result<()> {
        let mut db = TestDb::initialized()?.into_inner();
        let library_db_id = crate::test_db::insert_library(&mut db, "Music", "/music")?;
        let library = crate::libraries::get_by_id(&db, library_db_id)?.expect("library");
        sync_scanned(
            &mut db,
            &library,
            vec![
                scanned("/music/A", EntryKind::Dir),
                scanned("/music/A/1.flac", EntryKind::File),
            ],
        )?;
        let entries = load_existing(&db, library_db_id)?;
        let stored = StoredEntries::new(&library, entries.clone())?;
        let dir = entries
            .iter()
            .find(|entry| entry.kind == EntryKind::Dir)
            .expect("directory");

        assert!(stored.changes(entries.clone()).is_empty());
        assert!(
            stored
                .changes(vec![scanned("/music", EntryKind::Dir)])
                .is_empty(),
            "the library's own folder is no entry"
        );

        let touched = Entry {
            file_modified_at_ms: 2,
            ..dir.clone()
        };
        let changes = stored.changes(vec![touched]);
        let result = stored.apply_entry_changes(&mut db, changes)?;
        assert!(result.added.is_empty());
        assert_eq!(result.updated, 1);
        let updated = get_by_id(&db, dir.db_id.expect("directory db_id"))?.expect("directory");
        assert_eq!(updated.file_modified_at_ms, 2);
        assert_eq!(updated.id, dir.id);
        Ok(())
    }

    #[test]
    fn changed_entries_wait_for_their_sync_to_finish() -> anyhow::Result<()> {
        let mut db = TestDb::initialized()?.into_inner();
        let library_db_id = crate::test_db::insert_library(&mut db, "Music", "/music")?;
        let library = crate::libraries::get_by_id(&db, library_db_id)?.expect("library");
        let track = |file_modified_at_ms| Entry {
            file_modified_at_ms,
            ..scanned("/music/01.flac", EntryKind::File)
        };
        sync_scanned(&mut db, &library, vec![track(1)])?;
        let entry_id = load_existing(&db, library_db_id)?[0]
            .db_id
            .expect("entry db_id");
        let synced = |db: &DbAny| -> anyhow::Result<bool> {
            Ok(get_by_id(db, entry_id)?
                .expect("entry")
                .synced_at_ms
                .is_some())
        };
        assert!(!synced(&db)?);

        mark_synced(&mut db, &[entry_id])?;
        assert!(synced(&db)?);
        sync_scanned(&mut db, &library, vec![track(1)])?;
        assert!(synced(&db)?, "an unchanged entry lost its sync");

        sync_scanned(&mut db, &library, vec![track(2)])?;
        assert!(!synced(&db)?, "a changed entry kept its sync");
        Ok(())
    }

    #[test]
    fn marking_a_file_missing_unsyncs_its_album_and_cue_linked_entries() -> anyhow::Result<()> {
        let mut db = TestDb::initialized()?.into_inner();
        let library_db_id = crate::test_db::insert_library(&mut db, "Music", "/music")?;
        let library = crate::libraries::get_by_id(&db, library_db_id)?.expect("library");
        let paths = [
            "/music/Album/01.flac",
            "/music/Album/02.flac",
            "/music/Other/01.flac",
            "/music/Cue/album.cue",
            "/music/Audio/album.flac",
        ];
        let dirs = ["/music/Album", "/music/Other", "/music/Cue", "/music/Audio"];
        sync_scanned(
            &mut db,
            &library,
            dirs.iter()
                .map(|path| scanned(path, EntryKind::Dir))
                .chain(paths.iter().map(|path| scanned(path, EntryKind::File)))
                .collect(),
        )?;
        let id_of = |db: &DbAny, path: &str| -> anyhow::Result<DbId> {
            Ok(load_existing(db, library_db_id)?
                .into_iter()
                .find(|entry| entry.full_path == Path::new(path))
                .and_then(|entry| entry.db_id)
                .expect("entry"))
        };
        let ids = paths
            .iter()
            .map(|path| id_of(&db, path))
            .collect::<anyhow::Result<Vec<_>>>()?;
        mark_synced(&mut db, &ids)?;
        db.transaction_mut(|t| -> anyhow::Result<()> {
            let sheet = crate::cue::sheets::upsert(t, ids[3], "hash")?;
            crate::cue::tracks::upsert(t, sheet, ids[3], 1, ids[4], None, 0)?;
            Ok(())
        })?;

        let observed = [paths[1], paths[2]]
            .into_iter()
            .chain(std::iter::once(paths[4]))
            .chain(dirs)
            .map(PathBuf::from)
            .collect::<HashSet<_>>();
        mark_missing(
            &mut db,
            &library,
            &observed,
            &[],
            &[],
            |removed, surviving| {
                surviving
                    .iter()
                    .filter(|entry| {
                        removed
                            .iter()
                            .any(|gone| gone.full_path.parent() == entry.full_path.parent())
                    })
                    .filter_map(|entry| entry.db_id)
                    .collect()
            },
        )?;

        let synced = |db: &DbAny, path: &str| -> anyhow::Result<bool> {
            Ok(get_by_id(db, id_of(db, path)?)?
                .expect("entry")
                .synced_at_ms
                .is_some())
        };
        let missing = |db: &DbAny, path: &str| -> anyhow::Result<bool> {
            Ok(get_by_id(db, id_of(db, path)?)?
                .expect("entry")
                .is_missing())
        };
        assert!(
            !synced(&db, paths[1])?,
            "the album a file left kept its sync"
        );
        assert!(synced(&db, paths[2])?, "another album lost its sync");
        assert!(
            !synced(&db, paths[4])?,
            "the missing cue's audio kept its sync"
        );
        assert!(missing(&db, paths[0])? && missing(&db, paths[3])?);
        assert!(!missing(&db, paths[1])? && !missing(&db, paths[4])?);
        assert_eq!(missing_ids(&db)?.len(), 2);

        mark_synced(&mut db, &ids)?;
        let observed = paths
            .iter()
            .chain(&dirs)
            .map(PathBuf::from)
            .collect::<HashSet<_>>();
        let result = mark_missing(&mut db, &library, &observed, &[], &[], |_, _| Vec::new())?;
        assert_eq!(result.revived, 2);
        assert!(!missing(&db, paths[0])? && !missing(&db, paths[3])?);
        assert!(
            missing_ids(&db)?.is_empty(),
            "revived entries left the missing root"
        );
        assert!(
            !synced(&db, paths[4])?,
            "the revived cue's audio kept its sync"
        );
        Ok(())
    }

    /// The missing root holds exactly the entries with a missing time.
    fn assert_missing_root_matches(db: &DbAny, library_db_id: DbId) -> anyhow::Result<()> {
        let mut timed = load_existing(db, library_db_id)?
            .into_iter()
            .filter(Entry::is_missing)
            .filter_map(|entry| entry.db_id)
            .collect::<Vec<_>>();
        let mut rooted = missing_ids(db)?;
        timed.sort_unstable();
        rooted.sort_unstable();
        assert_eq!(rooted, timed);
        Ok(())
    }

    #[test]
    fn the_missing_root_follows_marking_reviving_and_removal() -> anyhow::Result<()> {
        let mut db = TestDb::initialized()?.into_inner();
        let library_db_id = crate::test_db::insert_library(&mut db, "Music", "/music")?;
        let library = crate::libraries::get_by_id(&db, library_db_id)?.expect("library");
        let paths = [
            "/music/A",
            "/music/A/1.flac",
            "/music/A/2.flac",
            "/music/B/1.flac",
        ];
        sync_scanned(
            &mut db,
            &library,
            vec![
                scanned(paths[0], EntryKind::Dir),
                scanned(paths[1], EntryKind::File),
                scanned(paths[2], EntryKind::File),
                scanned("/music/B", EntryKind::Dir),
                scanned(paths[3], EntryKind::File),
            ],
        )?;
        let seen = |kept: &[&str]| kept.iter().map(PathBuf::from).collect::<HashSet<_>>();
        let all = ["/music/A", paths[1], paths[2], "/music/B", paths[3]];

        mark_missing(&mut db, &library, &seen(&all[3..]), &[], &[], |_, _| {
            Vec::new()
        })?;
        assert_missing_root_matches(&db, library_db_id)?;
        mark_missing(
            &mut db,
            &library,
            &seen(&["/music/A", paths[1], "/music/B"]),
            &[],
            &[],
            |_, _| Vec::new(),
        )?;
        assert_missing_root_matches(&db, library_db_id)?;
        let gone = load_existing(&db, library_db_id)?
            .into_iter()
            .filter(|entry| entry.is_missing())
            .filter_map(|entry| entry.db_id)
            .collect::<Vec<_>>();
        remove_missing(&mut db, &library, &gone)?;
        assert_missing_root_matches(&db, library_db_id)?;
        mark_missing(&mut db, &library, &seen(&[]), &[], &[], |_, _| Vec::new())?;
        assert_missing_root_matches(&db, library_db_id)?;
        mark_missing(&mut db, &library, &seen(&all), &[], &[], |_, _| Vec::new())?;
        assert_missing_root_matches(&db, library_db_id)?;
        assert!(missing_ids(&db)?.is_empty());
        Ok(())
    }

    #[test]
    fn marking_keeps_protected_entries_and_removes_emptied_missing_dirs() -> anyhow::Result<()> {
        let mut db = TestDb::initialized()?.into_inner();
        let library_db_id = crate::test_db::insert_library(&mut db, "Music", "/music")?;
        let library = crate::libraries::get_by_id(&db, library_db_id)?.expect("library");
        sync_scanned(
            &mut db,
            &library,
            vec![
                scanned("/music/Locked", EntryKind::Dir),
                scanned("/music/Locked/01.flac", EntryKind::File),
                scanned("/music/Gone", EntryKind::Dir),
                scanned("/music/Gone/01.flac", EntryKind::File),
                scanned("/music/Empty", EntryKind::Dir),
            ],
        )?;
        let observed = HashSet::new();
        let result = mark_missing(
            &mut db,
            &library,
            &observed,
            &[PathBuf::from("/music/Locked")],
            &[],
            |_, _| Vec::new(),
        )?;
        assert_eq!((result.marked, result.removed_dirs), (2, 1));
        let entries = load_existing(&db, library_db_id)?;
        let state = |path: &str| {
            entries
                .iter()
                .find(|entry| entry.full_path == Path::new(path))
                .map(Entry::is_missing)
        };
        assert_eq!(state("/music/Locked/01.flac"), Some(false));
        assert_eq!(state("/music/Gone"), Some(true));
        assert_eq!(state("/music/Gone/01.flac"), Some(true));
        assert_eq!(state("/music/Empty"), None);
        Ok(())
    }

    #[test]
    fn get_by_track_traverses_through_track_source() -> anyhow::Result<()> {
        let mut db = TestDb::initialized()?.into_inner();

        let track = crate::tracks::Track {
            db_id: None,
            id: nanoid(),
            track_title: "Test Track".to_string(),
            sort_title: None,
            year: None,
            disc: None,
            disc_total: None,
            track: None,
            track_total: None,
            duration_ms: None,
            track_gain_db: None,
            album_gain_db: None,
            locked: None,
            created_at_ms: None,
            file_created_at_ms: None,
        };
        let track_db_id = db
            .exec_mut(QueryBuilder::insert().element(&track).query())?
            .ids()[0];
        roots::link(&mut db, Root::Tracks, track_db_id)?;

        let entry = Entry {
            db_id: None,
            id: nanoid(),
            full_path: PathBuf::from("/music/test.mp3"),
            kind: EntryKind::File,
            file_kind: Some("audio".to_string()),
            name: "test.mp3".to_string(),
            hash: None,
            size: 1,
            file_modified_at_ms: 1,
            file_created_at_ms: 1,
            synced_at_ms: None,
            missing_at_ms: None,
        };
        let entry_db_id = db
            .exec_mut(QueryBuilder::insert().element(&entry).query())?
            .ids()[0];

        let source = crate::track_sources::TrackSource {
            db_id: None,
            id: nanoid(),
            source_kind: "embedded_tags".to_string(),
            source_key: "key1".to_string(),
            identity: "key1".to_string(),
            is_primary: true,
            start_ms: None,
            end_ms: None,
            container: None,
            codec: None,
            sample_rate_hz: None,
            channel_count: None,
            bit_depth: None,
            bitrate_bps: None,
        };
        let source_id = db
            .exec_mut(QueryBuilder::insert().element(&source).query())?
            .ids()[0];

        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(track_db_id)
                .to(source_id)
                .query(),
        )?;
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(source_id)
                .to(entry_db_id)
                .query(),
        )?;

        let entries = get_by_track(&db, track_db_id)?;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].db_id, Some(entry_db_id));

        Ok(())
    }

    #[test]
    fn get_by_track_returns_empty_when_no_entries() -> anyhow::Result<()> {
        let mut db = TestDb::initialized()?.into_inner();

        let track = crate::tracks::Track {
            db_id: None,
            id: nanoid(),
            track_title: "Orphan Track".to_string(),
            sort_title: None,
            year: None,
            disc: None,
            disc_total: None,
            track: None,
            track_total: None,
            duration_ms: None,
            track_gain_db: None,
            album_gain_db: None,
            locked: None,
            created_at_ms: None,
            file_created_at_ms: None,
        };
        let track_db_id = db
            .exec_mut(QueryBuilder::insert().element(&track).query())?
            .ids()[0];

        let entries = get_by_track(&db, track_db_id)?;
        assert!(entries.is_empty());

        Ok(())
    }
}
