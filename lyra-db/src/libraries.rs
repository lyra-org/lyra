// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::HashSet,
    path::{
        Component,
        Path,
        PathBuf,
    },
};

use agdb::{
    CountComparison,
    DbAnyTransactionMut,
    DbElement,
    DbId,
    DbValue,
    QueryBuilder,
};
use anyhow::anyhow;
use unicode_normalization::UnicodeNormalization;
use unicode_properties::{
    GeneralCategory,
    UnicodeGeneralCategory,
};

/// `path` is raw user input; `path_key` and `name_key` are canonical forms
/// stored to keep Unicode/canonicalize work off the write lock.
///
/// Library deletion must be transactional and cascade access edges, owned
/// child entities, scoped HLS jobs, and any audit event.
#[derive(DbElement, Clone, Debug)]
pub struct Library {
    pub db_id: Option<DbId>,
    pub id: String,
    pub name: String,
    pub name_key: String,
    pub path: PathBuf,
    pub path_key: String,
    pub language: Option<String>,
    pub country: Option<String>,
    /// How many days an entry stays missing before a sync purges it; never without.
    pub purge_missing_after_days: Option<u32>,
    /// Whether scans and the files they find may lead outside the library through links; off
    /// without.
    pub follow_links: Option<bool>,
}

impl Library {
    pub fn follows_links(&self) -> bool {
        self.follow_links.unwrap_or(false)
    }
}

/// The bounds of [`Library::purge_missing_after_days`].
pub const PURGE_MISSING_AFTER_DAYS: std::ops::RangeInclusive<u32> = 1..=3650;

/// Every library, in the order they were created.
pub fn get(db: &impl super::DbAccess) -> anyhow::Result<Vec<Library>> {
    let mut libraries: Vec<Library> = db
        .exec(
            QueryBuilder::select()
                .elements::<Library>()
                .search()
                .from("libraries")
                .where_()
                .neighbor()
                .end_where()
                .query(),
        )?
        .try_into()?;
    // A node's edges read newest first.
    libraries.reverse();

    Ok(libraries)
}

/// Strip `Cf` (zero-widths, bidi, SHY, BOM) plus CGJ — CGJ blocks NFC and lets
/// `"Music\u{034F}"` duplicate `"Music"`. Mirrors [`crate::tags`].
fn is_invisible_strippable(c: char) -> bool {
    c == '\u{034F}' || c.general_category() == GeneralCategory::Format
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum LibraryNameError {
    #[error("library name cannot be empty after normalization")]
    Empty,
    #[error("library name contains control characters")]
    ContainsControl,
}

/// Strip-before-trim — invisibles aren't `White_Space`. Variation selectors
/// preserved for emoji; ZWSP/ZWJ dropped as copy-paste noise.
pub fn normalize_library_name_display(raw: &str) -> Result<String, LibraryNameError> {
    let stripped: String = raw
        .chars()
        .filter(|c| !is_invisible_strippable(*c))
        .collect();
    let trimmed = stripped.trim_matches(char::is_whitespace);
    let normalized: String = trimmed.nfc().collect();
    if normalized.is_empty() {
        return Err(LibraryNameError::Empty);
    }
    if normalized.chars().any(char::is_control) {
        return Err(LibraryNameError::ContainsControl);
    }
    Ok(normalized)
}

/// Second NFC pass: `to_lowercase` can decompose canonical forms.
fn lowercase_nfc(s: &str) -> String {
    s.to_lowercase().nfc().collect()
}

pub(crate) fn normalize_library_name(raw: &str) -> Result<(String, String), LibraryNameError> {
    let display = normalize_library_name_display(raw)?;
    let key = lowercase_nfc(&display);
    Ok((display, key))
}

/// Collapse `.`/`..`/`//`. Used as the comparison key when `canonicalize`
/// is unavailable (missing path, dead mount, etc.).
pub fn lexical_normalize_path(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => match out.components().next_back() {
                Some(Component::Normal(_)) => {
                    out.pop();
                }
                // `..` at root can't escape; preserve on relative paths.
                Some(Component::RootDir | Component::Prefix(_)) => {}
                _ => out.push(".."),
            },
            other => out.push(other),
        }
    }
    if out.as_os_str().is_empty() {
        out.push(".");
    }
    out
}

/// Sync syscall — wrap in `spawn_blocking` from async contexts. Lexical
/// fallback doesn't unify case on case-insensitive filesystems.
pub(crate) fn normalize_library_path(path: &Path) -> PathBuf {
    match std::fs::canonicalize(path) {
        Ok(canonical) => canonical,
        Err(_) => lexical_normalize_path(path),
    }
}

pub fn path_key_for(path: &Path) -> String {
    normalize_library_path(path).to_string_lossy().into_owned()
}

pub(crate) fn find_by_name_key(
    db: &impl super::DbAccess,
    name_key: &str,
) -> anyhow::Result<Option<Library>> {
    find_indexed_library(db, "name_key", name_key)
}

/// Compute `key` via [`path_key_for`] off the lock — `canonicalize` is a syscall.
pub fn find_by_path_key(db: &impl super::DbAccess, key: &str) -> anyhow::Result<Option<Library>> {
    find_indexed_library(db, "path_key", key)
}

// `index_name` must be in [`crate::bootstrap::CORE_INDEXES`].
fn find_indexed_library(
    db: &impl super::DbAccess,
    index_name: &str,
    value: &str,
) -> anyhow::Result<Option<Library>> {
    super::graph::fetch_typed_by_index(db, index_name, value, "Library")
}

pub fn get_by_id(
    db: &impl super::DbAccess,
    library_db_id: DbId,
) -> anyhow::Result<Option<Library>> {
    super::graph::fetch_typed_by_id(db, library_db_id, "Library")
}
/// All None branches do equal agdb work; see `mod benches` for parity. Admins
/// bypass via the service access policy and never reach this.
pub fn find_node_id_accessible_to_user(
    db: &impl super::DbAccess,
    user_db_id: DbId,
    public_id: &str,
) -> anyhow::Result<Option<DbId>> {
    // Padding queries are unconditional — gating on `found` would leak.
    let make_query = || {
        QueryBuilder::select()
            .values(vec![DbValue::from("db_element_id"), DbValue::from("id")])
            .search()
            .from(user_db_id)
            .where_()
            .distance(CountComparison::Equal(2))
            .and()
            .node()
            .end_where()
            .query()
    };
    let result = db.exec(make_query())?;
    let key_id = DbValue::from("id");
    let key_type = DbValue::from("db_element_id");
    let val_library = DbValue::from("Library");
    let val_target = DbValue::String(public_id.to_string());
    let found = result.elements.into_iter().find_map(|element| {
        if element.id.0 <= 0 {
            return None;
        }
        let (mut is_library, mut id_matches) = (false, false);
        for kv in &element.values {
            if kv.key == key_type {
                is_library |= kv.value == val_library;
            } else if kv.key == key_id {
                id_matches |= kv.value == val_target;
            }
        }
        (is_library && id_matches).then_some(element.id)
    });
    for _ in 0..2 {
        let _pad = db.exec(make_query())?;
        std::hint::black_box(&_pad);
    }
    Ok(found)
}

/// The search climbing from a node to the libraries holding it. It stops at users too: nothing in
/// a library leads to a user, and a user's listens, sessions, tags and playlists all point at
/// them, so climbing past a user who favorited or rated the node would walk their whole history.
macro_rules! holders_search {
    ($search:expr, $node_id:expr) => {
        $search
            .to($node_id)
            .where_()
            .not_beyond()
            .where_()
            .key("db_element_id")
            .value("Library")
            .or()
            .key("db_element_id")
            .value("User")
            .end_where()
            .query()
    };
}

/// The libraries holding the node, by database id, so the first is the one the server picks for
/// an entity in several.
pub fn get_for_entity(db: &impl super::DbAccess, node_id: DbId) -> anyhow::Result<Vec<Library>> {
    let mut libraries: Vec<Library> = db
        .exec(holders_search!(
            QueryBuilder::select().elements::<Library>().search(),
            node_id
        ))?
        .try_into()?;
    libraries.sort_by_key(|library| library.db_id.map(|id| id.0));
    Ok(libraries)
}

pub fn get_by_release(
    db: &impl super::DbAccess,
    release_db_id: DbId,
) -> anyhow::Result<Vec<Library>> {
    let libraries: Vec<Library> = db
        .exec(
            QueryBuilder::select()
                .elements::<Library>()
                .search()
                .to(release_db_id)
                .where_()
                .neighbor()
                .end_where()
                .query(),
        )?
        .try_into()?;

    Ok(libraries)
}

#[derive(Debug, thiserror::Error)]
pub enum LibraryCreateError {
    #[error("a library named '{0}' already exists")]
    NameInUse(String),
    #[error("a library already exists for directory: {}", .0.display())]
    PathInUse(PathBuf),
    #[error("invalid library name: {0}")]
    InvalidName(#[from] LibraryNameError),
    #[error(transparent)]
    Db(#[from] anyhow::Error),
}

impl From<agdb::DbError> for LibraryCreateError {
    fn from(e: agdb::DbError) -> Self {
        Self::Db(anyhow::Error::new(e))
    }
}

pub struct LibraryInsert {
    pub id: String,
    pub name: String,
    pub path: PathBuf,
    /// Compute via [`path_key_for`] off the lock — `canonicalize` is a syscall.
    pub path_key: String,
    pub language: Option<String>,
    pub country: Option<String>,
    pub follow_links: bool,
}

/// Auto-grants the creator atomically with the node insert. No syscalls inside;
/// `request.path_key` must already be canonical.
pub fn create_with_creator(
    db: &mut impl super::DbAccess,
    request: LibraryInsert,
    creator_user_db_id: DbId,
) -> Result<Library, LibraryCreateError> {
    let (created, library_db_id) = create_inner(db, request)?;
    grant_access(db, creator_user_db_id, library_db_id, AccessKind::ReadWrite)?;
    Ok(created)
}

/// No creator edge — admin-bypass-only until granted. Request handlers
/// must use [`create_with_creator`].
pub fn create_system(
    db: &mut impl super::DbAccess,
    request: LibraryInsert,
) -> Result<Library, LibraryCreateError> {
    let (created, _library_db_id) = create_inner(db, request)?;
    Ok(created)
}

/// Uniqueness + node insert + `from("libraries")` edge. Access edge is the
/// caller's job.
fn create_inner(
    db: &mut impl super::DbAccess,
    request: LibraryInsert,
) -> Result<(Library, DbId), LibraryCreateError> {
    let (name, name_key) = normalize_library_name(&request.name)?;

    if find_by_name_key(db, &name_key)?.is_some() {
        return Err(LibraryCreateError::NameInUse(name));
    }
    if find_by_path_key(db, &request.path_key)?.is_some() {
        return Err(LibraryCreateError::PathInUse(request.path));
    }

    let mut created = Library {
        db_id: None,
        id: request.id,
        name,
        name_key,
        path: request.path,
        path_key: request.path_key,
        language: request.language,
        country: request.country,
        purge_missing_after_days: None,
        follow_links: request.follow_links.then_some(true),
    };
    let qr = db.exec_mut(QueryBuilder::insert().element(&created).query())?;
    let library_db_id = qr
        .ids()
        .first()
        .copied()
        .ok_or_else(|| anyhow!("library insert missing id"))?;
    created.db_id = Some(library_db_id);
    db.exec_mut(
        QueryBuilder::insert()
            .edges()
            .from("libraries")
            .to(library_db_id)
            .query(),
    )?;

    Ok((created, library_db_id))
}

#[derive(Debug, thiserror::Error)]
pub enum LibraryUpdateError {
    #[error("a library named '{0}' already exists")]
    NameInUse(String),
    #[error("invalid library name: {0}")]
    InvalidName(#[from] LibraryNameError),
    #[error(transparent)]
    Db(#[from] anyhow::Error),
}

impl From<agdb::DbError> for LibraryUpdateError {
    fn from(e: agdb::DbError) -> Self {
        Self::Db(anyhow::Error::new(e))
    }
}

/// Re-derives `name_key`; excludes self from the uniqueness check. No
/// directory edits.
pub fn update(
    db: &mut DbAnyTransactionMut<'_>,
    library: &Library,
) -> Result<Library, LibraryUpdateError> {
    let library_db_id = library
        .db_id
        .ok_or_else(|| anyhow!("library update missing db_id"))?;

    let (display_name, new_name_key) = normalize_library_name(&library.name)?;
    if let Some(colliding) = find_by_name_key(db, &new_name_key)?
        && colliding.db_id != Some(library_db_id)
    {
        return Err(LibraryUpdateError::NameInUse(display_name));
    }

    let stored = Library {
        name: display_name,
        name_key: new_name_key,
        ..library.clone()
    };

    super::replace_element_in_transaction(
        db,
        library_db_id,
        [
            ("language", stored.language.is_none()),
            ("country", stored.country.is_none()),
            (
                "purge_missing_after_days",
                stored.purge_missing_after_days.is_none(),
            ),
            ("follow_links", stored.follow_links.is_none()),
        ],
        &stored,
    )?;

    Ok(stored)
}

fn library_entry_ids(db: &impl super::DbAccess, library_id: DbId) -> anyhow::Result<HashSet<DbId>> {
    Ok(super::entries::load_existing(db, library_id)?
        .into_iter()
        .filter_map(|entry| entry.db_id)
        .collect())
}

/// What deleting a library removed.
#[derive(Debug, Default)]
pub struct RemovedLibrary {
    /// Public ids of the removed tracks.
    pub track_ids: HashSet<String>,
}

/// Remove the library's exclusive media graph in the caller's transaction.
/// Media reachable from another library remains available there, and the artists only this
/// library's media referenced become candidates for collection.
pub fn delete(
    db: &mut DbAnyTransactionMut<'_>,
    library: &Library,
) -> anyhow::Result<RemovedLibrary> {
    let library_id = library
        .db_id
        .ok_or_else(|| anyhow!("library missing db_id"))?;
    let descendants = db.exec(
        QueryBuilder::select()
            .search()
            .from(library_id)
            .where_()
            .node()
            .query(),
    )?;
    let mut removed_entries = library_entry_ids(db, library_id)?;
    let mut retained = HashSet::new();
    for other in get(db)? {
        if let Some(id) = other.db_id.filter(|id| *id != library_id) {
            let other_entries = library_entry_ids(db, id)?;
            removed_entries.retain(|entry| !other_entries.contains(entry));
            retained.extend(
                db.exec(QueryBuilder::search().from(id).where_().node().query())?
                    .ids(),
            );
        }
    }
    let mut entities = Vec::new();
    let mut children: Vec<DbId> = Vec::new();
    let mut track_ids = HashSet::new();
    for element in descendants.elements {
        if retained.contains(&element.id) {
            continue;
        }
        let kind = element
            .values
            .iter()
            .find(|kv| kv.key == DbValue::from("db_element_id"))
            .and_then(|kv| kv.value.string().ok())
            .map(String::as_str);
        match kind {
            Some("Track" | "Release") => {
                entities.push(element.id);
                if kind == Some("Track")
                    && let Some(id) = element
                        .values
                        .iter()
                        .find(|kv| kv.key == DbValue::from("id"))
                        .and_then(|kv| kv.value.string().ok())
                {
                    track_ids.insert(id.clone());
                }
            }
            Some("TrackSource" | "CueSheet" | "CueTrack") => children.push(element.id),
            _ => {}
        }
    }
    let mut left = super::orphans::Candidates::default();
    for &entry_id in &removed_entries {
        for source_id in super::graph::inbound_neighbor_ids(db, entry_id, "TrackSource")? {
            let tracks = super::graph::inbound_neighbor_ids(db, source_id, "Track")?;
            for track in super::tracks::get_existing_by_ids(db, &tracks)?.into_values() {
                track_ids.insert(track.id);
            }
            children.push(source_id);
        }
    }
    // Cue sheets point to their file entry, so the forward media walk cannot find them.
    let mut cue_children = HashSet::new();
    for &id in &removed_entries {
        let sheets: Vec<super::CueSheet> = db
            .exec(
                QueryBuilder::select()
                    .elements::<super::CueSheet>()
                    .search()
                    .to(id)
                    .where_()
                    .neighbor()
                    .end_where()
                    .query(),
            )?
            .try_into()?;
        for sheet in sheets {
            let Some(sheet_id) = sheet.db_id else {
                continue;
            };
            cue_children.insert(sheet_id);
            let tracks: Vec<super::CueTrack> = db
                .exec(
                    QueryBuilder::select()
                        .elements::<super::CueTrack>()
                        .search()
                        .from(sheet_id)
                        .where_()
                        .neighbor()
                        .end_where()
                        .query(),
                )?
                .try_into()?;
            cue_children.extend(
                tracks
                    .into_iter()
                    .filter_map(|track| track.db_id)
                    .filter(|id| !retained.contains(id)),
            );
        }
    }
    for &entry_id in &removed_entries {
        let tracks: Vec<super::CueTrack> = db
            .exec(
                QueryBuilder::select()
                    .elements::<super::CueTrack>()
                    .search()
                    .to(entry_id)
                    .where_()
                    .neighbor()
                    .end_where()
                    .query(),
            )?
            .try_into()?;
        cue_children.extend(tracks.into_iter().filter_map(|track| track.db_id));
    }
    children.extend(cue_children);
    // The removals below are raw, so what each leaves is read before it goes: a cue track's
    // sheet may live on through a cue file another library holds.
    for &id in &children {
        left.tracks
            .extend(super::graph::inbound_neighbor_ids(db, id, "Track")?);
        left.cue_tracks
            .extend(super::graph::neighbor_ids(db, id, "CueTrack")?);
        left.cue_sheets
            .extend(super::graph::inbound_neighbor_ids(db, id, "CueSheet")?);
    }
    children.extend(removed_entries.iter().copied());
    children.push(library_id);
    for run in super::sync_runs::list_for_library(db, &library.id)? {
        if let Some(id) = run.db_id {
            children.extend(super::sync_runs::failure_ids(db, id)?);
            children.push(id);
        }
    }
    // Before anything is collected, so no id here names an element reissued since it was read.
    db.exec_mut(QueryBuilder::remove().ids(children).query())?;
    super::metadata::cascade_remove_entities_in_txn(db, &entities)?;
    // A track another library reaches may have played this library's files alone.
    super::orphans::collect(db, left)?;
    Ok(RemovedLibrary { track_ids })
}

const ACCESS_KIND_KEY: &str = "library_access_kind";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AccessKind {
    ReadWrite,
}

impl AccessKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::ReadWrite => "read_write",
        }
    }
}

/// Idempotent — existing edge has its `access_kind` overwritten.
pub fn grant_access(
    db: &mut impl super::DbAccess,
    user_db_id: DbId,
    library_db_id: DbId,
    kind: AccessKind,
) -> anyhow::Result<()> {
    if let Some(edge_id) = find_access_edge(db, user_db_id, library_db_id)? {
        db.exec_mut(
            QueryBuilder::insert()
                .values_uniform([(ACCESS_KIND_KEY, kind.as_str()).into()])
                .ids(edge_id)
                .query(),
        )?;
        return Ok(());
    }

    let edge_id = db
        .exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(user_db_id)
                .to(library_db_id)
                .query(),
        )?
        .ids()
        .first()
        .copied()
        .ok_or_else(|| anyhow!("library access edge insert missing id"))?;
    db.exec_mut(
        QueryBuilder::insert()
            .values_uniform([(ACCESS_KIND_KEY, kind.as_str()).into()])
            .ids(edge_id)
            .query(),
    )?;
    Ok(())
}

/// Idempotent — `true` iff an edge was removed.
pub fn revoke_access(
    db: &mut impl super::DbAccess,
    user_db_id: DbId,
    library_db_id: DbId,
) -> anyhow::Result<bool> {
    let Some(edge_id) = find_access_edge(db, user_db_id, library_db_id)? else {
        return Ok(false);
    };
    db.exec_mut(QueryBuilder::remove().ids(edge_id).query())?;
    Ok(true)
}

/// `&mut` receiver enforces transactional context — read snapshots can't call.
pub fn user_has_access_in_txn(
    txn: &mut DbAnyTransactionMut<'_>,
    user_db_id: DbId,
    library_db_id: DbId,
) -> anyhow::Result<bool> {
    Ok(find_access_edge(txn, user_db_id, library_db_id)?.is_some())
}

/// Sorted by `to_ascii_lowercase(username)` to match `db::users::get`.
pub fn users_with_access(
    db: &impl super::DbAccess,
    library_db_id: DbId,
) -> anyhow::Result<Vec<super::users::User>> {
    let edges = read_inbound_access_edges(db, library_db_id)?;
    let mut users: Vec<super::users::User> = Vec::with_capacity(edges.len());
    for edge in edges {
        let user_db_id = edge.from;
        if user_db_id.0 == 0 {
            continue;
        }
        // Skip orphan edges (user-node missing); real errors still propagate.
        if let Some(user) = super::users::get_by_id(db, user_db_id)? {
            users.push(user);
        }
    }
    users.sort_by_key(|user| user.username.to_ascii_lowercase());
    Ok(users)
}

/// Explicit-access only — no admin bypass here.
pub fn accessible_library_ids(
    db: &impl super::DbAccess,
    user_db_id: DbId,
) -> anyhow::Result<HashSet<String>> {
    let edges = read_outbound_access_edges(db, user_db_id)?;
    let mut ids = HashSet::with_capacity(edges.len());
    for edge in edges {
        let library_db_id = edge.to;
        if library_db_id.0 == 0 {
            continue;
        }
        if let Some(library) = get_by_id(db, library_db_id)? {
            ids.insert(library.id);
        }
    }
    Ok(ids)
}

pub fn accessible_track_ids(
    db: &impl super::DbAccess,
    user_db_id: DbId,
    track_db_ids: &[DbId],
) -> anyhow::Result<HashSet<DbId>> {
    let accessible_libraries = read_outbound_access_edges(db, user_db_id)?
        .into_iter()
        .filter_map(|edge| (edge.to.0 > 0).then_some(edge.to))
        .collect::<Vec<_>>();
    accessible_track_ids_for_library_db_ids(db, &accessible_libraries, track_db_ids)
}

pub fn db_ids_for_public_ids(
    db: &impl super::DbAccess,
    public_ids: &HashSet<String>,
) -> anyhow::Result<Vec<DbId>> {
    if public_ids.is_empty() {
        return Ok(Vec::new());
    }
    Ok(get(db)?
        .into_iter()
        .filter(|library| public_ids.contains(&library.id))
        .filter_map(|library| library.db_id)
        .collect())
}

pub fn accessible_track_ids_for_library_db_ids(
    db: &impl super::DbAccess,
    accessible_libraries: &[DbId],
    track_db_ids: &[DbId],
) -> anyhow::Result<HashSet<DbId>> {
    let requested = super::dedup_positive_ids(track_db_ids)
        .into_iter()
        .collect::<HashSet<_>>();
    if requested.is_empty() {
        return Ok(HashSet::new());
    }
    if accessible_libraries.is_empty() {
        return Ok(HashSet::new());
    }
    let requested_ids = requested.iter().copied().collect::<Vec<_>>();
    let result = db.exec(
        QueryBuilder::search()
            .from("libraries")
            .where_()
            .ids(requested_ids.clone())
            .and()
            .not_beyond()
            .where_()
            .key("db_element_id")
            .value("Library")
            .and()
            .not()
            .ids(accessible_libraries.to_vec())
            .end_where()
            .and()
            .not_beyond()
            .ids(requested_ids)
            .query(),
    )?;
    Ok(result
        .ids()
        .into_iter()
        .filter(|track_db_id| requested.contains(track_db_id))
        .collect())
}

/// Cascade hook for `db::users::delete_user`.
pub(crate) fn remove_access_edges_for_user(
    db: &mut impl super::DbAccess,
    user_db_id: DbId,
) -> anyhow::Result<()> {
    let edge_ids: Vec<DbId> = read_outbound_access_edges(db, user_db_id)?
        .into_iter()
        .map(|edge| edge.id)
        .collect();
    if !edge_ids.is_empty() {
        db.exec_mut(QueryBuilder::remove().ids(edge_ids).query())?;
    }
    Ok(())
}

fn find_access_edge(
    db: &impl super::DbAccess,
    user_db_id: DbId,
    library_db_id: DbId,
) -> anyhow::Result<Option<DbId>> {
    for element in read_outbound_access_edges(db, user_db_id)? {
        if element.to == library_db_id {
            return Ok(Some(element.id));
        }
    }
    Ok(None)
}

fn read_outbound_access_edges(
    db: &impl super::DbAccess,
    user_db_id: DbId,
) -> anyhow::Result<Vec<DbElement>> {
    let result = db.exec(
        QueryBuilder::select()
            .search()
            .from(user_db_id)
            .where_()
            .edge()
            .and()
            .distance(CountComparison::Equal(1))
            .end_where()
            .query(),
    )?;
    Ok(result
        .elements
        .into_iter()
        .filter(|element| element.from == user_db_id && element_is_access(element))
        .collect())
}

fn read_inbound_access_edges(
    db: &impl super::DbAccess,
    library_db_id: DbId,
) -> anyhow::Result<Vec<DbElement>> {
    let result = db.exec(
        QueryBuilder::select()
            .search()
            .to(library_db_id)
            .where_()
            .edge()
            .and()
            .distance(CountComparison::Equal(1))
            .end_where()
            .query(),
    )?;
    Ok(result
        .elements
        .into_iter()
        .filter(|element| element.to == library_db_id && element_is_access(element))
        .collect())
}

fn element_is_access(element: &DbElement) -> bool {
    element
        .values
        .iter()
        .any(|kv| matches!(&kv.key, DbValue::String(k) if k == ACCESS_KIND_KEY))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nanoid;
    use crate::test_db::new_test_db;
    use agdb::DbAny;

    #[test]
    fn libraries_come_in_the_order_they_were_created() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        for name in ["First", "Second", "Third"] {
            crate::test_db::insert_library(&mut db, name, &format!("/tmp/lyra-created-{name}"))?;
        }

        let names = get(&db)?
            .into_iter()
            .map(|library| library.name)
            .collect::<Vec<_>>();
        assert_eq!(names, ["First", "Second", "Third"]);
        Ok(())
    }

    #[test]
    fn libraries_for_an_entity_come_in_db_id_order() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let first = crate::test_db::insert_library(&mut db, "First", "/tmp/lyra-order-first")?;
        let second = crate::test_db::insert_library(&mut db, "Second", "/tmp/lyra-order-second")?;
        let release = crate::fixtures::insert_release(&mut db, "Release")?;
        crate::fixtures::connect(&mut db, second, release)?;
        crate::fixtures::connect(&mut db, first, release)?;

        let libraries = get_for_entity(&db, release)?
            .into_iter()
            .map(|library| library.db_id)
            .collect::<Vec<_>>();
        let mut expected = vec![Some(first), Some(second)];
        expected.sort_by_key(|id| id.map(|id| id.0));
        assert_eq!(libraries, expected);
        Ok(())
    }

    #[test]
    fn finding_a_nodes_libraries_does_not_walk_the_history_of_users_who_favorited_it()
    -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let library = crate::test_db::insert_library(&mut db, "Music", "/tmp/lyra-holders")?;
        let release = crate::fixtures::insert_release(&mut db, "Release")?;
        let track = crate::fixtures::insert_track(&mut db, "Song")?;
        let other = crate::fixtures::insert_track(&mut db, "Other")?;
        crate::fixtures::connect(&mut db, library, release)?;
        crate::fixtures::link_track(&mut db, release, track)?;
        crate::fixtures::link_track(&mut db, release, other)?;
        let user = crate::users::create(&mut db, &crate::fixtures::test_user("listener")?)?;
        crate::favorites::add(
            &mut db,
            user,
            track,
            crate::favorites::FavoriteKind::Track,
            1,
        )?;
        crate::ratings::upsert(
            &mut db,
            user,
            track,
            crate::ratings::RatingValue::new(4).expect("valid rating"),
            1,
        )?;
        crate::fixtures::insert_listen(&mut db, user, track, 1)?;
        let visited = |db: &DbAny| -> anyhow::Result<usize> {
            Ok(db
                .exec(holders_search!(QueryBuilder::search(), track))?
                .elements
                .len())
        };

        let before = visited(&db)?;
        for started_at_ms in 0..200 {
            crate::fixtures::insert_listen(&mut db, user, other, started_at_ms)?;
        }

        assert_eq!(
            visited(&db)?,
            before,
            "the user's other listens stay out of the search"
        );
        let libraries = get_for_entity(&db, track)?
            .into_iter()
            .map(|library| library.db_id)
            .collect::<Vec<_>>();
        assert_eq!(libraries, vec![Some(library)]);
        Ok(())
    }

    fn normalize_library_name_key(raw: &str) -> Result<String, LibraryNameError> {
        Ok(normalize_library_name(raw)?.1)
    }

    // Random path so canonicalize() falls through to the lexical fallback.
    fn insert_request(name: &str, dir_suffix: &str) -> LibraryInsert {
        let path = PathBuf::from(format!("/tmp/lyra-test-{}-{dir_suffix}", nanoid()));
        let path_key = path_key_for(&path);
        LibraryInsert {
            id: nanoid(),
            name: name.to_string(),
            path,
            path_key,
            language: None,
            country: None,
            follow_links: false,
        }
    }

    fn insert_request_at(name: &str, dir: &str) -> LibraryInsert {
        let path = PathBuf::from(dir);
        let path_key = path_key_for(&path);
        LibraryInsert {
            id: nanoid(),
            name: name.to_string(),
            path,
            path_key,
            language: None,
            country: None,
            follow_links: false,
        }
    }

    #[test]
    fn delete_removes_exclusive_media_and_preserves_shared_media() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = create_test_user(&mut db, "owner")?;
        let deleted = db.transaction_mut(|t| -> anyhow::Result<_> {
            Ok(create_with_creator(
                t,
                insert_request("Deleted", "deleted"),
                user,
            )?)
        })?;
        let kept = db.transaction_mut(|t| -> anyhow::Result<_> {
            Ok(create_system(t, insert_request("Kept", "kept"))?)
        })?;
        let deleted_id = deleted.db_id.unwrap();
        let kept_id = kept.db_id.unwrap();
        let release = crate::fixtures::insert_release(&mut db, "Deleted album")?;
        let track = crate::fixtures::insert_track(&mut db, "Deleted track")?;
        let shared = crate::fixtures::insert_track(&mut db, "Shared track")?;
        let artist = crate::fixtures::insert_artist(&mut db, "Shared artist")?;
        crate::fixtures::connect(&mut db, deleted_id, release)?;
        crate::fixtures::link_track(&mut db, release, track)?;
        crate::fixtures::connect(&mut db, deleted_id, shared)?;
        crate::fixtures::connect(&mut db, kept_id, shared)?;
        crate::fixtures::connect(&mut db, track, artist)?;
        crate::fixtures::connect(&mut db, shared, artist)?;
        crate::metadata::layers::upsert(
            &mut db,
            track,
            &crate::MetadataLayer::new("local-scanner", Default::default(), 0),
        )?;
        let layer_ids = crate::metadata::collect_layer_ids(&db, track)?;
        let track_public_id = crate::tracks::get_by_id(&db, track)?.unwrap().id;
        let removed = db.transaction_mut(|t| delete(t, &deleted))?;
        assert_eq!(removed.track_ids, HashSet::from([track_public_id]));
        assert!(get_by_id(&db, deleted_id)?.is_none());
        assert!(get_by_id(&db, kept_id)?.is_some());
        assert!(crate::tracks::get_by_id(&db, track)?.is_none());
        assert!(crate::releases::get_by_id(&db, release)?.is_none());
        assert!(crate::tracks::get_by_id(&db, shared)?.is_some());
        assert!(crate::artists::get_by_id(&db, artist)?.is_some());
        assert!(accessible_library_ids(&db, user)?.is_empty());
        for id in layer_ids {
            assert!(db.exec(QueryBuilder::select().ids(id).query()).is_err());
        }
        Ok(())
    }

    #[test]
    fn layer_edges_stay_out_of_library_views() -> anyhow::Result<()> {
        use crate::{
            CreditType,
            metadata::layers::edges,
        };
        let mut db = new_test_db()?;
        let deleted = db.transaction_mut(|t| -> anyhow::Result<_> {
            Ok(create_system(t, insert_request("Deleted", "deleted"))?)
        })?;
        let kept = db.transaction_mut(|t| -> anyhow::Result<_> {
            Ok(create_system(t, insert_request("Kept", "kept"))?)
        })?;
        let (deleted_id, kept_id) = (deleted.db_id.unwrap(), kept.db_id.unwrap());
        let deleted_release = crate::fixtures::insert_release(&mut db, "Deleted album")?;
        let deleted_track = crate::fixtures::insert_track(&mut db, "Deleted track")?;
        let kept_release = crate::fixtures::insert_release(&mut db, "Kept album")?;
        let kept_track = crate::fixtures::insert_track(&mut db, "Kept track")?;
        let layer_only_artist = crate::fixtures::insert_artist(&mut db, "Hidden")?;
        crate::fixtures::connect(&mut db, deleted_id, deleted_release)?;
        crate::fixtures::link_track(&mut db, deleted_release, deleted_track)?;
        crate::fixtures::connect(&mut db, kept_id, kept_release)?;
        crate::fixtures::link_track(&mut db, kept_release, kept_track)?;
        let jazz = crate::genres::resolve_by_name(&mut db, "Jazz")?;
        crate::genres::link_to_release(&mut db, jazz, deleted_release)?;
        for release in [deleted_release, kept_release] {
            crate::metadata::layers::upsert(
                &mut db,
                release,
                &crate::MetadataLayer::new("provider", Default::default(), 0),
            )?;
            let layer = crate::metadata::layers::layer_id(&db, release, "provider")?.unwrap();
            db.transaction_mut(|t| {
                edges::replace_genres(t, layer, &[jazz])?;
                edges::replace_credits(
                    t,
                    layer,
                    CreditType::Artist,
                    &[edges::LayerCreditInput {
                        artist_id: layer_only_artist,
                        detail: None,
                    }],
                )
            })?;
        }

        let release_ids = |releases: Vec<crate::Release>| -> Vec<DbId> {
            releases
                .into_iter()
                .filter_map(|release| release.db_id.map(DbId::from))
                .collect()
        };
        assert_eq!(
            release_ids(crate::releases::get(&db, kept_id)?),
            vec![kept_release]
        );
        assert_eq!(crate::tracks::get(&db, kept_release)?.len(), 1);
        assert!(get_for_entity(&db, layer_only_artist)?.is_empty());
        assert!(get_for_entity(&db, jazz)?.is_empty());

        let deleted_track_id = crate::tracks::get_by_id(&db, deleted_track)?.unwrap().id;
        let removed = db.transaction_mut(|t| delete(t, &deleted))?;
        assert_eq!(removed.track_ids, HashSet::from([deleted_track_id]));
        assert!(crate::tracks::get_by_id(&db, deleted_track)?.is_none());
        assert!(crate::tracks::get_by_id(&db, kept_track)?.is_some());
        assert!(crate::genres::get_by_id(&db, jazz)?.is_some());
        assert!(crate::artists::get_by_id(&db, layer_only_artist)?.is_some());
        Ok(())
    }

    #[test]
    fn delete_keeps_artists_that_another_librarys_layers_reference() -> anyhow::Result<()> {
        use crate::{
            ArtistRelationType,
            CreditType,
            metadata::layers::edges,
            test_db,
        };
        let mut db = new_test_db()?;
        let deleted = db.transaction_mut(|t| -> anyhow::Result<_> {
            Ok(create_system(t, insert_request("Deleted", "deleted"))?)
        })?;
        let kept = db.transaction_mut(|t| -> anyhow::Result<_> {
            Ok(create_system(t, insert_request("Kept", "kept"))?)
        })?;
        let (deleted_id, kept_id) = (deleted.db_id.unwrap(), kept.db_id.unwrap());
        let deleted_release = crate::fixtures::insert_release(&mut db, "Deleted album")?;
        let kept_release = crate::fixtures::insert_release(&mut db, "Kept album")?;
        let credited = crate::fixtures::insert_artist(&mut db, "Credited")?;
        let related = crate::fixtures::insert_artist(&mut db, "Related")?;
        let band = crate::fixtures::insert_artist(&mut db, "Band")?;
        crate::fixtures::connect(&mut db, deleted_id, deleted_release)?;
        crate::fixtures::connect(&mut db, kept_id, kept_release)?;
        test_db::connect_artist(&mut db, deleted_release, credited)?;
        test_db::connect_artist(&mut db, deleted_release, related)?;
        let exclusive = crate::fixtures::insert_artist(&mut db, "Exclusive")?;
        test_db::connect_artist(&mut db, deleted_release, exclusive)?;
        test_db::connect_artist(&mut db, kept_release, band)?;
        for node_id in [kept_release, band] {
            crate::metadata::layers::upsert(
                &mut db,
                node_id,
                &crate::MetadataLayer::new("provider", Default::default(), 0),
            )?;
        }
        let release_layer =
            crate::metadata::layers::layer_id(&db, kept_release, "provider")?.unwrap();
        let band_layer = crate::metadata::layers::layer_id(&db, band, "provider")?.unwrap();
        db.transaction_mut(|t| {
            edges::replace_credits(
                t,
                release_layer,
                CreditType::Producer,
                &[edges::LayerCreditInput {
                    artist_id: credited,
                    detail: None,
                }],
            )?;
            edges::replace_relations(
                t,
                band_layer,
                ArtistRelationType::MemberOf,
                &[edges::LayerRelationInput {
                    artist_id: related,
                    attributes: None,
                }],
            )
        })?;

        db.transaction_mut(|t| delete(t, &deleted))?;

        assert!(crate::releases::get_by_id(&db, deleted_release)?.is_none());
        assert!(
            crate::artists::get_by_id(&db, exclusive)?.is_some(),
            "artists wait for their collection"
        );
        crate::orphans::testing::assert_artists_tracked(&db)?;
        assert_eq!(crate::orphans::testing::drain_artists(&mut db)?, 1);
        assert!(crate::artists::get_by_id(&db, exclusive)?.is_none());
        assert!(crate::artists::get_by_id(&db, credited)?.is_some());
        assert!(crate::artists::get_by_id(&db, related)?.is_some());
        assert_eq!(edges::credits(&db, release_layer)?[0].target_id, credited);
        assert_eq!(edges::relations(&db, band_layer)?[0].target_id, related);
        Ok(())
    }

    #[test]
    fn delete_removes_owned_sources_and_cue_records_with_the_track_only_they_played()
    -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let deleted = db.transaction_mut(|t| -> anyhow::Result<_> {
            Ok(create_system(
                t,
                insert_request("Deleted", "sources-deleted"),
            )?)
        })?;
        let kept = db.transaction_mut(|t| -> anyhow::Result<_> {
            Ok(create_system(t, insert_request("Kept", "sources-kept"))?)
        })?;
        let file_path = std::env::temp_dir().join(format!("lyra-delete-test-{}", nanoid()));
        std::fs::write(&file_path, b"preserve music")?;
        let entry = crate::Entry {
            db_id: None,
            id: nanoid(),
            full_path: file_path.clone(),
            kind: crate::entries::EntryKind::File,
            file_kind: None,
            name: "music.flac".into(),
            hash: None,
            size: 14,
            file_modified_at_ms: 0,
            file_created_at_ms: 0,
            synced_at_ms: None,
            missing_at_ms: None,
        };
        let entry_id = db
            .exec_mut(QueryBuilder::insert().element(&entry).query())?
            .ids()[0];
        crate::fixtures::connect(&mut db, deleted.db_id.unwrap(), entry_id)?;
        let track = crate::fixtures::insert_track(&mut db, "Shared")?;
        crate::fixtures::connect(&mut db, deleted.db_id.unwrap(), track)?;
        crate::fixtures::connect(&mut db, kept.db_id.unwrap(), track)?;
        let (sheet, selected, unselected, source) =
            db.transaction_mut(|t| -> anyhow::Result<_> {
                let sheet = crate::cue::sheets::upsert(t, entry_id, "hash")?;
                let selected =
                    crate::cue::tracks::upsert(t, sheet, entry_id, 1, entry_id, None, 0)?;
                let unselected =
                    crate::cue::tracks::upsert(t, sheet, entry_id, 2, entry_id, None, 75)?;
                let source = crate::track_sources::upsert(
                    t,
                    track,
                    entry_id,
                    crate::track_sources::TrackSourceUpsert {
                        source_kind: "cue".into(),
                        source_key: "deleted-source".into(),
                        is_primary: true,
                        start_ms: Some(0),
                        end_ms: Some(1000),
                        format: Default::default(),
                    },
                    Some(selected),
                )?;
                Ok((sheet, selected, unselected, source))
            })?;
        let track_id = crate::tracks::get_by_id(&db, track)?.unwrap().id;
        let affected = db.transaction_mut(|t| delete(t, &deleted))?;
        assert!(affected.track_ids.contains(&track_id));
        // The other library reached the track, but only this library's file played it.
        for id in [entry_id, sheet, selected, unselected, source, track] {
            assert!(
                db.exec(QueryBuilder::select().ids(id).query()).is_err(),
                "node {} survived",
                id.0
            );
        }
        assert_eq!(std::fs::read(&file_path)?, b"preserve music");
        std::fs::remove_file(file_path)?;
        Ok(())
    }

    #[test]
    fn delete_collects_a_cue_sheet_another_library_holds_once_its_audio_is_gone()
    -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let deleted = db.transaction_mut(|t| -> anyhow::Result<_> {
            Ok(create_system(t, insert_request("Deleted", "cue-deleted"))?)
        })?;
        let kept = db.transaction_mut(|t| -> anyhow::Result<_> {
            Ok(create_system(t, insert_request("Kept", "cue-kept"))?)
        })?;
        let (deleted_id, kept_id) = (deleted.db_id.unwrap(), kept.db_id.unwrap());
        // The audio is the deleted library's, the cue file naming it the kept one's.
        let audio = crate::fixtures::insert_file_entry(&mut db, deleted_id, "image.flac")?;
        let cue = crate::fixtures::insert_file_entry(&mut db, kept_id, "image.cue")?;
        let release = crate::fixtures::insert_release(&mut db, "Shared")?;
        crate::fixtures::connect(&mut db, deleted_id, release)?;
        crate::fixtures::connect(&mut db, kept_id, release)?;
        let track = crate::fixtures::insert_track(&mut db, "Segment")?;
        crate::fixtures::link_track(&mut db, release, track)?;
        let (sheet, cue_track, source) = db.transaction_mut(|t| -> anyhow::Result<_> {
            let sheet = crate::cue::sheets::upsert(t, cue, "hash")?;
            let cue_track = crate::cue::tracks::upsert(t, sheet, cue, 1, audio, None, 0)?;
            let source = crate::track_sources::upsert(
                t,
                track,
                audio,
                crate::track_sources::TrackSourceUpsert {
                    source_kind: "cue".into(),
                    source_key: "shared-segment".into(),
                    is_primary: true,
                    start_ms: Some(0),
                    end_ms: Some(1000),
                    format: Default::default(),
                },
                Some(cue_track),
            )?;
            Ok((sheet, cue_track, source))
        })?;
        crate::orphans::testing::assert_collected(&db)?;

        db.transaction_mut(|t| delete(t, &deleted))?;

        for id in [audio, source, cue_track, sheet, track, release] {
            assert!(
                db.exec(QueryBuilder::select().ids(id).query()).is_err(),
                "node {} survived",
                id.0
            );
        }
        assert!(crate::entries::get_by_id(&db, cue)?.is_some());
        crate::orphans::testing::assert_collected(&db)
    }

    #[test]
    fn delete_rolls_back_the_whole_graph_on_transaction_failure() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let library = db.transaction_mut(|t| -> anyhow::Result<_> {
            Ok(create_system(t, insert_request("Rollback", "rollback"))?)
        })?;
        let track = crate::fixtures::insert_track(&mut db, "Retained")?;
        crate::fixtures::connect(&mut db, library.db_id.unwrap(), track)?;
        let result = db.transaction_mut(|t| -> anyhow::Result<()> {
            delete(t, &library)?;
            anyhow::bail!("abort")
        });
        assert!(result.is_err());
        assert!(get_by_id(&db, library.db_id.unwrap())?.is_some());
        assert!(crate::tracks::get_by_id(&db, track)?.is_some());
        Ok(())
    }

    #[test]
    fn normalize_name_key_nfc_collapses_decomposed_form() {
        let composed = normalize_library_name_key("café").unwrap();
        let decomposed = normalize_library_name_key("cafe\u{0301}").unwrap();
        assert_eq!(composed, decomposed);
    }

    #[test]
    fn normalize_name_key_strips_zwsp_and_cgj() {
        let plain = normalize_library_name_key("Music").unwrap();
        let with_zwsp = normalize_library_name_key("Music\u{200B}").unwrap();
        let with_cgj = normalize_library_name_key("Mu\u{034F}sic").unwrap();
        assert_eq!(plain, with_zwsp);
        assert_eq!(plain, with_cgj);
    }

    #[test]
    fn normalize_name_key_trims_whitespace_after_stripping_invisibles() {
        assert_eq!(
            normalize_library_name_key("  Music  ").unwrap(),
            normalize_library_name_key("Music").unwrap()
        );
        assert_eq!(
            normalize_library_name_key("\u{200B}\u{200B}").unwrap_err(),
            LibraryNameError::Empty
        );
    }

    #[test]
    fn normalize_name_key_rejects_control_chars() {
        assert_eq!(
            normalize_library_name_key("Mus\u{0007}ic").unwrap_err(),
            LibraryNameError::ContainsControl
        );
    }

    #[test]
    fn normalize_name_key_collapses_lowercase_decomposition() {
        // Lowercase splits these unless a second NFC pass runs.
        let decomposed = normalize_library_name_key("J\u{030C}").unwrap();
        let precomposed = normalize_library_name_key("\u{01F0}").unwrap();
        assert_eq!(decomposed, precomposed);
    }

    #[test]
    fn lexical_normalize_collapses_redundant_segments() {
        assert_eq!(
            lexical_normalize_path(Path::new("/a//b/./c/")),
            PathBuf::from("/a/b/c")
        );
        assert_eq!(
            lexical_normalize_path(Path::new("/a/b/../c")),
            PathBuf::from("/a/c")
        );
        assert_eq!(
            lexical_normalize_path(Path::new("/../a")),
            PathBuf::from("/a")
        );
    }

    #[test]
    fn lexical_normalize_empty_becomes_dot() {
        assert_eq!(lexical_normalize_path(Path::new("")), PathBuf::from("."));
    }

    fn create_test_user(db: &mut DbAny, username: &str) -> anyhow::Result<DbId> {
        super::super::users::create(db, &super::super::fixtures::test_user(username)?)
    }

    #[test]
    fn create_rejects_duplicate_name_case_insensitive() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        db.transaction_mut(|t| -> anyhow::Result<()> {
            create_system(t, insert_request("Music", "a"))?;
            Ok(())
        })?;

        let outcome = db.transaction_mut(|t| -> anyhow::Result<_> {
            Ok(create_system(t, insert_request("MUSIC", "b")))
        })?;
        assert!(matches!(outcome, Err(LibraryCreateError::NameInUse(_))));
        Ok(())
    }

    #[test]
    fn create_rejects_duplicate_directory_lexically_equivalent() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        // Randomized base so canonicalize doesn't resolve a stray pre-existing dir.
        let base = format!("/tmp/lyra-test-dup-{}/library", nanoid());
        db.transaction_mut(|t| -> anyhow::Result<()> {
            create_system(t, insert_request_at("First", &base))?;
            Ok(())
        })?;

        let dup_input = format!("{base}/../library/./");
        let outcome = db.transaction_mut(|t| -> anyhow::Result<_> {
            Ok(create_system(t, insert_request_at("Second", &dup_input)))
        })?;
        assert!(matches!(outcome, Err(LibraryCreateError::PathInUse(_))));
        Ok(())
    }

    #[test]
    fn update_clears_absent_optional_fields() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let seeded = db.transaction_mut(|t| -> anyhow::Result<Library> {
            let mut request = insert_request("Music", "clear-optionals");
            request.language = Some("en".to_string());
            request.country = Some("us".to_string());
            request.follow_links = true;
            Ok(create_system(t, request)?)
        })?;
        assert!(seeded.follows_links());
        let library_db_id = seeded.db_id.expect("created library has a db_id");

        db.transaction_mut(|t| -> anyhow::Result<()> {
            update(
                t,
                &Library {
                    language: None,
                    country: None,
                    follow_links: None,
                    ..seeded
                },
            )?;
            Ok(())
        })?;

        let keys = crate::test_db::stored_keys(&db, library_db_id)?;
        assert_eq!(
            keys,
            [
                "db_element_id",
                "id",
                "name",
                "name_key",
                "path",
                "path_key"
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
            "only non-Option keys may remain after an all-None update"
        );
        let stored = get_by_id(&db, library_db_id)?.expect("library stored");
        assert!(!stored.follows_links());
        Ok(())
    }

    #[test]
    fn update_rejects_rename_to_existing_library() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        db.transaction_mut(|t| -> anyhow::Result<()> {
            create_system(t, insert_request("Music", "rename-a"))?;
            Ok(())
        })?;
        let other = db.transaction_mut(|t| -> anyhow::Result<Library> {
            Ok(create_system(t, insert_request("Sound", "rename-b"))?)
        })?;

        let renamed = Library {
            name: "Music".to_string(),
            ..other
        };
        let outcome = db.transaction_mut(|t| -> anyhow::Result<_> { Ok(update(t, &renamed)) })?;
        assert!(matches!(outcome, Err(LibraryUpdateError::NameInUse(_))));
        Ok(())
    }

    #[test]
    fn update_allows_self_rename_noop() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let lib = db.transaction_mut(|t| -> anyhow::Result<Library> {
            Ok(create_system(t, insert_request("Music", "self"))?)
        })?;

        let outcome = db.transaction_mut(|t| -> anyhow::Result<_> { Ok(update(t, &lib)) })?;
        assert!(outcome.is_ok());
        Ok(())
    }

    #[test]
    fn find_by_name_key_uses_stored_key() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        db.transaction_mut(|t| -> anyhow::Result<()> {
            create_system(t, insert_request("café", "find-by-name"))?;
            Ok(())
        })?;

        let key = normalize_library_name_key("CAFE\u{0301}\u{200B}")?;
        let found = find_by_name_key(&db, &key)?;
        assert!(found.is_some());
        Ok(())
    }

    #[test]
    fn create_with_creator_inserts_access_edge() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = create_test_user(&mut db, "creator")?;
        let lib = db.transaction_mut(|t| -> anyhow::Result<Library> {
            Ok(create_with_creator(
                t,
                insert_request("Music", "creator"),
                user_db_id,
            )?)
        })?;

        let library_db_id = lib.db_id.expect("library db_id present after create");
        let user_view = accessible_library_ids(&db, user_db_id)?;
        assert!(user_view.contains(&lib.id));

        let listed = users_with_access(&db, library_db_id)?;
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].username, "creator");

        Ok(())
    }

    #[test]
    fn accessible_track_ids_accepts_any_accessible_library_path() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = create_test_user(&mut db, "listener")?;
        let inaccessible = db.transaction_mut(|t| -> anyhow::Result<Library> {
            Ok(create_system(t, insert_request("Private", "private"))?)
        })?;
        let accessible = db.transaction_mut(|t| -> anyhow::Result<Library> {
            Ok(create_with_creator(
                t,
                insert_request("Shared", "shared"),
                user_db_id,
            )?)
        })?;
        let shared_track = crate::fixtures::insert_track(&mut db, "Shared track")?;
        let private_track = crate::fixtures::insert_track(&mut db, "Private track")?;
        db.transaction_mut(|t| -> anyhow::Result<()> {
            crate::graph::ensure_owned_edge(
                t,
                inaccessible.db_id.expect("private library db_id"),
                shared_track,
            )?;
            crate::graph::ensure_owned_edge(
                t,
                inaccessible.db_id.expect("private library db_id"),
                private_track,
            )?;
            crate::graph::ensure_owned_edge(
                t,
                accessible.db_id.expect("shared library db_id"),
                shared_track,
            )?;
            Ok(())
        })?;

        let visible = accessible_track_ids(&db, user_db_id, &[shared_track, private_track])?;
        assert!(visible.contains(&shared_track));
        assert!(!visible.contains(&private_track));
        Ok(())
    }

    #[test]
    fn create_system_inserts_no_access_edge() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let lib = db.transaction_mut(|t| -> anyhow::Result<Library> {
            Ok(create_system(t, insert_request("Music", "system"))?)
        })?;
        let library_db_id = lib.db_id.expect("library db_id present");

        assert!(users_with_access(&db, library_db_id)?.is_empty());
        Ok(())
    }

    #[test]
    fn delete_user_cascade_removes_access_edges() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = create_test_user(&mut db, "alice")?;
        let lib = db.transaction_mut(|t| -> anyhow::Result<Library> {
            Ok(create_with_creator(
                t,
                insert_request("Music", "cascade"),
                user_db_id,
            )?)
        })?;
        let library_db_id = lib.db_id.expect("library db_id");

        assert_eq!(users_with_access(&db, library_db_id)?.len(), 1);

        super::super::users::delete_user(&mut db, user_db_id)?;

        assert!(users_with_access(&db, library_db_id)?.is_empty());
        assert!(get_by_id(&db, library_db_id)?.is_some());
        Ok(())
    }

    #[test]
    fn revoke_access_is_idempotent() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = create_test_user(&mut db, "alice")?;
        let lib = db.transaction_mut(|t| -> anyhow::Result<Library> {
            Ok(create_with_creator(
                t,
                insert_request("Music", "rev"),
                user_db_id,
            )?)
        })?;
        let library_db_id = lib.db_id.expect("library db_id");

        let removed = db.transaction_mut(|t| -> anyhow::Result<bool> {
            revoke_access(t, user_db_id, library_db_id)
        })?;
        assert!(removed);

        let removed_again = db.transaction_mut(|t| -> anyhow::Result<bool> {
            revoke_access(t, user_db_id, library_db_id)
        })?;
        assert!(!removed_again);
        Ok(())
    }

    #[test]
    fn find_node_id_accessible_to_user_resolves_library_with_edge() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = create_test_user(&mut db, "alice")?;
        let lib = db.transaction_mut(|t| -> anyhow::Result<Library> {
            Ok(create_with_creator(
                t,
                insert_request("Music", "fa-yes"),
                user_db_id,
            )?)
        })?;
        let resolved = find_node_id_accessible_to_user(&db, user_db_id, &lib.id)?;
        assert_eq!(resolved, lib.db_id);
        Ok(())
    }

    #[test]
    fn find_node_id_accessible_to_user_rejects_inaccessible_library() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let creator_db_id = create_test_user(&mut db, "creator")?;
        let viewer_db_id = create_test_user(&mut db, "viewer")?;
        let lib = db.transaction_mut(|t| -> anyhow::Result<Library> {
            Ok(create_with_creator(
                t,
                insert_request("Music", "fa-no"),
                creator_db_id,
            )?)
        })?;
        let resolved = find_node_id_accessible_to_user(&db, viewer_db_id, &lib.id)?;
        assert!(resolved.is_none());
        Ok(())
    }

    #[test]
    fn find_node_id_accessible_to_user_returns_none_for_missing_public_id() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = create_test_user(&mut db, "alice")?;
        let resolved =
            find_node_id_accessible_to_user(&db, user_db_id, "no-such-library-public-id")?;
        assert!(resolved.is_none());
        Ok(())
    }

    #[test]
    fn find_node_id_accessible_to_user_rejects_non_library_node() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = create_test_user(&mut db, "alice")?;
        let foreign_public_id = nanoid();
        db.exec_mut(
            QueryBuilder::insert()
                .nodes()
                .values([[("id", foreign_public_id.as_str()).into()]])
                .query(),
        )?;
        let resolved = find_node_id_accessible_to_user(&db, user_db_id, &foreign_public_id)?;
        assert!(resolved.is_none());
        Ok(())
    }
    #[cfg(feature = "nightly")]
    mod benches {
        extern crate test;

        use test::Bencher;

        use super::*;
        use crate::fixtures::test_user;
        use crate::nanoid;
        use crate::test_db::new_test_db;
        use crate::users;

        /// 2 owned libs + 1 inaccessible (other user) + 1 foreign-typed node.
        struct ParitySetup {
            db: agdb::DbAny,
            user_db_id: DbId,
            inaccessible_id: String,
            missing_id: String,
            foreign_id: String,
        }

        fn parity_setup() -> ParitySetup {
            let mut db = new_test_db().unwrap();
            let user_db_id = users::create(&mut db, &test_user("alice").unwrap()).unwrap();
            for i in 0..2 {
                db.transaction_mut(|t| -> anyhow::Result<()> {
                    let path = PathBuf::from(format!("/tmp/lyra-bench-own-{i}-{}", nanoid()));
                    let path_key = path_key_for(&path);
                    create_with_creator(
                        t,
                        LibraryInsert {
                            id: nanoid(),
                            name: format!("Owned-{i}"),
                            path,
                            path_key,
                            language: None,
                            country: None,
                            follow_links: false,
                        },
                        user_db_id,
                    )?;
                    Ok(())
                })
                .unwrap();
            }
            let other_user = users::create(&mut db, &test_user("other").unwrap()).unwrap();
            let inaccessible = db
                .transaction_mut(|t| -> anyhow::Result<Library> {
                    let path = PathBuf::from(format!("/tmp/lyra-bench-inacc-{}", nanoid()));
                    let path_key = path_key_for(&path);
                    Ok(create_with_creator(
                        t,
                        LibraryInsert {
                            id: nanoid(),
                            name: "Hidden".to_string(),
                            path,
                            path_key,
                            language: None,
                            country: None,
                            follow_links: false,
                        },
                        other_user,
                    )?)
                })
                .unwrap();
            let foreign_public_id = nanoid();
            db.exec_mut(
                QueryBuilder::insert()
                    .nodes()
                    .values([[("id", foreign_public_id.as_str()).into()]])
                    .query(),
            )
            .unwrap();
            ParitySetup {
                db,
                user_db_id,
                inaccessible_id: inaccessible.id,
                missing_id: "no-such-library-public-id".to_string(),
                foreign_id: foreign_public_id,
            }
        }

        #[bench]
        fn bench_find_node_id_accessible_to_user_inaccessible(b: &mut Bencher) {
            let s = parity_setup();
            b.iter(|| {
                find_node_id_accessible_to_user(&s.db, s.user_db_id, &s.inaccessible_id).unwrap()
            });
        }

        #[bench]
        fn bench_find_node_id_accessible_to_user_missing(b: &mut Bencher) {
            let s = parity_setup();
            b.iter(|| find_node_id_accessible_to_user(&s.db, s.user_db_id, &s.missing_id).unwrap());
        }

        #[bench]
        fn bench_find_node_id_accessible_to_user_foreign(b: &mut Bencher) {
            let s = parity_setup();
            b.iter(|| find_node_id_accessible_to_user(&s.db, s.user_db_id, &s.foreign_id).unwrap());
        }
    }
}
