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

/// `path` is the raw user input (preserves symlinks, surfaced to API);
/// `path_key` is the canonical form used only for uniqueness lookups.
/// `name_key` is `name` lowercased + NFC, indexed for the same reason.
/// Storing the keys avoids Unicode/canonicalize work under the write lock.
#[derive(DbElement, Clone, Debug)]
pub(crate) struct Library {
    pub(crate) db_id: Option<DbId>,
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) name_key: String,
    pub(crate) path: PathBuf,
    pub(crate) path_key: String,
    pub(crate) language: Option<String>,
    pub(crate) country: Option<String>,
}

impl mlua::IntoLua for Library {
    fn into_lua(self, lua: &mlua::Lua) -> mlua::Result<mlua::Value> {
        let table = lua.create_table()?;
        if let Some(db_id) = self.db_id {
            table.set("db_id", db_id.0)?;
        }
        table.set("id", self.id)?;
        table.set("name", self.name)?;
        table.set("path", self.path.to_string_lossy().to_string())?;
        if let Some(language) = self.language {
            table.set("language", language)?;
        }
        if let Some(country) = self.country {
            table.set("country", country)?;
        }
        Ok(mlua::Value::Table(table))
    }
}

pub(crate) fn get(db: &impl super::DbAccess) -> anyhow::Result<Vec<Library>> {
    let libraries: Vec<Library> = db
        .exec(
            QueryBuilder::select()
                .elements::<Library>()
                .search()
                .from("libraries")
                .query(),
        )?
        .try_into()?;

    Ok(libraries)
}

/// Strip `Cf` (zero-widths, bidi, SHY, BOM) plus CGJ, which blocks NFC
/// composition and lets `"Music\u{034F}"` duplicate `"Music"`. Same recipe
/// as [`crate::db::tags`].
fn is_invisible_strippable(c: char) -> bool {
    c == '\u{034F}' || c.general_category() == GeneralCategory::Format
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub(crate) enum LibraryNameError {
    #[error("library name cannot be empty after normalization")]
    Empty,
    #[error("library name contains control characters")]
    ContainsControl,
}

/// Display form: invisibles stripped, whitespace trimmed, NFC. Stripping must
/// precede trimming since the invisibles aren't `White_Space`. Variation
/// selectors are preserved (intentional for emoji); ZWSP/ZWJ are stripped as
/// copy-paste artifacts.
pub(crate) fn normalize_library_name_display(raw: &str) -> Result<String, LibraryNameError> {
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

/// `to_lowercase` can decompose an NFC string (e.g. `"J\u{030C}"` → `"j\u{030C}"`
/// while `"\u{01F0}"` stays precomposed); a second NFC pass is required so
/// canonically-equivalent names hash to the same key.
fn lowercase_nfc(s: &str) -> String {
    s.to_lowercase().nfc().collect()
}

/// Comparison key: display form + lowercase + NFC. Lowercase fold is imperfect
/// (`ß`/`ss`, final-sigma) but matches `db::labels`. Prefer
/// [`normalize_library_name`] when both forms are needed.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn normalize_library_name_key(raw: &str) -> Result<String, LibraryNameError> {
    Ok(lowercase_nfc(&normalize_library_name_display(raw)?))
}

/// Returns `(display, key)` from a single normalization pass.
pub(crate) fn normalize_library_name(raw: &str) -> Result<(String, String), LibraryNameError> {
    let display = normalize_library_name_display(raw)?;
    let key = lowercase_nfc(&display);
    Ok((display, key))
}

/// Collapse `.`/`..`/`//`. Used as the comparison key when `canonicalize`
/// is unavailable (missing path, dead mount, etc.).
fn lexical_normalize_path(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => match out.components().next_back() {
                Some(Component::Normal(_)) => {
                    out.pop();
                }
                // `..` at the root cannot escape; on a relative path, preserve it.
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

/// `canonicalize` (resolves symlinks), falling back to lexical normalization
/// on any IO error. **Sync syscall** — never call from inside a transaction;
/// wrap in `spawn_blocking` from async contexts. The lexical fallback won't
/// unify case on case-insensitive filesystems.
pub(crate) fn normalize_library_path(path: &Path) -> PathBuf {
    match std::fs::canonicalize(path) {
        Ok(canonical) => canonical,
        Err(_) => lexical_normalize_path(path),
    }
}

/// String form of [`normalize_library_path`] for agdb value-match.
pub(crate) fn path_key_for(path: &Path) -> String {
    normalize_library_path(path).to_string_lossy().into_owned()
}

/// Indexed lookup; safe to call inside a transaction.
pub(crate) fn find_by_name_key(
    db: &impl super::DbAccess,
    name_key: &str,
) -> anyhow::Result<Option<Library>> {
    find_indexed_library(db, "name_key", name_key)
}

/// Indexed lookup; safe to call inside a transaction. Compute `key` via
/// [`path_key_for`] *outside* the lock — `canonicalize` is a sync syscall.
pub(crate) fn find_by_path_key(
    db: &impl super::DbAccess,
    key: &str,
) -> anyhow::Result<Option<Library>> {
    find_indexed_library(db, "path_key", key)
}

// `index_name` must be registered in [`crate::db::bootstrap::CORE_INDEXES`].
// The libraries-alias filter guards against accidental key collisions on
// non-Library nodes.
fn find_indexed_library(
    db: &impl super::DbAccess,
    index_name: &str,
    value: &str,
) -> anyhow::Result<Option<Library>> {
    let candidate_ids: Vec<DbId> = db
        .exec(
            QueryBuilder::search()
                .index(index_name)
                .value(value)
                .query(),
        )?
        .ids()
        .into_iter()
        .filter(|id| id.0 > 0)
        .collect();
    for id in candidate_ids {
        if !super::lookup::collection_contains_id(db, "libraries", id)? {
            continue;
        }
        let library: Vec<Library> = db
            .exec(QueryBuilder::select().elements::<Library>().ids(id).query())?
            .try_into()?;
        if let Some(lib) = library.into_iter().next() {
            return Ok(Some(lib));
        }
    }
    Ok(None)
}

pub(crate) fn get_by_id(
    db: &impl super::DbAccess,
    library_db_id: DbId,
) -> anyhow::Result<Option<Library>> {
    super::graph::fetch_typed_by_id(db, library_db_id, "Library")
}

/// Both branches run `find_node_id_by_id` + `collection_contains_id` so
/// missing and non-library inputs are timing-indistinguishable.
#[allow(dead_code)]
pub(crate) fn find_accessible_node_id_by_id(
    db: &impl super::DbAccess,
    _principal: &crate::services::auth::Principal,
    public_id: &str,
) -> anyhow::Result<Option<DbId>> {
    let resolved = super::lookup::find_node_id_by_id(db, public_id)?;
    let in_libraries = match resolved {
        Some(id) => super::lookup::collection_contains_id(db, "libraries", id)?,
        None => {
            let _ = super::lookup::collection_contains_id(db, "libraries", DbId(0));
            false
        }
    };
    Ok(resolved.filter(|_| in_libraries))
}

pub(crate) fn get_by_alias(db: &impl super::DbAccess, alias: &str) -> anyhow::Result<Vec<Library>> {
    let libraries: Vec<Library> = db
        .exec(
            QueryBuilder::select()
                .elements::<Library>()
                .search()
                .from(alias)
                .query(),
        )?
        .try_into()?;

    Ok(libraries)
}

pub(crate) fn get_for_entity(
    db: &impl super::DbAccess,
    node_id: DbId,
) -> anyhow::Result<Vec<Library>> {
    let libraries: Vec<Library> = db
        .exec(
            QueryBuilder::select()
                .elements::<Library>()
                .search()
                .to(node_id)
                .where_()
                .not_beyond()
                .key("db_element_id")
                .value("Library")
                .query(),
        )?
        .try_into()?;
    Ok(libraries)
}

pub(crate) fn get_by_release(
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

/// Resolves the owning library for each entity, caching intermediate results.
pub(crate) fn get_for_entities(
    db: &impl super::DbAccess,
    entity_ids: &[DbId],
) -> anyhow::Result<std::collections::HashMap<DbId, Library>> {
    use std::collections::{
        HashMap,
        HashSet,
    };

    let unique_ids = super::dedup_positive_ids(entity_ids);
    if unique_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let all_libraries = get(db)?;
    if all_libraries.is_empty() {
        return Ok(HashMap::new());
    }

    let library_id_set: HashSet<DbId> = all_libraries.iter().filter_map(|lib| lib.db_id).collect();
    let libraries_by_id: HashMap<DbId, &Library> = all_libraries
        .iter()
        .filter_map(|lib| lib.db_id.map(|id| (id, lib)))
        .collect();

    let mut resolved_cache: HashMap<DbId, DbId> = HashMap::new();
    let mut result = HashMap::new();

    for entity_id in unique_ids {
        if library_id_set.contains(&entity_id) {
            if let Some(&lib) = libraries_by_id.get(&entity_id) {
                result.insert(entity_id, lib.clone());
            }
            continue;
        }

        if let Some(&lib_id) = resolved_cache.get(&entity_id) {
            if let Some(&lib) = libraries_by_id.get(&lib_id) {
                result.insert(entity_id, lib.clone());
            }
            continue;
        }

        let ancestors = db.exec(
            QueryBuilder::search()
                .to(entity_id)
                .where_()
                .node()
                .and()
                .not_beyond()
                .key("db_element_id")
                .value("Library")
                .query(),
        )?;

        for ancestor in &ancestors.elements {
            if ancestor.id.0 > 0 && library_id_set.contains(&ancestor.id) {
                if let Some(&lib) = libraries_by_id.get(&ancestor.id) {
                    result.insert(entity_id, lib.clone());
                    for node in &ancestors.elements {
                        if node.id.0 > 0 && node.id != ancestor.id {
                            resolved_cache.insert(node.id, ancestor.id);
                        }
                    }
                    resolved_cache.insert(entity_id, ancestor.id);
                }
                break;
            }
        }
    }

    Ok(result)
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum LibraryCreateError {
    #[error("a library named '{0}' already exists")]
    NameInUse(String),
    #[error("a library already exists for directory: {}", .0.display())]
    PathInUse(PathBuf),
    #[error("invalid library name: {0}")]
    InvalidName(#[from] LibraryNameError),
    #[error(transparent)]
    Db(#[from] anyhow::Error),
}

// `transaction_mut` requires `E: From<DbError>`; route through anyhow.
impl From<agdb::DbError> for LibraryCreateError {
    fn from(e: agdb::DbError) -> Self {
        Self::Db(anyhow::Error::new(e))
    }
}

pub(crate) struct LibraryInsert {
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) path: PathBuf,
    /// Computed via [`path_key_for`] off the lock — `canonicalize` is a sync syscall.
    pub(crate) path_key: String,
    pub(crate) language: Option<String>,
    pub(crate) country: Option<String>,
}

/// Auto-grants the creator atomically with the node insert. No syscalls inside;
/// `request.path_key` must already be canonical.
pub(crate) fn create_with_creator(
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
pub(crate) fn create_system(
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
pub(crate) enum LibraryUpdateError {
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

/// Re-derives `name_key` from `library.name`. Self (matching `db_id`) is
/// excluded from the uniqueness check. `directory`/`path_key` are not
/// validated — directory edits aren't supported; if added, recompute
/// `path_key` off the lock and re-check via [`find_by_path_key`].
pub(crate) fn update(
    db: &mut impl super::DbAccess,
    library: &Library,
    clear_language: bool,
    clear_country: bool,
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

    if clear_language {
        db.exec_mut(
            QueryBuilder::remove()
                .values(["language".to_string()])
                .ids(library_db_id)
                .query(),
        )?;
    }
    if clear_country {
        db.exec_mut(
            QueryBuilder::remove()
                .values(["country".to_string()])
                .ids(library_db_id)
                .query(),
        )?;
    }
    db.exec_mut(QueryBuilder::insert().element(&stored).query())?;

    Ok(stored)
}

// Library access edges

const ACCESS_KIND_KEY: &str = "library_access_kind";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AccessKind {
    ReadWrite,
}

impl AccessKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::ReadWrite => "read_write",
        }
    }

    #[allow(dead_code)] // entry point for the access-list route
    pub(crate) fn from_db_value(value: &DbValue) -> Option<Self> {
        match value {
            DbValue::String(s) if s == "read_write" => Some(Self::ReadWrite),
            _ => None,
        }
    }
}

/// Idempotent — existing edge has its `access_kind` overwritten.
pub(crate) fn grant_access(
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
#[allow(dead_code)] // entry point for the access-revoke route
pub(crate) fn revoke_access(
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
#[allow(dead_code)] // entry point for transactional grant/revoke authorization
pub(crate) fn user_has_access_in_txn(
    txn: &mut impl super::DbAccess,
    user_db_id: DbId,
    library_db_id: DbId,
) -> anyhow::Result<bool> {
    Ok(find_access_edge(txn, user_db_id, library_db_id)?.is_some())
}

/// Sorted by `to_ascii_lowercase(username)` to match `db::users::get`.
#[allow(dead_code)] // entry point for GET /libraries/{id}/access
pub(crate) fn users_with_access(
    db: &impl super::DbAccess,
    library_db_id: DbId,
) -> anyhow::Result<Vec<super::users::User>> {
    let edges = read_inbound_access_edges(db, library_db_id)?;
    let mut users: Vec<super::users::User> = Vec::with_capacity(edges.len());
    for edge in edges {
        let Some(user_db_id) = edge.from else { continue };
        // Skip orphan edges (user-node missing); real errors still propagate.
        if let Some(user) = super::users::get_by_id(db, user_db_id)? {
            users.push(user);
        }
    }
    users.sort_by_key(|user| user.username.to_ascii_lowercase());
    Ok(users)
}

/// Explicit-access only — no admin bypass here.
#[allow(dead_code)] // backs `Principal.accessible_library_ids`
pub(crate) fn accessible_library_ids(
    db: &impl super::DbAccess,
    user_db_id: DbId,
) -> anyhow::Result<HashSet<String>> {
    let edges = read_outbound_access_edges(db, user_db_id)?;
    let mut ids = HashSet::with_capacity(edges.len());
    for edge in edges {
        let Some(library_db_id) = edge.to else {
            continue;
        };
        if let Some(library) = get_by_id(db, library_db_id)? {
            ids.insert(library.id);
        }
    }
    Ok(ids)
}

/// Cascade hook for `db::users::delete_user`.
#[allow(dead_code)] // wired into delete_user cascade in a follow-up
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

/// Cascade hook for library deletion. Caller removes the library node afterward.
#[allow(dead_code)]
pub(crate) fn remove_access_edges_for_library(
    db: &mut impl super::DbAccess,
    library_db_id: DbId,
) -> anyhow::Result<()> {
    let edge_ids: Vec<DbId> = read_inbound_access_edges(db, library_db_id)?
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
        if element.to == Some(library_db_id) {
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
        .filter(|element| element.from == Some(user_db_id) && element_is_access(element))
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
            .end_where()
            .query(),
    )?;
    Ok(result
        .elements
        .into_iter()
        .filter(|element| element.to == Some(library_db_id) && element_is_access(element))
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
    use crate::db::test_db::new_test_db;
    use agdb::DbAny;
    use nanoid::nanoid;

    fn create_test_user(db: &mut DbAny, username: &str) -> anyhow::Result<DbId> {
        super::super::users::create(db, &super::super::users::test_user(username)?)
    }

    // Path is randomized so canonicalize() always falls through to the lexical
    // path — that's what these tests exercise.
    fn insert_request(name: &str, dir_suffix: &str) -> LibraryInsert {
        let path = PathBuf::from(format!("/tmp/lyra-test-{}-{dir_suffix}", nanoid!()));
        let path_key = path_key_for(&path);
        LibraryInsert {
            id: nanoid!(),
            name: name.to_string(),
            path,
            path_key,
            language: None,
            country: None,
        }
    }

    fn insert_request_at(name: &str, dir: &str) -> LibraryInsert {
        let path = PathBuf::from(dir);
        let path_key = path_key_for(&path);
        LibraryInsert {
            id: nanoid!(),
            name: name.to_string(),
            path,
            path_key,
            language: None,
            country: None,
        }
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
    fn normalize_name_key_lowercases() {
        assert_eq!(
            normalize_library_name_key("MUSIC").unwrap(),
            normalize_library_name_key("music").unwrap()
        );
    }

    #[test]
    fn normalize_name_key_trims_whitespace_after_stripping_invisibles() {
        assert_eq!(
            normalize_library_name_key("  Music  ").unwrap(),
            normalize_library_name_key("Music").unwrap()
        );
        // All-invisible input rejects rather than passing through.
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
    fn normalize_name_key_built_on_display() {
        // Guards against `_key` and `_display` drifting under future policy changes.
        let display = normalize_library_name_display("  Café\u{200B}  ").unwrap();
        let expected: String = display.to_lowercase().nfc().collect();
        let key = normalize_library_name_key("  Café\u{200B}  ").unwrap();
        assert_eq!(expected, key);
    }

    #[test]
    fn normalize_name_key_collapses_lowercase_decomposition() {
        // `J\u{030C}` and `\u{01F0}` are canonically equivalent but lowercase
        // produces distinct byte sequences without a second NFC pass.
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
        // `..` at the root cannot escape.
        assert_eq!(
            lexical_normalize_path(Path::new("/../a")),
            PathBuf::from("/a")
        );
    }

    #[test]
    fn lexical_normalize_empty_becomes_dot() {
        assert_eq!(lexical_normalize_path(Path::new("")), PathBuf::from("."));
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
        // Randomized base — if `/tmp/lyra-test-dup` happened to exist on the
        // dev machine, canonicalize would resolve it and the assertion would
        // be against a different key.
        let base = format!("/tmp/lyra-test-dup-{}/library", nanoid!());
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
        let outcome =
            db.transaction_mut(|t| -> anyhow::Result<_> { Ok(update(t, &renamed, false, false)) })?;
        assert!(matches!(outcome, Err(LibraryUpdateError::NameInUse(_))));
        Ok(())
    }

    #[test]
    fn update_allows_self_rename_noop() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let lib = db.transaction_mut(|t| -> anyhow::Result<Library> {
            Ok(create_system(t, insert_request("Music", "self"))?)
        })?;

        let outcome =
            db.transaction_mut(|t| -> anyhow::Result<_> { Ok(update(t, &lib, false, false)) })?;
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

    fn placeholder_principal() -> crate::services::auth::Principal {
        crate::services::auth::Principal {
            user_db_id: DbId(0),
            user_public_id: "test-principal".to_string(),
            username: "test".to_string(),
            permissions: vec![],
            role_name: None,
        }
    }

    #[test]
    fn find_accessible_node_id_by_id_resolves_existing_library() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        crate::db::indexes::ensure_index(&mut db, "id")?;
        let lib = db.transaction_mut(|t| -> anyhow::Result<Library> {
            Ok(create_system(t, insert_request("Music", "exists"))?)
        })?;
        let principal = placeholder_principal();

        let resolved = find_accessible_node_id_by_id(&db, &principal, &lib.id)?;
        assert_eq!(resolved, lib.db_id);
        Ok(())
    }

    #[test]
    fn find_accessible_node_id_by_id_rejects_non_library_node() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        crate::db::indexes::ensure_index(&mut db, "id")?;
        let foreign_public_id = nanoid!();
        db.exec_mut(
            agdb::QueryBuilder::insert()
                .nodes()
                .values([[("id", foreign_public_id.as_str()).into()]])
                .query(),
        )?;
        let principal = placeholder_principal();

        let resolved = find_accessible_node_id_by_id(&db, &principal, &foreign_public_id)?;
        assert!(resolved.is_none());
        Ok(())
    }

    #[test]
    fn find_accessible_node_id_by_id_returns_none_for_missing_public_id() -> anyhow::Result<()> {
        let db = new_test_db()?;
        let principal = placeholder_principal();

        let resolved = find_accessible_node_id_by_id(&db, &principal, "no-such-library-public-id")?;
        assert!(resolved.is_none());
        Ok(())
    }
}
