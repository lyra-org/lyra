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

/// `path` is raw user input; `path_key` and `name_key` are canonical forms
/// stored to keep Unicode/canonicalize work off the write lock.
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

/// Strip `Cf` (zero-widths, bidi, SHY, BOM) plus CGJ — CGJ blocks NFC and lets
/// `"Music\u{034F}"` duplicate `"Music"`. Mirrors [`crate::db::tags`].
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

/// Strip-before-trim — invisibles aren't `White_Space`. Variation selectors
/// preserved for emoji; ZWSP/ZWJ dropped as copy-paste noise.
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

/// Second NFC pass: `to_lowercase` can decompose canonical forms.
fn lowercase_nfc(s: &str) -> String {
    s.to_lowercase().nfc().collect()
}

/// Comparison key only; matches `db::labels`. Prefer [`normalize_library_name`]
/// when both display and key are needed.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn normalize_library_name_key(raw: &str) -> Result<String, LibraryNameError> {
    Ok(lowercase_nfc(&normalize_library_name_display(raw)?))
}

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

pub(crate) fn path_key_for(path: &Path) -> String {
    normalize_library_path(path).to_string_lossy().into_owned()
}

pub(crate) fn find_by_name_key(
    db: &impl super::DbAccess,
    name_key: &str,
) -> anyhow::Result<Option<Library>> {
    find_indexed_library(db, "name_key", name_key)
}

/// Compute `key` via [`path_key_for`] off the lock — `canonicalize` is a syscall.
pub(crate) fn find_by_path_key(
    db: &impl super::DbAccess,
    key: &str,
) -> anyhow::Result<Option<Library>> {
    find_indexed_library(db, "path_key", key)
}

// `index_name` must be in [`crate::db::bootstrap::CORE_INDEXES`]; alias filter
// guards against key collisions on non-Library nodes.
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

/// All None branches do equal agdb work; see `mod benches` for parity. Admins
/// bypass via the route gate, never reach this.
#[allow(dead_code)]
pub(crate) fn find_accessible_node_id_by_id(
    db: &impl super::DbAccess,
    principal: &crate::services::auth::Principal,
    public_id: &str,
) -> anyhow::Result<Option<DbId>> {
    // Padding queries are unconditional — gating on `found` would leak.
    let make_query = || {
        QueryBuilder::select()
            .values(vec![DbValue::from("db_element_id"), DbValue::from("id")])
            .search()
            .from(principal.user_db_id)
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

impl From<agdb::DbError> for LibraryCreateError {
    fn from(e: agdb::DbError) -> Self {
        Self::Db(anyhow::Error::new(e))
    }
}

pub(crate) struct LibraryInsert {
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) path: PathBuf,
    /// Compute via [`path_key_for`] off the lock — `canonicalize` is a syscall.
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

/// Re-derives `name_key`; excludes self from the uniqueness check. No
/// directory edits.
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
        let Some(user_db_id) = edge.from else {
            continue;
        };
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

    // Random path so canonicalize() falls through to the lexical fallback.
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
        // Guards `_key` and `_display` against drift.
        let display = normalize_library_name_display("  Café\u{200B}  ").unwrap();
        let expected: String = display.to_lowercase().nfc().collect();
        let key = normalize_library_name_key("  Café\u{200B}  ").unwrap();
        assert_eq!(expected, key);
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
        super::super::users::create(db, &super::super::users::test_user(username)?)
    }

    fn principal_for(user_db_id: DbId, user_public_id: &str) -> crate::services::auth::Principal {
        crate::services::auth::Principal {
            user_db_id,
            user_public_id: user_public_id.to_string(),
            username: format!("user-{}", user_db_id.0),
            permissions: vec![],
            role_name: None,
            accessible_library_ids: HashSet::new(),
        }
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
    fn find_accessible_node_id_by_id_resolves_for_principal_with_edge() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = create_test_user(&mut db, "alice")?;
        let user = super::super::users::get_by_id(&db, user_db_id)?.expect("user exists");
        let lib = db.transaction_mut(|t| -> anyhow::Result<Library> {
            Ok(create_with_creator(
                t,
                insert_request("Music", "fa-yes"),
                user_db_id,
            )?)
        })?;
        let principal = principal_for(user_db_id, &user.id);

        let resolved = find_accessible_node_id_by_id(&db, &principal, &lib.id)?;
        assert_eq!(resolved, lib.db_id);
        Ok(())
    }

    #[test]
    fn find_accessible_node_id_by_id_rejects_inaccessible_library() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let creator_db_id = create_test_user(&mut db, "creator")?;
        let viewer_db_id = create_test_user(&mut db, "viewer")?;
        let viewer = super::super::users::get_by_id(&db, viewer_db_id)?.expect("user exists");
        let lib = db.transaction_mut(|t| -> anyhow::Result<Library> {
            Ok(create_with_creator(
                t,
                insert_request("Music", "fa-no"),
                creator_db_id,
            )?)
        })?;
        let principal = principal_for(viewer_db_id, &viewer.id);

        let resolved = find_accessible_node_id_by_id(&db, &principal, &lib.id)?;
        assert!(resolved.is_none());
        Ok(())
    }

    #[test]
    fn find_accessible_node_id_by_id_returns_none_for_missing_public_id() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = create_test_user(&mut db, "alice")?;
        let user = super::super::users::get_by_id(&db, user_db_id)?.expect("user exists");
        let principal = principal_for(user_db_id, &user.id);

        let resolved = find_accessible_node_id_by_id(&db, &principal, "no-such-library-public-id")?;
        assert!(resolved.is_none());
        Ok(())
    }

    #[test]
    fn find_accessible_node_id_by_id_rejects_non_library_node() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = create_test_user(&mut db, "alice")?;
        let user = super::super::users::get_by_id(&db, user_db_id)?.expect("user exists");
        let foreign_public_id = nanoid!();
        db.exec_mut(
            QueryBuilder::insert()
                .nodes()
                .values([[("id", foreign_public_id.as_str()).into()]])
                .query(),
        )?;
        let principal = principal_for(user_db_id, &user.id);

        let resolved = find_accessible_node_id_by_id(&db, &principal, &foreign_public_id)?;
        assert!(resolved.is_none());
        Ok(())
    }
}

#[cfg(test)]
mod benches {
    extern crate test;

    use test::Bencher;

    use super::*;
    use crate::db::test_db::new_test_db;
    use nanoid::nanoid;

    /// 2 owned libs + 1 inaccessible (other user) + 1 foreign-typed node.
    struct ParitySetup {
        db: agdb::DbAny,
        principal: crate::services::auth::Principal,
        inaccessible_id: String,
        missing_id: String,
        foreign_id: String,
    }

    fn parity_setup() -> ParitySetup {
        let mut db = new_test_db().unwrap();
        let user_db_id =
            super::super::users::create(&mut db, &super::super::users::test_user("alice").unwrap())
                .unwrap();
        let user = super::super::users::get_by_id(&db, user_db_id)
            .unwrap()
            .expect("user exists");
        for i in 0..2 {
            db.transaction_mut(|t| -> anyhow::Result<()> {
                let path =
                    std::path::PathBuf::from(format!("/tmp/lyra-bench-own-{i}-{}", nanoid!()));
                let path_key = path_key_for(&path);
                create_with_creator(
                    t,
                    LibraryInsert {
                        id: nanoid!(),
                        name: format!("Owned-{i}"),
                        path,
                        path_key,
                        language: None,
                        country: None,
                    },
                    user_db_id,
                )?;
                Ok(())
            })
            .unwrap();
        }
        let other_user =
            super::super::users::create(&mut db, &super::super::users::test_user("other").unwrap())
                .unwrap();
        let inaccessible = db
            .transaction_mut(|t| -> anyhow::Result<Library> {
                let path = std::path::PathBuf::from(format!("/tmp/lyra-bench-inacc-{}", nanoid!()));
                let path_key = path_key_for(&path);
                Ok(create_with_creator(
                    t,
                    LibraryInsert {
                        id: nanoid!(),
                        name: "Hidden".to_string(),
                        path,
                        path_key,
                        language: None,
                        country: None,
                    },
                    other_user,
                )?)
            })
            .unwrap();
        let foreign_public_id = nanoid!();
        db.exec_mut(
            QueryBuilder::insert()
                .nodes()
                .values([[("id", foreign_public_id.as_str()).into()]])
                .query(),
        )
        .unwrap();
        let principal = crate::services::auth::Principal {
            user_db_id,
            user_public_id: user.id.clone(),
            username: format!("user-{}", user_db_id.0),
            permissions: vec![],
            role_name: None,
            accessible_library_ids: HashSet::new(),
        };
        ParitySetup {
            db,
            principal,
            inaccessible_id: inaccessible.id,
            missing_id: "no-such-library-public-id".to_string(),
            foreign_id: foreign_public_id,
        }
    }

    #[bench]
    fn bench_find_accessible_node_id_by_id_inaccessible(b: &mut Bencher) {
        let s = parity_setup();
        b.iter(|| find_accessible_node_id_by_id(&s.db, &s.principal, &s.inaccessible_id).unwrap());
    }

    #[bench]
    fn bench_find_accessible_node_id_by_id_missing(b: &mut Bencher) {
        let s = parity_setup();
        b.iter(|| find_accessible_node_id_by_id(&s.db, &s.principal, &s.missing_id).unwrap());
    }

    #[bench]
    fn bench_find_accessible_node_id_by_id_foreign(b: &mut Bencher) {
        let s = parity_setup();
        b.iter(|| find_accessible_node_id_by_id(&s.db, &s.principal, &s.foreign_id).unwrap());
    }
}
