// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::HashMap,
    fmt,
    path::{
        Path,
        PathBuf,
    },
};

use agdb::{
    DbAny,
    DbElement,
    DbError,
    DbId,
    DbTypeMarker,
    DbValue,
    QueryBuilder,
    QueryId,
};

use crate::Library;

#[derive(Clone, Copy, Debug, PartialEq, Eq, DbTypeMarker)]
pub(crate) enum EntryKind {
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
            _ => Err(DbError::from(format!("invalid EntryKind value '{value}'"))),
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
pub(crate) struct Entry {
    pub(crate) db_id: Option<DbId>,
    pub(crate) id: String,
    pub(crate) full_path: PathBuf,
    pub(crate) kind: EntryKind,
    pub(crate) file_kind: Option<String>,
    pub(crate) name: String,
    pub(crate) hash: Option<String>,
    pub(crate) size: u64,
    pub(crate) mtime: u64,
    pub(crate) ctime: u64,
}

pub(crate) const SUPPORTED_AUDIO_EXTENSIONS: &[&str] = lofty::file::EXTENSIONS;
pub(crate) const CUE_EXTENSION: &str = "cue";

pub(crate) fn classify_file_kind(path: &Path) -> Option<&'static str> {
    let ext = path.extension()?.to_str()?.to_ascii_lowercase();
    if ext == CUE_EXTENSION {
        return Some("cue");
    }
    if SUPPORTED_AUDIO_EXTENSIONS.contains(&ext.as_str()) {
        return Some("audio");
    }
    None
}

pub(crate) fn get(db: &DbAny, from: impl Into<QueryId>) -> anyhow::Result<Vec<Entry>> {
    let entries: Vec<Entry> = db
        .exec(
            QueryBuilder::select()
                .elements::<Entry>()
                .search()
                .from(from)
                .query(),
        )?
        .try_into()?;

    Ok(entries)
}

pub(crate) fn get_by_id(
    db: &impl super::DbAccess,
    entry_db_id: DbId,
) -> anyhow::Result<Option<Entry>> {
    super::graph::fetch_typed_by_id(db, entry_db_id, "Entry")
}

pub(crate) fn get_by_track(db: &DbAny, track_db_id: DbId) -> anyhow::Result<Vec<Entry>> {
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

pub(crate) fn load_existing(db: &DbAny, library_root: DbId) -> anyhow::Result<Vec<Entry>> {
    let qr = db.exec(
        QueryBuilder::select()
            .elements::<Entry>()
            .search()
            .from(library_root)
            .query(),
    )?;
    Ok(qr.try_into()?)
}

pub(crate) fn sync_entries(
    db: &mut DbAny,
    library: &Library,
    entries: Vec<Entry>,
) -> anyhow::Result<Vec<DbId>> {
    let library_db_id = library
        .db_id
        .ok_or_else(|| anyhow::anyhow!("library missing db_id"))?;
    let mut disk_by_path = HashMap::new();
    for e in &entries {
        disk_by_path.insert(e.full_path.clone(), e.clone());
    }

    let existing = load_existing(db, library_db_id)?;
    let mut db_by_path = HashMap::new();
    for e in existing.into_iter() {
        db_by_path.insert(e.full_path.clone(), e);
    }

    let mut to_delete = Vec::new();
    let mut to_add = Vec::new();

    // First collect all entries that need to be deleted
    for (path, e) in &db_by_path {
        if !disk_by_path.contains_key(path) {
            to_delete.push(e.clone());
        }
    }

    let mut dir_deletes: Vec<Entry> = to_delete
        .iter()
        .filter(|entry| entry.kind == EntryKind::Dir)
        .cloned()
        .collect();
    dir_deletes.sort_by_key(|entry| entry.full_path.components().count());
    let mut top_level_dirs = Vec::new();
    for entry in dir_deletes {
        if top_level_dirs
            .iter()
            .any(|parent: &Entry| entry.full_path.starts_with(&parent.full_path))
        {
            continue;
        }
        top_level_dirs.push(entry);
    }

    for (path, e) in &disk_by_path {
        // skip the root directory entry, as we store that as the library itself
        if path == &PathBuf::from(&library.directory) {
            continue;
        }

        if !db_by_path.contains_key(path) {
            to_add.push(e.clone());
        }
    }

    let mut to_update = Vec::new();
    for entry in &entries {
        let Some(existing) = db_by_path.get(&entry.full_path) else {
            continue;
        };
        if entry.kind != existing.kind
            || entry.file_kind != existing.file_kind
            || entry.name != existing.name
            || entry.size != existing.size
            || entry.mtime != existing.mtime
            || entry.hash != existing.hash
        {
            to_update.push(entry.clone());
        }
    }

    db.transaction_mut(|t| -> anyhow::Result<Vec<DbId>> {
        // HACK: get all the altered items to return for metadata
        let mut altered = Vec::new();

        for e in &top_level_dirs {
            let dir_id = e
                .db_id
                .ok_or_else(|| anyhow::anyhow!("directory entry missing db_id"))?;
            t.exec_mut(QueryBuilder::remove().search().from(dir_id).query())?;
        }

        if !to_delete.is_empty() {
            let ids: Vec<DbId> = to_delete
                .iter()
                .map(|e| {
                    e.db_id
                        .ok_or_else(|| anyhow::anyhow!("entry missing db_id during delete"))
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            t.exec_mut(QueryBuilder::remove().ids(&ids).query())?;

            // add all the ids to the altered list
            altered.extend(ids);
        }

        if !to_update.is_empty() {
            t.exec_mut(QueryBuilder::insert().elements(&to_update).query())?;
            altered.extend(to_update.iter().filter_map(|entry| entry.db_id));
        }

        let qr_ids = t.exec_mut(QueryBuilder::insert().elements(&to_add).query())?;
        let entries: Vec<Entry> = t
            .exec(
                QueryBuilder::select()
                    .elements::<Entry>()
                    .ids(&qr_ids)
                    .query(),
            )?
            .try_into()?;
        let mut path_to_id = HashMap::new();
        for e in &entries {
            let eid = e
                .db_id
                .ok_or_else(|| anyhow::anyhow!("newly inserted entry missing db_id"))?;
            path_to_id.insert(e.full_path.clone(), eid);
        }
        for e in &entries {
            let entry_id = e
                .db_id
                .ok_or_else(|| anyhow::anyhow!("entry missing db_id during edge insert"))?;
            altered.push(entry_id);

            let parent = e
                .full_path
                .parent()
                .map(|p| p.to_string_lossy().into_owned())
                .unwrap_or_default();
            let pid = path_to_id
                .get(&PathBuf::from(&parent))
                .copied()
                .or_else(|| {
                    db_by_path
                        .get(&PathBuf::from(&parent))
                        .and_then(|en| en.db_id)
                })
                .unwrap_or(library_db_id);

            t.exec_mut(
                QueryBuilder::insert()
                    .edges()
                    .from(pid)
                    .to(entry_id)
                    .values_uniform([("owned", 1).into()])
                    .query(),
            )?;
        }

        Ok(altered)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Library;
    use crate::db::test_db::TestDb;
    use crate::services::libraries::scanning::prepare_entries;
    use agdb::DbValue;
    use nanoid::nanoid;
    use std::collections::HashSet;
    use std::fs;
    use std::path::Path;
    use std::time::{
        SystemTime,
        UNIX_EPOCH,
    };

    fn temp_path(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock drift")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "lyra-items-{}-{}-{}",
            name,
            std::process::id(),
            nanos
        ))
    }

    fn new_db() -> anyhow::Result<DbAny> {
        Ok(TestDb::new()?.into_inner())
    }

    fn new_library(db: &mut DbAny, root: &Path) -> anyhow::Result<Library> {
        crate::db::test_db::insert_test_library_node(db, "Test", root.to_path_buf())
    }

    fn full_sync(db: &mut DbAny, library: &Library) -> anyhow::Result<Vec<DbId>> {
        let library_db_id = library
            .db_id
            .ok_or_else(|| anyhow::anyhow!("library missing db_id"))?;
        let existing = load_existing(db, library_db_id)?;
        let entries = prepare_entries(library, existing)?;
        sync_entries(db, library, entries)
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
            id: nanoid!(),
            full_path: PathBuf::from("/music/test.flac"),
            kind: EntryKind::File,
            file_kind: Some("audio".to_string()),
            name: "test.flac".to_string(),
            hash: None,
            size: 42,
            mtime: 1,
            ctime: 1,
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

    #[test]
    fn full_sync_removes_deleted_directory_subtree() -> anyhow::Result<()> {
        let root = temp_path("dir-delete");
        let album_dir = root.join("album");
        let disc_dir = album_dir.join("disc1");
        fs::create_dir_all(&disc_dir)?;
        fs::write(album_dir.join("track1.mp3"), b"track1")?;
        fs::write(disc_dir.join("track2.mp3"), b"track2")?;

        let mut db = new_db()?;
        let library = new_library(&mut db, &root)?;

        full_sync(&mut db, &library)?;
        let existing = load_existing(&db, library.db_id.unwrap())?;
        assert_eq!(existing.len(), 4);

        fs::remove_dir_all(&album_dir)?;
        full_sync(&mut db, &library)?;
        let remaining = load_existing(&db, library.db_id.unwrap())?;
        assert!(remaining.is_empty());

        let _ = fs::remove_dir_all(&root);
        Ok(())
    }

    #[test]
    fn full_sync_removes_only_missing_file() -> anyhow::Result<()> {
        let root = temp_path("file-delete");
        let album_dir = root.join("album");
        fs::create_dir_all(&album_dir)?;
        let track1 = album_dir.join("track1.mp3");
        let track2 = album_dir.join("track2.mp3");
        fs::write(&track1, b"track1")?;
        fs::write(&track2, b"track2")?;

        let mut db = new_db()?;
        let library = new_library(&mut db, &root)?;

        full_sync(&mut db, &library)?;
        fs::remove_file(&track1)?;
        full_sync(&mut db, &library)?;

        let remaining = load_existing(&db, library.db_id.unwrap())?;
        let paths: HashSet<PathBuf> = remaining
            .iter()
            .map(|entry| entry.full_path.clone())
            .collect();
        assert_eq!(remaining.len(), 2);
        assert!(paths.contains(&album_dir));
        assert!(paths.contains(&track2));
        assert!(remaining.iter().any(|entry| entry.kind == EntryKind::Dir));
        assert!(remaining.iter().any(|entry| entry.kind == EntryKind::File));

        let _ = fs::remove_dir_all(&root);
        Ok(())
    }

    #[test]
    fn full_sync_updates_modified_file() -> anyhow::Result<()> {
        let root = temp_path("file-update");
        let album_dir = root.join("album");
        fs::create_dir_all(&album_dir)?;
        let track = album_dir.join("track1.mp3");
        fs::write(&track, b"track1")?;

        let mut db = new_db()?;
        let library = new_library(&mut db, &root)?;

        full_sync(&mut db, &library)?;
        let existing = load_existing(&db, library.db_id.unwrap())?;
        let entry = existing
            .iter()
            .find(|entry| entry.full_path == track)
            .expect("entry exists");
        let entry_db_id = entry.db_id.unwrap();
        let old_hash = entry.hash.clone().unwrap();

        fs::write(&track, b"track1-updated")?;

        let altered = full_sync(&mut db, &library)?;
        let updated = load_existing(&db, library.db_id.unwrap())?;
        let updated_entry = updated
            .iter()
            .find(|entry| entry.full_path == track)
            .expect("entry exists");
        let new_hash = updated_entry.hash.clone().unwrap();

        assert_ne!(old_hash, new_hash);
        assert!(altered.contains(&entry_db_id));

        let _ = fs::remove_dir_all(&root);
        Ok(())
    }

    #[test]
    fn get_by_track_traverses_through_track_source() -> anyhow::Result<()> {
        let mut db = TestDb::initialized()?.into_inner();

        let track = crate::db::tracks::Track {
            db_id: None,
            id: nanoid!(),
            track_title: "Test Track".to_string(),
            sort_title: None,
            year: None,
            disc: None,
            disc_total: None,
            track: None,
            track_total: None,
            duration_ms: None,
            sample_rate_hz: None,
            channel_count: None,
            bit_depth: None,
            bitrate_bps: None,
            locked: None,
            created_at: None,
            ctime: None,
        };
        let track_db_id = db
            .exec_mut(QueryBuilder::insert().element(&track).query())?
            .ids()[0];
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from("tracks")
                .to(track_db_id)
                .query(),
        )?;

        let entry = Entry {
            db_id: None,
            id: nanoid!(),
            full_path: PathBuf::from("/music/test.mp3"),
            kind: EntryKind::File,
            file_kind: Some("audio".to_string()),
            name: "test.mp3".to_string(),
            hash: None,
            size: 1,
            mtime: 1,
            ctime: 1,
        };
        let entry_db_id = db
            .exec_mut(QueryBuilder::insert().element(&entry).query())?
            .ids()[0];

        let source = crate::db::track_sources::TrackSource {
            db_id: None,
            id: nanoid!(),
            source_kind: "embedded_tags".to_string(),
            source_key: "key1".to_string(),
            identity: "key1".to_string(),
            is_primary: true,
            start_ms: None,
            end_ms: None,
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

        let track = crate::db::tracks::Track {
            db_id: None,
            id: nanoid!(),
            track_title: "Orphan Track".to_string(),
            sort_title: None,
            year: None,
            disc: None,
            disc_total: None,
            track: None,
            track_total: None,
            duration_ms: None,
            sample_rate_hz: None,
            channel_count: None,
            bit_depth: None,
            bitrate_bps: None,
            locked: None,
            created_at: None,
            ctime: None,
        };
        let track_db_id = db
            .exec_mut(QueryBuilder::insert().element(&track).query())?
            .ids()[0];

        let entries = get_by_track(&db, track_db_id)?;
        assert!(entries.is_empty());

        Ok(())
    }
}
