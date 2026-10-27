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
    DbAny,
    DbElement,
    DbError,
    DbId,
    DbTypeMarker,
    DbValue,
    QueryBuilder,
    QueryId,
};

use crate::db::Library;

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

pub fn get_by_id(db: &impl super::DbAccess, entry_db_id: DbId) -> anyhow::Result<Option<Entry>> {
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

#[derive(Debug, Default)]
pub(crate) struct EntrySyncResult {
    pub(crate) altered: Vec<DbId>,
    pub(crate) added: usize,
    pub(crate) updated: usize,
    pub(crate) deleted: usize,
}

pub(crate) fn sync_entry_group(
    db: &mut DbAny,
    library: &Library,
    entries: Vec<Entry>,
) -> anyhow::Result<EntrySyncResult> {
    let library_db_id = library
        .db_id
        .ok_or_else(|| anyhow::anyhow!("library missing db_id"))?;
    let existing = load_existing(db, library_db_id)?;
    let mut db_by_path = HashMap::new();
    for entry in existing {
        db_by_path.insert(entry.full_path.clone(), entry);
    }

    let mut to_add = Vec::new();
    let mut to_update = Vec::new();
    for entry in entries {
        if entry.full_path == library.path {
            continue;
        }

        let Some(existing) = db_by_path.get(&entry.full_path) else {
            to_add.push(entry);
            continue;
        };

        if entry.kind != existing.kind
            || entry.file_kind != existing.file_kind
            || entry.name != existing.name
            || entry.size != existing.size
            || entry.mtime != existing.mtime
            || entry.hash != existing.hash
        {
            let mut updated = entry;
            updated.db_id = existing.db_id;
            updated.id = existing.id.clone();
            to_update.push(updated);
        }
    }

    let added_count = to_add.len();
    let updated_count = to_update.len();

    db.transaction_mut(|t| -> anyhow::Result<EntrySyncResult> {
        let mut altered = Vec::new();

        if !to_update.is_empty() {
            t.exec_mut(QueryBuilder::insert().elements(&to_update).query())?;
            altered.extend(to_update.iter().filter_map(|entry| entry.db_id));
        }

        if !to_add.is_empty() {
            let qr_ids = t.exec_mut(QueryBuilder::insert().elements(&to_add).query())?;
            let inserted: Vec<Entry> = t
                .exec(
                    QueryBuilder::select()
                        .elements::<Entry>()
                        .ids(&qr_ids)
                        .query(),
                )?
                .try_into()?;
            let mut path_to_id = HashMap::new();
            for entry in &inserted {
                let entry_id = entry
                    .db_id
                    .ok_or_else(|| anyhow::anyhow!("newly inserted entry missing db_id"))?;
                path_to_id.insert(entry.full_path.clone(), entry_id);
            }
            for entry in &inserted {
                let entry_id = entry
                    .db_id
                    .ok_or_else(|| anyhow::anyhow!("entry missing db_id during edge insert"))?;
                altered.push(entry_id);

                let parent = entry
                    .full_path
                    .parent()
                    .map(|p| p.to_string_lossy().into_owned())
                    .unwrap_or_default();
                let parent_id = path_to_id
                    .get(&PathBuf::from(&parent))
                    .copied()
                    .or_else(|| {
                        db_by_path
                            .get(&PathBuf::from(&parent))
                            .and_then(|entry| entry.db_id)
                    })
                    .unwrap_or(library_db_id);

                t.exec_mut(
                    QueryBuilder::insert()
                        .edges()
                        .from(parent_id)
                        .to(entry_id)
                        .values_uniform([("owned", 1).into()])
                        .query(),
                )?;
            }
        }

        Ok(EntrySyncResult {
            altered,
            added: added_count,
            updated: updated_count,
            deleted: 0,
        })
    })
}

pub(crate) fn prune_missing_entries(
    db: &mut DbAny,
    library: &Library,
    observed_paths: &HashSet<PathBuf>,
) -> anyhow::Result<EntrySyncResult> {
    let library_db_id = library
        .db_id
        .ok_or_else(|| anyhow::anyhow!("library missing db_id"))?;
    let existing = load_existing(db, library_db_id)?;
    let to_delete = existing
        .into_iter()
        .filter(|entry| !observed_paths.contains(&entry.full_path))
        .collect::<Vec<_>>();

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

    let deleted_count = to_delete.len();
    db.transaction_mut(|t| -> anyhow::Result<EntrySyncResult> {
        for entry in &top_level_dirs {
            let dir_id = entry
                .db_id
                .ok_or_else(|| anyhow::anyhow!("directory entry missing db_id"))?;
            t.exec_mut(QueryBuilder::remove().search().from(dir_id).query())?;
        }

        let ids = to_delete
            .iter()
            .map(|entry| {
                entry
                    .db_id
                    .ok_or_else(|| anyhow::anyhow!("entry missing db_id during delete"))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        if !ids.is_empty() {
            t.exec_mut(QueryBuilder::remove().ids(&ids).query())?;
        }

        Ok(EntrySyncResult {
            altered: ids,
            added: 0,
            updated: 0,
            deleted: deleted_count,
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::test_db::TestDb;
    use agdb::DbValue;
    use nanoid::nanoid;

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
            track_gain_db: None,
            album_gain_db: None,
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
            track_gain_db: None,
            album_gain_db: None,
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
