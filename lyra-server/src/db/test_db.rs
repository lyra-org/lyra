// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    panic::Location,
    path::Path,
    sync::atomic::{
        AtomicU64,
        Ordering,
    },
    time::{
        SystemTime,
        UNIX_EPOCH,
    },
};

use agdb::{
    DbAny,
    DbId,
    QueryBuilder,
};
use nanoid::nanoid;

use crate::config::DbKind;

static NEXT_TEST_DB_ID: AtomicU64 = AtomicU64::new(0);

pub(crate) struct TestDb {
    db: DbAny,
}

impl TestDb {
    #[track_caller]
    pub(crate) fn new() -> anyhow::Result<Self> {
        let db_name = db_name();
        let db = super::bootstrap::open(DbKind::Memory, db_name.as_str())?;

        Ok(Self { db })
    }

    #[track_caller]
    pub(crate) fn initialized() -> anyhow::Result<Self> {
        let mut db = Self::new()?;
        super::bootstrap::initialize(&mut db.db)?;
        Ok(db)
    }

    #[track_caller]
    pub(crate) fn with_root_aliases(aliases: &[&str]) -> anyhow::Result<Self> {
        let mut db = Self::new()?;
        super::bootstrap::initialize_root_aliases(&mut db.db, aliases)?;
        Ok(db)
    }

    pub(crate) fn into_inner(self) -> DbAny {
        self.db
    }
}

pub(crate) fn new_test_db() -> anyhow::Result<DbAny> {
    Ok(TestDb::initialized()?.into_inner())
}

pub(crate) fn insert_track(db: &mut DbAny, title: &str) -> anyhow::Result<DbId> {
    let track = super::tracks::Track {
        db_id: None,
        id: nanoid!(),
        track_title: title.to_string(),
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
    let track_id = db
        .exec_mut(QueryBuilder::insert().element(&track).query())?
        .ids()[0];
    db.exec_mut(
        QueryBuilder::insert()
            .edges()
            .from("tracks")
            .to(track_id)
            .query(),
    )?;
    Ok(track_id)
}

pub(crate) fn insert_release(db: &mut DbAny, title: &str) -> anyhow::Result<DbId> {
    let release = super::releases::Release {
        db_id: None,
        id: nanoid!(),
        release_title: title.to_string(),
        sort_title: None,
        release_type: None,
        release_date: None,
        locked: None,
        created_at: None,
        ctime: None,
    };
    let release_id = db
        .exec_mut(QueryBuilder::insert().element(&release).query())?
        .ids()[0];
    db.exec_mut(
        QueryBuilder::insert()
            .edges()
            .from("releases")
            .to(release_id)
            .query(),
    )?;
    Ok(release_id)
}

pub(crate) fn insert_artist(db: &mut DbAny, name: &str) -> anyhow::Result<DbId> {
    let artist = super::artists::Artist {
        db_id: None,
        id: nanoid!(),
        artist_name: name.to_string(),
        scan_name: name.to_lowercase(),
        sort_name: None,
        artist_type: None,
        description: None,
        verified: false,
        locked: None,
        created_at: None,
    };
    let artist_id = db
        .exec_mut(QueryBuilder::insert().element(&artist).query())?
        .ids()[0];
    db.exec_mut(
        QueryBuilder::insert()
            .edges()
            .from("artists")
            .to(artist_id)
            .query(),
    )?;
    Ok(artist_id)
}

pub(crate) fn insert_library(db: &mut DbAny, name: &str, directory: &str) -> anyhow::Result<DbId> {
    let library = build_test_library(name, std::path::PathBuf::from(directory))?;
    let library_id = db
        .exec_mut(QueryBuilder::insert().element(&library).query())?
        .ids()[0];
    db.exec_mut(
        QueryBuilder::insert()
            .edges()
            .from("libraries")
            .to(library_id)
            .query(),
    )?;
    Ok(library_id)
}

/// Library node without the `from("libraries")` edge — for ingestion tests
/// that need a graph entity unreachable from the root alias.
pub(crate) fn insert_test_library_node(
    db: &mut DbAny,
    name: &str,
    directory: std::path::PathBuf,
) -> anyhow::Result<super::libraries::Library> {
    let mut library = build_test_library(name, directory)?;
    let qr = db.exec_mut(QueryBuilder::insert().element(&library).query())?;
    library.db_id = Some(qr.elements[0].id);
    Ok(library)
}

fn build_test_library(
    name: &str,
    directory: std::path::PathBuf,
) -> anyhow::Result<super::libraries::Library> {
    let (display, key) = super::libraries::normalize_library_name(name)?;
    let directory_key = super::libraries::directory_key_for(&directory);
    Ok(super::libraries::Library {
        db_id: None,
        id: nanoid!(),
        name: display,
        name_key: key,
        directory,
        directory_key,
        language: None,
        country: None,
    })
}

pub(crate) fn connect(db: &mut DbAny, from: DbId, to: DbId) -> anyhow::Result<()> {
    db.exec_mut(QueryBuilder::insert().edges().from(from).to(to).query())?;
    Ok(())
}

pub(crate) fn connect_artist(db: &mut DbAny, owner: DbId, artist: DbId) -> anyhow::Result<()> {
    connect_artist_with_order(db, owner, artist, 0)
}

pub(crate) fn connect_artist_with_order(
    db: &mut DbAny,
    owner: DbId,
    artist: DbId,
    order: u64,
) -> anyhow::Result<()> {
    let credit = super::credits::Credit {
        db_id: None,
        id: nanoid!(),
        credit_type: super::credits::CreditType::Artist,
        detail: None,
    };
    let credit_id = db
        .exec_mut(QueryBuilder::insert().element(&credit).query())?
        .ids()[0];
    db.exec_mut(
        QueryBuilder::insert()
            .edges()
            .from("credits")
            .to(credit_id)
            .query(),
    )?;
    db.exec_mut(
        QueryBuilder::insert()
            .edges()
            .from(owner)
            .to(credit_id)
            .values_uniform([
                ("owned", 1).into(),
                (super::credits::EDGE_ORDER_KEY, order).into(),
            ])
            .query(),
    )?;
    db.exec_mut(
        QueryBuilder::insert()
            .edges()
            .from(credit_id)
            .to(artist)
            .query(),
    )?;
    Ok(())
}

#[track_caller]
fn db_name() -> String {
    let caller = Location::caller();
    let caller_label = Path::new(caller.file())
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or("test")
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect::<String>();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock drift")
        .as_nanos();
    let unique_id = NEXT_TEST_DB_ID.fetch_add(1, Ordering::Relaxed);

    std::env::temp_dir()
        .join(format!(
            "lyra-test-db-{caller_label}-{}-{}-{}-{unique_id}-{nanos}.agdb",
            caller.line(),
            caller.column(),
            std::process::id(),
        ))
        .to_string_lossy()
        .into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_test_db_initializes_root_aliases() -> anyhow::Result<()> {
        let db = new_test_db()?;
        let result = db.exec(QueryBuilder::select().ids("tracks").query())?;

        assert_eq!(result.ids().len(), 1);
        Ok(())
    }

    #[test]
    fn db_name_is_unique_per_call() {
        assert_ne!(db_name(), db_name());
    }
}
