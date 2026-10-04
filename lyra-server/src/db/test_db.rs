// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::HashSet,
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

pub(crate) use super::fixtures::{
    connect,
    connect_credit,
    insert_artist,
    insert_release,
    insert_track,
    test_user,
};
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

pub(crate) fn insert_user(db: &mut DbAny, username: &str) -> anyhow::Result<DbId> {
    super::users::create(db, &test_user(username)?)
}

/// Deletes a user the way the users route does and creates `replacement`, asserting agdb hands
/// the replacement the deleted user's `DbId`.
pub(crate) fn recycle_user(
    db: &mut DbAny,
    user_db_id: DbId,
    replacement: &str,
) -> anyhow::Result<DbId> {
    db.transaction_mut(|t| -> anyhow::Result<()> {
        super::api_keys::delete_all_for_user(t, user_db_id)?;
        super::users::delete_user(t, user_db_id)?;
        Ok(())
    })?;
    let replacement_db_id = insert_user(db, replacement)?;
    anyhow::ensure!(
        replacement_db_id == user_db_id,
        "expected agdb to reuse DbId {user_db_id:?}, got {replacement_db_id:?}"
    );
    Ok(replacement_db_id)
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

pub(crate) fn test_session(token_hash: &str) -> super::users::Session {
    let now = super::users::now_secs();
    super::users::Session {
        db_id: None,
        id: nanoid!(),
        token_hash: token_hash.to_string(),
        expires_at: 0,
        created_at: now,
        last_seen_at: now,
        user_agent: None,
        client_name: None,
    }
}

/// Library node without the `from("libraries")` edge — for ingestion tests
/// that need a graph entity unreachable from the root alias.
pub(crate) fn insert_test_library_node(
    db: &mut DbAny,
    name: &str,
    path: std::path::PathBuf,
) -> anyhow::Result<super::libraries::Library> {
    let mut library = build_test_library(name, path)?;
    let qr = db.exec_mut(QueryBuilder::insert().element(&library).query())?;
    library.db_id = Some(qr.elements[0].id);
    Ok(library)
}

fn build_test_library(
    name: &str,
    path: std::path::PathBuf,
) -> anyhow::Result<super::libraries::Library> {
    let (display, key) = super::libraries::normalize_library_name(name)?;
    let path_key = super::libraries::path_key_for(&path);
    Ok(super::libraries::Library {
        db_id: None,
        id: nanoid!(),
        name: display,
        name_key: key,
        path,
        path_key,
        language: None,
        country: None,
    })
}

pub(crate) fn connect_artist(db: &mut DbAny, owner: DbId, artist: DbId) -> anyhow::Result<()> {
    connect_credit(db, owner, artist, super::CreditType::Artist, None, 0)
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

/// Key names actually stored on `db_id`, for asserting that an `update` removed
/// the keys whose struct field is `None`.
pub(crate) fn stored_keys(db: &DbAny, db_id: DbId) -> anyhow::Result<HashSet<String>> {
    Ok(db
        .exec(QueryBuilder::select().keys().ids(db_id).query())?
        .elements
        .into_iter()
        .flat_map(|element| element.values)
        .map(|kv| kv.key.string().cloned())
        .collect::<Result<HashSet<String>, _>>()?)
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
