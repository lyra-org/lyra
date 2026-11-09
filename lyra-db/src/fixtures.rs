// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Inserts catalog entities and users directly, for data that tests run against.

use super::roots::{
    self,
    Root,
};
use agdb::{
    DbAny,
    DbId,
    QueryBuilder,
};
use nanoid::nanoid;

pub fn insert_track(db: &mut DbAny, title: &str) -> anyhow::Result<DbId> {
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
        track_gain_db: None,
        album_gain_db: None,
        locked: None,
        created_at_ms: None,
        file_created_at_ms: None,
    };
    let track_id = db
        .exec_mut(QueryBuilder::insert().element(&track).query())?
        .ids()[0];
    roots::link(db, Root::Tracks, track_id)?;
    Ok(track_id)
}

pub fn insert_release(db: &mut DbAny, title: &str) -> anyhow::Result<DbId> {
    let release = super::releases::Release {
        db_id: None,
        id: nanoid!(),
        release_title: title.to_string(),
        sort_title: None,
        release_type: None,
        release_date: None,
        media_formats: None,
        barcode: None,
        description: None,
        locked: None,
        created_at_ms: None,
        file_created_at_ms: None,
    };
    let release_id = db
        .exec_mut(QueryBuilder::insert().element(&release).query())?
        .ids()[0];
    roots::link(db, Root::Releases, release_id)?;
    Ok(release_id)
}

pub fn insert_artist(db: &mut DbAny, name: &str) -> anyhow::Result<DbId> {
    let artist = super::artists::Artist {
        db_id: None,
        id: nanoid!(),
        artist_name: name.to_string(),
        scan_name: name.to_lowercase(),
        scan_aliases: Vec::new(),
        sort_name: None,
        artist_type: None,
        description: None,
        credited: false,
        locked: None,
        created_at_ms: None,
    };
    let artist_id = db
        .exec_mut(QueryBuilder::insert().element(&artist).query())?
        .ids()[0];
    roots::link(db, Root::Artists, artist_id)?;
    Ok(artist_id)
}

/// A whole-track listen of the track by the user, started at `started_at_ms`.
pub fn insert_listen(
    db: &mut DbAny,
    user_db_id: DbId,
    track_db_id: DbId,
    started_at_ms: u64,
) -> anyhow::Result<DbId> {
    let track_public_id = super::lookup::find_id_by_db_id(db, track_db_id)?
        .ok_or_else(|| anyhow::anyhow!("track {track_db_id:?} has no public id"))?;
    let listen = super::Listen {
        db_id: None,
        id: nanoid!(),
        play_id: nanoid!(),
        track_public_id,
        started_at_ms,
        played_ms: Some(180_000),
        created_at_ms: started_at_ms,
    };
    db.transaction_mut(|t| super::listens::insert(t, &listen, track_db_id, user_db_id))
}

/// A valid argon2 hash that no test password matches.
pub const UNUSABLE_PASSWORD_HASH: &str = "$argon2id$v=19$m=19456,t=2,p=1$bHlyYWZpeHR1cmVzYWx0MDE$vcPx0NcnfcSiooHKqtJm/SsMVaSrfBboPZLCEwgKyaA";

pub fn test_user(username: &str) -> anyhow::Result<super::users::User> {
    Ok(super::users::User {
        db_id: None,
        id: nanoid!(),
        username: username.to_string(),
        password: UNUSABLE_PASSWORD_HASH.to_string(),
    })
}

pub fn connect(db: &mut DbAny, from: DbId, to: DbId) -> anyhow::Result<()> {
    db.exec_mut(QueryBuilder::insert().edges().from(from).to(to).query())?;
    Ok(())
}

/// An audio file entry named `name`, held by `parent`, a library or directory entry.
pub fn insert_file_entry(db: &mut DbAny, parent: DbId, name: &str) -> anyhow::Result<DbId> {
    let entry = super::entries::Entry {
        db_id: None,
        id: nanoid!(),
        full_path: format!("/music/{name}").into(),
        kind: super::entries::EntryKind::File,
        file_kind: Some("audio".to_string()),
        name: name.to_string(),
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
    connect(db, parent, entry_db_id)?;
    Ok(entry_db_id)
}

#[cfg(any(test, feature = "testing"))]
pub fn connect_credit(
    db: &mut DbAny,
    owner: DbId,
    artist: DbId,
    credit_type: super::CreditType,
    detail: Option<&str>,
    artist_order: u64,
) -> anyhow::Result<()> {
    super::credits::link(
        db,
        owner,
        artist,
        &super::Credit {
            credit_type,
            detail: detail.map(str::to_string),
            artist_order,
        },
    )?;
    Ok(())
}
