// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Inserts catalog entities and users directly, for data that tests run against.

use agdb::{
    DbAny,
    DbId,
    QueryBuilder,
};
use argon2::{
    Argon2,
    password_hash::{
        PasswordHasher,
        SaltString,
        rand_core::{
            OsRng,
            RngCore,
        },
    },
};
use nanoid::nanoid;

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
        track_gain_db: None,
        album_gain_db: None,
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
        media_formats: None,
        barcode: None,
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

pub(crate) fn test_user(username: &str) -> anyhow::Result<super::users::User> {
    Ok(super::users::User {
        db_id: None,
        id: nanoid!(),
        username: username.to_string(),
        password: hash_random_secret()?,
    })
}

pub(crate) fn connect(db: &mut DbAny, from: DbId, to: DbId) -> anyhow::Result<()> {
    db.exec_mut(QueryBuilder::insert().edges().from(from).to(to).query())?;
    Ok(())
}

pub(crate) fn connect_credit(
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

fn hash_random_secret() -> anyhow::Result<String> {
    let mut secret = [0_u8; 32];
    OsRng.fill_bytes(&mut secret);
    let salt = SaltString::generate(&mut OsRng);
    let argon2 = Argon2::default();
    let hash = argon2.hash_password(&secret, &salt)?.to_string();
    Ok(hash)
}
