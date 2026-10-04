// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Data a Luau test runs against, which the test runner reads from a `<test>.fixture.toml`.

use std::collections::BTreeMap;

use agdb::{
    DbAny,
    DbId,
    QueryId,
};
use anyhow::Context;
use serde::{
    Deserialize,
    Serialize,
};

use crate::db::{
    self,
    fixtures,
};
use crate::services;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Fixture {
    /// The fixture user the test runs as.
    run_as: String,
    #[serde(default)]
    users: Vec<User>,
    #[serde(default)]
    artists: Vec<Artist>,
    #[serde(default)]
    genres: Vec<Genre>,
    #[serde(default)]
    releases: Vec<Release>,
    #[serde(default)]
    tracks: Vec<Track>,
    #[serde(default)]
    playlists: Vec<Playlist>,
    #[serde(default)]
    listens: Vec<Listen>,
    #[serde(default)]
    favorites: Vec<Favorite>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct User {
    key: String,
    #[serde(default)]
    admin: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Artist {
    key: String,
    name: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Genre {
    key: String,
    name: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Release {
    key: String,
    title: String,
    added: Option<u64>,
    #[serde(default)]
    artists: Vec<String>,
    #[serde(default)]
    genres: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Track {
    key: String,
    title: String,
    release: Option<String>,
    disc: Option<u32>,
    track: Option<u32>,
    year: Option<u32>,
    added: Option<u64>,
    #[serde(default)]
    artists: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Playlist {
    key: String,
    owner: String,
    name: String,
    #[serde(default)]
    public: bool,
    #[serde(default)]
    tracks: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Listen {
    user: String,
    track: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Favorite {
    user: String,
    target: String,
}

/// What the test receives as its chunk's `...`: user ids and entity ids by fixture key, and each
/// playlist's entry ids in order.
#[derive(Debug, Default, Serialize)]
pub(crate) struct Seeded {
    users: BTreeMap<String, String>,
    ids: BTreeMap<String, i64>,
    entries: BTreeMap<String, Vec<String>>,
}

struct Keys {
    users: BTreeMap<String, (DbId, String)>,
    entities: BTreeMap<String, (DbId, db::favorites::FavoriteKind)>,
    genres: BTreeMap<String, String>,
}

impl Keys {
    fn user(&self, key: &str) -> anyhow::Result<DbId> {
        self.users
            .get(key)
            .map(|(db_id, _)| *db_id)
            .with_context(|| format!("fixture has no user `{key}`"))
    }

    fn entity(&self, key: &str) -> anyhow::Result<(DbId, db::favorites::FavoriteKind)> {
        self.entities
            .get(key)
            .copied()
            .with_context(|| format!("fixture has no artist, release, track or playlist `{key}`"))
    }

    fn claim(&self, key: &str) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.users.contains_key(key)
                && !self.entities.contains_key(key)
                && !self.genres.contains_key(key),
            "fixture key `{key}` is used twice"
        );
        Ok(())
    }
}

impl Fixture {
    /// Writes the fixture into `db`, returning what the test receives and the user it runs as.
    pub(crate) fn seed(&self, db: &mut DbAny) -> anyhow::Result<(Seeded, DbId)> {
        let mut keys = Keys {
            users: BTreeMap::new(),
            entities: BTreeMap::new(),
            genres: BTreeMap::new(),
        };
        let mut seeded = Seeded::default();

        if self.users.iter().any(|user| user.admin) {
            db::roles::ensure_builtin_roles(db)?;
        }
        for user in &self.users {
            keys.claim(&user.key)?;
            let record = fixtures::test_user(&user.key)?;
            let public_id = record.id.clone();
            let user_db_id = db::users::create(db, &record)?;
            if user.admin {
                let role = db::roles::get_by_name(db, db::roles::BUILTIN_ADMIN_ROLE)?
                    .and_then(|role| role.db_id)
                    .context("built-in admin role exists")?;
                db::roles::assign_role_to_user(db, user_db_id, role)?;
            }
            seeded.users.insert(user.key.clone(), public_id.clone());
            keys.users.insert(user.key.clone(), (user_db_id, public_id));
        }

        for genre in &self.genres {
            keys.claim(&genre.key)?;
            keys.genres.insert(genre.key.clone(), genre.name.clone());
        }

        for artist in &self.artists {
            keys.claim(&artist.key)?;
            let artist_db_id = fixtures::insert_artist(db, &artist.name)?;
            keys.entities.insert(
                artist.key.clone(),
                (artist_db_id, db::favorites::FavoriteKind::Artist),
            );
        }

        for release in &self.releases {
            keys.claim(&release.key)?;
            let release_db_id = fixtures::insert_release(db, &release.title)?;
            if release.added.is_some() {
                let mut stored =
                    db::releases::get_by_id(db, release_db_id)?.context("release exists")?;
                stored.ctime = release.added;
                db.transaction_mut(|t| db::releases::update_in_transaction(t, &stored))?;
            }
            credit(db, &keys, release_db_id, &release.artists)?;
            let genre_names = release
                .genres
                .iter()
                .map(|key| {
                    keys.genres
                        .get(key)
                        .cloned()
                        .with_context(|| format!("fixture has no genre `{key}`"))
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            if !genre_names.is_empty() {
                db::genres::sync_release_genres(db, release_db_id, &genre_names)?;
            }
            keys.entities.insert(
                release.key.clone(),
                (release_db_id, db::favorites::FavoriteKind::Release),
            );
        }

        for track in &self.tracks {
            keys.claim(&track.key)?;
            let track_db_id = fixtures::insert_track(db, &track.title)?;
            let mut stored = db::tracks::get_by_id(db, track_db_id)?.context("track exists")?;
            stored.disc = track.disc;
            stored.track = track.track;
            stored.year = track.year;
            stored.ctime = track.added;
            db.transaction_mut(|t| db::tracks::update_in_transaction(t, &stored))?;
            if let Some(release) = &track.release {
                let (release_db_id, _) = keys.entity(release)?;
                fixtures::connect(db, release_db_id, track_db_id)?;
            }
            credit(db, &keys, track_db_id, &track.artists)?;
            keys.entities.insert(
                track.key.clone(),
                (track_db_id, db::favorites::FavoriteKind::Track),
            );
        }

        for playlist in &self.playlists {
            keys.claim(&playlist.key)?;
            let track_db_ids = playlist
                .tracks
                .iter()
                .map(|key| keys.entity(key).map(|(db_id, _)| db_id))
                .collect::<anyhow::Result<Vec<_>>>()?;
            let playlist_db_id = services::playlists::create(
                db,
                &services::playlists::CreatePlaylistRequest {
                    user_db_id: keys.user(&playlist.owner)?,
                    name: playlist.name.clone(),
                    description: None,
                    is_public: Some(playlist.public),
                    created_at: None,
                    updated_at: None,
                    track_db_ids,
                },
            )?;
            let entries = services::playlists::get_tracks(db, QueryId::Id(playlist_db_id))?
                .into_iter()
                .map(|link| link.entry_id)
                .collect();
            seeded.entries.insert(playlist.key.clone(), entries);
            keys.entities.insert(
                playlist.key.clone(),
                (playlist_db_id, db::favorites::FavoriteKind::Playlist),
            );
        }

        for listen in &self.listens {
            let (track_db_id, _) = keys.entity(&listen.track)?;
            record_listen(db, track_db_id, keys.user(&listen.user)?)?;
        }

        for favorite in &self.favorites {
            let (target_db_id, kind) = keys.entity(&favorite.target)?;
            db::favorites::add(db, keys.user(&favorite.user)?, target_db_id, kind, 1)?;
        }

        for (key, (db_id, _)) in &keys.entities {
            seeded.ids.insert(key.clone(), db_id.0);
        }
        for (key, name) in &keys.genres {
            let genre_db_id = db::genres::find_by_name(db, name)?
                .with_context(|| format!("fixture genre `{key}` is on no release"))?;
            seeded.ids.insert(key.clone(), genre_db_id.0);
        }

        Ok((seeded, keys.user(&self.run_as)?))
    }
}

fn credit(db: &mut DbAny, keys: &Keys, owner: DbId, artists: &[String]) -> anyhow::Result<()> {
    for (order, key) in artists.iter().enumerate() {
        let (artist_db_id, _) = keys.entity(key)?;
        fixtures::connect_credit(
            db,
            owner,
            artist_db_id,
            db::CreditType::Artist,
            None,
            order as u64,
        )?;
    }
    Ok(())
}

/// A completed listen of the whole track.
fn record_listen(db: &mut DbAny, track_db_id: DbId, user_db_id: DbId) -> anyhow::Result<()> {
    let track_public_id = db::tracks::get_by_id(db, track_db_id)?
        .context("track exists")?
        .id;
    db::listens::create_and_mark_recorded(
        db,
        &db::listens::Listen {
            db_id: None,
            id: nanoid::nanoid!(),
            track_public_id,
            position_ms: 0,
            duration_ms: Some(180_000),
            activity_ms: 180_000,
            state: db::PlaybackState::Completed,
            listened_at_ms: 1_000,
            created_at_ms: 1_000,
        },
        track_db_id,
        user_db_id,
        &db::playback_sessions::PlaybackSession {
            db_id: None,
            id: nanoid::nanoid!(),
            client_name: None,
            position_ms: 0,
            duration_ms: Some(180_000),
            activity_ms: Some(180_000),
            last_position_ms: None,
            state: db::PlaybackState::Completed,
            listen_recorded: Some(true),
            updated_at_ms: 1_000,
            created_at_ms: 1_000,
        },
    )?;
    Ok(())
}
