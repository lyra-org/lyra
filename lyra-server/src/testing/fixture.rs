// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Data a Luau test runs against, which the test runner reads from a `<test>.fixture.toml`.

use std::collections::{
    BTreeMap,
    BTreeSet,
};
use std::path::{
    Path,
    PathBuf,
};

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

use crate::db::metadata::layers::edges::LayerCreditInput;
use crate::db::{
    self,
    MetadataField,
    fixtures,
};
use crate::services;
use crate::services::metadata::layers::LocalLayer;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Fixture {
    /// The fixture user the test runs as.
    run_as: String,
    #[serde(default)]
    users: Vec<User>,
    #[serde(default)]
    api_keys: Vec<ApiKey>,
    #[serde(default)]
    sessions: Vec<Session>,
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
    /// The directory fixture paths resolve against.
    #[serde(skip)]
    directory: PathBuf,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct User {
    key: String,
    #[serde(default)]
    admin: bool,
    /// Permissions of a role of the user's own, for a user who is not an admin.
    #[serde(default)]
    permissions: Vec<db::Permission>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ApiKey {
    key: String,
    user: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Session {
    key: String,
    user: String,
    client_name: Option<String>,
    /// Unix seconds of the session's last authenticated request.
    seen: Option<i64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Artist {
    key: String,
    name: String,
    #[serde(rename = "type")]
    artist_type: Option<db::ArtistType>,
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
    credits: Vec<Credit>,
    #[serde(default)]
    genres: Vec<String>,
    /// An image file, relative to the fixture, stored as the release's cover.
    cover: Option<PathBuf>,
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
    #[serde(default)]
    credits: Vec<Credit>,
    /// An audio file, relative to the fixture, linked as the track's primary source.
    file: Option<PathBuf>,
}

/// A credit of any type, listed after the owner's `artists`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Credit {
    artist: String,
    #[serde(rename = "type")]
    credit_type: db::CreditType,
    detail: Option<String>,
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

/// What the test receives as its chunk's `...`: user ids, API key secrets, session tokens and
/// entity ids by fixture key, and each playlist's entry ids in order.
#[derive(Debug, Default, Serialize)]
pub(crate) struct Seeded {
    users: BTreeMap<String, String>,
    api_keys: BTreeMap<String, String>,
    sessions: BTreeMap<String, String>,
    ids: BTreeMap<String, i64>,
    entries: BTreeMap<String, Vec<String>>,
}

struct Keys {
    users: BTreeMap<String, (DbId, String)>,
    api_keys: BTreeSet<String>,
    sessions: BTreeSet<String>,
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
                && !self.api_keys.contains(key)
                && !self.sessions.contains(key)
                && !self.entities.contains_key(key)
                && !self.genres.contains_key(key),
            "fixture key `{key}` is used twice"
        );
        Ok(())
    }
}

impl Fixture {
    /// Resolves the fixture's paths against `directory`, the one its file is in.
    pub fn in_directory(mut self, directory: &Path) -> Self {
        self.directory = directory.to_path_buf();
        self
    }

    /// Writes the fixture into `db`, returning what the test receives and the user it runs as.
    pub(crate) fn seed(&self, db: &mut DbAny) -> anyhow::Result<(Seeded, DbId)> {
        let mut keys = Keys {
            users: BTreeMap::new(),
            api_keys: BTreeSet::new(),
            sessions: BTreeSet::new(),
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
            anyhow::ensure!(
                !user.admin || user.permissions.is_empty(),
                "fixture user `{}` is an admin and takes no permissions",
                user.key
            );
            if user.admin {
                let role = db::roles::get_by_name(db, db::roles::BUILTIN_ADMIN_ROLE)?
                    .and_then(|role| role.db_id)
                    .context("built-in admin role exists")?;
                db::roles::assign_role_to_user(db, user_db_id, role)?;
            } else if !user.permissions.is_empty() {
                let role = db::roles::create(
                    db,
                    &db::roles::Role {
                        db_id: None,
                        id: nanoid::nanoid!(),
                        name: user.key.clone(),
                        permissions: user.permissions.clone(),
                    },
                )?;
                db::roles::assign_role_to_user(db, user_db_id, role)?;
            }
            seeded.users.insert(user.key.clone(), public_id.clone());
            keys.users.insert(user.key.clone(), (user_db_id, public_id));
        }

        for api_key in &self.api_keys {
            keys.claim(&api_key.key)?;
            let secret = services::auth::random_hex_secret::<16>();
            db::api_keys::create(
                db,
                keys.user(&api_key.user)?,
                &api_key.key,
                &services::auth::hash_secret(&secret),
                db::users::now_secs(),
            )?;
            seeded.api_keys.insert(api_key.key.clone(), secret);
            keys.api_keys.insert(api_key.key.clone());
        }

        for session in &self.sessions {
            keys.claim(&session.key)?;
            let token = services::auth::random_hex_secret::<16>();
            let now = db::users::now_secs();
            db::users::login(
                db,
                keys.user(&session.user)?,
                &db::users::Session {
                    db_id: None,
                    id: nanoid::nanoid!(),
                    token_hash: services::auth::hash_secret(&token),
                    expires_at: 0,
                    created_at: now,
                    last_seen_at: session.seen.unwrap_or(now),
                    user_agent: None,
                    client_name: session.client_name.clone(),
                },
            )?;
            seeded.sessions.insert(session.key.clone(), token);
            keys.sessions.insert(session.key.clone());
        }

        for genre in &self.genres {
            keys.claim(&genre.key)?;
            keys.genres.insert(genre.key.clone(), genre.name.clone());
        }

        for artist in &self.artists {
            keys.claim(&artist.key)?;
            let artist_db_id = fixtures::insert_artist(db, &artist.name)?;
            let mut stored = db::artists::get_by_id(db, artist_db_id)?.context("artist exists")?;
            // A scan names an artist by its exact spelling.
            stored.scan_name = artist.name.clone();
            db::artists::update(db, &stored)?;
            let mut layer = LocalLayer::default();
            layer.supply(MetadataField::ArtistName, Some(artist.name.as_str()))?;
            layer.supply(
                MetadataField::ArtistType,
                artist
                    .artist_type
                    .map(|artist_type| artist_type.to_string()),
            )?;
            save_local(db, artist_db_id, layer)?;
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
            let mut layer = LocalLayer::default();
            layer.supply(MetadataField::ReleaseTitle, Some(release.title.as_str()))?;
            layer.supply(
                MetadataField::Genres,
                (!genre_names.is_empty()).then_some(genre_names),
            )?;
            supply_credits(&mut layer, &keys, &release.artists, &release.credits)?;
            save_local(db, release_db_id, layer)?;
            if let Some(cover) = &release.cover {
                let path = self.directory.join(cover);
                anyhow::ensure!(
                    path.is_file(),
                    "fixture cover {} is not a file",
                    path.display()
                );
                db.transaction_mut(|t| {
                    db::covers::upsert(
                        t,
                        release_db_id,
                        db::Cover {
                            db_id: None,
                            id: nanoid::nanoid!(),
                            mime_type: services::covers::cover_mime_from_path(&path).into(),
                            path: path.to_string_lossy().into_owned(),
                            hash: release.key.clone(),
                            blurhash: None,
                        },
                    )
                })?;
            }
            keys.entities.insert(
                release.key.clone(),
                (release_db_id, db::favorites::FavoriteKind::Release),
            );
        }

        for track in &self.tracks {
            keys.claim(&track.key)?;
            let track_db_id = fixtures::insert_track(db, &track.title)?;
            if track.added.is_some() {
                let mut stored = db::tracks::get_by_id(db, track_db_id)?.context("track exists")?;
                stored.ctime = track.added;
                db.transaction_mut(|t| db::tracks::update_in_transaction(t, &stored))?;
            }
            if let Some(release) = &track.release {
                let (release_db_id, _) = keys.entity(release)?;
                fixtures::connect(db, release_db_id, track_db_id)?;
            }
            let mut layer = LocalLayer::default();
            layer.supply(MetadataField::TrackTitle, Some(track.title.as_str()))?;
            layer.supply(MetadataField::Disc, track.disc)?;
            layer.supply(MetadataField::Track, track.track)?;
            layer.supply(MetadataField::Year, track.year)?;
            supply_credits(&mut layer, &keys, &track.artists, &track.credits)?;
            save_local(db, track_db_id, layer)?;
            if let Some(file) = &track.file {
                let path = self.directory.join(file);
                anyhow::ensure!(
                    path.is_file(),
                    "fixture track file {} is not a file",
                    path.display()
                );
                let format = services::metadata::read_source_format(&path)?;
                let entry = db::Entry {
                    db_id: None,
                    id: nanoid::nanoid!(),
                    name: path
                        .file_name()
                        .context("fixture track file has a name")?
                        .to_string_lossy()
                        .into_owned(),
                    size: std::fs::metadata(&path)?.len(),
                    full_path: path,
                    kind: db::entries::EntryKind::File,
                    file_kind: Some("audio".into()),
                    hash: None,
                    mtime: 0,
                    ctime: 0,
                };
                let entry_db_id = db
                    .exec_mut(agdb::QueryBuilder::insert().element(&entry).query())?
                    .ids()[0];
                db.transaction_mut(|t| {
                    db::track_sources::upsert(
                        t,
                        track_db_id,
                        entry_db_id,
                        db::track_sources::TrackSourceUpsert {
                            source_kind: "embedded_tags".into(),
                            source_key: track.key.clone(),
                            is_primary: true,
                            start_ms: None,
                            end_ms: None,
                            format,
                        },
                        None,
                    )
                })?;
            }
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

/// Saves `layer` as the entity's local layer and resolves the entity from it,
/// as a scan does.
fn save_local(db: &mut DbAny, node_id: DbId, layer: LocalLayer) -> anyhow::Result<()> {
    db.transaction_mut(|t| layer.save(t, node_id))
}

/// Supplies an owner's `artists`, then its other `credits`, each role in order.
fn supply_credits(
    layer: &mut LocalLayer,
    keys: &Keys,
    artists: &[String],
    credits: &[Credit],
) -> anyhow::Result<()> {
    let artist_credits = artists
        .iter()
        .map(|key| (key, db::CreditType::Artist, None));
    let other_credits = credits
        .iter()
        .map(|credit| (&credit.artist, credit.credit_type, credit.detail.clone()));
    let mut by_role: BTreeMap<db::CreditType, Vec<LayerCreditInput>> = BTreeMap::new();
    for (key, credit_type, detail) in artist_credits.chain(other_credits) {
        let (artist_id, _) = keys.entity(key)?;
        by_role
            .entry(credit_type)
            .or_default()
            .push(LayerCreditInput { artist_id, detail });
    }
    for (credit_type, inputs) in by_role {
        layer.supply_credits(credit_type, inputs);
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
