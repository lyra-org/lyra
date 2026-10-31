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

use crate::services;
use crate::services::metadata::layers::LocalLayer;
use lyra_db::metadata::layers::FieldInput;
use lyra_db::metadata::layers::edges::LayerCreditInput;
use lyra_db::{
    MetadataField,
    fixtures,
};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Fixture {
    /// The fixture user the test runs as, if not a fresh one.
    #[serde(default)]
    run_as: Option<String>,
    /// Scopes the test plugin declares that it goes without for the test.
    #[serde(default)]
    without_scopes: Vec<String>,
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
    libraries: Vec<Library>,
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
    #[serde(default)]
    ratings: Vec<Rating>,
    #[serde(default)]
    tags: Vec<Tag>,
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
    permissions: Vec<lyra_db::Permission>,
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
    artist_type: Option<lyra_db::ArtistType>,
    /// A manually edited type, ranked above the scanned one.
    manual_type: Option<lyra_db::ArtistType>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Genre {
    key: String,
    name: String,
}

/// A library holding the releases that name it and their tracks' files.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Library {
    key: String,
    name: String,
    /// The users granted the library. Admins see every library without a grant.
    #[serde(default)]
    users: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Release {
    key: String,
    title: String,
    /// The library the release, and so its tracks, belongs to.
    library: Option<String>,
    added: Option<u64>,
    #[serde(default)]
    artists: Vec<String>,
    #[serde(default)]
    credits: Vec<Credit>,
    #[serde(default)]
    genres: Vec<String>,
    /// An image file, relative to the fixture, stored as the release's cover.
    cover: Option<PathBuf>,
    /// The cover's blurhash.
    blurhash: Option<String>,
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
    /// How long the track runs, as a scan reads it from the audio.
    duration_ms: Option<u64>,
    #[serde(default)]
    artists: Vec<String>,
    #[serde(default)]
    credits: Vec<Credit>,
    /// An audio file, relative to the fixture, linked as the track's primary source. Its entry sits
    /// in the library of the track's release, or in an ungranted library of its own.
    file: Option<PathBuf>,
    /// The directories, relative to the library, holding `file`'s entry. The entry keeps its path
    /// on disk.
    folder: Option<PathBuf>,
    /// Where the track starts within `file`, as a cue sheet would place it.
    start_ms: Option<u64>,
    /// Where the track ends within `file`.
    end_ms: Option<u64>,
}

/// A credit of any type, listed after the owner's `artists`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Credit {
    artist: String,
    #[serde(rename = "type")]
    credit_type: lyra_db::CreditType,
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

/// A user's rating, written without the check that the user can see its target.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Rating {
    user: String,
    target: String,
    value: u8,
}

/// A user's tag, written without the check that the user can see its target, as one the user
/// placed before losing access to it.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Tag {
    user: String,
    target: String,
    name: String,
}

/// What the test receives as its chunk's `...`: user and library ids, API key secrets, session
/// tokens and entity ids by fixture key, and each playlist's entry ids in order.
#[derive(Debug, Default, Serialize)]
pub(crate) struct Seeded {
    users: BTreeMap<String, String>,
    libraries: BTreeMap<String, String>,
    api_keys: BTreeMap<String, String>,
    sessions: BTreeMap<String, String>,
    ids: BTreeMap<String, i64>,
    entries: BTreeMap<String, Vec<String>>,
}

struct Keys {
    users: BTreeMap<String, (DbId, String)>,
    api_keys: BTreeSet<String>,
    sessions: BTreeSet<String>,
    libraries: BTreeMap<String, DbId>,
    entities: BTreeMap<String, (DbId, lyra_db::favorites::FavoriteKind)>,
    genres: BTreeMap<String, String>,
}

impl Keys {
    fn user(&self, key: &str) -> anyhow::Result<DbId> {
        self.users
            .get(key)
            .map(|(db_id, _)| *db_id)
            .with_context(|| format!("fixture has no user `{key}`"))
    }

    fn library(&self, key: &str) -> anyhow::Result<DbId> {
        self.libraries
            .get(key)
            .copied()
            .with_context(|| format!("fixture has no library `{key}`"))
    }

    fn entity(&self, key: &str) -> anyhow::Result<(DbId, lyra_db::favorites::FavoriteKind)> {
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
                && !self.libraries.contains_key(key)
                && !self.entities.contains_key(key)
                && !self.genres.contains_key(key),
            "fixture key `{key}` is used twice"
        );
        Ok(())
    }
}

/// The directory entries track files sit in, created as tracks name them.
#[derive(Default)]
struct Folders {
    directories: BTreeMap<(DbId, PathBuf), DbId>,
    loose_library: Option<DbId>,
}

impl Folders {
    /// The library holding files whose track belongs to none.
    fn loose_library(&mut self, db: &mut DbAny) -> anyhow::Result<DbId> {
        if let Some(library) = self.loose_library {
            return Ok(library);
        }
        let path = PathBuf::from("/fixture-files");
        let created = lyra_db::libraries::create_system(
            db,
            lyra_db::libraries::LibraryInsert {
                id: nanoid::nanoid!(),
                name: "Fixture files".into(),
                path_key: lyra_db::libraries::path_key_for(&path),
                path,
                language: None,
                country: None,
            },
        )?;
        let library = created.db_id.context("library has a db_id")?;
        self.loose_library = Some(library);
        Ok(library)
    }

    /// The directory entry at `folder` under `library`, or the library itself without one.
    fn place(
        &mut self,
        db: &mut DbAny,
        library: DbId,
        folder: Option<&Path>,
    ) -> anyhow::Result<DbId> {
        let Some(folder) = folder else {
            return Ok(library);
        };
        let root = lyra_db::libraries::get_by_id(db, library)?
            .context("library exists")?
            .path;
        let mut parent = library;
        let mut relative = PathBuf::new();
        for component in folder.components() {
            relative.push(component);
            if let Some(&directory) = self.directories.get(&(library, relative.clone())) {
                parent = directory;
                continue;
            }
            let full_path = root.join(&relative);
            let directory = lyra_db::Entry {
                db_id: None,
                id: nanoid::nanoid!(),
                name: component.as_os_str().to_string_lossy().into_owned(),
                full_path,
                kind: lyra_db::entries::EntryKind::Dir,
                file_kind: None,
                hash: None,
                size: 0,
                mtime: 0,
                ctime: 0,
            };
            let directory = db
                .exec_mut(agdb::QueryBuilder::insert().element(&directory).query())?
                .ids()[0];
            lyra_db::graph::ensure_owned_edge(db, parent, directory)?;
            self.directories
                .insert((library, relative.clone()), directory);
            parent = directory;
        }
        Ok(parent)
    }
}

impl Fixture {
    /// Resolves the fixture's paths against `directory`, the one its file is in.
    pub fn in_directory(mut self, directory: &Path) -> Self {
        self.directory = directory.to_path_buf();
        self
    }

    pub(crate) fn without_scopes(&self) -> &[String] {
        &self.without_scopes
    }

    /// The key of the library `track`'s release belongs to.
    fn release_library(&self, track: &Track) -> anyhow::Result<Option<&str>> {
        let Some(release) = &track.release else {
            return Ok(None);
        };
        let release = self
            .releases
            .iter()
            .find(|candidate| &candidate.key == release)
            .with_context(|| format!("fixture has no release `{release}`"))?;
        Ok(release.library.as_deref())
    }

    /// Writes the fixture into `db`, returning what the test receives and the user it runs as, if
    /// it names one.
    pub(crate) fn seed(&self, db: &mut DbAny) -> anyhow::Result<(Seeded, Option<DbId>)> {
        let mut keys = Keys {
            users: BTreeMap::new(),
            api_keys: BTreeSet::new(),
            sessions: BTreeSet::new(),
            libraries: BTreeMap::new(),
            entities: BTreeMap::new(),
            genres: BTreeMap::new(),
        };
        let mut seeded = Seeded::default();

        if self.users.iter().any(|user| user.admin) {
            lyra_db::roles::ensure_builtin_roles(db)?;
        }
        for user in &self.users {
            keys.claim(&user.key)?;
            let record = fixtures::test_user(&user.key)?;
            let public_id = record.id.clone();
            let user_db_id = lyra_db::users::create(db, &record)?;
            anyhow::ensure!(
                !user.admin || user.permissions.is_empty(),
                "fixture user `{}` is an admin and takes no permissions",
                user.key
            );
            if user.admin {
                let role = lyra_db::roles::get_by_name(db, lyra_db::roles::BUILTIN_ADMIN_ROLE)?
                    .and_then(|role| role.db_id)
                    .context("built-in admin role exists")?;
                lyra_db::roles::assign_role_to_user(db, user_db_id, role)?;
            } else if !user.permissions.is_empty() {
                let role = lyra_db::roles::create(
                    db,
                    &lyra_db::roles::Role {
                        db_id: None,
                        id: nanoid::nanoid!(),
                        name: user.key.clone(),
                        permissions: user.permissions.clone(),
                    },
                )?;
                lyra_db::roles::assign_role_to_user(db, user_db_id, role)?;
            }
            seeded.users.insert(user.key.clone(), public_id.clone());
            keys.users.insert(user.key.clone(), (user_db_id, public_id));
        }

        for api_key in &self.api_keys {
            keys.claim(&api_key.key)?;
            let secret = services::auth::random_hex_secret::<16>();
            lyra_db::api_keys::create(
                db,
                keys.user(&api_key.user)?,
                &api_key.key,
                &services::auth::hash_secret(&secret),
                lyra_db::users::now_secs(),
            )?;
            seeded.api_keys.insert(api_key.key.clone(), secret);
            keys.api_keys.insert(api_key.key.clone());
        }

        for session in &self.sessions {
            keys.claim(&session.key)?;
            let token = services::auth::random_hex_secret::<16>();
            let now = lyra_db::users::now_secs();
            lyra_db::users::login(
                db,
                keys.user(&session.user)?,
                &lyra_db::users::Session {
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

        for library in &self.libraries {
            keys.claim(&library.key)?;
            let path = PathBuf::from("/libraries").join(&library.key);
            let created = lyra_db::libraries::create_system(
                db,
                lyra_db::libraries::LibraryInsert {
                    id: nanoid::nanoid!(),
                    name: library.name.clone(),
                    path_key: lyra_db::libraries::path_key_for(&path),
                    path,
                    language: None,
                    country: None,
                },
            )?;
            let library_db_id = created.db_id.context("library has a db_id")?;
            for user in &library.users {
                lyra_db::libraries::grant_access(
                    db,
                    keys.user(user)?,
                    library_db_id,
                    lyra_db::libraries::AccessKind::ReadWrite,
                )?;
            }
            seeded.libraries.insert(library.key.clone(), created.id);
            seeded.ids.insert(library.key.clone(), library_db_id.0);
            keys.libraries.insert(library.key.clone(), library_db_id);
        }

        for artist in &self.artists {
            keys.claim(&artist.key)?;
            let artist_db_id = fixtures::insert_artist(db, &artist.name)?;
            let mut stored =
                lyra_db::artists::get_by_id(db, artist_db_id)?.context("artist exists")?;
            // A scan names an artist by its exact spelling.
            stored.scan_name = artist.name.clone();
            lyra_db::artists::update(db, &stored)?;
            let mut layer = LocalLayer::default();
            layer.supply(MetadataField::ArtistName, Some(artist.name.as_str()));
            layer.supply(MetadataField::ArtistType, artist.artist_type);
            save_local(db, artist_db_id, layer)?;
            if let Some(manual_type) = artist.manual_type {
                let layer = lyra_db::MetadataLayer::new(
                    services::metadata::layers::MANUAL_SOURCE_ID,
                    lyra_db::metadata::layers::LayerFields::from([(
                        MetadataField::ArtistType,
                        Some(manual_type.into()),
                    )]),
                    0,
                );
                db.transaction_mut(|t| {
                    lyra_db::metadata::layers::upsert_inside_tx(t, artist_db_id, &layer)?;
                    services::metadata::merging::apply_merged_metadata_to_entity_inside_tx(
                        t,
                        artist_db_id,
                    )
                })?;
            }
            keys.entities.insert(
                artist.key.clone(),
                (artist_db_id, lyra_db::favorites::FavoriteKind::Artist),
            );
        }

        for release in &self.releases {
            keys.claim(&release.key)?;
            let release_db_id = fixtures::insert_release(db, &release.title)?;
            if release.added.is_some() {
                let mut stored =
                    lyra_db::releases::get_by_id(db, release_db_id)?.context("release exists")?;
                stored.ctime = release.added;
                db.transaction_mut(|t| lyra_db::releases::update_in_transaction(t, &stored))?;
            }
            if let Some(library) = &release.library {
                lyra_db::graph::ensure_owned_edge(db, keys.library(library)?, release_db_id)?;
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
            layer.supply(MetadataField::ReleaseTitle, Some(release.title.as_str()));
            layer.supply_input(
                MetadataField::Genres,
                (!genre_names.is_empty()).then_some(Some(FieldInput::Genres(genre_names))),
            );
            supply_credits(&mut layer, &keys, &release.artists, &release.credits)?;
            save_local(db, release_db_id, layer)?;
            anyhow::ensure!(
                release.cover.is_some() || release.blurhash.is_none(),
                "fixture release `{}` has a blurhash but no cover",
                release.key
            );
            if let Some(cover) = &release.cover {
                let path = self.directory.join(cover);
                anyhow::ensure!(
                    path.is_file(),
                    "fixture cover {} is not a file",
                    path.display()
                );
                db.transaction_mut(|t| {
                    lyra_db::covers::upsert(
                        t,
                        release_db_id,
                        lyra_db::Cover {
                            db_id: None,
                            id: nanoid::nanoid!(),
                            mime_type: services::covers::cover_mime_from_path(&path).into(),
                            path: path.to_string_lossy().into_owned(),
                            hash: release.key.clone(),
                            blurhash: release.blurhash.clone(),
                        },
                    )
                })?;
            }
            keys.entities.insert(
                release.key.clone(),
                (release_db_id, lyra_db::favorites::FavoriteKind::Release),
            );
        }

        let mut folders = Folders::default();
        for track in &self.tracks {
            keys.claim(&track.key)?;
            let track_db_id = fixtures::insert_track(db, &track.title)?;
            if track.added.is_some() || track.duration_ms.is_some() {
                let mut stored =
                    lyra_db::tracks::get_by_id(db, track_db_id)?.context("track exists")?;
                stored.ctime = track.added;
                stored.duration_ms = track.duration_ms;
                db.transaction_mut(|t| lyra_db::tracks::update_in_transaction(t, &stored))?;
            }
            if let Some(release) = &track.release {
                let (release_db_id, _) = keys.entity(release)?;
                lyra_db::releases::replace_track_release(db, release_db_id, track_db_id)?;
            }
            let mut layer = LocalLayer::default();
            layer.supply(MetadataField::TrackTitle, Some(track.title.as_str()));
            layer.supply(MetadataField::Disc, track.disc);
            layer.supply(MetadataField::Track, track.track);
            layer.supply(MetadataField::Year, track.year);
            supply_credits(&mut layer, &keys, &track.artists, &track.credits)?;
            save_local(db, track_db_id, layer)?;
            anyhow::ensure!(
                track.file.is_some() || (track.start_ms.is_none() && track.end_ms.is_none()),
                "fixture track {} trims no file",
                track.key
            );
            anyhow::ensure!(
                track.file.is_some() || track.folder.is_none(),
                "fixture track {} has a folder but no file",
                track.key
            );
            if let Some(file) = &track.file {
                let library_db_id = match self.release_library(track)? {
                    Some(library) => keys.library(library)?,
                    None => folders.loose_library(db)?,
                };
                let parent = folders.place(db, library_db_id, track.folder.as_deref())?;
                let path = self.directory.join(file);
                anyhow::ensure!(
                    path.is_file(),
                    "fixture track file {} is not a file",
                    path.display()
                );
                let format = services::metadata::read_source_format(&path)?;
                let entry = lyra_db::Entry {
                    db_id: None,
                    id: nanoid::nanoid!(),
                    name: path
                        .file_name()
                        .context("fixture track file has a name")?
                        .to_string_lossy()
                        .into_owned(),
                    size: std::fs::metadata(&path)?.len(),
                    full_path: path,
                    kind: lyra_db::entries::EntryKind::File,
                    file_kind: Some("audio".into()),
                    hash: None,
                    mtime: 0,
                    ctime: 0,
                };
                let entry_db_id = db
                    .exec_mut(agdb::QueryBuilder::insert().element(&entry).query())?
                    .ids()[0];
                lyra_db::graph::ensure_owned_edge(db, parent, entry_db_id)?;
                db.transaction_mut(|t| {
                    lyra_db::track_sources::upsert(
                        t,
                        track_db_id,
                        entry_db_id,
                        lyra_db::track_sources::TrackSourceUpsert {
                            source_kind: "embedded_tags".into(),
                            source_key: track.key.clone(),
                            is_primary: true,
                            start_ms: track.start_ms,
                            end_ms: track.end_ms,
                            format,
                        },
                        None,
                    )
                })?;
            }
            keys.entities.insert(
                track.key.clone(),
                (track_db_id, lyra_db::favorites::FavoriteKind::Track),
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
                (playlist_db_id, lyra_db::favorites::FavoriteKind::Playlist),
            );
        }

        for listen in &self.listens {
            let (track_db_id, _) = keys.entity(&listen.track)?;
            lyra_db::fixtures::insert_listen(db, keys.user(&listen.user)?, track_db_id, 1_000)?;
        }

        for favorite in &self.favorites {
            let (target_db_id, kind) = keys.entity(&favorite.target)?;
            lyra_db::favorites::add(db, keys.user(&favorite.user)?, target_db_id, kind, 1)?;
        }

        for rating in &self.ratings {
            let (target_db_id, kind) = keys.entity(&rating.target)?;
            if kind == lyra_db::favorites::FavoriteKind::Playlist {
                anyhow::bail!("rating target {} is a playlist", rating.target);
            }
            let value = lyra_db::ratings::RatingValue::new(rating.value).ok_or_else(|| {
                anyhow::anyhow!("rating of {} must be from 1 to 5", rating.target)
            })?;
            lyra_db::ratings::upsert(db, keys.user(&rating.user)?, target_db_id, value, 1)?;
        }

        for tag in &self.tags {
            let (target_db_id, _) = keys.entity(&tag.target)?;
            lyra_db::tags::create(
                db,
                keys.user(&tag.user)?,
                target_db_id,
                &lyra_db::tags::normalize_tag_name(&tag.name)?,
                "#808080",
                0,
            )?;
        }

        for (key, (db_id, _)) in &keys.entities {
            seeded.ids.insert(key.clone(), db_id.0);
        }
        for (key, db_id) in &keys.libraries {
            seeded.ids.insert(key.clone(), db_id.0);
        }
        for (key, name) in &keys.genres {
            let genre_db_id = lyra_db::genres::find_by_name(db, name)?
                .with_context(|| format!("fixture genre `{key}` is on no release"))?;
            seeded.ids.insert(key.clone(), genre_db_id.0);
        }

        let run_as = self
            .run_as
            .as_deref()
            .map(|key| keys.user(key))
            .transpose()?;
        Ok((seeded, run_as))
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
        .map(|key| (key, lyra_db::CreditType::Artist, None));
    let other_credits = credits
        .iter()
        .map(|credit| (&credit.artist, credit.credit_type, credit.detail.clone()));
    let mut by_role: BTreeMap<lyra_db::CreditType, Vec<LayerCreditInput>> = BTreeMap::new();
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn libraries_hold_their_releases_for_the_users_granted_them() -> anyhow::Result<()> {
        let mut db = lyra_db::test_db::new_test_db()?;
        let fixture: Fixture = serde_json::from_value(serde_json::json!({
            "run_as": "viewer",
            "users": [{ "key": "viewer" }],
            "libraries": [
                { "key": "shared", "name": "Shared", "users": ["viewer"] },
                { "key": "private", "name": "Private" },
            ],
            "releases": [
                { "key": "open", "title": "Open", "library": "shared" },
                { "key": "closed", "title": "Closed", "library": "private" },
            ],
            "tracks": [
                { "key": "open_song", "title": "Open Song", "release": "open" },
                { "key": "closed_song", "title": "Closed Song", "release": "closed" },
            ],
        }))?;
        let (seeded, viewer) = fixture.seed(&mut db)?;
        let viewer = viewer.context("fixture runs as viewer")?;

        let id = |key: &str| DbId(seeded.ids[key]);
        for (library, release, track) in [
            ("shared", "open", "open_song"),
            ("private", "closed", "closed_song"),
        ] {
            let stored = lyra_db::libraries::get_by_id(&db, id(library))?.context("library")?;
            assert_eq!(stored.id, seeded.libraries[library]);
            assert!(lyra_db::graph::edge_exists(&db, id(library), id(release))?);
            assert!(lyra_db::graph::edge_exists(&db, id(release), id(track))?);
            let owners = lyra_db::libraries::get_for_entity(&db, id(track))?;
            assert_eq!(
                owners.iter().map(|owner| owner.db_id).collect::<Vec<_>>(),
                vec![Some(id(library))]
            );
        }

        let principal = services::auth::Principal::load(&db, viewer)?.context("viewer exists")?;
        assert_eq!(
            principal.accessible_library_ids,
            [seeded.libraries["shared"].clone()].into_iter().collect()
        );
        assert_eq!(
            lyra_db::libraries::accessible_track_ids(
                &db,
                viewer,
                &[id("open_song"), id("closed_song")]
            )?,
            [id("open_song")].into_iter().collect()
        );
        Ok(())
    }

    #[test]
    fn tags_reach_targets_their_user_cannot_see() -> anyhow::Result<()> {
        let mut db = lyra_db::test_db::new_test_db()?;
        let fixture: Fixture = serde_json::from_value(serde_json::json!({
            "run_as": "viewer",
            "users": [{ "key": "viewer" }],
            "libraries": [{ "key": "private", "name": "Private" }],
            "releases": [{ "key": "closed", "title": "Closed", "library": "private" }],
            "tags": [{ "user": "viewer", "target": "closed", "name": "Vinyl" }],
        }))?;
        let (seeded, viewer) = fixture.seed(&mut db)?;
        let viewer = viewer.context("fixture runs as viewer")?;

        let closed = DbId(seeded.ids["closed"]);
        let tags = lyra_db::tags::get_for_targets_many(&db, viewer, &[closed])?;
        assert_eq!(
            tags[&closed]
                .iter()
                .map(|tag| tag.tag.as_str())
                .collect::<Vec<_>>(),
            vec!["Vinyl"]
        );
        Ok(())
    }

    #[test]
    fn tracks_run_as_long_as_their_duration() -> anyhow::Result<()> {
        let mut db = lyra_db::test_db::new_test_db()?;
        let fixture: Fixture = serde_json::from_value(serde_json::json!({
            "run_as": "viewer",
            "users": [{ "key": "viewer" }],
            "tracks": [{ "key": "song", "title": "Song", "duration_ms": 180_000 }],
        }))?;
        let (seeded, _) = fixture.seed(&mut db)?;

        let song = lyra_db::tracks::get_by_id(&db, DbId(seeded.ids["song"]))?.context("song")?;
        assert_eq!(song.duration_ms, Some(180_000));
        Ok(())
    }
}
