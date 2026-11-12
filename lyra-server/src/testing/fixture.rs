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

/// A plugin of one Luau file, its `init` module, with no entrypoint: `init` runs on the first
/// require.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Plugin {
    pub(crate) id: String,
    /// Relative to the fixture.
    source: PathBuf,
    #[serde(default)]
    pub(crate) scopes: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Fixture {
    /// The fixture user the test runs as, if not a fresh one.
    #[serde(default)]
    run_as: Option<String>,
    /// Scopes the test plugin declares that it goes without for the test.
    #[serde(default)]
    without_scopes: Vec<String>,
    /// Whether the test runs with authentication disabled.
    #[serde(default)]
    auth_disabled: bool,
    /// Whether the test runs with rate limits off.
    #[serde(default)]
    rate_limit_disabled: bool,
    /// Library plugins the test's plugin depends on and can require.
    #[serde(default)]
    plugins: Vec<Plugin>,
    #[serde(default)]
    users: Vec<User>,
    #[serde(default)]
    api_keys: Vec<ApiKey>,
    #[serde(default)]
    sessions: Vec<Session>,
    #[serde(default)]
    connections: Vec<Connection>,
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
    bookmarks: Vec<Bookmark>,
    #[serde(default)]
    tags: Vec<Tag>,
    #[serde(default)]
    lyrics: Vec<LyricsEntry>,
    #[serde(default)]
    playbacks: Vec<Playback>,
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
    /// When the session last authenticated a request.
    seen: Option<i64>,
}

/// An open native WebSocket connection, registered for the test's length.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Connection {
    key: String,
    user: String,
    /// The session key the client connects with; the fixture key without one.
    session_key: Option<String>,
    client_name: Option<String>,
    #[serde(default)]
    commands: Vec<services::remote::constants::RemoteAction>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Artist {
    key: String,
    name: String,
    sort_name: Option<String>,
    #[serde(rename = "type")]
    artist_type: Option<lyra_db::ArtistType>,
    /// A manually edited type, ranked above the scanned one.
    manual_type: Option<lyra_db::ArtistType>,
    /// An image file, relative to the fixture, stored and pinned as the artist's manual cover,
    /// whose hash is the artist key.
    cover: Option<PathBuf>,
    description: Option<String>,
    /// Identifier values by scheme, stored as [`ID_PROVIDER`]'s, which declares each scheme.
    #[serde(default)]
    identifiers: BTreeMap<String, String>,
}

/// The provider fixture identifiers are stored under, registered by a plugin of the same id.
const ID_PROVIDER: &str = "fixture";

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
    language: Option<String>,
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
    /// Whether `cover` names a file that doesn't exist, as one deleted after a sync.
    #[serde(default)]
    cover_gone: bool,
    /// The cover's blurhash.
    blurhash: Option<String>,
    description: Option<String>,
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
    /// Whether `file`'s entry is missing, as a sync that no longer found it leaves it.
    #[serde(default)]
    missing: bool,
    /// Further libraries holding an entry of `file` of their own, as further sources of the track.
    #[serde(default)]
    also_in: Vec<String>,
    /// The path on disk `also_in`'s entries record in place of `file`'s. Nothing reads the file
    /// there.
    also_at: Option<PathBuf>,
    /// Identifier values by scheme, stored as [`ID_PROVIDER`]'s, which declares each scheme.
    #[serde(default)]
    identifiers: BTreeMap<String, String>,
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
    /// An image file, relative to the fixture, stored as the playlist's own uploaded cover, whose
    /// hash is the playlist key.
    cover: Option<PathBuf>,
}

/// How a fixture cover is stored.
struct SeededCover {
    source: lyra_db::covers::CoverSource,
    /// Whether the owner pins the source, as an upload pins an artist's or release's.
    pinned: bool,
    blurhash: Option<String>,
    /// Whether the file is meant to be missing.
    gone: bool,
}

impl SeededCover {
    /// A playlist's upload, shown without a pin.
    const UPLOAD: Self = Self {
        source: lyra_db::covers::CoverSource::Manual,
        pinned: false,
        blurhash: None,
        gone: false,
    };
    /// An artist's upload, pinned.
    const PINNED_UPLOAD: Self = Self {
        pinned: true,
        ..Self::UPLOAD
    };
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Listen {
    user: String,
    track: String,
    /// When the listen started, in Unix milliseconds.
    at: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Favorite {
    user: String,
    target: String,
    /// When the user first favorited the target, in Unix milliseconds.
    at: Option<i64>,
}

/// A user's rating, written without the check that the user can see its target.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Rating {
    user: String,
    target: String,
    value: u8,
}

/// A user's bookmark of a track, written without the check that the user can see it or clamping
/// its position.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Bookmark {
    user: String,
    track: String,
    position_ms: u64,
    comment: Option<String>,
    /// When the user set the bookmark, in Unix milliseconds.
    at: Option<i64>,
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

/// A lyrics entry of a track: a provider's, a user's personal one, or else the shared one.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct LyricsEntry {
    /// The fixture key of the entry's public id.
    key: Option<String>,
    track: String,
    /// The provider that fetched it, registered and enabled in order of first appearance.
    provider: Option<String>,
    /// The user whose personal entry it is.
    user: Option<String>,
    language: Option<String>,
    /// As an upload's `Content-Type` names it; `text/plain` by default.
    content_type: Option<String>,
    body: String,
}

/// A queue-backed playback, as a client playing its queue leaves it, or as a plugin's client saved
/// it.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Playback {
    /// The fixture key of the playback's public id.
    key: Option<String>,
    user: String,
    /// The queue's track keys, in order and with repeats.
    tracks: Vec<String>,
    /// The current track's 0-based index in `tracks`.
    #[serde(default)]
    current: u64,
    position_ms: Option<u64>,
    /// The current track's play; paused by default.
    state: Option<lyra_db::PlaybackState>,
    client_name: Option<String>,
    /// The plugin whose client `client_name` saved the queue, which leaves its play stopped and
    /// counting no listen.
    plugin: Option<String>,
    /// When the playback and its play last changed, in Unix milliseconds; now by default.
    updated: Option<u64>,
}

/// What the test receives as its chunk's first `...`: user and library ids, API key secrets, session
/// tokens, connection ids and entity ids by fixture key, and each playlist's entry ids in order.
#[derive(Debug, Default, Serialize)]
pub(crate) struct Seeded {
    users: BTreeMap<String, String>,
    libraries: BTreeMap<String, String>,
    api_keys: BTreeMap<String, String>,
    sessions: BTreeMap<String, String>,
    connections: BTreeMap<String, String>,
    ids: BTreeMap<String, String>,
    entries: BTreeMap<String, Vec<String>>,
    #[serde(skip)]
    pending_connections: Vec<PendingConnection>,
    /// The schemes of the identifiers seeded, which [`ID_PROVIDER`] declares, with the entity type
    /// each identifies.
    #[serde(skip)]
    id_schemes: BTreeMap<String, services::EntityType>,
}

#[derive(Debug)]
struct PendingConnection {
    key: String,
    user_public_id: String,
    session_key: String,
    client_name: Option<String>,
    commands: Vec<services::remote::constants::RemoteAction>,
}

/// The fixture's registered connections, kept open until [`Self::close`].
pub(crate) struct OpenConnections(Vec<services::remote::registry::RegisterResult>);

impl OpenConnections {
    pub(crate) async fn close(self) {
        for connection in self.0 {
            services::remote::registry::unregister(connection.connection_id).await;
        }
    }
}

impl Seeded {
    /// Registers what the fixture seeded outside the database, which needs it unlocked: the
    /// identifier schemes, so seeded identifiers read as a provider's do, and the connections,
    /// whose ids it records for the test. When a connection can't register, the ones before it
    /// are closed again.
    pub(crate) async fn register(&mut self) -> anyhow::Result<OpenConnections> {
        register_id_schemes(std::mem::take(&mut self.id_schemes)).await?;
        let mut open = OpenConnections(Vec::new());
        for pending in std::mem::take(&mut self.pending_connections) {
            let registered = match services::remote::registry::register(
                pending.user_public_id,
                pending.client_name,
                pending.session_key,
                tokio::sync::watch::channel(None).0,
            )
            .await
            {
                Ok(registered) => registered,
                Err(error) => {
                    open.close().await;
                    anyhow::bail!("register connection `{}`: {error}", pending.key);
                }
            };
            services::remote::registry::set_supported_commands(
                registered.connection_id,
                pending.commands.into_iter().collect(),
            )
            .await;
            self.connections
                .insert(pending.key, registered.public_id.clone());
            open.0.push(registered);
        }
        Ok(open)
    }
}

/// Stores `identifiers` on the entity as [`ID_PROVIDER`]'s, recording each scheme with the entity
/// type it identifies; a scheme identifies one type only.
fn seed_identifiers(
    db: &mut DbAny,
    seeded: &mut Seeded,
    db_id: DbId,
    entity: services::EntityType,
    identifiers: &BTreeMap<String, String>,
) -> anyhow::Result<()> {
    for (scheme, value) in identifiers {
        services::providers::validate_id_scheme(scheme).map_err(anyhow::Error::msg)?;
        let declared = *seeded.id_schemes.entry(scheme.clone()).or_insert(entity);
        anyhow::ensure!(
            declared == entity,
            "identifier scheme {scheme} identifies both {declared:?} and {entity:?} entities"
        );
        lyra_db::external_ids::upsert(
            db,
            db_id,
            ID_PROVIDER,
            scheme,
            value,
            lyra_db::IdSource::Resolved,
        )?;
    }
    Ok(())
}

/// Registers [`ID_PROVIDER`] with `schemes`, declared as a provider plugin declares its own: by
/// scheme alone, not unique, and without links.
async fn register_id_schemes(
    schemes: BTreeMap<String, services::EntityType>,
) -> anyhow::Result<()> {
    if schemes.is_empty() {
        return Ok(());
    }
    let registry = services::providers::provider_registry();
    let mut registry = registry.write().await;
    registry.register(
        services::plugin_scope::PluginId::new(ID_PROVIDER)?,
        ID_PROVIDER.to_string(),
        None,
    )?;
    for (scheme, entity) in schemes {
        registry
            .set_id_registration(
                ID_PROVIDER,
                services::providers::ProviderIdSpec {
                    id_type: scheme.clone(),
                    entity,
                    unique: false,
                    scheme,
                    label: None,
                },
                || None,
            )
            .map_err(anyhow::Error::msg)?;
    }
    Ok(())
}

struct Keys {
    users: BTreeMap<String, (DbId, String)>,
    api_keys: BTreeSet<String>,
    sessions: BTreeSet<String>,
    connections: BTreeSet<String>,
    libraries: BTreeMap<String, DbId>,
    entities: BTreeMap<String, (DbId, lyra_db::favorites::FavoriteKind)>,
    genres: BTreeMap<String, String>,
    lyrics: BTreeMap<String, DbId>,
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
                && !self.connections.contains(key)
                && !self.libraries.contains_key(key)
                && !self.entities.contains_key(key)
                && !self.genres.contains_key(key)
                && !self.lyrics.contains_key(key),
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
    fn loose_library(&mut self, db: &mut DbAny, directory: &Path) -> anyhow::Result<DbId> {
        if let Some(library) = self.loose_library {
            return Ok(library);
        }
        let created = lyra_db::libraries::create_system(
            db,
            lyra_db::libraries::LibraryInsert {
                id: lyra_db::nanoid(),
                name: "Fixture files".into(),
                path_key: "/fixture-files".into(),
                path: directory.to_path_buf(),
                language: None,
                country: None,
                follow_links: false,
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
                id: lyra_db::nanoid(),
                name: component.as_os_str().to_string_lossy().into_owned(),
                full_path,
                kind: lyra_db::entries::EntryKind::Dir,
                file_kind: None,
                hash: None,
                size: 0,
                file_modified_at_ms: 0,
                file_created_at_ms: 0,
                synced_at_ms: None,
                missing_at_ms: None,
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

    /// Stores `cover`, relative to the fixture, as the owner's cover from `seeded.source`, its
    /// hash the owner's fixture key.
    fn seed_cover(
        &self,
        db: &mut DbAny,
        owner_db_id: DbId,
        key: &str,
        cover: &Path,
        seeded: SeededCover,
    ) -> anyhow::Result<()> {
        let path = self.directory.join(cover);
        anyhow::ensure!(
            path.is_file() != seeded.gone,
            "fixture cover {} {}",
            path.display(),
            if seeded.gone {
                "exists but is meant to be gone"
            } else {
                "is not a file"
            }
        );
        db.transaction_mut(|t| {
            lyra_db::covers::upsert(
                t,
                owner_db_id,
                seeded.source,
                lyra_db::Cover {
                    db_id: None,
                    id: lyra_db::nanoid(),
                    mime_type: services::covers::cover_mime_from_path(&path).into(),
                    path: path.to_string_lossy().into_owned(),
                    hash: key.to_string(),
                    blurhash: seeded.blurhash,
                    provider_id: None,
                },
            )?;
            if seeded.pinned {
                lyra_db::covers::select(t, owner_db_id, Some(seeded.source))?;
            }
            anyhow::Ok(())
        })
    }

    pub(crate) fn auth_disabled(&self) -> bool {
        self.auth_disabled
    }

    pub(crate) fn rate_limit_disabled(&self) -> bool {
        self.rate_limit_disabled
    }

    pub(crate) fn without_scopes(&self) -> &[String] {
        &self.without_scopes
    }

    pub(crate) fn plugins(&self) -> &[Plugin] {
        &self.plugins
    }

    /// Writes `plugin` under `plugins_dir`, where a require finds it.
    pub(crate) fn install_plugin(&self, plugin: &Plugin, plugins_dir: &Path) -> anyhow::Result<()> {
        let directory = plugins_dir.join(&plugin.id);
        std::fs::create_dir_all(&directory)?;
        let source = self.directory.join(&plugin.source);
        std::fs::copy(&source, directory.join("init.luau"))
            .with_context(|| format!("copy plugin source {}", source.display()))?;
        Ok(())
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
            connections: BTreeSet::new(),
            libraries: BTreeMap::new(),
            entities: BTreeMap::new(),
            genres: BTreeMap::new(),
            lyrics: BTreeMap::new(),
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
                        id: lyra_db::nanoid(),
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
                lyra_db::users::now_ms(),
            )?;
            seeded.api_keys.insert(api_key.key.clone(), secret);
            keys.api_keys.insert(api_key.key.clone());
        }

        for session in &self.sessions {
            keys.claim(&session.key)?;
            let token = services::auth::random_hex_secret::<16>();
            let now = lyra_db::users::now_ms();
            lyra_db::users::login(
                db,
                keys.user(&session.user)?,
                &lyra_db::users::Session {
                    db_id: None,
                    id: lyra_db::nanoid(),
                    token_hash: services::auth::hash_secret(&token),
                    expires_at_ms: 0,
                    created_at_ms: now,
                    last_seen_at_ms: session.seen.unwrap_or(now),
                    user_agent: None,
                    client_name: session.client_name.clone(),
                },
            )?;
            seeded.sessions.insert(session.key.clone(), token);
            keys.sessions.insert(session.key.clone());
        }

        for connection in &self.connections {
            keys.claim(&connection.key)?;
            let (_, user_public_id) = keys
                .users
                .get(&connection.user)
                .with_context(|| format!("fixture has no user `{}`", connection.user))?;
            seeded.pending_connections.push(PendingConnection {
                key: connection.key.clone(),
                user_public_id: user_public_id.clone(),
                session_key: connection
                    .session_key
                    .clone()
                    .unwrap_or_else(|| connection.key.clone()),
                client_name: connection.client_name.clone(),
                commands: connection.commands.clone(),
            });
            keys.connections.insert(connection.key.clone());
        }

        for genre in &self.genres {
            keys.claim(&genre.key)?;
            keys.genres.insert(genre.key.clone(), genre.name.clone());
        }

        for library in &self.libraries {
            keys.claim(&library.key)?;
            let key_path = PathBuf::from("/libraries").join(&library.key);
            // The fixture keeps its files in one directory, which each library holding one has as
            // its root under a key of its own.
            let holds_files = self.tracks.iter().try_fold(false, |found, track| {
                let held = track.file.is_some()
                    && (self.release_library(track)? == Some(library.key.as_str())
                        || track.also_in.contains(&library.key));
                anyhow::Ok(found || held)
            })?;
            let created = lyra_db::libraries::create_system(
                db,
                lyra_db::libraries::LibraryInsert {
                    id: lyra_db::nanoid(),
                    name: library.name.clone(),
                    path_key: key_path.to_string_lossy().into_owned(),
                    path: if holds_files {
                        self.directory.clone()
                    } else {
                        key_path
                    },
                    language: library.language.clone(),
                    country: None,
                    follow_links: false,
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
            seeded.ids.insert(library.key.clone(), created.id.clone());
            seeded.libraries.insert(library.key.clone(), created.id);
            keys.libraries.insert(library.key.clone(), library_db_id);
        }

        if self
            .artists
            .iter()
            .any(|artist| !artist.identifiers.is_empty())
            || self
                .tracks
                .iter()
                .any(|track| !track.identifiers.is_empty())
        {
            lyra_db::providers::register(db, ID_PROVIDER)?;
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
            layer.supply(MetadataField::SortName, artist.sort_name.as_deref());
            layer.supply(MetadataField::ArtistType, artist.artist_type);
            layer.supply(MetadataField::Description, artist.description.as_deref());
            save_local(db, artist_db_id, layer)?;
            seed_identifiers(
                db,
                &mut seeded,
                artist_db_id,
                services::EntityType::Artist,
                &artist.identifiers,
            )?;
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
            if let Some(cover) = &artist.cover {
                self.seed_cover(
                    db,
                    artist_db_id,
                    &artist.key,
                    cover,
                    SeededCover::PINNED_UPLOAD,
                )?;
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
                stored.file_created_at_ms = release.added;
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
            layer.supply(MetadataField::Description, release.description.as_deref());
            layer.supply_input(
                MetadataField::Genres,
                (!genre_names.is_empty()).then_some(Some(FieldInput::Genres(genre_names))),
            );
            supply_credits(&mut layer, &keys, &release.artists, &release.credits)?;
            save_local(db, release_db_id, layer)?;
            anyhow::ensure!(
                release.cover.is_some() || (release.blurhash.is_none() && !release.cover_gone),
                "fixture release `{}` has a blurhash or cover_gone but no cover",
                release.key
            );
            if let Some(cover) = &release.cover {
                self.seed_cover(
                    db,
                    release_db_id,
                    &release.key,
                    cover,
                    SeededCover {
                        source: lyra_db::covers::CoverSource::Local,
                        pinned: false,
                        blurhash: release.blurhash.clone(),
                        gone: release.cover_gone,
                    },
                )?;
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
                stored.file_created_at_ms = track.added;
                stored.duration_ms = track.duration_ms;
                db.transaction_mut(|t| lyra_db::tracks::update_in_transaction(t, &stored))?;
            }
            if let Some(release) = &track.release {
                let (release_db_id, _) = keys.entity(release)?;
                // A track just made has no release to leave.
                let _ = lyra_db::releases::replace_track_release(db, release_db_id, track_db_id)?;
            }
            let mut layer = LocalLayer::default();
            layer.supply(MetadataField::TrackTitle, Some(track.title.as_str()));
            layer.supply(MetadataField::Disc, track.disc);
            layer.supply(MetadataField::Track, track.track);
            layer.supply(MetadataField::Year, track.year);
            supply_credits(&mut layer, &keys, &track.artists, &track.credits)?;
            save_local(db, track_db_id, layer)?;
            seed_identifiers(
                db,
                &mut seeded,
                track_db_id,
                services::EntityType::Track,
                &track.identifiers,
            )?;
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
            anyhow::ensure!(
                track.file.is_some() || !track.missing,
                "fixture track {} is missing but has no file",
                track.key
            );
            anyhow::ensure!(
                track.file.is_some() || track.also_in.is_empty(),
                "fixture track {} is held elsewhere but has no file",
                track.key
            );
            anyhow::ensure!(
                !track.also_in.is_empty() || track.also_at.is_none(),
                "fixture track {} has an also_at but no also_in",
                track.key
            );
            if let Some(file) = &track.file {
                let library_db_id = match self.release_library(track)? {
                    Some(library) => keys.library(library)?,
                    None => folders.loose_library(db, &self.directory)?,
                };
                let path = self.directory.join(file);
                anyhow::ensure!(
                    path.is_file(),
                    "fixture track file {} is not a file",
                    path.display()
                );
                let format = services::metadata::read_source_format(&path)?;
                let mut holders = vec![library_db_id];
                for library in &track.also_in {
                    holders.push(keys.library(library)?);
                }
                for (index, library_db_id) in holders.into_iter().enumerate() {
                    let parent = folders.place(db, library_db_id, track.folder.as_deref())?;
                    let entry = lyra_db::Entry {
                        db_id: None,
                        id: lyra_db::nanoid(),
                        name: path
                            .file_name()
                            .context("fixture track file has a name")?
                            .to_string_lossy()
                            .into_owned(),
                        size: std::fs::metadata(&path)?.len(),
                        full_path: track
                            .also_at
                            .as_ref()
                            .filter(|_| index > 0)
                            .unwrap_or(&path)
                            .clone(),
                        kind: lyra_db::entries::EntryKind::File,
                        file_kind: Some("audio".into()),
                        hash: None,
                        file_modified_at_ms: 0,
                        file_created_at_ms: 0,
                        synced_at_ms: None,
                        missing_at_ms: None,
                    };
                    let entry_db_id = db
                        .exec_mut(agdb::QueryBuilder::insert().element(&entry).query())?
                        .ids()[0];
                    lyra_db::graph::ensure_owned_edge(db, parent, entry_db_id)?;
                    if track.missing {
                        lyra_db::entries::set_missing(db, &[entry_db_id], 1)?;
                    }
                    db.transaction_mut(|t| {
                        lyra_db::track_sources::upsert(
                            t,
                            track_db_id,
                            entry_db_id,
                            lyra_db::track_sources::TrackSourceUpsert {
                                source_kind: "embedded_tags".into(),
                                source_key: format!("{}:{index}", track.key),
                                is_primary: index == 0,
                                start_ms: track.start_ms,
                                end_ms: track.end_ms,
                                format,
                            },
                            None,
                        )
                    })?;
                }
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
            let (playlist_db_id, _) = services::playlists::create(
                db,
                &services::playlists::CreatePlaylistRequest {
                    user_db_id: keys.user(&playlist.owner)?,
                    name: playlist.name.clone(),
                    description: None,
                    is_public: Some(playlist.public),
                    created_at_ms: None,
                    updated_at_ms: None,
                    track_db_ids,
                },
            )?;
            let entries = services::playlists::get_tracks(db, QueryId::Id(playlist_db_id))?
                .into_iter()
                .map(|link| link.entry_id)
                .collect();
            seeded.entries.insert(playlist.key.clone(), entries);
            if let Some(cover) = &playlist.cover {
                self.seed_cover(
                    db,
                    playlist_db_id,
                    &playlist.key,
                    cover,
                    SeededCover::UPLOAD,
                )?;
            }
            keys.entities.insert(
                playlist.key.clone(),
                (playlist_db_id, lyra_db::favorites::FavoriteKind::Playlist),
            );
        }

        for listen in &self.listens {
            let (track_db_id, _) = keys.entity(&listen.track)?;
            lyra_db::fixtures::insert_listen(
                db,
                keys.user(&listen.user)?,
                track_db_id,
                listen.at.unwrap_or(1_000),
            )?;
        }

        for favorite in &self.favorites {
            let (target_db_id, kind) = keys.entity(&favorite.target)?;
            lyra_db::favorites::add(
                db,
                keys.user(&favorite.user)?,
                target_db_id,
                kind,
                favorite.at.unwrap_or(1),
            )?;
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

        for bookmark in &self.bookmarks {
            let (track_db_id, kind) = keys.entity(&bookmark.track)?;
            if kind != lyra_db::favorites::FavoriteKind::Track {
                anyhow::bail!("bookmark target {} is not a track", bookmark.track);
            }
            lyra_db::bookmarks::upsert(
                db,
                keys.user(&bookmark.user)?,
                track_db_id,
                bookmark.position_ms,
                bookmark.comment.as_deref(),
                bookmark.at.unwrap_or(1),
            )?;
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

        for entry in &self.lyrics {
            let (track_db_id, _) = keys.entity(&entry.track)?;
            let track = lyra_db::tracks::get_by_id(db, track_db_id)?
                .with_context(|| format!("lyrics track `{}` is not a track", entry.track))?;
            let input = services::metadata::lyrics::input_from_upload(
                entry.content_type.as_deref().unwrap_or("text/plain"),
                entry.body.as_bytes(),
                entry.language.clone(),
                1,
            )
            .map_err(|error| anyhow::anyhow!("lyrics of `{}`: {error}", entry.track))?;
            let lyrics_db_id = match (&entry.provider, &entry.user) {
                (Some(_), Some(_)) => {
                    anyhow::bail!("lyrics of `{}` name a provider and a user", entry.track)
                }
                (Some(provider), None) => {
                    lyra_db::providers::register(db, provider)?;
                    lyra_db::lyrics::upsert_from_plugin(
                        db,
                        track_db_id,
                        input,
                        format!("{provider}-lyrics"),
                        provider,
                        track.duration_ms,
                    )?
                }
                (None, Some(user)) => lyra_db::lyrics::upsert_personal(
                    db,
                    track_db_id,
                    input,
                    keys.user(user)?,
                    track.duration_ms,
                )?,
                (None, None) => {
                    lyra_db::lyrics::upsert_shared(db, track_db_id, input, track.duration_ms)?
                }
            };
            if let Some(key) = &entry.key {
                keys.claim(key)?;
                keys.lyrics.insert(key.clone(), lyrics_db_id);
            }
        }

        for playback in &self.playbacks {
            self.seed_playback(db, &keys, &mut seeded, playback)?;
        }

        let public_id = |db_id: DbId| {
            lyra_db::lookup::find_id_by_db_id(db, db_id)?.context("fixture node has a public id")
        };
        for (key, (db_id, _)) in &keys.entities {
            seeded.ids.insert(key.clone(), public_id(*db_id)?);
        }
        for (key, db_id) in &keys.lyrics {
            seeded.ids.insert(key.clone(), public_id(*db_id)?);
        }
        for (key, name) in &keys.genres {
            let genre_db_id = lyra_db::genres::find_by_name(db, name)?
                .with_context(|| format!("fixture genre `{key}` is on no release"))?;
            seeded.ids.insert(key.clone(), public_id(genre_db_id)?);
        }

        let run_as = self
            .run_as
            .as_deref()
            .map(|key| keys.user(key))
            .transpose()?;
        Ok((seeded, run_as))
    }
}

impl Fixture {
    fn seed_playback(
        &self,
        db: &mut DbAny,
        keys: &Keys,
        seeded: &mut Seeded,
        playback: &Playback,
    ) -> anyhow::Result<()> {
        if let Some(key) = &playback.key {
            keys.claim(key)?;
            anyhow::ensure!(
                !seeded.ids.contains_key(key),
                "fixture key `{key}` is used twice"
            );
        }
        let user_db_id = keys.user(&playback.user)?;
        let mut track_ids = Vec::with_capacity(playback.tracks.len());
        let mut track_db_ids = Vec::with_capacity(playback.tracks.len());
        for key in &playback.tracks {
            let (track_db_id, kind) = keys.entity(key)?;
            anyhow::ensure!(
                kind == lyra_db::favorites::FavoriteKind::Track,
                "playback track `{key}` is not a track"
            );
            track_db_ids.push(track_db_id);
            track_ids.push(
                lyra_db::lookup::find_id_by_db_id(db, track_db_id)?
                    .context("fixture track has a public id")?,
            );
        }
        let current_track_db_id = *track_db_ids
            .get(playback.current as usize)
            .with_context(|| format!("playback current {} is past its tracks", playback.current))?;
        anyhow::ensure!(
            playback.plugin.is_none() || playback.state.is_none(),
            "a saved playback is stopped and takes no state"
        );
        let updated_at_ms = playback
            .updated
            .unwrap_or_else(|| lyra_db::users::now_ms() as u64);
        let duration_ms = lyra_db::tracks::get_by_id(db, current_track_db_id)?
            .and_then(|track| track.duration_ms);
        let position_ms = playback.position_ms.unwrap_or(0);
        let mut session = lyra_db::PlaybackSession {
            db_id: None,
            id: lyra_db::nanoid(),
            play_id: lyra_db::nanoid(),
            client_name: playback.client_name.clone(),
            plugin_id: playback.plugin.clone(),
            position_ms,
            duration_ms,
            played_ms: 0,
            last_position_ms: position_ms,
            last_position_at_ms: updated_at_ms,
            latency_allowance_used_ms: 0,
            rate_permille: 1_000,
            counts_as_listen: playback.plugin.is_none(),
            state: if playback.plugin.is_some() {
                lyra_db::PlaybackState::Stopped
            } else {
                playback.state.unwrap_or(lyra_db::PlaybackState::Paused)
            },
            updated_at_ms,
            live_until_ms: 0,
            created_at_ms: updated_at_ms,
        };
        let stored = lyra_db::playbacks::Playback {
            db_id: None,
            id: lyra_db::nanoid(),
            source: lyra_db::playbacks::PlaybackSource::Queue(lyra_db::playbacks::Queue {
                revision: 1,
                track_ids,
                current_index: playback.current,
                repeat_mode: lyra_db::playbacks::RepeatMode::None,
                shuffle_enabled: false,
            }),
            created_at_ms: updated_at_ms,
            updated_at_ms,
        };
        db.transaction_mut(|t| {
            let session_db_id = crate::services::playback_sessions::insert_session(
                t,
                &mut session,
                current_track_db_id,
                user_db_id,
            )?;
            let playback_db_id = lyra_db::playbacks::insert(t, &stored, user_db_id, session_db_id)?;
            if let Some(plugin) = &playback.plugin {
                lyra_db::playbacks::claim_saved(
                    t,
                    user_db_id,
                    plugin,
                    playback.client_name.as_deref().unwrap_or(""),
                    playback_db_id,
                )?;
            }
            anyhow::Ok(())
        })?;
        if let Some(key) = &playback.key {
            seeded.ids.insert(key.clone(), stored.id);
        }
        Ok(())
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

    #[tokio::test]
    async fn connections_that_cannot_all_open_leave_none_open() -> anyhow::Result<()> {
        let _guard = services::testing::runtime_test_lock().await;
        services::testing::init_default_test_state()?;
        for connection in services::remote::registry::list_connections().await {
            services::remote::registry::unregister(connection.connection_id).await;
        }
        let over_the_cap = services::remote::constants::MAX_CONNECTIONS_PER_USER + 1;
        let fixture: Fixture = serde_json::from_value(serde_json::json!({
            "users": [{ "key": "owner" }],
            "connections": (0..over_the_cap)
                .map(|index| serde_json::json!({ "key": format!("device-{index}"), "user": "owner" }))
                .collect::<Vec<_>>(),
        }))?;
        let (mut seeded, _) = fixture.seed(&mut *services::STATE.db.write().await)?;

        let error = seeded
            .register()
            .await
            .err()
            .expect("the connection past the cap is refused");
        assert!(
            error.to_string().contains("too many connections"),
            "{error}"
        );
        assert!(
            services::remote::registry::list_connections()
                .await
                .is_empty()
        );
        Ok(())
    }

    #[tokio::test]
    async fn artists_keep_their_description_and_identifiers_by_scheme() -> anyhow::Result<()> {
        let _guard = services::testing::runtime_test_lock().await;
        services::testing::init_default_test_state()?;
        let fixture: Fixture = serde_json::from_value(serde_json::json!({
            "artists": [
                {
                    "key": "described",
                    "name": "Described",
                    "description": "A long story.",
                    "identifiers": { "example:artist": " a-1 ", "isrc": "XX0000000001" },
                },
                { "key": "bare", "name": "Bare" },
            ],
        }))?;
        let mut db = services::STATE.db.write().await;
        let (mut seeded, _) = fixture.seed(&mut db)?;
        seeded.register().await?.close().await;

        let schemes = services::providers::id_schemes().await;
        let read = |key: &str| -> anyhow::Result<_> {
            let artist_db_id = node(&db, &seeded, key);
            let artist = lyra_db::artists::get_by_id(&*db, artist_db_id)?.context("artist")?;
            let identifiers =
                schemes.resolve(&lyra_db::external_ids::get_for_entity(&db, artist_db_id)?);
            Ok((artist.description, identifiers))
        };
        assert_eq!(
            read("described")?,
            (
                Some("A long story.".to_string()),
                BTreeMap::from([
                    ("example:artist".to_string(), "a-1".to_string()),
                    ("isrc".to_string(), "XX0000000001".to_string()),
                ])
            )
        );
        assert_eq!(read("bare")?, (None, BTreeMap::new()));
        Ok(())
    }

    #[test]
    fn identifier_schemes_must_be_valid() -> anyhow::Result<()> {
        let mut db = lyra_db::test_db::new_test_db()?;
        let fixture: Fixture = serde_json::from_value(serde_json::json!({
            "artists": [{ "key": "a", "name": "A", "identifiers": { "Not A Scheme": "1" } }],
        }))?;
        let error = fixture
            .seed(&mut db)
            .expect_err("an invalid scheme is refused");
        assert!(error.to_string().contains("Not A Scheme"), "{error}");
        Ok(())
    }

    #[test]
    fn a_scheme_identifies_one_entity_type() -> anyhow::Result<()> {
        let mut db = lyra_db::test_db::new_test_db()?;
        let fixture: Fixture = serde_json::from_value(serde_json::json!({
            "artists": [{ "key": "a", "name": "A", "identifiers": { "example:id": "1" } }],
            "tracks": [{ "key": "t", "title": "T", "identifiers": { "example:id": "2" } }],
        }))?;
        let error = fixture
            .seed(&mut db)
            .expect_err("a scheme on artists and tracks is refused");
        assert!(error.to_string().contains("example:id"), "{error}");
        Ok(())
    }

    #[test]
    fn covers_are_stored_under_their_keys() -> anyhow::Result<()> {
        let directory = tempfile::tempdir()?;
        for file in ["red.png", "blue.png"] {
            std::fs::write(directory.path().join(file), file)?;
        }
        let mut db = lyra_db::test_db::new_test_db()?;
        let fixture: Fixture = serde_json::from_value::<Fixture>(serde_json::json!({
            "users": [{ "key": "owner" }],
            "artists": [
                { "key": "singer", "name": "Singer", "cover": "red.png" },
                { "key": "bare", "name": "Bare" },
            ],
            "releases": [
                { "key": "red", "title": "Red", "cover": "red.png" },
                { "key": "blue", "title": "Blue", "cover": "blue.png" },
            ],
            "playlists": [{ "key": "mix", "owner": "owner", "name": "Mix", "cover": "blue.png" }],
        }))?
        .in_directory(directory.path());
        let (seeded, _) = fixture.seed(&mut db)?;

        let id = |key: &str| node(&db, &seeded, key);
        let shown = |key: &str| {
            lyra_db::covers::sources(&db, id(key)).map(|sources| {
                sources
                    .into_iter()
                    .map(|sourced| (sourced.source, sourced.selected, sourced.cover.hash))
                    .collect::<Vec<_>>()
            })
        };
        use lyra_db::covers::CoverSource::{
            Local,
            Manual,
        };
        // An artist's cover is a pinned upload, a playlist's an unpinned one, a release's its
        // own local cover; each hash is the owner's key.
        assert_eq!(shown("singer")?, [(Manual, true, "singer".to_string())]);
        assert_eq!(shown("mix")?, [(Manual, false, "mix".to_string())]);
        assert_eq!(shown("red")?, [(Local, false, "red".to_string())]);
        assert_eq!(shown("blue")?, [(Local, false, "blue".to_string())]);
        assert!(shown("bare")?.is_empty());
        let singer = lyra_db::covers::get(&db, id("singer"))?.context("the artist's cover")?;
        assert_eq!(
            (singer.path, singer.mime_type.as_str()),
            (
                directory
                    .path()
                    .join("red.png")
                    .to_string_lossy()
                    .into_owned(),
                "image/png"
            )
        );
        Ok(())
    }

    #[test]
    fn covers_must_name_files_and_known_fields() -> anyhow::Result<()> {
        let directory = tempfile::tempdir()?;
        for (field, entity) in [
            (
                "artists",
                serde_json::json!({ "key": "a", "name": "A", "image": "a.png" }),
            ),
            (
                "playlists",
                serde_json::json!({ "key": "p", "owner": "o", "name": "P", "image": "p.png" }),
            ),
        ] {
            let error = serde_json::from_value::<Fixture>(serde_json::json!({ field: [entity] }))
                .expect_err("an unknown field is refused");
            assert!(
                error.to_string().contains("unknown field `image`"),
                "{error}"
            );
        }

        let mut db = lyra_db::test_db::new_test_db()?;
        let fixture = serde_json::from_value::<Fixture>(serde_json::json!({
            "artists": [{ "key": "a", "name": "A", "cover": "missing.png" }],
        }))?
        .in_directory(directory.path());
        let error = fixture
            .seed(&mut db)
            .expect_err("a missing cover is refused");
        assert!(error.to_string().contains("is not a file"), "{error}");
        Ok(())
    }

    /// The node a fixture key's seeded id names.
    fn node(db: &agdb::DbAny, seeded: &Seeded, key: &str) -> DbId {
        lyra_db::lookup::find_node_id_by_id(db, &seeded.ids[key])
            .expect("lookup")
            .expect("seeded node")
    }

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

        let id = |key: &str| node(&db, &seeded, key);
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

        let closed = node(&db, &seeded, "closed");
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

        let song = lyra_db::tracks::get_by_id(&db, node(&db, &seeded, "song"))?.context("song")?;
        assert_eq!(song.duration_ms, Some(180_000));
        Ok(())
    }
}
