// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

mod fixture;
mod harness;

pub use fixture::Fixture;

pub use crate::services::testing::RuntimeTestGuard;

use std::collections::{
    BTreeMap,
    BTreeSet,
    HashMap,
};
use std::path::{
    Path,
    PathBuf,
};
use std::sync::Arc;
use std::time::{
    SystemTime,
    UNIX_EPOCH,
};

use agdb::{
    DbAny,
    DbId,
    QueryBuilder,
};
use anyhow::Context;
use harmony_core::ModuleSpec;
use lyra_db::nanoid;
use serde::{
    Deserialize,
    Serialize,
};

use crate::services;
use crate::services::STATE;
use crate::services::testing::runtime_test_lock;
use lyra_db::Entry;

#[derive(Debug, Clone)]
pub struct LibraryFixtureConfig {
    pub directory: PathBuf,
    pub language: Option<String>,
    pub country: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PreparedFixture {
    pub library_id: i64,
    pub release_id: i64,
    pub track_ids: Vec<i64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CreditSnapshot {
    pub artist_name: String,
    pub artist_id: Option<String>,
    pub credit_type: String,
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EntitySnapshot {
    pub node_id: i64,
    pub fields: BTreeMap<String, serde_json::Value>,
    pub external_ids: BTreeMap<String, String>,
    #[serde(default)]
    pub credits: Vec<CreditSnapshot>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FixtureSnapshot {
    pub release: Option<EntitySnapshot>,
    pub artists: Vec<EntitySnapshot>,
    pub tracks: Vec<EntitySnapshot>,
}

/// Initializes the test state and plugin runtime for `library`. Hold the guard for as long as the
/// state is in use.
pub async fn initialize_runtime(
    library: &LibraryFixtureConfig,
) -> anyhow::Result<RuntimeTestGuard> {
    let guard = runtime_test_lock().await;
    crate::plugins::testing::initialize_state(
        Some(services::testing::library_config(
            library.directory.clone(),
            library.language.clone(),
            library.country.clone(),
        )?),
        Default::default(),
    )?;
    crate::plugins::testing::start_runtime().await?;
    Ok(guard)
}

pub async fn prepare_fixture(
    library: &LibraryFixtureConfig,
    raw_tags: Vec<lyra_metadata::RawTrackTags>,
) -> anyhow::Result<PreparedFixture> {
    let mut db = STATE.db.write().await;
    let directory = library.directory.clone();
    let path_key = lyra_db::libraries::path_key_for(&directory);
    let created_library = db.transaction_mut(
        |t| -> Result<lyra_db::Library, lyra_db::libraries::LibraryCreateError> {
            lyra_db::libraries::create_system(
                t,
                lyra_db::libraries::LibraryInsert {
                    id: nanoid(),
                    name: "Harmony Test Library".to_string(),
                    path: directory,
                    path_key,
                    language: library.language.clone(),
                    country: library.country.clone(),
                    follow_links: false,
                },
            )
        },
    )?;
    let library_db_id = created_library
        .db_id
        .ok_or_else(|| anyhow::anyhow!("library insert missing db_id"))?;

    // Grouped, normalized and ordered as a library scan parses them.
    let mut path_to_entry: HashMap<PathBuf, (DbId, String)> = HashMap::new();
    let mut tracks_by_entry: BTreeMap<DbId, Vec<services::metadata::TrackMetadata>> =
        BTreeMap::new();
    let processed = services::metadata::mapping::process_tags(raw_tags);
    if !processed.rejected.is_empty() {
        let rejected = processed
            .rejected
            .iter()
            .map(|rejected| format!("{} (missing {:?})", rejected.file_path, rejected.missing))
            .collect::<Vec<_>>();
        anyhow::bail!(
            "raw_tags rows a library scan would skip: {}",
            rejected.join(", ")
        );
    }
    for track in processed.tracks {
        let file_path = PathBuf::from(
            track
                .file_path
                .clone()
                .ok_or_else(|| anyhow::anyhow!("processed track is missing file_path"))?,
        );
        let (entry_db_id, entry_public_id) = match path_to_entry.get(&file_path) {
            Some(entry) => entry.clone(),
            None => {
                let entry = insert_file_entry(&mut db, library_db_id, &file_path)?;
                path_to_entry.insert(file_path.clone(), entry.clone());
                entry
            }
        };
        // Most fixtures name files that don't exist; only real ones have a format to read.
        let format = if file_path.is_file() {
            services::metadata::read_source_format(&file_path)?
        } else {
            lyra_db::track_sources::SourceFormat::default()
        };
        tracks_by_entry.entry(entry_db_id).or_default().push(
            services::metadata::embedded_track_metadata(
                entry_db_id,
                &entry_public_id,
                format,
                track,
            ),
        );
    }
    // Grouped as a sync groups the library's files, so fixtures coalesce as a scan would.
    let entries = path_to_entry
        .iter()
        .map(|(path, (entry_db_id, _))| {
            let mut entry = lyra_db::entries::get_by_id(&*db, *entry_db_id)?
                .ok_or_else(|| anyhow::anyhow!("fixture entry missing"))?;
            entry.full_path = library.directory.join(path);
            Ok(entry)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let mut tracks_by_dir: BTreeMap<(usize, PathBuf), Vec<services::metadata::TrackMetadata>> =
        BTreeMap::new();
    for (key, group) in services::metadata::ingestion::album_groups(&library.directory, entries)
        .into_iter()
        .enumerate()
    {
        for entry in &group.entries {
            let Some(tracks) = entry.db_id.and_then(|id| tracks_by_entry.remove(&id)) else {
                continue;
            };
            tracks_by_dir
                .entry((key, group.candidate_dir(&entry.full_path)))
                .or_default()
                .extend(tracks);
        }
    }
    let parsed_groups = tracks_by_dir
        .into_iter()
        .map(|((coalesce_group_key, source_dir), mut metadata)| {
            services::metadata::sort_track_metadata(&mut metadata);
            services::metadata::ingestion::ParsedMetadataGroup {
                coalesce_group_key,
                source_dir,
                metadata,
            }
        })
        .collect();
    let mut coalesced = services::metadata::ingestion::coalesce_disc_groups(parsed_groups);
    if coalesced.is_empty() {
        anyhow::bail!("raw_tags produced no coalesced release groups");
    }
    if coalesced.len() != 1 {
        anyhow::bail!(
            "raw_tags produced {} release groups; lyra-harmony-test expects exactly one release context",
            coalesced.len()
        );
    }
    let metadata = coalesced.pop().expect("coalesced release group");

    services::metadata::ingestion::apply_metadata(&mut db, library_db_id, metadata)?;

    let mut track_db_ids = BTreeSet::new();
    for &(entry_db_id, _) in path_to_entry.values() {
        for track in lyra_db::tracks::get_by_entry(&db, entry_db_id)? {
            if let Some(track_db_id) = track.db_id {
                track_db_ids.insert(DbId::from(track_db_id));
            }
        }
    }
    let first_track_db_id = *track_db_ids
        .first()
        .ok_or_else(|| anyhow::anyhow!("fixture ingestion produced no tracks"))?;
    let mut releases = lyra_db::releases::get_by_track(&db, first_track_db_id)?;
    let release = releases
        .pop()
        .ok_or_else(|| anyhow::anyhow!("fixture ingestion produced no release"))?;
    let release_db_id = release
        .db_id
        .ok_or_else(|| anyhow::anyhow!("fixture release missing db_id"))?;

    Ok(PreparedFixture {
        library_id: library_db_id.0,
        release_id: DbId::from(release_db_id).0,
        track_ids: track_db_ids.into_iter().map(|id| id.0).collect(),
    })
}

/// The plugin with the nearest manifest at or above the test path.
pub struct PluginUnderTest {
    manifest: harmony_core::plugin::PluginManifest,
    directory: PathBuf,
}

impl PluginUnderTest {
    pub fn locate(test_path: &Path) -> anyhow::Result<Self> {
        let test_path = test_path
            .canonicalize()
            .with_context(|| format!("canonicalize {}", test_path.display()))?;
        let directory = test_path
            .ancestors()
            .find(|dir| {
                dir.join(harmony_core::plugin::PLUGIN_CONFIG_FILENAME)
                    .is_file()
            })
            .with_context(|| {
                format!(
                    "no {} found at or above {}",
                    harmony_core::plugin::PLUGIN_CONFIG_FILENAME,
                    test_path.display()
                )
            })?;
        let loaded = harmony_core::plugin::PluginManager::load_plugin_dir(
            directory,
            &crate::plugins::plugin_host(),
        )
        .map_err(|error| anyhow::anyhow!("{error}"))?;

        Ok(Self {
            manifest: loaded.manifest,
            directory: loaded.directory,
        })
    }

    /// The plugin the tests under `directory` of the modules the server provides run as. It
    /// declares every scope, so a test can go without any of them.
    pub fn placeholder(directory: &Path) -> anyhow::Result<Self> {
        let directory = directory
            .canonicalize()
            .with_context(|| format!("canonicalize {}", directory.display()))?;
        Ok(Self {
            manifest: library_manifest(
                "test",
                "Harness Module Tests",
                crate::plugins::scopes::Scope::ALL
                    .iter()
                    .map(|scope| scope.def().id.to_string())
                    .collect(),
            )?,
            directory,
        })
    }

    pub fn id(&self) -> &str {
        &self.manifest.id
    }

    /// The plugin's manifest without `scopes`, which it must declare. A scope another remaining
    /// one narrows can't go.
    fn manifest_without(
        &self,
        scopes: &[String],
    ) -> anyhow::Result<harmony_core::plugin::PluginManifest> {
        let mut manifest = self.manifest.clone();
        for scope in scopes {
            anyhow::ensure!(
                manifest.scopes.contains(scope),
                "plugin `{}` does not declare `{scope}`",
                manifest.id
            );
        }
        manifest.scopes.retain(|scope| !scopes.contains(scope));
        for (scope, parent) in crate::plugins::scopes::scope_parents() {
            anyhow::ensure!(
                !manifest.scopes.iter().any(|kept| kept == scope)
                    || manifest.scopes.iter().any(|kept| kept == parent),
                "`{scope}` stays declared without `{parent}`"
            );
        }
        Ok(manifest)
    }

    pub fn directory(&self) -> &Path {
        &self.directory
    }
}

/// The manifest of a plugin that exists only for a test, with no entrypoint.
fn library_manifest(
    id: &str,
    name: &str,
    scopes: Vec<String>,
) -> anyhow::Result<harmony_core::plugin::PluginManifest> {
    Ok(harmony_core::plugin::PluginManifest {
        schema_version: harmony_core::plugin::PLUGIN_SCHEMA_VERSION,
        id: id.to_string(),
        name: name.to_string(),
        version: semver::Version::new(1, 0, 0),
        host_requirement: semver::VersionReq::parse(concat!("=", env!("CARGO_PKG_VERSION")))?,
        description: "Luau tests for server-provided plugin modules".to_string(),
        entrypoint: None,
        scopes,
        scope_reasons: BTreeMap::new(),
        scope_grants: BTreeMap::new(),
        dependencies: BTreeMap::new(),
    })
}

/// Keep the returned directory alive so runtime callbacks can load plugin files.
pub async fn exec_plugins(
    plugin: &PluginUnderTest,
    http_module: ModuleSpec,
) -> anyhow::Result<TempPluginsDir> {
    let isolated = TempPluginsDir::new(plugin)?;
    let server_info = crate::plugins::server::load_server_info().await?;
    let auth_capabilities =
        crate::plugins::auth::AuthCapabilities::from_config(&STATE.config().auth);
    let (runtime, errors) =
        crate::plugins::executor::PluginExecutorHandle::discover_from_plugins_dir_with_db_and_modules(
            isolated.plugins_dir(),
            server_info,
            auth_capabilities,
            STATE.db.get(),
            vec![http_module],
            Some(STATE.settings.clone()),
        )?;
    if !errors.is_empty() {
        let details = errors
            .into_iter()
            .map(|error| error.to_string())
            .collect::<Vec<_>>()
            .join("; ");
        anyhow::bail!("plugin discovery failed: {details}");
    }

    runtime
        .scope()
        .plugin
        .manifests
        .replace(Arc::from(runtime.plugin_manifests().await?));
    crate::plugins::STATE.publish_runtime(&crate::services::STATE.generation(), runtime.clone());
    runtime.exec_all().await?;

    let mut db = STATE.db.write().await;
    for mut provider in lyra_db::providers::get(&db)? {
        provider.enabled = provider.provider_id == plugin.id();
        lyra_db::providers::upsert(&mut db, &provider)?;
    }

    Ok(isolated)
}

/// Username of the user a Luau test without a fixture dispatches as.
const LUAU_TEST_USERNAME: &str = "luau-tester";

/// Runs one `.luau` file as `plugin`, without executing the plugin's entrypoint. With a fixture,
/// the fixture is seeded, the plugin goes without the fixture's `without_scopes` and the test
/// receives the seeded ids as its first `...`; its second is always the [`harness`]. The
/// test dispatches as the fixture's `run_as` user, or otherwise as a fresh non-admin user named
/// `LUAU_TEST_USERNAME`, signed in with a session of their own.
pub async fn run_luau_plugin_test_file(
    plugin: &PluginUnderTest,
    test_path: &Path,
    fixture: Option<&Fixture>,
) -> anyhow::Result<()> {
    let _guard = initialize_runtime(&LibraryFixtureConfig {
        directory: plugin.directory.clone(),
        language: None,
        country: None,
    })
    .await?;

    let test_path = test_path
        .canonicalize()
        .with_context(|| format!("canonicalize Luau test {}", test_path.display()))?;
    let relative_path = test_path.strip_prefix(&plugin.directory).with_context(|| {
        format!(
            "Luau test {} is not under {}",
            test_path.display(),
            plugin.directory.display()
        )
    })?;
    let relative_path_text = relative_path.to_string_lossy().replace('\\', "/");

    if fixture.is_some_and(Fixture::rate_limit_disabled) {
        let mut startup = STATE.startup_config.get().as_ref().clone();
        startup.rate_limit.enabled = false;
        STATE.startup_config.replace(std::sync::Arc::new(startup));
    }

    if fixture.is_some_and(Fixture::auth_disabled) {
        let mut config = STATE.config().as_ref().clone();
        config.auth.enabled = false;
        services::testing::publish_config(config);
    }

    let (run_as, seeded) = {
        let mut db = STATE.db.write().await;
        let (run_as, seeded) = match fixture {
            Some(fixture) => {
                let (seeded, run_as) = fixture
                    .seed(&mut db)
                    .with_context(|| format!("seed fixture for {}", test_path.display()))?;
                (run_as, Some(seeded))
            }
            None => (None, None),
        };
        let user_db_id = match run_as {
            Some(user_db_id) => user_db_id,
            None => {
                let user = lyra_db::users::User {
                    db_id: None,
                    id: nanoid(),
                    username: LUAU_TEST_USERNAME.to_string(),
                    password: String::new(),
                };
                lyra_db::users::create(&mut db, &user)?
            }
        };
        (user_db_id, seeded)
    };
    let auth = services::testing::session_auth(run_as).await?;

    let isolated = TempPluginsDir::new(plugin)?;
    let server_info = crate::plugins::server::load_server_info().await?;
    let auth_capabilities =
        crate::plugins::auth::AuthCapabilities::from_config(&STATE.config().auth);
    let mut manifest = plugin.manifest_without(fixture.map_or(&[], Fixture::without_scopes))?;
    let plugin_id = manifest.id.clone();
    let db = STATE.db.get();
    let source_root = isolated.source_root();
    let plugins_dir = isolated.plugins_dir();
    let mut other_plugins = Vec::new();
    let mut other_manifests = Vec::new();
    if let Some(fixture) = fixture {
        for other in fixture.plugins() {
            fixture.install_plugin(other, &plugins_dir)?;
            manifest.dependencies.insert(
                other.id.clone(),
                harmony_core::plugin::Dependency {
                    version: semver::VersionReq::STAR,
                    optional: false,
                },
            );
            other_plugins.push(other.id.clone());
            other_manifests.push(library_manifest(
                &other.id,
                &other.id,
                other.scopes.clone(),
            )?);
        }
    }
    let manifests = std::iter::once(manifest)
        .chain(other_manifests)
        .collect::<Vec<_>>();
    let source = std::fs::read(&test_path)
        .with_context(|| format!("read Luau test {}", test_path.display()))?;

    // Published like the server's runtime, so the services the test calls dispatch to the
    // handlers it registers.
    let (runtime, _) = crate::plugins::executor::PluginExecutorHandle::spawn(move || {
        let runtime = crate::plugins::executor::PluginExecutor::with_filesystem_sources(
            Arc::from(manifests),
            server_info,
            auth_capabilities,
            db,
            source_root,
            plugins_dir,
        )?;
        Ok((runtime, Vec::new()))
    })?;
    crate::plugins::STATE.publish_runtime(&crate::services::STATE.generation(), runtime.clone());
    let mut seeded = seeded;
    let connections = match seeded.as_mut() {
        Some(seeded) => match seeded.register().await {
            Ok(connections) => Some(connections),
            Err(error) => {
                return Err(match runtime.shutdown().await {
                    Ok(()) => error,
                    Err(shutdown) => {
                        error.context(format!("runtime shutdown also failed: {shutdown}"))
                    }
                });
            }
        },
        None => None,
    };
    let result = async {
        // A require never starts a plugin's run, so the test's own requires need these running.
        for other in other_plugins {
            runtime
                .run_source(crate::plugins::executor::PluginSourceRun {
                    plugin_id: other,
                    path: "start.luau".to_string(),
                    source: Arc::from(&b""[..]),
                    args: Box::new(|_, _| Ok(Vec::new())),
                    auth: auth.clone(),
                })
                .await?;
        }
        runtime
            .run_source(crate::plugins::executor::PluginSourceRun {
                plugin_id,
                path: relative_path_text,
                source: Arc::from(source),
                args: Box::new(move |vm, origin| {
                    let fixture = seeded
                        .map(harmony_luau::serializable_to_luau_owned)
                        .transpose()?
                        .unwrap_or(harmony_luau::Value::Nil);
                    Ok(vec![fixture, harness::value(vm, origin)?])
                }),
                auth,
            })
            .await
    }
    .await
    .with_context(|| format!("run Luau test {}", test_path.display()));
    let shutdown = runtime.shutdown().await;
    if let Some(connections) = connections {
        connections.close().await;
    }
    result.and(shutdown)
}

pub async fn refresh_release(release_id: i64) -> anyhow::Result<()> {
    let db_id = DbId(release_id);
    let public_id = lyra_db::lookup::find_id_by_db_id(&*STATE.db.read().await, db_id)?
        .context("release has no public id")?;
    services::providers::refresh_entity_metadata(
        &services::plugin_dispatch::Published::interactive(),
        services::providers::RefreshTarget {
            db_id,
            public_id,
            viewer: services::catalog::Viewer::System,
        },
        services::providers::EntityRefreshOptions {
            covers: services::CoverSyncOptions::SKIP,
            provider_options: Default::default(),
        },
    )
    .await
    .map(|_| ())
    .map_err(|err| anyhow::anyhow!("{err:?}"))
}

pub async fn sync_provider(provider_id: &str) -> anyhow::Result<()> {
    services::providers::run_provider_sync(provider_id)
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))
}

pub async fn snapshot_fixture(prepared: &PreparedFixture) -> anyhow::Result<FixtureSnapshot> {
    let db = STATE.db.read().await;
    let track_ids: Vec<DbId> = prepared.track_ids.iter().copied().map(DbId).collect();

    let mut release_ids = BTreeSet::new();
    for track_id in &track_ids {
        for release in lyra_db::releases::get_by_track(&db, *track_id)? {
            if let Some(release_id) = release.db_id {
                release_ids.insert(DbId::from(release_id));
            }
        }
    }

    let release = match release_ids.iter().next().copied() {
        Some(release_id) => Some(snapshot_release(&db, release_id)?),
        None => None,
    };

    let mut artist_ids = BTreeSet::new();
    if let Some(release_id) = release_ids.iter().next().copied() {
        for artist in lyra_db::artists::get(&db, release_id)? {
            if let Some(artist_id) = artist.db_id {
                artist_ids.insert(DbId::from(artist_id));
            }
        }
    }
    for track_id in &track_ids {
        for artist in lyra_db::artists::get(&db, *track_id)? {
            if let Some(artist_id) = artist.db_id {
                artist_ids.insert(DbId::from(artist_id));
            }
        }
    }

    let mut artists_snapshot: Vec<EntitySnapshot> = artist_ids
        .into_iter()
        .map(|artist_id| snapshot_artist(&db, artist_id))
        .collect::<anyhow::Result<_>>()?;
    artists_snapshot.sort_by_key(|a| a.node_id);

    let mut tracks: Vec<EntitySnapshot> = track_ids
        .into_iter()
        .filter(|track_id| matches!(lyra_db::tracks::get_by_id(&db, *track_id), Ok(Some(_))))
        .map(|track_id| snapshot_track(&db, track_id))
        .collect::<anyhow::Result<_>>()?;
    tracks.sort_by_key(|a| a.node_id);

    Ok(FixtureSnapshot {
        release,
        artists: artists_snapshot,
        tracks,
    })
}

fn insert_file_entry(
    db: &mut DbAny,
    library_db_id: DbId,
    full_path: &Path,
) -> anyhow::Result<(DbId, String)> {
    let entry = Entry {
        db_id: None,
        id: nanoid(),
        full_path: full_path.to_path_buf(),
        kind: lyra_db::entries::EntryKind::File,
        file_kind: lyra_db::entries::classify_file_kind(full_path).map(ToString::to_string),
        name: full_path
            .file_name()
            .map(|name| name.to_string_lossy().to_string())
            .unwrap_or_else(|| full_path.to_string_lossy().to_string()),
        hash: None,
        size: 0,
        file_modified_at_ms: 0,
        file_created_at_ms: 0,
        synced_at_ms: None,
        missing_at_ms: None,
    };

    let qr = db.exec_mut(QueryBuilder::insert().element(&entry).query())?;
    let entry_db_id = qr
        .ids()
        .first()
        .copied()
        .ok_or_else(|| anyhow::anyhow!("entry insert missing id"))?;
    db.exec_mut(
        QueryBuilder::insert()
            .edges()
            .from(library_db_id)
            .to(entry_db_id)
            .query(),
    )?;

    Ok((entry_db_id, entry.id))
}

fn snapshot_release(db: &DbAny, release_db_id: DbId) -> anyhow::Result<EntitySnapshot> {
    let release = lyra_db::releases::get_by_id(db, release_db_id)?
        .with_context(|| format!("release {} disappeared during snapshot", release_db_id.0))?;
    snapshot_entity(db, release_db_id, &release)
}

fn snapshot_artist(db: &DbAny, artist_db_id: DbId) -> anyhow::Result<EntitySnapshot> {
    let artist = lyra_db::artists::get_by_id(db, artist_db_id)?
        .with_context(|| format!("artist {} disappeared during snapshot", artist_db_id.0))?;
    snapshot_entity(db, artist_db_id, &artist)
}

fn snapshot_track(db: &DbAny, track_db_id: DbId) -> anyhow::Result<EntitySnapshot> {
    let track = lyra_db::tracks::get_by_id(db, track_db_id)?
        .with_context(|| format!("track {} disappeared during snapshot", track_db_id.0))?;
    snapshot_entity(db, track_db_id, &track)
}

fn snapshot_credits(db: &DbAny, owner_id: DbId) -> anyhow::Result<Vec<CreditSnapshot>> {
    let mut snapshots = Vec::new();
    for link in lyra_db::credits::links_for_owner(db, owner_id)? {
        let credit_artist_ids = external_ids_map(db, link.artist_id)?;
        let artist = lyra_db::artists::get_by_id(db, link.artist_id)?
            .context("credit artist disappeared during snapshot")?;
        let artist_ext_id = credit_artist_ids
            .first_key_value()
            .map(|(_, id)| id.clone());
        snapshots.push(CreditSnapshot {
            artist_id: artist_ext_id,
            artist_name: artist.artist_name,
            credit_type: link.credit.credit_type.to_string(),
            detail: link.credit.detail,
        });
    }
    snapshots.sort_by(|a, b| {
        (&a.artist_id, &a.artist_name, &a.credit_type, &a.detail).cmp(&(
            &b.artist_id,
            &b.artist_name,
            &b.credit_type,
            &b.detail,
        ))
    });
    Ok(snapshots)
}

fn snapshot_entity<T: Serialize>(
    db: &DbAny,
    node_id: DbId,
    entity: &T,
) -> anyhow::Result<EntitySnapshot> {
    let mut fields = serialize_fields(entity)?;
    fields.extend(merged_provider_fields(db, node_id)?);
    Ok(EntitySnapshot {
        node_id: node_id.0,
        fields,
        external_ids: external_ids_map(db, node_id)?,
        credits: snapshot_credits(db, node_id)?,
    })
}

fn serialize_fields<T: Serialize>(
    entity: &T,
) -> anyhow::Result<BTreeMap<String, serde_json::Value>> {
    let fields = match serde_json::to_value(entity)? {
        serde_json::Value::Object(map) => map.into_iter().collect::<BTreeMap<_, _>>(),
        _ => anyhow::bail!("entity serialization did not produce an object"),
    };
    Ok(fields)
}

fn external_ids_map(db: &DbAny, node_id: DbId) -> anyhow::Result<BTreeMap<String, String>> {
    let mut ids = BTreeMap::new();
    for external_id in lyra_db::external_ids::get_for_entity(db, node_id)? {
        ids.insert(external_id.id_type, external_id.id_value);
    }
    Ok(ids)
}

fn merged_provider_fields(
    db: &DbAny,
    node_id: DbId,
) -> anyhow::Result<BTreeMap<String, serde_json::Value>> {
    let layers = lyra_db::metadata::layers::get_for_entity(db, node_id)?;
    if layers.is_empty() {
        return Ok(BTreeMap::new());
    }

    let providers = crate::services::providers::active_providers(db)?;
    let merged = crate::services::metadata::merging::merge_layers(layers, &providers);
    merged
        .fields
        .into_iter()
        .map(|(field, value)| {
            let value = crate::services::metadata::layers::json::value_json(db, value.as_ref())?;
            Ok((field.as_str().to_string(), value))
        })
        .collect()
}

/// A temporary directory holding the test plugin under its manifest ID.
#[must_use = "retain plugin sources while the test runtime is in use"]
pub struct TempPluginsDir {
    path: PathBuf,
}

impl TempPluginsDir {
    fn new(plugin: &PluginUnderTest) -> anyhow::Result<Self> {
        let path = std::env::temp_dir().join(format!(
            "lyra-harmony-plugins-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ));
        let isolated = Self { path };
        std::fs::create_dir_all(isolated.plugins_dir())?;
        std::fs::create_dir_all(isolated.source_root())?;
        link_or_copy_dir(&plugin.directory, &isolated.plugins_dir().join(plugin.id()))?;
        Ok(isolated)
    }

    fn source_root(&self) -> PathBuf {
        self.path.join("source")
    }

    fn plugins_dir(&self) -> PathBuf {
        self.path.join("plugins")
    }
}

impl Drop for TempPluginsDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

fn link_or_copy_dir(source: &Path, target: &Path) -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        if std::os::unix::fs::symlink(source, target).is_ok() {
            return Ok(());
        }
    }

    copy_dir_recursive(source, target)
}

fn copy_dir_recursive(source: &Path, target: &Path) -> anyhow::Result<()> {
    std::fs::create_dir_all(target)?;
    for entry in std::fs::read_dir(source)? {
        let entry = entry?;
        let entry_path = entry.path();
        let target_path = target.join(entry.file_name());
        if entry_path.is_dir() {
            copy_dir_recursive(&entry_path, &target_path)?;
        } else {
            std::fs::copy(&entry_path, &target_path)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::plugin_dispatch::MetadataRefreshRequest;

    #[tokio::test]
    async fn plugin_sources_remain_available_to_refresh_callbacks() -> anyhow::Result<()> {
        let source = tempfile::tempdir()?;
        std::fs::write(
            source.path().join("plugin.json"),
            serde_json::json!({
                "schema_version": 1,
                "id": "source-lifetime",
                "name": "Source lifetime test",
                "version": "1.0.0",
                "lyra": crate::plugins::testing::HOST_REQUIREMENT,
                "description": "",
                "entrypoint": "init.luau",
                "scopes": ["lyra.metadata"]
            })
            .to_string(),
        )?;
        std::fs::write(
            source.path().join("init.luau"),
            r#"
                local metadata = require("@lyra/metadata")
                metadata.Provider.new("source-lifetime"):refresh(metadata.EntityType.Release, function(_ctx)
                    return require("@self/value")
                end)
            "#,
        )?;
        std::fs::write(source.path().join("value.luau"), "return 42")?;
        let _guard = initialize_runtime(&LibraryFixtureConfig {
            directory: source.path().to_path_buf(),
            language: None,
            country: None,
        })
        .await?;
        let plugin = PluginUnderTest::locate(source.path())?;
        let _plugin_sources =
            exec_plugins(&plugin, harmony_http::HttpModule::module_spec()).await?;
        let handler = services::providers::provider_registry()
            .read()
            .await
            .get_refresh_callback("source-lifetime", services::EntityType::Release)
            .copied()
            .context("refresh callback")?;
        let scope = STATE.generation().latest().context("plugin runtime")?;
        let result = scope
            .dispatch()
            .dispatch_metadata_refresh(MetadataRefreshRequest {
                handler,
                context: serde_json::json!({}),
                deadline: std::time::Instant::now() + std::time::Duration::from_secs(5),
            })
            .await?;

        assert_eq!(result.values, vec![serde_json::json!(42)]);
        Ok(())
    }

    fn raw_track(directory: &Path, track: u32, artists: &[&str]) -> lyra_metadata::RawTrackTags {
        lyra_metadata::RawTrackTags {
            file_path: directory
                .join(format!("{track:02}.flac"))
                .to_string_lossy()
                .into_owned(),
            album: Some("Album".to_string()),
            album_artists: Vec::new(),
            artists: artists.iter().map(ToString::to_string).collect(),
            title: Some(format!("Track {track}")),
            date: None,
            copyright: None,
            genres: Vec::new(),
            label: None,
            catalog_number: None,
            media: None,
            barcode: None,
            disc: Some(1),
            disc_total: None,
            track: Some(track),
            track_total: None,
            duration_ms: 180_000,
            gain: Default::default(),
            title_from_tags_only: false,
            year_from_tags_only: false,
        }
    }

    #[tokio::test]
    async fn fixtures_ingest_tags_the_way_a_scan_does() -> anyhow::Result<()> {
        let library = tempfile::tempdir()?;
        let config = LibraryFixtureConfig {
            directory: library.path().to_path_buf(),
            language: None,
            country: None,
        };
        let _guard = initialize_runtime(&config).await?;

        // Track 2 comes first and spells Beta decomposed; a scan normalizes the
        // spelling and orders tracks before inferring the release's artists.
        let prepared = prepare_fixture(
            &config,
            vec![
                raw_track(library.path(), 2, &["Be\u{301}ta", "Alpha"]),
                raw_track(library.path(), 1, &["Alpha", "B\u{e9}ta"]),
            ],
        )
        .await?;

        let db = STATE.db.read().await;
        let names: Vec<String> = lyra_db::artists::get(&db, DbId(prepared.release_id))?
            .into_iter()
            .map(|artist| artist.artist_name)
            .collect();
        assert_eq!(names, vec!["Alpha".to_string(), "B\u{e9}ta".to_string()]);
        Ok(())
    }

    #[tokio::test]
    async fn fixtures_group_their_folders_the_way_a_sync_does() -> anyhow::Result<()> {
        let library = tempfile::tempdir()?;
        let config = LibraryFixtureConfig {
            directory: library.path().to_path_buf(),
            language: None,
            country: None,
        };
        let disc = |folder: &str, disc: u32| lyra_metadata::RawTrackTags {
            disc: Some(disc),
            ..raw_track(&library.path().join(folder), 1, &["Alpha"])
        };

        let _guard = initialize_runtime(&config).await?;
        let prepared = prepare_fixture(&config, vec![disc("Box/A", 1), disc("Box/B", 2)]).await?;
        assert_eq!(prepared.track_ids.len(), 2);
        drop(_guard);

        let _guard = initialize_runtime(&config).await?;
        let separate = prepare_fixture(&config, vec![disc("A", 1), disc("B", 2)]).await;
        let err = separate.expect_err("folders under the library merged");
        assert!(err.to_string().contains("2 release groups"), "{err:#}");
        Ok(())
    }
}
