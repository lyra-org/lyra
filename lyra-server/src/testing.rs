// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::{
    BTreeMap,
    BTreeSet,
    HashMap,
    HashSet,
};
use std::path::{
    Path,
    PathBuf,
};
use std::sync::Arc;
use std::sync::LazyLock;
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
use harmony_core::{
    Harmony,
    Module,
};
use nanoid::nanoid;
use serde::{
    Deserialize,
    Serialize,
};

use crate::STATE;
use crate::config::{
    self,
    Config,
};
use crate::db::{
    self,
    Entry,
    Library,
};
use crate::outbound_user_agent;
use crate::services;
use crate::services::hls::{
    cleanup,
    init as hls_init,
};
use tokio::sync::{
    Mutex,
    MutexGuard,
};

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
    pub artist_id: String,
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

static RUNTIME_TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

pub async fn runtime_test_lock() -> MutexGuard<'static, ()> {
    RUNTIME_TEST_LOCK.lock().await
}

pub async fn initialize_runtime(library: &LibraryFixtureConfig) -> anyhow::Result<()> {
    let unique_db_name = format!(
        "lyra-harmony-test-{}-{}",
        std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    );
    let config = Config {
        port: 0,
        published_url: None,
        cors: config::CorsConfig::default(),
        library: Some(config::LibraryConfig {
            path: Some(library.directory.clone()),
            language: library.language.clone(),
            country: library.country.clone(),
        }),
        covers_path: None,
        db: config::DbConfig {
            kind: config::DbKind::Memory,
            path: PathBuf::from(unique_db_name),
        },
        auth: config::AuthConfig::default(),
        sync: config::SyncConfig::default(),
        hls: config::HlsConfig::default(),
    };

    reset_runtime_state().await?;
    STATE.reset(config)?;
    harmony_http::set_default_user_agent(outbound_user_agent());
    hls_init::initialize_for_config(&STATE.config.get()).await;

    Ok(())
}

async fn reset_runtime_state() -> anyhow::Result<()> {
    crate::plugins::api::initialize_registry(HashSet::new()).await;
    crate::plugins::api::initialize_router(None).await;
    crate::plugins::runtime::initialize_registry().await;
    STATE
        .plugin_manifests
        .replace(Arc::from(Vec::<harmony_core::PluginManifest>::new()));
    crate::services::playback_sessions::reset_callback_registry_for_test().await;
    crate::services::mix::reset_mix_registry_for_test().await;
    crate::services::providers::reset_provider_registry_for_test().await;
    crate::services::libraries::reset_sync_states_for_test().await;
    crate::services::playback_sessions::reset_scopes_for_test();
    cleanup::reset_cleanup_worker_state();
    Ok(())
}

pub async fn prepare_fixture(
    library: &LibraryFixtureConfig,
    raw_tags: Vec<lyra_metadata::RawTrackTags>,
) -> anyhow::Result<PreparedFixture> {
    let processed = lyra_metadata::process_raw_tags(raw_tags);
    let fallback_source_dir = library.directory.to_string_lossy().to_string();
    let mut tracks_by_source_dir: BTreeMap<String, Vec<lyra_metadata::TrackMetadata>> =
        BTreeMap::new();
    for track in processed {
        let source_dir = track
            .file_path
            .as_deref()
            .and_then(|path| Path::new(path).parent())
            .map(|path| path.to_string_lossy().to_string())
            .unwrap_or_else(|| fallback_source_dir.clone());
        tracks_by_source_dir
            .entry(source_dir)
            .or_default()
            .push(track);
    }

    let parsed_groups: Vec<lyra_metadata::ParsedReleaseGroup<lyra_metadata::TrackMetadata>> =
        tracks_by_source_dir
            .into_iter()
            .map(|(source_dir, tracks)| lyra_metadata::ParsedReleaseGroup {
                coalesce_group_key: 0,
                source_dir,
                tracks,
            })
            .collect();
    let mut coalesced = lyra_metadata::coalesce_release_groups(parsed_groups);
    if coalesced.is_empty() {
        anyhow::bail!("raw_tags produced no coalesced release groups");
    }
    if coalesced.len() != 1 {
        anyhow::bail!(
            "raw_tags produced {} release groups; lyra-harmony-test expects exactly one release context",
            coalesced.len()
        );
    }
    let coalesced_tracks = coalesced.pop().expect("coalesced release group");

    let mut db = STATE.db.write().await;
    let created_library = db::libraries::create(
        &mut db,
        &Library {
            db_id: None,
            id: nanoid!(),
            name: "Harmony Test Library".to_string(),
            directory: library.directory.clone(),
            language: library.language.clone(),
            country: library.country.clone(),
        },
    )?;
    let library_db_id = created_library
        .db_id
        .ok_or_else(|| anyhow::anyhow!("library insert missing db_id"))?;

    let mut path_to_entry = HashMap::new();
    for track in &coalesced_tracks {
        let Some(file_path) = track.file_path.as_deref() else {
            anyhow::bail!("coalesced track is missing file_path");
        };
        let full_path = PathBuf::from(file_path);
        path_to_entry
            .entry(full_path.clone())
            .or_insert(insert_file_entry(&mut db, library_db_id, &full_path)?);
    }

    let metadata: Vec<services::metadata::TrackMetadata> = coalesced_tracks
        .into_iter()
        .map(|track| {
            let file_path = PathBuf::from(
                track
                    .file_path
                    .clone()
                    .ok_or_else(|| anyhow::anyhow!("coalesced track missing file_path"))?,
            );
            let entry_db_id = path_to_entry.get(&file_path).copied().ok_or_else(|| {
                anyhow::anyhow!("missing prepared entry for {}", file_path.display())
            })?;
            Ok(raw_track_to_service_metadata(entry_db_id, track))
        })
        .collect::<anyhow::Result<_>>()?;

    services::metadata::ingestion::apply_metadata(&mut db, library_db_id, metadata)?;

    let mut track_db_ids = BTreeSet::new();
    for entry_db_id in path_to_entry.values().copied() {
        for track in db::tracks::get_by_entry(&db, entry_db_id)? {
            if let Some(track_db_id) = track.db_id {
                track_db_ids.insert(DbId::from(track_db_id));
            }
        }
    }
    let first_track_db_id = *track_db_ids
        .first()
        .ok_or_else(|| anyhow::anyhow!("fixture ingestion produced no tracks"))?;
    let mut releases = db::releases::get_by_track(&db, first_track_db_id)?;
    let release = releases
        .pop()
        .ok_or_else(|| anyhow::anyhow!("fixture ingestion produced no release"))?;
    let release_db_id = release
        .db_id
        .ok_or_else(|| anyhow::anyhow!("fixture release missing db_id"))?;

    Ok(PreparedFixture {
        library_id: DbId::from(library_db_id).0,
        release_id: DbId::from(release_db_id).0,
        track_ids: track_db_ids.into_iter().map(|id| id.0).collect(),
    })
}

pub async fn exec_plugins(
    root_path: &Path,
    plugins_dir: PathBuf,
    http_module: Module,
    enabled_plugin_id: &str,
) -> anyhow::Result<()> {
    let isolated_plugins_dir = TempPluginsDir::new(&plugins_dir, enabled_plugin_id)?;
    let mut modules = crate::plugins::docs::runtime_modules();
    modules.retain(|module| module.path.as_ref() != "harmony/http");
    modules.push(http_module);

    let harmony = Harmony::new(
        STATE.lua.get(),
        root_path,
        Arc::from(modules),
        crate::plugins::globals::plugin_globals().into(),
        Some(crate::plugins::globals::caller_resolver()),
        Some(isolated_plugins_dir.path.clone()),
    )?;
    STATE
        .plugin_manifests
        .replace(Arc::from(harmony.plugin_manifests()));
    harmony.exec().await?;

    let mut db = STATE.db.write().await;
    for mut provider in db::providers::get(&db)? {
        provider.enabled = provider.provider_id == enabled_plugin_id;
        db::providers::upsert(&mut db, &provider)?;
    }

    Ok(())
}

pub async fn refresh_release(release_id: i64) -> anyhow::Result<()> {
    services::providers::refresh_entity_metadata(
        DbId(release_id),
        services::providers::EntityRefreshMode::MetadataOnly,
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
        for release in db::releases::get_by_track(&db, *track_id)? {
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
        for artist in db::artists::get(&db, release_id)? {
            if let Some(artist_id) = artist.db_id {
                artist_ids.insert(DbId::from(artist_id));
            }
        }
    }
    for track_id in &track_ids {
        for artist in db::artists::get(&db, *track_id)? {
            if let Some(artist_id) = artist.db_id {
                artist_ids.insert(DbId::from(artist_id));
            }
        }
    }

    let mut artists_snapshot: Vec<EntitySnapshot> = artist_ids
        .into_iter()
        .map(|artist_id| snapshot_artist(&db, artist_id))
        .collect::<anyhow::Result<_>>()?;
    artists_snapshot.sort_by(|a, b| a.node_id.cmp(&b.node_id));

    let mut tracks: Vec<EntitySnapshot> = track_ids
        .into_iter()
        .filter(|track_id| matches!(db::tracks::get_by_id(&db, *track_id), Ok(Some(_))))
        .map(|track_id| snapshot_track(&db, track_id))
        .collect::<anyhow::Result<_>>()?;
    tracks.sort_by(|a, b| a.node_id.cmp(&b.node_id));

    Ok(FixtureSnapshot {
        release,
        artists: artists_snapshot,
        tracks,
    })
}

fn raw_track_to_service_metadata(
    entry_db_id: DbId,
    track: lyra_metadata::TrackMetadata,
) -> services::metadata::TrackMetadata {
    services::metadata::TrackMetadata {
        entry_db_id,
        album: track.album,
        album_artists: track.album_artists,
        date: track.date,
        year: track.year,
        title: track.title,
        artists: track.artists,
        disc: track.disc,
        disc_total: track.disc_total,
        track: track.track,
        track_total: track.track_total,
        duration_ms: track.duration_ms,
        genres: track.genres,
        label: track.label,
        catalog_number: track.catalog_number,
        source_kind: Some("embedded_tags".to_string()),
        source_key: Some(format!("entry:{}:embedded", entry_db_id.0)),
        segment_start_ms: None,
        segment_end_ms: None,
        cue_sheet_entry_id: None,
        cue_sheet_hash: None,
        cue_track_no: None,
        cue_audio_entry_id: None,
        cue_index00_frames: None,
        cue_index01_frames: None,
        sample_rate_hz: track.sample_rate_hz,
        channel_count: track.channel_count,
        bit_depth: track.bit_depth,
        bitrate_bps: track.bitrate_bps,
    }
}

fn insert_file_entry(
    db: &mut DbAny,
    library_db_id: DbId,
    full_path: &Path,
) -> anyhow::Result<DbId> {
    let entry = Entry {
        db_id: None,
        id: nanoid!(),
        full_path: full_path.to_path_buf(),
        kind: db::entries::EntryKind::File,
        file_kind: db::entries::classify_file_kind(full_path).map(ToString::to_string),
        name: full_path
            .file_name()
            .map(|name| name.to_string_lossy().to_string())
            .unwrap_or_else(|| full_path.to_string_lossy().to_string()),
        hash: None,
        size: 0,
        mtime: 0,
        ctime: 0,
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
            .from("entries")
            .to(entry_db_id)
            .query(),
    )?;
    db.exec_mut(
        QueryBuilder::insert()
            .edges()
            .from(library_db_id)
            .to(entry_db_id)
            .query(),
    )?;

    Ok(entry_db_id)
}

fn snapshot_release(db: &DbAny, release_db_id: DbId) -> anyhow::Result<EntitySnapshot> {
    let release = db::releases::get_by_id(db, release_db_id)?
        .with_context(|| format!("release {} disappeared during snapshot", release_db_id.0))?;
    snapshot_entity(db, release_db_id, &release)
}

fn snapshot_artist(db: &DbAny, artist_db_id: DbId) -> anyhow::Result<EntitySnapshot> {
    let artist = db::artists::get_by_id(db, artist_db_id)?
        .with_context(|| format!("artist {} disappeared during snapshot", artist_db_id.0))?;
    snapshot_entity(db, artist_db_id, &artist)
}

fn snapshot_track(db: &DbAny, track_db_id: DbId) -> anyhow::Result<EntitySnapshot> {
    let track = db::tracks::get_by_id(db, track_db_id)?
        .with_context(|| format!("track {} disappeared during snapshot", track_db_id.0))?;
    snapshot_entity(db, track_db_id, &track)
}

fn snapshot_credits(db: &DbAny, owner_id: DbId) -> anyhow::Result<Vec<CreditSnapshot>> {
    let credits: Vec<db::Credit> = db
        .exec(
            agdb::QueryBuilder::select()
                .elements::<db::Credit>()
                .search()
                .from(owner_id)
                .where_()
                .neighbor()
                .end_where()
                .query(),
        )?
        .try_into()?;

    let mut snapshots = Vec::new();
    for credit in credits {
        let Some(credit_db_id) = credit.db_id.map(DbId::from) else {
            continue;
        };
        let edges = db::graph::direct_edges_from(db, credit_db_id)?;
        let credit_artist_db_id = match edges.iter().find_map(|e| e.to.filter(|id| id.0 > 0)) {
            Some(id) => id,
            None => continue,
        };
        let credit_artist_ids = external_ids_map(db, credit_artist_db_id)?;
        let Some((_, artist_ext_id)) = credit_artist_ids.first_key_value() else {
            continue;
        };
        snapshots.push(CreditSnapshot {
            artist_id: artist_ext_id.clone(),
            credit_type: credit.credit_type.to_string(),
            detail: credit.detail,
        });
    }
    snapshots.sort_by(|a, b| (&a.artist_id, &a.credit_type).cmp(&(&b.artist_id, &b.credit_type)));
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
    let mut fields = match serde_json::to_value(entity)? {
        serde_json::Value::Object(map) => map.into_iter().collect::<BTreeMap<_, _>>(),
        _ => anyhow::bail!("entity serialization did not produce an object"),
    };
    fields.remove("db_id");
    Ok(fields)
}

fn external_ids_map(db: &DbAny, node_id: DbId) -> anyhow::Result<BTreeMap<String, String>> {
    let mut ids = BTreeMap::new();
    for external_id in db::external_ids::get_for_entity(db, node_id)? {
        ids.insert(external_id.id_type, external_id.id_value);
    }
    Ok(ids)
}

fn merged_provider_fields(
    db: &DbAny,
    node_id: DbId,
) -> anyhow::Result<BTreeMap<String, serde_json::Value>> {
    let layers = db::metadata::layers::get_for_entity(db, node_id)?;
    if layers.is_empty() {
        return Ok(BTreeMap::new());
    }

    let providers = db::providers::get(db)?;
    let merged = crate::services::metadata::merging::merge_layers(layers, &providers);
    Ok(merged.fields.into_iter().collect())
}

struct TempPluginsDir {
    path: PathBuf,
}

impl TempPluginsDir {
    fn new(source_plugins_dir: &Path, plugin_id: &str) -> anyhow::Result<Self> {
        let path = std::env::temp_dir().join(format!(
            "lyra-harmony-plugins-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ));
        std::fs::create_dir_all(&path)?;

        let source_plugin_dir = source_plugins_dir.join(plugin_id);
        if !source_plugin_dir.is_dir() {
            anyhow::bail!(
                "plugin '{}' not found in {}",
                plugin_id,
                source_plugins_dir.display()
            );
        }

        let target_plugin_dir = path.join(plugin_id);
        link_or_copy_dir(&source_plugin_dir, &target_plugin_dir)?;

        Ok(Self { path })
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
