// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashMap;

use agdb::DbId;
use mlua::LuaSerdeExt;
use serde::Serialize;

use crate::{
    STATE,
    db::{
        self,
    },
    plugins::LUA_SERIALIZE_OPTIONS,
    services::{
        EntityType,
        deduplicate_artists_by_external_id,
        entities::build_release_context,
        metadata::{
            extract_raw_tags_from_lofty,
            mapping::MetadataMappingConfig,
            read_audio_tags,
        },
        providers::PROVIDER_REGISTRY,
    },
};

#[derive(Serialize)]
struct CaptureFile {
    /// Replay must use the same version — see
    /// [`crate::db::metadata::mapping_config`].
    mapping_version: u64,
    library: CaptureLibrary,
    providers: Vec<String>,
    releases: Vec<CapturedRelease>,
}

#[derive(Serialize)]
struct CaptureLibrary {
    directory: String,
    language: Option<String>,
    country: Option<String>,
}

#[derive(Serialize)]
struct CapturedRelease {
    context: serde_json::Value,
    raw_tags: Vec<lyra_metadata::RawTrackTags>,
    results: HashMap<String, HashMap<String, CapturedEntity>>,
}

#[derive(Serialize)]
struct CapturedEntity {
    ids: HashMap<String, String>,
    fields: HashMap<String, serde_json::Value>,
}

async fn load_mapping_config() -> anyhow::Result<MetadataMappingConfig> {
    {
        let db = STATE.db.read().await;
        if let Some(cfg) = crate::db::metadata::mapping_config::get(&db)? {
            return Ok(cfg);
        }
    }
    let mut db = STATE.db.write().await;
    crate::db::metadata::mapping_config::ensure(&mut db)
}

/// Extract raw tags for all tracks in a release by reading their file entries from the DB.
async fn extract_raw_tags_for_release(
    release_db_id: DbId,
    mapping_config: &MetadataMappingConfig,
) -> anyhow::Result<Vec<lyra_metadata::RawTrackTags>> {
    let (tracks, entries_by_track) = {
        let db = STATE.db.read().await;
        let tracks = db::tracks::get(&db, release_db_id)?;
        let track_ids: Vec<DbId> = tracks
            .iter()
            .filter_map(|t| t.db_id.clone().map(Into::into))
            .collect();
        let mut entries_by_track = HashMap::new();
        for &track_id in &track_ids {
            entries_by_track.insert(track_id, db::entries::get_by_track(&db, track_id)?);
        }
        (tracks, entries_by_track)
    };

    let mut raw_tags = Vec::new();
    for track in tracks {
        let Some(track_db_id) = track.db_id.map(Into::into) else {
            continue;
        };
        let entries = entries_by_track
            .get(&track_db_id)
            .cloned()
            .unwrap_or_default();
        for entry in entries {
            if entry.kind != db::entries::EntryKind::File {
                continue;
            }
            let path = entry.full_path.clone();
            let task_result = tokio::task::spawn_blocking(move || read_audio_tags(path)).await;
            match task_result {
                Ok(Ok((tag, tagged_file))) => {
                    let raw = extract_raw_tags_from_lofty(
                        &tag,
                        &tagged_file,
                        &entry.full_path.to_string_lossy(),
                        mapping_config,
                    );
                    raw_tags.push(raw);
                }
                Ok(Err(err)) => {
                    tracing::warn!(
                        path = %entry.full_path.display(),
                        "skipping raw tags: {err}"
                    );
                }
                Err(err) => {
                    tracing::warn!(
                        path = %entry.full_path.display(),
                        "raw tag task failed: {err}"
                    );
                }
            }
        }
    }
    Ok(raw_tags)
}

pub(crate) async fn run_capture(library_db_id: DbId, output_path: &str) -> anyhow::Result<()> {
    let (library, releases) = {
        let db = STATE.db.read().await;
        let library = db::libraries::get_by_id(&db, library_db_id)?
            .ok_or_else(|| anyhow::anyhow!("Library not found: {}", library_db_id.0))?;
        let releases = db::releases::get(&db, library_db_id)?;
        (library, releases)
    };

    let providers = {
        let db = STATE.db.read().await;
        db::providers::get(&db)?
            .into_iter()
            .filter(|p| p.enabled)
            .collect::<Vec<_>>()
    };

    let provider_ids: Vec<String> = providers.iter().map(|p| p.provider_id.clone()).collect();

    let capture_library = CaptureLibrary {
        directory: library.directory.to_string_lossy().to_string(),
        language: library.language.clone(),
        country: library.country.clone(),
    };

    let total = releases.len();

    // Snapshot the mapping config once at the start so extraction
    // and the file-header version field agree even if an admin
    // commits a new mapping mid-capture.
    let mapping_config = load_mapping_config().await?;

    // Pass 1: Snapshot all contexts and raw tags from the clean post-scan DB state,
    // before any handlers run and potentially corrupt shared entities.
    let mut release_snapshots: Vec<(DbId, serde_json::Value, Vec<lyra_metadata::RawTrackTags>)> =
        Vec::new();
    for release in releases {
        let Some(release_db_id) = release.db_id.clone() else {
            continue;
        };
        let node_id: DbId = release_db_id.into();
        let context = {
            let db = STATE.db.read().await;
            build_release_context(&db, node_id, library.db_id)?
        };
        let raw_tags = extract_raw_tags_for_release(node_id, &mapping_config).await?;
        release_snapshots.push((node_id, context, raw_tags));
    }

    // Collect handlers once
    let handlers: Vec<(String, crate::plugins::lifecycle::PluginFunctionHandle)> = {
        let registry = PROVIDER_REGISTRY.read().await;
        providers
            .iter()
            .filter_map(|p| {
                registry
                    .get_refresh_handler(&p.provider_id, EntityType::Release)
                    .map(|h| (p.provider_id.clone(), h.clone()))
            })
            .collect()
    };

    // Pass 2: Run handlers and collect results using the pre-handler contexts.
    let mut captured_releases = Vec::new();
    for (i, (_node_id, context, raw_tags)) in release_snapshots.into_iter().enumerate() {
        let release_title = context
            .get("release_title")
            .and_then(|v: &serde_json::Value| v.as_str())
            .unwrap_or("?");
        tracing::info!("[{}/{}] {}", i + 1, total, release_title);

        for (_provider_id, handler) in &handlers {
            let Some(lua) = handler.try_upgrade_lua() else {
                tracing::warn!("capture handler's lua instance is no longer valid, skipping");
                continue;
            };
            let lua_context = lua.to_value_with(&context, LUA_SERIALIZE_OPTIONS)?;
            if let Err(err) = handler.call_async::<_, ()>(lua_context).await {
                tracing::warn!("handler failed: {err}");
            }
        }

        let entity_db_ids = collect_release_context_node_ids(&context);

        let mut results: HashMap<String, HashMap<String, CapturedEntity>> = HashMap::new();

        for provider_id in &provider_ids {
            let db = STATE.db.read().await;
            let mut entities = HashMap::new();

            for &eid in &entity_db_ids {
                if let Some(captured) = capture_entity_results(&db, DbId(eid), provider_id)? {
                    entities.insert(eid.to_string(), captured);
                }
            }

            if !entities.is_empty() {
                results.insert(provider_id.clone(), entities);
            }
        }

        captured_releases.push(CapturedRelease {
            context,
            raw_tags,
            results,
        });
    }

    // Deduplicate artists after full refresh
    {
        let mut db_write = STATE.db.write().await;
        if let Err(err) = deduplicate_artists_by_external_id(&mut db_write) {
            tracing::warn!("artist deduplication failed: {err}");
        }
    }

    let capture_file = CaptureFile {
        mapping_version: mapping_config.version,
        library: capture_library,
        providers: provider_ids,
        releases: captured_releases,
    };

    let json = serde_json::to_string_pretty(&capture_file)?;
    std::fs::write(output_path, json)?;
    tracing::info!("capture written to {output_path} ({total} releases)");

    Ok(())
}

/// Extract all `db_id` values from a release context JSON.
fn collect_release_context_node_ids(context: &serde_json::Value) -> Vec<i64> {
    let mut ids = Vec::new();

    if let Some(id) = context.get("db_id").and_then(|v| v.as_i64()) {
        ids.push(id);
    }

    if let Some(artists) = context.get("artists").and_then(|v| v.as_array()) {
        for artist in artists {
            if let Some(id) = artist.get("db_id").and_then(|v| v.as_i64()) {
                ids.push(id);
            }
        }
    }

    if let Some(tracks) = context.get("tracks").and_then(|v| v.as_array()) {
        for track in tracks {
            if let Some(id) = track.get("db_id").and_then(|v| v.as_i64()) {
                ids.push(id);
            }
            if let Some(track_artists) = track.get("artists").and_then(|v| v.as_array()) {
                for artist in track_artists {
                    if let Some(id) = artist.get("db_id").and_then(|v| v.as_i64()) {
                        ids.push(id);
                    }
                }
            }
        }
    }

    ids.sort();
    ids.dedup();
    ids
}

/// Read metadata layers and external IDs for a single entity+provider from the DB.
fn capture_entity_results(
    db: &agdb::DbAny,
    node_id: DbId,
    provider_id: &str,
) -> anyhow::Result<Option<CapturedEntity>> {
    let layers = db::metadata::layers::get_for_entity(db, node_id)?;
    let ext_ids = db::external_ids::get_for_entity(db, node_id)?;

    let mut fields = HashMap::new();
    for layer in &layers {
        if layer.provider_id == provider_id {
            if let Ok(parsed) =
                serde_json::from_str::<HashMap<String, serde_json::Value>>(&layer.fields)
            {
                fields.extend(parsed);
            }
        }
    }

    let mut ids = HashMap::new();
    for ext_id in &ext_ids {
        if ext_id.provider_id == provider_id {
            ids.insert(ext_id.id_type.clone(), ext_id.id_value.clone());
        }
    }

    if fields.is_empty() && ids.is_empty() {
        Ok(None)
    } else {
        Ok(Some(CapturedEntity { ids, fields }))
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::collect_release_context_node_ids;

    #[test]
    fn collect_release_context_node_ids_reads_release_track_and_artist_ids() {
        let context = json!({
            "db_id": 1,
            "artists": [
                { "db_id": 2 },
                { "db_id": 3 }
            ],
            "tracks": [
                {
                    "db_id": 4,
                    "artists": [
                        { "db_id": 3 },
                        { "db_id": 5 }
                    ]
                },
                {
                    "db_id": 6
                }
            ]
        });

        assert_eq!(
            collect_release_context_node_ids(&context),
            vec![1, 2, 3, 4, 5, 6]
        );
    }
}
