// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::{
    DbAny,
    DbId,
    QueryBuilder,
    QueryId,
};
use serde::Serialize;
use serde_json::{
    Map,
    Value,
};
use std::collections::{
    HashMap,
    hash_map::Entry,
};

use crate::services::EntityType;
use crate::services::providers::IdSchemes;

use lyra_db::{
    Artist,
    NodeId,
    Release,
};

use super::{
    CreditedArtistProjectionInfo,
    EntityInclude,
    EntityProjectionInfo,
    projection::{
        DetectedEntityType,
        IdIncludes,
        PreFetchedIncludes,
        detect_entity_type,
        project_entity,
        project_release,
    },
};

#[derive(Debug, thiserror::Error)]
pub(crate) enum EntityContextError {
    #[error("entity not found: {0}")]
    EntityNotFound(i64),
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

fn insert_optional<T: Serialize>(
    object: &mut Map<String, Value>,
    key: &str,
    value: Option<T>,
) -> anyhow::Result<()> {
    if let Some(v) = value {
        object.insert(key.to_string(), serde_json::to_value(v)?);
    }
    Ok(())
}

fn json_object<T: Serialize>(value: T) -> anyhow::Result<Map<String, Value>> {
    let Value::Object(object) = serde_json::to_value(value)? else {
        anyhow::bail!("serialized entity projection base is not an object");
    };

    Ok(object)
}

fn merge_includes<T: Serialize>(
    object: &mut Map<String, Value>,
    includes: T,
) -> anyhow::Result<()> {
    let Value::Object(includes_map) = serde_json::to_value(includes)? else {
        anyhow::bail!("serialized includes is not an object");
    };
    for (key, value) in includes_map {
        if !value.is_null() {
            object.insert(key, value);
        }
    }
    Ok(())
}

fn custom_fields_value_for_entity(db: &DbAny, entity_id: DbId) -> anyhow::Result<Option<Value>> {
    let rows = lyra_db::metadata::custom_fields::get_for_entity(db, entity_id)?;
    if rows.is_empty() {
        return Ok(None);
    }

    let mut providers = Map::new();
    for row in rows {
        let fields = match serde_json::from_str::<Map<String, Value>>(&row.fields) {
            Ok(fields) => fields,
            Err(err) => {
                tracing::warn!(
                    provider_id = %row.provider_id,
                    version = row.version,
                    error = %err,
                    "failed to parse provider custom fields as JSON object, skipping row"
                );
                continue;
            }
        };
        let provider_entry = providers
            .entry(row.provider_id)
            .or_insert_with(|| Value::Object(Map::new()));
        if let Value::Object(versions) = provider_entry {
            versions.insert(format!("v{}", row.version), Value::Object(fields));
        }
    }

    if providers.is_empty() {
        Ok(None)
    } else {
        Ok(Some(Value::Object(providers)))
    }
}

/// The database id of each entity in `projection`, by its public id, taken before the projection
/// is serialized without them.
fn projection_db_ids(projection: &EntityProjectionInfo) -> HashMap<String, DbId> {
    fn add(ids: &mut HashMap<String, DbId>, db_id: &Option<NodeId>, id: &str) {
        if let Some(db_id) = db_id.clone() {
            ids.insert(id.to_string(), db_id.into());
        }
    }
    fn add_artists<'a>(
        ids: &mut HashMap<String, DbId>,
        artists: impl IntoIterator<Item = &'a Artist>,
    ) {
        for artist in artists {
            add(ids, &artist.db_id, &artist.id);
        }
    }
    fn add_credits(
        ids: &mut HashMap<String, DbId>,
        credits: &Option<Vec<CreditedArtistProjectionInfo>>,
    ) {
        add_artists(ids, credits.iter().flatten().map(|credit| &credit.artist));
    }
    fn add_releases(ids: &mut HashMap<String, DbId>, releases: &Option<Vec<Release>>) {
        for release in releases.iter().flatten() {
            add(ids, &release.db_id, &release.id);
        }
    }

    let mut ids = HashMap::new();
    match projection {
        EntityProjectionInfo::Release(projection) => {
            add(&mut ids, &projection.entity.db_id, &projection.entity.id);
            let includes = &projection.includes;
            add_artists(&mut ids, includes.artists.iter().flatten());
            for track in includes.tracks.iter().flatten() {
                add(&mut ids, &track.db_id, &track.id);
                add_artists(&mut ids, &track.artists);
            }
            add_credits(&mut ids, &includes.credits);
        }
        EntityProjectionInfo::Track(projection) => {
            add(&mut ids, &projection.entity.db_id, &projection.entity.id);
            let includes = &projection.includes;
            add_releases(&mut ids, &includes.releases);
            add_artists(&mut ids, includes.artists.iter().flatten());
            for entry in includes.entries.iter().flatten() {
                add(&mut ids, &entry.db_id, &entry.id);
            }
            add_credits(&mut ids, &includes.credits);
        }
        EntityProjectionInfo::Artist(projection) => {
            add(&mut ids, &projection.entity.db_id, &projection.entity.id);
            let includes = &projection.includes;
            add_releases(&mut ids, &includes.releases);
            for track in includes.tracks.iter().flatten() {
                add(&mut ids, &track.db_id, &track.id);
            }
        }
    }
    ids
}

/// The entity an object of a serialized projection describes, by its public `id`.
fn object_entity(object: &Map<String, Value>, ids: &HashMap<String, DbId>) -> Option<DbId> {
    ids.get(object.get("id")?.as_str()?).copied()
}

fn attach_custom_fields_to_context(
    db: &DbAny,
    context: &mut Value,
    ids: &HashMap<String, DbId>,
) -> anyhow::Result<()> {
    fn attach(
        db: &DbAny,
        value: &mut Value,
        ids: &HashMap<String, DbId>,
        cache: &mut HashMap<DbId, Option<Value>>,
    ) -> anyhow::Result<()> {
        match value {
            Value::Object(object) => {
                for (key, child) in object.iter_mut() {
                    if key != "custom_fields" {
                        attach(db, child, ids, cache)?;
                    }
                }

                if let Some(entity_id) = object_entity(object, ids) {
                    let custom_fields = if let Some(cached) = cache.get(&entity_id) {
                        cached.clone()
                    } else {
                        let loaded = custom_fields_value_for_entity(db, entity_id)?;
                        cache.insert(entity_id, loaded.clone());
                        loaded
                    };
                    if let Some(custom_fields) = custom_fields {
                        object.insert("custom_fields".to_string(), custom_fields);
                    }
                }
            }
            Value::Array(items) => {
                for child in items {
                    attach(db, child, ids, cache)?;
                }
            }
            _ => {}
        }

        Ok(())
    }

    let mut cache = HashMap::new();
    attach(db, context, ids, &mut cache)
}

fn flatten_projection_for_provider_context(
    projection: EntityProjectionInfo,
) -> anyhow::Result<Value> {
    match projection {
        EntityProjectionInfo::Release(projection) => {
            let mut object = json_object(projection.entity)?;
            insert_optional(&mut object, "lookup_hints", Some(projection.lookup_hints))?;
            merge_includes(&mut object, projection.includes)?;
            Ok(Value::Object(object))
        }
        EntityProjectionInfo::Track(projection) => {
            let mut object = json_object(projection.entity)?;
            merge_includes(&mut object, projection.includes)?;
            Ok(Value::Object(object))
        }
        EntityProjectionInfo::Artist(projection) => {
            let mut object = json_object(projection.entity)?;
            merge_includes(&mut object, projection.includes)?;
            Ok(Value::Object(object))
        }
    }
}

/// Gives each entity in the context its `external_ids` by scheme, and the `unmatched` and
/// `user_set` schemes, reading each entity's ids once, so every entity carries the same ones.
fn attach_ids_to_context(
    db: &DbAny,
    context: &mut Value,
    ids: &HashMap<String, DbId>,
    schemes: &IdSchemes,
) -> anyhow::Result<()> {
    const ID_KEYS: [&str; 3] = ["external_ids", "unmatched", "user_set"];

    fn entity_ids_value(
        db: &DbAny,
        entity_id: DbId,
        schemes: &IdSchemes,
    ) -> anyhow::Result<Map<String, Value>> {
        let rows = lyra_db::external_ids::get_for_entity(db, entity_id)?;
        json_object(schemes.refresh_ids(&rows))
    }

    fn attach(
        db: &DbAny,
        value: &mut Value,
        ids: &HashMap<String, DbId>,
        schemes: &IdSchemes,
        cache: &mut HashMap<DbId, Map<String, Value>>,
    ) -> anyhow::Result<()> {
        match value {
            Value::Object(object) => {
                for (key, child) in object.iter_mut() {
                    if key != "entries"
                        && key != "custom_fields"
                        && !ID_KEYS.contains(&key.as_str())
                    {
                        attach(db, child, ids, schemes, cache)?;
                    }
                }

                if let Some(entity_id) = object_entity(object, ids) {
                    let entity_ids = match cache.entry(entity_id) {
                        Entry::Occupied(entry) => entry.into_mut(),
                        Entry::Vacant(entry) => {
                            entry.insert(entity_ids_value(db, entity_id, schemes)?)
                        }
                    };
                    for (key, value) in entity_ids.iter() {
                        object.insert(key.clone(), value.clone());
                    }
                }
            }
            Value::Array(items) => {
                for child in items {
                    attach(db, child, ids, schemes, cache)?;
                }
            }
            _ => {}
        }

        Ok(())
    }

    let mut cache = HashMap::new();
    attach(db, context, ids, schemes, &mut cache)
}

/// `projection` flattened for a provider, with each entity's custom fields and ids.
fn provider_context(
    db: &DbAny,
    projection: EntityProjectionInfo,
    schemes: &IdSchemes,
) -> anyhow::Result<Value> {
    let ids = projection_db_ids(&projection);
    let mut context = flatten_projection_for_provider_context(projection)?;
    attach_custom_fields_to_context(db, &mut context, &ids)?;
    attach_ids_to_context(db, &mut context, &ids, schemes)?;
    Ok(context)
}

pub(crate) fn build_release_context(
    db: &DbAny,
    entity_id: DbId,
    library_id: Option<DbId>,
    schemes: &IdSchemes,
) -> anyhow::Result<Value> {
    let library_root = if let Some(lib_id) = library_id {
        lyra_db::libraries::get_by_id(db, lib_id)?
            .map(|library| library.path.to_string_lossy().to_string())
    } else {
        None
    };
    let release = lyra_db::releases::get_by_id(db, entity_id)?
        .ok_or_else(|| anyhow::anyhow!("release not found: {}", entity_id.0))?;
    let projection = project_release(
        db,
        entity_id,
        release,
        &[EntityInclude::Tracks, EntityInclude::Artists],
        library_root.as_deref(),
        &PreFetchedIncludes::default(),
        &mut IdIncludes::new(schemes, &[]),
    )?;
    let mut context = provider_context(db, EntityProjectionInfo::Release(projection), schemes)?;
    if let (Value::Object(map), Some(lib_id)) = (&mut context, library_id)
        && let Some(library_public_id) = lyra_db::lookup::find_id_by_db_id(db, lib_id)?
    {
        map.insert("library_id".to_string(), Value::String(library_public_id));
    }
    Ok(context)
}

fn build_track_context(db: &DbAny, entity_id: DbId, schemes: &IdSchemes) -> anyhow::Result<Value> {
    let projection = project_entity(
        db,
        QueryId::Id(entity_id),
        &[EntityInclude::Releases, EntityInclude::Artists],
        None,
        &mut IdIncludes::new(schemes, &[]),
    )?;
    provider_context(db, projection, schemes)
}

fn build_artist_context(db: &DbAny, entity_id: DbId, schemes: &IdSchemes) -> anyhow::Result<Value> {
    let projection = project_entity(
        db,
        QueryId::Id(entity_id),
        &[],
        None,
        &mut IdIncludes::new(schemes, &[]),
    )?;
    provider_context(db, projection, schemes)
}

pub(crate) fn build_entity_provider_context(
    db: &DbAny,
    entity_id: DbId,
    library_id: Option<DbId>,
    schemes: &IdSchemes,
) -> Result<(EntityType, Value), EntityContextError> {
    let result = db
        .exec(QueryBuilder::select().ids(entity_id).query())
        .map_err(anyhow::Error::from)?;
    let Some(element) = result.elements.into_iter().next() else {
        return Err(EntityContextError::EntityNotFound(entity_id.0));
    };
    let entity_type = detect_entity_type(&element)?;

    match entity_type {
        DetectedEntityType::Release => Ok((
            EntityType::Release,
            build_release_context(db, entity_id, library_id, schemes)?,
        )),
        DetectedEntityType::Track => Ok((
            EntityType::Track,
            build_track_context(db, entity_id, schemes)?,
        )),
        DetectedEntityType::Artist => Ok((
            EntityType::Artist,
            build_artist_context(db, entity_id, schemes)?,
        )),
    }
}

#[cfg(test)]
mod tests {
    use lyra_db::IdSource;
    use lyra_db::fixtures::{
        connect,
        insert_artist,
        insert_release,
        insert_track,
    };
    use lyra_db::metadata::custom_fields::ProviderCustomFields;
    use lyra_db::test_db::{
        connect_artist,
        new_test_db,
    };

    use super::*;

    #[test]
    fn custom_fields_reach_the_context_as_stored() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release = insert_release(&mut db, "Release")?;
        let track = insert_track(&mut db, "Track")?;
        connect(&mut db, release, track)?;
        for node in [release, track] {
            lyra_db::metadata::custom_fields::upsert(
                &mut db,
                node,
                &ProviderCustomFields {
                    db_id: None,
                    provider_id: "provider".to_string(),
                    version: 1,
                    fields: r#"{"reference":{"db_id":"remote-123"}}"#.to_string(),
                    updated_at_ms: 0,
                },
            )?;
        }

        let context = build_release_context(&db, release, None, &IdSchemes::default())?;

        let expected =
            serde_json::json!({ "provider": { "v1": { "reference": { "db_id": "remote-123" } } } });
        assert_eq!(context["custom_fields"], expected);
        assert_eq!(context["tracks"][0]["custom_fields"], expected);
        assert!(context.get("db_id").is_none());
        assert!(context["tracks"][0].get("db_id").is_none());
        Ok(())
    }

    #[test]
    fn every_entity_carries_its_ids_by_scheme() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release = insert_release(&mut db, "Release")?;
        let artist = insert_artist(&mut db, "Artist")?;
        connect_artist(&mut db, release, artist)?;
        let track = insert_track(&mut db, "Track")?;
        connect(&mut db, release, track)?;
        connect_artist(&mut db, track, artist)?;
        for (entity, provider_id, id_type, value, source) in [
            (
                artist,
                "musicbrainz",
                "artist_id",
                "mbid-1",
                IdSource::Resolved,
            ),
            (artist, "theaudiodb", "artist_id", "123", IdSource::Manual),
            (release, "musicbrainz", "release_id", "", IdSource::Resolved),
            (
                track,
                "musicbrainz",
                "recording_id",
                "rec-1",
                IdSource::Resolved,
            ),
        ] {
            lyra_db::external_ids::upsert(&mut db, entity, provider_id, id_type, value, source)?;
        }
        let mut schemes = IdSchemes::default();
        schemes.insert("musicbrainz", "artist_id", "musicbrainz:artist");
        schemes.insert("musicbrainz", "release_id", "musicbrainz:release");
        schemes.insert("musicbrainz", "recording_id", "musicbrainz:recording");
        schemes.insert("theaudiodb", "artist_id", "theaudiodb:artist");

        let context = build_release_context(&db, release, None, &schemes)?;

        let artist = &context["artists"][0];
        assert_eq!(
            artist["external_ids"],
            serde_json::json!({ "musicbrainz:artist": "mbid-1", "theaudiodb:artist": "123" })
        );
        assert_eq!(artist["user_set"], serde_json::json!(["theaudiodb:artist"]));
        assert_eq!(artist["unmatched"], serde_json::json!([]));
        assert_eq!(
            &context["tracks"][0]["artists"][0], artist,
            "a track's artist carries the same ids"
        );
        assert_eq!(context["external_ids"], serde_json::json!({}));
        assert_eq!(
            context["unmatched"],
            serde_json::json!(["musicbrainz:release"])
        );
        let track = &context["tracks"][0];
        assert_eq!(
            track["external_ids"],
            serde_json::json!({ "musicbrainz:recording": "rec-1" })
        );
        for entity in [&context, track, artist] {
            assert!(entity.get("identifiers").is_none());
            assert!(entity.get("manual_ids").is_none());
        }
        Ok(())
    }
}
