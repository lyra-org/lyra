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
use std::collections::HashMap;

use crate::db;

use crate::services::EntityType;

use super::{
    EntityInclude,
    EntityProjectionInfo,
    projection::{
        DetectedEntityType,
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
    let rows = db::metadata::custom_fields::get_for_entity(db, entity_id)?;
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

fn attach_custom_fields_to_context(db: &DbAny, context: &mut Value) -> anyhow::Result<()> {
    fn attach(
        db: &DbAny,
        value: &mut Value,
        cache: &mut HashMap<i64, Option<Value>>,
    ) -> anyhow::Result<()> {
        match value {
            Value::Object(object) => {
                for (key, child) in object.iter_mut() {
                    if key != "custom_fields" {
                        attach(db, child, cache)?;
                    }
                }

                if let Some(entity_id) = object.get("db_id").and_then(|v| v.as_i64())
                    && entity_id > 0
                {
                    let custom_fields = if let Some(cached) = cache.get(&entity_id) {
                        cached.clone()
                    } else {
                        let loaded = custom_fields_value_for_entity(db, DbId(entity_id))?;
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
                    attach(db, child, cache)?;
                }
            }
            _ => {}
        }

        Ok(())
    }

    let mut cache = HashMap::new();
    attach(db, context, &mut cache)
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

pub(crate) fn build_release_context(
    db: &DbAny,
    entity_id: DbId,
    library_id: Option<DbId>,
) -> anyhow::Result<Value> {
    let library_root = if let Some(lib_id) = library_id {
        db::libraries::get_by_id(db, lib_id)?
            .map(|library| library.path.to_string_lossy().to_string())
    } else {
        None
    };
    let release = db::releases::get_by_id(db, entity_id)?
        .ok_or_else(|| anyhow::anyhow!("release not found: {}", entity_id.0))?;
    let projection = project_release(
        db,
        entity_id,
        release,
        &[
            EntityInclude::Tracks,
            EntityInclude::Artists,
            EntityInclude::ExternalIds,
        ],
        library_root.as_deref(),
        &PreFetchedIncludes::default(),
    )?;
    let mut context =
        flatten_projection_for_provider_context(EntityProjectionInfo::Release(projection))?;
    if let (Value::Object(map), Some(lib_id)) = (&mut context, library_id) {
        map.insert("library_id".to_string(), serde_json::json!(lib_id.0));
    }
    attach_custom_fields_to_context(db, &mut context)?;
    Ok(context)
}

fn build_track_context(db: &DbAny, entity_id: DbId) -> anyhow::Result<Value> {
    let projection = project_entity(
        db,
        QueryId::Id(entity_id),
        &[
            EntityInclude::Releases,
            EntityInclude::Artists,
            EntityInclude::ExternalIds,
        ],
        None,
    )?;
    let mut context = flatten_projection_for_provider_context(projection)?;
    attach_custom_fields_to_context(db, &mut context)?;
    Ok(context)
}

fn build_artist_context(db: &DbAny, entity_id: DbId) -> anyhow::Result<Value> {
    let projection = project_entity(
        db,
        QueryId::Id(entity_id),
        &[EntityInclude::ExternalIds],
        None,
    )?;
    let mut context = flatten_projection_for_provider_context(projection)?;
    attach_custom_fields_to_context(db, &mut context)?;
    Ok(context)
}

pub(crate) fn build_entity_provider_context(
    db: &DbAny,
    entity_id: DbId,
    library_id: Option<DbId>,
) -> Result<(EntityType, Value), EntityContextError> {
    let result = db
        .exec(QueryBuilder::select().ids(entity_id).query())
        .map_err(anyhow::Error::from)?;
    let Some(element) = result.elements.into_iter().next() else {
        return Err(EntityContextError::EntityNotFound(entity_id.0));
    };
    let entity_type = detect_entity_type(&element).map_err(anyhow::Error::from)?;

    match entity_type {
        DetectedEntityType::Release => Ok((
            EntityType::Release,
            build_release_context(db, entity_id, library_id).map_err(anyhow::Error::from)?,
        )),
        DetectedEntityType::Track => Ok((
            EntityType::Track,
            build_track_context(db, entity_id).map_err(anyhow::Error::from)?,
        )),
        DetectedEntityType::Artist => Ok((
            EntityType::Artist,
            build_artist_context(db, entity_id).map_err(anyhow::Error::from)?,
        )),
    }
}
