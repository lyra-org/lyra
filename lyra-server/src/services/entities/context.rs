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
            .map(|library| library.directory.to_string_lossy().to_string())
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
    flatten_projection_for_provider_context(projection)
}

fn build_artist_context(db: &DbAny, entity_id: DbId) -> anyhow::Result<Value> {
    let projection = project_entity(
        db,
        QueryId::Id(entity_id),
        &[EntityInclude::ExternalIds],
        None,
    )?;
    flatten_projection_for_provider_context(projection)
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
