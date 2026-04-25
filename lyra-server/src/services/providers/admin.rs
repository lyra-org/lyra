// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use crate::{
    STATE,
    db::{
        self,
        IdSource,
        ProviderConfig,
    },
};

use super::{
    EntityRefreshMode,
    EntityRefreshResult,
    ProviderServiceError,
    refresh_entity_metadata,
};

#[derive(Debug, thiserror::Error)]
pub(crate) enum ProviderAdminError {
    #[error("Provider not found: {0}")]
    ProviderNotFound(String),
    #[error("Entity not found: {0}")]
    EntityNotFound(String),
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

#[derive(Clone, Debug)]
pub(crate) struct EntityExternalIdRecord {
    pub(crate) provider_id: String,
    pub(crate) id_type: String,
    pub(crate) id_value: String,
    pub(crate) source: String,
}

#[derive(Clone, Debug)]
pub(crate) struct SetEntityExternalIdRequest {
    pub(crate) provider_id: String,
    pub(crate) id_type: String,
    pub(crate) id_value: String,
}

pub(crate) async fn list_provider_configs() -> Result<Vec<ProviderConfig>, ProviderAdminError> {
    let db = STATE.db.read().await;
    Ok(db::providers::get(&db)?)
}

pub(crate) async fn update_provider_priority(
    provider_id: &str,
    priority: u32,
) -> Result<ProviderConfig, ProviderAdminError> {
    let mut db = STATE.db.write().await;
    db::providers::update_priority(&mut db, provider_id, priority)?;

    db::providers::get(&db)?
        .into_iter()
        .find(|provider| provider.provider_id == provider_id)
        .ok_or_else(|| ProviderAdminError::ProviderNotFound(provider_id.to_string()))
}

pub(crate) async fn list_entity_external_ids(
    node_id: &str,
) -> Result<Vec<EntityExternalIdRecord>, ProviderAdminError> {
    let db = STATE.db.read().await;
    let node_db_id = db::lookup::find_node_id_by_id(&db, node_id)?
        .ok_or_else(|| ProviderAdminError::EntityNotFound(node_id.to_string()))?;
    let ids = db::external_ids::get_for_entity(&db, node_db_id)?;

    Ok(ids
        .into_iter()
        .map(|id| EntityExternalIdRecord {
            provider_id: id.provider_id,
            id_type: id.id_type,
            id_value: id.id_value,
            source: id.source.to_string(),
        })
        .collect())
}

pub(crate) async fn set_entity_external_id(
    node_id: &str,
    request: SetEntityExternalIdRequest,
) -> Result<EntityExternalIdRecord, ProviderAdminError> {
    let mut db = STATE.db.write().await;
    let node_db_id = db::lookup::find_node_id_by_id(&db, node_id)?
        .ok_or_else(|| ProviderAdminError::EntityNotFound(node_id.to_string()))?;
    if !db::entities::exists(&db, node_db_id)? {
        return Err(ProviderAdminError::EntityNotFound(node_id.to_string()));
    }

    db::external_ids::upsert(
        &mut db,
        node_db_id,
        &request.provider_id,
        &request.id_type,
        &request.id_value,
        IdSource::User,
    )?;

    if request.id_type == "artist_db_id" && db::artists::get_by_id(&db, node_db_id)?.is_some() {
        let _ =
            crate::services::metadata::verification::recompute_artist_verified(&mut db, node_db_id);
    }

    Ok(EntityExternalIdRecord {
        provider_id: request.provider_id,
        id_type: request.id_type,
        id_value: request.id_value,
        source: "user".to_string(),
    })
}

pub(crate) async fn set_entity_locked(
    node_id: &str,
    locked: bool,
) -> Result<(), ProviderAdminError> {
    let mut db = STATE.db.write().await;
    let node_db_id = db::lookup::find_node_id_by_id(&db, node_id)?
        .ok_or_else(|| ProviderAdminError::EntityNotFound(node_id.to_string()))?;
    if !db::entities::set_locked(&mut db, node_db_id, locked)? {
        return Err(ProviderAdminError::EntityNotFound(node_id.to_string()));
    }
    Ok(())
}

pub(crate) async fn refresh_entity_by_id(
    node_id: &str,
    refresh_mode: EntityRefreshMode,
) -> Result<EntityRefreshResult, ProviderAdminError> {
    let node_db_id = {
        let db = STATE.db.read().await;
        db::lookup::find_node_id_by_id(&db, node_id)?
            .ok_or_else(|| ProviderAdminError::EntityNotFound(node_id.to_string()))?
    };

    refresh_entity_metadata(node_db_id, refresh_mode)
        .await
        .map_err(|error| match error {
            ProviderServiceError::EntityNotFound(_) => {
                ProviderAdminError::EntityNotFound(node_id.to_string())
            }
            ProviderServiceError::Internal(error) => ProviderAdminError::Internal(error),
            other => ProviderAdminError::Internal(anyhow::Error::new(other)),
        })
}
