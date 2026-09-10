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
    let mut config = db::providers::get(&db)?
        .into_iter()
        .find(|provider| provider.provider_id == provider_id)
        .ok_or_else(|| ProviderAdminError::ProviderNotFound(provider_id.to_string()))?;
    db.transaction_mut(|transaction| -> anyhow::Result<()> {
        db::providers::update_priority(transaction, provider_id, priority)?;
        for entity_id in db::metadata::layers::entity_ids_for_source(transaction, provider_id)? {
            crate::services::metadata::merging::apply_merged_metadata_to_entity_inside_tx(
                transaction,
                entity_id,
            )?;
        }
        Ok(())
    })?;
    config.priority = priority;
    Ok(config)
}

pub(crate) async fn list_entity_external_ids(
    entity_id: &str,
) -> Result<Vec<EntityExternalIdRecord>, ProviderAdminError> {
    let db = STATE.db.read().await;
    let entity_db_id = db::lookup::find_node_id_by_id(&db, entity_id)?
        .ok_or_else(|| ProviderAdminError::EntityNotFound(entity_id.to_string()))?;
    let ids = db::external_ids::get_for_entity(&db, entity_db_id)?;

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
    entity_id: &str,
    request: SetEntityExternalIdRequest,
) -> Result<EntityExternalIdRecord, ProviderAdminError> {
    let mut db = STATE.db.write().await;
    let entity_db_id = db::lookup::find_node_id_by_id(&db, entity_id)?
        .ok_or_else(|| ProviderAdminError::EntityNotFound(entity_id.to_string()))?;
    if !db::entities::exists(&db, entity_db_id)? {
        return Err(ProviderAdminError::EntityNotFound(entity_id.to_string()));
    }

    db::external_ids::upsert(
        &mut db,
        entity_db_id,
        &request.provider_id,
        &request.id_type,
        &request.id_value,
        IdSource::User,
    )?;

    if request.id_type == "artist_db_id" && db::artists::get_by_id(&db, entity_db_id)?.is_some() {
        let _ = crate::services::metadata::verification::recompute_artist_verified(
            &mut db,
            entity_db_id,
        );
    }

    Ok(EntityExternalIdRecord {
        provider_id: request.provider_id,
        id_type: request.id_type,
        id_value: request.id_value,
        source: "user".to_string(),
    })
}

pub(crate) async fn set_entity_locked(
    entity_id: &str,
    locked: bool,
) -> Result<(), ProviderAdminError> {
    let mut db = STATE.db.write().await;
    let entity_db_id = db::lookup::find_node_id_by_id(&db, entity_id)?
        .ok_or_else(|| ProviderAdminError::EntityNotFound(entity_id.to_string()))?;
    if !db::entities::set_locked(&mut db, entity_db_id, locked)? {
        return Err(ProviderAdminError::EntityNotFound(entity_id.to_string()));
    }
    if !locked {
        crate::services::metadata::merging::apply_merged_metadata_to_entity(&mut db, entity_db_id)?;
    }
    Ok(())
}

pub(crate) async fn refresh_entity_by_id(
    entity_id: &str,
    refresh_mode: EntityRefreshMode,
) -> Result<EntityRefreshResult, ProviderAdminError> {
    let entity_db_id = {
        let db = STATE.db.read().await;
        db::lookup::find_node_id_by_id(&db, entity_id)?
            .ok_or_else(|| ProviderAdminError::EntityNotFound(entity_id.to_string()))?
    };

    refresh_entity_metadata(entity_db_id, refresh_mode)
        .await
        .map_err(|error| match error {
            ProviderServiceError::EntityNotFound(_) => {
                ProviderAdminError::EntityNotFound(entity_id.to_string())
            }
            ProviderServiceError::Internal(error) => ProviderAdminError::Internal(error),
            other => ProviderAdminError::Internal(anyhow::Error::new(other)),
        })
}

#[cfg(test)]
mod tests {
    use std::collections::{
        HashMap,
        HashSet,
    };

    use super::*;
    use crate::db::test_db::insert_track;

    #[tokio::test]
    async fn unlocking_applies_layers_recorded_while_locked() -> anyhow::Result<()> {
        let _guard = crate::testing::runtime_test_lock().await;
        crate::testing::init_default_test_state()?;

        let track_id = {
            let mut db = STATE.db.write().await;
            let track_db_id = insert_track(&mut db, "Curated Title")?;
            let mut track = db::tracks::get_by_id(&db, track_db_id)?
                .ok_or_else(|| anyhow::anyhow!("track missing"))?;
            track.locked = Some(true);
            db::tracks::update(&mut db, &track)?;
            db::providers::upsert(
                &mut db,
                &ProviderConfig {
                    db_id: None,
                    provider_id: "test".to_string(),
                    display_name: "Test".to_string(),
                    priority: 100,
                    enabled: true,
                },
            )?;
            crate::services::metadata::layers::save_provider_layer(
                &mut db,
                track_db_id,
                "test",
                &HashMap::from([(
                    "track_title".to_string(),
                    serde_json::json!("Provider Title"),
                )]),
                &HashMap::new(),
                &HashMap::new(),
                &HashSet::new(),
            )?;
            assert_eq!(
                db::tracks::get_by_id(&db, track_db_id)?
                    .ok_or_else(|| anyhow::anyhow!("track missing"))?
                    .track_title,
                "Curated Title"
            );
            track.id
        };

        set_entity_locked(&track_id, false).await?;

        let db = STATE.db.read().await;
        let track_db_id = db::lookup::find_node_id_by_id(&db, &track_id)?
            .ok_or_else(|| anyhow::anyhow!("track node missing"))?;
        assert_eq!(
            db::tracks::get_by_id(&db, track_db_id)?
                .ok_or_else(|| anyhow::anyhow!("track missing"))?
                .track_title,
            "Provider Title"
        );
        Ok(())
    }

    #[tokio::test]
    async fn changing_priority_reapplies_affected_entities() -> anyhow::Result<()> {
        let _guard = crate::testing::runtime_test_lock().await;
        crate::testing::init_default_test_state()?;

        let track_id = {
            let mut db = STATE.db.write().await;
            let track_db_id = insert_track(&mut db, "Local Title")?;
            for (provider_id, priority, title) in
                [("low", 10, "Low Title"), ("high", 100, "High Title")]
            {
                db::providers::upsert(
                    &mut db,
                    &ProviderConfig {
                        db_id: None,
                        provider_id: provider_id.to_string(),
                        display_name: provider_id.to_string(),
                        priority,
                        enabled: true,
                    },
                )?;
                crate::services::metadata::layers::save_provider_layer(
                    &mut db,
                    track_db_id,
                    provider_id,
                    &HashMap::from([("track_title".to_string(), serde_json::json!(title))]),
                    &HashMap::new(),
                    &HashMap::new(),
                    &HashSet::new(),
                )?;
            }
            assert_eq!(
                db::tracks::get_by_id(&db, track_db_id)?
                    .ok_or_else(|| anyhow::anyhow!("track missing"))?
                    .track_title,
                "High Title"
            );
            db::tracks::get_by_id(&db, track_db_id)?
                .ok_or_else(|| anyhow::anyhow!("track missing"))?
                .id
        };

        let config = update_provider_priority("low", 200).await?;
        assert_eq!(config.priority, 200);

        let db = STATE.db.read().await;
        let track_db_id = db::lookup::find_node_id_by_id(&db, &track_id)?
            .ok_or_else(|| anyhow::anyhow!("track node missing"))?;
        assert_eq!(
            db::tracks::get_by_id(&db, track_db_id)?
                .ok_or_else(|| anyhow::anyhow!("track missing"))?
                .track_title,
            "Low Title"
        );
        Ok(())
    }
}
