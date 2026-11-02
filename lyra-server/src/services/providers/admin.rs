// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use crate::services::{
    STATE,
    auth::{
        Principal,
        Recheck,
    },
    catalog::Viewer,
};
use lyra_db::{
    IdSource,
    Permission,
    ProviderConfig,
};

use super::{
    EntityRefreshMode,
    EntityRefreshResult,
    ProviderServiceError,
    RefreshTarget,
    refresh_entity_metadata,
};

#[derive(Debug, thiserror::Error)]
pub(crate) enum ProviderAdminError {
    #[error("Provider not found: {0}")]
    ProviderNotFound(String),
    #[error("Entity not found: {0}")]
    EntityNotFound(String),
    #[error("No manual external ID: {0}")]
    ManualIdNotFound(String),
    #[error("External ID value is blank: {0}")]
    BlankIdValue(String),
    #[error("{0}")]
    Forbidden(String),
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl From<Recheck> for ProviderAdminError {
    fn from(error: Recheck) -> Self {
        match error {
            Recheck::Internal(error) => Self::Internal(error),
            other => Self::Forbidden(other.to_string()),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct EntityExternalIdRecord {
    pub(crate) provider_id: String,
    pub(crate) id_type: String,
    pub(crate) id_value: String,
    pub(crate) source: String,
    pub(crate) resolved_value: Option<String>,
}

impl From<lyra_db::external_ids::ExternalId> for EntityExternalIdRecord {
    fn from(id: lyra_db::external_ids::ExternalId) -> Self {
        Self {
            provider_id: id.provider_id,
            id_type: id.id_type,
            id_value: id.id_value,
            source: id.source.to_string(),
            resolved_value: id.resolved_value,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct SetEntityExternalIdRequest {
    pub(crate) provider_id: String,
    pub(crate) id_type: String,
    pub(crate) id_value: String,
}

pub(crate) async fn list_provider_configs() -> Result<Vec<ProviderConfig>, ProviderAdminError> {
    let db = STATE.db.read().await;
    Ok(lyra_db::providers::get(&db)?)
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ProviderUpdate {
    pub(crate) position: Option<u32>,
    pub(crate) enabled: Option<bool>,
}

/// Applies the update and returns every provider in priority order.
///
/// Moving or toggling one provider only changes how its layers rank against
/// the others, so only entities carrying its layers need their merge reapplied.
pub(crate) async fn update_provider(
    provider_id: &str,
    update: ProviderUpdate,
) -> Result<Vec<ProviderConfig>, ProviderAdminError> {
    let mut db = STATE.db.write().await;
    if lyra_db::providers::get_by_provider_id(&db, provider_id)?.is_none() {
        return Err(ProviderAdminError::ProviderNotFound(
            provider_id.to_string(),
        ));
    }
    db.transaction_mut(|transaction| -> anyhow::Result<()> {
        if let Some(position) = update.position {
            lyra_db::providers::move_to(transaction, provider_id, position)?;
        }
        if let Some(enabled) = update.enabled {
            lyra_db::providers::set_enabled(transaction, provider_id, enabled)?;
        }
        for entity_id in lyra_db::metadata::layers::entity_ids_for_source(transaction, provider_id)?
        {
            crate::services::metadata::merging::apply_merged_metadata_to_entity_inside_tx(
                transaction,
                entity_id,
            )?;
        }
        Ok(())
    })?;
    Ok(lyra_db::providers::get(&db)?)
}

pub(crate) async fn list_entity_external_ids(
    entity_id: &str,
) -> Result<Vec<EntityExternalIdRecord>, ProviderAdminError> {
    let db = STATE.db.read().await;
    let entity_db_id = lyra_db::lookup::find_node_id_by_id(&db, entity_id)?
        .ok_or_else(|| ProviderAdminError::EntityNotFound(entity_id.to_string()))?;
    let ids = lyra_db::external_ids::get_for_entity(&db, entity_db_id)?;

    Ok(ids.into_iter().map(Into::into).collect())
}

pub(crate) async fn set_entity_external_id(
    entity_id: &str,
    request: SetEntityExternalIdRequest,
) -> Result<EntityExternalIdRecord, ProviderAdminError> {
    if request.id_value.trim().is_empty() {
        return Err(ProviderAdminError::BlankIdValue(format!(
            "{}/{}",
            request.provider_id, request.id_type
        )));
    }
    let mut db = STATE.db.write().await;
    let entity_db_id = lyra_db::lookup::find_node_id_by_id(&db, entity_id)?
        .ok_or_else(|| ProviderAdminError::EntityNotFound(entity_id.to_string()))?;
    if !lyra_db::entities::exists(&db, entity_db_id)? {
        return Err(ProviderAdminError::EntityNotFound(entity_id.to_string()));
    }

    lyra_db::external_ids::upsert(
        &mut db,
        entity_db_id,
        &request.provider_id,
        &request.id_type,
        &request.id_value,
        IdSource::Manual,
    )?;

    lyra_db::external_ids::get(&db, entity_db_id, &request.provider_id, &request.id_type)?
        .map(Into::into)
        .ok_or_else(|| anyhow::anyhow!("external id missing after it was set").into())
}

pub(crate) async fn inherit_entity_external_id(
    entity_id: &str,
    provider_id: &str,
    id_type: &str,
) -> Result<(), ProviderAdminError> {
    let mut db = STATE.db.write().await;
    let entity_db_id = lyra_db::lookup::find_node_id_by_id(&db, entity_id)?
        .ok_or_else(|| ProviderAdminError::EntityNotFound(entity_id.to_string()))?;

    if !lyra_db::external_ids::remove_manual(&mut db, entity_db_id, provider_id, id_type)? {
        return Err(ProviderAdminError::ManualIdNotFound(format!(
            "{provider_id}/{id_type}"
        )));
    }

    Ok(())
}

pub(crate) async fn set_entity_locked(
    entity_id: &str,
    locked: bool,
) -> Result<(), ProviderAdminError> {
    let mut db = STATE.db.write().await;
    let entity_db_id = lyra_db::lookup::find_node_id_by_id(&db, entity_id)?
        .ok_or_else(|| ProviderAdminError::EntityNotFound(entity_id.to_string()))?;
    if !lyra_db::entities::set_locked(&mut db, entity_db_id, locked)? {
        return Err(ProviderAdminError::EntityNotFound(entity_id.to_string()));
    }
    if !locked {
        crate::services::metadata::merging::apply_merged_metadata_to_entity(&mut db, entity_db_id)?;
    }
    Ok(())
}

/// Refreshes the metadata of the release, artist or track `entity_id` names from every enabled
/// provider, once they have answered, for a principal holding `SyncMetadata`, as the database
/// has it now. An entity that is missing, hidden from the principal, or of another type is not
/// found.
pub(crate) async fn refresh_entity(
    principal: &Principal,
    entity_id: &str,
    refresh_mode: EntityRefreshMode,
) -> Result<EntityRefreshResult, ProviderAdminError> {
    let not_found = || ProviderAdminError::EntityNotFound(entity_id.to_string());
    let target = {
        let db = STATE.db.read().await;
        let current = principal.current_with(&db, Permission::SyncMetadata)?;
        let entity_db_id =
            lyra_db::lookup::find_node_id_by_id(&*db, entity_id)?.ok_or_else(not_found)?;
        if !matches!(
            lyra_db::graph::element_kind(&*db, entity_db_id)?.as_deref(),
            Some("Release" | "Artist" | "Track")
        ) {
            return Err(not_found());
        }
        let viewer = Viewer::user(&*db, current).map_err(|_| {
            ProviderAdminError::Forbidden("the caller is no longer authorized".to_string())
        })?;
        if !viewer.sees(&db, entity_db_id)? {
            return Err(not_found());
        }
        RefreshTarget {
            db_id: entity_db_id,
            public_id: entity_id.to_string(),
            viewer,
        }
    };

    refresh_entity_metadata(target, refresh_mode)
        .await
        .map_err(|error| match error {
            ProviderServiceError::EntityNotFound(_) => not_found(),
            ProviderServiceError::Internal(error) => ProviderAdminError::Internal(error),
            other => ProviderAdminError::Internal(anyhow::Error::new(other)),
        })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use lyra_db::fixtures::insert_track;

    #[tokio::test]
    async fn a_blank_manual_external_id_is_rejected() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;

        let track_id = {
            let mut db = STATE.db.write().await;
            let track_db_id = insert_track(&mut db, "Track")?;
            lyra_db::tracks::get_by_id(&db, track_db_id)?
                .ok_or_else(|| anyhow::anyhow!("track missing"))?
                .id
        };

        let result = set_entity_external_id(
            &track_id,
            SetEntityExternalIdRequest {
                provider_id: "musicbrainz".to_string(),
                id_type: "recording_id".to_string(),
                id_value: " ".to_string(),
            },
        )
        .await;
        assert!(matches!(result, Err(ProviderAdminError::BlankIdValue(_))));
        Ok(())
    }

    #[tokio::test]
    async fn unlocking_applies_layers_recorded_while_locked() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;

        let track_id = {
            let mut db = STATE.db.write().await;
            let track_db_id = insert_track(&mut db, "Curated Title")?;
            let mut track = lyra_db::tracks::get_by_id(&db, track_db_id)?
                .ok_or_else(|| anyhow::anyhow!("track missing"))?;
            track.locked = Some(true);
            lyra_db::tracks::update(&mut db, &track)?;
            lyra_db::providers::upsert(
                &mut db,
                &ProviderConfig {
                    db_id: None,
                    provider_id: "test".to_string(),
                    position: 0,
                    enabled: true,
                },
            )?;
            crate::services::metadata::layers::save_provider_layer(
                &mut db,
                track_db_id,
                "test",
                &crate::services::metadata::layers::ProviderValues {
                    fields: crate::services::metadata::layers::json::inputs_from_json(
                        serde_json::json!({"track_title": "Provider Title"}),
                    ),
                    ..Default::default()
                },
                &HashMap::new(),
                &Default::default(),
            )?;
            assert_eq!(
                lyra_db::tracks::get_by_id(&db, track_db_id)?
                    .ok_or_else(|| anyhow::anyhow!("track missing"))?
                    .track_title,
                "Curated Title"
            );
            track.id
        };

        set_entity_locked(&track_id, false).await?;

        let db = STATE.db.read().await;
        let track_db_id = lyra_db::lookup::find_node_id_by_id(&db, &track_id)?
            .ok_or_else(|| anyhow::anyhow!("track node missing"))?;
        assert_eq!(
            lyra_db::tracks::get_by_id(&db, track_db_id)?
                .ok_or_else(|| anyhow::anyhow!("track missing"))?
                .track_title,
            "Provider Title"
        );
        Ok(())
    }

    #[tokio::test]
    async fn updating_a_provider_reapplies_affected_entities() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;

        let track_id = {
            let mut db = STATE.db.write().await;
            let track_db_id = insert_track(&mut db, "Local Title")?;
            for (provider_id, position, title) in
                [("low", 1, "Low Title"), ("high", 0, "High Title")]
            {
                lyra_db::providers::upsert(
                    &mut db,
                    &ProviderConfig {
                        db_id: None,
                        provider_id: provider_id.to_string(),
                        position,
                        enabled: true,
                    },
                )?;
                crate::services::metadata::layers::save_provider_layer(
                    &mut db,
                    track_db_id,
                    provider_id,
                    &crate::services::metadata::layers::ProviderValues {
                        fields: crate::services::metadata::layers::json::inputs_from_json(
                            serde_json::json!({"track_title": title}),
                        ),
                        ..Default::default()
                    },
                    &HashMap::new(),
                    &Default::default(),
                )?;
            }
            assert_eq!(
                lyra_db::tracks::get_by_id(&db, track_db_id)?
                    .ok_or_else(|| anyhow::anyhow!("track missing"))?
                    .track_title,
                "High Title"
            );
            lyra_db::tracks::get_by_id(&db, track_db_id)?
                .ok_or_else(|| anyhow::anyhow!("track missing"))?
                .id
        };

        let track_title = || async {
            let db = STATE.db.read().await;
            let track_db_id = lyra_db::lookup::find_node_id_by_id(&db, &track_id)?
                .ok_or_else(|| anyhow::anyhow!("track node missing"))?;
            anyhow::Ok(
                lyra_db::tracks::get_by_id(&db, track_db_id)?
                    .ok_or_else(|| anyhow::anyhow!("track missing"))?
                    .track_title,
            )
        };

        let providers = update_provider(
            "low",
            ProviderUpdate {
                position: Some(0),
                enabled: None,
            },
        )
        .await?;
        let order = providers
            .iter()
            .map(|provider| (provider.provider_id.as_str(), provider.position))
            .collect::<Vec<_>>();
        assert_eq!(order, [("low", 0), ("high", 1)]);
        assert_eq!(track_title().await?, "Low Title");

        update_provider(
            "low",
            ProviderUpdate {
                position: None,
                enabled: Some(false),
            },
        )
        .await?;
        assert_eq!(track_title().await?, "High Title");
        Ok(())
    }

    #[tokio::test]
    async fn updating_a_missing_provider_fails() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;

        let result = update_provider("missing", ProviderUpdate::default()).await;
        assert!(matches!(
            result,
            Err(ProviderAdminError::ProviderNotFound(_))
        ));
        Ok(())
    }
}
