// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashMap;
use std::time::{
    SystemTime,
    UNIX_EPOCH,
};

use agdb::{
    DbAny,
    DbId,
};
use nanoid::nanoid;

use crate::db::{
    self,
    IdSource,
    MetadataLayer,
};

fn is_entity_locked(db: &DbAny, node_id: DbId) -> anyhow::Result<bool> {
    if let Some(release) = db::releases::get_by_id(db, node_id)? {
        return Ok(release.locked.unwrap_or(false));
    }
    if let Some(track) = db::tracks::get_by_id(db, node_id)? {
        return Ok(track.locked.unwrap_or(false));
    }
    if let Some(artist) = db::artists::get_by_id(db, node_id)? {
        return Ok(artist.locked.unwrap_or(false));
    }

    Ok(false)
}

pub(crate) fn ensure_entity_exists(db: &DbAny, node_id: DbId) -> anyhow::Result<()> {
    let exists = db::releases::get_by_id(db, node_id)?.is_some()
        || db::tracks::get_by_id(db, node_id)?.is_some()
        || db::artists::get_by_id(db, node_id)?.is_some();
    if exists {
        return Ok(());
    }

    anyhow::bail!("Entity not found: {}", node_id.0);
}

pub(crate) fn list_entity_external_ids(
    db: &DbAny,
    node_id: DbId,
) -> anyhow::Result<Vec<db::external_ids::ExternalId>> {
    ensure_entity_exists(db, node_id)?;

    let mut ids = db::external_ids::get_for_entity(db, node_id)?;
    ids.sort_by(|a, b| {
        a.provider_id
            .cmp(&b.provider_id)
            .then_with(|| a.id_type.cmp(&b.id_type))
            .then_with(|| a.id_value.cmp(&b.id_value))
    });

    Ok(ids)
}

pub(crate) fn save_provider_layer(
    db: &mut DbAny,
    node_id: DbId,
    provider_id: &str,
    fields: &HashMap<String, serde_json::Value>,
    external_ids: &HashMap<String, String>,
) -> anyhow::Result<()> {
    ensure_entity_exists(db, node_id)?;

    let is_locked = is_entity_locked(db, node_id)?;
    if !is_locked && !fields.is_empty() {
        let fields_json = serde_json::to_string(fields)?;
        let existing_layer = db::metadata::layers::get_for_entity(db, node_id)?
            .into_iter()
            .find(|layer| layer.provider_id == provider_id);
        let layer_changed = existing_layer
            .as_ref()
            .is_none_or(|existing| existing.fields != fields_json);

        if layer_changed {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);

            let layer = MetadataLayer {
                db_id: None,
                id: nanoid!(),
                provider_id: provider_id.to_string(),
                fields: fields_json,
                updated_at: now,
            };

            db::metadata::layers::upsert(db, node_id, &layer)?;
            super::merging::apply_merged_metadata_to_entity(db, node_id)?;
        }
    }

    if external_ids.is_empty() {
        return Ok(());
    }

    let artist = db::artists::get_by_id(db, node_id)?;
    let is_artist_entity = artist.is_some();
    let artist_is_verified = artist.as_ref().is_some_and(|a| a.verified);
    let mut should_recompute_artist_verification = false;

    for (id_type, id_value) in external_ids {
        let existing_id = db::external_ids::get(db, node_id, provider_id, id_type)?;

        if is_artist_entity && id_type == "artist_id" {
            let incoming_artist_id = id_value.trim();
            if !incoming_artist_id.is_empty()
                && artist_is_verified
                && let Some(existing) = existing_id.as_ref()
                && existing.source == IdSource::Plugin
            {
                let existing_artist_id = existing.id_value.trim();
                if !existing_artist_id.is_empty() && existing_artist_id != incoming_artist_id {
                    tracing::warn!(
                        node_id = node_id.0,
                        provider_id,
                        existing_artist_id = %existing_artist_id,
                        incoming_artist_id = %incoming_artist_id,
                        "skipping conflicting artist_id update for verified artist"
                    );
                    continue;
                }
            }
        }

        let external_id_changed = existing_id.as_ref().is_none_or(|existing| {
            existing.id_value != *id_value || existing.source != IdSource::Plugin
        });
        if !external_id_changed {
            continue;
        }

        db::external_ids::upsert(
            db,
            node_id,
            provider_id,
            id_type,
            id_value,
            IdSource::Plugin,
        )?;

        if is_artist_entity && id_type == "artist_db_id" {
            should_recompute_artist_verification = true;
        }
    }

    if is_artist_entity && should_recompute_artist_verification {
        super::verification::recompute_artist_verified(db, node_id)?;
    }

    Ok(())
}
