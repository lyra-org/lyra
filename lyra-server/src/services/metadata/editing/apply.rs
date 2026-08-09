// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::BTreeMap;

use agdb::{
    DbAny,
    DbAnyTransactionMut,
    DbId,
};
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::db::{
    self,
    DbAccess,
};
use crate::services::auth::{
    Principal,
    access,
};

use super::{
    EditPlan,
    MetadataEditingError,
    model::{
        MetadataCreditValue,
        MetadataEntityType,
        MetadataField,
        MetadataFieldConflict,
        MetadataLabelValue,
        MetadataRelationValue,
        MetadataValueSource,
    },
    normalize::label_accessible,
    state::{
        EntityState,
        internal_field_name,
    },
};

#[derive(Default)]
pub(super) struct PreparedReferences {
    labels: Option<Vec<db::labels::LabelLinkInput>>,
    credits: Option<Vec<db::credits::CreditLinkInput>>,
    relations: Option<Vec<db::artists::relations::ArtistRelationLinkInput>>,
}

fn normalized<T: DeserializeOwned>(value: &Value) -> anyhow::Result<T> {
    Ok(serde_json::from_value(value.clone())?)
}

fn apply_release_fields(
    db: &mut DbAnyTransactionMut<'_>,
    state: &EntityState,
    targets: &BTreeMap<MetadataField, Value>,
    references: &PreparedReferences,
) -> anyhow::Result<()> {
    let mut release = db::releases::get_by_id(db, state.db_id)?
        .ok_or_else(|| anyhow::anyhow!("release disappeared during metadata apply"))?;
    let mut scalar_changed = false;
    for (field, value) in targets {
        match field {
            MetadataField::Title => {
                release.release_title = normalized(value)?;
                scalar_changed = true;
            }
            MetadataField::SortTitle => {
                if let Some(sort_title) = normalized(value)? {
                    release.set_sort_title(sort_title);
                } else {
                    release.sort_title = None;
                }
                scalar_changed = true;
            }
            MetadataField::ReleaseType => {
                release.release_type = normalized(value)?;
                scalar_changed = true;
            }
            MetadataField::ReleaseDate => {
                release.release_date = normalized(value)?;
                scalar_changed = true;
            }
            MetadataField::Genres => {
                let genres: Vec<String> = normalized(value)?;
                db::genres::sync_release_genres(db, state.db_id, &genres)?;
            }
            MetadataField::Labels => {
                let inputs = references
                    .labels
                    .as_deref()
                    .ok_or_else(|| anyhow::anyhow!("metadata label edit was not prepared"))?;
                db::labels::sync_release_label_links_inside_tx(db, state.db_id, inputs)?;
            }
            MetadataField::Credits => {
                let inputs = references
                    .credits
                    .as_deref()
                    .ok_or_else(|| anyhow::anyhow!("metadata credit edit was not prepared"))?;
                db::credits::replace_for_owner(db, state.db_id, inputs)?;
            }
            _ => anyhow::bail!("invalid release metadata field '{}'", field.as_str()),
        }
    }
    if scalar_changed {
        db::releases::update_in_transaction(db, &release)?;
    }
    Ok(())
}

fn reference_conflict(
    field: MetadataField,
    expected: &Value,
    source: MetadataValueSource,
) -> MetadataEditingError {
    MetadataEditingError::Conflict(vec![MetadataFieldConflict {
        field,
        expected: expected.clone(),
        current: Value::Null,
        expected_source: source,
        current_source: source,
    }])
}

fn resolve_artist_reference(
    db: &impl DbAccess,
    principal: &Principal,
    public_id: &str,
    field: MetadataField,
    expected: &Value,
    source: MetadataValueSource,
) -> Result<DbId, MetadataEditingError> {
    let db_id = db::lookup::find_node_id_by_id(db, public_id)?
        .ok_or_else(|| reference_conflict(field, expected, source))?;
    if db::artists::get_by_id(db, db_id)?.is_none()
        || !access::artist_accessible(db, principal, db_id)?
    {
        return Err(reference_conflict(field, expected, source));
    }
    Ok(db_id)
}

pub(super) fn prepare_references(
    db: &impl DbAccess,
    principal: &Principal,
    state: &EntityState,
    plan: &EditPlan,
) -> Result<PreparedReferences, MetadataEditingError> {
    let mut prepared = PreparedReferences::default();
    if let Some(planned) = plan.fields.get(&MetadataField::Labels) {
        let value = &planned.after;
        let source = planned.source_after;
        let labels: Vec<MetadataLabelValue> = serde_json::from_value(value.clone())?;
        let mut inputs = Vec::with_capacity(labels.len());
        for label in labels {
            let label_id = db::lookup::find_node_id_by_id(db, &label.id)?
                .ok_or_else(|| reference_conflict(MetadataField::Labels, value, source))?;
            let stored = db::labels::get_by_id(db, label_id)?
                .ok_or_else(|| reference_conflict(MetadataField::Labels, value, source))?;
            if stored.id != label.id || stored.name != label.name {
                return Err(reference_conflict(MetadataField::Labels, value, source));
            }
            if source == MetadataValueSource::Manual && !label_accessible(db, principal, label_id)?
            {
                return Err(reference_conflict(MetadataField::Labels, value, source));
            }
            inputs.push(db::labels::LabelLinkInput {
                label_id,
                catalog_number: label.catalog_number,
            });
        }
        prepared.labels = Some(inputs);
    }
    if let Some(planned) = plan.fields.get(&MetadataField::Credits) {
        let value = &planned.after;
        let source = planned.source_after;
        let credits: Vec<MetadataCreditValue> = serde_json::from_value(value.clone())?;
        let mut inputs = Vec::with_capacity(credits.len());
        for credit in credits {
            inputs.push(db::credits::CreditLinkInput {
                artist_id: resolve_artist_reference(
                    db,
                    principal,
                    &credit.artist_id,
                    MetadataField::Credits,
                    value,
                    source,
                )?,
                credit_type: credit.credit_type,
                detail: credit.detail,
            });
        }
        prepared.credits = Some(inputs);
    }
    if let Some(planned) = plan.fields.get(&MetadataField::Relations) {
        let value = &planned.after;
        let source = planned.source_after;
        let relations: Vec<MetadataRelationValue> = serde_json::from_value(value.clone())?;
        let mut inputs = Vec::with_capacity(relations.len());
        for relation in relations {
            let target_artist_id = resolve_artist_reference(
                db,
                principal,
                &relation.target_artist_id,
                MetadataField::Relations,
                value,
                source,
            )?;
            if target_artist_id == state.db_id {
                return Err(reference_conflict(MetadataField::Relations, value, source));
            }
            inputs.push(db::artists::relations::ArtistRelationLinkInput {
                target_artist_id,
                relation_type: relation.relation_type,
                attributes: relation.attributes,
            });
        }
        prepared.relations = Some(inputs);
    }
    Ok(prepared)
}

fn apply_track_fields(
    db: &mut DbAnyTransactionMut<'_>,
    state: &EntityState,
    targets: &BTreeMap<MetadataField, Value>,
    references: &PreparedReferences,
) -> anyhow::Result<()> {
    let mut track = db::tracks::get_by_id(db, state.db_id)?
        .ok_or_else(|| anyhow::anyhow!("track disappeared during metadata apply"))?;
    let mut scalar_changed = false;
    for (field, value) in targets {
        match field {
            MetadataField::Title => track.track_title = normalized(value)?,
            MetadataField::SortTitle => {
                if let Some(sort_title) = normalized(value)? {
                    track.set_sort_title(sort_title);
                } else {
                    track.sort_title = None;
                }
            }
            MetadataField::Year => track.year = normalized(value)?,
            MetadataField::Disc => track.disc = normalized(value)?,
            MetadataField::DiscTotal => track.disc_total = normalized(value)?,
            MetadataField::Track => track.track = normalized(value)?,
            MetadataField::TrackTotal => track.track_total = normalized(value)?,
            MetadataField::Credits => {
                let inputs = references
                    .credits
                    .as_deref()
                    .ok_or_else(|| anyhow::anyhow!("metadata credit edit was not prepared"))?;
                db::credits::replace_for_owner(db, state.db_id, inputs)?;
                continue;
            }
            _ => anyhow::bail!("invalid track metadata field '{}'", field.as_str()),
        }
        scalar_changed = true;
    }
    if scalar_changed {
        db::tracks::update_in_transaction(db, &track)?;
    }
    Ok(())
}

fn apply_artist_fields(
    db: &mut DbAnyTransactionMut<'_>,
    state: &EntityState,
    targets: &BTreeMap<MetadataField, Value>,
    references: &PreparedReferences,
) -> anyhow::Result<()> {
    let mut artist = db::artists::get_by_id(db, state.db_id)?
        .ok_or_else(|| anyhow::anyhow!("artist disappeared during metadata apply"))?;
    let mut scalar_changed = false;
    for (field, value) in targets {
        match field {
            MetadataField::Name => artist.artist_name = normalized(value)?,
            MetadataField::SortName => {
                if let Some(sort_name) = normalized(value)? {
                    artist.set_sort_name(sort_name);
                } else {
                    artist.sort_name = None;
                }
            }
            MetadataField::ArtistType => artist.artist_type = normalized(value)?,
            MetadataField::Description => {
                if let Some(description) = normalized(value)? {
                    artist.set_description(description);
                } else {
                    artist.description = None;
                }
            }
            MetadataField::Relations => {
                let inputs = references
                    .relations
                    .as_deref()
                    .ok_or_else(|| anyhow::anyhow!("metadata relation edit was not prepared"))?;
                db::artists::relations::replace_from(db, state.db_id, inputs)?;
                continue;
            }
            _ => anyhow::bail!("invalid artist metadata field '{}'", field.as_str()),
        }
        scalar_changed = true;
    }
    if scalar_changed {
        db::artists::update_in_transaction(db, &artist)?;
    }
    Ok(())
}

pub(super) fn apply_plan(
    db: &mut DbAny,
    state: &EntityState,
    plan: &EditPlan,
    references: &PreparedReferences,
) -> anyhow::Result<()> {
    let targets = super::plan::targets(plan);
    let inherited_fields = super::plan::inherited_fields(plan);
    db.transaction_mut(|transaction| -> anyhow::Result<()> {
        match state.entity_type {
            MetadataEntityType::Release => {
                apply_release_fields(transaction, state, &targets, references)?
            }
            MetadataEntityType::Track => {
                apply_track_fields(transaction, state, &targets, references)?
            }
            MetadataEntityType::Artist => {
                apply_artist_fields(transaction, state, &targets, references)?
            }
        }

        let mut manual_fields = db::metadata::manual_overrides::get(transaction, state.db_id)?
            .map(|row| row.parsed_fields())
            .transpose()?
            .unwrap_or_default();
        for (field, value) in &targets {
            let internal_name = internal_field_name(state.entity_type, *field)
                .ok_or_else(|| anyhow::anyhow!("invalid manual metadata field"))?;
            if inherited_fields.contains(field) {
                manual_fields.remove(&internal_name);
            } else if internal_name.is_graph() {
                manual_fields.insert(internal_name, Value::Bool(true));
            } else {
                manual_fields.insert(internal_name, value.clone());
            }
        }
        db::metadata::manual_overrides::replace(transaction, state.db_id, &manual_fields)?;
        Ok(())
    })
}
