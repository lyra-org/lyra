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

use super::{
    MetadataEditingError,
    model::{
        MetadataCreditValue,
        MetadataEntityType,
        MetadataField,
        MetadataFieldDiff,
        MetadataLabelValue,
        MetadataRelationValue,
        MetadataValueSource,
    },
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

fn require_node_id(db: &impl DbAccess, public_id: &str) -> Result<DbId, MetadataEditingError> {
    db::lookup::find_node_id_by_id(db, public_id)?.ok_or_else(|| {
        MetadataEditingError::Internal(anyhow::anyhow!(
            "normalized metadata reference '{public_id}' disappeared before apply"
        ))
    })
}

fn planned_value<T: DeserializeOwned>(
    plan: &[MetadataFieldDiff],
    field: MetadataField,
) -> Result<Option<T>, MetadataEditingError> {
    plan.iter()
        .find(|diff| diff.field == field)
        .map(|diff| normalized(&diff.after.value))
        .transpose()
        .map_err(Into::into)
}

pub(super) fn prepare_references(
    db: &impl DbAccess,
    plan: &[MetadataFieldDiff],
) -> Result<PreparedReferences, MetadataEditingError> {
    let mut prepared = PreparedReferences::default();
    if let Some(labels) = planned_value::<Vec<MetadataLabelValue>>(plan, MetadataField::Labels)? {
        prepared.labels = Some(
            labels
                .into_iter()
                .map(|label| -> Result<_, MetadataEditingError> {
                    Ok(db::labels::LabelLinkInput {
                        label_id: require_node_id(db, &label.id)?,
                        catalog_number: label.catalog_number,
                    })
                })
                .collect::<Result<_, _>>()?,
        );
    }
    if let Some(credits) = planned_value::<Vec<MetadataCreditValue>>(plan, MetadataField::Credits)?
    {
        prepared.credits = Some(
            credits
                .into_iter()
                .map(|credit| -> Result<_, MetadataEditingError> {
                    Ok(db::credits::CreditLinkInput {
                        artist_id: require_node_id(db, &credit.artist_id)?,
                        credit_type: credit.credit_type,
                        detail: credit.detail,
                    })
                })
                .collect::<Result<_, _>>()?,
        );
    }
    if let Some(relations) =
        planned_value::<Vec<MetadataRelationValue>>(plan, MetadataField::Relations)?
    {
        prepared.relations = Some(
            relations
                .into_iter()
                .map(|relation| -> Result<_, MetadataEditingError> {
                    Ok(db::artists::relations::ArtistRelationLinkInput {
                        target_artist_id: require_node_id(db, &relation.target_artist_id)?,
                        relation_type: relation.relation_type,
                        attributes: relation.attributes,
                    })
                })
                .collect::<Result<_, _>>()?,
        );
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
    plan: &[MetadataFieldDiff],
    references: &PreparedReferences,
) -> anyhow::Result<()> {
    let targets: BTreeMap<MetadataField, Value> = plan
        .iter()
        .map(|diff| (diff.field, diff.after.value.clone()))
        .collect();
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

        let mut overrides = db::metadata::manual_overrides::get(transaction, state.db_id)?
            .map(|row| row.parsed_fields())
            .transpose()?
            .unwrap_or_default();
        for diff in plan {
            let internal_name = internal_field_name(state.entity_type, diff.field)
                .ok_or_else(|| anyhow::anyhow!("invalid manual metadata field"))?;
            if diff.after.source == MetadataValueSource::Resolved {
                overrides.remove(&internal_name);
            } else if internal_name.is_graph() {
                overrides.insert(internal_name, Value::Bool(true));
            } else {
                overrides.insert(internal_name, diff.after.value.clone());
            }
        }
        db::metadata::manual_overrides::replace(transaction, state.db_id, &overrides)?;
        Ok(())
    })
}
