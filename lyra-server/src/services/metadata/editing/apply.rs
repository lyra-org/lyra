// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

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

fn normalized<T: DeserializeOwned>(value: &Value) -> anyhow::Result<T> {
    Ok(serde_json::from_value(value.clone())?)
}

fn require_node_id(db: &impl DbAccess, public_id: &str) -> anyhow::Result<DbId> {
    db::lookup::find_node_id_by_id(db, public_id)?.ok_or_else(|| {
        anyhow::anyhow!("normalized metadata reference '{public_id}' disappeared before apply")
    })
}

fn credit_inputs(
    db: &impl DbAccess,
    value: &Value,
) -> anyhow::Result<Vec<db::credits::CreditLinkInput>> {
    let credits: Vec<MetadataCreditValue> = normalized(value)?;
    credits
        .into_iter()
        .map(|credit| {
            Ok(db::credits::CreditLinkInput {
                artist_id: require_node_id(db, &credit.artist_id)?,
                credit_type: credit.credit_type,
                detail: credit.detail,
            })
        })
        .collect()
}

fn apply_release_fields(
    db: &mut DbAnyTransactionMut<'_>,
    state: &EntityState,
    diff: &[MetadataFieldDiff],
) -> anyhow::Result<()> {
    let mut release = db::releases::get_by_id(db, state.db_id)?
        .ok_or_else(|| anyhow::anyhow!("release disappeared during metadata apply"))?;
    let mut scalar_changed = false;
    for entry in diff {
        let value = &entry.after.value;
        match entry.field {
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
                let labels: Vec<MetadataLabelValue> = normalized(value)?;
                let inputs = labels
                    .into_iter()
                    .map(|label| {
                        Ok(db::labels::LabelLinkInput {
                            label_id: require_node_id(db, &label.id)?,
                            catalog_number: label.catalog_number,
                        })
                    })
                    .collect::<anyhow::Result<Vec<_>>>()?;
                db::labels::sync_release_label_links_inside_tx(db, state.db_id, &inputs)?;
            }
            MetadataField::Credits => {
                let inputs = credit_inputs(db, value)?;
                db::credits::replace_for_owner(db, state.db_id, &inputs)?;
            }
            field => anyhow::bail!("invalid release metadata field '{}'", field.as_str()),
        }
    }
    if scalar_changed {
        db::releases::update_in_transaction(db, &release)?;
    }
    Ok(())
}

fn apply_track_fields(
    db: &mut DbAnyTransactionMut<'_>,
    state: &EntityState,
    diff: &[MetadataFieldDiff],
) -> anyhow::Result<()> {
    let mut track = db::tracks::get_by_id(db, state.db_id)?
        .ok_or_else(|| anyhow::anyhow!("track disappeared during metadata apply"))?;
    let mut scalar_changed = false;
    for entry in diff {
        let value = &entry.after.value;
        match entry.field {
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
                let inputs = credit_inputs(db, value)?;
                db::credits::replace_for_owner(db, state.db_id, &inputs)?;
                continue;
            }
            field => anyhow::bail!("invalid track metadata field '{}'", field.as_str()),
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
    diff: &[MetadataFieldDiff],
) -> anyhow::Result<()> {
    let mut artist = db::artists::get_by_id(db, state.db_id)?
        .ok_or_else(|| anyhow::anyhow!("artist disappeared during metadata apply"))?;
    let mut scalar_changed = false;
    for entry in diff {
        let value = &entry.after.value;
        match entry.field {
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
                let relations: Vec<MetadataRelationValue> = normalized(value)?;
                let inputs = relations
                    .into_iter()
                    .map(|relation| {
                        Ok(db::artists::relations::ArtistRelationLinkInput {
                            target_artist_id: require_node_id(db, &relation.target_artist_id)?,
                            relation_type: relation.relation_type,
                            attributes: relation.attributes,
                        })
                    })
                    .collect::<anyhow::Result<Vec<_>>>()?;
                db::artists::relations::replace_from(db, state.db_id, &inputs)?;
                continue;
            }
            field => anyhow::bail!("invalid artist metadata field '{}'", field.as_str()),
        }
        scalar_changed = true;
    }
    if scalar_changed {
        db::artists::update_in_transaction(db, &artist)?;
    }
    Ok(())
}

pub(super) fn apply_diff(
    db: &mut DbAny,
    state: &EntityState,
    diff: &[MetadataFieldDiff],
) -> anyhow::Result<()> {
    db.transaction_mut(|transaction| -> anyhow::Result<()> {
        match state.entity_type {
            MetadataEntityType::Release => apply_release_fields(transaction, state, diff)?,
            MetadataEntityType::Track => apply_track_fields(transaction, state, diff)?,
            MetadataEntityType::Artist => apply_artist_fields(transaction, state, diff)?,
        }

        let mut overrides = db::metadata::manual_overrides::get(transaction, state.db_id)?
            .map(|row| row.parsed_fields())
            .transpose()?
            .unwrap_or_default();
        for entry in diff {
            let internal_name = internal_field_name(state.entity_type, entry.field)
                .ok_or_else(|| anyhow::anyhow!("invalid manual metadata field"))?;
            if entry.after.source == MetadataValueSource::Resolved {
                overrides.remove(&internal_name);
            } else if internal_name.is_graph() {
                overrides.insert(internal_name, Value::Bool(true));
            } else {
                overrides.insert(internal_name, entry.after.value.clone());
            }
        }
        db::metadata::manual_overrides::replace(transaction, state.db_id, &overrides)?;
        Ok(())
    })
}
