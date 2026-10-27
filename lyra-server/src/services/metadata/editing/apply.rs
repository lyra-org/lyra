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
use serde_json::{
    Value,
    json,
};

use crate::services::metadata::{
    layers::{
        MANUAL_SOURCE_ID,
        now_secs,
    },
    merging::{
        self,
        LayerKeys,
    },
};
use lyra_db::{
    DbAccess,
    MetadataField,
    MetadataLayer,
    labels::LabelLinkInput,
    metadata::layers::edges::{
        LayerCreditInput,
        LayerRelationInput,
    },
};

use super::{
    model::{
        MetadataCreditValue,
        MetadataFieldDiff,
        MetadataLabelValue,
        MetadataRelationValue,
        MetadataValueSource,
    },
    state::EntityState,
};

fn normalized<T: DeserializeOwned>(value: &Value) -> anyhow::Result<T> {
    Ok(serde_json::from_value(value.clone())?)
}

fn require_node_id(db: &impl DbAccess, public_id: &str) -> anyhow::Result<DbId> {
    lyra_db::lookup::find_node_id_by_id(db, public_id)?.ok_or_else(|| {
        anyhow::anyhow!("normalized metadata reference '{public_id}' disappeared before apply")
    })
}

/// Writes one edited field into the manual layer: a set supplies it, a clear
/// supplies it empty, and an inherit stops supplying it. Credits and relations
/// supply every role or type, so ones the value leaves out clear.
fn edit_layer(
    db: &impl DbAccess,
    layer: &mut MetadataLayer,
    labels: &mut Vec<LabelLinkInput>,
    entry: &MetadataFieldDiff,
) -> anyhow::Result<()> {
    let manual = entry.after.source == MetadataValueSource::Manual;
    let value = &entry.after.value;
    match entry.field {
        MetadataField::Credits => {
            layer.credits.clear();
            if manual {
                layer.credits = lyra_db::CreditType::ALL
                    .into_iter()
                    .map(|credit_type| (credit_type, Vec::new()))
                    .collect();
                for credit in normalized::<Vec<MetadataCreditValue>>(value)? {
                    layer
                        .credits
                        .entry(credit.credit_type)
                        .or_default()
                        .push(LayerCreditInput {
                            artist_id: require_node_id(db, &credit.artist_id)?,
                            detail: credit.detail,
                        });
                }
            }
        }
        MetadataField::Relations => {
            layer.relations.clear();
            if manual {
                layer.relations = lyra_db::ArtistRelationType::ALL
                    .into_iter()
                    .map(|relation_type| (relation_type, Vec::new()))
                    .collect();
                for relation in normalized::<Vec<MetadataRelationValue>>(value)? {
                    layer
                        .relations
                        .entry(relation.relation_type)
                        .or_default()
                        .push(LayerRelationInput {
                            artist_id: require_node_id(db, &relation.artist_id)?,
                            attributes: relation.attributes,
                        });
                }
            }
        }
        MetadataField::Labels if manual => {
            let edited: Vec<MetadataLabelValue> = normalized(value)?;
            *labels = edited
                .iter()
                .map(|label| {
                    Ok(LabelLinkInput {
                        label_id: require_node_id(db, &label.id)?,
                        catalog_number: label.catalog_number.clone(),
                    })
                })
                .collect::<anyhow::Result<_>>()?;
            let names = edited
                .into_iter()
                .map(|label| json!({"name": label.name}))
                .collect();
            layer.fields.insert(entry.field, Value::Array(names));
        }
        field if manual => {
            layer.fields.insert(field, value.clone());
        }
        field => {
            layer.fields.remove(&field);
        }
    }
    Ok(())
}

fn released_keys(diff: &[MetadataFieldDiff]) -> LayerKeys {
    let mut keys = LayerKeys::default();
    for entry in diff {
        match entry.field {
            MetadataField::Credits => keys.credits.extend(lyra_db::CreditType::ALL),
            MetadataField::Relations => keys.relations.extend(lyra_db::ArtistRelationType::ALL),
            field => {
                keys.fields.insert(field);
            }
        }
    }
    keys
}

fn apply_inside_tx(
    db: &mut DbAnyTransactionMut<'_>,
    state: &EntityState,
    diff: &[MetadataFieldDiff],
) -> anyhow::Result<()> {
    let stored_id = lyra_db::metadata::layers::layer_id(db, state.db_id, MANUAL_SOURCE_ID)?;
    let mut layer = lyra_db::metadata::layers::find_valid(db, state.db_id, MANUAL_SOURCE_ID)?
        .unwrap_or_else(|| MetadataLayer::new(MANUAL_SOURCE_ID, Default::default(), 0));
    layer.updated_at = now_secs();
    let mut labels = match stored_id {
        Some(layer_id) if layer.fields.contains_key(&MetadataField::Labels) => {
            lyra_db::metadata::layers::label_links(db, layer_id)?
        }
        _ => Vec::new(),
    };
    for entry in diff {
        edit_layer(db, &mut layer, &mut labels, entry)?;
    }
    lyra_db::metadata::layers::replace_inside_tx(db, state.db_id, &layer, Some(&labels))?;
    // An edit applies even to a locked entity.
    merging::apply_through_lock(db, state.db_id, &released_keys(diff))
}

pub(super) fn apply_diff(
    db: &mut DbAny,
    state: &EntityState,
    diff: &[MetadataFieldDiff],
) -> anyhow::Result<()> {
    db.transaction_mut(|transaction| apply_inside_tx(transaction, state, diff))
}
