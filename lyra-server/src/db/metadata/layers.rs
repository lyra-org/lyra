// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::BTreeMap;

use agdb::{
    DbAny,
    DbElement,
    DbId,
    QueryBuilder,
};
use anyhow::Context;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use super::super::{
    DbAccess,
    MetadataField,
    NodeId,
    artists::ArtistType,
    labels::{
        LabelExternalIdInput,
        LabelInput,
    },
    releases::{
        ReleaseType,
        normalize_release_date,
    },
};

/// Layer values keyed by field; `null` explicitly clears a field.
pub(crate) type LayerFields = BTreeMap<MetadataField, Value>;

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(DbElement, Serialize, Clone, Debug)]
pub(crate) struct MetadataLayer {
    #[serde(skip)]
    pub(crate) db_id: Option<NodeId>,
    pub(crate) source_id: String,
    pub(crate) fields: String,
    pub(crate) updated_at: u64,
}

impl MetadataLayer {
    pub(crate) fn new(
        source_id: impl Into<String>,
        fields: &LayerFields,
        updated_at: u64,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            db_id: None,
            source_id: source_id.into(),
            fields: serde_json::to_string(fields)?,
            updated_at,
        })
    }

    pub(crate) fn parsed_fields(&self) -> anyhow::Result<LayerFields> {
        let fields: LayerFields = serde_json::from_str(&self.fields)
            .with_context(|| format!("invalid '{}' metadata layer", self.source_id))?;
        fields
            .into_iter()
            .map(|(field, value)| Ok((field, normalize_value(field, value)?)))
            .collect()
    }
}

/// Validates a layer value and returns its stored form.
pub(crate) fn normalize_value(field: MetadataField, value: Value) -> anyhow::Result<Value> {
    anyhow::ensure!(
        field.is_layered(),
        "metadata layers cannot supply '{field}'"
    );
    if value.is_null() {
        anyhow::ensure!(!field.is_required(), "'{field}' cannot be cleared");
        return Ok(value);
    }
    match field {
        MetadataField::ReleaseTitle | MetadataField::TrackTitle | MetadataField::ArtistName => {
            let title: String = decode(field, &value)?;
            anyhow::ensure!(!title.trim().is_empty(), "'{field}' cannot be empty");
        }
        MetadataField::SortTitle | MetadataField::SortName | MetadataField::Description => {
            decode::<String>(field, &value)?;
        }
        MetadataField::ReleaseType => {
            decode::<ReleaseType>(field, &value)?;
        }
        MetadataField::ArtistType => {
            decode::<ArtistType>(field, &value)?;
        }
        MetadataField::ReleaseDate => {
            let date: String = decode(field, &value)?;
            let normalized = normalize_release_date(&date).with_context(|| {
                format!("'{field}' must use YYYY, YYYY-MM, or YYYY-MM-DD, got '{date}'")
            })?;
            return Ok(Value::String(normalized));
        }
        MetadataField::Year
        | MetadataField::Disc
        | MetadataField::DiscTotal
        | MetadataField::Track
        | MetadataField::TrackTotal => {
            decode::<u32>(field, &value)?;
        }
        MetadataField::Genres => {
            decode::<Vec<String>>(field, &value)?;
        }
        MetadataField::Labels => {
            label_inputs(&value)?;
        }
        MetadataField::Credits | MetadataField::Relations => {
            unreachable!("graph-only fields are rejected above")
        }
    }
    Ok(value)
}

pub(crate) fn decode<T: DeserializeOwned>(
    field: MetadataField,
    value: &Value,
) -> anyhow::Result<T> {
    serde_json::from_value(value.clone())
        .with_context(|| format!("invalid value for '{field}': {value}"))
}

/// Decodes a `labels` value: objects with a required `name`, an optional
/// `catalog_number`, and an optional complete `external_id`.
pub(crate) fn label_inputs(value: &Value) -> anyhow::Result<Vec<LabelInput>> {
    let entries = value
        .as_array()
        .with_context(|| format!("'labels' must be an array, got {value}"))?;
    entries.iter().map(label_input).collect()
}

fn label_input(entry: &Value) -> anyhow::Result<LabelInput> {
    let object = entry
        .as_object()
        .with_context(|| format!("label entries must be objects, got {entry}"))?;
    let name = object
        .get("name")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .with_context(|| format!("label entry needs a non-empty 'name': {entry}"))?;
    let catalog_number = match object.get("catalog_number") {
        None | Some(Value::Null) => None,
        Some(Value::String(catalog_number)) => {
            Some(catalog_number.trim()).filter(|catalog_number| !catalog_number.is_empty())
        }
        Some(other) => anyhow::bail!("label 'catalog_number' must be a string, got {other}"),
    };
    let external_id = match object.get("external_id") {
        None | Some(Value::Null) => None,
        Some(external_id) => Some(label_external_id(external_id)?),
    };
    Ok(LabelInput {
        name: name.to_string(),
        catalog_number: catalog_number.map(str::to_string),
        external_id,
    })
}

fn label_external_id(value: &Value) -> anyhow::Result<LabelExternalIdInput> {
    let part = |key: &str| {
        value
            .get(key)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|part| !part.is_empty())
            .map(str::to_string)
            .with_context(|| format!("label 'external_id' needs a non-empty '{key}': {value}"))
    };
    Ok(LabelExternalIdInput {
        provider_id: part("provider_id")?,
        id_type: part("id_type")?,
        id_value: part("id_value")?,
    })
}

pub(crate) fn get_for_entity(
    db: &impl super::super::DbAccess,
    node_id: DbId,
) -> anyhow::Result<Vec<MetadataLayer>> {
    let layers: Vec<MetadataLayer> = db
        .exec(
            QueryBuilder::select()
                .elements::<MetadataLayer>()
                .search()
                .from(node_id)
                .where_()
                .neighbor()
                .end_where()
                .query(),
        )?
        .try_into()?;

    Ok(layers)
}

pub(crate) fn entity_ids_for_source(
    db: &impl DbAccess,
    source_id: &str,
) -> anyhow::Result<Vec<DbId>> {
    let layer_ids = super::super::indexes::node_ids(db, "source_id", source_id)?;
    let mut entity_ids = Vec::with_capacity(layer_ids.len());
    for layer_id in layer_ids {
        let result = db.exec(
            QueryBuilder::search()
                .to(layer_id)
                .where_()
                .edge()
                .end_where()
                .query(),
        )?;
        entity_ids.extend(
            result
                .elements
                .into_iter()
                .filter_map(|edge| (edge.to == layer_id && edge.from.0 > 0).then_some(edge.from)),
        );
    }
    entity_ids.sort_unstable();
    entity_ids.dedup();
    Ok(entity_ids)
}

pub(crate) fn upsert(db: &mut DbAny, node_id: DbId, layer: &MetadataLayer) -> anyhow::Result<DbId> {
    db.transaction_mut(|t| upsert_inside_tx(t, node_id, layer))
}

pub(crate) fn upsert_inside_tx(
    db: &mut impl DbAccess,
    node_id: DbId,
    layer: &MetadataLayer,
) -> anyhow::Result<DbId> {
    let layers: Vec<MetadataLayer> = db
        .exec(
            QueryBuilder::select()
                .elements::<MetadataLayer>()
                .search()
                .from(node_id)
                .where_()
                .neighbor()
                .end_where()
                .query(),
        )?
        .try_into()?;
    let existing = layers
        .into_iter()
        .find(|existing| existing.source_id == layer.source_id);

    if let Some(existing_layer) = &existing
        && existing_layer.fields == layer.fields
        && let Some(db_id) = existing_layer.db_id.clone()
    {
        return Ok(db_id.into());
    }

    let mut layer_to_save = layer.clone();
    if let Some(existing_layer) = &existing {
        layer_to_save.db_id = existing_layer.db_id.clone();
    }

    let result = db.exec_mut(QueryBuilder::insert().element(&layer_to_save).query())?;
    let layer_db_id = existing
        .as_ref()
        .and_then(|existing_layer| existing_layer.db_id.clone())
        .map(DbId::from)
        .or_else(|| result.elements.first().map(|element| element.id))
        .ok_or_else(|| {
            anyhow::anyhow!(
                "upsert metadata layer returned no id (node_id={}, source_id='{}')",
                node_id.0,
                layer.source_id
            )
        })?;

    if existing.is_none() {
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(node_id)
                .to(layer_db_id)
                .query(),
        )?;
    }

    Ok(layer_db_id)
}

/// Removes the layer `source_id` contributed to an entity. Returns false when
/// there was none.
pub(crate) fn remove_inside_tx(
    db: &mut impl DbAccess,
    node_id: DbId,
    source_id: &str,
) -> anyhow::Result<bool> {
    let Some(layer_db_id) = get_for_entity(db, node_id)?
        .into_iter()
        .find(|layer| layer.source_id == source_id)
        .and_then(|layer| layer.db_id)
        .map(DbId::from)
    else {
        return Ok(false);
    };

    db.exec_mut(QueryBuilder::remove().ids(layer_db_id).query())?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::test_db::TestDb;
    use agdb::QueryBuilder;
    use anyhow::anyhow;

    fn new_test_db() -> anyhow::Result<DbAny> {
        Ok(TestDb::new()?.into_inner())
    }

    fn insert_entity(db: &mut DbAny) -> anyhow::Result<DbId> {
        let result = db.exec_mut(QueryBuilder::insert().nodes().count(1).query())?;
        result
            .elements
            .first()
            .map(|element| element.id)
            .ok_or_else(|| anyhow!("entity insert did not return an id"))
    }

    #[test]
    fn upsert_layer_update_reuses_node_and_updates_values() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let node_id = insert_entity(&mut db)?;

        let first = MetadataLayer {
            db_id: None,
            source_id: "musicbrainz".to_string(),
            fields: r#"{"release_title":"first"}"#.to_string(),
            updated_at: 100,
        };
        let second = MetadataLayer {
            db_id: None,
            source_id: "musicbrainz".to_string(),
            fields: r#"{"release_title":"second"}"#.to_string(),
            updated_at: 200,
        };

        let first_id = upsert(&mut db, node_id, &first)?;
        let second_id = upsert(&mut db, node_id, &second)?;

        assert_eq!(first_id, second_id);

        let layers = get_for_entity(&db, node_id)?;
        assert_eq!(layers.len(), 1);
        assert_eq!(layers[0].source_id, "musicbrainz");
        assert_eq!(layers[0].fields, second.fields);
        assert_eq!(layers[0].updated_at, second.updated_at);

        Ok(())
    }

    #[test]
    fn upsert_layer_unchanged_fields_is_noop() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let node_id = insert_entity(&mut db)?;

        let first = MetadataLayer {
            db_id: None,
            source_id: "musicbrainz".to_string(),
            fields: r#"{"release_title":"same"}"#.to_string(),
            updated_at: 100,
        };
        let second = MetadataLayer {
            db_id: None,
            source_id: "musicbrainz".to_string(),
            fields: first.fields.clone(),
            updated_at: 200,
        };

        let first_id = upsert(&mut db, node_id, &first)?;
        let second_id = upsert(&mut db, node_id, &second)?;

        assert_eq!(first_id, second_id);

        let layers = get_for_entity(&db, node_id)?;
        assert_eq!(layers.len(), 1);
        assert_eq!(layers[0].fields, first.fields);
        assert_eq!(layers[0].updated_at, first.updated_at);

        Ok(())
    }

    #[test]
    fn normalize_value_rejects_values_the_field_cannot_hold() {
        use serde_json::json;

        for (field, value) in [
            (MetadataField::Credits, json!([])),
            (MetadataField::Relations, json!([])),
            (MetadataField::TrackTitle, Value::Null),
            (MetadataField::ReleaseTitle, json!("  ")),
            (MetadataField::Disc, json!("1")),
            (MetadataField::Year, json!(-1)),
            (MetadataField::ReleaseType, json!("mixtape")),
            (MetadataField::ArtistType, json!("Person")),
            (MetadataField::ReleaseDate, json!("1999-13")),
            (MetadataField::Genres, json!("Jazz")),
            (MetadataField::Genres, json!([1])),
        ] {
            assert!(
                normalize_value(field, value.clone()).is_err(),
                "{field} accepted {value}"
            );
        }
    }

    #[test]
    fn normalize_value_accepts_clears_and_normalizes_dates() -> anyhow::Result<()> {
        use serde_json::json;

        assert_eq!(
            normalize_value(MetadataField::SortTitle, Value::Null)?,
            Value::Null
        );
        assert_eq!(
            normalize_value(MetadataField::ReleaseDate, json!(" 1999-02 "))?,
            json!("1999-02")
        );
        assert_eq!(
            normalize_value(MetadataField::ReleaseType, json!("ep"))?,
            json!("ep")
        );
        Ok(())
    }

    #[test]
    fn parsed_fields_rejects_an_unknown_field() {
        let layer = MetadataLayer {
            db_id: None,
            source_id: "musicbrainz".to_string(),
            fields: r#"{"track_title":"Title","work_id":"w"}"#.to_string(),
            updated_at: 1,
        };

        assert!(layer.parsed_fields().is_err());
    }

    #[test]
    fn label_inputs_parse_the_full_shape() -> anyhow::Result<()> {
        let inputs = label_inputs(&serde_json::json!([
            {
                "name": "Blue Note",
                "catalog_number": " BN-1577 ",
                "external_id": {
                    "provider_id": "  mb  ",
                    "id_type": " label_id ",
                    "id_value": " bn-001 "
                }
            },
            {"name": "Impulse!", "catalog_number": "  ", "external_id": null}
        ]))?;

        assert_eq!(inputs.len(), 2);
        assert_eq!(inputs[0].name, "Blue Note");
        assert_eq!(inputs[0].catalog_number.as_deref(), Some("BN-1577"));
        let external_id = inputs[0].external_id.as_ref().expect("external_id present");
        assert_eq!(external_id.provider_id, "mb");
        assert_eq!(external_id.id_type, "label_id");
        assert_eq!(external_id.id_value, "bn-001");
        assert_eq!(inputs[1].name, "Impulse!");
        assert!(inputs[1].catalog_number.is_none());
        assert!(inputs[1].external_id.is_none());
        assert!(label_inputs(&serde_json::json!([]))?.is_empty());
        Ok(())
    }

    #[test]
    fn label_inputs_reject_malformed_entries() {
        use serde_json::json;

        for value in [
            json!("Blue Note"),
            json!({"name": "Blue Note"}),
            json!(["Blue Note"]),
            json!([{"catalog_number": "BN-1"}]),
            json!([{"name": "   "}]),
            json!([{"name": "Blue Note", "catalog_number": 12345}]),
            json!([{"name": "Blue Note", "external_id": "mb"}]),
            json!([{
                "name": "Blue Note",
                "external_id": {"provider_id": "mb", "id_type": "label_id"}
            }]),
            json!([{
                "name": "Blue Note",
                "external_id": {"provider_id": "mb", "id_type": "label_id", "id_value": "   "}
            }]),
        ] {
            assert!(label_inputs(&value).is_err(), "accepted {value}");
        }
    }

    #[test]
    fn entity_ids_for_source_returns_layer_owners() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        db.exec_mut(QueryBuilder::insert().index("source_id").query())?;
        let first = insert_entity(&mut db)?;
        let second = insert_entity(&mut db)?;
        let other = insert_entity(&mut db)?;

        for (node_id, source_id) in [
            (first, "musicbrainz"),
            (second, "musicbrainz"),
            (other, "wikidata"),
        ] {
            upsert(
                &mut db,
                node_id,
                &MetadataLayer {
                    db_id: None,
                    source_id: source_id.to_string(),
                    fields: "{}".to_string(),
                    updated_at: 1,
                },
            )?;
        }

        assert_eq!(
            entity_ids_for_source(&db, "musicbrainz")?,
            vec![first, second]
        );
        assert!(entity_ids_for_source(&db, "missing")?.is_empty());
        Ok(())
    }
}
