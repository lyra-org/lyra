// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Layer values as plugins and the editing API spell them in JSON.

use anyhow::Context;
use serde::de::DeserializeOwned;
use serde_json::{
    Value,
    json,
};

use lyra_db::{
    DbAccess,
    MetadataField,
    labels::{
        LabelExternalIdInput,
        LabelInput,
    },
    metadata::layers::{
        FieldInput,
        FieldValue,
        normalize_value,
    },
};

/// Reads a source's JSON value for `field`; `null` clears it.
pub(crate) fn input_from_json(
    field: MetadataField,
    value: Value,
) -> anyhow::Result<Option<FieldInput>> {
    if value.is_null() {
        return normalize_value(field, None).map(|_| None);
    }
    let value = match field {
        MetadataField::ReleaseTitle
        | MetadataField::TrackTitle
        | MetadataField::ArtistName
        | MetadataField::SortTitle
        | MetadataField::SortName
        | MetadataField::Description
        | MetadataField::ReleaseDate => FieldValue::Text(decode(field, &value)?),
        MetadataField::ReleaseType => FieldValue::ReleaseType(decode(field, &value)?),
        MetadataField::ArtistType => FieldValue::ArtistType(decode(field, &value)?),
        MetadataField::Year
        | MetadataField::Disc
        | MetadataField::DiscTotal
        | MetadataField::Track
        | MetadataField::TrackTotal => FieldValue::Number(decode(field, &value)?),
        MetadataField::Genres => return Ok(Some(FieldInput::Genres(decode(field, &value)?))),
        MetadataField::Labels => return Ok(Some(FieldInput::Labels(label_inputs(&value)?))),
        MetadataField::Credits | MetadataField::Relations => {
            anyhow::bail!(
                "metadata layers supply '{field}' per role or relation type, not as one value"
            )
        }
    };
    Ok(normalize_value(field, Some(value))?.map(FieldInput::Value))
}

/// A layer value as JSON: genres by name, and labels by name with their
/// catalog numbers.
pub(crate) fn value_json(db: &impl DbAccess, value: Option<&FieldValue>) -> anyhow::Result<Value> {
    let Some(value) = value else {
        return Ok(Value::Null);
    };
    Ok(match value {
        FieldValue::Text(text) => json!(text),
        FieldValue::Number(number) => json!(number),
        FieldValue::ReleaseType(release_type) => serde_json::to_value(release_type)?,
        FieldValue::ArtistType(artist_type) => serde_json::to_value(artist_type)?,
        FieldValue::Genres(genre_ids) => {
            let mut names = Vec::with_capacity(genre_ids.len());
            for &genre_id in genre_ids {
                let genre = lyra_db::genres::get_by_id(db, genre_id)?
                    .with_context(|| format!("genre {} is gone", genre_id.0))?;
                names.push(json!(genre.name));
            }
            Value::Array(names)
        }
        FieldValue::Labels(links) => {
            let mut values = Vec::with_capacity(links.len());
            for link in links {
                let label = lyra_db::labels::get_by_id(db, link.label_id)?
                    .with_context(|| format!("label {} is gone", link.label_id.0))?;
                let mut value = json!({ "name": label.name });
                if let Some(catalog_number) = &link.catalog_number {
                    value["catalog_number"] = json!(catalog_number);
                }
                values.push(value);
            }
            Value::Array(values)
        }
    })
}

fn decode<T: DeserializeOwned>(field: MetadataField, value: &Value) -> anyhow::Result<T> {
    serde_json::from_value(value.clone())
        .with_context(|| format!("invalid value for '{field}': {value}"))
}

/// Decodes a `labels` value: objects with a required `name`, an optional
/// `catalog_number`, and an optional complete `external_id`.
fn label_inputs(value: &Value) -> anyhow::Result<Vec<LabelInput>> {
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

/// Test field inputs from a JSON object of field names to values.
#[cfg(test)]
pub(crate) fn inputs_from_json(value: Value) -> lyra_db::metadata::layers::FieldInputs {
    let Value::Object(object) = value else {
        panic!("test layer fields are a JSON object, got {value}");
    };
    object
        .into_iter()
        .map(|(name, value)| {
            let field = MetadataField::from_name(&name)
                .unwrap_or_else(|| panic!("unknown test layer field '{name}'"));
            let input = input_from_json(field, value).expect("test layer field is valid");
            (field, input)
        })
        .collect()
}

/// Test layer fields from JSON that names no genres or labels.
#[cfg(test)]
pub(crate) fn fields_from_json(value: Value) -> lyra_db::metadata::layers::LayerFields {
    inputs_from_json(value)
        .into_iter()
        .map(|(field, input)| match input {
            Some(FieldInput::Value(value)) => (field, Some(value)),
            None => (field, None),
            Some(_) => panic!("test layer field '{field}' needs a database to resolve"),
        })
        .collect()
}

/// Test layer fields from JSON, finding or creating the genres and labels it
/// names.
#[cfg(test)]
pub(crate) fn resolved_fields_from_json(
    db: &mut impl DbAccess,
    value: Value,
) -> lyra_db::metadata::layers::LayerFields {
    lyra_db::metadata::layers::resolve_inputs(db, inputs_from_json(value))
        .expect("test layer fields resolve")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(field: MetadataField, value: Value) -> anyhow::Result<Option<FieldInput>> {
        input_from_json(field, value)
    }

    #[test]
    fn input_from_json_rejects_values_the_field_cannot_hold() {
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
                parse(field, value.clone()).is_err(),
                "{field} accepted {value}"
            );
        }
    }

    #[test]
    fn input_from_json_accepts_clears_and_normalizes_dates() -> anyhow::Result<()> {
        assert!(parse(MetadataField::SortTitle, Value::Null)?.is_none());
        assert!(matches!(
            parse(MetadataField::ReleaseDate, json!(" 1999-02 "))?,
            Some(FieldInput::Value(FieldValue::Text(date))) if date == "1999-02"
        ));
        assert!(matches!(
            parse(MetadataField::ReleaseType, json!("ep"))?,
            Some(FieldInput::Value(FieldValue::ReleaseType(
                lyra_db::releases::ReleaseType::EP
            )))
        ));
        Ok(())
    }

    #[test]
    fn label_inputs_parse_the_full_shape() -> anyhow::Result<()> {
        let inputs = label_inputs(&json!([
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
        assert!(label_inputs(&json!([]))?.is_empty());
        Ok(())
    }

    #[test]
    fn label_inputs_reject_malformed_entries() {
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
}
