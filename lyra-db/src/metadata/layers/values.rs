// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::{
    BTreeMap,
    HashSet,
};

use agdb::DbId;
use anyhow::Context;

use crate::{
    DbAccess,
    MetadataField,
    artists::ArtistType,
    genres,
    labels::{
        self,
        LabelInput,
        LabelLinkInput,
    },
    releases::{
        ReleaseType,
        normalize_release_date,
    },
};

/// A layer's value for one field; genres and labels link by id.
#[derive(Clone, Debug, PartialEq)]
pub enum FieldValue {
    Text(String),
    Number(u32),
    ReleaseType(ReleaseType),
    ArtistType(ArtistType),
    Genres(Vec<DbId>),
    Labels(Vec<LabelLinkInput>),
}

/// Layer values keyed by field; `None` explicitly clears a field.
pub type LayerFields = BTreeMap<MetadataField, Option<FieldValue>>;

/// A field value as a source names it: genres and labels by name, linked by id
/// once [`resolve_inputs`] finds or creates them.
#[derive(Clone, Debug)]
pub enum FieldInput {
    Value(FieldValue),
    Genres(Vec<String>),
    Labels(Vec<LabelInput>),
}

/// Field inputs keyed by field; `None` explicitly clears a field.
pub type FieldInputs = BTreeMap<MetadataField, Option<FieldInput>>;

/// A Rust type a [`FieldValue`] variant holds.
pub trait FieldType: Clone {
    fn of(value: &FieldValue) -> Option<&Self>;
}

macro_rules! field_type {
    ($type:ty, $variant:ident) => {
        impl FieldType for $type {
            fn of(value: &FieldValue) -> Option<&Self> {
                match value {
                    FieldValue::$variant(inner) => Some(inner),
                    _ => None,
                }
            }
        }

        impl From<$type> for FieldValue {
            fn from(value: $type) -> Self {
                Self::$variant(value)
            }
        }
    };
}

field_type!(String, Text);
field_type!(u32, Number);
field_type!(ReleaseType, ReleaseType);
field_type!(ArtistType, ArtistType);
field_type!(Vec<DbId>, Genres);
field_type!(Vec<LabelLinkInput>, Labels);

impl From<&str> for FieldValue {
    fn from(value: &str) -> Self {
        Self::Text(value.to_string())
    }
}

/// The value `fields` sets `field` to, or `None` when they clear it or do not
/// supply it.
pub fn field_value<T: FieldType>(
    fields: &LayerFields,
    field: MetadataField,
) -> anyhow::Result<Option<T>> {
    fields
        .get(&field)
        .and_then(Option::as_ref)
        .map(|value| {
            T::of(value)
                .cloned()
                .with_context(|| format!("invalid value for '{field}': {value:?}"))
        })
        .transpose()
}

/// Validates a layer value and returns its stored form. An empty genre or
/// label list clears like `None`.
pub fn normalize_value(
    field: MetadataField,
    value: Option<FieldValue>,
) -> anyhow::Result<Option<FieldValue>> {
    anyhow::ensure!(
        field.is_layered(),
        "metadata layers supply '{field}' per role or relation type, not as one value"
    );
    let Some(value) = value else {
        anyhow::ensure!(!field.is_required(), "'{field}' cannot be cleared");
        return Ok(None);
    };
    let value = match (field, value) {
        (
            MetadataField::ReleaseTitle | MetadataField::TrackTitle | MetadataField::ArtistName,
            FieldValue::Text(title),
        ) => {
            anyhow::ensure!(!title.trim().is_empty(), "'{field}' cannot be empty");
            FieldValue::Text(title)
        }
        (MetadataField::ReleaseDate, FieldValue::Text(date)) => {
            FieldValue::Text(normalize_release_date(&date).with_context(|| {
                format!("'{field}' must use YYYY, YYYY-MM, or YYYY-MM-DD, got '{date}'")
            })?)
        }
        (
            MetadataField::SortTitle | MetadataField::SortName | MetadataField::Description,
            value @ FieldValue::Text(_),
        )
        | (MetadataField::ReleaseType, value @ FieldValue::ReleaseType(_))
        | (MetadataField::ArtistType, value @ FieldValue::ArtistType(_))
        | (
            MetadataField::Year
            | MetadataField::Disc
            | MetadataField::DiscTotal
            | MetadataField::Track
            | MetadataField::TrackTotal,
            value @ FieldValue::Number(_),
        ) => value,
        (MetadataField::Genres, FieldValue::Genres(genre_ids)) => {
            let mut seen = HashSet::new();
            let genre_ids: Vec<DbId> = genre_ids
                .into_iter()
                .filter(|genre_id| seen.insert(*genre_id))
                .collect();
            if genre_ids.is_empty() {
                return Ok(None);
            }
            FieldValue::Genres(genre_ids)
        }
        (MetadataField::Labels, FieldValue::Labels(links)) => {
            let mut seen = HashSet::new();
            let links: Vec<LabelLinkInput> = links
                .into_iter()
                .filter(|link| seen.insert(link.label_id))
                .map(|link| LabelLinkInput {
                    catalog_number: link
                        .catalog_number
                        .map(|catalog_number| catalog_number.trim().to_string())
                        .filter(|catalog_number| !catalog_number.is_empty()),
                    ..link
                })
                .collect();
            if links.is_empty() {
                return Ok(None);
            }
            FieldValue::Labels(links)
        }
        (field, value) => anyhow::bail!("invalid value for '{field}': {value:?}"),
    };
    Ok(Some(value))
}

impl FieldInput {
    /// The value this input names, finding or creating its genres and labels.
    pub fn resolve(self, db: &mut impl DbAccess) -> anyhow::Result<FieldValue> {
        Ok(match self {
            Self::Value(value) => value,
            Self::Genres(names) => FieldValue::Genres(genres::resolve_names(db, &names)?),
            Self::Labels(inputs) => FieldValue::Labels(labels::resolve_label_inputs(db, &inputs)?),
        })
    }
}

/// Finds or creates the genres and labels `inputs` name.
pub fn resolve_inputs(db: &mut impl DbAccess, inputs: FieldInputs) -> anyhow::Result<LayerFields> {
    inputs
        .into_iter()
        .map(|(field, input)| Ok((field, input.map(|input| input.resolve(db)).transpose()?)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_value_rejects_values_the_field_cannot_hold() {
        for (field, value) in [
            (MetadataField::Credits, None),
            (MetadataField::Relations, None),
            (MetadataField::TrackTitle, None),
            (MetadataField::ReleaseTitle, Some("  ".into())),
            (MetadataField::ReleaseDate, Some("1999-13".into())),
            (MetadataField::Year, Some("1999".into())),
            (MetadataField::SortTitle, Some(FieldValue::Number(1))),
        ] {
            assert!(
                normalize_value(field, value.clone()).is_err(),
                "{field} accepted {value:?}"
            );
        }
    }

    #[test]
    fn normalize_value_normalizes_dates_and_lists() -> anyhow::Result<()> {
        assert_eq!(
            normalize_value(MetadataField::ReleaseDate, Some(" 1999-02 ".into()))?,
            Some("1999-02".into())
        );
        assert_eq!(
            normalize_value(
                MetadataField::Genres,
                Some(FieldValue::Genres(vec![DbId(2), DbId(1), DbId(2)]))
            )?,
            Some(FieldValue::Genres(vec![DbId(2), DbId(1)]))
        );
        let link = |id, catalog_number: Option<&str>| LabelLinkInput {
            label_id: DbId(id),
            catalog_number: catalog_number.map(str::to_string),
        };
        assert_eq!(
            normalize_value(
                MetadataField::Labels,
                Some(FieldValue::Labels(vec![
                    link(1, Some(" A-1 ")),
                    link(2, Some("  ")),
                    link(1, None),
                ]))
            )?,
            Some(FieldValue::Labels(vec![
                link(1, Some("A-1")),
                link(2, None)
            ]))
        );
        assert_eq!(
            normalize_value(MetadataField::Genres, Some(FieldValue::Genres(Vec::new())))?,
            None
        );
        Ok(())
    }
}
