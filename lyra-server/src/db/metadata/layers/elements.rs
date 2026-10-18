// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::{
    DbElement,
    DbId,
    DbType,
};
use anyhow::Context;
use serde::Serialize;
use serde_json::{
    Value,
    json,
};

use super::{
    LayerFields,
    MetadataLayer,
    decode,
    label_inputs,
    normalize_value,
};
use crate::db::{
    MetadataField,
    artists::ArtistType,
    entities::MetadataEntityType,
    labels::LabelInput,
    releases::ReleaseType,
};

/// A stored layer. Each field is a native property, and `supplied` names the
/// fields the source has an opinion on: a supplied field without its property
/// clears it, and an unsupplied field falls through to the next layer.
pub(super) trait LayerElement: DbType<ValueType = Self> + PartialEq + Sized {
    fn from_layer(layer: &MetadataLayer) -> anyhow::Result<Self>;
    fn to_layer(&self) -> anyhow::Result<MetadataLayer>;
    fn source_id(&self) -> &str;
    fn set_db_id(&mut self, db_id: DbId);
    /// Whether both supply the same values, whenever and wherever stored.
    fn same_values(&self, other: &Self) -> bool;
    fn optional_keys(&self) -> Vec<(&'static str, bool)>;
}

#[derive(DbElement, Clone, Debug, Default, PartialEq)]
pub(super) struct ReleaseLayer {
    db_id: Option<DbId>,
    source_id: String,
    updated_at: u64,
    supplied: Vec<String>,
    release_title: Option<String>,
    sort_title: Option<String>,
    release_type: Option<ReleaseType>,
    release_date: Option<String>,
    genres: Option<Vec<String>>,
    /// Labels as parallel lists; an empty string marks a missing part.
    label_names: Option<Vec<String>>,
    label_catalog_numbers: Option<Vec<String>>,
    label_id_providers: Option<Vec<String>>,
    label_id_types: Option<Vec<String>>,
    label_id_values: Option<Vec<String>>,
}

impl ReleaseLayer {
    fn set_labels(&mut self, labels: Vec<LabelInput>) {
        let mut names = Vec::with_capacity(labels.len());
        let mut catalog_numbers = Vec::with_capacity(labels.len());
        let mut providers = Vec::with_capacity(labels.len());
        let mut types = Vec::with_capacity(labels.len());
        let mut values = Vec::with_capacity(labels.len());
        for label in labels {
            names.push(label.name);
            catalog_numbers.push(label.catalog_number.unwrap_or_default());
            let external_id = label.external_id;
            providers.push(
                external_id
                    .as_ref()
                    .map(|id| id.provider_id.clone())
                    .unwrap_or_default(),
            );
            types.push(
                external_id
                    .as_ref()
                    .map(|id| id.id_type.clone())
                    .unwrap_or_default(),
            );
            values.push(external_id.map(|id| id.id_value).unwrap_or_default());
        }
        self.label_names = Some(names);
        self.label_catalog_numbers = Some(catalog_numbers);
        self.label_id_providers = Some(providers);
        self.label_id_types = Some(types);
        self.label_id_values = Some(values);
    }

    fn labels(&self) -> anyhow::Result<Option<Value>> {
        let columns = [
            &self.label_names,
            &self.label_catalog_numbers,
            &self.label_id_providers,
            &self.label_id_types,
            &self.label_id_values,
        ];
        if columns.iter().all(|column| column.is_none()) {
            return Ok(None);
        }
        let [
            Some(names),
            Some(catalog_numbers),
            Some(providers),
            Some(types),
            Some(values),
        ] = columns
        else {
            anyhow::bail!("'labels' is stored incompletely");
        };
        anyhow::ensure!(
            [catalog_numbers, providers, types, values]
                .iter()
                .all(|column| column.len() == names.len()),
            "'labels' lists differ in length"
        );
        let labels = (0..names.len())
            .map(|index| {
                let mut label = json!({ "name": names[index] });
                if !catalog_numbers[index].is_empty() {
                    label["catalog_number"] = json!(catalog_numbers[index]);
                }
                if [providers, types, values]
                    .iter()
                    .any(|column| !column[index].is_empty())
                {
                    label["external_id"] = json!({
                        "provider_id": providers[index],
                        "id_type": types[index],
                        "id_value": values[index],
                    });
                }
                label
            })
            .collect();
        Ok(Some(Value::Array(labels)))
    }
}

impl LayerElement for ReleaseLayer {
    fn from_layer(layer: &MetadataLayer) -> anyhow::Result<Self> {
        let fields = validated_fields(MetadataEntityType::Release, &layer.fields)?;
        let mut element = Self {
            source_id: layer.source_id.clone(),
            updated_at: layer.updated_at,
            supplied: fields
                .keys()
                .map(|field| field.as_str().to_string())
                .collect(),
            ..Self::default()
        };
        for (field, value) in fields.into_iter().filter(|(_, value)| !value.is_null()) {
            match field {
                MetadataField::ReleaseTitle => element.release_title = Some(decode(field, &value)?),
                MetadataField::SortTitle => element.sort_title = Some(decode(field, &value)?),
                MetadataField::ReleaseType => element.release_type = Some(decode(field, &value)?),
                MetadataField::ReleaseDate => element.release_date = Some(decode(field, &value)?),
                MetadataField::Genres => element.genres = Some(decode(field, &value)?),
                MetadataField::Labels => element.set_labels(label_inputs(&value)?),
                _ => unreachable!("validated_fields rejects fields a release cannot hold"),
            }
        }
        Ok(element)
    }

    fn to_layer(&self) -> anyhow::Result<MetadataLayer> {
        let fields = stored_fields(
            MetadataEntityType::Release,
            &self.supplied,
            [
                (
                    MetadataField::ReleaseTitle,
                    json_value(&self.release_title)?,
                ),
                (MetadataField::SortTitle, json_value(&self.sort_title)?),
                (MetadataField::ReleaseType, json_value(&self.release_type)?),
                (MetadataField::ReleaseDate, json_value(&self.release_date)?),
                (MetadataField::Genres, json_value(&self.genres)?),
                (MetadataField::Labels, self.labels()?),
            ],
        )?;
        Ok(MetadataLayer::new(
            self.source_id.clone(),
            fields,
            self.updated_at,
        ))
    }

    fn source_id(&self) -> &str {
        &self.source_id
    }

    fn set_db_id(&mut self, db_id: DbId) {
        self.db_id = Some(db_id);
    }

    fn same_values(&self, other: &Self) -> bool {
        Self {
            db_id: other.db_id,
            updated_at: other.updated_at,
            ..self.clone()
        } == *other
    }

    fn optional_keys(&self) -> Vec<(&'static str, bool)> {
        vec![
            ("release_title", self.release_title.is_none()),
            ("sort_title", self.sort_title.is_none()),
            ("release_type", self.release_type.is_none()),
            ("release_date", self.release_date.is_none()),
            ("genres", self.genres.is_none()),
            ("label_names", self.label_names.is_none()),
            (
                "label_catalog_numbers",
                self.label_catalog_numbers.is_none(),
            ),
            ("label_id_providers", self.label_id_providers.is_none()),
            ("label_id_types", self.label_id_types.is_none()),
            ("label_id_values", self.label_id_values.is_none()),
        ]
    }
}

#[derive(DbElement, Clone, Debug, Default, PartialEq)]
pub(super) struct TrackLayer {
    db_id: Option<DbId>,
    source_id: String,
    updated_at: u64,
    supplied: Vec<String>,
    track_title: Option<String>,
    sort_title: Option<String>,
    year: Option<u32>,
    disc: Option<u32>,
    disc_total: Option<u32>,
    track: Option<u32>,
    track_total: Option<u32>,
}

impl LayerElement for TrackLayer {
    fn from_layer(layer: &MetadataLayer) -> anyhow::Result<Self> {
        let fields = validated_fields(MetadataEntityType::Track, &layer.fields)?;
        let mut element = Self {
            source_id: layer.source_id.clone(),
            updated_at: layer.updated_at,
            supplied: fields
                .keys()
                .map(|field| field.as_str().to_string())
                .collect(),
            ..Self::default()
        };
        for (field, value) in fields.into_iter().filter(|(_, value)| !value.is_null()) {
            match field {
                MetadataField::TrackTitle => element.track_title = Some(decode(field, &value)?),
                MetadataField::SortTitle => element.sort_title = Some(decode(field, &value)?),
                MetadataField::Year => element.year = Some(decode(field, &value)?),
                MetadataField::Disc => element.disc = Some(decode(field, &value)?),
                MetadataField::DiscTotal => element.disc_total = Some(decode(field, &value)?),
                MetadataField::Track => element.track = Some(decode(field, &value)?),
                MetadataField::TrackTotal => element.track_total = Some(decode(field, &value)?),
                _ => unreachable!("validated_fields rejects fields a track cannot hold"),
            }
        }
        Ok(element)
    }

    fn to_layer(&self) -> anyhow::Result<MetadataLayer> {
        let fields = stored_fields(
            MetadataEntityType::Track,
            &self.supplied,
            [
                (MetadataField::TrackTitle, json_value(&self.track_title)?),
                (MetadataField::SortTitle, json_value(&self.sort_title)?),
                (MetadataField::Year, json_value(&self.year)?),
                (MetadataField::Disc, json_value(&self.disc)?),
                (MetadataField::DiscTotal, json_value(&self.disc_total)?),
                (MetadataField::Track, json_value(&self.track)?),
                (MetadataField::TrackTotal, json_value(&self.track_total)?),
            ],
        )?;
        Ok(MetadataLayer::new(
            self.source_id.clone(),
            fields,
            self.updated_at,
        ))
    }

    fn source_id(&self) -> &str {
        &self.source_id
    }

    fn set_db_id(&mut self, db_id: DbId) {
        self.db_id = Some(db_id);
    }

    fn same_values(&self, other: &Self) -> bool {
        Self {
            db_id: other.db_id,
            updated_at: other.updated_at,
            ..self.clone()
        } == *other
    }

    fn optional_keys(&self) -> Vec<(&'static str, bool)> {
        vec![
            ("track_title", self.track_title.is_none()),
            ("sort_title", self.sort_title.is_none()),
            ("year", self.year.is_none()),
            ("disc", self.disc.is_none()),
            ("disc_total", self.disc_total.is_none()),
            ("track", self.track.is_none()),
            ("track_total", self.track_total.is_none()),
        ]
    }
}

#[derive(DbElement, Clone, Debug, Default, PartialEq)]
pub(super) struct ArtistLayer {
    db_id: Option<DbId>,
    source_id: String,
    updated_at: u64,
    supplied: Vec<String>,
    artist_name: Option<String>,
    sort_name: Option<String>,
    artist_type: Option<ArtistType>,
    description: Option<String>,
}

impl LayerElement for ArtistLayer {
    fn from_layer(layer: &MetadataLayer) -> anyhow::Result<Self> {
        let fields = validated_fields(MetadataEntityType::Artist, &layer.fields)?;
        let mut element = Self {
            source_id: layer.source_id.clone(),
            updated_at: layer.updated_at,
            supplied: fields
                .keys()
                .map(|field| field.as_str().to_string())
                .collect(),
            ..Self::default()
        };
        for (field, value) in fields.into_iter().filter(|(_, value)| !value.is_null()) {
            match field {
                MetadataField::ArtistName => element.artist_name = Some(decode(field, &value)?),
                MetadataField::SortName => element.sort_name = Some(decode(field, &value)?),
                MetadataField::ArtistType => element.artist_type = Some(decode(field, &value)?),
                MetadataField::Description => element.description = Some(decode(field, &value)?),
                _ => unreachable!("validated_fields rejects fields an artist cannot hold"),
            }
        }
        Ok(element)
    }

    fn to_layer(&self) -> anyhow::Result<MetadataLayer> {
        let fields = stored_fields(
            MetadataEntityType::Artist,
            &self.supplied,
            [
                (MetadataField::ArtistName, json_value(&self.artist_name)?),
                (MetadataField::SortName, json_value(&self.sort_name)?),
                (MetadataField::ArtistType, json_value(&self.artist_type)?),
                (MetadataField::Description, json_value(&self.description)?),
            ],
        )?;
        Ok(MetadataLayer::new(
            self.source_id.clone(),
            fields,
            self.updated_at,
        ))
    }

    fn source_id(&self) -> &str {
        &self.source_id
    }

    fn set_db_id(&mut self, db_id: DbId) {
        self.db_id = Some(db_id);
    }

    fn same_values(&self, other: &Self) -> bool {
        Self {
            db_id: other.db_id,
            updated_at: other.updated_at,
            ..self.clone()
        } == *other
    }

    fn optional_keys(&self) -> Vec<(&'static str, bool)> {
        vec![
            ("artist_name", self.artist_name.is_none()),
            ("sort_name", self.sort_name.is_none()),
            ("artist_type", self.artist_type.is_none()),
            ("description", self.description.is_none()),
        ]
    }
}

/// Validates and normalizes a layer's fields for `entity`.
fn validated_fields(
    entity: MetadataEntityType,
    fields: &LayerFields,
) -> anyhow::Result<LayerFields> {
    fields
        .iter()
        .map(|(&field, value)| {
            anyhow::ensure!(
                entity.supports(field),
                "a {} has no '{field}' field",
                entity.as_str()
            );
            Ok((field, normalize_value(field, value.clone())?))
        })
        .collect()
}

/// Reads stored properties back as layer fields, `null` for a supplied field
/// without its property.
fn stored_fields(
    entity: MetadataEntityType,
    supplied: &[String],
    stored: impl IntoIterator<Item = (MetadataField, Option<Value>)>,
) -> anyhow::Result<LayerFields> {
    let mut fields = LayerFields::new();
    for name in supplied {
        let field = MetadataField::from_name(name)
            .with_context(|| format!("unknown supplied field '{name}'"))?;
        anyhow::ensure!(
            entity.supports(field),
            "a {} has no '{field}' field",
            entity.as_str()
        );
        anyhow::ensure!(
            fields.insert(field, Value::Null).is_none(),
            "'{field}' is supplied twice"
        );
    }
    for (field, value) in stored {
        let Some(value) = value else {
            continue;
        };
        *fields
            .get_mut(&field)
            .with_context(|| format!("'{field}' is stored but not supplied"))? = value;
    }
    fields
        .into_iter()
        .map(|(field, value)| Ok((field, normalize_value(field, value)?)))
        .collect()
}

fn json_value<T: Serialize>(value: &Option<T>) -> anyhow::Result<Option<Value>> {
    Ok(value.as_ref().map(serde_json::to_value).transpose()?)
}
