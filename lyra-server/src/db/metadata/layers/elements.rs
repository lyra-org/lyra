// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::{
    DbAnyTransactionMut,
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
    LayerCredits,
    LayerFields,
    LayerRelations,
    MetadataLayer,
    decode,
    edges,
    label_inputs,
    normalize_value,
};
use crate::db::{
    DbAccess,
    MetadataField,
    artists::{
        ArtistType,
        relations::ArtistRelationType,
    },
    credits::CreditType,
    entities::MetadataEntityType,
    genres,
    labels::{
        self,
        LabelLinkInput,
    },
    releases::ReleaseType,
};

/// A stored layer. Each field is a native property, and `supplied` names the
/// fields the source has an opinion on: a supplied field without its property
/// clears it, and an unsupplied field falls through to the next layer.
pub(super) trait LayerElement: DbType<ValueType = Self> + PartialEq + Sized {
    fn from_layer(layer: &MetadataLayer) -> anyhow::Result<Self>;
    /// Reads the stored layer, with relationship fields from its edges.
    fn to_layer(&self, db: &impl DbAccess) -> anyhow::Result<MetadataLayer>;
    /// Writes the relationship fields as edges, linking `labels` by id when
    /// given. Returns false when unchanged.
    fn save_edges(
        _db: &mut DbAnyTransactionMut<'_>,
        _layer_id: DbId,
        _layer: &MetadataLayer,
        _labels: Option<&[LabelLinkInput]>,
    ) -> anyhow::Result<bool> {
        Ok(false)
    }
    fn source_id(&self) -> &str;
    fn updated_at(&self) -> u64;
    fn set_db_id(&mut self, db_id: Option<DbId>);
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
}

impl LayerElement for ReleaseLayer {
    fn from_layer(layer: &MetadataLayer) -> anyhow::Result<Self> {
        let fields = validated_fields(MetadataEntityType::Release, &layer.fields)?;
        anyhow::ensure!(layer.relations.is_empty(), "only an artist has relations");
        let mut element = Self {
            source_id: layer.source_id.clone(),
            updated_at: layer.updated_at,
            supplied: fields
                .keys()
                .map(|field| field.as_str().to_string())
                .chain(
                    layer
                        .credits
                        .keys()
                        .map(|&credit_type| credit_key(credit_type)),
                )
                .collect(),
            ..Self::default()
        };
        for (field, value) in fields.into_iter().filter(|(_, value)| !value.is_null()) {
            match field {
                MetadataField::ReleaseTitle => element.release_title = Some(decode(field, &value)?),
                MetadataField::SortTitle => element.sort_title = Some(decode(field, &value)?),
                MetadataField::ReleaseType => element.release_type = Some(decode(field, &value)?),
                MetadataField::ReleaseDate => element.release_date = Some(decode(field, &value)?),
                // Stored as edges by `save_edges`.
                MetadataField::Genres | MetadataField::Labels => {}
                _ => unreachable!("validated_fields rejects fields a release cannot hold"),
            }
        }
        Ok(element)
    }

    fn to_layer(&self, db: &impl DbAccess) -> anyhow::Result<MetadataLayer> {
        let layer_id = self.db_id.context("a stored release layer has an id")?;
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
                (MetadataField::Genres, genre_names(db, layer_id)?),
                (MetadataField::Labels, label_values(db, layer_id)?),
            ],
        )?;
        Ok(
            MetadataLayer::new(self.source_id.clone(), fields, self.updated_at)
                .with_credits(stored_credits(db, layer_id, &self.supplied)?),
        )
    }

    fn source_id(&self) -> &str {
        &self.source_id
    }

    fn updated_at(&self) -> u64 {
        self.updated_at
    }

    fn set_db_id(&mut self, db_id: Option<DbId>) {
        self.db_id = db_id;
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
        ]
    }

    fn save_edges(
        db: &mut DbAnyTransactionMut<'_>,
        layer_id: DbId,
        layer: &MetadataLayer,
        labels: Option<&[LabelLinkInput]>,
    ) -> anyhow::Result<bool> {
        let supplied = |field| layer.fields.get(&field).filter(|value| !value.is_null());
        let genre_ids = match supplied(MetadataField::Genres) {
            Some(value) => {
                genres::resolve_names(db, &decode::<Vec<String>>(MetadataField::Genres, value)?)?
            }
            None => Vec::new(),
        };
        let label_links = match (supplied(MetadataField::Labels), labels) {
            (Some(_), Some(labels)) => labels.to_vec(),
            (Some(value), None) => labels::resolve_label_inputs(db, &label_inputs(value)?)?,
            (None, _) => Vec::new(),
        };
        let genres_changed = edges::replace_genres(db, layer_id, &genre_ids)?;
        let labels_changed = edges::replace_labels(db, layer_id, &label_links)?;
        let credits_changed = save_credits(db, layer_id, layer)?;
        Ok(genres_changed || labels_changed || credits_changed)
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
        anyhow::ensure!(layer.relations.is_empty(), "only an artist has relations");
        let mut element = Self {
            source_id: layer.source_id.clone(),
            updated_at: layer.updated_at,
            supplied: fields
                .keys()
                .map(|field| field.as_str().to_string())
                .chain(
                    layer
                        .credits
                        .keys()
                        .map(|&credit_type| credit_key(credit_type)),
                )
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

    fn to_layer(&self, db: &impl DbAccess) -> anyhow::Result<MetadataLayer> {
        let layer_id = self.db_id.context("a stored track layer has an id")?;
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
        Ok(
            MetadataLayer::new(self.source_id.clone(), fields, self.updated_at)
                .with_credits(stored_credits(db, layer_id, &self.supplied)?),
        )
    }

    fn source_id(&self) -> &str {
        &self.source_id
    }

    fn updated_at(&self) -> u64 {
        self.updated_at
    }

    fn set_db_id(&mut self, db_id: Option<DbId>) {
        self.db_id = db_id;
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

    fn save_edges(
        db: &mut DbAnyTransactionMut<'_>,
        layer_id: DbId,
        layer: &MetadataLayer,
        _labels: Option<&[LabelLinkInput]>,
    ) -> anyhow::Result<bool> {
        save_credits(db, layer_id, layer)
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
        anyhow::ensure!(layer.credits.is_empty(), "an artist has no credits");
        let mut element = Self {
            source_id: layer.source_id.clone(),
            updated_at: layer.updated_at,
            supplied: fields
                .keys()
                .map(|field| field.as_str().to_string())
                .chain(
                    layer
                        .relations
                        .keys()
                        .map(|&relation_type| relation_key(relation_type)),
                )
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

    fn to_layer(&self, db: &impl DbAccess) -> anyhow::Result<MetadataLayer> {
        let layer_id = self.db_id.context("a stored artist layer has an id")?;
        let mut relations = LayerRelations::new();
        for name in &self.supplied {
            if let Some(relation_type) = parse_relation_key(name)? {
                anyhow::ensure!(
                    relations.insert(relation_type, Vec::new()).is_none(),
                    "'{name}' is supplied twice"
                );
            }
        }
        for link in edges::relations(db, layer_id)? {
            relations
                .get_mut(&link.edge.relation_type)
                .with_context(|| {
                    format!(
                        "'{}' is stored but not supplied",
                        relation_key(link.edge.relation_type)
                    )
                })?
                .push(edges::LayerRelationInput {
                    artist_id: link.target_id,
                    attributes: link.edge.attributes,
                });
        }
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
        Ok(
            MetadataLayer::new(self.source_id.clone(), fields, self.updated_at)
                .with_relations(relations),
        )
    }

    fn source_id(&self) -> &str {
        &self.source_id
    }

    fn updated_at(&self) -> u64 {
        self.updated_at
    }

    fn set_db_id(&mut self, db_id: Option<DbId>) {
        self.db_id = db_id;
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

    fn save_edges(
        db: &mut DbAnyTransactionMut<'_>,
        layer_id: DbId,
        layer: &MetadataLayer,
        _labels: Option<&[LabelLinkInput]>,
    ) -> anyhow::Result<bool> {
        let mut changed = false;
        for relation_type in ArtistRelationType::ALL {
            let desired = layer
                .relations
                .get(&relation_type)
                .map(Vec::as_slice)
                .unwrap_or_default();
            changed |= edges::replace_relations(db, layer_id, relation_type, desired)?;
        }
        Ok(changed)
    }
}

/// The supplied key of one credit role, such as `credits.composer`.
fn credit_key(credit_type: CreditType) -> String {
    format!("credits.{credit_type}")
}

/// The credit role a supplied key names, or `None` for any other key.
fn parse_credit_key(name: &str) -> anyhow::Result<Option<CreditType>> {
    let Some(credit_type) = name.strip_prefix("credits.") else {
        return Ok(None);
    };
    Ok(Some(CreditType::from_db_str(credit_type).with_context(
        || format!("unknown supplied credit role '{credit_type}'"),
    )?))
}

/// A release or track layer's supplied credit roles, each in credit order.
fn stored_credits(
    db: &impl DbAccess,
    layer_id: DbId,
    supplied: &[String],
) -> anyhow::Result<LayerCredits> {
    let mut credits = LayerCredits::new();
    for name in supplied {
        if let Some(credit_type) = parse_credit_key(name)? {
            anyhow::ensure!(
                credits.insert(credit_type, Vec::new()).is_none(),
                "'{name}' is supplied twice"
            );
        }
    }
    for link in edges::credits(db, layer_id)? {
        credits
            .get_mut(&link.edge.credit_type)
            .with_context(|| {
                format!(
                    "'{}' is stored but not supplied",
                    credit_key(link.edge.credit_type)
                )
            })?
            .push(edges::LayerCreditInput {
                artist_id: link.target_id,
                detail: link.edge.detail,
            });
    }
    Ok(credits)
}

fn save_credits(
    db: &mut DbAnyTransactionMut<'_>,
    layer_id: DbId,
    layer: &MetadataLayer,
) -> anyhow::Result<bool> {
    let mut changed = false;
    for credit_type in CreditType::ALL {
        let desired = layer
            .credits
            .get(&credit_type)
            .map(Vec::as_slice)
            .unwrap_or_default();
        changed |= edges::replace_credits(db, layer_id, credit_type, desired)?;
    }
    Ok(changed)
}

/// The supplied key of one relation type, such as `relations.voice_actor`.
pub(super) fn relation_key(relation_type: ArtistRelationType) -> String {
    format!("relations.{relation_type}")
}

/// The relation type a supplied key names, or `None` for a field name.
fn parse_relation_key(name: &str) -> anyhow::Result<Option<ArtistRelationType>> {
    let Some(relation_type) = name.strip_prefix("relations.") else {
        return Ok(None);
    };
    Ok(Some(
        ArtistRelationType::from_db_str(relation_type)
            .with_context(|| format!("unknown supplied relation type '{relation_type}'"))?,
    ))
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
        if parse_relation_key(name)?.is_some() {
            anyhow::ensure!(
                entity == MetadataEntityType::Artist,
                "a {} has no relations",
                entity.as_str()
            );
            continue;
        }
        if parse_credit_key(name)?.is_some() {
            anyhow::ensure!(
                entity != MetadataEntityType::Artist,
                "an artist has no credits"
            );
            continue;
        }
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

/// A release layer's genres, by name, or `None` when it links none.
fn genre_names(db: &impl DbAccess, layer_id: DbId) -> anyhow::Result<Option<Value>> {
    let mut names = Vec::new();
    for link in edges::genres(db, layer_id)? {
        let genre = genres::get_by_id(db, link.target_id)?
            .with_context(|| format!("genre {} is gone", link.target_id.0))?;
        names.push(Value::String(genre.name));
    }
    Ok((!names.is_empty()).then_some(Value::Array(names)))
}

/// A release layer's labels with their catalog numbers, or `None` when it links none.
fn label_values(db: &impl DbAccess, layer_id: DbId) -> anyhow::Result<Option<Value>> {
    let mut values = Vec::new();
    for link in edges::labels(db, layer_id)? {
        let label = labels::get_by_id(db, link.target_id)?
            .with_context(|| format!("label {} is gone", link.target_id.0))?;
        let mut value = json!({ "name": label.name });
        if let Some(catalog_number) = link.edge.catalog_number {
            value["catalog_number"] = json!(catalog_number);
        }
        values.push(value);
    }
    Ok((!values.is_empty()).then_some(Value::Array(values)))
}

fn json_value<T: Serialize>(value: &Option<T>) -> anyhow::Result<Option<Value>> {
    Ok(value.as_ref().map(serde_json::to_value).transpose()?)
}
