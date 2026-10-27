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

use super::{
    FieldValue,
    LayerCredits,
    LayerFields,
    LayerRelations,
    MetadataLayer,
    edges,
    field_value,
    normalize_value,
};
use crate::{
    DbAccess,
    MetadataField,
    artists::{
        ArtistType,
        relations::ArtistRelationType,
    },
    credits::CreditType,
    entities::MetadataEntityType,
    labels::LabelLinkInput,
    releases::ReleaseType,
};

/// A stored layer. Each field is a native property, and `supplied` names the
/// fields the source has an opinion on: a supplied field without its property
/// clears it, and an unsupplied field falls through to the next layer.
pub(super) trait LayerElement: DbType<ValueType = Self> + PartialEq + Sized {
    const ENTITY: MetadataEntityType;
    /// Builds the element from a layer [`validated`] for its entity.
    fn from_layer(layer: &MetadataLayer) -> anyhow::Result<Self>;
    /// Reads the stored layer, with relationship fields from its edges.
    fn to_layer(&self, db: &impl DbAccess) -> anyhow::Result<MetadataLayer>;
    /// Writes the relationship fields as edges. Returns false when unchanged.
    fn save_edges(
        db: &mut DbAnyTransactionMut<'_>,
        layer_id: DbId,
        layer: &MetadataLayer,
    ) -> anyhow::Result<bool>;
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
    const ENTITY: MetadataEntityType = MetadataEntityType::Release;

    fn from_layer(layer: &MetadataLayer) -> anyhow::Result<Self> {
        let fields = &layer.fields;
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
        element.release_title = field_value(fields, MetadataField::ReleaseTitle)?;
        element.sort_title = field_value(fields, MetadataField::SortTitle)?;
        element.release_type = field_value(fields, MetadataField::ReleaseType)?;
        element.release_date = field_value(fields, MetadataField::ReleaseDate)?;
        Ok(element)
    }

    fn to_layer(&self, db: &impl DbAccess) -> anyhow::Result<MetadataLayer> {
        let layer_id = self.db_id.context("a stored release layer has an id")?;
        let fields = stored_fields(
            MetadataEntityType::Release,
            &self.supplied,
            [
                (MetadataField::ReleaseTitle, stored(&self.release_title)),
                (MetadataField::SortTitle, stored(&self.sort_title)),
                (MetadataField::ReleaseType, stored(&self.release_type)),
                (MetadataField::ReleaseDate, stored(&self.release_date)),
                (MetadataField::Genres, genre_ids(db, layer_id)?),
                (MetadataField::Labels, label_links(db, layer_id)?),
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
    ) -> anyhow::Result<bool> {
        let genre_ids: Vec<DbId> =
            field_value(&layer.fields, MetadataField::Genres)?.unwrap_or_default();
        let label_links: Vec<LabelLinkInput> =
            field_value(&layer.fields, MetadataField::Labels)?.unwrap_or_default();
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
    const ENTITY: MetadataEntityType = MetadataEntityType::Track;

    fn from_layer(layer: &MetadataLayer) -> anyhow::Result<Self> {
        let fields = &layer.fields;
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
        element.track_title = field_value(fields, MetadataField::TrackTitle)?;
        element.sort_title = field_value(fields, MetadataField::SortTitle)?;
        element.year = field_value(fields, MetadataField::Year)?;
        element.disc = field_value(fields, MetadataField::Disc)?;
        element.disc_total = field_value(fields, MetadataField::DiscTotal)?;
        element.track = field_value(fields, MetadataField::Track)?;
        element.track_total = field_value(fields, MetadataField::TrackTotal)?;
        Ok(element)
    }

    fn to_layer(&self, db: &impl DbAccess) -> anyhow::Result<MetadataLayer> {
        let layer_id = self.db_id.context("a stored track layer has an id")?;
        let fields = stored_fields(
            MetadataEntityType::Track,
            &self.supplied,
            [
                (MetadataField::TrackTitle, stored(&self.track_title)),
                (MetadataField::SortTitle, stored(&self.sort_title)),
                (MetadataField::Year, stored(&self.year)),
                (MetadataField::Disc, stored(&self.disc)),
                (MetadataField::DiscTotal, stored(&self.disc_total)),
                (MetadataField::Track, stored(&self.track)),
                (MetadataField::TrackTotal, stored(&self.track_total)),
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
    const ENTITY: MetadataEntityType = MetadataEntityType::Artist;

    fn from_layer(layer: &MetadataLayer) -> anyhow::Result<Self> {
        let fields = &layer.fields;
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
        element.artist_name = field_value(fields, MetadataField::ArtistName)?;
        element.sort_name = field_value(fields, MetadataField::SortName)?;
        element.artist_type = field_value(fields, MetadataField::ArtistType)?;
        element.description = field_value(fields, MetadataField::Description)?;
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
                (MetadataField::ArtistName, stored(&self.artist_name)),
                (MetadataField::SortName, stored(&self.sort_name)),
                (MetadataField::ArtistType, stored(&self.artist_type)),
                (MetadataField::Description, stored(&self.description)),
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

/// `layer` checked against what `entity` holds, with its values normalized.
pub(super) fn validated(
    entity: MetadataEntityType,
    layer: &MetadataLayer,
) -> anyhow::Result<MetadataLayer> {
    let is_artist = entity == MetadataEntityType::Artist;
    anyhow::ensure!(
        is_artist || layer.relations.is_empty(),
        "only an artist has relations"
    );
    anyhow::ensure!(
        !is_artist || layer.credits.is_empty(),
        "an artist has no credits"
    );
    let fields = layer
        .fields
        .iter()
        .map(|(&field, value)| {
            anyhow::ensure!(
                entity.supports(field),
                "a {} has no '{field}' field",
                entity.as_str()
            );
            Ok((field, normalize_value(field, value.clone())?))
        })
        .collect::<anyhow::Result<_>>()?;
    Ok(MetadataLayer {
        fields,
        ..layer.clone()
    })
}

/// Reads stored values back as layer fields, `None` for a supplied field
/// without a stored value.
fn stored_fields(
    entity: MetadataEntityType,
    supplied: &[String],
    stored: impl IntoIterator<Item = (MetadataField, Option<FieldValue>)>,
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
            fields.insert(field, None).is_none(),
            "'{field}' is supplied twice"
        );
    }
    for (field, value) in stored {
        let Some(value) = value else {
            continue;
        };
        *fields
            .get_mut(&field)
            .with_context(|| format!("'{field}' is stored but not supplied"))? = Some(value);
    }
    fields
        .into_iter()
        .map(|(field, value)| Ok((field, normalize_value(field, value)?)))
        .collect()
}

fn stored<T: Clone + Into<FieldValue>>(value: &Option<T>) -> Option<FieldValue> {
    value.clone().map(Into::into)
}

/// A release layer's genres, in order, or `None` when it links none.
fn genre_ids(db: &impl DbAccess, layer_id: DbId) -> anyhow::Result<Option<FieldValue>> {
    let genre_ids: Vec<DbId> = edges::genres(db, layer_id)?
        .into_iter()
        .map(|link| link.target_id)
        .collect();
    Ok((!genre_ids.is_empty()).then_some(FieldValue::Genres(genre_ids)))
}

/// A release layer's labels with their catalog numbers, in order, or `None`
/// when it links none.
fn label_links(db: &impl DbAccess, layer_id: DbId) -> anyhow::Result<Option<FieldValue>> {
    let links: Vec<LabelLinkInput> = edges::labels(db, layer_id)?
        .into_iter()
        .map(|link| LabelLinkInput {
            label_id: link.target_id,
            catalog_number: link.edge.catalog_number,
        })
        .collect();
    Ok((!links.is_empty()).then_some(FieldValue::Labels(links)))
}
