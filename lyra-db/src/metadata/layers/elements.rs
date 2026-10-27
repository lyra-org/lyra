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

/// The [`LayerElement`] methods every element implements alike over its
/// `db_id`, `source_id`, and `updated_at`.
macro_rules! shared_methods {
    () => {
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
    };
}

/// A stored layer. `supplied` names the fields the source has an opinion on,
/// each held as a native property or, for genres and labels, as edges: a
/// supplied field with neither clears it, and an unsupplied field falls through
/// to the next layer. `supplied_credits` and `supplied_relations` do the same
/// for credit roles and relation types. Values stored for anything unsupplied
/// are never read.
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

#[derive(DbElement, Clone, Debug, PartialEq)]
pub(super) struct ReleaseLayer {
    db_id: Option<DbId>,
    source_id: String,
    updated_at: u64,
    supplied: Vec<MetadataField>,
    supplied_credits: Vec<CreditType>,
    release_title: Option<String>,
    sort_title: Option<String>,
    release_type: Option<ReleaseType>,
    release_date: Option<String>,
}

impl LayerElement for ReleaseLayer {
    const ENTITY: MetadataEntityType = MetadataEntityType::Release;

    fn from_layer(layer: &MetadataLayer) -> anyhow::Result<Self> {
        let fields = &layer.fields;
        Ok(Self {
            db_id: None,
            source_id: layer.source_id.clone(),
            updated_at: layer.updated_at,
            supplied: fields.keys().copied().collect(),
            supplied_credits: layer.credits.keys().copied().collect(),
            release_title: field_value(fields, MetadataField::ReleaseTitle)?,
            sort_title: field_value(fields, MetadataField::SortTitle)?,
            release_type: field_value(fields, MetadataField::ReleaseType)?,
            release_date: field_value(fields, MetadataField::ReleaseDate)?,
        })
    }

    fn to_layer(&self, db: &impl DbAccess) -> anyhow::Result<MetadataLayer> {
        let layer_id = self.db_id.context("a stored release layer has an id")?;
        let mut fields = read_fields(
            &self.supplied,
            [
                (MetadataField::ReleaseTitle, stored(&self.release_title)),
                (MetadataField::SortTitle, stored(&self.sort_title)),
                (MetadataField::ReleaseType, stored(&self.release_type)),
                (MetadataField::ReleaseDate, stored(&self.release_date)),
            ],
        );
        if self.supplied.contains(&MetadataField::Genres) {
            fields.insert(MetadataField::Genres, genre_ids(db, layer_id)?);
        }
        if self.supplied.contains(&MetadataField::Labels) {
            fields.insert(MetadataField::Labels, label_links(db, layer_id)?);
        }
        Ok(
            MetadataLayer::new(self.source_id.clone(), fields, self.updated_at)
                .with_credits(read_credits(db, layer_id, &self.supplied_credits)?),
        )
    }

    shared_methods!();

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

#[derive(DbElement, Clone, Debug, PartialEq)]
pub(super) struct TrackLayer {
    db_id: Option<DbId>,
    source_id: String,
    updated_at: u64,
    supplied: Vec<MetadataField>,
    supplied_credits: Vec<CreditType>,
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
        Ok(Self {
            db_id: None,
            source_id: layer.source_id.clone(),
            updated_at: layer.updated_at,
            supplied: fields.keys().copied().collect(),
            supplied_credits: layer.credits.keys().copied().collect(),
            track_title: field_value(fields, MetadataField::TrackTitle)?,
            sort_title: field_value(fields, MetadataField::SortTitle)?,
            year: field_value(fields, MetadataField::Year)?,
            disc: field_value(fields, MetadataField::Disc)?,
            disc_total: field_value(fields, MetadataField::DiscTotal)?,
            track: field_value(fields, MetadataField::Track)?,
            track_total: field_value(fields, MetadataField::TrackTotal)?,
        })
    }

    fn to_layer(&self, db: &impl DbAccess) -> anyhow::Result<MetadataLayer> {
        let layer_id = self.db_id.context("a stored track layer has an id")?;
        let fields = read_fields(
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
        );
        Ok(
            MetadataLayer::new(self.source_id.clone(), fields, self.updated_at)
                .with_credits(read_credits(db, layer_id, &self.supplied_credits)?),
        )
    }

    shared_methods!();

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

#[derive(DbElement, Clone, Debug, PartialEq)]
pub(super) struct ArtistLayer {
    db_id: Option<DbId>,
    source_id: String,
    updated_at: u64,
    supplied: Vec<MetadataField>,
    supplied_relations: Vec<ArtistRelationType>,
    artist_name: Option<String>,
    sort_name: Option<String>,
    artist_type: Option<ArtistType>,
    description: Option<String>,
}

impl LayerElement for ArtistLayer {
    const ENTITY: MetadataEntityType = MetadataEntityType::Artist;

    fn from_layer(layer: &MetadataLayer) -> anyhow::Result<Self> {
        let fields = &layer.fields;
        Ok(Self {
            db_id: None,
            source_id: layer.source_id.clone(),
            updated_at: layer.updated_at,
            supplied: fields.keys().copied().collect(),
            supplied_relations: layer.relations.keys().copied().collect(),
            artist_name: field_value(fields, MetadataField::ArtistName)?,
            sort_name: field_value(fields, MetadataField::SortName)?,
            artist_type: field_value(fields, MetadataField::ArtistType)?,
            description: field_value(fields, MetadataField::Description)?,
        })
    }

    fn to_layer(&self, db: &impl DbAccess) -> anyhow::Result<MetadataLayer> {
        let layer_id = self.db_id.context("a stored artist layer has an id")?;
        let fields = read_fields(
            &self.supplied,
            [
                (MetadataField::ArtistName, stored(&self.artist_name)),
                (MetadataField::SortName, stored(&self.sort_name)),
                (MetadataField::ArtistType, stored(&self.artist_type)),
                (MetadataField::Description, stored(&self.description)),
            ],
        );
        Ok(
            MetadataLayer::new(self.source_id.clone(), fields, self.updated_at)
                .with_relations(read_relations(db, layer_id, &self.supplied_relations)?),
        )
    }

    shared_methods!();

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

/// A release or track layer's supplied credit roles, each in credit order.
fn read_credits(
    db: &impl DbAccess,
    layer_id: DbId,
    supplied: &[CreditType],
) -> anyhow::Result<LayerCredits> {
    if supplied.is_empty() {
        return Ok(LayerCredits::new());
    }
    let mut credits: LayerCredits = supplied
        .iter()
        .map(|&credit_type| (credit_type, Vec::new()))
        .collect();
    for link in edges::credits(db, layer_id)? {
        if let Some(inputs) = credits.get_mut(&link.edge.credit_type) {
            inputs.push(edges::LayerCreditInput {
                artist_id: link.target_id,
                detail: link.edge.detail,
            });
        }
    }
    Ok(credits)
}

/// An artist layer's supplied relation types.
fn read_relations(
    db: &impl DbAccess,
    layer_id: DbId,
    supplied: &[ArtistRelationType],
) -> anyhow::Result<LayerRelations> {
    if supplied.is_empty() {
        return Ok(LayerRelations::new());
    }
    let mut relations: LayerRelations = supplied
        .iter()
        .map(|&relation_type| (relation_type, Vec::new()))
        .collect();
    for link in edges::relations(db, layer_id)? {
        if let Some(inputs) = relations.get_mut(&link.edge.relation_type) {
            inputs.push(edges::LayerRelationInput {
                artist_id: link.target_id,
                attributes: link.edge.attributes,
            });
        }
    }
    Ok(relations)
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

/// An element's supplied fields, `None` for one it stores no value for.
fn read_fields(
    supplied: &[MetadataField],
    stored: impl IntoIterator<Item = (MetadataField, Option<FieldValue>)>,
) -> LayerFields {
    stored
        .into_iter()
        .filter(|(field, _)| supplied.contains(field))
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
