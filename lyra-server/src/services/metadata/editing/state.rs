// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::BTreeMap;

use agdb::DbId;
use serde_json::Value;

use crate::services::metadata::layers::MANUAL_SOURCE_ID;
use lyra_db::{
    DbAccess,
    MetadataField,
    MetadataLayer,
    entities::MetadataEntityType,
};

use super::{
    MetadataEditingError,
    model::{
        FieldState,
        MetadataCreditValue,
        MetadataLabelValue,
        MetadataRelationValue,
        MetadataSnapshot,
        MetadataValueSource,
    },
    normalize::sort_relations,
};

pub(super) struct EntityState {
    pub(super) db_id: DbId,
    pub(super) public_id: String,
    pub(super) entity_type: MetadataEntityType,
    pub(super) fields: BTreeMap<MetadataField, FieldState>,
}

impl EntityState {
    pub(super) fn field_state(
        &self,
        field: MetadataField,
    ) -> Result<&FieldState, MetadataEditingError> {
        self.fields.get(&field).ok_or_else(|| {
            MetadataEditingError::BadRequest(format!(
                "field '{}' is not editable for {}",
                field.as_str(),
                self.entity_type.as_str(),
            ))
        })
    }

    pub(super) fn response(&self) -> MetadataSnapshot {
        MetadataSnapshot {
            entity_id: self.public_id.clone(),
            entity_type: self.entity_type,
            fields: self
                .fields
                .iter()
                .map(|(field, state)| (field.as_str().to_string(), state.clone()))
                .collect(),
        }
    }
}

/// Whether `layer` supplies `field`; credits and relations by any role or type.
pub(super) fn supplies(layer: &MetadataLayer, field: MetadataField) -> bool {
    match field {
        MetadataField::Credits => !layer.credits.is_empty(),
        MetadataField::Relations => !layer.relations.is_empty(),
        field => layer.fields.contains_key(&field),
    }
}

fn optional_value<T: serde::Serialize>(value: Option<T>) -> anyhow::Result<Value> {
    Ok(serde_json::to_value(value)?)
}

pub(super) fn credit_values(
    db: &impl DbAccess,
    owner_id: DbId,
) -> anyhow::Result<Vec<MetadataCreditValue>> {
    Ok(lyra_db::artists::get_credited(db, owner_id)?
        .into_iter()
        .map(|credited| MetadataCreditValue {
            artist_id: credited.artist.id,
            credit_type: credited.credit.credit_type,
            detail: credited.credit.detail,
        })
        .collect())
}

fn release_fields(
    db: &impl DbAccess,
    release: lyra_db::Release,
    release_id: DbId,
) -> anyhow::Result<BTreeMap<MetadataField, Value>> {
    let mut genres: Vec<String> = lyra_db::genres::get_for_release(db, release_id)?
        .into_iter()
        .map(|genre| genre.name)
        .collect();
    genres.sort_by_key(|name| name.to_lowercase());

    let mut labels: Vec<MetadataLabelValue> = lyra_db::labels::get_for_release(db, release_id)?
        .into_iter()
        .map(|entry| MetadataLabelValue {
            id: entry.label.id,
            name: entry.label.name,
            catalog_number: entry.catalog_number,
        })
        .collect();
    labels.sort_by(|a, b| {
        lyra_db::labels::normalize_label_name(&a.name)
            .cmp(&lyra_db::labels::normalize_label_name(&b.name))
            .then_with(|| a.id.cmp(&b.id))
            .then_with(|| a.catalog_number.cmp(&b.catalog_number))
    });

    Ok(BTreeMap::from([
        (
            MetadataField::ReleaseTitle,
            Value::String(release.release_title),
        ),
        (
            MetadataField::SortTitle,
            optional_value(release.sort_title)?,
        ),
        (
            MetadataField::ReleaseType,
            optional_value(release.release_type)?,
        ),
        (
            MetadataField::ReleaseDate,
            optional_value(release.release_date)?,
        ),
        (MetadataField::Genres, serde_json::to_value(genres)?),
        (MetadataField::Labels, serde_json::to_value(labels)?),
        (
            MetadataField::Credits,
            serde_json::to_value(credit_values(db, release_id)?)?,
        ),
    ]))
}

fn track_fields(
    db: &impl DbAccess,
    track: lyra_db::Track,
    track_id: DbId,
) -> anyhow::Result<BTreeMap<MetadataField, Value>> {
    Ok(BTreeMap::from([
        (MetadataField::TrackTitle, Value::String(track.track_title)),
        (MetadataField::SortTitle, optional_value(track.sort_title)?),
        (MetadataField::Year, optional_value(track.year)?),
        (MetadataField::Disc, optional_value(track.disc)?),
        (MetadataField::DiscTotal, optional_value(track.disc_total)?),
        (MetadataField::Track, optional_value(track.track)?),
        (
            MetadataField::TrackTotal,
            optional_value(track.track_total)?,
        ),
        (
            MetadataField::Credits,
            serde_json::to_value(credit_values(db, track_id)?)?,
        ),
    ]))
}

fn artist_fields(
    db: &agdb::DbAny,
    artist: lyra_db::Artist,
    artist_id: DbId,
) -> anyhow::Result<BTreeMap<MetadataField, Value>> {
    let mut fields = BTreeMap::from([
        (MetadataField::ArtistName, Value::String(artist.artist_name)),
        (MetadataField::SortName, optional_value(artist.sort_name)?),
        (
            MetadataField::ArtistType,
            optional_value(artist.artist_type)?,
        ),
        (
            MetadataField::Description,
            optional_value(artist.description)?,
        ),
    ]);
    let mut relations = Vec::new();
    for (relation, source_id) in lyra_db::artists::relations::get_relations_to(db, artist_id, None)?
    {
        let Some(source) = lyra_db::artists::get_by_id(db, source_id)? else {
            return Ok(fields);
        };
        relations.push(MetadataRelationValue {
            artist_id: source.id,
            relation_type: relation.relation_type,
            attributes: relation.attributes,
        });
    }
    sort_relations(&mut relations);

    fields.insert(MetadataField::Relations, serde_json::to_value(relations)?);
    Ok(fields)
}

pub(super) fn load_entity_state(
    db: &agdb::DbAny,
    db_id: DbId,
) -> Result<EntityState, MetadataEditingError> {
    let (public_id, entity_type, fields) =
        if let Some(release) = lyra_db::releases::get_by_id(db, db_id)? {
            let public_id = release.id.clone();
            (
                public_id,
                MetadataEntityType::Release,
                release_fields(db, release, db_id)?,
            )
        } else if let Some(track) = lyra_db::tracks::get_by_id(db, db_id)? {
            let public_id = track.id.clone();
            (
                public_id,
                MetadataEntityType::Track,
                track_fields(db, track, db_id)?,
            )
        } else if let Some(artist) = lyra_db::artists::get_by_id(db, db_id)? {
            let public_id = artist.id.clone();
            (
                public_id,
                MetadataEntityType::Artist,
                artist_fields(db, artist, db_id)?,
            )
        } else {
            return Err(MetadataEditingError::EntityNotFound(db_id.0.to_string()));
        };

    let manual = lyra_db::metadata::layers::find(db, db_id, MANUAL_SOURCE_ID)?;
    let fields = fields
        .into_iter()
        .map(|(field, value)| {
            let source = if manual.as_ref().is_some_and(|layer| supplies(layer, field)) {
                MetadataValueSource::Manual
            } else {
                MetadataValueSource::Resolved
            };
            (field, FieldState { value, source })
        })
        .collect();

    Ok(EntityState {
        db_id,
        public_id,
        entity_type,
        fields,
    })
}
