// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::{
    BTreeMap,
    BTreeSet,
};

use agdb::DbId;
use serde_json::Value;

use crate::db::{
    self,
    DbAccess,
    metadata::manual_overrides::ManualMetadataField,
};
use crate::services::auth::{
    Principal,
    access,
};

use super::{
    MetadataEditingError,
    model::{
        FieldState,
        MetadataCreditValue,
        MetadataEntityType,
        MetadataField,
        MetadataLabelValue,
        MetadataRelationValue,
        MetadataSnapshot,
        MetadataValueSource,
    },
};

#[derive(Clone)]
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
                entity_type_name(self.entity_type),
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

fn entity_type_name(entity_type: MetadataEntityType) -> &'static str {
    match entity_type {
        MetadataEntityType::Release => "release",
        MetadataEntityType::Track => "track",
        MetadataEntityType::Artist => "artist",
    }
}

pub(super) fn internal_field_name(
    entity_type: MetadataEntityType,
    field: MetadataField,
) -> Option<ManualMetadataField> {
    match (entity_type, field) {
        (MetadataEntityType::Release, MetadataField::Title) => {
            Some(ManualMetadataField::ReleaseTitle)
        }
        (MetadataEntityType::Release, MetadataField::SortTitle) => {
            Some(ManualMetadataField::SortTitle)
        }
        (MetadataEntityType::Release, MetadataField::ReleaseType) => {
            Some(ManualMetadataField::ReleaseType)
        }
        (MetadataEntityType::Release, MetadataField::ReleaseDate) => {
            Some(ManualMetadataField::ReleaseDate)
        }
        (MetadataEntityType::Release, MetadataField::Genres) => Some(ManualMetadataField::Genres),
        (MetadataEntityType::Release, MetadataField::Labels) => Some(ManualMetadataField::Labels),
        (MetadataEntityType::Release, MetadataField::Credits) => Some(ManualMetadataField::Credits),
        (MetadataEntityType::Track, MetadataField::Title) => Some(ManualMetadataField::TrackTitle),
        (MetadataEntityType::Track, MetadataField::SortTitle) => {
            Some(ManualMetadataField::SortTitle)
        }
        (MetadataEntityType::Track, MetadataField::Year) => Some(ManualMetadataField::Year),
        (MetadataEntityType::Track, MetadataField::Disc) => Some(ManualMetadataField::Disc),
        (MetadataEntityType::Track, MetadataField::DiscTotal) => {
            Some(ManualMetadataField::DiscTotal)
        }
        (MetadataEntityType::Track, MetadataField::Track) => Some(ManualMetadataField::Track),
        (MetadataEntityType::Track, MetadataField::TrackTotal) => {
            Some(ManualMetadataField::TrackTotal)
        }
        (MetadataEntityType::Track, MetadataField::Credits) => Some(ManualMetadataField::Credits),
        (MetadataEntityType::Artist, MetadataField::Name) => Some(ManualMetadataField::ArtistName),
        (MetadataEntityType::Artist, MetadataField::SortName) => {
            Some(ManualMetadataField::SortName)
        }
        (MetadataEntityType::Artist, MetadataField::ArtistType) => {
            Some(ManualMetadataField::ArtistType)
        }
        (MetadataEntityType::Artist, MetadataField::Description) => {
            Some(ManualMetadataField::Description)
        }
        (MetadataEntityType::Artist, MetadataField::Relations) => {
            Some(ManualMetadataField::Relations)
        }
        _ => None,
    }
}

fn api_field_from_internal(
    entity_type: MetadataEntityType,
    mut fields: impl Iterator<Item = MetadataField>,
    internal_name: ManualMetadataField,
) -> Option<MetadataField> {
    fields.find(|field| {
        internal_field_name(entity_type, *field).is_some_and(|name| name == internal_name)
    })
}

fn optional_value<T: serde::Serialize>(value: Option<T>) -> anyhow::Result<Value> {
    Ok(serde_json::to_value(value)?)
}

pub(super) fn credit_values(
    db: &impl DbAccess,
    owner_id: DbId,
) -> anyhow::Result<Vec<MetadataCreditValue>> {
    Ok(db::artists::get_credited(db, owner_id)?
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
    release: db::Release,
    release_id: DbId,
) -> anyhow::Result<BTreeMap<MetadataField, Value>> {
    let mut genres: Vec<String> = db::genres::get_for_release(db, release_id)?
        .into_iter()
        .map(|genre| genre.name)
        .collect();
    genres.sort_by_key(|name| name.to_lowercase());

    let mut labels: Vec<MetadataLabelValue> = db::labels::get_for_release(db, release_id)?
        .into_iter()
        .map(|entry| MetadataLabelValue {
            id: entry.label.id,
            name: entry.label.name,
            catalog_number: entry.catalog_number,
        })
        .collect();
    labels.sort_by(|a, b| {
        db::labels::normalize_label_name(&a.name)
            .cmp(&db::labels::normalize_label_name(&b.name))
            .then_with(|| a.id.cmp(&b.id))
            .then_with(|| a.catalog_number.cmp(&b.catalog_number))
    });

    Ok(BTreeMap::from([
        (MetadataField::Title, Value::String(release.release_title)),
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
    track: db::Track,
    track_id: DbId,
) -> anyhow::Result<BTreeMap<MetadataField, Value>> {
    Ok(BTreeMap::from([
        (MetadataField::Title, Value::String(track.track_title)),
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
    principal: &Principal,
    artist: db::Artist,
    artist_id: DbId,
) -> anyhow::Result<BTreeMap<MetadataField, Value>> {
    let mut fields = BTreeMap::from([
        (MetadataField::Name, Value::String(artist.artist_name)),
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
    for (relation, target_id) in db::artists::relations::get_relations_from(db, artist_id, None)? {
        if !access::artist_accessible(db, principal, target_id)? {
            return Ok(fields);
        }
        let Some(target) = db::artists::get_by_id(db, target_id)? else {
            return Ok(fields);
        };
        relations.push(MetadataRelationValue {
            target_artist_id: target.id,
            relation_type: relation.relation_type,
            attributes: relation.attributes,
        });
    }
    relations.sort_by(|a, b| {
        a.target_artist_id
            .cmp(&b.target_artist_id)
            .then_with(|| {
                a.relation_type
                    .to_string()
                    .cmp(&b.relation_type.to_string())
            })
            .then_with(|| a.attributes.cmp(&b.attributes))
    });

    fields.insert(MetadataField::Relations, serde_json::to_value(relations)?);
    Ok(fields)
}

pub(super) fn load_entity_state(
    db: &agdb::DbAny,
    principal: &Principal,
    db_id: DbId,
) -> Result<EntityState, MetadataEditingError> {
    let (public_id, entity_type, fields) =
        if let Some(release) = db::releases::get_by_id(db, db_id)? {
            let public_id = release.id.clone();
            (
                public_id,
                MetadataEntityType::Release,
                release_fields(db, release, db_id)?,
            )
        } else if let Some(track) = db::tracks::get_by_id(db, db_id)? {
            let public_id = track.id.clone();
            (
                public_id,
                MetadataEntityType::Track,
                track_fields(db, track, db_id)?,
            )
        } else if let Some(artist) = db::artists::get_by_id(db, db_id)? {
            let public_id = artist.id.clone();
            (
                public_id,
                MetadataEntityType::Artist,
                artist_fields(db, principal, artist, db_id)?,
            )
        } else {
            return Err(MetadataEditingError::EntityNotFound(db_id.0.to_string()));
        };

    let mut manual_fields = BTreeSet::new();
    for internal_name in db::metadata::manual_overrides::field_names(db, db_id)? {
        let Some(field) =
            api_field_from_internal(entity_type, fields.keys().copied(), internal_name)
        else {
            if entity_type == MetadataEntityType::Artist
                && internal_name == ManualMetadataField::Relations
            {
                continue;
            }
            return Err(MetadataEditingError::Internal(anyhow::anyhow!(
                "stored manual metadata field '{}' is invalid for {} {}",
                internal_name.as_str(),
                entity_type_name(entity_type),
                public_id,
            )));
        };
        manual_fields.insert(field);
    }
    let fields = fields
        .into_iter()
        .map(|(field, value)| {
            let source = if manual_fields.contains(&field) {
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
