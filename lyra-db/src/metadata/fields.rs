// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::{
    DbError,
    DbTypeMarker,
    DbValue,
};
use serde::{
    Deserialize,
    Serialize,
};

use crate::entities::MetadataEntityType;

/// A resolvable metadata field, named after the entity property it resolves.
#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(
    Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, DbTypeMarker,
)]
#[serde(rename_all = "snake_case")]
pub enum MetadataField {
    ReleaseTitle,
    TrackTitle,
    ArtistName,
    SortTitle,
    SortName,
    ReleaseType,
    ReleaseDate,
    Genres,
    Labels,
    Credits,
    Year,
    Disc,
    DiscTotal,
    Track,
    TrackTotal,
    ArtistType,
    Description,
    Relations,
}

impl MetadataField {
    pub const ALL: [Self; 18] = [
        Self::ReleaseTitle,
        Self::TrackTitle,
        Self::ArtistName,
        Self::SortTitle,
        Self::SortName,
        Self::ReleaseType,
        Self::ReleaseDate,
        Self::Genres,
        Self::Labels,
        Self::Credits,
        Self::Year,
        Self::Disc,
        Self::DiscTotal,
        Self::Track,
        Self::TrackTotal,
        Self::ArtistType,
        Self::Description,
        Self::Relations,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReleaseTitle => "release_title",
            Self::TrackTitle => "track_title",
            Self::ArtistName => "artist_name",
            Self::SortTitle => "sort_title",
            Self::SortName => "sort_name",
            Self::ReleaseType => "release_type",
            Self::ReleaseDate => "release_date",
            Self::Genres => "genres",
            Self::Labels => "labels",
            Self::Credits => "credits",
            Self::Year => "year",
            Self::Disc => "disc",
            Self::DiscTotal => "disc_total",
            Self::Track => "track",
            Self::TrackTotal => "track_total",
            Self::ArtistType => "artist_type",
            Self::Description => "description",
            Self::Relations => "relations",
        }
    }

    pub fn from_name(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|field| field.as_str() == name)
    }

    /// Fields stored as graph relationships rather than entity properties.
    pub const fn is_graph(self) -> bool {
        matches!(
            self,
            Self::Genres | Self::Labels | Self::Credits | Self::Relations
        )
    }

    /// Fields a layer supplies as one value; it supplies credits and relations
    /// per role and per relation type instead.
    pub const fn is_layered(self) -> bool {
        !matches!(self, Self::Credits | Self::Relations)
    }

    /// Fields that must always hold a value, so they cannot be cleared.
    pub const fn is_required(self) -> bool {
        matches!(
            self,
            Self::ReleaseTitle | Self::TrackTitle | Self::ArtistName
        )
    }
}

impl std::fmt::Display for MetadataField {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<MetadataField> for DbValue {
    fn from(value: MetadataField) -> Self {
        Self::from(value.as_str())
    }
}

impl From<&MetadataField> for DbValue {
    fn from(value: &MetadataField) -> Self {
        (*value).into()
    }
}

impl TryFrom<DbValue> for MetadataField {
    type Error = DbError;

    fn try_from(value: DbValue) -> Result<Self, Self::Error> {
        let name = value.string()?;
        Self::from_name(name).ok_or_else(|| {
            DbError::serialization(
                agdb::DbErrorType::TypeError,
                format!("invalid MetadataField value '{name}'"),
            )
        })
    }
}

impl MetadataEntityType {
    pub(crate) const fn fields(self) -> &'static [MetadataField] {
        match self {
            Self::Release => &[
                MetadataField::ReleaseTitle,
                MetadataField::SortTitle,
                MetadataField::ReleaseType,
                MetadataField::ReleaseDate,
                MetadataField::Description,
                MetadataField::Genres,
                MetadataField::Labels,
                MetadataField::Credits,
            ],
            Self::Track => &[
                MetadataField::TrackTitle,
                MetadataField::SortTitle,
                MetadataField::Year,
                MetadataField::Disc,
                MetadataField::DiscTotal,
                MetadataField::Track,
                MetadataField::TrackTotal,
                MetadataField::Credits,
            ],
            Self::Artist => &[
                MetadataField::ArtistName,
                MetadataField::SortName,
                MetadataField::ArtistType,
                MetadataField::Description,
                MetadataField::Relations,
            ],
        }
    }

    pub fn supports(self, field: MetadataField) -> bool {
        self.fields().contains(&field)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_round_trip_through_serde_and_lookup() {
        for field in MetadataField::ALL {
            assert_eq!(
                serde_json::to_value(field).expect("field serializes"),
                serde_json::Value::String(field.as_str().to_string())
            );
            assert_eq!(MetadataField::from_name(field.as_str()), Some(field));
        }
        assert_eq!(MetadataField::from_name("work_id"), None);
    }

    #[test]
    fn every_field_belongs_to_an_entity_type() {
        for field in MetadataField::ALL {
            assert!(
                [
                    MetadataEntityType::Release,
                    MetadataEntityType::Track,
                    MetadataEntityType::Artist,
                ]
                .into_iter()
                .any(|entity_type| entity_type.supports(field)),
                "{field} is not supported by any entity type"
            );
        }
    }
}
