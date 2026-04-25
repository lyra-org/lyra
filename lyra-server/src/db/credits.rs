// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::{
    DbElement,
    DbError,
    DbTypeMarker,
    DbValue,
};
use schemars::JsonSchema;
use serde::{
    Deserialize,
    Serialize,
};

use super::NodeId;

pub(crate) const EDGE_ORDER_KEY: &str = "artist_order";

#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    JsonSchema,
    DbTypeMarker,
)]
#[serde(rename_all = "lowercase")]
#[harmony_macros::enumeration]
pub(crate) enum CreditType {
    #[default]
    Artist,
    Vocalist,
    Instrumentalist,
    Composer,
    Lyricist,
    Arranger,
    Writer,
    Producer,
    Conductor,
    Engineer,
    Mixer,
    Remixer,
}

impl CreditType {
    fn as_db_str(self) -> &'static str {
        match self {
            Self::Artist => "artist",
            Self::Vocalist => "vocalist",
            Self::Instrumentalist => "instrumentalist",
            Self::Composer => "composer",
            Self::Lyricist => "lyricist",
            Self::Arranger => "arranger",
            Self::Writer => "writer",
            Self::Producer => "producer",
            Self::Conductor => "conductor",
            Self::Engineer => "engineer",
            Self::Mixer => "mixer",
            Self::Remixer => "remixer",
        }
    }

    pub(crate) fn from_db_str(value: &str) -> Result<Self, DbError> {
        match value {
            "artist" => Ok(Self::Artist),
            "vocalist" => Ok(Self::Vocalist),
            "instrumentalist" => Ok(Self::Instrumentalist),
            "composer" => Ok(Self::Composer),
            "lyricist" => Ok(Self::Lyricist),
            "arranger" => Ok(Self::Arranger),
            "writer" => Ok(Self::Writer),
            "producer" => Ok(Self::Producer),
            "conductor" => Ok(Self::Conductor),
            "engineer" => Ok(Self::Engineer),
            "mixer" => Ok(Self::Mixer),
            "remixer" => Ok(Self::Remixer),
            _ => Err(DbError::from(format!("invalid CreditType value '{value}'"))),
        }
    }
}

impl std::fmt::Display for CreditType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_db_str())
    }
}

impl From<CreditType> for DbValue {
    fn from(value: CreditType) -> Self {
        Self::from(value.as_db_str())
    }
}

impl From<&CreditType> for DbValue {
    fn from(value: &CreditType) -> Self {
        (*value).into()
    }
}

impl TryFrom<DbValue> for CreditType {
    type Error = DbError;

    fn try_from(value: DbValue) -> Result<Self, Self::Error> {
        Self::from_db_str(value.string()?)
    }
}

harmony_macros::compile!(type_path = CreditType, variants = true);

#[derive(DbElement, Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[harmony_macros::structure]
pub(crate) struct Credit {
    pub(crate) db_id: Option<NodeId>,
    pub(crate) id: String,
    pub(crate) credit_type: CreditType,
    pub(crate) detail: Option<String>,
}
