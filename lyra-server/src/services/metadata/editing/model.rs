// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::BTreeMap;

use serde::{
    Deserialize,
    Serialize,
};
use serde_json::Value;

use crate::db::{
    self,
    MetadataField,
    entities::MetadataEntityType,
};

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum MetadataEditOperation {
    Set { value: Value },
    Inherit,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct MetadataChangeRequest {
    pub(crate) field: MetadataField,
    #[serde(flatten)]
    pub(crate) edit: MetadataEditOperation,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MetadataPreviewRequest {
    pub(crate) changes: Vec<MetadataChangeRequest>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MetadataValueSource {
    Resolved,
    Manual,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FieldState {
    pub(crate) value: Value,
    pub(crate) source: MetadataValueSource,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MetadataApplyRequest {
    pub(crate) changes: Vec<MetadataChangeRequest>,
    pub(crate) expected: Vec<MetadataFieldDiff>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MetadataFieldDiff {
    pub(crate) field: MetadataField,
    pub(crate) before: FieldState,
    pub(crate) after: FieldState,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize)]
pub(crate) struct MetadataSnapshot {
    pub(crate) entity_id: String,
    pub(crate) entity_type: MetadataEntityType,
    pub(crate) fields: BTreeMap<String, FieldState>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct MetadataCreditValue {
    pub(crate) artist_id: String,
    #[serde(rename = "type")]
    pub(crate) credit_type: db::CreditType,
    pub(crate) detail: Option<String>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct MetadataLabelValue {
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) catalog_number: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MetadataLabelEditValue {
    pub(crate) id: String,
    pub(crate) catalog_number: Option<String>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct MetadataRelationValue {
    pub(crate) target_artist_id: String,
    #[serde(rename = "type")]
    pub(crate) relation_type: db::ArtistRelationType,
    pub(crate) attributes: Option<String>,
}
