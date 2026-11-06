// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use lyra_db::{
    IdSource,
    external_ids::ExternalId,
};
use serde::{
    Deserialize,
    Deserializer,
    Serialize,
};
use serde_json::Value;

use crate::services::metadata::editing::model::{
    MetadataChangeRequest,
    MetadataFieldDiff,
    MetadataSnapshot,
};

/// One curation change. The `expected` of an operation is what preview reported for it: applying
/// requires it, and a preview ignores it.
#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum MetadataChange {
    /// Sets fields of a release, track or artist, or returns them to their resolved values.
    EditMetadata {
        entity_id: String,
        edits: Vec<MetadataChangeRequest>,
        /// The `diff` preview reported.
        expected: Option<Vec<MetadataFieldDiff>>,
    },
    /// Sets the manual value of an external ID, over the resolved value, which it keeps.
    SetExternalId {
        entity_id: String,
        provider_id: String,
        id_type: String,
        id_value: String,
        /// The `current` record preview reported, `null` for none.
        #[serde(default, deserialize_with = "present")]
        expected: Option<Option<ExternalIdState>>,
    },
    /// Removes the manual value of an external ID, so the resolved value takes effect again.
    InheritExternalId {
        entity_id: String,
        provider_id: String,
        id_type: String,
        /// The `current` record preview reported.
        expected: Option<ExternalIdState>,
    },
    /// Removes a value kept from a merged duplicate, so that it stops finding the entity.
    RemoveExternalIdAlias {
        entity_id: String,
        provider_id: String,
        id_type: String,
        id_value: String,
    },
    /// Merges `loser_ids` into `winner_id`, which can't be undone: what is attached to the losers
    /// moves to the winner, and they are deleted. Their public IDs stop resolving, while their
    /// names and differing external IDs keep finding the winner.
    Merge {
        winner_id: String,
        /// The entities to merge in, of the winner's kind, at most 50 across the set.
        loser_ids: Vec<String>,
        /// The `manual_fills` preview reported.
        expected: Option<Vec<ManualFill>>,
    },
    /// Renames a genre, trimming the name. A change beyond letter case keeps the old name as an
    /// alias, so tags and providers using it still land on this genre, and drops an alias of this
    /// genre holding the new name.
    RenameGenre {
        genre_id: String,
        name: String,
        /// The `current_name` preview reported.
        expected: Option<String>,
    },
}

/// Tells a `null` from an absent key, which `#[serde(default)]` leaves as `None`.
fn present<'de, D, T>(deserializer: D) -> Result<Option<Option<T>>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::deserialize(deserializer).map(Some)
}

impl MetadataChange {
    pub(super) fn is_merge(&self) -> bool {
        matches!(self, Self::Merge { .. })
    }

    /// Whether the change stays off libraries a sync, refresh or purge is running on.
    pub(super) fn needs_run_check(&self) -> bool {
        matches!(self, Self::Merge { .. } | Self::RenameGenre { .. })
    }

    /// The public IDs of the entities the change works on.
    pub(super) fn entity_ids(&self) -> Vec<&str> {
        match self {
            Self::EditMetadata { entity_id, .. }
            | Self::SetExternalId { entity_id, .. }
            | Self::InheritExternalId { entity_id, .. }
            | Self::RemoveExternalIdAlias { entity_id, .. } => vec![entity_id],
            Self::Merge {
                winner_id,
                loser_ids,
                ..
            } => std::iter::once(winner_id)
                .chain(loser_ids)
                .map(String::as_str)
                .collect(),
            Self::RenameGenre { genre_id, .. } => vec![genre_id],
        }
    }
}

/// An external ID's values, as a change expects them and as they are stored.
#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ExternalIdState {
    pub(crate) id_value: String,
    pub(crate) source: IdSource,
    /// The provider's value beneath a manual one, which returns when the ID is inherited again.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) resolved_value: Option<String>,
}

impl From<ExternalId> for ExternalIdState {
    fn from(id: ExternalId) -> Self {
        Self {
            id_value: id.id_value,
            source: id.source,
            resolved_value: id.resolved_value,
        }
    }
}

/// A manual value a merge's winner would take from a loser, for a key it leaves out. It takes
/// effect on a locked winner too.
#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManualFill {
    /// The field, credit role or relation type.
    pub(crate) key: String,
    pub(crate) value: Value,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MergeKind {
    Artist,
    Release,
    Genre,
    Label,
}

impl MergeKind {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Artist => "artist",
            Self::Release => "release",
            Self::Genre => "genre",
            Self::Label => "label",
        }
    }
}

/// What the losers hold that moves to the winner. A kind counts only what it has.
#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub(crate) struct MergeCounts {
    pub(crate) losers: usize,
    /// Releases credited to or linked with the losers.
    pub(crate) releases: usize,
    /// Tracks credited to the losers, or on the losing releases.
    pub(crate) tracks: usize,
    /// Parent and child genres of the losers.
    pub(crate) relatives: usize,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct MergeOutcome {
    pub(crate) winner_id: String,
    pub(crate) kind: MergeKind,
    pub(crate) counts: MergeCounts,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize)]
pub(crate) struct KeptExternalId {
    pub(crate) provider_id: String,
    pub(crate) id_type: String,
    pub(crate) id_value: String,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct TrackCollision {
    pub(crate) disc: u32,
    pub(crate) track: u32,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize)]
pub(crate) struct MergePreview {
    #[serde(flatten)]
    pub(crate) outcome: MergeOutcome,
    /// The differing external IDs the winner would keep as aliases.
    pub(crate) kept_external_ids: Vec<KeptExternalId>,
    /// Disc and track numbers more than one of the releases hold. They don't block the merge, as
    /// split discs share them.
    pub(crate) track_collisions: Vec<TrackCollision>,
    /// The locked entities among the winner and losers. A merge overrides a loser's lock.
    pub(crate) locked_ids: Vec<String>,
    /// The manual values the winner would take from a loser, ordered by key.
    pub(crate) manual_fills: Vec<ManualFill>,
}

/// What an operation would do, against the data as it stands.
#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub(crate) enum ChangePreview {
    EditMetadata {
        entity_id: String,
        /// One entry per field whose value or source would change.
        diff: Vec<MetadataFieldDiff>,
    },
    SetExternalId {
        entity_id: String,
        provider_id: String,
        id_type: String,
        id_value: String,
        /// The stored record, `null` for none.
        current: Option<ExternalIdState>,
    },
    InheritExternalId {
        entity_id: String,
        provider_id: String,
        id_type: String,
        current: ExternalIdState,
    },
    RemoveExternalIdAlias {
        entity_id: String,
        provider_id: String,
        id_type: String,
        id_value: String,
    },
    Merge(MergePreview),
    RenameGenre {
        genre_id: String,
        name: String,
        current_name: String,
    },
}

/// What an applied operation did, as things stood right after it.
#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub(crate) enum ChangeResult {
    EditMetadata(MetadataSnapshot),
    SetExternalId {
        entity_id: String,
        provider_id: String,
        id_type: String,
        record: ExternalIdState,
    },
    InheritExternalId {
        entity_id: String,
        provider_id: String,
        id_type: String,
    },
    RemoveExternalIdAlias {
        entity_id: String,
        provider_id: String,
        id_type: String,
        id_value: String,
    },
    Merge(MergeOutcome),
    RenameGenre {
        genre_id: String,
        name: String,
    },
}
