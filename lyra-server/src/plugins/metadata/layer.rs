// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::{
        BTreeMap,
        HashMap,
    },
    sync::{
        Arc,
        Mutex,
    },
};

use harmony_core::UserDataBinding;
use harmony_luau as luau;

use crate::plugins::types::{
    LuauArtistRelationType,
    LuauCreditType,
};
use serde_json::Value as JsonValue;

use super::parsing::parse_custom_field_version;
use crate::plugins::errors::{
    ErrorKind,
    kinded,
};
use crate::plugins::ids;
use crate::services::metadata::layers::json::input_from_json;
use crate::services::metadata::layers::{
    CustomFieldChanges,
    ProviderValues,
};
use crate::services::metadata::merge_journal::MergeJournal;
use lyra_db::ArtistRelationType;
use lyra_db::CreditType;
use lyra_db::DbAsync;
use lyra_db::MetadataField;
use lyra_db::entities::MetadataEntityType;
use lyra_db::metadata::layers::FieldInputs;
use lyra_db::metadata::layers::LayerCredits;
use lyra_db::metadata::layers::LayerRelations;
use lyra_db::metadata::layers::edges::LayerCreditInput;
use lyra_db::metadata::layers::edges::LayerRelationInput;

/// The `options` argument of `provider:layer`.
#[derive(serde::Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub(super) struct LayerOptions {
    #[serde(default)]
    replace: bool,
}

impl LayerOptions {
    pub(super) fn from_luau(options: Option<luau::Value>) -> luau::runtime::Result<Self> {
        match options {
            None | Some(luau::Value::Nil) => Ok(Self::default()),
            Some(options) => serde_json::from_value(harmony_serde::luau_to_json(&options)?)
                .map_err(|error| {
                    crate::plugins::runtime_error(format!("invalid layer options: {error}"))
                }),
        }
    }
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct CreditEntry {
    artist_id: String,
    detail: Option<String>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RelationEntry {
    artist_id: String,
    attributes: Option<String>,
}

#[harmony_macros::userdata(name = "Layer", description = "Metadata layer builder.")]
#[derive(Clone)]
pub(super) struct MetadataLayer {
    db: Option<DbAsync>,
    state: Arc<Mutex<LayerBuilderState>>,
}

#[derive(Clone)]
struct LayerBuilderState {
    node_id: String,
    provider_id: String,
    fields: FieldInputs,
    /// Relations and credits by artist public id, resolved when the layer is saved.
    relations: BTreeMap<ArtistRelationType, Vec<(String, Option<String>)>>,
    credits: BTreeMap<CreditType, Vec<(String, Option<String>)>>,
    external_ids: HashMap<String, String>,
    custom_fields: CustomFieldChanges,
    replace: bool,
}

#[harmony_macros::userdata_methods]
impl MetadataLayer {
    fn set_id(&self, id_type: String, id_value: String) -> luau::runtime::Result<()> {
        if id_type.trim().is_empty() {
            return Err(crate::plugins::runtime_error(
                "metadata layer id key must not be empty",
            ));
        }
        self.state
            .lock()
            .expect("metadata layer mutex poisoned")
            .external_ids
            .insert(id_type, id_value);
        Ok(())
    }

    #[harmony(args(name: String, value: luau::JsonValue))]
    fn set_field(&self, name: String, value: luau::Value) -> luau::runtime::Result<()> {
        let field = MetadataField::from_name(&name).ok_or_else(|| {
            crate::plugins::runtime_error(format!("unknown metadata layer field '{name}'"))
        })?;
        let value = input_from_json(field, harmony_serde::luau_to_json(&value)?)?;
        self.state
            .lock()
            .expect("metadata layer mutex poisoned")
            .fields
            .insert(field, value);
        Ok(())
    }

    /// Replaces this release or track layer's credits in `role`, in credit
    /// order. An empty list clears the role; roles never set keep what this
    /// source stored before.
    #[harmony(args(role: LuauCreditType, credits: luau::JsonValue))]
    fn set_credits(&self, role: LuauCreditType, credits: luau::Value) -> luau::runtime::Result<()> {
        let entries: Vec<CreditEntry> =
            serde_json::from_value(harmony_serde::luau_to_json(&credits)?).map_err(|error| {
                crate::plugins::runtime_error(format!(
                    "credits must be a list of {{ artist_id, detail? }}: {error}"
                ))
            })?;
        let inputs = entries
            .into_iter()
            .map(|entry| {
                (
                    entry.artist_id,
                    entry
                        .detail
                        .map(|detail| detail.trim().to_string())
                        .filter(|detail| !detail.is_empty()),
                )
            })
            .collect();
        self.state
            .lock()
            .expect("metadata layer mutex poisoned")
            .credits
            .insert(role.into(), inputs);
        Ok(())
    }

    /// Replaces the artists related to this layer's artist by `relation_type`,
    /// such as a character's voice actors or a group's members. An empty list
    /// clears the type; types never set keep what this source stored before.
    #[harmony(args(relation_type: LuauArtistRelationType, relations: luau::JsonValue))]
    fn set_relations(
        &self,
        relation_type: LuauArtistRelationType,
        relations: luau::Value,
    ) -> luau::runtime::Result<()> {
        let entries: Vec<RelationEntry> =
            serde_json::from_value(harmony_serde::luau_to_json(&relations)?).map_err(|error| {
                crate::plugins::runtime_error(format!(
                    "relations must be a list of {{ artist_id, attributes? }}: {error}"
                ))
            })?;
        let inputs = entries
            .into_iter()
            .map(|entry| {
                (
                    entry.artist_id,
                    entry
                        .attributes
                        .map(|attributes| attributes.trim().to_string())
                        .filter(|attributes| !attributes.is_empty()),
                )
            })
            .collect();
        self.state
            .lock()
            .expect("metadata layer mutex poisoned")
            .relations
            .insert(relation_type.into(), inputs);
        Ok(())
    }

    fn save(&self) -> luau::runtime::Result<luau::ScheduledFuture> {
        let Some(db) = self.db.clone() else {
            return Err(crate::plugins::runtime_error(
                "metadata layer save requires a database-backed plugin executor",
            ));
        };
        let layer = self
            .state
            .lock()
            .expect("metadata layer mutex poisoned")
            .clone();
        Ok(luau::ScheduledFuture::new(async move {
            let mut db = db.write().await;
            let generation = crate::services::STATE.generation();
            let journal = &generation.merge_journal;
            let target = layer.target(&db, journal)?;
            let node_id = target.db_id;
            let artist_id = |id: &str| {
                // Not kinded, unlike a missing target: the release is live and partly written, so
                // its refresh is reported as failed rather than skipped.
                let not_an_artist =
                    || crate::plugins::runtime_error(format!("artist_id {id} is not an artist"));
                let artist = write_target(&db, journal, id)?.ok_or_else(not_an_artist)?;
                if ids::kind(&db, artist.db_id)?.as_deref() != Some("Artist") {
                    return Err(not_an_artist());
                }
                Ok(artist)
            };
            let mut relations = LayerRelations::new();
            for (relation_type, entries) in layer.relations {
                let mut inputs = Vec::with_capacity(entries.len());
                for (id, attributes) in entries {
                    let artist = artist_id(&id)?;
                    // A merge can make a stale relation point at its own owner, which a
                    // provider did not ask for; naming the owner itself is still refused.
                    if artist.db_id == node_id
                        && (artist.merged || target.merged)
                        && id != layer.node_id
                    {
                        continue;
                    }
                    inputs.push(LayerRelationInput {
                        artist_id: artist.db_id,
                        attributes,
                    });
                }
                relations.insert(relation_type, inputs);
            }
            let mut credits = LayerCredits::new();
            for (role, entries) in layer.credits {
                let inputs = entries
                    .into_iter()
                    .map(|(id, detail)| {
                        Ok(LayerCreditInput {
                            artist_id: artist_id(&id)?.db_id,
                            detail,
                        })
                    })
                    .collect::<luau::runtime::Result<Vec<_>>>()?;
                credits.insert(role, inputs);
            }
            let write = if layer.replace {
                crate::services::metadata::layers::replace_provider_layer
            } else {
                crate::services::metadata::layers::save_provider_layer
            };
            write(
                &mut db,
                node_id,
                &layer.provider_id,
                &ProviderValues {
                    fields: layer.fields,
                    relations,
                    credits,
                },
                &layer.external_ids,
                &layer.custom_fields,
            )
            .map_err(crate::plugins::runtime_error)
        }))
    }

    #[harmony(args(version: String, name: String, value: luau::JsonValue))]
    fn set_custom_field(
        &self,
        version: String,
        name: String,
        value: luau::Value,
    ) -> luau::runtime::Result<()> {
        let version = parse_custom_field_version(&version)?;
        let name = name.trim().to_string();
        if name.is_empty() {
            return Err(crate::plugins::runtime_error(
                "custom field name must be a non-empty string",
            ));
        }
        let value = harmony_serde::luau_to_json(&value)?;
        let mut layer = self.state.lock().expect("metadata layer mutex poisoned");
        layer.custom_fields.remove.remove(&version);
        layer
            .custom_fields
            .set
            .entry(version)
            .or_default()
            .insert(name, value);
        Ok(())
    }

    #[harmony(args(version: String, fields: luau::JsonValue))]
    fn set_custom_fields(&self, version: String, fields: luau::Value) -> luau::runtime::Result<()> {
        let version = parse_custom_field_version(&version)?;
        let JsonValue::Object(fields) = harmony_serde::luau_to_json(&fields)? else {
            return Err(crate::plugins::runtime_error(
                "custom fields payload must be a JSON object",
            ));
        };
        let mut layer = self.state.lock().expect("metadata layer mutex poisoned");
        layer.custom_fields.remove.remove(&version);
        layer
            .custom_fields
            .set
            .insert(version, fields.into_iter().collect());
        Ok(())
    }

    fn clear_custom_fields(&self, version: String) -> luau::runtime::Result<()> {
        let version = parse_custom_field_version(&version)?;
        let mut layer = self.state.lock().expect("metadata layer mutex poisoned");
        layer.custom_fields.set.remove(&version);
        layer.custom_fields.remove.insert(version);
        Ok(())
    }
}

/// A node a provider write names: the one its id names, or, for an id a merge took, the winner.
pub(super) struct WriteTarget {
    pub(super) db_id: agdb::DbId,
    pub(super) merged: bool,
}

/// The node a provider write lands on; `None` when `id` names nothing, or a merge took it and the
/// winner is gone too.
pub(super) fn write_target(
    db: &agdb::DbAny,
    journal: &MergeJournal,
    id: &str,
) -> luau::runtime::Result<Option<WriteTarget>> {
    if let Some(db_id) = ids::db_id(db, id)? {
        return Ok(Some(WriteTarget {
            db_id,
            merged: false,
        }));
    }
    let Some(winner) = journal.resolve(id) else {
        return Ok(None);
    };
    Ok(ids::db_id(db, &winner)?.map(|db_id| WriteTarget {
        db_id,
        merged: true,
    }))
}

impl LayerBuilderState {
    /// The release, track or artist the layer describes, checked against what the layer sets.
    fn target(
        &self,
        db: &agdb::DbAny,
        journal: &MergeJournal,
    ) -> luau::runtime::Result<WriteTarget> {
        let not_an_entity =
            || format!("node_id {} is not a release, track or artist", self.node_id);
        // The kind is what makes a refresh count as skipped rather than failed.
        let target = write_target(db, journal, &self.node_id)?
            .ok_or_else(|| kinded(ErrorKind::NotFound, not_an_entity()))?;
        let kind = lyra_db::entities::metadata_entity_type(db, target.db_id)
            .map_err(crate::plugins::runtime_error)?
            .ok_or_else(|| crate::plugins::runtime_error(not_an_entity()))?;
        let is_artist = kind == MetadataEntityType::Artist;
        if is_artist && !self.credits.is_empty() {
            return Err(crate::plugins::runtime_error(format!(
                "node_id {} is an artist, which has no credits",
                self.node_id
            )));
        }
        if !is_artist && !self.relations.is_empty() {
            return Err(crate::plugins::runtime_error(format!(
                "node_id {} is not an artist, so it has no relations",
                self.node_id
            )));
        }
        Ok(target)
    }
}

impl MetadataLayer {
    pub(super) fn new_value(
        vm: &luau::Vm,
        origin: &luau::ChunkOrigin,
        db: Option<DbAsync>,
        node_id: String,
        provider_id: String,
        options: LayerOptions,
    ) -> luau::runtime::Result<luau::Value> {
        let layer = Self {
            db,
            state: Arc::new(Mutex::new(LayerBuilderState {
                node_id,
                provider_id,
                fields: FieldInputs::new(),
                relations: BTreeMap::new(),
                credits: BTreeMap::new(),
                external_ids: HashMap::new(),
                custom_fields: CustomFieldChanges::default(),
                replace: options.replace,
            })),
        };
        Self::class().create_value(vm, origin, layer)
    }
}
