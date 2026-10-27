// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::HashMap,
    sync::{
        Arc,
        Mutex,
    },
};

use agdb::DbId;
use harmony_luau as luau;

use serde_json::Value as JsonValue;

use super::parsing::parse_custom_field_version;
use crate::db::ArtistRelationType;
use crate::db::CreditType;
use crate::db::DbAsync;
use crate::db::MetadataField;
use crate::db::metadata::layers::LayerCredits;
use crate::db::metadata::layers::LayerFields;
use crate::db::metadata::layers::LayerRelations;
use crate::db::metadata::layers::edges::LayerCreditInput;
use crate::db::metadata::layers::edges::LayerRelationInput;
use crate::db::metadata::layers::normalize_value;
use crate::plugins::args;
use crate::services::metadata::layers::{
    CustomFieldChanges,
    ProviderValues,
};

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct CreditEntry {
    artist_id: i64,
    detail: Option<String>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RelationEntry {
    artist_id: i64,
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
    node_id: DbId,
    provider_id: String,
    fields: LayerFields,
    relations: LayerRelations,
    credits: LayerCredits,
    external_ids: HashMap<String, String>,
    custom_fields: CustomFieldChanges,
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
    fn set_field(
        &self,
        vm: &luau::Vm,
        name: String,
        value: luau::Value,
    ) -> luau::runtime::Result<()> {
        let field = MetadataField::from_name(&name).ok_or_else(|| {
            crate::plugins::runtime_error(format!("unknown metadata layer field '{name}'"))
        })?;
        let value = normalize_value(field, harmony_serde::luau_to_json(vm, &value, 0)?)
            .map_err(crate::plugins::runtime_error)?;
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
    #[harmony(args(role: CreditType, credits: luau::JsonValue))]
    fn set_credits(
        &self,
        vm: &luau::Vm,
        role: CreditType,
        credits: luau::Value,
    ) -> luau::runtime::Result<()> {
        let entries: Vec<CreditEntry> = serde_json::from_value(harmony_serde::luau_to_json(
            vm, &credits, 0,
        )?)
        .map_err(|error| {
            crate::plugins::runtime_error(format!(
                "credits must be a list of {{ artist_id, detail? }}: {error}"
            ))
        })?;
        let mut inputs = Vec::with_capacity(entries.len());
        for entry in entries {
            inputs.push(LayerCreditInput {
                artist_id: args::positive_id(entry.artist_id, "artist_id")?,
                detail: entry
                    .detail
                    .map(|detail| detail.trim().to_string())
                    .filter(|detail| !detail.is_empty()),
            });
        }
        self.state
            .lock()
            .expect("metadata layer mutex poisoned")
            .credits
            .insert(role, inputs);
        Ok(())
    }

    /// Replaces the artists related to this layer's artist by `relation_type`,
    /// such as a character's voice actors or a group's members. An empty list
    /// clears the type; types never set keep what this source stored before.
    #[harmony(args(relation_type: ArtistRelationType, relations: luau::JsonValue))]
    fn set_relations(
        &self,
        vm: &luau::Vm,
        relation_type: ArtistRelationType,
        relations: luau::Value,
    ) -> luau::runtime::Result<()> {
        let entries: Vec<RelationEntry> = serde_json::from_value(harmony_serde::luau_to_json(
            vm, &relations, 0,
        )?)
        .map_err(|error| {
            crate::plugins::runtime_error(format!(
                "relations must be a list of {{ artist_id, attributes? }}: {error}"
            ))
        })?;
        let mut inputs = Vec::with_capacity(entries.len());
        for entry in entries {
            inputs.push(LayerRelationInput {
                artist_id: args::positive_id(entry.artist_id, "artist_id")?,
                attributes: entry
                    .attributes
                    .map(|attributes| attributes.trim().to_string())
                    .filter(|attributes| !attributes.is_empty()),
            });
        }
        self.state
            .lock()
            .expect("metadata layer mutex poisoned")
            .relations
            .insert(relation_type, inputs);
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
            crate::services::metadata::layers::save_provider_layer(
                &mut db,
                layer.node_id,
                &layer.provider_id,
                &ProviderValues {
                    fields: layer.fields,
                    relations: layer.relations,
                    credits: layer.credits,
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
        vm: &luau::Vm,
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
        let value = harmony_serde::luau_to_json(vm, &value, 0)?;
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
    fn set_custom_fields(
        &self,
        vm: &luau::Vm,
        version: String,
        fields: luau::Value,
    ) -> luau::runtime::Result<()> {
        let version = parse_custom_field_version(&version)?;
        let JsonValue::Object(fields) = harmony_serde::luau_to_json(vm, &fields, 0)? else {
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

impl MetadataLayer {
    pub(super) fn new_value(
        vm: &luau::Vm,
        origin: &luau::ChunkOrigin,
        db: Option<DbAsync>,
        node_id: DbId,
        provider_id: String,
    ) -> luau::runtime::Result<luau::Value> {
        let layer = Self {
            db,
            state: Arc::new(Mutex::new(LayerBuilderState {
                node_id,
                provider_id,
                fields: LayerFields::new(),
                relations: LayerRelations::new(),
                credits: LayerCredits::new(),
                external_ids: HashMap::new(),
                custom_fields: CustomFieldChanges::default(),
            })),
        };
        Self::_harmony_userdata_class().create_value(vm, origin, layer)
    }
}
