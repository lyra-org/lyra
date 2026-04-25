// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::sync::Arc;

use harmony_core::LuaAsyncExt;
use mlua::{
    ExternalResult,
    Lua,
    LuaSerdeExt,
    Result,
    Table,
    Value,
};
use serde::{
    Deserialize,
    Serialize,
};

use crate::{
    STATE,
    db::{
        self,
        NodeId,
        labels::{
            LabelExternalIdInput,
            LabelInput,
            ResolveExternalId,
            ResolveLabel,
        },
    },
};

#[derive(Debug, Deserialize)]
#[harmony_macros::interface]
struct LabelExternalId {
    provider_id: String,
    id_type: String,
    id: String,
}

#[derive(Debug, Deserialize)]
#[harmony_macros::interface]
struct LabelAddRequest {
    name: String,
    catalog_number: Option<String>,
    external_id: Option<LabelExternalId>,
}

#[derive(Debug, Deserialize)]
#[harmony_macros::interface]
struct LabelResolveRequest {
    name: String,
    external_id: Option<LabelExternalId>,
}

#[derive(Debug, Serialize)]
#[harmony_macros::interface]
struct LabelInfo {
    db_id: Option<NodeId>,
    id: String,
    name: String,
}

#[derive(Debug, Serialize)]
#[harmony_macros::interface]
struct LabelForReleaseInfo {
    label: LabelInfo,
    catalog_number: Option<String>,
}

fn label_to_info(label: db::labels::Label) -> LabelInfo {
    LabelInfo {
        db_id: label.db_id,
        id: label.id,
        name: label.name,
    }
}

struct LabelsModule;

#[harmony_macros::module(
    plugin_scoped,
    name = "Labels",
    local = "labels",
    path = "lyra/labels",
    interfaces(
        LabelExternalId,
        LabelAddRequest,
        LabelResolveRequest,
        LabelInfo,
        LabelForReleaseInfo
    )
)]
impl LabelsModule {
    /// Resolve-or-create a label and link it to a release with an optional
    /// catalog number. Resolution + linking run atomically.
    ///
    /// Locked-release no-op: returns sentinel `NodeId(DbId(0))`. Locked
    /// releases skip because resolving-without-linking would leak a Label
    /// that refcount GC cannot reach.
    #[harmony(args(release_id: NodeId, request: LabelAddRequest))]
    pub(crate) async fn add(
        lua: Lua,
        _plugin_id: Option<Arc<str>>,
        release_id: Value,
        request: Value,
    ) -> Result<NodeId> {
        let release_id: agdb::DbId = lua.from_value::<NodeId>(release_id)?.into();
        let request: LabelAddRequest = crate::plugins::from_lua_json_value(&lua, request)?;

        let mut db = STATE.db.write().await;

        let is_locked = db::releases::get_by_id(&db, release_id)
            .into_lua_err()?
            .is_some_and(|r| r.locked.unwrap_or(false));

        if is_locked {
            return Ok(NodeId::from(agdb::DbId(0)));
        }

        let ext = request.external_id.as_ref().map(|e| ResolveExternalId {
            provider_id: &e.provider_id,
            id_type: &e.id_type,
            id_value: &e.id,
        });
        let label_id = db::labels::add_label_to_release(
            &mut db,
            release_id,
            &ResolveLabel {
                name: &request.name,
                external_id: ext,
            },
            request.catalog_number.as_deref(),
        )
        .into_lua_err()?;

        Ok(label_id.into())
    }

    /// Resolve or create a label without linking. Useful when a plugin needs
    /// the db_id before an unrelated operation.
    ///
    /// Not gated on release locks — no release is referenced. A bare Label
    /// created here and never linked via `add` or `sync_for_release` becomes
    /// a permanent orphan: refcount GC only fires on the last `ReleaseLabel`
    /// unlink, and an unlinked Label has no trigger. Caller owns the
    /// follow-up.
    #[harmony(args(request: LabelResolveRequest))]
    pub(crate) async fn resolve(
        lua: Lua,
        _plugin_id: Option<Arc<str>>,
        request: Value,
    ) -> Result<NodeId> {
        let request: LabelResolveRequest = crate::plugins::from_lua_json_value(&lua, request)?;
        let ext = request.external_id.as_ref().map(|e| ResolveExternalId {
            provider_id: &e.provider_id,
            id_type: &e.id_type,
            id_value: &e.id,
        });
        let mut db = STATE.db.write().await;
        let label_id = db::labels::resolve(
            &mut db,
            &ResolveLabel {
                name: &request.name,
                external_id: ext,
            },
        )
        .into_lua_err()?;
        Ok(label_id.into())
    }

    /// Replace the labels on a release with the given set (authoritative
    /// sync). Mirrors what the merged-metadata pipeline does.
    ///
    /// Errors on locked releases: reconciling against an empty set would
    /// destroy the curated state the lock exists to protect. An explicit
    /// error surfaces the refusal; a silent no-op would look like success.
    #[harmony(args(release_id: NodeId, requests: Vec<LabelAddRequest>))]
    pub(crate) async fn sync_for_release(
        lua: Lua,
        _plugin_id: Option<Arc<str>>,
        release_id: Value,
        requests: Value,
    ) -> Result<()> {
        let release_id: agdb::DbId = lua.from_value::<NodeId>(release_id)?.into();
        let requests: Vec<LabelAddRequest> = crate::plugins::from_lua_json_value(&lua, requests)?;

        let mut db = STATE.db.write().await;

        let is_locked = db::releases::get_by_id(&db, release_id)
            .into_lua_err()?
            .is_some_and(|r| r.locked.unwrap_or(false));
        if is_locked {
            return Err(mlua::Error::runtime(format!(
                "release {} is locked; refusing to sync labels",
                release_id.0
            )));
        }

        let mut inputs: Vec<LabelInput> = Vec::with_capacity(requests.len());
        for (idx, r) in requests.into_iter().enumerate() {
            let name = r.name.trim().to_string();
            if name.is_empty() {
                return Err(mlua::Error::runtime(format!("labels[{idx}].name is blank")));
            }
            let catalog_number = r
                .catalog_number
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty());
            let external_id = match r.external_id {
                None => None,
                Some(e) => {
                    let provider_id = e.provider_id.trim().to_string();
                    let id_type = e.id_type.trim().to_string();
                    let id_value = e.id.trim().to_string();
                    if provider_id.is_empty() || id_type.is_empty() || id_value.is_empty() {
                        return Err(mlua::Error::runtime(format!(
                            "labels[{idx}].external_id has blank component (provider_id / id_type / id required)"
                        )));
                    }
                    Some(LabelExternalIdInput {
                        provider_id,
                        id_type,
                        id_value,
                    })
                }
            };
            inputs.push(LabelInput {
                name,
                catalog_number,
                external_id,
            });
        }

        db::labels::sync_release_labels(&mut db, release_id, &inputs).into_lua_err()?;
        Ok(())
    }

    #[harmony(returns(Option<LabelInfo>))]
    pub(crate) async fn get_by_id(
        lua: Lua,
        _plugin_id: Option<Arc<str>>,
        label_id: NodeId,
    ) -> Result<Value> {
        let db = STATE.db.read().await;
        let label = db::labels::get_by_id(&*db, label_id.into()).into_lua_err()?;
        match label {
            Some(l) => lua.to_value_with(&label_to_info(l), crate::plugins::LUA_SERIALIZE_OPTIONS),
            None => Ok(Value::Nil),
        }
    }

    #[harmony(returns(Vec<LabelForReleaseInfo>))]
    pub(crate) async fn get_for_release(
        lua: Lua,
        _plugin_id: Option<Arc<str>>,
        release_id: NodeId,
    ) -> Result<Value> {
        let db = STATE.db.read().await;
        let joined = db::labels::get_for_release(&*db, release_id.into()).into_lua_err()?;
        let infos: Vec<LabelForReleaseInfo> = joined
            .into_iter()
            .map(|lr| LabelForReleaseInfo {
                label: label_to_info(lr.label),
                catalog_number: lr.catalog_number,
            })
            .collect();
        lua.to_value_with(&infos, crate::plugins::LUA_SERIALIZE_OPTIONS)
    }

    #[harmony(args(release_ids: Vec<u64>), returns(std::collections::BTreeMap<u64, Vec<LabelForReleaseInfo>>))]
    pub(crate) async fn get_for_releases_many(
        lua: Lua,
        _plugin_id: Option<Arc<str>>,
        release_ids: Table,
    ) -> Result<Table> {
        let ids = crate::plugins::parse_ids(release_ids)?;
        let db = STATE.db.read().await;
        let result = db::labels::get_for_releases_many(&*db, &ids).into_lua_err()?;
        let table = lua.create_table()?;
        for id in ids {
            let joined = result.get(&id).cloned().unwrap_or_default();
            let infos: Vec<LabelForReleaseInfo> = joined
                .into_iter()
                .map(|lr| LabelForReleaseInfo {
                    label: label_to_info(lr.label),
                    catalog_number: lr.catalog_number,
                })
                .collect();
            table.set(
                id.0,
                lua.to_value_with(&infos, crate::plugins::LUA_SERIALIZE_OPTIONS)?,
            )?;
        }
        Ok(table)
    }

    #[harmony(returns(Vec<u64>))]
    pub(crate) async fn get_releases(
        _lua: Lua,
        _plugin_id: Option<Arc<str>>,
        label_id: NodeId,
    ) -> Result<Vec<NodeId>> {
        let db = STATE.db.read().await;
        let release_ids = db::labels::get_releases(&*db, label_id.into()).into_lua_err()?;
        Ok(release_ids.into_iter().map(NodeId::from).collect())
    }

    #[harmony(args(label_ids: Vec<u64>), returns(std::collections::BTreeMap<u64, Vec<u64>>))]
    pub(crate) async fn get_releases_many(
        _lua: Lua,
        _plugin_id: Option<Arc<str>>,
        label_ids: Table,
    ) -> Result<Table> {
        let ids = crate::plugins::parse_ids(label_ids)?;
        let db = STATE.db.read().await;
        let result = db::labels::get_releases_many(&*db, &ids).into_lua_err()?;
        let lua = _lua;
        let table = lua.create_table()?;
        for id in ids {
            let release_ids = result.get(&id).cloned().unwrap_or_default();
            let release_id_values: Vec<NodeId> =
                release_ids.into_iter().map(NodeId::from).collect();
            table.set(
                id.0,
                lua.to_value_with(&release_id_values, crate::plugins::LUA_SERIALIZE_OPTIONS)?,
            )?;
        }
        Ok(table)
    }
}

crate::plugins::plugin_surface_exports!(
    LabelsModule,
    "lyra.labels",
    "Read and modify record labels.",
    Low
);
