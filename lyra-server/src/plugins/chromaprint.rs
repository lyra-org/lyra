// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::fmt;
use std::time::Duration;

use harmony_core::{
    Danger,
    Module,
    ModuleBuilder,
    ModuleExport,
};
use harmony_luau::ModuleFunctionDescriptor;
use mlua::{
    ExternalResult,
    Lua,
    Result,
    Table,
};

use crate::{
    STATE,
    plugins::caller::RequestCaller,
    plugins::db,
    plugins::db::NodeId,
};

const DECODE_TIMEOUT: Duration = Duration::from_secs(30);

async fn compute(lua: Lua, caller: RequestCaller, entry_id: NodeId) -> Result<Table> {
    let db = STATE.db.read().await;
    let entry_db_id = entry_id.into();
    if !crate::routes::entity_accessible_to_principal(&db, &caller.principal, entry_db_id)
        .into_lua_err()?
    {
        return Err(mlua::Error::runtime("entry not found"));
    }
    let entry = db::entries::get_by_id(&db, entry_db_id)
        .into_lua_err()?
        .ok_or_else(|| mlua::Error::runtime("entry not found"))?;

    drop(db);

    let (fingerprint, duration) = lyra_chromaprint::compute_fingerprint_from_file(
        &entry.full_path,
        None,
        Some(DECODE_TIMEOUT),
    )
    .into_lua_err()?;

    let table = lua.create_table()?;
    table.set("fingerprint", fingerprint)?;
    table.set("duration", duration)?;
    Ok(table)
}

fn module_export() -> ModuleExport {
    ModuleBuilder::empty("lyra/chromaprint", "Chromaprint", "chromaprint")
        .scope_id(
            "lyra.chromaprint",
            "Compute audio fingerprints from track files.",
            Danger::Low,
        )
        .async_function_with_prelude(
            ModuleFunctionDescriptor::new("compute")
                .description(
                    "Computes a Chromaprint fingerprint for an entry.\nReturns a dictionary with `fingerprint` (string) and `duration` (number, seconds).",
                )
                .param_type::<NodeId>("entry_id")
                .returns::<std::collections::BTreeMap<String, String>>()
                .yields(),
            crate::plugins::caller::request_caller_from_stack,
            |lua, caller, entry_id| async move {
                let caller = caller?;
                compute(lua, caller, entry_id).await
            },
        )
        .build()
}

pub(crate) fn get_module() -> Module {
    module_export().into_module()
}

pub(crate) fn render_luau_definition() -> std::result::Result<String, fmt::Error> {
    module_export().render_luau_definition()
}
