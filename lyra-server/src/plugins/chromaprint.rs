// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::time::Duration;

use harmony_core::LuaAsyncExt;
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

struct ChromaprintModule;

#[harmony_macros::module(
    plugin_scoped,
    name = "Chromaprint",
    local = "chromaprint",
    path = "lyra/chromaprint"
)]
impl ChromaprintModule {
    /// Computes a Chromaprint fingerprint for an entry.
    /// Returns a dictionary with `fingerprint` (string) and `duration` (number, seconds).
    #[harmony(returns(std::collections::BTreeMap<String, String>))]
    pub(crate) async fn compute(
        lua: Lua,
        #[harmony_context] caller: RequestCaller,
        entry_id: NodeId,
    ) -> Result<Table> {
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
}

crate::plugins::plugin_surface_exports!(
    ChromaprintModule,
    "lyra.chromaprint",
    "Compute audio fingerprints from track files.",
    Low
);
