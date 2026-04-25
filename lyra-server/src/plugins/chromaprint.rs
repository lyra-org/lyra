// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::sync::Arc;
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
    db,
    db::NodeId,
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
        _plugin_id: Option<Arc<str>>,
        entry_id: NodeId,
    ) -> Result<Table> {
        let db = STATE.db.read().await;
        let entry = db::entries::get_by_id(&db, entry_id.into())
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
