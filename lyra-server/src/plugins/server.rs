// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::sync::{
    Arc,
    LazyLock,
};

use harmony_core::LuaAsyncExt;
use mlua::{
    ExternalResult,
    Lua,
    LuaSerdeExt,
    Result,
    Value,
};
use serde::Serialize;

use crate::{
    STATE,
    db,
    plugins::LUA_SERIALIZE_OPTIONS,
};

static HOSTNAME: LazyLock<String> =
    LazyLock::new(|| gethostname::gethostname().to_string_lossy().into_owned());

#[harmony_macros::interface]
#[derive(Clone, Debug, Serialize)]
struct ServerInfo {
    id: String,
    version: String,
    commit_hash: String,
    hostname: String,
    port: u16,
    published_url: Option<String>,
    setup_complete: bool,
}

struct ServerModule;

#[harmony_macros::module(
    plugin_scoped,
    name = "Server",
    local = "server",
    path = "lyra/server",
    interfaces(ServerInfo)
)]
impl ServerModule {
    #[harmony(args(), returns(ServerInfo))]
    pub(crate) async fn info(lua: Lua, _plugin_id: Option<Arc<str>>, _args: ()) -> Result<Value> {
        let db = STATE.db.read().await;
        let info = db::server::get(&db)
            .into_lua_err()?
            .ok_or_else(|| mlua::Error::runtime("server info not initialized"))?;

        let default_username = &STATE.config.get().auth.default_username;
        let setup_complete =
            db::roles::has_non_default_admin(&db, default_username).into_lua_err()?;

        let config = STATE.config.get();

        let server_info = ServerInfo {
            id: info.id,
            version: env!("CARGO_PKG_VERSION").to_string(),
            commit_hash: env!("LYRA_GIT_HASH").to_string(),
            hostname: HOSTNAME.clone(),
            port: config.port,
            published_url: config.published_url.clone(),
            setup_complete,
        };

        lua.to_value_with(&server_info, LUA_SERIALIZE_OPTIONS)
    }
}

crate::plugins::plugin_surface_exports!(
    ServerModule,
    "lyra.server",
    "Inspect and control the running server.",
    High
);
