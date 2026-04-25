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
    Value,
};
use serde::Serialize;

use crate::{
    STATE,
    db,
    plugins::LUA_SERIALIZE_OPTIONS,
};

#[harmony_macros::interface]
#[derive(Clone, Debug, Serialize)]
struct PublicUser {
    user_id: i64,
    username: String,
    role: Option<String>,
}

fn to_public_user(db_ref: &agdb::DbAny, user: db::User) -> Option<PublicUser> {
    let db_id = user.db_id?;
    let role_name = db::roles::get_role_for_user(db_ref, db_id)
        .ok()
        .flatten()
        .map(|role| role.name);
    Some(PublicUser {
        user_id: db_id.0,
        username: user.username,
        role: role_name,
    })
}

struct UsersModule;

#[harmony_macros::module(
    plugin_scoped,
    name = "Users",
    local = "users",
    path = "lyra/users",
    interfaces(PublicUser)
)]
impl UsersModule {
    /// Lists public users.
    #[harmony(args(), returns(Vec<PublicUser>))]
    pub(crate) async fn list(lua: Lua, _plugin_id: Option<Arc<str>>, _args: ()) -> Result<Value> {
        let db = STATE.db.read().await;
        let users = db::users::get(&db).into_lua_err()?;
        let users: Vec<PublicUser> = users
            .into_iter()
            .filter_map(|user| to_public_user(&db, user))
            .collect();

        lua.to_value_with(&users, LUA_SERIALIZE_OPTIONS)
    }
}

crate::plugins::plugin_surface_exports!(
    UsersModule,
    "lyra.users",
    "Read and modify user accounts.",
    High
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_user_serialization_excludes_password() -> anyhow::Result<()> {
        let public_user = PublicUser {
            user_id: 7,
            username: "alice".to_string(),
            role: Some("user".to_string()),
        };
        let value = serde_json::to_value(public_user)?;
        let obj = value.as_object().expect("should be object");

        assert_eq!(obj.get("user_id"), Some(&serde_json::json!(7)));
        assert_eq!(obj.get("username"), Some(&serde_json::json!("alice")));
        assert_eq!(obj.get("role"), Some(&serde_json::json!("user")));
        assert!(obj.get("password").is_none());
        assert_eq!(obj.len(), 3);

        Ok(())
    }
}
