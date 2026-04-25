// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::sync::Arc;

use harmony_core::{
    LuaAsyncExt,
    Module,
};
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
    plugins::LUA_SERIALIZE_OPTIONS,
    services::auth::{
        AuthCredential as ServiceAuthCredential,
        Principal as ServicePrincipal,
        ResolvedAuth as ServiceResolvedAuth,
        login_with_password,
        logout_with_token,
        resolve_auth_from_bearer,
    },
};

#[harmony_macros::interface]
#[derive(Serialize)]
pub(crate) struct Principal {
    pub(crate) user_id: i64,
    pub(crate) username: String,
    pub(crate) role: Option<String>,
    pub(crate) permissions: Vec<String>,
}

#[harmony_macros::interface]
#[derive(Serialize)]
pub(crate) struct AuthCredential {
    pub(crate) session_id: Option<i64>,
    pub(crate) api_key_id: Option<i64>,
    pub(crate) api_key_name: Option<String>,
}

#[harmony_macros::interface]
#[derive(Serialize)]
pub(crate) struct ResolvedAuth {
    pub(crate) principal: Principal,
    pub(crate) credential: AuthCredential,
}

#[harmony_macros::interface]
#[derive(Serialize)]
struct LoginResult {
    principal: Principal,
    token: String,
}

#[harmony_macros::interface]
#[derive(Serialize)]
struct AuthCapabilities {
    enabled: bool,
    allow_default_login_when_disabled: bool,
    default_username: String,
}

pub(crate) fn to_plugin_principal(principal: ServicePrincipal) -> Principal {
    Principal {
        user_id: principal.user_db_id.0,
        username: principal.username,
        role: principal.role_name,
        permissions: principal
            .permissions
            .iter()
            .filter_map(|p| {
                serde_json::to_value(p)
                    .ok()
                    .and_then(|v| v.as_str().map(String::from))
            })
            .collect(),
    }
}

pub(crate) fn to_plugin_credential(credential: ServiceAuthCredential) -> AuthCredential {
    match credential {
        ServiceAuthCredential::Session { session_id } => AuthCredential {
            session_id: Some(session_id.0),
            api_key_id: None,
            api_key_name: None,
        },
        ServiceAuthCredential::ApiKey { api_key_id, name } => AuthCredential {
            session_id: None,
            api_key_id: Some(api_key_id.0),
            api_key_name: Some(name),
        },
        ServiceAuthCredential::Default => AuthCredential {
            session_id: None,
            api_key_id: None,
            api_key_name: None,
        },
    }
}

pub(crate) fn to_plugin_auth(auth: ServiceResolvedAuth) -> ResolvedAuth {
    ResolvedAuth {
        principal: to_plugin_principal(auth.principal),
        credential: to_plugin_credential(auth.credential),
    }
}

fn normalize_token(token: Option<String>) -> Option<String> {
    token.and_then(|value| {
        let trimmed = value.trim().to_string();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed)
        }
    })
}

pub(crate) fn plugin_auth_to_value(lua: &Lua, auth: ResolvedAuth) -> mlua::Result<Value> {
    lua.to_value_with(&auth, LUA_SERIALIZE_OPTIONS)
}

fn auth_to_value(auth: ServiceResolvedAuth) -> mlua::Result<Value> {
    let lua = STATE.lua.get();
    plugin_auth_to_value(&lua, to_plugin_auth(auth))
}

struct AuthModule;

#[harmony_macros::module(
    plugin_scoped,
    name = "Auth",
    local = "auth",
    path = "lyra/auth",
    interfaces(AuthCapabilities, Principal, AuthCredential, ResolvedAuth, LoginResult)
)]
impl AuthModule {
    /// Resolves a bearer credential to the authenticated principal and credential metadata.
    #[harmony(returns(Option<ResolvedAuth>))]
    pub(crate) async fn resolve_auth(
        _plugin_id: Option<Arc<str>>,
        bearer: Option<String>,
    ) -> Result<Value> {
        let bearer = normalize_token(bearer);

        let auth = resolve_auth_from_bearer(bearer.as_deref())
            .await
            .into_lua_err()?;

        match auth {
            Some(auth) => auth_to_value(auth),
            None => Ok(Value::Nil),
        }
    }

    /// Revokes the session identified by the provided token.
    pub(crate) async fn logout_session(
        _plugin_id: Option<Arc<str>>,
        token: Option<String>,
    ) -> Result<bool> {
        let token = normalize_token(token);
        logout_with_token(token.as_deref()).await.into_lua_err()
    }

    /// Attempts to log in and returns a principal plus session token.
    #[harmony(returns(Option<LoginResult>))]
    pub(crate) async fn login(
        _plugin_id: Option<Arc<str>>,
        username: String,
        password: Option<String>,
    ) -> Result<Value> {
        let username = username.trim().to_string();
        if username.is_empty() {
            return Ok(Value::Nil);
        }

        let password = password.unwrap_or_default();
        let login_result = login_with_password(&username, &password)
            .await
            .into_lua_err()?;

        match login_result {
            Some(login_result) => {
                let lua = STATE.lua.get();
                let login_result = LoginResult {
                    principal: to_plugin_principal(login_result.principal),
                    token: login_result.token,
                };
                lua.to_value_with(&login_result, LUA_SERIALIZE_OPTIONS)
            }
            None => Ok(Value::Nil),
        }
    }

    /// Returns the current authentication capabilities.
    #[harmony(returns(AuthCapabilities))]
    pub(crate) fn capabilities() -> Result<Value> {
        let lua = STATE.lua.get();
        let config = STATE.config.get();
        let capabilities = AuthCapabilities {
            enabled: config.auth.enabled,
            allow_default_login_when_disabled: config.auth.allow_default_login_when_disabled,
            default_username: config.auth.default_username.clone(),
        };
        lua.to_value_with(&capabilities, LUA_SERIALIZE_OPTIONS)
    }
}

pub(crate) fn get_module() -> Module {
    Module {
        path: "lyra/auth".into(),
        setup: std::sync::Arc::new(|lua: &Lua| Ok(AuthModule::_harmony_module_table(lua)?)),
        scope: harmony_core::Scope {
            id: "lyra.auth".into(),
            description: "Manage authentication sessions and tokens.",
            danger: harmony_core::Danger::High,
        },
    }
}

pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    AuthModule::render_luau_definition()
}

#[cfg(test)]
mod tests {
    use super::normalize_token;

    #[test]
    fn normalize_token_trims_empty_values_to_none() {
        assert_eq!(normalize_token(None), None);
        assert_eq!(normalize_token(Some("  ".to_string())), None);
        assert_eq!(
            normalize_token(Some("  abc123  ".to_string())).as_deref(),
            Some("abc123")
        );
    }
}
