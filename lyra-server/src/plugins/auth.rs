// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use harmony_core::{
    FunctionSpec,
    ModuleExport,
    ModuleSpec,
};
use harmony_luau as luau;
#[cfg(feature = "docgen")]
use harmony_luau::DescribeTypeAlias;
use harmony_luau::JsonValue;
#[cfg(feature = "docgen")]
use harmony_luau::render_definition_file_with_support;
use harmony_luau::{
    DescribeInterface,
    FieldDescriptor,
    InterfaceDescriptor,
    LuauType,
    LuauTypeInfo,
};
#[cfg(feature = "docgen")]
use harmony_luau::{
    ModuleDescriptor,
    ModuleFunctionDescriptor,
    ParameterDescriptor,
};
use serde::Serialize;
use std::sync::Arc;

use crate::services::auth::{
    AuthCredential as ServiceAuthCredential,
    AuthError,
    Principal as ServicePrincipal,
    ResolvedAuth as ServiceResolvedAuth,
    login_with_password,
    logout_with_token,
    resolve_auth_from_bearer,
    sessions::SessionMetadata,
};

#[derive(Serialize)]
pub(crate) struct Principal {
    /// The user's public id.
    pub(crate) user_id: String,
    pub(crate) username: String,
    pub(crate) role: Option<String>,
    pub(crate) permissions: Vec<String>,
}

#[derive(Serialize)]
pub(crate) struct AuthCredential {
    pub(crate) kind: &'static str,
    /// The session's or api key's public id.
    pub(crate) id: Option<String>,
    pub(crate) api_key_name: Option<String>,
}

#[derive(Serialize)]
pub(crate) struct ResolvedAuth {
    pub(crate) principal: Principal,
    pub(crate) credential: AuthCredential,
}

#[derive(Serialize)]
struct LoginResult {
    principal: Principal,
    token: String,
}

#[derive(Serialize)]
struct LoginOutcome {
    status: &'static str,
    result: Option<LoginResult>,
    retry_after_seconds: Option<u64>,
}

impl LoginOutcome {
    fn ok(result: LoginResult) -> Self {
        Self {
            status: "ok",
            result: Some(result),
            retry_after_seconds: None,
        }
    }

    fn invalid_credentials() -> Self {
        Self {
            status: "invalid_credentials",
            result: None,
            retry_after_seconds: None,
        }
    }

    fn rate_limited(retry_after: std::time::Duration) -> Self {
        Self {
            status: "rate_limited",
            result: None,
            retry_after_seconds: Some(retry_after.as_secs().max(1)),
        }
    }

    fn invalid_client_name() -> Self {
        Self {
            status: "invalid_client_name",
            result: None,
            retry_after_seconds: None,
        }
    }
}

#[derive(Clone, Serialize)]
pub(crate) struct AuthCapabilities {
    pub(crate) enabled: bool,
    pub(crate) allow_default_login_when_disabled: bool,
}

impl AuthCapabilities {
    pub(crate) fn from_config(config: &crate::config::AuthConfig) -> Self {
        Self {
            enabled: config.enabled,
            allow_default_login_when_disabled: config.allow_default_login_when_disabled,
        }
    }
}

pub(crate) fn to_plugin_principal(principal: ServicePrincipal) -> Principal {
    Principal {
        user_id: principal.user_public_id,
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
        ServiceAuthCredential::Session { public_id } => AuthCredential {
            kind: "session",
            id: Some(public_id),
            api_key_name: None,
        },
        ServiceAuthCredential::ApiKey { public_id, name } => AuthCredential {
            kind: "api_key",
            id: Some(public_id),
            api_key_name: Some(name),
        },
        ServiceAuthCredential::Default => AuthCredential {
            kind: "default",
            id: None,
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

struct AuthModule;

pub(crate) fn module_spec() -> ModuleSpec {
    ModuleSpec::new("lyra/auth")
        .capability("lyra.auth")
        .function(auth_capabilities_spec())
        .function(resolve_auth_spec())
        .function(login_spec())
        .function(logout_session_spec())
        .function(get_session_data_spec())
        .function(set_session_data_spec())
        .install(|_| Ok(ModuleExport::new(AuthModule)))
}

/// Per-dispatch client identity resolved at the HTTP boundary, so host calls
/// made on behalf of a plugin-served request (e.g. login) can be attributed
/// to the remote client rather than the server process.
#[derive(Clone)]
pub(crate) struct DispatchClient(pub(crate) Option<String>);

/// Per-dispatch slot holding the principal currently acting for the dispatch.
/// Seeded from boundary bearer auth at dispatch start, overwritten whenever
/// the host resolves a credential via `auth.resolve_auth`, read by
/// principal-requiring host functions, and bound to the API response so
/// request handling outside the VM can authorize against it.
#[derive(Clone, Default)]
pub(crate) struct DispatchAuth(Arc<std::sync::Mutex<Option<ServicePrincipal>>>);

impl DispatchAuth {
    pub(crate) fn record(&self, principal: ServicePrincipal) {
        *self.0.lock().expect("dispatch auth slot poisoned") = Some(principal);
    }

    /// Replaces the principal only while the slot still acts for the same user,
    /// leaving a principal the plugin resolved for someone else in place.
    pub(crate) fn refresh(&self, principal: ServicePrincipal) {
        let mut slot = self.0.lock().expect("dispatch auth slot poisoned");
        if slot
            .as_ref()
            .is_some_and(|current| current.user_public_id == principal.user_public_id)
        {
            *slot = Some(principal);
        }
    }

    /// Empties the slot, refusing principal-requiring host calls until a
    /// credential is resolved again.
    pub(crate) fn clear(&self) {
        *self.0.lock().expect("dispatch auth slot poisoned") = None;
    }

    pub(crate) fn principal(&self) -> Option<ServicePrincipal> {
        self.0.lock().expect("dispatch auth slot poisoned").clone()
    }
}

/// The principal acting for this dispatch, or `None` for server-side dispatches,
/// which carry no [`DispatchAuth`]. A client dispatch that has not resolved a
/// credential is refused rather than treated as server-side.
pub(crate) fn dispatch_principal(
    context: &luau::CallContext,
) -> luau::runtime::Result<Option<ServicePrincipal>> {
    let Ok(auth) = context.caller.get::<DispatchAuth>() else {
        return Ok(None);
    };
    auth.principal().map(Some).ok_or_else(no_dispatch_principal)
}

pub(crate) fn require_dispatch_principal(
    context: &luau::CallContext,
) -> luau::runtime::Result<ServicePrincipal> {
    dispatch_principal(context)?.ok_or_else(no_dispatch_principal)
}

fn no_dispatch_principal() -> luau::runtime::Error {
    crate::plugins::runtime_error(
        "no authenticated caller bound to this dispatch; resolve a credential via lyra/auth \
         first",
    )
}

/// [`ServicePrincipal::require`] as a Luau runtime error, for plugin callbacks.
pub(crate) fn require_user_db_id(
    principal: &ServicePrincipal,
    db: &impl crate::plugins::db::DbAccess,
) -> luau::runtime::Result<agdb::DbId> {
    principal.require(db).map_err(crate::plugins::runtime_error)
}

fn resolve_auth_spec() -> FunctionSpec {
    FunctionSpec::async_fn("resolve_auth")
        .arg_name("bearer")
        .args::<Option<String>>()
        .returns::<Option<ResolvedAuth>>()
        .call_async(Arc::new(resolve_auth_callback))
}

fn resolve_auth_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let bearer: Option<String> = frame.args.read_named("bearer")?;
    let dispatch_auth = frame.context.caller.get::<DispatchAuth>().ok();
    Ok(luau::ScheduledFuture::new(async move {
        let resolved = match resolve_auth_from_bearer(bearer.as_deref()).await {
            Ok(Some(resolved)) => resolved,
            Ok(None) | Err(AuthError::SessionExpired) => return Ok(luau::Value::Nil),
            Err(err) => return Err(crate::plugins::runtime_error(err)),
        };
        match resolved.principal.require(&*crate::STATE.db.read().await) {
            Ok(_) => {}
            Err(AuthError::InvalidBearerCredential) => return Ok(luau::Value::Nil),
            Err(err) => return Err(crate::plugins::runtime_error(err)),
        }
        if let Some(dispatch_auth) = &dispatch_auth {
            dispatch_auth.record(resolved.principal.clone());
        }
        harmony_luau::serializable_to_luau_owned(to_plugin_auth(resolved))
    }))
}

fn login_spec() -> FunctionSpec {
    FunctionSpec::async_fn("login")
        .arg_name("username")
        .args::<String>()
        .arg_name("password")
        .args::<Option<String>>()
        .arg_name("user_agent")
        .args::<Option<String>>()
        .arg_name("client_name")
        .args::<Option<String>>()
        .returns::<LoginOutcome>()
        .call_async(Arc::new(login_callback))
}

fn login_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let username: String = frame.args.read_named("username")?;
    let password: Option<String> = frame.args.read_named("password")?;
    let user_agent: Option<String> = frame.args.read_named("user_agent")?;
    let client_name: Option<String> = frame.args.read_named("client_name")?;
    let client = frame
        .context
        .caller
        .get::<DispatchClient>()
        .ok()
        .and_then(|client| client.0.clone());
    Ok(luau::ScheduledFuture::new(async move {
        let metadata = SessionMetadata {
            user_agent,
            client_name,
        };
        let login = match login_with_password(
            &username,
            password.as_deref().unwrap_or(""),
            metadata,
            client.as_deref(),
        )
        .await
        {
            Ok(login) => login,
            Err(AuthError::RateLimited(retry_after)) => {
                return harmony_luau::serializable_to_luau_owned(LoginOutcome::rate_limited(
                    retry_after,
                ));
            }
            // A rejected `client_name` is caller input, not a runtime fault:
            // report it like the other input failures rather than raising.
            Err(AuthError::InvalidClientName(_)) => {
                return harmony_luau::serializable_to_luau_owned(
                    LoginOutcome::invalid_client_name(),
                );
            }
            Err(err) => return Err(crate::plugins::runtime_error(err)),
        };
        let Some(login) = login else {
            return harmony_luau::serializable_to_luau_owned(LoginOutcome::invalid_credentials());
        };
        let resolved = resolve_auth_from_bearer(Some(&login.token))
            .await
            .map_err(crate::plugins::runtime_error)?
            .ok_or_else(|| {
                crate::plugins::runtime_error("freshly issued session token failed to resolve")
            })?;
        harmony_luau::serializable_to_luau_owned(LoginOutcome::ok(LoginResult {
            principal: to_plugin_principal(resolved.principal),
            token: login.token,
        }))
    }))
}

fn logout_session_spec() -> FunctionSpec {
    FunctionSpec::async_fn("logout_session")
        .arg_name("token")
        .args::<Option<String>>()
        .returns::<bool>()
        .call_async(Arc::new(logout_session_callback))
}

fn logout_session_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let token: Option<String> = frame.args.read_named("token")?;
    Ok(luau::ScheduledFuture::new(async move {
        let revoked = logout_with_token(token.as_deref())
            .await
            .map_err(crate::plugins::runtime_error)?;
        Ok(luau::Value::Boolean(revoked))
    }))
}

fn get_session_data_spec() -> FunctionSpec {
    FunctionSpec::async_fn("get_session_data")
        .named_arg::<String>("token")
        .returns::<JsonValue>()
        .call_async(Arc::new(|mut frame| {
            let token: String = frame.args.read_named("token")?;
            let plugin_id = frame.context.origin.plugin.clone().ok_or_else(|| {
                crate::plugins::runtime_error("session data requires plugin origin")
            })?;
            Ok(luau::ScheduledFuture::new(async move {
                let value = crate::services::auth::sessions::get_plugin_data(&token, &plugin_id)
                    .await
                    .map_err(crate::plugins::runtime_error)?;
                harmony_luau::serializable_to_luau_owned(value)
            }))
        }))
}

fn set_session_data_spec() -> FunctionSpec {
    FunctionSpec::async_fn("set_session_data")
        .named_arg::<String>("token")
        .named_arg::<JsonValue>("data")
        .returns::<()>()
        .call_async(Arc::new(|mut frame| {
            let token: String = frame.args.read_named("token")?;
            let value: luau::Value = frame.args.read_named("data")?;
            let value = harmony_serde::luau_to_json(frame.vm, &value, 0)?;
            let plugin_id = frame.context.origin.plugin.clone().ok_or_else(|| {
                crate::plugins::runtime_error("session data requires plugin origin")
            })?;
            Ok(luau::ScheduledFuture::new(async move {
                crate::services::auth::sessions::set_plugin_data(&token, &plugin_id, value)
                    .await
                    .map_err(crate::plugins::runtime_error)?;
                Ok(luau::Value::Nil)
            }))
        }))
}

fn auth_capabilities_spec() -> FunctionSpec {
    let spec = FunctionSpec::sync_fn("capabilities").returns::<AuthCapabilities>();
    spec.call(auth_capabilities_callback)
}
#[derive(Clone)]
pub(crate) struct AuthCapabilitiesModuleStore {
    capabilities: AuthCapabilities,
    settings: Option<crate::SettingsHandle>,
}
impl AuthCapabilitiesModuleStore {
    pub(crate) fn new(
        capabilities: AuthCapabilities,
        settings: Option<crate::SettingsHandle>,
    ) -> Self {
        Self {
            capabilities,
            settings,
        }
    }
}
fn auth_capabilities_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let store = frame.vm.data().get::<AuthCapabilitiesModuleStore>()?;
    let capabilities = match &store.settings {
        Some(settings) => AuthCapabilities::from_config(&settings.get().config.auth),
        None => store.capabilities.clone(),
    };
    frame
        .returns
        .write(auth_capabilities_table(&capabilities))?;
    Ok(())
}
fn auth_capabilities_table(capabilities: &AuthCapabilities) -> luau::OwnedTable {
    let mut table = luau::OwnedTable::with_capacity(0, 2);
    table.set_field("enabled", luau::Value::Boolean(capabilities.enabled));
    table.set_field(
        "allow_default_login_when_disabled",
        luau::Value::Boolean(capabilities.allow_default_login_when_disabled),
    );
    table
}

impl LuauTypeInfo for Principal {
    fn luau_type() -> LuauType {
        LuauType::literal("Principal")
    }
}

impl DescribeInterface for Principal {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("Principal", None);
        descriptor.fields.extend([
            FieldDescriptor {
                name: "user_id",
                ty: String::luau_type(),
                description: Some("The user's public id."),
            },
            FieldDescriptor {
                name: "username",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "role",
                ty: Option::<String>::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "permissions",
                ty: Vec::<String>::luau_type(),
                description: None,
            },
        ]);
        descriptor
    }
}

impl LuauTypeInfo for AuthCredential {
    fn luau_type() -> LuauType {
        LuauType::literal("AuthCredential")
    }
}

impl DescribeInterface for AuthCredential {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("AuthCredential", None);
        descriptor.fields.extend([
            FieldDescriptor {
                name: "kind",
                ty: LuauType::union(vec![
                    LuauType::literal("\"session\""),
                    LuauType::literal("\"api_key\""),
                    LuauType::literal("\"default\""),
                ]),
                description: None,
            },
            FieldDescriptor {
                name: "id",
                ty: Option::<String>::luau_type(),
                description: Some("The session's or api key's public id."),
            },
            FieldDescriptor {
                name: "api_key_name",
                ty: Option::<String>::luau_type(),
                description: None,
            },
        ]);
        descriptor
    }
}

impl LuauTypeInfo for ResolvedAuth {
    fn luau_type() -> LuauType {
        LuauType::literal("ResolvedAuth")
    }
}

impl DescribeInterface for ResolvedAuth {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("ResolvedAuth", None);
        descriptor.fields.extend([
            FieldDescriptor {
                name: "principal",
                ty: Principal::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "credential",
                ty: AuthCredential::luau_type(),
                description: None,
            },
        ]);
        descriptor
    }
}

impl LuauTypeInfo for LoginResult {
    fn luau_type() -> LuauType {
        LuauType::literal("LoginResult")
    }
}

#[cfg(feature = "docgen")]
impl DescribeInterface for LoginResult {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("LoginResult", None);
        descriptor.fields.extend([
            FieldDescriptor {
                name: "principal",
                ty: Principal::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "token",
                ty: String::luau_type(),
                description: None,
            },
        ]);
        descriptor
    }
}

impl LuauTypeInfo for LoginOutcome {
    fn luau_type() -> LuauType {
        LuauType::literal("LoginOutcome")
    }
}

#[cfg(feature = "docgen")]
impl DescribeInterface for LoginOutcome {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("LoginOutcome", None);
        descriptor.fields.extend([
            FieldDescriptor {
                name: "status",
                ty: LuauType::literal(
                    "\"ok\" | \"invalid_credentials\" | \"rate_limited\" | \"invalid_client_name\"",
                ),
                description: None,
            },
            FieldDescriptor {
                name: "result",
                ty: Option::<LoginResult>::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "retry_after_seconds",
                ty: Option::<u64>::luau_type(),
                description: None,
            },
        ]);
        descriptor
    }
}

impl LuauTypeInfo for AuthCapabilities {
    fn luau_type() -> LuauType {
        LuauType::literal("AuthCapabilities")
    }
}

impl DescribeInterface for AuthCapabilities {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("AuthCapabilities", None);
        descriptor.fields.extend([
            FieldDescriptor {
                name: "enabled",
                ty: bool::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "allow_default_login_when_disabled",
                ty: bool::luau_type(),
                description: None,
            },
        ]);
        descriptor
    }
}

#[cfg(feature = "docgen")]
fn param(name: &'static str, ty: LuauType) -> ParameterDescriptor {
    ParameterDescriptor {
        name,
        ty,
        description: None,
        variadic: false,
    }
}

#[cfg(feature = "docgen")]
fn module_descriptor() -> ModuleDescriptor {
    ModuleDescriptor {
        name: "Auth",
        local_name: "auth",
        description: None,
        fields: Vec::new(),
        functions: vec![
            ModuleFunctionDescriptor {
                path: vec!["get_session_data"],
                description: Some(
                    "Reads this plugin's JSON data on a live authenticated session. Invalid, revoked, or expired tokens fail.",
                ),
                params: vec![param("token", String::luau_type())],
                returns: vec![JsonValue::luau_type()],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["set_session_data"],
                description: Some(
                    "Stores up to 16 KiB of JSON for this plugin on a live session. Nil clears it; revoking the session removes the data.",
                ),
                params: vec![
                    param("token", String::luau_type()),
                    param("data", JsonValue::luau_type()),
                ],
                returns: vec![],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["resolve_auth"],
                description: Some(
                    "Resolves a bearer credential to the authenticated principal and credential metadata. \
                     On success, the calling dispatch and the tasks it spawned act as that \
                     principal from then on.",
                ),
                params: vec![param("bearer", Option::<String>::luau_type())],
                returns: vec![Option::<ResolvedAuth>::luau_type()],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["logout_session"],
                description: Some("Revokes the session identified by the provided token."),
                params: vec![param("token", Option::<String>::luau_type())],
                returns: vec![bool::luau_type()],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["login"],
                description: Some(
                    "Attempts to log in and returns the outcome, including a principal plus \
                     session token on success.",
                ),
                params: vec![
                    param("username", String::luau_type()),
                    param("password", Option::<String>::luau_type()),
                    param("user_agent", Option::<String>::luau_type()),
                    param("client_name", Option::<String>::luau_type()),
                ],
                returns: vec![LoginOutcome::luau_type()],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["capabilities"],
                description: Some("Returns the current authentication capabilities."),
                params: Vec::new(),
                returns: vec![AuthCapabilities::luau_type()],
                yields: false,
            },
        ],
    }
}

#[cfg(feature = "docgen")]
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    render_definition_file_with_support(
        &module_descriptor(),
        &[JsonValue::type_alias_descriptor()],
        &[
            AuthCapabilities::interface_descriptor(),
            Principal::interface_descriptor(),
            AuthCredential::interface_descriptor(),
            ResolvedAuth::interface_descriptor(),
            LoginResult::interface_descriptor(),
            LoginOutcome::interface_descriptor(),
        ],
        &[],
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plugin_credentials_expose_kind_and_public_id_only() -> anyhow::Result<()> {
        let session = serde_json::to_value(to_plugin_credential(ServiceAuthCredential::Session {
            public_id: "session-public-id".to_string(),
        }))?;
        assert_eq!(
            session,
            serde_json::json!({ "kind": "session", "id": "session-public-id", "api_key_name": null })
        );

        let api_key = serde_json::to_value(to_plugin_credential(ServiceAuthCredential::ApiKey {
            public_id: "key-public-id".to_string(),
            name: "laptop".to_string(),
        }))?;
        assert_eq!(
            api_key,
            serde_json::json!({ "kind": "api_key", "id": "key-public-id", "api_key_name": "laptop" })
        );

        let default = serde_json::to_value(to_plugin_credential(ServiceAuthCredential::Default))?;
        assert_eq!(
            default,
            serde_json::json!({ "kind": "default", "id": null, "api_key_name": null })
        );
        Ok(())
    }
}
