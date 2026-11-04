// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use harmony_luau as luau;
#[cfg(feature = "docgen")]
use harmony_luau::DescribeTypeAlias;
use harmony_luau::JsonValue;
use harmony_luau::{
    DescribeInterface,
    FieldDescriptor,
    InterfaceDescriptor,
    LuauType,
    LuauTypeInfo,
};
use serde::{
    Deserialize,
    Serialize,
};
use std::sync::Arc;

use crate::plugins::args::JsonArg;
use crate::services::auth::{
    AuthCredential as ServiceAuthCredential,
    AuthError,
    Principal as ServicePrincipal,
    ResolvedAuth as ServiceResolvedAuth,
    login_with_password,
    logout_with_token,
    owners::Owners,
    resolve_auth_from_bearer,
    sessions::{
        self,
        SessionMetadata,
    },
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
}

/// Whose data a read covers, as plugins name it: the caller's without either field.
#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OwnersQuery {
    user_id: Option<String>,
    #[serde(default)]
    all_users: bool,
}

impl OwnersQuery {
    pub(crate) fn owners(&self, function: &str) -> luau::runtime::Result<Owners<'_>> {
        match (self.user_id.as_deref(), self.all_users) {
            (Some(_), true) => Err(crate::plugins::runtime_error(format!(
                "{function} takes user_id or all_users, not both"
            ))),
            (Some(user_id), false) => Ok(Owners::User(user_id)),
            (None, true) => Ok(Owners::All),
            (None, false) => Ok(Owners::Principal),
        }
    }

    /// [`Self::owners`] for a listing of other users' data, which takes no query for the caller's.
    pub(crate) fn named_owners(&self, function: &str) -> luau::runtime::Result<Owners<'_>> {
        if self.user_id.is_none() && !self.all_users {
            return Err(crate::plugins::runtime_error(format!(
                "{function} takes user_id or all_users"
            )));
        }
        self.owners(function)
    }
}

impl<'vm> luau::FromLuau<'vm> for OwnersQuery {
    fn read(reader: &mut luau::ArgReader<'vm>) -> luau::runtime::Result<Self> {
        let query: luau::Value = reader.read()?;
        match harmony_serde::luau_to_json(&query, 0)? {
            // An empty table converts to an empty array.
            serde_json::Value::Array(values) if values.is_empty() => Ok(Self::default()),
            query => serde_json::from_value(query).map_err(crate::plugins::runtime_error),
        }
    }
}

#[derive(Serialize)]
struct SessionInfo {
    session_id: String,
    user_id: String,
    username: String,
    created_at_ms: i64,
    last_seen_at_ms: i64,
    client_name: Option<String>,
    user_agent: Option<String>,
    data: serde_json::Value,
}

impl From<sessions::SessionInfo> for SessionInfo {
    fn from(session: sessions::SessionInfo) -> Self {
        Self {
            session_id: session.id,
            user_id: session.user_id,
            username: session.username,
            created_at_ms: session.created_at_ms,
            last_seen_at_ms: session.last_seen_at_ms,
            client_name: session.client_name,
            user_agent: session.user_agent,
            data: session.plugin_data,
        }
    }
}

#[derive(Clone, Serialize)]
pub(crate) struct AuthCapabilities {
    pub(crate) enabled: bool,
    pub(crate) allow_default_login_when_disabled: bool,
}

impl AuthCapabilities {
    pub(crate) fn from_config(config: &crate::services::settings::server::AuthConfig) -> Self {
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

pub(crate) struct AuthModule;

/// The dispatch's principal slot, when the dispatch has one.
struct CurrentDispatchAuth(Option<Arc<DispatchAuth>>);

impl luau::FromCallFrame for CurrentDispatchAuth {
    fn from_call_frame(_vm: &luau::Vm, context: &luau::CallContext) -> luau::runtime::Result<Self> {
        Ok(Self(context.caller.get::<DispatchAuth>().ok()))
    }
}

/// The remote client the dispatch serves, or the default address for a server-side dispatch.
struct LoginClient(crate::services::auth::caller::ClientAddress);

impl luau::FromCallFrame for LoginClient {
    fn from_call_frame(_vm: &luau::Vm, context: &luau::CallContext) -> luau::runtime::Result<Self> {
        Ok(Self(
            context
                .caller
                .get::<DispatchClient>()
                .map(|client| client.0)
                .unwrap_or_default(),
        ))
    }
}

crate::plugins::limits::module_limits!(&[crate::plugins::limits::Limit {
    name: "MAX_SESSION_DATA_BYTES",
    value: crate::services::auth::sessions::MAX_PLUGIN_SESSION_DATA_BYTES,
    description: "The most bytes of JSON `set_session_data` stores; it raises past them.",
}]);

#[harmony_macros::module(
    id = "lyra/auth",
    description = "Sessions, logins, and the principal a dispatch acts for. A coroutine acts with the auth of the call that created it, as does code a module runs when it is first required.",
    docs_feature = "docgen",
    extend = install_limits,
    describe = describe_limits
)]
impl AuthModule {
    #[harmony(
        description = "Reads this plugin's JSON data on a live authenticated session. Invalid, revoked, or expired tokens fail.",
        returns(JsonValue)
    )]
    async fn get_session_data(
        token: String,
        #[context] origin: luau::ChunkOrigin,
    ) -> luau::runtime::Result<luau::Value> {
        let plugin_id = origin
            .plugin
            .ok_or_else(|| crate::plugins::runtime_error("session data requires plugin origin"))?;
        let value = crate::services::auth::sessions::get_plugin_data(&token, &plugin_id).await?;
        harmony_luau::serializable_to_luau_owned(value)
    }

    #[harmony(
        description = "Stores up to `MAX_SESSION_DATA_BYTES` bytes of JSON for this plugin on a live session. Nil clears it; revoking the session removes the data.",
        returns()
    )]
    async fn set_session_data(
        token: String,
        data: JsonArg,
        #[context] origin: luau::ChunkOrigin,
    ) -> luau::runtime::Result<luau::Value> {
        let plugin_id = origin
            .plugin
            .ok_or_else(|| crate::plugins::runtime_error("session data requires plugin origin"))?;
        crate::services::auth::sessions::set_plugin_data(&token, &plugin_id, data.0).await?;
        Ok(luau::Value::Nil)
    }

    #[harmony(
        description = "Resolves a bearer credential to the authenticated principal and credential metadata. \
                     On success, the calling dispatch and the tasks it spawned act as that \
                     principal from then on.",
        returns(Option<ResolvedAuth>)
    )]
    async fn resolve_auth(
        bearer: Option<String>,
        #[context] dispatch_auth: CurrentDispatchAuth,
    ) -> luau::runtime::Result<luau::Value> {
        let resolved = match resolve_auth_from_bearer(bearer.as_deref()).await {
            Ok(Some(resolved)) => resolved,
            Ok(None) | Err(AuthError::SessionExpired) => return Ok(luau::Value::Nil),
            Err(err) => return Err(crate::plugins::runtime_error(err)),
        };
        match resolved
            .principal
            .require(&*crate::services::STATE.db.read().await)
        {
            Ok(_) => {}
            Err(AuthError::InvalidBearerCredential) => return Ok(luau::Value::Nil),
            Err(err) => return Err(crate::plugins::runtime_error(err)),
        }
        if let Some(dispatch_auth) = &dispatch_auth.0 {
            dispatch_auth.record(resolved.principal.clone());
        }
        harmony_luau::serializable_to_luau_owned(to_plugin_auth(resolved))
    }

    #[harmony(description = "Revokes the session identified by the provided token.")]
    async fn logout_session(token: Option<String>) -> luau::runtime::Result<bool> {
        let revoked = logout_with_token(token.as_deref())
            .await
            .map_err(crate::plugins::runtime_error)?;
        Ok(revoked)
    }

    #[harmony(
        description = "Lists live sessions, oldest first, never their tokens. Without a query, lists \
                     the calling principal's own; another user's or all users' require the \
                     manage_users permission, and an admin's also the admin permission. All \
                     users' leaves out the admins' without it.",
        returns(Vec<SessionInfo>)
    )]
    async fn list_sessions(
        query: Option<OwnersQuery>,
        #[context] principal: ServicePrincipal,
        #[context] origin: luau::ChunkOrigin,
    ) -> luau::runtime::Result<luau::Value> {
        let query = query.unwrap_or_default();
        let plugin_id = origin.plugin.ok_or_else(|| {
            crate::plugins::runtime_error("session listing requires plugin origin")
        })?;
        let owners = query.owners("list_sessions")?;
        let sessions = sessions::list_sessions(&principal, owners, &plugin_id)
            .await
            .map_err(crate::plugins::runtime_error)?;
        harmony_luau::serializable_to_luau_owned(
            sessions
                .into_iter()
                .map(SessionInfo::from)
                .collect::<Vec<_>>(),
        )
    }

    #[harmony(
        description = "Revokes a session by its public id and reports whether it was revoked. \
                     Another user's session counts only with the manage_users permission, and an \
                     admin's also with the admin permission."
    )]
    async fn revoke_session(
        session_id: String,
        #[context] principal: ServicePrincipal,
    ) -> luau::runtime::Result<bool> {
        let revoked = sessions::revoke_session(&principal, &session_id)
            .await
            .map_err(crate::plugins::runtime_error)?;
        Ok(revoked)
    }

    #[harmony(
        description = "Attempts to log in and returns the outcome, including a principal plus \
                     session token on success. The session's `client_name` has control and \
                     invisible characters stripped and is cut to 128 characters.",
        returns(LoginOutcome)
    )]
    async fn login(
        username: String,
        password: Option<String>,
        user_agent: Option<String>,
        client_name: Option<String>,
        #[context] client: LoginClient,
    ) -> luau::runtime::Result<luau::Value> {
        let metadata = SessionMetadata {
            user_agent,
            client_name: sessions::tidy_client_name(client_name),
        };
        let login = match login_with_password(
            &username,
            password.as_deref().unwrap_or(""),
            metadata,
            client.0,
        )
        .await
        {
            Ok(login) => login,
            Err(AuthError::RateLimited(retry_after)) => {
                return harmony_luau::serializable_to_luau_owned(LoginOutcome::rate_limited(
                    retry_after,
                ));
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
    }

    #[harmony(
        description = "Returns the current authentication capabilities.",
        returns(AuthCapabilities)
    )]
    fn capabilities(vm: &luau::Vm) -> luau::runtime::Result<luau::OwnedTable> {
        let store = vm.data().get::<AuthCapabilitiesModuleStore>()?;
        let capabilities = match &store.settings {
            Some(settings) => AuthCapabilities::from_config(&settings.get().config.auth),
            None => store.capabilities.clone(),
        };
        Ok(auth_capabilities_table(&capabilities))
    }
}

/// Per-dispatch client identity resolved at the HTTP boundary, so host calls
/// made on behalf of a plugin-served request (e.g. login) can be attributed
/// to the remote client rather than the server process.
#[derive(Clone)]
pub(crate) struct DispatchClient(pub(crate) crate::services::auth::caller::ClientAddress);

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

/// A module function's principal is the dispatch's, which it requires.
impl luau::FromCallFrame for ServicePrincipal {
    fn from_call_frame(_vm: &luau::Vm, context: &luau::CallContext) -> luau::runtime::Result<Self> {
        require_dispatch_principal(context)
    }
}

/// [`dispatch_principal`], as a module function takes it from its call: `None` for a
/// server-side dispatch.
pub(crate) struct DispatchPrincipal(pub(crate) Option<ServicePrincipal>);

impl luau::FromCallFrame for DispatchPrincipal {
    fn from_call_frame(_vm: &luau::Vm, context: &luau::CallContext) -> luau::runtime::Result<Self> {
        dispatch_principal(context).map(Self)
    }
}

/// Whether `principal` holds the manage_libraries permission; the system, `None`, does.
pub(crate) fn manages_libraries(principal: Option<&ServicePrincipal>) -> bool {
    principal.is_none_or(|principal| {
        lyra_db::roles::has_permission(&principal.permissions, lyra_db::Permission::ManageLibraries)
    })
}

impl DispatchPrincipal {
    /// Whether the dispatch may change the shared library, by the permissions it started with:
    /// a server-side one may, a user's needs manage_libraries. A refusal stands, as permissions
    /// only shrink when rechecked; a grant needs [`Self::manages_libraries_now`] under the write.
    pub(crate) fn can_manage_libraries(&self) -> bool {
        manages_libraries(self.0.as_ref())
    }

    /// [`Self::can_manage_libraries`] as the database has the caller now, under the caller's
    /// guard. A caller no longer authorized can't.
    pub(crate) fn manages_libraries_now(&self, db: &agdb::DbAny) -> luau::runtime::Result<bool> {
        let Some(principal) = &self.0 else {
            return Ok(true);
        };
        match principal.current_with(db, lyra_db::Permission::ManageLibraries) {
            Ok(_) => Ok(true),
            Err(crate::services::auth::Recheck::Internal(error)) => {
                Err(crate::plugins::runtime_error(error))
            }
            Err(_) => Ok(false),
        }
    }
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
    db: &impl lyra_db::DbAccess,
) -> luau::runtime::Result<agdb::DbId> {
    principal.require(db).map_err(crate::plugins::runtime_error)
}

#[derive(Clone)]
pub(crate) struct AuthCapabilitiesModuleStore {
    capabilities: AuthCapabilities,
    settings: Option<crate::services::state::SettingsHandle>,
}
impl AuthCapabilitiesModuleStore {
    pub(crate) fn new(
        capabilities: AuthCapabilities,
        settings: Option<crate::services::state::SettingsHandle>,
    ) -> Self {
        Self {
            capabilities,
            settings,
        }
    }
}
fn auth_capabilities_table(capabilities: &AuthCapabilities) -> luau::OwnedTable {
    let mut table = luau::OwnedTable::with_capacity(0, 2);
    table.set_field("enabled", capabilities.enabled);
    table.set_field(
        "allow_default_login_when_disabled",
        capabilities.allow_default_login_when_disabled,
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
                ty: LuauType::literal("\"ok\" | \"invalid_credentials\" | \"rate_limited\""),
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

impl LuauTypeInfo for OwnersQuery {
    fn luau_type() -> LuauType {
        LuauType::literal("OwnersQuery")
    }
}

impl DescribeInterface for OwnersQuery {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new(
            "OwnersQuery",
            Some(
                "Whose data a listing reads: the caller's without either field, where the listing reads the caller's.",
            ),
        );
        descriptor.fields.extend([
            FieldDescriptor {
                name: "user_id",
                ty: Option::<String>::luau_type(),
                description: Some("Reads this user's instead of the caller's."),
            },
            FieldDescriptor {
                name: "all_users",
                ty: Option::<bool>::luau_type(),
                description: Some("Reads every user's."),
            },
        ]);
        descriptor
    }
}

impl LuauTypeInfo for SessionInfo {
    fn luau_type() -> LuauType {
        LuauType::literal("SessionInfo")
    }
}

impl DescribeInterface for SessionInfo {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("SessionInfo", None);
        descriptor.fields.extend([
            FieldDescriptor {
                name: "session_id",
                ty: String::luau_type(),
                description: Some("The session's public id."),
            },
            FieldDescriptor {
                name: "user_id",
                ty: String::luau_type(),
                description: Some("The owner's public id."),
            },
            FieldDescriptor {
                name: "username",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "created_at_ms",
                ty: i64::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "last_seen_at_ms",
                ty: i64::luau_type(),
                description: Some("When the session last authenticated a request."),
            },
            FieldDescriptor {
                name: "client_name",
                ty: Option::<String>::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "user_agent",
                ty: Option::<String>::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "data",
                ty: JsonValue::luau_type(),
                description: Some("This plugin's data on the session."),
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
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    use harmony_luau::DescribeModule;

    harmony_luau::render_definition_file_with_support(
        &AuthModule::module_descriptor(),
        &[JsonValue::type_alias_descriptor()],
        &[
            AuthCapabilities::interface_descriptor(),
            Principal::interface_descriptor(),
            AuthCredential::interface_descriptor(),
            ResolvedAuth::interface_descriptor(),
            LoginResult::interface_descriptor(),
            LoginOutcome::interface_descriptor(),
            OwnersQuery::interface_descriptor(),
            SessionInfo::interface_descriptor(),
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
