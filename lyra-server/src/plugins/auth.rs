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

use harmony_luau::StringEnum;

use crate::plugins::args::{
    self,
    JsonArg,
};
use crate::plugins::types::LuauPermission;
use crate::services::auth::{
    AuthCredential as ServiceAuthCredential,
    AuthError,
    Principal as ServicePrincipal,
    ResolvedAuth as ServiceResolvedAuth,
    device_logins::{
        self,
        DeviceLoginClaim,
        DeviceLoginError,
    },
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
    pub(crate) permissions: Vec<&'static str>,
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

/// The `options` argument of `begin_device_login`.
#[derive(Default)]
struct BeginDeviceLoginOptions {
    code: Option<String>,
    client_name: Option<String>,
    user_agent: Option<String>,
    data: Option<serde_json::Value>,
}

impl<'vm> luau::FromLuau<'vm> for BeginDeviceLoginOptions {
    fn read(reader: &mut luau::ArgReader<'vm>) -> luau::runtime::Result<Self> {
        let options: luau::Table = reader.read()?;
        args::reject_unknown_fields(
            &options,
            "options",
            &["code", "client_name", "user_agent", "data"],
        )?;
        Ok(Self {
            code: options.optional_string_field("code")?,
            client_name: options.optional_string_field("client_name")?,
            user_agent: options.optional_string_field("user_agent")?,
            data: Some(harmony_serde::luau_to_json(&options.get("data")?)?),
        })
    }
}

/// The `options` argument of `approve_device_login`.
#[derive(Default)]
struct ApproveDeviceLoginOptions {
    user_id: Option<String>,
}

impl<'vm> luau::FromLuau<'vm> for ApproveDeviceLoginOptions {
    fn read(reader: &mut luau::ArgReader<'vm>) -> luau::runtime::Result<Self> {
        let options: luau::Table = reader.read()?;
        args::reject_unknown_fields(&options, "options", &["user_id"])?;
        Ok(Self {
            user_id: options.optional_string_field("user_id")?,
        })
    }
}

#[derive(Serialize)]
struct BegunDeviceLogin {
    code: String,
    secret: String,
    created_at_ms: i64,
    expires_at_ms: i64,
}

#[derive(Serialize)]
struct BeginDeviceLoginOutcome {
    status: &'static str,
    result: Option<BegunDeviceLogin>,
    retry_after_seconds: Option<u64>,
}

impl BeginDeviceLoginOutcome {
    fn refused(status: &'static str, retry_after: Option<std::time::Duration>) -> Self {
        Self {
            status,
            result: None,
            retry_after_seconds: retry_after_seconds(retry_after),
        }
    }
}

#[derive(Serialize)]
struct DeviceLoginRequest {
    code: String,
    client_name: Option<String>,
    data: Option<serde_json::Value>,
    created_at_ms: i64,
    expires_at_ms: i64,
}

#[derive(Serialize)]
struct DeviceLoginStatusOutcome {
    status: &'static str,
    result: Option<DeviceLoginRequest>,
}

#[derive(Serialize)]
struct ApproveDeviceLoginOutcome {
    status: &'static str,
    retry_after_seconds: Option<u64>,
}

impl ApproveDeviceLoginOutcome {
    fn new(status: &'static str, retry_after: Option<std::time::Duration>) -> Self {
        Self {
            status,
            retry_after_seconds: retry_after_seconds(retry_after),
        }
    }
}

#[derive(Serialize)]
struct ClaimDeviceLoginOutcome {
    status: &'static str,
    result: Option<LoginResult>,
    data: Option<serde_json::Value>,
}

impl ClaimDeviceLoginOutcome {
    fn unresolved(status: &'static str) -> Self {
        Self {
            status,
            result: None,
            data: None,
        }
    }
}

fn retry_after_seconds(retry_after: Option<std::time::Duration>) -> Option<u64> {
    retry_after.map(|retry_after| retry_after.as_secs().max(1))
}

/// A device login failure that isn't an outcome a plugin handles, raised for it.
fn device_login_error(error: DeviceLoginError) -> luau::Error {
    match error {
        DeviceLoginError::Auth(error) => crate::plugins::errors::auth_error(error),
        error => crate::plugins::runtime_error(error),
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

/// The verified principal's database id, and the users `owners` covers whose data it may read.
pub(crate) fn owned_users(
    db: &agdb::DbAny,
    principal: &ServicePrincipal,
    owners: Owners<'_>,
) -> luau::runtime::Result<(agdb::DbId, Vec<lyra_db::User>)> {
    let viewer_db_id = require_user_db_id(principal, db)?;
    let current = principal
        .current(db)
        .map_err(crate::plugins::errors::auth_error)?;
    let users = crate::services::auth::owners::resolve(db, &current, viewer_db_id, owners)
        .map_err(crate::plugins::errors::auth_error)?;
    Ok((viewer_db_id, users))
}

impl<'vm> luau::FromLuau<'vm> for OwnersQuery {
    fn read(reader: &mut luau::ArgReader<'vm>) -> luau::runtime::Result<Self> {
        let query: luau::Value = reader.read()?;
        match harmony_serde::luau_to_json(&query)? {
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
        permissions: permission_names(&principal.permissions),
    }
}

/// The permissions as plugins spell them.
pub(crate) fn permission_names(permissions: &[lyra_db::Permission]) -> Vec<&'static str> {
    permissions
        .iter()
        .map(|permission| LuauPermission::from(*permission).as_str())
        .collect()
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

crate::plugins::limits::module_limits!(&[
    crate::plugins::limits::Limit {
        name: "MAX_SESSION_DATA_BYTES",
        value: crate::services::auth::sessions::MAX_PLUGIN_SESSION_DATA_BYTES,
        description: "The most bytes of JSON `set_session_data` stores; it raises past them.",
    },
    crate::plugins::limits::Limit {
        name: "MAX_DEVICE_LOGIN_DATA_BYTES",
        value: crate::services::auth::device_logins::MAX_DEVICE_LOGIN_DATA_BYTES,
        description: "The most bytes of JSON `begin_device_login` keeps as `data`; it raises past them.",
    }
]);

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
            Err(err) => return Err(crate::plugins::errors::auth_error(err)),
        };
        match resolved
            .principal
            .require(&*crate::services::STATE.db.read().await)
        {
            Ok(_) => {}
            Err(AuthError::InvalidBearerCredential) => return Ok(luau::Value::Nil),
            Err(err) => return Err(crate::plugins::errors::auth_error(err)),
        }
        if let Some(dispatch_auth) = &dispatch_auth.0 {
            dispatch_auth.record_auth(&resolved);
        }
        harmony_luau::serializable_to_luau_owned(to_plugin_auth(resolved))
    }

    #[harmony(description = "Revokes the session identified by the provided token.")]
    async fn logout_session(token: Option<String>) -> luau::runtime::Result<bool> {
        let revoked = logout_with_token(token.as_deref())
            .await
            .map_err(crate::plugins::errors::auth_error)?;
        Ok(revoked)
    }

    #[harmony(
        description = "Lists live sessions, oldest first, never their tokens. Without a query, lists \
                     the calling principal's own; another user's or all users' require the \
                     manage_users permission and every permission that user holds. All users' \
                     leaves out those the principal lacks a permission of.",
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
            .map_err(crate::plugins::errors::auth_error)?;
        harmony_luau::serializable_to_luau_owned(
            sessions
                .into_iter()
                .map(SessionInfo::from)
                .collect::<Vec<_>>(),
        )
    }

    #[harmony(
        description = "Revokes a session by its public id and reports whether it was revoked. \
                     Another user's session counts only with the manage_users permission and \
                     every permission that user holds."
    )]
    async fn revoke_session(
        session_id: String,
        #[context] principal: ServicePrincipal,
    ) -> luau::runtime::Result<bool> {
        let revoked = sessions::revoke_session(&principal, &session_id)
            .await
            .map_err(crate::plugins::errors::auth_error)?;
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
            Err(err) => return Err(crate::plugins::errors::auth_error(err)),
        };
        let Some(login) = login else {
            return harmony_luau::serializable_to_luau_owned(LoginOutcome::invalid_credentials());
        };
        let resolved = resolve_auth_from_bearer(Some(&login.token))
            .await
            .map_err(crate::plugins::errors::auth_error)?
            .ok_or_else(|| {
                crate::plugins::runtime_error("freshly issued session token failed to resolve")
            })?;
        harmony_luau::serializable_to_luau_owned(LoginOutcome::ok(LoginResult {
            principal: to_plugin_principal(resolved.principal),
            token: login.token,
        }))
    }

    #[harmony(
        description = "Starts a device login for a client that wants to sign in, charged as a login attempt against the request's address. The client shows `code` for a signed-in device to approve, and claims with `secret`. It lives 10 minutes. `status` is `code_taken` when another device login holds the `code` the caller gave. It raises when `data` is past `MAX_DEVICE_LOGIN_DATA_BYTES` bytes of JSON, and for a `code` that isn't 6 to 32 ASCII letters and digits. `status` is `disabled` while authentication is, which makes every device login function answer so.",
        returns(BeginDeviceLoginOutcome)
    )]
    async fn begin_device_login(
        options: Option<BeginDeviceLoginOptions>,
        #[context] client: LoginClient,
    ) -> luau::runtime::Result<luau::Value> {
        let options = options.unwrap_or_default();
        let metadata = SessionMetadata {
            user_agent: options.user_agent,
            client_name: sessions::tidy_client_name(options.client_name),
        };
        let outcome =
            match device_logins::begin(metadata, client.0, options.data, options.code.as_deref()) {
                Ok(begun) => BeginDeviceLoginOutcome {
                    status: "ok",
                    result: Some(BegunDeviceLogin {
                        code: begun.code,
                        secret: begun.secret,
                        created_at_ms: begun.created_at_ms,
                        expires_at_ms: begun.expires_at_ms,
                    }),
                    retry_after_seconds: None,
                },
                Err(DeviceLoginError::Disabled) => {
                    BeginDeviceLoginOutcome::refused("disabled", None)
                }
                Err(DeviceLoginError::CodeTaken) => {
                    BeginDeviceLoginOutcome::refused("code_taken", None)
                }
                Err(DeviceLoginError::Auth(AuthError::RateLimited(retry_after))) => {
                    BeginDeviceLoginOutcome::refused("rate_limited", Some(retry_after))
                }
                Err(error) => return Err(device_login_error(error)),
            };
        harmony_luau::serializable_to_luau_owned(outcome)
    }

    #[harmony(
        description = "Reads the device login behind `secret` and never signs anyone in. `result` is nil unless `status` is `pending` or `approved`; an unknown or expired secret is `not_found`.",
        returns(DeviceLoginStatusOutcome)
    )]
    async fn device_login_status(secret: String) -> luau::runtime::Result<luau::Value> {
        let outcome = match device_logins::status(&secret) {
            Ok(status) => DeviceLoginStatusOutcome {
                status: if status.approved {
                    "approved"
                } else {
                    "pending"
                },
                result: Some(DeviceLoginRequest {
                    code: status.code,
                    client_name: status.client_name,
                    data: status.data,
                    created_at_ms: status.created_at_ms,
                    expires_at_ms: status.expires_at_ms,
                }),
            },
            Err(DeviceLoginError::Disabled) => DeviceLoginStatusOutcome {
                status: "disabled",
                result: None,
            },
            Err(DeviceLoginError::NotFound) => DeviceLoginStatusOutcome {
                status: "not_found",
                result: None,
            },
            Err(error) => return Err(device_login_error(error)),
        };
        harmony_luau::serializable_to_luau_owned(outcome)
    }

    #[harmony(
        description = "Approves the device login behind `code`, ignoring case and whitespace, for the calling principal or `user_id`. Naming another user needs the manage_users permission and every permission that user holds, and raises `forbidden` otherwise. The approving credential must be a session this dispatch resolved, and the approval dies with it; an API key raises `forbidden`, since a key must not sign anyone in. The first approval wins. Too many codes it doesn't find make it `rate_limited` for that principal.",
        returns(ApproveDeviceLoginOutcome)
    )]
    async fn approve_device_login(
        code: String,
        options: Option<ApproveDeviceLoginOptions>,
        #[context] caller: CredentialedCaller,
    ) -> luau::runtime::Result<luau::Value> {
        let options = options.unwrap_or_default();
        if matches!(caller.credential, ServiceAuthCredential::ApiKey { .. }) {
            return Err(crate::plugins::errors::auth_error(AuthError::Forbidden(
                "an api key cannot approve a device login".to_string(),
            )));
        }
        let outcome = match device_logins::approve(
            &caller.principal,
            &caller.credential,
            &code,
            options.user_id.as_deref(),
        )
        .await
        {
            Ok(()) => ApproveDeviceLoginOutcome::new("ok", None),
            Err(DeviceLoginError::Disabled) => ApproveDeviceLoginOutcome::new("disabled", None),
            Err(DeviceLoginError::NotFound) => ApproveDeviceLoginOutcome::new("not_found", None),
            Err(DeviceLoginError::AlreadyApproved) => {
                ApproveDeviceLoginOutcome::new("already_approved", None)
            }
            Err(DeviceLoginError::UserNotFound) => {
                ApproveDeviceLoginOutcome::new("user_not_found", None)
            }
            Err(DeviceLoginError::Auth(AuthError::RateLimited(retry_after))) => {
                ApproveDeviceLoginOutcome::new("rate_limited", Some(retry_after))
            }
            Err(error) => return Err(device_login_error(error)),
        };
        harmony_luau::serializable_to_luau_owned(outcome)
    }

    #[harmony(
        description = "Signs in the client holding `secret`. The first call after the approval creates a session; repeats return the same result until the device login expires, 10 minutes after the first. `result` is what `login` gives when `status` is `ok`, and nil otherwise, as is `data`, what the device login began with; one whose user was deleted, whose user's password changed, or whose approving session was revoked since the approval, or whose session is gone, is `not_found`.",
        returns(ClaimDeviceLoginOutcome)
    )]
    async fn claim_device_login(secret: String) -> luau::runtime::Result<luau::Value> {
        let (login, data) = match device_logins::claim(&secret).await {
            Ok(DeviceLoginClaim::Complete { login, data }) => (login, data),
            Ok(DeviceLoginClaim::Pending) => {
                return harmony_luau::serializable_to_luau_owned(
                    ClaimDeviceLoginOutcome::unresolved("pending"),
                );
            }
            Err(DeviceLoginError::Disabled) => {
                return harmony_luau::serializable_to_luau_owned(
                    ClaimDeviceLoginOutcome::unresolved("disabled"),
                );
            }
            Err(DeviceLoginError::NotFound) => {
                return harmony_luau::serializable_to_luau_owned(
                    ClaimDeviceLoginOutcome::unresolved("not_found"),
                );
            }
            Err(error) => return Err(device_login_error(error)),
        };
        let resolved = match resolve_auth_from_bearer(Some(&login.token)).await {
            Ok(Some(resolved)) => resolved,
            Ok(None) | Err(AuthError::SessionExpired) => {
                return harmony_luau::serializable_to_luau_owned(
                    ClaimDeviceLoginOutcome::unresolved("not_found"),
                );
            }
            Err(error) => return Err(crate::plugins::errors::auth_error(error)),
        };
        harmony_luau::serializable_to_luau_owned(ClaimDeviceLoginOutcome {
            status: "ok",
            result: Some(LoginResult {
                principal: to_plugin_principal(resolved.principal),
                token: login.token,
            }),
            data,
        })
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
pub(crate) struct DispatchAuth(Arc<std::sync::Mutex<Option<DispatchCaller>>>);

#[derive(Clone)]
struct DispatchCaller {
    principal: ServicePrincipal,
    /// What the principal authenticated with; none for a principal recorded without one.
    credential: Option<ServiceAuthCredential>,
}

impl DispatchAuth {
    /// Acts for `principal` with no credential to show for it.
    pub(crate) fn record(&self, principal: ServicePrincipal) {
        *self.0.lock().expect("dispatch auth slot poisoned") = Some(DispatchCaller {
            principal,
            credential: None,
        });
    }

    pub(crate) fn record_auth(&self, auth: &ServiceResolvedAuth) {
        *self.0.lock().expect("dispatch auth slot poisoned") = Some(DispatchCaller {
            principal: auth.principal.clone(),
            credential: Some(auth.credential.clone()),
        });
    }

    /// Replaces the principal only while the slot still acts for the same user,
    /// leaving a principal the plugin resolved for someone else in place.
    pub(crate) fn refresh(&self, principal: ServicePrincipal) {
        let mut slot = self.0.lock().expect("dispatch auth slot poisoned");
        if let Some(current) = slot
            .as_mut()
            .filter(|current| current.principal.user_public_id == principal.user_public_id)
        {
            current.principal = principal;
        }
    }

    /// Empties the slot, refusing principal-requiring host calls until a
    /// credential is resolved again.
    pub(crate) fn clear(&self) {
        *self.0.lock().expect("dispatch auth slot poisoned") = None;
    }

    pub(crate) fn principal(&self) -> Option<ServicePrincipal> {
        self.caller().map(|caller| caller.principal)
    }

    fn caller(&self) -> Option<DispatchCaller> {
        self.0.lock().expect("dispatch auth slot poisoned").clone()
    }
}

/// The dispatch's principal and the credential it authenticated with, which a function that binds
/// its effect to that credential takes from its call. Refused without either.
struct CredentialedCaller {
    principal: ServicePrincipal,
    credential: ServiceAuthCredential,
}

impl luau::FromCallFrame for CredentialedCaller {
    fn from_call_frame(_vm: &luau::Vm, context: &luau::CallContext) -> luau::runtime::Result<Self> {
        let caller = context
            .caller
            .get::<DispatchAuth>()
            .ok()
            .and_then(|auth| auth.caller());
        match caller {
            Some(DispatchCaller {
                principal,
                credential: Some(credential),
            }) => Ok(Self {
                principal,
                credential,
            }),
            _ => Err(no_dispatch_principal()),
        }
    }
}

/// Whom a response this dispatch returns is served for, as Lyra reads it when serving: the
/// principal in the dispatch's slot, or none when the slot is empty or the dispatch is
/// server-side, and the client's address, the default for a server-side dispatch.
pub(crate) struct ResponseCaller {
    pub(crate) principal: Option<ServicePrincipal>,
    pub(crate) client: crate::services::auth::caller::ClientAddress,
}

impl luau::FromCallFrame for ResponseCaller {
    fn from_call_frame(_vm: &luau::Vm, context: &luau::CallContext) -> luau::runtime::Result<Self> {
        Ok(Self {
            principal: context
                .caller
                .get::<DispatchAuth>()
                .ok()
                .and_then(|auth| auth.principal()),
            client: context
                .caller
                .get::<DispatchClient>()
                .map(|client| client.0)
                .unwrap_or_default(),
        })
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
    principal
        .require(db)
        .map_err(crate::plugins::errors::auth_error)
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
                ty: Vec::<LuauPermission>::luau_type(),
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

impl LuauTypeInfo for BeginDeviceLoginOptions {
    fn luau_type() -> LuauType {
        LuauType::literal("BeginDeviceLoginOptions")
    }
}

impl LuauTypeInfo for ApproveDeviceLoginOptions {
    fn luau_type() -> LuauType {
        LuauType::literal("ApproveDeviceLoginOptions")
    }
}

impl LuauTypeInfo for BegunDeviceLogin {
    fn luau_type() -> LuauType {
        LuauType::literal("BegunDeviceLogin")
    }
}

impl LuauTypeInfo for BeginDeviceLoginOutcome {
    fn luau_type() -> LuauType {
        LuauType::literal("BeginDeviceLoginOutcome")
    }
}

impl LuauTypeInfo for DeviceLoginRequest {
    fn luau_type() -> LuauType {
        LuauType::literal("DeviceLoginRequest")
    }
}

impl LuauTypeInfo for DeviceLoginStatusOutcome {
    fn luau_type() -> LuauType {
        LuauType::literal("DeviceLoginStatusOutcome")
    }
}

impl LuauTypeInfo for ApproveDeviceLoginOutcome {
    fn luau_type() -> LuauType {
        LuauType::literal("ApproveDeviceLoginOutcome")
    }
}

impl LuauTypeInfo for ClaimDeviceLoginOutcome {
    fn luau_type() -> LuauType {
        LuauType::literal("ClaimDeviceLoginOutcome")
    }
}

#[cfg(feature = "docgen")]
fn described(
    name: &'static str,
    ty: LuauType,
    description: Option<&'static str>,
) -> FieldDescriptor {
    FieldDescriptor {
        name,
        ty,
        description,
    }
}

#[cfg(feature = "docgen")]
fn device_login_interfaces() -> Vec<InterfaceDescriptor> {
    fn interface(name: &'static str, fields: Vec<FieldDescriptor>) -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new(name, None);
        descriptor.fields.extend(fields);
        descriptor
    }
    let retry_after = || described("retry_after_seconds", Option::<u64>::luau_type(), None);
    vec![
        interface(
            "BeginDeviceLoginOptions",
            vec![
                described(
                    "code",
                    Option::<String>::luau_type(),
                    Some(
                        "The caller's own code, for a client whose codes have a shape of their own: 6 to 32 ASCII letters and digits, stored without whitespace and in upper case. Default six letters the server draws.",
                    ),
                ),
                described(
                    "client_name",
                    Option::<String>::luau_type(),
                    Some(
                        "Labels the client to approvers and on its session; control and invisible characters are stripped and it is cut to 128 characters. Default none.",
                    ),
                ),
                described(
                    "user_agent",
                    Option::<String>::luau_type(),
                    Some("Recorded on the session. Default none."),
                ),
                described(
                    "data",
                    Option::<JsonValue>::luau_type(),
                    Some(
                        "JSON kept with the device login and returned by `device_login_status` and `claim_device_login` to whoever holds the secret. Default none.",
                    ),
                ),
            ],
        ),
        interface(
            "ApproveDeviceLoginOptions",
            vec![described(
                "user_id",
                Option::<String>::luau_type(),
                Some("Signs in this user instead of the caller. Default the caller."),
            )],
        ),
        interface(
            "BegunDeviceLogin",
            vec![
                described("code", String::luau_type(), Some("The code to show.")),
                described(
                    "secret",
                    String::luau_type(),
                    Some("What the client claims with."),
                ),
                described("created_at_ms", i64::luau_type(), None),
                described("expires_at_ms", i64::luau_type(), None),
            ],
        ),
        interface(
            "BeginDeviceLoginOutcome",
            vec![
                described(
                    "status",
                    LuauType::literal("\"ok\" | \"code_taken\" | \"disabled\" | \"rate_limited\""),
                    None,
                ),
                described("result", Option::<BegunDeviceLogin>::luau_type(), None),
                retry_after(),
            ],
        ),
        interface(
            "DeviceLoginRequest",
            vec![
                described("code", String::luau_type(), None),
                described("client_name", Option::<String>::luau_type(), None),
                described(
                    "data",
                    Option::<JsonValue>::luau_type(),
                    Some("What the device login began with."),
                ),
                described("created_at_ms", i64::luau_type(), None),
                described(
                    "expires_at_ms",
                    i64::luau_type(),
                    Some("Restarts when the device login is approved and when it is claimed."),
                ),
            ],
        ),
        interface(
            "DeviceLoginStatusOutcome",
            vec![
                described(
                    "status",
                    LuauType::literal("\"pending\" | \"approved\" | \"not_found\" | \"disabled\""),
                    None,
                ),
                described("result", Option::<DeviceLoginRequest>::luau_type(), None),
            ],
        ),
        interface(
            "ApproveDeviceLoginOutcome",
            vec![
                described(
                    "status",
                    LuauType::literal(
                        "\"ok\" | \"not_found\" | \"already_approved\" | \"user_not_found\" | \"rate_limited\" | \"disabled\"",
                    ),
                    None,
                ),
                retry_after(),
            ],
        ),
        interface(
            "ClaimDeviceLoginOutcome",
            vec![
                described(
                    "status",
                    LuauType::literal("\"ok\" | \"pending\" | \"not_found\" | \"disabled\""),
                    None,
                ),
                described("result", Option::<LoginResult>::luau_type(), None),
                described(
                    "data",
                    Option::<JsonValue>::luau_type(),
                    Some("What the device login began with, when `status` is `ok`."),
                ),
            ],
        ),
    ]
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
                description: Some(
                    "Reads this user's instead of the caller's. The default user reads as missing while authentication is enabled.",
                ),
            },
            FieldDescriptor {
                name: "all_users",
                ty: Option::<bool>::luau_type(),
                description: Some(
                    "Reads every user's, leaving out the default user's while authentication is enabled.",
                ),
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
        &[
            JsonValue::type_alias_descriptor(),
            LuauPermission::type_alias_descriptor(),
        ],
        &[
            AuthCapabilities::interface_descriptor(),
            Principal::interface_descriptor(),
            AuthCredential::interface_descriptor(),
            ResolvedAuth::interface_descriptor(),
            LoginResult::interface_descriptor(),
            LoginOutcome::interface_descriptor(),
            OwnersQuery::interface_descriptor(),
            SessionInfo::interface_descriptor(),
        ]
        .into_iter()
        .chain(device_login_interfaces())
        .collect::<Vec<_>>(),
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
