// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashMap;

#[cfg(feature = "docgen")]
use aide::transform::TransformOperation;
use axum::{
    Json,
    Router,
    http::{
        HeaderMap,
        StatusCode,
    },
    routing::{
        get,
        patch,
    },
};
use serde::{
    Deserialize,
    Serialize,
};

use crate::{
    STATE,
    config::BootConfig,
    db::{
        self,
        Permission,
    },
    services::{
        auth::{
            require_manage_plugins,
            require_manage_server,
            require_permission,
        },
        settings::server::{
            self as server_settings,
            ApplyMode,
            EffectiveSetting,
            Kind,
            SettingSource,
            UpdateError,
        },
    },
};

use super::{
    AppError,
    forbid_api_key_credential,
};

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct ServerInfoResponse {
    server_id: String,
    version: String,
    published_url: Option<String>,
    setup: crate::services::setup::Status,
    auth_enabled: bool,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum SettingSourceResponse {
    Default,
    Database,
    File,
}

impl From<SettingSource> for SettingSourceResponse {
    fn from(source: SettingSource) -> Self {
        match source {
            SettingSource::Default => Self::Default,
            SettingSource::Database => Self::Database,
            SettingSource::File => Self::File,
        }
    }
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct SettingPropsResponse {
    label: String,
    description: String,
    required: bool,
    /// Set in `config.json`; the API cannot change it.
    locked: bool,
    source: SettingSourceResponse,
    /// A change takes effect on the next restart rather than live.
    restart_required: bool,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
#[serde(tag = "type")]
enum SettingFieldResponse {
    #[serde(rename = "string")]
    String {
        key: String,
        #[serde(flatten)]
        props: SettingPropsResponse,
        value: Option<String>,
        default: Option<String>,
    },
    /// Integers travel as JSON integers so large values round-trip exactly.
    #[serde(rename = "number")]
    Number {
        key: String,
        #[serde(flatten)]
        props: SettingPropsResponse,
        min: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        max: Option<u64>,
        value: Option<u64>,
        default: Option<u64>,
    },
    #[serde(rename = "bool")]
    Bool {
        key: String,
        #[serde(flatten)]
        props: SettingPropsResponse,
        value: Option<bool>,
        default: Option<bool>,
    },
    #[serde(rename = "string_list")]
    StringList {
        key: String,
        #[serde(flatten)]
        props: SettingPropsResponse,
        value: Option<Vec<String>>,
        default: Option<Vec<String>>,
    },
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct SettingGroupResponse {
    id: String,
    label: String,
    fields: Vec<SettingFieldResponse>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct BootDbResponse {
    kind: String,
    path: String,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct BootResponse {
    port: u16,
    data_dir: String,
    db: BootDbResponse,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct ServerSettingsResponse {
    groups: Vec<SettingGroupResponse>,
    /// Restart-required settings whose effective value differs from the one
    /// this process started with.
    pending_restart: Vec<String>,
    boot: BootResponse,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct UpdateServerSettingsRequest {
    /// `null` clears the stored value so the default applies again.
    values: HashMap<String, serde_json::Value>,
}

fn map_update_error(error: UpdateError) -> AppError {
    match error {
        UpdateError::Undeclared(_) | UpdateError::Invalid { .. } => {
            AppError::bad_request(error.to_string())
        }
        UpdateError::Locked(_) => AppError::conflict(error.to_string()),
    }
}

fn string_of(value: &serde_json::Value) -> Option<String> {
    value.as_str().map(String::from)
}

fn string_list_of(value: &serde_json::Value) -> Option<Vec<String>> {
    value.as_array().map(|items| {
        items
            .iter()
            .filter_map(|item| item.as_str().map(String::from))
            .collect()
    })
}

fn field_response(
    setting: &EffectiveSetting,
    boot: &BootConfig,
) -> anyhow::Result<SettingFieldResponse> {
    let definition = setting.definition;
    let kind = definition.kind;
    let key = definition.key.to_string();
    let default = kind.default(boot)?;
    let value = &setting.value;
    let props = |required: bool| SettingPropsResponse {
        label: definition.label.to_string(),
        description: definition.description.to_string(),
        required,
        locked: setting.locked,
        source: setting.source.into(),
        restart_required: definition.apply == ApplyMode::RestartRequired,
    };

    Ok(match kind {
        Kind::Bool { .. } => SettingFieldResponse::Bool {
            key,
            props: props(true),
            value: value.as_bool(),
            default: default.as_bool(),
        },
        Kind::U32 { .. } | Kind::U64 { .. } | Kind::NullableU64 => SettingFieldResponse::Number {
            key,
            props: props(!matches!(kind, Kind::NullableU64)),
            min: 0,
            max: matches!(kind, Kind::U32 { .. }).then_some(u64::from(u32::MAX)),
            value: value.as_u64(),
            default: default.as_u64(),
        },
        Kind::NullableOrigin | Kind::Path { .. } => SettingFieldResponse::String {
            key,
            props: props(!matches!(kind, Kind::NullableOrigin)),
            value: string_of(value),
            default: string_of(&default),
        },
        Kind::OriginList { .. } | Kind::IpList { .. } => SettingFieldResponse::StringList {
            key,
            props: props(true),
            value: string_list_of(value),
            default: string_list_of(&default),
        },
    })
}

/// One snapshot of `STATE.settings` feeds the whole body, so values,
/// provenance, and the pending-restart list describe the same resolution.
fn settings_response() -> Result<ServerSettingsResponse, AppError> {
    let boot = STATE.boot.get();
    let current = STATE.settings.get();
    let startup = STATE.startup_settings.get();

    let mut groups: Vec<SettingGroupResponse> = Vec::new();
    for setting in &current.effective {
        let group = setting.definition.group;
        let field = field_response(setting, &boot)?;
        match groups.iter_mut().find(|existing| existing.id == group.id) {
            Some(existing) => existing.fields.push(field),
            None => groups.push(SettingGroupResponse {
                id: group.id.to_string(),
                label: group.label.to_string(),
                fields: vec![field],
            }),
        }
    }

    let pending_restart = current
        .effective
        .iter()
        .filter(|setting| setting.definition.apply == ApplyMode::RestartRequired)
        .filter(|setting| {
            startup
                .iter()
                .find(|started| started.definition.key == setting.definition.key)
                .is_none_or(|started| started.value != setting.value)
        })
        .map(|setting| setting.definition.key.to_string())
        .collect();

    Ok(ServerSettingsResponse {
        groups,
        pending_restart,
        boot: BootResponse {
            port: boot.port,
            data_dir: boot.data_dir.display().to_string(),
            db: BootDbResponse {
                kind: boot.db.kind.as_str().to_string(),
                path: boot.db.path.display().to_string(),
            },
        },
    })
}

/// Writes need an interactive session holding `manage_server`; an API key
/// alone must not be able to disable authentication.
async fn require_manage_server_session(headers: &HeaderMap) -> Result<(), AppError> {
    let auth = forbid_api_key_credential(headers).await?;
    require_permission(&auth.principal, Permission::ManageServer)?;
    Ok(())
}

async fn get_server_info() -> Result<Json<ServerInfoResponse>, AppError> {
    let db = STATE.db.read().await;
    let info =
        db::server::get(&db)?.ok_or_else(|| AppError::not_found("server info not initialized"))?;

    let config = STATE.config();
    drop(db);
    let setup = crate::services::setup::status().await?;

    Ok(Json(ServerInfoResponse {
        server_id: info.id,
        version: env!("CARGO_PKG_VERSION").to_string(),
        published_url: config.published_url.clone(),
        setup,
        auth_enabled: config.auth.enabled,
    }))
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UpdateSetupRequest {
    plugin_selection_skipped: bool,
}

async fn update_setup(
    headers: HeaderMap,
    Json(request): Json<UpdateSetupRequest>,
) -> Result<StatusCode, AppError> {
    require_manage_plugins(&headers).await?;
    let mut db = STATE.db.write().await;
    db::server::set_plugin_selection_skipped(&mut *db, request.plugin_selection_skipped)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn get_server_settings(headers: HeaderMap) -> Result<Json<ServerSettingsResponse>, AppError> {
    let _principal = require_manage_server(&headers).await?;
    Ok(Json(settings_response()?))
}

async fn update_server_settings(
    headers: HeaderMap,
    Json(request): Json<UpdateServerSettingsRequest>,
) -> Result<Json<ServerSettingsResponse>, AppError> {
    require_manage_server_session(&headers).await?;
    let boot = STATE.boot.get();
    {
        let mut db = STATE.db.write().await;
        // Read under the write lock so lock flags reflect the latest republish.
        let current = STATE.settings.get();
        let changes = server_settings::validate_updates(&current, &boot, &request.values)
            .map_err(map_update_error)?;
        server_settings::apply_updates(&mut db, &changes)?;
        server_settings::republish(&mut db)?;
    }
    server_settings::apply_live().await;
    Ok(Json(settings_response()?))
}

async fn delete_server_settings(
    headers: HeaderMap,
) -> Result<Json<ServerSettingsResponse>, AppError> {
    require_manage_server_session(&headers).await?;
    {
        let mut db = STATE.db.write().await;
        server_settings::reset_stored(&mut db)?;
        server_settings::republish(&mut db)?;
    }
    server_settings::apply_live().await;
    Ok(Json(settings_response()?))
}

#[cfg(feature = "docgen")]
fn get_server_info_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Get public server info").description(
        "Returns server identity, setup status, and whether authentication is enabled.",
    )
}

#[cfg(feature = "docgen")]
fn update_setup_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Update server setup")
        .description(
            "Records or clears the plugin-selection skip. Requires manage_plugins permission.",
        )
        .response::<204, ()>()
}

#[cfg(feature = "docgen")]
fn get_server_settings_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Get server settings").description(
        "Returns every server setting grouped for display with its effective value, default, source, lock state, and whether a change needs a restart, plus the restart-required settings changed since startup and the boot values in use.",
    )
}

#[cfg(feature = "docgen")]
fn update_server_settings_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Update server settings").description(
        "Stores the given values (`null` clears one back to its default) and returns the updated settings. Requires a session credential. Undeclared keys and invalid values are rejected with 400. A request touching any key set in `config.json` is rejected with 409 listing the locked keys, and nothing is written.",
    )
}

#[cfg(feature = "docgen")]
fn delete_server_settings_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Reset server settings").description(
        "Clears every stored server setting so file values and defaults apply again, and returns the updated settings. Requires a session credential.",
    )
}

pub fn server_routes() -> Router {
    Router::new()
        .route("/public", get(get_server_info))
        .route("/setup", patch(update_setup))
        .route(
            "/settings",
            get(get_server_settings)
                .patch(update_server_settings)
                .delete(delete_server_settings),
        )
}

#[cfg(feature = "docgen")]
pub(crate) fn server_openapi_routes() -> aide::axum::ApiRouter {
    use aide::axum::routing::{
        delete_with,
        get_with,
        patch_with,
    };

    aide::axum::ApiRouter::new()
        .api_route("/public", get_with(get_server_info, get_server_info_docs))
        .api_route("/setup", patch_with(update_setup, update_setup_docs))
        .api_route(
            "/settings",
            get_with(get_server_settings, get_server_settings_docs),
        )
        .api_route(
            "/settings",
            patch_with(update_server_settings, update_server_settings_docs),
        )
        .api_route(
            "/settings",
            delete_with(delete_server_settings, delete_server_settings_docs),
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        db::{
            roles::Role,
            users::User,
        },
        services::auth::{
            api_keys,
            sessions,
        },
        testing::runtime_test_lock,
    };
    use axum::{
        body::to_bytes,
        http::{
            StatusCode,
            header::AUTHORIZATION,
        },
        response::IntoResponse,
    };
    use nanoid::nanoid;
    use serde_json::{
        Value,
        json,
    };

    async fn initialize_test_state(file: &[(&str, Value)]) -> anyhow::Result<()> {
        crate::testing::init_test_state_with_file_settings(
            file.iter()
                .map(|(key, value)| ((*key).to_string(), value.clone()))
                .collect(),
        )?;
        {
            let mut db = STATE.db.write().await;
            db::server::ensure(&mut db)?;
        }
        // A file that disables auth resolves every request to the default
        // user, which the server creates at startup.
        crate::services::auth::ensure_default_user(&STATE.config()).await
    }

    async fn user_with(permissions: Vec<Permission>) -> anyhow::Result<agdb::DbId> {
        let mut db = STATE.db.write().await;
        db::roles::ensure_builtin_roles(&mut db)?;
        let user_db_id = db::users::create(
            &mut db,
            &User {
                db_id: None,
                id: nanoid!(),
                username: format!("server-route-test-{}", nanoid!()),
                password: "unused".to_string(),
            },
        )?;
        let role_name = format!("server-route-test-{}", nanoid!());
        db::roles::create(
            &mut db,
            &Role {
                db_id: None,
                id: nanoid!(),
                name: role_name.clone(),
                permissions,
            },
        )?;
        db::roles::ensure_user_has_role(&mut db, user_db_id, &role_name)?;
        Ok(user_db_id)
    }

    fn bearer_headers(token: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            format!("Bearer {token}")
                .parse()
                .expect("valid auth header"),
        );
        headers
    }

    async fn headers_with(permissions: Vec<Permission>) -> anyhow::Result<HeaderMap> {
        let user_db_id = user_with(permissions).await?;
        let session = sessions::create_session_for_user(user_db_id, Default::default()).await?;
        Ok(bearer_headers(&session.token))
    }

    async fn manage_server_headers() -> anyhow::Result<HeaderMap> {
        headers_with(vec![Permission::ManageServer]).await
    }

    fn request(values: &[(&str, Value)]) -> Json<UpdateServerSettingsRequest> {
        Json(UpdateServerSettingsRequest {
            values: values
                .iter()
                .map(|(key, value)| ((*key).to_string(), value.clone()))
                .collect(),
        })
    }

    fn field<'a>(body: &'a Value, key: &str) -> &'a Value {
        body["groups"]
            .as_array()
            .expect("groups")
            .iter()
            .flat_map(|group| group["fields"].as_array().expect("fields"))
            .find(|field| field["key"] == key)
            .unwrap_or_else(|| panic!("field {key} missing"))
    }

    fn body(result: Result<Json<ServerSettingsResponse>, AppError>) -> Value {
        match result {
            Ok(Json(response)) => serde_json::to_value(response).expect("serializes"),
            Err(error) => panic!("expected success, got {error:?}"),
        }
    }

    async fn error<T>(result: Result<T, AppError>) -> (StatusCode, String) {
        let Err(error) = result else {
            panic!("expected an error response");
        };
        let response = error.into_response();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        (status, String::from_utf8_lossy(&bytes).into_owned())
    }

    async fn stored_keys() -> anyhow::Result<Vec<String>> {
        let db = STATE.db.read().await;
        let mut keys: Vec<String> = db::settings::server::get_all_with(&db)?
            .into_iter()
            .map(|entry| entry.key)
            .collect();
        keys.sort_unstable();
        Ok(keys)
    }

    #[tokio::test]
    async fn public_setup_tracks_account_creation_and_explicit_skip() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        initialize_test_state(&[]).await.expect("request succeeds");
        let Json(info) = get_server_info().await.expect("request succeeds");
        let value = serde_json::to_value(info)?;
        assert!(value.get("setup_complete").is_none());
        assert_eq!(value["setup"]["account_required"], true);

        let headers = headers_with(vec![Permission::Admin])
            .await
            .expect("request succeeds");
        assert_eq!(
            update_setup(
                headers,
                Json(UpdateSetupRequest {
                    plugin_selection_skipped: true
                })
            )
            .await
            .expect("request succeeds"),
            StatusCode::NO_CONTENT
        );
        let Json(info) = get_server_info().await.expect("request succeeds");
        assert!(!info.setup.account_required);
        assert!(!info.setup.plugin_selection_required);
        Ok(())
    }

    #[tokio::test]
    async fn setup_changes_require_manage_plugins() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        initialize_test_state(&[]).await.expect("request succeeds");
        let request = || {
            Json(UpdateSetupRequest {
                plugin_selection_skipped: true,
            })
        };
        assert_eq!(
            error(update_setup(HeaderMap::new(), request()).await)
                .await
                .0,
            StatusCode::UNAUTHORIZED
        );
        let headers = headers_with(vec![Permission::ManageServer])
            .await
            .expect("request succeeds");
        assert_eq!(
            error(update_setup(headers, request()).await).await.0,
            StatusCode::FORBIDDEN
        );
        assert!(!db::server::plugin_selection_skipped(
            &*STATE.db.read().await
        )?);
        let headers = headers_with(vec![Permission::ManagePlugins])
            .await
            .expect("request succeeds");
        update_setup(headers.clone(), request())
            .await
            .expect("request succeeds");
        assert!(db::server::plugin_selection_skipped(
            &*STATE.db.read().await
        )?);
        update_setup(
            headers,
            Json(UpdateSetupRequest {
                plugin_selection_skipped: false,
            }),
        )
        .await
        .expect("request succeeds");
        assert!(!db::server::plugin_selection_skipped(
            &*STATE.db.read().await
        )?);
        Ok(())
    }

    #[test]
    fn setup_patch_rejects_unknown_or_missing_fields() {
        for request in [
            json!({}),
            json!({"plugin_selection_skipped": true, "account_required": false}),
        ] {
            assert!(serde_json::from_value::<UpdateSetupRequest>(request).is_err());
        }
    }

    #[tokio::test]
    async fn get_reports_shape_and_file_locks() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        initialize_test_state(&[("auth.enabled", json!(false))]).await?;
        let headers = manage_server_headers().await?;

        let body = body(get_server_settings(headers).await);

        let group_ids: Vec<&str> = body["groups"]
            .as_array()
            .expect("groups")
            .iter()
            .map(|group| group["id"].as_str().expect("id"))
            .collect();
        assert_eq!(
            group_ids,
            vec!["server", "cors", "rate_limit", "auth", "sync", "hls"]
        );
        assert_eq!(body["groups"][3]["label"], json!("Authentication"));

        let auth_enabled = field(&body, "auth.enabled");
        assert_eq!(auth_enabled["type"], json!("bool"));
        assert_eq!(auth_enabled["value"], json!(false));
        assert_eq!(auth_enabled["default"], json!(true));
        assert_eq!(auth_enabled["locked"], json!(true));
        assert_eq!(auth_enabled["source"], json!("file"));
        assert_eq!(auth_enabled["required"], json!(true));
        assert_eq!(auth_enabled["restart_required"], json!(false));
        assert_eq!(auth_enabled["label"], json!("Enabled"));

        let login_burst = field(&body, "rate_limit.login_burst");
        assert_eq!(login_burst["type"], json!("number"));
        assert_eq!(login_burst["value"], json!(3));
        assert_eq!(login_burst["min"], json!(0));
        assert_eq!(login_burst["max"], json!(u32::MAX));
        assert_eq!(login_burst["locked"], json!(false));
        assert_eq!(login_burst["source"], json!("default"));
        assert_eq!(login_burst["restart_required"], json!(true));

        let ttl = field(&body, "auth.session_ttl_seconds");
        assert_eq!(ttl["value"], json!(2_592_000));
        assert!(ttl.get("max").is_none());

        let budget = field(&body, "hls.temp_disk_budget_bytes");
        assert_eq!(budget["type"], json!("number"));
        assert_eq!(budget["value"], Value::Null);
        assert_eq!(budget["required"], json!(false));

        let published = field(&body, "published_url");
        assert_eq!(published["type"], json!("string"));
        assert_eq!(published["value"], Value::Null);
        assert_eq!(published["default"], Value::Null);
        assert_eq!(published["required"], json!(false));

        let origins = field(&body, "cors.allowed_origins");
        assert_eq!(origins["type"], json!("string_list"));
        assert_eq!(origins["value"], json!([]));

        assert_eq!(body["pending_restart"], json!([]));
        assert_eq!(body["boot"]["port"], json!(0));
        assert_eq!(body["boot"]["db"]["kind"], json!("memory"));
        assert!(body["boot"]["data_dir"].is_string());
        Ok(())
    }

    #[tokio::test]
    async fn patch_locked_key_returns_conflict_and_writes_nothing() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        initialize_test_state(&[("auth.enabled", json!(false))]).await?;
        let headers = manage_server_headers().await?;

        let (status, message) = error(
            update_server_settings(
                headers,
                request(&[
                    ("auth.enabled", json!(true)),
                    ("sync.interval_secs", json!(5)),
                ]),
            )
            .await,
        )
        .await;

        assert_eq!(status, StatusCode::CONFLICT);
        assert!(message.contains("auth.enabled"), "{message}");
        assert!(stored_keys().await?.is_empty());
        assert_eq!(STATE.config().sync.interval_secs, 0);
        assert!(!STATE.config().auth.enabled);
        Ok(())
    }

    #[tokio::test]
    async fn patch_rejects_undeclared_keys_and_invalid_values() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        initialize_test_state(&[]).await?;
        let headers = manage_server_headers().await?;

        let (status, message) = error(
            update_server_settings(headers.clone(), request(&[("nope.key", json!(1))])).await,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(message.contains("nope.key"), "{message}");

        let (status, message) = error(
            update_server_settings(headers, request(&[("auth.enabled", json!("yes"))])).await,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(message.contains("auth.enabled"), "{message}");
        assert!(message.contains("boolean"), "{message}");

        assert!(stored_keys().await?.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn patch_updates_config_and_stored_entry() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        initialize_test_state(&[]).await?;
        let headers = manage_server_headers().await?;

        let body = body(
            update_server_settings(
                headers,
                request(&[
                    ("sync.interval_secs", json!(5)),
                    ("published_url", json!("http://LOCALHOST:8080/")),
                ]),
            )
            .await,
        );

        let config = STATE.config();
        assert_eq!(config.sync.interval_secs, 5);
        assert_eq!(
            config.published_url.as_deref(),
            Some("http://localhost:8080")
        );
        assert_eq!(
            stored_keys().await?,
            vec!["published_url", "sync.interval_secs"]
        );

        let interval = field(&body, "sync.interval_secs");
        assert_eq!(interval["value"], json!(5));
        assert_eq!(interval["source"], json!("database"));
        assert_eq!(interval["locked"], json!(false));
        assert_eq!(
            field(&body, "published_url")["value"],
            json!("http://localhost:8080")
        );
        Ok(())
    }

    #[tokio::test]
    async fn integers_round_trip_exactly() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        initialize_test_state(&[]).await?;
        let headers = manage_server_headers().await?;

        let first = body(
            update_server_settings(
                headers.clone(),
                request(&[("hls.temp_disk_budget_bytes", json!(u64::MAX))]),
            )
            .await,
        );
        assert_eq!(
            field(&first, "hls.temp_disk_budget_bytes")["value"],
            json!(u64::MAX)
        );
        assert_eq!(STATE.config().hls.temp_disk_budget_bytes, Some(u64::MAX));

        // Feed every integer the GET body reports straight back.
        let echoed: Vec<(String, Value)> = first["groups"]
            .as_array()
            .expect("groups")
            .iter()
            .flat_map(|group| group["fields"].as_array().expect("fields"))
            .filter(|field| field["type"] == "number")
            .map(|field| {
                (
                    field["key"].as_str().expect("key").to_string(),
                    field["value"].clone(),
                )
            })
            .collect();
        let values: HashMap<String, Value> = echoed.into_iter().collect();
        assert!(values.contains_key("auth.session_ttl_seconds"));

        let second = body(
            update_server_settings(
                headers.clone(),
                Json(UpdateServerSettingsRequest { values }),
            )
            .await,
        );
        assert_eq!(
            field(&second, "auth.session_ttl_seconds")["value"],
            json!(2_592_000)
        );
        assert_eq!(
            field(&second, "hls.temp_disk_budget_bytes")["value"],
            json!(u64::MAX)
        );

        let third = body(
            update_server_settings(headers, request(&[("sync.interval_secs", json!(5.0))])).await,
        );
        assert_eq!(field(&third, "sync.interval_secs")["value"], json!(5));
        Ok(())
    }

    #[tokio::test]
    async fn patch_null_clears_back_to_default() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        initialize_test_state(&[]).await?;
        let headers = manage_server_headers().await?;

        body(
            update_server_settings(
                headers.clone(),
                request(&[("sync.interval_secs", json!(5))]),
            )
            .await,
        );
        assert_eq!(STATE.config().sync.interval_secs, 5);

        let body = body(
            update_server_settings(headers, request(&[("sync.interval_secs", Value::Null)])).await,
        );

        assert_eq!(STATE.config().sync.interval_secs, 0);
        assert!(stored_keys().await?.is_empty());
        let interval = field(&body, "sync.interval_secs");
        assert_eq!(interval["value"], json!(0));
        assert_eq!(interval["source"], json!("default"));
        Ok(())
    }

    #[tokio::test]
    async fn delete_resets_stored_values() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        initialize_test_state(&[]).await?;
        let headers = manage_server_headers().await?;

        body(
            update_server_settings(
                headers.clone(),
                request(&[
                    ("sync.interval_secs", json!(5)),
                    ("hls.max_concurrent_transcodes", json!(2)),
                ]),
            )
            .await,
        );
        assert_eq!(stored_keys().await?.len(), 2);

        let body = body(delete_server_settings(headers).await);

        assert!(stored_keys().await?.is_empty());
        let config = STATE.config();
        assert_eq!(config.sync.interval_secs, 0);
        assert_eq!(config.hls.max_concurrent_transcodes, 0);
        assert_eq!(
            field(&body, "hls.max_concurrent_transcodes")["source"],
            json!("default")
        );
        Ok(())
    }

    #[tokio::test]
    async fn pending_restart_tracks_restart_required_changes() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        initialize_test_state(&[]).await?;
        let headers = manage_server_headers().await?;

        let first = body(
            update_server_settings(
                headers.clone(),
                request(&[
                    ("rate_limit.login_burst", json!(9)),
                    ("sync.interval_secs", json!(5)),
                ]),
            )
            .await,
        );
        assert_eq!(first["pending_restart"], json!(["rate_limit.login_burst"]));
        assert_eq!(STATE.config().rate_limit.login_burst, 9);

        let second = body(
            update_server_settings(
                headers.clone(),
                request(&[("rate_limit.login_burst", json!(3))]),
            )
            .await,
        );
        assert_eq!(second["pending_restart"], json!([]));
        assert_eq!(
            field(&second, "rate_limit.login_burst")["source"],
            json!("database")
        );

        let third = body(
            update_server_settings(
                headers,
                request(&[("hls.cleanup_startup_purge", json!(false))]),
            )
            .await,
        );
        assert_eq!(
            third["pending_restart"],
            json!(["hls.cleanup_startup_purge"])
        );
        Ok(())
    }

    #[tokio::test]
    async fn user_without_manage_server_is_forbidden() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        initialize_test_state(&[]).await?;
        let headers =
            headers_with(vec![Permission::ManageUsers, Permission::ManagePlugins]).await?;

        let (status, _) = error(get_server_settings(headers.clone()).await).await;
        assert_eq!(status, StatusCode::FORBIDDEN);

        let (status, _) = error(
            update_server_settings(
                headers.clone(),
                request(&[("sync.interval_secs", json!(5))]),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert!(stored_keys().await?.is_empty());

        let (status, _) = error(delete_server_settings(headers).await).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        Ok(())
    }

    #[tokio::test]
    async fn writes_require_a_session_credential() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        initialize_test_state(&[]).await?;
        let user_db_id = user_with(vec![Permission::ManageServer]).await?;
        let api_key = api_keys::create_api_key(user_db_id, "automation").await?;
        let headers = bearer_headers(&api_key.key);

        body(get_server_settings(headers.clone()).await);

        let (status, message) = error(
            update_server_settings(headers.clone(), request(&[("auth.enabled", json!(false))]))
                .await,
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert!(message.contains("api key"), "{message}");
        assert!(STATE.config().auth.enabled);

        let (status, _) = error(delete_server_settings(headers).await).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        Ok(())
    }

    #[tokio::test]
    async fn public_info_reports_auth_enabled() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        initialize_test_state(&[("auth.enabled", json!(false))]).await?;

        let info = match get_server_info().await {
            Ok(Json(info)) => info,
            Err(error) => panic!("server info should resolve, got {error:?}"),
        };

        assert!(!info.auth_enabled);
        assert_eq!(info.version, env!("CARGO_PKG_VERSION"));
        Ok(())
    }
}
