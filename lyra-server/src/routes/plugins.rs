// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashMap;

use aide::axum::{
    ApiRouter,
    routing::{
        delete_with,
        get_with,
        patch_with,
        post_with,
    },
};
use aide::transform::TransformOperation;
use axum::{
    Json,
    extract::Path,
    http::{
        HeaderMap,
        StatusCode,
    },
};
use schemars::JsonSchema;
use serde::{
    Deserialize,
    Serialize,
};

use crate::{
    STATE,
    plugins::{
        lifecycle::{
            PluginId,
            PluginRestartError,
        },
        runtime::{
            self,
            FieldDefinition,
            FieldGroupDefinition,
            FieldProps,
            Schema,
            SettingsScope,
        },
    },
    routes::AppError,
    services::{
        auth::{
            require_authenticated,
            require_manage_plugins,
        },
        plugin_settings as settings,
    },
};

fn map_settings_state_error(error: anyhow::Error) -> AppError {
    if error
        .downcast_ref::<settings::InvalidStoredSettings>()
        .is_some()
    {
        AppError::conflict(format!("{error:#}"))
    } else {
        error.into()
    }
}

fn map_plugin_restart_error(error: PluginRestartError) -> AppError {
    match &error {
        PluginRestartError::NotFound(plugin_id) => {
            AppError::not_found(format!("plugin not found: {plugin_id}"))
        }
        PluginRestartError::Failed { .. } => AppError::conflict(error.to_string()),
    }
}

#[derive(Serialize, JsonSchema)]
struct PluginManifestResponse {
    schema_version: u32,
    id: String,
    name: String,
    version: String,
    description: String,
    entrypoint: String,
}

#[derive(Serialize, JsonSchema)]
struct ChoiceOptionResponse {
    value: String,
    label: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
}

#[derive(Serialize, JsonSchema)]
struct FieldPropsResponse {
    label: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    required: bool,
}

#[derive(Serialize, JsonSchema)]
#[serde(tag = "type")]
enum FieldResponse {
    #[serde(rename = "string")]
    String {
        key: String,
        #[serde(flatten)]
        props: FieldPropsResponse,
        value: Option<String>,
    },
    #[serde(rename = "number")]
    Number {
        key: String,
        #[serde(flatten)]
        props: FieldPropsResponse,
        #[serde(skip_serializing_if = "Option::is_none")]
        min: Option<f64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        max: Option<f64>,
        value: Option<f64>,
    },
    #[serde(rename = "bool")]
    Bool {
        key: String,
        #[serde(flatten)]
        props: FieldPropsResponse,
        value: Option<bool>,
    },
    #[serde(rename = "choice")]
    Choice {
        key: String,
        #[serde(flatten)]
        props: FieldPropsResponse,
        options: Vec<ChoiceOptionResponse>,
        value: Option<String>,
    },
}

#[derive(Serialize, JsonSchema)]
struct GroupResponse {
    id: String,
    label: String,
    fields: Vec<FieldResponse>,
}

#[derive(Serialize, JsonSchema)]
struct PluginSettingsResponse {
    plugin_id: String,
    groups: Vec<GroupResponse>,
}

#[derive(Deserialize, JsonSchema)]
struct UpdateSettingsRequest {
    values: HashMap<String, serde_json::Value>,
}

fn manifest_responses(manifests: &[harmony_core::PluginManifest]) -> Vec<PluginManifestResponse> {
    manifests
        .iter()
        .map(|manifest| PluginManifestResponse {
            schema_version: manifest.schema_version,
            id: manifest.id.clone(),
            name: manifest.name.clone(),
            version: manifest.version.clone(),
            description: manifest.description.clone(),
            entrypoint: manifest.entrypoint.clone(),
        })
        .collect()
}

fn props_to_response(props: &FieldProps) -> FieldPropsResponse {
    FieldPropsResponse {
        label: props.label.clone(),
        description: props.description.clone(),
        required: props.required,
    }
}

fn resolve_string(
    stored: &HashMap<String, serde_json::Value>,
    key: &str,
    default: &Option<serde_json::Value>,
) -> Option<String> {
    stored
        .get(key)
        .and_then(|v| v.as_str().map(String::from))
        .or_else(|| default.as_ref().and_then(|v| v.as_str().map(String::from)))
}

fn resolve_number(
    stored: &HashMap<String, serde_json::Value>,
    key: &str,
    default: &Option<serde_json::Value>,
) -> Option<f64> {
    stored
        .get(key)
        .and_then(|v| v.as_f64())
        .or_else(|| default.as_ref().and_then(|v| v.as_f64()))
}

fn resolve_bool(
    stored: &HashMap<String, serde_json::Value>,
    key: &str,
    default: &Option<serde_json::Value>,
) -> Option<bool> {
    stored
        .get(key)
        .and_then(|v| v.as_bool())
        .or_else(|| default.as_ref().and_then(|v| v.as_bool()))
}

fn field_to_response(
    field: &FieldDefinition,
    stored: &HashMap<String, serde_json::Value>,
) -> anyhow::Result<FieldResponse> {
    let key = field.key();
    if let Some(value) = stored.get(key) {
        field.validate_value(value)?;
    }

    match field {
        FieldDefinition::String { key, props } => Ok(FieldResponse::String {
            key: key.clone(),
            value: resolve_string(stored, key, &props.default_value),
            props: props_to_response(props),
        }),
        FieldDefinition::Number {
            key,
            props,
            min,
            max,
        } => Ok(FieldResponse::Number {
            key: key.clone(),
            value: resolve_number(stored, key, &props.default_value),
            props: props_to_response(props),
            min: *min,
            max: *max,
        }),
        FieldDefinition::Bool { key, props } => Ok(FieldResponse::Bool {
            key: key.clone(),
            value: resolve_bool(stored, key, &props.default_value),
            props: props_to_response(props),
        }),
        FieldDefinition::Choice {
            key,
            props,
            options,
        } => Ok(FieldResponse::Choice {
            key: key.clone(),
            value: resolve_string(stored, key, &props.default_value),
            props: props_to_response(props),
            options: options
                .iter()
                .map(|o| ChoiceOptionResponse {
                    value: o.value.clone(),
                    label: o.label.clone(),
                    description: o.description.clone(),
                })
                .collect(),
        }),
    }
}

fn group_to_response(
    group: &FieldGroupDefinition,
    stored: &HashMap<String, serde_json::Value>,
) -> anyhow::Result<GroupResponse> {
    let fields = group
        .fields
        .iter()
        .map(|field| field_to_response(field, stored))
        .collect::<anyhow::Result<_>>()?;

    Ok(GroupResponse {
        id: group.id.clone(),
        label: group.label.clone(),
        fields,
    })
}

async fn load_registered_schema(plugin_id: &str, scope: SettingsScope) -> Result<Schema, AppError> {
    let registry = runtime::REGISTRY.read().await;
    let typed_id = crate::plugins::lifecycle::PluginId::new(plugin_id.to_string())
        .map_err(|_| AppError::not_found(format!("plugin not found: {plugin_id}")))?;
    if !registry.is_frozen_for_plugin(&typed_id) && registry.get_schema(plugin_id, scope).is_none()
    {
        return Err(AppError::service_unavailable(
            "plugin settings are still initializing",
        ));
    }
    registry
        .get_schema(plugin_id, scope)
        .cloned()
        .ok_or_else(|| AppError::not_found(format!("plugin not found: {plugin_id}")))
}

async fn load_settings_response(
    plugin_id: String,
    schema: Schema,
) -> Result<Json<PluginSettingsResponse>, AppError> {
    let stored =
        settings::load_validated_stored_values(&*STATE.db.read().await, &plugin_id, &schema)
            .map_err(map_settings_state_error)?;
    let groups = schema
        .groups
        .iter()
        .map(|group| group_to_response(group, &stored))
        .collect::<anyhow::Result<_>>()?;

    Ok(Json(PluginSettingsResponse { plugin_id, groups }))
}

async fn list_plugins(headers: HeaderMap) -> Result<Json<Vec<PluginManifestResponse>>, AppError> {
    let _principal = require_manage_plugins(&headers).await?;
    let manifests = STATE.plugin_manifests.get();
    Ok(Json(manifest_responses(manifests.as_ref())))
}

async fn restart_plugin(
    headers: HeaderMap,
    Path(plugin_id): Path<String>,
) -> Result<StatusCode, AppError> {
    let _principal = require_manage_plugins(&headers).await?;
    let plugin_id = PluginId::new(plugin_id)
        .map_err(|err| AppError::bad_request(format!("invalid plugin id: {err}")))?;
    let harmony = STATE
        .plugin_runtime
        .get()
        .ok_or_else(|| AppError::service_unavailable("plugin runtime is not ready"))?;

    STATE
        .plugin_registries
        .restart_plugin(&plugin_id, harmony)
        .await
        .map_err(map_plugin_restart_error)?;

    Ok(StatusCode::NO_CONTENT)
}

async fn get_settings(
    headers: HeaderMap,
    Path(plugin_id): Path<String>,
) -> Result<Json<PluginSettingsResponse>, AppError> {
    let _principal = require_manage_plugins(&headers).await?;
    let schema = load_registered_schema(&plugin_id, SettingsScope::Global).await?;
    load_settings_response(plugin_id, schema).await
}

async fn update_settings(
    headers: HeaderMap,
    Path(plugin_id): Path<String>,
    Json(request): Json<UpdateSettingsRequest>,
) -> Result<Json<PluginSettingsResponse>, AppError> {
    let _principal = require_manage_plugins(&headers).await?;
    let schema = load_registered_schema(&plugin_id, SettingsScope::Global).await?;
    let changes = settings::validate_updates(&schema, &request.values)
        .map_err(|error| AppError::bad_request(error.to_string()))?;
    settings::apply_updates(&mut *STATE.db.write().await, &plugin_id, &schema, &changes)
        .map_err(map_settings_state_error)?;

    load_settings_response(plugin_id, schema).await
}

async fn delete_settings(
    headers: HeaderMap,
    Path(plugin_id): Path<String>,
) -> Result<(), AppError> {
    let _principal = require_manage_plugins(&headers).await?;
    settings::clear_stored_values(&mut *STATE.db.write().await, &plugin_id)?;
    Ok(())
}

async fn load_user_settings_response(
    plugin_id: String,
    user_db_id: agdb::DbId,
    schema: Schema,
) -> Result<Json<PluginSettingsResponse>, AppError> {
    let stored = settings::load_validated_user_stored_values(
        &*STATE.db.read().await,
        user_db_id,
        &plugin_id,
        &schema,
    )
    .map_err(map_settings_state_error)?;
    let groups = schema
        .groups
        .iter()
        .map(|group| group_to_response(group, &stored))
        .collect::<anyhow::Result<_>>()?;

    Ok(Json(PluginSettingsResponse { plugin_id, groups }))
}

async fn get_user_settings(
    headers: HeaderMap,
    Path(plugin_id): Path<String>,
) -> Result<Json<PluginSettingsResponse>, AppError> {
    let principal = require_authenticated(&headers).await?;
    let schema = load_registered_schema(&plugin_id, SettingsScope::User).await?;
    load_user_settings_response(plugin_id, principal.user_db_id, schema).await
}

async fn update_user_settings(
    headers: HeaderMap,
    Path(plugin_id): Path<String>,
    Json(request): Json<UpdateSettingsRequest>,
) -> Result<Json<PluginSettingsResponse>, AppError> {
    let principal = require_authenticated(&headers).await?;
    let schema = load_registered_schema(&plugin_id, SettingsScope::User).await?;
    let changes = settings::validate_updates(&schema, &request.values)
        .map_err(|error| AppError::bad_request(error.to_string()))?;
    settings::apply_user_updates(
        &mut *STATE.db.write().await,
        principal.user_db_id,
        &plugin_id,
        &schema,
        &changes,
    )
    .map_err(map_settings_state_error)?;

    load_user_settings_response(plugin_id, principal.user_db_id, schema).await
}

async fn delete_user_settings(
    headers: HeaderMap,
    Path(plugin_id): Path<String>,
) -> Result<(), AppError> {
    let principal = require_authenticated(&headers).await?;
    settings::clear_user_stored_values(
        &mut *STATE.db.write().await,
        principal.user_db_id,
        &plugin_id,
    )?;
    Ok(())
}

fn get_settings_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Get plugin settings")
        .description("Returns the settings schema and current values for a plugin.")
}

fn list_plugins_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List plugins")
        .description("Returns the loaded plugin manifests discovered at startup.")
}

fn restart_plugin_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Restart plugin").description(
        "Tears down the plugin's current runtime registrations, re-runs its entrypoint, and activates its routes.",
    ).response::<204, ()>()
}

fn update_settings_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Update plugin settings")
        .description("Updates setting values for a plugin and returns the updated schema.")
}

fn delete_settings_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Delete plugin settings")
        .description("Deletes all stored settings for a plugin. Use this to clear stale plugin settings after a schema change.")
}

fn get_user_settings_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Get user plugin settings").description(
        "Returns the user-scoped settings schema and the authenticated user's current values.",
    )
}

fn update_user_settings_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Update user plugin settings")
        .description("Updates user-scoped setting values for the authenticated user.")
}

fn delete_user_settings_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Delete user plugin settings")
        .description("Deletes all user-scoped settings for the authenticated user.")
}

pub fn plugin_routes() -> ApiRouter {
    ApiRouter::new()
        .api_route("/", get_with(list_plugins, list_plugins_docs))
        .api_route(
            "/{plugin_id}/restart",
            post_with(restart_plugin, restart_plugin_docs),
        )
        .api_route(
            "/{plugin_id}/settings",
            get_with(get_settings, get_settings_docs),
        )
        .api_route(
            "/{plugin_id}/settings",
            patch_with(update_settings, update_settings_docs),
        )
        .api_route(
            "/{plugin_id}/settings",
            delete_with(delete_settings, delete_settings_docs),
        )
}

pub(super) fn me_plugin_settings_routes() -> ApiRouter {
    ApiRouter::new()
        .api_route(
            "/{plugin_id}/settings",
            get_with(get_user_settings, get_user_settings_docs),
        )
        .api_route(
            "/{plugin_id}/settings",
            patch_with(update_user_settings, update_user_settings_docs),
        )
        .api_route(
            "/{plugin_id}/settings",
            delete_with(delete_user_settings, delete_user_settings_docs),
        )
}

#[cfg(test)]
mod tests {
    use std::{
        path::PathBuf,
        sync::LazyLock,
        time::{
            SystemTime,
            UNIX_EPOCH,
        },
    };

    use super::*;
    use crate::{
        db::{
            self,
            Permission,
            roles::Role,
            users::User,
        },
        services,
        testing::{
            LibraryFixtureConfig,
            initialize_runtime,
            runtime_test_lock,
        },
    };
    use axum::{
        http::{
            HeaderMap,
            StatusCode,
        },
        response::IntoResponse,
    };
    use nanoid::nanoid;
    use tokio::sync::Mutex;

    static REGISTRY_TEST_GUARD: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    fn props(required: bool) -> FieldProps {
        FieldProps {
            label: "Label".to_string(),
            description: None,
            required,
            default_value: None,
        }
    }

    fn group(id: &str, fields: Vec<FieldDefinition>) -> FieldGroupDefinition {
        FieldGroupDefinition {
            id: id.to_string(),
            label: format!("{id} label"),
            fields,
        }
    }

    async fn initialize_auth_test_runtime() -> anyhow::Result<PathBuf> {
        let test_dir = std::env::temp_dir().join(format!(
            "lyra-plugin-routes-test-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ));
        std::fs::create_dir_all(&test_dir)?;
        initialize_runtime(&LibraryFixtureConfig {
            directory: test_dir.clone(),
            language: None,
            country: None,
        })
        .await?;
        Ok(test_dir)
    }

    async fn manage_plugins_headers() -> anyhow::Result<HeaderMap> {
        let user_db_id = {
            let mut db = STATE.db.write().await;
            db::roles::ensure_builtin_roles(&mut db)?;
            let user = User {
                db_id: None,
                id: nanoid!(),
                username: format!("plugin-route-test-{}", nanoid!()),
                password: "unused".to_string(),
            };
            let user_db_id = db::users::create(&mut db, &user)?;
            let role_name = format!("plugin-route-test-{}", nanoid!());
            db::roles::create(
                &mut db,
                &Role {
                    db_id: None,
                    id: nanoid!(),
                    name: role_name.clone(),
                    permissions: vec![Permission::ManagePlugins],
                },
            )?;
            db::roles::ensure_user_has_role(&mut db, user_db_id, &role_name)?;
            user_db_id
        };

        let session = services::auth::sessions::create_session_for_user(user_db_id).await?;
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            format!("Bearer {}", session.token)
                .parse()
                .expect("valid auth header"),
        );
        Ok(headers)
    }

    #[test]
    fn field_to_response_rejects_invalid_stored_value_types() {
        let field = FieldDefinition::Number {
            key: "volume".to_string(),
            props: props(false),
            min: Some(0.0),
            max: Some(10.0),
        };
        let stored = HashMap::from([(
            "volume".to_string(),
            serde_json::Value::String("loud".to_string()),
        )]);

        let error = field_to_response(&field, &stored)
            .err()
            .expect("invalid stored value should be rejected");
        assert!(error.to_string().contains("must be a number"));
    }

    #[test]
    fn field_to_response_keeps_missing_bool_as_null() -> anyhow::Result<()> {
        let field = FieldDefinition::Bool {
            key: "enabled".to_string(),
            props: props(false),
        };
        let stored = HashMap::new();

        let response = field_to_response(&field, &stored)?;
        match response {
            FieldResponse::Bool { value, .. } => assert!(value.is_none()),
            _ => panic!("expected bool field"),
        }

        Ok(())
    }

    #[test]
    fn group_to_response_preserves_group_metadata_and_field_order() -> anyhow::Result<()> {
        let response = group_to_response(
            &group(
                "credentials",
                vec![
                    FieldDefinition::String {
                        key: "token".to_string(),
                        props: props(true),
                    },
                    FieldDefinition::Bool {
                        key: "enabled".to_string(),
                        props: props(false),
                    },
                ],
            ),
            &HashMap::from([
                ("token".to_string(), serde_json::json!("abc")),
                ("enabled".to_string(), serde_json::json!(true)),
            ]),
        )?;

        assert_eq!(response.id, "credentials");
        assert_eq!(response.label, "credentials label");
        assert_eq!(response.fields.len(), 2);
        assert!(matches!(response.fields[0], FieldResponse::String { .. }));
        assert!(matches!(response.fields[1], FieldResponse::Bool { .. }));

        Ok(())
    }

    #[test]
    fn restart_error_maps_missing_plugin_to_not_found() -> anyhow::Result<()> {
        let plugin_id = PluginId::new("demo")?;
        let response =
            map_plugin_restart_error(PluginRestartError::NotFound(plugin_id)).into_response();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        Ok(())
    }

    #[test]
    fn restart_error_maps_failed_restart_to_conflict() -> anyhow::Result<()> {
        let plugin_id = PluginId::new("demo")?;
        let response = map_plugin_restart_error(PluginRestartError::Failed {
            plugin_id,
            source: anyhow::anyhow!("boom"),
        })
        .into_response();

        assert_eq!(response.status(), StatusCode::CONFLICT);
        Ok(())
    }

    #[tokio::test]
    async fn restart_plugin_rejects_invalid_plugin_id_after_auth() -> anyhow::Result<()> {
        let _registry_guard = REGISTRY_TEST_GUARD.lock().await;
        let _runtime_guard = runtime_test_lock().await;
        let _test_dir = initialize_auth_test_runtime().await?;
        let headers = manage_plugins_headers().await?;

        let response = restart_plugin(headers, Path("bad id".to_string()))
            .await
            .expect_err("invalid plugin id should be rejected")
            .into_response();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        Ok(())
    }

    #[tokio::test]
    async fn restart_plugin_returns_service_unavailable_until_runtime_ready() -> anyhow::Result<()>
    {
        let _registry_guard = REGISTRY_TEST_GUARD.lock().await;
        let _runtime_guard = runtime_test_lock().await;
        let _test_dir = initialize_auth_test_runtime().await?;
        let headers = manage_plugins_headers().await?;

        let response = restart_plugin(headers, Path("demo".to_string()))
            .await
            .expect_err("missing Harmony runtime should return 503")
            .into_response();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        Ok(())
    }

    #[tokio::test]
    async fn load_registered_schema_returns_service_unavailable_while_registry_populates() {
        let _guard = REGISTRY_TEST_GUARD.lock().await;
        runtime::initialize_registry().await;

        let response = load_registered_schema("demo", SettingsScope::Global)
            .await
            .expect_err("missing schema should report initializing while registry is mutable")
            .into_response();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn load_registered_schema_returns_not_found_after_registry_freezes() {
        let _guard = REGISTRY_TEST_GUARD.lock().await;
        runtime::initialize_registry().await;
        runtime::freeze_registry().await;

        let response = load_registered_schema("demo", SettingsScope::Global)
            .await
            .expect_err("missing schema should be a 404 once startup completes")
            .into_response();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
