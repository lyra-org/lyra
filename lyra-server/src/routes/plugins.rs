// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashMap;

use agdb::{
    DbAny,
    DbId,
};
#[cfg(feature = "docgen")]
use aide::transform::TransformOperation;
use axum::{
    Json,
    extract::Path,
    http::{
        HeaderMap,
        StatusCode,
    },
};
use axum::{
    Router,
    routing::{
        delete,
        get,
        patch,
        post,
    },
};
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
        settings::{
            self as plugin_settings_registry,
            FieldDefinition,
            FieldGroupDefinition,
            FieldProps,
            Registry,
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
        plugin_repositories as repositories_service,
        plugin_settings as settings_service,
    },
};

fn map_repository_error(error: repositories_service::PluginRepoError) -> AppError {
    use repositories_service::PluginRepoError;
    match error {
        PluginRepoError::BadRequest(message) => AppError::bad_request(message),
        PluginRepoError::NotFound(message) => AppError::not_found(message),
        PluginRepoError::Conflict(message) => AppError::conflict(message),
        PluginRepoError::Internal(error) => error.into(),
    }
}

fn map_settings_state_error(error: anyhow::Error) -> AppError {
    if error
        .downcast_ref::<settings_service::InvalidStoredSettings>()
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

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct PluginManifestResponse {
    schema_version: u32,
    id: String,
    name: String,
    version: String,
    description: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    entrypoint: Option<String>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct ChoiceOptionResponse {
    value: String,
    label: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct FieldPropsResponse {
    label: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    required: bool,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
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

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct GroupResponse {
    id: String,
    label: String,
    fields: Vec<FieldResponse>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct PluginSettingsResponse {
    plugin_id: String,
    groups: Vec<GroupResponse>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum PluginSettingsEntry {
    Ready {
        plugin_id: String,
        groups: Vec<GroupResponse>,
    },
    Initializing {
        plugin_id: String,
    },
    NotDeclared {
        plugin_id: String,
    },
    Invalid {
        plugin_id: String,
        message: String,
    },
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct PluginSettingsListResponse {
    entries: Vec<PluginSettingsEntry>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct UpdateSettingsRequest {
    values: HashMap<String, serde_json::Value>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct RepositoryUrlRequest {
    url: String,
    #[serde(default, rename = "ref")]
    git_ref: Option<String>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct InstallPluginsRequest {
    url: String,
    #[serde(default, rename = "ref")]
    git_ref: Option<String>,
    /// Plugin ids to install; omitted installs everything the
    /// repository provides.
    #[serde(default)]
    plugins: Option<Vec<String>>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct PluginPreviewResponse {
    id: String,
    name: String,
    version: String,
    description: String,
    /// Capability scopes the plugin will be granted when installed.
    scopes: Vec<String>,
    origin: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    subpath: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    commit: Option<String>,
    installed: bool,
    managed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    update_available: Option<bool>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct RepositoryPreviewResponse {
    origin: String,
    #[serde(rename = "ref")]
    git_ref: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    commit: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    plugins: Vec<PluginPreviewResponse>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct InstalledPluginResponse {
    id: String,
    version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    commit: Option<String>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct FailedInstallResponse {
    id: String,
    error: String,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct InstallPluginsResponse {
    installed: Vec<InstalledPluginResponse>,
    failed: Vec<FailedInstallResponse>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum UpdatePluginResponse {
    Updated {
        #[serde(skip_serializing_if = "Option::is_none")]
        commit: Option<String>,
    },
    UpToDate,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct PluginRepositoryResponse {
    id: String,
    origin: String,
    name: String,
    description: String,
    #[serde(rename = "ref", skip_serializing_if = "Option::is_none")]
    git_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_commit: Option<String>,
    refreshed_at_ms: u64,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct PluginRepositoriesResponse {
    repositories: Vec<PluginRepositoryResponse>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct RepositoryWithPreviewResponse {
    repository: PluginRepositoryResponse,
    preview: RepositoryPreviewResponse,
}

fn preview_response(preview: repositories_service::RepositoryPreview) -> RepositoryPreviewResponse {
    RepositoryPreviewResponse {
        origin: preview.origin,
        git_ref: preview.git_ref,
        commit: preview.commit,
        name: preview.name,
        description: preview.description,
        plugins: preview
            .plugins
            .into_iter()
            .map(|plugin| PluginPreviewResponse {
                id: plugin.id,
                name: plugin.name,
                version: plugin.version,
                description: plugin.description,
                scopes: plugin.scopes,
                origin: plugin.origin,
                subpath: plugin.subpath,
                commit: plugin.commit,
                installed: plugin.installed,
                managed: plugin.managed,
                update_available: plugin.update_available,
            })
            .collect(),
    }
}

fn repository_response(
    record: crate::db::plugin_repositories::PluginRepository,
) -> PluginRepositoryResponse {
    PluginRepositoryResponse {
        id: record.id,
        origin: record.origin,
        name: record.name,
        description: record.description,
        git_ref: record.git_ref,
        last_commit: record.last_commit,
        refreshed_at_ms: record.refreshed_at_ms,
    }
}

fn manifest_responses(
    manifests: &[harmony_core::plugin::PluginManifest],
) -> Vec<PluginManifestResponse> {
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
    let registry = plugin_settings_registry::REGISTRY.read().await;
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
    let stored = settings_service::load_validated_stored_values(
        &*STATE.db.read().await,
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

fn build_entry(
    registry: &Registry,
    db: &DbAny,
    plugin_id: &str,
    scope: SettingsScope,
    user_db_id: Option<DbId>,
) -> PluginSettingsEntry {
    let typed = match PluginId::new(plugin_id.to_string()) {
        Ok(id) => id,
        Err(_) => {
            return PluginSettingsEntry::NotDeclared {
                plugin_id: plugin_id.to_string(),
            };
        }
    };

    let schema = match registry.get_schema(plugin_id, scope) {
        Some(schema) => schema,
        None if !registry.is_frozen_for_plugin(&typed) => {
            return PluginSettingsEntry::Initializing {
                plugin_id: plugin_id.to_string(),
            };
        }
        None => {
            return PluginSettingsEntry::NotDeclared {
                plugin_id: plugin_id.to_string(),
            };
        }
    };

    let stored = match user_db_id {
        Some(user_db_id) => {
            settings_service::load_validated_user_stored_values(db, user_db_id, plugin_id, schema)
        }
        None => settings_service::load_validated_stored_values(db, plugin_id, schema),
    };
    let stored = match stored {
        Ok(values) => values,
        Err(error) => {
            return PluginSettingsEntry::Invalid {
                plugin_id: plugin_id.to_string(),
                message: format!("{error:#}"),
            };
        }
    };

    match schema
        .groups
        .iter()
        .map(|group| group_to_response(group, &stored))
        .collect::<anyhow::Result<Vec<_>>>()
    {
        Ok(groups) => PluginSettingsEntry::Ready {
            plugin_id: plugin_id.to_string(),
            groups,
        },
        Err(error) => PluginSettingsEntry::Invalid {
            plugin_id: plugin_id.to_string(),
            message: format!("{error:#}"),
        },
    }
}

async fn list_plugins(headers: HeaderMap) -> Result<Json<Vec<PluginManifestResponse>>, AppError> {
    let _principal = require_manage_plugins(&headers).await?;
    let manifests = STATE.plugin_manifests.get();
    Ok(Json(manifest_responses(manifests.as_ref())))
}

async fn collect_settings_entries(
    scope: SettingsScope,
    user_db_id: Option<DbId>,
) -> PluginSettingsListResponse {
    let manifests = STATE.plugin_manifests.get();
    let registry = plugin_settings_registry::REGISTRY.read().await;
    let db = STATE.db.read().await;

    let entries = manifests
        .as_ref()
        .iter()
        .map(|manifest| build_entry(&registry, &db, &manifest.id, scope, user_db_id))
        .collect();

    PluginSettingsListResponse { entries }
}

async fn list_all_settings(
    headers: HeaderMap,
) -> Result<Json<PluginSettingsListResponse>, AppError> {
    let _principal = require_manage_plugins(&headers).await?;
    Ok(Json(
        collect_settings_entries(SettingsScope::Global, None).await,
    ))
}

async fn list_all_user_settings(
    headers: HeaderMap,
) -> Result<Json<PluginSettingsListResponse>, AppError> {
    let principal = require_authenticated(&headers).await?;
    Ok(Json(
        collect_settings_entries(SettingsScope::User, Some(principal.user_db_id)).await,
    ))
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

async fn resolve_repository(
    headers: HeaderMap,
    Json(request): Json<RepositoryUrlRequest>,
) -> Result<Json<RepositoryPreviewResponse>, AppError> {
    let _principal = require_manage_plugins(&headers).await?;
    let preview = repositories_service::resolve_preview(&request.url, request.git_ref.as_deref())
        .await
        .map_err(map_repository_error)?;
    Ok(Json(preview_response(preview)))
}

async fn install_plugins(
    headers: HeaderMap,
    Json(request): Json<InstallPluginsRequest>,
) -> Result<Json<InstallPluginsResponse>, AppError> {
    let _principal = require_manage_plugins(&headers).await?;
    let report = repositories_service::install(
        &request.url,
        request.git_ref.as_deref(),
        request.plugins.as_deref(),
    )
    .await
    .map_err(map_repository_error)?;

    Ok(Json(InstallPluginsResponse {
        installed: report
            .installed
            .into_iter()
            .map(|plugin| InstalledPluginResponse {
                id: plugin.id,
                version: plugin.version,
                commit: plugin.commit,
            })
            .collect(),
        failed: report
            .failed
            .into_iter()
            .map(|failure| FailedInstallResponse {
                id: failure.id,
                error: failure.error,
            })
            .collect(),
    }))
}

async fn update_installed_plugin(
    headers: HeaderMap,
    Path(plugin_id): Path<String>,
) -> Result<Json<UpdatePluginResponse>, AppError> {
    let _principal = require_manage_plugins(&headers).await?;
    let outcome = repositories_service::update_plugin(&plugin_id)
        .await
        .map_err(map_repository_error)?;
    Ok(Json(match outcome {
        repositories_service::UpdateOutcome::Updated { commit } => {
            UpdatePluginResponse::Updated { commit }
        }
        repositories_service::UpdateOutcome::UpToDate => UpdatePluginResponse::UpToDate,
    }))
}

async fn uninstall_installed_plugin(
    headers: HeaderMap,
    Path(plugin_id): Path<String>,
) -> Result<StatusCode, AppError> {
    let _principal = require_manage_plugins(&headers).await?;
    repositories_service::uninstall(&plugin_id)
        .await
        .map_err(map_repository_error)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn list_plugin_repositories(
    headers: HeaderMap,
) -> Result<Json<PluginRepositoriesResponse>, AppError> {
    let _principal = require_manage_plugins(&headers).await?;
    let repositories = repositories_service::list_repositories()
        .await
        .map_err(map_repository_error)?;
    Ok(Json(PluginRepositoriesResponse {
        repositories: repositories.into_iter().map(repository_response).collect(),
    }))
}

async fn add_plugin_repository(
    headers: HeaderMap,
    Json(request): Json<RepositoryUrlRequest>,
) -> Result<Json<RepositoryWithPreviewResponse>, AppError> {
    let _principal = require_manage_plugins(&headers).await?;
    let (record, preview) =
        repositories_service::add_repository(&request.url, request.git_ref.as_deref())
            .await
            .map_err(map_repository_error)?;
    Ok(Json(RepositoryWithPreviewResponse {
        repository: repository_response(record),
        preview: preview_response(preview),
    }))
}

async fn refresh_plugin_repository(
    headers: HeaderMap,
    Path(repository_id): Path<String>,
) -> Result<Json<RepositoryWithPreviewResponse>, AppError> {
    let _principal = require_manage_plugins(&headers).await?;
    let (record, preview) = repositories_service::refresh_repository(&repository_id)
        .await
        .map_err(map_repository_error)?;
    Ok(Json(RepositoryWithPreviewResponse {
        repository: repository_response(record),
        preview: preview_response(preview),
    }))
}

async fn delete_plugin_repository(
    headers: HeaderMap,
    Path(repository_id): Path<String>,
) -> Result<StatusCode, AppError> {
    let _principal = require_manage_plugins(&headers).await?;
    repositories_service::remove_repository(&repository_id)
        .await
        .map_err(map_repository_error)?;
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
    let changes = settings_service::validate_updates(&schema, &request.values)
        .map_err(|error| AppError::bad_request(error.to_string()))?;
    settings_service::apply_updates(&mut *STATE.db.write().await, &plugin_id, &schema, &changes)
        .map_err(map_settings_state_error)?;

    load_settings_response(plugin_id, schema).await
}

async fn delete_settings(
    headers: HeaderMap,
    Path(plugin_id): Path<String>,
) -> Result<(), AppError> {
    let _principal = require_manage_plugins(&headers).await?;
    settings_service::clear_stored_values(&mut *STATE.db.write().await, &plugin_id)?;
    Ok(())
}

async fn load_user_settings_response(
    plugin_id: String,
    user_db_id: agdb::DbId,
    schema: Schema,
) -> Result<Json<PluginSettingsResponse>, AppError> {
    let stored = settings_service::load_validated_user_stored_values(
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
    let changes = settings_service::validate_updates(&schema, &request.values)
        .map_err(|error| AppError::bad_request(error.to_string()))?;
    settings_service::apply_user_updates(
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
    settings_service::clear_user_stored_values(
        &mut *STATE.db.write().await,
        principal.user_db_id,
        &plugin_id,
    )?;
    Ok(())
}

#[cfg(feature = "docgen")]
fn get_settings_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Get plugin settings")
        .description("Returns the settings schema and current values for a plugin.")
}

#[cfg(feature = "docgen")]
fn list_plugins_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List plugins")
        .description("Returns the loaded plugin manifests discovered at startup.")
}

#[cfg(feature = "docgen")]
fn list_all_settings_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List all plugin settings").description(
        "Returns the global settings schema and current values for every loaded plugin in a single response. Each entry carries a `status` field: `ready`, `initializing` (registry not yet frozen), `not_declared` (plugin did not declare this scope), or `invalid` (stored state is stale or malformed).",
    )
}

#[cfg(feature = "docgen")]
fn list_all_user_settings_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List all user plugin settings").description(
        "Returns the user-scoped settings schema and the authenticated user's current values for every loaded plugin in a single response. Per-entry `status` mirrors the admin list endpoint.",
    )
}

#[cfg(feature = "docgen")]
fn resolve_repository_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Resolve plugin repository").description(
        "Fetches a Git repository URL and returns the plugins it provides, including the capability scopes each plugin requests, without installing anything.",
    )
}

#[cfg(feature = "docgen")]
fn install_plugins_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Install plugins from repository").description(
        "Resolves a Git repository URL, installs all (or the selected) plugins it provides, and reloads the plugin runtime. Returns per-plugin results.",
    )
}

#[cfg(feature = "docgen")]
fn update_installed_plugin_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Update installed plugin").description(
        "Re-resolves the plugin's recorded origin and reinstalls it when the resolved commit differs. Branch refs track new commits; tag and commit refs are pinned.",
    )
}

#[cfg(feature = "docgen")]
fn uninstall_installed_plugin_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Uninstall plugin")
        .description(
            "Removes a repository-managed plugin from disk and reloads the plugin runtime. Local plugins without a source record are refused.",
        )
        .response::<204, ()>()
}

#[cfg(feature = "docgen")]
fn list_plugin_repositories_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List plugin repositories")
        .description("Returns the subscribed plugin repositories.")
}

#[cfg(feature = "docgen")]
fn add_plugin_repository_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Add plugin repository").description(
        "Resolves a Git repository URL, remembers it as a subscription, and returns the repository record together with its current plugin listing.",
    )
}

#[cfg(feature = "docgen")]
fn refresh_plugin_repository_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Refresh plugin repository").description(
        "Re-resolves a subscribed repository and returns its updated record and plugin listing, including per-plugin update availability.",
    )
}

#[cfg(feature = "docgen")]
fn delete_plugin_repository_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Remove plugin repository")
        .description(
            "Forgets a subscribed repository. Plugins installed from it stay on disk and remain individually updatable.",
        )
        .response::<204, ()>()
}

#[cfg(feature = "docgen")]
fn restart_plugin_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Restart plugin").description(
        "Tears down the plugin's current runtime registrations, re-runs its entrypoint, and activates its routes.",
    ).response::<204, ()>()
}

#[cfg(feature = "docgen")]
fn update_settings_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Update plugin settings")
        .description("Updates setting values for a plugin and returns the updated schema.")
}

#[cfg(feature = "docgen")]
fn delete_settings_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Delete plugin settings")
        .description("Deletes all stored settings for a plugin. Use this to clear stale plugin settings after a schema change.")
}

#[cfg(feature = "docgen")]
fn get_user_settings_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Get user plugin settings").description(
        "Returns the user-scoped settings schema and the authenticated user's current values.",
    )
}

#[cfg(feature = "docgen")]
fn update_user_settings_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Update user plugin settings")
        .description("Updates user-scoped setting values for the authenticated user.")
}

#[cfg(feature = "docgen")]
fn delete_user_settings_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Delete user plugin settings")
        .description("Deletes all user-scoped settings for the authenticated user.")
}

pub fn plugin_routes() -> Router {
    Router::new()
        .route("/", get(list_plugins))
        .route("/settings", get(list_all_settings))
        .route("/resolve", post(resolve_repository))
        .route("/install", post(install_plugins))
        .route("/repositories", get(list_plugin_repositories))
        .route("/repositories", post(add_plugin_repository))
        .route(
            "/repositories/{repository_id}/refresh",
            post(refresh_plugin_repository),
        )
        .route(
            "/repositories/{repository_id}",
            delete(delete_plugin_repository),
        )
        .route("/{plugin_id}", delete(uninstall_installed_plugin))
        .route("/{plugin_id}/update", post(update_installed_plugin))
        .route("/{plugin_id}/restart", post(restart_plugin))
        .route("/{plugin_id}/settings", get(get_settings))
        .route("/{plugin_id}/settings", patch(update_settings))
        .route("/{plugin_id}/settings", delete(delete_settings))
}

#[cfg(feature = "docgen")]
pub(crate) fn plugin_openapi_routes() -> aide::axum::ApiRouter {
    use aide::axum::routing::{
        delete_with,
        get_with,
        patch_with,
        post_with,
    };

    aide::axum::ApiRouter::new()
        .api_route("/", get_with(list_plugins, list_plugins_docs))
        .api_route(
            "/settings",
            get_with(list_all_settings, list_all_settings_docs),
        )
        .api_route(
            "/resolve",
            post_with(resolve_repository, resolve_repository_docs),
        )
        .api_route("/install", post_with(install_plugins, install_plugins_docs))
        .api_route(
            "/repositories",
            get_with(list_plugin_repositories, list_plugin_repositories_docs),
        )
        .api_route(
            "/repositories",
            post_with(add_plugin_repository, add_plugin_repository_docs),
        )
        .api_route(
            "/repositories/{repository_id}/refresh",
            post_with(refresh_plugin_repository, refresh_plugin_repository_docs),
        )
        .api_route(
            "/repositories/{repository_id}",
            delete_with(delete_plugin_repository, delete_plugin_repository_docs),
        )
        .api_route(
            "/{plugin_id}",
            delete_with(uninstall_installed_plugin, uninstall_installed_plugin_docs),
        )
        .api_route(
            "/{plugin_id}/update",
            post_with(update_installed_plugin, update_installed_plugin_docs),
        )
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

pub(super) fn me_plugin_settings_routes() -> Router {
    Router::new()
        .route("/settings", get(list_all_user_settings))
        .route("/{plugin_id}/settings", get(get_user_settings))
        .route("/{plugin_id}/settings", patch(update_user_settings))
        .route("/{plugin_id}/settings", delete(delete_user_settings))
}

#[cfg(feature = "docgen")]
pub(super) fn me_plugin_settings_openapi_routes() -> aide::axum::ApiRouter {
    use aide::axum::routing::{
        delete_with,
        get_with,
        patch_with,
    };

    aide::axum::ApiRouter::new()
        .api_route(
            "/settings",
            get_with(list_all_user_settings, list_all_user_settings_docs),
        )
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

        let session =
            services::auth::sessions::create_session_for_user(user_db_id, Default::default())
                .await?;
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
        let _runtime_guard = runtime_test_lock().await;
        plugin_settings_registry::initialize_registry().await;

        let response = load_registered_schema("demo", SettingsScope::Global)
            .await
            .expect_err("missing schema should report initializing while registry is mutable")
            .into_response();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn load_registered_schema_returns_not_found_after_registry_freezes() {
        let _guard = REGISTRY_TEST_GUARD.lock().await;
        let _runtime_guard = runtime_test_lock().await;
        plugin_settings_registry::initialize_registry().await;
        plugin_settings_registry::freeze_registry().await;

        let response = load_registered_schema("demo", SettingsScope::Global)
            .await
            .expect_err("missing schema should be a 404 once startup completes")
            .into_response();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        plugin_settings_registry::initialize_registry().await;
    }

    fn empty_schema() -> Schema {
        Schema { groups: Vec::new() }
    }

    #[test]
    fn build_entry_returns_initializing_for_unknown_plugin_while_registry_is_open() {
        let db = db::test_db::new_test_db().expect("test db");
        let registry = Registry::new();

        let entry = build_entry(&registry, &db, "demo", SettingsScope::Global, None);

        match entry {
            PluginSettingsEntry::Initializing { plugin_id } => assert_eq!(plugin_id, "demo"),
            _ => panic!("expected Initializing"),
        }
    }

    #[test]
    fn build_entry_returns_not_declared_for_unknown_plugin_when_registry_is_frozen() {
        let db = db::test_db::new_test_db().expect("test db");
        let mut registry = Registry::new();
        registry.freeze();

        let entry = build_entry(&registry, &db, "demo", SettingsScope::Global, None);

        match entry {
            PluginSettingsEntry::NotDeclared { plugin_id } => assert_eq!(plugin_id, "demo"),
            _ => panic!("expected NotDeclared"),
        }
    }

    #[test]
    fn build_entry_returns_ready_when_schema_is_registered() -> anyhow::Result<()> {
        let db = db::test_db::new_test_db()?;
        let mut registry = Registry::new();
        registry.register_schema(
            PluginId::new("demo")?,
            SettingsScope::Global,
            empty_schema(),
        )?;

        let entry = build_entry(&registry, &db, "demo", SettingsScope::Global, None);

        match entry {
            PluginSettingsEntry::Ready { plugin_id, groups } => {
                assert_eq!(plugin_id, "demo");
                assert!(groups.is_empty());
            }
            _ => panic!("expected Ready"),
        }
        Ok(())
    }
}
