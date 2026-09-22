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
use harmony_repository::SourceRecord;
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
        settings::plugins as settings_service,
    },
};

fn map_repository_error(error: repositories_service::PluginRepoError) -> AppError {
    use repositories_service::PluginRepoError;
    match error {
        PluginRepoError::BadRequest(message) => AppError::bad_request(message),
        PluginRepoError::NotFound(message) => AppError::not_found(message),
        PluginRepoError::Conflict(message) => AppError::conflict(message),
        PluginRepoError::BadGateway(message) => AppError::bad_gateway(message),
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
    id: String,
    name: String,
    version: String,
    description: String,
    source: PluginSourceResponse,
}

/// Where an installed plugin came from. `local` plugins were placed in the
/// plugins directory by hand and are not managed by repository tooling.
/// `invalid` plugins carry a source record that could not be read, for
/// example one written by an older schema; uninstall or reinstall them.
#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum PluginSourceResponse {
    Local,
    Invalid {
        error: String,
    },
    Repository {
        origin: String,
        #[serde(rename = "ref", skip_serializing_if = "Option::is_none")]
        git_ref: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        commit: Option<String>,
        /// True when `ref` is a tag or commit, so updates never move the
        /// plugin. Branch installs track new commits.
        pinned: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        installed_at: Option<String>,
    },
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
    groups: Vec<GroupResponse>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum PluginSettingsStatus {
    Ready { groups: Vec<GroupResponse> },
    Initializing,
    NotDeclared,
    Invalid { message: String },
}

/// One plugin in a settings listing. Listings only include plugins that
/// declare the requested scope, so the entry carries enough of the manifest
/// for a client to present the plugin without the manifest list.
#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct PluginSettingsEntry {
    plugin_id: String,
    name: String,
    version: String,
    #[serde(flatten)]
    status: PluginSettingsStatus,
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
#[derive(Deserialize)]
struct RepositoryInstallRequest {
    /// Plugin ids to install; omitted installs everything the
    /// repository provides. An empty list is rejected.
    #[serde(default)]
    plugins: Option<Vec<String>>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct UpdatePluginsRequest {
    /// Plugin ids to update; omitted updates every repository-managed
    /// plugin. An empty list is rejected.
    #[serde(default)]
    plugins: Option<Vec<String>>,
}

/// Where a catalogue plugin stands relative to the plugins directory.
/// `unknown` means it was installed from a repository but no commit is
/// recorded on one side; `local` means it was installed without a source
/// record and repository tooling leaves it alone.
#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum CatalogStatusResponse {
    Available,
    UpToDate,
    UpdateAvailable,
    Unknown,
    Local,
}

/// Set when a multi-plugin index points at a plugin living in another
/// repository.
#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct PluginPreviewSourceResponse {
    origin: String,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    commit: Option<String>,
    status: CatalogStatusResponse,
    #[serde(skip_serializing_if = "Option::is_none")]
    source: Option<PluginPreviewSourceResponse>,
}

/// A resolved repository and the plugins it provides. `id` is present only
/// for subscribed repositories, `refreshed_at` only once one has been
/// refreshed.
#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct ResolvedRepositoryResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<String>,
    origin: String,
    name: String,
    description: String,
    /// The subscribed or requested ref, when one was given.
    #[serde(rename = "ref", skip_serializing_if = "Option::is_none")]
    git_ref: Option<String>,
    /// The branch, tag, or commit the repository resolved to.
    resolved_ref: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    commit: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    refreshed_at: Option<String>,
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
struct UpdatedPluginResponse {
    id: String,
    version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    commit: Option<String>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct UpdatePluginsResponse {
    updated: Vec<UpdatedPluginResponse>,
    up_to_date: Vec<String>,
    failed: Vec<FailedInstallResponse>,
}

/// A subscribed repository as remembered by the server.
#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct PluginRepositoryResponse {
    id: String,
    origin: String,
    name: String,
    description: String,
    #[serde(rename = "ref", skip_serializing_if = "Option::is_none")]
    git_ref: Option<String>,
    /// Commit seen at the last refresh.
    #[serde(skip_serializing_if = "Option::is_none")]
    commit: Option<String>,
    /// Absent until the first refresh.
    #[serde(skip_serializing_if = "Option::is_none")]
    refreshed_at: Option<String>,
}

fn catalog_status_response(status: repositories_service::CatalogStatus) -> CatalogStatusResponse {
    use repositories_service::CatalogStatus;
    match status {
        CatalogStatus::Available => CatalogStatusResponse::Available,
        CatalogStatus::UpToDate => CatalogStatusResponse::UpToDate,
        CatalogStatus::UpdateAvailable => CatalogStatusResponse::UpdateAvailable,
        CatalogStatus::Unknown => CatalogStatusResponse::Unknown,
        CatalogStatus::Local => CatalogStatusResponse::Local,
    }
}

fn resolved_repository_response(
    record: Option<&crate::db::plugin_repositories::PluginRepository>,
    preview: repositories_service::RepositoryPreview,
) -> ResolvedRepositoryResponse {
    ResolvedRepositoryResponse {
        id: record.map(|record| record.id.clone()),
        origin: preview.origin,
        name: preview.name,
        description: preview.description,
        git_ref: preview.git_ref,
        resolved_ref: preview.resolved_ref,
        commit: preview.commit,
        refreshed_at: record
            .and_then(|record| record.refreshed_at_ms)
            .map(super::unix_ms_to_rfc3339_u64),
        plugins: preview
            .plugins
            .into_iter()
            .map(|plugin| PluginPreviewResponse {
                id: plugin.id,
                name: plugin.name,
                version: plugin.version,
                description: plugin.description,
                scopes: plugin.scopes,
                commit: plugin.commit,
                status: catalog_status_response(plugin.status),
                source: plugin
                    .source_origin
                    .map(|origin| PluginPreviewSourceResponse { origin }),
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
        commit: record.commit,
        refreshed_at: record.refreshed_at_ms.map(super::unix_ms_to_rfc3339_u64),
    }
}

fn source_response(record: Option<SourceRecord>) -> PluginSourceResponse {
    match record {
        None => PluginSourceResponse::Local,
        Some(record) => PluginSourceResponse::Repository {
            origin: record.origin,
            git_ref: record.git_ref,
            commit: record.commit,
            pinned: record.pinned,
            installed_at: record.installed_at,
        },
    }
}

fn manifest_responses(
    manifests: &[harmony_core::plugin::PluginManifest],
    plugins_dir: &std::path::Path,
) -> Vec<PluginManifestResponse> {
    manifests
        .iter()
        .map(|manifest| PluginManifestResponse {
            id: manifest.id.clone(),
            name: manifest.name.clone(),
            version: manifest.version.clone(),
            description: manifest.description.clone(),
            source: match SourceRecord::load(&plugins_dir.join(&manifest.id)) {
                Ok(record) => source_response(record),
                Err(error) => PluginSourceResponse::Invalid {
                    error: error.to_string(),
                },
            },
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
    let registry = plugin_settings_registry::settings_registry()
        .read_owned()
        .await;
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

    Ok(Json(PluginSettingsResponse { groups }))
}

fn build_status(
    registry: &Registry,
    db: &DbAny,
    plugin_id: &str,
    scope: SettingsScope,
    user_db_id: Option<DbId>,
) -> PluginSettingsStatus {
    let typed = match PluginId::new(plugin_id.to_string()) {
        Ok(id) => id,
        Err(_) => return PluginSettingsStatus::NotDeclared,
    };

    let schema = match registry.get_schema(plugin_id, scope) {
        Some(schema) => schema,
        None if !registry.is_frozen_for_plugin(&typed) => {
            return PluginSettingsStatus::Initializing;
        }
        None => return PluginSettingsStatus::NotDeclared,
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
            return PluginSettingsStatus::Invalid {
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
        Ok(groups) => PluginSettingsStatus::Ready { groups },
        Err(error) => PluginSettingsStatus::Invalid {
            message: format!("{error:#}"),
        },
    }
}

async fn list_plugins(headers: HeaderMap) -> Result<Json<Vec<PluginManifestResponse>>, AppError> {
    let _principal = require_manage_plugins(&headers).await?;
    let manifests = STATE.generation().plugin_manifests.get();
    let plugins_dir = crate::plugins::bootstrap::plugins_dir();
    Ok(Json(manifest_responses(manifests.as_ref(), &plugins_dir)))
}

async fn collect_settings_entries(
    scope: SettingsScope,
    user_db_id: Option<DbId>,
) -> Vec<PluginSettingsEntry> {
    let manifests = STATE.generation().plugin_manifests.get();
    let registry = plugin_settings_registry::settings_registry()
        .read_owned()
        .await;
    let db = STATE.db.read().await;

    manifests
        .as_ref()
        .iter()
        .filter_map(|manifest| {
            let status = build_status(&registry, &db, &manifest.id, scope, user_db_id);
            (!matches!(status, PluginSettingsStatus::NotDeclared)).then(|| PluginSettingsEntry {
                plugin_id: manifest.id.clone(),
                name: manifest.name.clone(),
                version: manifest.version.clone(),
                status,
            })
        })
        .collect()
}

async fn list_all_settings(headers: HeaderMap) -> Result<Json<Vec<PluginSettingsEntry>>, AppError> {
    let _principal = require_manage_plugins(&headers).await?;
    Ok(Json(
        collect_settings_entries(SettingsScope::Global, None).await,
    ))
}

async fn list_all_user_settings(
    headers: HeaderMap,
) -> Result<Json<Vec<PluginSettingsEntry>>, AppError> {
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
        .generation()
        .plugin_runtime
        .get()
        .ok_or_else(|| AppError::service_unavailable("plugin runtime is not ready"))?;

    STATE
        .generation()
        .plugin_registries
        .restart_plugin(&plugin_id, harmony)
        .await
        .map_err(map_plugin_restart_error)?;

    Ok(StatusCode::NO_CONTENT)
}

async fn resolve_repository(
    headers: HeaderMap,
    Json(request): Json<RepositoryUrlRequest>,
) -> Result<Json<ResolvedRepositoryResponse>, AppError> {
    let _principal = require_manage_plugins(&headers).await?;
    let preview = repositories_service::resolve_preview(&request.url, request.git_ref.as_deref())
        .await
        .map_err(map_repository_error)?;
    Ok(Json(resolved_repository_response(None, preview)))
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
    Ok(Json(install_report_response(report)))
}

async fn install_repository_plugins(
    headers: HeaderMap,
    Path(repository_id): Path<String>,
    Json(request): Json<RepositoryInstallRequest>,
) -> Result<Json<InstallPluginsResponse>, AppError> {
    let _principal = require_manage_plugins(&headers).await?;
    let report =
        repositories_service::install_from_repository(&repository_id, request.plugins.as_deref())
            .await
            .map_err(map_repository_error)?;
    Ok(Json(install_report_response(report)))
}

fn install_report_response(report: repositories_service::InstallReport) -> InstallPluginsResponse {
    InstallPluginsResponse {
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
    }
}

async fn update_installed_plugins(
    headers: HeaderMap,
    Json(request): Json<UpdatePluginsRequest>,
) -> Result<Json<UpdatePluginsResponse>, AppError> {
    let _principal = require_manage_plugins(&headers).await?;
    let report = repositories_service::update_plugins(request.plugins.as_deref())
        .await
        .map_err(map_repository_error)?;
    Ok(Json(update_report_response(report)))
}

fn update_report_response(report: repositories_service::UpdateReport) -> UpdatePluginsResponse {
    UpdatePluginsResponse {
        updated: report
            .updated
            .into_iter()
            .map(|plugin| UpdatedPluginResponse {
                id: plugin.id,
                version: plugin.version,
                commit: plugin.commit,
            })
            .collect(),
        up_to_date: report.up_to_date,
        failed: report
            .failed
            .into_iter()
            .map(|failure| FailedInstallResponse {
                id: failure.id,
                error: failure.error,
            })
            .collect(),
    }
}

async fn reload_plugins(headers: HeaderMap) -> Result<StatusCode, AppError> {
    let _principal = require_manage_plugins(&headers).await?;
    repositories_service::reload_plugins()
        .await
        .map_err(map_repository_error)?;
    Ok(StatusCode::NO_CONTENT)
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
) -> Result<Json<Vec<PluginRepositoryResponse>>, AppError> {
    let _principal = require_manage_plugins(&headers).await?;
    let repositories = repositories_service::list_repositories()
        .await
        .map_err(map_repository_error)?;
    Ok(Json(
        repositories.into_iter().map(repository_response).collect(),
    ))
}

async fn add_plugin_repository(
    headers: HeaderMap,
    Json(request): Json<RepositoryUrlRequest>,
) -> Result<Json<ResolvedRepositoryResponse>, AppError> {
    let _principal = require_manage_plugins(&headers).await?;
    let (record, preview) =
        repositories_service::add_repository(&request.url, request.git_ref.as_deref())
            .await
            .map_err(map_repository_error)?;
    Ok(Json(resolved_repository_response(Some(&record), preview)))
}

async fn refresh_plugin_repository(
    headers: HeaderMap,
    Path(repository_id): Path<String>,
) -> Result<Json<ResolvedRepositoryResponse>, AppError> {
    let _principal = require_manage_plugins(&headers).await?;
    let (record, preview) = repositories_service::refresh_repository(&repository_id)
        .await
        .map_err(map_repository_error)?;
    Ok(Json(resolved_repository_response(Some(&record), preview)))
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

    Ok(Json(PluginSettingsResponse { groups }))
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
        .description("Returns the settings groups, with schema and current values, for a plugin.")
}

#[cfg(feature = "docgen")]
fn list_plugins_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List plugins").description(
        "Returns the loaded plugin manifests along with where each plugin was installed from: a `source` of kind `repository` (origin, ref, commit, pinned, installed_at), `local`, or `invalid` (error) when the source record could not be read and the plugin should be uninstalled or reinstalled. `pinned` is true for tag and commit installs, which updates never move.",
    )
}

#[cfg(feature = "docgen")]
fn list_all_settings_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List all plugin settings").description(
        "Returns a list with the global settings schema and current values for every loaded plugin that declares global settings. Each entry carries the plugin's name and version plus a `status` field: `ready`, `initializing` (registry not yet frozen), or `invalid` (stored state is stale or malformed).",
    )
}

#[cfg(feature = "docgen")]
fn list_all_user_settings_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List all user plugin settings").description(
        "Returns a list with the user-scoped settings schema and the authenticated user's current values for every loaded plugin that declares user settings. Entries mirror the admin list endpoint.",
    )
}

#[cfg(feature = "docgen")]
fn resolve_repository_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Resolve plugin repository").description(
        "Fetches a Git repository URL and returns it in the same shape as a subscribed repository, minus `id` and `refreshed_at`: name, description, resolved ref, commit, and the plugins it provides with the capability scopes each requests and a `status` against the plugins directory (`available`, `up_to_date`, `update_available`, `unknown`, `local`). Nothing is installed.",
    )
}

#[cfg(feature = "docgen")]
fn install_plugins_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Install plugins from URL").description(
        "Resolves a Git repository URL, installs all (or the selected) plugins it provides, and reloads the plugin runtime. An empty `plugins` list is rejected. Returns per-plugin results.",
    )
}

#[cfg(feature = "docgen")]
fn install_repository_plugins_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Install plugins from subscribed repository").description(
        "Resolves a subscribed repository's origin and ref, installs all (or the selected) plugins it provides, and reloads the plugin runtime. An empty `plugins` list is rejected. Returns per-plugin results.",
    )
}

#[cfg(feature = "docgen")]
fn update_installed_plugins_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Update installed plugins").description(
        "Re-resolves the recorded origin of the selected plugins (or every repository-managed plugin when `plugins` is omitted) and reinstalls those whose resolved commit differs. Pinned plugins are reported `up_to_date` without a forge call. Each origin is resolved once and the plugin runtime reloads once. Returns per-plugin results.",
    )
}

#[cfg(feature = "docgen")]
fn reload_plugins_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Reload plugins").description(
        "Reloads the plugin runtime from the plugins directory. Use it after a CLI install or when a previous reload failed.",
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
    op.summary("List plugin repositories").description(
        "Returns the subscribed plugin repositories as remembered by the server, without resolving them. `commit` and `refreshed_at` reflect the last refresh and are absent until one has happened.",
    )
}

#[cfg(feature = "docgen")]
fn add_plugin_repository_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Add plugin repository").description(
        "Resolves a Git repository URL, remembers it as a subscription, and returns the resolved repository with its `id`, `refreshed_at`, and current plugin listing.",
    )
}

#[cfg(feature = "docgen")]
fn refresh_plugin_repository_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Refresh plugin repository").description(
        "Re-resolves a subscribed repository and returns it with its updated plugin listing; each plugin's `status` reports whether it is installed and whether an update is available.",
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
        "Tears down the plugin's current runtime registrations, re-runs its entrypoint from the files on disk, and activates its routes. Manifest changes require a full reload. Other plugins that already required this plugin's modules keep the old exports until they restart.",
    ).response::<204, ()>()
}

#[cfg(feature = "docgen")]
fn update_settings_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Update plugin settings")
        .description("Updates setting values for a plugin and returns the updated settings groups.")
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
        .route("/update", post(update_installed_plugins))
        .route("/reload", post(reload_plugins))
        .route("/repositories", get(list_plugin_repositories))
        .route("/repositories", post(add_plugin_repository))
        .route(
            "/repositories/{repository_id}/refresh",
            post(refresh_plugin_repository),
        )
        .route(
            "/repositories/{repository_id}/install",
            post(install_repository_plugins),
        )
        .route(
            "/repositories/{repository_id}",
            delete(delete_plugin_repository),
        )
        .route("/{plugin_id}", delete(uninstall_installed_plugin))
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
            "/update",
            post_with(update_installed_plugins, update_installed_plugins_docs),
        )
        .api_route("/reload", post_with(reload_plugins, reload_plugins_docs))
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
            "/repositories/{repository_id}/install",
            post_with(install_repository_plugins, install_repository_plugins_docs),
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

    fn manifest(id: &str) -> harmony_core::plugin::PluginManifest {
        harmony_core::plugin::PluginManifest {
            schema_version: 1,
            id: id.to_string(),
            name: id.to_string(),
            version: "1.0.0".to_string(),
            description: String::new(),
            entrypoint: None,
            scopes: Vec::new(),
            dependencies: Vec::new(),
        }
    }

    #[test]
    fn manifest_responses_report_plugin_sources() -> anyhow::Result<()> {
        let plugins_dir = tempfile::TempDir::new()?;
        std::fs::create_dir(plugins_dir.path().join("local"))?;
        std::fs::create_dir(plugins_dir.path().join("managed"))?;
        std::fs::create_dir(plugins_dir.path().join("broken"))?;
        std::fs::write(
            plugins_dir
                .path()
                .join("broken")
                .join(harmony_repository::manifest::SOURCE_RECORD_FILENAME),
            r#"{"schema_version": 1}"#,
        )?;
        SourceRecord {
            schema_version: harmony_repository::manifest::SOURCE_RECORD_SCHEMA_VERSION,
            origin: "https://github.com/o/r".into(),
            forge: harmony_repository::Forge::GitHub,
            git_ref: Some("v2".into()),
            commit: Some("a".repeat(40)),
            pinned: true,
            subpath: None,
            via_repository: None,
            installed_at: None,
        }
        .store(&plugins_dir.path().join("managed"))?;

        let responses = manifest_responses(
            &[manifest("local"), manifest("managed"), manifest("broken")],
            plugins_dir.path(),
        );
        let json = serde_json::to_value(&responses)?;

        assert_eq!(json[0]["source"], serde_json::json!({ "kind": "local" }));
        assert_eq!(json[1]["source"]["kind"], "repository");
        assert_eq!(json[1]["source"]["origin"], "https://github.com/o/r");
        assert_eq!(json[1]["source"]["ref"], "v2");
        assert_eq!(json[1]["source"]["commit"], "a".repeat(40));
        assert_eq!(json[1]["source"]["pinned"], true);
        assert!(json[1]["source"].get("forge").is_none());
        assert!(json[1].get("schema_version").is_none());
        assert_eq!(json[2]["source"]["kind"], "invalid");
        let error = json[2]["source"]["error"].as_str().unwrap_or_default();
        assert!(error.contains("schema_version 1"), "{error}");
        Ok(())
    }

    #[test]
    fn resolved_repository_response_flattens_record_and_preview() -> anyhow::Result<()> {
        use services::plugin_repositories::{
            CatalogStatus,
            PluginPreview,
            RepositoryPreview,
        };

        let preview = RepositoryPreview {
            origin: "https://github.com/o/r".into(),
            git_ref: Some("main".into()),
            resolved_ref: "main".into(),
            commit: Some("a".repeat(40)),
            name: "Catalog".into(),
            description: "Plugins".into(),
            plugins: vec![
                PluginPreview {
                    id: "here".into(),
                    name: "Here".into(),
                    version: "1.0.0".into(),
                    description: String::new(),
                    scopes: vec!["metadata".into()],
                    commit: Some("a".repeat(40)),
                    status: CatalogStatus::UpdateAvailable,
                    source_origin: None,
                },
                PluginPreview {
                    id: "there".into(),
                    name: "There".into(),
                    version: "2.0.0".into(),
                    description: String::new(),
                    scopes: Vec::new(),
                    commit: None,
                    status: CatalogStatus::Available,
                    source_origin: Some("https://github.com/o/other".into()),
                },
            ],
        };
        let record = db::plugin_repositories::PluginRepository {
            db_id: None,
            id: "repo".into(),
            origin: "https://github.com/o/r".into(),
            name: "Stale".into(),
            description: String::new(),
            git_ref: Some("main".into()),
            commit: None,
            refreshed_at_ms: Some(1_000),
        };

        let json = serde_json::to_value(resolved_repository_response(None, preview.clone()))?;
        assert!(json.get("id").is_none());
        assert!(json.get("refreshed_at").is_none());
        assert_eq!(json["name"], "Catalog");
        assert_eq!(json["ref"], "main");
        assert_eq!(json["resolved_ref"], "main");
        assert_eq!(json["plugins"][0]["status"], "update_available");
        assert!(json["plugins"][0].get("source").is_none());
        assert_eq!(json["plugins"][1]["status"], "available");
        assert_eq!(
            json["plugins"][1]["source"]["origin"],
            "https://github.com/o/other"
        );

        let json = serde_json::to_value(resolved_repository_response(Some(&record), preview))?;
        assert_eq!(json["id"], "repo");
        assert_eq!(json["refreshed_at"], "1970-01-01T00:00:01Z");
        assert_eq!(json["name"], "Catalog");
        Ok(())
    }

    #[test]
    fn repository_response_omits_refresh_time_until_refreshed() -> anyhow::Result<()> {
        let json = serde_json::to_value(repository_response(
            db::plugin_repositories::PluginRepository {
                db_id: None,
                id: "repo".into(),
                origin: "https://github.com/o/r".into(),
                name: "Catalog".into(),
                description: String::new(),
                git_ref: None,
                commit: Some("a".repeat(40)),
                refreshed_at_ms: None,
            },
        ))?;
        assert_eq!(json["commit"], "a".repeat(40));
        assert!(json.get("refreshed_at").is_none());
        assert!(json.get("ref").is_none());
        assert!(json.get("last_commit").is_none());
        Ok(())
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
    async fn update_installed_plugins_reports_invalid_ids_as_failed() -> anyhow::Result<()> {
        let _registry_guard = REGISTRY_TEST_GUARD.lock().await;
        let _runtime_guard = runtime_test_lock().await;
        let _test_dir = initialize_auth_test_runtime().await?;
        let headers = manage_plugins_headers().await?;

        let Json(response) = update_installed_plugins(
            headers,
            Json(UpdatePluginsRequest {
                plugins: Some(vec!["bad id".to_string()]),
            }),
        )
        .await
        .map_err(|error| anyhow::anyhow!("{error:?}"))?;

        assert!(response.updated.is_empty());
        assert!(response.up_to_date.is_empty());
        assert_eq!(response.failed.len(), 1);
        assert_eq!(response.failed[0].id, "bad id");
        assert!(response.failed[0].error.contains("invalid plugin id"));
        Ok(())
    }

    #[tokio::test]
    async fn install_repository_plugins_rejects_unknown_repository() -> anyhow::Result<()> {
        let _registry_guard = REGISTRY_TEST_GUARD.lock().await;
        let _runtime_guard = runtime_test_lock().await;
        let _test_dir = initialize_auth_test_runtime().await?;
        let headers = manage_plugins_headers().await?;

        let Err(error) = install_repository_plugins(
            headers,
            Path("missing".to_string()),
            Json(RepositoryInstallRequest {
                plugins: Some(vec!["demo".to_string()]),
            }),
        )
        .await
        else {
            panic!("unknown repository should be rejected");
        };
        let response = error.into_response();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        Ok(())
    }

    #[test]
    fn update_report_response_maps_every_outcome() {
        let response = update_report_response(services::plugin_repositories::UpdateReport {
            updated: vec![services::plugin_repositories::UpdatedPlugin {
                id: "a".into(),
                version: "1.2.3".into(),
                commit: Some("c".repeat(40)),
            }],
            up_to_date: vec!["b".into()],
            failed: vec![services::plugin_repositories::FailedInstall {
                id: "c".into(),
                error: "boom".into(),
            }],
        });

        assert_eq!(response.updated.len(), 1);
        assert_eq!(response.updated[0].id, "a");
        assert_eq!(response.updated[0].version, "1.2.3");
        assert_eq!(
            response.updated[0].commit.as_deref(),
            Some("c".repeat(40).as_str())
        );
        assert_eq!(response.up_to_date, vec!["b".to_string()]);
        assert_eq!(response.failed[0].id, "c");
        assert_eq!(response.failed[0].error, "boom");
    }

    #[tokio::test]
    async fn reload_plugins_requires_manage_plugins() -> anyhow::Result<()> {
        let _registry_guard = REGISTRY_TEST_GUARD.lock().await;
        let _runtime_guard = runtime_test_lock().await;
        let _test_dir = initialize_auth_test_runtime().await?;

        let response = reload_plugins(HeaderMap::new())
            .await
            .expect_err("unauthenticated reload should be rejected")
            .into_response();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
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
        crate::testing::init_default_test_state().expect("init test state");

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
        crate::testing::init_default_test_state().expect("init test state");
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
    fn build_status_returns_initializing_for_unknown_plugin_while_registry_is_open() {
        let db = db::test_db::new_test_db().expect("test db");
        let registry = Registry::default();

        let status = build_status(&registry, &db, "demo", SettingsScope::Global, None);

        assert!(matches!(status, PluginSettingsStatus::Initializing));
    }

    #[test]
    fn build_status_returns_not_declared_for_unknown_plugin_when_registry_is_frozen() {
        let db = db::test_db::new_test_db().expect("test db");
        let mut registry = Registry::default();
        registry.freeze();

        let status = build_status(&registry, &db, "demo", SettingsScope::Global, None);

        assert!(matches!(status, PluginSettingsStatus::NotDeclared));
    }

    #[test]
    fn build_status_returns_ready_when_schema_is_registered() -> anyhow::Result<()> {
        let db = db::test_db::new_test_db()?;
        let mut registry = Registry::default();
        registry.register_schema(
            PluginId::new("demo")?,
            SettingsScope::Global,
            empty_schema(),
        )?;

        let status = build_status(&registry, &db, "demo", SettingsScope::Global, None);

        match status {
            PluginSettingsStatus::Ready { groups } => assert!(groups.is_empty()),
            _ => panic!("expected Ready"),
        }
        Ok(())
    }
}
