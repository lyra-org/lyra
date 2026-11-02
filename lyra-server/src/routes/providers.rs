// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashMap;

#[cfg(feature = "docgen")]
use aide::transform::TransformOperation;
use axum::Router;
use axum::{
    Json,
    extract::{
        Path,
        Query,
    },
    http::{
        HeaderMap,
        StatusCode,
    },
};
use serde::{
    Deserialize,
    Serialize,
};
use serde_json::Value;

use crate::routes::request::bearer_credential;
use crate::{
    routes::AppError,
    services::STATE,
    services::{
        CoverSyncOptions,
        CoverUpdate,
        EntityRefreshOptions,
        EntityType,
        NormalizedProviderArtistSearchResult,
        NormalizedProviderReleaseSearchResult,
        NormalizedProviderSearchResult,
        NormalizedProviderTrackSearchResult,
        ProviderSearchError,
        ProviderSearchRequest,
        auth::{
            require_authenticated,
            require_manage_metadata,
            require_manage_providers,
            require_sync_metadata,
        },
        options::{
            OptionDeclaration,
            OptionType,
        },
        providers::provider_registry,
        providers::{
            EntityExternalIdRecord,
            ProviderUpdate,
            SetEntityExternalIdRequest as ProviderSetEntityExternalIdRequest,
            inherit_entity_external_id as inherit_entity_external_id_service,
            list_entity_external_ids as list_entity_external_ids_service,
            list_provider_configs,
            refresh_entity as refresh_entity_service,
            set_entity_external_id as set_entity_external_id_service,
            set_entity_locked,
            update_provider as update_provider_service,
        },
        run_provider_sync,
        search_provider as search_provider_results,
    },
};

/// A request's optional provider filter, rejected unless it is a valid provider id.
pub(super) fn parse_provider_filter(provider: Option<&str>) -> Result<Option<&str>, AppError> {
    let Some(provider_id) = provider else {
        return Ok(None);
    };
    lyra_db::providers::validate_provider_id(provider_id)
        .map_err(|error| AppError::bad_request(error.to_string()))?;
    Ok(Some(provider_id))
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
#[non_exhaustive]
pub struct OptionResponse {
    pub name: String,
    pub label: String,
    #[serde(rename = "type")]
    pub option_type: String,
    /// The values a `choice` option takes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub options: Option<Vec<OptionChoiceResponse>>,
    pub default: serde_json::Value,
    pub available: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unavailable_reason: Option<String>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
#[non_exhaustive]
pub struct OptionChoiceResponse {
    pub value: String,
    pub label: String,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
#[non_exhaustive]
pub struct ProviderResponse {
    pub provider_id: String,
    pub display_name: String,
    /// Zero-based rank; lower positions take precedence.
    pub position: u32,
    pub enabled: bool,
    pub options: Vec<OptionResponse>,
}

impl From<EntityExternalIdRecord> for ExternalIdResponse {
    fn from(value: EntityExternalIdRecord) -> Self {
        Self {
            provider_id: value.provider_id,
            id_type: value.id_type,
            id_value: value.id_value,
            source: value.source,
            resolved_value: value.resolved_value,
        }
    }
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
#[serde(tag = "entity_type", rename_all = "lowercase")]
pub enum SearchResult {
    #[serde(rename = "release")]
    Release(ReleaseSearchResult),
    #[serde(rename = "artist")]
    Artist(ArtistSearchResult),
    #[serde(rename = "track")]
    Track(TrackSearchResult),
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SearchEntityType {
    Release,
    Artist,
    Track,
}

impl From<SearchEntityType> for EntityType {
    fn from(value: SearchEntityType) -> Self {
        match value {
            SearchEntityType::Release => Self::Release,
            SearchEntityType::Artist => Self::Artist,
            SearchEntityType::Track => Self::Track,
        }
    }
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
#[non_exhaustive]
pub struct ReleaseSearchResult {
    pub title: String,
    pub redirect_url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artist_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Release date as YYYY, YYYY-MM, or YYYY-MM-DD.")
    )]
    pub release_date: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub genres: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort_title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ids: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cover_url: Option<String>,
    /// Provider payload untouched.
    pub raw: Value,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
pub struct TrackSearchResult {
    pub title: String,
    pub redirect_url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artist_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub release_title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort_title: Option<String>,
    /// Duration in milliseconds if available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disc: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disc_total: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub track: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub track_total: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ids: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cover_url: Option<String>,
    /// Provider payload untouched.
    pub raw: Value,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
#[non_exhaustive]
pub struct ArtistSearchResult {
    pub title: String,
    pub redirect_url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artist_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ids: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cover_url: Option<String>,
    /// Provider payload untouched.
    pub raw: Value,
}

impl From<NormalizedProviderSearchResult> for SearchResult {
    fn from(value: NormalizedProviderSearchResult) -> Self {
        match value {
            NormalizedProviderSearchResult::Release(result) => {
                SearchResult::Release(ReleaseSearchResult::from(result))
            }
            NormalizedProviderSearchResult::Artist(result) => {
                SearchResult::Artist(ArtistSearchResult::from(result))
            }
            NormalizedProviderSearchResult::Track(result) => {
                SearchResult::Track(TrackSearchResult::from(result))
            }
        }
    }
}

impl From<NormalizedProviderReleaseSearchResult> for ReleaseSearchResult {
    fn from(value: NormalizedProviderReleaseSearchResult) -> Self {
        Self {
            title: value.title,
            redirect_url: value.redirect_url,
            artist_name: value.artist_name,
            release_date: value.release_date,
            genres: value.genres,
            description: value.description,
            sort_name: value.sort_name,
            sort_title: value.sort_title,
            ids: value.ids,
            cover_url: value.cover_url,
            raw: value.raw,
        }
    }
}

impl From<NormalizedProviderArtistSearchResult> for ArtistSearchResult {
    fn from(value: NormalizedProviderArtistSearchResult) -> Self {
        Self {
            title: value.title,
            redirect_url: value.redirect_url,
            artist_name: value.artist_name,
            sort_name: value.sort_name,
            description: value.description,
            ids: value.ids,
            cover_url: value.cover_url,
            raw: value.raw,
        }
    }
}

impl From<NormalizedProviderTrackSearchResult> for TrackSearchResult {
    fn from(value: NormalizedProviderTrackSearchResult) -> Self {
        Self {
            title: value.title,
            redirect_url: value.redirect_url,
            artist_name: value.artist_name,
            release_title: value.release_title,
            sort_title: value.sort_title,
            duration_ms: value.duration_ms,
            disc: value.disc,
            disc_total: value.disc_total,
            track: value.track,
            track_total: value.track_total,
            ids: value.ids,
            cover_url: value.cover_url,
            raw: value.raw,
        }
    }
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
#[non_exhaustive]
pub struct SearchQuery {
    #[serde(rename = "type")]
    pub entity_type: SearchEntityType,
    pub q: String,
    #[serde(default)]
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Resolve provider cover URLs for release or artist search results using provider cover handlers."
        )
    )]
    pub include_cover_urls: bool,
    #[serde(default)]
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "With `include_cover_urls`, look covers up again instead of using cached lookups."
        )
    )]
    pub bypass_cover_cache: bool,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
pub struct UpdateProviderRequest {
    /// Moves the provider to this zero-based rank, clamped to the last one.
    pub position: Option<u32>,
    pub enabled: Option<bool>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
#[non_exhaustive]
pub struct SetExternalIdRequest {
    pub id_value: String,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
#[non_exhaustive]
pub struct ExternalIdResponse {
    pub provider_id: String,
    pub id_type: String,
    pub id_value: String,
    /// `manual` when a user set the value, `resolved` when a provider did.
    pub source: String,
    /// The provider's value beneath a manual one, which returns when the ID is
    /// inherited again.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolved_value: Option<String>,
}

async fn list_providers(headers: HeaderMap) -> Result<Json<Vec<ProviderResponse>>, AppError> {
    let _principal = require_authenticated(bearer_credential(&headers)).await?;

    let providers = list_provider_configs().await?;
    Ok(Json(build_provider_responses(providers).await))
}

async fn build_provider_responses(
    providers: Vec<lyra_db::ProviderConfig>,
) -> Vec<ProviderResponse> {
    let providers = {
        let registry = provider_registry().read_owned().await;
        providers
            .into_iter()
            .map(|config| {
                let options = registry.get_options(&config.provider_id).to_vec();
                let display_name = registry.display_name(&config.provider_id);
                (config, options, display_name)
            })
            .collect::<Vec<_>>()
    };
    let db = STATE.db.read().await;

    providers
        .into_iter()
        .map(|(config, options, display_name)| {
            let options = option_responses(&db, &config.provider_id, &options);
            ProviderResponse {
                provider_id: config.provider_id,
                display_name,
                position: config.position,
                enabled: config.enabled,
                options,
            }
        })
        .collect()
}

/// The declared options, each available once `plugin_id` has the settings it requires.
pub(super) fn option_responses(
    db: &agdb::DbAny,
    plugin_id: &str,
    options: &[OptionDeclaration],
) -> Vec<OptionResponse> {
    options
        .iter()
        .map(|opt| {
            let (available, unavailable_reason) = if opt.requires_settings.is_empty() {
                (true, None)
            } else {
                check_settings_availability(db, plugin_id, &opt.requires_settings)
            };
            OptionResponse {
                name: opt.name.clone(),
                label: opt.label.clone(),
                option_type: opt.option_type.name().to_string(),
                options: match &opt.option_type {
                    OptionType::Choice(choices) => Some(
                        choices
                            .iter()
                            .map(|choice| OptionChoiceResponse {
                                value: choice.value.clone(),
                                label: choice.label.clone(),
                            })
                            .collect(),
                    ),
                    _ => None,
                },
                default: opt.default.clone(),
                available,
                unavailable_reason,
            }
        })
        .collect()
}

/// Checks whether all required settings for an option have non-empty values.
fn check_settings_availability(
    db: &agdb::DbAny,
    plugin_id: &str,
    required_settings: &[String],
) -> (bool, Option<String>) {
    let plugin_settings = match lyra_db::settings::plugins::find_with(db, plugin_id) {
        Ok(Some(ps)) => ps,
        Ok(None) => {
            return (
                false,
                Some(format!(
                    "missing settings: {}",
                    required_settings.join(", ")
                )),
            );
        }
        Err(err) => {
            tracing::warn!(
                plugin_id,
                error = %err,
                "failed to read plugin settings for option availability check"
            );
            return (
                false,
                Some(format!(
                    "missing settings: {}",
                    required_settings.join(", ")
                )),
            );
        }
    };
    let Some(plugin_db_id) = plugin_settings.db_id.map(agdb::DbId::from) else {
        return (
            false,
            Some(format!(
                "missing settings: {}",
                required_settings.join(", ")
            )),
        );
    };
    let all_entries = match lyra_db::settings::get_all_settings_with(db, plugin_db_id) {
        Ok(entries) => entries,
        Err(err) => {
            tracing::warn!(
                plugin_id,
                error = %err,
                "failed to read settings entries for option availability check"
            );
            return (
                false,
                Some(format!(
                    "missing settings: {}",
                    required_settings.join(", ")
                )),
            );
        }
    };
    let mut missing = Vec::new();
    for key in required_settings {
        let has_value = all_entries
            .iter()
            .any(|e| e.key == *key && !e.value.trim().is_empty());
        if !has_value {
            missing.push(key.as_str());
        }
    }
    if missing.is_empty() {
        (true, None)
    } else {
        (
            false,
            Some(format!("missing settings: {}", missing.join(", "))),
        )
    }
}

async fn search_provider(
    headers: HeaderMap,
    Path(provider_id): Path<String>,
    Json(query): Json<SearchQuery>,
) -> Result<Json<Vec<SearchResult>>, AppError> {
    let _principal = require_manage_metadata(bearer_credential(&headers)).await?;

    let results = search_provider_results(ProviderSearchRequest {
        provider_id: &provider_id,
        entity_type: query.entity_type.into(),
        query: &query.q,
        include_cover_urls: query.include_cover_urls,
        bypass_cache: query.bypass_cover_cache,
    })
    .await
    .map_err(|err| match err {
        ProviderSearchError::NoSearchHandler {
            provider_id,
            entity_type,
        } => AppError::not_found(format!(
            "No search handler for provider '{}' and type '{}'",
            provider_id, entity_type
        )),
        ProviderSearchError::Internal(err) => AppError::from(err),
    })?;

    Ok(Json(results.into_iter().map(Into::into).collect()))
}

async fn update_provider(
    headers: HeaderMap,
    Path(provider_id): Path<String>,
    Json(request): Json<UpdateProviderRequest>,
) -> Result<Json<Vec<ProviderResponse>>, AppError> {
    let _principal = require_manage_providers(bearer_credential(&headers)).await?;

    let providers = update_provider_service(
        &provider_id,
        ProviderUpdate {
            position: request.position,
            enabled: request.enabled,
        },
    )
    .await?;
    Ok(Json(build_provider_responses(providers).await))
}

async fn get_entity_external_ids(
    headers: HeaderMap,
    Path(entity_id): Path<String>,
) -> Result<Json<Vec<ExternalIdResponse>>, AppError> {
    let _principal = require_manage_metadata(bearer_credential(&headers)).await?;

    let response: Vec<ExternalIdResponse> = list_entity_external_ids_service(&entity_id)
        .await?
        .into_iter()
        .map(Into::into)
        .collect();

    Ok(Json(response))
}

async fn set_entity_external_id(
    headers: HeaderMap,
    Path((entity_id, provider_id, id_type)): Path<(String, String, String)>,
    Json(request): Json<SetExternalIdRequest>,
) -> Result<Json<ExternalIdResponse>, AppError> {
    let _principal = require_manage_metadata(bearer_credential(&headers)).await?;

    let record = set_entity_external_id_service(
        &entity_id,
        ProviderSetEntityExternalIdRequest {
            provider_id,
            id_type,
            id_value: request.id_value,
        },
    )
    .await?;

    Ok(Json(record.into()))
}

async fn inherit_entity_external_id(
    headers: HeaderMap,
    Path((entity_id, provider_id, id_type)): Path<(String, String, String)>,
) -> Result<StatusCode, AppError> {
    let _principal = require_manage_metadata(bearer_credential(&headers)).await?;
    inherit_entity_external_id_service(&entity_id, &provider_id, &id_type).await?;

    Ok(StatusCode::NO_CONTENT)
}

async fn lock_entity(
    headers: HeaderMap,
    Path(entity_id): Path<String>,
) -> Result<Json<serde_json::Value>, AppError> {
    let _principal = require_manage_metadata(bearer_credential(&headers)).await?;
    set_entity_locked(&entity_id, true).await?;

    Ok(Json(serde_json::json!({ "locked": true })))
}

async fn unlock_entity(
    headers: HeaderMap,
    Path(entity_id): Path<String>,
) -> Result<Json<serde_json::Value>, AppError> {
    let _principal = require_manage_metadata(bearer_credential(&headers)).await?;
    set_entity_locked(&entity_id, false).await?;

    Ok(Json(serde_json::json!({ "locked": false })))
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
#[non_exhaustive]
pub struct RefreshResponse {
    pub refreshed: bool,
    pub entity_type: EntityType,
    pub providers_called: Vec<String>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize, Default)]
pub struct RefreshEntityQuery {
    #[serde(default)]
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "The cover to fetch for a refreshed release or artist: `skip`, `missing` (the default, only when none is set) or `replace`."
        )
    )]
    pub covers: CoverUpdate,
    #[serde(default)]
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Look the cover up again instead of using cached lookups. Rejected with `covers=skip`."
        )
    )]
    pub bypass_cover_cache: bool,
    #[serde(flatten)]
    #[cfg_attr(feature = "docgen", schemars(skip))]
    pub extra: HashMap<String, String>,
}

const KNOWN_REFRESH_PARAMS: &[&str] = &["covers", "bypass_cover_cache"];

async fn refresh_entity(
    headers: HeaderMap,
    Path(entity_id): Path<String>,
    Query(query): Query<RefreshEntityQuery>,
) -> Result<Json<RefreshResponse>, AppError> {
    let principal = require_authenticated(bearer_credential(&headers)).await?;
    let covers = CoverSyncOptions::new(query.covers, query.bypass_cover_cache)?;
    let mut provider_options: HashMap<String, String> = query.extra;
    provider_options.retain(|key, _| !KNOWN_REFRESH_PARAMS.contains(&key.as_str()));
    let result = refresh_entity_service(
        &principal,
        &entity_id,
        EntityRefreshOptions {
            covers,
            provider_options,
        },
    )
    .await?;

    Ok(Json(RefreshResponse {
        refreshed: true,
        entity_type: result.entity_type,
        providers_called: result.providers_called,
    }))
}

#[cfg(feature = "docgen")]
fn list_providers_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List providers").description(
        "Returns all registered metadata providers in priority order, with their positions and option availability. Requires authentication.",
    )
}

#[cfg(feature = "docgen")]
fn search_provider_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Search provider").description(
        "Invokes a provider's search handler for a given entity type and query.\n\n\
        Request body: `{ type: \"release\"|\"artist\"|\"track\", q: string }`.\n\
        `raw` always contains the original provider payload, and `title` is normalized from provider keys in priority order: `title`, `album_title`, `track_title`, `artist_name`, `name`.\n\n\
        Set `include_cover_urls=true` to resolve `cover_url` for release and artist results via provider cover handlers. Requires ManageMetadata permission.",
    )
}

#[cfg(feature = "docgen")]
fn update_provider_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Update provider").description(
        "Moves a metadata provider to `position` and/or sets `enabled`, then returns every provider in priority order. Positions are zero-based and gap-free; position 0 takes precedence, and a position past the end moves the provider last. Requires ManageProviders permission.",
    )
}

#[cfg(feature = "docgen")]
fn get_external_ids_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Get entity external IDs").description(
        "Returns all external IDs associated with an entity. Requires ManageMetadata permission.",
    )
}

#[cfg(feature = "docgen")]
fn set_external_id_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Set entity external ID").description(
        "Sets the manual value of one external ID on an entity, keyed by `provider_id` and `id_type` in the path. It takes effect over the resolved value, which is kept and returns when the ID is inherited again. Returns 400 when the value is blank. Requires ManageMetadata permission.",
    )
}

#[cfg(feature = "docgen")]
fn inherit_external_id_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Inherit entity external ID").description(
        "Removes the manual value of one external ID on an entity, so the resolved value takes effect again. Returns 404 when the ID has no manual value. Requires ManageMetadata permission.",
    )
}

#[cfg(feature = "docgen")]
fn lock_entity_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Lock entity")
        .description("Locks an entity so scans and providers no longer change its editable metadata; manual edits still apply. Requires ManageMetadata permission.")
}

#[cfg(feature = "docgen")]
fn unlock_entity_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Unlock entity")
        .description("Unlocks an entity so scans and providers update its editable metadata again. Requires ManageMetadata permission.")
}

#[cfg(feature = "docgen")]
fn refresh_entity_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Refresh entity metadata").description(
        "Triggers a metadata refresh for an entity from all enabled providers.\n\n\
            For release and artist entities, `covers` picks the cover to fetch once the providers answered: `skip`, `missing` (the default, only when none is set) or `replace`. \
            `bypass_cover_cache=true` looks it up again instead of using cached lookups, and is rejected with 400 alongside `covers=skip`.\n\n\
            Pass provider options listed by `GET /providers` as `<provider_id>.<option>=<value>`; \
            each provider receives only its own declared options.\n\n\
            Answers once every provider has. Returns 404 when the entity is missing or hidden from the caller. Requires SyncMetadata permission.",
    )
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
pub struct SyncResponse {
    pub started: bool,
    pub provider_id: String,
}

async fn sync_provider(
    headers: HeaderMap,
    Path(provider_id): Path<String>,
) -> Result<Json<SyncResponse>, AppError> {
    let _principal = require_sync_metadata(bearer_credential(&headers)).await?;

    run_provider_sync(&provider_id).await?;

    Ok(Json(SyncResponse {
        started: true,
        provider_id,
    }))
}

#[cfg(feature = "docgen")]
fn sync_provider_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Sync provider").description(
        "Triggers a full sync for a provider. Requires SyncMetadata permission. Returns 409 if a sync is already running.",
    )
}

pub fn provider_routes() -> Router {
    use axum::routing::{
        get,
        patch,
        post,
    };

    Router::new()
        .route("/", get(list_providers))
        .route("/{provider_id}/search", post(search_provider))
        .route("/{provider_id}", patch(update_provider))
        .route("/{provider_id}/sync", post(sync_provider))
}

#[cfg(feature = "docgen")]
pub(crate) fn provider_openapi_routes() -> aide::axum::ApiRouter {
    use aide::axum::routing::{
        get_with,
        patch_with,
        post_with,
    };

    aide::axum::ApiRouter::new()
        .api_route("/", get_with(list_providers, list_providers_docs))
        .api_route(
            "/{provider_id}/search",
            post_with(search_provider, search_provider_docs),
        )
        .api_route(
            "/{provider_id}",
            patch_with(update_provider, update_provider_docs),
        )
        .api_route(
            "/{provider_id}/sync",
            post_with(sync_provider, sync_provider_docs),
        )
}

pub fn entity_routes() -> Router {
    use axum::routing::{
        delete,
        get,
        post,
        put,
    };

    Router::new()
        .route("/{id}/external-ids", get(get_entity_external_ids))
        .route(
            "/{id}/external-ids/{provider_id}/{id_type}",
            put(set_entity_external_id).delete(inherit_entity_external_id),
        )
        .route("/{id}/lock", put(lock_entity))
        .route("/{id}/lock", delete(unlock_entity))
        .route("/{id}/refresh", post(refresh_entity))
}

#[cfg(feature = "docgen")]
pub(crate) fn entity_openapi_routes() -> aide::axum::ApiRouter {
    use aide::axum::routing::{
        delete_with,
        get_with,
        post_with,
        put_with,
    };

    aide::axum::ApiRouter::new()
        .api_route(
            "/{id}/external-ids",
            get_with(get_entity_external_ids, get_external_ids_docs),
        )
        .api_route(
            "/{id}/external-ids/{provider_id}/{id_type}",
            put_with(set_entity_external_id, set_external_id_docs)
                .delete_with(inherit_entity_external_id, inherit_external_id_docs),
        )
        .api_route("/{id}/lock", put_with(lock_entity, lock_entity_docs))
        .api_route("/{id}/lock", delete_with(unlock_entity, unlock_entity_docs))
        .api_route(
            "/{id}/refresh",
            post_with(refresh_entity, refresh_entity_docs),
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::entities::build_release_context;
    use crate::services::providers::IdSchemes;
    use agdb::{
        DbAny,
        DbId,
        QueryBuilder,
    };
    use anyhow::anyhow;
    use lyra_db::roots::{
        self,
        Root,
    };
    use lyra_db::{
        Entry,
        IdSource,
        Track,
    };
    use nanoid::nanoid;
    use std::path::PathBuf;

    use lyra_db::fixtures::{
        connect,
        insert_artist,
        insert_release,
    };

    #[test]
    fn search_body_names_the_cover_cache_bypass() {
        let query: SearchQuery = serde_json::from_str(
            r#"{"type":"release","q":"loveless","include_cover_urls":true,"bypass_cover_cache":true}"#,
        )
        .expect("parses");
        assert!(query.include_cover_urls && query.bypass_cover_cache);
        let defaults: SearchQuery =
            serde_json::from_str(r#"{"type":"artist","q":"mbv"}"#).expect("parses");
        assert!(!defaults.bypass_cover_cache);
    }

    fn refresh_query(query: &str) -> Result<RefreshEntityQuery, String> {
        let uri: axum::http::Uri = format!("/?{query}").parse().expect("uri");
        Query::<RefreshEntityQuery>::try_from_uri(&uri)
            .map(|Query(query)| query)
            .map_err(|rejection| rejection.body_text())
    }

    #[test]
    fn refresh_query_reads_the_cover_options_beside_provider_options() {
        let query =
            refresh_query("covers=replace&bypass_cover_cache=true&demo.mode=deep").expect("parses");
        assert_eq!(query.covers, CoverUpdate::Replace);
        assert!(query.bypass_cover_cache);
        assert_eq!(
            query.extra.get("demo.mode").map(String::as_str),
            Some("deep")
        );

        let defaults = refresh_query("").expect("parses");
        assert_eq!(defaults.covers, CoverUpdate::Missing);
        assert!(!defaults.bypass_cover_cache);

        let unknown = refresh_query("covers=always").err().expect("rejected");
        assert!(
            unknown.contains("`skip`, `missing`, `replace`"),
            "{unknown}"
        );
    }

    #[tokio::test]
    async fn refresh_skipping_covers_while_bypassing_their_cache_is_a_bad_request()
    -> anyhow::Result<()> {
        use axum::response::IntoResponse;

        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::plugins::testing::init_default_test_state()?;
        let user = lyra_db::users::create(
            &mut *STATE.db.write().await,
            &lyra_db::fixtures::test_user("cover-options")?,
        )?;
        let session = crate::services::testing::create_session(user, Default::default()).await?;
        let mut headers = HeaderMap::new();
        headers.insert(
            "Authorization",
            format!("Bearer {}", session.token).parse()?,
        );
        let status = |headers: HeaderMap| async move {
            let query = refresh_query("covers=skip&bypass_cover_cache=true").expect("parses");
            refresh_entity(headers, Path("entity".to_string()), Query(query))
                .await
                .map(|_| ())
                .expect_err("rejected")
                .into_response()
                .status()
        };
        assert_eq!(status(HeaderMap::new()).await, StatusCode::UNAUTHORIZED);
        assert_eq!(status(headers).await, StatusCode::BAD_REQUEST);
        Ok(())
    }

    #[test]
    fn parse_provider_filter_accepts_only_valid_provider_ids() {
        assert_eq!(parse_provider_filter(None).ok(), Some(None));
        assert_eq!(
            parse_provider_filter(Some("musicbrainz")).ok(),
            Some(Some("musicbrainz"))
        );
        for provider in ["", " musicbrainz", "music brainz"] {
            let error = parse_provider_filter(Some(provider))
                .err()
                .unwrap_or_else(|| panic!("provider {provider:?} must be rejected"));
            assert_eq!(
                axum::response::IntoResponse::into_response(error).status(),
                axum::http::StatusCode::BAD_REQUEST
            );
        }
    }
    use lyra_db::test_db::{
        insert_library,
        new_test_db,
    };

    #[tokio::test]
    async fn provider_response_database_wait_releases_registry_for_queued_writer()
    -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::plugins::testing::init_default_test_state()?;
        let plugin_id = crate::services::plugin_scope::PluginId::new("provider-lock-test")?;
        let provider_id = "provider-lock-test".to_string();
        let registry = provider_registry();
        {
            let mut registry = registry.write().await;
            registry.register(
                plugin_id.clone(),
                provider_id.clone(),
                Some("Lock Test Provider".to_string()),
            )?;
            registry
                .declare_option(
                    &provider_id,
                    crate::services::options::OptionDeclaration {
                        name: "cached".into(),
                        label: "Use cache".into(),
                        option_type: OptionType::Boolean,
                        default: serde_json::json!(true),
                        requires_settings: Vec::new(),
                    },
                )
                .map_err(anyhow::Error::msg)?;
        }

        let db_write = STATE.db.write().await;
        let registry_read = registry.read().await;
        let response = build_provider_responses(vec![lyra_db::ProviderConfig {
            db_id: None,
            provider_id: provider_id.clone(),
            position: 0,
            enabled: true,
        }]);
        tokio::pin!(response);
        assert!(futures::poll!(&mut response).is_pending());

        let registry_writer = registry.write();
        tokio::pin!(registry_writer);
        assert!(futures::poll!(&mut registry_writer).is_pending());
        drop(registry_read);
        let mut registry_write =
            tokio::time::timeout(std::time::Duration::from_secs(1), &mut registry_writer)
                .await
                .expect("provider response retained the registry while waiting for the database");
        registry_write
            .declare_option(
                &provider_id,
                crate::services::options::OptionDeclaration {
                    name: "new-option".into(),
                    label: "Added after snapshot".into(),
                    option_type: OptionType::Boolean,
                    default: serde_json::json!(false),
                    requires_settings: Vec::new(),
                },
            )
            .map_err(anyhow::Error::msg)?;
        drop(registry_write);
        drop(db_write);

        let response = tokio::time::timeout(std::time::Duration::from_secs(1), response).await?;
        assert_eq!(response.len(), 1);
        assert_eq!(response[0].provider_id, provider_id);
        assert_eq!(response[0].display_name, "Lock Test Provider");
        assert_eq!(response[0].options.len(), 1);
        assert_eq!(response[0].options[0].name, "cached");
        assert!(response[0].options[0].available);
        crate::services::providers::teardown_plugin_providers(&plugin_id).await;
        Ok(())
    }

    fn insert_track(
        db: &mut DbAny,
        title: &str,
        disc: Option<u32>,
        track: Option<u32>,
    ) -> anyhow::Result<DbId> {
        let track = Track {
            db_id: None,
            id: nanoid!(),
            track_title: title.to_string(),
            sort_title: None,
            year: None,
            disc,
            disc_total: None,
            track,
            track_total: None,
            duration_ms: None,
            track_gain_db: None,
            album_gain_db: None,
            locked: None,
            created_at_ms: None,
            file_created_at_ms: None,
        };
        let qr = db.exec_mut(QueryBuilder::insert().element(&track).query())?;
        let track_db_id = qr
            .elements
            .first()
            .map(|element| element.id)
            .ok_or_else(|| anyhow!("track insert missing id"))?;
        roots::link(db, Root::Tracks, track_db_id)?;
        Ok(track_db_id)
    }

    fn connect_track_to_entry_source(
        db: &mut DbAny,
        track_db_id: DbId,
        entry_db_id: DbId,
    ) -> anyhow::Result<()> {
        let source_key = format!("entry:{}:embedded", entry_db_id.0);
        db.transaction_mut(|t| {
            lyra_db::track_sources::upsert(
                t,
                track_db_id,
                entry_db_id,
                lyra_db::track_sources::TrackSourceUpsert {
                    source_kind: "embedded_tags".to_string(),
                    source_key,
                    is_primary: true,
                    start_ms: None,
                    end_ms: None,
                    format: Default::default(),
                },
                None,
            )
        })?;
        Ok(())
    }

    fn insert_entry(db: &mut DbAny, full_path: &str) -> anyhow::Result<DbId> {
        let entry = Entry {
            db_id: None,
            id: nanoid!(),
            full_path: PathBuf::from(full_path),
            kind: lyra_db::entries::EntryKind::File,
            file_kind: Some("audio".to_string()),
            name: PathBuf::from(full_path)
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or(full_path)
                .to_string(),
            hash: None,
            size: 0,
            file_modified_at_ms: 0,
            file_created_at_ms: 0,
            synced_at_ms: None,
        };
        let qr = db.exec_mut(QueryBuilder::insert().element(&entry).query())?;
        qr.elements
            .first()
            .map(|element| element.id)
            .ok_or_else(|| anyhow!("entry insert missing id"))
    }

    #[test]
    fn build_release_context_sorts_tracks_for_refresh() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_db_id = insert_release(&mut db, "Sorted Release")?;

        let track_2 = insert_track(&mut db, "Track 2", Some(1), Some(2))?;
        let disc_2_track_1 = insert_track(&mut db, "Disc 2 Track 1", Some(2), Some(1))?;
        let track_1 = insert_track(&mut db, "Track 1", Some(1), Some(1))?;

        connect(&mut db, release_db_id, track_2)?;
        connect(&mut db, release_db_id, disc_2_track_1)?;
        connect(&mut db, release_db_id, track_1)?;

        let context = build_release_context(&db, release_db_id, None, &IdSchemes::default())?;
        let tracks = context
            .get("tracks")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("tracks missing from release context"))?;

        let titles: Vec<&str> = tracks
            .iter()
            .filter_map(|track| track.get("track_title").and_then(Value::as_str))
            .collect();
        assert_eq!(titles, vec!["Track 1", "Track 2", "Disc 2 Track 1"]);

        Ok(())
    }

    #[test]
    fn build_release_context_includes_track_external_ids() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_db_id = insert_release(&mut db, "IDs Release")?;
        let track_db_id = insert_track(&mut db, "Track With ID", Some(1), Some(1))?;
        connect(&mut db, release_db_id, track_db_id)?;

        lyra_db::external_ids::upsert(
            &mut db,
            track_db_id,
            "musicbrainz",
            "recording_id",
            "recording-123",
            IdSource::Resolved,
        )?;

        let context = build_release_context(&db, release_db_id, None, &IdSchemes::default())?;
        let tracks = context
            .get("tracks")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("tracks missing from release context"))?;
        assert_eq!(tracks.len(), 1);

        let external_ids = tracks[0]
            .get("external_ids")
            .and_then(Value::as_object)
            .ok_or_else(|| anyhow!("track external_ids missing"))?;
        let musicbrainz_ids = external_ids
            .get("musicbrainz")
            .and_then(Value::as_object)
            .ok_or_else(|| anyhow!("musicbrainz track external_ids missing"))?;
        assert_eq!(
            musicbrainz_ids.get("recording_id").and_then(Value::as_str),
            Some("recording-123")
        );

        Ok(())
    }

    #[test]
    fn build_release_context_lists_manual_ids_apart_from_resolved_ids() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_db_id = insert_release(&mut db, "Manual Release")?;
        let track_db_id = insert_track(&mut db, "Plugin Track", Some(1), Some(1))?;
        connect(&mut db, release_db_id, track_db_id)?;

        lyra_db::external_ids::upsert(
            &mut db,
            release_db_id,
            "musicbrainz",
            "release_id",
            "plugin-release",
            IdSource::Resolved,
        )?;
        lyra_db::external_ids::upsert(
            &mut db,
            release_db_id,
            "musicbrainz",
            "release_id",
            "user-release",
            IdSource::Manual,
        )?;
        lyra_db::external_ids::upsert(
            &mut db,
            track_db_id,
            "musicbrainz",
            "recording_id",
            "plugin-recording",
            IdSource::Resolved,
        )?;

        let context = build_release_context(&db, release_db_id, None, &IdSchemes::default())?;
        assert_eq!(
            context.pointer("/external_ids/musicbrainz/release_id"),
            Some(&Value::from("user-release"))
        );
        assert_eq!(
            context.get("manual_ids"),
            Some(&serde_json::json!({ "musicbrainz": { "release_id": "user-release" } }))
        );
        assert_eq!(context.pointer("/tracks/0/manual_ids"), None);

        Ok(())
    }

    #[test]
    fn build_release_context_includes_lookup_hints() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_db_id = insert_release(&mut db, "Lookup Hints Release")?;
        let track_db_id = insert_track(&mut db, "Track With Path", Some(1), Some(1))?;
        let entry_db_id = insert_entry(
            &mut db,
            "/music/Aimer - Daydream (2016) [FLAC]/01 - Insane Dream.flac",
        )?;
        connect(&mut db, release_db_id, track_db_id)?;
        connect_track_to_entry_source(&mut db, track_db_id, entry_db_id)?;

        let context = build_release_context(&db, release_db_id, None, &IdSchemes::default())?;

        let album_hints = context
            .get("lookup_hints")
            .and_then(Value::as_object)
            .ok_or_else(|| anyhow!("release lookup_hints missing"))?;
        assert_eq!(
            album_hints.get("artist_name").and_then(Value::as_str),
            Some("Aimer")
        );
        assert_eq!(
            album_hints.get("release_title").and_then(Value::as_str),
            Some("Daydream")
        );
        assert_eq!(album_hints.get("year").and_then(Value::as_u64), Some(2016));

        let tracks = context
            .get("tracks")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("tracks missing from release context"))?;
        assert_eq!(tracks.len(), 1);

        let track_hints = tracks[0]
            .get("lookup_hints")
            .and_then(Value::as_object)
            .ok_or_else(|| anyhow!("track lookup_hints missing"))?;
        assert_eq!(
            track_hints.get("artist_name").and_then(Value::as_str),
            Some("Aimer")
        );
        assert_eq!(
            track_hints.get("release_title").and_then(Value::as_str),
            Some("Daydream")
        );
        assert_eq!(track_hints.get("year").and_then(Value::as_u64), Some(2016));

        Ok(())
    }

    #[test]
    fn build_release_context_uses_library_root_for_lookup_hints() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let library_db_id = insert_library(&mut db, "Test Library", "/mnt/mini/music/japanese")?;
        let release_db_id = insert_release(&mut db, "Lookup Hints Release")?;
        let track_db_id = insert_track(&mut db, "Track With Path", Some(1), Some(1))?;
        let entry_db_id = insert_entry(
            &mut db,
            "/mnt/mini/music/japanese/Green Apelsin/Северный ветер [2021]/01 - Северный ветер.flac",
        )?;
        connect(&mut db, release_db_id, track_db_id)?;
        connect_track_to_entry_source(&mut db, track_db_id, entry_db_id)?;

        let context = build_release_context(
            &db,
            release_db_id,
            Some(library_db_id),
            &IdSchemes::default(),
        )?;

        let album_hints = context
            .get("lookup_hints")
            .and_then(Value::as_object)
            .ok_or_else(|| anyhow!("release lookup_hints missing"))?;
        assert_eq!(
            album_hints.get("artist_name").and_then(Value::as_str),
            Some("Green Apelsin")
        );
        assert_eq!(
            album_hints.get("release_title").and_then(Value::as_str),
            Some("Северный ветер")
        );
        assert_eq!(album_hints.get("year").and_then(Value::as_u64), Some(2021));

        let tracks = context
            .get("tracks")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("tracks missing from release context"))?;
        assert_eq!(tracks.len(), 1);

        let track_hints = tracks[0]
            .get("lookup_hints")
            .and_then(Value::as_object)
            .ok_or_else(|| anyhow!("track lookup_hints missing"))?;
        assert_eq!(
            track_hints.get("artist_name").and_then(Value::as_str),
            Some("Green Apelsin")
        );
        assert_eq!(
            track_hints.get("release_title").and_then(Value::as_str),
            Some("Северный ветер")
        );
        assert_eq!(track_hints.get("year").and_then(Value::as_u64), Some(2021));

        Ok(())
    }

    #[test]
    fn build_release_context_tracks_use_album_artist_fallback() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_db_id = insert_release(&mut db, "Fallback Release")?;
        let track_db_id = insert_track(&mut db, "Track Missing Direct Artist", Some(1), Some(1))?;
        let artist_db_id = insert_artist(&mut db, "Fallback Artist")?;
        connect(&mut db, release_db_id, track_db_id)?;
        connect(&mut db, release_db_id, artist_db_id)?;

        let context = build_release_context(&db, release_db_id, None, &IdSchemes::default())?;
        let tracks = context
            .get("tracks")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("tracks missing from release context"))?;
        assert_eq!(tracks.len(), 1);

        let artists = tracks[0]
            .get("artists")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("track artists missing"))?;
        assert_eq!(artists.len(), 1);
        assert_eq!(
            artists[0].get("artist_name").and_then(Value::as_str),
            Some("Fallback Artist")
        );

        Ok(())
    }

    #[test]
    fn build_release_context_preserves_album_artist_order() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_db_id = insert_release(&mut db, "Ordered Release")?;
        let alphabetic_artist_db_id = insert_artist(&mut db, "Alpha Artist")?;
        let lead_artist_db_id = insert_artist(&mut db, "Zulu Artist")?;
        lyra_db::fixtures::connect_credit(
            &mut db,
            release_db_id,
            lead_artist_db_id,
            lyra_db::CreditType::Artist,
            None,
            0,
        )?;
        lyra_db::fixtures::connect_credit(
            &mut db,
            release_db_id,
            alphabetic_artist_db_id,
            lyra_db::CreditType::Artist,
            None,
            1,
        )?;

        let context = build_release_context(&db, release_db_id, None, &IdSchemes::default())?;
        let artists = context
            .get("artists")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("artists missing from release context"))?;

        let names: Vec<&str> = artists
            .iter()
            .filter_map(|artist| artist.get("artist_name").and_then(Value::as_str))
            .collect();
        assert_eq!(names, vec!["Zulu Artist", "Alpha Artist"]);

        Ok(())
    }

    #[test]
    fn build_release_context_preserves_direct_track_artist_order() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_db_id = insert_release(&mut db, "Track Artist Order Release")?;
        let track_db_id = insert_track(&mut db, "Ordered Track", Some(1), Some(1))?;
        let alphabetic_artist_db_id = insert_artist(&mut db, "Alpha Artist")?;
        let lead_artist_db_id = insert_artist(&mut db, "Zulu Artist")?;
        connect(&mut db, release_db_id, track_db_id)?;
        lyra_db::fixtures::connect_credit(
            &mut db,
            track_db_id,
            lead_artist_db_id,
            lyra_db::CreditType::Artist,
            None,
            0,
        )?;
        lyra_db::fixtures::connect_credit(
            &mut db,
            track_db_id,
            alphabetic_artist_db_id,
            lyra_db::CreditType::Artist,
            None,
            1,
        )?;

        let context = build_release_context(&db, release_db_id, None, &IdSchemes::default())?;
        let tracks = context
            .get("tracks")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("tracks missing from release context"))?;
        let artists = tracks[0]
            .get("artists")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("track artists missing"))?;

        let names: Vec<&str> = artists
            .iter()
            .filter_map(|artist| artist.get("artist_name").and_then(Value::as_str))
            .collect();
        assert_eq!(names, vec!["Zulu Artist", "Alpha Artist"]);

        Ok(())
    }

    #[test]
    fn build_release_context_track_artist_fallback_uses_album_artist_order() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_db_id = insert_release(&mut db, "Fallback Order Release")?;
        let track_db_id = insert_track(&mut db, "Track Missing Direct Artist", Some(1), Some(1))?;
        let alphabetic_artist_db_id = insert_artist(&mut db, "Alpha Artist")?;
        let lead_artist_db_id = insert_artist(&mut db, "Zulu Artist")?;
        connect(&mut db, release_db_id, track_db_id)?;
        lyra_db::fixtures::connect_credit(
            &mut db,
            release_db_id,
            lead_artist_db_id,
            lyra_db::CreditType::Artist,
            None,
            0,
        )?;
        lyra_db::fixtures::connect_credit(
            &mut db,
            release_db_id,
            alphabetic_artist_db_id,
            lyra_db::CreditType::Artist,
            None,
            1,
        )?;

        let context = build_release_context(&db, release_db_id, None, &IdSchemes::default())?;
        let tracks = context
            .get("tracks")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("tracks missing from release context"))?;
        let artists = tracks[0]
            .get("artists")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("track artists missing"))?;

        let names: Vec<&str> = artists
            .iter()
            .filter_map(|artist| artist.get("artist_name").and_then(Value::as_str))
            .collect();
        assert_eq!(names, vec!["Zulu Artist", "Alpha Artist"]);

        Ok(())
    }

    #[tokio::test]
    async fn refreshing_an_entity_takes_sync_metadata_and_sight_of_it() -> anyhow::Result<()> {
        use axum::response::IntoResponse;

        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::plugins::testing::init_default_test_state()?;
        let (syncer, listener, shown, hidden) = {
            let mut db = STATE.db.write().await;
            let user = |db: &mut DbAny, name: &str, permissions| -> anyhow::Result<DbId> {
                let user = lyra_db::users::create(db, &lyra_db::fixtures::test_user(name)?)?;
                let role = lyra_db::roles::create(
                    db,
                    &lyra_db::roles::Role {
                        db_id: None,
                        id: nanoid!(),
                        name: format!("{name}-role"),
                        permissions,
                    },
                )?;
                lyra_db::roles::assign_role_to_user(db, user, role)?;
                Ok(user)
            };
            let syncer = user(&mut db, "syncer", vec![lyra_db::Permission::SyncMetadata])?;
            let listener = user(&mut db, "listener", Vec::new())?;
            let shown_library = lyra_db::test_db::insert_library(&mut db, "Shown", "/shown")?;
            let hidden_library = lyra_db::test_db::insert_library(&mut db, "Hidden", "/hidden")?;
            for user in [syncer, listener] {
                lyra_db::libraries::grant_access(
                    &mut db,
                    user,
                    shown_library,
                    lyra_db::libraries::AccessKind::ReadWrite,
                )?;
            }
            let shown = insert_release(&mut db, "Shown")?;
            let hidden = insert_release(&mut db, "Hidden")?;
            connect(&mut db, shown_library, shown)?;
            connect(&mut db, hidden_library, hidden)?;
            let public = |id| lyra_db::lookup::find_id_by_db_id(&*db, id);
            let (shown, hidden) = (public(shown)?, public(hidden)?);
            (syncer, listener, shown.expect("id"), hidden.expect("id"))
        };
        let token = |user| async move {
            let session =
                crate::services::testing::create_session(user, Default::default()).await?;
            let mut headers = HeaderMap::new();
            headers.insert(
                "Authorization",
                format!("Bearer {}", session.token).parse()?,
            );
            anyhow::Ok(headers)
        };
        let status = |headers: HeaderMap, id: String| async move {
            match refresh_entity(headers, Path(id), Query(RefreshEntityQuery::default())).await {
                Ok(Json(response)) => {
                    assert!(response.providers_called.is_empty());
                    StatusCode::OK
                }
                Err(error) => error.into_response().status(),
            }
        };

        let syncer = token(syncer).await?;
        assert_eq!(status(syncer.clone(), shown.clone()).await, StatusCode::OK);
        assert_eq!(status(syncer.clone(), hidden).await, StatusCode::NOT_FOUND);
        assert_eq!(
            status(syncer, "missing".to_string()).await,
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            status(token(listener).await?, shown).await,
            StatusCode::FORBIDDEN
        );
        Ok(())
    }
}
