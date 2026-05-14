// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashMap;

use aide::axum::ApiRouter;
use aide::transform::TransformOperation;
use axum::{
    Json,
    extract::{
        Path,
        Query,
    },
    http::HeaderMap,
};
use schemars::JsonSchema;
use serde::{
    Deserialize,
    Serialize,
};
use serde_json::Value;

use crate::{
    STATE,
    db,
    routes::AppError,
    services::{
        EntityRefreshMode,
        EntityType,
        NormalizedProviderArtistSearchResult,
        NormalizedProviderReleaseSearchResult,
        NormalizedProviderSearchResult,
        NormalizedProviderTrackSearchResult,
        ProviderSearchError,
        ProviderSearchRequest,
        auth::{
            require_manage_metadata,
            require_manage_providers,
            require_sync_metadata,
        },
        options::OptionType,
        providers::PROVIDER_REGISTRY,
        providers::{
            EntityExternalIdRecord,
            SetEntityExternalIdRequest as ProviderSetEntityExternalIdRequest,
            list_entity_external_ids as list_entity_external_ids_service,
            list_provider_configs,
            refresh_entity_by_id,
            set_entity_external_id as set_entity_external_id_service,
            set_entity_locked,
            update_provider_priority as update_provider_priority_service,
        },
        run_provider_sync,
        search_provider as search_provider_results,
    },
};

#[derive(Serialize, JsonSchema)]
#[non_exhaustive]
pub struct OptionResponse {
    pub name: String,
    pub label: String,
    #[serde(rename = "type")]
    pub option_type: String,
    pub default: serde_json::Value,
    pub available: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unavailable_reason: Option<String>,
}

#[derive(Serialize, JsonSchema)]
#[non_exhaustive]
pub struct ProviderResponse {
    pub provider_id: String,
    pub display_name: String,
    pub priority: u32,
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
        }
    }
}

#[derive(Serialize, JsonSchema)]
#[serde(tag = "entity_type", rename_all = "lowercase")]
pub enum SearchResult {
    #[serde(rename = "release")]
    Release(ReleaseSearchResult),
    #[serde(rename = "artist")]
    Artist(ArtistSearchResult),
    #[serde(rename = "track")]
    Track(TrackSearchResult),
}

#[derive(Clone, Copy, Deserialize, Serialize, JsonSchema)]
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

#[derive(Serialize, JsonSchema)]
#[non_exhaustive]
pub struct ReleaseSearchResult {
    pub title: String,
    pub redirect_url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artist_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(description = "Release date as YYYY, YYYY-MM, or YYYY-MM-DD.")]
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

#[derive(Serialize, JsonSchema)]
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

#[derive(Serialize, JsonSchema)]
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

#[derive(Deserialize, JsonSchema)]
#[non_exhaustive]
pub struct SearchQuery {
    #[serde(rename = "type")]
    pub entity_type: SearchEntityType,
    pub q: String,
    #[serde(default)]
    #[schemars(
        description = "Resolve provider cover URLs for release or artist search results using provider cover handlers."
    )]
    pub include_cover_urls: bool,
    #[serde(default)]
    #[schemars(description = "Bypass cached provider cover resolution and refresh it.")]
    pub force_refresh: bool,
}

#[derive(Deserialize, JsonSchema)]
pub struct UpdatePriorityRequest {
    pub priority: u32,
}

#[derive(Deserialize, JsonSchema)]
#[non_exhaustive]
pub struct SetExternalIdRequest {
    pub id_value: String,
}

#[derive(Serialize, JsonSchema)]
#[non_exhaustive]
pub struct ExternalIdResponse {
    pub provider_id: String,
    pub id_type: String,
    pub id_value: String,
    pub source: String,
}

async fn list_providers(headers: HeaderMap) -> Result<Json<Vec<ProviderResponse>>, AppError> {
    let _principal = require_manage_metadata(&headers).await?;

    let providers = list_provider_configs().await?;
    let registry = PROVIDER_REGISTRY.read().await;
    let db = STATE.db.read().await;

    let response: Vec<ProviderResponse> = providers
        .into_iter()
        .map(|config| {
            let options = registry
                .get_options(&config.provider_id)
                .iter()
                .map(|opt| {
                    let (available, unavailable_reason) = if opt.requires_settings.is_empty() {
                        (true, None)
                    } else {
                        check_settings_availability(
                            &db,
                            &config.provider_id,
                            &opt.requires_settings,
                        )
                    };
                    OptionResponse {
                        name: opt.name.clone(),
                        label: opt.label.clone(),
                        option_type: match &opt.option_type {
                            OptionType::Boolean => "boolean".to_string(),
                            OptionType::String => "string".to_string(),
                            OptionType::Number => "number".to_string(),
                        },
                        default: opt.default.clone(),
                        available,
                        unavailable_reason,
                    }
                })
                .collect();
            ProviderResponse {
                provider_id: config.provider_id,
                display_name: config.display_name,
                priority: config.priority,
                enabled: config.enabled,
                options,
            }
        })
        .collect();
    Ok(Json(response))
}

/// Checks whether all required settings for an option have non-empty values.
fn check_settings_availability(
    db: &agdb::DbAny,
    plugin_id: &str,
    required_settings: &[String],
) -> (bool, Option<String>) {
    let plugin_settings = match db::settings::find_plugin_settings_with(db, plugin_id) {
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
    let all_entries = match db::settings::get_all_settings_with(db, plugin_db_id) {
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
    let _principal = require_manage_metadata(&headers).await?;

    let results = search_provider_results(ProviderSearchRequest {
        provider_id: &provider_id,
        entity_type: query.entity_type.into(),
        query: &query.q,
        include_cover_urls: query.include_cover_urls,
        force_refresh: query.force_refresh,
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

async fn update_provider_priority(
    headers: HeaderMap,
    Path(provider_id): Path<String>,
    Json(request): Json<UpdatePriorityRequest>,
) -> Result<Json<ProviderResponse>, AppError> {
    let _principal = require_manage_providers(&headers).await?;

    let config = update_provider_priority_service(&provider_id, request.priority).await?;
    Ok(Json(ProviderResponse {
        provider_id: config.provider_id,
        display_name: config.display_name,
        priority: config.priority,
        enabled: config.enabled,
        options: Vec::new(),
    }))
}

async fn get_entity_external_ids(
    headers: HeaderMap,
    Path(node_id): Path<String>,
) -> Result<Json<Vec<ExternalIdResponse>>, AppError> {
    let _principal = require_manage_metadata(&headers).await?;

    let response: Vec<ExternalIdResponse> = list_entity_external_ids_service(&node_id)
        .await?
        .into_iter()
        .map(Into::into)
        .collect();

    Ok(Json(response))
}

async fn set_entity_external_id(
    headers: HeaderMap,
    Path((node_id, provider_id, id_type)): Path<(String, String, String)>,
    Json(request): Json<SetExternalIdRequest>,
) -> Result<Json<ExternalIdResponse>, AppError> {
    let _principal = require_manage_metadata(&headers).await?;

    let record = set_entity_external_id_service(
        &node_id,
        ProviderSetEntityExternalIdRequest {
            provider_id,
            id_type,
            id_value: request.id_value,
        },
    )
    .await?;

    Ok(Json(record.into()))
}

async fn lock_entity(
    headers: HeaderMap,
    Path(node_id): Path<String>,
) -> Result<Json<serde_json::Value>, AppError> {
    let _principal = require_manage_metadata(&headers).await?;
    set_entity_locked(&node_id, true).await?;

    Ok(Json(serde_json::json!({ "locked": true })))
}

async fn unlock_entity(
    headers: HeaderMap,
    Path(node_id): Path<String>,
) -> Result<Json<serde_json::Value>, AppError> {
    let _principal = require_manage_metadata(&headers).await?;
    set_entity_locked(&node_id, false).await?;

    Ok(Json(serde_json::json!({ "locked": false })))
}

#[derive(Serialize, JsonSchema)]
#[non_exhaustive]
pub struct RefreshResponse {
    pub refreshed: bool,
    pub entity_type: EntityType,
    pub providers_called: Vec<String>,
}

#[derive(Deserialize, JsonSchema, Default)]
pub struct RefreshEntityQuery {
    #[serde(default)]
    #[schemars(
        description = "Replace existing cover image with provider results for release or artist entity refresh."
    )]
    pub replace_cover: bool,
    #[serde(default)]
    #[schemars(description = "Bypass cached provider cover resolution and refresh it.")]
    pub force_refresh: bool,
    #[serde(flatten)]
    #[schemars(skip)]
    pub extra: HashMap<String, String>,
}

const KNOWN_REFRESH_PARAMS: &[&str] = &["replace_cover", "force_refresh"];

async fn refresh_entity(
    headers: HeaderMap,
    Path(node_id): Path<String>,
    Query(query): Query<RefreshEntityQuery>,
) -> Result<Json<RefreshResponse>, AppError> {
    let _principal = require_sync_metadata(&headers).await?;
    let mut options: HashMap<String, String> = query.extra;
    options.retain(|key, _| !KNOWN_REFRESH_PARAMS.contains(&key.as_str()));
    let result = refresh_entity_by_id(
        &node_id,
        EntityRefreshMode::WithReleaseArtifacts {
            replace_cover: query.replace_cover,
            force_refresh: query.force_refresh,
            options,
        },
    )
    .await?;

    Ok(Json(RefreshResponse {
        refreshed: true,
        entity_type: result.entity_type,
        providers_called: result.providers_called,
    }))
}

fn list_providers_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List providers").description(
        "Returns all registered metadata providers with their priorities and option availability. Requires ManageMetadata permission.",
    )
}

fn search_provider_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Search provider").description(
        "Invokes a provider's search handler for a given entity type and query.\n\n\
        Request body: `{ type: \"release\"|\"artist\"|\"track\", q: string }`.\n\
        `raw` always contains the original provider payload, and `title` is normalized from provider keys in priority order: `title`, `album_title`, `track_title`, `artist_name`, `name`.\n\n\
        Set `include_cover_urls=true` to resolve `cover_url` for release and artist results via provider cover handlers. Requires ManageMetadata permission.",
    )
}

fn update_priority_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Update provider priority").description(
        "Updates the priority of a metadata provider. Higher priority providers take precedence. Requires ManageProviders permission.",
    )
}

fn get_external_ids_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Get entity external IDs").description(
        "Returns all external IDs associated with an entity. Requires ManageMetadata permission.",
    )
}

fn set_external_id_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Set entity external ID").description(
        "Creates or replaces one external ID on an entity, keyed by `provider_id` and `id_type` in the path. User-set IDs take priority over plugin-set IDs. Requires ManageMetadata permission.",
    )
}

fn lock_entity_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Lock entity")
        .description("Locks an entity to prevent automatic metadata updates from providers. Requires ManageMetadata permission.")
}

fn unlock_entity_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Unlock entity")
        .description("Unlocks an entity to allow automatic metadata updates from providers. Requires ManageMetadata permission.")
}

fn refresh_entity_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Refresh entity metadata").description(
        "Triggers a metadata refresh for an entity from all enabled providers.\n\n\
            For release and artist entities, cover downloads replace only missing covers by default.\
            Set `replace_cover=true` to overwrite an existing cover. Requires SyncMetadata permission.",
    )
}

#[derive(Serialize, JsonSchema)]
pub struct SyncResponse {
    pub started: bool,
    pub provider_id: String,
}

async fn sync_provider(
    headers: HeaderMap,
    Path(provider_id): Path<String>,
) -> Result<Json<SyncResponse>, AppError> {
    let _principal = require_sync_metadata(&headers).await?;

    run_provider_sync(&provider_id).await?;

    Ok(Json(SyncResponse {
        started: true,
        provider_id,
    }))
}

fn sync_provider_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Sync provider").description(
        "Triggers a full sync for a provider. Requires SyncMetadata permission. Returns 409 if a sync is already running.",
    )
}

pub fn provider_routes() -> ApiRouter {
    use aide::axum::routing::{
        get_with,
        post_with,
        put_with,
    };

    ApiRouter::new()
        .api_route("/", get_with(list_providers, list_providers_docs))
        .api_route(
            "/{id}/search",
            post_with(search_provider, search_provider_docs),
        )
        .api_route(
            "/{id}/priority",
            put_with(update_provider_priority, update_priority_docs),
        )
        .api_route("/{id}/sync", post_with(sync_provider, sync_provider_docs))
}

pub fn entity_routes() -> ApiRouter {
    use aide::axum::routing::{
        delete_with,
        get_with,
        post_with,
        put_with,
    };

    ApiRouter::new()
        .api_route(
            "/{id}/external-ids",
            get_with(get_entity_external_ids, get_external_ids_docs),
        )
        .api_route(
            "/{id}/external-ids/{provider_id}/{id_type}",
            put_with(set_entity_external_id, set_external_id_docs),
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
    use crate::db::{
        self,
        Entry,
        IdSource,
        Track,
    };
    use crate::services::entities::build_release_context;
    use agdb::{
        DbAny,
        DbId,
        QueryBuilder,
    };
    use anyhow::anyhow;
    use nanoid::nanoid;
    use std::path::PathBuf;

    use crate::db::test_db::{
        connect,
        insert_artist,
        insert_library,
        insert_release,
        new_test_db,
    };

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
            sample_rate_hz: None,
            channel_count: None,
            bit_depth: None,
            bitrate_bps: None,
            locked: None,
            created_at: None,
            ctime: None,
        };
        let qr = db.exec_mut(QueryBuilder::insert().element(&track).query())?;
        let track_db_id = qr
            .elements
            .first()
            .map(|element| element.id)
            .ok_or_else(|| anyhow!("track insert missing id"))?;
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from("tracks")
                .to(track_db_id)
                .query(),
        )?;
        Ok(track_db_id)
    }

    fn connect_artist_ordered(
        db: &mut DbAny,
        owner_db_id: DbId,
        artist_db_id: DbId,
        order: u64,
    ) -> anyhow::Result<()> {
        let credit = db::Credit {
            db_id: None,
            id: nanoid::nanoid!(),
            credit_type: db::CreditType::Artist,
            detail: None,
        };
        let insert_result = db.exec_mut(QueryBuilder::insert().element(&credit).query())?;
        let credit_db_id = insert_result
            .elements
            .first()
            .map(|e| e.id)
            .ok_or_else(|| anyhow::anyhow!("credit insert missing id"))?;
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from("credits")
                .to(credit_db_id)
                .query(),
        )?;
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(owner_db_id)
                .to(credit_db_id)
                .values_uniform([
                    ("owned", 1).into(),
                    (db::credits::EDGE_ORDER_KEY, order).into(),
                ])
                .query(),
        )?;
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(credit_db_id)
                .to(artist_db_id)
                .query(),
        )?;
        Ok(())
    }

    fn connect_track_to_entry_source(
        db: &mut DbAny,
        track_db_id: DbId,
        entry_db_id: DbId,
    ) -> anyhow::Result<()> {
        let source_key = format!("entry:{}:embedded", entry_db_id.0);
        db::track_sources::upsert(
            db,
            track_db_id,
            entry_db_id,
            db::track_sources::TrackSourceUpsert {
                source_kind: "embedded_tags".to_string(),
                source_key,
                is_primary: true,
                start_ms: None,
                end_ms: None,
            },
            None,
        )?;
        Ok(())
    }

    fn insert_entry(db: &mut DbAny, full_path: &str) -> anyhow::Result<DbId> {
        let entry = Entry {
            db_id: None,
            id: nanoid!(),
            full_path: PathBuf::from(full_path),
            kind: crate::db::entries::EntryKind::File,
            file_kind: Some("audio".to_string()),
            name: PathBuf::from(full_path)
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or(full_path)
                .to_string(),
            hash: None,
            size: 0,
            mtime: 0,
            ctime: 0,
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

        let context = build_release_context(&db, release_db_id, None)?;
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

        db::external_ids::upsert(
            &mut db,
            track_db_id,
            "musicbrainz",
            "recording_id",
            "recording-123",
            IdSource::Plugin,
        )?;

        let context = build_release_context(&db, release_db_id, None)?;
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

        let context = build_release_context(&db, release_db_id, None)?;

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

        let context = build_release_context(&db, release_db_id, Some(library_db_id))?;

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

        let context = build_release_context(&db, release_db_id, None)?;
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
        connect_artist_ordered(&mut db, release_db_id, lead_artist_db_id, 0)?;
        connect_artist_ordered(&mut db, release_db_id, alphabetic_artist_db_id, 1)?;

        let context = build_release_context(&db, release_db_id, None)?;
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
        connect_artist_ordered(&mut db, track_db_id, lead_artist_db_id, 0)?;
        connect_artist_ordered(&mut db, track_db_id, alphabetic_artist_db_id, 1)?;

        let context = build_release_context(&db, release_db_id, None)?;
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
        connect_artist_ordered(&mut db, release_db_id, lead_artist_db_id, 0)?;
        connect_artist_ordered(&mut db, release_db_id, alphabetic_artist_db_id, 1)?;

        let context = build_release_context(&db, release_db_id, None)?;
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
}
