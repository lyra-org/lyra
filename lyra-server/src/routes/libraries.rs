// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::path::PathBuf;

use aide::axum::{
    ApiRouter,
    routing::post_with,
};
use aide::transform::TransformOperation;
use anyhow::anyhow;
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
use nanoid::nanoid;
use schemars::JsonSchema;
use serde::{
    Deserialize,
    Serialize,
};

use crate::{
    STATE,
    db::{
        self,
        Library,
        Permission,
    },
    locale::{
        validate_country,
        validate_language,
    },
    routes::AppError,
    services::{
        LibraryRefreshOptions,
        auth::{
            require_authenticated,
            require_manage_libraries,
        },
        get_library_sync_state,
        refresh_library_metadata,
        releases,
        start_library_sync,
    },
};

use super::releases::{
    ReleaseQuery,
    detail_to_release_response,
    parse_release_includes,
};
use super::responses::ReleaseResponse;

#[derive(Deserialize, JsonSchema)]
struct LibraryRequest {
    #[schemars(description = "Human-friendly library name.")]
    #[serde(alias = "_name")]
    name: String,
    #[schemars(description = "Filesystem path to scan for media.")]
    #[serde(alias = "_directory")]
    directory: String,
    #[schemars(description = "ISO 639 language code (e.g. \"jpn\", \"en\", \"Japanese\").")]
    language: Option<String>,
    #[schemars(description = "ISO 3166 country code (e.g. \"JP\", \"US\", \"Japan\").")]
    country: Option<String>,
}

#[derive(Serialize, JsonSchema)]
struct LibraryResponse {
    id: String,
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    directory: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    language: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    country: Option<String>,
}

impl LibraryResponse {
    fn from_library(lib: Library, include_directory: bool) -> Self {
        Self {
            id: lib.id,
            name: lib.name,
            directory: include_directory.then_some(lib.directory),
            language: lib.language,
            country: lib.country,
        }
    }
}

#[derive(Deserialize, JsonSchema)]
struct LibraryUpdateRequest {
    #[schemars(description = "Updated library name.")]
    name: Option<String>,
    #[schemars(description = "Updated language code; set to null to clear.")]
    language: Option<Option<String>>,
    #[schemars(description = "Updated country code; set to null to clear.")]
    country: Option<Option<String>>,
}

#[derive(Serialize, JsonSchema)]
struct LibrarySyncStartResponse {
    started: bool,
    run_id: u64,
}

#[derive(Serialize, JsonSchema)]
struct LibraryRefreshResponse {
    refreshed_count: usize,
    entity_type: String,
}

#[derive(Deserialize, JsonSchema)]
struct LibraryRefreshQuery {
    #[serde(default)]
    #[schemars(
        description = "Replace existing cover images with downloaded provider results when set."
    )]
    replace_cover: bool,
    #[serde(default)]
    #[schemars(description = "Bypass cached provider cover resolution and refresh it.")]
    force_refresh: bool,
}

async fn refresh_library(
    headers: HeaderMap,
    Path(library_id): Path<String>,
    Query(query): Query<LibraryRefreshQuery>,
) -> Result<Json<LibraryRefreshResponse>, AppError> {
    let _principal = require_manage_libraries(&headers).await?;

    let library_db_id = {
        let db = STATE.db.read().await;
        db::lookup::find_node_id_by_id(&*db, &library_id)?
            .ok_or_else(|| AppError::not_found(format!("not found: {library_id}")))?
    };

    let options = LibraryRefreshOptions {
        replace_cover: query.replace_cover,
        force_refresh: query.force_refresh,
        apply_sync_filters: false,
        provider_id: None,
    };
    let refreshed_count = refresh_library_metadata(library_db_id, &options).await?;

    Ok(Json(LibraryRefreshResponse {
        refreshed_count,
        entity_type: "release".to_string(),
    }))
}

async fn create_library(
    headers: HeaderMap,
    Json(library): Json<LibraryRequest>,
) -> Result<StatusCode, AppError> {
    let _principal = require_manage_libraries(&headers).await?;

    let name = library.name.trim();
    if name.is_empty() {
        return Err(AppError::bad_request("library name cannot be empty"));
    }

    let directory = library.directory.trim();
    if directory.is_empty() {
        return Err(AppError::bad_request("library directory cannot be empty"));
    }

    let directory = PathBuf::from(directory);
    if !directory.is_dir() {
        return Err(AppError::bad_request(format!(
            "library directory not found: {}",
            directory.display()
        )));
    }

    let language = library
        .language
        .map(|l| validate_language(&l))
        .transpose()
        .map_err(|e| AppError::bad_request(e.to_string()))?;

    let country = library
        .country
        .map(|c| validate_country(&c))
        .transpose()
        .map_err(|e| AppError::bad_request(e.to_string()))?;

    {
        let db_read = STATE.db.read().await;
        let existing = db::libraries::get(&db_read)?;
        if existing.iter().any(|lib| lib.directory == directory) {
            return Err(AppError::bad_request(format!(
                "a library already exists for directory: {}",
                directory.display()
            )));
        }
    }

    let library = {
        let mut db_write = STATE.db.write().await;
        db::libraries::create(
            &mut db_write,
            &Library {
                db_id: None,
                id: nanoid!(),
                name: name.to_string(),
                directory,
                language,
                country,
            },
        )?
    };

    let library_db_id = library
        .db_id
        .ok_or_else(|| anyhow!("library insert missing id"))?;
    match start_library_sync(STATE.db.get(), library).await? {
        crate::services::StartLibrarySyncResult::Started { run_id } => {
            tracing::info!(library_id = library_db_id.0, run_id, "started library sync");
            Ok(StatusCode::ACCEPTED)
        }
        crate::services::StartLibrarySyncResult::AlreadyRunning { run_id } => {
            Err(AppError::conflict(format!(
                "library sync already running for id {} (run_id={})",
                library_db_id.0, run_id
            )))
        }
    }
}

async fn update_library(
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(update): Json<LibraryUpdateRequest>,
) -> Result<Json<LibraryResponse>, AppError> {
    let _principal = require_manage_libraries(&headers).await?;

    if update.name.is_none() && update.language.is_none() && update.country.is_none() {
        return Err(AppError::bad_request("no library fields provided"));
    }

    let mut db = STATE.db.write().await;
    let library_db_id = db::lookup::find_node_id_by_id(&*db, &id)?
        .ok_or_else(|| AppError::not_found(format!("not found: {id}")))?;
    let library = db::libraries::get_by_id(&db, library_db_id)?
        .ok_or_else(|| AppError::not_found(format!("Library not found: {}", id)))?;

    let mut updated_name = library.name;
    let mut updated_language = library.language;
    let mut updated_country = library.country;

    if let Some(name) = update.name {
        if name.trim().is_empty() {
            return Err(AppError::bad_request("name cannot be empty"));
        }
        updated_name = name;
    }

    let mut clear_language = false;
    if let Some(language) = update.language {
        match language {
            Some(value) => {
                updated_language = Some(
                    validate_language(&value).map_err(|e| AppError::bad_request(e.to_string()))?,
                );
            }
            None => {
                updated_language = None;
                clear_language = true;
            }
        }
    }

    let mut clear_country = false;
    if let Some(country) = update.country {
        match country {
            Some(value) => {
                updated_country = Some(
                    validate_country(&value).map_err(|e| AppError::bad_request(e.to_string()))?,
                );
            }
            None => {
                updated_country = None;
                clear_country = true;
            }
        }
    }

    let updated = Library {
        db_id: Some(library_db_id),
        id: library.id,
        name: updated_name,
        directory: library.directory,
        language: updated_language,
        country: updated_country,
    };

    db::libraries::update(&mut db, &updated, clear_language, clear_country)?;

    Ok(Json(LibraryResponse::from_library(updated, true)))
}

fn create_library_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Create library")
        .description(
            "Creates a new library entry and starts background ingestion. Returns 202 when accepted.",
        )
        .response::<202, ()>()
}

fn update_library_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Update library").description(
        "Updates library name, language, and country. Set language or country to null to clear.",
    )
}

async fn list_libraries(headers: HeaderMap) -> Result<Json<Vec<LibraryResponse>>, AppError> {
    let principal = require_authenticated(&headers).await?;
    let include_directory =
        db::roles::has_permission(&principal.permissions, Permission::ManageLibraries);

    let db = STATE.db.read().await;
    let libraries = db::libraries::get(&db)?;
    let response: Vec<LibraryResponse> = libraries
        .into_iter()
        .map(|library| LibraryResponse::from_library(library, include_directory))
        .collect();
    Ok(Json(response))
}

fn list_libraries_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List libraries").description(
        "Returns all libraries. `directory` is included only for authenticated users with ManageLibraries permission.",
    )
}

async fn get_library_releases(
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<ReleaseQuery>,
) -> Result<Json<Vec<ReleaseResponse>>, AppError> {
    let principal = require_authenticated(&headers).await?;
    let include_entry_paths =
        db::roles::has_permission(&principal.permissions, Permission::ManageLibraries);

    let db = &*STATE.db.read().await;
    let library_db_id = db::lookup::find_node_id_by_id(db, &id)?
        .ok_or_else(|| AppError::not_found(format!("not found: {id}")))?;
    db::libraries::get_by_id(db, library_db_id)?
        .ok_or_else(|| AppError::not_found(format!("Library not found: {}", id)))?;

    let (includes, include_covers, include_genres) = parse_release_includes(query.inc)?;
    let details = releases::list_details_for_scope(db, library_db_id, includes)?;

    let mut response = Vec::with_capacity(details.len());
    for detail in details {
        response.push(detail_to_release_response(
            db,
            detail,
            include_covers,
            include_genres,
            include_entry_paths,
        )?);
    }

    Ok(Json(response))
}

fn get_library_releases_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List library releases").description(
        "Returns releases belonging to a library. Use `inc` to include artists, tracks, entries, and/or covers. When `inc=entries`, `full_path` is included only for authenticated users with ManageLibraries permission.",
    )
}

fn refresh_library_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Refresh library metadata")
        .description(
            "Triggers a metadata refresh for all releases in a library from all enabled providers.\n\
            Set `replace_cover` to true to overwrite existing cover images when provider results include cover URLs.",
        )
}

async fn get_library_sync_status(
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<crate::services::LibrarySyncState>, AppError> {
    let _principal = require_manage_libraries(&headers).await?;

    let library_db_id = {
        let db = STATE.db.read().await;
        let library_db_id = db::lookup::find_node_id_by_id(&*db, &id)?
            .ok_or_else(|| AppError::not_found(format!("not found: {id}")))?;
        db::libraries::get_by_id(&db, library_db_id)?
            .ok_or_else(|| AppError::not_found(format!("Library not found: {}", id)))?;
        library_db_id
    };

    let status = get_library_sync_state(library_db_id).await;
    Ok(Json(status))
}

async fn start_library_sync_for_library(
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<LibrarySyncStartResponse>, AppError> {
    let _principal = require_manage_libraries(&headers).await?;
    let library = {
        let db = STATE.db.read().await;
        let library_db_id = db::lookup::find_node_id_by_id(&*db, &id)?
            .ok_or_else(|| AppError::not_found(format!("not found: {id}")))?;
        db::libraries::get_by_id(&db, library_db_id)?
            .ok_or_else(|| AppError::not_found(format!("Library not found: {}", id)))?
    };

    match start_library_sync(STATE.db.get(), library).await? {
        crate::services::StartLibrarySyncResult::Started { run_id } => {
            Ok(Json(LibrarySyncStartResponse {
                started: true,
                run_id,
            }))
        }
        crate::services::StartLibrarySyncResult::AlreadyRunning { run_id } => {
            Err(AppError::conflict(format!(
                "library sync already running for id {} (run_id={})",
                id, run_id
            )))
        }
    }
}

fn get_library_sync_status_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Get library sync status").description(
        "Returns the in-memory sync state for a library. Requires ManageLibraries permission.",
    )
}

fn start_library_sync_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Start library sync")
        .description("Starts a background library sync. Returns 409 if one is already running.")
}

pub fn library_routes() -> ApiRouter {
    use aide::axum::routing::{
        get_with,
        patch_with,
    };

    ApiRouter::new()
        .api_route(
            "/",
            get_with(list_libraries, list_libraries_docs)
                .post_with(create_library, create_library_docs),
        )
        .api_route("/{id}", patch_with(update_library, update_library_docs))
        .api_route(
            "/{id}/releases",
            get_with(get_library_releases, get_library_releases_docs),
        )
        .api_route(
            "/{id}/refresh",
            post_with(refresh_library, refresh_library_docs),
        )
        .api_route(
            "/{id}/sync",
            get_with(get_library_sync_status, get_library_sync_status_docs)
                .post_with(start_library_sync_for_library, start_library_sync_docs),
        )
}
