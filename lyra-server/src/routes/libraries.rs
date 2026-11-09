// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::path::PathBuf;

#[cfg(feature = "docgen")]
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
use axum::{
    Router,
    routing::post,
};
use nanoid::nanoid;
use serde::{
    Deserialize,
    Serialize,
};

use lyra_metadata::locale::{
    validate_country,
    validate_language,
};

use crate::routes::request::bearer_credential;
use crate::{
    routes::{
        AppError,
        double_option,
    },
    services::STATE,
    services::{
        CoverSyncOptions,
        CoverUpdate,
        SyncRunStartResponse,
        auth::{
            access,
            accounts,
            require_authenticated,
            require_can_create_library,
        },
        libraries::{
            LibraryRun,
            LibrarySyncOptions,
            RunFilter,
            SyncReleases,
            control::{
                self,
                require_manage_libraries_on,
            },
        },
        start_library_sync,
    },
};
use lyra_db::{
    Library,
    Permission,
};

#[cfg(feature = "docgen")]
const FOLLOW_LINKS_DESCRIPTION: &str = "Whether syncs follow symbolic links, and files a link leads to outside every library can be served when they are audio, cue sheets or `cover` images. Off by default: syncs report links instead of following them. A change reaches the catalog with the next sync.";

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct LibraryRequest {
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Human-friendly library name.")
    )]
    name: String,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Filesystem path to scan for media.")
    )]
    directory: String,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "ISO 639 language code (e.g. \"jpn\", \"en\", \"Japanese\").")
    )]
    language: Option<String>,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "ISO 3166 country code (e.g. \"JP\", \"US\", \"Japan\").")
    )]
    country: Option<String>,
    #[cfg_attr(feature = "docgen", schemars(description = FOLLOW_LINKS_DESCRIPTION))]
    #[serde(default)]
    follow_links: bool,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct LibraryResponse {
    id: String,
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    directory: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    language: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    country: Option<String>,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Whole days an entry stays missing before a sync purges its tracks; omitted when missing tracks stay until purged by hand."
        )
    )]
    #[serde(skip_serializing_if = "Option::is_none")]
    purge_missing_after_days: Option<u32>,
    #[cfg_attr(feature = "docgen", schemars(description = FOLLOW_LINKS_DESCRIPTION))]
    follow_links: bool,
}

impl LibraryResponse {
    fn from_library(lib: Library, include_directory: bool) -> Self {
        Self {
            follow_links: lib.follows_links(),
            id: lib.id,
            name: lib.name,
            directory: include_directory.then_some(lib.path),
            language: lib.language,
            country: lib.country,
            purge_missing_after_days: lib.purge_missing_after_days,
        }
    }
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct LibraryUpdateRequest {
    #[cfg_attr(feature = "docgen", schemars(description = "Updated library name."))]
    name: Option<String>,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Updated language code; set to null to clear.")
    )]
    #[serde(default, deserialize_with = "double_option")]
    language: Option<Option<String>>,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Updated country code; set to null to clear.")
    )]
    #[serde(default, deserialize_with = "double_option")]
    country: Option<Option<String>>,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Updated whole days, 1 to 3650, an entry stays missing before a sync purges its tracks; set to null to keep missing tracks until purged by hand."
        )
    )]
    #[serde(default, deserialize_with = "double_option")]
    purge_missing_after_days: Option<Option<u32>>,
    #[cfg_attr(feature = "docgen", schemars(description = FOLLOW_LINKS_DESCRIPTION))]
    follow_links: Option<bool>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Default, Deserialize, Serialize)]
enum LibraryAccessKind {
    #[default]
    ReadWrite,
}

impl From<LibraryAccessKind> for lyra_db::libraries::AccessKind {
    fn from(kind: LibraryAccessKind) -> Self {
        match kind {
            LibraryAccessKind::ReadWrite => Self::ReadWrite,
        }
    }
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct LibraryAccessResponse {
    id: String,
    username: String,
    role: Option<String>,
    kind: LibraryAccessKind,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct LibraryGrantAccessRequest {
    user_id: String,
    #[serde(default)]
    kind: LibraryAccessKind,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Default, Deserialize)]
struct LibraryRefreshQuery {
    #[serde(default)]
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "The covers to fetch for the refreshed releases and the artists credited on them or their tracks: `skip`, `missing` (the default, only where none is set) or `replace`."
        )
    )]
    covers: CoverUpdate,
    #[serde(default)]
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Look covers up again instead of using cached lookups. Rejected with `covers=skip`."
        )
    )]
    bypass_cover_cache: bool,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Default, Deserialize)]
struct LibrarySyncQuery {
    #[serde(default)]
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "The releases to ask the providers about: `changed` (the default, those the scan found new or changed) or `all`."
        )
    )]
    releases: SyncReleases,
    #[serde(default)]
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "The covers to fetch for the releases the providers are asked about and for the artists credited on them or their tracks: `skip`, `missing` (the default, only where none is set) or `replace`."
        )
    )]
    covers: CoverUpdate,
    #[serde(default)]
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Look covers up again instead of using cached lookups. Rejected with `covers=skip`."
        )
    )]
    bypass_cover_cache: bool,
}

fn library_not_found(id: &str) -> AppError {
    AppError::not_found(format!("library not found: {id}"))
}

fn public_user_role(db: &impl lyra_db::DbAccess, user_db_id: agdb::DbId) -> Option<String> {
    lyra_db::roles::get_role_for_user(db, user_db_id)
        .ok()
        .flatten()
        .map(|role| role.name)
}

async fn refresh_library(
    headers: HeaderMap,
    Path(library_id): Path<String>,
    Query(query): Query<LibraryRefreshQuery>,
) -> Result<Json<SyncRunStartResponse>, AppError> {
    let principal = require_authenticated(bearer_credential(&headers)).await?;
    let covers = CoverSyncOptions::new(query.covers, query.bypass_cover_cache)?;
    Ok(Json(
        control::start(&principal, &library_id, LibraryRun::Refresh(covers)).await?,
    ))
}

async fn create_library(
    headers: HeaderMap,
    Json(library): Json<LibraryRequest>,
) -> Result<(StatusCode, Json<LibraryResponse>), AppError> {
    let principal = require_can_create_library(bearer_credential(&headers)).await?;

    let directory_input = library.directory.trim();
    if directory_input.is_empty() {
        return Err(AppError::bad_request("library directory cannot be empty"));
    }

    let directory = PathBuf::from(directory_input);
    // Sync syscalls offloaded; raw `directory` preserved for symlink retargeting.
    let path_key = {
        let candidate = directory.clone();
        tokio::task::spawn_blocking(move || -> Result<String, AppError> {
            if !candidate.is_dir() {
                return Err(AppError::bad_request(format!(
                    "library directory not found: {}",
                    candidate.display()
                )));
            }
            Ok(lyra_db::libraries::path_key_for(&candidate))
        })
        .await
        .map_err(|e| anyhow!("directory canonicalize task panicked: {e}"))??
    };

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

    let library = {
        let mut db_write = STATE.db.write().await;
        let user_db_id = principal.require(&db_write)?;
        let outcome = db_write.transaction_mut(
            |t| -> Result<Library, lyra_db::libraries::LibraryCreateError> {
                lyra_db::libraries::create_with_creator(
                    t,
                    lyra_db::libraries::LibraryInsert {
                        id: nanoid!(),
                        name: library.name,
                        path: directory,
                        path_key,
                        language,
                        country,
                        follow_links: library.follow_links,
                    },
                    user_db_id,
                )
            },
        );
        match outcome {
            Ok(library) => library,
            Err(err @ lyra_db::libraries::LibraryCreateError::InvalidName(_)) => {
                return Err(AppError::bad_request(err.to_string()));
            }
            Err(
                err @ (lyra_db::libraries::LibraryCreateError::NameInUse(_)
                | lyra_db::libraries::LibraryCreateError::PathInUse(_)),
            ) => {
                return Err(AppError::conflict(err.to_string()));
            }
            Err(lyra_db::libraries::LibraryCreateError::Db(e)) => return Err(AppError::from(e)),
        }
    };

    let library_db_id = library
        .db_id
        .ok_or_else(|| anyhow!("library insert missing id"))?;
    let response = LibraryResponse::from_library(library.clone(), true);
    let sync = start_library_sync(STATE.db.get(), library).await?;
    tracing::info!(
        library_id = library_db_id.0,
        run_id = sync.run.run.id.as_deref().unwrap_or(""),
        started = sync.started,
        "requested library sync"
    );
    Ok((StatusCode::CREATED, Json(response)))
}

async fn update_library(
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(update): Json<LibraryUpdateRequest>,
) -> Result<Json<LibraryResponse>, AppError> {
    let _principal = require_manage_libraries_on(bearer_credential(&headers), &id).await?;

    if update.name.is_none()
        && update.language.is_none()
        && update.country.is_none()
        && update.purge_missing_after_days.is_none()
        && update.follow_links.is_none()
    {
        return Err(AppError::bad_request("no library fields provided"));
    }
    if let Some(Some(days)) = update.purge_missing_after_days
        && !lyra_db::libraries::PURGE_MISSING_AFTER_DAYS.contains(&days)
    {
        return Err(AppError::bad_request(
            "purge_missing_after_days must be from 1 to 3650",
        ));
    }

    let mut db = STATE.db.write().await;
    let library_db_id = lyra_db::lookup::find_node_id_by_id(&*db, &id)?
        .ok_or_else(|| AppError::not_found(format!("not found: {id}")))?;
    let library =
        lyra_db::libraries::get_by_id(&db, library_db_id)?.ok_or_else(|| library_not_found(&id))?;

    let mut updated_name = library.name;
    let mut updated_language = library.language;
    let mut updated_country = library.country;
    let updated_purge = update
        .purge_missing_after_days
        .unwrap_or(library.purge_missing_after_days);
    let updated_follow_links = update
        .follow_links
        .map(Some)
        .unwrap_or(library.follow_links);

    if let Some(name) = update.name {
        updated_name = name;
    }

    if let Some(language) = update.language {
        updated_language = language
            .map(|value| validate_language(&value))
            .transpose()
            .map_err(|e| AppError::bad_request(e.to_string()))?;
    }

    if let Some(country) = update.country {
        updated_country = country
            .map(|value| validate_country(&value))
            .transpose()
            .map_err(|e| AppError::bad_request(e.to_string()))?;
    }

    // `name_key` is rederived inside `update`; pass the prior key (not empty)
    // so reads before overwrite still see a valid value.
    let updated = Library {
        db_id: Some(library_db_id),
        id: library.id,
        name: updated_name,
        name_key: library.name_key,
        path: library.path,
        path_key: library.path_key,
        language: updated_language,
        country: updated_country,
        purge_missing_after_days: updated_purge,
        follow_links: updated_follow_links,
    };

    let outcome = db.transaction_mut(
        |t| -> Result<Library, lyra_db::libraries::LibraryUpdateError> {
            lyra_db::libraries::update(t, &updated)
        },
    );
    let stored = match outcome {
        Ok(library) => library,
        Err(err @ lyra_db::libraries::LibraryUpdateError::InvalidName(_)) => {
            return Err(AppError::bad_request(err.to_string()));
        }
        Err(err @ lyra_db::libraries::LibraryUpdateError::NameInUse(_)) => {
            return Err(AppError::conflict(err.to_string()));
        }
        Err(lyra_db::libraries::LibraryUpdateError::Db(e)) => return Err(AppError::from(e)),
    };

    Ok(Json(LibraryResponse::from_library(stored, true)))
}

async fn delete_library(
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<StatusCode, AppError> {
    let principal = require_manage_libraries_on(bearer_credential(&headers), &id).await?;
    let _guard = crate::services::libraries::library_mutation_guard().await;
    if crate::services::libraries::library_has_active_run(&id).await? {
        return Err(AppError::conflict(
            "Wait for the library sync or refresh to finish before deleting it.",
        ));
    }
    let removed = {
        let mut db = STATE.db.write().await;
        // The caller may have lost the permission or the library while waiting on the guard.
        let (principal, library) = control::managed_library(&db, &principal, &id)?;
        let user_db_id = principal.require(&db)?;
        let library_db_id = library.db_id.ok_or_else(|| library_not_found(&id))?;
        db.transaction_mut(|t| -> Result<_, AppError> {
            if !principal.permissions.contains(&Permission::Admin)
                && !lyra_db::libraries::user_has_access_in_txn(t, user_db_id, library_db_id)?
            {
                return Err(library_not_found(&id));
            }
            Ok(lyra_db::libraries::delete(t, &library)?)
        })?
    };
    crate::services::covers::discard_unreferenced_cover_files(removed.orphaned_covers);
    crate::services::hls::state::teardown_hls_jobs_for_tracks(&removed.track_ids).await;
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(feature = "docgen")]
fn delete_library_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Delete library").description(
        "Deletes the library and its exclusive indexed media, preserving files on disk and media shared with other libraries. Requires ManageLibraries and library access. Returns 409 during an active sync or refresh."
    ).response::<204, ()>()
}

#[cfg(feature = "docgen")]
fn create_library_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Create library")
        .description(
            "Creates a new library entry and starts background ingestion. Returns 201 when created.",
        )
        .response::<201, Json<LibraryResponse>>()
}

#[cfg(feature = "docgen")]
fn update_library_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Update library").description(
        "Updates library name, language, and country. Set language or country to null to clear.",
    )
}

async fn list_libraries(headers: HeaderMap) -> Result<Json<Vec<LibraryResponse>>, AppError> {
    let principal = require_authenticated(bearer_credential(&headers)).await?;
    let include_directory =
        lyra_db::roles::has_permission(&principal.permissions, Permission::ManageLibraries);

    let db = STATE.db.read().await;
    let response: Vec<LibraryResponse> = access::libraries(&*db, &principal)?
        .into_iter()
        .map(|library| LibraryResponse::from_library(library, include_directory))
        .collect();
    Ok(Json(response))
}

#[cfg(feature = "docgen")]
fn list_libraries_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List libraries").description(
        "Returns libraries visible to the authenticated user. `directory` is included only for users with ManageLibraries permission.",
    )
}

#[cfg(feature = "docgen")]
fn refresh_library_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Refresh library metadata")
        .description(
            "Starts a background metadata refresh of every release in a library from all enabled providers, then fetches the covers of the releases it refreshed and of the artists credited on them or their tracks.\n\
            `covers` picks the covers: `skip`, `missing` (the default, only where none is set) or `replace`; `bypass_cover_cache=true` looks them up again instead of using cached lookups, and is rejected with 400 alongside `covers=skip`.\n\
            While a sync or refresh of the library is running, starts nothing and returns 200 with `started` false and the running one, ignoring the parameters. Requires ManageLibraries and library access.",
        )
}

async fn get_library_sync_status(
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<crate::services::LibrarySyncStatus>, AppError> {
    let principal = require_authenticated(bearer_credential(&headers)).await?;
    control::sync_status(
        Some(&principal),
        std::slice::from_ref(&id),
        RunFilter::default(),
    )
    .await?
    .remove(&id)
    .map(Json)
    .ok_or_else(|| library_not_found(&id))
}

async fn list_library_sync_runs(
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<Vec<crate::services::LibrarySyncStatus>>, AppError> {
    let principal = require_authenticated(bearer_credential(&headers)).await?;
    control::recent_sync_runs(&principal, &id)
        .await?
        .map(Json)
        .ok_or_else(|| library_not_found(&id))
}

async fn start_library_sync_for_library(
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<LibrarySyncQuery>,
) -> Result<Json<SyncRunStartResponse>, AppError> {
    let principal = require_authenticated(bearer_credential(&headers)).await?;
    let options = LibrarySyncOptions {
        releases: query.releases,
        covers: CoverSyncOptions::new(query.covers, query.bypass_cover_cache)?,
    };
    Ok(Json(
        control::start(&principal, &id, LibraryRun::Sync(options)).await?,
    ))
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Default, Deserialize)]
struct PurgeRequest {
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Public ids of the library's unavailable tracks to purge, with the missing files only they need. Omitted, every missing file of the library and the tracks only they hold."
        )
    )]
    track_ids: Option<Vec<String>>,
}

async fn purge_library(
    headers: HeaderMap,
    Path(id): Path<String>,
    body: Option<Json<PurgeRequest>>,
) -> Result<Json<SyncRunStartResponse>, AppError> {
    let principal = require_authenticated(bearer_credential(&headers)).await?;
    let Json(request) = body.unwrap_or_default();
    match control::start_purge(&principal, &id, request.track_ids).await {
        Ok(response) => Ok(Json(response)),
        Err(control::PurgeStartError::Control(err)) => Err(err.into()),
        Err(err @ control::PurgeStartError::NotMissing(_)) => {
            Err(AppError::conflict(err.to_string()))
        }
        Err(err @ control::PurgeStartError::Busy) => Err(AppError::conflict(err.to_string())),
    }
}

#[cfg(feature = "docgen")]
fn purge_library_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Purge missing files").description(
        "Starts a background purge of what a library's missing files leave: with `track_ids`, those unavailable tracks and the missing files no other track needs; without, every missing file and the tracks only they hold. A purged track goes with its favorites, ratings, tags, listens, bookmarks, playlist entries, chains, lyrics and covers; releases and artists left with nothing go too. Answers 409 when a named track isn't an unavailable track of the library, or while a sync, refresh or purge of the library is running. The run's `purge` stage counts the tracks purged. Requires ManageLibraries and library access.",
    )
}

#[cfg(feature = "docgen")]
fn get_library_sync_status_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Get library sync status").description(
        "Returns a compact sync status for a library, including run state, aggregate progress, current work, active-unit count, and failure count: the active run, else the latest, else an idle status for a library that never ran. Requires library access. `run.error` and `current.subject` are null unless the caller has ManageLibraries.",
    )
}

#[cfg(feature = "docgen")]
fn list_library_sync_runs_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List library sync runs").description(
        "Returns the library's newest 50 sync, refresh and purge runs, newest first, as a bare array of the summaries `GET /api/sync/runs/{run_id}` returns; each carries `run.kind`. An active run appears once with its live state. A library that never ran returns an empty array. Older runs are removed when another starts: a library keeps its newest 50 runs and the latest finished run of each kind. Requires library access; a missing or hidden library is 404. `run.error` and `current.subject` are null unless the caller has ManageLibraries.",
    )
}

#[cfg(feature = "docgen")]
fn start_library_sync_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Start library sync")
        .description("Starts a background library sync: a scan of its files, then a metadata refresh from all enabled providers of the releases `releases` picks, `changed` (the default, those the scan found new or changed) or `all`, and the covers of those releases and of the artists credited on them or their tracks.\n\
            `covers` and `bypass_cover_cache` pick the covers as they do for a library refresh.\n\
            While a sync or refresh of the library is running, starts nothing and returns 200 with `started` false and the running one, ignoring the parameters. Requires ManageLibraries and library access.")
}

async fn list_library_access(
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<Vec<LibraryAccessResponse>>, AppError> {
    let _principal = require_manage_libraries_on(bearer_credential(&headers), &id).await?;
    let db = STATE.db.read().await;
    let library_db_id =
        lyra_db::lookup::find_node_id_by_id(&*db, &id)?.ok_or_else(|| library_not_found(&id))?;
    lyra_db::libraries::get_by_id(&db, library_db_id)?.ok_or_else(|| library_not_found(&id))?;

    let users = lyra_db::libraries::users_with_access(&db, library_db_id)?;
    let response = users
        .into_iter()
        .filter(accounts::is_account)
        .map(|user| {
            let role = user.db_id.and_then(|db_id| public_user_role(&db, db_id));
            LibraryAccessResponse {
                id: user.id,
                username: user.username,
                role,
                kind: LibraryAccessKind::ReadWrite,
            }
        })
        .collect();
    Ok(Json(response))
}

async fn grant_library_access(
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<LibraryGrantAccessRequest>,
) -> Result<StatusCode, AppError> {
    let principal = require_manage_libraries_on(bearer_credential(&headers), &id).await?;
    let mut db = STATE.db.write().await;
    let user_db_id = principal.require(&db)?;
    let library_db_id =
        lyra_db::lookup::find_node_id_by_id(&*db, &id)?.ok_or_else(|| library_not_found(&id))?;
    lyra_db::libraries::get_by_id(&db, library_db_id)?.ok_or_else(|| library_not_found(&id))?;

    let target_user = lyra_db::users::get_by_public_id(&db, &request.user_id)?
        .or_else(|| {
            lyra_db::users::get_by_username(&db, &request.user_id)
                .ok()
                .flatten()
        })
        .ok_or_else(|| AppError::not_found(format!("user not found: {}", request.user_id)))?;
    let target_user_db_id = target_user
        .db_id
        .ok_or_else(|| AppError::not_found(format!("user has no db_id: {}", request.user_id)))?;

    let authorized = principal.permissions.contains(&Permission::Admin);
    let granted = db.transaction_mut(|t| -> anyhow::Result<bool> {
        if !authorized && !lyra_db::libraries::user_has_access_in_txn(t, user_db_id, library_db_id)?
        {
            return Ok(false);
        }
        lyra_db::libraries::grant_access(t, target_user_db_id, library_db_id, request.kind.into())?;
        Ok(true)
    })?;
    if !granted {
        return Err(library_not_found(&id));
    }
    Ok(StatusCode::NO_CONTENT)
}

async fn revoke_library_access(
    headers: HeaderMap,
    Path((id, user_id)): Path<(String, String)>,
) -> Result<StatusCode, AppError> {
    let principal = require_manage_libraries_on(bearer_credential(&headers), &id).await?;
    let mut db = STATE.db.write().await;
    let user_db_id = principal.require(&db)?;
    let library_db_id =
        lyra_db::lookup::find_node_id_by_id(&*db, &id)?.ok_or_else(|| library_not_found(&id))?;
    lyra_db::libraries::get_by_id(&db, library_db_id)?.ok_or_else(|| library_not_found(&id))?;

    let target_user = lyra_db::users::get_by_public_id(&db, &user_id)?
        .or_else(|| {
            lyra_db::users::get_by_username(&db, &user_id)
                .ok()
                .flatten()
        })
        .ok_or_else(|| AppError::not_found(format!("user not found: {user_id}")))?;
    let target_user_db_id = target_user
        .db_id
        .ok_or_else(|| AppError::not_found(format!("user has no db_id: {user_id}")))?;

    if target_user_db_id == user_db_id {
        tracing::warn!(
            user_public_id = %principal.user_public_id,
            library_public_id = %id,
            "user revoked their own library access"
        );
    }

    let authorized = principal.permissions.contains(&Permission::Admin);
    let revoked = db.transaction_mut(|t| -> anyhow::Result<bool> {
        if !authorized && !lyra_db::libraries::user_has_access_in_txn(t, user_db_id, library_db_id)?
        {
            return Ok(false);
        }
        lyra_db::libraries::revoke_access(t, target_user_db_id, library_db_id)?;
        Ok(true)
    })?;
    if !revoked {
        return Err(library_not_found(&id));
    }
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(feature = "docgen")]
fn list_library_access_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List library access").description(
        "Returns explicit user grants for a library. Admin bypass is not listed, and the default \
         user is left out while authentication is enabled.",
    )
}

#[cfg(feature = "docgen")]
fn grant_library_access_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Grant library access")
        .description("Grants a user access to a library.")
}

#[cfg(feature = "docgen")]
fn revoke_library_access_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Revoke library access")
        .description("Revokes a user's explicit library access grant.")
}

pub fn library_routes() -> Router {
    use axum::routing::{
        delete,
        get,
        patch,
    };

    Router::new()
        .route("/", get(list_libraries).post(create_library))
        .route("/{id}", patch(update_library).delete(delete_library))
        .route("/{id}/refresh", post(refresh_library))
        .route("/{id}/purge", post(purge_library))
        .route(
            "/{id}/sync",
            get(get_library_sync_status).post(start_library_sync_for_library),
        )
        .route("/{id}/sync/runs", get(list_library_sync_runs))
        .route(
            "/{id}/access",
            get(list_library_access).post(grant_library_access),
        )
        .route("/{id}/access/{user_id}", delete(revoke_library_access))
}

#[cfg(feature = "docgen")]
pub(crate) fn library_openapi_routes() -> aide::axum::ApiRouter {
    use aide::axum::routing::{
        delete_with,
        get_with,
        patch_with,
        post_with,
    };

    aide::axum::ApiRouter::new()
        .api_route(
            "/",
            get_with(list_libraries, list_libraries_docs)
                .post_with(create_library, create_library_docs),
        )
        .api_route(
            "/{id}",
            patch_with(update_library, update_library_docs)
                .delete_with(delete_library, delete_library_docs),
        )
        .api_route(
            "/{id}/refresh",
            post_with(refresh_library, refresh_library_docs),
        )
        .api_route("/{id}/purge", post_with(purge_library, purge_library_docs))
        .api_route(
            "/{id}/sync",
            get_with(get_library_sync_status, get_library_sync_status_docs)
                .post_with(start_library_sync_for_library, start_library_sync_docs),
        )
        .api_route(
            "/{id}/sync/runs",
            get_with(list_library_sync_runs, list_library_sync_runs_docs),
        )
        .api_route(
            "/{id}/access",
            get_with(list_library_access, list_library_access_docs)
                .post_with(grant_library_access, grant_library_access_docs),
        )
        .api_route(
            "/{id}/access/{user_id}",
            delete_with(revoke_library_access, revoke_library_access_docs),
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An explicit `null` must reach the service as `Some(None)` (a clear) and
    /// must not trip the "no fields provided" 400 guard.
    #[test]
    fn library_update_request_distinguishes_null_from_absent() {
        let cleared: LibraryUpdateRequest =
            serde_json::from_str(r#"{"language":null,"country":null}"#).expect("null body parses");
        assert_eq!(cleared.language, Some(None));
        assert_eq!(cleared.country, Some(None));
        assert!(
            !(cleared.name.is_none() && cleared.language.is_none() && cleared.country.is_none()),
            "explicit null must not be treated as an empty patch"
        );

        let absent: LibraryUpdateRequest =
            serde_json::from_str(r#"{"name":"n"}"#).expect("absent body parses");
        assert_eq!(absent.language, None);
        assert_eq!(absent.country, None);
    }

    fn query<T: serde::de::DeserializeOwned>(query: &str) -> Result<T, String> {
        let uri: axum::http::Uri = format!("/?{query}").parse().expect("uri");
        Query::<T>::try_from_uri(&uri)
            .map(|Query(query)| query)
            .map_err(|rejection| rejection.body_text())
    }

    #[test]
    fn sync_and_refresh_queries_read_the_cover_options() {
        let sync: LibrarySyncQuery =
            query("releases=all&covers=replace&bypass_cover_cache=true").expect("parses");
        assert_eq!(sync.releases, SyncReleases::All);
        assert_eq!(sync.covers, CoverUpdate::Replace);
        assert!(sync.bypass_cover_cache);

        let defaults: LibrarySyncQuery = query("").expect("parses");
        assert_eq!(defaults.releases, SyncReleases::Changed);
        assert_eq!(defaults.covers, CoverUpdate::Missing);
        assert!(!defaults.bypass_cover_cache);

        let refresh: LibraryRefreshQuery = query("covers=skip").expect("parses");
        assert_eq!(refresh.covers, CoverUpdate::Skip);

        let unknown = query::<LibraryRefreshQuery>("covers=all")
            .err()
            .expect("rejected");
        assert!(
            unknown.contains("`skip`, `missing`, `replace`"),
            "the rejection names the accepted values: {unknown}"
        );
        let unknown = query::<LibrarySyncQuery>("releases=new")
            .err()
            .expect("rejected");
        assert!(unknown.contains("`changed` or `all`"), "{unknown}");
    }

    #[tokio::test]
    async fn skipping_covers_while_bypassing_their_cache_is_a_bad_request() -> anyhow::Result<()> {
        use axum::response::IntoResponse;

        let _guard = crate::services::testing::runtime_test_lock().await;
        setup_route_test().await?;
        let manager = create_user_with_permissions(
            &mut *STATE.db.write().await,
            "cover-options",
            vec![Permission::ManageLibraries],
        )?;
        let session = crate::services::testing::create_session(manager, Default::default()).await?;
        let headers = bearer_headers(&session.token);

        let unauthenticated = refresh_library(
            HeaderMap::new(),
            Path("library".to_string()),
            Query(LibraryRefreshQuery {
                covers: CoverUpdate::Skip,
                bypass_cover_cache: true,
            }),
        )
        .await
        .map(|_| ())
        .expect_err("rejected")
        .into_response();
        assert_eq!(unauthenticated.status(), StatusCode::UNAUTHORIZED);

        let refresh = refresh_library(
            headers.clone(),
            Path("library".to_string()),
            Query(LibraryRefreshQuery {
                covers: CoverUpdate::Skip,
                bypass_cover_cache: true,
            }),
        )
        .await;
        let sync = start_library_sync_for_library(
            headers,
            Path("library".to_string()),
            Query(LibrarySyncQuery {
                covers: CoverUpdate::Skip,
                bypass_cover_cache: true,
                ..LibrarySyncQuery::default()
            }),
        )
        .await;
        for response in [refresh.map(|_| ()), sync.map(|_| ())] {
            let response = response.expect_err("rejected").into_response();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        }
        Ok(())
    }

    fn bearer_headers(token: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            "Authorization",
            format!("Bearer {token}").parse().expect("valid header"),
        );
        headers
    }

    fn library_insert(name: &str, suffix: &str) -> lyra_db::libraries::LibraryInsert {
        let path = std::path::PathBuf::from(format!("/tmp/lyra-route-library-{suffix}"));
        let path_key = lyra_db::libraries::path_key_for(&path);
        lyra_db::libraries::LibraryInsert {
            id: nanoid!(),
            name: name.to_string(),
            path,
            path_key,
            language: None,
            country: None,
            follow_links: false,
        }
    }

    fn create_user_with_permissions(
        db: &mut agdb::DbAny,
        username: &str,
        permissions: Vec<Permission>,
    ) -> anyhow::Result<agdb::DbId> {
        let user_db_id = lyra_db::users::create(db, &lyra_db::fixtures::test_user(username)?)?;
        let role = lyra_db::roles::Role {
            db_id: None,
            id: nanoid!(),
            name: format!("{username}-role"),
            permissions,
        };
        let role_db_id = lyra_db::roles::create(db, &role)?;
        lyra_db::roles::assign_role_to_user(db, user_db_id, role_db_id)?;
        Ok(user_db_id)
    }

    async fn setup_route_test() -> anyhow::Result<()> {
        crate::plugins::testing::init_library_test_state(std::path::PathBuf::from(".")).await
    }

    fn finished_run(
        library_id: &str,
        id: &str,
        sequence: u64,
    ) -> lyra_db::sync_runs::SyncRunRecord {
        lyra_db::sync_runs::SyncRunRecord {
            db_id: None,
            id: id.to_string(),
            library_id: library_id.to_string(),
            kind: "library_sync".into(),
            status: "failed".into(),
            started_at_ms: sequence,
            updated_at_ms: sequence,
            finished_at_ms: Some(sequence),
            error: Some("cannot read /secret".into()),
            cancellation_requested: false,
            sequence,
            progress_mode: "determinate".into(),
            total_state: "final".into(),
            completed_units: 0,
            failed_units: 0,
            skipped_units: 0,
            total_units: 0,
            current_stage: None,
            current_subject: None,
            active_units: 0,
            failure_count: 0,
        }
    }

    #[tokio::test]
    async fn the_run_list_is_limited_redacted_and_shows_an_active_run_once() -> anyhow::Result<()> {
        use crate::services::libraries::{
            SyncRunStatus,
            SyncStageKey,
            SyncWorkDetails,
            begin_test_run,
            end_test_run,
        };
        use axum::response::IntoResponse;
        let _guard = crate::services::testing::runtime_test_lock().await;
        setup_route_test().await?;
        let (manager, listener, shown, hidden, idle) = {
            let mut db = STATE.db.write().await;
            let manager = create_user_with_permissions(
                &mut db,
                "manager",
                vec![Permission::ManageLibraries],
            )?;
            let listener = create_user_with_permissions(&mut db, "listener", vec![])?;
            let shown = lyra_db::test_db::insert_library(&mut db, "Shown", "/runs/shown")?;
            let hidden = lyra_db::test_db::insert_library(&mut db, "Hidden", "/runs/hidden")?;
            let idle = lyra_db::test_db::insert_library(&mut db, "Idle", "/runs/idle")?;
            for user in [manager, listener] {
                for library in [shown, idle] {
                    lyra_db::libraries::grant_access(
                        &mut db,
                        user,
                        library,
                        lyra_db::libraries::AccessKind::ReadWrite,
                    )?;
                }
            }
            let [shown, hidden, idle] = [shown, hidden, idle].map(|library| {
                lyra_db::libraries::get_by_id(&db, library)
                    .expect("library reads")
                    .expect("library exists")
            });
            for sequence in 1..=60 {
                lyra_db::sync_runs::create(
                    &mut db,
                    &finished_run(&shown.id, &format!("run-{sequence}"), sequence),
                )?;
            }
            (manager, listener, shown, hidden, idle)
        };
        let manager_headers = bearer_headers(
            &crate::services::testing::create_session(manager, Default::default())
                .await?
                .token,
        );
        let listener_headers = bearer_headers(
            &crate::services::testing::create_session(listener, Default::default())
                .await?
                .token,
        );
        let list = |headers: &HeaderMap, id: &str| {
            list_library_sync_runs(headers.clone(), Path(id.to_string()))
        };
        let status_of = |result: Result<Json<Vec<_>>, AppError>| {
            result
                .map(|_| StatusCode::OK)
                .unwrap_or_else(|error| error.into_response().status())
        };
        let ok = |result: Result<Json<Vec<crate::services::LibrarySyncStatus>>, AppError>| {
            result.map(|Json(runs)| runs).map_err(|e| anyhow!("{e}"))
        };

        let managed = ok(list(&manager_headers, &shown.id).await)?;
        assert_eq!(managed.len(), 50, "bounded before any pruning happened");
        assert_eq!(managed[0].run.id.as_deref(), Some("run-60"));
        assert_eq!(managed[49].run.id.as_deref(), Some("run-11"));
        assert!(managed[0].run.error.is_some());
        let listened = ok(list(&listener_headers, &shown.id).await)?;
        assert_eq!(listened.len(), 50);
        assert!(listened.iter().all(|run| run.run.error.is_none()));

        assert!(ok(list(&listener_headers, &idle.id).await)?.is_empty());
        assert_eq!(
            status_of(list(&listener_headers, &hidden.id).await),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            status_of(list(&listener_headers, "missing").await),
            StatusCode::NOT_FOUND
        );

        let active = begin_test_run(&shown).await;
        let runs = ok(list(&manager_headers, &shown.id).await)?;
        assert_eq!(runs.len(), 50);
        assert_eq!(runs[0].run.id.as_deref(), Some(active.run_id()));
        assert_eq!(runs[0].run.status, SyncRunStatus::Queued);

        active.set_status(SyncRunStatus::Running).await;
        let runs = ok(list(&manager_headers, &shown.id).await)?;
        assert_eq!(runs.len(), 50);
        assert_eq!(
            runs.iter()
                .filter(|run| run.run.id.as_deref() == Some(active.run_id()))
                .count(),
            1,
            "an active run's stored record doesn't list it twice"
        );
        assert_eq!(runs[0].run.id.as_deref(), Some(active.run_id()));
        assert_eq!(runs[0].run.status, SyncRunStatus::Running);

        active
            .start_work(
                SyncStageKey::EntrySync,
                SyncWorkDetails::new("entry_group_sync").source_dir(Some("/secret".to_string())),
            )
            .await;
        let subject = |runs: &[crate::services::LibrarySyncStatus]| {
            runs[0]
                .current
                .as_ref()
                .expect("current work")
                .subject
                .clone()
        };
        let managed = ok(list(&manager_headers, &shown.id).await)?;
        assert_eq!(subject(&managed).as_deref(), Some("/secret"));
        let listened = ok(list(&listener_headers, &shown.id).await)?;
        assert_eq!(subject(&listened), None);
        end_test_run(&active).await;
        Ok(())
    }

    #[tokio::test]
    async fn delete_library_requires_permission_access_and_an_idle_library() -> anyhow::Result<()> {
        use axum::response::IntoResponse;
        let _guard = crate::services::testing::runtime_test_lock().await;
        setup_route_test().await?;
        let (manager, listener, library, run_id) = {
            let mut db = STATE.db.write().await;
            let manager = create_user_with_permissions(
                &mut db,
                "deleter",
                vec![Permission::ManageLibraries],
            )?;
            let listener = create_user_with_permissions(&mut db, "listener", vec![])?;
            let library = db.transaction_mut(|t| -> anyhow::Result<_> {
                Ok(lyra_db::libraries::create_with_creator(
                    t,
                    library_insert("Delete me", "delete-route"),
                    manager,
                )?)
            })?;
            let record = lyra_db::sync_runs::SyncRunRecord {
                db_id: None,
                id: nanoid!(),
                library_id: library.id.clone(),
                kind: "library_sync".into(),
                status: "running".into(),
                started_at_ms: 1,
                updated_at_ms: 1,
                finished_at_ms: None,
                error: None,
                cancellation_requested: false,
                sequence: 1,
                progress_mode: "indeterminate".into(),
                total_state: "discovering".into(),
                completed_units: 0,
                failed_units: 0,
                skipped_units: 0,
                total_units: 0,
                current_stage: None,
                current_subject: None,
                active_units: 0,
                failure_count: 0,
            };
            let run_id = lyra_db::sync_runs::create(&mut db, &record)?;
            (manager, listener, library, run_id)
        };
        let manager_session =
            crate::services::testing::create_session(manager, Default::default()).await?;
        let listener_session =
            crate::services::testing::create_session(listener, Default::default()).await?;
        assert_eq!(
            delete_library(HeaderMap::new(), Path(library.id.clone()))
                .await
                .unwrap_err()
                .into_response()
                .status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            delete_library(
                bearer_headers(&listener_session.token),
                Path(library.id.clone())
            )
            .await
            .unwrap_err()
            .into_response()
            .status(),
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            delete_library(
                bearer_headers(&manager_session.token),
                Path(library.id.clone())
            )
            .await
            .unwrap_err()
            .into_response()
            .status(),
            StatusCode::CONFLICT
        );
        {
            let mut db = STATE.db.write().await;
            db.exec_mut(agdb::QueryBuilder::remove().ids(run_id).query())?;
            db.transaction_mut(|t| {
                lyra_db::libraries::revoke_access(t, manager, library.db_id.unwrap())
            })?;
        }
        assert_eq!(
            delete_library(
                bearer_headers(&manager_session.token),
                Path(library.id.clone())
            )
            .await
            .unwrap_err()
            .into_response()
            .status(),
            StatusCode::NOT_FOUND
        );
        {
            let mut db = STATE.db.write().await;
            db.transaction_mut(|t| {
                lyra_db::libraries::grant_access(
                    t,
                    manager,
                    library.db_id.unwrap(),
                    lyra_db::libraries::AccessKind::ReadWrite,
                )
            })?;
        }
        assert_eq!(
            delete_library(
                bearer_headers(&manager_session.token),
                Path(library.id.clone())
            )
            .await
            .map_err(|e| anyhow!("{e:?}"))?,
            StatusCode::NO_CONTENT
        );
        assert!(
            start_library_sync(STATE.db.get(), library.clone())
                .await
                .is_err()
        );
        assert_eq!(
            delete_library(bearer_headers(&manager_session.token), Path(library.id))
                .await
                .unwrap_err()
                .into_response()
                .status(),
            StatusCode::NOT_FOUND
        );
        Ok(())
    }

    #[tokio::test]
    async fn follow_links_starts_off_and_changes_through_update() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        setup_route_test().await?;
        let (manager, library) = {
            let mut db = STATE.db.write().await;
            let manager = create_user_with_permissions(
                &mut db,
                "follow-links",
                vec![Permission::ManageLibraries],
            )?;
            let library = db.transaction_mut(|t| -> anyhow::Result<lyra_db::Library> {
                Ok(lyra_db::libraries::create_with_creator(
                    t,
                    library_insert("Links", "follow-links"),
                    manager,
                )?)
            })?;
            (manager, library)
        };
        assert!(!library.follows_links());
        let session = crate::services::testing::create_session(manager, Default::default()).await?;
        let update = |follow_links| {
            update_library(
                bearer_headers(&session.token),
                Path(library.id.clone()),
                Json(LibraryUpdateRequest {
                    name: None,
                    language: None,
                    country: None,
                    purge_missing_after_days: None,
                    follow_links: Some(follow_links),
                }),
            )
        };

        let Json(enabled) = update(true)
            .await
            .map_err(|err| anyhow::anyhow!("{err:?}"))?;
        assert!(enabled.follow_links);
        let stored = lyra_db::libraries::get_by_id(
            &*STATE.db.read().await,
            library.db_id.expect("created library has a db_id"),
        )?
        .expect("library stored");
        assert!(stored.follows_links());

        let Json(disabled) = update(false)
            .await
            .map_err(|err| anyhow::anyhow!("{err:?}"))?;
        assert!(!disabled.follow_links);
        Ok(())
    }

    #[tokio::test]
    async fn list_libraries_returns_only_accessible_libraries() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        setup_route_test().await?;

        let (user_db_id, visible_id) = {
            let mut db = STATE.db.write().await;
            let user_db_id =
                lyra_db::users::create(&mut db, &lyra_db::fixtures::test_user("listener")?)?;
            let visible = db.transaction_mut(|t| -> anyhow::Result<lyra_db::Library> {
                Ok(lyra_db::libraries::create_with_creator(
                    t,
                    library_insert("Visible", "visible"),
                    user_db_id,
                )?)
            })?;
            db.transaction_mut(|t| -> anyhow::Result<lyra_db::Library> {
                Ok(lyra_db::libraries::create_system(
                    t,
                    library_insert("Hidden", "hidden"),
                )?)
            })?;
            (user_db_id, visible.id)
        };
        let session =
            crate::services::testing::create_session(user_db_id, Default::default()).await?;

        let Json(libraries) = list_libraries(bearer_headers(&session.token))
            .await
            .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        assert_eq!(libraries.len(), 1);
        assert_eq!(libraries[0].id, visible_id);
        assert!(libraries[0].directory.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn library_access_endpoints_grant_list_and_revoke() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        setup_route_test().await?;

        let (manager_db_id, target_id, library_id) = {
            let mut db = STATE.db.write().await;
            let manager_db_id = create_user_with_permissions(
                &mut db,
                "manager",
                vec![Permission::ManageLibraries],
            )?;
            let target_db_id =
                lyra_db::users::create(&mut db, &lyra_db::fixtures::test_user("target")?)?;
            let target_id = lyra_db::users::get_by_id(&db, target_db_id)?
                .expect("target user")
                .id;
            let library = db.transaction_mut(|t| -> anyhow::Result<lyra_db::Library> {
                Ok(lyra_db::libraries::create_with_creator(
                    t,
                    library_insert("Managed", "managed"),
                    manager_db_id,
                )?)
            })?;
            (manager_db_id, target_id, library.id)
        };
        let session =
            crate::services::testing::create_session(manager_db_id, Default::default()).await?;
        let headers = bearer_headers(&session.token);

        let granted = grant_library_access(
            headers.clone(),
            Path(library_id.clone()),
            Json(LibraryGrantAccessRequest {
                user_id: target_id.clone(),
                kind: LibraryAccessKind::ReadWrite,
            }),
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;
        assert_eq!(granted, StatusCode::NO_CONTENT);

        let Json(access) = list_library_access(headers.clone(), Path(library_id.clone()))
            .await
            .map_err(|err| anyhow::anyhow!("{err:?}"))?;
        assert!(access.iter().any(|grant| grant.id == target_id));

        let revoked = revoke_library_access(
            headers.clone(),
            Path((library_id.clone(), target_id.clone())),
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;
        assert_eq!(revoked, StatusCode::NO_CONTENT);

        let revoked_again = revoke_library_access(headers, Path((library_id, target_id)))
            .await
            .map_err(|err| anyhow::anyhow!("{err:?}"))?;
        assert_eq!(revoked_again, StatusCode::NO_CONTENT);
        Ok(())
    }

    #[tokio::test]
    async fn library_access_lists_the_default_user_only_while_auth_is_off() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        setup_route_test().await?;
        crate::services::auth::ensure_default_user(&STATE.config()).await?;

        let (manager_db_id, default_id, library_id) = {
            let mut db = STATE.db.write().await;
            let manager_db_id = create_user_with_permissions(
                &mut db,
                "manager",
                vec![Permission::ManageLibraries],
            )?;
            let default_id =
                lyra_db::users::get_by_username(&db, lyra_db::users::DEFAULT_USERNAME)?
                    .expect("default user")
                    .id;
            let library = db.transaction_mut(|t| -> anyhow::Result<lyra_db::Library> {
                Ok(lyra_db::libraries::create_with_creator(
                    t,
                    library_insert("Managed", "default-access"),
                    manager_db_id,
                )?)
            })?;
            (manager_db_id, default_id, library.id)
        };
        let session =
            crate::services::testing::create_session(manager_db_id, Default::default()).await?;
        let headers = bearer_headers(&session.token);
        grant_library_access(
            headers.clone(),
            Path(library_id.clone()),
            Json(LibraryGrantAccessRequest {
                user_id: default_id.clone(),
                kind: LibraryAccessKind::ReadWrite,
            }),
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        let Json(access) = list_library_access(headers, Path(library_id.clone()))
            .await
            .map_err(|err| anyhow::anyhow!("{err:?}"))?;
        assert!(!access.iter().any(|grant| grant.id == default_id));

        let mut config = STATE.config().as_ref().clone();
        config.auth.enabled = false;
        crate::services::testing::publish_config(config);
        let Json(access) = list_library_access(HeaderMap::new(), Path(library_id))
            .await
            .map_err(|err| anyhow::anyhow!("{err:?}"))?;
        assert!(access.iter().any(|grant| grant.id == default_id));
        Ok(())
    }
}
