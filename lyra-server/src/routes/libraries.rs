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
    routes::{
        AppError,
        double_option,
    },
    services::{
        LibraryRefreshRunOptions,
        SyncRunStartResponse,
        auth::{
            require_authenticated,
            require_can_create_library,
            require_manage_libraries_on,
        },
        get_library_sync_status as get_library_sync_status_summary,
        start_library_refresh,
        start_library_sync,
    },
};

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct LibraryRequest {
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Human-friendly library name.")
    )]
    #[serde(alias = "_name")]
    name: String,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Filesystem path to scan for media.")
    )]
    #[serde(alias = "_directory")]
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
}

impl LibraryResponse {
    fn from_library(lib: Library, include_directory: bool) -> Self {
        Self {
            id: lib.id,
            name: lib.name,
            directory: include_directory.then_some(lib.path),
            language: lib.language,
            country: lib.country,
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
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Default, Deserialize, Serialize)]
enum LibraryAccessKind {
    #[default]
    ReadWrite,
}

impl From<LibraryAccessKind> for db::libraries::AccessKind {
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
#[derive(Deserialize)]
struct LibraryRefreshQuery {
    #[serde(default)]
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Replace existing cover images with downloaded provider results when set."
        )
    )]
    replace_cover: bool,
    #[serde(default)]
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Bypass cached provider cover resolution and refresh it.")
    )]
    force_refresh: bool,
}

fn library_not_found(id: &str) -> AppError {
    AppError::not_found(format!("library not found: {id}"))
}

fn public_user_role(db: &impl db::DbAccess, user_db_id: agdb::DbId) -> Option<String> {
    db::roles::get_role_for_user(db, user_db_id)
        .ok()
        .flatten()
        .map(|role| role.name)
}

async fn refresh_library(
    headers: HeaderMap,
    Path(library_id): Path<String>,
    Query(query): Query<LibraryRefreshQuery>,
) -> Result<Json<SyncRunStartResponse>, AppError> {
    let _principal = require_manage_libraries_on(&headers, &library_id).await?;

    let library = {
        let db = STATE.db.read().await;
        let library_db_id = db::lookup::find_node_id_by_id(&*db, &library_id)?
            .ok_or_else(|| AppError::not_found(format!("not found: {library_id}")))?;
        db::libraries::get_by_id(&db, library_db_id)?
            .ok_or_else(|| AppError::not_found(format!("Library not found: {library_id}")))?
    };

    let options = LibraryRefreshRunOptions {
        replace_cover: query.replace_cover,
        force_refresh: query.force_refresh,
    };
    Ok(Json(
        start_library_refresh(STATE.db.get(), library, options).await?,
    ))
}

async fn create_library(
    headers: HeaderMap,
    Json(library): Json<LibraryRequest>,
) -> Result<(StatusCode, Json<LibraryResponse>), AppError> {
    let principal = require_can_create_library(&headers).await?;

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
            Ok(db::libraries::path_key_for(&candidate))
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
        let outcome =
            db_write.transaction_mut(|t| -> Result<Library, db::libraries::LibraryCreateError> {
                db::libraries::create_with_creator(
                    t,
                    db::libraries::LibraryInsert {
                        id: nanoid!(),
                        name: library.name,
                        path: directory,
                        path_key,
                        language,
                        country,
                    },
                    principal.user_db_id,
                )
            });
        match outcome {
            Ok(library) => library,
            Err(err @ db::libraries::LibraryCreateError::InvalidName(_)) => {
                return Err(AppError::bad_request(err.to_string()));
            }
            Err(
                err @ (db::libraries::LibraryCreateError::NameInUse(_)
                | db::libraries::LibraryCreateError::PathInUse(_)),
            ) => {
                return Err(AppError::conflict(err.to_string()));
            }
            Err(db::libraries::LibraryCreateError::Db(e)) => return Err(AppError::from(e)),
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
    let _principal = require_manage_libraries_on(&headers, &id).await?;

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
    };

    let outcome = db.transaction_mut(|t| -> Result<Library, db::libraries::LibraryUpdateError> {
        db::libraries::update(t, &updated)
    });
    let stored = match outcome {
        Ok(library) => library,
        Err(err @ db::libraries::LibraryUpdateError::InvalidName(_)) => {
            return Err(AppError::bad_request(err.to_string()));
        }
        Err(err @ db::libraries::LibraryUpdateError::NameInUse(_)) => {
            return Err(AppError::conflict(err.to_string()));
        }
        Err(db::libraries::LibraryUpdateError::Db(e)) => return Err(AppError::from(e)),
    };

    Ok(Json(LibraryResponse::from_library(stored, true)))
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
    let principal = require_authenticated(&headers).await?;
    let include_directory =
        db::roles::has_permission(&principal.permissions, Permission::ManageLibraries);

    let db = STATE.db.read().await;
    let libraries = db::libraries::get(&db)?;
    let response: Vec<LibraryResponse> = libraries
        .into_iter()
        .filter(|library| principal.accessible_library_ids.contains(&library.id))
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
            "Triggers a metadata refresh for all releases in a library from all enabled providers.\n\
            Set `replace_cover` to true to overwrite existing cover images when provider results include cover URLs.",
        )
}

async fn get_library_sync_status(
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<crate::services::LibrarySyncStatus>, AppError> {
    let _principal = require_manage_libraries_on(&headers, &id).await?;

    let library_db_id = {
        let db = STATE.db.read().await;
        let library_db_id = db::lookup::find_node_id_by_id(&*db, &id)?
            .ok_or_else(|| AppError::not_found(format!("not found: {id}")))?;
        db::libraries::get_by_id(&db, library_db_id)?
            .ok_or_else(|| AppError::not_found(format!("Library not found: {}", id)))?;
        library_db_id
    };

    let status = get_library_sync_status_summary(library_db_id).await;
    Ok(Json(status))
}

async fn start_library_sync_for_library(
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<SyncRunStartResponse>, AppError> {
    let _principal = require_manage_libraries_on(&headers, &id).await?;
    let library = {
        let db = STATE.db.read().await;
        let library_db_id = db::lookup::find_node_id_by_id(&*db, &id)?
            .ok_or_else(|| AppError::not_found(format!("not found: {id}")))?;
        db::libraries::get_by_id(&db, library_db_id)?
            .ok_or_else(|| AppError::not_found(format!("Library not found: {}", id)))?
    };

    Ok(Json(start_library_sync(STATE.db.get(), library).await?))
}

#[cfg(feature = "docgen")]
fn get_library_sync_status_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Get library sync status").description(
        "Returns a compact sync status for a library, including run state, aggregate progress, current work, active-unit count, and failure count. Requires ManageLibraries permission.",
    )
}

#[cfg(feature = "docgen")]
fn start_library_sync_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Start library sync")
        .description("Starts a background library sync. Returns 409 if one is already running.")
}

async fn list_library_access(
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<Vec<LibraryAccessResponse>>, AppError> {
    let _principal = require_manage_libraries_on(&headers, &id).await?;
    let db = STATE.db.read().await;
    let library_db_id =
        db::lookup::find_node_id_by_id(&*db, &id)?.ok_or_else(|| library_not_found(&id))?;
    db::libraries::get_by_id(&db, library_db_id)?.ok_or_else(|| library_not_found(&id))?;

    let users = db::libraries::users_with_access(&db, library_db_id)?;
    let response = users
        .into_iter()
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
    let principal = require_manage_libraries_on(&headers, &id).await?;
    let mut db = STATE.db.write().await;
    let library_db_id =
        db::lookup::find_node_id_by_id(&*db, &id)?.ok_or_else(|| library_not_found(&id))?;
    db::libraries::get_by_id(&db, library_db_id)?.ok_or_else(|| library_not_found(&id))?;

    let target_user = db::users::get_by_public_id(&db, &request.user_id)?
        .or_else(|| {
            db::users::get_by_username(&db, &request.user_id)
                .ok()
                .flatten()
        })
        .ok_or_else(|| AppError::not_found(format!("user not found: {}", request.user_id)))?;
    let target_user_db_id = target_user
        .db_id
        .ok_or_else(|| AppError::not_found(format!("user has no db_id: {}", request.user_id)))?;

    let authorized = principal.permissions.contains(&Permission::Admin);
    let granted = db.transaction_mut(|t| -> anyhow::Result<bool> {
        if !authorized
            && !db::libraries::user_has_access_in_txn(t, principal.user_db_id, library_db_id)?
        {
            return Ok(false);
        }
        db::libraries::grant_access(t, target_user_db_id, library_db_id, request.kind.into())?;
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
    let principal = require_manage_libraries_on(&headers, &id).await?;
    let mut db = STATE.db.write().await;
    let library_db_id =
        db::lookup::find_node_id_by_id(&*db, &id)?.ok_or_else(|| library_not_found(&id))?;
    db::libraries::get_by_id(&db, library_db_id)?.ok_or_else(|| library_not_found(&id))?;

    let target_user = db::users::get_by_public_id(&db, &user_id)?
        .or_else(|| db::users::get_by_username(&db, &user_id).ok().flatten())
        .ok_or_else(|| AppError::not_found(format!("user not found: {user_id}")))?;
    let target_user_db_id = target_user
        .db_id
        .ok_or_else(|| AppError::not_found(format!("user has no db_id: {user_id}")))?;

    if target_user_db_id == principal.user_db_id {
        tracing::warn!(
            user_public_id = %principal.user_public_id,
            library_public_id = %id,
            "user revoked their own library access"
        );
    }

    let authorized = principal.permissions.contains(&Permission::Admin);
    let revoked = db.transaction_mut(|t| -> anyhow::Result<bool> {
        if !authorized
            && !db::libraries::user_has_access_in_txn(t, principal.user_db_id, library_db_id)?
        {
            return Ok(false);
        }
        db::libraries::revoke_access(t, target_user_db_id, library_db_id)?;
        Ok(true)
    })?;
    if !revoked {
        return Err(library_not_found(&id));
    }
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(feature = "docgen")]
fn list_library_access_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List library access")
        .description("Returns explicit user grants for a library. Admin bypass is not listed.")
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
        .route("/{id}", patch(update_library))
        .route("/{id}/refresh", post(refresh_library))
        .route(
            "/{id}/sync",
            get(get_library_sync_status).post(start_library_sync_for_library),
        )
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
        .api_route("/{id}", patch_with(update_library, update_library_docs))
        .api_route(
            "/{id}/refresh",
            post_with(refresh_library, refresh_library_docs),
        )
        .api_route(
            "/{id}/sync",
            get_with(get_library_sync_status, get_library_sync_status_docs)
                .post_with(start_library_sync_for_library, start_library_sync_docs),
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
    use crate::{
        db,
        services::auth::sessions,
    };

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

    fn bearer_headers(token: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            "Authorization",
            format!("Bearer {token}").parse().expect("valid header"),
        );
        headers
    }

    fn library_insert(name: &str, suffix: &str) -> db::libraries::LibraryInsert {
        let path = std::path::PathBuf::from(format!("/tmp/lyra-route-library-{suffix}"));
        let path_key = db::libraries::path_key_for(&path);
        db::libraries::LibraryInsert {
            id: nanoid!(),
            name: name.to_string(),
            path,
            path_key,
            language: None,
            country: None,
        }
    }

    fn create_user_with_permissions(
        db: &mut agdb::DbAny,
        username: &str,
        permissions: Vec<Permission>,
    ) -> anyhow::Result<agdb::DbId> {
        let user_db_id = db::users::create(db, &db::test_db::test_user(username)?)?;
        let role = db::roles::Role {
            db_id: None,
            id: nanoid!(),
            name: format!("{username}-role"),
            permissions,
        };
        let role_db_id = db::roles::create(db, &role)?;
        db::roles::assign_role_to_user(db, user_db_id, role_db_id)?;
        Ok(user_db_id)
    }

    async fn setup_route_test() -> anyhow::Result<()> {
        crate::testing::initialize_runtime(&crate::testing::LibraryFixtureConfig {
            directory: std::path::PathBuf::from("."),
            language: None,
            country: None,
        })
        .await
    }

    #[tokio::test]
    async fn list_libraries_returns_only_accessible_libraries() -> anyhow::Result<()> {
        let _guard = crate::testing::runtime_test_lock().await;
        setup_route_test().await?;

        let (user_db_id, visible_id, hidden_id) = {
            let mut db = STATE.db.write().await;
            let user_db_id = db::users::create(&mut db, &db::test_db::test_user("listener")?)?;
            let visible = db.transaction_mut(|t| -> anyhow::Result<db::Library> {
                Ok(db::libraries::create_with_creator(
                    t,
                    library_insert("Visible", "visible"),
                    user_db_id,
                )?)
            })?;
            let hidden = db.transaction_mut(|t| -> anyhow::Result<db::Library> {
                Ok(db::libraries::create_system(
                    t,
                    library_insert("Hidden", "hidden"),
                )?)
            })?;
            (user_db_id, visible.id, hidden.id)
        };
        let session = sessions::create_session_for_user(user_db_id, Default::default()).await?;

        let Json(libraries) = list_libraries(bearer_headers(&session.token))
            .await
            .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        assert_eq!(libraries.len(), 1);
        assert_eq!(libraries[0].id, visible_id);
        assert_ne!(libraries[0].id, hidden_id);
        assert!(libraries[0].directory.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn library_access_endpoints_grant_list_and_revoke() -> anyhow::Result<()> {
        let _guard = crate::testing::runtime_test_lock().await;
        setup_route_test().await?;

        let (manager_db_id, target_id, library_id) = {
            let mut db = STATE.db.write().await;
            let manager_db_id = create_user_with_permissions(
                &mut db,
                "manager",
                vec![Permission::ManageLibraries],
            )?;
            let target_db_id = db::users::create(&mut db, &db::test_db::test_user("target")?)?;
            let target_id = db::users::get_by_id(&db, target_db_id)?
                .expect("target user")
                .id;
            let library = db.transaction_mut(|t| -> anyhow::Result<db::Library> {
                Ok(db::libraries::create_with_creator(
                    t,
                    library_insert("Managed", "managed"),
                    manager_db_id,
                )?)
            })?;
            (manager_db_id, target_id, library.id)
        };
        let session = sessions::create_session_for_user(manager_db_id, Default::default()).await?;
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
}
