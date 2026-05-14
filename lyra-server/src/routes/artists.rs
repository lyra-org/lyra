// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

#[cfg(feature = "docgen")]
use aide::transform::TransformOperation;
use axum::{
    Json,
    extract::{
        Path,
        Query,
    },
    http::HeaderMap,
};
use axum::{
    Router,
    routing::{
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
    db::{
        self,
        ListOptions,
        SortDirection,
        SortKey,
        SortSpec,
    },
    routes::AppError,
    routes::{
        covers as route_covers,
        deserialize_inc,
        responses::{
            ArtistRelationResponse,
            ArtistResponse,
            PageResponse,
            RelatedArtistResponse,
            RelationDirectionResponse,
            ReleaseResponse,
        },
    },
    services::{
        artists as artist_service,
        auth::{
            Principal,
            require_authenticated,
        },
        covers,
    },
};

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct ArtistQuery {
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Comma-separated or repeated values: releases, tracks, relations, covers."
        )
    )]
    #[serde(default, deserialize_with = "deserialize_inc")]
    inc: Option<Vec<String>>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct ArtistListQuery {
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Comma-separated or repeated values: releases, tracks, relations, covers."
        )
    )]
    #[serde(default, deserialize_with = "deserialize_inc")]
    inc: Option<Vec<String>>,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Optional fuzzy text query matched against artist names.")
    )]
    query: Option<String>,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Optional public library ID to scope returned artists.")
    )]
    library_id: Option<String>,
    #[serde(flatten)]
    page: super::PageQuery,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct ArtistUpdateRequest {
    #[cfg_attr(feature = "docgen", schemars(description = "Updated artist name."))]
    name: Option<String>,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Updated sort name; set to null to clear.")
    )]
    sort_name: Option<Option<String>>,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Updated description; set to null to clear.")
    )]
    description: Option<Option<String>>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
#[non_exhaustive]
pub struct ArtistCoverSearchResponse {
    pub artist_id: String,
    pub results: Vec<route_covers::ProviderCoverSearchResponse>,
}

#[derive(Clone, Copy)]
struct ArtistRouteIncludes {
    service: artist_service::ArtistIncludes,
    covers: bool,
}

fn parse_inc(inc: Option<Vec<String>>) -> Result<ArtistRouteIncludes, AppError> {
    let values = super::parse_inc_values(inc, &["releases", "tracks", "relations", "covers"])?;
    let mut result = ArtistRouteIncludes {
        service: artist_service::ArtistIncludes {
            releases: false,
            tracks: false,
            relations: false,
        },
        covers: false,
    };
    for value in values {
        match value.as_str() {
            "releases" => result.service.releases = true,
            "tracks" => result.service.tracks = true,
            "relations" => result.service.relations = true,
            "covers" => result.covers = true,
            _ => {}
        }
    }
    Ok(result)
}

fn default_artist_sort() -> Vec<SortSpec> {
    vec![SortSpec {
        key: SortKey::SortName,
        direction: SortDirection::Ascending,
    }]
}

fn artist_detail_to_response(
    db: &impl db::DbAccess,
    detail: artist_service::ArtistDetails,
    include_covers: bool,
) -> anyhow::Result<ArtistResponse> {
    let artist_db_id = detail.artist.db_id.clone().map(agdb::DbId::from);
    let cover = match artist_db_id {
        Some(artist_db_id) => route_covers::build_cover_response(db, artist_db_id, include_covers)?,
        None if include_covers => Some(None),
        None => None,
    };
    let releases = detail
        .releases
        .map(|v| v.into_iter().map(ReleaseResponse::from).collect());
    let relations = detail.relations.map(|v| {
        v.into_iter()
            .map(|r| ArtistRelationResponse {
                relation_type: r.relation_type,
                attributes: r.attributes,
                direction: match r.direction {
                    artist_service::RelationDirection::Incoming => {
                        RelationDirectionResponse::Incoming
                    }
                    artist_service::RelationDirection::Outgoing => {
                        RelationDirectionResponse::Outgoing
                    }
                },
                artist: RelatedArtistResponse {
                    id: r.artist.id,
                    name: r.artist.artist_name,
                    artist_type: r.artist.artist_type,
                },
            })
            .collect()
    });
    Ok(ArtistResponse {
        id: detail.artist.id,
        name: detail.artist.artist_name,
        sort_name: detail.artist.sort_name,
        description: detail.artist.description,
        verified: detail.artist.verified,
        credit: None,
        releases,
        tracks: detail
            .tracks
            .map(|v| v.into_iter().map(Into::into).collect()),
        relations,
        cover,
    })
}

pub(crate) async fn list_artist_responses(
    principal: &Principal,
    inc: Option<Vec<String>>,
    query: Option<String>,
    library_id: Option<String>,
    page_options: super::PageOptions,
) -> Result<PageResponse<ArtistResponse>, AppError> {
    let db = &*STATE.db.read().await;
    let includes = parse_inc(inc)?;
    let search_term = super::parse_text_query(query);
    let options = ListOptions {
        sort: default_artist_sort(),
        offset: None,
        limit: None,
        search_term,
    };
    let library_scope =
        super::resolve_optional_library_filter(db, principal, library_id.as_deref())?;

    let page = match library_scope {
        Some(library_db_id) => artist_service::query_credited(
            db,
            Some(&db::ResolveId::DbId(library_db_id)),
            &artist_service::CreditedArtistFilters::default(),
            &ListOptions {
                offset: Some(page_options.offset),
                limit: Some(page_options.limit),
                ..options
            },
        )?,
        None => {
            let artists = db::artists::get(db, "artists")?;
            let mut accessible_artists = Vec::with_capacity(artists.len());
            for artist in artists {
                let Some(artist_db_id) = artist.db_id.clone().map(agdb::DbId::from) else {
                    continue;
                };
                if !super::entity_accessible_to_principal(db, principal, artist_db_id)? {
                    continue;
                }
                accessible_artists.push(artist);
            }

            db::artists::query_items(
                accessible_artists,
                &ListOptions {
                    offset: Some(page_options.offset),
                    limit: Some(page_options.limit),
                    ..options
                },
            )
        }
    };
    let next_cursor = super::next_page_cursor(page.offset, page.entries.len(), page.total_count);
    let details = artist_service::list_details_for_artists(db, includes.service, page.entries)?;

    let mut items = Vec::with_capacity(details.len());
    for detail in details {
        items.push(artist_detail_to_response(db, detail, includes.covers)?);
    }
    Ok(PageResponse { items, next_cursor })
}

pub(crate) async fn get_artist_response(
    principal: &Principal,
    id: String,
    inc: Option<Vec<String>>,
) -> Result<ArtistResponse, AppError> {
    let db = &*STATE.db.read().await;
    let includes = parse_inc(inc)?;
    let artist_db_id = db::lookup::find_node_id_by_id(db, &id)?
        .ok_or_else(|| AppError::not_found(format!("not found: {id}")))?;
    super::require_entity_accessible(db, principal, artist_db_id, || {
        AppError::not_found(format!("Artist not found: {id}"))
    })?;
    let detail = artist_service::get_details(db, artist_db_id, includes.service)?
        .ok_or_else(|| AppError::not_found(format!("Artist not found: {}", id)))?;

    Ok(artist_detail_to_response(db, detail, includes.covers)?)
}

async fn get_artists(
    headers: HeaderMap,
    Query(list_query): Query<ArtistListQuery>,
) -> Result<Json<PageResponse<ArtistResponse>>, AppError> {
    let ArtistListQuery {
        inc,
        query,
        library_id,
        page,
    } = list_query;
    let page = page.resolve()?;
    let principal = require_authenticated(&headers).await?;
    Ok(Json(
        list_artist_responses(&principal, inc, query, library_id, page).await?,
    ))
}

async fn get_artist(
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<ArtistQuery>,
) -> Result<Json<ArtistResponse>, AppError> {
    let principal = require_authenticated(&headers).await?;
    Ok(Json(get_artist_response(&principal, id, query.inc).await?))
}

async fn search_artist_covers(
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(query): Json<route_covers::CoverSearchQuery>,
) -> Result<Json<ArtistCoverSearchResponse>, AppError> {
    let principal = require_authenticated(&headers).await?;

    let artist_db_id = {
        let db = STATE.db.read().await;
        let artist_db_id = db::lookup::find_node_id_by_id(&*db, &id)?
            .ok_or_else(|| AppError::not_found(format!("not found: {id}")))?;
        super::require_entity_accessible(&*db, &principal, artist_db_id, || {
            AppError::not_found(format!("Artist not found: {id}"))
        })?;
        if db::artists::get_by_id(&db, artist_db_id)?.is_none() {
            return Err(AppError::not_found(format!("Artist not found: {}", id)));
        }
        artist_db_id
    };

    let provider_filter = query.provider.as_deref();
    let found =
        covers::search_artist_cover_candidates(artist_db_id, provider_filter, query.force_refresh)
            .await?;
    let results = route_covers::map_provider_cover_search_results(found);

    Ok(Json(ArtistCoverSearchResponse {
        artist_id: id,
        results,
    }))
}

async fn update_artist(
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(update): Json<ArtistUpdateRequest>,
) -> Result<Json<ArtistResponse>, AppError> {
    let principal = require_authenticated(&headers).await?;

    if update.name.is_none() && update.sort_name.is_none() && update.description.is_none() {
        return Err(AppError::bad_request("no artist fields provided"));
    }

    let ArtistUpdateRequest {
        name: update_name,
        sort_name: update_sort_name,
        description: update_description,
    } = update;

    let mut db = STATE.db.write().await;
    let artist_db_id = db::lookup::find_node_id_by_id(&*db, &id)?
        .ok_or_else(|| AppError::not_found(format!("not found: {id}")))?;
    super::require_entity_accessible(&*db, &principal, artist_db_id, || {
        AppError::not_found(format!("Artist not found: {id}"))
    })?;
    if let Some(name) = update_name.as_ref()
        && name.trim().is_empty()
    {
        return Err(AppError::bad_request("name cannot be empty"));
    }

    if let Some(Some(sort_name)) = update_sort_name.as_ref()
        && sort_name.trim().is_empty()
    {
        return Err(AppError::bad_request("sort_name cannot be empty"));
    }

    if let Some(Some(description)) = update_description.as_ref()
        && description.trim().is_empty()
    {
        return Err(AppError::bad_request("description cannot be empty"));
    }

    let updated = artist_service::update(
        &mut db,
        artist_db_id,
        update_name,
        update_sort_name,
        update_description,
    )?
    .ok_or_else(|| AppError::not_found(format!("Artist not found: {}", id)))?;

    Ok(Json(ArtistResponse {
        id: updated.id,
        name: updated.artist_name,
        sort_name: updated.sort_name,
        description: updated.description,
        verified: updated.verified,
        credit: None,
        releases: None,
        tracks: None,
        relations: None,
        cover: None,
    }))
}

#[cfg(feature = "docgen")]
fn list_artists_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List artists").description(
        "Returns artists as `{ items, next_cursor }`. Supported query parameters: `inc`, `query`, `library_id`, `limit`, `cursor`. `library_id` scopes results to artists credited by releases or tracks belonging to that public library ID. `limit` defaults to 100 and is capped at 500. Drive pagination from `next_cursor`; it is `null` on the last page. `query` is a fuzzy text match against artist names. Use `inc` to include releases, tracks, relations, and/or covers. When `inc=covers`, cover metadata includes a public image URL. The `credit` field is not present on artist-level responses; it only appears when artists are included via track or release endpoints.",
    )
}

#[cfg(feature = "docgen")]
fn get_artist_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Get artist by ID").description(
        "Returns a single artist. 404 if not found. Use `inc` to include releases, tracks, relations, and/or covers. When `inc=covers`, cover metadata includes a public image URL. The `credit` field is not present on artist-level responses; it only appears when artists are included via track or release endpoints.",
    )
}

#[cfg(feature = "docgen")]
fn search_artist_covers_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Search artist cover candidates").description(
        "Returns provider cover candidates for an artist. Request body (JSON): `{ provider?, force_refresh? }`; \
        `force_refresh=true` bypasses cached provider cover resolution. Providers may return \
        width, height, and selected_index for automatic selection.",
    )
}

#[cfg(feature = "docgen")]
fn update_artist_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Update artist")
        .description("Updates artist name, sort name, and description. Set sort_name or description to null to clear.")
}

pub fn artist_routes() -> Router {
    Router::new()
        .route("/", get(get_artists))
        .route("/{id}", get(get_artist))
        .route("/{id}/covers/search", post(search_artist_covers))
        .route("/{id}", patch(update_artist))
}

#[cfg(feature = "docgen")]
pub(crate) fn artist_openapi_routes() -> aide::axum::ApiRouter {
    use aide::axum::routing::{
        get_with,
        patch_with,
        post_with,
    };

    aide::axum::ApiRouter::new()
        .api_route("/", get_with(get_artists, list_artists_docs))
        .api_route("/{id}", get_with(get_artist, get_artist_docs))
        .api_route(
            "/{id}/covers/search",
            post_with(search_artist_covers, search_artist_covers_docs),
        )
        .api_route("/{id}", patch_with(update_artist, update_artist_docs))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        db::test_db::{
            connect,
            connect_artist,
            insert_artist,
            insert_library,
            insert_release,
            insert_track,
        },
        services::auth::sessions,
        testing::{
            LibraryFixtureConfig,
            initialize_runtime,
            runtime_test_lock,
        },
    };
    use agdb::DbId;
    use axum::{
        Json,
        extract::Path,
        http::{
            HeaderMap,
            header::AUTHORIZATION,
        },
    };
    use std::{
        collections::HashSet,
        path::PathBuf,
        time::{
            SystemTime,
            UNIX_EPOCH,
        },
    };

    struct TestDirGuard(PathBuf);

    impl Drop for TestDirGuard {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    async fn initialize_test_runtime() -> anyhow::Result<TestDirGuard> {
        let test_dir = std::env::temp_dir().join(format!(
            "lyra-artist-routes-test-{}-{}",
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
        Ok(TestDirGuard(test_dir))
    }

    async fn create_authenticated_headers(username: &str) -> anyhow::Result<HeaderMap> {
        let user_db_id = {
            let mut db = STATE.db.write().await;
            db::roles::ensure_builtin_roles(&mut db)?;
            let user_db_id = db::users::create(&mut db, &db::users::test_user(username)?)?;
            db::roles::ensure_user_has_role(&mut db, user_db_id, db::roles::BUILTIN_ADMIN_ROLE)?;
            user_db_id
        };

        let session = sessions::create_session_for_user(user_db_id, Default::default()).await?;
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            format!("Bearer {}", session.token)
                .parse()
                .expect("valid auth header"),
        );
        Ok(headers)
    }

    fn admin_principal(accessible_library_ids: HashSet<String>) -> Principal {
        Principal {
            user_db_id: DbId(1),
            user_public_id: "admin".to_string(),
            username: "admin".to_string(),
            permissions: vec![db::Permission::Admin],
            role_name: Some("admin".to_string()),
            accessible_library_ids,
        }
    }

    #[test]
    fn parse_inc_accepts_covers() {
        let parsed = match parse_inc(Some(vec!["releases,covers".to_string()])) {
            Ok(parsed) => parsed,
            Err(_) => panic!("covers inc should parse"),
        };

        assert!(parsed.service.releases);
        assert!(!parsed.service.tracks);
        assert!(!parsed.service.relations);
        assert!(parsed.covers);
    }

    #[tokio::test]
    async fn list_artist_responses_scopes_by_library_id() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let _test_dir = initialize_test_runtime().await?;

        let (visible_library_id, hidden_library_id) = {
            let mut db = STATE.db.write().await;
            let visible_library =
                insert_library(&mut db, "Visible Artists", "/tmp/lyra-visible-artists")?;
            let hidden_library =
                insert_library(&mut db, "Hidden Artists", "/tmp/lyra-hidden-artists")?;
            let visible_release = insert_release(&mut db, "Visible Artist Release")?;
            let hidden_release = insert_release(&mut db, "Hidden Artist Release")?;
            let visible_track = insert_track(&mut db, "Visible Artist Track")?;
            let hidden_track = insert_track(&mut db, "Hidden Artist Track")?;
            let visible_release_artist = insert_artist(&mut db, "Visible Release Artist")?;
            let visible_track_artist = insert_artist(&mut db, "Visible Track Artist")?;
            let hidden_artist = insert_artist(&mut db, "Hidden Artist")?;

            connect(&mut db, visible_library, visible_release)?;
            connect(&mut db, visible_release, visible_track)?;
            connect_artist(&mut db, visible_release, visible_release_artist)?;
            connect_artist(&mut db, visible_track, visible_track_artist)?;
            connect(&mut db, hidden_library, hidden_release)?;
            connect(&mut db, hidden_release, hidden_track)?;
            connect_artist(&mut db, hidden_release, hidden_artist)?;

            let visible_library_id = db::libraries::get_by_id(&db, visible_library)?
                .ok_or_else(|| anyhow::anyhow!("visible library missing"))?
                .id;
            let hidden_library_id = db::libraries::get_by_id(&db, hidden_library)?
                .ok_or_else(|| anyhow::anyhow!("hidden library missing"))?
                .id;
            (visible_library_id, hidden_library_id)
        };
        let principal = admin_principal(HashSet::from([
            visible_library_id.clone(),
            hidden_library_id,
        ]));

        let page = list_artist_responses(
            &principal,
            None,
            None,
            Some(visible_library_id),
            super::super::PageOptions {
                limit: 100,
                offset: 0,
            },
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;
        let mut names: Vec<String> = page.items.into_iter().map(|artist| artist.name).collect();
        names.sort();

        assert_eq!(
            names,
            vec![
                "Visible Release Artist".to_string(),
                "Visible Track Artist".to_string()
            ]
        );
        assert!(page.next_cursor.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn search_artist_covers_returns_empty_results_without_providers() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let _test_dir = initialize_test_runtime().await?;

        let artist_id = {
            let mut db = STATE.db.write().await;
            let artist_db_id = insert_artist(&mut db, "Coverless Artist")?;
            db::artists::get_by_id(&db, artist_db_id)?
                .expect("artist should exist")
                .id
        };

        let headers = create_authenticated_headers("artist-cover-tester").await?;
        let Json(response) = search_artist_covers(
            headers,
            Path(artist_id.clone()),
            Json(route_covers::CoverSearchQuery::default()),
        )
        .await
        .map_err(|err| anyhow::anyhow!("search_artist_covers failed: {err:?}"))?;

        assert_eq!(response.artist_id, artist_id);
        assert!(response.results.is_empty());
        Ok(())
    }
}
