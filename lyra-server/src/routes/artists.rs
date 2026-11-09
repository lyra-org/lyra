// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::{
    DbAny,
    DbId,
};
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
        post,
    },
};
use serde::{
    Deserialize,
    Serialize,
};

use crate::routes::request::bearer_credential;
use crate::{
    routes::AppError,
    routes::{
        PersonalFilterQuery,
        covers as route_covers,
        deserialize_inc,
        releases as route_releases,
        responses::{
            ArtistRelationResponse,
            ArtistResponse,
            PageResponse,
            RelatedArtistResponse,
            RelationDirectionResponse,
            ReleaseResponse,
            TrackChains,
        },
    },
    services::STATE,
    services::{
        artists as artist_service,
        auth::{
            Principal,
            access,
            require_authenticated,
        },
        catalog::{
            self,
            artists::{
                ArtistFilter,
                Artists,
            },
            pipeline::Catalog,
        },
        covers::{
            self,
            ResolvedCover,
            shown::CoverTarget,
        },
        pagination::SnapshotKey,
        releases as release_service,
    },
};
use std::collections::HashMap;

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct ArtistQuery {
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Comma-separated or repeated values: releases, tracks, relations, covers, relation_covers, release_artists, release_covers, artist_covers."
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
            description = "Comma-separated or repeated values: releases, tracks, relations, covers, relation_covers, release_artists, release_covers, artist_covers."
        )
    )]
    #[serde(default, deserialize_with = "deserialize_inc")]
    inc: Option<Vec<String>>,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Optional text query matched against artist names and sort names, and the names of duplicates merged into the artist; each of its words must start a word, ignoring case, accents, apostrophes and punctuation; no character is an operator, and a word in a script written without spaces, such as Japanese, matches anywhere in a word. Exact names rank first, then names starting with the query, then names holding every word, then names holding some, then matches only through related items."
        )
    )]
    query: Option<String>,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Optional public library ID to scope returned artists.")
    )]
    library_id: Option<String>,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Comma-separated or repeated values: name, sort_name, date_created, release_count, track_count, total_duration, listen_count, last_played_at, rating, rated_at, favorited_at, relevance (with query), random, id."
        )
    )]
    #[serde(default, deserialize_with = "deserialize_inc")]
    sort_by: Option<Vec<String>>,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Sort direction: ascending or descending.")
    )]
    sort_order: Option<String>,
    #[serde(flatten)]
    personal: PersonalFilterQuery,
    #[serde(flatten)]
    pub(crate) available: super::AvailableQuery,
    #[serde(flatten)]
    page: super::PageQuery,
}

pub(crate) struct ArtistListOptions {
    inc: Option<Vec<String>>,
    query: Option<String>,
    library_id: Option<String>,
    sort_by: Option<Vec<String>>,
    sort_order: Option<String>,
    personal: catalog::PersonalFilter,
    available: Option<bool>,
    page_request: super::SnapshotPageRequest,
}

impl ArtistListOptions {
    /// The options `GET /api/artists?query=` runs with only `inc`, personal filters and paging, so a
    /// search cursor resumes on that endpoint. Any other filter or sort would change the cursor key.
    pub(crate) fn for_query(
        inc: Option<Vec<String>>,
        query: String,
        personal: catalog::PersonalFilter,
        available: Option<bool>,
        page_request: super::SnapshotPageRequest,
    ) -> Self {
        Self {
            inc,
            query: Some(query),
            library_id: None,
            sort_by: None,
            sort_order: None,
            personal,
            available,
            page_request,
        }
    }
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
    relation_covers: bool,
    release_artists: bool,
    release_covers: bool,
    artist_covers: bool,
}

fn parse_inc(inc: Option<Vec<String>>) -> Result<ArtistRouteIncludes, AppError> {
    let values = super::parse_inc_values(
        inc,
        &[
            "releases",
            "tracks",
            "relations",
            "covers",
            "relation_covers",
            "release_artists",
            "release_covers",
            "artist_covers",
        ],
    )?;
    let mut result = ArtistRouteIncludes {
        service: artist_service::ArtistIncludes {
            releases: false,
            tracks: false,
            relations: false,
        },
        covers: false,
        relation_covers: false,
        release_artists: false,
        release_covers: false,
        artist_covers: false,
    };
    for value in values {
        match value.as_str() {
            "releases" => result.service.releases = true,
            "tracks" => result.service.tracks = true,
            "relations" => result.service.relations = true,
            "covers" => result.covers = true,
            "relation_covers" => result.relation_covers = true,
            "release_artists" => result.release_artists = true,
            "release_covers" => result.release_covers = true,
            "artist_covers" => result.artist_covers = true,
            _ => {}
        }
    }
    Ok(result)
}

fn filter_accessible_releases(
    db: &DbAny,
    principal: &Principal,
    releases: Vec<lyra_db::Release>,
) -> anyhow::Result<Vec<lyra_db::Release>> {
    let mut filtered = Vec::with_capacity(releases.len());
    for release in releases {
        let Some(release_db_id) = release.db_id.clone().map(DbId::from) else {
            continue;
        };
        if access::entity_accessible(db, principal, release_db_id)? {
            filtered.push(release);
        }
    }
    Ok(filtered)
}

fn filter_accessible_tracks(
    db: &DbAny,
    principal: &Principal,
    tracks: Vec<lyra_db::Track>,
) -> anyhow::Result<Vec<lyra_db::Track>> {
    let mut filtered = Vec::with_capacity(tracks.len());
    for track in tracks {
        let Some(track_db_id) = track.db_id.clone().map(DbId::from) else {
            continue;
        };
        if access::entity_accessible(db, principal, track_db_id)? {
            filtered.push(track);
        }
    }
    Ok(filtered)
}

fn filter_accessible_relations(
    db: &DbAny,
    principal: &Principal,
    relations: Vec<artist_service::ResolvedRelation>,
) -> anyhow::Result<Vec<artist_service::ResolvedRelation>> {
    let mut filtered = Vec::with_capacity(relations.len());
    for relation in relations {
        let Some(artist_db_id) = relation.artist.db_id.clone().map(DbId::from) else {
            continue;
        };
        if access::artist_accessible(db, principal, artist_db_id)? {
            filtered.push(relation);
        }
    }
    Ok(filtered)
}

/// An artist's details without what the caller can't see, with its releases' details when its
/// release responses carry more than the release.
struct ArtistParts {
    detail: artist_service::ArtistDetails,
    release_details: Option<Vec<release_service::ReleaseDetails>>,
}

fn prepare_artist(
    db: &DbAny,
    principal: &Principal,
    viewer: &catalog::Viewer,
    mut detail: artist_service::ArtistDetails,
    includes: ArtistRouteIncludes,
) -> anyhow::Result<ArtistParts> {
    if let Some(releases) = detail.releases.take() {
        detail.releases = Some(filter_accessible_releases(db, principal, releases)?);
    }
    if let Some(tracks) = detail.tracks.take() {
        detail.tracks = Some(filter_accessible_tracks(db, principal, tracks)?);
    }
    if let Some(relations) = detail.relations.take() {
        detail.relations = Some(filter_accessible_relations(db, principal, relations)?);
    }
    let release_details = match detail.releases.take() {
        Some(releases) if includes.release_artists || includes.release_covers => {
            Some(release_service::list_details_for_releases(
                db,
                viewer,
                release_service::ReleaseIncludes {
                    artists: includes.release_artists,
                    tracks: false,
                    track_artists: false,
                    entries: false,
                },
                releases,
            )?)
        }
        releases => {
            detail.releases = releases;
            None
        }
    };
    Ok(ArtistParts {
        detail,
        release_details,
    })
}

/// The responses of `details`, with the covers included resolved together.
fn artist_responses(
    db: &DbAny,
    principal: &Principal,
    details: Vec<artist_service::ArtistDetails>,
    includes: ArtistRouteIncludes,
) -> Result<Vec<ArtistResponse>, AppError> {
    let viewer = catalog::Viewer::user(db, principal.clone())?;
    let parts = details
        .into_iter()
        .map(|detail| prepare_artist(db, principal, &viewer, detail, includes))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let mut targets = Vec::new();
    for part in &parts {
        if includes.covers
            && let Some(artist_db_id) = part.detail.artist.db_id.clone().map(DbId::from)
        {
            targets.push(CoverTarget::Artist(artist_db_id));
        }
        if includes.relation_covers
            && let Some(relations) = &part.detail.relations
        {
            targets.extend(
                relations
                    .iter()
                    .filter_map(|relation| relation.artist.db_id.clone().map(DbId::from))
                    .map(CoverTarget::Artist),
            );
        }
        if let Some(release_details) = &part.release_details {
            route_releases::release_cover_targets(
                release_details,
                includes.release_covers,
                includes.artist_covers,
                &mut targets,
            );
        }
    }
    let covers = route_covers::resolve_principal_covers(db, principal, &targets)?;
    let mut items = Vec::with_capacity(parts.len());
    for part in parts {
        items.push(artist_to_response(db, part, includes, &covers)?);
    }
    Ok(items)
}

fn artist_to_response(
    db: &DbAny,
    parts: ArtistParts,
    includes: ArtistRouteIncludes,
    covers: &HashMap<DbId, ResolvedCover>,
) -> anyhow::Result<ArtistResponse> {
    let ArtistParts {
        detail,
        release_details,
    } = parts;
    let cover = route_covers::cover_field(
        covers,
        includes.covers,
        detail.artist.db_id.clone().map(DbId::from),
    );
    let availability = crate::services::catalog::Availability::of(db)?;
    let releases = match release_details {
        Some(release_details) => Some(
            release_details
                .into_iter()
                .map(|detail| {
                    route_releases::detail_to_release_response(
                        db,
                        detail,
                        covers,
                        includes.release_covers,
                        includes.artist_covers,
                        false,
                        false,
                    )
                })
                .collect::<anyhow::Result<Vec<_>>>()?,
        ),
        None => detail.releases.map(|releases| {
            releases
                .into_iter()
                .map(|release| ReleaseResponse::new(release, &availability))
                .collect()
        }),
    };
    let relations = detail.relations.map(|v| {
        v.into_iter()
            .map(|r| {
                let cover = route_covers::cover_field(
                    covers,
                    includes.relation_covers,
                    r.artist.db_id.clone().map(DbId::from),
                );
                ArtistRelationResponse {
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
                        cover,
                    },
                }
            })
            .collect()
    });
    Ok(ArtistResponse {
        available: Some(
            detail
                .artist
                .db_id
                .clone()
                .map(DbId::from)
                .is_none_or(|id| availability.artist(id)),
        ),
        id: detail.artist.id,
        name: detail.artist.artist_name,
        sort_name: detail.artist.sort_name,
        description: detail.artist.description,
        credit: None,
        releases,
        tracks: detail
            .tracks
            .map(|tracks| TrackChains::responses(db, tracks))
            .transpose()?,
        relations,
        cover,
    })
}

pub(crate) async fn list_artist_responses(
    principal: &Principal,
    options: ArtistListOptions,
) -> Result<PageResponse<ArtistResponse>, AppError> {
    artist_page(&*STATE.db.read().await, principal, options)
}

pub(crate) fn artist_page(
    db: &DbAny,
    principal: &Principal,
    options: ArtistListOptions,
) -> Result<PageResponse<ArtistResponse>, AppError> {
    let ArtistListOptions {
        inc,
        query,
        library_id,
        sort_by,
        sort_order,
        personal,
        available,
        page_request,
    } = options;
    let includes = parse_inc(inc)?;
    let search_term = super::parse_text_query(query);
    let snapshot_key = SnapshotKey::builder(&principal.user_public_id, "artists")
        .field(search_term.as_deref())
        .field(library_id.as_deref())
        .values(sort_by.as_deref())
        .field(sort_order.as_deref())
        .personal(personal)
        .flag(available)
        .finish();
    let sort = super::parse_catalog_sort(sort_by, sort_order)?;
    let library = crate::services::auth::access::resolve_optional_library_filter(
        db,
        principal,
        library_id.as_deref(),
    )?;

    let (artists, next_cursor) = if let Some(page) = page_request.resume(&snapshot_key)? {
        let artists = super::load_snapshot_items(
            db,
            &page.item_ids,
            lyra_db::artists::get_by_id,
            |db, artist_db_id| access::artist_accessible(db, principal, artist_db_id),
        )?;
        (artists, page.next_cursor)
    } else {
        let viewer = catalog::Viewer::user(db, principal.clone())?;
        let mut query = catalog::Query::<Artists>::new(ArtistFilter {
            library,
            personal,
            available,
            ..ArtistFilter::default()
        });
        query.search = search_term;
        query.sort = sort;
        query.seed = rand::random();
        let ids = catalog::leading(db, &viewer, &query, page_request.rank_limit())?;
        let page = page_request.start(&snapshot_key, super::public_ids(db, &ids)?)?;
        (
            Artists::hydrate(db, &ids[..page.item_ids.len()])?
                .into_iter()
                .map(|listed| listed.item)
                .collect(),
            page.next_cursor,
        )
    };
    let details = artist_service::list_details_for_artists(db, includes.service, artists)?;
    let items = artist_responses(db, principal, details, includes)?;
    Ok(PageResponse { items, next_cursor })
}

pub(crate) async fn get_artist_response(
    principal: &Principal,
    id: String,
    inc: Option<Vec<String>>,
) -> Result<ArtistResponse, AppError> {
    let db = &*STATE.db.read().await;
    let includes = parse_inc(inc)?;
    let artist_db_id = lyra_db::lookup::find_node_id_by_id(db, &id)?
        .ok_or_else(|| AppError::not_found(format!("not found: {id}")))?;
    if !access::artist_accessible(db, principal, artist_db_id)? {
        return Err(AppError::not_found(format!("Artist not found: {id}")));
    }
    let detail = artist_service::get_details(db, artist_db_id, includes.service)?
        .ok_or_else(|| AppError::not_found(format!("Artist not found: {}", id)))?;

    let mut responses = artist_responses(db, principal, vec![detail], includes)?;
    Ok(responses.remove(0))
}

async fn get_artists(
    headers: HeaderMap,
    Query(list_query): Query<ArtistListQuery>,
) -> Result<Json<PageResponse<ArtistResponse>>, AppError> {
    let ArtistListQuery {
        inc,
        query,
        library_id,
        sort_by,
        sort_order,
        personal,
        available,
        page,
    } = list_query;
    let page = page.resolve_snapshot();
    let principal = require_authenticated(bearer_credential(&headers)).await?;
    let personal = personal.parse()?;
    Ok(Json(
        list_artist_responses(
            &principal,
            ArtistListOptions {
                inc,
                query,
                library_id,
                sort_by,
                sort_order,
                personal,
                available: available.available,
                page_request: page,
            },
        )
        .await?,
    ))
}

async fn get_artist(
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<ArtistQuery>,
) -> Result<Json<ArtistResponse>, AppError> {
    let principal = require_authenticated(bearer_credential(&headers)).await?;
    Ok(Json(get_artist_response(&principal, id, query.inc).await?))
}

async fn search_artist_covers(
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(query): Json<route_covers::CoverSearchQuery>,
) -> Result<Json<ArtistCoverSearchResponse>, AppError> {
    let principal = require_authenticated(bearer_credential(&headers)).await?;
    let provider_filter = query.provider_filter()?;

    {
        let db = STATE.db.read().await;
        let artist_db_id = lyra_db::lookup::find_node_id_by_id(&*db, &id)?
            .ok_or_else(|| AppError::not_found(format!("not found: {id}")))?;
        if !access::artist_accessible(&db, &principal, artist_db_id)? {
            return Err(AppError::not_found(format!("Artist not found: {id}")));
        }
        if lyra_db::artists::get_by_id(&db, artist_db_id)?.is_none() {
            return Err(AppError::not_found(format!("Artist not found: {}", id)));
        }
    }

    let found = covers::search_artist_cover_candidates(
        &STATE.db.get(),
        &id,
        provider_filter,
        query.bypass_cover_cache,
    )
    .await?;
    let results = route_covers::map_provider_cover_search_results(found);

    Ok(Json(ArtistCoverSearchResponse {
        artist_id: id,
        results,
    }))
}

#[cfg(feature = "docgen")]
fn list_artists_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List artists").description(
        "Returns artists as `{ items, next_cursor }`. Supported query parameters: `inc`, `query`, `library_id`, `sort_by`, `sort_order`, `rated`, `min_rating`, `max_rating`, `favorite`, `listened`, `available`, `limit`, `cursor`. `available=true` keeps the artists whose `available` field is true, counting only their tracks that can play; `available=false` keeps the artists credited on a track that can't play, or on a release holding one, partly playable ones included, counting only such tracks. `min_rating` and `max_rating` filter artists by the authenticated user's inclusive personal rating range; either bound excludes unrated artists. `rated=true` keeps only artists the user has rated and `rated=false` only those they have not; `rated=false` cannot be combined with a rating bound. `favorite=true` keeps only artists the user has favorited and `favorite=false` the rest. `listened=true` keeps only artists with at least one listened track among their credited tracks or releases and `listened=false` the rest. `library_id` scopes results to artists credited by releases or tracks belonging to that public library ID, and the `release_count`, `track_count`, `total_duration`, `listen_count` and `last_played_at` sort keys to the releases in it and their tracks. `sort_by` supports `sort_name`, `name`, `date_created`, `last_played_at`, `listen_count`, `release_count`, `track_count`, `total_duration`, `rating` (the authenticated user's personal rating; unrated artists sort last in both directions), `rated_at` (when the authenticated user last set their rating), `favorited_at` (when they first favorited the artist), and `id`; `sort_order` supports `ascending` and `descending`. `limit` defaults to 100 and is capped at 500. Drive pagination from `next_cursor`; it is `null` on the last page. `query` matches artist names and sort names, and the names of duplicates merged into the artist; each of its words must start a word, ignoring case, accents, apostrophes and punctuation; no character is an operator, and a word in a script written without spaces, such as Japanese, matches anywhere in a word. It defaults ordering to relevance: exact names rank first, then names starting with the query, then names holding every word, then names holding some, then matches only through related items. Use `inc` to include releases, tracks, relations, and/or covers. When `inc=covers`, artist cover metadata includes a public image URL. When `inc=relations`, add `relation_covers` to include public image metadata for related artists. When `inc=releases`, use `release_artists`, `release_covers`, and/or `artist_covers` to hydrate those nested release fields. The `credit` field is not present on artist-level responses; it only appears when artists are included via track or release endpoints.",
    )
}

#[cfg(feature = "docgen")]
fn get_artist_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Get artist by ID").description(
        "Returns a single artist. 404 if not found. Use `inc` to include releases, tracks, relations, and/or covers. When `inc=covers`, artist cover metadata includes a public image URL. When `inc=relations`, add `relation_covers` to include public image metadata for related artists. When `inc=releases`, use `release_artists`, `release_covers`, and/or `artist_covers` to hydrate those nested release fields. The `credit` field is not present on artist-level responses; it only appears when artists are included via track or release endpoints.",
    )
}

#[cfg(feature = "docgen")]
fn search_artist_covers_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Search artist cover candidates").description(
        "Returns provider cover candidates for an artist. Request body (JSON): `{ provider?, bypass_cover_cache? }`; an invalid `provider` id is rejected with 400; \
        `bypass_cover_cache: true` looks covers up again instead of using cached lookups. Providers may return \
        width, height, and selected_index for automatic selection.",
    )
}

pub fn artist_routes() -> Router {
    Router::new()
        .route("/", get(get_artists))
        .route("/{id}", get(get_artist))
        .route("/{id}/covers/search", post(search_artist_covers))
        .merge(route_covers::owner_cover_routes::<route_covers::ArtistCovers>())
}

#[cfg(feature = "docgen")]
pub(crate) fn artist_openapi_routes() -> aide::axum::ApiRouter {
    use aide::axum::routing::{
        get_with,
        post_with,
    };

    aide::axum::ApiRouter::new()
        .api_route("/", get_with(get_artists, list_artists_docs))
        .api_route("/{id}", get_with(get_artist, get_artist_docs))
        .api_route(
            "/{id}/covers/search",
            post_with(search_artist_covers, search_artist_covers_docs),
        )
        .merge(route_covers::owner_cover_openapi_routes::<
            route_covers::ArtistCovers,
        >())
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        plugins::testing::init_library_test_state,
        services::testing::runtime_test_lock,
    };
    use lyra_db::fixtures::{
        connect,
        insert_artist,
        insert_release,
        insert_track,
    };
    use lyra_db::test_db::{
        connect_artist,
        insert_credited_artist,
        insert_library,
    };

    use agdb::{
        DbAny,
        DbId,
    };
    use axum::{
        Json,
        extract::Path,
        http::{
            HeaderMap,
            header::AUTHORIZATION,
        },
    };
    use nanoid::nanoid;
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
        init_library_test_state(test_dir.clone()).await?;
        Ok(TestDirGuard(test_dir))
    }

    async fn create_authenticated_headers(username: &str) -> anyhow::Result<HeaderMap> {
        let user_db_id = {
            let mut db = STATE.db.write().await;
            lyra_db::roles::ensure_builtin_roles(&mut db)?;
            let user_db_id =
                lyra_db::users::create(&mut db, &lyra_db::fixtures::test_user(username)?)?;
            lyra_db::roles::ensure_user_has_role(
                &mut db,
                user_db_id,
                lyra_db::roles::BUILTIN_ADMIN_ROLE,
            )?;
            user_db_id
        };

        let session =
            crate::services::testing::create_session(user_db_id, Default::default()).await?;
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            format!("Bearer {}", session.token)
                .parse()
                .expect("valid auth header"),
        );
        Ok(headers)
    }

    async fn principal(
        permissions: Vec<lyra_db::Permission>,
        accessible_library_ids: HashSet<String>,
    ) -> Principal {
        let mut db = STATE.db.write().await;
        let user_db_id = lyra_db::test_db::insert_user(&mut db, &nanoid!()).expect("insert user");
        Principal::for_user(&*db, user_db_id, permissions, accessible_library_ids)
    }

    async fn admin_principal(accessible_library_ids: HashSet<String>) -> Principal {
        principal(vec![lyra_db::Permission::Admin], accessible_library_ids).await
    }

    async fn user_principal(accessible_library_ids: HashSet<String>) -> Principal {
        principal(Vec::new(), accessible_library_ids).await
    }

    fn insert_cover_for(db: &mut DbAny, owner_db_id: DbId) -> anyhow::Result<lyra_db::Cover> {
        db.transaction_mut(|t| {
            lyra_db::covers::upsert(
                t,
                owner_db_id,
                lyra_db::covers::CoverSource::Local,
                lyra_db::Cover {
                    db_id: None,
                    id: nanoid!(),
                    path: crate::services::testing::existing_cover_path(),
                    mime_type: "image/jpeg".to_string(),
                    hash: "a".repeat(64),
                    blurhash: Some("LKO2?U%2Tw=w]~RBVZRi};RPxuwH".to_string()),
                    provider_id: None,
                },
            )
        })
        .map(|upserted| upserted.cover)
    }

    #[tokio::test]
    async fn list_artist_responses_counts_only_accessible_releases() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let _test_dir = initialize_test_runtime().await?;

        let visible_library_id = {
            let mut db = STATE.db.write().await;
            let visible_library =
                insert_library(&mut db, "Visible Artist Counts", "/tmp/lyra-visible-counts")?;
            let hidden_library =
                insert_library(&mut db, "Hidden Artist Counts", "/tmp/lyra-hidden-counts")?;
            let mostly_hidden = insert_artist(&mut db, "Mostly Hidden Artist")?;
            let visible_heavy = insert_artist(&mut db, "Visible Heavy Artist")?;

            let visible_release = insert_release(&mut db, "Only Visible Release")?;
            connect(&mut db, visible_library, visible_release)?;
            connect_artist(&mut db, visible_release, mostly_hidden)?;
            for title in ["Hidden One", "Hidden Two", "Hidden Three"] {
                let release = insert_release(&mut db, title)?;
                connect(&mut db, hidden_library, release)?;
                connect_artist(&mut db, release, mostly_hidden)?;
            }
            for title in ["Visible One", "Visible Two"] {
                let release = insert_release(&mut db, title)?;
                connect(&mut db, visible_library, release)?;
                connect_artist(&mut db, release, visible_heavy)?;
            }
            lyra_db::libraries::get_by_id(&db, visible_library)?
                .ok_or_else(|| anyhow::anyhow!("visible library missing"))?
                .id
        };
        let principal = user_principal(HashSet::from([visible_library_id])).await;

        assert_eq!(
            list_artist_names(
                &principal,
                list_options(Some("release_count"), Some("descending"))
            )
            .await?,
            vec!["Visible Heavy Artist", "Mostly Hidden Artist"]
        );
        Ok(())
    }

    #[test]
    fn parse_inc_accepts_covers() {
        let parsed = match parse_inc(Some(vec![
            "releases,covers,relation_covers,artist_covers".to_string(),
        ])) {
            Ok(parsed) => parsed,
            Err(_) => panic!("covers inc should parse"),
        };

        assert!(parsed.service.releases);
        assert!(!parsed.service.tracks);
        assert!(!parsed.service.relations);
        assert!(parsed.covers);
        assert!(parsed.relation_covers);
        assert!(!parsed.release_artists);
        assert!(!parsed.release_covers);
        assert!(parsed.artist_covers);
    }

    #[tokio::test]
    async fn get_artist_response_keeps_cover_includes_separate() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let _test_dir = initialize_test_runtime().await?;

        let artist_public_id = {
            let mut db = STATE.db.write().await;
            let release_id = insert_release(&mut db, "Covered Release")?;
            let artist_id = insert_artist(&mut db, "Release Artist")?;
            connect_artist(&mut db, release_id, artist_id)?;
            insert_cover_for(&mut db, release_id)?;

            lyra_db::artists::get_by_id(&db, artist_id)?
                .ok_or_else(|| anyhow::anyhow!("artist should exist"))?
                .id
        };
        let principal = admin_principal(HashSet::new()).await;

        let shallow = get_artist_response(
            &principal,
            artist_public_id.clone(),
            Some(vec!["releases,covers".to_string()]),
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;
        assert!(matches!(shallow.cover, Some(None)));

        let shallow_releases = shallow.releases.expect("releases included");
        assert_eq!(shallow_releases.len(), 1);
        assert!(shallow_releases[0].artists.is_none());
        assert!(shallow_releases[0].cover.is_none());

        let hydrated = get_artist_response(
            &principal,
            artist_public_id,
            Some(vec!["releases,release_artists,release_covers".to_string()]),
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;
        assert!(hydrated.cover.is_none());

        let hydrated_releases = hydrated.releases.expect("releases included");
        assert_eq!(hydrated_releases.len(), 1);
        let release = &hydrated_releases[0];
        let artists = release.artists.as_ref().expect("release artists included");
        assert_eq!(artists.len(), 1);
        assert_eq!(artists[0].name, "Release Artist");
        let cover = release
            .cover
            .as_ref()
            .and_then(Option::as_ref)
            .expect("release cover included");
        assert_eq!(cover.mime_type, "image/jpeg");
        Ok(())
    }

    #[tokio::test]
    async fn get_artist_response_hydrates_relation_covers() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let _test_dir = initialize_test_runtime().await?;

        let (artist_public_id, cover_id) = {
            let mut db = STATE.db.write().await;
            let character = insert_artist(&mut db, "Character Artist")?;
            let voice_actor = insert_artist(&mut db, "Voice Actor")?;
            lyra_db::artists::relations::link(
                &mut db,
                voice_actor,
                character,
                lyra_db::ArtistRelationType::VoiceActor,
                Some("main".to_string()),
            )?;
            let cover_id = insert_cover_for(&mut db, voice_actor)?.id;
            let artist_public_id = lyra_db::artists::get_by_id(&db, character)?
                .ok_or_else(|| anyhow::anyhow!("artist should exist"))?
                .id;
            (artist_public_id, cover_id)
        };
        let principal = admin_principal(HashSet::new()).await;

        let artist = get_artist_response(
            &principal,
            artist_public_id,
            Some(vec!["relations,relation_covers".to_string()]),
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        let relation = artist
            .relations
            .as_ref()
            .and_then(|relations| relations.first())
            .ok_or_else(|| anyhow::anyhow!("expected artist relation"))?;
        assert_eq!(relation.artist.name, "Voice Actor");
        let cover = relation
            .artist
            .cover
            .as_ref()
            .and_then(Option::as_ref)
            .ok_or_else(|| anyhow::anyhow!("expected relation artist cover"))?;
        assert_eq!(cover.id, cover_id);
        Ok(())
    }

    #[tokio::test]
    async fn get_artist_response_filters_includes_by_library_access() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let _test_dir = initialize_test_runtime().await?;

        let (artist_public_id, visible_library_id) = {
            let mut db = STATE.db.write().await;
            let visible_library = insert_library(
                &mut db,
                "Visible Artist Include",
                "/tmp/lyra-visible-artist-inc",
            )?;
            let hidden_library = insert_library(
                &mut db,
                "Hidden Artist Include",
                "/tmp/lyra-hidden-artist-inc",
            )?;
            let artist = insert_artist(&mut db, "Shared Artist")?;
            let hidden_relation = insert_artist(&mut db, "Hidden Relation")?;

            let visible_release = insert_release(&mut db, "Visible Release")?;
            let hidden_release = insert_release(&mut db, "Hidden Release")?;
            let visible_track = insert_track(&mut db, "Visible Track")?;
            let hidden_track = insert_track(&mut db, "Hidden Track")?;

            connect(&mut db, visible_library, visible_release)?;
            connect(&mut db, visible_release, visible_track)?;
            connect_artist(&mut db, visible_release, artist)?;
            connect_artist(&mut db, visible_track, artist)?;

            connect(&mut db, hidden_library, hidden_release)?;
            connect(&mut db, hidden_release, hidden_track)?;
            connect_artist(&mut db, hidden_release, artist)?;
            connect_artist(&mut db, hidden_track, artist)?;
            connect_artist(&mut db, hidden_release, hidden_relation)?;
            lyra_db::artists::relations::link(
                &mut db,
                artist,
                hidden_relation,
                lyra_db::ArtistRelationType::MemberOf,
                None,
            )?;

            let artist_public_id = lyra_db::artists::get_by_id(&db, artist)?
                .ok_or_else(|| anyhow::anyhow!("artist missing"))?
                .id;
            let visible_library_id = lyra_db::libraries::get_by_id(&db, visible_library)?
                .ok_or_else(|| anyhow::anyhow!("visible library missing"))?
                .id;
            (artist_public_id, visible_library_id)
        };
        let principal = user_principal(HashSet::from([visible_library_id])).await;

        let artist = get_artist_response(
            &principal,
            artist_public_id,
            Some(vec!["releases,tracks,relations".to_string()]),
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        let releases = artist.releases.expect("releases included");
        let release_titles: Vec<String> =
            releases.into_iter().map(|release| release.title).collect();
        assert_eq!(release_titles, vec!["Visible Release"]);

        let tracks = artist.tracks.expect("tracks included");
        let track_titles: Vec<String> = tracks.into_iter().map(|track| track.title).collect();
        assert_eq!(track_titles, vec!["Visible Track"]);

        let relations = artist.relations.expect("relations included");
        assert!(
            relations.is_empty(),
            "hidden relation peers must not be included in artist responses",
        );
        Ok(())
    }

    #[tokio::test]
    async fn list_artist_responses_orders_query_matches_by_relevance() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let _test_dir = initialize_test_runtime().await?;

        {
            let mut db = STATE.db.write().await;
            for name in ["A Blue", "Blue"] {
                insert_credited_artist(&mut db, name)?;
            }
        }
        let principal = admin_principal(HashSet::new()).await;

        let page = list_artist_responses(
            &principal,
            ArtistListOptions {
                inc: None,
                query: Some("blue".to_string()),
                library_id: None,
                sort_by: None,
                sort_order: None,
                personal: catalog::PersonalFilter::default(),
                page_request: super::super::SnapshotPageRequest::first_page(100),
                available: None,
            },
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        let names: Vec<String> = page.items.into_iter().map(|artist| artist.name).collect();
        assert_eq!(names, vec!["Blue", "A Blue"]);
        Ok(())
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

            let visible_library_id = lyra_db::libraries::get_by_id(&db, visible_library)?
                .ok_or_else(|| anyhow::anyhow!("visible library missing"))?
                .id;
            let hidden_library_id = lyra_db::libraries::get_by_id(&db, hidden_library)?
                .ok_or_else(|| anyhow::anyhow!("hidden library missing"))?
                .id;
            (visible_library_id, hidden_library_id)
        };
        let principal = admin_principal(HashSet::from([
            visible_library_id.clone(),
            hidden_library_id,
        ]))
        .await;

        let page = list_artist_responses(
            &principal,
            ArtistListOptions {
                inc: None,
                query: None,
                library_id: Some(visible_library_id),
                sort_by: None,
                sort_order: None,
                personal: catalog::PersonalFilter::default(),
                page_request: super::super::SnapshotPageRequest::first_page(100),
                available: None,
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
    async fn list_artist_responses_filters_exact_personal_rating() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let _test_dir = initialize_test_runtime().await?;

        let principal = {
            let mut db = STATE.db.write().await;
            let user_db_id = lyra_db::users::create(
                &mut db,
                &lyra_db::fixtures::test_user("artist-rating-user")?,
            )?;
            let four_star = insert_artist(&mut db, "Four Star Artist")?;
            let five_star = insert_artist(&mut db, "Five Star Artist")?;
            insert_artist(&mut db, "Unrated Artist")?;
            for (target, value) in [(four_star, 4), (five_star, 5)] {
                lyra_db::ratings::upsert(
                    &mut *db,
                    user_db_id,
                    target,
                    lyra_db::ratings::RatingValue::new(value).unwrap(),
                    1,
                )?;
            }
            Principal::for_user(
                &*db,
                user_db_id,
                vec![lyra_db::Permission::Admin],
                HashSet::new(),
            )
        };
        let exact_four = lyra_db::ratings::RatingFilter::new(
            None,
            lyra_db::ratings::RatingValue::new(4),
            lyra_db::ratings::RatingValue::new(4),
        )
        .unwrap();

        let page = list_artist_responses(
            &principal,
            ArtistListOptions {
                inc: None,
                query: None,
                library_id: None,
                sort_by: None,
                sort_order: None,
                personal: catalog::PersonalFilter {
                    rating: exact_four,
                    ..catalog::PersonalFilter::default()
                },
                page_request: super::super::SnapshotPageRequest::first_page(100),
                available: None,
            },
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].name, "Four Star Artist");
        assert!(page.next_cursor.is_none());
        Ok(())
    }

    fn list_options(sort_by: Option<&str>, sort_order: Option<&str>) -> ArtistListOptions {
        ArtistListOptions {
            inc: None,
            query: None,
            library_id: None,
            sort_by: sort_by.map(|value| vec![value.to_string()]),
            sort_order: sort_order.map(str::to_string),
            personal: catalog::PersonalFilter::default(),
            page_request: super::super::SnapshotPageRequest::first_page(100),
            available: None,
        }
    }

    async fn list_artist_names(
        principal: &Principal,
        options: ArtistListOptions,
    ) -> anyhow::Result<Vec<String>> {
        let page = list_artist_responses(principal, options)
            .await
            .map_err(|err| anyhow::anyhow!("{err:?}"))?;
        Ok(page.items.into_iter().map(|artist| artist.name).collect())
    }

    #[tokio::test]
    async fn list_artist_responses_defaults_to_sort_name_then_name_then_id() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let _test_dir = initialize_test_runtime().await?;

        {
            let mut db = STATE.db.write().await;
            insert_credited_artist(&mut db, "Charlie")?;
            let zulu = insert_credited_artist(&mut db, "Zulu")?;
            let mut artist = lyra_db::artists::get_by_id(&db, zulu)?
                .ok_or_else(|| anyhow::anyhow!("artist missing"))?;
            artist.sort_name = Some("Alpha".to_string());
            lyra_db::artists::update(&mut db, &artist)?;
            insert_credited_artist(&mut db, "Beta")?;
            insert_credited_artist(&mut db, "beta")?;
            for name in ["Mike", "Lima"] {
                let artist_db_id = insert_credited_artist(&mut db, name)?;
                let mut artist = lyra_db::artists::get_by_id(&db, artist_db_id)?
                    .ok_or_else(|| anyhow::anyhow!("artist missing"))?;
                artist.sort_name = Some("Delta".to_string());
                lyra_db::artists::update(&mut db, &artist)?;
            }
        }
        let principal = admin_principal(HashSet::new()).await;

        assert_eq!(
            list_artist_names(&principal, list_options(None, None)).await?,
            vec!["Zulu", "Beta", "beta", "Charlie", "Lima", "Mike"]
        );
        assert_eq!(
            list_artist_names(
                &principal,
                list_options(Some("sort_name"), Some("descending"))
            )
            .await?,
            vec!["Lima", "Mike", "Charlie", "Beta", "beta", "Zulu"]
        );
        Ok(())
    }

    #[tokio::test]
    async fn list_artist_responses_resume_rechecks_access() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let _test_dir = initialize_test_runtime().await?;

        let (library_id, library, release) = {
            let mut db = STATE.db.write().await;
            let library = insert_library(&mut db, "Artist Resume", "/tmp/lyra-artist-resume")?;
            let release = insert_release(&mut db, "Resume Release")?;
            connect(&mut db, library, release)?;
            for name in ["First", "Second"] {
                let artist = insert_artist(&mut db, name)?;
                connect_artist(&mut db, release, artist)?;
            }
            let library_id = lyra_db::libraries::get_by_id(&db, library)?
                .ok_or_else(|| anyhow::anyhow!("library missing"))?
                .id;
            (library_id, library, release)
        };
        let principal = user_principal(HashSet::from([library_id])).await;
        let page = |cursor: Option<String>| ArtistListOptions {
            page_request: super::super::PageQuery {
                limit: Some(1),
                cursor,
            }
            .resolve_snapshot(),
            ..list_options(None, None)
        };

        let first = list_artist_responses(&principal, page(None))
            .await
            .map_err(|err| anyhow::anyhow!("{err:?}"))?;
        assert_eq!(first.items.len(), 1);
        let cursor = first
            .next_cursor
            .ok_or_else(|| anyhow::anyhow!("expected a second page"))?;

        {
            let mut db = STATE.db.write().await;
            lyra_db::graph::remove_edges_between(&mut *db, library, release)?;
        }

        let second = list_artist_responses(&principal, page(Some(cursor)))
            .await
            .map_err(|err| anyhow::anyhow!("{err:?}"))?;
        assert!(second.items.is_empty());
        assert!(second.next_cursor.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn artists_credited_only_on_tracks_follow_track_visibility() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let _test_dir = initialize_test_runtime().await?;

        let (visible_library_id, visible_artist_id, hidden_artist_id) = {
            let mut db = STATE.db.write().await;
            let visible_library = insert_library(
                &mut db,
                "Track Credit Visible",
                "/tmp/lyra-track-credit-vis",
            )?;
            let hidden_library =
                insert_library(&mut db, "Track Credit Hidden", "/tmp/lyra-track-credit-hid")?;
            let visible_release = insert_release(&mut db, "Visible Compilation")?;
            let hidden_release = insert_release(&mut db, "Hidden Compilation")?;
            let visible_track = insert_track(&mut db, "Visible Feature")?;
            let hidden_track = insert_track(&mut db, "Hidden Feature")?;
            let visible_artist = insert_artist(&mut db, "Featured Visible")?;
            let hidden_artist = insert_artist(&mut db, "Featured Hidden")?;
            connect(&mut db, visible_library, visible_release)?;
            connect(&mut db, visible_release, visible_track)?;
            connect_artist(&mut db, visible_track, visible_artist)?;
            connect(&mut db, hidden_library, hidden_release)?;
            connect(&mut db, hidden_release, hidden_track)?;
            connect_artist(&mut db, hidden_track, hidden_artist)?;

            let visible_library_id = lyra_db::libraries::get_by_id(&db, visible_library)?
                .ok_or_else(|| anyhow::anyhow!("visible library missing"))?
                .id;
            let public_id = |artist| -> anyhow::Result<String> {
                Ok(lyra_db::artists::get_by_id(&db, artist)?
                    .ok_or_else(|| anyhow::anyhow!("artist missing"))?
                    .id)
            };
            (
                visible_library_id,
                public_id(visible_artist)?,
                public_id(hidden_artist)?,
            )
        };
        let principal = user_principal(HashSet::from([visible_library_id])).await;

        assert_eq!(
            list_artist_names(&principal, list_options(None, None)).await?,
            vec!["Featured Visible"]
        );
        let artist = get_artist_response(&principal, visible_artist_id, None)
            .await
            .map_err(|err| anyhow::anyhow!("{err:?}"))?;
        assert_eq!(artist.name, "Featured Visible");
        assert!(
            get_artist_response(&principal, hidden_artist_id, None)
                .await
                .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn list_artist_responses_applies_sort_options() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let _test_dir = initialize_test_runtime().await?;

        {
            let mut db = STATE.db.write().await;
            insert_credited_artist(&mut db, "Alpha")?;
            insert_credited_artist(&mut db, "Charlie")?;
            insert_credited_artist(&mut db, "Bravo")?;
        }
        let principal = admin_principal(HashSet::new()).await;

        let page = list_artist_responses(
            &principal,
            ArtistListOptions {
                inc: None,
                query: None,
                library_id: None,
                sort_by: Some(vec!["name".to_string()]),
                sort_order: Some("descending".to_string()),
                personal: catalog::PersonalFilter::default(),
                page_request: super::super::SnapshotPageRequest::first_page(100),
                available: None,
            },
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        let names: Vec<String> = page.items.into_iter().map(|artist| artist.name).collect();
        assert_eq!(names, vec!["Charlie", "Bravo", "Alpha"]);
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
            lyra_db::artists::get_by_id(&db, artist_db_id)?
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

#[cfg(all(test, feature = "nightly"))]
mod benches {
    extern crate test;

    use agdb::{
        DbAny,
        DbId,
    };
    use test::{
        Bencher,
        black_box,
    };

    use super::*;
    use crate::services::catalog::artists::ArtistKey;
    use lyra_db::fixtures::{
        connect,
        insert_artist,
        insert_release,
        insert_track,
        test_user,
    };
    use lyra_db::test_db::{
        connect_artist,
        new_test_db,
    };

    struct ArtistSortBench {
        db: DbAny,
        viewer: catalog::Viewer,
    }

    fn update_track_duration(db: &mut DbAny, track_db_id: DbId, duration_ms: u64) {
        let mut track = lyra_db::tracks::get_by_id(db, track_db_id)
            .unwrap()
            .expect("track exists");
        track.duration_ms = Some(duration_ms);
        lyra_db::tracks::update(db, &track).unwrap();
    }

    fn seed_artist_sort_bench(
        artist_count: usize,
        releases_per_artist: usize,
        tracks_per_release: usize,
        listens_per_track: usize,
    ) -> ArtistSortBench {
        let mut db = new_test_db().unwrap();
        let user_db_id =
            lyra_db::users::create(&mut db, &test_user("artist-sort-bench").unwrap()).unwrap();
        for artist_idx in 0..artist_count {
            let artist_db_id = insert_artist(&mut db, &format!("Artist {artist_idx:04}")).unwrap();
            for release_idx in 0..releases_per_artist {
                let release_db_id = insert_release(
                    &mut db,
                    &format!("Artist {artist_idx:04} Release {release_idx:02}"),
                )
                .unwrap();
                connect_artist(&mut db, release_db_id, artist_db_id).unwrap();
                for track_idx in 0..tracks_per_release {
                    let track_db_id = insert_track(
                        &mut db,
                        &format!(
                            "Artist {artist_idx:04} Release {release_idx:02} Track {track_idx:02}"
                        ),
                    )
                    .unwrap();
                    update_track_duration(
                        &mut db,
                        track_db_id,
                        60_000 + ((artist_idx + release_idx + track_idx) % 300) as u64 * 1_000,
                    );
                    for listen_idx in 0..listens_per_track {
                        lyra_db::fixtures::insert_listen(
                            &mut db,
                            user_db_id,
                            track_db_id,
                            ((artist_idx * releases_per_artist * tracks_per_release)
                                + (release_idx * tracks_per_release)
                                + track_idx
                                + listen_idx) as u64
                                * 1_000,
                        )
                        .unwrap();
                    }
                    connect(&mut db, release_db_id, track_db_id).unwrap();
                }
            }
        }

        let principal = Principal::for_user(
            &db,
            user_db_id,
            vec![lyra_db::Permission::Admin],
            Default::default(),
        );
        let viewer = catalog::Viewer::user(&db, principal).unwrap();
        ArtistSortBench { db, viewer }
    }

    fn bench_order(b: &mut Bencher, setup: &ArtistSortBench, sort: catalog::SortSpec<ArtistKey>) {
        let mut query = catalog::Query::<Artists>::new(ArtistFilter::default());
        query.sort = sort;
        b.iter(|| catalog::order(&setup.db, &setup.viewer, black_box(&query)).unwrap());
    }

    #[bench]
    fn route_sort_artists_sort_name_500(b: &mut Bencher) {
        let setup = seed_artist_sort_bench(500, 0, 0, 0);
        bench_order(b, &setup, Vec::new());
    }

    #[bench]
    fn route_sort_artists_total_duration_100_artists_2000_tracks(b: &mut Bencher) {
        let setup = seed_artist_sort_bench(100, 5, 4, 0);
        bench_order(
            b,
            &setup,
            vec![(ArtistKey::TotalDuration, catalog::Direction::Descending)],
        );
    }

    #[bench]
    fn route_sort_artists_listen_count_100_artists_2000_listens(b: &mut Bencher) {
        let setup = seed_artist_sort_bench(100, 5, 4, 1);
        bench_order(
            b,
            &setup,
            vec![(ArtistKey::ListenCount, catalog::Direction::Descending)],
        );
    }
}
