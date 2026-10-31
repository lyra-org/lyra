// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

#[cfg(feature = "docgen")]
use aide::transform::TransformOperation;
use axum::{
    Json,
    extract::Query,
    http::HeaderMap,
};
use axum::{
    Router,
    routing::get,
};
use serde::{
    Deserialize,
    Serialize,
};

use crate::routes::request::bearer_credential;
use crate::{
    routes::{
        AppError,
        SnapshotPageRequest,
        artists::{
            ArtistListOptions,
            artist_page,
        },
        deserialize_inc,
        releases::{
            ReleaseListOptions,
            release_page,
        },
        responses::{
            ArtistResponse,
            PageResponse,
            ReleaseResponse,
            TrackResponse,
        },
        tracks::{
            TrackListOptions,
            track_page,
        },
    },
    services::{
        STATE,
        auth::{
            Principal,
            require_authenticated,
        },
    },
};

use super::PersonalFilterQuery;

const MAX_QUERY_LEN: usize = 256;
const DEFAULT_LIMIT: u64 = 20;
const MAX_LIMIT: u64 = 50;

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct SearchQuery {
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Text query matched against the names and sort names of tracks, \
        artists, and releases, and the names of related items the caller can see; see the \
        operation description for the rules. Required, 1-256 characters after trimming."
        )
    )]
    query: String,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Optional per-entity page size. Defaults to 20, capped at 50.")
    )]
    #[serde(default, deserialize_with = "super::deserialize_optional_scalar")]
    limit: Option<u64>,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Whether a branch with more than `limit` matches returns a `next_cursor` \
        for its list endpoint. Defaults to false, which keeps no cursor snapshot."
        )
    )]
    #[serde(default)]
    paginate: bool,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Track `inc` values, as on `GET /api/tracks`. An unsupported value fails \
        the whole search with 400."
        )
    )]
    #[serde(default, deserialize_with = "deserialize_inc")]
    track_inc: Option<Vec<String>>,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Artist `inc` values, as on `GET /api/artists`. An unsupported value fails \
        the whole search with 400."
        )
    )]
    #[serde(default, deserialize_with = "deserialize_inc")]
    artist_inc: Option<Vec<String>>,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Release `inc` values, as on `GET /api/releases`. An unsupported value fails \
        the whole search with 400."
        )
    )]
    #[serde(default, deserialize_with = "deserialize_inc")]
    release_inc: Option<Vec<String>>,
    #[serde(flatten)]
    personal: PersonalFilterQuery,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
pub(crate) struct SearchResponse {
    pub tracks: PageResponse<TrackResponse>,
    pub artists: PageResponse<ArtistResponse>,
    pub releases: PageResponse<ReleaseResponse>,
}

async fn search(
    headers: HeaderMap,
    Query(params): Query<SearchQuery>,
) -> Result<Json<SearchResponse>, AppError> {
    let principal = require_authenticated(bearer_credential(&headers)).await?;
    Ok(Json(search_responses(principal, params).await?))
}

async fn search_responses(
    principal: Principal,
    params: SearchQuery,
) -> Result<SearchResponse, AppError> {
    let query = params.query.trim().to_string();
    if query.is_empty() {
        return Err(AppError::bad_request("query must not be empty"));
    }
    if query.chars().count() > MAX_QUERY_LEN {
        return Err(AppError::bad_request(format!(
            "query exceeds {MAX_QUERY_LEN} characters"
        )));
    }
    let personal = params.personal.parse()?;
    let limit = page_limit(params.limit);
    let page_request = || {
        if params.paginate {
            SnapshotPageRequest::first_page(limit)
        } else {
            SnapshotPageRequest::only_page(limit)
        }
    };
    let tracks =
        TrackListOptions::for_query(params.track_inc, query.clone(), personal, page_request());
    let artists =
        ArtistListOptions::for_query(params.artist_inc, query.clone(), personal, page_request());
    let releases =
        ReleaseListOptions::for_query(params.release_inc, query, personal, page_request());

    // One guard keeps the branches consistent; ranking each catalog in memory is CPU-bound.
    let db = STATE.db.read().await;
    tokio::task::spawn_blocking(move || {
        Ok(SearchResponse {
            tracks: track_page(&db, &principal, tracks)?,
            artists: artist_page(&db, &principal, artists)?,
            releases: release_page(&db, &principal, releases)?,
        })
    })
    .await
    .map_err(|err| AppError::from(anyhow::anyhow!("search task failed: {err}")))?
}

fn page_limit(limit: Option<u64>) -> usize {
    limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT) as usize
}

#[cfg(feature = "docgen")]
fn search_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Cross-entity search").description(
        "Returns the first page of `GET /api/tracks`, `GET /api/artists` and `GET /api/releases` \
         for `query`, ranked by relevance, as `tracks`, `artists` and `releases`. Each branch has \
         the list endpoint's item shape; `track_inc`, `artist_inc` and `release_inc` take that \
         endpoint's `inc` values. `limit` is the page size of each branch, defaulting to 20 and \
         capped at 50. `query` is 1-256 characters after trimming. Each of its words must start \
         a word of a display name or sort name, ignoring case, accents, apostrophes and \
         punctuation: `ac dc` and `acdc` find `AC/DC`. Tracks also match through their artists \
         and the titles and artists of their visible releases, releases through their artists, \
         and artists through the names of duplicates merged into them. Words in scripts written \
         without spaces, such as Japanese, match anywhere in a word. No character is an \
         operator. Exact names rank first, then names starting with the query, then names \
         holding every word, then names holding some, then matches only through related items. \
         `min_rating` and `max_rating` apply an inclusive range over the \
         authenticated user's personal ratings; either bound excludes unrated entities, and an \
         equal pair matches an exact rating. `rated=true` keeps only rated entities and \
         `rated=false` only unrated ones; `rated=false` cannot be combined with a bound. \
         `favorite` and `listened` filter as on each list endpoint. \
         Without `paginate=true`, every `next_cursor` is `null`. With it, a branch with more \
         than `limit` matches returns a `next_cursor` for its list endpoint, resumed there with \
         the same `query`, `favorite`, `listened`, `rated`, `min_rating` and `max_rating` and no \
         other filter or sort; `limit` \
         and `inc` may differ. Each such cursor takes one of the user's limited cursor \
         snapshots and may evict their oldest.",
    )
}

pub fn search_routes() -> Router {
    Router::new().route("/", get(search))
}

#[cfg(feature = "docgen")]
pub(crate) fn search_openapi_routes() -> aide::axum::ApiRouter {
    use aide::axum::routing::get_with;

    aide::axum::ApiRouter::new().api_route("/", get_with(search, search_docs))
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use agdb::DbId;
    use axum::{
        http::{
            StatusCode,
            Uri,
        },
        response::IntoResponse,
    };

    use super::*;
    use crate::{
        routes::releases::list_release_responses,
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

    async fn setup() -> anyhow::Result<()> {
        crate::plugins::testing::init_library_test_state(std::path::PathBuf::from(".")).await
    }

    async fn principal(admin: bool, accessible_library_ids: HashSet<String>) -> Principal {
        let mut db = STATE.db.write().await;
        let user_db_id = lyra_db::test_db::insert_user(&mut db, "search").expect("insert user");
        let permissions = if admin {
            vec![lyra_db::Permission::Admin]
        } else {
            Vec::new()
        };
        Principal::for_user(&*db, user_db_id, permissions, accessible_library_ids)
    }

    async fn search_with(principal: &Principal, query: &str) -> Result<SearchResponse, AppError> {
        let uri: Uri = format!("/api/search?{query}").parse().expect("valid uri");
        let Query(params) = Query::<SearchQuery>::try_from_uri(&uri).expect("valid query");
        search_responses(principal.clone(), params).await
    }

    async fn search(principal: &Principal, query: &str) -> anyhow::Result<SearchResponse> {
        search_with(principal, query)
            .await
            .map_err(|err| anyhow::anyhow!("{err:?}"))
    }

    fn names(response: &SearchResponse) -> (Vec<&str>, Vec<&str>, Vec<&str>) {
        (
            response
                .tracks
                .items
                .iter()
                .map(|track| track.title.as_str())
                .collect(),
            response
                .artists
                .items
                .iter()
                .map(|artist| artist.name.as_str())
                .collect(),
            response
                .releases
                .items
                .iter()
                .map(|release| release.title.as_str())
                .collect(),
        )
    }

    #[tokio::test]
    async fn search_ranks_each_type_before_the_limit_and_keeps_no_cursor() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        setup().await?;
        {
            let mut db = STATE.db.write().await;
            for title in ["A b l u e", "Blue horizon", "B l u e", "Blue"] {
                insert_track(&mut db, title)?;
                insert_credited_artist(&mut db, title)?;
                insert_release(&mut db, title)?;
            }
        }
        let principal = principal(true, HashSet::new()).await;

        let response = search(&principal, "query=blue&limit=1").await?;
        assert_eq!(names(&response), (vec!["Blue"], vec!["Blue"], vec!["Blue"]));
        assert!(response.tracks.next_cursor.is_none());
        assert!(response.artists.next_cursor.is_none());
        assert!(response.releases.next_cursor.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn search_matches_operator_characters_literally() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        setup().await?;
        {
            let mut db = STATE.db.write().await;
            for title in ["!!!", "Blue Moon", "$uicideboy$"] {
                insert_release(&mut db, title)?;
            }
        }
        let principal = principal(true, HashSet::new()).await;

        let response = search(&principal, "query=%21%21%21").await?;
        assert_eq!(names(&response).2, vec!["!!!"]);
        let response = search(&principal, "query=%24uicideboy%24").await?;
        assert_eq!(names(&response).2, vec!["$uicideboy$"]);
        let response = search(&principal, "query=%5Eblue%20%21moon").await?;
        assert_eq!(names(&response).2, vec!["Blue Moon"]);
        let response = search(&principal, "query=%5E%5E%5E").await?;
        assert!(response.releases.items.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn paginated_search_continues_on_the_list_endpoint() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        setup().await?;
        {
            let mut db = STATE.db.write().await;
            for title in ["Blue", "Blue Train", "Kind of Blue"] {
                insert_release(&mut db, title)?;
            }
        }
        let principal = principal(true, HashSet::new()).await;

        let first = search(&principal, "query=blue&limit=1&paginate=true").await?;
        assert_eq!(names(&first).2, vec!["Blue"]);
        let cursor = first
            .releases
            .next_cursor
            .ok_or_else(|| anyhow::anyhow!("expected a release cursor"))?;
        assert!(first.tracks.next_cursor.is_none());

        let rest = list_release_responses(
            &principal,
            ReleaseListOptions::for_query(
                None,
                "blue".to_string(),
                crate::services::catalog::PersonalFilter::default(),
                super::super::PageQuery {
                    limit: Some(10),
                    cursor: Some(cursor),
                }
                .resolve_snapshot(),
            ),
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;
        let mut rest = rest
            .items
            .into_iter()
            .map(|release| release.title)
            .collect::<Vec<_>>();
        rest.sort();
        assert_eq!(rest, vec!["Blue Train", "Kind of Blue"]);
        Ok(())
    }

    #[tokio::test]
    async fn unpaginated_searches_keep_no_snapshot_to_evict_a_cursor() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        setup().await?;
        {
            let mut db = STATE.db.write().await;
            for title in ["Blue", "Blue Train", "Kind of Blue"] {
                insert_release(&mut db, title)?;
            }
        }
        let principal = principal(true, HashSet::new()).await;
        let cursor = search(&principal, "query=blue&limit=1&paginate=true")
            .await?
            .releases
            .next_cursor
            .ok_or_else(|| anyhow::anyhow!("expected a release cursor"))?;

        // More searches than a user has snapshots, each with more matches than its page holds.
        for _ in 0..12 {
            search(&principal, "query=blue&limit=1").await?;
        }
        let rest = list_release_responses(
            &principal,
            ReleaseListOptions::for_query(
                None,
                "blue".to_string(),
                crate::services::catalog::PersonalFilter::default(),
                super::super::PageQuery {
                    limit: Some(10),
                    cursor: Some(cursor),
                }
                .resolve_snapshot(),
            ),
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;
        assert_eq!(rest.items.len(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn search_finds_what_a_restricted_user_can_browse() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        setup().await?;
        let visible_library_id = {
            let mut db = STATE.db.write().await;
            let visible = insert_library(&mut db, "Visible", "/search-test/visible")?;
            let hidden = insert_library(&mut db, "Hidden", "/search-test/hidden")?;
            let release = insert_release(&mut db, "Blue Release")?;
            let track = insert_track(&mut db, "Blue Track")?;
            connect(&mut db, visible, release)?;
            connect(&mut db, release, track)?;
            let on_release = insert_artist(&mut db, "Blue Release Artist")?;
            let on_track = insert_artist(&mut db, "Blue Track Artist")?;
            connect_artist(&mut db, release, on_release)?;
            connect_artist(&mut db, track, on_track)?;

            let hidden_release = insert_release(&mut db, "Blue Hidden")?;
            let hidden_track = insert_track(&mut db, "Blue Hidden")?;
            let hidden_artist = insert_artist(&mut db, "Blue Hidden")?;
            connect(&mut db, hidden, hidden_release)?;
            connect(&mut db, hidden_release, hidden_track)?;
            connect_artist(&mut db, hidden_release, hidden_artist)?;
            lyra_db::libraries::get_by_id(&db, visible)?
                .ok_or_else(|| anyhow::anyhow!("library missing"))?
                .id
        };
        let principal = principal(false, HashSet::from([visible_library_id])).await;

        let response = search(&principal, "query=blue").await?;
        let (tracks, mut artists, releases) = names(&response);
        artists.sort();
        assert_eq!(tracks, vec!["Blue Track"]);
        assert_eq!(artists, vec!["Blue Release Artist", "Blue Track Artist"]);
        assert_eq!(releases, vec!["Blue Release"]);
        Ok(())
    }

    #[tokio::test]
    async fn search_filters_personal_ratings_before_the_limit() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        setup().await?;
        let principal = principal(true, HashSet::new()).await;
        {
            let mut db = STATE.db.write().await;
            insert_track(&mut db, "Blue")?;
            let rated_track = insert_track(&mut db, "A Distant Blue Track")?;
            let rated_artist = insert_credited_artist(&mut db, "Blue Artist")?;
            let low_release = insert_release(&mut db, "Blue Low Release")?;
            let high_release = insert_release(&mut db, "Blue High Release")?;
            let user_db_id: DbId = principal.require(&*db)?;
            for (target, value) in [
                (rated_track, 4),
                (rated_artist, 3),
                (low_release, 2),
                (high_release, 5),
            ] {
                lyra_db::ratings::upsert(
                    &mut db,
                    user_db_id,
                    target,
                    lyra_db::ratings::RatingValue::new(value).unwrap(),
                    100,
                )?;
            }
        }

        let response = search(&principal, "query=blue&limit=1&min_rating=3&max_rating=4").await?;
        assert_eq!(
            names(&response),
            (vec!["A Distant Blue Track"], vec!["Blue Artist"], vec![])
        );
        Ok(())
    }

    #[tokio::test]
    async fn search_includes_related_data_per_type() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        setup().await?;
        {
            let mut db = STATE.db.write().await;
            let release = insert_release(&mut db, "Blue")?;
            let artist = insert_artist(&mut db, "Artist")?;
            connect_artist(&mut db, release, artist)?;
        }
        let principal = principal(true, HashSet::new()).await;

        let bare = search(&principal, "query=blue").await?;
        assert!(bare.releases.items[0].artists.is_none());
        let included = search(&principal, "query=blue&release_inc=artists").await?;
        let artists = included.releases.items[0]
            .artists
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("expected release artists"))?;
        assert_eq!(artists[0].name, "Artist");
        Ok(())
    }

    #[tokio::test]
    async fn search_rejects_an_empty_query_and_unknown_includes() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        setup().await?;
        let principal = principal(true, HashSet::new()).await;

        for query in ["query=%20%20", "query=blue&track_inc=covers"] {
            let err = search_with(&principal, query)
                .await
                .err()
                .ok_or_else(|| anyhow::anyhow!("expected {query} to fail"))?;
            assert_eq!(err.into_response().status(), StatusCode::BAD_REQUEST);
        }
        Ok(())
    }

    #[test]
    fn search_limit_defaults_and_clamps() {
        assert_eq!(page_limit(None), DEFAULT_LIMIT as usize);
        assert_eq!(page_limit(Some(0)), 1);
        assert_eq!(page_limit(Some(9_999)), MAX_LIMIT as usize);
    }
}
