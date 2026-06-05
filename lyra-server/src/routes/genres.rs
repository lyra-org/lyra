// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    cmp::Ordering,
    collections::{
        HashMap,
        HashSet,
    },
};

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
    routing::get,
};
use serde::{
    Deserialize,
    Serialize,
};

use crate::{
    STATE,
    db::{
        self,
        SortDirection,
        genres,
    },
    routes::{
        AppError,
        covers as route_covers,
        deserialize_inc,
        parse_inc_values,
        responses::CoverResponse,
    },
    services::{
        auth::require_authenticated,
        covers as cover_services,
    },
};

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct GenreResponse {
    id: String,
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    parents: Option<Vec<GenreSummary>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    children: Option<Vec<GenreSummary>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cover: Option<Option<CoverResponse>>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct GenreSummary {
    id: String,
    name: String,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct GenreListQuery {
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Comma-separated or repeated values: covers.")
    )]
    #[serde(default, deserialize_with = "deserialize_inc")]
    inc: Option<Vec<String>>,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Optional public library ID to scope returned genres.")
    )]
    library_id: Option<String>,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Comma-separated or repeated values: name, last_played_at, listen_count, release_count, track_count, total_duration, id."
        )
    )]
    #[serde(default, deserialize_with = "deserialize_inc")]
    sort_by: Option<Vec<String>>,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Sort direction: ascending or descending.")
    )]
    sort_order: Option<String>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct GenreQuery {
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Comma-separated or repeated values: parents, children.")
    )]
    #[serde(default, deserialize_with = "deserialize_inc")]
    inc: Option<Vec<String>>,
}

struct GenreInc {
    parents: bool,
    children: bool,
    covers: bool,
}

fn parse_genre_inc(inc: Option<Vec<String>>) -> Result<GenreInc, AppError> {
    let values = parse_inc_values(inc, &["parents", "children", "covers"])?;
    let mut result = GenreInc {
        parents: false,
        children: false,
        covers: false,
    };
    for value in values {
        match value.as_str() {
            "parents" => result.parents = true,
            "children" => result.children = true,
            "covers" => result.covers = true,
            _ => {}
        }
    }
    Ok(result)
}

fn genre_to_summary(genre: genres::Genre) -> GenreSummary {
    GenreSummary {
        id: genre.id,
        name: genre.name,
    }
}

fn genre_to_response(genre: genres::Genre) -> GenreResponse {
    GenreResponse {
        id: genre.id,
        name: genre.name,
        parents: None,
        children: None,
        cover: None,
    }
}

fn release_ids_for_library(db: &DbAny, library_db_id: DbId) -> anyhow::Result<Vec<DbId>> {
    Ok(db::releases::get_direct(db, library_db_id)?
        .into_iter()
        .filter_map(|release| release.db_id.map(DbId::from))
        .collect())
}

fn release_ids_for_accessible_libraries(
    db: &DbAny,
    accessible_library_ids: &HashSet<String>,
) -> anyhow::Result<Vec<DbId>> {
    let mut release_ids = Vec::new();
    let mut seen_release_ids = HashSet::new();
    for library_id in accessible_library_ids {
        let Some(library_db_id) = db::lookup::find_node_id_by_id(db, library_id)? else {
            continue;
        };
        if db::libraries::get_by_id(db, library_db_id)?.is_none() {
            continue;
        }
        for release_id in release_ids_for_library(db, library_db_id)? {
            if seen_release_ids.insert(release_id) {
                release_ids.push(release_id);
            }
        }
    }
    Ok(release_ids)
}

#[derive(Clone, Copy, Debug)]
enum GenreRouteSortKey {
    Name,
    ListenCount,
    LastPlayedAt,
    ReleaseCount,
    TrackCount,
    TotalDuration,
    Id,
}

type GenreRouteSortSpec = super::RouteSortSpec<GenreRouteSortKey>;

fn genre_sort_supported_values() -> &'static str {
    "name, last_played_at, listen_count, release_count, track_count, total_duration, id"
}

fn default_genre_sort() -> Vec<GenreRouteSortSpec> {
    vec![GenreRouteSortSpec {
        key: GenreRouteSortKey::Name,
        direction: SortDirection::Ascending,
    }]
}

fn parse_genre_sort_specs(
    sort_by: Option<Vec<String>>,
    sort_order: Option<String>,
) -> Result<Vec<GenreRouteSortSpec>, AppError> {
    super::parse_route_sort_specs(
        sort_by,
        sort_order,
        |token| match token {
            "name" => Some(GenreRouteSortKey::Name),
            "listen_count" => Some(GenreRouteSortKey::ListenCount),
            "last_played_at" => Some(GenreRouteSortKey::LastPlayedAt),
            "release_count" => Some(GenreRouteSortKey::ReleaseCount),
            "track_count" => Some(GenreRouteSortKey::TrackCount),
            "total_duration" => Some(GenreRouteSortKey::TotalDuration),
            "id" => Some(GenreRouteSortKey::Id),
            _ => None,
        },
        genre_sort_supported_values(),
    )
}

struct GenreRouteSortEntry {
    genre: genres::Genre,
    release_count: u64,
    track_count: u64,
    listen_count: u64,
    last_played_at: Option<u64>,
    total_duration: u64,
}

fn compare_genre_route_field(
    a: &GenreRouteSortEntry,
    b: &GenreRouteSortEntry,
    key: GenreRouteSortKey,
) -> Ordering {
    match key {
        GenreRouteSortKey::Name => a
            .genre
            .scan_name
            .cmp(&b.genre.scan_name)
            .then_with(|| a.genre.name.cmp(&b.genre.name)),
        GenreRouteSortKey::ListenCount => a.listen_count.cmp(&b.listen_count),
        GenreRouteSortKey::LastPlayedAt => db::compare_option(&a.last_played_at, &b.last_played_at),
        GenreRouteSortKey::ReleaseCount => a.release_count.cmp(&b.release_count),
        GenreRouteSortKey::TrackCount => a.track_count.cmp(&b.track_count),
        GenreRouteSortKey::TotalDuration => a.total_duration.cmp(&b.total_duration),
        GenreRouteSortKey::Id => a.genre.id.cmp(&b.genre.id),
    }
}

fn compare_genre_route_entries(
    a: &GenreRouteSortEntry,
    b: &GenreRouteSortEntry,
    sort: &[GenreRouteSortSpec],
) -> Ordering {
    for spec in sort {
        let ord = db::apply_direction(compare_genre_route_field(a, b, spec.key), spec.direction);
        if ord != Ordering::Equal {
            return ord;
        }
    }

    a.genre
        .scan_name
        .cmp(&b.genre.scan_name)
        .then_with(|| a.genre.name.cmp(&b.genre.name))
        .then_with(|| a.genre.id.cmp(&b.genre.id))
}

fn genre_sort_needs_track_metrics(sort: &[GenreRouteSortSpec]) -> bool {
    sort.iter().any(|spec| {
        matches!(
            spec.key,
            GenreRouteSortKey::ListenCount
                | GenreRouteSortKey::LastPlayedAt
                | GenreRouteSortKey::TrackCount
                | GenreRouteSortKey::TotalDuration
        )
    })
}

fn genre_sort_needs_listens(sort: &[GenreRouteSortSpec]) -> bool {
    sort.iter().any(|spec| {
        matches!(
            spec.key,
            GenreRouteSortKey::ListenCount | GenreRouteSortKey::LastPlayedAt
        )
    })
}

fn query_genre_route_items(
    db: &DbAny,
    release_ids: &[DbId],
    sort: &[GenreRouteSortSpec],
    user_db_id: DbId,
) -> anyhow::Result<Vec<genres::Genre>> {
    let genres_by_release = genres::get_for_releases_many(db, release_ids)?;
    let needs_track_metrics = genre_sort_needs_track_metrics(sort);
    let needs_listens = genre_sort_needs_listens(sort);
    let tracks_by_release = if needs_track_metrics {
        db::tracks::get_direct_many(db, release_ids)?
    } else {
        HashMap::new()
    };
    let mut genres_by_id = HashMap::new();
    let mut release_ids_by_genre: HashMap<DbId, HashSet<DbId>> = HashMap::new();
    let mut track_ids_by_genre: HashMap<DbId, HashSet<DbId>> = HashMap::new();
    let mut track_count_by_genre: HashMap<DbId, u64> = HashMap::new();
    let mut total_duration_by_genre: HashMap<DbId, u64> = HashMap::new();
    let mut all_track_ids = Vec::new();
    let mut seen_all_track_ids = HashSet::new();

    for release_id in release_ids {
        let Some(release_genres) = genres_by_release.get(release_id) else {
            continue;
        };
        let mut release_genre_ids = Vec::new();
        let mut seen_release_genre_ids = HashSet::new();
        for genre in release_genres {
            let Some(genre_db_id) = genre.db_id.clone().map(DbId::from) else {
                continue;
            };
            genres_by_id
                .entry(genre_db_id)
                .or_insert_with(|| genre.clone());
            release_ids_by_genre
                .entry(genre_db_id)
                .or_default()
                .insert(*release_id);
            if seen_release_genre_ids.insert(genre_db_id) {
                release_genre_ids.push(genre_db_id);
            }
        }

        if needs_track_metrics {
            let Some(release_tracks) = tracks_by_release.get(release_id) else {
                continue;
            };
            for genre_db_id in release_genre_ids {
                for track in release_tracks {
                    let Some(track_db_id) = track.db_id.clone().map(DbId::from) else {
                        *track_count_by_genre.entry(genre_db_id).or_default() += 1;
                        if let Some(duration) = track.duration_ms {
                            let total_duration =
                                total_duration_by_genre.entry(genre_db_id).or_default();
                            *total_duration = total_duration.saturating_add(duration);
                        }
                        continue;
                    };
                    let genre_track_ids = track_ids_by_genre.entry(genre_db_id).or_default();
                    if !genre_track_ids.insert(track_db_id) {
                        continue;
                    }
                    *track_count_by_genre.entry(genre_db_id).or_default() += 1;
                    if needs_listens && seen_all_track_ids.insert(track_db_id) {
                        all_track_ids.push(track_db_id);
                    }
                    if let Some(duration) = track.duration_ms {
                        let total_duration =
                            total_duration_by_genre.entry(genre_db_id).or_default();
                        *total_duration = total_duration.saturating_add(duration);
                    }
                }
            }
        }
    }

    let listen_stats: HashMap<DbId, db::listens::ListenStats> = if needs_listens {
        db::listens::get_stats_for_user_tracks(db, &all_track_ids, user_db_id)?
            .into_iter()
            .map(|stats| (stats.db_id, stats))
            .collect()
    } else {
        HashMap::new()
    };

    let mut entries = Vec::with_capacity(genres_by_id.len());
    for (genre_db_id, genre) in genres_by_id {
        let release_count = release_ids_by_genre
            .get(&genre_db_id)
            .map(|ids| ids.len() as u64)
            .unwrap_or(0);
        let track_count = track_count_by_genre.get(&genre_db_id).copied().unwrap_or(0);
        let total_duration = total_duration_by_genre
            .get(&genre_db_id)
            .copied()
            .unwrap_or(0);
        let mut listen_count = 0u64;
        let mut last_played_at = None;
        if let Some(track_ids) = track_ids_by_genre.get(&genre_db_id) {
            for track_db_id in track_ids {
                let Some(stats) = listen_stats.get(track_db_id) else {
                    continue;
                };
                listen_count = listen_count.saturating_add(stats.count);
                last_played_at = last_played_at.max(stats.last_played);
            }
        }

        entries.push(GenreRouteSortEntry {
            genre,
            release_count,
            track_count,
            listen_count,
            last_played_at,
            total_duration,
        });
    }

    entries.sort_by(|a, b| compare_genre_route_entries(a, b, sort));
    Ok(entries.into_iter().map(|entry| entry.genre).collect())
}

async fn list_genres(
    headers: HeaderMap,
    Query(query): Query<GenreListQuery>,
) -> Result<Json<Vec<GenreResponse>>, AppError> {
    let principal = require_authenticated(&headers).await?;
    let inc = parse_genre_inc(query.inc)?;

    let db = &*STATE.db.read().await;
    let library_scope =
        super::resolve_optional_library_filter(db, &principal, query.library_id.as_deref())?;
    let mut sort = parse_genre_sort_specs(query.sort_by, query.sort_order)?;
    if sort.is_empty() {
        sort = default_genre_sort();
    }
    let release_ids = match library_scope {
        Some(library_db_id) => release_ids_for_library(db, library_db_id)?,
        None => release_ids_for_accessible_libraries(db, &principal.accessible_library_ids)?,
    };
    let all_genres = query_genre_route_items(db, &release_ids, &sort, principal.user_db_id)?;
    let visible_release_ids = release_ids.iter().copied().collect::<HashSet<_>>();
    let covers = if inc.covers {
        Some(cover_services::display::genres::covers_for_genres(
            db,
            &all_genres,
            &principal.user_public_id,
            Some(&visible_release_ids),
        )?)
    } else {
        None
    };

    let responses: Vec<GenreResponse> = all_genres
        .into_iter()
        .map(|genre| {
            let genre_db_id = genre.db_id.clone().map(DbId::from);
            let mut response = genre_to_response(genre);
            if inc.covers {
                response.cover = Some(
                    genre_db_id
                        .and_then(|genre_db_id| covers.as_ref()?.get(&genre_db_id).cloned())
                        .map(route_covers::cover_to_response),
                );
            }
            response
        })
        .collect();

    Ok(Json(responses))
}

async fn get_genre(
    headers: HeaderMap,
    Path(id): Path<String>,
    axum::extract::Query(query): axum::extract::Query<GenreQuery>,
) -> Result<Json<GenreResponse>, AppError> {
    let principal = require_authenticated(&headers).await?;
    let inc = parse_genre_inc(query.inc)?;

    let db = &*STATE.db.read().await;
    let genre_db_id = db::lookup::find_node_id_by_id(db, &id)?
        .ok_or_else(|| AppError::not_found(format!("not found: {id}")))?;
    let genre = genres::get_by_id(db, genre_db_id)?
        .ok_or_else(|| AppError::not_found(format!("Genre not found: {id}")))?;

    let parents = if inc.parents {
        Some(
            genres::get_parents(db, genre_db_id)?
                .into_iter()
                .map(genre_to_summary)
                .collect(),
        )
    } else {
        None
    };

    let children = if inc.children {
        Some(
            genres::get_children(db, genre_db_id)?
                .into_iter()
                .map(genre_to_summary)
                .collect(),
        )
    } else {
        None
    };
    let cover = if inc.covers {
        let visible_release_ids =
            release_ids_for_accessible_libraries(db, &principal.accessible_library_ids)?
                .into_iter()
                .collect::<HashSet<_>>();
        Some(
            cover_services::display::genres::cover_for_genre(
                db,
                &genre,
                &principal.user_public_id,
                Some(&visible_release_ids),
            )?
            .map(route_covers::cover_to_response),
        )
    } else {
        None
    };

    Ok(Json(GenreResponse {
        id: genre.id,
        name: genre.name,
        parents,
        children,
        cover,
    }))
}

#[cfg(feature = "docgen")]
fn list_genres_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List genres").description(
        "Returns genres attached to releases visible to the authenticated user. `library_id` scopes results to releases belonging to that public library ID. `inc=covers` includes personalized display cover metadata. `sort_by` supports `name`, `last_played_at`, `listen_count`, `release_count`, `track_count`, `total_duration`, and `id`; `sort_order` supports `ascending` and `descending`.",
    )
}

#[cfg(feature = "docgen")]
fn get_genre_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Get genre by ID")
        .description("Returns a single genre. Use `inc=parents,children,covers` to include hierarchy and personalized display cover metadata.")
}

pub fn genre_routes() -> Router {
    Router::new()
        .route("/", get(list_genres))
        .route("/{id}", get(get_genre))
        .route("/{id}/mix", get(super::mix::get_genre_mix))
}

#[cfg(test)]
mod tests {
    use axum::http::{
        HeaderMap,
        header::AUTHORIZATION,
    };
    use nanoid::nanoid;

    use crate::{
        db::test_db::{
            connect,
            insert_library,
            insert_release as insert_test_release,
            insert_track,
            new_test_db,
        },
        services::auth::sessions,
        testing::{
            LibraryFixtureConfig,
            initialize_runtime,
            runtime_test_lock,
        },
    };

    use super::*;

    async fn setup_route_test() -> anyhow::Result<()> {
        initialize_runtime(&LibraryFixtureConfig {
            directory: std::path::PathBuf::from("."),
            language: None,
            country: None,
        })
        .await
    }

    async fn create_admin_headers(username: &str) -> anyhow::Result<HeaderMap> {
        let user_db_id = {
            let mut db = STATE.db.write().await;
            db::roles::ensure_builtin_roles(&mut db)?;
            let user_db_id = db::users::create(&mut db, &db::test_db::test_user(username)?)?;
            db::roles::ensure_user_has_role(&mut db, user_db_id, db::roles::BUILTIN_ADMIN_ROLE)?;
            user_db_id
        };

        create_headers_for_user(user_db_id).await
    }

    async fn create_headers_for_user(user_db_id: DbId) -> anyhow::Result<HeaderMap> {
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

    fn insert_cover_for_release(
        db: &mut DbAny,
        release_db_id: DbId,
        cover_id: &str,
    ) -> anyhow::Result<db::Cover> {
        db::covers::upsert(
            db,
            release_db_id,
            db::Cover {
                db_id: None,
                id: cover_id.to_string(),
                path: format!("/music/{cover_id}.jpg"),
                mime_type: "image/jpeg".to_string(),
                hash: "a".repeat(64),
                blurhash: None,
            },
        )
    }

    fn record_listen(
        db: &mut DbAny,
        user_db_id: DbId,
        track_db_id: DbId,
        listened_at_ms: u64,
    ) -> anyhow::Result<()> {
        let track = db::tracks::get_by_id(db, track_db_id)?
            .ok_or_else(|| anyhow::anyhow!("track missing"))?;
        let listen = db::Listen {
            db_id: None,
            id: nanoid!(),
            track_public_id: track.id,
            position_ms: 0,
            duration_ms: Some(180_000),
            activity_ms: 180_000,
            state: db::PlaybackState::Completed,
            listened_at_ms,
            created_at_ms: listened_at_ms,
        };
        let session = db::PlaybackSession {
            db_id: None,
            id: nanoid!(),
            position_ms: 0,
            duration_ms: Some(180_000),
            activity_ms: Some(180_000),
            last_position_ms: None,
            state: db::PlaybackState::Completed,
            listen_recorded: Some(true),
            updated_at_ms: listened_at_ms,
            created_at_ms: listened_at_ms,
        };
        db::listens::create_and_mark_recorded(db, &listen, track_db_id, user_db_id, &session)
    }

    #[test]
    fn parse_genre_sort_specs_accepts_supported_values() -> anyhow::Result<()> {
        let specs = parse_genre_sort_specs(
            Some(vec![
                "name,last_played_at,listen_count".to_string(),
                "release_count,track_count,total_duration,id".to_string(),
            ]),
            Some("descending".to_string()),
        )
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        assert_eq!(specs.len(), 7);
        assert!(matches!(specs[0].key, GenreRouteSortKey::Name));
        assert!(matches!(specs[1].key, GenreRouteSortKey::LastPlayedAt));
        assert!(matches!(specs[2].key, GenreRouteSortKey::ListenCount));
        assert!(matches!(specs[3].key, GenreRouteSortKey::ReleaseCount));
        assert!(matches!(specs[4].key, GenreRouteSortKey::TrackCount));
        assert!(matches!(specs[5].key, GenreRouteSortKey::TotalDuration));
        assert!(matches!(specs[6].key, GenreRouteSortKey::Id));
        assert!(
            specs
                .iter()
                .all(|spec| matches!(spec.direction, SortDirection::Descending))
        );
        Ok(())
    }

    #[test]
    fn parse_genre_inc_accepts_covers() -> anyhow::Result<()> {
        let inc = parse_genre_inc(Some(vec!["parents,covers".to_string()]))
            .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        assert!(inc.parents);
        assert!(inc.covers);
        assert!(!inc.children);
        Ok(())
    }

    #[test]
    fn query_genre_route_items_sorts_by_track_count() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let rock_release = insert_test_release(&mut db, "Rock Release")?;
        let jazz_release = insert_test_release(&mut db, "Jazz Release")?;
        let rock_track_a = insert_track(&mut db, "Rock Track A")?;
        let rock_track_b = insert_track(&mut db, "Rock Track B")?;
        let jazz_track = insert_track(&mut db, "Jazz Track")?;
        connect(&mut db, rock_release, rock_track_a)?;
        connect(&mut db, rock_release, rock_track_b)?;
        connect(&mut db, jazz_release, jazz_track)?;
        db::genres::sync_release_genres(&mut db, rock_release, &["Rock".to_string()])?;
        db::genres::sync_release_genres(&mut db, jazz_release, &["Jazz".to_string()])?;

        let genres = query_genre_route_items(
            &db,
            &[rock_release, jazz_release],
            &[GenreRouteSortSpec {
                key: GenreRouteSortKey::TrackCount,
                direction: SortDirection::Descending,
            }],
            DbId(1),
        )?;

        let names: Vec<String> = genres.into_iter().map(|genre| genre.name).collect();
        assert_eq!(names, vec!["Rock", "Jazz"]);
        Ok(())
    }

    #[tokio::test]
    async fn list_genres_scopes_by_library_id() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        setup_route_test().await?;

        let visible_library_id = {
            let mut db = STATE.db.write().await;
            let visible_library =
                insert_library(&mut db, "Visible Genres", "/tmp/lyra-visible-genres")?;
            let hidden_library =
                insert_library(&mut db, "Hidden Genres", "/tmp/lyra-hidden-genres")?;
            let visible_release = insert_test_release(&mut db, "Visible Rock")?;
            let hidden_release = insert_test_release(&mut db, "Hidden Jazz")?;
            connect(&mut db, visible_library, visible_release)?;
            connect(&mut db, hidden_library, hidden_release)?;
            db::genres::sync_release_genres(&mut *db, visible_release, &["Rock".to_string()])?;
            db::genres::sync_release_genres(&mut *db, hidden_release, &["Jazz".to_string()])?;

            db::libraries::get_by_id(&db, visible_library)?
                .ok_or_else(|| anyhow::anyhow!("visible library missing"))?
                .id
        };
        let headers = create_admin_headers("genre-scope-admin").await?;

        let Json(genres) = list_genres(
            headers,
            Query(GenreListQuery {
                inc: None,
                library_id: Some(visible_library_id),
                sort_by: None,
                sort_order: None,
            }),
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        assert_eq!(genres.len(), 1);
        assert_eq!(genres[0].name, "Rock");
        Ok(())
    }

    #[tokio::test]
    async fn list_genres_without_library_id_uses_accessible_libraries() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        setup_route_test().await?;

        let user_db_id = {
            let mut db = STATE.db.write().await;
            let user_db_id =
                db::users::create(&mut db, &db::test_db::test_user("genre-listener")?)?;
            let visible_library =
                insert_library(&mut db, "Accessible Genres", "/tmp/lyra-accessible-genres")?;
            let hidden_library = insert_library(
                &mut db,
                "Inaccessible Genres",
                "/tmp/lyra-inaccessible-genres",
            )?;
            let visible_release = insert_test_release(&mut db, "Accessible Rock")?;
            let hidden_release = insert_test_release(&mut db, "Inaccessible Jazz")?;
            connect(&mut db, visible_library, visible_release)?;
            connect(&mut db, hidden_library, hidden_release)?;
            db::libraries::grant_access(
                &mut *db,
                user_db_id,
                visible_library,
                db::libraries::AccessKind::ReadWrite,
            )?;
            db::genres::sync_release_genres(&mut *db, visible_release, &["Rock".to_string()])?;
            db::genres::sync_release_genres(&mut *db, hidden_release, &["Jazz".to_string()])?;
            user_db_id
        };
        let headers = create_headers_for_user(user_db_id).await?;

        let Json(genres) = list_genres(
            headers,
            Query(GenreListQuery {
                inc: None,
                library_id: None,
                sort_by: None,
                sort_order: None,
            }),
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        assert_eq!(genres.len(), 1);
        assert_eq!(genres[0].name, "Rock");
        Ok(())
    }

    #[tokio::test]
    async fn list_genres_includes_random_cover_when_listens_are_weak() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        setup_route_test().await?;

        let (user_db_id, cover_id) = {
            let mut db = STATE.db.write().await;
            let user_db_id =
                db::users::create(&mut db, &db::test_db::test_user("genre-random-cover")?)?;
            let library = insert_library(&mut db, "Genre Random", "/tmp/lyra-genre-random")?;
            let release = insert_test_release(&mut db, "Random Rock")?;
            connect(&mut db, library, release)?;
            db::libraries::grant_access(
                &mut *db,
                user_db_id,
                library,
                db::libraries::AccessKind::ReadWrite,
            )?;
            db::genres::sync_release_genres(&mut *db, release, &["Rock".to_string()])?;
            let cover_id = insert_cover_for_release(&mut db, release, "genre-random-cover")?.id;
            (user_db_id, cover_id)
        };
        let headers = create_headers_for_user(user_db_id).await?;

        let Json(genres) = list_genres(
            headers,
            Query(GenreListQuery {
                inc: Some(vec!["covers".to_string()]),
                library_id: None,
                sort_by: None,
                sort_order: None,
            }),
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        assert_eq!(genres.len(), 1);
        let cover = genres[0]
            .cover
            .as_ref()
            .and_then(|cover| cover.as_ref())
            .ok_or_else(|| anyhow::anyhow!("expected random cover"))?;
        assert_eq!(cover.id, cover_id);
        Ok(())
    }

    #[tokio::test]
    async fn list_genres_prefers_personal_cover_when_user_signal_is_enough() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        setup_route_test().await?;

        let (user_db_id, expected_cover_id) = {
            let mut db = STATE.db.write().await;
            let user_db_id =
                db::users::create(&mut db, &db::test_db::test_user("genre-personal-cover")?)?;
            let library = insert_library(&mut db, "Genre Personal", "/tmp/lyra-genre-personal")?;
            let release_a = insert_test_release(&mut db, "Personal A")?;
            let release_b = insert_test_release(&mut db, "Personal B")?;
            let track_a = insert_track(&mut db, "Personal A Track")?;
            let track_b = insert_track(&mut db, "Personal B Track")?;
            connect(&mut db, library, release_a)?;
            connect(&mut db, library, release_b)?;
            connect(&mut db, release_a, track_a)?;
            connect(&mut db, release_b, track_b)?;
            db::libraries::grant_access(
                &mut *db,
                user_db_id,
                library,
                db::libraries::AccessKind::ReadWrite,
            )?;
            db::genres::sync_release_genres(&mut *db, release_a, &["Rock".to_string()])?;
            db::genres::sync_release_genres(&mut *db, release_b, &["Rock".to_string()])?;
            insert_cover_for_release(&mut db, release_a, "genre-personal-a")?;
            let expected_cover_id =
                insert_cover_for_release(&mut db, release_b, "genre-personal-b")?.id;
            record_listen(&mut db, user_db_id, track_b, 1_000)?;
            record_listen(&mut db, user_db_id, track_b, 2_000)?;
            record_listen(&mut db, user_db_id, track_b, 3_000)?;
            (user_db_id, expected_cover_id)
        };
        let headers = create_headers_for_user(user_db_id).await?;

        let Json(genres) = list_genres(
            headers,
            Query(GenreListQuery {
                inc: Some(vec!["covers".to_string()]),
                library_id: None,
                sort_by: None,
                sort_order: None,
            }),
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        let cover = genres[0]
            .cover
            .as_ref()
            .and_then(|cover| cover.as_ref())
            .ok_or_else(|| anyhow::anyhow!("expected personal cover"))?;
        assert_eq!(cover.id, expected_cover_id);
        Ok(())
    }

    #[tokio::test]
    async fn list_genres_uses_instance_cover_when_user_signal_is_weak() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        setup_route_test().await?;

        let (request_user_db_id, expected_cover_id) = {
            let mut db = STATE.db.write().await;
            let request_user_db_id =
                db::users::create(&mut db, &db::test_db::test_user("genre-instance-request")?)?;
            let library = insert_library(&mut db, "Genre Instance", "/tmp/lyra-genre-instance")?;
            let release_a = insert_test_release(&mut db, "Instance A")?;
            let release_b = insert_test_release(&mut db, "Instance B")?;
            let track_a = insert_track(&mut db, "Instance A Track")?;
            let track_b = insert_track(&mut db, "Instance B Track")?;
            connect(&mut db, library, release_a)?;
            connect(&mut db, library, release_b)?;
            connect(&mut db, release_a, track_a)?;
            connect(&mut db, release_b, track_b)?;
            db::libraries::grant_access(
                &mut *db,
                request_user_db_id,
                library,
                db::libraries::AccessKind::ReadWrite,
            )?;
            db::genres::sync_release_genres(&mut *db, release_a, &["Rock".to_string()])?;
            db::genres::sync_release_genres(&mut *db, release_b, &["Rock".to_string()])?;
            insert_cover_for_release(&mut db, release_a, "genre-instance-a")?;
            let expected_cover_id =
                insert_cover_for_release(&mut db, release_b, "genre-instance-b")?.id;

            for user_idx in 0..3 {
                let user_db_id = db::users::create(
                    &mut db,
                    &db::test_db::test_user(&format!("genre-instance-listener-{user_idx}"))?,
                )?;
                for listen_idx in 0..4 {
                    record_listen(
                        &mut db,
                        user_db_id,
                        track_b,
                        1_000 + (user_idx * 10 + listen_idx) as u64,
                    )?;
                }
            }

            (request_user_db_id, expected_cover_id)
        };
        let headers = create_headers_for_user(request_user_db_id).await?;

        let Json(genres) = list_genres(
            headers,
            Query(GenreListQuery {
                inc: Some(vec!["covers".to_string()]),
                library_id: None,
                sort_by: None,
                sort_order: None,
            }),
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        let cover = genres[0]
            .cover
            .as_ref()
            .and_then(|cover| cover.as_ref())
            .ok_or_else(|| anyhow::anyhow!("expected instance cover"))?;
        assert_eq!(cover.id, expected_cover_id);
        Ok(())
    }

    #[tokio::test]
    async fn list_genres_does_not_leak_hidden_display_cover() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        setup_route_test().await?;

        let user_db_id = {
            let mut db = STATE.db.write().await;
            let user_db_id =
                db::users::create(&mut db, &db::test_db::test_user("genre-hidden-cover")?)?;
            let visible_library = insert_library(
                &mut db,
                "Genre Visible Covers",
                "/tmp/lyra-genre-visible-covers",
            )?;
            let hidden_library = insert_library(
                &mut db,
                "Genre Hidden Covers",
                "/tmp/lyra-genre-hidden-covers",
            )?;
            let visible_release = insert_test_release(&mut db, "Visible Rock")?;
            let hidden_release = insert_test_release(&mut db, "Hidden Rock")?;
            connect(&mut db, visible_library, visible_release)?;
            connect(&mut db, hidden_library, hidden_release)?;
            db::libraries::grant_access(
                &mut *db,
                user_db_id,
                visible_library,
                db::libraries::AccessKind::ReadWrite,
            )?;
            db::genres::sync_release_genres(&mut *db, visible_release, &["Rock".to_string()])?;
            db::genres::sync_release_genres(&mut *db, hidden_release, &["Rock".to_string()])?;
            insert_cover_for_release(&mut db, hidden_release, "genre-hidden-cover")?;
            user_db_id
        };
        let headers = create_headers_for_user(user_db_id).await?;

        let Json(genres) = list_genres(
            headers,
            Query(GenreListQuery {
                inc: Some(vec!["covers".to_string()]),
                library_id: None,
                sort_by: None,
                sort_order: None,
            }),
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        assert_eq!(genres.len(), 1);
        assert!(matches!(genres[0].cover, Some(None)));
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
    use nanoid::nanoid;
    use test::{
        Bencher,
        black_box,
    };

    use super::*;
    use crate::db::test_db::{
        connect,
        insert_release as insert_test_release,
        insert_track,
        new_test_db,
        test_user,
    };

    struct GenreSortBench {
        db: DbAny,
        user_db_id: DbId,
        release_ids: Vec<DbId>,
    }

    fn record_listen(db: &mut DbAny, user_db_id: DbId, track_db_id: DbId, listened_at_ms: u64) {
        let track = db::tracks::get_by_id(db, track_db_id)
            .unwrap()
            .expect("track exists");
        let listen = db::Listen {
            db_id: None,
            id: nanoid!(),
            track_public_id: track.id,
            position_ms: 0,
            duration_ms: Some(180_000),
            activity_ms: 180_000,
            state: db::PlaybackState::Completed,
            listened_at_ms,
            created_at_ms: listened_at_ms,
        };
        let session = db::PlaybackSession {
            db_id: None,
            id: nanoid!(),
            position_ms: 0,
            duration_ms: Some(180_000),
            activity_ms: Some(180_000),
            last_position_ms: None,
            state: db::PlaybackState::Completed,
            listen_recorded: Some(true),
            updated_at_ms: listened_at_ms,
            created_at_ms: listened_at_ms,
        };
        db::listens::create_and_mark_recorded(db, &listen, track_db_id, user_db_id, &session)
            .unwrap();
    }

    fn seed_genre_sort_bench(
        genre_count: usize,
        releases_per_genre: usize,
        tracks_per_release: usize,
        listens_per_track: usize,
    ) -> GenreSortBench {
        let mut db = new_test_db().unwrap();
        let user_db_id =
            db::users::create(&mut db, &test_user("genre-sort-bench").unwrap()).unwrap();
        let mut release_ids = Vec::with_capacity(genre_count * releases_per_genre);
        for genre_idx in 0..genre_count {
            let genre_name = format!("Genre {genre_idx:04}");
            for release_idx in 0..releases_per_genre {
                let release_db_id = insert_test_release(
                    &mut db,
                    &format!("Genre {genre_idx:04} Release {release_idx:02}"),
                )
                .unwrap();
                db::genres::sync_release_genres(&mut db, release_db_id, &[genre_name.clone()])
                    .unwrap();
                for track_idx in 0..tracks_per_release {
                    let track_db_id = insert_track(
                        &mut db,
                        &format!(
                            "Genre {genre_idx:04} Release {release_idx:02} Track {track_idx:02}"
                        ),
                    )
                    .unwrap();
                    for listen_idx in 0..listens_per_track {
                        record_listen(
                            &mut db,
                            user_db_id,
                            track_db_id,
                            ((genre_idx * releases_per_genre * tracks_per_release)
                                + (release_idx * tracks_per_release)
                                + track_idx
                                + listen_idx) as u64
                                * 1_000,
                        );
                    }
                    connect(&mut db, release_db_id, track_db_id).unwrap();
                }
                release_ids.push(release_db_id);
            }
        }

        GenreSortBench {
            db,
            user_db_id,
            release_ids,
        }
    }

    #[bench]
    fn route_sort_genres_name_100(b: &mut Bencher) {
        let setup = seed_genre_sort_bench(100, 1, 0, 0);
        let sort = default_genre_sort();
        b.iter(|| {
            query_genre_route_items(
                &setup.db,
                black_box(&setup.release_ids),
                &sort,
                setup.user_db_id,
            )
            .unwrap()
        });
    }

    #[bench]
    fn route_sort_genres_track_count_100_genres_4000_tracks(b: &mut Bencher) {
        let setup = seed_genre_sort_bench(100, 5, 8, 0);
        let sort = vec![GenreRouteSortSpec {
            key: GenreRouteSortKey::TrackCount,
            direction: SortDirection::Descending,
        }];
        b.iter(|| {
            query_genre_route_items(
                &setup.db,
                black_box(&setup.release_ids),
                &sort,
                setup.user_db_id,
            )
            .unwrap()
        });
    }

    #[bench]
    fn route_sort_genres_listen_count_100_genres_4000_listens(b: &mut Bencher) {
        let setup = seed_genre_sort_bench(100, 5, 8, 1);
        let sort = vec![GenreRouteSortSpec {
            key: GenreRouteSortKey::ListenCount,
            direction: SortDirection::Descending,
        }];
        b.iter(|| {
            query_genre_route_items(
                &setup.db,
                black_box(&setup.release_ids),
                &sort,
                setup.user_db_id,
            )
            .unwrap()
        });
    }
}

#[cfg(feature = "docgen")]
pub(crate) fn genre_openapi_routes() -> aide::axum::ApiRouter {
    use aide::axum::routing::get_with;

    aide::axum::ApiRouter::new()
        .api_route("/", get_with(list_genres, list_genres_docs))
        .api_route("/{id}", get_with(get_genre, get_genre_docs))
        .api_route(
            "/{id}/mix",
            get_with(super::mix::get_genre_mix, super::mix::genre_mix_docs),
        )
}
