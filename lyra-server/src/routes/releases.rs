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
use std::{
    cmp::Ordering,
    collections::{
        HashMap,
        HashSet,
    },
};

use crate::{
    STATE,
    db::{
        self,
        ListOptions,
        Permission,
        SortDirection,
        SortKey,
    },
    routes::AppError,
    routes::{
        covers as route_covers,
        deserialize_inc,
        responses::{
            EntryResponse,
            PageResponse,
            ReleaseResponse,
            TrackResponse,
        },
    },
    services::{
        auth::require_authenticated,
        covers,
        pagination::SnapshotKey,
        releases,
    },
};

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
#[non_exhaustive]
pub struct ReleaseCoverSearchResponse {
    pub release_id: String,
    pub results: Vec<route_covers::ProviderCoverSearchResponse>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
pub(crate) struct ReleaseListQuery {
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Comma-separated or repeated values: artists, tracks, track_artists, entries, covers, artist_covers, genres."
        )
    )]
    #[serde(default, deserialize_with = "deserialize_inc")]
    pub(crate) inc: Option<Vec<String>>,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Optional text query matched against release titles.")
    )]
    pub(crate) query: Option<String>,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Optional exact release year filter derived from `release_date`.")
    )]
    pub(crate) year: Option<u32>,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Optional public library ID to scope returned releases.")
    )]
    pub(crate) library_id: Option<String>,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Comma-separated or repeated public genre IDs.")
    )]
    #[serde(default, deserialize_with = "deserialize_inc")]
    pub(crate) genre_id: Option<Vec<String>>,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Comma-separated or repeated values: sort_name, name, date_created, release_date, last_played_at, listen_count, total_duration, id."
        )
    )]
    #[serde(default, deserialize_with = "deserialize_inc")]
    pub(crate) sort_by: Option<Vec<String>>,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Sort order for all sort keys: ascending or descending.")
    )]
    pub(crate) sort_order: Option<String>,
    #[serde(flatten)]
    pub(crate) page: super::PageQuery,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
pub(crate) struct ReleaseQuery {
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Comma-separated or repeated values: artists, tracks, track_artists, entries, covers, artist_covers, genres."
        )
    )]
    #[serde(default, deserialize_with = "deserialize_inc")]
    pub(crate) inc: Option<Vec<String>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ReleaseInc {
    pub(crate) artists: bool,
    pub(crate) tracks: bool,
    pub(crate) track_artists: bool,
    pub(crate) entries: bool,
    pub(crate) covers: bool,
    pub(crate) artist_covers: bool,
    pub(crate) genres: bool,
}

pub(crate) fn parse_inc(inc: Option<Vec<String>>) -> Result<ReleaseInc, AppError> {
    let values = super::parse_inc_values(
        inc,
        &[
            "artists",
            "tracks",
            "track_artists",
            "entries",
            "covers",
            "artist_covers",
            "genres",
        ],
    )?;
    let mut result = ReleaseInc {
        artists: false,
        tracks: false,
        track_artists: false,
        entries: false,
        covers: false,
        artist_covers: false,
        genres: false,
    };
    for value in values {
        match value.as_str() {
            "artists" => result.artists = true,
            "tracks" => result.tracks = true,
            "track_artists" => result.track_artists = true,
            "entries" => result.entries = true,
            "covers" => result.covers = true,
            "artist_covers" => result.artist_covers = true,
            "genres" => result.genres = true,
            _ => {}
        }
    }
    Ok(result)
}

pub(crate) fn parse_release_includes(
    inc: Option<Vec<String>>,
) -> Result<(releases::ReleaseIncludes, bool, bool, bool), AppError> {
    let parsed = parse_inc(inc)?;
    let includes = releases::ReleaseIncludes {
        artists: parsed.artists,
        tracks: parsed.tracks,
        track_artists: parsed.track_artists,
        entries: parsed.entries,
    };

    Ok((includes, parsed.covers, parsed.genres, parsed.artist_covers))
}

#[derive(Clone, Copy, Debug)]
enum ReleaseRouteSortKey {
    Field(SortKey),
    ListenCount,
    LastPlayedAt,
    TotalDuration,
}

type ReleaseRouteSortSpec = super::RouteSortSpec<ReleaseRouteSortKey>;

fn parse_sort_specs(
    sort_by: Option<Vec<String>>,
    sort_order: Option<String>,
) -> Result<Vec<ReleaseRouteSortSpec>, AppError> {
    super::parse_route_sort_specs(
        sort_by,
        sort_order,
        |token| match token {
            "listen_count" => Some(ReleaseRouteSortKey::ListenCount),
            "last_played_at" => Some(ReleaseRouteSortKey::LastPlayedAt),
            "total_duration" => Some(ReleaseRouteSortKey::TotalDuration),
            _ => SortKey::from_token(token).and_then(|key| match key {
                SortKey::SortName
                | SortKey::Name
                | SortKey::DateCreated
                | SortKey::ReleaseDate
                | SortKey::DbId => Some(ReleaseRouteSortKey::Field(key)),
                SortKey::TrackNumber | SortKey::DiscNumber | SortKey::Duration => None,
            }),
        },
        release_sort_supported_values(),
    )
}

fn release_sort_supported_values() -> &'static str {
    "sort_name, name, date_created, release_date, last_played_at, listen_count, total_duration, id"
}

fn default_release_sort() -> Vec<ReleaseRouteSortSpec> {
    vec![ReleaseRouteSortSpec {
        key: ReleaseRouteSortKey::Field(SortKey::SortName),
        direction: SortDirection::Ascending,
    }]
}

fn parse_genre_id_filter(genre_id: Option<Vec<String>>) -> Vec<String> {
    let mut values = Vec::new();
    if let Some(entries) = genre_id {
        for entry in entries {
            for token in entry.split(',') {
                let token = token.trim();
                if token.is_empty() {
                    continue;
                }
                values.push(token.to_string());
            }
        }
    }
    values
}

fn resolve_genre_id_filter(
    db: &impl db::DbAccess,
    genre_ids: &[String],
) -> Result<Vec<DbId>, AppError> {
    let mut resolved = Vec::new();
    for genre_id in genre_ids {
        let genre_db_id = db::lookup::find_node_id_by_id(db, genre_id)?
            .ok_or_else(|| AppError::not_found(format!("Genre not found: {genre_id}")))?;
        db::genres::get_by_id(db, genre_db_id)?
            .ok_or_else(|| AppError::not_found(format!("Genre not found: {genre_id}")))?;
        resolved.push(genre_db_id);
    }
    Ok(resolved)
}

struct ReleaseRouteSortEntry {
    release: db::Release,
    lower_title: String,
    lower_sort_title: Option<String>,
    db_id: Option<i64>,
    release_date: Option<String>,
    date_created: Option<u64>,
    listen_count: u64,
    last_played_at: Option<u64>,
    total_duration: u64,
    match_score: u32,
}

impl ReleaseRouteSortEntry {
    fn new(
        release: db::Release,
        listen_count: u64,
        last_played_at: Option<u64>,
        total_duration: u64,
    ) -> Self {
        Self {
            lower_title: release.release_title.to_lowercase(),
            lower_sort_title: release
                .sort_title
                .as_ref()
                .map(|value| value.to_lowercase()),
            db_id: release.db_id.as_ref().map(|id| DbId::from(id.clone()).0),
            release_date: release.release_date.clone(),
            date_created: release.ctime.or(release.created_at),
            release,
            listen_count,
            last_played_at,
            total_duration,
            match_score: 0,
        }
    }
}

fn compare_release_route_field(
    a: &ReleaseRouteSortEntry,
    b: &ReleaseRouteSortEntry,
    key: ReleaseRouteSortKey,
) -> Ordering {
    match key {
        ReleaseRouteSortKey::Field(SortKey::SortName) => a
            .lower_sort_title
            .as_deref()
            .unwrap_or(a.lower_title.as_str())
            .cmp(
                b.lower_sort_title
                    .as_deref()
                    .unwrap_or(b.lower_title.as_str()),
            ),
        ReleaseRouteSortKey::Field(SortKey::Name) => a.lower_title.cmp(&b.lower_title),
        ReleaseRouteSortKey::Field(SortKey::DateCreated) => {
            db::compare_option(&a.date_created, &b.date_created)
        }
        ReleaseRouteSortKey::Field(SortKey::ReleaseDate) => {
            db::compare_option(&a.release_date, &b.release_date)
        }
        ReleaseRouteSortKey::Field(SortKey::DbId) => db::compare_option(&a.db_id, &b.db_id),
        ReleaseRouteSortKey::ListenCount => a.listen_count.cmp(&b.listen_count),
        ReleaseRouteSortKey::LastPlayedAt => {
            db::compare_option(&a.last_played_at, &b.last_played_at)
        }
        ReleaseRouteSortKey::TotalDuration => a.total_duration.cmp(&b.total_duration),
        ReleaseRouteSortKey::Field(
            SortKey::TrackNumber | SortKey::DiscNumber | SortKey::Duration,
        ) => Ordering::Equal,
    }
}

fn compare_release_route_entries(
    a: &ReleaseRouteSortEntry,
    b: &ReleaseRouteSortEntry,
    sort: &[ReleaseRouteSortSpec],
) -> Ordering {
    for spec in sort {
        let ord = db::apply_direction(compare_release_route_field(a, b, spec.key), spec.direction);
        if ord != Ordering::Equal {
            return ord;
        }
    }

    b.match_score
        .cmp(&a.match_score)
        .then_with(|| a.lower_title.cmp(&b.lower_title))
        .then_with(|| db::compare_option(&a.db_id, &b.db_id))
}

fn release_sort_needs_tracks(sort: &[ReleaseRouteSortSpec]) -> bool {
    sort.iter().any(|spec| {
        matches!(
            spec.key,
            ReleaseRouteSortKey::ListenCount
                | ReleaseRouteSortKey::LastPlayedAt
                | ReleaseRouteSortKey::TotalDuration
        )
    })
}

fn release_sort_needs_listens(sort: &[ReleaseRouteSortSpec]) -> bool {
    sort.iter().any(|spec| {
        matches!(
            spec.key,
            ReleaseRouteSortKey::ListenCount | ReleaseRouteSortKey::LastPlayedAt
        )
    })
}

fn query_release_route_items(
    db: &DbAny,
    releases: Vec<db::Release>,
    sort: &[ReleaseRouteSortSpec],
    search_term: Option<&str>,
    user_db_id: DbId,
) -> anyhow::Result<Vec<db::Release>> {
    let release_ids: Vec<DbId> = releases
        .iter()
        .filter_map(|release| release.db_id.clone().map(DbId::from))
        .collect();
    let needs_tracks = release_sort_needs_tracks(sort);
    let needs_listens = release_sort_needs_listens(sort);
    let tracks_by_release = if needs_tracks {
        db::tracks::get_direct_many(db, &release_ids)?
    } else {
        HashMap::new()
    };

    let mut all_track_ids = Vec::new();
    let mut seen_track_ids = HashSet::new();
    if needs_listens {
        for tracks in tracks_by_release.values() {
            for track in tracks {
                let Some(track_db_id) = track.db_id.clone().map(DbId::from) else {
                    continue;
                };
                if seen_track_ids.insert(track_db_id) {
                    all_track_ids.push(track_db_id);
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

    let mut entries: Vec<ReleaseRouteSortEntry> = releases
        .into_iter()
        .map(|release| {
            let release_db_id = release.db_id.clone().map(DbId::from);
            let mut seen_release_track_ids = HashSet::new();
            let mut listen_count = 0u64;
            let mut last_played_at = None;
            let mut total_duration = 0u64;

            if let Some(tracks) = release_db_id.and_then(|id| tracks_by_release.get(&id)) {
                for track in tracks {
                    let Some(track_db_id) = track.db_id.clone().map(DbId::from) else {
                        continue;
                    };
                    if !seen_release_track_ids.insert(track_db_id) {
                        continue;
                    }
                    if let Some(duration) = track.duration_ms {
                        total_duration = total_duration.saturating_add(duration);
                    }
                    if let Some(stats) = listen_stats.get(&track_db_id) {
                        listen_count = listen_count.saturating_add(stats.count);
                        last_played_at = last_played_at.max(stats.last_played);
                    }
                }
            }

            ReleaseRouteSortEntry::new(release, listen_count, last_played_at, total_duration)
        })
        .collect();

    if let Some(term) = search_term {
        db::search::fuzzy_filter(
            &mut entries,
            term,
            |entry| entry.release.release_title.as_str(),
            |entry, score| entry.match_score = score,
        );
    }

    entries.sort_by(|a, b| compare_release_route_entries(a, b, sort));
    Ok(entries.into_iter().map(|entry| entry.release).collect())
}

pub(crate) fn detail_to_release_response(
    db: &DbAny,
    detail: releases::ReleaseDetails,
    include_covers: bool,
    include_artist_covers: bool,
    include_genres: bool,
    include_entry_paths: bool,
) -> anyhow::Result<ReleaseResponse> {
    let artist_covers = if include_artist_covers {
        let mut artist_db_ids = Vec::new();
        if let Some(artists) = detail.artists.as_ref() {
            artist_db_ids.extend(super::db_ids_from_credited_artists(artists));
        }
        if let Some(track_artists) = detail.track_artists.as_ref() {
            for artists in track_artists.values() {
                artist_db_ids.extend(super::db_ids_from_credited_artists(artists));
            }
        }
        Some(db::covers::get_many(db, &artist_db_ids)?)
    } else {
        None
    };
    let entries = detail.entries.map(|entries| {
        entries
            .into_iter()
            .map(|entry| EntryResponse::from_entry(entry, include_entry_paths))
            .collect::<Vec<EntryResponse>>()
    });

    let cover = route_covers::build_cover_response(db, detail.release_db_id, include_covers)?;
    let genres = if include_genres {
        db::genres::get_names_for_release(db, detail.release_db_id)?
    } else {
        None
    };

    Ok(ReleaseResponse {
        id: detail.release.id,
        title: detail.release.release_title,
        sort_title: detail.release.sort_title,
        release_date: detail.release.release_date,
        genres,
        cover,
        artists: detail
            .artists
            .map(|v| super::credited_artist_responses(v, artist_covers.as_ref())),
        tracks: detail.tracks.map(|tracks| {
            tracks
                .into_iter()
                .map(|track| {
                    let artists = detail.track_artists.as_ref().and_then(|m| {
                        let db_id = track.db_id.clone().map(DbId::from)?;
                        Some(super::credited_artist_responses(
                            m.get(&db_id)?.clone(),
                            artist_covers.as_ref(),
                        ))
                    });
                    let mut resp = TrackResponse::from(track);
                    resp.artists = artists;
                    resp
                })
                .collect()
        }),
        entries,
    })
}

async fn get_releases(
    headers: HeaderMap,
    Query(list_query): Query<ReleaseListQuery>,
) -> Result<Json<PageResponse<ReleaseResponse>>, AppError> {
    let ReleaseListQuery {
        inc,
        query,
        year,
        library_id,
        genre_id,
        sort_by,
        sort_order,
        page,
    } = list_query;
    let page_request = page.resolve_snapshot();
    let principal = require_authenticated(&headers).await?;
    let include_entry_paths =
        db::roles::has_permission(&principal.permissions, Permission::ManageLibraries);

    let db = &*STATE.db.read().await;
    let (includes, include_covers, include_genres, include_artist_covers) =
        parse_release_includes(inc)?;
    let search_term = super::parse_text_query(query);
    let year_context = year.map(|year| year.to_string());
    let snapshot_key = SnapshotKey::builder(&principal.user_public_id, "releases")
        .field(search_term.as_deref())
        .field(year_context.as_deref())
        .field(library_id.as_deref())
        .values(genre_id.as_deref())
        .values(sort_by.as_deref())
        .field(sort_order.as_deref())
        .finish();
    let mut sort = parse_sort_specs(sort_by, sort_order)?;
    if sort.is_empty() {
        sort = default_release_sort();
    }
    let library_scope =
        super::resolve_optional_library_filter(db, &principal, library_id.as_deref())?;

    let (release_items, next_cursor) = if let Some(page) = page_request.resume(&snapshot_key)? {
        let release_items = super::load_snapshot_items(
            db,
            &page.item_ids,
            db::releases::get_by_id,
            |db, release_db_id| {
                super::entity_accessible_to_principal(db, &principal, release_db_id)
            },
        )?;
        (release_items, page.next_cursor)
    } else {
        let genre_filter = parse_genre_id_filter(genre_id);
        let genre_db_ids = resolve_genre_id_filter(db, &genre_filter)?;
        let query_filters = db::releases::ReleaseQueryFilters {
            year,
            ids: if genre_db_ids.is_empty() {
                None
            } else {
                Some(db::genres::release_ids_matching_genre_ids(
                    db,
                    &genre_db_ids,
                )?)
            },
        };
        let accessible_releases = match library_scope {
            Some(library_db_id) => {
                db::releases::query(
                    db,
                    library_db_id,
                    &ListOptions {
                        sort: Vec::new(),
                        offset: None,
                        limit: None,
                        search_term: None,
                    },
                    &query_filters,
                )?
                .entries
            }
            None => {
                let releases = db::releases::query(
                    db,
                    "releases",
                    &ListOptions {
                        sort: Vec::new(),
                        offset: None,
                        limit: None,
                        search_term: None,
                    },
                    &query_filters,
                )?
                .entries;
                let mut accessible_releases = Vec::with_capacity(releases.len());
                for release in releases {
                    let Some(release_db_id) = release.db_id.clone().map(DbId::from) else {
                        continue;
                    };
                    if super::entity_accessible_to_principal(db, &principal, release_db_id)? {
                        accessible_releases.push(release);
                    }
                }
                accessible_releases
            }
        };
        let mut release_items = query_release_route_items(
            db,
            accessible_releases,
            &sort,
            search_term.as_deref(),
            principal.user_db_id,
        )?;
        let page = page_request.start(
            &snapshot_key,
            release_items
                .iter()
                .map(|release| release.id.clone())
                .collect(),
        )?;
        release_items.truncate(page.item_ids.len());
        (release_items, page.next_cursor)
    };
    let details = releases::list_details_for_releases(db, includes, release_items)?;

    let mut items: Vec<ReleaseResponse> = Vec::with_capacity(details.len());
    for detail in details {
        items.push(detail_to_release_response(
            db,
            detail,
            include_covers,
            include_artist_covers,
            include_genres,
            include_entry_paths,
        )?);
    }

    Ok(Json(PageResponse { items, next_cursor }))
}

async fn get_release(
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<ReleaseQuery>,
) -> Result<Json<ReleaseResponse>, AppError> {
    let principal = require_authenticated(&headers).await?;
    let include_entry_paths =
        db::roles::has_permission(&principal.permissions, Permission::ManageLibraries);

    let db = &*STATE.db.read().await;
    let (includes, include_covers, include_genres, include_artist_covers) =
        parse_release_includes(query.inc)?;
    let release_db_id = db::lookup::find_node_id_by_id(db, &id)?
        .ok_or_else(|| AppError::not_found(format!("not found: {id}")))?;
    super::require_entity_accessible(db, &principal, release_db_id, || {
        AppError::not_found(format!("Release not found: {id}"))
    })?;
    let detail = releases::get_details(db, release_db_id, includes)?
        .ok_or_else(|| AppError::not_found(format!("Release not found: {}", id)))?;

    Ok(Json(detail_to_release_response(
        db,
        detail,
        include_covers,
        include_artist_covers,
        include_genres,
        include_entry_paths,
    )?))
}

async fn search_release_covers(
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(query): Json<route_covers::CoverSearchQuery>,
) -> Result<Json<ReleaseCoverSearchResponse>, AppError> {
    let principal = require_authenticated(&headers).await?;

    let release_db_id = {
        let db = STATE.db.read().await;
        let release_db_id = db::lookup::find_node_id_by_id(&*db, &id)?
            .ok_or_else(|| AppError::not_found(format!("not found: {id}")))?;
        super::require_entity_accessible(&*db, &principal, release_db_id, || {
            AppError::not_found(format!("Release not found: {id}"))
        })?;
        if db::releases::get_by_id(&db, release_db_id)?.is_none() {
            return Err(AppError::not_found(format!("Release not found: {}", id)));
        }
        release_db_id
    };

    let provider_filter = query.provider.as_deref();
    let found = covers::search_release_cover_candidates(
        release_db_id,
        provider_filter,
        query.force_refresh,
    )
    .await?;
    let results = route_covers::map_provider_cover_search_results(found);

    Ok(Json(ReleaseCoverSearchResponse {
        release_id: id,
        results,
    }))
}

#[cfg(feature = "docgen")]
fn list_releases_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List releases").description(
        "Returns releases as `{ items, next_cursor }`. Supported query parameters: `inc`, `query`, `year`, `library_id`, `genre_id`, `sort_by`, `sort_order`, `limit`, `cursor`. `library_id` scopes results to releases belonging to that public library ID. `genre_id` filters by one or more public genre IDs. `sort_by` supports `sort_name`, `name`, `date_created`, `release_date`, `last_played_at`, `listen_count`, `total_duration`, and `id`; `sort_order` supports `ascending` and `descending`. `limit` defaults to 100 and is capped at 500. Drive pagination from `next_cursor`; it is `null` on the last page. Supported `inc` values: `artists`, `tracks`, `track_artists`, `entries`, `covers`, `artist_covers`, `genres`. When `inc=covers`, cover metadata includes a public image URL. When `inc=artists`, each artist carries a `credit` object with `type`, `detail`, and `source`; add `artist_covers` to include public artist image metadata. An artist may appear multiple times with different credits (for example, artist and producer). Track artists without direct credits inherit from the release (`source: release`). When `inc=entries`, `full_path` is included only for authenticated users with ManageLibraries permission.",
    )
}

#[cfg(feature = "docgen")]
fn get_release_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Get release by ID").description(
        "Returns a single release. 404 if not found. Use `inc` to include artists, tracks, track_artists, entries, covers, artist_covers, and/or genres. When `inc=covers`, cover metadata includes a public image URL. When `inc=artists`, each artist carries a `credit` object with `type`, `detail`, and `source`; add `artist_covers` to include public artist image metadata. An artist may appear multiple times with different credits. Track artists without direct credits inherit from the release (`source: release`). When `inc=entries`, `full_path` is included only for authenticated users with ManageLibraries permission.",
    )
}

#[cfg(feature = "docgen")]
fn search_release_covers_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Search release cover candidates").description(
        "Returns provider cover candidates for a release. Request body (JSON): `{ provider?, force_refresh? }`; \
        `force_refresh=true` bypasses cached provider cover resolution. Providers may return \
        width, height, and selected_index for automatic selection.",
    )
}

pub fn release_routes() -> Router {
    Router::new()
        .route("/", get(get_releases))
        .route("/{id}", get(get_release))
        .route("/{id}/mix", get(super::mix::get_release_mix))
        .route("/{id}/covers/search", post(search_release_covers))
}

#[cfg(feature = "docgen")]
pub(crate) fn release_openapi_routes() -> aide::axum::ApiRouter {
    use aide::axum::routing::{
        get_with,
        post_with,
    };

    aide::axum::ApiRouter::new()
        .api_route("/", get_with(get_releases, list_releases_docs))
        .api_route("/{id}", get_with(get_release, get_release_docs))
        .api_route(
            "/{id}/mix",
            get_with(super::mix::get_release_mix, super::mix::release_mix_docs),
        )
        .api_route(
            "/{id}/covers/search",
            post_with(search_release_covers, search_release_covers_docs),
        )
}

#[cfg(test)]
mod tests {
    use agdb::{
        DbAny,
        QueryBuilder,
    };
    use axum::{
        body::to_bytes,
        http::{
            HeaderMap,
            StatusCode,
            header::AUTHORIZATION,
        },
        response::IntoResponse,
    };

    use crate::db::SortDirection;
    use crate::db::test_db::{
        TestDb,
        connect,
        insert_library,
        insert_release as insert_test_release,
        insert_track,
    };
    use crate::services::auth::sessions;
    use crate::testing::{
        LibraryFixtureConfig,
        initialize_runtime,
        runtime_test_lock,
    };

    use super::*;
    use nanoid::nanoid;

    fn new_test_db() -> anyhow::Result<DbAny> {
        Ok(TestDb::new()?.into_inner())
    }

    fn insert_release_node(db: &mut DbAny) -> anyhow::Result<DbId> {
        let result = db.exec_mut(QueryBuilder::insert().nodes().count(1).query())?;
        result
            .ids()
            .first()
            .copied()
            .ok_or_else(|| anyhow::anyhow!("release insert returned no id"))
    }

    fn insert_cover_for_release(db: &mut DbAny, release_db_id: DbId) -> anyhow::Result<()> {
        let cover = db::Cover {
            db_id: None,
            id: nanoid!(),
            path: "/music/release/cover.jpg".to_string(),
            mime_type: "image/jpeg".to_string(),
            hash: "a".repeat(64),
            blurhash: Some("LKO2?U%2Tw=w]~RBVZRi};RPxuwH".to_string()),
        };

        let result = db.exec_mut(QueryBuilder::insert().element(&cover).query())?;
        let cover_id = result
            .ids()
            .first()
            .copied()
            .ok_or_else(|| anyhow::anyhow!("cover insert returned no id"))?;

        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(release_db_id)
                .to(cover_id)
                .query(),
        )?;

        Ok(())
    }

    fn update_track_duration(
        db: &mut DbAny,
        track_db_id: DbId,
        duration_ms: u64,
    ) -> anyhow::Result<()> {
        let mut track = db::tracks::get_by_id(db, track_db_id)?
            .ok_or_else(|| anyhow::anyhow!("track missing"))?;
        track.duration_ms = Some(duration_ms);
        db::tracks::update(db, &track)
    }

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

    #[test]
    fn parse_inc_accepts_covers() {
        let parsed = match parse_inc(Some(vec!["artists,covers,artist_covers".to_string()])) {
            Ok(value) => value,
            Err(_) => panic!("covers inc should parse"),
        };
        assert!(parsed.artists);
        assert!(!parsed.tracks);
        assert!(!parsed.entries);
        assert!(parsed.covers);
        assert!(parsed.artist_covers);
    }

    #[tokio::test]
    async fn parse_inc_error_mentions_covers() -> anyhow::Result<()> {
        let err = parse_inc(Some(vec!["unknown".to_string()])).expect_err("expected parse error");
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        let text = std::str::from_utf8(&body)?;
        assert!(
            text.contains(
                "Supported values: artists, tracks, track_artists, entries, covers, artist_covers, genres"
            )
        );
        Ok(())
    }

    #[test]
    fn parse_sort_specs_accepts_supported_values() -> anyhow::Result<()> {
        let specs = match parse_sort_specs(
            Some(vec![
                "sort_name,name".to_string(),
                "release_date,last_played_at,listen_count,total_duration".to_string(),
            ]),
            Some("descending".to_string()),
        ) {
            Ok(specs) => specs,
            Err(_) => return Err(anyhow::anyhow!("expected valid sort specs")),
        };
        assert_eq!(specs.len(), 6);
        assert!(matches!(
            specs[0].key,
            ReleaseRouteSortKey::Field(SortKey::SortName)
        ));
        assert!(matches!(
            specs[1].key,
            ReleaseRouteSortKey::Field(SortKey::Name)
        ));
        assert!(matches!(
            specs[2].key,
            ReleaseRouteSortKey::Field(SortKey::ReleaseDate)
        ));
        assert!(matches!(specs[3].key, ReleaseRouteSortKey::LastPlayedAt));
        assert!(matches!(specs[4].key, ReleaseRouteSortKey::ListenCount));
        assert!(matches!(specs[5].key, ReleaseRouteSortKey::TotalDuration));
        assert!(
            specs
                .iter()
                .all(|spec| matches!(spec.direction, SortDirection::Descending))
        );
        Ok(())
    }

    #[tokio::test]
    async fn parse_sort_specs_rejects_unsupported_values() -> anyhow::Result<()> {
        let err = parse_sort_specs(
            Some(vec!["duration,unknown".to_string()]),
            Some("ascending".to_string()),
        )
        .expect_err("expected sort parse error");
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        let text = std::str::from_utf8(&body)?;
        assert!(text.contains(release_sort_supported_values()));
        Ok(())
    }

    #[tokio::test]
    async fn parse_sort_specs_rejects_invalid_sort_order() -> anyhow::Result<()> {
        let err = parse_sort_specs(Some(vec!["name".to_string()]), Some("upward".to_string()))
            .expect_err("expected sort_order parse error");
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        let text = std::str::from_utf8(&body)?;
        assert!(text.contains("Supported values: ascending, descending"));
        Ok(())
    }

    #[test]
    fn parse_genre_id_filter_splits_and_trims_values() {
        let genre_ids = parse_genre_id_filter(Some(vec![
            "genre-rock, genre-jazz".to_string(),
            "genre-electronic".to_string(),
        ]));
        assert_eq!(
            genre_ids,
            vec!["genre-rock", "genre-jazz", "genre-electronic"]
        );
    }

    #[test]
    fn query_release_route_items_sorts_by_total_duration() -> anyhow::Result<()> {
        let mut db = crate::db::test_db::new_test_db()?;
        let short_release_id = insert_test_release(&mut db, "Short Release")?;
        let long_release_id = insert_test_release(&mut db, "Long Release")?;
        let short_track = insert_track(&mut db, "Short Track")?;
        let long_track_a = insert_track(&mut db, "Long Track A")?;
        let long_track_b = insert_track(&mut db, "Long Track B")?;
        update_track_duration(&mut db, short_track, 60_000)?;
        update_track_duration(&mut db, long_track_a, 120_000)?;
        update_track_duration(&mut db, long_track_b, 180_000)?;
        connect(&mut db, short_release_id, short_track)?;
        connect(&mut db, long_release_id, long_track_a)?;
        connect(&mut db, long_release_id, long_track_b)?;
        let releases = vec![
            db::releases::get_by_id(&db, short_release_id)?
                .ok_or_else(|| anyhow::anyhow!("short release missing"))?,
            db::releases::get_by_id(&db, long_release_id)?
                .ok_or_else(|| anyhow::anyhow!("long release missing"))?,
        ];

        let releases = query_release_route_items(
            &db,
            releases,
            &[ReleaseRouteSortSpec {
                key: ReleaseRouteSortKey::TotalDuration,
                direction: SortDirection::Descending,
            }],
            None,
            DbId(1),
        )?;

        let titles: Vec<String> = releases
            .into_iter()
            .map(|release| release.release_title)
            .collect();
        assert_eq!(titles, vec!["Long Release", "Short Release"]);
        Ok(())
    }

    #[tokio::test]
    async fn get_releases_scopes_by_library_id() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        setup_route_test().await?;

        let visible_library_id = {
            let mut db = STATE.db.write().await;
            let visible_library =
                insert_library(&mut db, "Visible Releases", "/tmp/lyra-visible-releases")?;
            let hidden_library =
                insert_library(&mut db, "Hidden Releases", "/tmp/lyra-hidden-releases")?;
            let visible_release = insert_test_release(&mut db, "Visible Release")?;
            let hidden_release = insert_test_release(&mut db, "Hidden Release")?;
            connect(&mut db, visible_library, visible_release)?;
            connect(&mut db, hidden_library, hidden_release)?;

            db::libraries::get_by_id(&db, visible_library)?
                .ok_or_else(|| anyhow::anyhow!("visible library missing"))?
                .id
        };
        let headers = create_admin_headers("release-scope-admin").await?;

        let Json(page) = get_releases(
            headers,
            Query(ReleaseListQuery {
                inc: None,
                query: None,
                year: None,
                library_id: Some(visible_library_id),
                genre_id: None,
                sort_by: None,
                sort_order: None,
                page: super::super::PageQuery::default(),
            }),
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].title, "Visible Release");
        assert!(page.next_cursor.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn get_releases_filters_by_genre_id() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        setup_route_test().await?;

        let rock_genre_id = {
            let mut db = STATE.db.write().await;
            let rock_release = insert_test_release(&mut db, "Rock Release")?;
            let jazz_release = insert_test_release(&mut db, "Jazz Release")?;
            db::genres::sync_release_genres(&mut *db, rock_release, &["Rock".to_string()])?;
            db::genres::sync_release_genres(&mut *db, jazz_release, &["Jazz".to_string()])?;
            db::genres::get_for_release(&*db, rock_release)?
                .into_iter()
                .next()
                .ok_or_else(|| anyhow::anyhow!("rock genre missing"))?
                .id
        };
        let headers = create_admin_headers("release-genre-filter-admin").await?;

        let Json(page) = get_releases(
            headers,
            Query(ReleaseListQuery {
                inc: None,
                query: None,
                year: None,
                library_id: None,
                genre_id: Some(vec![rock_genre_id]),
                sort_by: None,
                sort_order: None,
                page: super::super::PageQuery::default(),
            }),
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].title, "Rock Release");
        assert!(page.next_cursor.is_none());
        Ok(())
    }

    #[test]
    fn parse_cover_transform_options_accepts_common_values() -> anyhow::Result<()> {
        let query = route_covers::CoverQuery {
            format: Some("webp".to_string()),
            quality: Some(85),
            max_width: Some(640),
            max_height: Some(640),
        };

        let options = match route_covers::parse_cover_transform_options(&query) {
            Ok(options) => options,
            Err(_) => return Err(anyhow::anyhow!("expected valid transform options")),
        }
        .ok_or_else(|| anyhow::anyhow!("expected transform options"))?;
        assert_eq!(options.format, Some(image::ImageFormat::WebP));
        assert_eq!(options.quality, Some(85));
        assert_eq!(options.max_width, Some(640));
        assert_eq!(options.max_height, Some(640));
        Ok(())
    }

    #[test]
    fn parse_cover_transform_options_empty_is_none() -> anyhow::Result<()> {
        let query = route_covers::CoverQuery::default();
        let options = match route_covers::parse_cover_transform_options(&query) {
            Ok(options) => options,
            Err(_) => return Err(anyhow::anyhow!("expected empty transform options")),
        };
        assert!(options.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn parse_cover_transform_options_rejects_invalid_format() -> anyhow::Result<()> {
        let query = route_covers::CoverQuery {
            format: Some("gif".to_string()),
            quality: None,
            max_width: None,
            max_height: None,
        };

        let err = route_covers::parse_cover_transform_options(&query)
            .expect_err("expected invalid format error");
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        let text = std::str::from_utf8(&body)?;
        assert!(text.contains("Supported formats: jpg, png, webp"));
        Ok(())
    }

    #[tokio::test]
    async fn parse_cover_transform_options_rejects_invalid_quality_and_bounds() -> anyhow::Result<()>
    {
        let query = route_covers::CoverQuery {
            format: Some("jpg".to_string()),
            quality: Some(101),
            max_width: Some(0),
            max_height: None,
        };

        let err = route_covers::parse_cover_transform_options(&query)
            .expect_err("expected validation error");
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        let text = std::str::from_utf8(&body)?;
        assert!(
            text.contains("quality must be between 0 and 100")
                || text.contains("max_width must be greater than 0")
        );
        Ok(())
    }

    #[tokio::test]
    async fn parse_cover_transform_options_rejects_zero_bounds() -> anyhow::Result<()> {
        let query = route_covers::CoverQuery {
            format: None,
            quality: None,
            max_width: Some(0),
            max_height: Some(320),
        };

        let err =
            route_covers::parse_cover_transform_options(&query).expect_err("expected bounds error");
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        let text = std::str::from_utf8(&body)?;
        assert!(text.contains("max_width must be greater than 0"));
        Ok(())
    }

    #[test]
    fn release_response_omits_cover_field_when_not_requested() -> anyhow::Result<()> {
        let response = ReleaseResponse {
            id: String::new(),
            title: "Test Release".to_string(),
            sort_title: None,
            artists: None,
            tracks: None,
            entries: None,
            release_date: None,
            genres: None,
            cover: None,
        };

        let value = serde_json::to_value(response)?;
        assert!(value.get("cover").is_none());
        Ok(())
    }

    #[test]
    fn release_response_serializes_cover_as_null() -> anyhow::Result<()> {
        let response = ReleaseResponse {
            id: String::new(),
            title: "Test Release".to_string(),
            sort_title: None,
            artists: None,
            tracks: None,
            entries: None,
            release_date: None,
            genres: None,
            cover: Some(None),
        };

        let value = serde_json::to_value(response)?;
        assert!(value.get("cover").is_some_and(serde_json::Value::is_null));
        Ok(())
    }

    #[test]
    fn release_response_serializes_cover_object() -> anyhow::Result<()> {
        let response = ReleaseResponse {
            id: String::new(),
            title: "Test Release".to_string(),
            sort_title: None,
            artists: None,
            tracks: None,
            entries: None,
            release_date: None,
            genres: None,
            cover: Some(Some(crate::routes::responses::CoverResponse {
                id: "cover-1".to_string(),
                url: format!("/api/covers/cover-1?v={}", "b".repeat(64)),
                mime_type: "image/jpeg".to_string(),
                hash: "b".repeat(64),
                blurhash: Some("LKO2?U%2Tw=w]~RBVZRi};RPxuwH".to_string()),
            })),
        };

        let value = serde_json::to_value(response)?;
        let cover = value
            .get("cover")
            .and_then(serde_json::Value::as_object)
            .ok_or_else(|| anyhow::anyhow!("missing cover object"))?;
        assert_eq!(cover.get("id"), Some(&serde_json::json!("cover-1")));
        assert_eq!(
            cover.get("url"),
            Some(&serde_json::json!(format!(
                "/api/covers/cover-1?v={}",
                "b".repeat(64)
            )))
        );
        assert_eq!(
            cover.get("mime_type"),
            Some(&serde_json::json!("image/jpeg"))
        );
        assert_eq!(cover.get("hash"), Some(&serde_json::json!("b".repeat(64))));
        Ok(())
    }

    #[test]
    fn build_cover_response_omits_when_not_requested() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_db_id = insert_release_node(&mut db)?;
        let cover = route_covers::build_cover_response(&db, release_db_id, false)?;
        assert!(cover.is_none());
        Ok(())
    }

    #[test]
    fn build_cover_response_returns_null_when_missing() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_db_id = insert_release_node(&mut db)?;
        let cover = route_covers::build_cover_response(&db, release_db_id, true)?;
        assert!(matches!(cover, Some(None)));
        Ok(())
    }

    #[test]
    fn build_cover_response_returns_cover_metadata() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_db_id = insert_release_node(&mut db)?;
        insert_cover_for_release(&mut db, release_db_id)?;

        let cover = route_covers::build_cover_response(&db, release_db_id, true)?
            .flatten()
            .ok_or_else(|| anyhow::anyhow!("expected cover metadata"))?;

        assert_eq!(
            cover.url,
            format!("/api/covers/{}?v={}", cover.id, cover.hash)
        );
        assert_eq!(cover.mime_type, "image/jpeg");
        assert_eq!(cover.hash, "a".repeat(64));
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

    struct ReleaseSortBench {
        db: DbAny,
        user_db_id: DbId,
        releases: Vec<db::Release>,
    }

    fn update_track_duration(db: &mut DbAny, track_db_id: DbId, duration_ms: u64) {
        let mut track = db::tracks::get_by_id(db, track_db_id)
            .unwrap()
            .expect("track exists");
        track.duration_ms = Some(duration_ms);
        db::tracks::update(db, &track).unwrap();
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

    fn seed_release_sort_bench(
        release_count: usize,
        tracks_per_release: usize,
        listens_per_track: usize,
    ) -> ReleaseSortBench {
        let mut db = new_test_db().unwrap();
        let user_db_id =
            db::users::create(&mut db, &test_user("release-sort-bench").unwrap()).unwrap();
        let mut releases = Vec::with_capacity(release_count);
        for release_idx in 0..release_count {
            let release_db_id =
                insert_test_release(&mut db, &format!("Release {release_idx:04}")).unwrap();
            for track_idx in 0..tracks_per_release {
                let track_db_id = insert_track(
                    &mut db,
                    &format!("Release {release_idx:04} Track {track_idx:02}"),
                )
                .unwrap();
                update_track_duration(
                    &mut db,
                    track_db_id,
                    60_000 + ((release_idx + track_idx) % 300) as u64 * 1_000,
                );
                for listen_idx in 0..listens_per_track {
                    record_listen(
                        &mut db,
                        user_db_id,
                        track_db_id,
                        ((release_idx * tracks_per_release * listens_per_track)
                            + (track_idx * listens_per_track)
                            + listen_idx) as u64
                            * 1_000,
                    );
                }
                connect(&mut db, release_db_id, track_db_id).unwrap();
            }
            releases.push(
                db::releases::get_by_id(&db, release_db_id)
                    .unwrap()
                    .expect("release exists"),
            );
        }

        ReleaseSortBench {
            db,
            user_db_id,
            releases,
        }
    }

    #[bench]
    fn route_sort_releases_sort_name_500(b: &mut Bencher) {
        let setup = seed_release_sort_bench(500, 0, 0);
        let sort = default_release_sort();
        b.iter(|| {
            query_release_route_items(
                &setup.db,
                black_box(setup.releases.clone()),
                &sort,
                None,
                setup.user_db_id,
            )
            .unwrap()
        });
    }

    #[bench]
    fn route_sort_releases_total_duration_500_releases_4000_tracks(b: &mut Bencher) {
        let setup = seed_release_sort_bench(500, 8, 0);
        let sort = vec![ReleaseRouteSortSpec {
            key: ReleaseRouteSortKey::TotalDuration,
            direction: SortDirection::Descending,
        }];
        b.iter(|| {
            query_release_route_items(
                &setup.db,
                black_box(setup.releases.clone()),
                &sort,
                None,
                setup.user_db_id,
            )
            .unwrap()
        });
    }

    #[bench]
    fn route_sort_releases_listen_count_500_releases_4000_listens(b: &mut Bencher) {
        let setup = seed_release_sort_bench(500, 8, 1);
        let sort = vec![ReleaseRouteSortSpec {
            key: ReleaseRouteSortKey::ListenCount,
            direction: SortDirection::Descending,
        }];
        b.iter(|| {
            query_release_route_items(
                &setup.db,
                black_box(setup.releases.clone()),
                &sort,
                None,
                setup.user_db_id,
            )
            .unwrap()
        });
    }
}
