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
        patch,
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
        SortDirection,
        SortKey,
    },
    routes::AppError,
    routes::{
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
        },
    },
    services::{
        artists as artist_service,
        auth::{
            Principal,
            require_authenticated,
            require_manage_metadata,
        },
        covers,
        pagination::SnapshotKey,
        releases as release_service,
    },
};

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
        schemars(description = "Optional fuzzy text query matched against artist names.")
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
            description = "Comma-separated or repeated values: sort_name, name, date_created, last_played_at, listen_count, release_count, track_count, total_duration, id."
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

#[derive(Clone, Copy, Debug)]
enum ArtistRouteSortKey {
    Field(SortKey),
    ListenCount,
    LastPlayedAt,
    ReleaseCount,
    TrackCount,
    TotalDuration,
}

type ArtistRouteSortSpec = super::RouteSortSpec<ArtistRouteSortKey>;

fn default_artist_sort() -> Vec<ArtistRouteSortSpec> {
    vec![ArtistRouteSortSpec {
        key: ArtistRouteSortKey::Field(SortKey::SortName),
        direction: SortDirection::Ascending,
    }]
}

fn artist_sort_supported_values() -> &'static str {
    "sort_name, name, date_created, last_played_at, listen_count, release_count, track_count, total_duration, id"
}

fn parse_artist_sort_specs(
    sort_by: Option<Vec<String>>,
    sort_order: Option<String>,
) -> Result<Vec<ArtistRouteSortSpec>, AppError> {
    super::parse_route_sort_specs(
        sort_by,
        sort_order,
        |token| match token {
            "listen_count" => Some(ArtistRouteSortKey::ListenCount),
            "last_played_at" => Some(ArtistRouteSortKey::LastPlayedAt),
            "release_count" => Some(ArtistRouteSortKey::ReleaseCount),
            "track_count" => Some(ArtistRouteSortKey::TrackCount),
            "total_duration" => Some(ArtistRouteSortKey::TotalDuration),
            _ => SortKey::from_token(token).and_then(|key| match key {
                SortKey::SortName | SortKey::Name | SortKey::DateCreated | SortKey::DbId => {
                    Some(ArtistRouteSortKey::Field(key))
                }
                SortKey::ReleaseDate
                | SortKey::TrackNumber
                | SortKey::DiscNumber
                | SortKey::Duration => None,
            }),
        },
        artist_sort_supported_values(),
    )
}

struct ArtistRouteSortEntry {
    artist: db::Artist,
    lower_name: String,
    lower_sort_name: Option<String>,
    db_id: Option<i64>,
    date_created: Option<u64>,
    listen_count: u64,
    last_played_at: Option<u64>,
    release_count: u64,
    track_count: u64,
    total_duration: u64,
    match_score: u32,
}

impl ArtistRouteSortEntry {
    fn new(
        artist: db::Artist,
        listen_count: u64,
        last_played_at: Option<u64>,
        release_count: u64,
        track_count: u64,
        total_duration: u64,
    ) -> Self {
        Self {
            lower_name: artist.artist_name.to_lowercase(),
            lower_sort_name: artist.sort_name.as_ref().map(|value| value.to_lowercase()),
            db_id: artist.db_id.as_ref().map(|id| DbId::from(id.clone()).0),
            date_created: artist.created_at,
            artist,
            listen_count,
            last_played_at,
            release_count,
            track_count,
            total_duration,
            match_score: 0,
        }
    }
}

fn compare_artist_route_field(
    a: &ArtistRouteSortEntry,
    b: &ArtistRouteSortEntry,
    key: ArtistRouteSortKey,
) -> Ordering {
    match key {
        ArtistRouteSortKey::Field(SortKey::SortName) => a
            .lower_sort_name
            .as_deref()
            .unwrap_or(a.lower_name.as_str())
            .cmp(
                b.lower_sort_name
                    .as_deref()
                    .unwrap_or(b.lower_name.as_str()),
            ),
        ArtistRouteSortKey::Field(SortKey::Name) => a.lower_name.cmp(&b.lower_name),
        ArtistRouteSortKey::Field(SortKey::DateCreated) => {
            db::compare_option(&a.date_created, &b.date_created)
        }
        ArtistRouteSortKey::Field(SortKey::DbId) => db::compare_option(&a.db_id, &b.db_id),
        ArtistRouteSortKey::ListenCount => a.listen_count.cmp(&b.listen_count),
        ArtistRouteSortKey::LastPlayedAt => {
            db::compare_option(&a.last_played_at, &b.last_played_at)
        }
        ArtistRouteSortKey::ReleaseCount => a.release_count.cmp(&b.release_count),
        ArtistRouteSortKey::TrackCount => a.track_count.cmp(&b.track_count),
        ArtistRouteSortKey::TotalDuration => a.total_duration.cmp(&b.total_duration),
        ArtistRouteSortKey::Field(
            SortKey::ReleaseDate | SortKey::TrackNumber | SortKey::DiscNumber | SortKey::Duration,
        ) => Ordering::Equal,
    }
}

fn compare_artist_route_entries(
    a: &ArtistRouteSortEntry,
    b: &ArtistRouteSortEntry,
    sort: &[ArtistRouteSortSpec],
) -> Ordering {
    for spec in sort {
        let ord = db::apply_direction(compare_artist_route_field(a, b, spec.key), spec.direction);
        if ord != Ordering::Equal {
            return ord;
        }
    }

    b.match_score
        .cmp(&a.match_score)
        .then_with(|| a.lower_name.cmp(&b.lower_name))
        .then_with(|| db::compare_option(&a.db_id, &b.db_id))
}

fn artist_sort_needs_release_count(sort: &[ArtistRouteSortSpec]) -> bool {
    sort.iter()
        .any(|spec| matches!(spec.key, ArtistRouteSortKey::ReleaseCount))
}

fn artist_sort_needs_track_metrics(sort: &[ArtistRouteSortSpec]) -> bool {
    sort.iter().any(|spec| {
        matches!(
            spec.key,
            ArtistRouteSortKey::ListenCount
                | ArtistRouteSortKey::LastPlayedAt
                | ArtistRouteSortKey::TrackCount
                | ArtistRouteSortKey::TotalDuration
        )
    })
}

fn artist_sort_needs_listens(sort: &[ArtistRouteSortSpec]) -> bool {
    sort.iter().any(|spec| {
        matches!(
            spec.key,
            ArtistRouteSortKey::ListenCount | ArtistRouteSortKey::LastPlayedAt
        )
    })
}

fn query_artist_route_items(
    db: &DbAny,
    artists: Vec<db::Artist>,
    sort: &[ArtistRouteSortSpec],
    search_term: Option<&str>,
    principal: &Principal,
) -> anyhow::Result<Vec<db::Artist>> {
    let mut release_ids_by_artist: HashMap<DbId, HashSet<DbId>> = HashMap::new();
    let mut tracks_by_artist: HashMap<DbId, Vec<db::Track>> = HashMap::new();
    let mut all_track_ids = Vec::new();
    let mut seen_all_track_ids = HashSet::new();
    let needs_release_count = artist_sort_needs_release_count(sort);
    let needs_track_metrics = artist_sort_needs_track_metrics(sort);
    let needs_listens = artist_sort_needs_listens(sort);

    for artist in &artists {
        let Some(artist_db_id) = artist.db_id.clone().map(DbId::from) else {
            continue;
        };

        if needs_release_count {
            let mut release_ids = HashSet::new();
            for release in db::releases::get_by_artist(db, artist_db_id)? {
                let Some(release_db_id) = release.db_id.clone().map(DbId::from) else {
                    continue;
                };
                if release_accessible_to_principal(db, principal, release_db_id)? {
                    release_ids.insert(release_db_id);
                }
            }
            release_ids_by_artist.insert(artist_db_id, release_ids);
        }

        if needs_track_metrics {
            let mut tracks = db::tracks::get_by_artist(db, artist_db_id)?;
            tracks.extend(db::tracks::get_by_release_artists(db, &[artist_db_id])?);
            tracks = filter_accessible_tracks(db, principal, tracks)?;
            if needs_listens {
                for track in &tracks {
                    let Some(track_db_id) = track.db_id.clone().map(DbId::from) else {
                        continue;
                    };
                    if seen_all_track_ids.insert(track_db_id) {
                        all_track_ids.push(track_db_id);
                    }
                }
            }
            tracks_by_artist.insert(artist_db_id, tracks);
        }
    }

    let listen_stats: HashMap<DbId, db::listens::ListenStats> = if needs_listens {
        db::listens::get_stats_for_user_tracks(db, &all_track_ids, principal.user_db_id)?
            .into_iter()
            .map(|stats| (stats.db_id, stats))
            .collect()
    } else {
        HashMap::new()
    };

    let mut entries: Vec<ArtistRouteSortEntry> = artists
        .into_iter()
        .map(|artist| {
            let artist_db_id = artist.db_id.clone().map(DbId::from);
            let release_count = artist_db_id
                .and_then(|id| release_ids_by_artist.get(&id))
                .map(|ids| ids.len() as u64)
                .unwrap_or(0);
            let mut seen_artist_track_ids = HashSet::new();
            let mut listen_count = 0u64;
            let mut last_played_at = None;
            let mut track_count = 0u64;
            let mut total_duration = 0u64;

            if let Some(tracks) = artist_db_id.and_then(|id| tracks_by_artist.get(&id)) {
                for track in tracks {
                    let Some(track_db_id) = track.db_id.clone().map(DbId::from) else {
                        track_count = track_count.saturating_add(1);
                        if let Some(duration) = track.duration_ms {
                            total_duration = total_duration.saturating_add(duration);
                        }
                        continue;
                    };
                    if !seen_artist_track_ids.insert(track_db_id) {
                        continue;
                    }
                    track_count = track_count.saturating_add(1);
                    if let Some(duration) = track.duration_ms {
                        total_duration = total_duration.saturating_add(duration);
                    }
                    if let Some(stats) = listen_stats.get(&track_db_id) {
                        listen_count = listen_count.saturating_add(stats.count);
                        last_played_at = last_played_at.max(stats.last_played);
                    }
                }
            }

            ArtistRouteSortEntry::new(
                artist,
                listen_count,
                last_played_at,
                release_count,
                track_count,
                total_duration,
            )
        })
        .collect();

    if let Some(term) = search_term {
        db::search::fuzzy_filter(
            &mut entries,
            term,
            |entry| entry.artist.artist_name.as_str(),
            |entry, score| entry.match_score = score,
        );
    }

    entries.sort_by(|a, b| compare_artist_route_entries(a, b, sort));
    Ok(entries.into_iter().map(|entry| entry.artist).collect())
}

fn is_admin(principal: &Principal) -> bool {
    principal.permissions.contains(&db::Permission::Admin)
}

fn release_accessible_to_principal(
    db: &DbAny,
    principal: &Principal,
    release_db_id: DbId,
) -> anyhow::Result<bool> {
    if is_admin(principal) {
        return Ok(true);
    }
    Ok(db::libraries::get_by_release(db, release_db_id)?
        .into_iter()
        .any(|library| principal.accessible_library_ids.contains(&library.id)))
}

fn track_accessible_to_principal(
    db: &DbAny,
    principal: &Principal,
    track_db_id: DbId,
) -> anyhow::Result<bool> {
    if is_admin(principal) {
        return Ok(true);
    }
    for release in db::releases::get_by_track(db, track_db_id)? {
        let Some(release_db_id) = release.db_id.clone().map(DbId::from) else {
            continue;
        };
        if release_accessible_to_principal(db, principal, release_db_id)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn artist_accessible_to_principal(
    db: &DbAny,
    principal: &Principal,
    artist_db_id: DbId,
) -> anyhow::Result<bool> {
    if is_admin(principal) {
        return Ok(true);
    }
    for release in db::releases::get_by_artist(db, artist_db_id)? {
        let Some(release_db_id) = release.db_id.clone().map(DbId::from) else {
            continue;
        };
        if release_accessible_to_principal(db, principal, release_db_id)? {
            return Ok(true);
        }
    }
    for track in db::tracks::get_by_artist(db, artist_db_id)? {
        let Some(track_db_id) = track.db_id.clone().map(DbId::from) else {
            continue;
        };
        if track_accessible_to_principal(db, principal, track_db_id)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn filter_accessible_releases(
    db: &DbAny,
    principal: &Principal,
    releases: Vec<db::Release>,
) -> anyhow::Result<Vec<db::Release>> {
    let mut filtered = Vec::with_capacity(releases.len());
    for release in releases {
        let Some(release_db_id) = release.db_id.clone().map(DbId::from) else {
            continue;
        };
        if release_accessible_to_principal(db, principal, release_db_id)? {
            filtered.push(release);
        }
    }
    Ok(filtered)
}

fn filter_accessible_tracks(
    db: &DbAny,
    principal: &Principal,
    tracks: Vec<db::Track>,
) -> anyhow::Result<Vec<db::Track>> {
    let mut filtered = Vec::with_capacity(tracks.len());
    for track in tracks {
        let Some(track_db_id) = track.db_id.clone().map(DbId::from) else {
            continue;
        };
        if track_accessible_to_principal(db, principal, track_db_id)? {
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
        if artist_accessible_to_principal(db, principal, artist_db_id)? {
            filtered.push(relation);
        }
    }
    Ok(filtered)
}

fn artist_detail_to_response(
    db: &DbAny,
    principal: &Principal,
    mut detail: artist_service::ArtistDetails,
    includes: ArtistRouteIncludes,
) -> anyhow::Result<ArtistResponse> {
    if let Some(releases) = detail.releases.take() {
        detail.releases = Some(filter_accessible_releases(db, principal, releases)?);
    }
    if let Some(tracks) = detail.tracks.take() {
        detail.tracks = Some(filter_accessible_tracks(db, principal, tracks)?);
    }
    if let Some(relations) = detail.relations.take() {
        detail.relations = Some(filter_accessible_relations(db, principal, relations)?);
    }

    let artist_db_id = detail.artist.db_id.clone().map(DbId::from);
    let cover = match artist_db_id {
        Some(artist_db_id) => {
            route_covers::build_cover_response(db, artist_db_id, includes.covers)?
        }
        None if includes.covers => Some(None),
        None => None,
    };
    let releases = match detail.releases {
        Some(releases) if includes.release_artists || includes.release_covers => {
            let release_details = release_service::list_details_for_releases(
                db,
                release_service::ReleaseIncludes {
                    artists: includes.release_artists,
                    tracks: false,
                    track_artists: false,
                    entries: false,
                },
                releases,
            )?;
            Some(
                release_details
                    .into_iter()
                    .map(|detail| {
                        route_releases::detail_to_release_response(
                            db,
                            detail,
                            includes.release_covers,
                            includes.artist_covers,
                            false,
                            false,
                        )
                    })
                    .collect::<anyhow::Result<Vec<_>>>()?,
            )
        }
        Some(releases) => Some(releases.into_iter().map(ReleaseResponse::from).collect()),
        None => None,
    };
    let relation_covers = if includes.relation_covers {
        let related_artist_db_ids = detail
            .relations
            .as_ref()
            .map(|relations| {
                relations
                    .iter()
                    .filter_map(|relation| relation.artist.db_id.clone().map(DbId::from))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        Some(db::covers::get_many(db, &related_artist_db_ids)?)
    } else {
        None
    };
    let relations = detail.relations.map(|v| {
        v.into_iter()
            .map(|r| {
                let related_artist_db_id = r.artist.db_id.clone().map(DbId::from);
                let cover = relation_covers.as_ref().map(|covers| {
                    related_artist_db_id
                        .and_then(|id| covers.get(&id).cloned())
                        .map(route_covers::cover_to_response)
                });
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
    sort_by: Option<Vec<String>>,
    sort_order: Option<String>,
    page_request: super::SnapshotPageRequest,
) -> Result<PageResponse<ArtistResponse>, AppError> {
    let db = &*STATE.db.read().await;
    let includes = parse_inc(inc)?;
    let search_term = super::parse_text_query(query);
    let snapshot_key = SnapshotKey::builder(&principal.user_public_id, "artists")
        .field(search_term.as_deref())
        .field(library_id.as_deref())
        .values(sort_by.as_deref())
        .field(sort_order.as_deref())
        .finish();
    let mut sort = parse_artist_sort_specs(sort_by, sort_order)?;
    if sort.is_empty() {
        sort = default_artist_sort();
    }
    let library_scope =
        super::resolve_optional_library_filter(db, principal, library_id.as_deref())?;

    let (artists, next_cursor) = if let Some(page) = page_request.resume(&snapshot_key)? {
        let artists = super::load_snapshot_items(
            db,
            &page.item_ids,
            db::artists::get_by_id,
            |db, artist_db_id| artist_accessible_to_principal(db, principal, artist_db_id),
        )?;
        (artists, page.next_cursor)
    } else {
        let accessible_artists = match library_scope {
            Some(library_db_id) => {
                artist_service::query_credited(
                    db,
                    Some(&db::ResolveId::DbId(library_db_id)),
                    &artist_service::CreditedArtistFilters::default(),
                    &ListOptions {
                        sort: Vec::new(),
                        offset: None,
                        limit: None,
                        search_term: None,
                    },
                )?
                .entries
            }
            None => {
                let artists = db::artists::get(db, "artists")?;
                let mut accessible_artists = Vec::with_capacity(artists.len());
                for artist in artists {
                    let Some(artist_db_id) = artist.db_id.clone().map(DbId::from) else {
                        continue;
                    };
                    if artist_accessible_to_principal(db, principal, artist_db_id)? {
                        accessible_artists.push(artist);
                    }
                }
                accessible_artists
            }
        };
        let mut artists = query_artist_route_items(
            db,
            accessible_artists,
            &sort,
            search_term.as_deref(),
            principal,
        )?;
        let page = page_request.start(
            &snapshot_key,
            artists.iter().map(|artist| artist.id.clone()).collect(),
        )?;
        artists.truncate(page.item_ids.len());
        (artists, page.next_cursor)
    };
    let details = artist_service::list_details_for_artists(db, includes.service, artists)?;

    let mut items = Vec::with_capacity(details.len());
    for detail in details {
        items.push(artist_detail_to_response(db, principal, detail, includes)?);
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
    if !artist_accessible_to_principal(db, principal, artist_db_id)? {
        return Err(AppError::not_found(format!("Artist not found: {id}")));
    }
    let detail = artist_service::get_details(db, artist_db_id, includes.service)?
        .ok_or_else(|| AppError::not_found(format!("Artist not found: {}", id)))?;

    Ok(artist_detail_to_response(db, principal, detail, includes)?)
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
        page,
    } = list_query;
    let page = page.resolve_snapshot();
    let principal = require_authenticated(&headers).await?;
    Ok(Json(
        list_artist_responses(
            &principal, inc, query, library_id, sort_by, sort_order, page,
        )
        .await?,
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
        if !artist_accessible_to_principal(&db, &principal, artist_db_id)? {
            return Err(AppError::not_found(format!("Artist not found: {id}")));
        }
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
    let principal = require_manage_metadata(&headers).await?;

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
    if !artist_accessible_to_principal(&db, &principal, artist_db_id)? {
        return Err(AppError::not_found(format!("Artist not found: {id}")));
    }
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
        "Returns artists as `{ items, next_cursor }`. Supported query parameters: `inc`, `query`, `library_id`, `sort_by`, `sort_order`, `limit`, `cursor`. `library_id` scopes results to artists credited by releases or tracks belonging to that public library ID. `sort_by` supports `sort_name`, `name`, `date_created`, `last_played_at`, `listen_count`, `release_count`, `track_count`, `total_duration`, and `id`; `sort_order` supports `ascending` and `descending`. `limit` defaults to 100 and is capped at 500. Drive pagination from `next_cursor`; it is `null` on the last page. `query` is a fuzzy text match against artist names. Use `inc` to include releases, tracks, relations, and/or covers. When `inc=covers`, artist cover metadata includes a public image URL. When `inc=relations`, add `relation_covers` to include public image metadata for related artists. When `inc=releases`, use `release_artists`, `release_covers`, and/or `artist_covers` to hydrate those nested release fields. The `credit` field is not present on artist-level responses; it only appears when artists are included via track or release endpoints.",
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
        .route("/{id}/mix", get(super::mix::get_artist_mix))
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
            "/{id}/mix",
            get_with(super::mix::get_artist_mix, super::mix::artist_mix_docs),
        )
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
            new_test_db,
        },
        services::auth::sessions,
        testing::{
            LibraryFixtureConfig,
            initialize_runtime,
            runtime_test_lock,
        },
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

    fn user_principal(accessible_library_ids: HashSet<String>) -> Principal {
        Principal {
            user_db_id: DbId(1),
            user_public_id: "user".to_string(),
            username: "user".to_string(),
            permissions: Vec::new(),
            role_name: Some("user".to_string()),
            accessible_library_ids,
        }
    }

    fn insert_cover_for(db: &mut DbAny, owner_db_id: DbId) -> anyhow::Result<db::Cover> {
        db::covers::upsert(
            db,
            owner_db_id,
            db::Cover {
                db_id: None,
                id: nanoid!(),
                path: "/music/cover.jpg".to_string(),
                mime_type: "image/jpeg".to_string(),
                hash: "a".repeat(64),
                blurhash: Some("LKO2?U%2Tw=w]~RBVZRi};RPxuwH".to_string()),
            },
        )
    }

    #[test]
    fn query_artist_route_items_counts_only_accessible_releases() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let visible_library =
            insert_library(&mut db, "Visible Artist Counts", "/tmp/lyra-visible-counts")?;
        let hidden_library =
            insert_library(&mut db, "Hidden Artist Counts", "/tmp/lyra-hidden-counts")?;
        let visible_library_id = db::libraries::get_by_id(&db, visible_library)?
            .ok_or_else(|| anyhow::anyhow!("visible library missing"))?
            .id;

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

        let artists = vec![
            db::artists::get_by_id(&db, mostly_hidden)?
                .ok_or_else(|| anyhow::anyhow!("mostly hidden artist missing"))?,
            db::artists::get_by_id(&db, visible_heavy)?
                .ok_or_else(|| anyhow::anyhow!("visible heavy artist missing"))?,
        ];
        let principal = user_principal(HashSet::from([visible_library_id]));

        let artists = query_artist_route_items(
            &db,
            artists,
            &[ArtistRouteSortSpec {
                key: ArtistRouteSortKey::ReleaseCount,
                direction: SortDirection::Descending,
            }],
            None,
            &principal,
        )?;

        let names: Vec<String> = artists
            .into_iter()
            .map(|artist| artist.artist_name)
            .collect();
        assert_eq!(names, vec!["Visible Heavy Artist", "Mostly Hidden Artist"]);
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

    #[test]
    fn parse_artist_sort_specs_accepts_supported_values() -> anyhow::Result<()> {
        let specs = match parse_artist_sort_specs(
            Some(vec![
                "sort_name,name".to_string(),
                "date_created,last_played_at,listen_count,release_count,track_count,total_duration,id".to_string(),
            ]),
            Some("descending".to_string()),
        ) {
            Ok(specs) => specs,
            Err(_) => return Err(anyhow::anyhow!("expected valid artist sort specs")),
        };

        assert_eq!(specs.len(), 9);
        assert!(matches!(
            specs[0].key,
            ArtistRouteSortKey::Field(SortKey::SortName)
        ));
        assert!(matches!(
            specs[1].key,
            ArtistRouteSortKey::Field(SortKey::Name)
        ));
        assert!(matches!(
            specs[2].key,
            ArtistRouteSortKey::Field(SortKey::DateCreated)
        ));
        assert!(matches!(specs[3].key, ArtistRouteSortKey::LastPlayedAt));
        assert!(matches!(specs[4].key, ArtistRouteSortKey::ListenCount));
        assert!(matches!(specs[5].key, ArtistRouteSortKey::ReleaseCount));
        assert!(matches!(specs[6].key, ArtistRouteSortKey::TrackCount));
        assert!(matches!(specs[7].key, ArtistRouteSortKey::TotalDuration));
        assert!(matches!(
            specs[8].key,
            ArtistRouteSortKey::Field(SortKey::DbId)
        ));
        assert!(
            specs
                .iter()
                .all(|spec| matches!(spec.direction, SortDirection::Descending))
        );
        Ok(())
    }

    #[test]
    fn parse_artist_sort_specs_rejects_track_only_values() {
        assert!(parse_artist_sort_specs(Some(vec!["disc,track".to_string()]), None).is_err());
    }

    #[test]
    fn query_artist_route_items_sorts_by_release_count() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let one_release_artist_id = insert_artist(&mut db, "One Release Artist")?;
        let two_release_artist_id = insert_artist(&mut db, "Two Release Artist")?;
        let first_release = insert_release(&mut db, "First Release")?;
        let second_release = insert_release(&mut db, "Second Release")?;
        let third_release = insert_release(&mut db, "Third Release")?;
        connect_artist(&mut db, first_release, one_release_artist_id)?;
        connect_artist(&mut db, second_release, two_release_artist_id)?;
        connect_artist(&mut db, third_release, two_release_artist_id)?;
        let artists = vec![
            db::artists::get_by_id(&db, one_release_artist_id)?
                .ok_or_else(|| anyhow::anyhow!("one release artist missing"))?,
            db::artists::get_by_id(&db, two_release_artist_id)?
                .ok_or_else(|| anyhow::anyhow!("two release artist missing"))?,
        ];

        let artists = query_artist_route_items(
            &db,
            artists,
            &[ArtistRouteSortSpec {
                key: ArtistRouteSortKey::ReleaseCount,
                direction: SortDirection::Descending,
            }],
            None,
            &admin_principal(HashSet::new()),
        )?;

        let names: Vec<String> = artists
            .into_iter()
            .map(|artist| artist.artist_name)
            .collect();
        assert_eq!(names, vec!["Two Release Artist", "One Release Artist"]);
        Ok(())
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

            db::artists::get_by_id(&db, artist_id)?
                .ok_or_else(|| anyhow::anyhow!("artist should exist"))?
                .id
        };
        let principal = admin_principal(HashSet::new());

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
            db::artists::relations::link(
                &mut db,
                voice_actor,
                character,
                db::ArtistRelationType::VoiceActor,
                Some("main".to_string()),
            )?;
            let cover_id = insert_cover_for(&mut db, voice_actor)?.id;
            let artist_public_id = db::artists::get_by_id(&db, character)?
                .ok_or_else(|| anyhow::anyhow!("artist should exist"))?
                .id;
            (artist_public_id, cover_id)
        };
        let principal = admin_principal(HashSet::new());

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
            db::artists::relations::link(
                &mut db,
                artist,
                hidden_relation,
                db::ArtistRelationType::MemberOf,
                None,
            )?;

            let artist_public_id = db::artists::get_by_id(&db, artist)?
                .ok_or_else(|| anyhow::anyhow!("artist missing"))?
                .id;
            let visible_library_id = db::libraries::get_by_id(&db, visible_library)?
                .ok_or_else(|| anyhow::anyhow!("visible library missing"))?
                .id;
            (artist_public_id, visible_library_id)
        };
        let principal = user_principal(HashSet::from([visible_library_id]));

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
            None,
            None,
            super::super::SnapshotPageRequest::first_page(100),
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
    async fn list_artist_responses_applies_sort_options() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let _test_dir = initialize_test_runtime().await?;

        {
            let mut db = STATE.db.write().await;
            insert_artist(&mut db, "Alpha")?;
            insert_artist(&mut db, "Charlie")?;
            insert_artist(&mut db, "Bravo")?;
        }
        let principal = admin_principal(HashSet::new());

        let page = list_artist_responses(
            &principal,
            None,
            None,
            None,
            Some(vec!["name".to_string()]),
            Some("descending".to_string()),
            super::super::SnapshotPageRequest::first_page(100),
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
        connect_artist,
        insert_artist,
        insert_release,
        insert_track,
        new_test_db,
        test_user,
    };

    struct ArtistSortBench {
        db: DbAny,
        user_db_id: DbId,
        principal: Principal,
        artists: Vec<db::Artist>,
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

    fn seed_artist_sort_bench(
        artist_count: usize,
        releases_per_artist: usize,
        tracks_per_release: usize,
        listens_per_track: usize,
    ) -> ArtistSortBench {
        let mut db = new_test_db().unwrap();
        let user_db_id =
            db::users::create(&mut db, &test_user("artist-sort-bench").unwrap()).unwrap();
        let mut artists = Vec::with_capacity(artist_count);
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
                        record_listen(
                            &mut db,
                            user_db_id,
                            track_db_id,
                            ((artist_idx * releases_per_artist * tracks_per_release)
                                + (release_idx * tracks_per_release)
                                + track_idx
                                + listen_idx) as u64
                                * 1_000,
                        );
                    }
                    connect(&mut db, release_db_id, track_db_id).unwrap();
                }
            }
            artists.push(
                db::artists::get_by_id(&db, artist_db_id)
                    .unwrap()
                    .expect("artist exists"),
            );
        }

        ArtistSortBench {
            db,
            user_db_id,
            principal: Principal {
                user_db_id,
                user_public_id: "artist-sort-bench".to_string(),
                username: "artist-sort-bench".to_string(),
                permissions: vec![db::Permission::Admin],
                role_name: Some("admin".to_string()),
                accessible_library_ids: HashSet::new(),
            },
            artists,
        }
    }

    #[bench]
    fn route_sort_artists_sort_name_500(b: &mut Bencher) {
        let setup = seed_artist_sort_bench(500, 0, 0, 0);
        let sort = default_artist_sort();
        b.iter(|| {
            query_artist_route_items(
                &setup.db,
                black_box(setup.artists.clone()),
                &sort,
                None,
                &setup.principal,
            )
            .unwrap()
        });
    }

    #[bench]
    fn route_sort_artists_total_duration_100_artists_2000_tracks(b: &mut Bencher) {
        let setup = seed_artist_sort_bench(100, 5, 4, 0);
        let sort = vec![ArtistRouteSortSpec {
            key: ArtistRouteSortKey::TotalDuration,
            direction: SortDirection::Descending,
        }];
        b.iter(|| {
            query_artist_route_items(
                &setup.db,
                black_box(setup.artists.clone()),
                &sort,
                None,
                &setup.principal,
            )
            .unwrap()
        });
    }

    #[bench]
    fn route_sort_artists_listen_count_100_artists_2000_listens(b: &mut Bencher) {
        let setup = seed_artist_sort_bench(100, 5, 4, 1);
        let sort = vec![ArtistRouteSortSpec {
            key: ArtistRouteSortKey::ListenCount,
            direction: SortDirection::Descending,
        }];
        b.iter(|| {
            query_artist_route_items(
                &setup.db,
                black_box(setup.artists.clone()),
                &sort,
                None,
                &setup.principal,
            )
            .unwrap()
        });
    }
}
