// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::DbId;
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
use std::{
    cmp::Ordering,
    collections::{
        BTreeMap,
        HashMap,
        HashSet,
    },
};

use crate::routes::request::bearer_credential;
use crate::services::catalog::Direction;
use crate::{
    routes::{
        self,
        AppError,
        responses::PageResponse,
    },
    services::STATE,
    services::{
        auth::{
            Principal,
            require_permission,
            require_principal,
        },
        listens::{
            self as listen_service,
            MAX_SUBMITTED_PLAYS,
            RecordOutcome,
            SubmissionOutcome,
            SubmittedPlay,
        },
        pagination::SnapshotKey,
        playback_sessions as playback_service,
        providers::provider_registry,
    },
};
use lyra_db::{
    Permission,
    ResolveId,
};

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
pub(super) struct MeListenQuery {
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Optional track ID to narrow the summary to one track.")
    )]
    track_id: Option<String>,
    #[serde(flatten)]
    page: routes::PageQuery,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Comma-separated or repeated values: listen_count, count, last_played_at, last_played, track_id, id."
        )
    )]
    #[serde(default, deserialize_with = "routes::deserialize_inc")]
    sort_by: Option<Vec<String>>,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Sort order for all sort keys: ascending or descending.")
    )]
    sort_order: Option<String>,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "When `track_id` is supplied, also counts listens for tracks that share a provider-declared unique track external ID."
        )
    )]
    merge_unique_external_ids: Option<bool>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct ListenQuery {
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Optional user ID to narrow the summaries to one user.")
    )]
    user_id: Option<String>,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Optional track ID to narrow the summary to one track.")
    )]
    track_id: Option<String>,
    #[serde(flatten)]
    page: routes::PageQuery,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Comma-separated or repeated values: listen_count, count, last_played_at, last_played, user_id, track_id, id."
        )
    )]
    #[serde(default, deserialize_with = "routes::deserialize_inc")]
    sort_by: Option<Vec<String>>,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Sort order for all sort keys: ascending or descending.")
    )]
    sort_order: Option<String>,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "When `track_id` is supplied, also counts listens for tracks that share a provider-declared unique track external ID."
        )
    )]
    merge_unique_external_ids: Option<bool>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
pub(super) struct MeListenResponse {
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Track ID represented by this summary.")
    )]
    track_id: String,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Number of listens for the authenticated user and track.")
    )]
    listen_count: u64,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Most recent listen time for the authenticated user and track.")
    )]
    last_played_at: Option<String>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct ListenResponse {
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "User ID represented by this summary.")
    )]
    user_id: String,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Track ID represented by this summary.")
    )]
    track_id: String,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Number of listens for this user and track.")
    )]
    listen_count: u64,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Most recent listen time for this user and track.")
    )]
    last_played_at: Option<String>,
}

#[derive(Clone)]
struct ListenRow {
    user_id: String,
    track_id: String,
    listen_count: u64,
    last_played_ms: Option<u64>,
}

struct ResolvedTrackFilter {
    track_db_ids: Vec<DbId>,
    track_public_ids: HashSet<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ListenSortKey {
    ListenCount,
    LastPlayedAt,
    UserId,
    TrackId,
}

fn parse_sort_order(
    sort_order: Option<String>,
    default_direction: Direction,
) -> Result<Direction, AppError> {
    Ok(sort_order
        .as_deref()
        .map(Direction::parse)
        .transpose()?
        .unwrap_or(default_direction))
}

fn supported_sort_values(include_user_id: bool) -> &'static str {
    if include_user_id {
        "listen_count, count, last_played_at, last_played, user_id, track_id, id"
    } else {
        "listen_count, count, last_played_at, last_played, track_id, id"
    }
}

fn parse_sort_keys(
    sort_by: Option<Vec<String>>,
    include_user_id: bool,
) -> Result<Vec<ListenSortKey>, AppError> {
    let Some(values) = sort_by else {
        return Ok(Vec::new());
    };

    let mut keys = Vec::new();
    let mut unknown = Vec::new();
    for value in values {
        for entry in value.split(',') {
            let entry = entry.trim();
            if entry.is_empty() {
                continue;
            }

            let token = entry.to_ascii_lowercase();
            let key = match token.as_str() {
                "listen_count" | "count" => ListenSortKey::ListenCount,
                "last_played_at" | "last_played" => ListenSortKey::LastPlayedAt,
                "user_id" if include_user_id => ListenSortKey::UserId,
                "track_id" | "id" => ListenSortKey::TrackId,
                _ => {
                    unknown.push(entry.to_string());
                    continue;
                }
            };
            if !keys.contains(&key) {
                keys.push(key);
            }
        }
    }

    if !unknown.is_empty() {
        return Err(AppError::bad_request(format!(
            "Unsupported sort_by value(s): {}. Supported values: {}",
            unknown.join(", "),
            supported_sort_values(include_user_id),
        )));
    }

    Ok(keys)
}

fn compare_optional_ms(a: Option<u64>, b: Option<u64>, direction: Direction) -> Ordering {
    match (a, b) {
        (Some(a), Some(b)) => direction.apply(a.cmp(&b)),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

fn compare_listen_rows(
    a: &ListenRow,
    b: &ListenRow,
    keys: &[ListenSortKey],
    direction: Direction,
) -> Ordering {
    for key in keys {
        let ord = match key {
            ListenSortKey::ListenCount => direction.apply(a.listen_count.cmp(&b.listen_count)),
            ListenSortKey::LastPlayedAt => {
                compare_optional_ms(a.last_played_ms, b.last_played_ms, direction)
            }
            ListenSortKey::UserId => direction.apply(a.user_id.cmp(&b.user_id)),
            ListenSortKey::TrackId => direction.apply(a.track_id.cmp(&b.track_id)),
        };
        if ord != Ordering::Equal {
            return ord;
        }
    }

    Ordering::Equal
}

fn sort_listen_rows(
    rows: &mut [ListenRow],
    sort_by: Option<Vec<String>>,
    sort_order: Option<String>,
    include_user_id: bool,
) -> Result<(), AppError> {
    let keys = parse_sort_keys(sort_by, include_user_id)?;
    if keys.is_empty() {
        let direction = parse_sort_order(sort_order, Direction::Descending)?;
        rows.sort_by(|a, b| {
            direction
                .apply(a.listen_count.cmp(&b.listen_count))
                .then_with(|| compare_optional_ms(a.last_played_ms, b.last_played_ms, direction))
                .then_with(|| a.user_id.cmp(&b.user_id))
                .then_with(|| a.track_id.cmp(&b.track_id))
        });
        return Ok(());
    }

    let direction = parse_sort_order(sort_order, Direction::Ascending)?;
    rows.sort_by(|a, b| {
        compare_listen_rows(a, b, &keys, direction)
            .then_with(|| a.user_id.cmp(&b.user_id))
            .then_with(|| a.track_id.cmp(&b.track_id))
    });
    Ok(())
}

fn listen_row_to_me_response(row: ListenRow) -> MeListenResponse {
    MeListenResponse {
        track_id: row.track_id,
        listen_count: row.listen_count,
        last_played_at: row
            .last_played_ms
            .map(crate::services::timestamps::unix_ms_to_rfc3339_u64),
    }
}

fn listen_row_to_response(row: ListenRow) -> ListenResponse {
    ListenResponse {
        user_id: row.user_id,
        track_id: row.track_id,
        listen_count: row.listen_count,
        last_played_at: row
            .last_played_ms
            .map(crate::services::timestamps::unix_ms_to_rfc3339_u64),
    }
}

fn listen_row_id(row: &ListenRow) -> String {
    format!(
        "{}\0{}\0{}\0{}",
        row.user_id,
        row.track_id,
        row.listen_count,
        row.last_played_ms
            .map(|value| value.to_string())
            .unwrap_or_default(),
    )
}

fn parse_listen_row_id(raw: &str) -> Option<ListenRow> {
    let mut fields = raw.splitn(4, '\0');
    let user_id = fields.next()?.to_string();
    let track_id = fields.next()?.to_string();
    let listen_count = fields.next()?.parse().ok()?;
    let last_played_ms = match fields.next()? {
        "" => None,
        value => Some(value.parse().ok()?),
    };
    Some(ListenRow {
        user_id,
        track_id,
        listen_count,
        last_played_ms,
    })
}

fn validate_merge_usage(
    track_id: Option<&str>,
    merge_unique_external_ids: bool,
) -> Result<(), AppError> {
    if track_id.is_none() && merge_unique_external_ids {
        return Err(AppError::bad_request(
            "merge_unique_external_ids requires track_id",
        ));
    }
    Ok(())
}

fn aggregate_stats(stats: Vec<lyra_db::listens::ListenStats>) -> (u64, Option<u64>) {
    let mut listen_count = 0u64;
    let mut last_played_ms: Option<u64> = None;
    for stat in stats {
        listen_count = listen_count.saturating_add(stat.count);
        if let Some(last_played) = stat.last_played
            && last_played > last_played_ms.unwrap_or(0)
        {
            last_played_ms = Some(last_played);
        }
    }
    (listen_count, last_played_ms)
}

/// Provider id pairs to merge play counts across, or `None` when merging is off.
async fn merge_track_id_pairs(
    merge_unique_external_ids: bool,
) -> Option<HashSet<(String, String)>> {
    if !merge_unique_external_ids {
        return None;
    }
    Some(
        provider_registry()
            .read_owned()
            .await
            .unique_track_id_pairs(),
    )
}

/// Resolves the track filter under the guard that reads listens with it.
fn resolve_accessible_track_filter(
    db: &agdb::DbAny,
    principal: &Principal,
    track_id: &str,
    merge_pairs: Option<&HashSet<(String, String)>>,
) -> Result<ResolvedTrackFilter, AppError> {
    let track_db_id = lyra_db::lookup::find_node_id_by_id(db, track_id)?
        .ok_or_else(|| AppError::not_found(format!("Track not found: {}", track_id)))?;
    crate::services::auth::access::require_entity_accessible(db, principal, track_db_id, || {
        AppError::not_found(format!("Track not found: {}", track_id))
    })?;

    let stat_track_ids = match merge_pairs {
        Some(pairs) => {
            playback_service::resolve_merged_track_ids_for_play_count(db, track_db_id, pairs)?
        }
        None => vec![track_db_id],
    };

    let mut accessible_track_ids = Vec::with_capacity(stat_track_ids.len());
    for id in stat_track_ids {
        if crate::services::auth::access::entity_accessible(db, principal, id)? {
            accessible_track_ids.push(id);
        }
    }

    let track_public_ids = lyra_db::lookup::find_ids_by_db_ids(db, &accessible_track_ids)?
        .into_values()
        .collect::<HashSet<_>>();

    Ok(ResolvedTrackFilter {
        track_db_ids: accessible_track_ids,
        track_public_ids,
    })
}

/// Whose listens a per-user listing reads, resolved under the guard that reads them.
enum ListenOwner<'a> {
    Principal,
    Public(&'a str),
}

impl ListenOwner<'_> {
    fn resolve(&self, db: &agdb::DbAny, principal: &Principal) -> Result<(DbId, String), AppError> {
        match self {
            Self::Principal => Ok((principal.require(db)?, principal.user_public_id.clone())),
            Self::Public(user_id) => {
                let user = lyra_db::users::get_by_public_id(db, user_id)?
                    .ok_or_else(|| AppError::not_found(format!("User not found: {user_id}")))?;
                let user_db_id = user
                    .db_id
                    .ok_or_else(|| anyhow::anyhow!("user missing db_id"))?;
                Ok((user_db_id, user.id))
            }
        }
    }
}

async fn rows_for_user(
    principal: &Principal,
    owner: ListenOwner<'_>,
    track_id: Option<String>,
    merge_unique_external_ids: bool,
) -> Result<Vec<ListenRow>, AppError> {
    if let Some(track_id) = track_id {
        let merge_pairs = merge_track_id_pairs(merge_unique_external_ids).await;
        let db = STATE.db.read().await;
        let track_filter =
            resolve_accessible_track_filter(&db, principal, &track_id, merge_pairs.as_ref())?;
        let (user_db_id, user_public_id) = owner.resolve(&db, principal)?;
        let stats = lyra_db::listens::get_stats(&db, &track_filter.track_db_ids, user_db_id)?;
        let (listen_count, last_played_ms) = aggregate_stats(stats);
        return Ok(vec![ListenRow {
            user_id: user_public_id,
            track_id,
            listen_count,
            last_played_ms,
        }]);
    }

    let db = STATE.db.read().await;
    let (user_db_id, _) = owner.resolve(&db, principal)?;
    let summaries = lyra_db::listens::list_summaries_for_user(&db, user_db_id)?;
    let mut rows = Vec::with_capacity(summaries.len());
    for summary in summaries {
        if !crate::services::auth::access::entity_accessible(&*db, principal, summary.track_db_id)?
        {
            continue;
        }
        rows.push(ListenRow {
            user_id: summary.user_public_id,
            track_id: summary.track_public_id,
            listen_count: summary.count,
            last_played_ms: summary.last_played,
        });
    }
    Ok(rows)
}

async fn rows_for_managed_listens(
    principal: &Principal,
    query: &ListenQuery,
) -> Result<Vec<ListenRow>, AppError> {
    let merge_unique_external_ids = query.merge_unique_external_ids.unwrap_or(false);
    validate_merge_usage(query.track_id.as_deref(), merge_unique_external_ids)?;

    if let Some(user_id) = query.user_id.as_deref() {
        return rows_for_user(
            principal,
            ListenOwner::Public(user_id),
            query.track_id.clone(),
            merge_unique_external_ids,
        )
        .await;
    }

    let merge_pairs = merge_track_id_pairs(merge_unique_external_ids).await;
    let db = STATE.db.read().await;
    let track_filter = query
        .track_id
        .as_deref()
        .map(|track_id| {
            resolve_accessible_track_filter(&db, principal, track_id, merge_pairs.as_ref())
        })
        .transpose()?;
    let summaries = lyra_db::listens::list_summaries(
        &db,
        lyra_db::listens::ListenOwners::All,
        track_filter.as_ref().map(|filter| &filter.track_public_ids),
    )?;

    if let Some(track_id) = query.track_id.as_deref() {
        let mut rows_by_user: HashMap<String, ListenRow> = HashMap::new();
        for summary in summaries {
            if !crate::services::auth::access::entity_accessible(
                &*db,
                principal,
                summary.track_db_id,
            )? {
                continue;
            }
            let entry = rows_by_user
                .entry(summary.user_public_id.clone())
                .or_insert_with(|| ListenRow {
                    user_id: summary.user_public_id,
                    track_id: track_id.to_string(),
                    listen_count: 0,
                    last_played_ms: None,
                });
            entry.listen_count = entry.listen_count.saturating_add(summary.count);
            if let Some(last_played) = summary.last_played
                && last_played > entry.last_played_ms.unwrap_or(0)
            {
                entry.last_played_ms = Some(last_played);
            }
        }
        return Ok(rows_by_user.into_values().collect());
    }

    let mut rows = Vec::with_capacity(summaries.len());
    for summary in summaries {
        if !crate::services::auth::access::entity_accessible(&*db, principal, summary.track_db_id)?
        {
            continue;
        }
        rows.push(ListenRow {
            user_id: summary.user_public_id,
            track_id: summary.track_public_id,
            listen_count: summary.count,
            last_played_ms: summary.last_played,
        });
    }
    Ok(rows)
}

async fn hydrate_listen_rows(
    principal: &Principal,
    item_ids: &[String],
) -> Result<Vec<ListenRow>, AppError> {
    let rows = item_ids
        .iter()
        .filter_map(|item_id| parse_listen_row_id(item_id))
        .collect::<Vec<_>>();
    let db = STATE.db.read().await;
    let track_public_ids = rows
        .iter()
        .map(|row| row.track_id.as_str())
        .collect::<Vec<_>>();
    let track_db_ids = lyra_db::lookup::find_node_ids_by_ids(&db, &track_public_ids)?;
    let mut visible_users = HashMap::new();
    let mut hydrated = Vec::with_capacity(rows.len());
    for row in rows {
        let user_exists = match visible_users.get(&row.user_id).copied() {
            Some(exists) => exists,
            None => {
                let exists = lyra_db::users::get_by_public_id(&db, &row.user_id)?.is_some();
                visible_users.insert(row.user_id.clone(), exists);
                exists
            }
        };
        if !user_exists {
            continue;
        }
        let Some(track_db_id) = track_db_ids.get(&row.track_id).copied() else {
            continue;
        };
        if crate::services::auth::access::entity_accessible(&db, principal, track_db_id)? {
            hydrated.push(row);
        }
    }
    Ok(hydrated)
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
pub(super) struct ListenSubmissionRequest {
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "The plays to judge, at most 500.")
    )]
    plays: Vec<SubmittedPlayRequest>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct SubmittedPlayRequest {
    #[cfg_attr(
        feature = "docgen",
        schemars(description = listen_service::PLAY_ID_DESCRIPTION)
    )]
    play_id: String,
    track_id: String,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "When the play started, as an RFC 3339 timestamp.")
    )]
    started_at: String,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = listen_service::SUBMITTED_PLAYED_MS_DESCRIPTION)
    )]
    played_ms: Option<u64>,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = listen_service::LISTENED_DESCRIPTION)
    )]
    listened: Option<bool>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
pub(super) struct ListenSubmissionResponse {
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Each submitted play's outcome, keyed by `play_id`.")
    )]
    outcomes: BTreeMap<String, PlayOutcome>,
}

/// What the server made of one submitted play. Every outcome is final.
#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum PlayOutcome {
    /// The play counted as a listen.
    Recorded,
    /// The play already counted, live or in an earlier submission.
    Duplicate,
    /// The play was too short to count, and wasn't declared a listen.
    NotListened,
    /// The track doesn't exist or isn't visible to the caller.
    TrackNotFound,
    /// The play starts before 2010 or ends after the server's current time.
    InvalidTime,
}

impl From<SubmissionOutcome> for PlayOutcome {
    fn from(outcome: SubmissionOutcome) -> Self {
        match outcome {
            SubmissionOutcome::Judged(RecordOutcome::Recorded(_)) => Self::Recorded,
            SubmissionOutcome::Judged(RecordOutcome::Duplicate) => Self::Duplicate,
            SubmissionOutcome::Judged(RecordOutcome::NotListened) => Self::NotListened,
            SubmissionOutcome::Judged(RecordOutcome::InvalidTime) => Self::InvalidTime,
            SubmissionOutcome::TrackNotFound => Self::TrackNotFound,
        }
    }
}

fn submitted_play(request: SubmittedPlayRequest) -> Result<SubmittedPlay, AppError> {
    if !listen_service::is_play_id(&request.play_id) {
        return Err(AppError::bad_request(format!(
            "malformed play_id: {}",
            request.play_id
        )));
    }
    let started_at_ms = crate::services::timestamps::rfc3339_to_unix_ms_u64(&request.started_at)
        .ok_or_else(|| {
            AppError::bad_request(format!(
                "started_at is not an RFC 3339 timestamp: {}",
                request.started_at
            ))
        })?;
    let listened = request.listened.unwrap_or(false);
    listen_service::check_submitted_played_ms(request.played_ms, listened)
        .map_err(AppError::bad_request)?;
    Ok(SubmittedPlay {
        play_id: request.play_id,
        track: ResolveId::Nanoid(request.track_id),
        started_at_ms,
        played_ms: request.played_ms,
        listened,
    })
}

pub(super) async fn submit_me_listens(
    headers: HeaderMap,
    Json(request): Json<ListenSubmissionRequest>,
) -> Result<Json<ListenSubmissionResponse>, AppError> {
    let principal = require_principal(bearer_credential(&headers)).await?;
    if request.plays.len() > MAX_SUBMITTED_PLAYS {
        return Err(AppError::bad_request(format!(
            "plays cap exceeded: {} > {MAX_SUBMITTED_PLAYS}",
            request.plays.len()
        )));
    }
    let plays = request
        .plays
        .into_iter()
        .map(submitted_play)
        .collect::<Result<Vec<_>, _>>()?;
    let now_ms = playback_service::now_ms()?;

    let mut db = STATE.db.write().await;
    let user_db_id = principal.require(&db)?;
    let outcomes =
        listen_service::record_submitted(&mut db, &principal, user_db_id, plays, now_ms)?;
    drop(db);

    Ok(Json(ListenSubmissionResponse {
        outcomes: outcomes
            .into_iter()
            .map(|(play_id, outcome)| (play_id, outcome.into()))
            .collect(),
    }))
}

pub(super) async fn get_me_listens(
    headers: HeaderMap,
    Query(query): Query<MeListenQuery>,
) -> Result<Json<PageResponse<MeListenResponse>>, AppError> {
    let principal = require_principal(bearer_credential(&headers)).await?;
    let page_request = query.page.clone().resolve_snapshot();
    let merge_unique_external_ids = query.merge_unique_external_ids.unwrap_or(false);
    validate_merge_usage(query.track_id.as_deref(), merge_unique_external_ids)?;
    let merge_context = merge_unique_external_ids.to_string();
    let snapshot_key = SnapshotKey::builder(&principal.user_public_id, "me-listens")
        .field(query.track_id.as_deref())
        .values(query.sort_by.as_deref())
        .field(query.sort_order.as_deref())
        .field(Some(&merge_context))
        .finish();

    let (rows, next_cursor) = if let Some(page) = page_request.resume(&snapshot_key)? {
        let rows = hydrate_listen_rows(&principal, &page.item_ids).await?;
        (rows, page.next_cursor)
    } else {
        let mut rows = rows_for_user(
            &principal,
            ListenOwner::Principal,
            query.track_id.clone(),
            merge_unique_external_ids,
        )
        .await?;
        sort_listen_rows(
            &mut rows,
            query.sort_by.clone(),
            query.sort_order.clone(),
            false,
        )?;
        let page = page_request.start(&snapshot_key, rows.iter().map(listen_row_id).collect())?;
        rows.truncate(page.item_ids.len());
        (rows, page.next_cursor)
    };

    let items = rows.into_iter().map(listen_row_to_me_response).collect();
    Ok(Json(PageResponse { items, next_cursor }))
}

async fn get_listens(
    headers: HeaderMap,
    Query(query): Query<ListenQuery>,
) -> Result<Json<PageResponse<ListenResponse>>, AppError> {
    let principal = require_principal(bearer_credential(&headers)).await?;
    require_permission(&principal, Permission::ManageLibraries)?;
    let page_request = query.page.clone().resolve_snapshot();
    let merge_unique_external_ids = query.merge_unique_external_ids.unwrap_or(false);
    validate_merge_usage(query.track_id.as_deref(), merge_unique_external_ids)?;
    let merge_context = merge_unique_external_ids.to_string();
    let snapshot_key = SnapshotKey::builder(&principal.user_public_id, "listens")
        .field(query.user_id.as_deref())
        .field(query.track_id.as_deref())
        .values(query.sort_by.as_deref())
        .field(query.sort_order.as_deref())
        .field(Some(&merge_context))
        .finish();

    let (rows, next_cursor) = if let Some(page) = page_request.resume(&snapshot_key)? {
        let rows = hydrate_listen_rows(&principal, &page.item_ids).await?;
        (rows, page.next_cursor)
    } else {
        let mut rows = rows_for_managed_listens(&principal, &query).await?;
        sort_listen_rows(
            &mut rows,
            query.sort_by.clone(),
            query.sort_order.clone(),
            true,
        )?;
        let page = page_request.start(&snapshot_key, rows.iter().map(listen_row_id).collect())?;
        rows.truncate(page.item_ids.len());
        (rows, page.next_cursor)
    };

    let items = rows.into_iter().map(listen_row_to_response).collect();
    Ok(Json(PageResponse { items, next_cursor }))
}

#[cfg(feature = "docgen")]
pub(super) fn get_me_listens_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List my listen summaries").description(
        "Returns authenticated user-scoped per-track listen summaries as `{ items, next_cursor }`. Defaults to listen_count descending, then last_played_at descending. The first page creates a bounded snapshot of row identities and summary values.",
    )
}

#[cfg(feature = "docgen")]
pub(super) fn submit_me_listens_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Submit plays").description(
        "Submits up to 500 plays the client recorded without reporting them live, such as while offline. The server judges each one with the same rule as live playback: a play counts when it played at least 30 seconds, or 20% of a track under 3 minutes, measured against the catalog duration. A `play_id` counts at most once, so retrying a submission, or submitting a play that was also reported live, is safe. Each play gets an outcome keyed by its `play_id`; a `play_id` repeated in one submission is judged by its first occurrence. Every outcome is final: the client deletes the play once it has any outcome. A 400, 413, 415 or 422 means the submission itself is malformed, for example more than 500 plays, a malformed `play_id` or `started_at`, or a missing field; it judges nothing and fails again if resent unchanged, so the client fixes or drops the offending plays. Any other failure, such as a transport error, 401, 429 or 5xx, judges nothing either: the client keeps the plays and retries, after signing in again for a 401 and after waiting for a 429.",
    )
}

#[cfg(feature = "docgen")]
fn get_listens_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List listen summaries").description(
        "Requires ManageLibraries or Admin. Returns per-user per-track listen summaries for visible library content as `{ items, next_cursor }`. The first page creates a bounded snapshot; continuation pages recheck current user and track visibility.",
    )
}

pub fn listen_routes() -> Router {
    Router::new().route("/", get(get_listens))
}

#[cfg(feature = "docgen")]
pub(crate) fn listen_openapi_routes() -> aide::axum::ApiRouter {
    use aide::axum::routing::get_with;

    aide::axum::ApiRouter::new().api_route("/", get_with(get_listens, get_listens_docs))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(
        user_id: &str,
        track_id: &str,
        listen_count: u64,
        last_played_ms: Option<u64>,
    ) -> ListenRow {
        ListenRow {
            user_id: user_id.to_string(),
            track_id: track_id.to_string(),
            listen_count,
            last_played_ms,
        }
    }

    #[test]
    fn sort_listen_rows_defaults_to_top_order() {
        let mut rows = vec![
            row("user", "b", 2, Some(2_000)),
            row("user", "c", 1, Some(9_000)),
            row("user", "a", 2, Some(3_000)),
        ];

        sort_listen_rows(&mut rows, None, None, false).expect("sort should succeed");

        let ids = rows.into_iter().map(|row| row.track_id).collect::<Vec<_>>();
        assert_eq!(ids, vec!["a", "b", "c"]);
    }

    #[test]
    fn sort_listen_rows_accepts_explicit_sort_key() {
        let mut rows = vec![
            row("user", "b", 2, Some(2_000)),
            row("user", "a", 1, Some(3_000)),
            row("user", "c", 3, None),
        ];

        sort_listen_rows(
            &mut rows,
            Some(vec!["track_id".to_string()]),
            Some("ascending".to_string()),
            false,
        )
        .expect("sort should succeed");

        let ids = rows.into_iter().map(|row| row.track_id).collect::<Vec<_>>();
        assert_eq!(ids, vec!["a", "b", "c"]);
    }

    #[test]
    fn sort_listen_rows_keeps_never_played_last_in_both_directions() {
        for (direction, expected) in [
            ("ascending", ["a", "b", "c"]),
            ("descending", ["b", "a", "c"]),
        ] {
            let mut rows = vec![
                row("user", "c", 1, None),
                row("user", "b", 1, Some(2_000)),
                row("user", "a", 1, Some(1_000)),
            ];

            sort_listen_rows(
                &mut rows,
                Some(vec!["last_played_at".to_string()]),
                Some(direction.to_string()),
                false,
            )
            .expect("sort should succeed");

            let ids = rows.into_iter().map(|row| row.track_id).collect::<Vec<_>>();
            assert_eq!(ids, expected, "{direction}");
        }
    }

    #[test]
    fn sort_listen_rows_allows_user_id_for_managed_endpoint() {
        let mut rows = vec![
            row("user-b", "track", 1, Some(2_000)),
            row("user-a", "track", 1, Some(3_000)),
        ];

        sort_listen_rows(
            &mut rows,
            Some(vec!["user_id".to_string()]),
            Some("ascending".to_string()),
            true,
        )
        .expect("sort should succeed");

        let ids = rows.into_iter().map(|row| row.user_id).collect::<Vec<_>>();
        assert_eq!(ids, vec!["user-a", "user-b"]);
    }
    mod submission {
        use axum::{
            http::{
                StatusCode,
                header::AUTHORIZATION,
            },
            response::IntoResponse,
        };
        use nanoid::nanoid;

        use super::*;

        const STARTED_AT: &str = "2026-10-01T12:00:00Z";

        struct Fixture {
            headers: HeaderMap,
            user_db_id: DbId,
            long_track_id: String,
            long_track_db_id: DbId,
        }

        async fn headers_for(user_db_id: DbId) -> anyhow::Result<HeaderMap> {
            let session =
                crate::services::testing::create_session(user_db_id, Default::default()).await?;
            let mut headers = HeaderMap::new();
            headers.insert(AUTHORIZATION, format!("Bearer {}", session.token).parse()?);
            Ok(headers)
        }

        fn insert_track(db: &mut agdb::DbAny, duration_ms: u64) -> anyhow::Result<(DbId, String)> {
            let track_db_id = lyra_db::fixtures::insert_track(db, "Track")?;
            let mut track = lyra_db::tracks::get_by_id(db, track_db_id)?
                .ok_or_else(|| anyhow::anyhow!("track missing"))?;
            track.duration_ms = Some(duration_ms);
            lyra_db::tracks::update(db, &track)?;
            Ok((track_db_id, track.id))
        }

        async fn setup(admin: bool) -> anyhow::Result<Fixture> {
            crate::plugins::testing::init_library_test_state(std::path::PathBuf::from(".")).await?;
            let (user_db_id, long) = {
                let mut db = STATE.db.write().await;
                lyra_db::roles::ensure_builtin_roles(&mut db)?;
                let user = lyra_db::fixtures::test_user(&format!("listener-{}", nanoid!()))?;
                let user_db_id = lyra_db::users::create(&mut db, &user)?;
                if admin {
                    lyra_db::roles::ensure_user_has_role(
                        &mut db,
                        user_db_id,
                        lyra_db::roles::BUILTIN_ADMIN_ROLE,
                    )?;
                }
                (user_db_id, insert_track(&mut db, 300_000)?)
            };
            Ok(Fixture {
                headers: headers_for(user_db_id).await?,
                user_db_id,
                long_track_db_id: long.0,
                long_track_id: long.1,
            })
        }

        fn play(
            play_id: &str,
            track_id: &str,
            started_at: &str,
            played_ms: u64,
        ) -> SubmittedPlayRequest {
            SubmittedPlayRequest {
                play_id: play_id.to_string(),
                track_id: track_id.to_string(),
                started_at: started_at.to_string(),
                played_ms: Some(played_ms),
                listened: None,
            }
        }

        async fn submit(
            headers: &HeaderMap,
            plays: Vec<SubmittedPlayRequest>,
        ) -> Result<BTreeMap<String, PlayOutcome>, AppError> {
            let Json(response) =
                submit_me_listens(headers.clone(), Json(ListenSubmissionRequest { plays })).await?;
            Ok(response.outcomes)
        }

        async fn listen_count(fixture: &Fixture) -> anyhow::Result<u64> {
            let db = STATE.db.read().await;
            Ok(
                lyra_db::listens::get_counts(&db, &[fixture.long_track_db_id], fixture.user_db_id)?
                    .get(&fixture.long_track_db_id)
                    .copied()
                    .unwrap_or(0),
            )
        }

        fn outcomes(pairs: &[(&str, PlayOutcome)]) -> BTreeMap<String, PlayOutcome> {
            pairs
                .iter()
                .map(|(play_id, outcome)| (play_id.to_string(), *outcome))
                .collect()
        }

        #[tokio::test]
        async fn every_play_gets_its_outcome() -> anyhow::Result<()> {
            let _guard = crate::services::testing::runtime_test_lock().await;
            let fixture = setup(true).await?;
            let long = fixture.long_track_id.as_str();
            let user_public_id = {
                let db = STATE.db.read().await;
                lyra_db::users::get_by_id(&db, fixture.user_db_id)?
                    .expect("user exists")
                    .id
            };

            let result = submit(
                &fixture.headers,
                vec![
                    play("recorded-play-0001", long, STARTED_AT, 60_000),
                    play("not-listened-00001", long, STARTED_AT, 29_999),
                    play("recorded-play-0001", long, STARTED_AT, 90_000),
                    play("missing-track-0001", "missing-track-id", STARTED_AT, 60_000),
                    play("not-a-track-000001", &user_public_id, STARTED_AT, 60_000),
                    play("reset-clock-000001", long, "2009-12-31T23:59:59Z", 60_000),
                    play("future-play-000001", long, "2999-01-01T00:00:00Z", 60_000),
                ],
            )
            .await
            .map_err(|error| anyhow::anyhow!("{error:?}"))?;

            assert_eq!(
                result,
                outcomes(&[
                    ("recorded-play-0001", PlayOutcome::Recorded),
                    ("not-listened-00001", PlayOutcome::NotListened),
                    ("missing-track-0001", PlayOutcome::TrackNotFound),
                    ("not-a-track-000001", PlayOutcome::TrackNotFound),
                    ("reset-clock-000001", PlayOutcome::InvalidTime),
                    ("future-play-000001", PlayOutcome::InvalidTime),
                ])
            );
            assert_eq!(listen_count(&fixture).await?, 1);
            let db = STATE.db.read().await;
            let listen_db_id =
                lyra_db::listens::find_by_play_id(&db, fixture.user_db_id, "recorded-play-0001")?
                    .expect("recorded listen");
            let listen = lyra_db::graph::bulk_fetch_typed::<lyra_db::Listen>(
                &db,
                vec![listen_db_id],
                "Listen",
            )?
            .remove(&listen_db_id)
            .expect("listen exists");
            assert_eq!(
                Some(listen.started_at_ms),
                crate::services::timestamps::rfc3339_to_unix_ms_u64(STARTED_AT)
            );
            assert_eq!(
                listen.played_ms,
                Some(60_000),
                "a repeat in the batch changes nothing"
            );
            Ok(())
        }

        #[tokio::test]
        async fn a_retried_submission_counts_each_play_once() -> anyhow::Result<()> {
            let _guard = crate::services::testing::runtime_test_lock().await;
            let fixture = setup(true).await?;
            let plays = || {
                vec![
                    play(
                        "retried-play-00001",
                        &fixture.long_track_id,
                        STARTED_AT,
                        60_000,
                    ),
                    play(
                        "retried-short-0001",
                        &fixture.long_track_id,
                        STARTED_AT,
                        5_000,
                    ),
                ]
            };

            let first = submit(&fixture.headers, plays())
                .await
                .map_err(|error| anyhow::anyhow!("{error:?}"))?;
            let retry = submit(&fixture.headers, plays())
                .await
                .map_err(|error| anyhow::anyhow!("{error:?}"))?;

            assert_eq!(first["retried-play-00001"], PlayOutcome::Recorded);
            assert_eq!(retry["retried-play-00001"], PlayOutcome::Duplicate);
            assert_eq!(retry["retried-short-0001"], PlayOutcome::NotListened);
            assert_eq!(listen_count(&fixture).await?, 1);
            Ok(())
        }

        #[tokio::test]
        async fn a_submission_announces_its_listens_as_one_update() -> anyhow::Result<()> {
            let _guard = crate::services::testing::runtime_test_lock().await;
            let fixture = setup(true).await?;
            let dispatch = crate::services::testing::record_plugin_dispatch();
            let long = fixture.long_track_id.as_str();
            let plays = || {
                vec![
                    play("announced-one-0001", long, STARTED_AT, 60_000),
                    play("announced-two-0001", long, STARTED_AT, 90_000),
                    play("announced-short-01", long, STARTED_AT, 5_000),
                ]
            };

            submit(&fixture.headers, plays())
                .await
                .map_err(|error| anyhow::anyhow!("{error:?}"))?;
            submit(&fixture.headers, plays())
                .await
                .map_err(|error| anyhow::anyhow!("{error:?}"))?;

            let updates = dispatch.listen_updates();
            assert_eq!(updates.len(), 1, "a retry announces nothing: {updates:?}");
            let played = updates[0]
                .listens
                .iter()
                .map(|listen| listen.played_ms)
                .collect::<Vec<_>>();
            assert_eq!(
                played,
                vec![Some(60_000), Some(90_000)],
                "listens keep submission order"
            );
            assert!(
                updates[0]
                    .listens
                    .iter()
                    .all(|listen| listen.track_id == long)
            );
            Ok(())
        }

        #[tokio::test]
        async fn an_inaccessible_track_reads_as_missing() -> anyhow::Result<()> {
            let _guard = crate::services::testing::runtime_test_lock().await;
            let fixture = setup(false).await?;

            let result = submit(
                &fixture.headers,
                vec![play(
                    "hidden-track-00001",
                    &fixture.long_track_id,
                    STARTED_AT,
                    60_000,
                )],
            )
            .await
            .map_err(|error| anyhow::anyhow!("{error:?}"))?;

            assert_eq!(
                result,
                outcomes(&[("hidden-track-00001", PlayOutcome::TrackNotFound)])
            );
            assert_eq!(listen_count(&fixture).await?, 0);
            Ok(())
        }

        #[tokio::test]
        async fn a_declared_listen_counts_without_playing_time() -> anyhow::Result<()> {
            let _guard = crate::services::testing::runtime_test_lock().await;
            let fixture = setup(true).await?;
            let declared = SubmittedPlayRequest {
                played_ms: None,
                listened: Some(true),
                ..play("declared-play-0001", &fixture.long_track_id, STARTED_AT, 0)
            };

            let result = submit(&fixture.headers, vec![declared])
                .await
                .map_err(|error| anyhow::anyhow!("{error:?}"))?;

            assert_eq!(
                result,
                outcomes(&[("declared-play-0001", PlayOutcome::Recorded)])
            );
            assert_eq!(listen_count(&fixture).await?, 1);
            Ok(())
        }

        #[tokio::test]
        async fn malformed_submissions_judge_nothing() -> anyhow::Result<()> {
            let _guard = crate::services::testing::runtime_test_lock().await;
            let fixture = setup(true).await?;
            let long = fixture.long_track_id.as_str();
            let valid = || play("valid-play-0000001", long, STARTED_AT, 60_000);

            let too_many = (0..=MAX_SUBMITTED_PLAYS)
                .map(|index| play(&format!("capped-play-{index:06}"), long, STARTED_AT, 60_000))
                .collect();
            for plays in [
                too_many,
                vec![valid(), play("short", long, STARTED_AT, 60_000)],
                vec![
                    valid(),
                    play("bad-timestamp-0001", long, "yesterday", 60_000),
                ],
                vec![
                    valid(),
                    SubmittedPlayRequest {
                        played_ms: None,
                        ..play("untimed-play-00001", long, STARTED_AT, 0)
                    },
                ],
            ] {
                let error = submit(&fixture.headers, plays)
                    .await
                    .expect_err("a malformed submission is rejected");
                assert_eq!(error.into_response().status(), StatusCode::BAD_REQUEST);
            }
            assert_eq!(listen_count(&fixture).await?, 0);

            let at_cap = (0..MAX_SUBMITTED_PLAYS)
                .map(|index| play(&format!("capped-play-{index:06}"), long, STARTED_AT, 60_000))
                .collect();
            let result = submit(&fixture.headers, at_cap)
                .await
                .map_err(|error| anyhow::anyhow!("{error:?}"))?;
            assert_eq!(result.len(), MAX_SUBMITTED_PLAYS);
            Ok(())
        }
    }
}
