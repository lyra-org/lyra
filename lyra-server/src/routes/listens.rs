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
use crate::services::catalog::{
    self,
    Direction,
    tracks::Tracks,
};
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
            accounts,
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
                let user = accounts::get_by_public_id(db, user_id)?
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
    summary_rows(&db, principal, user_db_id)
}

/// Keeps the summaries of tracks `principal` can see, as the catalog lists tracks for it,
/// whether or not they can play.
fn retain_visible(
    db: &DbAny,
    principal: &Principal,
    summaries: &mut Vec<lyra_db::listens::ListenSummary>,
) -> Result<(), AppError> {
    let viewer = catalog::Viewer::user(db, principal.clone())?;
    if viewer.sees_everything() {
        return Ok(());
    }
    let tracks = summaries
        .iter()
        .map(|summary| summary.track_db_id)
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let visible = catalog::lookups::visible::<Tracks>(db, &viewer, &tracks)?;
    summaries.retain(|summary| visible.contains(&summary.track_db_id));
    Ok(())
}

/// A row for each track `user_db_id` has listened to that `principal` can see.
fn summary_rows(
    db: &DbAny,
    principal: &Principal,
    user_db_id: DbId,
) -> Result<Vec<ListenRow>, AppError> {
    let mut summaries = lyra_db::listens::list_summaries_for_user(db, user_db_id)?;
    retain_visible(db, principal, &mut summaries)?;
    Ok(summaries
        .into_iter()
        .map(|summary| ListenRow {
            user_id: summary.user_public_id,
            track_id: summary.track_public_id,
            listen_count: summary.count,
            last_played_ms: summary.last_played,
        })
        .collect())
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
    let account_ids = accounts::list(&*db)?
        .into_iter()
        .map(|user| user.id)
        .collect::<HashSet<_>>();
    let mut summaries = lyra_db::listens::list_summaries(
        &db,
        lyra_db::listens::ListenOwners::All,
        track_filter.as_ref().map(|filter| &filter.track_public_ids),
    )?;
    summaries.retain(|summary| account_ids.contains(&summary.user_public_id));
    retain_visible(&db, principal, &mut summaries)?;

    if let Some(track_id) = query.track_id.as_deref() {
        let mut rows_by_user: HashMap<String, ListenRow> = HashMap::new();
        for summary in summaries {
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

    Ok(summaries
        .into_iter()
        .map(|summary| ListenRow {
            user_id: summary.user_public_id,
            track_id: summary.track_public_id,
            listen_count: summary.count,
            last_played_ms: summary.last_played,
        })
        .collect())
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
    let page_request = query.page.resolve();
    let merge_unique_external_ids = query.merge_unique_external_ids.unwrap_or(false);
    validate_merge_usage(query.track_id.as_deref(), merge_unique_external_ids)?;

    let mut rows = rows_for_user(
        &principal,
        ListenOwner::Principal,
        query.track_id,
        merge_unique_external_ids,
    )
    .await?;
    sort_listen_rows(&mut rows, query.sort_by, query.sort_order, false)?;
    Ok(Json(
        page_request.slice(rows).map(listen_row_to_me_response),
    ))
}

async fn get_listens(
    headers: HeaderMap,
    Query(query): Query<ListenQuery>,
) -> Result<Json<PageResponse<ListenResponse>>, AppError> {
    let principal = require_principal(bearer_credential(&headers)).await?;
    require_permission(&principal, Permission::ManageLibraries)?;
    let page_request = query.page.clone().resolve();

    let mut rows = rows_for_managed_listens(&principal, &query).await?;
    sort_listen_rows(&mut rows, query.sort_by, query.sort_order, true)?;
    Ok(Json(page_request.slice(rows).map(listen_row_to_response)))
}

#[cfg(feature = "docgen")]
pub(super) fn get_me_listens_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List my listen summaries").description(
        "Returns authenticated user-scoped per-track listen summaries as `{ items, total, offset }`. Defaults to listen_count descending, then last_played_at descending. `offset` and `limit` page through the summaries as they are at each request; `limit` defaults to 100 and is capped at 500.",
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
        "Requires ManageLibraries or Admin. Returns per-user per-track listen summaries for visible library content as `{ items, total, offset }`. The default user's listens are left out, and `user_id` naming it finds no user, while authentication is enabled. `offset` and `limit` page through the summaries as they are at each request; `limit` defaults to 100 and is capped at 500.",
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
    /// A release holding one track, in `library`, with one listen of the track by `user`.
    fn listened_track(
        db: &mut DbAny,
        user: DbId,
        library: DbId,
        title: &str,
    ) -> anyhow::Result<String> {
        let release = lyra_db::fixtures::insert_release(db, title)?;
        let track = lyra_db::fixtures::insert_track(db, title)?;
        lyra_db::fixtures::connect(db, library, release)?;
        lyra_db::fixtures::connect(db, release, track)?;
        lyra_db::fixtures::insert_listen(db, user, track, 1_700_000_000_000)?;
        lyra_db::lookup::find_id_by_db_id(db, track)?
            .ok_or_else(|| anyhow::anyhow!("track has no public id"))
    }

    fn track_ids(rows: Vec<ListenRow>) -> HashSet<String> {
        rows.into_iter().map(|row| row.track_id).collect()
    }

    #[test]
    fn summary_rows_hold_only_tracks_the_catalog_shows_the_viewer() -> anyhow::Result<()> {
        use axum::response::IntoResponse;

        let mut db = lyra_db::test_db::new_test_db()?;
        let user = lyra_db::test_db::insert_user(&mut db, "limited-listener")?;
        let shown = lyra_db::test_db::insert_library(&mut db, "Shown", "/listens/shown")?;
        let hidden = lyra_db::test_db::insert_library(&mut db, "Hidden", "/listens/hidden")?;
        let seen = listened_track(&mut db, user, shown, "Seen")?;
        let unseen = listened_track(&mut db, user, hidden, "Unseen")?;
        let orphan = lyra_db::fixtures::insert_track(&mut db, "Orphan")?;
        lyra_db::fixtures::insert_listen(&mut db, user, orphan, 1_700_000_000_000)?;
        let orphan = lyra_db::lookup::find_id_by_db_id(&db, orphan)?.expect("track exists");

        let shown_id = lyra_db::libraries::get_by_id(&db, shown)?
            .expect("library exists")
            .id;
        let listed = |db: &DbAny, permissions, libraries| {
            let principal = Principal::for_user(db, user, permissions, libraries);
            summary_rows(db, &principal, user).map(track_ids)
        };
        let ok = |listed: Result<HashSet<String>, AppError>| {
            listed.map_err(|error| anyhow::anyhow!("{error:?}"))
        };

        assert_eq!(
            ok(listed(&db, Vec::new(), HashSet::from([shown_id.clone()])))?,
            HashSet::from([seen.clone()])
        );
        assert!(ok(listed(&db, Vec::new(), HashSet::new()))?.is_empty());
        assert_eq!(
            ok(listed(&db, vec![Permission::Admin], HashSet::new()))?,
            HashSet::from([seen, unseen, orphan])
        );

        // A caller who no longer exists is refused, as the catalog lists refuse it.
        let caller = lyra_db::test_db::insert_user(&mut db, "deleted-caller")?;
        let principal = Principal::for_user(&db, caller, vec![Permission::Admin], HashSet::new());
        lyra_db::test_db::recycle_user(&mut db, caller, "replacement")?;
        let Err(error) = summary_rows(&db, &principal, user) else {
            anyhow::bail!("a deleted caller listed listens");
        };
        assert_eq!(
            error.into_response().status(),
            axum::http::StatusCode::UNAUTHORIZED
        );
        Ok(())
    }

    #[tokio::test]
    async fn all_users_listens_hold_only_tracks_the_catalog_shows_the_caller() -> anyhow::Result<()>
    {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::plugins::testing::init_library_test_state(std::path::PathBuf::from(".")).await?;
        let (manager, admin, seen, unseen) = {
            let mut db = STATE.db.write().await;
            let listener = lyra_db::test_db::insert_user(&mut db, "all-users-listener")?;
            let caller = lyra_db::test_db::insert_user(&mut db, "all-users-manager")?;
            let shown = lyra_db::test_db::insert_library(&mut db, "Shown", "/all-listens/shown")?;
            let hidden =
                lyra_db::test_db::insert_library(&mut db, "Hidden", "/all-listens/hidden")?;
            let seen = listened_track(&mut db, listener, shown, "Seen")?;
            let unseen = listened_track(&mut db, listener, hidden, "Unseen")?;
            let shown_id = lyra_db::libraries::get_by_id(&db, shown)?
                .expect("library exists")
                .id;
            let principal = |permission, libraries| {
                Principal::for_user(&*db, caller, vec![permission], libraries)
            };
            (
                principal(Permission::ManageLibraries, HashSet::from([shown_id])),
                principal(Permission::Admin, HashSet::new()),
                seen,
                unseen,
            )
        };
        let listed = async |principal: &Principal, track_id: Option<&String>| {
            let query = ListenQuery {
                user_id: None,
                track_id: track_id.cloned(),
                page: routes::PageQuery::default(),
                sort_by: None,
                sort_order: None,
                merge_unique_external_ids: None,
            };
            rows_for_managed_listens(principal, &query)
                .await
                .map(track_ids)
        };
        let both = HashSet::from([seen.clone(), unseen.clone()]);

        assert_eq!(
            listed(&manager, None).await.ok(),
            Some(HashSet::from([seen.clone()]))
        );
        assert_eq!(
            listed(&manager, Some(&seen)).await.ok(),
            Some(HashSet::from([seen]))
        );
        assert!(listed(&manager, Some(&unseen)).await.is_err());
        assert_eq!(listed(&admin, None).await.ok(), Some(both));
        assert_eq!(
            listed(&admin, Some(&unseen)).await.ok(),
            Some(HashSet::from([unseen]))
        );
        Ok(())
    }

    mod default_user {
        use axum::{
            http::{
                StatusCode,
                header::AUTHORIZATION,
            },
            response::IntoResponse,
        };
        use lyra_db::users::DEFAULT_USERNAME;

        use super::*;

        struct Fixture {
            headers: HeaderMap,
            admin_id: String,
            default_id: String,
            track_id: String,
        }

        fn set_auth_enabled(enabled: bool) {
            let mut config = STATE.config().as_ref().clone();
            config.auth.enabled = enabled;
            crate::services::testing::publish_config(config);
        }

        async fn submit_listen(headers: &HeaderMap, track_id: &str) -> anyhow::Result<()> {
            let _ = submit_me_listens(
                headers.clone(),
                Json(ListenSubmissionRequest {
                    plays: vec![SubmittedPlayRequest {
                        play_id: lyra_db::nanoid(),
                        track_id: track_id.to_string(),
                        started_at: "2026-10-01T12:00:00Z".to_string(),
                        played_ms: None,
                        listened: Some(true),
                    }],
                }),
            )
            .await
            .map_err(|error| anyhow::anyhow!("{error:?}"))?;
            Ok(())
        }

        /// An admin and the default user, each with a listen of the one track.
        async fn setup() -> anyhow::Result<Fixture> {
            crate::plugins::testing::init_library_test_state(std::path::PathBuf::from(".")).await?;
            crate::services::auth::ensure_default_user(&STATE.config()).await?;
            let (admin_db_id, admin_id, default_id, track_id) = {
                let mut db = STATE.db.write().await;
                let admin = lyra_db::fixtures::test_user("listening-admin")?;
                let admin_id = admin.id.clone();
                let admin_db_id = lyra_db::users::create(&mut db, &admin)?;
                lyra_db::roles::ensure_user_has_role(
                    &mut db,
                    admin_db_id,
                    lyra_db::roles::BUILTIN_ADMIN_ROLE,
                )?;
                let default_id = lyra_db::users::get_by_username(&db, DEFAULT_USERNAME)?
                    .expect("default user exists")
                    .id;
                let track_db_id = lyra_db::fixtures::insert_track(&mut db, "Track")?;
                let track_id = lyra_db::tracks::get_by_id(&db, track_db_id)?
                    .expect("track exists")
                    .id;
                (admin_db_id, admin_id, default_id, track_id)
            };
            let session =
                crate::services::testing::create_session(admin_db_id, Default::default()).await?;
            let mut headers = HeaderMap::new();
            headers.insert(AUTHORIZATION, format!("Bearer {}", session.token).parse()?);

            submit_listen(&headers, &track_id).await?;
            set_auth_enabled(false);
            submit_listen(&HeaderMap::new(), &track_id).await?;
            set_auth_enabled(true);

            Ok(Fixture {
                headers,
                admin_id,
                default_id,
                track_id,
            })
        }

        /// The users with a row in the listing, sorted, or the refusal's status.
        async fn listed_users(
            fixture: &Fixture,
            user_id: Option<&str>,
            track_id: Option<&str>,
        ) -> Result<Vec<String>, StatusCode> {
            let Json(page) = get_listens(
                fixture.headers.clone(),
                Query(ListenQuery {
                    user_id: user_id.map(str::to_string),
                    track_id: track_id.map(str::to_string),
                    page: routes::PageQuery::default(),
                    sort_by: None,
                    sort_order: None,
                    merge_unique_external_ids: None,
                }),
            )
            .await
            .map_err(|error| error.into_response().status())?;
            let mut users = page
                .items
                .into_iter()
                .map(|item| item.user_id)
                .collect::<Vec<_>>();
            users.sort();
            Ok(users)
        }

        fn sorted(mut ids: Vec<String>) -> Vec<String> {
            ids.sort();
            ids
        }

        #[tokio::test]
        async fn all_users_listens_show_it_only_while_auth_is_off() -> anyhow::Result<()> {
            let _guard = crate::services::testing::runtime_test_lock().await;
            let fixture = setup().await?;
            let track = Some(fixture.track_id.as_str());

            let only_admin = vec![fixture.admin_id.clone()];
            assert_eq!(
                listed_users(&fixture, None, None).await,
                Ok(only_admin.clone())
            );
            assert_eq!(listed_users(&fixture, None, track).await, Ok(only_admin));

            set_auth_enabled(false);
            let both = sorted(vec![fixture.admin_id.clone(), fixture.default_id.clone()]);
            assert_eq!(listed_users(&fixture, None, None).await, Ok(both.clone()));
            assert_eq!(listed_users(&fixture, None, track).await, Ok(both));
            Ok(())
        }

        #[tokio::test]
        async fn naming_it_finds_no_user_only_while_auth_is_on() -> anyhow::Result<()> {
            let _guard = crate::services::testing::runtime_test_lock().await;
            let fixture = setup().await?;
            let default_id = Some(fixture.default_id.as_str());
            let track = Some(fixture.track_id.as_str());

            for track_id in [None, track] {
                assert_eq!(
                    listed_users(&fixture, default_id, track_id).await,
                    Err(StatusCode::NOT_FOUND)
                );
            }

            set_auth_enabled(false);
            for track_id in [None, track] {
                assert_eq!(
                    listed_users(&fixture, default_id, track_id).await,
                    Ok(vec![fixture.default_id.clone()])
                );
            }
            Ok(())
        }

        #[tokio::test]
        async fn pages_count_it_only_while_auth_is_off() -> anyhow::Result<()> {
            let _guard = crate::services::testing::runtime_test_lock().await;
            let fixture = setup().await?;
            let page = async |offset: u64| {
                get_listens(
                    fixture.headers.clone(),
                    Query(ListenQuery {
                        user_id: None,
                        track_id: None,
                        page: routes::PageQuery {
                            limit: Some(1),
                            offset: Some(offset),
                        },
                        sort_by: Some(vec!["user_id".to_string()]),
                        sort_order: None,
                        merge_unique_external_ids: None,
                    }),
                )
                .await
                .map(|Json(page)| page)
                .map_err(|error| anyhow::anyhow!("{error:?}"))
            };

            let first = page(0).await?;
            assert_eq!(first.total, 1);
            assert_eq!(first.items[0].user_id, fixture.admin_id);
            assert!(page(1).await?.items.is_empty());

            set_auth_enabled(false);
            let mut users = Vec::new();
            for offset in [0, 1] {
                let page = page(offset).await?;
                assert_eq!(page.total, 2);
                assert_eq!(page.offset, offset);
                users.extend(page.items.into_iter().map(|item| item.user_id));
            }
            assert_eq!(
                users,
                sorted(vec![fixture.admin_id.clone(), fixture.default_id.clone()])
            );
            let past = page(5).await?;
            assert!(past.items.is_empty());
            assert_eq!((past.total, past.offset), (2, 2));
            Ok(())
        }
    }

    mod submission {
        use axum::{
            http::{
                StatusCode,
                header::AUTHORIZATION,
            },
            response::IntoResponse,
        };
        use lyra_db::nanoid;

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
                let user = lyra_db::fixtures::test_user(&format!("listener-{}", nanoid()))?;
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

/// A report of what a page of the caller's listens costs with a large history. Run it with
/// `cargo +nightly test --release -p lyra-server --features nightly --lib -- routes::listens::benches::report --ignored --nocapture`.
#[cfg(all(test, feature = "nightly"))]
mod benches {
    use super::*;
    use crate::services::catalog::benches::{
        median,
        ms,
        search_fixture,
    };

    const LIMIT: u64 = 100;

    #[tokio::test]
    #[ignore = "a report for release builds"]
    async fn report() {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::plugins::testing::init_library_test_state(std::path::PathBuf::from("."))
            .await
            .unwrap();
        let count = |name: &str, default: usize| {
            std::env::var(name)
                .ok()
                .and_then(|count| count.parse().ok())
                .unwrap_or(default)
        };
        let track_count = count("CATALOG_REPORT_TRACKS", 100_000);
        let listened = count("LISTENS_REPORT_TRACKS", 20_000).min(track_count);
        let (mut db, principal) = search_fixture(track_count, 1, false);
        let user_db_id = principal.require(&db).unwrap();
        let tracks = lyra_db::roots::member_ids(&db, lyra_db::roots::Root::Tracks).unwrap();
        let step = (tracks.len() / listened).max(1);
        let mut listens = 0_u64;
        for (index, track) in tracks.into_iter().step_by(step).take(listened).enumerate() {
            for play in 0..=index % 5 {
                lyra_db::fixtures::insert_listen(
                    &mut db,
                    user_db_id,
                    track,
                    1_700_000_000_000 + listens * 200_000 + play as u64,
                )
                .unwrap();
                listens += 1;
            }
        }
        // The rows read the catalog index, which only the server's database keeps between
        // requests.
        *STATE.db.write().await = db;
        let db = STATE.db.read().await;

        let page = |offset: u64| {
            let mut rows = summary_rows(&db, &principal, user_db_id).unwrap();
            sort_listen_rows(&mut rows, None, None, false).unwrap();
            let page = routes::PageQuery {
                limit: Some(LIMIT),
                offset: Some(offset),
            }
            .resolve()
            .slice(rows);
            (page.items.len() as u64, page.total)
        };
        println!(
            "{track_count} tracks, one-library user with {listens} listens of {listened} tracks, \
             pages of {LIMIT}"
        );
        for offset in [0, listened as u64 / 2] {
            let (items, total) = page(offset);
            assert_eq!(items, LIMIT);
            println!(
                "  listens page at offset {offset} of {total}: {}",
                ms(median(9, || page(offset).0))
            );
        }
        println!(
            "    reading the summaries and keeping the visible ones: {}",
            ms(median(9, || {
                summary_rows(&db, &principal, user_db_id).unwrap().len() as u64
            }))
        );
        println!(
            "    reading the summaries: {}",
            ms(median(9, || {
                lyra_db::listens::list_summaries_for_user(&db, user_db_id)
                    .unwrap()
                    .len() as u64
            }))
        );
    }
}
