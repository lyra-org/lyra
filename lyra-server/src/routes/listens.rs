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
use std::collections::{
    BTreeMap,
    HashMap,
    HashSet,
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
            summaries::{
                self,
                SortKey,
                Summary,
            },
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
            description = "Comma-separated or repeated values: listen_count, count, last_played_at, last_played."
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
            description = "Comma-separated or repeated values: listen_count, count, last_played_at, last_played, user_id."
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

fn parse_sort_order(sort_order: Option<String>) -> Result<Option<Direction>, AppError> {
    Ok(sort_order.as_deref().map(Direction::parse).transpose()?)
}

fn parse_sort_keys(
    sort_by: Option<Vec<String>>,
    include_user_id: bool,
) -> Result<Vec<SortKey>, AppError> {
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
                "listen_count" | "count" => SortKey::ListenCount,
                "last_played_at" | "last_played" => SortKey::LastPlayedAt,
                "user_id" if include_user_id => SortKey::UserId,
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
            if include_user_id {
                "listen_count, count, last_played_at, last_played, user_id"
            } else {
                "listen_count, count, last_played_at, last_played"
            },
        )));
    }

    Ok(keys)
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

/// The track a list is narrowed to and the tracks whose listens count as its own, resolved
/// under the guard that reads listens with them.
fn track_filter(
    db: &DbAny,
    principal: &Principal,
    track_id: &str,
    merge_pairs: Option<&HashSet<(String, String)>>,
) -> Result<(DbId, Vec<DbId>), AppError> {
    let track_db_id = lyra_db::lookup::find_node_id_by_id(db, track_id)?
        .ok_or_else(|| AppError::not_found(format!("Track not found: {}", track_id)))?;
    crate::services::auth::access::require_entity_accessible(db, principal, track_db_id, || {
        AppError::not_found(format!("Track not found: {}", track_id))
    })?;
    let counted = summaries::counted_tracks(db, principal, track_db_id, merge_pairs)?;
    Ok((track_db_id, counted))
}

/// The summaries of one user's listens that `principal` may list.
fn user_summaries(
    db: &DbAny,
    principal: &Principal,
    user_db_id: DbId,
    track_id: Option<&str>,
    merge_pairs: Option<&HashSet<(String, String)>>,
) -> Result<Vec<Summary>, AppError> {
    let Some(track_id) = track_id else {
        return Ok(summaries::of_users(db, principal, &[user_db_id])?);
    };
    let (track_db_id, counted) = track_filter(db, principal, track_id, merge_pairs)?;
    Ok(vec![summaries::of_track(
        db,
        user_db_id,
        track_db_id,
        &counted,
    )?])
}

/// The summaries the all-users list holds for `query`, with the public id of each user in them.
fn managed_summaries(
    db: &DbAny,
    principal: &Principal,
    query: &ListenQuery,
    merge_pairs: Option<&HashSet<(String, String)>>,
) -> Result<(Vec<Summary>, HashMap<DbId, String>), AppError> {
    let track_id = query.track_id.as_deref();
    if let Some(user_id) = query.user_id.as_deref() {
        // The track is resolved first, as the caller's own list resolves it.
        let track = track_id
            .map(|track_id| track_filter(db, principal, track_id, merge_pairs))
            .transpose()?;
        let user = accounts::get_by_public_id(db, user_id)?
            .ok_or_else(|| AppError::not_found(format!("User not found: {user_id}")))?;
        let user_db_id = user
            .db_id
            .ok_or_else(|| anyhow::anyhow!("user missing db_id"))?;
        let summaries = match track {
            Some((track_db_id, counted)) => {
                vec![summaries::of_track(db, user_db_id, track_db_id, &counted)?]
            }
            None => summaries::of_users(db, principal, &[user_db_id])?,
        };
        return Ok((summaries, HashMap::from([(user_db_id, user.id)])));
    }

    let user_ids = accounts::list(db)?
        .into_iter()
        .filter_map(|user| Some((user.db_id?, user.id)))
        .collect::<HashMap<_, _>>();
    let users = user_ids.keys().copied().collect::<Vec<_>>();
    let summaries = match track_id {
        Some(track_id) => {
            let (track_db_id, counted) = track_filter(db, principal, track_id, merge_pairs)?;
            summaries::of_track_by_users(db, principal, &users, track_db_id, &counted)?
        }
        None => summaries::of_users(db, principal, &users)?,
    };
    Ok((summaries, user_ids))
}

/// The page's summaries, each with the public id of its track.
fn with_track_ids(
    db: &DbAny,
    page: PageResponse<Summary>,
) -> Result<PageResponse<(Summary, String)>, AppError> {
    page.try_map_items(|summaries| {
        let tracks = summaries
            .iter()
            .map(|summary| summary.track_db_id)
            .collect::<Vec<_>>();
        let track_ids = lyra_db::lookup::find_ids_by_db_ids(db, &tracks)?;
        summaries
            .into_iter()
            .map(|summary| {
                let track_id = track_ids
                    .get(&summary.track_db_id)
                    .cloned()
                    .ok_or_else(|| anyhow::anyhow!("listened track has no public id"))?;
                Ok((summary, track_id))
            })
            .collect::<Result<Vec<_>, AppError>>()
    })
}

fn last_played_at(summary: &Summary) -> Option<String> {
    summary
        .stats
        .last_played_at_ms
        .map(crate::services::timestamps::unix_ms_to_rfc3339_u64)
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
    let keys = parse_sort_keys(query.sort_by, false)?;
    let direction = parse_sort_order(query.sort_order)?;
    let merge_pairs = merge_track_id_pairs(merge_unique_external_ids).await;

    let db = STATE.db.read().await;
    let user_db_id = principal.require(&db)?;
    let mut summaries = user_summaries(
        &db,
        &principal,
        user_db_id,
        query.track_id.as_deref(),
        merge_pairs.as_ref(),
    )?;
    summaries::sort(&mut summaries, &keys, direction, &HashMap::new());
    let page = with_track_ids(&db, page_request.slice(summaries))?;
    Ok(Json(page.map(|(summary, track_id)| MeListenResponse {
        track_id,
        listen_count: summary.stats.count,
        last_played_at: last_played_at(&summary),
    })))
}

async fn get_listens(
    headers: HeaderMap,
    Query(query): Query<ListenQuery>,
) -> Result<Json<PageResponse<ListenResponse>>, AppError> {
    let principal = require_principal(bearer_credential(&headers)).await?;
    require_permission(&principal, Permission::ManageLibraries)?;
    let page_request = query.page.clone().resolve();
    let merge_unique_external_ids = query.merge_unique_external_ids.unwrap_or(false);
    validate_merge_usage(query.track_id.as_deref(), merge_unique_external_ids)?;
    let keys = parse_sort_keys(query.sort_by.clone(), true)?;
    let direction = parse_sort_order(query.sort_order.clone())?;
    let merge_pairs = merge_track_id_pairs(merge_unique_external_ids).await;

    let db = STATE.db.read().await;
    let (mut summaries, user_ids) =
        managed_summaries(&db, &principal, &query, merge_pairs.as_ref())?;
    summaries::sort(&mut summaries, &keys, direction, &user_ids);
    let page = with_track_ids(&db, page_request.slice(summaries))?;
    page.try_map_items(|items| {
        items
            .into_iter()
            .map(|(summary, track_id)| {
                Ok(ListenResponse {
                    user_id: user_ids
                        .get(&summary.user_db_id)
                        .cloned()
                        .ok_or_else(|| anyhow::anyhow!("listener has no public id"))?,
                    track_id,
                    listen_count: summary.stats.count,
                    last_played_at: last_played_at(&summary),
                })
            })
            .collect::<Result<Vec<_>, AppError>>()
    })
    .map(Json)
}

#[cfg(feature = "docgen")]
pub(super) fn get_me_listens_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List my listen summaries").description(
        "Returns authenticated user-scoped per-track listen summaries as `{ items, total, offset }`. `sort_by` takes `listen_count` and `last_played_at`, ascending unless `sort_order` says otherwise; without it, the summaries come by `listen_count` then `last_played_at`, descending unless `sort_order` says otherwise. Summaries equal on those keys come by the track's internal id, a stable order unrelated to its public id. `offset` and `limit` page through the summaries as they are at each request; `limit` defaults to 100 and is capped at 500.",
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
        "Requires ManageLibraries or Admin. Returns per-user per-track listen summaries for visible library content as `{ items, total, offset }`. The default user's listens are left out, and `user_id` naming it finds no user, while authentication is enabled. `sort_by` takes `listen_count`, `last_played_at` and `user_id`, ascending unless `sort_order` says otherwise; without it, the summaries come by `listen_count` then `last_played_at`, descending unless `sort_order` says otherwise. Summaries equal on those keys come by the user's internal id, then the track's, a stable order unrelated to the public ids. `offset` and `limit` page through the summaries as they are at each request; `limit` defaults to 100 and is capped at 500.",
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

    #[test]
    fn the_track_id_sort_key_is_gone() {
        for include_user_id in [false, true] {
            for key in ["track_id", "id"] {
                assert!(parse_sort_keys(Some(vec![key.to_string()]), include_user_id).is_err());
            }
        }
        assert!(parse_sort_keys(Some(vec!["user_id".to_string()]), false).is_err());
        assert_eq!(
            parse_sort_keys(Some(vec!["count,user_id".to_string()]), true).ok(),
            Some(vec![SortKey::ListenCount, SortKey::UserId])
        );
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
            let db = STATE.db.read().await;
            let (summaries, _) = managed_summaries(&db, principal, &query, None)?;
            let tracks = summaries
                .iter()
                .map(|summary| summary.track_db_id)
                .collect::<Vec<_>>();
            Ok::<_, AppError>(
                lyra_db::lookup::find_ids_by_db_ids(&*db, &tracks)?
                    .into_values()
                    .collect::<HashSet<_>>(),
            )
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

    mod paging {
        use axum::http::header::AUTHORIZATION;

        use super::*;

        struct Fixture {
            headers: HeaderMap,
            admin: (DbId, String),
            other: (DbId, String),
            /// Each track's internal and public id, by internal id.
            tracks: Vec<(DbId, String)>,
        }

        /// An admin with two listens of the last of five tracks and one of each other, all
        /// last played at the same time, and another user with one of the first two.
        async fn setup() -> anyhow::Result<Fixture> {
            crate::plugins::testing::init_library_test_state(std::path::PathBuf::from(".")).await?;
            let mut db = STATE.db.write().await;
            lyra_db::roles::ensure_builtin_roles(&mut db)?;
            let mut user = |name: &str| -> anyhow::Result<(DbId, String)> {
                let user = lyra_db::fixtures::test_user(name)?;
                Ok((lyra_db::users::create(&mut db, &user)?, user.id))
            };
            let admin = user("paging-admin")?;
            let other = user("paging-other")?;
            lyra_db::roles::ensure_user_has_role(
                &mut db,
                admin.0,
                lyra_db::roles::BUILTIN_ADMIN_ROLE,
            )?;
            let mut tracks = Vec::new();
            for title in ["A", "B", "C", "D", "E"] {
                let track = lyra_db::fixtures::insert_track(&mut db, title)?;
                let id = lyra_db::lookup::find_id_by_db_id(&*db, track)?.expect("public id");
                tracks.push((track, id));
            }
            tracks.sort();
            // Listened to in another order than the ids', so neither order passes for the other.
            for index in [3, 0, 4, 2, 1, 4] {
                lyra_db::fixtures::insert_listen(&mut db, admin.0, tracks[index].0, 5_000)?;
            }
            for index in [1, 0] {
                lyra_db::fixtures::insert_listen(&mut db, other.0, tracks[index].0, 5_000)?;
            }
            drop(db);
            let session =
                crate::services::testing::create_session(admin.0, Default::default()).await?;
            let mut headers = HeaderMap::new();
            headers.insert(AUTHORIZATION, format!("Bearer {}", session.token).parse()?);
            Ok(Fixture {
                headers,
                admin,
                other,
                tracks,
            })
        }

        #[tokio::test]
        async fn my_listens_page_in_one_order_with_ties_by_internal_track_id() -> anyhow::Result<()>
        {
            let _guard = crate::services::testing::runtime_test_lock().await;
            let fixture = setup().await?;
            let mut listed = Vec::new();
            for offset in [0, 2, 4] {
                let Json(page) = get_me_listens(
                    fixture.headers.clone(),
                    Query(MeListenQuery {
                        track_id: None,
                        page: routes::PageQuery {
                            limit: Some(2),
                            offset: Some(offset),
                        },
                        sort_by: None,
                        sort_order: None,
                        merge_unique_external_ids: None,
                    }),
                )
                .await
                .map_err(|error| anyhow::anyhow!("{error:?}"))?;
                assert_eq!((page.total, page.offset), (5, offset));
                assert!(page.items.len() <= 2);
                listed.extend(
                    page.items
                        .into_iter()
                        .map(|item| (item.track_id, item.listen_count)),
                );
            }

            let id = |index: usize| fixture.tracks[index].1.clone();
            assert_eq!(
                listed,
                vec![(id(4), 2), (id(0), 1), (id(1), 1), (id(2), 1), (id(3), 1)]
            );
            Ok(())
        }

        #[tokio::test]
        async fn everyones_listens_tie_by_internal_user_then_track_id() -> anyhow::Result<()> {
            let _guard = crate::services::testing::runtime_test_lock().await;
            let fixture = setup().await?;
            let listed = async |sort_by: Option<&str>, sort_order: Option<&str>| {
                let mut listed = Vec::new();
                for offset in [0, 3, 6] {
                    let Json(page) = get_listens(
                        fixture.headers.clone(),
                        Query(ListenQuery {
                            user_id: None,
                            track_id: None,
                            page: routes::PageQuery {
                                limit: Some(3),
                                offset: Some(offset),
                            },
                            sort_by: sort_by.map(|key| vec![key.to_string()]),
                            sort_order: sort_order.map(str::to_string),
                            merge_unique_external_ids: None,
                        }),
                    )
                    .await
                    .map_err(|error| anyhow::anyhow!("{error:?}"))?;
                    assert_eq!(page.total, 7);
                    listed.extend(
                        page.items
                            .into_iter()
                            .map(|item| (item.user_id, item.track_id)),
                    );
                }
                anyhow::Ok(listed)
            };
            let row = |user: &(DbId, String), track: usize| {
                (user.1.clone(), fixture.tracks[track].1.clone())
            };
            let (first, second) = if fixture.admin.0 < fixture.other.0 {
                (&fixture.admin, &fixture.other)
            } else {
                (&fixture.other, &fixture.admin)
            };
            let of = |user: &(DbId, String)| {
                let tracks = if user.0 == fixture.admin.0 {
                    0..5
                } else {
                    0..2
                };
                tracks.map(|track| row(user, track)).collect::<Vec<_>>()
            };

            // By count, the admin's twice-played track leads; the rest tie.
            let mut by_count = vec![row(&fixture.admin, 4)];
            by_count.extend(
                [of(first), of(second)]
                    .concat()
                    .into_iter()
                    .filter(|listed| *listed != row(&fixture.admin, 4)),
            );
            assert_eq!(listed(None, None).await?, by_count);

            let (low, high) = if fixture.admin.1 < fixture.other.1 {
                (&fixture.admin, &fixture.other)
            } else {
                (&fixture.other, &fixture.admin)
            };
            assert_eq!(
                listed(Some("user_id"), None).await?,
                [of(low), of(high)].concat()
            );
            assert_eq!(
                listed(Some("user_id"), Some("descending")).await?,
                [of(high), of(low)].concat()
            );
            Ok(())
        }
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
                lyra_db::listens::total(&*db, fixture.user_db_id, fixture.long_track_db_id)?
                    .map_or(0, |total| total.count),
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
            let mut summaries = summaries::of_users(&db, &principal, &[user_db_id]).unwrap();
            summaries::sort(&mut summaries, &[], None, &HashMap::new());
            let page = routes::PageQuery {
                limit: Some(LIMIT),
                offset: Some(offset),
            }
            .resolve()
            .slice(summaries);
            let page = with_track_ids(&db, page).ok().unwrap();
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
            "    reading the totals and keeping the visible ones: {}",
            ms(median(9, || {
                summaries::of_users(&db, &principal, &[user_db_id])
                    .unwrap()
                    .len() as u64
            }))
        );
        println!(
            "    reading the totals: {}",
            ms(median(9, || {
                lyra_db::listens::totals(&*db, user_db_id).unwrap().len() as u64
            }))
        );
    }
}
