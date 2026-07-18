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
        HashMap,
        HashSet,
    },
};

use crate::{
    STATE,
    db::{
        self,
        Permission,
        SortDirection,
    },
    routes::{
        self,
        AppError,
        responses::PageResponse,
    },
    services::{
        auth::{
            Principal,
            require_permission,
            require_principal,
        },
        pagination::SnapshotKey,
        playback_sessions as playback_service,
        providers::provider_registry,
    },
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
    default_direction: SortDirection,
) -> Result<SortDirection, AppError> {
    match sort_order {
        None => Ok(default_direction),
        Some(raw) => db::parse_sort_direction(Some(raw), true).map_err(|err| match err {
            db::SortSpecParseError::UnsupportedSortOrder(raw) => AppError::bad_request(format!(
                "Unsupported sort_order value: {}. Supported values: ascending, descending",
                raw
            )),
            other => AppError::bad_request(other.to_string()),
        }),
    }
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

fn compare_optional_ms(a: Option<u64>, b: Option<u64>, direction: SortDirection) -> Ordering {
    match (a, b) {
        (Some(a), Some(b)) => db::apply_direction(a.cmp(&b), direction),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

fn compare_listen_rows(
    a: &ListenRow,
    b: &ListenRow,
    keys: &[ListenSortKey],
    direction: SortDirection,
) -> Ordering {
    for key in keys {
        let ord = match key {
            ListenSortKey::ListenCount => {
                db::apply_direction(a.listen_count.cmp(&b.listen_count), direction)
            }
            ListenSortKey::LastPlayedAt => {
                compare_optional_ms(a.last_played_ms, b.last_played_ms, direction)
            }
            ListenSortKey::UserId => db::apply_direction(a.user_id.cmp(&b.user_id), direction),
            ListenSortKey::TrackId => db::apply_direction(a.track_id.cmp(&b.track_id), direction),
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
        let direction = parse_sort_order(sort_order, SortDirection::Descending)?;
        rows.sort_by(|a, b| {
            db::apply_direction(a.listen_count.cmp(&b.listen_count), direction)
                .then_with(|| compare_optional_ms(a.last_played_ms, b.last_played_ms, direction))
                .then_with(|| a.user_id.cmp(&b.user_id))
                .then_with(|| a.track_id.cmp(&b.track_id))
        });
        return Ok(());
    }

    let direction = parse_sort_order(sort_order, SortDirection::Ascending)?;
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
        last_played_at: row.last_played_ms.map(super::unix_ms_to_rfc3339_u64),
    }
}

fn listen_row_to_response(row: ListenRow) -> ListenResponse {
    ListenResponse {
        user_id: row.user_id,
        track_id: row.track_id,
        listen_count: row.listen_count,
        last_played_at: row.last_played_ms.map(super::unix_ms_to_rfc3339_u64),
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

fn aggregate_stats(stats: Vec<db::listens::ListenStats>) -> (u64, Option<u64>) {
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

async fn resolve_accessible_track_filter(
    principal: &Principal,
    track_id: &str,
    merge_unique_external_ids: bool,
) -> Result<ResolvedTrackFilter, AppError> {
    let unique_track_id_pairs = if merge_unique_external_ids {
        let registry = provider_registry().read_owned().await;
        registry.unique_track_id_pairs()
    } else {
        HashSet::new()
    };

    let db = STATE.db.read().await;
    let track_db_id = db::lookup::find_node_id_by_id(&*db, track_id)?
        .ok_or_else(|| AppError::not_found(format!("Track not found: {}", track_id)))?;
    routes::require_entity_accessible(&*db, principal, track_db_id, || {
        AppError::not_found(format!("Track not found: {}", track_id))
    })?;

    let stat_track_ids = if merge_unique_external_ids {
        playback_service::resolve_merged_track_ids_for_play_count(
            &db,
            track_db_id,
            &unique_track_id_pairs,
        )?
    } else {
        vec![track_db_id]
    };

    let mut accessible_track_ids = Vec::with_capacity(stat_track_ids.len());
    for id in stat_track_ids {
        if routes::entity_accessible_to_principal(&*db, principal, id)? {
            accessible_track_ids.push(id);
        }
    }

    let track_public_ids = db::lookup::find_ids_by_db_ids(&*db, &accessible_track_ids)?
        .into_values()
        .collect::<HashSet<_>>();

    Ok(ResolvedTrackFilter {
        track_db_ids: accessible_track_ids,
        track_public_ids,
    })
}

async fn rows_for_user(
    principal: &Principal,
    user_db_id: DbId,
    user_public_id: String,
    track_id: Option<String>,
    merge_unique_external_ids: bool,
) -> Result<Vec<ListenRow>, AppError> {
    if let Some(track_id) = track_id {
        let track_filter =
            resolve_accessible_track_filter(principal, &track_id, merge_unique_external_ids)
                .await?;
        let db = STATE.db.read().await;
        let stats = db::listens::get_stats(&db, &track_filter.track_db_ids, Some(user_db_id))?;
        let (listen_count, last_played_ms) = aggregate_stats(stats);
        return Ok(vec![ListenRow {
            user_id: user_public_id,
            track_id,
            listen_count,
            last_played_ms,
        }]);
    }

    let db = STATE.db.read().await;
    let summaries = db::listens::list_summaries_for_user(&db, user_db_id)?;
    let mut rows = Vec::with_capacity(summaries.len());
    for summary in summaries {
        if !routes::entity_accessible_to_principal(&*db, principal, summary.track_db_id)? {
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

async fn resolve_user_by_public_id(user_id: &str) -> Result<(DbId, String), AppError> {
    let db = STATE.db.read().await;
    let user = db::users::get_by_public_id(&db, user_id)?
        .ok_or_else(|| AppError::not_found(format!("User not found: {}", user_id)))?;
    let user_db_id = user
        .db_id
        .ok_or_else(|| anyhow::anyhow!("user missing db_id"))?;
    Ok((user_db_id, user.id))
}

async fn rows_for_managed_listens(
    principal: &Principal,
    query: &ListenQuery,
) -> Result<Vec<ListenRow>, AppError> {
    let merge_unique_external_ids = query.merge_unique_external_ids.unwrap_or(false);
    validate_merge_usage(query.track_id.as_deref(), merge_unique_external_ids)?;

    if let Some(user_id) = query.user_id.as_deref() {
        let (user_db_id, user_public_id) = resolve_user_by_public_id(user_id).await?;
        return rows_for_user(
            principal,
            user_db_id,
            user_public_id,
            query.track_id.clone(),
            merge_unique_external_ids,
        )
        .await;
    }

    let track_filter = if let Some(track_id) = query.track_id.as_deref() {
        Some(resolve_accessible_track_filter(principal, track_id, merge_unique_external_ids).await?)
    } else {
        None
    };

    let db = STATE.db.read().await;
    let summaries = db::listens::list_summaries(
        &db,
        None,
        track_filter.as_ref().map(|filter| &filter.track_public_ids),
    )?;

    if let Some(track_id) = query.track_id.as_deref() {
        let mut rows_by_user: HashMap<String, ListenRow> = HashMap::new();
        for summary in summaries {
            if !routes::entity_accessible_to_principal(&*db, principal, summary.track_db_id)? {
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
        if !routes::entity_accessible_to_principal(&*db, principal, summary.track_db_id)? {
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
    let track_db_ids = db::lookup::find_node_ids_by_ids(&db, &track_public_ids)?;
    let mut visible_users = HashMap::new();
    let mut hydrated = Vec::with_capacity(rows.len());
    for row in rows {
        let user_exists = match visible_users.get(&row.user_id).copied() {
            Some(exists) => exists,
            None => {
                let exists = db::users::get_by_public_id(&db, &row.user_id)?.is_some();
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
        if routes::entity_accessible_to_principal(&db, principal, track_db_id)? {
            hydrated.push(row);
        }
    }
    Ok(hydrated)
}

pub(super) async fn get_me_listens(
    headers: HeaderMap,
    Query(query): Query<MeListenQuery>,
) -> Result<Json<PageResponse<MeListenResponse>>, AppError> {
    let principal = require_principal(&headers).await?;
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
            principal.user_db_id,
            principal.user_public_id.clone(),
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
    let principal = require_principal(&headers).await?;
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
}
