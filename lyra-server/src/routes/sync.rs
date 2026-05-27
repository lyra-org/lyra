// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::convert::Infallible;

#[cfg(feature = "docgen")]
use aide::{
    openapi::{
        MediaType,
        SchemaObject,
    },
    transform::TransformOperation,
};
use axum::{
    Json,
    Router,
    extract::{
        Path,
        Query,
    },
    http::HeaderMap,
    response::sse::{
        Event,
        KeepAlive,
        Sse,
    },
    routing::{
        get,
        post,
    },
};
use futures::{
    Stream,
    StreamExt,
    stream,
};
use serde::Deserialize;

use crate::{
    routes::AppError,
    services::{
        SyncRunEvent,
        SyncRunSummary,
        auth::require_manage_libraries_on,
        cancel_sync_run,
        get_sync_run,
        subscribe_sync_run_events,
        sync_run_events_after,
    },
};

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct SyncRunEventsQuery {
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Last observed sync event sequence number.")
    )]
    after: Option<u64>,
}

async fn load_authorized_run(
    headers: &HeaderMap,
    run_id: &str,
) -> Result<SyncRunSummary, AppError> {
    let summary = get_sync_run(run_id)
        .await?
        .ok_or_else(|| AppError::not_found(format!("sync run not found: {run_id}")))?;
    let _principal = require_manage_libraries_on(headers, &summary.run.library_id).await?;
    Ok(summary)
}

async fn get_run(
    headers: HeaderMap,
    Path(run_id): Path<String>,
) -> Result<Json<SyncRunSummary>, AppError> {
    Ok(Json(load_authorized_run(&headers, &run_id).await?))
}

async fn cancel_run(
    headers: HeaderMap,
    Path(run_id): Path<String>,
) -> Result<Json<SyncRunSummary>, AppError> {
    let _summary = load_authorized_run(&headers, &run_id).await?;
    let summary = cancel_sync_run(&run_id)
        .await?
        .ok_or_else(|| AppError::not_found(format!("sync run not found: {run_id}")))?;
    Ok(Json(summary))
}

async fn build_run_events_sse(
    headers: HeaderMap,
    run_id: String,
    query: SyncRunEventsQuery,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, AppError> {
    let summary = load_authorized_run(&headers, &run_id).await?;
    let after = query.after.unwrap_or(0);
    let receiver = subscribe_sync_run_events();
    let backlog = sync_run_events_after(&run_id, after).await;
    let send_latest = should_send_latest_sync_event(
        after,
        summary.sequence,
        backlog.first().map(|event| event.sequence),
        backlog.last().map(|event| event.sequence),
    );
    let live_after = if send_latest {
        summary.sequence
    } else {
        backlog.last().map(|event| event.sequence).unwrap_or(after)
    };
    let backlog_stream = if send_latest {
        stream::iter(vec![sync_sse_summary_event(summary.sequence, summary)]).left_stream()
    } else {
        stream::iter(backlog.into_iter().map(sync_sse_event)).right_stream()
    };
    let live_stream = stream::unfold(
        (run_id, live_after, receiver),
        |(run_id, mut last_sequence, mut receiver)| async move {
            loop {
                match receiver.recv().await {
                    Ok(event) if event.run_id == run_id && event.sequence > last_sequence => {
                        last_sequence = event.sequence;
                        return Some((sync_sse_event(event), (run_id, last_sequence, receiver)));
                    }
                    Ok(_) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return None,
                }
            }
        },
    );

    Ok(Sse::new(backlog_stream.chain(live_stream)).keep_alive(KeepAlive::default()))
}

#[cfg(not(feature = "docgen"))]
async fn get_run_events(
    headers: HeaderMap,
    Path(run_id): Path<String>,
    Query(query): Query<SyncRunEventsQuery>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, AppError> {
    build_run_events_sse(headers, run_id, query).await
}

#[cfg(feature = "docgen")]
async fn get_run_events(
    headers: HeaderMap,
    Path(run_id): Path<String>,
    Query(query): Query<SyncRunEventsQuery>,
) -> Result<aide::NoApi<Sse<impl Stream<Item = Result<Event, Infallible>>>>, AppError> {
    build_run_events_sse(headers, run_id, query)
        .await
        .map(aide::NoApi)
}

fn sync_sse_event(event: SyncRunEvent) -> Result<Event, Infallible> {
    sync_sse_summary_event(event.sequence, event.summary)
}

fn sync_sse_summary_event(sequence: u64, summary: SyncRunSummary) -> Result<Event, Infallible> {
    let data = serde_json::to_string(&summary).unwrap_or_else(|_| "{}".to_string());
    Ok(Event::default()
        .event("snapshot")
        .id(sequence.to_string())
        .data(data))
}

fn should_send_latest_sync_event(
    after: u64,
    latest_sequence: u64,
    first_replay_sequence: Option<u64>,
    last_replay_sequence: Option<u64>,
) -> bool {
    latest_sequence > after
        && (last_replay_sequence.is_none()
            || first_replay_sequence.is_some_and(|sequence| sequence > after.saturating_add(1))
            || last_replay_sequence.is_some_and(|sequence| sequence < latest_sequence))
}

#[cfg(feature = "docgen")]
fn get_run_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Get sync run")
        .description("Returns a compact library sync or refresh run summary.")
}

#[cfg(feature = "docgen")]
fn get_run_events_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Stream sync run events")
        .description(
            "Streams replayable Server-Sent Events for a sync run. Use the `after` query parameter \
         with the last observed sequence number when reconnecting.",
        )
        .response_with::<200, Json<SyncRunSummary>, _>(|mut response| {
            {
                let inner = response.inner();
                inner.content.clear();
                inner.content.insert(
                    "text/event-stream".into(),
                    MediaType {
                        item_schema: Some(sync_run_sse_item_schema()),
                        ..Default::default()
                    },
                );
            }
            response.description("Server-Sent Events carrying sync run summaries.")
        })
}

#[cfg(feature = "docgen")]
fn sync_run_sse_item_schema() -> SchemaObject {
    SchemaObject {
        json_schema: schemars::json_schema!({
            "type": "object",
            "required": ["event", "id", "data"],
            "properties": {
                "event": {
                    "const": "snapshot",
                    "type": "string"
                },
                "id": {
                    "type": "string"
                },
                "data": {
                    "type": "string",
                    "contentMediaType": "application/json",
                    "contentSchema": {
                        "$ref": "#/components/schemas/SyncRunSummary"
                    }
                }
            }
        }),
        external_docs: None,
        example: None,
    }
}

#[cfg(feature = "docgen")]
fn cancel_run_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Cancel sync run")
        .description("Requests cancellation for a running sync or refresh run.")
}

pub fn sync_routes() -> Router {
    Router::new()
        .route("/runs/{run_id}", get(get_run))
        .route("/runs/{run_id}/events", get(get_run_events))
        .route("/runs/{run_id}/cancel", post(cancel_run))
}

#[cfg(feature = "docgen")]
pub(crate) fn sync_openapi_routes() -> aide::axum::ApiRouter {
    use aide::axum::routing::{
        get_with,
        post_with,
    };

    aide::axum::ApiRouter::new()
        .api_route("/runs/{run_id}", get_with(get_run, get_run_docs))
        .api_route(
            "/runs/{run_id}/events",
            get_with(get_run_events, get_run_events_docs),
        )
        .api_route(
            "/runs/{run_id}/cancel",
            post_with(cancel_run, cancel_run_docs),
        )
}

#[cfg(test)]
mod tests {
    use super::should_send_latest_sync_event;

    #[test]
    fn sse_reconnect_sends_latest_when_replay_is_unavailable() {
        assert!(should_send_latest_sync_event(10, 15, None, None));
        assert!(should_send_latest_sync_event(10, 15, Some(12), Some(15)));
        assert!(should_send_latest_sync_event(10, 15, Some(11), Some(14)));
    }

    #[test]
    fn sse_reconnect_uses_replay_when_it_is_current_and_contiguous() {
        assert!(!should_send_latest_sync_event(10, 10, None, None));
        assert!(!should_send_latest_sync_event(10, 15, Some(11), Some(15)));
    }
}
