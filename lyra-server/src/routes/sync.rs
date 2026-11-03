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

use crate::routes::request::bearer_credential;
use crate::{
    routes::AppError,
    services::{
        SyncRunEvent,
        SyncRunSummary,
        auth::require_authenticated,
        cancel_sync_run,
        libraries::control::{
            self,
            require_manage_libraries_on,
        },
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

/// The run `run_id` names, when the caller may manage its library. A run the caller cannot read
/// is not found, as with [`get_run`].
async fn load_authorized_run(
    headers: &HeaderMap,
    run_id: &str,
) -> Result<SyncRunSummary, AppError> {
    let bearer = bearer_credential(headers);
    let principal = require_authenticated(bearer).await?;
    let summary = control::sync_run(&principal, run_id)
        .await?
        .ok_or_else(|| run_not_found(run_id))?;
    let _principal = require_manage_libraries_on(bearer, &summary.run.library_id).await?;
    Ok(summary)
}

async fn get_run(
    headers: HeaderMap,
    Path(run_id): Path<String>,
) -> Result<Json<SyncRunSummary>, AppError> {
    let principal = require_authenticated(bearer_credential(&headers)).await?;
    control::sync_run(&principal, &run_id)
        .await?
        .map(Json)
        .ok_or_else(|| run_not_found(&run_id))
}

fn run_not_found(run_id: &str) -> AppError {
    AppError::not_found(format!("sync run not found: {run_id}"))
}

async fn cancel_run(
    headers: HeaderMap,
    Path(run_id): Path<String>,
) -> Result<Json<SyncRunSummary>, AppError> {
    let _summary = load_authorized_run(&headers, &run_id).await?;
    let summary = cancel_sync_run(&run_id)
        .await?
        .ok_or_else(|| run_not_found(&run_id))?;
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
    op.summary("Get sync run").description(
        "Returns a compact library sync or refresh run summary. Requires access to the run's \
             library. `run.error` and `current.subject` are null unless the caller has \
             ManageLibraries.",
    )
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
    use agdb::{
        DbAny,
        DbId,
    };
    use axum::{
        http::{
            StatusCode,
            header::AUTHORIZATION,
        },
        response::IntoResponse,
    };
    use lyra_db::Permission;

    use super::*;
    use crate::services::{
        STATE,
        libraries::{
            SyncRunProgress,
            SyncStageKey,
            SyncWorkDetails,
            begin_test_run,
            end_test_run,
        },
        testing::{
            create_session,
            init_default_test_state,
            runtime_test_lock,
        },
    };

    const SECRET: &str = "/sync/shown/secret";

    struct Fixture {
        manager: DbId,
        listener: DbId,
        shown: DbId,
        hidden: DbId,
    }

    fn user_with(db: &mut DbAny, name: &str, permissions: Vec<Permission>) -> anyhow::Result<DbId> {
        let user = lyra_db::users::create(db, &lyra_db::fixtures::test_user(name)?)?;
        let role = lyra_db::roles::create(
            db,
            &lyra_db::roles::Role {
                db_id: None,
                id: nanoid::nanoid!(),
                name: format!("{name}-role"),
                permissions,
            },
        )?;
        lyra_db::roles::assign_role_to_user(db, user, role)?;
        Ok(user)
    }

    /// A manager and a listener who both see `shown` and not `hidden`.
    async fn setup() -> anyhow::Result<Fixture> {
        init_default_test_state()?;
        let mut db = STATE.db.write().await;
        let manager = user_with(&mut db, "manager", vec![Permission::ManageLibraries])?;
        let listener = user_with(&mut db, "listener", Vec::new())?;
        let shown = lyra_db::test_db::insert_library(&mut db, "Shown", "/sync/shown")?;
        let hidden = lyra_db::test_db::insert_library(&mut db, "Hidden", "/sync/hidden")?;
        for user in [manager, listener] {
            lyra_db::libraries::grant_access(
                &mut db,
                user,
                shown,
                lyra_db::libraries::AccessKind::ReadWrite,
            )?;
        }
        Ok(Fixture {
            manager,
            listener,
            shown,
            hidden,
        })
    }

    async fn library_id(library: DbId) -> anyhow::Result<String> {
        Ok(
            lyra_db::libraries::get_by_id(&*STATE.db.read().await, library)?
                .expect("library exists")
                .id,
        )
    }

    /// A run on `library` that failed a unit under [`SECRET`] and is working on it.
    async fn run_on(library: DbId) -> anyhow::Result<SyncRunProgress> {
        let library = lyra_db::libraries::get_by_id(&*STATE.db.read().await, library)?
            .expect("library exists");
        let progress = begin_test_run(&library).await;
        progress
            .set_test_error(&format!("cannot read {SECRET}"))
            .await;
        progress
            .start_work(
                SyncStageKey::EntrySync,
                SyncWorkDetails::new("entry_group_sync").source_dir(Some(SECRET.to_string())),
            )
            .await;
        Ok(progress)
    }

    async fn token(user: DbId) -> anyhow::Result<String> {
        Ok(create_session(user, Default::default()).await?.token)
    }

    fn headers(token: &str) -> anyhow::Result<HeaderMap> {
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, format!("Bearer {token}").parse()?);
        Ok(headers)
    }

    fn status_of<T>(result: Result<T, AppError>) -> StatusCode {
        match result {
            Ok(_) => StatusCode::OK,
            Err(error) => error.into_response().status(),
        }
    }

    #[tokio::test]
    async fn a_run_reads_for_library_viewers_with_details_for_managers() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let fixture = setup().await?;
        let shown = run_on(fixture.shown).await?;
        let hidden = run_on(fixture.hidden).await?;
        let manager = headers(&token(fixture.manager).await?)?;
        let listener = headers(&token(fixture.listener).await?)?;
        let get = |headers: &HeaderMap, progress: &SyncRunProgress| {
            get_run(headers.clone(), Path(progress.run_id().to_string()))
        };

        let Json(managed) = get(&manager, &shown)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        assert_eq!(managed.run.library_id, library_id(fixture.shown).await?);
        assert!(
            managed
                .run
                .error
                .is_some_and(|error| error.contains(SECRET))
        );
        assert_eq!(
            managed.current.expect("current work").subject.as_deref(),
            Some(SECRET)
        );

        let Json(listened) = get(&listener, &shown)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        assert_eq!(listened.run.id, managed.run.id);
        assert_eq!(listened.run.error, None);
        assert_eq!(listened.current.expect("current work").subject, None);

        for caller in [&manager, &listener] {
            assert_eq!(status_of(get(caller, &hidden).await), StatusCode::NOT_FOUND);
        }
        assert_eq!(
            status_of(get_run(listener.clone(), Path("missing".to_string())).await),
            StatusCode::NOT_FOUND
        );

        assert_eq!(
            status_of(cancel_run(listener.clone(), Path(shown.run_id().to_string())).await),
            StatusCode::FORBIDDEN
        );
        for caller in [&manager, &listener] {
            for run_id in [hidden.run_id(), "missing"] {
                assert_eq!(
                    status_of(cancel_run(caller.clone(), Path(run_id.to_string())).await),
                    StatusCode::NOT_FOUND,
                    "a run the caller cannot read is not found"
                );
            }
        }
        assert_eq!(
            status_of(cancel_run(manager.clone(), Path(shown.run_id().to_string())).await),
            StatusCode::OK
        );
        end_test_run(&shown).await;
        end_test_run(&hidden).await;
        Ok(())
    }

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
