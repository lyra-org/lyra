// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::{
        HashMap,
        HashSet,
        VecDeque,
    },
    convert::Infallible,
    mem,
    time::Duration,
};

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
    extract::Path,
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
    FutureExt,
    Stream,
    stream,
};
use tokio::{
    sync::broadcast::{
        self,
        error::RecvError,
    },
    time::{
        Instant,
        Interval,
        MissedTickBehavior,
        interval,
        interval_at,
    },
};

use crate::routes::request::bearer_credential;
use crate::{
    routes::AppError,
    services::{
        SyncRunSummary,
        auth::{
            AuthError,
            require_authenticated,
        },
        cancel_sync_run,
        libraries::{
            RunFilter,
            control::{
                self,
                LibraryControlError,
                SyncReader,
                require_manage_libraries_on,
            },
        },
        remote::constants::{
            AUTH_CHECK_INTERVAL,
            MAX_CONSECUTIVE_AUTH_ERRORS,
        },
        subscribe_sync_run_events,
    },
};

/// How long a library's progress is held back so that a burst of it goes out as one status.
const COALESCE_WINDOW: Duration = Duration::from_millis(250);

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

async fn build_events_sse(
    headers: HeaderMap,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, AppError> {
    let statuses = sync_statuses(
        bearer_credential(&headers).map(str::to_owned),
        AUTH_CHECK_INTERVAL,
    )
    .await?;
    Ok(Sse::new(futures::StreamExt::map(statuses, status_event)).keep_alive(KeepAlive::default()))
}

#[cfg(not(feature = "docgen"))]
async fn get_events(
    headers: HeaderMap,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, AppError> {
    build_events_sse(headers).await
}

#[cfg(feature = "docgen")]
async fn get_events(
    headers: HeaderMap,
) -> Result<aide::NoApi<Sse<impl Stream<Item = Result<Event, Infallible>>>>, AppError> {
    build_events_sse(headers).await.map(aide::NoApi)
}

fn status_event(summary: SyncRunSummary) -> Result<Event, Infallible> {
    let data = serde_json::to_string(&summary).unwrap_or_else(|_| "{}".to_string());
    Ok(Event::default().event("status").data(data))
}

/// The sync status of each library `bearer` can see, then each change to it, as [`SyncReader`]
/// reads them. Every `recheck_interval` the caller is read again, before anything more is sent:
/// the stream ends once `bearer` no longer authenticates, and follows the libraries and details
/// the caller sees now.
async fn sync_statuses(
    bearer: Option<String>,
    recheck_interval: Duration,
) -> Result<impl Stream<Item = SyncRunSummary>, AppError> {
    let principal = require_authenticated(bearer.as_deref()).await?;
    let receiver = subscribe_sync_run_events();
    let reader = SyncReader::of(Some(&principal)).await?;
    let mut feed = SyncFeed::new(bearer, reader, receiver, recheck_interval);
    feed.send_owed().await?;
    Ok(stream::unfold(feed, |mut feed| async move {
        let summary = feed.next().await?;
        Some((summary, feed))
    }))
}

struct SyncFeed {
    bearer: Option<String>,
    reader: SyncReader,
    receiver: broadcast::Receiver<SyncRunSummary>,
    recheck: Interval,
    flush: Interval,
    /// Libraries whose current status the caller is owed, kept until it loads: each newly
    /// visible one, every one after the caller's details change or summaries were missed.
    owed: HashSet<String>,
    /// Each library's newest active summary not yet sent, held back to coalesce progress.
    pending: HashMap<String, SyncRunSummary>,
    /// Each library's newest sequence taken, so that an older summary of it is dropped.
    taken: HashMap<String, u64>,
    outbox: VecDeque<SyncRunSummary>,
    consecutive_errors: u32,
}

enum Reread {
    Ended,
    Failed(anyhow::Error),
}

impl SyncFeed {
    fn new(
        bearer: Option<String>,
        reader: SyncReader,
        receiver: broadcast::Receiver<SyncRunSummary>,
        recheck_interval: Duration,
    ) -> Self {
        let mut flush = interval(COALESCE_WINDOW);
        flush.set_missed_tick_behavior(MissedTickBehavior::Delay);
        Self {
            owed: reader.library_ids().map(str::to_owned).collect(),
            bearer,
            reader,
            receiver,
            recheck: interval_at(Instant::now() + recheck_interval, recheck_interval),
            flush,
            pending: HashMap::new(),
            taken: HashMap::new(),
            outbox: VecDeque::new(),
            consecutive_errors: 0,
        }
    }

    /// The next status to send, or `None` once the stream ends.
    async fn next(&mut self) -> Option<SyncRunSummary> {
        loop {
            if self.recheck.tick().now_or_never().is_some() && !self.follow_caller().await {
                return None;
            }
            if let Some(summary) = self.outbox.pop_front() {
                match self.reader.read(summary) {
                    Some(summary) => return Some(summary),
                    None => continue,
                }
            }
            tokio::select! {
                biased;
                _ = self.recheck.tick() => {
                    if !self.follow_caller().await {
                        return None;
                    }
                }
                _ = self.flush.tick(), if !self.pending.is_empty() => self.flush_pending(),
                received = self.receiver.recv() => match received {
                    Ok(summary) => self.take(summary),
                    Err(RecvError::Lagged(_)) => {
                        self.owed.extend(self.reader.library_ids().map(str::to_owned));
                        if !self.follow_caller().await {
                            return None;
                        }
                    }
                    Err(RecvError::Closed) => return None,
                },
            }
        }
    }

    /// Reads the caller again, drops what it no longer sees, and queues the statuses it is owed;
    /// false once the stream must end.
    async fn follow_caller(&mut self) -> bool {
        let previous = match self.reread().await {
            Ok(previous) => previous,
            Err(Reread::Ended) => return false,
            Err(Reread::Failed(error)) => return self.tolerate(error),
        };
        let reader = &self.reader;
        let details_changed = previous.shows_details() != reader.shows_details();
        self.owed.extend(
            reader
                .library_ids()
                .filter(|library_id| details_changed || !previous.sees(library_id))
                .map(str::to_owned),
        );
        self.owed.retain(|library_id| reader.sees(library_id));
        self.pending.retain(|library_id, _| reader.sees(library_id));
        match self.send_owed().await {
            Ok(()) => {
                self.consecutive_errors = 0;
                true
            }
            Err(error) => self.tolerate(error.into()),
        }
    }

    /// Counts an internal error; false once [`MAX_CONSECUTIVE_AUTH_ERRORS`] came in a row.
    fn tolerate(&mut self, error: anyhow::Error) -> bool {
        self.consecutive_errors += 1;
        tracing::warn!(
            consecutive_errors = self.consecutive_errors,
            error = %error,
            "sync events recheck failed"
        );
        self.consecutive_errors < MAX_CONSECUTIVE_AUTH_ERRORS
    }

    fn flush_pending(&mut self) {
        let mut flushed: Vec<_> = self.pending.drain().map(|(_, summary)| summary).collect();
        flushed.sort_by(|a, b| a.run.library_id.cmp(&b.run.library_id));
        self.outbox.extend(flushed);
    }

    /// Takes a published summary: a finished run's goes out at once, an active one's waits for
    /// the next flush in place of any older one of its library.
    fn take(&mut self, summary: SyncRunSummary) {
        if !self.reader.sees(&summary.run.library_id)
            || self
                .taken
                .get(&summary.run.library_id)
                .is_some_and(|taken| summary.sequence <= *taken)
        {
            return;
        }
        self.mark_taken(&summary);
        if summary.run.status.is_active() {
            self.pending.insert(summary.run.library_id.clone(), summary);
        } else {
            self.pending.remove(&summary.run.library_id);
            self.outbox.push_back(summary);
        }
    }

    fn mark_taken(&mut self, summary: &SyncRunSummary) {
        let taken = self
            .taken
            .entry(summary.run.library_id.clone())
            .or_default();
        *taken = (*taken).max(summary.sequence);
    }

    /// Queues the current status of each library owed, in place of anything held back for it.
    async fn send_owed(&mut self) -> Result<(), LibraryControlError> {
        if self.owed.is_empty() {
            return Ok(());
        }
        let owed = &self.owed;
        let mut statuses: Vec<_> = self
            .reader
            .statuses(|library_id| owed.contains(library_id), RunFilter::default())
            .await?
            .into_values()
            .collect();
        statuses.sort_by(|a, b| a.run.library_id.cmp(&b.run.library_id));
        for summary in statuses {
            self.pending.remove(&summary.run.library_id);
            self.mark_taken(&summary);
            self.outbox.push_back(summary);
        }
        self.owed.clear();
        Ok(())
    }

    /// Takes the caller as the database has it now and returns the reader it replaces; ended once
    /// the credential no longer authenticates.
    async fn reread(&mut self) -> Result<SyncReader, Reread> {
        let reader = match require_authenticated(self.bearer.as_deref()).await {
            Ok(principal) => SyncReader::of(Some(&principal)).await,
            Err(AuthError::Internal(error)) => return Err(Reread::Failed(error)),
            Err(_) => return Err(Reread::Ended),
        };
        match reader {
            Ok(reader) => Ok(mem::replace(&mut self.reader, reader)),
            Err(LibraryControlError::Internal(error)) => Err(Reread::Failed(error)),
            Err(_) => Err(Reread::Ended),
        }
    }
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
fn get_events_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Stream library sync statuses")
        .description(
            "Streams Server-Sent Events carrying the sync status of every library the caller can \
             see: first the current status of each, as `GET /api/libraries/{id}/sync` returns \
             it, then each change. A burst of progress on one library arrives as its newest \
             status; a finished run's status always arrives. `run.error` and `current.subject` are \
             null unless the caller has ManageLibraries. The caller is checked again \
             periodically: libraries it gains arrive with their current status, libraries it \
             loses stop arriving, and the stream ends once its credential no longer \
             authenticates. Reconnecting starts over with the current statuses.",
        )
        .response_with::<200, Json<SyncRunSummary>, _>(|mut response| {
            {
                let inner = response.inner();
                inner.content.clear();
                inner.content.insert(
                    "text/event-stream".into(),
                    MediaType {
                        item_schema: Some(sync_status_sse_item_schema()),
                        ..Default::default()
                    },
                );
            }
            response.description("Server-Sent Events carrying library sync statuses.")
        })
}

#[cfg(feature = "docgen")]
fn sync_status_sse_item_schema() -> SchemaObject {
    SchemaObject {
        json_schema: schemars::json_schema!({
            "type": "object",
            "required": ["event", "data"],
            "properties": {
                "event": {
                    "const": "status",
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
        .route("/events", get(get_events))
        .route("/runs/{run_id}", get(get_run))
        .route("/runs/{run_id}/cancel", post(cancel_run))
}

#[cfg(feature = "docgen")]
pub(crate) fn sync_openapi_routes() -> aide::axum::ApiRouter {
    use aide::axum::routing::{
        get_with,
        post_with,
    };

    aide::axum::ApiRouter::new()
        .api_route("/events", get_with(get_events, get_events_docs))
        .api_route("/runs/{run_id}", get_with(get_run, get_run_docs))
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
    use futures::StreamExt;
    use lyra_db::Permission;

    use super::*;
    use crate::services::{
        STATE,
        libraries::{
            SyncRunProgress,
            SyncRunStatus,
            SyncStageKey,
            SyncWorkDetails,
            begin_test_run,
            end_test_run,
            get_sync_run,
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

    const RECHECK: Duration = Duration::from_millis(20);
    const WAIT: Duration = Duration::from_secs(5);
    const QUIET: Duration = Duration::from_millis(300);

    type Feed = std::pin::Pin<Box<dyn Stream<Item = SyncRunSummary> + Send>>;

    async fn feed(token: &str) -> anyhow::Result<Feed> {
        let feed = sync_statuses(Some(token.to_string()), RECHECK)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        Ok(Box::pin(feed))
    }

    /// The next status `wanted` keeps, skipping the others.
    async fn next_where(
        feed: &mut Feed,
        mut wanted: impl FnMut(&SyncRunSummary) -> bool,
    ) -> anyhow::Result<SyncRunSummary> {
        tokio::time::timeout(WAIT, async {
            while let Some(summary) = feed.next().await {
                if wanted(&summary) {
                    return Ok(summary);
                }
            }
            Err(anyhow::anyhow!("the feed ended"))
        })
        .await?
    }

    async fn assert_quiet(feed: &mut Feed) {
        if let Ok(next) = tokio::time::timeout(QUIET, feed.next()).await {
            panic!("expected no status, got {next:?}");
        }
    }

    #[tokio::test]
    async fn the_feed_opens_with_each_visible_library_read_for_the_caller() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let fixture = setup().await?;
        let shown = run_on(fixture.shown).await?;
        let hidden = run_on(fixture.hidden).await?;
        let shown_id = library_id(fixture.shown).await?;

        let mut managed = feed(&token(fixture.manager).await?).await?;
        let opening = next_where(&mut managed, |_| true).await?;
        assert_eq!(opening.run.library_id, shown_id);
        assert_eq!(opening.run.id.as_deref(), Some(shown.run_id()));
        assert!(
            opening
                .run
                .error
                .is_some_and(|error| error.contains(SECRET))
        );
        assert_eq!(
            opening.current.expect("current work").subject.as_deref(),
            Some(SECRET)
        );
        assert_quiet(&mut managed).await;

        let mut listened = feed(&token(fixture.listener).await?).await?;
        let opening = next_where(&mut listened, |_| true).await?;
        assert_eq!(opening.run.library_id, shown_id);
        assert_eq!(opening.run.error, None);
        assert_eq!(opening.current.expect("current work").subject, None);
        assert_quiet(&mut listened).await;
        end_test_run(&shown).await;
        end_test_run(&hidden).await;
        Ok(())
    }

    #[tokio::test]
    async fn the_feed_carries_changes_to_visible_libraries_only() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let fixture = setup().await?;
        let shown_id = library_id(fixture.shown).await?;
        let mut listened = feed(&token(fixture.listener).await?).await?;
        let idle = next_where(&mut listened, |_| true).await?;
        assert_eq!(idle.run.library_id, shown_id);
        assert_eq!(idle.run.status, SyncRunStatus::Idle);

        let hidden = run_on(fixture.hidden).await?;
        hidden.set_status(SyncRunStatus::Succeeded).await;
        let shown = run_on(fixture.shown).await?;
        shown.set_status(SyncRunStatus::Running).await;
        shown.set_status(SyncRunStatus::Failed).await;

        let mut seen = Vec::new();
        let finished = next_where(&mut listened, |summary| {
            seen.push(summary.run.library_id.clone());
            summary.run.status == SyncRunStatus::Failed
        })
        .await?;
        assert_eq!(finished.run.id.as_deref(), Some(shown.run_id()));
        assert_eq!(finished.run.error, None, "a listener never reads the error");
        assert!(seen.iter().all(|id| *id == shown_id), "{seen:?}");
        assert_quiet(&mut listened).await;
        end_test_run(&shown).await;
        end_test_run(&hidden).await;
        Ok(())
    }

    #[tokio::test]
    async fn the_feed_follows_the_caller_and_ends_with_its_credential() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let fixture = setup().await?;
        let shown = run_on(fixture.shown).await?;
        let hidden = run_on(fixture.hidden).await?;
        let (shown_id, hidden_id) = (
            library_id(fixture.shown).await?,
            library_id(fixture.hidden).await?,
        );
        let token = token(fixture.listener).await?;
        let mut listened = feed(&token).await?;
        let opening = next_where(&mut listened, |_| true).await?;
        assert_eq!(opening.run.error, None);

        {
            let mut db = STATE.db.write().await;
            let role = lyra_db::roles::get_role_for_user(&*db, fixture.listener)?.expect("role");
            lyra_db::roles::update(
                &mut *db,
                &lyra_db::roles::Role {
                    permissions: vec![Permission::ManageLibraries],
                    ..role
                },
            )?;
        }
        let detailed = next_where(&mut listened, |_| true).await?;
        assert_eq!(detailed.run.library_id, shown_id);
        assert!(
            detailed
                .run
                .error
                .is_some_and(|error| error.contains(SECRET))
        );

        lyra_db::libraries::grant_access(
            &mut *STATE.db.write().await,
            fixture.listener,
            fixture.hidden,
            lyra_db::libraries::AccessKind::ReadWrite,
        )?;
        let gained = next_where(&mut listened, |_| true).await?;
        assert_eq!(gained.run.library_id, hidden_id);
        assert_eq!(gained.run.id.as_deref(), Some(hidden.run_id()));

        lyra_db::libraries::revoke_access(
            &mut *STATE.db.write().await,
            fixture.listener,
            fixture.hidden,
        )?;
        assert_quiet(&mut listened).await;
        hidden.set_status(SyncRunStatus::Succeeded).await;
        assert_quiet(&mut listened).await;

        crate::services::auth::sessions::revoke_session_by_token(&token).await?;
        assert!(
            tokio::time::timeout(WAIT, listened.next()).await?.is_none(),
            "the feed ends once its credential is revoked"
        );
        end_test_run(&shown).await;
        end_test_run(&hidden).await;
        Ok(())
    }

    async fn set_manager_permissions(
        user: DbId,
        permissions: Vec<Permission>,
    ) -> anyhow::Result<()> {
        let mut db = STATE.db.write().await;
        let role = lyra_db::roles::get_role_for_user(&*db, user)?.expect("role");
        lyra_db::roles::update(
            &mut *db,
            &lyra_db::roles::Role {
                permissions,
                ..role
            },
        )?;
        Ok(())
    }

    #[tokio::test]
    async fn a_due_recheck_comes_before_anything_queued() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let fixture = setup().await?;
        lyra_db::libraries::grant_access(
            &mut *STATE.db.write().await,
            fixture.manager,
            fixture.hidden,
            lyra_db::libraries::AccessKind::ReadWrite,
        )?;
        let shown = run_on(fixture.shown).await?;
        let hidden = run_on(fixture.hidden).await?;

        let mut managed = feed(&token(fixture.manager).await?).await?;
        let first = next_where(&mut managed, |_| true).await?;
        assert!(first.run.error.is_some(), "opened with details");
        tokio::time::sleep(RECHECK * 3).await;
        set_manager_permissions(fixture.manager, Vec::new()).await?;
        let queued = next_where(&mut managed, |_| true).await?;
        assert_eq!(queued.run.error, None, "read with the current permission");
        assert_eq!(queued.current.and_then(|current| current.subject), None);

        let token = token(fixture.manager).await?;
        let mut unread = feed(&token).await?;
        next_where(&mut unread, |_| true).await?;
        tokio::time::sleep(RECHECK * 3).await;
        crate::services::auth::sessions::revoke_session_by_token(&token).await?;
        assert!(
            tokio::time::timeout(WAIT, unread.next()).await?.is_none(),
            "nothing queued goes out after the credential is revoked"
        );
        end_test_run(&shown).await;
        end_test_run(&hidden).await;
        Ok(())
    }

    async fn listener_feed(fixture: &Fixture) -> anyhow::Result<SyncFeed> {
        let principal = crate::services::testing::user_principal(fixture.listener).await?;
        let mut feed = SyncFeed::new(
            None,
            SyncReader::of(Some(&principal)).await?,
            subscribe_sync_run_events(),
            Duration::from_secs(3600),
        );
        feed.send_owed().await?;
        Ok(feed)
    }

    /// The run's summary as published, started at `started_at_ms`.
    async fn summary_at(
        progress: &SyncRunProgress,
        started_at_ms: Option<u64>,
    ) -> anyhow::Result<SyncRunSummary> {
        let mut summary = get_sync_run(progress.run_id()).await?.expect("run");
        summary.run.started_at_ms = started_at_ms;
        Ok(summary)
    }

    #[tokio::test]
    async fn an_earlier_run_never_follows_a_later_one_started_the_same_millisecond()
    -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let fixture = setup().await?;
        let library = lyra_db::libraries::get_by_id(&*STATE.db.read().await, fixture.shown)?
            .expect("library exists");
        let earlier = begin_test_run(&library).await;
        earlier.set_status(SyncRunStatus::Running).await;
        let started_at = get_sync_run(earlier.run_id())
            .await?
            .expect("run")
            .run
            .started_at_ms;
        let earlier_running = summary_at(&earlier, started_at).await?;
        earlier.set_status(SyncRunStatus::Succeeded).await;
        let earlier_finished = summary_at(&earlier, started_at).await?;
        end_test_run(&earlier).await;

        // Only the stored record knows the earlier run now, as after a restart.
        let later = begin_test_run(&library).await;
        let later_queued = summary_at(&later, started_at).await?;
        assert!(later_queued.sequence > earlier_finished.sequence);
        later.set_status(SyncRunStatus::Planning).await;
        let later_planning = summary_at(&later, started_at).await?;
        later.set_status(SyncRunStatus::Running).await;

        let mut feed = listener_feed(&fixture).await?;
        let opening: Vec<_> = feed.outbox.drain(..).collect();
        assert_eq!(opening.len(), 1);
        assert_eq!(opening[0].run.id.as_deref(), Some(later.run_id()));
        assert_eq!(opening[0].run.status, SyncRunStatus::Running);
        feed.take(earlier_finished.clone());
        feed.take(later_planning.clone());
        assert!(feed.outbox.is_empty() && feed.pending.is_empty());

        let mut live = SyncFeed::new(
            None,
            feed.reader.clone(),
            subscribe_sync_run_events(),
            Duration::from_secs(3600),
        );
        live.owed.clear();
        live.take(earlier_running);
        live.take(earlier_finished.clone());
        live.take(later_queued);
        live.take(later_planning);
        later.set_status(SyncRunStatus::Succeeded).await;
        live.take(summary_at(&later, started_at).await?);
        live.take(earlier_finished);
        let statuses: Vec<_> = live
            .outbox
            .iter()
            .map(|summary| (summary.run.id.clone(), summary.run.status))
            .collect();
        assert_eq!(
            statuses,
            vec![
                (Some(earlier.run_id().to_string()), SyncRunStatus::Succeeded),
                (Some(later.run_id().to_string()), SyncRunStatus::Succeeded),
            ]
        );
        assert!(live.pending.is_empty());
        end_test_run(&later).await;
        Ok(())
    }

    #[tokio::test]
    async fn held_back_progress_goes_out_while_summaries_keep_arriving() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let fixture = setup().await?;
        let shown = run_on(fixture.shown).await?;
        let hidden = run_on(fixture.hidden).await?;
        let mut feed = listener_feed(&fixture).await?;
        feed.outbox.clear();
        tokio::time::pause();
        feed.flush.tick().await;

        shown.set_status(SyncRunStatus::Running).await;
        let progress = feed.receiver.recv().await?;
        feed.take(progress);
        assert_eq!(feed.pending.len(), 1, "progress is held back");
        tokio::time::advance(COALESCE_WINDOW * 2).await;
        for _ in 0..100 {
            hidden.set_status(SyncRunStatus::Running).await;
        }

        let sent = feed.next().await.expect("a status");
        assert_eq!(sent.run.id.as_deref(), Some(shown.run_id()));
        assert!(
            !feed.receiver.is_empty(),
            "the due flush went out before the waiting summaries were read"
        );
        end_test_run(&shown).await;
        end_test_run(&hidden).await;
        Ok(())
    }
}
