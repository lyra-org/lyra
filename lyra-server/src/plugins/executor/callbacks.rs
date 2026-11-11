// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use super::{
    MetadataRefreshResult,
    PluginExecutor,
    SimilarReleasesDispatchResult,
};
use crate::services::plugin_dispatch::RefreshTargetGone;
use anyhow::{
    Context,
    Result,
    bail,
};
use harmony_core::{
    LocalLuauTaskCompletion,
    LocalScheduler,
};
use harmony_luau::StringEnum;
use std::{
    collections::{
        HashMap,
        HashSet,
    },
    rc::Rc,
    sync::{
        Arc,
        Mutex,
        PoisonError,
    },
    time::{
        Duration,
        Instant,
    },
};

const DISPATCH_CANCELLED: &str = "plugin handler dispatch was cancelled";
pub(super) const DEADLINE_EXPIRED: &str = "plugin handler deadline expired";

/// How many handlers each plugin has running on the executor thread, so a restart can wait for
/// one plugin's without waiting on the others'.
#[derive(Clone, Default)]
pub(crate) struct RunningHandlers {
    inner: Arc<RunningHandlersInner>,
}

#[derive(Default)]
struct RunningHandlersInner {
    counts: Mutex<HashMap<Arc<str>, usize>>,
    changed: tokio::sync::Notify,
    /// Plugins whose new dispatches are refused while they are replaced.
    quiesced: Mutex<HashSet<Arc<str>>>,
}

impl RunningHandlers {
    pub(super) fn quiesce(&self, plugin: &str) {
        self.quiesced().insert(Arc::from(plugin));
    }

    pub(super) fn reopen(&self, plugin: &str) {
        self.quiesced().remove(plugin);
    }

    pub(super) fn is_quiesced(&self, plugin: Option<&str>) -> bool {
        plugin.is_some_and(|plugin| self.quiesced().contains(plugin))
    }

    fn quiesced(&self) -> std::sync::MutexGuard<'_, HashSet<Arc<str>>> {
        self.inner
            .quiesced
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    pub(super) fn enter(&self, plugin: Option<Arc<str>>) -> Option<RunningHandler> {
        let plugin = plugin?;
        *self
            .inner
            .counts
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(plugin.clone())
            .or_default() += 1;
        Some(RunningHandler {
            inner: self.inner.clone(),
            plugin,
        })
    }

    pub(super) async fn wait_idle(&self, plugin: &str) {
        loop {
            let changed = self.inner.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if !self
                .inner
                .counts
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .contains_key(plugin)
            {
                return;
            }
            changed.await;
        }
    }
}

/// One running handler, counted until dropped.
pub(super) struct RunningHandler {
    inner: Arc<RunningHandlersInner>,
    plugin: Arc<str>,
}

impl Drop for RunningHandler {
    fn drop(&mut self) {
        let mut counts = self
            .inner
            .counts
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(count) = counts.get_mut(&self.plugin) {
            *count -= 1;
            if *count == 0 {
                counts.remove(&self.plugin);
            }
        }
        drop(counts);
        self.inner.changed.notify_waiters();
    }
}

pub(super) enum CallbackReply {
    Refresh(tokio::sync::oneshot::Sender<Result<MetadataRefreshResult>>),
    Api {
        reply: tokio::sync::oneshot::Sender<Result<super::ApiHandlerResponse>>,
        request: Box<super::ApiHandlerRequest>,
        auth: crate::plugins::auth::DispatchAuth,
        /// What the handler ran with, for the producer of a streamed response to run with too.
        context: harmony_luau::CallContext,
    },
    Mix {
        reply: tokio::sync::oneshot::Sender<Result<super::MixHandlerResult>>,
        mixer_id: String,
    },
    Similar {
        reply: tokio::sync::oneshot::Sender<Result<SimilarReleasesDispatchResult>>,
        cancellation: super::DispatchCancellation,
        max_candidates: usize,
    },
    Source(tokio::sync::oneshot::Sender<Result<()>>),
}

/// A metadata refresh or sync filter's failure: `not_found` says the entity it was writing is gone.
pub(super) fn refresh_failure(error: harmony_luau::RaisedError) -> anyhow::Error {
    if error.kind == Some(crate::plugins::errors::ErrorKind::NotFound.as_str()) {
        anyhow::Error::new(RefreshTargetGone(error.message))
    } else {
        anyhow::Error::msg(error.message)
    }
}

impl CallbackReply {
    /// Whether the caller stopped waiting for the reply.
    fn is_cancelled(&self) -> bool {
        match self {
            Self::Refresh(reply) => reply.is_closed(),
            Self::Api { reply, .. } => reply.is_closed(),
            Self::Mix { reply, .. } => reply.is_closed(),
            Self::Source(reply) => reply.is_closed(),
            Self::Similar {
                reply,
                cancellation,
                ..
            } => reply.is_closed() || cancellation.is_cancelled(),
        }
    }

    /// Completes once the caller stops waiting for the reply.
    async fn cancelled(&mut self) {
        match self {
            Self::Refresh(reply) => reply.closed().await,
            Self::Api { reply, .. } => reply.closed().await,
            Self::Mix { reply, .. } => reply.closed().await,
            Self::Source(reply) => reply.closed().await,
            Self::Similar {
                reply,
                cancellation,
                ..
            } => {
                tokio::select! {
                    () = reply.closed() => {}
                    () = cancellation.cancelled() => {}
                }
            }
        }
    }

    /// A handler's failure as this reply's receiver reads it.
    fn failure(&self, error: harmony_luau::RaisedError) -> anyhow::Error {
        match self {
            Self::Refresh(_) => refresh_failure(error),
            _ => anyhow::Error::msg(error.message),
        }
    }

    /// Replies with `result`; a streamed response's producer starts writing before `deadline`.
    fn send(
        self,
        vm: &harmony_luau::Vm,
        deadline: Instant,
        result: Result<Vec<harmony_luau::Value>>,
    ) {
        match self {
            Self::Api {
                reply,
                request,
                auth,
                context,
            } => {
                let result = result.and_then(|values| {
                    let parsed = super::api::parse_api_response(&request, values)?;
                    let mut response = parsed.response;
                    response.auth = auth.auth();
                    response.client = request.client;
                    if let Some(produce) = parsed.produce {
                        let body = if super::stream::has_body(&request.method, response.status) {
                            let request = *request;
                            super::queue_stream(
                                vm,
                                produce,
                                context,
                                deadline,
                                super::StreamOrigin {
                                    plugin_id: request.plugin_id,
                                    method: request.method,
                                    path: request.path,
                                },
                            )?
                        } else {
                            super::ApiResponseStream::finished()
                        };
                        response.body = Some(super::ApiResponseBody::Stream(body));
                    }
                    Ok(response)
                });
                let _ = reply.send(result);
            }
            Self::Mix { reply, mixer_id } => {
                let result =
                    result.and_then(|values| super::mix::parse_mix_result(&mixer_id, values));
                let _ = reply.send(result);
            }
            Self::Source(reply) => {
                let _ = reply.send(result.map(|_| ()));
            }
            Self::Refresh(reply) => {
                let result = result.and_then(|values| {
                    let values = values
                        .iter()
                        .map(|value| harmony_serde::luau_to_json(value).map_err(anyhow::Error::new))
                        .collect::<Result<Vec<_>>>()?;
                    Ok(MetadataRefreshResult { values })
                });
                let _ = reply.send(result);
            }
            Self::Similar {
                reply,
                max_candidates,
                ..
            } => {
                let result = result.and_then(|values| {
                    let candidates = super::metadata::decode_similar_releases_result(
                        values.first(),
                        max_candidates,
                    )?;
                    Ok(SimilarReleasesDispatchResult { candidates })
                });
                let _ = reply.send(result);
            }
        }
    }
}

impl PluginExecutor {
    #[cfg(test)]
    pub(super) fn drive_callback<T>(
        &self,
        start: impl FnOnce(tokio::sync::oneshot::Sender<Result<T>>, tokio::sync::OwnedSemaphorePermit),
    ) -> Result<T> {
        let (reply, mut result) = tokio::sync::oneshot::channel();
        let permit = std::sync::Arc::new(tokio::sync::Semaphore::new(1)).try_acquire_owned()?;
        start(reply, permit);
        loop {
            self.poll_background_tasks();
            match result.try_recv() {
                Ok(result) => return result,
                Err(tokio::sync::oneshot::error::TryRecvError::Closed) => {
                    bail!("callback dropped response")
                }
                Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
        }
    }

    /// Schedules the handler `prepare` makes and a task that watches it: the task waits for the
    /// handler to end, its caller to leave or `deadline` to pass, cancels a handler that hasn't
    /// ended, and replies. Each round runs the task ahead of the handler, which isn't resumed
    /// once its caller is gone or its deadline has passed. The handler holds `permit` until it is
    /// gone.
    pub(super) fn start_callback(
        &self,
        deadline: Instant,
        mut reply: CallbackReply,
        permit: tokio::sync::OwnedSemaphorePermit,
        plugin: Option<Arc<str>>,
        prepare: impl FnOnce(
            Duration,
            Rc<LocalLuauTaskCompletion>,
        ) -> Result<(Rc<LocalScheduler>, harmony_luau::Thread)>,
    ) {
        let completion = Rc::new(LocalLuauTaskCompletion::default());
        let scheduled = (|| {
            if self.is_quiesced(plugin.as_deref()) {
                return Err(anyhow::Error::new(
                    crate::services::plugin_dispatch::DispatchNotStarted,
                ));
            }
            if reply.is_cancelled() {
                bail!("{DISPATCH_CANCELLED}");
            }
            let timeout = deadline
                .checked_duration_since(Instant::now())
                .filter(|timeout| !timeout.is_zero())
                .context("plugin handler deadline expired before execution")?;
            prepare(timeout, completion.clone())
        })();
        let (scheduler, thread) = match scheduled {
            Ok(scheduled) => scheduled,
            Err(error) => return reply.send(&self.vm, deadline, Err(error)),
        };
        let expired = scheduler.sleep_until(deadline);
        let running = self.running.enter(plugin.clone());
        let vm = self.vm.clone();
        let handler = super::WatchedThread::new(&scheduler, thread);
        scheduler.schedule_watcher(super::watcher_context(plugin), async move {
            let ended = tokio::select! {
                biased;
                () = reply.cancelled() => Err(DISPATCH_CANCELLED),
                result = completion.wait() => Ok(result),
                () = expired => Err(DEADLINE_EXPIRED),
            };
            let result = match ended {
                // A handler that ended in time is answered with what it returned, however late
                // this task reads it. One that ended past its deadline is not.
                Ok(_) if completion.ended_at().is_some_and(|ended| ended > deadline) => {
                    Err(anyhow::anyhow!(DEADLINE_EXPIRED))
                }
                Ok(result) => result.map_err(|error| reply.failure(error)),
                Err(reason) => {
                    handler.cancel().await;
                    Err(anyhow::anyhow!(reason))
                }
            };
            reply.send(&vm, deadline, result);
            drop((handler, permit, running));
            Ok(())
        });
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::oneshot;

    use super::*;

    fn not_found() -> harmony_luau::RaisedError {
        harmony_luau::RaisedError {
            kind: Some("not_found"),
            message: "gone".to_string(),
        }
    }

    #[test]
    fn only_a_metadata_refresh_reads_not_found_as_its_target_gone() {
        let (refresh, _refresh_answer) = oneshot::channel();
        assert!(
            CallbackReply::Refresh(refresh)
                .failure(not_found())
                .is::<RefreshTargetGone>()
        );

        let (source, _source_answer) = oneshot::channel();
        let (mix, _mix_answer) = oneshot::channel();
        for reply in [
            CallbackReply::Source(source),
            CallbackReply::Mix {
                reply: mix,
                mixer_id: "mixer".to_string(),
            },
        ] {
            let failure = reply.failure(not_found());
            assert!(!failure.is::<RefreshTargetGone>());
            assert_eq!(failure.to_string(), "gone");
        }
    }
}
