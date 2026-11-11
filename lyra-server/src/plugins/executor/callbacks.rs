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

pub(super) struct PendingCallbackTask {
    completion: Rc<LocalLuauTaskCompletion>,
    scheduler: Rc<LocalScheduler>,
    thread: harmony_luau::Thread,
    deadline: Instant,
    reply: CallbackReply,
    _permit: tokio::sync::OwnedSemaphorePermit,
    _running: Option<RunningHandler>,
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
        executor: &PluginExecutor,
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
                                &executor.vm,
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

    pub(super) fn start_callback(
        &self,
        deadline: Instant,
        reply: CallbackReply,
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
                bail!("plugin handler dispatch was cancelled");
            }
            let timeout = deadline
                .checked_duration_since(Instant::now())
                .filter(|timeout| !timeout.is_zero())
                .context("plugin handler deadline expired before execution")?;
            prepare(timeout, completion.clone())
        })();
        match scheduled {
            Ok((scheduler, thread)) => self.callback_tasks.borrow_mut().push(PendingCallbackTask {
                completion,
                scheduler,
                thread,
                deadline,
                reply,
                _permit: permit,
                _running: self.running.enter(plugin),
            }),
            Err(error) => reply.send(self, deadline, Err(error)),
        }
    }

    pub(super) fn poll_callback_tasks(&self) {
        let mut tasks = self.callback_tasks.borrow_mut();
        let mut index = 0;
        while index < tasks.len() {
            let task = &tasks[index];
            let result = if task.reply.is_cancelled() {
                Some(Err(anyhow::anyhow!(
                    "plugin handler dispatch was cancelled"
                )))
            } else if Instant::now() >= task.deadline {
                Some(Err(anyhow::anyhow!("plugin handler deadline expired")))
            } else {
                let mut context = std::task::Context::from_waker(std::task::Waker::noop());
                match task.completion.poll(&mut context) {
                    std::task::Poll::Ready(result) => {
                        Some(result.map_err(|error| task.reply.failure(error)))
                    }
                    std::task::Poll::Pending => None,
                }
            };
            if let Some(result) = result {
                let task = tasks.swap_remove(index);
                task.scheduler.cancel_luau_thread(&task.thread);
                task.reply.send(self, task.deadline, result);
            } else {
                index += 1;
            }
        }
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
