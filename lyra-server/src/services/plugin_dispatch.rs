// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{
            AtomicBool,
            Ordering,
        },
    },
    time::Duration,
};

use anyhow::Result;
use futures::future::BoxFuture;

use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

use crate::services::{
    STATE,
    bookmarks::BookmarkUpdatePayload,
    favorites::FavoriteUpdatePayload,
    listens::ListenUpdatePayload,
    mix::{
        MixRegistry,
        MixSeed,
    },
    playback_sessions::PlaybackUpdatePayload,
    plugin_scope::RuntimeScope,
    providers::ProviderRegistry,
    ratings::RatingUpdatePayload,
};

/// A handler a plugin registered, as services hold it: the runtime that
/// registered it and its id there. Every runtime numbers its handlers from 1,
/// so the runtime id keeps a handle from reaching another runtime's handler.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct PluginCallbackHandle {
    pub(crate) vm_id: u64,
    pub(crate) handler_id: u64,
}

impl PluginCallbackHandle {
    /// The handler's id in the runtime `vm_id`; fails for a handle another
    /// runtime issued.
    pub(crate) fn handler_id_in(self, vm_id: u64) -> Result<u64> {
        if self.vm_id != vm_id {
            return Err(anyhow::Error::new(DispatchNotStarted).context(format!(
                "plugin handler {} belongs to plugin runtime {}, not to runtime {vm_id}",
                self.handler_id, self.vm_id
            )));
        }
        Ok(self.handler_id)
    }
}

/// A dispatch the runtime never admitted, because it is being replaced or has stopped: no handler
/// ran, so the caller can treat the handler as absent.
#[derive(Debug, thiserror::Error)]
#[error("plugin executor queue is unavailable")]
pub(crate) struct DispatchNotStarted;

impl DispatchNotStarted {
    pub(crate) fn caused(error: &anyhow::Error) -> bool {
        error.chain().any(|cause| cause.is::<Self>())
    }
}

/// The loaded plugin runtime as services reach it: dispatch to the handlers
/// plugins registered with service registries. The plugin executor
/// implements it and publishes it per generation.
pub(crate) trait PluginDispatch: Send + Sync {
    fn dispatch_metadata_refresh(
        &self,
        request: MetadataRefreshRequest,
    ) -> BoxFuture<'_, Result<MetadataRefreshResult>>;

    fn dispatch_mix_handler(
        &self,
        request: MixHandlerRequest,
    ) -> BoxFuture<'_, Result<MixHandlerResult>>;

    fn dispatch_similar_releases(
        &self,
        request: SimilarReleasesDispatchRequest,
    ) -> BoxFuture<'_, Result<SimilarReleasesDispatchResult>>;

    /// Lossy: fails instead of waiting when the runtime is saturated.
    fn dispatch_playback_update(&self, payload: PlaybackUpdatePayload) -> Result<()>;

    /// Lossy like [`Self::dispatch_playback_update`].
    fn dispatch_listen_update(&self, payload: ListenUpdatePayload) -> Result<()>;

    /// Lossy like [`Self::dispatch_playback_update`].
    fn dispatch_favorite_update(&self, payload: FavoriteUpdatePayload) -> Result<()>;

    /// Lossy like [`Self::dispatch_playback_update`].
    fn dispatch_rating_update(&self, payload: RatingUpdatePayload) -> Result<()>;

    /// Lossy like [`Self::dispatch_playback_update`].
    fn dispatch_bookmark_update(&self, payload: BookmarkUpdatePayload) -> Result<()>;
}

/// Hands the published runtime an update of `kind` a service just committed, through `send`.
/// Best effort: nothing is sent without a runtime, and one that is saturated or being replaced
/// drops the update.
pub(crate) fn send_update(
    kind: &'static str,
    send: impl FnOnce(&dyn PluginDispatch) -> Result<()>,
) {
    let Some(scope) = STATE.generation().latest() else {
        return;
    };
    if let Err(error) = send(scope.dispatch().as_ref()) {
        if DispatchNotStarted::caused(&error) {
            tracing::debug!(kind, "dropped plugin on_update dispatch during a reload");
        } else {
            tracing::warn!(kind, error = %error, "failed to enqueue plugin on_update dispatch");
        }
    }
}

/// How long a call may wait for a transition covering its target.
#[derive(Clone)]
pub(crate) enum TransitionWait {
    /// Until the transition settles, which its guard guarantees, or `cancel` fires.
    Settled {
        cancel: CancellationToken,
    },
    Until(tokio::time::Instant),
}

impl TransitionWait {
    /// What someone is waiting on gets this long.
    pub(crate) fn interactive() -> Self {
        Self::Until(tokio::time::Instant::now() + INTERACTIVE_TRANSITION_WAIT)
    }

    /// A background pass waits as long as the transition takes, or until it is cancelled.
    pub(crate) fn background(cancel: CancellationToken) -> Self {
        Self::Settled { cancel }
    }
}

/// The transition budget of a call someone is waiting on.
pub(crate) const INTERACTIVE_TRANSITION_WAIT: Duration = Duration::from_secs(15);

/// A handler call the runtime can run, with the registries its handlers are read from.
pub(crate) trait RuntimeCalls {
    fn providers(&self) -> impl Future<Output = Arc<RwLock<ProviderRegistry>>>;

    fn mixers(&self) -> impl Future<Output = Arc<RwLock<MixRegistry>>>;

    fn metadata_refresh(
        &self,
        request: MetadataRefreshRequest,
    ) -> impl Future<Output = Result<MetadataRefreshResult>>;

    fn mix_handler(
        &self,
        request: MixHandlerRequest,
    ) -> impl Future<Output = Result<MixHandlerResult>>;

    fn similar_releases(
        &self,
        request: SimilarReleasesDispatchRequest,
    ) -> impl Future<Output = Result<SimilarReleasesDispatchResult>>;
}

/// Where a service gets its [`RuntimeCalls`]: the published runtime, queued, or the VM making the
/// call, whose executor can't serve a queue while it is the one waiting.
pub(crate) trait CallSource {
    type Calls: RuntimeCalls;

    /// The calls to reach `target`, once no transition holds it up; `None` when nothing can reach
    /// it: no runtime, or the caller's wait ended first. Nothing was dispatched then.
    fn reach(&self, target: &str) -> impl Future<Output = Option<Self::Calls>>;

    /// The calls as they stand, without waiting for anything.
    fn current(&self) -> Option<Self::Calls>;

    fn in_transition(&self, target: &str) -> bool;
}

/// The published runtime, as seen from outside any VM.
#[derive(Clone)]
pub(crate) struct Published {
    wait: TransitionWait,
}

impl Published {
    pub(crate) fn new(wait: TransitionWait) -> Self {
        Self { wait }
    }

    pub(crate) fn interactive() -> Self {
        Self::new(TransitionWait::interactive())
    }

    pub(crate) fn background(cancel: CancellationToken) -> Self {
        Self::new(TransitionWait::background(cancel))
    }
}

impl CallSource for Published {
    type Calls = PublishedCalls;

    async fn reach(&self, target: &str) -> Option<PublishedCalls> {
        let scope = STATE.generation().live(target, &self.wait).await.ok()??;
        Some(PublishedCalls { scope })
    }

    fn current(&self) -> Option<PublishedCalls> {
        let scope = STATE.generation().latest()?;
        Some(PublishedCalls { scope })
    }

    fn in_transition(&self, target: &str) -> bool {
        STATE.generation().in_transition(target)
    }
}

/// One published scope's handlers and dispatcher, which always belong to each other.
pub(crate) struct PublishedCalls {
    scope: Arc<RuntimeScope>,
}

impl RuntimeCalls for PublishedCalls {
    async fn providers(&self) -> Arc<RwLock<ProviderRegistry>> {
        self.scope.providers().registry()
    }

    async fn mixers(&self) -> Arc<RwLock<MixRegistry>> {
        self.scope.mix().registry()
    }

    async fn metadata_refresh(
        &self,
        request: MetadataRefreshRequest,
    ) -> Result<MetadataRefreshResult> {
        self.scope
            .dispatch()
            .dispatch_metadata_refresh(request)
            .await
    }

    async fn mix_handler(&self, request: MixHandlerRequest) -> Result<MixHandlerResult> {
        self.scope.dispatch().dispatch_mix_handler(request).await
    }

    async fn similar_releases(
        &self,
        request: SimilarReleasesDispatchRequest,
    ) -> Result<SimilarReleasesDispatchResult> {
        self.scope
            .dispatch()
            .dispatch_similar_releases(request)
            .await
    }
}

#[derive(Clone, Debug)]
pub(crate) struct MixHandlerRequest {
    pub(crate) handler: PluginCallbackHandle,
    pub(crate) seeds: Vec<MixSeed>,
    pub(crate) query: Option<String>,
    pub(crate) limit: Option<usize>,
    pub(crate) options: serde_json::Map<String, serde_json::Value>,
}

#[derive(Clone, Debug)]
pub(crate) struct MixHandlerResult {
    pub(crate) tracks: Vec<MixHandlerTrack>,
}

/// A track a mixer returned, with its relevance to the request in `0..=1` when the mixer gave
/// one.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct MixHandlerTrack {
    pub(crate) track_id: String,
    pub(crate) score: Option<f64>,
}

/// Set by a caller that gives up on a dispatch, and read by whoever runs it.
#[derive(Clone, Debug, Default)]
pub(crate) struct DispatchCancellation {
    inner: Arc<DispatchCancellationInner>,
}

#[derive(Debug, Default)]
struct DispatchCancellationInner {
    cancelled: AtomicBool,
    waker: futures::task::AtomicWaker,
}

impl DispatchCancellation {
    pub(crate) fn cancel(&self) {
        self.inner.cancelled.store(true, Ordering::Release);
        self.inner.waker.wake();
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.inner.cancelled.load(Ordering::Acquire)
    }

    /// Whether the dispatch was cancelled; if not, `cx` is woken once it is.
    fn poll_cancelled(&self, cx: &mut std::task::Context<'_>) -> bool {
        if self.is_cancelled() {
            return true;
        }
        self.inner.waker.register(cx.waker());
        self.is_cancelled()
    }

    /// Completes once the dispatch is cancelled, for the one task that waits on it.
    pub(crate) async fn cancelled(&self) {
        std::future::poll_fn(|cx| match self.poll_cancelled(cx) {
            true => std::task::Poll::Ready(()),
            false => std::task::Poll::Pending,
        })
        .await
    }
}

#[derive(Clone, Debug)]
pub(crate) struct MetadataRefreshRequest {
    pub(crate) handler: PluginCallbackHandle,
    pub(crate) context: serde_json::Value,
    /// How long the handler has from when the runtime takes the request up, which for a queued
    /// one is after its wait for the runtime.
    pub(crate) timeout: Duration,
}

/// How long past a handler's timeout its caller still waits for the plugin runtime's answer. The
/// handler's time starts when the runtime takes the request up, so this covers the request's
/// wait for the runtime, and then the runtime's cancelling of a late handler's thread, which it
/// answers only after. A caller that holds something across the call, as a provider's call lock
/// keeps one provider's calls from overlapping, releases it after that answer only when the wait
/// for the runtime fits in here.
pub(crate) const HANDLER_ANSWER_GRACE: Duration = Duration::from_secs(5);

/// Calls a metadata handler through `calls` and waits for its answer, no longer than the
/// request's timeout and [`HANDLER_ANSWER_GRACE`]. `handler` names it in that failure, as
/// "lyrics handler".
pub(crate) async fn call_metadata_handler<C: RuntimeCalls>(
    calls: &C,
    handler: &str,
    request: MetadataRefreshRequest,
) -> Result<MetadataRefreshResult> {
    let timeout = request.timeout;
    tokio::time::timeout(
        timeout.saturating_add(HANDLER_ANSWER_GRACE),
        calls.metadata_refresh(request),
    )
    .await
    .map_err(|_| anyhow::anyhow!("{handler} timed out after {} ms", timeout.as_millis()))?
}

#[derive(Clone, Debug)]
pub(crate) struct MetadataRefreshResult {
    pub(crate) values: Vec<serde_json::Value>,
}

/// A refresh handler ended because the entity it was writing is gone, which
/// [`PluginDispatch::dispatch_metadata_refresh`] fails with.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub(crate) struct RefreshTargetGone(pub(crate) String);

impl RefreshTargetGone {
    pub(crate) fn caused(error: &anyhow::Error) -> bool {
        error.chain().any(|cause| cause.is::<Self>())
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct SimilarReleaseExternalRef {
    pub(crate) provider_id: String,
    pub(crate) id_type: String,
    pub(crate) id_value: String,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) enum SimilarReleaseCandidate {
    External(SimilarReleaseExternalRef),
    Local { release_id: String },
}

#[derive(Clone, Debug)]
pub(crate) struct SimilarReleasesDispatchRequest {
    pub(crate) provider_id: String,
    pub(crate) handler: PluginCallbackHandle,
    pub(crate) context: serde_json::Value,
    pub(crate) timeout: Duration,
    pub(crate) cancellation: DispatchCancellation,
    pub(crate) max_candidates: usize,
}

#[derive(Clone, Debug)]
pub(crate) struct SimilarReleasesDispatchResult {
    pub(crate) candidates: Vec<SimilarReleaseCandidate>,
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use super::*;

    struct CountingWaker(AtomicUsize);

    impl std::task::Wake for CountingWaker {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn cancelling_a_dispatch_wakes_whoever_polled_it() {
        let cancellation = DispatchCancellation::default();
        let woken = Arc::new(CountingWaker(AtomicUsize::new(0)));
        let waker = std::task::Waker::from(woken.clone());
        let mut cx = std::task::Context::from_waker(&waker);

        assert!(!cancellation.poll_cancelled(&mut cx));
        assert_eq!(woken.0.load(Ordering::Relaxed), 0);
        cancellation.clone().cancel();
        assert_eq!(woken.0.load(Ordering::Relaxed), 1);
        assert!(cancellation.poll_cancelled(&mut cx));
    }
}
