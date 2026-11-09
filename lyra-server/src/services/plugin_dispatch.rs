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

use anyhow::{
    Result,
    ensure,
};
use futures::future::BoxFuture;

use tokio::sync::RwLock;

use crate::services::{
    STATE,
    listens::ListenUpdatePayload,
    mix::{
        MixRegistry,
        MixSeed,
    },
    playback_sessions::PlaybackUpdatePayload,
    plugin_scope::RuntimeScope,
    providers::ProviderRegistry,
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
        ensure!(
            self.vm_id == vm_id,
            "plugin handler {} belongs to plugin runtime {}, not to runtime {vm_id}",
            self.handler_id,
            self.vm_id
        );
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
}

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

    /// The calls as they stand; `None` when no runtime can make them.
    fn current(&self) -> Option<Self::Calls>;
}

/// The published runtime, as seen from outside any VM.
#[derive(Clone)]
pub(crate) struct Published;

impl CallSource for Published {
    type Calls = PublishedCalls;

    fn current(&self) -> Option<PublishedCalls> {
        let scope = STATE.generation().latest()?;
        Some(PublishedCalls { scope })
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

#[derive(Clone, Debug, Default)]
pub(crate) struct DispatchCancellation {
    cancelled: Arc<AtomicBool>,
}

impl DispatchCancellation {
    pub(crate) fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct MetadataRefreshRequest {
    pub(crate) handler: PluginCallbackHandle,
    pub(crate) context: serde_json::Value,
    pub(crate) deadline: std::time::Instant,
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
