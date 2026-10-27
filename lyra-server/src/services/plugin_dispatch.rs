// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
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

use crate::services::playback_sessions::PlaybackUpdatePayload;

/// The loaded plugin runtime as services reach it: dispatch to the handlers
/// plugins registered with service registries. The plugin executor
/// implements it and publishes it per generation.
pub(crate) trait PluginDispatch: Send + Sync {
    /// Identifies the runtime instance, so callers can detect a swap.
    fn vm_id(&self) -> u64;

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
}

#[derive(Clone, Debug)]
pub(crate) struct MixHandlerRequest {
    pub(crate) handler_id: u64,
    pub(crate) seed_id: Option<i64>,
    pub(crate) limit: Option<usize>,
    pub(crate) recent_track_ids: Vec<i64>,
    pub(crate) options: serde_json::Map<String, serde_json::Value>,
}

#[derive(Clone, Debug)]
pub(crate) struct MixHandlerResult {
    pub(crate) track_ids: Vec<i64>,
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
    pub(crate) handler_id: u64,
    pub(crate) context: serde_json::Value,
    pub(crate) deadline: std::time::Instant,
}

#[derive(Clone, Debug)]
pub(crate) struct MetadataRefreshResult {
    pub(crate) values: Vec<serde_json::Value>,
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
    Local {
        release_db_id: i64,
        release_id: String,
    },
}

#[derive(Clone, Debug)]
pub(crate) struct SimilarReleasesDispatchRequest {
    pub(crate) provider_id: String,
    pub(crate) handler_id: u64,
    pub(crate) context: serde_json::Value,
    pub(crate) timeout: Duration,
    pub(crate) cancellation: DispatchCancellation,
    pub(crate) max_candidates: usize,
}

#[derive(Clone, Debug)]
pub(crate) struct SimilarReleasesDispatchResult {
    pub(crate) candidates: Vec<SimilarReleaseCandidate>,
}
