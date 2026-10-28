// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::HashMap,
    fmt,
    sync::{
        Arc,
        atomic::{
            AtomicBool,
            Ordering,
        },
    },
};

use anyhow::Result;
use harmony_core::plugin::PluginManifest;

use crate::services::playback_sessions::PlaybackUpdatePayload;
use crate::services::plugin_dispatch::{
    MetadataRefreshRequest,
    MetadataRefreshResult,
    MixHandlerRequest,
    MixHandlerResult,
    SimilarReleasesDispatchRequest,
    SimilarReleasesDispatchResult,
};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) struct TaskIdKey(pub(super) u64);

#[derive(Debug)]
pub(crate) struct WebSocketState {
    closed: AtomicBool,
    close_signal: tokio::sync::Notify,
}

impl WebSocketState {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            closed: AtomicBool::new(false),
            close_signal: tokio::sync::Notify::new(),
        })
    }

    pub(crate) fn request_close(&self) {
        self.close_signal.notify_one();
    }

    pub(crate) fn mark_closed(&self) {
        self.closed.store(true, Ordering::Release);
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    pub(crate) async fn closed(&self) {
        self.close_signal.notified().await;
    }
}

pub(super) enum PluginExecutorCommand {
    PluginManifests(tokio::sync::oneshot::Sender<Result<Vec<PluginManifest>>>),
    HasPlugin {
        plugin_id: String,
        reply: tokio::sync::oneshot::Sender<Result<bool>>,
    },
    #[cfg(test)]
    ExecPlugin {
        plugin_id: String,
        reply: tokio::sync::oneshot::Sender<Result<()>>,
    },
    VerifyPluginManifest {
        plugin_id: String,
        reply: tokio::sync::oneshot::Sender<Result<()>>,
    },
    StopPlugin {
        plugin_id: String,
        reply: tokio::sync::oneshot::Sender<Result<()>>,
    },
    RestartPlugin {
        plugin_id: String,
        reply: tokio::sync::oneshot::Sender<Result<()>>,
    },
    ClearPluginCallbacks {
        plugin_id: String,
        reply: tokio::sync::oneshot::Sender<Result<()>>,
    },
    ExecAll(tokio::sync::oneshot::Sender<Result<Vec<String>>>),
    MixHandler {
        request: MixHandlerRequest,
        reply: tokio::sync::oneshot::Sender<Result<MixHandlerResult>>,
    },
    MetadataRefresh {
        request: MetadataRefreshRequest,
        reply: tokio::sync::oneshot::Sender<Result<MetadataRefreshResult>>,
    },
    SimilarReleases {
        request: SimilarReleasesDispatchRequest,
        reply: tokio::sync::oneshot::Sender<Result<SimilarReleasesDispatchResult>>,
    },
    ApiHandler {
        request: ApiHandlerRequest,
        reply: tokio::sync::oneshot::Sender<Result<ApiHandlerResponse>>,
    },
    StartWebSocket {
        request: WebSocketStartRequest,
        reply: tokio::sync::oneshot::Sender<Result<()>>,
    },
    PlaybackUpdate(PlaybackUpdatePayload),
    ListenUpdate(crate::services::listens::ListenUpdatePayload),
}

#[derive(Clone, Debug)]
pub(crate) struct MetadataDispatchContext;

/// Marks calls made from an id link generator.
#[derive(Clone, Debug)]
pub(crate) struct IdLinkDispatchContext;

#[derive(Clone, Debug)]
pub(crate) struct ApiHandlerRequest {
    pub(crate) handler_id: u64,
    pub(crate) plugin_id: String,
    pub(crate) method: String,
    pub(crate) path: String,
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) query: HashMap<String, Vec<String>>,
    pub(crate) params: HashMap<String, String>,
    pub(crate) body: Vec<u8>,
    pub(crate) auth: Option<crate::services::auth::ResolvedAuth>,
    pub(crate) client: crate::services::auth::caller::ClientAddress,
}

#[derive(Clone, Debug)]
pub(crate) enum ApiResponseBody {
    Json(serde_json::Value),
    Bytes(Vec<u8>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ApiResponseKind {
    Json,
    Empty,
    Text,
    Bytes,
    Redirect,
    File,
    StreamTrack,
    DownloadTrack,
    HlsPlaylist,
}

impl ApiResponseKind {
    pub(crate) fn parse(raw: &str) -> Option<Self> {
        Some(match raw {
            "json" => Self::Json,
            "empty" => Self::Empty,
            "text" => Self::Text,
            "bytes" => Self::Bytes,
            "redirect" => Self::Redirect,
            "file" => Self::File,
            "stream_track" => Self::StreamTrack,
            "download_track" => Self::DownloadTrack,
            "hls_playlist" => Self::HlsPlaylist,
            _ => return None,
        })
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::Empty => "empty",
            Self::Text => "text",
            Self::Bytes => "bytes",
            Self::Redirect => "redirect",
            Self::File => "file",
            Self::StreamTrack => "stream_track",
            Self::DownloadTrack => "download_track",
            Self::HlsPlaylist => "hls_playlist",
        }
    }
}

impl fmt::Display for ApiResponseKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ApiHandlerResponse {
    pub(crate) kind: ApiResponseKind,
    pub(crate) status: u16,
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) body: Option<ApiResponseBody>,
    pub(crate) path: Option<String>,
    pub(crate) transform: Option<serde_json::Value>,
    pub(crate) track_id: Option<i64>,
    pub(crate) options: Option<serde_json::Value>,
    /// Principal acting for the dispatch: seeded from boundary bearer auth
    /// and overwritten by `auth.resolve_auth`.
    pub(crate) principal: Option<crate::services::auth::Principal>,
    /// The requesting client's address, from the dispatch's request.
    pub(crate) client: crate::services::auth::caller::ClientAddress,
}

pub(crate) struct WebSocketStartRequest {
    pub(crate) handler_id: u64,
    pub(crate) plugin_id: String,
    pub(crate) method: String,
    pub(crate) path: String,
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) query: HashMap<String, Vec<String>>,
    pub(crate) params: HashMap<String, String>,
    pub(crate) auth: Option<crate::services::auth::ResolvedAuth>,
    /// The socket's acting-principal slot, shared by every task on the socket.
    /// Seeded and refreshed by the driver only for auth-required routes.
    pub(crate) dispatch_auth: crate::plugins::auth::DispatchAuth,
    pub(crate) inbound: Arc<tokio::sync::Mutex<tokio::sync::mpsc::Receiver<String>>>,
    pub(crate) outbound: tokio::sync::mpsc::Sender<String>,
    pub(crate) state: Arc<WebSocketState>,
}
