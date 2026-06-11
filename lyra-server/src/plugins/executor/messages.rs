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
    ExecPlugin {
        plugin_id: String,
        reply: tokio::sync::oneshot::Sender<Result<()>>,
    },
    ExecAll(tokio::sync::oneshot::Sender<Result<()>>),
    MixHandler {
        request: MixHandlerRequest,
        reply: tokio::sync::oneshot::Sender<Result<MixHandlerResult>>,
    },
    MetadataRefresh {
        request: MetadataRefreshRequest,
        reply: tokio::sync::oneshot::Sender<Result<MetadataRefreshResult>>,
    },
    ApiHandler {
        request: ApiHandlerRequest,
        reply: tokio::sync::oneshot::Sender<Result<ApiHandlerResponse>>,
    },
    StartWebSocket {
        request: WebSocketStartRequest,
        reply: tokio::sync::oneshot::Sender<Result<()>>,
    },
    PlaybackUpdate(crate::services::playback_sessions::PlaybackUpdatePayload),
}

#[derive(Clone, Debug)]
pub(crate) struct MixHandlerRequest {
    pub(crate) handler_id: u64,
    pub(crate) seed_id: i64,
    pub(crate) limit: Option<usize>,
    pub(crate) user_id: Option<i64>,
    pub(crate) recent_track_ids: Vec<i64>,
    pub(crate) options: serde_json::Map<String, serde_json::Value>,
}

#[derive(Clone, Debug)]
pub(crate) struct MixHandlerResult {
    pub(crate) track_ids: Vec<i64>,
}

#[derive(Clone, Debug)]
pub(crate) struct MetadataRefreshRequest {
    pub(crate) handler_id: u64,
    pub(crate) context: serde_json::Value,
}

#[derive(Clone, Debug)]
pub(crate) struct MetadataRefreshResult {
    pub(crate) values: Vec<serde_json::Value>,
}

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
    pub(crate) client_key: Option<String>,
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
    pub(crate) inbound: Arc<tokio::sync::Mutex<tokio::sync::mpsc::Receiver<String>>>,
    pub(crate) outbound: tokio::sync::mpsc::Sender<String>,
    pub(crate) state: Arc<WebSocketState>,
}
