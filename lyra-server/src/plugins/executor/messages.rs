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
use crate::services::playbacks::socket_claims;
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
    socket: socket_claims::SocketId,
    closed: AtomicBool,
    close_signal: tokio::sync::Notify,
}

impl WebSocketState {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            socket: socket_claims::SocketId::next(),
            closed: AtomicBool::new(false),
            close_signal: tokio::sync::Notify::new(),
        })
    }

    pub(crate) fn request_close(&self) {
        self.close_signal.notify_one();
    }

    /// Marks the websocket closed and lets go of the sessions it claimed, returning what
    /// [`crate::services::playbacks::pause_after_socket_close`] pauses for them.
    #[must_use]
    pub(crate) fn mark_closed(&self) -> Vec<socket_claims::ReleasedClaim> {
        socket_claims::release_socket(self.socket, || self.closed.store(true, Ordering::Release))
    }

    /// Claims the session until the websocket closes; returns whether it does, which it doesn't
    /// once closed.
    pub(crate) fn claim_session(&self, session: socket_claims::ClaimedSession) -> bool {
        socket_claims::claim(self.socket, session, || !self.is_closed())
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
    /// Panics on the executor thread, outside any plugin call.
    #[cfg(test)]
    Panic(tokio::sync::oneshot::Sender<Result<()>>),
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
    ExecAll(tokio::sync::oneshot::Sender<Result<Vec<String>>>),
    Shutdown(tokio::sync::oneshot::Sender<Result<()>>),
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
    RunPluginSource {
        request: PluginSourceRun,
        reply: tokio::sync::oneshot::Sender<Result<()>>,
    },
}

pub(crate) type PluginSourceArgs = dyn FnOnce(&harmony_luau::Vm, &harmony_luau::ChunkOrigin) -> Result<Vec<harmony_luau::Value>>
    + Send
    + Sync;

/// A chunk run as a plugin's code without its entrypoint, dispatching as `principal`, the way a
/// Luau test runs. It runs beside dispatched handlers, so it can reach handlers it registers.
pub(crate) struct PluginSourceRun {
    pub(crate) plugin_id: String,
    pub(crate) path: String,
    pub(crate) source: Arc<[u8]>,
    /// Builds the chunk's `...` on the executor thread, where Luau values live.
    pub(crate) args: Box<PluginSourceArgs>,
    pub(crate) principal: crate::services::auth::Principal,
}

#[derive(Clone, Debug)]
pub(crate) struct MetadataDispatchContext;

/// Marks calls made from an id link generator.
#[derive(Clone, Debug)]
pub(crate) struct IdLinkDispatchContext;

#[derive(Clone, Debug)]
pub(crate) struct ApiHandlerRequest {
    pub(crate) handler: crate::services::plugin_dispatch::PluginCallbackHandle,
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

#[derive(Debug)]
pub(crate) enum ApiResponseBody {
    Json(serde_json::Value),
    Bytes(Vec<u8>),
    /// This many bytes of filler, generated as they are sent.
    Filler(u64),
    /// Written by the plugin's producer as it is sent.
    Stream(super::ApiResponseStream),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ApiResponseKind {
    Json,
    Empty,
    Text,
    Bytes,
    Redirect,
    Cover,
    StreamTrack,
    DownloadTrack,
    HlsPlaylist,
    Filler,
    Stream,
}

impl ApiResponseKind {
    pub(crate) fn parse(raw: &str) -> Option<Self> {
        Some(match raw {
            "json" => Self::Json,
            "empty" => Self::Empty,
            "text" => Self::Text,
            "bytes" => Self::Bytes,
            "redirect" => Self::Redirect,
            "cover" => Self::Cover,
            "stream_track" => Self::StreamTrack,
            "download_track" => Self::DownloadTrack,
            "hls_playlist" => Self::HlsPlaylist,
            "filler" => Self::Filler,
            "stream" => Self::Stream,
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
            Self::Cover => "cover",
            Self::StreamTrack => "stream_track",
            Self::DownloadTrack => "download_track",
            Self::HlsPlaylist => "hls_playlist",
            Self::Filler => "filler",
            Self::Stream => "stream",
        }
    }

    /// Whether Lyra serves the body itself, so a failure to serve it can be answered with the
    /// response's fallback.
    pub(crate) fn takes_fallback(self) -> bool {
        matches!(
            self,
            Self::Cover | Self::StreamTrack | Self::DownloadTrack | Self::HlsPlaylist
        )
    }

    /// Whether the response is whole once built, as a fallback must be.
    pub(crate) fn is_body_only(self) -> bool {
        matches!(self, Self::Json | Self::Empty | Self::Text | Self::Bytes)
    }
}

impl fmt::Display for ApiResponseKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug)]
pub(crate) struct ApiHandlerResponse {
    pub(crate) kind: ApiResponseKind,
    pub(crate) status: u16,
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) body: Option<ApiResponseBody>,
    /// The cover a `cover` response serves, read as it is served.
    pub(crate) cover: Option<crate::plugins::api::options::CoverRequest>,
    pub(crate) track_id: Option<String>,
    /// The options of a track response, read as it is served.
    pub(crate) options: Option<crate::plugins::api::options::TrackResponseOptions>,
    /// Principal acting for the dispatch: seeded from boundary bearer auth
    /// and overwritten by `auth.resolve_auth`.
    pub(crate) principal: Option<crate::services::auth::Principal>,
    /// The requesting client's address, from the dispatch's request.
    pub(crate) client: crate::services::auth::caller::ClientAddress,
    /// What to answer in place of Lyra's error when it can't serve the response.
    pub(crate) fallback: Option<ApiFallbackResponse>,
}

/// A body-only response a plugin sends when Lyra fails to serve the one it returned.
#[derive(Debug)]
pub(crate) struct ApiFallbackResponse {
    pub(crate) kind: ApiResponseKind,
    pub(crate) status: u16,
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) body: Option<ApiResponseBody>,
}

pub(crate) struct WebSocketStartRequest {
    pub(crate) handler: crate::services::plugin_dispatch::PluginCallbackHandle,
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
