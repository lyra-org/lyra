// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

#[cfg(feature = "docgen")]
use aide::transform::TransformOperation;
use anyhow::anyhow;
use axum::{
    Router,
    routing::get,
};
use axum::{
    body::Body,
    extract::{
        Path,
        Query,
    },
    http::{
        HeaderMap,
        Response,
        header,
    },
};
use bytes::Bytes;
use lyra_ffmpeg::{
    FfmpegContext,
    Output,
    SeekRequest,
    SeekResult,
    WriteResult,
};
use serde::Deserialize;
use std::sync::mpsc as std_mpsc;
use std::sync::mpsc::RecvTimeoutError;
use std::sync::{
    Arc,
    atomic::{
        AtomicBool,
        Ordering,
    },
};
use std::time::Duration;
use tokio::sync::{
    mpsc as tokio_mpsc,
    oneshot,
};

use crate::routes::AppError;

use super::{
    DeliveryTarget,
    ServeTrackOptions,
    TranscodeKnobs,
    apply_request_start_offset,
    file_response,
    resolve_delivery,
    validate_and_get_track_source,
    validate_request,
};

const STREAM_SHUTDOWN_POLL_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
struct StreamChannelError {
    message: String,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct StreamQuery {
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Scoped media token returned by `POST /api/tracks/{id}/playback-url`."
        )
    )]
    media_token: Option<String>,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Optional output format (e.g. mp3, flac, wav, ogg, webm, aac, opus)."
        )
    )]
    format: Option<String>,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Optional ordered audio codec preferences (e.g. opus,aac or pcm_s24le,pcm_s16le)."
        )
    )]
    codec: Option<String>,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Target bitrate cap in bits per second. Applied for lossy outputs when below the source bitrate; ignored for lossless codecs or when above source."
        )
    )]
    bitrate_bps: Option<u32>,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Target sample rate in Hz. Triggers transcoding when supplied.")
    )]
    sample_rate_hz: Option<u32>,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Target channel count. Triggers transcoding when supplied.")
    )]
    channels: Option<u32>,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Prefer VBR for lossy transcodes when the selected encoder supports it."
        )
    )]
    prefer_vbr: Option<bool>,
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Per-request playback start offset in milliseconds.")
    )]
    start_offset_ms: Option<u64>,
}

async fn get_stream(
    headers: HeaderMap,
    Path(track_id): Path<String>,
    Query(query): Query<StreamQuery>,
) -> Result<Response<Body>, AppError> {
    let track_db_id = {
        let db = crate::STATE.db.read().await;
        crate::db::lookup::find_node_id_by_id(&*db, &track_id)?
            .ok_or_else(|| AppError::not_found(format!("not found: {track_id}")))?
    };
    super::require_stream_access(&headers, query.media_token.as_deref(), track_db_id).await?;
    stream_track_response(
        &headers,
        track_db_id,
        ServeTrackOptions {
            format: query.format,
            codec: query.codec,
            bitrate_bps: query.bitrate_bps,
            sample_rate_hz: query.sample_rate_hz,
            channels: query.channels,
            prefer_vbr: query.prefer_vbr,
            start_offset_ms: query.start_offset_ms,
        },
    )
    .await
}

pub(crate) async fn stream_track_response(
    headers: &HeaderMap,
    track_db_id: agdb::DbId,
    options: ServeTrackOptions,
) -> Result<Response<Body>, AppError> {
    let ServeTrackOptions {
        format,
        codec,
        bitrate_bps,
        sample_rate_hz,
        channels,
        prefer_vbr,
        start_offset_ms,
    } = options;
    let validated = validate_request(format, codec)?;
    let source = apply_request_start_offset(
        validate_and_get_track_source(track_db_id).await?,
        start_offset_ms,
    )?;

    let delivery = resolve_delivery(
        &validated,
        &source,
        TranscodeKnobs {
            bitrate_bps,
            sample_rate_hz,
            channels,
            prefer_vbr,
        },
        DeliveryTarget::Stream,
    )?;

    if delivery.direct_passthrough {
        return file_response(&source.full_path, delivery.content_type, headers).await;
    }

    let (sync_tx, sync_rx) = std_mpsc::sync_channel::<Vec<u8>>(1024);
    let (tokio_tx, tokio_rx) = tokio_mpsc::channel::<Result<Bytes, std::io::Error>>(64);
    let (started_tx, started_rx) = oneshot::channel::<()>();
    let (done_tx, done_rx) = oneshot::channel::<Result<(), StreamChannelError>>();
    let client_disconnected = Arc::new(AtomicBool::new(false));
    let shutdown = crate::services::shutdown::token();

    let write_callback = {
        let sync_tx = sync_tx.clone();
        let shutdown = shutdown.clone();
        move |buf: &[u8]| -> WriteResult {
            if shutdown.is_cancelled() {
                return WriteResult::Finished;
            }
            let bytes = buf.to_vec();
            let len = bytes.len();
            match sync_tx.send(bytes) {
                Ok(()) => WriteResult::Wrote(len),
                Err(_) => WriteResult::Finished,
            }
        }
    };

    let seek_callback = move |request: SeekRequest| -> SeekResult {
        if matches!(request, SeekRequest::Size) {
            return SeekResult::Unsupported;
        }
        SeekResult::Unseekable
    };

    let output = Output::with_callback(write_callback)
        .streaming()
        .set_seek_callback(seek_callback);
    let output = delivery.configure_output(output);

    let context = FfmpegContext::builder()
        .input(source.input_path)
        .start_ms(source.start_ms)
        .end_ms(source.end_ms)
        .output(output)
        .build()?;

    let client_disconnected_for_forwarder = Arc::clone(&client_disconnected);
    let shutdown_for_forwarder = shutdown.clone();
    tokio::task::spawn_blocking(move || {
        let mut started_tx = Some(started_tx);
        loop {
            if shutdown_for_forwarder.is_cancelled() {
                client_disconnected_for_forwarder.store(true, Ordering::Relaxed);
                break;
            }
            let chunk = match sync_rx.recv_timeout(STREAM_SHUTDOWN_POLL_INTERVAL) {
                Ok(chunk) => chunk,
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => break,
            };
            if let Some(tx) = started_tx.take() {
                let _ = tx.send(());
            }
            let bytes = Bytes::from(chunk);
            if tokio_tx.blocking_send(Ok(bytes)).is_err() {
                client_disconnected_for_forwarder.store(true, Ordering::Relaxed);
                break;
            }
        }
    });

    let client_disconnected_for_worker = Arc::clone(&client_disconnected);
    let shutdown_for_transcode = shutdown.clone();
    tokio::task::spawn_blocking(move || {
        let result = match context.start() {
            Ok(mut handle) => loop {
                if shutdown_for_transcode.is_cancelled()
                    || client_disconnected_for_worker.load(Ordering::Relaxed)
                {
                    client_disconnected_for_worker.store(true, Ordering::Relaxed);
                    break handle
                        .abort_with_timeout(crate::services::shutdown::TRANSCODE_ABORT_TIMEOUT);
                }
                if let Some(result) = handle.wait_completion_timeout(STREAM_SHUTDOWN_POLL_INTERVAL)
                {
                    break result;
                }
            },
            Err(err) => Err(err),
        };
        drop(sync_tx);
        let _ = done_tx.send(
            result
                .as_ref()
                .map(|_| ())
                .map_err(|err| StreamChannelError {
                    message: err.to_string(),
                }),
        );
        match result {
            Err(e) => {
                if client_disconnected_for_worker.load(Ordering::Relaxed)
                    || matches!(e, lyra_ffmpeg::Error::OutputStopped)
                {
                    tracing::debug!("stream ended (client disconnected)");
                } else {
                    tracing::warn!("ffmpeg error: {}", e);
                }
            }
            Ok(()) => tracing::debug!("stream completed successfully"),
        }
    });

    tokio::select! {
        started = started_rx => {
            started.map_err(|_| anyhow!("stream startup failed"))?;
        }
        done = done_rx => {
            let result = done.map_err(|_| anyhow!("stream startup failed"))?;
            match result {
                Ok(()) => {
                    return Err(anyhow!("stream ended before sending data").into());
                }
                Err(err) => {
                    return Err(anyhow!("ffmpeg error before streaming: {}", err.message).into());
                }
            }
        }
    }

    let shutdown_for_body = shutdown.clone();
    let stream = futures::stream::unfold(
        (tokio_rx, shutdown_for_body),
        |(mut rx, shutdown)| async move {
            let shutdown_for_select = shutdown.clone();
            tokio::select! {
                _ = shutdown_for_select.cancelled_owned() => None,
                item = rx.recv() => item.map(|item| (item, (rx, shutdown))),
            }
        },
    );
    let body = Body::from_stream(stream);

    let response = Response::builder()
        .header(header::CONTENT_TYPE, delivery.content_type)
        .header(header::CACHE_CONTROL, "no-cache, no-store, must-revalidate")
        .header(header::TRANSFER_ENCODING, "chunked")
        .header(header::ACCEPT_RANGES, "none")
        .header(header::PRAGMA, "no-cache")
        .header(header::EXPIRES, "0")
        .body(body)?;

    Ok(response)
}

#[cfg(feature = "docgen")]
fn stream_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Stream audio")
        .description(
            "Streams audio data for the track ID, including cue-derived virtual segments, optionally transcoded to the requested format or codec. Requires either bearer authentication with access to the track or a scoped `media_token` from `POST /api/tracks/{track_id}/playback-url`. Supports streamable formats: mp3, flac, wav, ogg, aac, opus, and aiff. Direct-copy stream responses support byte ranges; transcoded stream responses are chunked. HLS playback is available via `/api/stream/{track_id}/hls.m3u8`.",
        )
}

pub fn stream_routes() -> Router {
    Router::new().route("/{track_id}", get(get_stream))
}

#[cfg(feature = "docgen")]
pub(crate) fn stream_openapi_routes() -> aide::axum::ApiRouter {
    use aide::axum::routing::get_with;

    aide::axum::ApiRouter::new().api_route("/{track_id}", get_with(get_stream, stream_docs))
}
