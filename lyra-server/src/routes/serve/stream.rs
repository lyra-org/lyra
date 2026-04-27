// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use aide::axum::{
    ApiRouter,
    routing::get_with,
};
use aide::transform::TransformOperation;
use anyhow::anyhow;
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
    AVERROR,
    AVERROR_EOF,
    AVSEEK_SIZE,
    AudioCodec,
    ENOSYS,
    ESPIPE,
    FfmpegContext,
    Output,
};
use schemars::JsonSchema;
use serde::Deserialize;
use std::sync::mpsc as std_mpsc;
use std::sync::{
    Arc,
    atomic::{
        AtomicBool,
        Ordering,
    },
};
use tokio::sync::{
    mpsc as tokio_mpsc,
    oneshot,
};
use tokio_stream::wrappers::ReceiverStream;

use crate::routes::AppError;

use super::{
    apply_request_start_offset,
    apply_transcode_policy,
    configure_output,
    file_response,
    require_download_access,
    resolve_codec,
    resolve_output_format,
    validate_and_get_track_source,
    validate_request,
};

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
struct StreamChannelError {
    message: String,
}

#[derive(Deserialize, JsonSchema)]
struct StreamQuery {
    #[schemars(
        description = "Optional output format (e.g. mp3, flac, wav, ogg, webm, aac, opus)."
    )]
    format: Option<String>,
    #[schemars(
        description = "Optional ordered audio codec preferences (e.g. opus,aac or pcm_s24le,pcm_s16le)."
    )]
    codec: Option<String>,
    #[schemars(
        description = "Target bitrate cap in bits per second. Applied for lossy outputs when below the source bitrate; ignored for lossless codecs or when above source."
    )]
    bitrate_bps: Option<u32>,
    #[schemars(description = "Target sample rate in Hz. Triggers transcoding when supplied.")]
    sample_rate_hz: Option<u32>,
    #[schemars(description = "Target channel count. Triggers transcoding when supplied.")]
    channels: Option<u32>,
    #[schemars(description = "Per-request playback start offset in milliseconds.")]
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
    stream_track_response(
        &headers,
        track_db_id,
        query.format,
        query.codec,
        query.bitrate_bps,
        query.sample_rate_hz,
        query.channels,
        query.start_offset_ms,
    )
    .await
}

pub(crate) async fn stream_track_response(
    headers: &HeaderMap,
    track_db_id: agdb::DbId,
    format: Option<String>,
    codec: Option<String>,
    bitrate_bps: Option<u32>,
    sample_rate_hz: Option<u32>,
    channels: Option<u32>,
    start_offset_ms: Option<u64>,
) -> Result<Response<Body>, AppError> {
    let _principal = require_download_access(headers).await?;
    let validated = validate_request(format, codec)?;
    let source = apply_request_start_offset(
        validate_and_get_track_source(track_db_id).await?,
        start_offset_ms,
    )?;

    let initial_output_format = resolve_output_format(
        validated.format,
        &validated.preferred_codecs,
        source.entry_format,
        &source.full_path,
        true,
    )?;

    if !initial_output_format.supports_streaming() {
        return Err(AppError::bad_request(format!(
            "Format '{}' does not support streaming. Use /download endpoint or choose a streamable format (mp3, flac, wav, ogg, webm, aac, opus, aiff).",
            initial_output_format.extension()
        )));
    }

    let initial_codec = resolve_codec(
        &validated.preferred_codecs,
        initial_output_format,
        source.entry_format,
        true,
    )?;
    let provisional_output_format = if matches!(initial_codec, AudioCodec::Copy) {
        resolve_output_format(
            validated.format,
            &validated.preferred_codecs,
            source.entry_format,
            &source.full_path,
            false,
        )?
    } else {
        initial_output_format
    };
    let provisional_codec = if matches!(initial_codec, AudioCodec::Copy) {
        resolve_codec(
            &validated.preferred_codecs,
            provisional_output_format,
            source.entry_format,
            false,
        )?
    } else {
        initial_codec
    };
    let policy = apply_transcode_policy(
        bitrate_bps,
        sample_rate_hz,
        channels,
        provisional_codec,
        source.source_bitrate_bps,
    )?;
    let forcing_transcode = policy.bitrate_bps.is_some()
        || policy.sample_rate_hz.is_some()
        || policy.channels.is_some();

    let mut output_format = initial_output_format;
    let mut codec = initial_codec;
    if (source.start_ms.is_some() || source.end_ms.is_some() || forcing_transcode)
        && matches!(codec, AudioCodec::Copy)
    {
        output_format = provisional_output_format;
        codec = provisional_codec;
    }
    let output_mime = output_format.mime_type(true);

    if matches!(codec, AudioCodec::Copy)
        && source.entry_format == Some(output_format)
        && source.start_ms.is_none()
        && source.end_ms.is_none()
        && !forcing_transcode
    {
        return file_response(&source.full_path, output_mime, headers).await;
    }

    let (sync_tx, sync_rx) = std_mpsc::sync_channel::<Vec<u8>>(1024);
    let (tokio_tx, tokio_rx) = tokio_mpsc::channel::<Result<Bytes, std::io::Error>>(64);
    let (started_tx, started_rx) = oneshot::channel::<()>();
    let (done_tx, done_rx) = oneshot::channel::<Result<(), StreamChannelError>>();
    let client_disconnected = Arc::new(AtomicBool::new(false));

    let write_callback = {
        let sync_tx = sync_tx.clone();
        move |buf: &[u8]| -> i32 {
            let bytes = buf.to_vec();
            let len = bytes.len() as i32;
            match sync_tx.send(bytes) {
                Ok(()) => len,
                Err(_) => AVERROR_EOF,
            }
        }
    };

    let seek_callback = move |_offset: i64, whence: i32| -> i64 {
        if whence == AVSEEK_SIZE {
            return AVERROR(ENOSYS) as i64;
        }
        AVERROR(ESPIPE) as i64
    };

    let output = Output::with_callback(write_callback)
        .streaming()
        .set_seek_callback(seek_callback);
    let output = configure_output(output, output_format, codec, &policy);

    let context = FfmpegContext::builder()
        .input(source.input_path)
        .start_ms(source.start_ms)
        .end_ms(source.end_ms)
        .output(output)
        .build()?;

    let client_disconnected_for_forwarder = Arc::clone(&client_disconnected);
    tokio::task::spawn_blocking(move || {
        let mut started_tx = Some(started_tx);
        while let Ok(chunk) = sync_rx.recv() {
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

    let client_disconnected_for_logger = Arc::clone(&client_disconnected);
    tokio::spawn(async move {
        let result = context.start().and_then(|handle| handle.wait());
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
                let msg = e.to_string();
                let eof_str = AVERROR_EOF.to_string();
                if client_disconnected_for_logger.load(Ordering::Relaxed)
                    || msg.contains("End of file")
                    || msg.contains(&eof_str)
                {
                    tracing::debug!("stream ended (client disconnected)");
                } else {
                    tracing::error!("ffmpeg error: {}", e);
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

    let stream = ReceiverStream::new(tokio_rx);
    let body = Body::from_stream(stream);

    let response = Response::builder()
        .header(header::CONTENT_TYPE, output_mime)
        .header(header::CACHE_CONTROL, "no-cache, no-store, must-revalidate")
        .header(header::TRANSFER_ENCODING, "chunked")
        .header(header::ACCEPT_RANGES, "none")
        .header(header::PRAGMA, "no-cache")
        .header(header::EXPIRES, "0")
        .body(body)?;

    Ok(response)
}

fn stream_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Stream audio")
        .description(
            "Streams audio data for the track ID, including cue-derived virtual segments, optionally transcoded to the requested format or codec. Supports streamable formats: mp3, flac, wav, ogg, aac, opus, and aiff. Direct-copy stream responses support byte ranges; transcoded stream responses are chunked. HLS playback is available via `/api/stream/{track_id}/hls.m3u8`.",
        )
}

pub fn stream_routes() -> ApiRouter {
    ApiRouter::new().api_route("/{track_id}", get_with(get_stream, stream_docs))
}
