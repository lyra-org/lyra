// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use axum::{
    body::Body,
    http::{
        HeaderMap,
        Method,
        Response,
        header,
    },
};

use crate::{
    routes::AppError,
    services::STATE,
    services::playback_sources::SourceVersion,
    services::{
        auth::caller::Caller,
        transcoding::{
            cache::{
                CacheKey,
                EncodeSpec,
                Lookup,
                TranscodeFailed,
                TranscodeUnavailable,
                UNAVAILABLE_RETRY_AFTER,
            },
            capacity::{
                SATURATED_RETRY_AFTER,
                Saturated,
                try_acquire_for,
            },
        },
    },
};

use super::{
    DeliveryTarget,
    NO_STORE,
    ValidatedTrackSource,
    decision::DeliveryDecision,
    head_body,
    open_file_response,
};

impl From<TranscodeFailed> for AppError {
    fn from(err: TranscodeFailed) -> Self {
        anyhow::Error::from(err).into()
    }
}

/// The cache key of `delivery` from `source`: the exact source file and range, and every setting
/// the encoder and muxer receive.
fn cache_key(
    source: &ValidatedTrackSource,
    delivery: &DeliveryDecision,
    target: DeliveryTarget,
) -> CacheKey {
    CacheKey::of(&(
        &source.version,
        &source.input_path,
        source.start_ms,
        source.end_ms,
        delivery.output_format,
        delivery.codec,
        delivery.encoder,
        target.is_streaming(),
    ))
}

/// Serves the transcode `delivery` describes. A finished transcode is served from the cache with
/// byte ranges and its length. Otherwise a stream follows the encode as it writes, while a
/// download waits for it to finish. Requests for the same transcode share one encode, and `HEAD`
/// never starts one. Starting an encode takes a transcoding permit for `caller`; when none is free
/// the request is refused with 429 rather than queued.
pub(super) async fn transcoded_response(
    method: &Method,
    headers: &HeaderMap,
    caller: Caller,
    source: ValidatedTrackSource,
    delivery: DeliveryDecision,
    target: DeliveryTarget,
) -> Result<Response<Body>, AppError> {
    let key = cache_key(&source, &delivery, target);
    let cache = STATE.generation().transcode_cache.clone();

    if method == Method::HEAD {
        let found = tokio::task::spawn_blocking(move || cache.find_ready(&key)).await??;
        return match found {
            Some(ready) => {
                open_file_response(ready.file, ready.size, delivery.content_type, headers)
            }
            None => unfinished_head_response(delivery.content_type, target),
        };
    }

    let streaming = target.is_streaming();
    let version = source.version.clone();
    let full_path = source.full_path.clone();
    let spec = EncodeSpec {
        input_unchanged: Box::new(move || {
            SourceVersion::read(version.source_public_id.clone(), &full_path)
                .is_ok_and(|current| current == version)
        }),
        input_path: source.input_path,
        start_ms: source.start_ms,
        end_ms: source.end_ms,
        extension: delivery.output_format.extension(),
        configure: Box::new(move |output| {
            let output = if streaming {
                output.streaming()
            } else {
                output
            };
            delivery.configure_output(output)
        }),
    };
    let dir = STATE.boot.get().transcode_cache_dir();
    let lookup = tokio::task::spawn_blocking(move || {
        cache.get_or_start(key, &dir, spec, || try_acquire_for(&caller))
    })
    .await?
    .map_err(refusal_to_app_error)?;
    match lookup {
        Lookup::Ready(ready) => {
            open_file_response(ready.file, ready.size, delivery.content_type, headers)
        }
        Lookup::Encoding(encoding) if streaming => {
            let mut encoding = encoding;
            encoding.started().await?;
            encoding_stream_response(
                delivery.content_type,
                Body::from_stream(encoding.into_stream()),
            )
        }
        Lookup::Encoding(encoding) => {
            // Nothing is sent until the file is complete, so the encode must not depend on the
            // client still waiting: a retry then finds it finished or further along.
            encoding.keep_running();
            let ready = encoding.finished().await?;
            open_file_response(ready.file, ready.size, delivery.content_type, headers)
        }
    }
}

fn refusal_to_app_error(err: anyhow::Error) -> AppError {
    if err.is::<Saturated>() {
        return AppError::too_many_requests(SATURATED_RETRY_AFTER);
    }
    match err.downcast::<TranscodeUnavailable>() {
        Ok(unavailable) => {
            AppError::service_unavailable_for(unavailable.to_string(), UNAVAILABLE_RETRY_AFTER)
        }
        Err(err) => AppError::from(err),
    }
}

/// The headers a `GET` would answer with while the transcode is not finished, without its length.
fn unfinished_head_response(
    content_type: &str,
    target: DeliveryTarget,
) -> Result<Response<Body>, AppError> {
    if target.is_streaming() {
        return encoding_stream_response(content_type, head_body());
    }
    Ok(Response::builder()
        .header(header::CONTENT_TYPE, content_type)
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::CACHE_CONTROL, NO_STORE)
        .header(header::PRAGMA, "no-cache")
        .header(header::EXPIRES, "0")
        .body(head_body())?)
}

fn encoding_stream_response(content_type: &str, body: Body) -> Result<Response<Body>, AppError> {
    Ok(Response::builder()
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CACHE_CONTROL, NO_STORE)
        .header(header::TRANSFER_ENCODING, "chunked")
        .header(header::ACCEPT_RANGES, "none")
        .header(header::PRAGMA, "no-cache")
        .header(header::EXPIRES, "0")
        .body(body)?)
}
