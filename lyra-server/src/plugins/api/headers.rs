// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use super::*;
use crate::plugins::executor::ApiResponseKind;

pub(super) fn header_pairs(headers: &HeaderMap) -> Vec<(String, String)> {
    headers
        .iter()
        .filter_map(|(name, value)| {
            Some((name.as_str().to_string(), value.to_str().ok()?.to_string()))
        })
        .collect()
}

pub(super) async fn plugin_api_response_to_axum(
    response: crate::plugins::executor::ApiHandlerResponse,
    request_headers: &HeaderMap,
) -> Result<Response> {
    let mut status =
        StatusCode::from_u16(response.status).context("invalid response status code")?;
    match response.kind {
        ApiResponseKind::StreamTrack => {
            let track_id = response
                .track_id
                .ok_or_else(|| anyhow::anyhow!("stream_track response requires track_id"))?;
            let options = parse_track_serve_options(response.options.as_ref())?;
            return Ok(
                match stream_track_response(
                    request_headers,
                    agdb::DbId(track_id),
                    options.format,
                    join_preferred_codecs(options.preferred_codecs),
                    options.bitrate_bps,
                    options.sample_rate_hz,
                    options.channels,
                    options.prefer_vbr,
                    options.start_offset_ms,
                )
                .await
                {
                    Ok(response) => response,
                    Err(error) => error.into_response(),
                },
            );
        }
        ApiResponseKind::DownloadTrack => {
            let track_id = response
                .track_id
                .ok_or_else(|| anyhow::anyhow!("download_track response requires track_id"))?;
            let options = parse_track_serve_options(response.options.as_ref())?;
            return Ok(
                match download_track_response(
                    request_headers,
                    agdb::DbId(track_id),
                    options.format,
                    join_preferred_codecs(options.preferred_codecs),
                    options.bitrate_bps,
                    options.sample_rate_hz,
                    options.channels,
                    options.prefer_vbr,
                    options.start_offset_ms,
                    None,
                )
                .await
                {
                    Ok(response) => response,
                    Err(error) => error.into_response(),
                },
            );
        }
        ApiResponseKind::HlsPlaylist => {
            let track_id = response
                .track_id
                .ok_or_else(|| anyhow::anyhow!("hls_playlist response requires track_id"))?;
            let options = parse_hls_serve_options(response.options.as_ref())?;
            return Ok(
                match serve_hls_playlist_for_track(
                    agdb::DbId(track_id),
                    join_preferred_codecs(options.preferred_codecs),
                    options.bitrate_bps,
                    options.sample_rate_hz,
                    options.channels,
                    options.prefer_vbr,
                    options.start_offset_ms,
                )
                .await
                {
                    Ok(response) => response,
                    Err(error) => error.into_response(),
                },
            );
        }
        ApiResponseKind::Json
        | ApiResponseKind::Empty
        | ApiResponseKind::Text
        | ApiResponseKind::Bytes
        | ApiResponseKind::Redirect
        | ApiResponseKind::File => {}
    }

    let mut content_type = None::<HeaderValue>;
    let mut content_length = None::<HeaderValue>;
    let mut content_range = None::<HeaderValue>;
    let mut accept_ranges = None::<HeaderValue>;

    let body = if let Some(path) = response.path {
        if response.kind != ApiResponseKind::File {
            bail!("{} response cannot include path", response.kind);
        }
        let trimmed = path.trim();
        if trimmed.is_empty() {
            bail!("file path must not be empty");
        }
        if let Some(transform) = parse_image_transform_options(response.transform.as_ref())? {
            let owned_path = trimmed.to_owned();
            let (bytes, format) =
                tokio::task::spawn_blocking(move || transform_image(&owned_path, &transform))
                    .await
                    .map_err(|err| {
                        anyhow::anyhow!("failed to join image transform task: {err}")
                    })??;
            content_type = Some(HeaderValue::from_static(image_format_mime(format)));
            Body::from(bytes)
        } else {
            let ranged = build_ranged_file_body(
                FsPath::new(trimmed),
                request_headers.get(RANGE),
                status,
                None,
            )
            .await?;
            status = ranged.status;
            content_type = Some(HeaderValue::from_static(infer_content_type(trimmed)));
            content_length = Some(ranged.content_length);
            content_range = ranged.content_range;
            accept_ranges = Some(HeaderValue::from_static("bytes"));
            ranged.body
        }
    } else {
        match response.body {
            Some(crate::plugins::executor::ApiResponseBody::Json(value)) => {
                content_type = Some(HeaderValue::from_static("application/json"));
                Body::from(serde_json::to_vec(&value)?)
            }
            Some(crate::plugins::executor::ApiResponseBody::Bytes(bytes)) => Body::from(bytes),
            None => Body::empty(),
        }
    };

    let mut response_out = Response::builder()
        .status(status)
        .body(body)
        .map_err(|err| anyhow::anyhow!("failed to build response: {err}"))?;

    for (name, value) in response.headers {
        let name = HeaderName::from_bytes(name.as_bytes())
            .with_context(|| format!("invalid header name '{name}'"))?;
        let value = HeaderValue::from_str(&value)
            .with_context(|| format!("invalid header value for '{name}'"))?;
        response_out.headers_mut().insert(name, value);
    }
    if let Some(content_type) = content_type {
        response_out
            .headers_mut()
            .entry(CONTENT_TYPE)
            .or_insert(content_type);
    }
    if let Some(content_length) = content_length {
        response_out
            .headers_mut()
            .entry(CONTENT_LENGTH)
            .or_insert(content_length);
    }
    if let Some(content_range) = content_range {
        response_out
            .headers_mut()
            .insert(CONTENT_RANGE, content_range);
    }
    if let Some(accept_ranges) = accept_ranges {
        response_out
            .headers_mut()
            .insert(ACCEPT_RANGES, accept_ranges);
    }
    Ok(response_out)
}
