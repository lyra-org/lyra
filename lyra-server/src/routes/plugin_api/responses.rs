// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use axum::body::Bytes;
use axum::http::header::{
    CACHE_CONTROL,
    EXPIRES,
    PRAGMA,
    RETRY_AFTER,
    TRANSFER_ENCODING,
};

use super::*;
use crate::plugins::api::options::{
    CoverRequest,
    TrackResponseOptions,
    TrackStreamRequest,
};
use crate::plugins::executor::{
    ApiFallbackResponse,
    ApiHandlerResponse,
    ApiResponseBody,
    ApiResponseKind,
};
use crate::routes::AppError;
use crate::routes::covers::serve_cover;
use crate::routes::settle_if_range;
use crate::routes::streamed_body::streamed_body;
use crate::services::auth::media_tokens::MediaTokenPurpose;
use crate::services::delivery::{
    DeliveryTarget,
    require_plugin_track_access,
    stream_track_access,
};

/// The headers that frame the bodies Lyra serves for `cover`, `stream_track`, `download_track`,
/// `hls_playlist`, `filler` and `stream` responses, which those responses may not set.
const FRAMING_HEADERS: [HeaderName; 4] = [
    CONTENT_LENGTH,
    CONTENT_RANGE,
    ACCEPT_RANGES,
    TRANSFER_ENCODING,
];

/// The headers of Lyra's error that a client acts on, which a fallback sent in its place keeps.
const CARRIED_ERROR_HEADERS: [HeaderName; 1] = [RETRY_AFTER];

pub(super) fn header_pairs(headers: &HeaderMap) -> Vec<(String, String)> {
    headers
        .iter()
        .filter_map(|(name, value)| {
            Some((name.as_str().to_string(), value.to_str().ok()?.to_string()))
        })
        .collect()
}

pub(super) async fn plugin_api_response_to_axum(
    mut response: ApiHandlerResponse,
    request_method: &Method,
    request_headers: &HeaderMap,
) -> Result<Response> {
    let headers = framed_headers(response.kind, &response.headers)?;
    let status = StatusCode::from_u16(response.status).context("invalid response status code")?;
    let fallback = response.fallback.take().map(Fallback::of).transpose()?;
    // Lyra's own bodies carry no validators, so an If-Range can only match the plugin's.
    let request_headers = &*settle_if_range(request_headers, &headers);
    let served = match response.kind {
        ApiResponseKind::StreamTrack
        | ApiResponseKind::DownloadTrack
        | ApiResponseKind::HlsPlaylist => {
            let status = served_status(response.kind, status)?;
            let track = served_track(&response)?;
            serve_track(&response, track, request_method, request_headers)
                .await
                .map(|served| with_plugin_status_and_headers(served, status, headers))
        }
        ApiResponseKind::Cover => {
            let status = served_status(response.kind, status)?;
            let CoverRequest { cover_id, options } =
                response.cover.context("cover response requires cover_id")?;
            serve_cover(&cover_id, options, request_headers)
                .await
                .map(|served| with_plugin_status_and_headers(served, status, headers))
        }
        ApiResponseKind::Json
        | ApiResponseKind::Empty
        | ApiResponseKind::Text
        | ApiResponseKind::Bytes
        | ApiResponseKind::Redirect
        | ApiResponseKind::Filler
        | ApiResponseKind::Stream => {
            return body_response(status, headers, response.body);
        }
    };
    match (served, fallback) {
        (Ok(served), _) => Ok(served),
        (Err(error), None) => Ok(error.into_response()),
        (Err(error), Some(fallback)) => fallback.answer(error),
    }
}

/// A response's fallback, checked before Lyra serves the response.
struct Fallback {
    status: StatusCode,
    headers: HeaderMap,
    body: Option<ApiResponseBody>,
}

impl Fallback {
    fn of(fallback: ApiFallbackResponse) -> Result<Self> {
        Ok(Self {
            status: StatusCode::from_u16(fallback.status)
                .context("invalid fallback status code")?,
            headers: framed_headers(fallback.kind, &fallback.headers)?,
            body: fallback.body,
        })
    }

    /// The fallback in place of the response Lyra failed to serve with `error`, with the headers
    /// of Lyra's error that a client acts on.
    fn answer(self, error: AppError) -> Result<Response> {
        tracing::debug!(%error, "answering a failed plugin response with its fallback");
        // Lyra's own answer also logs an internal error and acts on a poisoned database.
        let lyra = error.into_response();
        let mut response = body_response(self.status, self.headers, self.body)?;
        for name in CARRIED_ERROR_HEADERS {
            if let Some(value) = lyra.headers().get(&name) {
                response.headers_mut().insert(name, value.clone());
            }
        }
        Ok(response)
    }
}

/// A response whose body is whole once built.
fn body_response(
    status: StatusCode,
    headers: HeaderMap,
    body: Option<ApiResponseBody>,
) -> Result<Response> {
    let mut content_type = None::<HeaderValue>;
    let mut content_length = None::<HeaderValue>;
    let body = match body {
        Some(ApiResponseBody::Json(value)) => {
            content_type = Some(HeaderValue::from_static("application/json"));
            Body::from(serde_json::to_vec(&value)?)
        }
        Some(ApiResponseBody::Bytes(bytes)) => Body::from(bytes),
        Some(ApiResponseBody::Filler(size)) => {
            content_type = Some(HeaderValue::from_static("application/octet-stream"));
            content_length = Some(HeaderValue::from(size));
            filler_body(size)
        }
        Some(ApiResponseBody::Stream(stream)) => {
            content_type = Some(HeaderValue::from_static("application/octet-stream"));
            streamed_body(stream)
        }
        None => Body::empty(),
    };
    let mut response = with_headers(status, body, headers, content_type)?;
    if let Some(content_length) = content_length {
        response
            .headers_mut()
            .insert(CONTENT_LENGTH, content_length);
    }
    Ok(response)
}

/// A response of `status` and `body` with `headers`, and `content_type` where they set none.
fn with_headers(
    status: StatusCode,
    body: Body,
    headers: HeaderMap,
    content_type: Option<HeaderValue>,
) -> Result<Response> {
    let mut response = Response::builder()
        .status(status)
        .body(body)
        .map_err(|err| anyhow::anyhow!("failed to build response: {err}"))?;
    *response.headers_mut() = headers;
    if let Some(content_type) = content_type {
        response
            .headers_mut()
            .entry(CONTENT_TYPE)
            .or_insert(content_type);
    }
    Ok(response)
}

/// `size` zero bytes, sent a chunk at a time from one shared buffer.
fn filler_body(size: u64) -> Body {
    static CHUNK: [u8; 64 * 1024] = [0; 64 * 1024];
    let chunk_len = CHUNK.len() as u64;
    let chunks = (0..size.div_ceil(chunk_len)).map(move |index| {
        let len = (size - index * chunk_len).min(chunk_len) as usize;
        Ok::<_, std::convert::Infallible>(Bytes::from_static(&CHUNK[..len]))
    });
    Body::from_stream(futures::stream::iter(chunks))
}

/// A response's headers, refusing the framing headers on the kinds whose body Lyra frames.
fn framed_headers(kind: ApiResponseKind, pairs: &[(String, String)]) -> Result<HeaderMap> {
    let framed = matches!(
        kind,
        ApiResponseKind::Cover
            | ApiResponseKind::StreamTrack
            | ApiResponseKind::DownloadTrack
            | ApiResponseKind::HlsPlaylist
            | ApiResponseKind::Filler
            | ApiResponseKind::Stream
    );
    let mut headers = HeaderMap::with_capacity(pairs.len());
    for (name, value) in pairs {
        let name = HeaderName::from_bytes(name.as_bytes())
            .with_context(|| format!("invalid header name '{name}'"))?;
        if framed && FRAMING_HEADERS.contains(&name) {
            bail!(
                "{kind} responses cannot set the {name} header; Lyra sets it for the body it serves"
            );
        }
        let value = HeaderValue::from_str(value)
            .with_context(|| format!("invalid header value for '{name}'"))?;
        headers.insert(name, value);
    }
    Ok(headers)
}

/// The status a track or cover response asks to replace Lyra's 200 with: one that carries the
/// whole body.
fn served_status(kind: ApiResponseKind, status: StatusCode) -> Result<StatusCode> {
    if !(200..=203).contains(&status.as_u16()) {
        bail!("{kind} response status must be 200 to 203, got {status}");
    }
    Ok(status)
}

/// `served` with the plugin's status in place of a 200 and its headers over Lyra's. A 416 is
/// left as Lyra answers it.
fn with_plugin_status_and_headers(
    mut served: Response,
    status: StatusCode,
    headers: HeaderMap,
) -> Response {
    if !served.status().is_success() {
        return served;
    }
    if served.status() == StatusCode::OK {
        *served.status_mut() = status;
    }
    if headers.contains_key(CACHE_CONTROL) {
        // The plugin's caching replaces Lyra's, including the HTTP/1.0 headers that say no-store.
        served.headers_mut().remove(PRAGMA);
        served.headers_mut().remove(EXPIRES);
    }
    for (name, value) in &headers {
        served.headers_mut().insert(name, value.clone());
    }
    served
}

/// The track a `stream_track`, `download_track` or `hls_playlist` response serves, and the
/// options it is served with.
fn served_track(response: &ApiHandlerResponse) -> Result<(&str, TrackResponseOptions)> {
    let kind = response.kind;
    let track_id = response
        .track_id
        .as_deref()
        .with_context(|| format!("{kind} response requires track_id"))?;
    let options = response
        .options
        .clone()
        .with_context(|| format!("{kind} response has no options"))?;
    Ok((track_id, options))
}

/// What Lyra answers a `stream_track`, `download_track` or `hls_playlist` response with, before
/// the plugin's status and headers apply. A 416 for an unsatisfiable range is an answer, not an
/// error.
async fn serve_track(
    response: &ApiHandlerResponse,
    (track_id, options): (&str, TrackResponseOptions),
    request_method: &Method,
    request_headers: &HeaderMap,
) -> Result<Response, AppError> {
    let principal = response.principal.as_ref();
    match options {
        TrackResponseOptions::Hls(options) => {
            let (track_id, access) =
                require_plugin_track_access(principal, track_id, MediaTokenPurpose::HlsPlaylist)
                    .await?;
            serve_hls_playlist_for_track(request_method, &track_id, &access, options).await
        }
        TrackResponseOptions::Stream(TrackStreamRequest {
            public,
            wait_for_transcode,
            options,
        }) => {
            let (track_id, access) =
                stream_track_access(principal, response.client, track_id, public).await?;
            track_response(
                request_method,
                request_headers,
                &track_id,
                &access,
                options,
                DeliveryTarget::Stream,
                wait_for_transcode,
            )
            .await
        }
        TrackResponseOptions::Download(options) => {
            let (track_id, access) =
                require_plugin_track_access(principal, track_id, MediaTokenPurpose::Download)
                    .await?;
            track_response(
                request_method,
                request_headers,
                &track_id,
                &access,
                options,
                DeliveryTarget::Download,
                false,
            )
            .await
        }
    }
}

#[cfg(test)]
mod tests {
    use axum::http::HeaderMap;

    use crate::plugins::api::options::{
        CoverRequest,
        TrackResponseOptions,
        TrackStreamRequest,
    };
    use crate::services::auth::caller::{
        Caller,
        ClientAddress,
    };
    use crate::services::covers::CoverTransformOptions;
    use crate::services::delivery::HlsPlaylistOptions;
    use std::collections::HashSet;
    use std::time::{
        SystemTime,
        UNIX_EPOCH,
    };

    use anyhow::Context as _;
    use axum::http::{
        Method,
        StatusCode,
    };
    use axum::response::IntoResponse;

    use super::{
        plugin_api_response_to_axum,
        require_plugin_track_access,
        stream_track_access,
    };
    use crate::plugins::executor::{
        ApiFallbackResponse,
        ApiHandlerResponse,
        ApiResponseBody,
        ApiResponseKind,
    };
    use crate::routes::AppError;
    use crate::{
        plugins::testing::init_library_test_state,
        services::STATE,
        services::auth::{
            Principal,
            media_tokens::MediaTokenPurpose,
        },
        services::testing::runtime_test_lock,
    };
    use lyra_db::Permission;

    async fn status_of(
        principal: Option<&Principal>,
        track_id: &str,
        purpose: MediaTokenPurpose,
    ) -> Result<(), StatusCode> {
        require_plugin_track_access(principal, track_id, purpose)
            .await
            .map(|_| ())
            .map_err(|error| AppError::from(error).into_response().status())
    }

    fn address(raw: &str) -> ClientAddress {
        ClientAddress::new(raw.parse().expect("valid ip"))
    }

    fn unique_test_dir(prefix: &str) -> anyhow::Result<std::path::PathBuf> {
        Ok(std::env::temp_dir().join(format!(
            "{prefix}-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        )))
    }

    #[tokio::test]
    async fn plugin_track_access_enforces_library_access_and_permissions() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = unique_test_dir("lyra-plugin-bridge-auth-test")?;
        std::fs::create_dir_all(&test_dir)?;
        init_library_test_state(test_dir.clone()).await?;

        let (user_db_id, user_public_id, library_public_id, track_id) = {
            let mut db = STATE.db.write().await;
            let user = lyra_db::fixtures::test_user("plugin-bridge-user")?;
            let user_public_id = user.id.clone();
            let user_db_id = lyra_db::users::create(&mut db, &user)?;
            let library_db_id = lyra_db::test_db::insert_library(
                &mut db,
                "Plugin Bridge Lib",
                "/tmp/plugin-bridge",
            )?;
            let library_public_id = lyra_db::lookup::find_id_by_db_id(&*db, library_db_id)?
                .context("inserted library has public id")?;
            let track_db_id = lyra_db::fixtures::insert_track(&mut db, "Plugin Bridge Track")?;
            lyra_db::fixtures::connect(&mut db, library_db_id, track_db_id)?;
            let track_id = lyra_db::lookup::find_id_by_db_id(&*db, track_db_id)?
                .context("inserted track has public id")?;
            (user_db_id, user_public_id, library_public_id, track_id)
        };

        let principal = |permissions: Vec<Permission>, library_ids: HashSet<String>| {
            Principal::from_parts(
                user_db_id,
                user_public_id.clone(),
                "plugin-bridge-user".to_string(),
                permissions,
                None,
                library_ids,
            )
        };
        let accessible_libraries = HashSet::from([library_public_id.clone()]);

        assert_eq!(
            status_of(None, &track_id, MediaTokenPurpose::Stream).await,
            Err(StatusCode::UNAUTHORIZED),
            "responses without a resolved principal must not serve tracks"
        );

        let no_library_access = principal(Vec::new(), HashSet::new());
        assert_eq!(
            status_of(
                Some(&no_library_access),
                &track_id,
                MediaTokenPurpose::Stream
            )
            .await,
            Err(StatusCode::NOT_FOUND),
            "tracks outside the caller's libraries must read as not found"
        );

        let with_access = principal(Vec::new(), accessible_libraries.clone());
        assert_eq!(
            status_of(Some(&with_access), &track_id, MediaTokenPurpose::Stream).await,
            Ok(()),
            "library access must allow streaming"
        );

        assert_eq!(
            status_of(Some(&with_access), &track_id, MediaTokenPurpose::Download).await,
            Err(StatusCode::FORBIDDEN),
            "downloads require the download permission even with library access"
        );

        let downloader = principal(vec![Permission::Download], accessible_libraries.clone());
        assert_eq!(
            status_of(Some(&downloader), &track_id, MediaTokenPurpose::Download).await,
            Ok(()),
            "download permission plus library access must allow downloads"
        );

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    fn stream_options(public: bool) -> TrackResponseOptions {
        TrackResponseOptions::Stream(TrackStreamRequest {
            public,
            ..Default::default()
        })
    }

    fn stream_response(track_id: &str, public: bool) -> ApiHandlerResponse {
        ApiHandlerResponse {
            kind: ApiResponseKind::StreamTrack,
            status: 200,
            headers: Vec::new(),
            body: None,
            cover: None,
            track_id: Some(track_id.to_string()),
            options: Some(stream_options(public)),
            principal: None,
            client: ClientAddress::default(),
            fallback: None,
        }
    }

    #[tokio::test]
    async fn public_plugin_streams_skip_the_caller_access_check() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = unique_test_dir("lyra-plugin-bridge-public-test")?;
        std::fs::create_dir_all(&test_dir)?;
        init_library_test_state(test_dir.clone()).await?;

        let (track_id, release_id) = {
            let mut db = STATE.db.write().await;
            let track_db_id = lyra_db::fixtures::insert_track(&mut db, "Public Stream Track")?;
            let track_id = lyra_db::lookup::find_id_by_db_id(&*db, track_db_id)?
                .context("inserted track has public id")?;
            let release_db_id =
                lyra_db::fixtures::insert_release(&mut db, "Public Stream Release")?;
            let release_id = lyra_db::lookup::find_id_by_db_id(&*db, release_db_id)?
                .context("inserted release has public id")?;
            (track_id, release_id)
        };

        let (served_id, access) =
            stream_track_access(None, ClientAddress::default(), &track_id, true)
                .await
                .map_err(|error| anyhow::anyhow!("public access failed: {error:?}"))?;
        assert_eq!(served_id, track_id);
        assert!(matches!(
            access,
            crate::services::delivery::TrackAccess::PublicPluginRoute(_)
        ));
        assert_eq!(access.caller(), Caller::Client(ClientAddress::default()));

        let caller = crate::services::auth::Principal::from_parts(
            agdb::DbId(1),
            "caller-public-id".to_string(),
            "caller".to_string(),
            Vec::new(),
            None,
            HashSet::new(),
        );
        let (_, access) = stream_track_access(Some(&caller), address("10.0.0.1"), &track_id, true)
            .await
            .map_err(|error| anyhow::anyhow!("public access failed: {error:?}"))?;
        assert_eq!(
            access.caller(),
            Caller::User("caller-public-id".to_string()),
            "an authenticated caller's transcodes count against them on a public route"
        );
        let (_, first) = stream_track_access(None, address("10.0.0.1"), &track_id, true)
            .await
            .map_err(|error| anyhow::anyhow!("public access failed: {error:?}"))?;
        let (_, second) = stream_track_access(None, address("10.0.0.2"), &track_id, true)
            .await
            .map_err(|error| anyhow::anyhow!("public access failed: {error:?}"))?;
        assert_eq!(first.caller(), Caller::Client(address("10.0.0.1")));
        assert_eq!(
            second.caller(),
            Caller::Client(address("10.0.0.2")),
            "anonymous clients each get their own allowance"
        );

        let unknown = stream_track_access(None, ClientAddress::default(), "missing-track-id", true)
            .await
            .map(|_| ())
            .map_err(|error| AppError::from(error).into_response().status());
        assert_eq!(unknown, Err(StatusCode::NOT_FOUND));

        let headers = axum::http::HeaderMap::new();
        let private =
            plugin_api_response_to_axum(stream_response(&track_id, false), &Method::GET, &headers)
                .await?;
        assert_eq!(
            private.status(),
            StatusCode::UNAUTHORIZED,
            "streams without the public option still need a principal"
        );
        let private_head =
            plugin_api_response_to_axum(stream_response(&track_id, false), &Method::HEAD, &headers)
                .await?;
        assert_eq!(
            private_head.status(),
            StatusCode::UNAUTHORIZED,
            "HEAD fails as GET does"
        );

        let public =
            plugin_api_response_to_axum(stream_response(&release_id, true), &Method::GET, &headers)
                .await?;
        assert_eq!(
            public.status(),
            StatusCode::NOT_FOUND,
            "public streams serve tracks only"
        );

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    fn cover_response(
        cover_id: &str,
        options: CoverTransformOptions,
        status: u16,
        headers: &[(&str, &str)],
    ) -> ApiHandlerResponse {
        ApiHandlerResponse {
            kind: ApiResponseKind::Cover,
            status,
            headers: headers
                .iter()
                .map(|(name, value)| (name.to_string(), value.to_string()))
                .collect(),
            body: None,
            cover: Some(CoverRequest {
                cover_id: cover_id.to_string(),
                options,
            }),
            track_id: None,
            options: None,
            principal: None,
            client: ClientAddress::default(),
            fallback: None,
        }
    }

    fn bounded(max_width: u32) -> CoverTransformOptions {
        CoverTransformOptions {
            max_width: Some(max_width),
            ..CoverTransformOptions::default()
        }
    }

    /// A 200x200 PNG stored as a release's cover in a library at `dir`, returning the cover's
    /// public id and its file.
    async fn stored_cover(
        dir: &std::path::Path,
        hash: &str,
    ) -> anyhow::Result<(String, std::path::PathBuf)> {
        let path = dir.join("cover.png");
        image::DynamicImage::new_rgb8(200, 200).save(&path)?;
        let mut db = STATE.db.write().await;
        lyra_db::test_db::insert_library(
            &mut db,
            "Covers",
            dir.to_str().context("UTF-8 temp path")?,
        )?;
        let release = lyra_db::fixtures::insert_release(&mut db, "Release")?;
        let cover_id = db
            .transaction_mut(|t| {
                lyra_db::covers::upsert(
                    t,
                    release,
                    lyra_db::covers::CoverSource::Local,
                    lyra_db::Cover {
                        db_id: None,
                        id: nanoid::nanoid!(),
                        path: path.to_string_lossy().into_owned(),
                        mime_type: "image/png".to_string(),
                        hash: hash.to_string(),
                        blurhash: None,
                        provider_id: None,
                    },
                )
            })?
            .cover
            .id;
        Ok((cover_id, path))
    }

    #[tokio::test]
    async fn cover_responses_serve_cached_variants_with_the_plugin_status_and_headers()
    -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let dir = tempfile::tempdir()?;
        let hash = "0123456789abcdef0123456789abcdef";
        let (cover_id, _) = stored_cover(dir.path(), hash).await?;
        let plugin_headers = [
            ("cache-control", "public"),
            ("etag", "\"tag\""),
            ("x-plugin", "kept"),
        ];
        let no_range = HeaderMap::new();

        let served = plugin_api_response_to_axum(
            cover_response(&cover_id, bounded(100), 203, &plugin_headers),
            &Method::GET,
            &no_range,
        )
        .await?;
        assert_eq!(served.status(), StatusCode::NON_AUTHORITATIVE_INFORMATION);
        assert_eq!(header(&served, "cache-control"), Some("public"));
        assert_eq!(header(&served, "etag"), Some("\"tag\""));
        assert_eq!(header(&served, "x-plugin"), Some("kept"));
        assert_eq!(header(&served, "content-type"), Some("image/png"));
        assert_eq!(header(&served, "accept-ranges"), Some("bytes"));
        let body = axum::body::to_bytes(served.into_body(), usize::MAX).await?;
        let image = image::load_from_memory(&body)?;
        assert_eq!(
            (image.width(), image.height()),
            (128, 128),
            "bounds snap as the REST route's do"
        );
        let cached = STATE
            .boot
            .get()
            .cover_cache_dir
            .join(hash)
            .join("128x0.png");
        assert_eq!(
            std::fs::read(&cached)?,
            body,
            "the variant is kept in the cache"
        );

        let mut range = HeaderMap::new();
        range.insert(
            axum::http::header::RANGE,
            axum::http::HeaderValue::from_static("bytes=0-3"),
        );
        let ranged = plugin_api_response_to_axum(
            cover_response(&cover_id, bounded(100), 203, &plugin_headers),
            &Method::GET,
            &range,
        )
        .await?;
        assert_eq!(ranged.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(header(&ranged, "content-length"), Some("4"));
        assert_eq!(header(&ranged, "x-plugin"), Some("kept"));
        let ranged = axum::body::to_bytes(ranged.into_body(), usize::MAX).await?;
        assert_eq!(&ranged[..], &body[..4], "a range of the cached variant");

        for (if_range, partial) in [("\"tag\"", true), ("\"old\"", false)] {
            range.insert(
                axum::http::header::IF_RANGE,
                axum::http::HeaderValue::from_static(if_range),
            );
            let resumed = plugin_api_response_to_axum(
                cover_response(&cover_id, bounded(100), 200, &plugin_headers),
                &Method::GET,
                &range,
            )
            .await?;
            let status = resumed.status();
            let resumed = axum::body::to_bytes(resumed.into_body(), usize::MAX).await?;
            if partial {
                assert_eq!(
                    status,
                    StatusCode::PARTIAL_CONTENT,
                    "the plugin's ETag matches"
                );
                assert_eq!(&resumed[..], &body[..4]);
            } else {
                assert_eq!(
                    status,
                    StatusCode::OK,
                    "another version sends the whole body"
                );
                assert_eq!(resumed, body);
            }
        }

        let original = plugin_api_response_to_axum(
            cover_response(&cover_id, CoverTransformOptions::default(), 200, &[]),
            &Method::GET,
            &no_range,
        )
        .await?;
        assert_eq!(original.status(), StatusCode::OK);
        assert_eq!(header(&original, "content-type"), Some("image/png"));
        assert_eq!(
            header(&original, "cache-control"),
            Some("public, max-age=31536000, immutable"),
            "Lyra's headers stay where the plugin sets none"
        );
        let original = axum::body::to_bytes(original.into_body(), usize::MAX).await?;
        assert_eq!(image::load_from_memory(&original)?.width(), 200);

        let refused = plugin_api_response_to_axum(
            cover_response(&cover_id, bounded(100), 200, &[("content-length", "1")]),
            &Method::GET,
            &no_range,
        )
        .await;
        assert!(refused.is_err_and(|error| error.to_string().contains("cannot set the")));
        let refused = plugin_api_response_to_axum(
            cover_response(&cover_id, bounded(100), 206, &[]),
            &Method::GET,
            &no_range,
        )
        .await;
        assert!(refused.is_err_and(|error| error.to_string().contains("must be 200 to 203")));
        Ok(())
    }

    fn track_kind_response(
        kind: ApiResponseKind,
        track_id: &str,
        status: u16,
        headers: &[(&str, &str)],
        options: TrackResponseOptions,
    ) -> ApiHandlerResponse {
        ApiHandlerResponse {
            kind,
            status,
            headers: headers
                .iter()
                .map(|(name, value)| (name.to_string(), value.to_string()))
                .collect(),
            options: Some(options),
            ..stream_response(track_id, false)
        }
    }

    fn filler_response(size: u64, headers: &[(&str, &str)]) -> ApiHandlerResponse {
        ApiHandlerResponse {
            kind: ApiResponseKind::Filler,
            status: 200,
            headers: headers
                .iter()
                .map(|(name, value)| (name.to_string(), value.to_string()))
                .collect(),
            body: Some(crate::plugins::executor::ApiResponseBody::Filler(size)),
            track_id: None,
            options: None,
            ..stream_response("", false)
        }
    }

    #[tokio::test]
    async fn filler_responses_stream_the_bytes_they_state() -> anyhow::Result<()> {
        let headers = axum::http::HeaderMap::new();
        for size in [0, 1, 64 * 1024, 200_001] {
            let response = plugin_api_response_to_axum(
                filler_response(size, &[("x-plugin", "kept")]),
                &Method::GET,
                &headers,
            )
            .await?;
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(
                header(&response, "content-length"),
                Some(size.to_string().as_str())
            );
            assert_eq!(
                header(&response, "content-type"),
                Some("application/octet-stream")
            );
            assert_eq!(header(&response, "x-plugin"), Some("kept"));
            let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
            assert_eq!(body.len() as u64, size);
            assert!(body.iter().all(|byte| *byte == 0));
        }

        let typed = plugin_api_response_to_axum(
            filler_response(3, &[("content-type", "text/plain")]),
            &Method::GET,
            &headers,
        )
        .await?;
        assert_eq!(header(&typed, "content-type"), Some("text/plain"));

        let refused = plugin_api_response_to_axum(
            filler_response(3, &[("content-length", "1")]),
            &Method::GET,
            &headers,
        )
        .await;
        assert!(refused.is_err_and(|error| error.to_string().contains("cannot set the")));
        Ok(())
    }

    fn header<'a>(response: &'a axum::response::Response, name: &str) -> Option<&'a str> {
        response
            .headers()
            .get(name)
            .and_then(|value| value.to_str().ok())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn track_responses_keep_the_plugin_status_and_headers() -> anyhow::Result<()> {
        use crate::routes::serve::tests::{
            create_user_with_permissions,
            initialize_test_runtime,
            prepare_streamable_track,
        };

        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let track_id = prepare_streamable_track(&test_dir).await?;
        let downloader =
            create_user_with_permissions("plugin-downloader", vec![Permission::Download]).await?;
        let downloader = crate::services::auth::require_authenticated(
            crate::routes::request::bearer_credential(&downloader),
        )
        .await?;
        let plugin_headers = [
            ("cache-control", "max-age=60"),
            ("content-type", "audio/x-plugin"),
            ("x-plugin", "kept"),
        ];
        let no_range = axum::http::HeaderMap::new();

        for (kind, options) in [
            (ApiResponseKind::StreamTrack, stream_options(true)),
            (
                ApiResponseKind::DownloadTrack,
                TrackResponseOptions::Download(Default::default()),
            ),
        ] {
            let served = plugin_api_response_to_axum(
                ApiHandlerResponse {
                    principal: Some(downloader.clone()),
                    ..track_kind_response(kind, &track_id, 203, &plugin_headers, options)
                },
                &Method::GET,
                &no_range,
            )
            .await?;
            assert_eq!(
                served.status(),
                StatusCode::NON_AUTHORITATIVE_INFORMATION,
                "{kind}"
            );
            assert_eq!(header(&served, "x-plugin"), Some("kept"), "{kind}");
            assert_eq!(
                header(&served, "cache-control"),
                Some("max-age=60"),
                "{kind}"
            );
            assert_eq!(header(&served, "pragma"), None, "{kind}");
            assert_eq!(header(&served, "expires"), None, "{kind}");
            assert_eq!(
                header(&served, "content-type"),
                Some("audio/x-plugin"),
                "{kind}"
            );
            assert_eq!(header(&served, "accept-ranges"), Some("bytes"), "{kind}");
            assert!(header(&served, "content-length").is_some(), "{kind}");
        }

        let mut range = axum::http::HeaderMap::new();
        range.insert(
            axum::http::header::RANGE,
            axum::http::HeaderValue::from_static("bytes=0-9"),
        );
        let ranged = plugin_api_response_to_axum(
            track_kind_response(
                ApiResponseKind::StreamTrack,
                &track_id,
                203,
                &plugin_headers,
                stream_options(true),
            ),
            &Method::GET,
            &range,
        )
        .await?;
        assert_eq!(
            ranged.status(),
            StatusCode::PARTIAL_CONTENT,
            "a range keeps its 206"
        );
        assert_eq!(header(&ranged, "content-length"), Some("10"));
        assert!(
            header(&ranged, "content-range").is_some_and(|value| value.starts_with("bytes 0-9/"))
        );
        assert_eq!(header(&ranged, "x-plugin"), Some("kept"));

        range.insert(
            axum::http::header::RANGE,
            axum::http::HeaderValue::from_static("bytes=999999999-"),
        );
        let unsatisfiable = plugin_api_response_to_axum(
            track_kind_response(
                ApiResponseKind::StreamTrack,
                &track_id,
                203,
                &plugin_headers,
                stream_options(true),
            ),
            &Method::GET,
            &range,
        )
        .await?;
        assert_eq!(unsatisfiable.status(), StatusCode::RANGE_NOT_SATISFIABLE);
        assert_eq!(
            header(&unsatisfiable, "x-plugin"),
            None,
            "Lyra's 416 is its own"
        );

        let missing = plugin_api_response_to_axum(
            track_kind_response(
                ApiResponseKind::StreamTrack,
                "missing-track",
                203,
                &plugin_headers,
                stream_options(true),
            ),
            &Method::GET,
            &no_range,
        )
        .await?;
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            header(&missing, "x-plugin"),
            None,
            "Lyra's errors are its own"
        );

        for framing in [
            "content-length",
            "Content-Range",
            "accept-ranges",
            "transfer-encoding",
        ] {
            let refused = plugin_api_response_to_axum(
                track_kind_response(
                    ApiResponseKind::StreamTrack,
                    &track_id,
                    200,
                    &[(framing, "1")],
                    stream_options(true),
                ),
                &Method::GET,
                &no_range,
            )
            .await;
            assert!(
                refused.is_err_and(|error| error.to_string().contains("cannot set the")),
                "{framing} is Lyra's"
            );
        }
        for status in [204, 206, 404] {
            let refused = plugin_api_response_to_axum(
                track_kind_response(
                    ApiResponseKind::DownloadTrack,
                    &track_id,
                    status,
                    &[],
                    stream_options(true),
                ),
                &Method::GET,
                &no_range,
            )
            .await;
            assert!(
                refused.is_err_and(|error| error.to_string().contains("must be 200 to 203")),
                "status {status}"
            );
        }

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn plugin_downloads_need_the_download_permission() -> anyhow::Result<()> {
        use crate::routes::serve::tests::{
            create_user_with_permissions,
            initialize_test_runtime,
            prepare_streamable_track,
        };

        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let track_id = prepare_streamable_track(&test_dir).await?;
        let headers = axum::http::HeaderMap::new();
        let download = |principal| ApiHandlerResponse {
            principal,
            ..track_kind_response(
                ApiResponseKind::DownloadTrack,
                &track_id,
                200,
                &[],
                TrackResponseOptions::Download(Default::default()),
            )
        };
        let authenticate = |headers: HeaderMap| async move {
            crate::services::auth::require_authenticated(crate::routes::request::bearer_credential(
                &headers,
            ))
            .await
        };

        let listener =
            authenticate(create_user_with_permissions("plugin-listener", Vec::new()).await?)
                .await?;
        let refused =
            plugin_api_response_to_axum(download(Some(listener)), &Method::GET, &headers).await?;
        assert_eq!(refused.status(), StatusCode::FORBIDDEN);
        let anonymous = plugin_api_response_to_axum(download(None), &Method::GET, &headers).await?;
        assert_eq!(anonymous.status(), StatusCode::UNAUTHORIZED);
        let downloader = authenticate(
            create_user_with_permissions("plugin-downloader", vec![Permission::Download]).await?,
        )
        .await?;
        let allowed =
            plugin_api_response_to_axum(download(Some(downloader)), &Method::GET, &headers).await?;
        assert_eq!(allowed.status(), StatusCode::OK);

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn hls_playlists_keep_the_plugin_status_and_headers() -> anyhow::Result<()> {
        use crate::routes::serve::tests::{
            create_user_with_permissions,
            initialize_test_runtime,
            prepare_streamable_track,
        };

        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let track_id = prepare_streamable_track(&test_dir).await?;
        let listener = create_user_with_permissions("plugin-hls-listener", Vec::new()).await?;
        let listener = crate::services::auth::require_authenticated(
            crate::routes::request::bearer_credential(&listener),
        )
        .await?;
        let playlist = |options: HlsPlaylistOptions| ApiHandlerResponse {
            principal: Some(listener.clone()),
            ..track_kind_response(
                ApiResponseKind::HlsPlaylist,
                &track_id,
                201,
                &[("content-type", "application/vnd.apple.mpegurl")],
                TrackResponseOptions::Hls(options),
            )
        };

        let served = plugin_api_response_to_axum(
            playlist(HlsPlaylistOptions {
                codec: Some("aac".to_string()),
                ..Default::default()
            }),
            &Method::HEAD,
            &axum::http::HeaderMap::new(),
        )
        .await?;
        assert_eq!(served.status(), StatusCode::CREATED);
        assert_eq!(
            header(&served, "content-type"),
            Some("application/vnd.apple.mpegurl")
        );
        assert_eq!(
            header(&served, "cache-control"),
            Some("no-cache, no-store, must-revalidate"),
            "Lyra's headers stay where the plugin sets none"
        );

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    fn text_fallback(status: u16, headers: &[(&str, &str)]) -> ApiFallbackResponse {
        ApiFallbackResponse {
            kind: ApiResponseKind::Text,
            status,
            headers: headers
                .iter()
                .map(|(name, value)| (name.to_string(), value.to_string()))
                .collect(),
            body: Some(ApiResponseBody::Bytes(b"fallback".to_vec())),
        }
    }

    async fn body_text(response: axum::response::Response) -> anyhow::Result<String> {
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
        Ok(String::from_utf8(body.to_vec())?)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn failed_track_responses_answer_with_their_fallback() -> anyhow::Result<()> {
        use crate::routes::serve::tests::{
            create_user_with_permissions,
            initialize_test_runtime,
            prepare_streamable_track,
        };

        let _guard = runtime_test_lock().await;
        let test_dir = initialize_test_runtime().await?;
        let track_id = prepare_streamable_track(&test_dir).await?;
        let listener = create_user_with_permissions("plugin-fallback-listener", Vec::new()).await?;
        let listener = crate::services::auth::require_authenticated(
            crate::routes::request::bearer_credential(&listener),
        )
        .await?;
        let plugin_headers = [("x-plugin", "kept")];
        let with_fallback = |kind, track_id: &str, options| ApiHandlerResponse {
            principal: Some(listener.clone()),
            fallback: Some(text_fallback(
                200,
                &[("content-type", "text/xml"), ("retry-after", "99")],
            )),
            ..track_kind_response(kind, track_id, 200, &plugin_headers, options)
        };
        let no_range = axum::http::HeaderMap::new();

        for (kind, track_id, options) in [
            (
                ApiResponseKind::StreamTrack,
                "missing-track",
                stream_options(false),
            ),
            (
                ApiResponseKind::DownloadTrack,
                track_id.as_str(),
                TrackResponseOptions::Download(Default::default()),
            ),
            (
                ApiResponseKind::HlsPlaylist,
                "missing-track",
                TrackResponseOptions::Hls(Default::default()),
            ),
        ] {
            let answered = plugin_api_response_to_axum(
                with_fallback(kind, track_id, options),
                &Method::GET,
                &no_range,
            )
            .await?;
            assert_eq!(answered.status(), StatusCode::OK, "{kind}");
            assert_eq!(
                header(&answered, "content-type"),
                Some("text/xml"),
                "{kind}"
            );
            assert_eq!(
                header(&answered, "x-plugin"),
                None,
                "{kind}: the failed response's headers don't apply"
            );
            assert_eq!(
                header(&answered, "retry-after"),
                Some("99"),
                "{kind}: the fallback keeps its own headers"
            );
            assert_eq!(body_text(answered).await?, "fallback", "{kind}");
        }

        let mut range = axum::http::HeaderMap::new();
        range.insert(
            axum::http::header::RANGE,
            axum::http::HeaderValue::from_static("bytes=999999999-"),
        );
        let unsatisfiable = plugin_api_response_to_axum(
            with_fallback(
                ApiResponseKind::StreamTrack,
                &track_id,
                stream_options(false),
            ),
            &Method::GET,
            &range,
        )
        .await?;
        assert_eq!(
            unsatisfiable.status(),
            StatusCode::RANGE_NOT_SATISFIABLE,
            "a 416 stays Lyra's"
        );

        let served = plugin_api_response_to_axum(
            with_fallback(
                ApiResponseKind::StreamTrack,
                &track_id,
                stream_options(false),
            ),
            &Method::GET,
            &no_range,
        )
        .await?;
        assert_eq!(served.status(), StatusCode::OK);
        assert_eq!(header(&served, "x-plugin"), Some("kept"));
        let body = axum::body::to_bytes(served.into_body(), usize::MAX).await?;
        assert_ne!(&body[..], b"fallback", "a served track ignores it");

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    #[tokio::test]
    async fn failed_cover_responses_answer_with_their_fallback() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let dir = tempfile::tempdir()?;
        let (cover_id, path) = stored_cover(dir.path(), "fedcba9876543210fedcba9876543210").await?;
        let no_range = HeaderMap::new();
        let with_fallback = |cover_id: &str| ApiHandlerResponse {
            fallback: Some(text_fallback(200, &[("content-type", "text/xml")])),
            ..cover_response(cover_id, bounded(64), 200, &[("x-plugin", "kept")])
        };

        let missing = plugin_api_response_to_axum(
            cover_response("missing", bounded(64), 200, &[]),
            &Method::GET,
            &no_range,
        )
        .await?;
        assert_eq!(
            missing.status(),
            StatusCode::NOT_FOUND,
            "without a fallback Lyra answers with its own error"
        );

        let answered =
            plugin_api_response_to_axum(with_fallback("missing"), &Method::GET, &no_range).await?;
        assert_eq!(answered.status(), StatusCode::OK);
        assert_eq!(header(&answered, "content-type"), Some("text/xml"));
        assert_eq!(header(&answered, "x-plugin"), None);
        assert_eq!(body_text(answered).await?, "fallback");

        std::fs::remove_file(&path)?;
        let gone =
            plugin_api_response_to_axum(with_fallback(&cover_id), &Method::GET, &no_range).await?;
        assert_eq!(
            body_text(gone).await?,
            "fallback",
            "a cover whose file is gone"
        );
        let db = STATE.db.read().await;
        assert!(
            lyra_db::covers::get_by_public_id(&*db, &cover_id)?.is_none(),
            "a cover confirmed gone is removed, as the REST route removes it"
        );
        Ok(())
    }

    #[tokio::test]
    async fn malformed_cover_responses_ignore_their_fallback() {
        let answered = plugin_api_response_to_axum(
            ApiHandlerResponse {
                cover: None,
                fallback: Some(text_fallback(404, &[])),
                ..cover_response("cover", CoverTransformOptions::default(), 200, &[])
            },
            &Method::GET,
            &axum::http::HeaderMap::new(),
        )
        .await;
        assert!(
            answered.is_err(),
            "a cover response without a cover was answered"
        );
    }

    #[test]
    fn fallbacks_carry_lyras_retry_after() -> anyhow::Result<()> {
        for error in [
            AppError::too_many_requests(std::time::Duration::from_secs(5)),
            AppError::service_unavailable_for("busy", std::time::Duration::from_secs(5)),
        ] {
            let answered =
                super::Fallback::of(text_fallback(200, &[("retry-after", "99")]))?.answer(error)?;
            assert_eq!(answered.status(), StatusCode::OK);
            assert_eq!(header(&answered, "retry-after"), Some("5"));
        }
        let busy = super::Fallback::of(text_fallback(200, &[]))?
            .answer(crate::services::covers::CoverTransformError::Busy.into())?;
        assert_eq!(
            header(&busy, "retry-after"),
            Some("1"),
            "busy cover transforms carry Lyra's retry-after"
        );
        let answered =
            super::Fallback::of(text_fallback(200, &[]))?.answer(AppError::not_found("gone"))?;
        assert_eq!(header(&answered, "retry-after"), None);
        Ok(())
    }
}
