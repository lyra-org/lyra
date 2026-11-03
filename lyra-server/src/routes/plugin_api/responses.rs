// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use axum::http::header::{
    CACHE_CONTROL,
    EXPIRES,
    PRAGMA,
    TRANSFER_ENCODING,
};

use super::*;
use crate::plugins::api::options::{
    TrackResponseOptions,
    TrackStreamRequest,
};
use crate::plugins::executor::{
    ApiHandlerResponse,
    ApiResponseKind,
};
use crate::routes::AppError;
use crate::services::auth::media_tokens::MediaTokenPurpose;
use crate::services::delivery::{
    DeliveryTarget,
    require_plugin_track_access,
    stream_track_access,
};

/// The headers that frame the bodies Lyra serves for `file`, `stream_track`, `download_track`,
/// `hls_playlist`, `filler` and `stream` responses, which those responses may not set.
const FRAMING_HEADERS: [HeaderName; 4] = [
    CONTENT_LENGTH,
    CONTENT_RANGE,
    ACCEPT_RANGES,
    TRANSFER_ENCODING,
];

pub(super) fn header_pairs(headers: &HeaderMap) -> Vec<(String, String)> {
    headers
        .iter()
        .filter_map(|(name, value)| {
            Some((name.as_str().to_string(), value.to_str().ok()?.to_string()))
        })
        .collect()
}

pub(super) async fn plugin_api_response_to_axum(
    response: ApiHandlerResponse,
    request_method: &Method,
    request_headers: &HeaderMap,
) -> Result<Response> {
    let headers = response_headers(&response)?;
    let mut status =
        StatusCode::from_u16(response.status).context("invalid response status code")?;
    match response.kind {
        ApiResponseKind::StreamTrack
        | ApiResponseKind::DownloadTrack
        | ApiResponseKind::HlsPlaylist => {
            let status = served_status(response.kind, status)?;
            let served = serve_track(&response, request_method, request_headers).await?;
            return Ok(with_plugin_status_and_headers(served, status, headers));
        }
        ApiResponseKind::Json
        | ApiResponseKind::Empty
        | ApiResponseKind::Text
        | ApiResponseKind::Bytes
        | ApiResponseKind::Redirect
        | ApiResponseKind::File
        | ApiResponseKind::Filler
        | ApiResponseKind::Stream => {}
    }

    let principal = response.principal.as_ref();
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
        let confined =
            crate::services::libraries::confine_to_media_roots(trimmed, principal).await?;
        if let Some(transform) = parse_image_transform_options(response.transform.as_ref())? {
            let owned_path = confined
                .to_str()
                .with_context(|| format!("file response path '{trimmed}' is not valid UTF-8"))?
                .to_owned();
            let (bytes, format) =
                tokio::task::spawn_blocking(move || transform_image(&owned_path, &transform))
                    .await
                    .map_err(|err| {
                        anyhow::anyhow!("failed to join image transform task: {err}")
                    })??;
            content_type = Some(HeaderValue::from_static(image_format_mime(format)));
            Body::from(bytes)
        } else {
            let ranged =
                build_ranged_file_body(confined.as_path(), request_headers.get(RANGE), status)
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
            Some(crate::plugins::executor::ApiResponseBody::Filler(size)) => {
                content_type = Some(HeaderValue::from_static("application/octet-stream"));
                content_length = Some(HeaderValue::from(size));
                filler_body(size)
            }
            Some(crate::plugins::executor::ApiResponseBody::Stream(stream)) => {
                content_type = Some(HeaderValue::from_static("application/octet-stream"));
                Body::from_stream(stream)
            }
            None => Body::empty(),
        }
    };

    let mut response_out = Response::builder()
        .status(status)
        .body(body)
        .map_err(|err| anyhow::anyhow!("failed to build response: {err}"))?;
    *response_out.headers_mut() = headers;
    if let Some(content_type) = content_type {
        response_out
            .headers_mut()
            .entry(CONTENT_TYPE)
            .or_insert(content_type);
    }
    for (name, value) in [
        (CONTENT_LENGTH, content_length),
        (CONTENT_RANGE, content_range),
        (ACCEPT_RANGES, accept_ranges),
    ] {
        if let Some(value) = value {
            response_out.headers_mut().insert(name, value);
        }
    }
    Ok(response_out)
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

/// The response's headers, refusing the framing headers on the kinds whose body Lyra frames.
fn response_headers(response: &ApiHandlerResponse) -> Result<HeaderMap> {
    let framed = matches!(
        response.kind,
        ApiResponseKind::File
            | ApiResponseKind::StreamTrack
            | ApiResponseKind::DownloadTrack
            | ApiResponseKind::HlsPlaylist
            | ApiResponseKind::Filler
            | ApiResponseKind::Stream
    );
    let mut headers = HeaderMap::with_capacity(response.headers.len());
    for (name, value) in &response.headers {
        let name = HeaderName::from_bytes(name.as_bytes())
            .with_context(|| format!("invalid header name '{name}'"))?;
        if framed && FRAMING_HEADERS.contains(&name) {
            bail!(
                "{} responses cannot set the {name} header; Lyra sets it for the body it serves",
                response.kind
            );
        }
        let value = HeaderValue::from_str(value)
            .with_context(|| format!("invalid header value for '{name}'"))?;
        headers.insert(name, value);
    }
    Ok(headers)
}

/// The status a track response asks to replace Lyra's 200 with: one that carries the whole body.
fn served_status(kind: ApiResponseKind, status: StatusCode) -> Result<StatusCode> {
    if !(200..=203).contains(&status.as_u16()) {
        bail!("{kind} response status must be 200 to 203, got {status}");
    }
    Ok(status)
}

/// `served` with the plugin's status in place of a 200 and its headers over Lyra's. Lyra's
/// errors, a 416 among them, are left as Lyra answers them.
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

/// What Lyra answers a `stream_track`, `download_track` or `hls_playlist` response with, before
/// the plugin's status and headers apply.
async fn serve_track(
    response: &ApiHandlerResponse,
    request_method: &Method,
    request_headers: &HeaderMap,
) -> Result<Response> {
    let kind = response.kind;
    let principal = response.principal.as_ref();
    let track_id = response
        .track_id
        .as_deref()
        .with_context(|| format!("{kind} response requires track_id"))?;
    let options = response
        .options
        .clone()
        .with_context(|| format!("{kind} response has no options"))?;
    let served = match options {
        TrackResponseOptions::Hls(options) => {
            let (track_id, access) = match require_plugin_track_access(
                principal,
                track_id,
                MediaTokenPurpose::HlsPlaylist,
            )
            .await
            {
                Ok(resolved) => resolved,
                Err(error) => return Ok(AppError::from(error).into_response()),
            };
            serve_hls_playlist_for_track(request_method, &track_id, &access, options).await
        }
        TrackResponseOptions::Stream(TrackStreamRequest {
            public,
            wait_for_transcode,
            options,
        }) => {
            let (track_id, access) =
                match stream_track_access(principal, response.client, track_id, public).await {
                    Ok(resolved) => resolved,
                    Err(error) => return Ok(AppError::from(error).into_response()),
                };
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
                match require_plugin_track_access(principal, track_id, MediaTokenPurpose::Download)
                    .await
                {
                    Ok(resolved) => resolved,
                    Err(error) => return Ok(AppError::from(error).into_response()),
                };
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
    };
    Ok(served.unwrap_or_else(IntoResponse::into_response))
}

#[cfg(test)]
mod tests {
    use axum::http::HeaderMap;

    use crate::plugins::api::options::{
        TrackResponseOptions,
        TrackStreamRequest,
    };
    use crate::services::auth::caller::{
        Caller,
        ClientAddress,
    };
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
        ApiHandlerResponse,
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
            path: None,
            transform: None,
            track_id: Some(track_id.to_string()),
            options: Some(stream_options(public)),
            principal: None,
            client: ClientAddress::default(),
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

    fn file_response(path: &std::path::Path) -> ApiHandlerResponse {
        ApiHandlerResponse {
            kind: ApiResponseKind::File,
            status: 200,
            headers: Vec::new(),
            body: None,
            path: Some(path.to_string_lossy().into_owned()),
            transform: None,
            track_id: None,
            options: None,
            principal: None,
            client: ClientAddress::default(),
        }
    }

    #[tokio::test]
    async fn plugin_file_responses_are_confined_to_library_roots() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = unique_test_dir("lyra-plugin-bridge-file-test")?;
        let library_dir = test_dir.join("library");
        std::fs::create_dir_all(&library_dir)?;
        let outside_dir = unique_test_dir("lyra-plugin-bridge-file-outside")?;
        std::fs::create_dir_all(&outside_dir)?;
        init_library_test_state(test_dir.clone()).await?;

        let hidden_dir = test_dir.join("hidden");
        std::fs::create_dir_all(&hidden_dir)?;
        let principal = {
            let mut db = STATE.db.write().await;
            let library_db_id = lyra_db::test_db::insert_library(
                &mut db,
                "File Bridge Lib",
                library_dir.to_str().context("library dir is utf-8")?,
            )?;
            lyra_db::test_db::insert_library(
                &mut db,
                "File Bridge Hidden",
                hidden_dir.to_str().context("hidden dir is utf-8")?,
            )?;
            let library_public_id = lyra_db::lookup::find_id_by_db_id(&*db, library_db_id)?
                .context("inserted library has public id")?;
            let user = lyra_db::fixtures::test_user("file-bridge-user")?;
            let user_public_id = user.id.clone();
            let user_db_id = lyra_db::users::create(&mut db, &user)?;
            Principal::from_parts(
                user_db_id,
                user_public_id,
                "file-bridge-user".to_string(),
                Vec::new(),
                None,
                HashSet::from([library_public_id]),
            )
        };

        let inside = library_dir.join("cover.jpg");
        std::fs::write(&inside, b"inside")?;
        let outside = outside_dir.join("secret.txt");
        std::fs::write(&outside, b"outside")?;
        let escape_link = library_dir.join("escape.jpg");
        std::os::unix::fs::symlink(&outside, &escape_link)?;

        let headers = axum::http::HeaderMap::new();

        let served =
            plugin_api_response_to_axum(file_response(&inside), &Method::GET, &headers).await?;
        assert_eq!(
            served.status(),
            StatusCode::OK,
            "files inside a library root must serve"
        );

        let rejected =
            plugin_api_response_to_axum(file_response(&outside), &Method::GET, &headers).await;
        assert!(
            rejected.is_err_and(|err| err.to_string().contains("outside library")),
            "files outside all roots must be rejected"
        );

        let symlinked =
            plugin_api_response_to_axum(file_response(&escape_link), &Method::GET, &headers).await;
        assert!(
            symlinked.is_err_and(|err| err.to_string().contains("outside library")),
            "symlinks escaping a library root must be rejected"
        );

        let hidden = hidden_dir.join("cover.jpg");
        std::fs::write(&hidden, b"hidden")?;
        let as_principal = |path: &std::path::Path| ApiHandlerResponse {
            principal: Some(principal.clone()),
            client: ClientAddress::default(),
            ..file_response(path)
        };
        let served =
            plugin_api_response_to_axum(as_principal(&inside), &Method::GET, &headers).await?;
        assert_eq!(served.status(), StatusCode::OK);
        let rejected =
            plugin_api_response_to_axum(as_principal(&hidden), &Method::GET, &headers).await;
        assert!(
            rejected.is_err_and(|err| err.to_string().contains("outside library")),
            "files in libraries the principal can't see must be rejected"
        );
        let served =
            plugin_api_response_to_axum(file_response(&hidden), &Method::GET, &headers).await?;
        assert_eq!(
            served.status(),
            StatusCode::OK,
            "responses without a principal serve from every library"
        );

        let _ = std::fs::remove_dir_all(test_dir);
        let _ = std::fs::remove_dir_all(outside_dir);
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
}
