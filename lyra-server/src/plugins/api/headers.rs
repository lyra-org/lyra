// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use super::*;
use crate::plugins::executor::ApiResponseKind;
use crate::routes::AppError;
use crate::services::auth::Principal;
use crate::services::auth::media_tokens::MediaTokenPurpose;
use crate::services::transcoding::capacity::Account;

pub(super) fn header_pairs(headers: &HeaderMap) -> Vec<(String, String)> {
    headers
        .iter()
        .filter_map(|(name, value)| {
            Some((name.as_str().to_string(), value.to_str().ok()?.to_string()))
        })
        .collect()
}

/// Plugin responses skip the native route guards, so track serving must
/// re-apply the same permission and library-access enforcement here.
async fn require_plugin_track_access(
    principal: Option<&Principal>,
    track_db_id: agdb::DbId,
    purpose: MediaTokenPurpose,
) -> Result<(String, TrackAccess), AppError> {
    let Some(principal) = principal else {
        return Err(AppError::unauthorized("authentication required"));
    };
    let db = crate::STATE.db.read().await;
    principal.require(&*db)?;
    crate::routes::authorize_track_purpose(principal, purpose)?;
    let not_found = || AppError::not_found(format!("Track not found: {}", track_db_id.0));
    crate::services::auth::access::require_entity_accessible(
        &*db,
        principal,
        track_db_id,
        not_found,
    )?;
    let track_id =
        crate::plugins::db::lookup::find_id_by_db_id(&*db, track_db_id)?.ok_or_else(not_found)?;
    Ok((track_id, TrackAccess::Principal(principal.clone())))
}

/// A plugin route its protocol makes public streams any track, without the caller's access check.
/// Its transcodes still count against someone: the caller, when the handler resolved one, or
/// else the client's address.
async fn public_plugin_track_access(
    principal: Option<&Principal>,
    client_key: Option<&str>,
    track_db_id: agdb::DbId,
) -> Result<(String, TrackAccess), AppError> {
    let db = crate::STATE.db.read().await;
    let track_id = crate::plugins::db::lookup::find_id_by_db_id(&*db, track_db_id)?
        .ok_or_else(|| AppError::not_found(format!("Track not found: {}", track_db_id.0)))?;
    let account = match (principal, client_key) {
        (Some(principal), _) => Account::User(principal.user_public_id.clone()),
        (None, Some(client_key)) => Account::client(client_key),
        (None, None) => Account::Unknown,
    };
    Ok((track_id, TrackAccess::PublicPluginRoute(account)))
}

/// Plugins may name any filesystem path in a file response, so resolve the
/// target (following symlinks) and refuse anything outside the roots of the
/// libraries the principal can see and the configured cover storage root.
async fn confine_file_response_path(path: &str, principal: Option<&Principal>) -> Result<PathBuf> {
    let canonical = tokio::fs::canonicalize(path)
        .await
        .with_context(|| format!("failed to resolve file response path '{path}'"))?;
    let mut roots = crate::services::libraries::library_roots(principal).await?;
    roots.push(crate::services::covers::configured_covers_root());
    for root in roots {
        let Ok(root) = tokio::fs::canonicalize(&root).await else {
            continue;
        };
        if canonical.starts_with(&root) {
            return Ok(canonical);
        }
    }
    bail!("file response path '{path}' is outside library and cover roots");
}

pub(super) async fn plugin_api_response_to_axum(
    response: crate::plugins::executor::ApiHandlerResponse,
    request_method: &Method,
    request_headers: &HeaderMap,
) -> Result<Response> {
    let principal = response.principal.as_ref();
    let mut status =
        StatusCode::from_u16(response.status).context("invalid response status code")?;
    match response.kind {
        ApiResponseKind::StreamTrack => {
            let track_id = response
                .track_id
                .ok_or_else(|| anyhow::anyhow!("stream_track response requires track_id"))?;
            let track_db_id = agdb::DbId(track_id);
            let access = if parse_public_stream(response.options.as_ref())? {
                public_plugin_track_access(principal, response.client_key.as_deref(), track_db_id)
                    .await
            } else {
                require_plugin_track_access(principal, track_db_id, MediaTokenPurpose::Stream).await
            };
            let (track_id, access) = match access {
                Ok(resolved) => resolved,
                Err(error) => return Ok(error.into_response()),
            };
            let options = parse_track_serve_options(response.options.as_ref())?;
            return Ok(
                match stream_track_response(
                    request_method,
                    request_headers,
                    &track_id,
                    &access,
                    ServeTrackOptions {
                        format: options.format,
                        codec: join_preferred_codecs(options.preferred_codecs),
                        bitrate_bps: options.bitrate_bps,
                        sample_rate_hz: options.sample_rate_hz,
                        channels: options.channels,
                        prefer_vbr: options.prefer_vbr,
                        start_offset_ms: options.start_offset_ms,
                    },
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
            let (track_id, access) = match require_plugin_track_access(
                principal,
                agdb::DbId(track_id),
                MediaTokenPurpose::Download,
            )
            .await
            {
                Ok(resolved) => resolved,
                Err(error) => return Ok(error.into_response()),
            };
            let options = parse_track_serve_options(response.options.as_ref())?;
            return Ok(
                match download_track_response(
                    request_method,
                    request_headers,
                    &track_id,
                    &access,
                    ServeTrackOptions {
                        format: options.format,
                        codec: join_preferred_codecs(options.preferred_codecs),
                        bitrate_bps: options.bitrate_bps,
                        sample_rate_hz: options.sample_rate_hz,
                        channels: options.channels,
                        prefer_vbr: options.prefer_vbr,
                        start_offset_ms: options.start_offset_ms,
                    },
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
            let (track_id, access) = match require_plugin_track_access(
                principal,
                agdb::DbId(track_id),
                MediaTokenPurpose::HlsPlaylist,
            )
            .await
            {
                Ok(resolved) => resolved,
                Err(error) => return Ok(error.into_response()),
            };
            let options = parse_hls_serve_options(response.options.as_ref())?;
            return Ok(
                match serve_hls_playlist_for_track(
                    request_method,
                    &track_id,
                    &access,
                    HlsPlaylistOptions {
                        codec: join_preferred_codecs(options.preferred_codecs),
                        bitrate_bps: options.bitrate_bps,
                        sample_rate_hz: options.sample_rate_hz,
                        channels: options.channels,
                        prefer_vbr: options.prefer_vbr,
                        start_offset_ms: options.start_offset_ms,
                    },
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
        let confined = confine_file_response_path(trimmed, principal).await?;
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

#[cfg(test)]
mod tests {
    use crate::services::transcoding::capacity::Account;
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
        public_plugin_track_access,
        require_plugin_track_access,
    };
    use crate::plugins::executor::{
        ApiHandlerResponse,
        ApiResponseKind,
    };
    use crate::{
        STATE,
        plugins::db::{
            self,
            Permission,
        },
        services::auth::{
            Principal,
            media_tokens::MediaTokenPurpose,
        },
        testing::{
            LibraryFixtureConfig,
            initialize_runtime,
            runtime_test_lock,
        },
    };

    async fn status_of(
        principal: Option<&Principal>,
        track_db_id: agdb::DbId,
        purpose: MediaTokenPurpose,
    ) -> Result<(), StatusCode> {
        require_plugin_track_access(principal, track_db_id, purpose)
            .await
            .map(|_| ())
            .map_err(|error| error.into_response().status())
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
        initialize_runtime(&LibraryFixtureConfig {
            directory: test_dir.clone(),
            language: None,
            country: None,
        })
        .await?;

        let (user_db_id, user_public_id, library_public_id, track_db_id) = {
            let mut db = STATE.db.write().await;
            let user = db::test_db::test_user("plugin-bridge-user")?;
            let user_public_id = user.id.clone();
            let user_db_id = db::users::create(&mut db, &user)?;
            let library_db_id =
                db::test_db::insert_library(&mut db, "Plugin Bridge Lib", "/tmp/plugin-bridge")?;
            let library_public_id = db::lookup::find_id_by_db_id(&*db, library_db_id)?
                .context("inserted library has public id")?;
            let track_db_id = db::test_db::insert_track(&mut db, "Plugin Bridge Track")?;
            db::test_db::connect(&mut db, library_db_id, track_db_id)?;
            (user_db_id, user_public_id, library_public_id, track_db_id)
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
            status_of(None, track_db_id, MediaTokenPurpose::Stream).await,
            Err(StatusCode::UNAUTHORIZED),
            "responses without a resolved principal must not serve tracks"
        );

        let no_library_access = principal(Vec::new(), HashSet::new());
        assert_eq!(
            status_of(
                Some(&no_library_access),
                track_db_id,
                MediaTokenPurpose::Stream
            )
            .await,
            Err(StatusCode::NOT_FOUND),
            "tracks outside the caller's libraries must read as not found"
        );

        let with_access = principal(Vec::new(), accessible_libraries.clone());
        assert_eq!(
            status_of(Some(&with_access), track_db_id, MediaTokenPurpose::Stream).await,
            Ok(()),
            "library access must allow streaming"
        );

        assert_eq!(
            status_of(Some(&with_access), track_db_id, MediaTokenPurpose::Download).await,
            Err(StatusCode::FORBIDDEN),
            "downloads require the download permission even with library access"
        );

        let downloader = principal(vec![Permission::Download], accessible_libraries.clone());
        assert_eq!(
            status_of(Some(&downloader), track_db_id, MediaTokenPurpose::Download).await,
            Ok(()),
            "download permission plus library access must allow downloads"
        );

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }

    fn stream_response(
        track_db_id: agdb::DbId,
        options: Option<serde_json::Value>,
    ) -> ApiHandlerResponse {
        ApiHandlerResponse {
            kind: ApiResponseKind::StreamTrack,
            status: 200,
            headers: Vec::new(),
            body: None,
            path: None,
            transform: None,
            track_id: Some(track_db_id.0),
            options,
            principal: None,
            client_key: None,
        }
    }

    #[tokio::test]
    async fn public_plugin_streams_skip_the_caller_access_check() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = unique_test_dir("lyra-plugin-bridge-public-test")?;
        std::fs::create_dir_all(&test_dir)?;
        initialize_runtime(&LibraryFixtureConfig {
            directory: test_dir.clone(),
            language: None,
            country: None,
        })
        .await?;

        let (track_db_id, track_public_id, release_db_id) = {
            let mut db = STATE.db.write().await;
            let track_db_id = db::test_db::insert_track(&mut db, "Public Stream Track")?;
            let track_public_id = db::lookup::find_id_by_db_id(&*db, track_db_id)?
                .context("inserted track has public id")?;
            let release_db_id = db::test_db::insert_release(&mut db, "Public Stream Release")?;
            (track_db_id, track_public_id, release_db_id)
        };

        let (track_id, access) = public_plugin_track_access(None, None, track_db_id)
            .await
            .map_err(|error| anyhow::anyhow!("public access failed: {error:?}"))?;
        assert_eq!(track_id, track_public_id);
        assert!(matches!(
            access,
            crate::routes::TrackAccess::PublicPluginRoute(Account::Unknown)
        ));
        assert_eq!(access.capacity_account(), Account::Unknown);

        let caller = crate::services::auth::Principal::from_parts(
            agdb::DbId(1),
            "caller-public-id".to_string(),
            "caller".to_string(),
            Vec::new(),
            None,
            HashSet::new(),
        );
        let (_, access) = public_plugin_track_access(Some(&caller), Some("10.0.0.1"), track_db_id)
            .await
            .map_err(|error| anyhow::anyhow!("public access failed: {error:?}"))?;
        assert_eq!(
            access.capacity_account(),
            Account::User("caller-public-id".to_string()),
            "an authenticated caller's transcodes count against them on a public route"
        );
        let (_, first) = public_plugin_track_access(None, Some("10.0.0.1"), track_db_id)
            .await
            .map_err(|error| anyhow::anyhow!("public access failed: {error:?}"))?;
        let (_, second) = public_plugin_track_access(None, Some("10.0.0.2"), track_db_id)
            .await
            .map_err(|error| anyhow::anyhow!("public access failed: {error:?}"))?;
        assert_eq!(first.capacity_account(), Account::client("10.0.0.1"));
        assert_eq!(
            second.capacity_account(),
            Account::client("10.0.0.2"),
            "anonymous clients each get their own allowance"
        );

        let unknown = public_plugin_track_access(None, None, agdb::DbId(i64::MAX))
            .await
            .map(|_| ())
            .map_err(|error| error.into_response().status());
        assert_eq!(unknown, Err(StatusCode::NOT_FOUND));

        let headers = axum::http::HeaderMap::new();
        let private =
            plugin_api_response_to_axum(stream_response(track_db_id, None), &Method::GET, &headers)
                .await?;
        assert_eq!(
            private.status(),
            StatusCode::UNAUTHORIZED,
            "streams without the public option still need a principal"
        );
        let private_head = plugin_api_response_to_axum(
            stream_response(track_db_id, None),
            &Method::HEAD,
            &headers,
        )
        .await?;
        assert_eq!(
            private_head.status(),
            StatusCode::UNAUTHORIZED,
            "HEAD fails as GET does"
        );

        let public = plugin_api_response_to_axum(
            stream_response(release_db_id, Some(serde_json::json!({ "public": true }))),
            &Method::GET,
            &headers,
        )
        .await?;
        assert_eq!(
            public.status(),
            StatusCode::NOT_FOUND,
            "public streams serve tracks only"
        );

        let malformed = plugin_api_response_to_axum(
            stream_response(track_db_id, Some(serde_json::json!({ "public": "yes" }))),
            &Method::GET,
            &headers,
        )
        .await;
        assert!(
            malformed.is_err(),
            "a non-boolean public option is rejected"
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
            client_key: None,
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
        initialize_runtime(&LibraryFixtureConfig {
            directory: test_dir.clone(),
            language: None,
            country: None,
        })
        .await?;

        let hidden_dir = test_dir.join("hidden");
        std::fs::create_dir_all(&hidden_dir)?;
        let principal = {
            let mut db = STATE.db.write().await;
            let library_db_id = db::test_db::insert_library(
                &mut db,
                "File Bridge Lib",
                library_dir.to_str().context("library dir is utf-8")?,
            )?;
            db::test_db::insert_library(
                &mut db,
                "File Bridge Hidden",
                hidden_dir.to_str().context("hidden dir is utf-8")?,
            )?;
            let library_public_id = db::lookup::find_id_by_db_id(&*db, library_db_id)?
                .context("inserted library has public id")?;
            let user = db::test_db::test_user("file-bridge-user")?;
            let user_public_id = user.id.clone();
            let user_db_id = db::users::create(&mut db, &user)?;
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
            client_key: None,
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
}
