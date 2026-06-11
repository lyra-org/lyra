// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use super::*;
use crate::plugins::db::Permission;
use crate::plugins::executor::ApiResponseKind;
use crate::routes::AppError;
use crate::services::auth::{
    ResolvedAuth,
    require_permission,
};

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
    auth: Option<&ResolvedAuth>,
    track_db_id: agdb::DbId,
    permission: Option<Permission>,
) -> Result<(), AppError> {
    let Some(auth) = auth else {
        return Err(AppError::unauthorized("authentication required"));
    };
    let principal = &auth.principal;
    let db = crate::STATE.db.read().await;
    if !principal.revalidate(&db) {
        return Err(AppError::unauthorized("invalid bearer credential"));
    }
    if let Some(permission) = permission {
        require_permission(principal, permission)?;
    }
    crate::routes::require_entity_accessible(&*db, principal, track_db_id, || {
        AppError::not_found(format!("Track not found: {}", track_db_id.0))
    })
}

pub(super) async fn plugin_api_response_to_axum(
    response: crate::plugins::executor::ApiHandlerResponse,
    request_headers: &HeaderMap,
    auth: Option<&ResolvedAuth>,
) -> Result<Response> {
    let mut status =
        StatusCode::from_u16(response.status).context("invalid response status code")?;
    match response.kind {
        ApiResponseKind::StreamTrack => {
            let track_id = response
                .track_id
                .ok_or_else(|| anyhow::anyhow!("stream_track response requires track_id"))?;
            if let Err(error) = require_plugin_track_access(auth, agdb::DbId(track_id), None).await
            {
                return Ok(error.into_response());
            }
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
            if let Err(error) =
                require_plugin_track_access(auth, agdb::DbId(track_id), Some(Permission::Download))
                    .await
            {
                return Ok(error.into_response());
            }
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
            if let Err(error) = require_plugin_track_access(auth, agdb::DbId(track_id), None).await
            {
                return Ok(error.into_response());
            }
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

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::time::{
        SystemTime,
        UNIX_EPOCH,
    };

    use anyhow::Context as _;
    use axum::http::StatusCode;
    use axum::response::IntoResponse;

    use super::require_plugin_track_access;
    use crate::{
        STATE,
        plugins::db::{
            self,
            Permission,
        },
        services::auth::{
            AuthCredential,
            Principal,
            ResolvedAuth,
        },
        testing::{
            LibraryFixtureConfig,
            initialize_runtime,
            runtime_test_lock,
        },
    };

    fn resolved_auth(principal: Principal) -> ResolvedAuth {
        ResolvedAuth {
            principal,
            credential: AuthCredential::Default,
        }
    }

    async fn status_of(
        auth: Option<&ResolvedAuth>,
        track_db_id: agdb::DbId,
        permission: Option<Permission>,
    ) -> Result<(), StatusCode> {
        require_plugin_track_access(auth, track_db_id, permission)
            .await
            .map_err(|error| error.into_response().status())
    }

    #[tokio::test]
    async fn plugin_track_access_enforces_library_access_and_permissions() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = std::env::temp_dir().join(format!(
            "lyra-plugin-bridge-auth-test-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ));
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

        let principal = |permissions: Vec<Permission>, library_ids: HashSet<String>| Principal {
            user_db_id,
            user_public_id: user_public_id.clone(),
            username: "plugin-bridge-user".to_string(),
            permissions,
            role_name: None,
            accessible_library_ids: library_ids,
        };
        let accessible_libraries = HashSet::from([library_public_id.clone()]);

        assert_eq!(
            status_of(None, track_db_id, None).await,
            Err(StatusCode::UNAUTHORIZED),
            "responses without a resolved principal must not serve tracks"
        );

        let no_library_access = resolved_auth(principal(Vec::new(), HashSet::new()));
        assert_eq!(
            status_of(Some(&no_library_access), track_db_id, None).await,
            Err(StatusCode::NOT_FOUND),
            "tracks outside the caller's libraries must read as not found"
        );

        let with_access = resolved_auth(principal(Vec::new(), accessible_libraries.clone()));
        assert_eq!(
            status_of(Some(&with_access), track_db_id, None).await,
            Ok(()),
            "library access must allow streaming"
        );

        assert_eq!(
            status_of(Some(&with_access), track_db_id, Some(Permission::Download)).await,
            Err(StatusCode::FORBIDDEN),
            "downloads require the download permission even with library access"
        );

        let downloader = resolved_auth(principal(
            vec![Permission::Download],
            accessible_libraries.clone(),
        ));
        assert_eq!(
            status_of(Some(&downloader), track_db_id, Some(Permission::Download)).await,
            Ok(()),
            "download permission plus library access must allow downloads"
        );

        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }
}
