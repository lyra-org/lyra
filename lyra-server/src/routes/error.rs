// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

#[cfg(feature = "docgen")]
use aide::generate::GenContext;
#[cfg(feature = "docgen")]
use aide::openapi::{
    Operation,
    Response as ApiResponse,
    StatusCode as ApiStatusCode,
};
#[cfg(feature = "docgen")]
use aide::operation::OperationOutput;
use std::time::Duration;

use axum::{
    http::{
        HeaderValue,
        StatusCode,
        header,
    },
    response::{
        IntoResponse,
        Response,
    },
};

use crate::services::{
    auth::{
        AuthError,
        access::AccessError,
        api_keys::ApiKeyServiceError,
    },
    catalog::{
        CatalogError,
        Direction,
        SortSpec,
        pipeline::SortKey,
    },
    delivery::DeliveryError,
    entries::EntryServiceError,
    hls::HlsError,
    mix::MixerAdminError,
    playback_sessions::PlaybackServiceError,
    providers::{
        ProviderAdminError,
        ProviderServiceError,
    },
};

pub(crate) struct AppError {
    error: anyhow::Error,
    status_code: StatusCode,
    retry_after: Option<Duration>,
    json_body: Option<serde_json::Value>,
}

impl std::fmt::Debug for AppError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AppError({}: {})", self.status_code, self.error)
    }
}

impl std::fmt::Display for AppError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.status_code, self.error)
    }
}

impl AppError {
    fn with_status(status_code: StatusCode, message: String) -> Self {
        Self {
            error: anyhow::anyhow!(message),
            status_code,
            retry_after: None,
            json_body: None,
        }
    }

    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self::with_status(StatusCode::UNAUTHORIZED, message.into())
    }

    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::with_status(StatusCode::BAD_REQUEST, message.into())
    }

    pub fn forbidden(message: impl Into<String>) -> Self {
        Self::with_status(StatusCode::FORBIDDEN, message.into())
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::with_status(StatusCode::NOT_FOUND, message.into())
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self::with_status(StatusCode::CONFLICT, message.into())
    }

    pub fn json_conflict(body: serde_json::Value) -> Self {
        Self {
            error: anyhow::anyhow!("conflict"),
            status_code: StatusCode::CONFLICT,
            retry_after: None,
            json_body: Some(body),
        }
    }

    pub fn gone(message: impl Into<String>) -> Self {
        Self::with_status(StatusCode::GONE, message.into())
    }

    pub fn bad_gateway(message: impl Into<String>) -> Self {
        Self::with_status(StatusCode::BAD_GATEWAY, message.into())
    }

    pub fn service_unavailable(message: impl Into<String>) -> Self {
        Self::with_status(StatusCode::SERVICE_UNAVAILABLE, message.into())
    }

    pub fn not_acceptable(message: impl Into<String>) -> Self {
        Self::with_status(StatusCode::NOT_ACCEPTABLE, message.into())
    }

    pub fn unsupported_media_type(message: impl Into<String>) -> Self {
        Self::with_status(StatusCode::UNSUPPORTED_MEDIA_TYPE, message.into())
    }

    pub fn service_unavailable_for(message: impl Into<String>, retry_after: Duration) -> Self {
        Self {
            retry_after: Some(retry_after),
            ..Self::service_unavailable(message)
        }
    }

    pub fn too_many_requests(retry_after: Duration) -> Self {
        Self {
            retry_after: Some(retry_after),
            ..Self::with_status(
                StatusCode::TOO_MANY_REQUESTS,
                "too many requests".to_string(),
            )
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let mut response = if let Some(body) = self.json_body {
            (self.status_code, axum::Json(body)).into_response()
        } else if self.status_code == StatusCode::INTERNAL_SERVER_ERROR {
            tracing::error!(error = %self.error, "internal server error");
            if lyra_db::is_poisoned(&self.error) {
                crate::services::shutdown::fail("database poisoned; restart to reopen it");
            }
            (self.status_code, "Error: internal server error").into_response()
        } else {
            (self.status_code, format!("Error: {}", self.error)).into_response()
        };

        if let Some(retry_after) = self.retry_after {
            let seconds = retry_after.as_secs().max(1).to_string();
            response.headers_mut().insert(
                header::RETRY_AFTER,
                HeaderValue::from_str(&seconds)
                    .expect("retry-after seconds should be a valid header"),
            );
        }

        response
    }
}

#[cfg(feature = "docgen")]
impl OperationOutput for AppError {
    type Inner = String;

    fn operation_response(ctx: &mut GenContext, operation: &mut Operation) -> Option<ApiResponse> {
        String::operation_response(ctx, operation)
    }

    fn inferred_responses(
        ctx: &mut GenContext,
        operation: &mut Operation,
    ) -> Vec<(Option<ApiStatusCode>, ApiResponse)> {
        let Some(response) = String::operation_response(ctx, operation) else {
            return Vec::new();
        };

        vec![(None, response)]
    }
}

impl From<AuthError> for AppError {
    fn from(err: AuthError) -> Self {
        match err {
            AuthError::MissingBearerCredential => Self::unauthorized("missing bearer credential"),
            AuthError::InvalidBearerCredential => Self::unauthorized("invalid bearer credential"),
            AuthError::SessionExpired => Self::unauthorized("session expired"),
            AuthError::InvalidClientName(err) => Self::bad_request(err.to_string()),
            AuthError::Forbidden(msg) => Self::forbidden(msg),
            AuthError::NotFound(msg) => Self::not_found(msg),
            AuthError::RateLimited(retry_after) => Self::too_many_requests(retry_after),
            AuthError::Internal(err) => err.into(),
        }
    }
}

impl From<AccessError> for AppError {
    fn from(err: AccessError) -> Self {
        match err {
            AccessError::InvalidRequest(message) => Self::bad_request(message),
            AccessError::LibraryNotFound(id) => Self::not_found(format!("library not found: {id}")),
            AccessError::Internal(err) => err.into(),
        }
    }
}

impl From<CatalogError> for AppError {
    fn from(err: CatalogError) -> Self {
        match err {
            CatalogError::Invalid(message) => Self::bad_request(message),
            CatalogError::Auth(err) => err.into(),
            CatalogError::Internal(err) => err.into(),
        }
    }
}

/// Catalog sort keys from `sort_by` tokens, all in the `sort_order` direction.
pub(crate) fn parse_catalog_sort<K: SortKey>(
    sort_by: Option<Vec<String>>,
    sort_order: Option<String>,
) -> Result<SortSpec<K>, AppError> {
    let direction = sort_order
        .as_deref()
        .map(Direction::parse)
        .transpose()?
        .unwrap_or(Direction::Ascending);
    let mut sort = Vec::new();
    for value in sort_by.into_iter().flatten() {
        for token in value
            .split(',')
            .map(str::trim)
            .filter(|token| !token.is_empty())
        {
            sort.push((K::parse(&token.to_ascii_lowercase())?, direction));
        }
    }
    Ok(sort)
}

impl From<ApiKeyServiceError> for AppError {
    fn from(err: ApiKeyServiceError) -> Self {
        match err {
            ApiKeyServiceError::BadRequest(message) => Self::bad_request(message),
            ApiKeyServiceError::Internal(err) => err.into(),
        }
    }
}

impl From<ProviderServiceError> for AppError {
    fn from(err: ProviderServiceError) -> Self {
        match err {
            ProviderServiceError::EntityNotFound(id) => {
                Self::not_found(format!("Entity not found: {id}"))
            }
            ProviderServiceError::LibraryNotFound(id) => {
                Self::not_found(format!("Library not found: {id}"))
            }
            ProviderServiceError::SyncAlreadyRunning(provider_id) => {
                Self::conflict(format!("Sync already running for provider '{provider_id}'"))
            }
            ProviderServiceError::RefreshAlreadyRunning(library_id) => {
                Self::conflict(format!("Refresh already running for library {library_id}"))
            }
            ProviderServiceError::NoRefreshHandler(provider_id) => {
                Self::not_found(format!("No refresh handler for provider '{provider_id}'"))
            }
            ProviderServiceError::Internal(err) => err.into(),
        }
    }
}

impl From<HlsError> for AppError {
    fn from(err: HlsError) -> Self {
        match err {
            HlsError::UnsupportedCodec => Self::bad_request(
                "Unsupported HLS codec. Supported values: copy, aac, alac, flac, opus, mp3.",
            ),
            HlsError::JobNotFound => Self::not_found("HLS transcode job not found"),
            HlsError::Saturated => Self::too_many_requests(
                crate::services::transcoding::capacity::SATURATED_RETRY_AFTER,
            ),
            HlsError::Internal(err) => err.into(),
        }
    }
}

impl From<DeliveryError> for AppError {
    fn from(err: DeliveryError) -> Self {
        match err {
            DeliveryError::BadRequest(message) => Self::bad_request(message),
            DeliveryError::Unauthorized(message) => Self::unauthorized(message),
            DeliveryError::NotFound(message) => Self::not_found(message),
            DeliveryError::Gone(message) => Self::gone(message),
            DeliveryError::ServiceUnavailable(message) => Self::service_unavailable(message),
            DeliveryError::Auth(err) => err.into(),
            DeliveryError::Internal(err) => err.into(),
        }
    }
}

impl From<PlaybackServiceError> for AppError {
    fn from(err: PlaybackServiceError) -> Self {
        match err {
            PlaybackServiceError::BadRequest(message) => Self::bad_request(message),
            PlaybackServiceError::NotFound(message) => Self::not_found(message),
            PlaybackServiceError::Internal(err) => err.into(),
        }
    }
}

impl From<EntryServiceError> for AppError {
    fn from(err: EntryServiceError) -> Self {
        match err {
            EntryServiceError::NotFound(message) => Self::not_found(message),
            EntryServiceError::Internal(err) => err.into(),
        }
    }
}

impl From<ProviderAdminError> for AppError {
    fn from(err: ProviderAdminError) -> Self {
        match err {
            ProviderAdminError::ProviderNotFound(message) => Self::not_found(message),
            ProviderAdminError::EntityNotFound(message) => Self::not_found(message),
            ProviderAdminError::ManualIdNotFound(message) => Self::not_found(message),
            ProviderAdminError::BlankIdValue(message) => Self::bad_request(message),
            ProviderAdminError::Internal(err) => err.into(),
        }
    }
}

impl From<MixerAdminError> for AppError {
    fn from(err: MixerAdminError) -> Self {
        match err {
            MixerAdminError::MixerNotFound(message) => Self::not_found(message),
            MixerAdminError::Internal(err) => err.into(),
        }
    }
}

impl From<anyhow::Error> for AppError {
    fn from(err: anyhow::Error) -> Self {
        match err.downcast::<AuthError>() {
            Ok(err) => err.into(),
            Err(err) => Self {
                error: err,
                status_code: StatusCode::INTERNAL_SERVER_ERROR,
                retry_after: None,
                json_body: None,
            },
        }
    }
}

macro_rules! internal_app_error_from {
    ($($ty:path),* $(,)?) => {
        $(
            impl From<$ty> for AppError {
                fn from(err: $ty) -> Self {
                    Self::from(anyhow::Error::from(err))
                }
            }
        )*
    };
}

internal_app_error_from!(
    agdb::DbError,
    argon2::password_hash::Error,
    axum::http::Error,
    lyra_ffmpeg::Error,
    serde_json::Error,
    std::io::Error,
    tokio::task::JoinError,
);

#[cfg(test)]
mod tests {
    use axum::{
        http::StatusCode,
        response::IntoResponse,
    };

    use super::AppError;
    use crate::{
        plugins::playback_sources::delivery_error_message,
        services::{
            auth::{
                AuthError,
                sessions::ClientNameError,
            },
            delivery::DeliveryError,
        },
    };

    #[test]
    fn delivery_errors_map_to_their_statuses() {
        let caf = "Format 'caf' does not support streaming. Use /api/download or choose a \
                   streamable format (mp3, flac, wav, ogg, webm, aac, m4a, opus, aiff).";
        let cases = [
            (
                DeliveryError::bad_request("bitrate_bps must be greater than zero"),
                StatusCode::BAD_REQUEST,
                "400 Bad Request: bitrate_bps must be greater than zero".to_string(),
            ),
            (
                DeliveryError::bad_request("sample_rate_hz must be greater than zero"),
                StatusCode::BAD_REQUEST,
                "400 Bad Request: sample_rate_hz must be greater than zero".to_string(),
            ),
            (
                DeliveryError::bad_request("channels must be greater than zero"),
                StatusCode::BAD_REQUEST,
                "400 Bad Request: channels must be greater than zero".to_string(),
            ),
            (
                DeliveryError::bad_request(caf),
                StatusCode::BAD_REQUEST,
                format!("400 Bad Request: {caf}"),
            ),
            (
                DeliveryError::unauthorized("authentication required"),
                StatusCode::UNAUTHORIZED,
                "401 Unauthorized: authentication required".to_string(),
            ),
            (
                DeliveryError::not_found("Track not found: 7"),
                StatusCode::NOT_FOUND,
                "404 Not Found: Track not found: 7".to_string(),
            ),
            (
                DeliveryError::gone("the track's source changed since this media token was issued"),
                StatusCode::GONE,
                "410 Gone: the track's source changed since this media token was issued"
                    .to_string(),
            ),
            (
                AuthError::InvalidBearerCredential.into(),
                StatusCode::UNAUTHORIZED,
                "401 Unauthorized: invalid bearer credential".to_string(),
            ),
            (
                anyhow::anyhow!("broken").into(),
                StatusCode::INTERNAL_SERVER_ERROR,
                "500 Internal Server Error: broken".to_string(),
            ),
        ];
        for (error, status, display) in cases {
            let app_error = AppError::from(error);
            assert_eq!(app_error.to_string(), display);
            assert_eq!(app_error.into_response().status(), status);
        }
    }

    #[test]
    fn delivery_errors_answer_as_plugins_report_them() {
        let cases: Vec<(fn() -> DeliveryError, StatusCode)> = vec![
            (
                || DeliveryError::bad_request("bad"),
                StatusCode::BAD_REQUEST,
            ),
            (
                || DeliveryError::unauthorized("who"),
                StatusCode::UNAUTHORIZED,
            ),
            (|| DeliveryError::not_found("gone"), StatusCode::NOT_FOUND),
            (|| DeliveryError::gone("changed"), StatusCode::GONE),
            (
                || DeliveryError::service_unavailable("later"),
                StatusCode::SERVICE_UNAVAILABLE,
            ),
            (
                || AuthError::MissingBearerCredential.into(),
                StatusCode::UNAUTHORIZED,
            ),
            (
                || AuthError::InvalidBearerCredential.into(),
                StatusCode::UNAUTHORIZED,
            ),
            (
                || AuthError::SessionExpired.into(),
                StatusCode::UNAUTHORIZED,
            ),
            (
                || AuthError::InvalidClientName(ClientNameError::TooLong).into(),
                StatusCode::BAD_REQUEST,
            ),
            (
                || AuthError::Forbidden("no".to_string()).into(),
                StatusCode::FORBIDDEN,
            ),
            (
                || AuthError::NotFound("none".to_string()).into(),
                StatusCode::NOT_FOUND,
            ),
            (
                || AuthError::RateLimited(std::time::Duration::from_secs(1)).into(),
                StatusCode::TOO_MANY_REQUESTS,
            ),
            (
                || AuthError::Internal(anyhow::anyhow!("broken")).into(),
                StatusCode::INTERNAL_SERVER_ERROR,
            ),
            (
                || anyhow::Error::from(AuthError::SessionExpired).into(),
                StatusCode::UNAUTHORIZED,
            ),
            (
                || anyhow::anyhow!("broken").into(),
                StatusCode::INTERNAL_SERVER_ERROR,
            ),
        ];
        for (error, status) in cases {
            let app_error = AppError::from(error());
            assert_eq!(app_error.to_string(), delivery_error_message(error()));
            assert_eq!(app_error.into_response().status(), status);
        }
    }
}
