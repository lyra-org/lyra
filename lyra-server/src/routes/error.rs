// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use aide::generate::GenContext;
use aide::openapi::{
    Operation,
    Response as ApiResponse,
    StatusCode as ApiStatusCode,
};
use aide::operation::OperationOutput;
use axum::{
    http::StatusCode,
    response::{
        IntoResponse,
        Response,
    },
};

use crate::services::{
    auth::{
        AuthError,
        api_keys::ApiKeyServiceError,
    },
    entries::EntryServiceError,
    hls::HlsError,
    playback_sessions::PlaybackServiceError,
    providers::{
        ProviderAdminError,
        ProviderServiceError,
    },
};

pub(crate) struct AppError {
    error: anyhow::Error,
    status_code: StatusCode,
}

impl std::fmt::Debug for AppError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AppError({}: {})", self.status_code, self.error)
    }
}

impl AppError {
    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self {
            error: anyhow::anyhow!(message.into()),
            status_code: StatusCode::UNAUTHORIZED,
        }
    }

    pub fn bad_request(message: impl Into<String>) -> Self {
        Self {
            error: anyhow::anyhow!(message.into()),
            status_code: StatusCode::BAD_REQUEST,
        }
    }

    pub fn forbidden(message: impl Into<String>) -> Self {
        Self {
            error: anyhow::anyhow!(message.into()),
            status_code: StatusCode::FORBIDDEN,
        }
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self {
            error: anyhow::anyhow!(message.into()),
            status_code: StatusCode::NOT_FOUND,
        }
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self {
            error: anyhow::anyhow!(message.into()),
            status_code: StatusCode::CONFLICT,
        }
    }

    pub fn service_unavailable(message: impl Into<String>) -> Self {
        Self {
            error: anyhow::anyhow!(message.into()),
            status_code: StatusCode::SERVICE_UNAVAILABLE,
        }
    }

    pub fn not_acceptable(message: impl Into<String>) -> Self {
        Self {
            error: anyhow::anyhow!(message.into()),
            status_code: StatusCode::NOT_ACCEPTABLE,
        }
    }

    pub fn unsupported_media_type(message: impl Into<String>) -> Self {
        Self {
            error: anyhow::anyhow!(message.into()),
            status_code: StatusCode::UNSUPPORTED_MEDIA_TYPE,
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        if self.status_code == StatusCode::INTERNAL_SERVER_ERROR {
            tracing::error!(error = %self.error, "internal server error");
            (self.status_code, "Error: internal server error").into_response()
        } else {
            (self.status_code, format!("Error: {}", self.error)).into_response()
        }
    }
}

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
            AuthError::Forbidden(msg) => Self::forbidden(msg),
            AuthError::Internal(err) => err.into(),
        }
    }
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
            HlsError::UnsupportedCodec => {
                Self::bad_request("Unsupported HLS codec. Supported values: aac, alac, flac.")
            }
            HlsError::TranscodeCapacityUnavailable => {
                Self::service_unavailable("transcode capacity unavailable")
            }
            HlsError::JobNotFound => Self::not_found("HLS transcode job not found"),
            HlsError::SessionNotFound => Self::not_found("HLS session not found"),
            HlsError::SessionForbidden => {
                Self::forbidden("HLS session does not belong to current user")
            }
            HlsError::Internal(err) => err.into(),
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
            ProviderAdminError::Internal(err) => err.into(),
        }
    }
}

impl From<anyhow::Error> for AppError {
    fn from(err: anyhow::Error) -> Self {
        Self {
            error: err,
            status_code: StatusCode::INTERNAL_SERVER_ERROR,
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
    mlua::Error,
    serde_json::Error,
    std::io::Error,
    tokio::task::JoinError,
);
