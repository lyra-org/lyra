// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

#[cfg(feature = "docgen")]
use aide::transform::TransformOperation;
use axum::{
    Json,
    Router,
    extract::Path,
    http::HeaderMap,
};
use lyra_db::mixers::MixerConfig;
use serde::{
    Deserialize,
    Serialize,
};

use crate::routes::request::bearer_credential;
use crate::{
    routes::AppError,
    services::{
        auth::{
            require_authenticated,
            require_manage_providers,
        },
        mix::{
            MixerUpdate,
            list_mixer_configs,
            update_mixer as update_mixer_service,
        },
    },
};

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
#[non_exhaustive]
pub struct MixerResponse {
    pub mixer_id: String,
    pub display_name: String,
    /// Zero-based rank; lower positions take precedence.
    pub position: u32,
    pub enabled: bool,
}

impl From<MixerConfig> for MixerResponse {
    fn from(config: MixerConfig) -> Self {
        Self {
            mixer_id: config.mixer_id,
            display_name: config.display_name,
            position: config.position,
            enabled: config.enabled,
        }
    }
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
pub struct UpdateMixerRequest {
    /// Moves the mixer to this zero-based rank, clamped to the last one.
    pub position: Option<u32>,
    pub enabled: Option<bool>,
}

async fn list_mixers(headers: HeaderMap) -> Result<Json<Vec<MixerResponse>>, AppError> {
    let _principal = require_authenticated(bearer_credential(&headers)).await?;

    let mixers = list_mixer_configs().await?;
    Ok(Json(mixers.into_iter().map(Into::into).collect()))
}

async fn update_mixer(
    headers: HeaderMap,
    Path(mixer_id): Path<String>,
    Json(request): Json<UpdateMixerRequest>,
) -> Result<Json<Vec<MixerResponse>>, AppError> {
    let _principal = require_manage_providers(bearer_credential(&headers)).await?;

    let mixers = update_mixer_service(
        &mixer_id,
        MixerUpdate {
            position: request.position,
            enabled: request.enabled,
        },
    )
    .await?;
    Ok(Json(mixers.into_iter().map(Into::into).collect()))
}

#[cfg(feature = "docgen")]
fn list_mixers_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List mixers").description(
        "Returns all registered mixers in priority order. Mix generation tries enabled mixers from position 0 onward and uses the first non-empty result. Requires authentication.",
    )
}

#[cfg(feature = "docgen")]
fn update_mixer_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Update mixer").description(
        "Moves a mixer to `position` and/or sets `enabled`, then returns every mixer in priority order. Positions are zero-based and gap-free; position 0 takes precedence, and a position past the end moves the mixer last. Requires ManageProviders permission.",
    )
}

pub fn mixer_routes() -> Router {
    use axum::routing::{
        get,
        patch,
    };

    Router::new()
        .route("/", get(list_mixers))
        .route("/{mixer_id}", patch(update_mixer))
}

#[cfg(feature = "docgen")]
pub(crate) fn mixer_openapi_routes() -> aide::axum::ApiRouter {
    use aide::axum::routing::{
        get_with,
        patch_with,
    };

    aide::axum::ApiRouter::new()
        .api_route("/", get_with(list_mixers, list_mixers_docs))
        .api_route("/{mixer_id}", patch_with(update_mixer, update_mixer_docs))
}
