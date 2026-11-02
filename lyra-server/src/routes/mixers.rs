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
use serde::{
    Deserialize,
    Serialize,
};

use crate::routes::request::bearer_credential;
use crate::{
    routes::{
        AppError,
        providers::{
            OptionResponse,
            option_responses,
        },
    },
    services::{
        STATE,
        auth::{
            require_authenticated,
            require_manage_providers,
        },
        mix::{
            MixHandlerSpec,
            MixSeedType,
            MixerUpdate,
            MixersListing,
            list_mixers as list_mixers_service,
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
    /// Options a mix request sets as `<mixer_id>.<name>`.
    pub options: Vec<OptionResponse>,
    /// What the mixer's blend handler takes; null without one.
    pub blend: Option<MixHandlerResponse>,
    /// What the mixer's path handler takes; null without one.
    pub path: Option<MixHandlerResponse>,
}

/// What one of a mixer's handlers takes. A request goes to the handler only when it covers the
/// request's seed types, seed count, excluded seeds and query.
#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
#[non_exhaustive]
pub struct MixHandlerResponse {
    pub(crate) seeds: Vec<MixSeedType>,
    /// The most explicit seeds it takes; `from_history` fills the rest with recent listens when
    /// it takes track seeds.
    pub(crate) max_seeds: usize,
    pub(crate) exclude: bool,
    pub(crate) query: bool,
}

/// The mixers in priority order, and the built-in one that serves what none of them does.
#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
#[non_exhaustive]
pub struct MixersResponse {
    pub builtin: BuiltinMixerResponse,
    pub mixers: Vec<MixerResponse>,
}

/// What the built-in mixer takes. It makes blends only, and is never tried for a request that
/// pins a mixer.
#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
#[non_exhaustive]
pub struct BuiltinMixerResponse {
    pub blend: MixHandlerResponse,
}

impl From<MixHandlerSpec> for MixHandlerResponse {
    fn from(spec: MixHandlerSpec) -> Self {
        Self {
            seeds: spec.seed_types,
            max_seeds: spec.max_seeds,
            exclude: spec.exclude,
            query: spec.query,
        }
    }
}

async fn mixers_response(listing: MixersListing) -> MixersResponse {
    let db = STATE.db.read().await;
    let mixers = listing
        .mixers
        .into_iter()
        .map(|mixer| MixerResponse {
            options: mixer
                .plugin_id
                .map(|plugin_id| option_responses(&db, plugin_id.as_ref(), &mixer.options))
                .unwrap_or_default(),
            blend: mixer.blend.map(Into::into),
            path: mixer.path.map(Into::into),
            mixer_id: mixer.config.mixer_id,
            display_name: mixer.config.display_name,
            position: mixer.config.position,
            enabled: mixer.config.enabled,
        })
        .collect();
    MixersResponse {
        builtin: BuiltinMixerResponse {
            blend: listing.builtin.into(),
        },
        mixers,
    }
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
pub struct UpdateMixerRequest {
    /// Moves the mixer to this zero-based rank, clamped to the last one.
    pub position: Option<u32>,
    pub enabled: Option<bool>,
}

async fn list_mixers(headers: HeaderMap) -> Result<Json<MixersResponse>, AppError> {
    let _principal = require_authenticated(bearer_credential(&headers)).await?;

    let mixers = list_mixers_service().await?;
    Ok(Json(mixers_response(mixers).await))
}

async fn update_mixer(
    headers: HeaderMap,
    Path(mixer_id): Path<String>,
    Json(request): Json<UpdateMixerRequest>,
) -> Result<Json<MixersResponse>, AppError> {
    let _principal = require_manage_providers(bearer_credential(&headers)).await?;

    let mixers = update_mixer_service(
        &mixer_id,
        MixerUpdate {
            position: request.position,
            enabled: request.enabled,
        },
    )
    .await?;
    Ok(Json(mixers_response(mixers).await))
}

#[cfg(feature = "docgen")]
fn list_mixers_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List mixers").description(
        "Returns all registered mixers in priority order, with the options a mix request can set for each and what each one's `blend` and `path` handlers take, and what the built-in mixer takes. `POST /api/mix` tries the enabled mixers whose handler takes the request, from position 0 onward, and uses the first non-empty result; the built-in mixer serves a blend none of them produced tracks for, when it takes the request. Requires authentication.",
    )
}

#[cfg(feature = "docgen")]
fn update_mixer_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Update mixer").description(
        "Moves a mixer to `position` and/or sets `enabled`, then lists the mixers as `GET /api/mixers` does. Positions are zero-based and gap-free; position 0 takes precedence, and a position past the end moves the mixer last. Requires ManageProviders permission.",
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

#[cfg(test)]
mod tests {
    use axum::{
        body::{
            Body,
            to_bytes,
        },
        http::{
            Request,
            StatusCode,
            header::AUTHORIZATION,
        },
    };
    use tower::ServiceExt;

    use crate::services::{
        STATE,
        mix::{
            MixHandlerRegistration,
            MixHandlerSpec,
            MixSeedType,
            MixShape,
            mix_registry,
        },
        options::{
            OptionChoice,
            OptionDeclaration,
            OptionType,
        },
        plugin_dispatch::PluginCallbackHandle,
        plugin_scope::PluginId,
        testing::runtime_test_lock,
    };

    #[tokio::test]
    async fn mixers_list_their_options_and_what_their_handlers_take() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        let test_dir = tempfile::tempdir()?;
        crate::plugins::testing::init_library_test_state(test_dir.path().to_path_buf()).await?;
        let user_db_id = {
            let mut db = STATE.db.write().await;
            lyra_db::mixers::register(&mut db, "similar", Some("Similar Songs"))?;
            lyra_db::roles::ensure_builtin_roles(&mut db)?;
            lyra_db::users::create(&mut db, &lyra_db::fixtures::test_user("mixer-lister")?)?
        };
        {
            let mut registry = mix_registry().write_owned().await;
            registry.register(PluginId::new("similar".to_string())?, "similar".to_string())?;
            registry.set_handler(
                "similar",
                MixShape::Blend,
                MixHandlerRegistration {
                    spec: MixHandlerSpec {
                        seed_types: vec![MixSeedType::Track, MixSeedType::Artist],
                        max_seeds: 5,
                        exclude: true,
                        query: false,
                    },
                    timeout: std::time::Duration::from_secs(1),
                    handler: PluginCallbackHandle {
                        vm_id: 1,
                        handler_id: 1,
                    },
                },
            )?;
            registry
                .declare_option(
                    "similar",
                    OptionDeclaration {
                        name: "depth".to_string(),
                        label: "Depth".to_string(),
                        option_type: OptionType::Number,
                        default: serde_json::json!(2),
                        requires_settings: vec!["token".to_string()],
                    },
                )
                .map_err(anyhow::Error::msg)?;
            registry
                .declare_option(
                    "similar",
                    OptionDeclaration {
                        name: "space".to_string(),
                        label: "Space".to_string(),
                        option_type: OptionType::Choice(vec![
                            OptionChoice {
                                value: "audio".to_string(),
                                label: "Audio".to_string(),
                            },
                            OptionChoice {
                                value: "lyrics".to_string(),
                                label: "Lyrics".to_string(),
                            },
                        ]),
                        default: serde_json::json!("audio"),
                        requires_settings: Vec::new(),
                    },
                )
                .map_err(anyhow::Error::msg)?;
        }
        let session =
            crate::services::testing::create_session(user_db_id, Default::default()).await?;

        let response = super::mixer_routes()
            .oneshot(
                Request::builder()
                    .uri("/")
                    .header(AUTHORIZATION, format!("Bearer {}", session.token))
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await?)?;
        assert_eq!(
            body,
            serde_json::json!({
                "builtin": {
                    "blend": {
                        "seeds": ["track", "release", "artist", "genre", "playlist"],
                        "max_seeds": 1,
                        "exclude": false,
                        "query": false,
                    },
                },
                "mixers": [{
                "mixer_id": "similar",
                "display_name": "Similar Songs",
                "position": 0,
                "enabled": true,
                "options": [{
                    "name": "depth",
                    "label": "Depth",
                    "type": "number",
                    "default": 2,
                    "available": false,
                    "unavailable_reason": "missing settings: token",
                }, {
                    "name": "space",
                    "label": "Space",
                    "type": "choice",
                    "options": [
                        { "value": "audio", "label": "Audio" },
                        { "value": "lyrics", "label": "Lyrics" },
                    ],
                    "default": "audio",
                    "available": true,
                }],
                "blend": {
                    "seeds": ["track", "artist"],
                    "max_seeds": 5,
                    "exclude": true,
                    "query": false,
                },
                "path": null,
                }],
            })
        );
        Ok(())
    }
}
