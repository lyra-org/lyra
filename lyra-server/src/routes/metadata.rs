// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use aide::axum::{
    ApiRouter,
    routing::{
        get_with,
        post_with,
    },
};
use aide::transform::TransformOperation;
use axum::{
    Json,
    http::{
        HeaderMap,
        StatusCode,
    },
    response::IntoResponse,
};
use schemars::JsonSchema;
use serde::{
    Deserialize,
    Serialize,
};

use crate::{
    STATE,
    db,
    routes::AppError,
    services::{
        auth::require_manage_metadata,
        metadata::{
            mapping::{
                self,
                FieldName,
                MappingRule,
                MetadataMappingConfig,
                SUPPORTED_KEY_NAMES,
            },
            mapping_admin::{
                DryRunReport,
                ReingestGuard,
                commit_and_reingest,
                dry_run_config,
                reingest_in_progress,
            },
        },
    },
};

#[derive(Debug, Serialize, JsonSchema)]
struct MetadataMappingResponse {
    version: u64,
    rules: Vec<MetadataMappingRule>,
    supported_source_keys: &'static [&'static str],
    /// `true` while a committed config is still reingesting libraries.
    /// Poll this endpoint after `PUT /api/metadata/mapping` to wait for
    /// completion.
    reingest_in_progress: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
struct MetadataMappingRule {
    source_key: String,
    destination: FieldName,
}

impl From<MappingRule> for MetadataMappingRule {
    fn from(rule: MappingRule) -> Self {
        Self {
            source_key: rule.source_key,
            destination: rule.destination,
        }
    }
}

impl From<MetadataMappingRule> for MappingRule {
    fn from(rule: MetadataMappingRule) -> Self {
        Self {
            source_key: rule.source_key,
            destination: rule.destination,
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
struct PutMetadataMappingRequest {
    rules: Vec<MetadataMappingRule>,
    /// Opt-in to replace even if the dry-run reports file read
    /// failures. Without this, any read failure aborts the replace.
    #[serde(default)]
    force_partial: bool,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct PreviewMetadataMappingRequest {
    rules: Vec<MetadataMappingRule>,
}

/// The write is async: the commit + reingest run in a background
/// task and the client polls `GET /api/metadata/mapping` to observe
/// `reingest_in_progress` and the post-commit `version`.
#[derive(Debug, Serialize, JsonSchema)]
struct PutMetadataMappingResponse {
    accepted: bool,
    rules_accepted: usize,
}

async fn get_metadata_mapping(
    headers: HeaderMap,
) -> Result<Json<MetadataMappingResponse>, AppError> {
    require_manage_metadata(&headers).await?;
    let config = {
        let db = STATE.db.read().await;
        db::metadata::mapping_config::get(&db)?
    };
    let config = match config {
        Some(c) => c,
        None => {
            let mut db = STATE.db.write().await;
            db::metadata::mapping_config::ensure(&mut db)?
        }
    };
    Ok(Json(MetadataMappingResponse {
        version: config.version,
        rules: config
            .rules
            .into_iter()
            .map(MetadataMappingRule::from)
            .collect(),
        supported_source_keys: SUPPORTED_KEY_NAMES,
        reingest_in_progress: reingest_in_progress(),
    }))
}

fn get_metadata_mapping_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Get metadata mapping configuration")
        .description(
            "Returns the currently active metadata mapping rules, the monotonic version \
             counter, and the supported set of `ItemKey`-variant source key names.",
        )
}

async fn put_metadata_mapping(
    headers: HeaderMap,
    Json(body): Json<PutMetadataMappingRequest>,
) -> Result<axum::response::Response, AppError> {
    require_manage_metadata(&headers).await?;

    // Atomic CAS at the route boundary: concurrent PUTs race here
    // before any work runs. Losers get 409 immediately instead of a
    // misleading 202 followed by the spawned task silently bailing.
    let guard = ReingestGuard::try_acquire()
        .ok_or_else(|| AppError::conflict("a metadata mapping reingest is already in progress"))?;

    let candidate = build_candidate(body.rules)?;
    // Dry-run is synchronous — a full library scan proportional to
    // the file count. The PUT itself re-runs the dry-run here, so
    // large libraries may exceed typical HTTP client timeouts:
    // callers should raise the commit call's timeout accordingly.
    // `POST /preview` lets admins see the impact report separately
    // without committing; it does not skip the PUT's own re-run.
    // Background-job dry-run with polling is v2 scope.
    let report = dry_run_config(&candidate).await?;
    if !body.force_partial && (report.read_failures > 0 || report.would_reject > 0) {
        return Err(AppError::bad_request(format!(
            "dry-run surfaced {} read failure(s) and {} track(s) that would be rejected \
             on post-mapping required-field check; pass force_partial=true to replace \
             anyway",
            report.read_failures, report.would_reject,
        )));
    }

    let rules_accepted = candidate.rules.len();
    tokio::spawn(async move {
        if let Err(err) = commit_and_reingest(candidate, guard).await {
            tracing::error!(error = %err, "metadata mapping commit_and_reingest failed");
        }
    });
    Ok((
        StatusCode::ACCEPTED,
        Json(PutMetadataMappingResponse {
            accepted: true,
            rules_accepted,
        }),
    )
        .into_response())
}

fn put_metadata_mapping_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Replace metadata mapping configuration")
        .description(
            "Runs a dry-run against the candidate rules. Rejected with 400 if the \
             dry-run shows any read failures or tracks that would fail the post-mapping \
             required-field check, unless `force_partial` is true. When accepted, returns \
             202 immediately and performs the persist + reingest in a background task; \
             poll `GET /api/metadata/mapping` for `reingest_in_progress=false` and the new \
             `version`. Returns 409 if a metadata mapping reingest is already running.",
        )
        .response::<202, Json<PutMetadataMappingResponse>>()
}

async fn preview_metadata_mapping(
    headers: HeaderMap,
    Json(body): Json<PreviewMetadataMappingRequest>,
) -> Result<Json<DryRunReport>, AppError> {
    require_manage_metadata(&headers).await?;
    let candidate = build_candidate(body.rules)?;
    let report = dry_run_config(&candidate).await?;
    Ok(Json(report))
}

fn preview_metadata_mapping_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Preview metadata mapping configuration")
        .description(
            "Scans every audio file in the library graph, applies the candidate mapping, \
             and reports how many files would be rejected (missing required fields) and \
             how many reads would fail. Does not modify state.",
        )
}

fn build_candidate(rules: Vec<MetadataMappingRule>) -> Result<MetadataMappingConfig, AppError> {
    for rule in &rules {
        if mapping::resolve_item_key(&rule.source_key).is_none() {
            return Err(AppError::bad_request(format!(
                "unsupported source_key '{}': not a recognised ItemKey variant",
                rule.source_key
            )));
        }
    }
    Ok(MetadataMappingConfig {
        rules: rules.into_iter().map(MappingRule::from).collect(),
        version: 0,
    })
}

pub fn metadata_routes() -> ApiRouter {
    ApiRouter::new()
        .api_route(
            "/mapping",
            get_with(get_metadata_mapping, get_metadata_mapping_docs)
                .put_with(put_metadata_mapping, put_metadata_mapping_docs),
        )
        .api_route(
            "/mapping/preview",
            post_with(preview_metadata_mapping, preview_metadata_mapping_docs),
        )
}
