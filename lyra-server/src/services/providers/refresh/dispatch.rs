// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::time::Duration;

use anyhow::Context;

use crate::services::{
    STATE,
    plugin_dispatch::{
        MetadataRefreshRequest,
        PluginCallbackHandle,
    },
    providers::{
        ProviderCallStage,
        ProviderServiceError,
        with_provider_call,
    },
};

const DEFAULT_METADATA_REFRESH_TIMEOUT: Duration = Duration::from_secs(60);

/// How long past a handler's deadline the server still waits for the plugin
/// runtime's answer. The runtime answers a late handler only once it has
/// cancelled its thread, so the provider's call lock, which keeps one
/// provider's refreshes from overlapping, is released only after that.
const METADATA_REFRESH_DEADLINE_GRACE: Duration = Duration::from_secs(5);

pub(super) async fn dispatch_refresh_callback(
    provider_id: &str,
    handler: PluginCallbackHandle,
    context: serde_json::Value,
) -> Result<(), ProviderServiceError> {
    with_provider_call(
        provider_id,
        ProviderCallStage::MetadataRefresh,
        || async move {
            let runtime = match STATE.generation().plugin_dispatch.get() {
                Some(runtime) => runtime,
                None => {
                    return Err(anyhow::anyhow!("plugin runtime is not initialized"));
                }
            };
            tokio::time::timeout(
                DEFAULT_METADATA_REFRESH_TIMEOUT + METADATA_REFRESH_DEADLINE_GRACE,
                runtime.dispatch_metadata_refresh(MetadataRefreshRequest {
                    handler,
                    context,
                    deadline: std::time::Instant::now() + DEFAULT_METADATA_REFRESH_TIMEOUT,
                }),
            )
            .await
            .map_err(|_| {
                anyhow::anyhow!(
                    "metadata refresh handler timed out after {} ms",
                    DEFAULT_METADATA_REFRESH_TIMEOUT.as_millis()
                )
            })?
            .map(|_| ())
        },
    )
    .await
    .map_err(ProviderServiceError::Internal)
}

pub(super) async fn dispatch_sync_filter_callback(
    provider_id: &str,
    handler: PluginCallbackHandle,
    context: serde_json::Value,
) -> anyhow::Result<bool> {
    with_provider_call(
        provider_id,
        ProviderCallStage::MetadataRefresh,
        || async move {
            let runtime = STATE
                .generation()
                .plugin_dispatch
                .get()
                .context("plugin runtime is not initialized")?;
            let result = tokio::time::timeout(
                DEFAULT_METADATA_REFRESH_TIMEOUT + METADATA_REFRESH_DEADLINE_GRACE,
                runtime.dispatch_metadata_refresh(MetadataRefreshRequest {
                    handler,
                    context,
                    deadline: std::time::Instant::now() + DEFAULT_METADATA_REFRESH_TIMEOUT,
                }),
            )
            .await
            .map_err(|_| {
                anyhow::anyhow!(
                    "sync filter handler timed out after {} ms",
                    DEFAULT_METADATA_REFRESH_TIMEOUT.as_millis()
                )
            })??;
            Ok(result
                .values
                .first()
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false))
        },
    )
    .await
}
