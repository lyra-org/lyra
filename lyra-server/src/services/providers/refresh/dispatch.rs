// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::time::Duration;

use anyhow::Context;

use crate::services::{
    EntityType,
    STATE,
    plugin_dispatch::{
        MetadataRefreshRequest,
        MetadataRefreshResult,
        PluginCallbackHandle,
    },
    providers::{
        ProviderCallError,
        ProviderCallStage,
        ProviderLookup,
        with_provider_call,
    },
};

const DEFAULT_METADATA_REFRESH_TIMEOUT: Duration = Duration::from_secs(60);

/// How long past a handler's deadline the server still waits for the plugin
/// runtime's answer. The runtime answers a late handler only once it has
/// cancelled its thread, so the provider's call lock, which keeps one
/// provider's refreshes from overlapping, is released only after that.
const METADATA_REFRESH_DEADLINE_GRACE: Duration = Duration::from_secs(5);

async fn run_metadata_handler(
    role: &str,
    handler: PluginCallbackHandle,
    context: serde_json::Value,
) -> anyhow::Result<MetadataRefreshResult> {
    let runtime = STATE
        .generation()
        .plugin_dispatch
        .get()
        .context("plugin runtime is not initialized")?;
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
            "{role} handler timed out after {} ms",
            DEFAULT_METADATA_REFRESH_TIMEOUT.as_millis()
        )
    })?
}

pub(super) async fn dispatch_refresh(
    provider_id: &str,
    entity_type: EntityType,
    context: serde_json::Value,
) -> Result<(), ProviderCallError> {
    with_provider_call(
        provider_id,
        ProviderCallStage::MetadataRefresh,
        |registry| {
            registry
                .get_refresh_callback(provider_id, entity_type)
                .copied()
                .map(ProviderLookup::Call)
        },
        |handler| async move {
            run_metadata_handler("metadata refresh", handler, context)
                .await
                .map(|_| ())
        },
    )
    .await
}

/// Whether the provider's sync filter lets the item through. Without a filter, or when the pass
/// applies none, a provider with a refresh handler runs unfiltered.
pub(super) async fn dispatch_sync_filter(
    provider_id: &str,
    entity_type: EntityType,
    apply_sync_filters: bool,
    context: &serde_json::Value,
) -> Result<bool, ProviderCallError> {
    with_provider_call(
        provider_id,
        ProviderCallStage::MetadataRefresh,
        |registry| {
            if apply_sync_filters
                && let Some(filter) = registry.get_sync_filter_callback(provider_id, entity_type)
            {
                return Some(ProviderLookup::Call(*filter));
            }
            registry
                .has_refresh_callback(provider_id, entity_type)
                .then_some(ProviderLookup::Answer(true))
        },
        |handler| async move {
            let result = run_metadata_handler("sync filter", handler, context.clone()).await?;
            Ok(result
                .values
                .first()
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false))
        },
    )
    .await
}
