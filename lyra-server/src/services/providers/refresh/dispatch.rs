// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::time::Duration;

use crate::services::{
    EntityType,
    plugin_dispatch::{
        CallSource,
        MetadataRefreshRequest,
        MetadataRefreshResult,
        PluginCallbackHandle,
        RuntimeCalls,
        call_metadata_handler,
    },
    providers::{
        ProviderCallError,
        ProviderCallStage,
        ProviderLookup,
        with_provider_call_on,
    },
};

const DEFAULT_METADATA_REFRESH_TIMEOUT: Duration = Duration::from_secs(60);

async fn run_metadata_handler<C: RuntimeCalls>(
    role: &str,
    calls: &C,
    handler: PluginCallbackHandle,
    context: serde_json::Value,
) -> anyhow::Result<MetadataRefreshResult> {
    call_metadata_handler(
        calls,
        &format!("{role} handler"),
        MetadataRefreshRequest {
            handler,
            context,
            timeout: DEFAULT_METADATA_REFRESH_TIMEOUT,
        },
    )
    .await
}

pub(super) async fn dispatch_refresh<S: CallSource>(
    source: &S,
    provider_id: &str,
    entity_type: EntityType,
    context: serde_json::Value,
) -> Result<(), ProviderCallError> {
    with_provider_call_on(
        source,
        provider_id,
        ProviderCallStage::MetadataRefresh,
        |registry| {
            registry
                .get_refresh_callback(provider_id, entity_type)
                .copied()
                .map(ProviderLookup::Call)
        },
        |handler, calls| {
            let context = context.clone();
            async move {
                run_metadata_handler("metadata refresh", &calls, handler, context)
                    .await
                    .map(|_| ())
            }
        },
    )
    .await
}

/// Whether the provider's sync filter lets the item through. Without a filter, or when the pass
/// applies none, a provider with a refresh handler runs unfiltered.
pub(super) async fn dispatch_sync_filter<S: CallSource>(
    source: &S,
    provider_id: &str,
    entity_type: EntityType,
    apply_sync_filters: bool,
    context: &serde_json::Value,
) -> Result<bool, ProviderCallError> {
    with_provider_call_on(
        source,
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
        |handler, calls| async move {
            let result =
                run_metadata_handler("sync filter", &calls, handler, context.clone()).await?;
            Ok(result
                .values
                .first()
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false))
        },
    )
    .await
}
