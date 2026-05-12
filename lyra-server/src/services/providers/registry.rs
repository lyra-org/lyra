// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::{
        HashMap,
        HashSet,
    },
    future::Future,
    sync::{
        Arc,
        LazyLock,
    },
    time::{
        Duration,
        Instant,
    },
};

use anyhow::{
    Result,
    bail,
};
use tokio::sync::RwLock;

use super::super::options::OptionDeclaration;
use crate::plugins::lifecycle::{
    PluginFunctionHandle,
    PluginId,
    PluginScopedInner,
    ScopedRegistry,
};
use crate::services::EntityType;
use crate::services::metadata::lyrics::providers::unregister_handlers_for_plugin as unregister_lyrics_handlers_for_plugin;

pub(crate) static PROVIDER_REGISTRY: LazyLock<Arc<RwLock<ProviderRegistry>>> =
    LazyLock::new(|| Arc::new(RwLock::new(ProviderRegistry::new())));

pub(crate) static SYNC_LOCKS: LazyLock<Arc<tokio::sync::Mutex<HashSet<String>>>> =
    LazyLock::new(|| Arc::new(tokio::sync::Mutex::new(HashSet::new())));

pub(crate) static LIBRARY_REFRESH_LOCKS: LazyLock<Arc<tokio::sync::Mutex<HashSet<agdb::DbId>>>> =
    LazyLock::new(|| Arc::new(tokio::sync::Mutex::new(HashSet::new())));

static PROVIDER_CALL_LOCKS: LazyLock<
    Arc<tokio::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
> = LazyLock::new(|| Arc::new(tokio::sync::Mutex::new(HashMap::new())));

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProviderCallStage {
    MetadataRefresh,
    CoverSearch,
    Lyrics,
}

impl ProviderCallStage {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::MetadataRefresh => "metadata_refresh",
            Self::CoverSearch => "cover_search",
            Self::Lyrics => "lyrics",
        }
    }
}

pub(crate) async fn with_provider_call<T, F, Fut>(
    provider_id: &str,
    stage: ProviderCallStage,
    call: F,
) -> T
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = T>,
{
    let lock = {
        let mut locks = PROVIDER_CALL_LOCKS.lock().await;
        locks
            .entry(provider_id.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    };

    let wait_started = Instant::now();
    let _guard = lock.lock().await;
    let waited = wait_started.elapsed();
    tracing::debug!(
        provider_id,
        stage = stage.as_str(),
        waited_ms = waited.as_millis() as u64,
        waited = waited > Duration::ZERO,
        "provider call lock acquired"
    );

    call().await
}

/// Registered metadata providers, bucketed by the plugin that declared them.
/// `plugin_by_provider` is the derived O(1) dispatch index rebuilt after
/// every teardown — the outer map is the source of truth.
#[derive(Default)]
pub(crate) struct ProviderRegistry {
    providers: HashMap<PluginId, HashMap<String, ProviderState>>,
    plugin_by_provider: HashMap<String, PluginId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ProviderIdSpec {
    pub(crate) id: String,
    pub(crate) entity: EntityType,
    pub(crate) unique: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct ProviderCoverRequireSpec {
    pub(crate) all_of: Vec<String>,
    pub(crate) any_of: Vec<String>,
}

#[derive(Clone)]
pub(crate) struct ProviderCoverSpec {
    pub(crate) priority: i64,
    /// Per-call handler timeout. Mirrors the lyrics dispatcher's `timeout`
    /// field; defaulted at parse time to `DEFAULT_COVER_HANDLER_TIMEOUT`
    /// so existing plugins that don't pass `timeout_ms` keep working.
    pub(crate) timeout: Duration,
    pub(crate) require: ProviderCoverRequireSpec,
    pub(crate) handler: PluginFunctionHandle,
}

impl ProviderRegistry {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn clear(&mut self) {
        self.providers.clear();
        self.plugin_by_provider.clear();
    }

    pub(crate) fn register(&mut self, plugin_id: PluginId, id: String) -> Result<()> {
        if let Some(existing) = self.plugin_by_provider.get(&id) {
            bail!("provider '{id}' already registered by plugin '{existing}'");
        }
        self.providers
            .entry(plugin_id.clone())
            .or_default()
            .insert(id.clone(), ProviderState::default());
        self.plugin_by_provider.insert(id, plugin_id);
        Ok(())
    }

    fn state(&self, provider_id: &str) -> Option<&ProviderState> {
        let plugin_id = self.plugin_by_provider.get(provider_id)?;
        self.providers.get(plugin_id)?.get(provider_id)
    }

    fn state_mut(&mut self, provider_id: &str) -> Option<&mut ProviderState> {
        let plugin_id = self.plugin_by_provider.get(provider_id)?.clone();
        self.providers.get_mut(&plugin_id)?.get_mut(provider_id)
    }

    fn iter_states(&self) -> impl Iterator<Item = (&String, &ProviderState)> {
        self.providers.values().flat_map(|bucket| bucket.iter())
    }

    pub(crate) fn set_id_registration(
        &mut self,
        provider_id: &str,
        id_spec: ProviderIdSpec,
        generator: Option<PluginFunctionHandle>,
    ) {
        if let Some(provider) = self.state_mut(provider_id) {
            provider
                .id_specs
                .insert(id_spec.id.clone(), id_spec.clone());
            if let Some(generator) = generator {
                provider.id_generators.insert(id_spec.id, generator);
            } else {
                provider.id_generators.remove(&id_spec.id);
            }
        }
    }

    pub(crate) fn set_search_handler(
        &mut self,
        provider_id: &str,
        entity_type: EntityType,
        handler: PluginFunctionHandle,
    ) {
        if let Some(provider) = self.state_mut(provider_id) {
            provider.search_handlers.insert(entity_type, handler);
        }
    }

    pub(crate) fn get_search_handler(
        &self,
        provider_id: &str,
        entity_type: EntityType,
    ) -> Option<&PluginFunctionHandle> {
        self.state(provider_id)
            .and_then(|provider| provider.search_handlers.get(&entity_type))
    }

    pub(crate) fn set_cover_handler(
        &mut self,
        provider_id: &str,
        entity_type: EntityType,
        spec: ProviderCoverSpec,
    ) {
        if let Some(provider) = self.state_mut(provider_id) {
            let handlers = provider.cover_handlers.entry(entity_type).or_default();
            handlers.push(spec);
            handlers.sort_by(|a, b| b.priority.cmp(&a.priority));
        }
    }

    pub(crate) fn get_cover_handlers(
        &self,
        provider_id: &str,
        entity_type: EntityType,
    ) -> Vec<ProviderCoverSpec> {
        self.state(provider_id)
            .and_then(|provider| provider.cover_handlers.get(&entity_type))
            .cloned()
            .unwrap_or_default()
    }

    pub(crate) fn set_refresh_handler(
        &mut self,
        provider_id: &str,
        entity_type: EntityType,
        handler: PluginFunctionHandle,
    ) {
        if let Some(provider) = self.state_mut(provider_id) {
            provider.refresh_handlers.insert(entity_type, handler);
        }
    }

    pub(crate) fn get_refresh_handler(
        &self,
        provider_id: &str,
        entity_type: EntityType,
    ) -> Option<&PluginFunctionHandle> {
        self.state(provider_id)
            .and_then(|provider| provider.refresh_handlers.get(&entity_type))
    }

    pub(crate) fn set_sync_filter(
        &mut self,
        provider_id: &str,
        entity_type: EntityType,
        filter: PluginFunctionHandle,
    ) {
        if let Some(provider) = self.state_mut(provider_id) {
            provider.sync_filters.insert(entity_type, filter);
        }
    }

    pub(crate) fn get_sync_filter(
        &self,
        provider_id: &str,
        entity_type: EntityType,
    ) -> Option<&PluginFunctionHandle> {
        self.state(provider_id)
            .and_then(|provider| provider.sync_filters.get(&entity_type))
    }

    pub(crate) fn providers_with_refresh_handler(&self, entity_type: EntityType) -> Vec<String> {
        self.iter_states()
            .filter(|(_, state)| state.refresh_handlers.contains_key(&entity_type))
            .map(|(id, _)| id.clone())
            .collect()
    }

    pub(crate) fn unique_id_pairs(&self, entity: EntityType) -> HashSet<(String, String)> {
        let mut pairs = HashSet::new();
        for (provider_id, state) in self.iter_states() {
            for spec in state.id_specs.values() {
                if spec.entity == entity && spec.unique {
                    pairs.insert((provider_id.clone(), spec.id.clone()));
                }
            }
        }
        pairs
    }

    pub(crate) fn unique_track_id_pairs(&self) -> HashSet<(String, String)> {
        self.unique_id_pairs(EntityType::Track)
    }

    #[cfg(test)]
    pub(crate) fn id_registration(
        &self,
        provider_id: &str,
        id_type: &str,
    ) -> Option<(ProviderIdSpec, bool)> {
        let provider = self.state(provider_id)?;
        let spec = provider.id_specs.get(id_type)?.clone();
        let has_generator = provider.id_generators.contains_key(id_type);
        Some((spec, has_generator))
    }

    pub(crate) fn id_spec_matches_entity(
        &self,
        provider_id: &str,
        id_type: &str,
        entity: EntityType,
    ) -> bool {
        self.state(provider_id)
            .and_then(|state| state.id_specs.get(id_type))
            .is_some_and(|spec| spec.entity == entity)
    }

    pub(crate) fn declare_option(
        &mut self,
        provider_id: &str,
        option: OptionDeclaration,
    ) -> std::result::Result<(), String> {
        if let Some(provider) = self.state_mut(provider_id) {
            if provider.options.iter().any(|o| o.name == option.name) {
                return Err(format!(
                    "option '{}' already declared on provider '{}'",
                    option.name, provider_id
                ));
            }
            provider.options.push(option);
            Ok(())
        } else {
            Err(format!("provider '{}' not registered", provider_id))
        }
    }

    pub(crate) fn get_options(&self, provider_id: &str) -> &[OptionDeclaration] {
        self.state(provider_id)
            .map(|p| p.options.as_slice())
            .unwrap_or(&[])
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{
            AtomicUsize,
            Ordering,
        },
    };

    use tokio::{
        sync::Barrier,
        time::{
            Duration,
            sleep,
            timeout,
        },
    };

    use super::{
        ProviderCallStage,
        with_provider_call,
    };

    #[tokio::test]
    async fn provider_calls_are_serialized_per_provider() {
        let provider_id = format!("test-provider-{}", nanoid::nanoid!());
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));

        let make_call = |active: Arc<AtomicUsize>, max_active: Arc<AtomicUsize>| {
            let provider_id = provider_id.clone();
            async move {
                with_provider_call(&provider_id, ProviderCallStage::MetadataRefresh, || async {
                    let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                    max_active.fetch_max(current, Ordering::SeqCst);
                    sleep(Duration::from_millis(5)).await;
                    active.fetch_sub(1, Ordering::SeqCst);
                })
                .await;
            }
        };

        tokio::join!(
            make_call(active.clone(), max_active.clone()),
            make_call(active.clone(), max_active.clone()),
            make_call(active.clone(), max_active.clone())
        );

        assert_eq!(max_active.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn provider_calls_for_different_providers_can_overlap() {
        let provider_a = format!("test-provider-a-{}", nanoid::nanoid!());
        let provider_b = format!("test-provider-b-{}", nanoid::nanoid!());
        let barrier = Arc::new(Barrier::new(2));

        let make_call = |provider_id: String| {
            let barrier = barrier.clone();
            async move {
                with_provider_call(&provider_id, ProviderCallStage::MetadataRefresh, || async {
                    barrier.wait().await;
                })
                .await;
            }
        };

        let joined = timeout(Duration::from_secs(1), async {
            tokio::join!(make_call(provider_a), make_call(provider_b));
        })
        .await;

        assert!(
            joined.is_ok(),
            "different providers should not share a lock"
        );
    }
}

impl PluginScopedInner for ProviderRegistry {
    fn clear_bucket(&mut self, plugin_id: &PluginId) {
        self.providers.remove(plugin_id);
    }

    fn rebuild_derived(&mut self) {
        self.plugin_by_provider.clear();
        for (plugin_id, bucket) in &self.providers {
            for provider_id in bucket.keys() {
                self.plugin_by_provider
                    .insert(provider_id.clone(), plugin_id.clone());
            }
        }
    }
}

#[derive(Default)]
struct ProviderState {
    id_generators: HashMap<String, PluginFunctionHandle>,
    id_specs: HashMap<String, ProviderIdSpec>,
    search_handlers: HashMap<EntityType, PluginFunctionHandle>,
    cover_handlers: HashMap<EntityType, Vec<ProviderCoverSpec>>,
    refresh_handlers: HashMap<EntityType, PluginFunctionHandle>,
    sync_filters: HashMap<EntityType, PluginFunctionHandle>,
    options: Vec<OptionDeclaration>,
}

pub(crate) async fn reset_provider_registry_for_test() {
    PROVIDER_REGISTRY.write().await.clear();
    SYNC_LOCKS.lock().await.clear();
    LIBRARY_REFRESH_LOCKS.lock().await.clear();
    PROVIDER_CALL_LOCKS.lock().await.clear();
}

pub(crate) async fn teardown_plugin_providers(plugin_id: &PluginId) {
    // Capture the plugin's provider_ids before the registry bucket is
    // cleared so we can also purge the out-of-band SYNC_LOCKS entries
    // they own. Without this, a plugin that crashed mid-sync would see
    // "sync already in progress" forever after restart — the lock
    // lives outside the registry and never hears about teardown
    // otherwise.
    let owned_provider_ids: Vec<String> = {
        let registry = PROVIDER_REGISTRY.read().await;
        registry
            .providers
            .get(plugin_id)
            .map(|bucket| bucket.keys().cloned().collect())
            .unwrap_or_default()
    };

    ScopedRegistry::from_shared(PROVIDER_REGISTRY.clone())
        .teardown(plugin_id)
        .await;

    // Lyrics handlers have their own registry; without this purge a reload
    // would leave handles backed by a torn-down Lua function.
    unregister_lyrics_handlers_for_plugin(plugin_id).await;

    if !owned_provider_ids.is_empty() {
        let mut locks = SYNC_LOCKS.lock().await;
        for id in &owned_provider_ids {
            locks.remove(id);
        }
        drop(locks);

        let mut call_locks = PROVIDER_CALL_LOCKS.lock().await;
        for id in &owned_provider_ids {
            call_locks.remove(id);
        }
    }

    // LIBRARY_REFRESH_LOCKS is keyed by library db_id (not plugin) and
    // outlives any single plugin — a library's refresh task owns its
    // own lock lifecycle. Intentionally untouched here.
}
