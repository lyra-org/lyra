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
    sync::Arc,
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
use super::schemes::IdSchemes;
use crate::plugins::lifecycle::{
    PluginId,
    PluginScopedInner,
    ScopedRegistry,
};
use crate::services::EntityType;
use crate::services::metadata::layers::LOCAL_SOURCE_ID;
use crate::services::metadata::lyrics::providers::unregister_handlers_for_plugin as unregister_lyrics_handlers_for_plugin;

pub(crate) const DEFAULT_SIMILAR_RELEASES_HANDLER_TIMEOUT: Duration = Duration::from_secs(10);
pub(crate) const MAX_SIMILAR_RELEASES_HANDLER_TIMEOUT: Duration = Duration::from_secs(10);

/// Generation-owned provider state: the registry itself plus the in-flight
/// sync/refresh/call locks keyed by provider and library ids.
#[derive(Default)]
pub(crate) struct ProviderRegistries {
    registry: Arc<RwLock<ProviderRegistry>>,
    sync_locks: Arc<tokio::sync::Mutex<HashSet<String>>>,
    library_refresh_locks: Arc<tokio::sync::Mutex<HashSet<agdb::DbId>>>,
    call_locks: Arc<tokio::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
}

impl ProviderRegistries {
    pub(crate) fn registry(&self) -> Arc<RwLock<ProviderRegistry>> {
        self.registry.clone()
    }
}

pub(crate) fn provider_registry() -> Arc<RwLock<ProviderRegistry>> {
    crate::STATE.generation().providers.registry()
}

pub(crate) async fn id_schemes() -> IdSchemes {
    provider_registry().read().await.id_schemes()
}

pub(crate) fn sync_locks() -> Arc<tokio::sync::Mutex<HashSet<String>>> {
    crate::STATE.generation().providers.sync_locks.clone()
}

pub(crate) fn library_refresh_locks() -> Arc<tokio::sync::Mutex<HashSet<agdb::DbId>>> {
    crate::STATE
        .generation()
        .providers
        .library_refresh_locks
        .clone()
}

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
    let generation = crate::STATE.generation();
    let lock = {
        let mut locks = generation.providers.call_locks.clone().lock_owned().await;
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
    pub(crate) id_type: String,
    pub(crate) entity: EntityType,
    pub(crate) unique: bool,
    pub(crate) scheme: Option<String>,
}

#[derive(Clone)]
pub(crate) enum ProviderIdUrlGenerator {
    Template(String),
}

#[derive(Clone, Debug)]
pub(crate) struct ProviderCallbackHandle {
    pub(crate) handler_id: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct ProviderRequireSpec {
    pub(crate) all_of: Vec<String>,
    pub(crate) any_of: Vec<String>,
}

#[derive(Clone)]
pub(crate) struct ProviderCoverSpec {
    pub(crate) priority: i64,
    /// Per-call handler timeout, defaulted while parsing provider config.
    pub(crate) timeout: Duration,
    pub(crate) require: ProviderRequireSpec,
    pub(crate) handler: ProviderCallbackHandle,
}

#[derive(Clone)]
pub(crate) struct ProviderSimilarReleasesSpec {
    pub(crate) timeout: Duration,
    pub(crate) require: ProviderRequireSpec,
    pub(crate) handler: ProviderCallbackHandle,
}

impl ProviderRegistry {
    pub(crate) fn register(&mut self, plugin_id: PluginId, id: String) -> Result<()> {
        if id == LOCAL_SOURCE_ID {
            bail!(
                "provider id '{LOCAL_SOURCE_ID}' is reserved for file-derived metadata and cannot be registered by a plugin"
            );
        }
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

    /// Schemes resolve by `(provider_id, id_type)`, so every entity
    /// registering an id type must declare the same scheme.
    pub(crate) fn set_id_registration(
        &mut self,
        provider_id: &str,
        id_spec: ProviderIdSpec,
        generator: Option<ProviderIdUrlGenerator>,
    ) -> std::result::Result<(), String> {
        let Some(provider) = self.state_mut(provider_id) else {
            return Ok(());
        };
        if let Some(conflict) = provider.id_specs.values().find(|existing| {
            existing.id_type == id_spec.id_type
                && existing.entity != id_spec.entity
                && existing.scheme != id_spec.scheme
        }) {
            return Err(format!(
                "id_type '{}' on provider '{provider_id}' is registered for {} with scheme '{}'; {} must use the same scheme",
                id_spec.id_type,
                conflict.entity,
                conflict.scheme.as_deref().unwrap_or("none"),
                id_spec.entity
            ));
        }
        let key = (id_spec.entity, id_spec.id_type.clone());
        match generator {
            Some(ProviderIdUrlGenerator::Template(template)) => {
                provider
                    .id_generators
                    .insert(key.clone(), ProviderIdUrlGenerator::Template(template));
            }
            None => {
                provider.id_generators.remove(&key);
            }
        }
        provider.id_specs.insert(key, id_spec);
        Ok(())
    }

    pub(crate) fn set_refresh_callback(
        &mut self,
        provider_id: &str,
        entity_type: EntityType,
        handler: ProviderCallbackHandle,
    ) {
        if let Some(provider) = self.state_mut(provider_id) {
            provider.refresh_callbacks.insert(entity_type, handler);
        }
    }

    pub(crate) fn get_refresh_callback(
        &self,
        provider_id: &str,
        entity_type: EntityType,
    ) -> Option<&ProviderCallbackHandle> {
        self.state(provider_id)
            .and_then(|provider| provider.refresh_callbacks.get(&entity_type))
    }

    pub(crate) fn providers_with_refresh_handler(&self, entity_type: EntityType) -> Vec<String> {
        let mut providers = self
            .iter_states()
            .filter(|(_, state)| state.refresh_callbacks.contains_key(&entity_type))
            .map(|(provider_id, _)| provider_id.clone())
            .collect::<Vec<_>>();
        providers.sort();
        providers
    }

    pub(crate) fn set_sync_filter_callback(
        &mut self,
        provider_id: &str,
        entity_type: EntityType,
        filter: ProviderCallbackHandle,
    ) {
        if let Some(provider) = self.state_mut(provider_id) {
            provider.sync_filter_callbacks.insert(entity_type, filter);
        }
    }

    pub(crate) fn set_search_callback(
        &mut self,
        provider_id: &str,
        entity_type: EntityType,
        handler: ProviderCallbackHandle,
    ) {
        if let Some(provider) = self.state_mut(provider_id) {
            provider.search_callbacks.insert(entity_type, handler);
        }
    }

    pub(crate) fn get_search_callback(
        &self,
        provider_id: &str,
        entity_type: EntityType,
    ) -> Option<&ProviderCallbackHandle> {
        self.state(provider_id)
            .and_then(|provider| provider.search_callbacks.get(&entity_type))
    }

    pub(crate) fn set_cover_handler(
        &mut self,
        provider_id: &str,
        entity_type: EntityType,
        spec: ProviderCoverSpec,
    ) {
        if let Some(provider) = self.state_mut(provider_id) {
            provider.cover_handlers.insert(entity_type, spec);
        }
    }

    pub(crate) fn get_cover_handler(
        &self,
        provider_id: &str,
        entity_type: EntityType,
    ) -> Option<&ProviderCoverSpec> {
        self.state(provider_id)
            .and_then(|provider| provider.cover_handlers.get(&entity_type))
    }

    pub(crate) fn set_similar_releases_handler(
        &mut self,
        provider_id: &str,
        spec: ProviderSimilarReleasesSpec,
    ) {
        if let Some(provider) = self.state_mut(provider_id) {
            provider.similar_releases_handler = Some(spec);
        }
    }

    pub(crate) fn get_similar_releases_handler(
        &self,
        provider_id: &str,
    ) -> Option<&ProviderSimilarReleasesSpec> {
        self.state(provider_id)
            .and_then(|provider| provider.similar_releases_handler.as_ref())
    }

    pub(crate) fn get_sync_filter_callback(
        &self,
        provider_id: &str,
        entity_type: EntityType,
    ) -> Option<&ProviderCallbackHandle> {
        self.state(provider_id)
            .and_then(|provider| provider.sync_filter_callbacks.get(&entity_type))
    }

    pub(crate) fn unique_id_pairs(&self, entity: EntityType) -> HashSet<(String, String)> {
        let mut pairs = HashSet::new();
        for (provider_id, state) in self.iter_states() {
            for spec in state.id_specs.values() {
                if spec.entity == entity && spec.unique {
                    pairs.insert((provider_id.clone(), spec.id_type.clone()));
                }
            }
        }
        pairs
    }

    pub(crate) fn id_pairs(&self, entity: EntityType) -> HashSet<(String, String)> {
        let mut pairs = HashSet::new();
        for (provider_id, state) in self.iter_states() {
            for spec in state.id_specs.values() {
                if spec.entity == entity {
                    pairs.insert((provider_id.clone(), spec.id_type.clone()));
                }
            }
        }
        pairs
    }

    pub(crate) fn id_schemes(&self) -> IdSchemes {
        let mut schemes = IdSchemes::default();
        for (provider_id, state) in self.iter_states() {
            for spec in state.id_specs.values() {
                if let Some(scheme) = &spec.scheme {
                    schemes.insert(provider_id, &spec.id_type, scheme);
                }
            }
        }
        schemes
    }

    pub(crate) fn unique_track_id_pairs(&self) -> HashSet<(String, String)> {
        self.unique_id_pairs(EntityType::Track)
    }

    pub(crate) fn id_spec_matches_entity(
        &self,
        provider_id: &str,
        id_type: &str,
        entity: EntityType,
    ) -> bool {
        self.state(provider_id)
            .is_some_and(|state| state.id_specs.contains_key(&(entity, id_type.to_string())))
    }

    pub(crate) fn id_spec_entities(&self, provider_id: &str, id_type: &str) -> Vec<EntityType> {
        self.state(provider_id)
            .map(|state| {
                state
                    .id_specs
                    .values()
                    .filter(|spec| spec.id_type == id_type)
                    .map(|spec| spec.entity)
                    .collect()
            })
            .unwrap_or_default()
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
pub(crate) mod tests {
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

    use super::*;

    pub(crate) fn id_registration(
        registry: &ProviderRegistry,
        provider_id: &str,
        entity: EntityType,
        id_type: &str,
    ) -> Option<(ProviderIdSpec, bool)> {
        let provider = registry.state(provider_id)?;
        let key = (entity, id_type.to_string());
        let spec = provider.id_specs.get(&key)?.clone();
        let has_generator = provider.id_generators.contains_key(&key);
        Some((spec, has_generator))
    }

    pub(crate) fn id_url_template(
        registry: &ProviderRegistry,
        provider_id: &str,
        entity: EntityType,
        id_type: &str,
    ) -> Option<String> {
        let provider = registry.state(provider_id)?;
        match provider.id_generators.get(&(entity, id_type.to_string()))? {
            ProviderIdUrlGenerator::Template(template) => Some(template.clone()),
        }
    }

    #[test]
    fn register_rejects_the_reserved_local_source_id() {
        let mut registry = ProviderRegistry::default();
        let plugin_id = PluginId::new("demo").expect("valid plugin id");

        let err = registry
            .register(plugin_id, LOCAL_SOURCE_ID.to_string())
            .expect_err("reserved source id must be rejected");

        assert!(err.to_string().contains(LOCAL_SOURCE_ID));
    }

    fn id_spec(id_type: &str, entity: EntityType, scheme: Option<&str>) -> ProviderIdSpec {
        ProviderIdSpec {
            id_type: id_type.to_string(),
            entity,
            unique: entity == EntityType::Release,
            scheme: scheme.map(str::to_string),
        }
    }

    fn template(url: &str) -> Option<ProviderIdUrlGenerator> {
        Some(ProviderIdUrlGenerator::Template(url.to_string()))
    }

    #[test]
    fn id_type_registered_for_several_entities_keeps_each_registration() {
        let mut registry = ProviderRegistry::default();
        let plugin_id = PluginId::new("demo").expect("valid plugin id");
        registry
            .register(plugin_id, "demo".to_string())
            .expect("register provider");

        for (entity, url) in [
            (EntityType::Release, "https://example.test/release/{id}"),
            (EntityType::Artist, "https://example.test/artist/{id}"),
        ] {
            registry
                .set_id_registration(
                    "demo",
                    id_spec("item", entity, Some("example:item")),
                    template(url),
                )
                .expect("same scheme is accepted");
        }

        for entity in [EntityType::Release, EntityType::Artist] {
            assert!(registry.id_spec_matches_entity("demo", "item", entity));
            assert!(
                registry
                    .id_pairs(entity)
                    .contains(&("demo".to_string(), "item".to_string()))
            );
        }
        assert!(!registry.id_spec_matches_entity("demo", "item", EntityType::Track));
        assert!(
            registry
                .unique_id_pairs(EntityType::Release)
                .contains(&("demo".to_string(), "item".to_string()))
        );
        assert!(registry.unique_id_pairs(EntityType::Artist).is_empty());
        assert_eq!(
            id_url_template(&registry, "demo", EntityType::Artist, "item").as_deref(),
            Some("https://example.test/artist/{id}")
        );
        let mut entities = registry.id_spec_entities("demo", "item");
        entities.sort_by_key(|entity| entity.as_str());
        assert_eq!(entities, vec![EntityType::Artist, EntityType::Release]);
    }

    #[test]
    fn id_type_rejects_a_different_scheme_on_another_entity() {
        let mut registry = ProviderRegistry::default();
        let plugin_id = PluginId::new("demo").expect("valid plugin id");
        registry
            .register(plugin_id, "demo".to_string())
            .expect("register provider");
        registry
            .set_id_registration(
                "demo",
                id_spec("item", EntityType::Release, Some("example:item")),
                None,
            )
            .expect("first registration");

        let err = registry
            .set_id_registration(
                "demo",
                id_spec("item", EntityType::Artist, Some("example:other")),
                None,
            )
            .expect_err("conflicting scheme must be rejected");

        assert!(err.contains("same scheme"), "{err}");
        assert!(!registry.id_spec_matches_entity("demo", "item", EntityType::Artist));
    }

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
    id_generators: HashMap<(EntityType, String), ProviderIdUrlGenerator>,
    id_specs: HashMap<(EntityType, String), ProviderIdSpec>,
    search_callbacks: HashMap<EntityType, ProviderCallbackHandle>,
    refresh_callbacks: HashMap<EntityType, ProviderCallbackHandle>,
    sync_filter_callbacks: HashMap<EntityType, ProviderCallbackHandle>,
    cover_handlers: HashMap<EntityType, ProviderCoverSpec>,
    similar_releases_handler: Option<ProviderSimilarReleasesSpec>,
    options: Vec<OptionDeclaration>,
}

pub(crate) async fn teardown_plugin_providers(plugin_id: &PluginId) {
    // One generation snapshot for the whole teardown so the captured
    // provider_ids and the purged locks belong to the same registries.
    let generation = crate::STATE.generation();
    let providers = &generation.providers;

    // Capture the plugin's provider_ids before the registry bucket is
    // cleared so we can also purge the out-of-band sync_locks entries
    // they own. Without this, a plugin that crashed mid-sync would see
    // "sync already in progress" forever after restart — the lock
    // lives outside the registry and never hears about teardown
    // otherwise.
    let owned_provider_ids: Vec<String> = {
        let registry = providers.registry.read().await;
        registry
            .providers
            .get(plugin_id)
            .map(|bucket| bucket.keys().cloned().collect())
            .unwrap_or_default()
    };

    ScopedRegistry::from_shared(providers.registry.clone())
        .teardown(plugin_id)
        .await;

    // Lyrics handlers have their own registry; without this purge a reload
    // would leave handles backed by a torn-down Lua function.
    unregister_lyrics_handlers_for_plugin(plugin_id).await;

    if !owned_provider_ids.is_empty() {
        let mut locks = providers.sync_locks.lock().await;
        for id in &owned_provider_ids {
            locks.remove(id);
        }
        drop(locks);

        let mut call_locks = providers.call_locks.lock().await;
        for id in &owned_provider_ids {
            call_locks.remove(id);
        }
    }

    // library_refresh_locks is keyed by library db_id (not plugin) and
    // outlives any single plugin — a library's refresh task owns its
    // own lock lifecycle. Intentionally untouched here.
}
