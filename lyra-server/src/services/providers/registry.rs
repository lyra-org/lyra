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
use tokio_util::sync::CancellationToken;

use super::super::options::OptionDeclaration;
use super::links::{
    IdLinkCache,
    IdLinkGenerator,
    IdLinkGenerators,
};
use super::schemes::IdSchemes;
use crate::services::EntityType;
use crate::services::metadata::id_links::DeclaredIds;
use crate::services::metadata::layers::{
    LOCAL_SOURCE_ID,
    MANUAL_SOURCE_ID,
};
use crate::services::plugin_dispatch::{
    CallSource,
    DispatchNotStarted,
    PluginCallbackHandle,
    Published,
    PublishedCalls,
    RefreshTargetGone,
    RuntimeCalls,
    TransitionWait,
};
use crate::services::plugin_scope::{
    PluginId,
    PluginScopedInner,
    ScopedRegistry,
};

pub(crate) const DEFAULT_SIMILAR_RELEASES_HANDLER_TIMEOUT: Duration = Duration::from_secs(10);
pub(crate) const MAX_SIMILAR_RELEASES_HANDLER_TIMEOUT: Duration = Duration::from_secs(10);

/// A lyrics dispatch in flight: the track's db id and the provider id.
pub(crate) type LyricsInFlightKey = (i64, Arc<str>);
pub(crate) type LyricsInFlight = Arc<std::sync::Mutex<HashSet<LyricsInFlightKey>>>;

/// Generation-owned provider locks, keyed by provider and library ids. They belong to the passes
/// holding them, which outlive any one runtime.
#[derive(Default)]
pub(crate) struct ProviderLocks {
    sync_locks: Arc<std::sync::Mutex<HashSet<String>>>,
    library_refresh_locks: Arc<tokio::sync::Mutex<HashSet<agdb::DbId>>>,
    call_locks: Arc<tokio::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
}

/// The provider state of one plugin runtime: the registry, the links its generators produced, and
/// the lyrics dispatches in flight keyed by track and provider.
#[derive(Clone, Default)]
pub(crate) struct ProviderRuntimeState {
    registry: Arc<RwLock<ProviderRegistry>>,
    id_links: Arc<std::sync::Mutex<IdLinkCache>>,
    lyrics_in_flight: LyricsInFlight,
}

impl ProviderRuntimeState {
    pub(crate) fn registry(&self) -> Arc<RwLock<ProviderRegistry>> {
        self.registry.clone()
    }

    pub(crate) fn id_link_cache(&self) -> Arc<std::sync::Mutex<IdLinkCache>> {
        self.id_links.clone()
    }

    /// The link cache and in-flight lyrics stay: both are keyed by handler or track, not plugin,
    /// and entries of a removed handler are never read again.
    pub(crate) async fn teardown_plugin(&self, plugin_id: &PluginId) {
        let Self {
            registry,
            id_links: _,
            lyrics_in_flight: _,
        } = self;
        ScopedRegistry::from_shared(registry.clone())
            .teardown(plugin_id)
            .await;
    }

    /// Ends the lyrics work of a scope that is being replaced. Dropping the registry would too,
    /// but calls still holding the scope keep it alive.
    pub(crate) async fn cancel_lyrics_handlers(&self) {
        self.registry
            .read()
            .await
            .iter_states()
            .for_each(|(_, state)| state.cancel_lyrics_handler());
    }
}

/// The published scope's registry; an empty one before any scope is published.
pub(crate) fn provider_registry() -> Arc<RwLock<ProviderRegistry>> {
    crate::services::STATE
        .generation()
        .latest()
        .map_or_else(Default::default, |scope| scope.providers().registry())
}

pub(crate) async fn id_schemes() -> Arc<IdSchemes> {
    provider_registry().read().await.id_schemes()
}

pub(crate) async fn declared_artist_ids() -> DeclaredIds {
    provider_registry()
        .read()
        .await
        .declared_ids(EntityType::Artist)
}

pub(crate) fn sync_locks() -> Arc<std::sync::Mutex<HashSet<String>>> {
    crate::services::STATE
        .generation()
        .providers
        .sync_locks
        .clone()
}

pub(crate) fn library_refresh_locks() -> Arc<tokio::sync::Mutex<HashSet<agdb::DbId>>> {
    crate::services::STATE
        .generation()
        .providers
        .library_refresh_locks
        .clone()
}

pub(crate) fn lyrics_in_flight() -> LyricsInFlight {
    crate::services::STATE
        .generation()
        .latest()
        .map_or_else(Default::default, |scope| {
            scope.providers().lyrics_in_flight.clone()
        })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProviderCallStage {
    MetadataRefresh,
    CoverSearch,
    Lyrics,
    Search,
}

impl ProviderCallStage {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::MetadataRefresh => "metadata_refresh",
            Self::CoverSearch => "cover_search",
            Self::Lyrics => "lyrics",
            Self::Search => "search",
        }
    }
}

/// How a provider call ended without the handler's answer.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ProviderCallError {
    /// No handler is registered at the moment of the call, as during a plugin reload, or the
    /// runtime holding it never started the call because it is being replaced.
    #[error("no handler is registered for the call")]
    NotRegistered,
    /// The handler stopped because something it was writing is gone.
    #[error(transparent)]
    TargetGone(anyhow::Error),
    #[error(transparent)]
    Failed(anyhow::Error),
}

impl ProviderCallError {
    fn of_handler(error: anyhow::Error) -> Self {
        if RefreshTargetGone::caused(&error) {
            Self::TargetGone(error)
        } else {
            Self::Failed(error)
        }
    }
}

/// What a provider call's lookup found: `None` from the lookup means no handler is registered.
pub(crate) enum ProviderLookup<H, T> {
    Call(H),
    /// The answer the handler's settings already give, with no call to make.
    Answer(T),
}

/// Runs `call` with the handler `lookup` copies out of the registry and the scope that holds it,
/// under the provider's call lock. The registry lock is released before `call`: a running handler
/// can register others.
///
/// `lookup` runs before the lock too, so a provider with nothing to call neither waits behind
/// its other calls nor gets a lock entry, keeping those to providers that had a handler. The read
/// under the lock is the one that counts.
pub(crate) async fn with_provider_call<H, T, L, F, Fut>(
    provider_id: &str,
    stage: ProviderCallStage,
    wait: TransitionWait,
    lookup: L,
    call: F,
) -> Result<T, ProviderCallError>
where
    L: Fn(&ProviderRegistry) -> Option<ProviderLookup<H, T>>,
    F: Fn(H, PublishedCalls) -> Fut,
    Fut: Future<Output = Result<T>>,
{
    with_provider_call_on(&Published::new(wait), provider_id, stage, lookup, call).await
}

/// [`with_provider_call`] through `source`. The call lock is taken wherever the handler runs: it
/// keeps one provider's calls from overlapping. A wait never holds it.
///
/// A handler that never started is retried once from a fresh read, since no Luau ran. One that
/// started is not.
pub(crate) async fn with_provider_call_on<S, H, T, L, F, Fut>(
    source: &S,
    provider_id: &str,
    stage: ProviderCallStage,
    lookup: L,
    call: F,
) -> Result<T, ProviderCallError>
where
    S: CallSource,
    L: Fn(&ProviderRegistry) -> Option<ProviderLookup<H, T>>,
    F: Fn(H, S::Calls) -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let generation = crate::services::STATE.generation();
    let read_lookup = async |calls: &S::Calls| {
        let registry = calls.providers().await;
        lookup(&*registry.read().await).ok_or(ProviderCallError::NotRegistered)
    };

    let mut may_retry = true;
    loop {
        let calls = source
            .reach(provider_id)
            .await
            .ok_or(ProviderCallError::NotRegistered)?;
        if let ProviderLookup::Answer(answer) = read_lookup(&calls).await? {
            return Ok(answer);
        }

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

        // Waiting for the lock may have outlasted the scope read above; the next loop waits for
        // the transition without holding the lock.
        if source.in_transition(provider_id) {
            continue;
        }

        let calls = source.current().ok_or(ProviderCallError::NotRegistered)?;
        let handler = match read_lookup(&calls).await? {
            ProviderLookup::Call(handler) => handler,
            ProviderLookup::Answer(answer) => return Ok(answer),
        };
        match call(handler, calls).await {
            Ok(answer) => return Ok(answer),
            Err(error) if !DispatchNotStarted::caused(&error) => {
                return Err(ProviderCallError::of_handler(error));
            }
            Err(_) if may_retry => may_retry = false,
            Err(_) => return Err(ProviderCallError::NotRegistered),
        }
    }
}

/// The registry `source` reads handlers from, empty before anything is published.
pub(crate) async fn registry_for<S: CallSource>(source: &S) -> Arc<RwLock<ProviderRegistry>> {
    match source.current() {
        Some(calls) => calls.providers().await,
        None => Arc::default(),
    }
}

/// Registered metadata providers, bucketed by the plugin that declared them.
/// `plugin_by_provider` and the id snapshots are derived from the outer map,
/// the source of truth, and rebuilt whenever it changes.
#[derive(Default)]
pub(crate) struct ProviderRegistry {
    providers: HashMap<PluginId, HashMap<String, ProviderState>>,
    plugin_by_provider: HashMap<String, PluginId>,
    id_schemes: Arc<IdSchemes>,
    id_link_generators: Arc<IdLinkGenerators>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ProviderIdSpec {
    pub(crate) id_type: String,
    pub(crate) entity: EntityType,
    pub(crate) unique: bool,
    pub(crate) scheme: String,
    /// Site the links point to, when it isn't the provider itself.
    pub(crate) label: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) enum ProviderIdUrlGenerator {
    /// Absolute http(s) URL with a single `{id}` placeholder.
    Template(String),
    /// Plugin function called as `(id, ctx)` on the VM that registered it.
    Function(PluginCallbackHandle),
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct ProviderRequireSpec {
    pub(crate) all_of: Vec<String>,
    pub(crate) any_of: Vec<String>,
}

#[derive(Clone)]
pub(crate) struct ProviderCoverSpec {
    /// Per-call handler timeout, defaulted while parsing provider config.
    pub(crate) timeout: Duration,
    pub(crate) require: ProviderRequireSpec,
    pub(crate) handler: PluginCallbackHandle,
}

#[derive(Clone)]
pub(crate) struct ProviderSimilarReleasesSpec {
    pub(crate) timeout: Duration,
    pub(crate) require: ProviderRequireSpec,
    pub(crate) handler: PluginCallbackHandle,
}

/// `cancel` is cancelled once the handler leaves the registry: replaced,
/// torn down with its plugin, or with its runtime scope replaced.
#[derive(Clone)]
pub(crate) struct ProviderLyricsSpec {
    pub(crate) timeout: Duration,
    pub(crate) require: ProviderRequireSpec,
    pub(crate) handler: PluginCallbackHandle,
    pub(crate) cancel: CancellationToken,
}

impl ProviderLyricsSpec {
    pub(crate) fn new(
        timeout: Duration,
        require: ProviderRequireSpec,
        handler: PluginCallbackHandle,
    ) -> Self {
        Self {
            timeout,
            require,
            handler,
            cancel: CancellationToken::new(),
        }
    }
}

impl ProviderRegistry {
    pub(crate) fn register(
        &mut self,
        plugin_id: PluginId,
        id: String,
        display_name: Option<String>,
    ) -> Result<()> {
        lyra_db::providers::validate_provider_id(&id)?;
        if id == LOCAL_SOURCE_ID || id == MANUAL_SOURCE_ID {
            bail!(
                "provider id '{id}' is a reserved metadata source and cannot be registered by a plugin"
            );
        }
        if let Some(existing) = self.plugin_by_provider.get(&id) {
            bail!("provider '{id}' already registered by plugin '{existing}'");
        }
        self.providers.entry(plugin_id.clone()).or_default().insert(
            id.clone(),
            ProviderState {
                display_name,
                ..ProviderState::default()
            },
        );
        self.plugin_by_provider.insert(id, plugin_id);
        Ok(())
    }

    /// Declared display name, else the provider id.
    pub(crate) fn display_name(&self, provider_id: &str) -> String {
        self.state(provider_id)
            .and_then(|state| state.display_name.clone())
            .unwrap_or_else(|| provider_id.to_string())
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
    /// registering an id type must declare the same scheme, and a scheme
    /// belongs to the one provider that registers it, so no other provider's
    /// value can stand in for its own. `generator` runs only once the
    /// registration is accepted; the generator it replaces is returned.
    pub(crate) fn set_id_registration(
        &mut self,
        provider_id: &str,
        id_spec: ProviderIdSpec,
        generator: impl FnOnce() -> Option<ProviderIdUrlGenerator>,
    ) -> std::result::Result<Option<ProviderIdUrlGenerator>, String> {
        if let Some((owner, _)) = self.iter_states().find(|(other, state)| {
            other.as_str() != provider_id
                && state
                    .id_specs
                    .values()
                    .any(|existing| existing.scheme == id_spec.scheme)
        }) {
            return Err(format!(
                "scheme '{}' is registered by provider '{owner}'",
                id_spec.scheme
            ));
        }
        let Some(provider) = self.state_mut(provider_id) else {
            return Ok(None);
        };
        if let Some(conflict) = provider.id_specs.values().find(|existing| {
            existing.id_type == id_spec.id_type
                && existing.entity != id_spec.entity
                && existing.scheme != id_spec.scheme
        }) {
            return Err(format!(
                "id_type '{}' on provider '{provider_id}' is registered for {} with scheme '{}'; {} must use the same scheme",
                id_spec.id_type, conflict.entity, conflict.scheme, id_spec.entity
            ));
        }
        let key = (id_spec.entity, id_spec.id_type.clone());
        let replaced = match generator() {
            Some(generator) => provider.id_generators.insert(key.clone(), generator),
            None => provider.id_generators.remove(&key),
        };
        provider.id_specs.insert(key, id_spec);
        self.rebuild_id_snapshots();
        Ok(replaced)
    }

    pub(crate) fn set_refresh_callback(
        &mut self,
        provider_id: &str,
        entity_type: EntityType,
        handler: PluginCallbackHandle,
    ) {
        if let Some(provider) = self.state_mut(provider_id) {
            provider.refresh_callbacks.insert(entity_type, handler);
        }
    }

    pub(crate) fn get_refresh_callback(
        &self,
        provider_id: &str,
        entity_type: EntityType,
    ) -> Option<&PluginCallbackHandle> {
        self.state(provider_id)
            .and_then(|provider| provider.refresh_callbacks.get(&entity_type))
    }

    /// A read-only view of this registry with `plugin_id`'s bucket replaced by `staged`'s. It
    /// leaves the lyrics handlers out: those are cancelled when a registry holding them drops,
    /// and nothing reads them through a view.
    pub(crate) fn overlaid(&self, plugin_id: &PluginId, staged: &Self) -> Self {
        let copy_bucket = |bucket: &HashMap<String, ProviderState>| {
            bucket
                .iter()
                .map(|(id, state)| (id.clone(), state.without_lyrics()))
                .collect::<HashMap<_, _>>()
        };
        let mut providers = self
            .providers
            .iter()
            .filter(|(owner, _)| *owner != plugin_id)
            .map(|(owner, bucket)| (owner.clone(), copy_bucket(bucket)))
            .collect::<HashMap<_, _>>();
        if let Some(bucket) = staged.providers.get(plugin_id) {
            providers.insert(plugin_id.clone(), copy_bucket(bucket));
        }
        let mut view = Self::default();
        view.providers.extend(providers);
        view.rebuild_derived();
        view
    }

    pub(crate) fn provider_ids(&self, plugin_id: &PluginId) -> HashSet<String> {
        self.providers
            .get(plugin_id)
            .map(|bucket| bucket.keys().cloned().collect())
            .unwrap_or_default()
    }

    pub(crate) fn has_refresh_callback(&self, provider_id: &str, entity_type: EntityType) -> bool {
        self.get_refresh_callback(provider_id, entity_type)
            .is_some()
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
        filter: PluginCallbackHandle,
    ) {
        if let Some(provider) = self.state_mut(provider_id) {
            provider.sync_filter_callbacks.insert(entity_type, filter);
        }
    }

    pub(crate) fn set_search_callback(
        &mut self,
        provider_id: &str,
        entity_type: EntityType,
        handler: PluginCallbackHandle,
    ) {
        if let Some(provider) = self.state_mut(provider_id) {
            provider.search_callbacks.insert(entity_type, handler);
        }
    }

    pub(crate) fn get_search_callback(
        &self,
        provider_id: &str,
        entity_type: EntityType,
    ) -> Option<&PluginCallbackHandle> {
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

    pub(crate) fn has_cover_handler(&self, provider_id: &str, entity_type: EntityType) -> bool {
        self.get_cover_handler(provider_id, entity_type).is_some()
    }

    /// The plugin that owns the provider's cover handler for `entity_type`,
    /// while one is registered.
    pub(crate) fn cover_handler_owner(
        &self,
        provider_id: &str,
        entity_type: EntityType,
    ) -> Option<&PluginId> {
        self.get_cover_handler(provider_id, entity_type)?;
        self.plugin_by_provider.get(provider_id)
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

    pub(crate) fn has_similar_releases_handler(&self, provider_id: &str) -> bool {
        self.get_similar_releases_handler(provider_id).is_some()
    }

    /// The stored handler's cancel token, or `None` when the provider is not
    /// registered and nothing was stored.
    pub(crate) fn set_lyrics_handler(
        &mut self,
        provider_id: &str,
        spec: ProviderLyricsSpec,
    ) -> Option<CancellationToken> {
        let provider = self.state_mut(provider_id)?;
        let cancel = spec.cancel.clone();
        if let Some(replaced) = provider.lyrics_handler.replace(spec) {
            replaced.cancel.cancel();
        }
        Some(cancel)
    }

    pub(crate) fn get_lyrics_handler(&self, provider_id: &str) -> Option<&ProviderLyricsSpec> {
        self.state(provider_id)
            .and_then(|provider| provider.lyrics_handler.as_ref())
    }

    pub(crate) fn has_lyrics_handler(&self, provider_id: &str) -> bool {
        self.get_lyrics_handler(provider_id).is_some()
    }

    pub(crate) fn get_sync_filter_callback(
        &self,
        provider_id: &str,
        entity_type: EntityType,
    ) -> Option<&PluginCallbackHandle> {
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

    pub(crate) fn declared_ids(&self, entity: EntityType) -> DeclaredIds {
        DeclaredIds::new(self.unique_id_pairs(entity), self.id_pairs(entity))
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

    pub(crate) fn id_schemes(&self) -> Arc<IdSchemes> {
        self.id_schemes.clone()
    }

    pub(crate) fn id_link_generators(&self) -> Arc<IdLinkGenerators> {
        self.id_link_generators.clone()
    }

    fn rebuild_id_snapshots(&mut self) {
        let mut schemes = IdSchemes::default();
        let mut generators = IdLinkGenerators::default();
        for (provider_id, state) in self.iter_states() {
            for spec in state.id_specs.values() {
                schemes.insert(provider_id, &spec.id_type, &spec.scheme);
            }
            for ((entity, id_type), generator) in &state.id_generators {
                let spec = state.id_specs.get(&(*entity, id_type.clone()));
                generators.insert(
                    provider_id,
                    *entity,
                    id_type,
                    IdLinkGenerator {
                        scheme: spec.map(|spec| spec.scheme.clone()),
                        label: spec
                            .and_then(|spec| spec.label.clone())
                            .unwrap_or_else(|| self.display_name(provider_id)),
                        generator: generator.clone(),
                    },
                );
            }
        }
        self.id_schemes = Arc::new(schemes);
        generators.set_schemes(self.id_schemes.clone());
        self.id_link_generators = Arc::new(generators);
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

    pub(crate) async fn call_lock_count() -> usize {
        crate::services::STATE
            .generation()
            .providers
            .call_locks
            .lock()
            .await
            .len()
    }

    /// How many calls hold or are waiting for `provider_id`'s call lock.
    pub(crate) async fn calls_at_lock(provider_id: &str) -> usize {
        crate::services::STATE
            .generation()
            .providers
            .call_locks
            .lock()
            .await
            .get(provider_id)
            .map_or(0, |lock| Arc::strong_count(lock) - 1)
    }

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
            ProviderIdUrlGenerator::Function { .. } => None,
        }
    }

    #[test]
    fn a_handlers_failure_is_target_gone_anywhere_in_its_chain() {
        #[derive(Debug, thiserror::Error)]
        #[error("wrapped")]
        struct Wrapped(#[source] RefreshTargetGone);

        let gone = || RefreshTargetGone("gone".to_string());
        for error in [
            anyhow::Error::new(gone()),
            anyhow::Error::new(Wrapped(gone())),
            anyhow::Error::new(gone()).context("outer"),
        ] {
            assert!(matches!(
                ProviderCallError::of_handler(error),
                ProviderCallError::TargetGone(_)
            ));
        }
        assert!(matches!(
            ProviderCallError::of_handler(anyhow::anyhow!("other")),
            ProviderCallError::Failed(_)
        ));
    }

    #[test]
    fn register_rejects_the_reserved_source_ids() {
        let mut registry = ProviderRegistry::default();
        let plugin_id = PluginId::new("demo").expect("valid plugin id");

        for reserved in [LOCAL_SOURCE_ID, MANUAL_SOURCE_ID] {
            let err = registry
                .register(plugin_id.clone(), reserved.to_string(), None)
                .expect_err("reserved source id must be rejected");
            assert!(err.to_string().contains(reserved));
        }
    }

    #[test]
    fn register_applies_the_database_provider_id_rules() {
        let mut registry = ProviderRegistry::default();
        let plugin_id = PluginId::new("demo").expect("valid plugin id");

        for provider_id in ["lrclib", "provider-with-punctuation_1"] {
            registry
                .register(plugin_id.clone(), provider_id.to_string(), None)
                .unwrap_or_else(|err| panic!("expected {provider_id:?} to be accepted: {err}"));
        }
        for provider_id in ["", "two words", " leading", "trailing\t", "métadata"] {
            assert!(
                registry
                    .register(plugin_id.clone(), provider_id.to_string(), None)
                    .is_err(),
                "expected {provider_id:?} to be rejected"
            );
        }
    }

    fn id_spec(id_type: &str, entity: EntityType, scheme: &str) -> ProviderIdSpec {
        ProviderIdSpec {
            id_type: id_type.to_string(),
            entity,
            unique: entity == EntityType::Release,
            scheme: scheme.to_string(),
            label: None,
        }
    }

    fn template(url: &str) -> impl FnOnce() -> Option<ProviderIdUrlGenerator> {
        let url = url.to_string();
        move || Some(ProviderIdUrlGenerator::Template(url))
    }

    #[test]
    fn id_type_registered_for_several_entities_keeps_each_registration() {
        let mut registry = ProviderRegistry::default();
        let plugin_id = PluginId::new("demo").expect("valid plugin id");
        registry
            .register(plugin_id, "demo".to_string(), None)
            .expect("register provider");

        for (entity, url) in [
            (EntityType::Release, "https://example.test/release/{id}"),
            (EntityType::Artist, "https://example.test/artist/{id}"),
        ] {
            registry
                .set_id_registration(
                    "demo",
                    id_spec("item", entity, "example:item"),
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
    fn id_link_labels_prefer_registration_then_display_name_then_id() {
        let mut registry = ProviderRegistry::default();
        let plugin_id = PluginId::new("demo").expect("valid plugin id");
        registry
            .register(
                plugin_id.clone(),
                "named".to_string(),
                Some("Named Site".to_string()),
            )
            .expect("register named provider");
        registry
            .register(plugin_id, "plain".to_string(), None)
            .expect("register plain provider");
        let labelled = ProviderIdSpec {
            label: Some("Other Site".to_string()),
            ..id_spec("other_id", EntityType::Release, "other:thing")
        };
        for (provider_id, spec) in [
            ("named", labelled),
            (
                "named",
                id_spec("own_id", EntityType::Release, "named:thing"),
            ),
            (
                "plain",
                id_spec("own_id", EntityType::Release, "plain:thing"),
            ),
        ] {
            registry
                .set_id_registration(provider_id, spec, template("https://example.test/{id}"))
                .expect("registration");
        }

        let generators = registry.id_link_generators();
        let label = |provider_id, id_type| {
            generators
                .get(provider_id, EntityType::Release, id_type)
                .map(|generator| generator.label.clone())
        };
        assert_eq!(label("named", "other_id").as_deref(), Some("Other Site"));
        assert_eq!(label("named", "own_id").as_deref(), Some("Named Site"));
        assert_eq!(label("plain", "own_id").as_deref(), Some("plain"));
        assert_eq!(registry.display_name("named"), "Named Site");
        assert_eq!(registry.display_name("plain"), "plain");
        assert_eq!(registry.display_name("missing"), "missing");
    }

    #[test]
    fn id_snapshots_follow_registrations_and_teardown() {
        let mut registry = ProviderRegistry::default();
        let plugin_id = PluginId::new("demo").expect("valid plugin id");
        registry
            .register(plugin_id.clone(), "demo".to_string(), None)
            .expect("register provider");
        registry
            .set_id_registration(
                "demo",
                id_spec("item", EntityType::Release, "example:item"),
                template("https://example.test/{id}"),
            )
            .expect("registration");
        let registered = registry.id_link_generators();
        assert!(
            registered
                .get("demo", EntityType::Release, "item")
                .is_some()
        );
        let rows = [lyra_db::external_ids::ExternalId {
            db_id: None,
            provider_id: "demo".to_string(),
            id_type: "item".to_string(),
            id_value: "x".to_string(),
            source: lyra_db::IdSource::Resolved,
            resolved_value: None,
        }];
        assert!(
            registry
                .id_schemes()
                .resolve(&rows)
                .contains_key("example:item")
        );

        registry.replace_bucket(&plugin_id, Default::default());
        registry.rebuild_derived();

        assert!(registry.id_schemes().resolve(&rows).is_empty());
        assert!(
            registry
                .id_link_generators()
                .get("demo", EntityType::Release, "item")
                .is_none()
        );
        assert!(
            registered
                .get("demo", EntityType::Release, "item")
                .is_some(),
            "earlier snapshots are unaffected"
        );
    }

    #[test]
    fn id_type_rejects_a_different_scheme_on_another_entity() {
        let mut registry = ProviderRegistry::default();
        let plugin_id = PluginId::new("demo").expect("valid plugin id");
        registry
            .register(plugin_id, "demo".to_string(), None)
            .expect("register provider");
        registry
            .set_id_registration(
                "demo",
                id_spec("item", EntityType::Release, "example:item"),
                || None,
            )
            .expect("first registration");

        let err = registry
            .set_id_registration(
                "demo",
                id_spec("item", EntityType::Artist, "example:other"),
                || None,
            )
            .expect_err("conflicting scheme must be rejected");

        assert!(err.contains("same scheme"), "{err}");
        assert!(!registry.id_spec_matches_entity("demo", "item", EntityType::Artist));
    }

    #[test]
    fn a_scheme_belongs_to_the_provider_that_registered_it() {
        let mut registry = ProviderRegistry::default();
        for provider_id in ["first", "second"] {
            registry
                .register(
                    PluginId::new(provider_id).expect("valid plugin id"),
                    provider_id.to_string(),
                    None,
                )
                .expect("register provider");
        }
        registry
            .set_id_registration(
                "first",
                id_spec("item", EntityType::Release, "example:item"),
                || None,
            )
            .expect("first registration");
        registry
            .set_id_registration(
                "first",
                id_spec("item", EntityType::Artist, "example:item"),
                || None,
            )
            .expect("the owner registers it again");

        let err = registry
            .set_id_registration(
                "second",
                id_spec("item", EntityType::Release, "example:item"),
                || None,
            )
            .expect_err("another provider's scheme must be rejected");

        assert_eq!(
            err,
            "scheme 'example:item' is registered by provider 'first'"
        );
        assert!(!registry.id_spec_matches_entity("second", "item", EntityType::Release));
    }

    #[tokio::test]
    async fn provider_calls_are_serialized_per_provider() {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state().expect("test state");
        let provider_id = format!("test-provider-{}", lyra_db::nanoid());
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));

        let make_call = |active: Arc<AtomicUsize>, max_active: Arc<AtomicUsize>| {
            let provider_id = provider_id.clone();
            async move {
                with_provider_call(
                    &provider_id,
                    ProviderCallStage::MetadataRefresh,
                    TransitionWait::interactive(),
                    |_| Some(ProviderLookup::Call(())),
                    |(), _| async {
                        let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                        max_active.fetch_max(current, Ordering::SeqCst);
                        sleep(Duration::from_millis(5)).await;
                        active.fetch_sub(1, Ordering::SeqCst);
                        Ok(())
                    },
                )
                .await
                .expect("provider call");
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
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state().expect("test state");
        let provider_a = format!("test-provider-a-{}", lyra_db::nanoid());
        let provider_b = format!("test-provider-b-{}", lyra_db::nanoid());
        let barrier = Arc::new(Barrier::new(2));

        let make_call = |provider_id: String| {
            let barrier = barrier.clone();
            async move {
                with_provider_call(
                    &provider_id,
                    ProviderCallStage::MetadataRefresh,
                    TransitionWait::interactive(),
                    |_| Some(ProviderLookup::Call(())),
                    |(), _| async {
                        barrier.wait().await;
                        Ok(())
                    },
                )
                .await
                .expect("provider call");
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

    #[tokio::test]
    async fn a_torn_down_providers_running_call_still_holds_its_lock() {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state().expect("test state");
        let plugin_id = PluginId::new("demo").expect("valid plugin id");
        provider_registry()
            .write()
            .await
            .register(plugin_id.clone(), "demo".to_string(), None)
            .expect("register provider");
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Semaphore::new(0));

        let running = tokio::spawn({
            let (entered, release) = (entered.clone(), release.clone());
            async move {
                with_provider_call(
                    "demo",
                    ProviderCallStage::MetadataRefresh,
                    TransitionWait::interactive(),
                    |_| Some(ProviderLookup::Call(())),
                    |(), _| async {
                        entered.notify_one();
                        release.acquire().await.expect("gate").forget();
                        Ok(())
                    },
                )
                .await
                .expect("provider call");
            }
        });
        entered.notified().await;
        teardown_plugin_providers(&plugin_id).await;

        let next = || {
            with_provider_call(
                "demo",
                ProviderCallStage::MetadataRefresh,
                TransitionWait::interactive(),
                |_| Some(ProviderLookup::Call(())),
                |(), _| async { Ok(()) },
            )
        };
        assert!(
            timeout(Duration::from_millis(50), next()).await.is_err(),
            "a call after the teardown waits for the one still running"
        );
        release.add_permits(1);
        running.await.expect("running call");
        next().await.expect("provider call");
    }

    #[tokio::test]
    async fn a_call_without_a_handler_is_not_registered_and_runs_nothing() {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state().expect("test state");

        let result = with_provider_call(
            "unregistered",
            ProviderCallStage::MetadataRefresh,
            TransitionWait::interactive(),
            |_| None::<ProviderLookup<(), ()>>,
            |(), _| async { Ok(()) },
        )
        .await;

        assert!(matches!(result, Err(ProviderCallError::NotRegistered)));
    }

    #[tokio::test]
    async fn a_call_with_nothing_to_do_takes_no_lock() {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state().expect("test state");

        let missing = with_provider_call(
            "unregistered",
            ProviderCallStage::MetadataRefresh,
            TransitionWait::interactive(),
            |_| None::<ProviderLookup<(), ()>>,
            |(), _| async { Ok(()) },
        )
        .await;
        let answered = with_provider_call(
            "answered",
            ProviderCallStage::MetadataRefresh,
            TransitionWait::interactive(),
            |_| Some(ProviderLookup::<(), _>::Answer(7)),
            |(), _| async { Ok(0) },
        )
        .await
        .expect("answer");

        assert!(matches!(missing, Err(ProviderCallError::NotRegistered)));
        assert_eq!(answered, 7);
        assert_eq!(call_lock_count().await, 0);
    }

    async fn call_through_transition(
        wait: TransitionWait,
    ) -> (
        Arc<AtomicUsize>,
        tokio::task::JoinHandle<Result<(), ProviderCallError>>,
    ) {
        let dispatched = Arc::new(AtomicUsize::new(0));
        let running = tokio::spawn({
            let dispatched = dispatched.clone();
            async move {
                with_provider_call(
                    "waiting",
                    ProviderCallStage::MetadataRefresh,
                    wait,
                    |_| Some(ProviderLookup::Call(())),
                    |(), _| {
                        dispatched.fetch_add(1, Ordering::SeqCst);
                        async { Ok(()) }
                    },
                )
                .await
            }
        });
        (dispatched, running)
    }

    #[tokio::test(start_paused = true)]
    async fn a_pass_outlasts_any_transition_and_then_runs() {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state().expect("test state");
        let generation = crate::services::STATE.generation();
        let transition = generation.begin_transition(crate::services::state::Transition::Runtime);
        let (dispatched, running) =
            call_through_transition(TransitionWait::background(CancellationToken::new())).await;

        sleep(Duration::from_secs(4000)).await;
        assert_eq!(dispatched.load(Ordering::SeqCst), 0);
        assert!(!running.is_finished());

        drop(transition);
        running.await.expect("join").expect("the pass goes on");
        assert_eq!(dispatched.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_passed_deadline_reports_not_registered_without_dispatching() {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state().expect("test state");
        let generation = crate::services::STATE.generation();
        let _transition = generation.begin_transition(crate::services::state::Transition::Runtime);
        let (dispatched, running) = call_through_transition(TransitionWait::Until(
            tokio::time::Instant::now() + Duration::from_secs(1),
        ))
        .await;

        let result = running.await.expect("join");

        assert!(matches!(result, Err(ProviderCallError::NotRegistered)));
        assert_eq!(dispatched.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_cancelled_pass_stops_waiting_without_dispatching() {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state().expect("test state");
        let generation = crate::services::STATE.generation();
        let _transition = generation.begin_transition(crate::services::state::Transition::Runtime);
        let cancel = CancellationToken::new();
        let (dispatched, running) =
            call_through_transition(TransitionWait::background(cancel.clone())).await;
        sleep(Duration::from_secs(1)).await;

        cancel.cancel();

        let result = running.await.expect("join");
        assert!(matches!(result, Err(ProviderCallError::NotRegistered)));
        assert_eq!(dispatched.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn a_handler_that_never_started_is_retried_once() {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state().expect("test state");
        let calls = AtomicUsize::new(0);

        let result = with_provider_call(
            "not-started",
            ProviderCallStage::Lyrics,
            TransitionWait::interactive(),
            |_| Some(ProviderLookup::Call(())),
            |(), _| {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Err::<(), _>(anyhow::Error::new(DispatchNotStarted)) }
            },
        )
        .await;

        assert!(matches!(result, Err(ProviderCallError::NotRegistered)));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn a_handler_that_started_is_not_retried() {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state().expect("test state");
        let calls = AtomicUsize::new(0);

        let result = with_provider_call(
            "started",
            ProviderCallStage::Lyrics,
            TransitionWait::interactive(),
            |_| Some(ProviderLookup::Call(())),
            |(), _| {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Err::<(), _>(anyhow::anyhow!("the handler raised")) }
            },
        )
        .await;

        assert!(matches!(result, Err(ProviderCallError::Failed(_))));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_call_looks_its_handler_up_again_once_it_has_the_lock() {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state().expect("test state");
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let running = tokio::spawn({
            let (entered, release) = (entered.clone(), release.clone());
            async move {
                with_provider_call(
                    "demo",
                    ProviderCallStage::MetadataRefresh,
                    TransitionWait::interactive(),
                    |_| Some(ProviderLookup::Call(())),
                    |(), _| async {
                        entered.notify_one();
                        release.acquire().await.expect("gate").forget();
                        Ok(())
                    },
                )
                .await
            }
        });
        entered.notified().await;
        let looked_up = Arc::new(AtomicUsize::new(0));

        let waiting = with_provider_call(
            "demo",
            ProviderCallStage::MetadataRefresh,
            TransitionWait::interactive(),
            {
                let looked_up = looked_up.clone();
                move |_: &ProviderRegistry| {
                    looked_up.fetch_add(1, Ordering::SeqCst);
                    Some(ProviderLookup::Call(()))
                }
            },
            |(), _| async { Ok(()) },
        );
        let waiting = tokio::spawn(waiting);
        sleep(Duration::from_millis(20)).await;
        assert_eq!(
            looked_up.load(Ordering::SeqCst),
            1,
            "the read before the lock"
        );

        release.add_permits(1);
        running.await.expect("join").expect("running call");
        waiting.await.expect("join").expect("waiting call");
        assert_eq!(looked_up.load(Ordering::SeqCst), 2);
    }
}

impl PluginScopedInner for ProviderRegistry {
    type Bucket = HashMap<String, ProviderState>;

    fn take_bucket(&mut self, plugin_id: &PluginId) -> Self::Bucket {
        self.providers.remove(plugin_id).unwrap_or_default()
    }

    fn check_bucket(&self, plugin_id: &PluginId, new: &Self::Bucket) -> Result<()> {
        for (provider_id, state) in new {
            if let Some(existing) = self
                .plugin_by_provider
                .get(provider_id)
                .filter(|existing| *existing != plugin_id)
            {
                bail!("provider '{provider_id}' already registered by plugin '{existing}'");
            }
            for scheme in state.id_specs.values().map(|spec| &spec.scheme) {
                if let Some((owner, _)) = self
                    .providers
                    .iter()
                    .filter(|(other, _)| *other != plugin_id)
                    .flat_map(|(_, bucket)| bucket.iter())
                    .find(|(_, state)| state.id_specs.values().any(|spec| spec.scheme == *scheme))
                {
                    bail!("scheme '{scheme}' is registered by provider '{owner}'");
                }
            }
        }
        Ok(())
    }

    fn replace_bucket(&mut self, plugin_id: &PluginId, new: Self::Bucket) {
        let replaced = if new.is_empty() {
            self.providers.remove(plugin_id)
        } else {
            self.providers.insert(plugin_id.clone(), new)
        };
        if let Some(bucket) = replaced {
            bucket
                .values()
                .for_each(ProviderState::cancel_lyrics_handler);
        }
    }

    fn rebuild_derived(&mut self) {
        self.plugin_by_provider.clear();
        for (plugin_id, bucket) in &self.providers {
            for provider_id in bucket.keys() {
                self.plugin_by_provider
                    .insert(provider_id.clone(), plugin_id.clone());
            }
        }
        self.rebuild_id_snapshots();
    }
}

#[derive(Default)]
pub(crate) struct ProviderState {
    display_name: Option<String>,
    id_generators: HashMap<(EntityType, String), ProviderIdUrlGenerator>,
    id_specs: HashMap<(EntityType, String), ProviderIdSpec>,
    search_callbacks: HashMap<EntityType, PluginCallbackHandle>,
    refresh_callbacks: HashMap<EntityType, PluginCallbackHandle>,
    sync_filter_callbacks: HashMap<EntityType, PluginCallbackHandle>,
    cover_handlers: HashMap<EntityType, ProviderCoverSpec>,
    similar_releases_handler: Option<ProviderSimilarReleasesSpec>,
    lyrics_handler: Option<ProviderLyricsSpec>,
    options: Vec<OptionDeclaration>,
}

impl ProviderState {
    fn without_lyrics(&self) -> Self {
        let Self {
            display_name,
            id_generators,
            id_specs,
            search_callbacks,
            refresh_callbacks,
            sync_filter_callbacks,
            cover_handlers,
            similar_releases_handler,
            lyrics_handler: _,
            options,
        } = self;
        Self {
            display_name: display_name.clone(),
            id_generators: id_generators.clone(),
            id_specs: id_specs.clone(),
            search_callbacks: search_callbacks.clone(),
            refresh_callbacks: refresh_callbacks.clone(),
            sync_filter_callbacks: sync_filter_callbacks.clone(),
            cover_handlers: cover_handlers.clone(),
            similar_releases_handler: similar_releases_handler.clone(),
            lyrics_handler: None,
            options: options.clone(),
        }
    }

    fn cancel_lyrics_handler(&self) {
        if let Some(spec) = &self.lyrics_handler {
            spec.cancel.cancel();
        }
    }
}

impl Drop for ProviderRegistry {
    fn drop(&mut self) {
        self.iter_states()
            .for_each(|(_, state)| state.cancel_lyrics_handler());
    }
}

/// Empties the plugin's bucket in the published scope.
#[cfg(test)]
pub(crate) async fn teardown_plugin_providers(plugin_id: &PluginId) {
    let scope = crate::services::STATE
        .generation()
        .latest()
        .expect("published test scope");
    scope.providers().teardown_plugin(plugin_id).await;
}
