// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    any::Any,
    borrow::Borrow,
    collections::HashSet,
    fmt,
    sync::Arc,
};

use anyhow::{
    Result,
    bail,
};
use tokio::sync::{
    OwnedRwLockWriteGuard,
    RwLock,
};
use tokio_util::sync::CancellationToken;

use crate::services::{
    mix::{
        MixRegistries,
        MixRegistry,
    },
    plugin_dispatch::PluginDispatch,
    providers::{
        ProviderRegistry,
        ProviderRuntimeState,
    },
};

/// The registries a plugin runtime's plugins register into. They are built before the runtime's
/// VM, which reaches them through its own data, so a late task of a replaced runtime can only
/// write into the scope dying with it.
#[derive(Clone, Default)]
pub(crate) struct ScopeRegistries {
    pub(crate) providers: ProviderRuntimeState,
    pub(crate) mix: MixRegistries,
    /// Fires once when the scope goes live.
    pub(crate) published: CancellationToken,
}

impl ScopeRegistries {
    /// Empties the plugin's buckets. A new field fails to compile here until it is addressed.
    pub(crate) async fn teardown_plugin(&self, plugin_id: &PluginId) {
        let Self {
            providers,
            mix,
            published: _,
        } = self;
        providers.teardown_plugin(plugin_id).await;
        mix.teardown_plugin(plugin_id).await;
    }

    /// Locks every registry for moving `staged`'s buckets of the plugin here, failing on a
    /// conflict with another plugin.
    pub(crate) async fn prepare_commit(
        &self,
        staged: &Self,
        plugin_id: &PluginId,
    ) -> Result<ScopeCommit> {
        let Self {
            providers,
            mix,
            published: _,
        } = self;
        Ok(ScopeCommit {
            providers: BucketCommit::prepare(
                &providers.registry(),
                &staged.providers.registry(),
                plugin_id,
            )
            .await?,
            mix: BucketCommit::prepare(&mix.registry(), &staged.mix.registry(), plugin_id).await?,
        })
    }

    /// The provider and mixer ids the plugin registered here.
    pub(crate) async fn plugin_targets(&self, plugin_id: &PluginId) -> HashSet<String> {
        let mut targets = self
            .providers
            .registry()
            .read()
            .await
            .provider_ids(plugin_id);
        targets.extend(self.mix.registry().read().await.mixer_ids(plugin_id));
        targets
    }
}

/// The services half of moving a restarted plugin's buckets into the live scope.
pub(crate) struct ScopeCommit {
    providers: BucketCommit<ProviderRegistry>,
    mix: BucketCommit<MixRegistry>,
}

impl ScopeCommit {
    /// The provider and mixer ids being published.
    pub(crate) fn targets(&self) -> HashSet<String> {
        let Self { providers, mix } = self;
        let providers = providers
            .bucket()
            .into_iter()
            .flat_map(|bucket| bucket.keys());
        let mixers = mix.bucket().into_iter().flat_map(|bucket| bucket.keys());
        providers.chain(mixers).cloned().collect()
    }

    pub(crate) fn apply(&mut self) {
        let Self { providers, mix } = self;
        providers.apply();
        mix.apply();
    }
}

/// One plugin runtime as services use it: the dispatcher and the registries holding the handles
/// it answers, published together. A call holds the `Arc` it read its handler from, so the handle
/// and the dispatcher always belong to each other.
pub(crate) struct RuntimeScope {
    dispatch: Arc<dyn PluginDispatch>,
    registries: ScopeRegistries,
    /// What the layer above keeps in the scope. A test of services alone publishes none.
    plugin_layer: Option<Arc<dyn Any + Send + Sync>>,
}

impl RuntimeScope {
    pub(crate) fn new(
        dispatch: Arc<dyn PluginDispatch>,
        registries: ScopeRegistries,
        plugin_layer: Arc<dyn Any + Send + Sync>,
    ) -> Self {
        Self {
            dispatch,
            registries,
            plugin_layer: Some(plugin_layer),
        }
    }

    /// A scope for a test of services, with no layer above it.
    #[cfg(test)]
    pub(crate) fn without_plugin_layer(
        dispatch: Arc<dyn PluginDispatch>,
        registries: ScopeRegistries,
    ) -> Self {
        Self {
            dispatch,
            registries,
            plugin_layer: None,
        }
    }

    /// This scope's layer above, with `dispatch` and fresh registries of its own.
    #[cfg(test)]
    pub(crate) fn republished(&self, dispatch: Arc<dyn PluginDispatch>) -> Self {
        Self {
            dispatch,
            registries: ScopeRegistries::default(),
            plugin_layer: self.plugin_layer.clone(),
        }
    }

    pub(crate) fn dispatch(&self) -> &Arc<dyn PluginDispatch> {
        &self.dispatch
    }

    pub(crate) fn providers(&self) -> &ProviderRuntimeState {
        &self.registries.providers
    }

    pub(crate) fn mix(&self) -> &MixRegistries {
        &self.registries.mix
    }

    /// The state the layer above keeps in the scope, which services never name.
    pub(crate) fn plugin_layer(&self) -> Option<&Arc<dyn Any + Send + Sync>> {
        self.plugin_layer.as_ref()
    }
}

/// Validated plugin identifier. Outer key for every plugin-scoped registry.
#[derive(Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct PluginId(Arc<str>);

impl PluginId {
    pub(crate) fn new(value: impl Into<Arc<str>>) -> Result<Self> {
        let value = value.into();
        if value.is_empty() {
            bail!("plugin id must not be empty");
        }
        if value.chars().any(char::is_whitespace) {
            bail!("plugin id must not contain whitespace");
        }
        Ok(Self(value))
    }

    /// An id the plugin runtime checked when it loaded the plugin, as a chunk's origin holds.
    pub(crate) fn validated(value: Arc<str>) -> Self {
        Self(value)
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl Borrow<str> for PluginId {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for PluginId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.0, f)
    }
}

impl fmt::Display for PluginId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl AsRef<str> for PluginId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// A registry of per-plugin buckets, whose buckets are replaced whole: a restart moves a staged
/// bucket in, and teardown replaces a plugin's with an empty one.
pub(crate) trait PluginScopedInner: Send + Sync + 'static {
    type Bucket: Default + Send + Sync;

    fn take_bucket(&mut self, plugin_id: &PluginId) -> Self::Bucket;

    /// What only the live registry can tell: whether `new` conflicts with another plugin's bucket.
    fn check_bucket(&self, plugin_id: &PluginId, new: &Self::Bucket) -> Result<()>;

    /// Infallible, so a plugin can't be left half replaced.
    fn replace_bucket(&mut self, plugin_id: &PluginId, new: Self::Bucket);

    /// Rebuild derived side-car state from the surviving buckets.
    fn rebuild_derived(&mut self);
}

/// Pairs `replace_bucket` with `rebuild_derived` so impls cannot skip the rebuild.
pub(crate) struct ScopedRegistry<T: PluginScopedInner> {
    inner: Arc<RwLock<T>>,
}

impl<T: PluginScopedInner> ScopedRegistry<T> {
    pub(crate) fn from_shared(inner: Arc<RwLock<T>>) -> Self {
        Self { inner }
    }

    pub(crate) async fn teardown(&self, plugin_id: &PluginId) {
        let mut guard = self.inner.write().await;
        guard.replace_bucket(plugin_id, T::Bucket::default());
        guard.rebuild_derived();
    }
}

impl<T: PluginScopedInner> Clone for ScopedRegistry<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

/// One registry's half of moving a staged bucket into the live registry. Holds both write locks
/// from the check through the replacement. A commit abandoned before `apply` puts its bucket back,
/// so the staged registry ends what the bucket started when it is dropped.
pub(crate) struct BucketCommit<T: PluginScopedInner> {
    live: OwnedRwLockWriteGuard<T>,
    staged: OwnedRwLockWriteGuard<T>,
    plugin_id: PluginId,
    bucket: Option<T::Bucket>,
}

impl<T: PluginScopedInner> BucketCommit<T> {
    pub(crate) async fn prepare(
        live: &Arc<RwLock<T>>,
        staged: &Arc<RwLock<T>>,
        plugin_id: &PluginId,
    ) -> Result<Self> {
        let mut staged = staged.clone().write_owned().await;
        let live = live.clone().write_owned().await;
        let bucket = staged.take_bucket(plugin_id);
        let commit = Self {
            live,
            staged,
            plugin_id: plugin_id.clone(),
            bucket: Some(bucket),
        };
        if let Some(bucket) = &commit.bucket {
            commit.live.check_bucket(plugin_id, bucket)?;
        }
        Ok(commit)
    }

    /// What is about to replace the plugin's bucket.
    fn bucket(&self) -> Option<&T::Bucket> {
        self.bucket.as_ref()
    }

    /// Keeps both locks: they are released when every registry of the commit has been applied.
    pub(crate) fn apply(&mut self) {
        if let Some(bucket) = self.bucket.take() {
            self.live.replace_bucket(&self.plugin_id, bucket);
            self.live.rebuild_derived();
        }
    }
}

impl<T: PluginScopedInner> Drop for BucketCommit<T> {
    fn drop(&mut self) {
        if let Some(bucket) = self.bucket.take() {
            self.staged.replace_bucket(&self.plugin_id, bucket);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl<T: PluginScopedInner> ScopedRegistry<T> {
        fn new(inner: T) -> Self {
            Self {
                inner: Arc::new(RwLock::new(inner)),
            }
        }

        fn shared(&self) -> Arc<RwLock<T>> {
            self.inner.clone()
        }
    }

    #[test]
    fn plugin_id_rejects_empty() {
        assert!(PluginId::new("").is_err());
    }

    #[test]
    fn plugin_id_rejects_whitespace() {
        assert!(PluginId::new("   ").is_err());
        assert!(PluginId::new("  demo  ").is_err());
        assert!(PluginId::new("de mo").is_err());
    }

    struct TestInner {
        replaced: Vec<String>,
        rebuilt: usize,
    }

    impl PluginScopedInner for TestInner {
        type Bucket = ();

        fn take_bucket(&mut self, _plugin_id: &PluginId) {}

        fn check_bucket(&self, _plugin_id: &PluginId, _new: &()) -> Result<()> {
            Ok(())
        }

        fn replace_bucket(&mut self, plugin_id: &PluginId, _new: ()) {
            self.replaced.push(plugin_id.to_string());
        }

        fn rebuild_derived(&mut self) {
            self.rebuilt += 1;
        }
    }

    #[tokio::test]
    async fn a_commit_holds_its_locks_past_apply() {
        let plugin_id = PluginId::new("demo").unwrap();
        let live = Arc::new(RwLock::new(TestInner {
            replaced: Vec::new(),
            rebuilt: 0,
        }));
        let staged = Arc::new(RwLock::new(TestInner {
            replaced: Vec::new(),
            rebuilt: 0,
        }));

        let mut commit = BucketCommit::prepare(&live, &staged, &plugin_id)
            .await
            .unwrap();
        commit.apply();

        assert!(live.try_read().is_err(), "applied, but not released");
        drop(commit);
        assert_eq!(live.read().await.replaced, vec!["demo".to_string()]);
    }

    #[tokio::test]
    async fn scoped_registry_always_rebuilds_after_replacing() {
        let registry = ScopedRegistry::new(TestInner {
            replaced: Vec::new(),
            rebuilt: 0,
        });
        let id = PluginId::new("demo").unwrap();

        registry.teardown(&id).await;

        let guard = registry.shared();
        let inner = guard.read().await;
        assert_eq!(inner.replaced, vec!["demo".to_string()]);
        assert_eq!(inner.rebuilt, 1);
    }
}
