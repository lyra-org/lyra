// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    borrow::Borrow,
    collections::{
        HashMap,
        HashSet,
    },
    fmt,
    sync::{
        Arc,
        atomic::{
            AtomicUsize,
            Ordering,
        },
    },
};

use anyhow::{
    Result,
    bail,
};
use mlua::{
    Function,
    IntoLuaMulti,
    Lua,
};
use tokio::sync::{
    Mutex,
    Notify,
    RwLock,
    RwLockReadGuard,
};

/// Resolve the plugin whose Lua source is on the current call stack.
///
/// MUST be called at a sync Lua-callback boundary — inside mlua's
/// scheduler-driven async path the coroutine is suspended, so the stack
/// walk sees no plugin frame.
pub(crate) fn resolve_caller_plugin_id(lua: &Lua) -> Option<PluginId> {
    let mut level = 1usize;
    while let Some(function) = lua.inspect_stack(level, |debug| debug.function()) {
        if let Ok(raw) = crate::plugins::id_from_function(&function)
            && let Ok(plugin_id) = PluginId::new(raw)
        {
            return Some(plugin_id);
        }
        level += 1;
    }
    None
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

/// Per-plugin in-flight counter paired with a `Notify` so a drain gate
/// can await counter-reaches-zero without spin-polling.
pub(crate) struct PluginInflight {
    count: AtomicUsize,
    notify: Notify,
}

impl PluginInflight {
    fn new() -> Self {
        Self {
            count: AtomicUsize::new(0),
            notify: Notify::new(),
        }
    }
}

/// Private inner `Function` so dispatch cannot bypass the in-flight counter.
pub(crate) struct PluginFunctionHandle {
    plugin_id: PluginId,
    inflight: Arc<PluginInflight>,
    func: Function,
}

impl PluginFunctionHandle {
    pub(crate) fn new(plugin_id: PluginId, inflight: Arc<PluginInflight>, func: Function) -> Self {
        inflight.count.fetch_add(1, Ordering::Release);
        Self {
            plugin_id,
            inflight,
            func,
        }
    }

    pub(crate) fn plugin_id(&self) -> &PluginId {
        &self.plugin_id
    }

    pub(crate) fn try_upgrade_lua(&self) -> Option<mlua::Lua> {
        self.func.weak_lua().try_upgrade()
    }

    /// Borrow the underlying Lua function. Callers that need to pass it
    /// to mlua APIs that consume a `Function` (e.g. `create_thread`) can
    /// clone this, but must keep the owning `PluginFunctionHandle` alive
    /// for the duration so the in-flight counter stays incremented.
    pub(crate) fn inner_function(&self) -> &Function {
        &self.func
    }

    pub(crate) async fn call_async<A, R>(&self, args: A) -> mlua::Result<R>
    where
        A: IntoLuaMulti,
        R: mlua::FromLuaMulti,
    {
        use harmony_core::LuaFunctionAsyncExt;
        self.func.call_async(args).await
    }
}

impl Clone for PluginFunctionHandle {
    fn clone(&self) -> Self {
        self.inflight.count.fetch_add(1, Ordering::Release);
        Self {
            plugin_id: self.plugin_id.clone(),
            inflight: self.inflight.clone(),
            func: self.func.clone(),
        }
    }
}

impl Drop for PluginFunctionHandle {
    fn drop(&mut self) {
        // Single-op check: `fetch_sub` returns the PRE-decrement value, so
        // `== 1` means we're about to hit zero. AcqRel pairs with drain's
        // Acquire load below. `notify_waiters` inside the if is critical:
        // a separate load-then-notify introduces a window where another
        // thread increments between them and the waiter never wakes.
        if self.inflight.count.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.inflight.notify.notify_waiters();
        }
    }
}

impl fmt::Debug for PluginFunctionHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PluginFunctionHandle")
            .field("plugin_id", &self.plugin_id)
            .field("inflight", &self.inflight.count.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

/// Infallible: all validation must happen at register time so teardown
/// cannot leave a half-live plugin.
pub(crate) trait PluginScopedInner: Send + Sync + 'static {
    /// Missing ids are a no-op.
    fn clear_bucket(&mut self, plugin_id: &PluginId);

    /// Rebuild derived side-car state from the surviving buckets.
    fn rebuild_derived(&mut self);
}

/// Pairs `clear_bucket` with `rebuild_derived` so impls cannot skip the rebuild.
pub(crate) struct ScopedRegistry<T: PluginScopedInner> {
    inner: Arc<RwLock<T>>,
}

impl<T: PluginScopedInner> ScopedRegistry<T> {
    #[cfg(test)]
    pub(crate) fn new(inner: T) -> Self {
        Self {
            inner: Arc::new(RwLock::new(inner)),
        }
    }

    pub(crate) fn from_shared(inner: Arc<RwLock<T>>) -> Self {
        Self { inner }
    }

    #[cfg(test)]
    pub(crate) fn shared(&self) -> Arc<RwLock<T>> {
        self.inner.clone()
    }

    pub(crate) async fn teardown(&self, plugin_id: &PluginId) {
        let mut guard = self.inner.write().await;
        guard.clear_bucket(plugin_id);
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

pub(crate) struct PluginRegistrationGuard<'a> {
    _guard: RwLockReadGuard<'a, HashSet<PluginId>>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum PluginRestartError {
    #[error("plugin not found: {0}")]
    NotFound(PluginId),
    #[error("failed to restart plugin '{plugin_id}': {source:#}")]
    Failed {
        plugin_id: PluginId,
        #[source]
        source: anyhow::Error,
    },
}

/// Aggregate of plugin-scoped registries. `counters` maps `PluginId` to
/// a shared `Arc<PluginInflight>` — a single atomic + notify per plugin
/// that every `PluginFunctionHandle` clone counts into, so a drain gate
/// can await all of a plugin's in-flight dispatches from one point.
///
/// `teardown_lock` serializes `teardown_plugin`. The teardown path
/// clears registry buckets, snapshots for rebuild, and hot-swaps the
/// live Axum router — none of those are guarded by a single lock, so
/// two concurrent teardowns could interleave and leave a stale router
/// installed last, resurrecting dead routes for one plugin and
/// hanging drain for the other. A global mutex here keeps teardown
/// serial, which matches the expected restart cadence (one plugin at
/// a time from `POST /restart`).
#[derive(Clone)]
pub(crate) struct PluginRegistries {
    counters: Arc<RwLock<HashMap<PluginId, Arc<PluginInflight>>>>,
    restart_lock: Arc<Mutex<()>>,
    teardown_lock: Arc<Mutex<()>>,
    /// Plugins currently being torn down. Registration paths reject
    /// new adds under these ids so an in-flight handler can't insert
    /// a `PluginFunctionHandle` into a registry bucket that teardown
    /// has already cleared — that would pin the counter above zero
    /// and hang `drain_plugin` forever.
    teardown_in_progress: Arc<RwLock<HashSet<PluginId>>>,
}

impl Default for PluginRegistries {
    fn default() -> Self {
        Self::new()
    }
}

impl PluginRegistries {
    pub(crate) fn new() -> Self {
        Self {
            counters: Arc::new(RwLock::new(HashMap::new())),
            restart_lock: Arc::new(Mutex::new(())),
            teardown_lock: Arc::new(Mutex::new(())),
            teardown_in_progress: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    /// Reject new registrations for a plugin that's tearing down.
    /// Callers must invoke this at every plugin-identified
    /// registration entry point — `PluginFunctionHandle::new` call
    /// sites, declare_option / declare_settings, and the
    /// Provider/Mixer `new` constructors — before touching any
    /// plugin-scoped registry.
    pub(crate) async fn ensure_registrations_open(
        &self,
        plugin_id: &PluginId,
    ) -> mlua::Result<PluginRegistrationGuard<'_>> {
        let guard = self.teardown_in_progress.read().await;
        if guard.contains(plugin_id) {
            return Err(mlua::Error::runtime(format!(
                "plugin '{plugin_id}' is tearing down; new registrations rejected"
            )));
        }
        Ok(PluginRegistrationGuard { _guard: guard })
    }

    /// Returns the plugin's shared counter; repeat calls return clones of the same Arc.
    pub(crate) async fn inflight_counter(&self, plugin_id: &PluginId) -> Arc<PluginInflight> {
        {
            let counters = self.counters.read().await;
            if let Some(counter) = counters.get(plugin_id) {
                return counter.clone();
            }
        }
        let mut counters = self.counters.write().await;
        counters
            .entry(plugin_id.clone())
            .or_insert_with(|| Arc::new(PluginInflight::new()))
            .clone()
    }

    /// Await zero in-flight dispatches for `plugin_id`. Returns immediately
    /// if no counter has been created for the plugin (no handlers were
    /// ever registered) or the current count is already zero.
    ///
    /// `Notified::enable()` is load-bearing: a `Notified` future registers
    /// its waker on first poll, NOT on creation. Without `enable()`, the
    /// canonical register-before-check order degrades into a check-then-
    /// await race — the last `Drop` can fire `notify_waiters` between the
    /// counter load and the `.await`, with zero registered waiters, and
    /// drain hangs forever.
    ///
    /// Caller's responsibility: every registry that owns
    /// `PluginFunctionHandle` clones for this plugin must already have
    /// been cleared before `drain_plugin` runs. Registry-held clones pin
    /// the counter above the in-flight count, so drain can't reach zero
    /// until those clones drop. Ingress must also be stopped so new
    /// dispatches stop incrementing.
    pub(crate) async fn drain_plugin(&self, plugin_id: &PluginId) {
        let Some(counter) = ({
            let counters = self.counters.read().await;
            counters.get(plugin_id).cloned()
        }) else {
            return;
        };

        loop {
            let notified = counter.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if counter.count.load(Ordering::Acquire) == 0 {
                return;
            }
            notified.await;
        }
    }

    /// Clear every plugin-scoped registry bucket for `plugin_id`, hot-swap
    /// derived dispatch state, then wait for all old handles to drop.
    ///
    /// Set `reopen_registrations` for restart preparation: after old
    /// registrations are gone and drained, the plugin receives temporary
    /// route/settings exemptions so its init chunk can re-declare those
    /// surfaces while the rest of the process remains frozen. The caller
    /// that re-executes the plugin must refreeze those exemptions after
    /// registration completes.
    pub(crate) async fn teardown_plugin(&self, plugin_id: &PluginId, reopen_registrations: bool) {
        let _teardown = self.teardown_lock.lock().await;

        {
            let mut teardown_in_progress = self.teardown_in_progress.write().await;
            teardown_in_progress.insert(plugin_id.clone());
        }

        crate::plugins::api::refreeze_plugin_routes(plugin_id).await;
        crate::plugins::runtime::refreeze_plugin_settings(plugin_id).await;

        crate::plugins::api::teardown_plugin_routes(plugin_id).await;
        crate::services::providers::teardown_plugin_providers(plugin_id).await;
        crate::services::mix::teardown_plugin_mixers(plugin_id).await;
        crate::services::playback_sessions::teardown_plugin_callbacks(plugin_id).await;
        crate::plugins::runtime::teardown_plugin_settings(plugin_id).await;

        self.drain_plugin(plugin_id).await;
        self.counters.write().await.remove(plugin_id);

        if reopen_registrations {
            crate::plugins::api::unfreeze_plugin_routes(plugin_id.clone()).await;
            crate::plugins::runtime::unfreeze_plugin_settings(plugin_id.clone()).await;
        }

        self.teardown_in_progress.write().await.remove(plugin_id);
    }

    pub(crate) async fn restart_plugin(
        &self,
        plugin_id: &PluginId,
        harmony: Arc<harmony_core::Harmony>,
    ) -> std::result::Result<(), PluginRestartError> {
        let _restart = self.restart_lock.lock().await;

        if !harmony.has_plugin(plugin_id.as_str()) {
            return Err(PluginRestartError::NotFound(plugin_id.clone()));
        }

        if let Err(err) = harmony.reload_plugin_manifest(plugin_id.as_str()) {
            return Err(PluginRestartError::Failed {
                plugin_id: plugin_id.clone(),
                source: err.context("failed to reload plugin manifest"),
            });
        }
        crate::STATE
            .plugin_manifests
            .replace(Arc::from(harmony.plugin_manifests()));

        self.teardown_plugin(plugin_id, true).await;

        match harmony.exec_plugin(plugin_id.as_str()).await {
            Ok(()) => {
                refreeze_plugin_registration_exemptions(plugin_id).await;
                if let Err(err) = crate::plugins::api::rebuild_registered_routes().await {
                    self.teardown_plugin(plugin_id, false).await;
                    return Err(PluginRestartError::Failed {
                        plugin_id: plugin_id.clone(),
                        source: err.context("failed to rebuild plugin API routes"),
                    });
                }
                Ok(())
            }
            Err(err) => {
                refreeze_plugin_registration_exemptions(plugin_id).await;
                self.teardown_plugin(plugin_id, false).await;
                Err(PluginRestartError::Failed {
                    plugin_id: plugin_id.clone(),
                    source: err.context("plugin execution failed"),
                })
            }
        }
    }
}

async fn refreeze_plugin_registration_exemptions(plugin_id: &PluginId) {
    crate::plugins::api::refreeze_plugin_routes(plugin_id).await;
    crate::plugins::runtime::refreeze_plugin_settings(plugin_id).await;
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn plugin_function_handle_counts_clones_and_drops() {
        let lua = mlua::Lua::new();
        let func: Function = lua.load("return function() end").eval().unwrap();
        let plugin_id = PluginId::new("demo").unwrap();
        let inflight = Arc::new(PluginInflight::new());

        let handle = PluginFunctionHandle::new(plugin_id, inflight.clone(), func);
        assert_eq!(inflight.count.load(Ordering::Acquire), 1);

        let cloned = handle.clone();
        assert_eq!(inflight.count.load(Ordering::Acquire), 2);

        drop(cloned);
        assert_eq!(inflight.count.load(Ordering::Acquire), 1);

        drop(handle);
        assert_eq!(inflight.count.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn drain_plugin_wakes_when_last_handle_drops() {
        let lua = mlua::Lua::new();
        let func: Function = lua.load("return function() end").eval().unwrap();
        let plugin_id = PluginId::new("demo").unwrap();
        let registries = PluginRegistries::new();
        let inflight = registries.inflight_counter(&plugin_id).await;

        let handle = PluginFunctionHandle::new(plugin_id.clone(), inflight.clone(), func);

        let drain_registries = registries.clone();
        let drain_id = plugin_id.clone();
        let drain = tokio::spawn(async move {
            drain_registries.drain_plugin(&drain_id).await;
        });

        tokio::task::yield_now().await;
        assert!(!drain.is_finished());

        drop(handle);

        drain.await.unwrap();
        assert_eq!(inflight.count.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn drain_plugin_returns_immediately_when_no_counter() {
        let registries = PluginRegistries::new();
        let plugin_id = PluginId::new("demo").unwrap();
        registries.drain_plugin(&plugin_id).await;
    }

    #[tokio::test]
    async fn ensure_registrations_open_rejects_during_teardown() {
        let registries = PluginRegistries::new();
        let plugin_id = PluginId::new("demo").unwrap();

        assert!(
            registries
                .ensure_registrations_open(&plugin_id)
                .await
                .is_ok()
        );

        registries
            .teardown_in_progress
            .write()
            .await
            .insert(plugin_id.clone());

        assert!(
            registries
                .ensure_registrations_open(&plugin_id)
                .await
                .is_err()
        );

        let other = PluginId::new("other").unwrap();
        assert!(registries.ensure_registrations_open(&other).await.is_ok());

        registries
            .teardown_in_progress
            .write()
            .await
            .remove(&plugin_id);
        assert!(
            registries
                .ensure_registrations_open(&plugin_id)
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn registration_guard_blocks_teardown_marker_until_dropped() {
        let registries = PluginRegistries::new();
        let plugin_id = PluginId::new("demo").unwrap();
        let registration = registries
            .ensure_registrations_open(&plugin_id)
            .await
            .unwrap();

        let teardown_registries = registries.clone();
        let teardown_id = plugin_id.clone();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let mut mark_teardown = tokio::spawn(async move {
            started_tx.send(()).unwrap();
            teardown_registries
                .teardown_in_progress
                .write()
                .await
                .insert(teardown_id);
        });

        started_rx.await.unwrap();
        tokio::task::yield_now().await;
        assert!(!mark_teardown.is_finished());

        drop(registration);
        (&mut mark_teardown).await.unwrap();

        assert!(
            registries
                .teardown_in_progress
                .read()
                .await
                .contains(&plugin_id)
        );
    }

    struct TestInner {
        cleared: Vec<String>,
        rebuilt: usize,
    }

    impl PluginScopedInner for TestInner {
        fn clear_bucket(&mut self, plugin_id: &PluginId) {
            self.cleared.push(plugin_id.to_string());
        }

        fn rebuild_derived(&mut self) {
            self.rebuilt += 1;
        }
    }

    #[tokio::test]
    async fn scoped_registry_always_rebuilds_after_clear() {
        let registry = ScopedRegistry::new(TestInner {
            cleared: Vec::new(),
            rebuilt: 0,
        });
        let id = PluginId::new("demo").unwrap();

        registry.teardown(&id).await;

        let guard = registry.shared();
        let inner = guard.read().await;
        assert_eq!(inner.cleared, vec!["demo".to_string()]);
        assert_eq!(inner.rebuilt, 1);
    }
}
