// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Lyrics provider dispatcher. Handlers MUST resolve cooldown / known-miss
//! / known-instrumental without awaiting HTTP — they run sequentially.

use std::{
    collections::{
        HashMap,
        HashSet,
    },
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        LazyLock,
        Mutex,
    },
    time::Duration,
};

use agdb::DbId;
use anyhow::Result;
use futures::stream::{
    self,
    StreamExt,
};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

use crate::{
    STATE,
    db::{
        self,
    },
    plugins::lifecycle::PluginId,
    services::metadata::lyrics::{
        scorer::{
            self,
            LocalTrackContext,
            LyricsHandlerCandidate,
            ScoreInput,
        },
        selection,
        upload,
    },
};

pub(crate) const DEFAULT_HANDLER_TIMEOUT: Duration = Duration::from_secs(15);
pub(crate) const MAX_CONCURRENT_DISPATCHES: usize = 4;

#[derive(Clone, Debug)]
pub(crate) struct LyricsTrackContext {
    pub(crate) track_db_id: i64,
    pub(crate) track_name: String,
    pub(crate) artist_name: String,
    pub(crate) album_name: Option<String>,
    pub(crate) duration_ms: Option<u64>,
    pub(crate) external_ids: HashMap<String, HashMap<String, String>>,
    /// Skip Miss/Instrumental fast path; cooldown still applies.
    pub(crate) force_refresh: bool,
}

/// Closed kind set. Anything else surfaces as a Lua error.
#[derive(Clone, Debug)]
pub(crate) enum LyricsHandlerResult {
    /// Up to 5 candidates; the dispatcher caps on read.
    Hit {
        candidates: Vec<LyricsHandlerCandidate>,
    },
    Miss,
    Instrumental,
    RateLimited { retry_after_ms: Option<u64> },
}

/// Paths reference the [`LyricsTrackContext`] tree.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct LyricsRequireSpec {
    pub all_of: Vec<String>,
    pub any_of: Vec<String>,
}

pub(crate) type HandlerFuture = Pin<Box<dyn Future<Output = Result<LyricsHandlerResult>> + Send>>;
pub(crate) type HandlerFn =
    Arc<dyn Fn(LyricsTrackContext) -> HandlerFuture + Send + Sync + 'static>;

#[derive(Clone)]
pub(crate) struct RegisteredHandler {
    pub(crate) provider_id: Arc<str>,
    pub(crate) plugin_id: PluginId,
    pub(crate) priority: i32,
    pub(crate) timeout: Duration,
    pub(crate) require: LyricsRequireSpec,
    pub(crate) handler: HandlerFn,
    pub(crate) cancel: CancellationToken,
}

#[derive(Default)]
pub(crate) struct LyricsProviderRegistry {
    handlers: HashMap<String, RegisteredHandler>,
    plugin_cancels: HashMap<PluginId, CancellationToken>,
}

impl LyricsProviderRegistry {
    fn ensure_plugin_token(&mut self, plugin_id: &PluginId) -> CancellationToken {
        self.plugin_cancels
            .entry(plugin_id.clone())
            .or_insert_with(CancellationToken::new)
            .clone()
    }

    pub(crate) fn insert(&mut self, handler: RegisteredHandler) {
        self.handlers
            .insert(handler.provider_id.as_ref().to_string(), handler);
    }

    pub(crate) fn remove_for_plugin(&mut self, plugin_id: &PluginId) {
        if let Some(parent) = self.plugin_cancels.remove(plugin_id) {
            parent.cancel();
        }
        self.handlers
            .retain(|_, handler| &handler.plugin_id != plugin_id);
    }

    pub(crate) fn list_sorted(&self) -> Vec<RegisteredHandler> {
        let mut all: Vec<RegisteredHandler> = self.handlers.values().cloned().collect();
        // Higher priority first, provider_id ascending breaks ties.
        all.sort_by(|a, b| {
            b.priority
                .cmp(&a.priority)
                .then_with(|| a.provider_id.cmp(&b.provider_id))
        });
        all
    }

    #[cfg(test)]
    pub(crate) fn clear(&mut self) {
        for token in self.plugin_cancels.values() {
            token.cancel();
        }
        self.plugin_cancels.clear();
        self.handlers.clear();
    }
}

pub(crate) static LYRICS_PROVIDER_REGISTRY: LazyLock<RwLock<LyricsProviderRegistry>> =
    LazyLock::new(|| RwLock::new(LyricsProviderRegistry::default()));

static IN_FLIGHT: LazyLock<Mutex<HashSet<(i64, Arc<str>)>>> =
    LazyLock::new(|| Mutex::new(HashSet::new()));

struct InFlightGuard {
    key: (i64, Arc<str>),
}

impl InFlightGuard {
    fn try_acquire(track_db_id: DbId, provider_id: &Arc<str>) -> Option<Self> {
        let mut set = IN_FLIGHT.lock().expect("in-flight mutex poisoned");
        let key = (track_db_id.0, provider_id.clone());
        if set.contains(&key) {
            return None;
        }
        set.insert(key.clone());
        Some(Self { key })
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        if let Ok(mut set) = IN_FLIGHT.lock() {
            set.remove(&self.key);
        }
    }
}

/// Spawns a startup rescan sharing the handler's cancel token.
pub(crate) async fn register_handler(handler: RegisteredHandler) {
    let cancel = handler.cancel.clone();
    {
        let mut registry = LYRICS_PROVIDER_REGISTRY.write().await;
        registry.insert(handler);
    }

    tokio::spawn(async move {
        if let Err(err) = rescan(false, cancel).await {
            tracing::warn!(error = %err, "lyrics startup rescan failed");
        }
    });
}

/// Multiple registrations from one plugin tear down together.
pub(crate) async fn make_plugin_cancellation_child(plugin_id: &PluginId) -> CancellationToken {
    let mut registry = LYRICS_PROVIDER_REGISTRY.write().await;
    let parent = registry.ensure_plugin_token(plugin_id);
    parent.child_token()
}

pub(crate) async fn unregister_handlers_for_plugin(plugin_id: &PluginId) {
    let mut registry = LYRICS_PROVIDER_REGISTRY.write().await;
    registry.remove_for_plugin(plugin_id);
}

#[cfg(test)]
pub(crate) async fn reset_registry_for_test() {
    let mut registry = LYRICS_PROVIDER_REGISTRY.write().await;
    registry.clear();
    drop(registry);
    if let Ok(mut set) = IN_FLIGHT.lock() {
        set.clear();
    }
}

/// Run every matching handler in priority order. Not stop-on-first-hit —
/// each gate-passing `Hit` writes a row; `selection.rs` picks the winner.
pub(crate) async fn dispatch_for_track(track_db_id: DbId, force_refresh: bool) -> Result<()> {
    let now = upload::now_ms().map_err(|err| anyhow::anyhow!("now_ms() failed: {err}"))?;
    let (mut context, preferred_languages, local) = match build_track_context(track_db_id, now)
        .await?
    {
        Some(parts) => parts,
        None => return Ok(()),
    };
    context.force_refresh = force_refresh;

    let registry_snapshot = {
        let registry = LYRICS_PROVIDER_REGISTRY.read().await;
        registry.list_sorted()
    };

    for handler in registry_snapshot {
        if !require_matches(&context, &handler.require) {
            continue;
        }
        if handler.cancel.is_cancelled() {
            continue;
        }

        let guard = match InFlightGuard::try_acquire(track_db_id, &handler.provider_id) {
            Some(guard) => guard,
            None => {
                tracing::debug!(
                    provider = handler.provider_id.as_ref(),
                    track_db_id = track_db_id.0,
                    "lyrics dispatch deduped: another caller has this (track, provider) in flight"
                );
                continue;
            }
        };

        let cancel = handler.cancel.clone();
        let dispatch_fut = dispatch_one(
            &handler,
            &context,
            &preferred_languages,
            &local,
            track_db_id,
        );
        tokio::select! {
            _ = cancel.cancelled() => {
                tracing::debug!(
                    provider = handler.provider_id.as_ref(),
                    track_db_id = track_db_id.0,
                    "lyrics dispatch cancelled before completion"
                );
            }
            res = dispatch_fut => {
                if let Err(err) = res {
                    tracing::warn!(
                        provider = handler.provider_id.as_ref(),
                        track_db_id = track_db_id.0,
                        error = %err,
                        "lyrics dispatch failed for provider"
                    );
                }
            }
        }
        drop(guard);
    }

    Ok(())
}

async fn dispatch_one(
    handler: &RegisteredHandler,
    context: &LyricsTrackContext,
    preferred_languages: &[String],
    local: &LocalTrackContext,
    track_db_id: DbId,
) -> Result<()> {
    let provider_id = handler.provider_id.as_ref();

    let timeout = if handler.timeout.is_zero() {
        DEFAULT_HANDLER_TIMEOUT
    } else {
        handler.timeout
    };
    let context_owned = context.clone();
    let handler_fn = handler.handler.clone();
    let call_result = tokio::time::timeout(timeout, handler_fn(context_owned)).await;

    let outcome = match call_result {
        Ok(Ok(outcome)) => outcome,
        Ok(Err(err)) => {
            tracing::warn!(
                provider = provider_id,
                track_db_id = track_db_id.0,
                error = %err,
                "lyrics handler returned error"
            );
            return Ok(());
        }
        Err(_elapsed) => {
            tracing::warn!(
                provider = provider_id,
                track_db_id = track_db_id.0,
                timeout_ms = timeout.as_millis() as u64,
                "lyrics handler timed out"
            );
            return Ok(());
        }
    };

    match outcome {
        LyricsHandlerResult::Hit { candidates } => {
            // Defense in depth — contract is ≤5.
            let mut capped = candidates;
            if capped.len() > 5 {
                capped.truncate(5);
            }
            let picked_idx = pick_index(local, &capped, preferred_languages);
            match picked_idx {
                Some(idx) => {
                    let chosen = capped.into_iter().nth(idx).expect("idx in range");
                    record_hit(track_db_id, provider_id, chosen).await?;
                }
                None => {
                    tracing::debug!(
                        provider = provider_id,
                        track_db_id = track_db_id.0,
                        "lyrics handler hit failed scorer gates; dropped"
                    );
                }
            }
        }
        LyricsHandlerResult::Miss => {
            tracing::trace!(
                provider = provider_id,
                track_db_id = track_db_id.0,
                "lyrics handler miss"
            );
        }
        LyricsHandlerResult::Instrumental => {
            tracing::trace!(
                provider = provider_id,
                track_db_id = track_db_id.0,
                "lyrics handler reports instrumental"
            );
        }
        LyricsHandlerResult::RateLimited { retry_after_ms } => {
            tracing::debug!(
                provider = provider_id,
                track_db_id = track_db_id.0,
                retry_after_ms = retry_after_ms.unwrap_or(0),
                "lyrics dispatch deferred: rate_limited"
            );
        }
    }

    Ok(())
}

fn pick_index(
    local: &LocalTrackContext,
    candidates: &[LyricsHandlerCandidate],
    preferred_languages: &[String],
) -> Option<usize> {
    let chosen = scorer::pick_best(ScoreInput {
        local_track: local,
        candidates,
        preferred_languages,
    })?;
    // Pointer identity avoids needing Eq/Hash on the candidate.
    candidates.iter().position(|c| std::ptr::eq(c, chosen))
}

async fn record_hit(
    track_db_id: DbId,
    provider_id: &str,
    candidate: LyricsHandlerCandidate,
) -> Result<()> {
    let LyricsHandlerCandidate { lyrics, .. } = candidate;
    let now = upload::now_ms().map_err(|err| anyhow::anyhow!("now_ms() failed: {err}"))?;
    let lyrics_input = match lyrics.into_lyrics_input(now) {
        Ok(input) => input,
        Err(err) => {
            tracing::warn!(
                provider = provider_id,
                track_db_id = track_db_id.0,
                error = %err,
                "lyrics handler hit had malformed PluginLyricsInput; dropped"
            );
            return Ok(());
        }
    };

    let mut db = STATE.db.write().await;
    upload::upsert_plugin_lyrics(&mut *db, track_db_id, lyrics_input, provider_id.to_string())
        .map_err(|err| anyhow::anyhow!("upsert_plugin_lyrics failed: {err}"))?;
    Ok(())
}

fn require_matches(context: &LyricsTrackContext, require: &LyricsRequireSpec) -> bool {
    if !require
        .all_of
        .iter()
        .all(|path| context_path_present(context, path))
    {
        return false;
    }
    if !require.any_of.is_empty()
        && !require
            .any_of
            .iter()
            .any(|path| context_path_present(context, path))
    {
        return false;
    }
    true
}

fn context_path_present(context: &LyricsTrackContext, path: &str) -> bool {
    let mut parts = path.split('.');
    let head = match parts.next() {
        Some(h) => h,
        None => return false,
    };
    match head {
        "track_name" => !context.track_name.trim().is_empty() && parts.next().is_none(),
        "artist_name" => !context.artist_name.trim().is_empty() && parts.next().is_none(),
        "album_name" => {
            context
                .album_name
                .as_deref()
                .is_some_and(|s| !s.trim().is_empty())
                && parts.next().is_none()
        }
        "duration_ms" => context.duration_ms.is_some() && parts.next().is_none(),
        "external_ids" => match (parts.next(), parts.next()) {
            (None, _) => !context.external_ids.is_empty(),
            (Some(provider), None) => context
                .external_ids
                .get(provider)
                .is_some_and(|m| !m.is_empty()),
            (Some(provider), Some(id_type)) => context
                .external_ids
                .get(provider)
                .and_then(|m| m.get(id_type))
                .is_some_and(|v| !v.trim().is_empty()),
        },
        _ => false,
    }
}

async fn build_track_context(
    track_db_id: DbId,
    _now_ms: u64,
) -> Result<Option<(LyricsTrackContext, Vec<String>, LocalTrackContext)>> {
    let db = STATE.db.read().await;
    let Some(track) = db::tracks::get_by_id(&*db, track_db_id)? else {
        return Ok(None);
    };

    let artists = db::artists::get(&*db, track_db_id)?;
    let primary_artist = artists
        .iter()
        .map(|a| a.artist_name.trim())
        .find(|n| !n.is_empty())
        .unwrap_or("")
        .to_string();

    let release = db::releases::get_by_track(&*db, track_db_id)?
        .into_iter()
        .next();
    let album_name = release.as_ref().map(|r| r.release_title.clone());

    let mut external_ids: HashMap<String, HashMap<String, String>> = HashMap::new();
    for ext in db::providers::external_ids::get_for_entity(&*db, track_db_id)? {
        external_ids
            .entry(ext.provider_id)
            .or_default()
            .insert(ext.id_type, ext.id_value);
    }

    // Library language stands in for a per-track language preference.
    let preferred_languages: Vec<String> = db::libraries::get_for_entity(&*db, track_db_id)
        .ok()
        .into_iter()
        .flatten()
        .filter_map(|lib| lib.language.clone())
        .filter(|s| !s.trim().is_empty())
        .collect();

    drop(db);

    let context = LyricsTrackContext {
        track_db_id: track_db_id.0,
        track_name: track.track_title.clone(),
        artist_name: primary_artist.clone(),
        album_name,
        duration_ms: track.duration_ms,
        external_ids,
        force_refresh: false,
    };
    let local = LocalTrackContext {
        track_title: track.track_title,
        artist_name: primary_artist,
        duration_ms: track.duration_ms,
    };
    Ok(Some((context, preferred_languages, local)))
}

/// `force_refresh = true` includes tracks that already have preferred lyrics.
pub(crate) async fn rescan(force_refresh: bool, cancel: CancellationToken) -> Result<()> {
    let track_ids: Vec<DbId> = {
        let db = STATE.db.read().await;
        let tracks = db::tracks::get(&*db, "tracks")?;
        let mut filtered = Vec::with_capacity(tracks.len());
        for track in tracks {
            let Some(track_db_id) = track.db_id.clone().map(DbId::from) else {
                continue;
            };
            if !force_refresh
                && selection::get_preferred_detail(&*db, track_db_id, None, false)?.is_some()
            {
                continue;
            }
            filtered.push(track_db_id);
        }
        filtered
    };

    if track_ids.is_empty() {
        return Ok(());
    }

    let cancel_for_stream = cancel.clone();
    stream::iter(track_ids)
        .for_each_concurrent(MAX_CONCURRENT_DISPATCHES, |track_db_id| {
            let cancel = cancel_for_stream.clone();
            async move {
                if cancel.is_cancelled() {
                    return;
                }
                tokio::select! {
                    _ = cancel.cancelled() => {}
                    res = dispatch_for_track(track_db_id, force_refresh) => {
                        if let Err(err) = res {
                            tracing::warn!(
                                track_db_id = track_db_id.0,
                                error = %err,
                                "lyrics rescan dispatch failed"
                            );
                        }
                    }
                }
            }
        })
        .await;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::test_db;
    use crate::plugins::lyrics::{
        PluginLyricLineInput,
        PluginLyricsInput,
    };
    use std::sync::atomic::{
        AtomicUsize,
        Ordering,
    };

    fn lyrics_payload(text: &str) -> PluginLyricsInput {
        PluginLyricsInput {
            id: "test-payload".to_string(),
            language: "eng".to_string(),
            plain_text: text.to_string(),
            lines: vec![PluginLyricLineInput {
                ts_ms: 1_000,
                text: text.to_string(),
                words: Vec::new(),
            }],
        }
    }

    fn test_plugin_id() -> PluginId {
        PluginId::new("test_plugin").expect("valid plugin id")
    }

    fn match_candidate() -> LyricsHandlerCandidate {
        LyricsHandlerCandidate {
            lyrics: lyrics_payload("la la la"),
            title: "A Long Song Title Here".to_string(),
            artist: "An Artist Group Name".to_string(),
            duration_ms: Some(180_000),
            language: Some("eng".to_string()),
        }
    }

    async fn install_track_in_state_db(title: &str, artist: &str, duration_ms: u64) -> DbId {
        let mut db = STATE.db.write().await;
        let track_id = test_db::insert_track(&mut *db, title).expect("insert track");
        let mut track = db::tracks::get_by_id(&*db, track_id)
            .expect("get_by_id")
            .expect("track");
        track.duration_ms = Some(duration_ms);
        db.exec_mut(agdb::QueryBuilder::insert().element(&track).query())
            .expect("update track");
        let artist_id = test_db::insert_artist(&mut *db, artist).expect("insert artist");
        test_db::connect(&mut *db, track_id, artist_id).expect("connect");
        track_id
    }

    async fn initialize_test_runtime() -> anyhow::Result<()> {
        crate::testing::initialize_runtime(&crate::testing::LibraryFixtureConfig {
            directory: std::path::PathBuf::from("."),
            language: None,
            country: None,
        })
        .await?;
        reset_registry_for_test().await;
        Ok(())
    }

    fn make_handler(
        provider_id: &str,
        plugin_id: PluginId,
        cancel: CancellationToken,
        handler_fn: HandlerFn,
    ) -> RegisteredHandler {
        RegisteredHandler {
            provider_id: Arc::from(provider_id),
            plugin_id,
            priority: 1,
            timeout: Duration::from_secs(5),
            require: LyricsRequireSpec::default(),
            handler: handler_fn,
            cancel,
        }
    }

    /// Skips `register_handler`'s startup rescan to keep tests deterministic.
    async fn install_handler_for_test(handler: RegisteredHandler) {
        let mut registry = LYRICS_PROVIDER_REGISTRY.write().await;
        registry.insert(handler);
    }

    #[tokio::test]
    async fn hit_path_writes_lyrics_row_with_no_lock_held_during_handler() -> anyhow::Result<()> {
        let _guard = crate::testing::runtime_test_lock().await;
        initialize_test_runtime().await?;
        let track_id =
            install_track_in_state_db("A Long Song Title Here", "An Artist Group Name", 180_000)
                .await;

        let observed = Arc::new(AtomicUsize::new(0));
        let observed_clone = observed.clone();
        let handler: HandlerFn = Arc::new(move |_ctx| {
            let observed = observed_clone.clone();
            Box::pin(async move {
                // try_write succeeds only when nobody holds the write lock.
                let attempted = STATE.db.get();
                if attempted.try_write().is_ok() {
                    observed.fetch_add(1, Ordering::SeqCst);
                }
                Ok(LyricsHandlerResult::Hit {
                    candidates: vec![match_candidate()],
                })
            })
        });
        let cancel = make_plugin_cancellation_child(&test_plugin_id()).await;
        install_handler_for_test(make_handler(
            "test_hit",
            test_plugin_id(),
            cancel,
            handler,
        ))
        .await;

        dispatch_for_track(track_id, false).await?;

        assert_eq!(
            observed.load(Ordering::SeqCst),
            1,
            "handler must run with no write lock held"
        );

        let db = STATE.db.read().await;
        let lyrics = db::lyrics::get_for_track(&*db, track_id)?;
        assert!(
            lyrics.iter().any(|l| l.provider_id == "test_hit"),
            "lyrics row from test_hit must exist"
        );
        Ok(())
    }

    #[tokio::test]
    async fn force_refresh_threads_through_to_handler() -> anyhow::Result<()> {
        let _guard = crate::testing::runtime_test_lock().await;
        initialize_test_runtime().await?;
        let track_id =
            install_track_in_state_db("Title For Force", "Artist For Force", 200_000).await;

        let saw_force = Arc::new(AtomicUsize::new(0));
        let saw_force_clone = saw_force.clone();
        let handler: HandlerFn = Arc::new(move |ctx| {
            let saw_force = saw_force_clone.clone();
            Box::pin(async move {
                if ctx.force_refresh {
                    saw_force.fetch_add(1, Ordering::SeqCst);
                }
                Ok(LyricsHandlerResult::Miss)
            })
        });
        let cancel = make_plugin_cancellation_child(&test_plugin_id()).await;
        install_handler_for_test(make_handler(
            "test_force",
            test_plugin_id(),
            cancel,
            handler,
        ))
        .await;

        dispatch_for_track(track_id, true).await?;
        assert_eq!(
            saw_force.load(Ordering::SeqCst),
            1,
            "handler must observe force_refresh = true"
        );
        Ok(())
    }

    #[tokio::test]
    async fn in_flight_dedupe_skips_concurrent_call_for_same_key() -> anyhow::Result<()> {
        let _guard = crate::testing::runtime_test_lock().await;
        initialize_test_runtime().await?;
        let track_id =
            install_track_in_state_db("Title For Dedupe", "Artist For Dedupe", 200_000).await;

        let calls = Arc::new(AtomicUsize::new(0));
        let calls_clone = calls.clone();
        let release = Arc::new(tokio::sync::Notify::new());
        let release_for_handler = release.clone();
        let handler: HandlerFn = Arc::new(move |_ctx| {
            let calls = calls_clone.clone();
            let release = release_for_handler.clone();
            Box::pin(async move {
                calls.fetch_add(1, Ordering::SeqCst);
                release.notified().await;
                Ok(LyricsHandlerResult::Miss)
            })
        });
        let cancel = make_plugin_cancellation_child(&test_plugin_id()).await;
        install_handler_for_test(make_handler(
            "test_dedupe",
            test_plugin_id(),
            cancel,
            handler,
        ))
        .await;

        let first = tokio::spawn(async move { dispatch_for_track(track_id, false).await });
        // Wait until the first call has claimed the in-flight slot.
        for _ in 0..50 {
            if calls.load(Ordering::SeqCst) == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        dispatch_for_track(track_id, false).await?;
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "second concurrent dispatch must dedupe (handler called exactly once)"
        );

        release.notify_waiters();
        first.await.expect("join first")?;
        Ok(())
    }

    #[tokio::test]
    async fn cancellation_aborts_parked_dispatches() -> anyhow::Result<()> {
        let _guard = crate::testing::runtime_test_lock().await;
        initialize_test_runtime().await?;

        let mut track_ids = Vec::with_capacity(100);
        for i in 0..100 {
            let track_id = install_track_in_state_db(
                &format!("Cancel Title {i}"),
                &format!("Cancel Artist {i}"),
                180_000,
            )
            .await;
            track_ids.push(track_id);
        }

        let started = Arc::new(AtomicUsize::new(0));
        let started_clone = started.clone();
        let plugin = test_plugin_id();
        let cancel = make_plugin_cancellation_child(&plugin).await;
        let cancel_for_handler = cancel.clone();
        let handler: HandlerFn = Arc::new(move |_ctx| {
            let started = started_clone.clone();
            let cancel = cancel_for_handler.clone();
            Box::pin(async move {
                started.fetch_add(1, Ordering::SeqCst);
                // Park until cancel — keeps the handler from burning through
                // before the test can observe how many actually started.
                tokio::select! {
                    _ = cancel.cancelled() => {}
                    _ = tokio::time::sleep(Duration::from_secs(60)) => {}
                }
                Ok(LyricsHandlerResult::Miss)
            })
        });
        install_handler_for_test(make_handler(
            "test_cancel",
            plugin.clone(),
            cancel.clone(),
            handler,
        ))
        .await;

        let rescan_cancel = cancel.clone();
        let handle = tokio::spawn(async move { rescan(false, rescan_cancel).await });
        // Wait for the bound's worth of handlers to start.
        for _ in 0..200 {
            if started.load(Ordering::SeqCst) >= MAX_CONCURRENT_DISPATCHES {
                break;
            }
            tokio::task::yield_now().await;
        }
        let started_at_cancel = started.load(Ordering::SeqCst);
        unregister_handlers_for_plugin(&plugin).await;

        handle.await.expect("rescan join")?;
        let final_started = started.load(Ordering::SeqCst);
        assert!(
            final_started <= started_at_cancel + MAX_CONCURRENT_DISPATCHES,
            "after unregister at most N more handler calls may execute (started_at_cancel={started_at_cancel}, final={final_started}, N={MAX_CONCURRENT_DISPATCHES})"
        );
        Ok(())
    }
}
