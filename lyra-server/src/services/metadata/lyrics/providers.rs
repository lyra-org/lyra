// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Lyrics provider dispatcher. Handlers MUST resolve cooldown / known-miss
//! / known-instrumental without awaiting HTTP — they run sequentially.

use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use agdb::DbId;
use anyhow::{
    Result,
    bail,
};
use futures::stream::{
    self,
    StreamExt,
};
use tokio_util::sync::CancellationToken;

use crate::{
    services::STATE,
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
    services::providers::{
        IdentifiersByScheme,
        LyricsInFlight,
        ProviderCallStage,
        ProviderLyricsSpec,
        ProviderRequireSpec,
        active_providers,
        id_schemes,
        lyrics_in_flight,
        provider_registry,
        requirements_match_with,
        with_provider_call,
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
    pub(crate) identifiers: IdentifiersByScheme,
    /// Skip Miss/Instrumental fast path; cooldown still applies.
    pub(crate) force_refresh: bool,
}

pub(crate) fn track_context_to_json(context: &LyricsTrackContext) -> serde_json::Value {
    serde_json::json!({
        "track_db_id": context.track_db_id,
        "track_name": context.track_name,
        "artist_name": context.artist_name,
        "album_name": context.album_name,
        "duration_ms": context.duration_ms,
        "external_ids": context.external_ids,
        "identifiers": context.identifiers,
        "force_refresh": context.force_refresh,
    })
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
    RateLimited {
        retry_after_ms: Option<u64>,
    },
}

pub(crate) fn parse_handler_result(value: serde_json::Value) -> Result<LyricsHandlerResult> {
    let serde_json::Value::Object(mut object) = value else {
        bail!("provider:lyrics handler must return a table/object");
    };
    let kind = object
        .remove("kind")
        .and_then(|value| value.as_str().map(str::to_string))
        .ok_or_else(|| {
            anyhow::anyhow!("provider:lyrics handler return is missing string 'kind'")
        })?;

    match kind.as_str() {
        "hit" => {
            let candidates = object
                .remove("candidates")
                .ok_or_else(|| anyhow::anyhow!("provider:lyrics 'hit' requires candidates"))?;
            let candidates: Vec<LyricsHandlerCandidate> = serde_json::from_value(candidates)
                .map_err(|err| {
                    anyhow::anyhow!(
                        "provider:lyrics 'hit' candidates did not match LyricsHandlerCandidate: {err}"
                    )
                })?;
            if candidates.is_empty() {
                bail!("provider:lyrics 'hit' must include at least one candidate");
            }
            if candidates.len() > 5 {
                bail!("provider:lyrics 'hit' may include at most 5 candidates");
            }
            Ok(LyricsHandlerResult::Hit { candidates })
        }
        "miss" => Ok(LyricsHandlerResult::Miss),
        "instrumental" => Ok(LyricsHandlerResult::Instrumental),
        "rate_limited" => {
            let retry_after_ms = match object.remove("retry_after_ms") {
                None | Some(serde_json::Value::Null) => None,
                Some(serde_json::Value::Number(number)) => number.as_u64(),
                Some(other) => bail!(
                    "provider:lyrics 'rate_limited' retry_after_ms must be a non-negative integer when set, got {}",
                    other
                ),
            };
            Ok(LyricsHandlerResult::RateLimited { retry_after_ms })
        }
        other => bail!(
            "provider:lyrics handler 'kind' must be one of 'hit', 'miss', 'instrumental', 'rate_limited' (got '{other}')"
        ),
    }
}

pub(crate) type HandlerFuture = Pin<Box<dyn Future<Output = Result<LyricsHandlerResult>> + Send>>;
pub(crate) type HandlerFn =
    Arc<dyn Fn(LyricsTrackContext) -> HandlerFuture + Send + Sync + 'static>;

pub(crate) type InFlightKey = (i64, Arc<str>);

struct InFlightGuard {
    in_flight: LyricsInFlight,
    key: InFlightKey,
}

impl InFlightGuard {
    fn try_acquire(track_db_id: DbId, provider_id: &Arc<str>) -> Option<Self> {
        let in_flight = lyrics_in_flight();
        let key = (track_db_id.0, provider_id.clone());
        if !in_flight
            .lock()
            .expect("in-flight mutex poisoned")
            .insert(key.clone())
        {
            return None;
        }
        Some(Self { in_flight, key })
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        if let Ok(mut set) = self.in_flight.lock() {
            set.remove(&self.key);
        }
    }
}

/// Spawns a startup rescan sharing the handler's cancel token.
pub(crate) async fn register_handler(provider_id: &str, spec: ProviderLyricsSpec) {
    let Some(cancel) = provider_registry()
        .write()
        .await
        .set_lyrics_handler(provider_id, spec)
    else {
        return;
    };

    tokio::spawn(async move {
        // Rescan dispatch closures resolve STATE.generation().plugin_dispatch at call time;
        // plugin init can register handlers before the runtime is published.
        if !wait_for_plugin_dispatch(&cancel).await {
            return;
        }
        if let Err(err) = rescan(false, cancel).await {
            tracing::warn!(error = %err, "lyrics startup rescan failed");
        }
    });
}

async fn wait_for_plugin_dispatch(cancel: &CancellationToken) -> bool {
    loop {
        if crate::services::STATE
            .generation()
            .plugin_dispatch
            .get()
            .is_some()
        {
            return true;
        }
        tokio::select! {
            _ = cancel.cancelled() => return false,
            _ = tokio::time::sleep(std::time::Duration::from_millis(50)) => {}
        }
    }
}

/// Run every matching handler of an active provider in provider order. Not
/// stop-on-first-hit — each gate-passing `Hit` writes a row; `selection.rs`
/// picks the winner.
pub(crate) async fn dispatch_for_track(track_db_id: DbId, force_refresh: bool) -> Result<()> {
    dispatch_for_track_identity(track_db_id, None, force_refresh).await
}

pub(crate) async fn dispatch_for_track_identity(
    track_db_id: DbId,
    expected_public_id: Option<&str>,
    force_refresh: bool,
) -> Result<()> {
    let now = upload::now_ms().map_err(|err| anyhow::anyhow!("now_ms() failed: {err}"))?;
    let (mut context, preferred_languages, local, track_public_id) =
        match build_track_context(track_db_id, now).await? {
            Some(parts) => parts,
            None => return Ok(()),
        };
    context.force_refresh = force_refresh;
    if expected_public_id.is_some_and(|expected| expected != track_public_id) {
        return Ok(());
    }

    let providers = active_providers(&*STATE.db.read().await)?;
    let registry_snapshot: Vec<(Arc<str>, ProviderLyricsSpec)> = {
        let registry = provider_registry().read_owned().await;
        providers
            .iter()
            .filter_map(|provider| {
                let spec = registry.get_lyrics_handler(&provider.provider_id)?;
                Some((Arc::from(provider.provider_id.as_str()), spec.clone()))
            })
            .collect()
    };

    let mut failures = Vec::new();
    for (provider_id, handler) in registry_snapshot {
        if !require_matches(&context, &handler.require) {
            continue;
        }
        if handler.cancel.is_cancelled() {
            continue;
        }

        let guard = match InFlightGuard::try_acquire(track_db_id, &provider_id) {
            Some(guard) => guard,
            None => {
                tracing::debug!(
                    provider = provider_id.as_ref(),
                    track_db_id = track_db_id.0,
                    "lyrics dispatch deduped: another caller has this (track, provider) in flight"
                );
                continue;
            }
        };

        let cancel = handler.cancel.clone();
        let dispatch_fut = dispatch_one(
            &provider_id,
            &handler,
            &context,
            &preferred_languages,
            &local,
            track_db_id,
            &track_public_id,
        );
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                tracing::debug!(
                    provider = provider_id.as_ref(),
                    track_db_id = track_db_id.0,
                    "lyrics dispatch cancelled before completion"
                );
            }
            res = dispatch_fut => {
                if let Err(err) = res {
                    failures.push(format!("{provider_id}: {err:#}"));
                }
            }
        }
        drop(guard);
    }

    if !failures.is_empty() {
        bail!("lyrics dispatch failed: {}", failures.join("; "));
    }
    Ok(())
}

async fn dispatch_one(
    provider_id: &str,
    handler: &ProviderLyricsSpec,
    context: &LyricsTrackContext,
    preferred_languages: &[String],
    local: &LocalTrackContext,
    track_db_id: DbId,
    track_public_id: &str,
) -> Result<()> {
    let timeout = if handler.timeout.is_zero() {
        DEFAULT_HANDLER_TIMEOUT
    } else {
        handler.timeout
    };
    let context_owned = context.clone();
    let handler_fn = handler.handler.clone();
    let call_result: Result<Option<LyricsHandlerResult>> =
        with_provider_call(provider_id, ProviderCallStage::Lyrics, || async {
            {
                let db = STATE.db.read().await;
                if lyra_db::tracks::get_by_id(&*db, track_db_id)?
                    .is_none_or(|track| track.id != track_public_id)
                {
                    return Ok(None);
                }
            }
            tokio::time::timeout(timeout, handler_fn(context_owned))
                .await
                .map_err(|_| {
                    anyhow::anyhow!("lyrics handler timed out after {} ms", timeout.as_millis())
                })?
                .map(Some)
        })
        .await;

    let Some(outcome) = call_result? else {
        return Ok(());
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
                    record_hit(track_db_id, track_public_id, provider_id, chosen).await?;
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
    track_public_id: &str,
    provider_id: &str,
    candidate: LyricsHandlerCandidate,
) -> Result<()> {
    let LyricsHandlerCandidate { lyrics, .. } = candidate;
    let now = upload::now_ms().map_err(|err| anyhow::anyhow!("now_ms() failed: {err}"))?;
    let (lyrics_id, lyrics_input) = match lyrics.into_lyrics_input(now) {
        Ok(parts) => parts,
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
    let current_track = lyra_db::tracks::get_by_id(&*db, track_db_id)?;
    if current_track
        .as_ref()
        .is_none_or(|track| track.id != track_public_id)
    {
        tracing::debug!(
            provider = provider_id,
            track_db_id = track_db_id.0,
            track_public_id,
            "lyrics handler result dropped: track identity changed during dispatch"
        );
        return Ok(());
    }
    upload::upsert_plugin_lyrics(&mut db, track_db_id, lyrics_input, lyrics_id, provider_id)
        .map_err(|err| anyhow::anyhow!("upsert_plugin_lyrics failed: {err}"))?;
    Ok(())
}

fn require_matches(context: &LyricsTrackContext, require: &ProviderRequireSpec) -> bool {
    requirements_match_with(require, |path| context_path_present(context, path))
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
        "identifiers" => match parts.next() {
            None => !context.identifiers.is_empty(),
            Some(scheme) => parts.next().is_none() && context.identifiers.contains_key(scheme),
        },
        _ => false,
    }
}

async fn build_track_context(
    track_db_id: DbId,
    _now_ms: u64,
) -> Result<Option<(LyricsTrackContext, Vec<String>, LocalTrackContext, String)>> {
    // Collect everything we need under one short read lock; defer all CPU-side
    // shaping (string trimming, HashMap building) until after the lock drops
    // so concurrent dispatches don't pin readers across owned-data work.
    let schemes = id_schemes().await;
    let (track, artists, release, external_ids_raw, libraries) = {
        let db = STATE.db.read().await;
        let Some(track) = lyra_db::tracks::get_by_id(&db, track_db_id)? else {
            return Ok(None);
        };
        let artists = lyra_db::artists::get(&db, track_db_id)?;
        let release = lyra_db::releases::get_by_track(&db, track_db_id)?
            .into_iter()
            .next();
        let external_ids_raw = lyra_db::providers::external_ids::get_for_entity(&db, track_db_id)?;
        let libraries = lyra_db::libraries::get_for_entity(&db, track_db_id)
            .ok()
            .unwrap_or_default();
        (track, artists, release, external_ids_raw, libraries)
    };

    let primary_artist = artists
        .iter()
        .map(|a| a.artist_name.trim())
        .find(|n| !n.is_empty())
        .unwrap_or("")
        .to_string();
    let album_name = release.map(|r| r.release_title);

    let identifiers = schemes.resolve(&external_ids_raw);
    let mut external_ids: HashMap<String, HashMap<String, String>> = HashMap::new();
    for ext in external_ids_raw {
        external_ids
            .entry(ext.provider_id)
            .or_default()
            .insert(ext.id_type, ext.id_value);
    }

    // Library language stands in for a per-track language preference.
    let preferred_languages: Vec<String> = libraries
        .into_iter()
        .filter_map(|lib| lib.language)
        .filter(|s| !s.trim().is_empty())
        .collect();

    let context = LyricsTrackContext {
        track_db_id: track_db_id.0,
        track_name: track.track_title.clone(),
        artist_name: primary_artist.clone(),
        album_name,
        duration_ms: track.duration_ms,
        external_ids,
        identifiers,
        force_refresh: false,
    };
    let local = LocalTrackContext {
        track_title: track.track_title,
        artist_name: primary_artist,
        duration_ms: track.duration_ms,
    };
    Ok(Some((context, preferred_languages, local, track.id)))
}

/// `force_refresh = true` includes tracks that already have preferred lyrics.
pub(crate) async fn rescan(force_refresh: bool, cancel: CancellationToken) -> Result<()> {
    let track_ids: Vec<(DbId, String)> = {
        let db = STATE.db.read().await;
        let tracks = lyra_db::tracks::get(&db, "tracks")?;
        let mut filtered = Vec::with_capacity(tracks.len());
        for track in tracks {
            let Some(track_db_id) = track.db_id.clone().map(DbId::from) else {
                continue;
            };
            if !force_refresh
                && selection::get_preferred_detail(&db, track_db_id, None, None, None, false)?
                    .is_some()
            {
                continue;
            }
            filtered.push((track_db_id, track.id));
        }
        filtered
    };

    if track_ids.is_empty() {
        return Ok(());
    }

    let cancel_for_stream = cancel.clone();
    stream::iter(track_ids)
        .for_each_concurrent(MAX_CONCURRENT_DISPATCHES, |(track_db_id, track_public_id)| {
            let cancel = cancel_for_stream.clone();
            async move {
                if cancel.is_cancelled() {
                    return;
                }
                tokio::select! {
                    _ = cancel.cancelled() => {}
                    res = dispatch_for_track_identity(track_db_id, Some(&track_public_id), force_refresh) => {
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
    use crate::services::metadata::lyrics::payload::{
        PluginLyricLineInput,
        PluginLyricsInput,
    };
    use crate::services::plugin_scope::PluginId;
    use crate::services::providers::teardown_plugin_providers;

    use std::sync::atomic::{
        AtomicUsize,
        Ordering,
    };

    #[test]
    fn require_paths_check_identifiers() {
        let context = LyricsTrackContext {
            track_db_id: 1,
            track_name: "Track".to_string(),
            artist_name: "Artist".to_string(),
            album_name: None,
            duration_ms: None,
            external_ids: HashMap::new(),
            identifiers: IdentifiersByScheme::from([(
                "example:thing".to_string(),
                "thing-1".to_string(),
            )]),
            force_refresh: false,
        };

        assert!(context_path_present(&context, "identifiers"));
        assert!(context_path_present(&context, "identifiers.example:thing"));
        assert!(!context_path_present(&context, "identifiers.example:other"));
    }

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
        let track_id = lyra_db::fixtures::insert_track(&mut db, title).expect("insert track");
        let mut track = lyra_db::tracks::get_by_id(&db, track_id)
            .expect("get_by_id")
            .expect("track");
        track.duration_ms = Some(duration_ms);
        db.exec_mut(agdb::QueryBuilder::insert().element(&track).query())
            .expect("update track");
        let artist_id = lyra_db::fixtures::insert_artist(&mut db, artist).expect("insert artist");
        lyra_db::fixtures::connect(&mut db, track_id, artist_id).expect("connect");
        track_id
    }

    async fn initialize_test_runtime() -> anyhow::Result<()> {
        crate::services::testing::init_library_test_state(std::path::PathBuf::from(".")).await?;
        Ok(())
    }

    fn make_handler(handler_fn: HandlerFn) -> ProviderLyricsSpec {
        ProviderLyricsSpec::new(
            Duration::from_secs(5),
            ProviderRequireSpec::default(),
            handler_fn,
        )
    }

    fn miss_handler() -> ProviderLyricsSpec {
        make_handler(Arc::new(|_| {
            Box::pin(async { Ok(LyricsHandlerResult::Miss) })
        }))
    }

    async fn install_handler_for_test(
        provider_id: &str,
        handler: ProviderLyricsSpec,
    ) -> CancellationToken {
        install_plugin_handler_for_test(test_plugin_id(), provider_id, handler).await
    }

    /// Declares the handler's provider and skips `register_handler`'s startup
    /// rescan to keep tests deterministic.
    async fn install_plugin_handler_for_test(
        plugin_id: PluginId,
        provider_id: &str,
        handler: ProviderLyricsSpec,
    ) -> CancellationToken {
        lyra_db::providers::register(&mut *STATE.db.write().await, provider_id)
            .expect("register provider");
        let registry = provider_registry();
        let mut registry = registry.write().await;
        registry
            .register(plugin_id, provider_id.to_string(), None)
            .expect("register provider");
        registry
            .set_lyrics_handler(provider_id, handler)
            .expect("lyrics handler stored")
    }

    #[tokio::test]
    async fn disabled_providers_are_not_dispatched() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        initialize_test_runtime().await?;
        let track_id = install_track_in_state_db("Disabled Track", "Artist", 180_000).await;
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        install_handler_for_test(
            "disabled",
            make_handler(Arc::new(move |_| {
                observed.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { bail!("upstream 503") })
            })),
        )
        .await;
        lyra_db::providers::set_enabled(&mut *STATE.db.write().await, "disabled", false)?;

        dispatch_for_track(track_id, true).await?;
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn failures_are_reported_without_skipping_other_providers() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        initialize_test_runtime().await?;
        let track_id = install_track_in_state_db("Failure Track", "Artist", 180_000).await;
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        for (provider, callback) in [
            (
                "a_error",
                Arc::new(|_| Box::pin(async { bail!("upstream 503") }) as HandlerFuture)
                    as HandlerFn,
            ),
            (
                "b_timeout",
                Arc::new(|_| Box::pin(std::future::pending()) as HandlerFuture) as HandlerFn,
            ),
            (
                "c_success",
                Arc::new(move |_| {
                    observed.fetch_add(1, Ordering::SeqCst);
                    Box::pin(async { Ok(LyricsHandlerResult::Miss) }) as HandlerFuture
                }) as HandlerFn,
            ),
        ] {
            let mut handler = make_handler(callback);
            handler.timeout = Duration::from_millis(5);
            install_handler_for_test(provider, handler).await;
        }

        let error = dispatch_for_track(track_id, false)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("a_error: upstream 503"), "{error}");
        assert!(
            error.contains("b_timeout: lyrics handler timed out"),
            "{error}"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn snapshot_dispatch_skips_deleted_and_reused_track_ids() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        initialize_test_runtime().await?;
        let track_id = install_track_in_state_db("Original", "Artist", 180_000).await;
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        install_handler_for_test(
            "snapshot",
            make_handler(Arc::new(move |_| {
                observed.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { Ok(LyricsHandlerResult::Miss) })
            })),
        )
        .await;
        let mut original = {
            let mut db = STATE.db.write().await;
            let track = lyra_db::tracks::get_by_id(&*db, track_id)?.unwrap();
            db.exec_mut(agdb::QueryBuilder::remove().ids(track_id).query())?;
            track
        };

        dispatch_for_track_identity(track_id, Some(&original.id), false).await?;
        let original_public_id = original.id.clone();
        original.db_id = None;
        original.id = "replacement-public-id".into();
        {
            let mut db = STATE.db.write().await;
            let replacement_id = db
                .exec_mut(agdb::QueryBuilder::insert().element(&original).query())?
                .ids()[0];
            assert_eq!(replacement_id, track_id);
        }
        dispatch_for_track_identity(track_id, Some(&original_public_id), false).await?;
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        dispatch_for_track(track_id, false).await?;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn hit_path_writes_lyrics_row_with_no_lock_held_during_handler() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
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
        install_handler_for_test("test_hit", make_handler(handler)).await;

        dispatch_for_track(track_id, false).await?;

        assert_eq!(
            observed.load(Ordering::SeqCst),
            1,
            "handler must run with no write lock held"
        );

        let db = STATE.db.read().await;
        let lyrics = lyra_db::lyrics::get_visible_for_track(&*db, track_id, None)?;
        assert!(
            lyrics
                .iter()
                .any(|l| l.provider_id.as_deref() == Some("test_hit")),
            "lyrics row from test_hit must exist"
        );
        Ok(())
    }

    #[tokio::test]
    async fn hit_is_dropped_when_track_db_id_is_reused_during_handler() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        initialize_test_runtime().await?;
        let track_id =
            install_track_in_state_db("A Long Song Title Here", "An Artist Group Name", 180_000)
                .await;

        let replacement = {
            let db = STATE.db.read().await;
            let mut track = lyra_db::tracks::get_by_id(&*db, track_id)?
                .ok_or_else(|| anyhow::anyhow!("original track missing"))?;
            track.db_id = None;
            track.id = "replacement-track-public-id".to_string();
            track.track_title = "Replacement Track".to_string();
            track
        };
        let replacement_public_id = replacement.id.clone();
        let handler: HandlerFn = Arc::new(move |_ctx| {
            let replacement = replacement.clone();
            Box::pin(async move {
                let mut db = STATE.db.write().await;
                db.exec_mut(agdb::QueryBuilder::remove().ids(track_id).query())?;
                let replacement_id = db
                    .exec_mut(agdb::QueryBuilder::insert().element(&replacement).query())?
                    .ids()[0];
                assert_eq!(
                    replacement_id, track_id,
                    "test requires agdb to reuse the deleted track's numeric ID"
                );
                Ok(LyricsHandlerResult::Hit {
                    candidates: vec![match_candidate()],
                })
            })
        });
        install_handler_for_test("test_recycled_track", make_handler(handler)).await;

        dispatch_for_track(track_id, false).await?;

        let db = STATE.db.read().await;
        let current = lyra_db::tracks::get_by_id(&*db, track_id)?
            .ok_or_else(|| anyhow::anyhow!("replacement track missing"))?;
        assert_eq!(current.id, replacement_public_id);
        assert!(
            lyra_db::lyrics::get_visible_for_track(&*db, track_id, None)?.is_empty(),
            "stale provider result must not be written to the replacement track"
        );
        Ok(())
    }

    #[tokio::test]
    async fn force_refresh_threads_through_to_handler() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
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
        install_handler_for_test("test_force", make_handler(handler)).await;

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
        let _guard = crate::services::testing::runtime_test_lock().await;
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
        install_handler_for_test("test_dedupe", make_handler(handler)).await;

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
        let _guard = crate::services::testing::runtime_test_lock().await;
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
        let handler: HandlerFn = Arc::new(move |_ctx| {
            let started = started_clone.clone();
            Box::pin(async move {
                started.fetch_add(1, Ordering::SeqCst);
                // Park until the dispatch's cancel race drops this future —
                // keeps the handler from burning through before the test can
                // observe how many actually started.
                tokio::time::sleep(Duration::from_secs(60)).await;
                Ok(LyricsHandlerResult::Miss)
            })
        });
        let cancel = install_handler_for_test("test_cancel", make_handler(handler)).await;

        let handle = tokio::spawn(async move { rescan(false, cancel).await });
        // Wait for the bound's worth of handlers to start.
        for _ in 0..200 {
            if started.load(Ordering::SeqCst) >= MAX_CONCURRENT_DISPATCHES {
                break;
            }
            tokio::task::yield_now().await;
        }
        let started_at_cancel = started.load(Ordering::SeqCst);
        teardown_plugin_providers(&plugin).await;

        handle.await.expect("rescan join")?;
        let final_started = started.load(Ordering::SeqCst);
        assert!(
            final_started <= started_at_cancel + MAX_CONCURRENT_DISPATCHES,
            "after unregister at most N more handler calls may execute (started_at_cancel={started_at_cancel}, final={final_started}, N={MAX_CONCURRENT_DISPATCHES})"
        );
        Ok(())
    }

    #[tokio::test]
    async fn teardown_removes_and_cancels_only_the_plugin_handlers() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        initialize_test_runtime().await?;
        let torn_down_cancel = install_handler_for_test("test_torn_down", miss_handler()).await;
        let kept_cancel = install_plugin_handler_for_test(
            PluginId::new("other_plugin")?,
            "test_kept",
            miss_handler(),
        )
        .await;

        teardown_plugin_providers(&test_plugin_id()).await;

        let registry = provider_registry().read_owned().await;
        assert!(registry.get_lyrics_handler("test_torn_down").is_none());
        assert!(torn_down_cancel.is_cancelled());
        assert!(registry.get_lyrics_handler("test_kept").is_some());
        assert!(!kept_cancel.is_cancelled());
        Ok(())
    }

    #[tokio::test]
    async fn reset_leaves_no_handlers_or_in_flight_dispatches() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        initialize_test_runtime().await?;
        let cancel = install_handler_for_test("test_reset", miss_handler()).await;
        let provider_id: Arc<str> = Arc::from("test_reset");
        let stale = InFlightGuard::try_acquire(DbId(1), &provider_id).expect("in-flight slot");

        initialize_test_runtime().await?;

        assert!(
            provider_registry()
                .read()
                .await
                .get_lyrics_handler("test_reset")
                .is_none()
        );
        assert!(cancel.is_cancelled());
        assert!(lyrics_in_flight().lock().expect("in-flight").is_empty());
        drop(stale);
        assert!(InFlightGuard::try_acquire(DbId(1), &provider_id).is_some());
        Ok(())
    }
}
