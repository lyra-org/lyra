// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Lyrics provider dispatcher. Handlers MUST resolve cooldown / known-miss
//! / known-instrumental without awaiting HTTP — they run sequentially.

use std::{
    sync::Arc,
    time::Duration,
};

use agdb::DbId;
use anyhow::{
    Context,
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
    services::plugin_dispatch::{
        MetadataRefreshRequest,
        RuntimeCalls,
    },
    services::providers::{
        ExternalIdsByScheme,
        LyricsInFlight,
        LyricsInFlightKey,
        ProviderCallStage,
        ProviderLookup,
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
use lyra_db::roots::{
    self,
    Root,
};

pub(crate) const DEFAULT_HANDLER_TIMEOUT: Duration = Duration::from_secs(15);
pub(crate) const MAX_CONCURRENT_DISPATCHES: usize = 4;

#[derive(Clone, Debug)]
struct LyricsTrackContext {
    track_id: String,
    track_name: String,
    artist_name: String,
    album_name: Option<String>,
    duration_ms: Option<u64>,
    external_ids: ExternalIdsByScheme,
    /// Skip Miss/Instrumental fast path; cooldown still applies.
    force_refresh: bool,
}

fn track_context_to_json(context: &LyricsTrackContext) -> serde_json::Value {
    serde_json::json!({
        "track_id": context.track_id,
        "track_name": context.track_name,
        "artist_name": context.artist_name,
        "album_name": context.album_name,
        "duration_ms": context.duration_ms,
        "external_ids": context.external_ids,
        "force_refresh": context.force_refresh,
    })
}

/// Closed kind set. Anything else surfaces as a Lua error.
#[derive(Clone, Debug)]
enum LyricsHandlerResult {
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

fn parse_handler_result(value: serde_json::Value) -> Result<LyricsHandlerResult> {
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

struct InFlightGuard {
    in_flight: LyricsInFlight,
    key: LyricsInFlightKey,
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

/// Spawns a startup rescan sharing the handler's cancel token. It waits for `published`, its own
/// scope going live: a rescan dispatches through the published scope, and the handler isn't there
/// until then.
pub(crate) fn register_handler(
    registry: &mut crate::services::providers::ProviderRegistry,
    published: CancellationToken,
    provider_id: &str,
    spec: ProviderLyricsSpec,
) {
    let Some(cancel) = registry.set_lyrics_handler(provider_id, spec) else {
        return;
    };

    tokio::spawn(async move {
        tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            () = published.cancelled() => {}
        }
        if let Err(err) = rescan(false, cancel).await {
            tracing::warn!(error = %err, "lyrics startup rescan failed");
        }
    });
}

/// The providers a dispatch could not finish for its track.
#[derive(Debug, Default)]
pub(crate) struct LyricsDispatchOutcome {
    pub(crate) failed: Vec<(Arc<str>, anyhow::Error)>,
    /// Skipped because a lookup for this track and provider was already running.
    pub(crate) in_progress: Vec<Arc<str>>,
}

impl LyricsDispatchOutcome {
    fn into_result(self) -> Result<()> {
        if self.failed.is_empty() {
            return Ok(());
        }
        let failures: Vec<String> = self
            .failed
            .iter()
            .map(|(provider_id, error)| format!("{provider_id}: {error:#}"))
            .collect();
        bail!("lyrics dispatch failed: {}", failures.join("; "));
    }
}

/// [`dispatch_for_track_identity`], failing when any provider failed.
pub(crate) async fn dispatch_for_track(track_db_id: DbId, force_refresh: bool) -> Result<()> {
    dispatch_for_track_identity(track_db_id, None, force_refresh)
        .await?
        .into_result()
}

/// Run every matching handler of an active provider in provider order. Not
/// stop-on-first-hit — each gate-passing `Hit` writes a row; `selection.rs`
/// picks the winner.
pub(crate) async fn dispatch_for_track_identity(
    track_db_id: DbId,
    expected_public_id: Option<&str>,
    force_refresh: bool,
) -> Result<LyricsDispatchOutcome> {
    let now = upload::now_ms().map_err(|err| anyhow::anyhow!("now_ms() failed: {err}"))?;
    let (mut context, preferred_languages, local, track_public_id) =
        match build_track_context(track_db_id, now).await? {
            Some(parts) => parts,
            None => return Ok(LyricsDispatchOutcome::default()),
        };
    context.force_refresh = force_refresh;
    if expected_public_id.is_some_and(|expected| expected != track_public_id) {
        return Ok(LyricsDispatchOutcome::default());
    }

    let providers = active_providers(&*STATE.db.read().await)?;
    let provider_ids: Vec<Arc<str>> = {
        let registry = provider_registry().read_owned().await;
        providers
            .iter()
            .filter(|provider| registry.has_lyrics_handler(&provider.provider_id))
            .map(|provider| Arc::from(provider.provider_id.as_str()))
            .collect()
    };

    let mut outcome = LyricsDispatchOutcome::default();
    for provider_id in provider_ids {
        let Some(guard) = InFlightGuard::try_acquire(track_db_id, &provider_id) else {
            tracing::debug!(
                provider = provider_id.as_ref(),
                track_db_id = track_db_id.0,
                "lyrics dispatch deduped: another caller has this (track, provider) in flight"
            );
            outcome.in_progress.push(provider_id);
            continue;
        };

        if let Err(err) = dispatch_one(
            &provider_id,
            &context,
            &preferred_languages,
            &local,
            track_db_id,
            &track_public_id,
        )
        .await
        {
            outcome.failed.push((provider_id, err));
        }
        drop(guard);
    }

    Ok(outcome)
}

/// Calls the handler registered when the provider's turn comes, then records its answer. The
/// spec's cancel token ends the call, and the recording after it, when the spec is replaced or
/// torn down.
async fn dispatch_one(
    provider_id: &str,
    context: &LyricsTrackContext,
    preferred_languages: &[String],
    local: &LocalTrackContext,
    track_db_id: DbId,
    track_public_id: &str,
) -> Result<()> {
    let call = with_provider_call(
        provider_id,
        ProviderCallStage::Lyrics,
        |registry| {
            let spec = registry.get_lyrics_handler(provider_id)?;
            Some(if require_matches(context, &spec.require) {
                ProviderLookup::Call(spec.clone())
            } else {
                ProviderLookup::Answer(None)
            })
        },
        |spec, calls| async move {
            let answered = unless_cancelled(
                &spec.cancel,
                provider_id,
                track_db_id,
                call_lyrics_handler(&calls, &spec, context, track_db_id, track_public_id),
            )
            .await
            .transpose()?
            .flatten();
            Ok(answered.map(|outcome| (outcome, spec.cancel.clone())))
        },
    )
    .await;
    let (outcome, cancel) = match call {
        Ok(Some(answered)) => answered,
        Ok(None) => return Ok(()),
        // Enumerated with a handler, so a lookup that finds none now leaves this track unasked.
        Err(error) => return Err(error.into()),
    };

    unless_cancelled(
        &cancel,
        provider_id,
        track_db_id,
        record_outcome(
            provider_id,
            outcome,
            preferred_languages,
            local,
            track_db_id,
            track_public_id,
        ),
    )
    .await
    .unwrap_or(Ok(()))
}

async fn unless_cancelled<T>(
    cancel: &CancellationToken,
    provider_id: &str,
    track_db_id: DbId,
    work: impl std::future::Future<Output = T>,
) -> Option<T> {
    tokio::select! {
        biased;
        _ = cancel.cancelled() => {
            tracing::debug!(
                provider = provider_id,
                track_db_id = track_db_id.0,
                "lyrics dispatch cancelled before completion"
            );
            None
        }
        output = work => Some(output),
    }
}

async fn call_lyrics_handler<C: RuntimeCalls>(
    calls: &C,
    spec: &ProviderLyricsSpec,
    context: &LyricsTrackContext,
    track_db_id: DbId,
    track_public_id: &str,
) -> Result<Option<LyricsHandlerResult>> {
    let timeout = if spec.timeout.is_zero() {
        DEFAULT_HANDLER_TIMEOUT
    } else {
        spec.timeout
    };
    {
        let db = STATE.db.read().await;
        if lyra_db::tracks::get_by_id(&*db, track_db_id)?
            .is_none_or(|track| track.id != track_public_id)
        {
            return Ok(None);
        }
    }
    let request = MetadataRefreshRequest {
        handler: spec.handler,
        context: track_context_to_json(context),
        deadline: std::time::Instant::now() + timeout,
    };
    let result = tokio::time::timeout(timeout, calls.metadata_refresh(request))
        .await
        .map_err(|_| {
            anyhow::anyhow!("lyrics handler timed out after {} ms", timeout.as_millis())
        })??;
    let value = result
        .values
        .into_iter()
        .next()
        .context("lyrics handler returned no value")?;
    parse_handler_result(value).map(Some)
}

async fn record_outcome(
    provider_id: &str,
    outcome: LyricsHandlerResult,
    preferred_languages: &[String],
    local: &LocalTrackContext,
    track_db_id: DbId,
    track_public_id: &str,
) -> Result<()> {
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
        "external_ids" => match parts.next() {
            None => !context.external_ids.is_empty(),
            Some(scheme) => parts.next().is_none() && context.external_ids.contains_key(scheme),
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

    let external_ids = schemes.resolve(&external_ids_raw);

    // Library language stands in for a per-track language preference.
    let preferred_languages: Vec<String> = libraries
        .into_iter()
        .filter_map(|lib| lib.language)
        .filter(|s| !s.trim().is_empty())
        .collect();

    let context = LyricsTrackContext {
        track_id: track.id.clone(),
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
    Ok(Some((context, preferred_languages, local, track.id)))
}

/// `force_refresh = true` includes tracks that already have preferred lyrics.
pub(crate) async fn rescan(force_refresh: bool, cancel: CancellationToken) -> Result<()> {
    let track_ids: Vec<(DbId, String)> = {
        let db = STATE.db.read().await;
        let tracks: Vec<lyra_db::tracks::Track> = roots::members(&*db, Root::Tracks)?;
        let providers = crate::services::providers::active_providers(&*db)?;
        let mut filtered = Vec::with_capacity(tracks.len());
        for track in tracks {
            let Some(track_db_id) = track.db_id.clone().map(DbId::from) else {
                continue;
            };
            if !force_refresh {
                let candidates = selection::visible_for_track(&db, track_db_id, None)?;
                let ranking = selection::Ranking::default();
                if selection::pick_preferred(&candidates, &providers, ranking, track.duration_ms)
                    .is_some()
                {
                    continue;
                }
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
                        if let Err(err) = res.and_then(LyricsDispatchOutcome::into_result) {
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
    use crate::services::plugin_dispatch::PluginCallbackHandle;
    use crate::services::plugin_scope::PluginId;
    use crate::services::providers::teardown_plugin_providers;
    use crate::services::testing::{
        FakePluginDispatch,
        FakeResponse,
    };

    use tokio::sync::Semaphore;

    #[test]
    fn require_paths_check_external_ids_by_scheme() {
        let context = LyricsTrackContext {
            track_id: "track-1".to_string(),
            track_name: "Track".to_string(),
            artist_name: "Artist".to_string(),
            album_name: None,
            duration_ms: None,
            external_ids: ExternalIdsByScheme::from([(
                "example:thing".to_string(),
                "thing-1".to_string(),
            )]),
            force_refresh: false,
        };

        assert!(context_path_present(&context, "external_ids"));
        assert!(context_path_present(&context, "external_ids.example:thing"));
        assert!(!context_path_present(
            &context,
            "external_ids.example:other"
        ));
        assert!(!context_path_present(&context, "identifiers.example:thing"));
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

    fn miss() -> FakeResponse {
        FakeResponse::Value(serde_json::json!({ "kind": "miss" }))
    }

    fn match_hit() -> FakeResponse {
        FakeResponse::Value(serde_json::json!({
            "kind": "hit",
            "candidates": [{
                "lyrics": lyrics_payload("la la la"),
                "title": "A Long Song Title Here",
                "artist": "An Artist Group Name",
                "duration_ms": 180_000,
                "language": "eng",
            }],
        }))
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

    async fn initialize_test_runtime() -> anyhow::Result<Arc<FakePluginDispatch>> {
        crate::services::testing::init_library_test_state(std::path::PathBuf::from(".")).await?;
        Ok(FakePluginDispatch::install())
    }

    fn handler(callback: PluginCallbackHandle) -> ProviderLyricsSpec {
        ProviderLyricsSpec::new(
            Duration::from_secs(5),
            ProviderRequireSpec::default(),
            callback,
        )
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
        let dispatch = initialize_test_runtime().await?;
        let track_id = install_track_in_state_db("Disabled Track", "Artist", 180_000).await;
        let callback = dispatch.add(FakeResponse::Error("upstream 503".to_string()));
        install_handler_for_test("disabled", handler(callback)).await;
        lyra_db::providers::set_enabled(&mut *STATE.db.write().await, "disabled", false)?;

        dispatch_for_track(track_id, true).await?;
        assert!(dispatch.calls(callback).is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn failures_are_reported_without_skipping_other_providers() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        let dispatch = initialize_test_runtime().await?;
        let track_id = install_track_in_state_db("Failure Track", "Artist", 180_000).await;
        let mut success_id = None;
        for (provider, response) in [
            ("a_error", FakeResponse::Error("upstream 503".to_string())),
            ("b_timeout", FakeResponse::Stall),
            ("c_success", miss()),
        ] {
            let callback = dispatch.add(response);
            success_id = Some(callback);
            let mut handler = handler(callback);
            handler.timeout = Duration::from_millis(5);
            install_handler_for_test(provider, handler).await;
        }

        let success_id = success_id.expect("success handler");

        let outcome = dispatch_for_track_identity(track_id, None, false).await?;
        let failed: Vec<(&str, String)> = outcome
            .failed
            .iter()
            .map(|(provider_id, error)| (provider_id.as_ref(), format!("{error:#}")))
            .collect();
        assert_eq!(failed.len(), 2, "{failed:?}");
        assert_eq!(failed[0].0, "a_error");
        assert!(failed[0].1.contains("upstream 503"), "{failed:?}");
        assert_eq!(failed[1].0, "b_timeout");
        assert!(
            failed[1].1.contains("lyrics handler timed out"),
            "{failed:?}"
        );
        assert!(outcome.in_progress.is_empty());
        assert_eq!(dispatch.calls(success_id).len(), 1);

        let error = dispatch_for_track(track_id, false)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            error.starts_with("lyrics dispatch failed: a_error: "),
            "{error}"
        );
        assert!(error.contains("upstream 503"), "{error}");
        assert!(
            error.contains("; b_timeout: lyrics handler timed out"),
            "{error}"
        );
        assert_eq!(dispatch.calls(success_id).len(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn snapshot_dispatch_skips_deleted_and_reused_track_ids() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        let dispatch = initialize_test_runtime().await?;
        let track_id = install_track_in_state_db("Original", "Artist", 180_000).await;
        let callback = dispatch.add(miss());
        install_handler_for_test("snapshot", handler(callback)).await;
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
        let outcome =
            dispatch_for_track_identity(track_id, Some(&original_public_id), false).await?;
        assert!(outcome.failed.is_empty() && outcome.in_progress.is_empty());
        assert!(dispatch.calls(callback).is_empty());
        dispatch_for_track(track_id, false).await?;
        assert_eq!(dispatch.calls(callback).len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn hit_path_writes_lyrics_row_with_no_lock_held_during_handler() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        let dispatch = initialize_test_runtime().await?;
        let track_id =
            install_track_in_state_db("A Long Song Title Here", "An Artist Group Name", 180_000)
                .await;
        let release = Arc::new(Semaphore::new(0));
        let callback = dispatch.add(FakeResponse::Held(release.clone(), Box::new(match_hit())));
        install_handler_for_test("test_hit", handler(callback)).await;

        let running = tokio::spawn(async move { dispatch_for_track(track_id, false).await });
        dispatch.wait_for_calls(callback, 1).await;
        assert!(
            STATE.db.get().try_write().is_ok(),
            "handler must run with no database lock held"
        );
        release.add_permits(1);
        running.await??;

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
        let dispatch = initialize_test_runtime().await?;
        let track_id =
            install_track_in_state_db("A Long Song Title Here", "An Artist Group Name", 180_000)
                .await;
        let release = Arc::new(Semaphore::new(0));
        let callback = dispatch.add(FakeResponse::Held(release.clone(), Box::new(match_hit())));
        install_handler_for_test("test_recycled_track", handler(callback)).await;

        let running = tokio::spawn(async move { dispatch_for_track(track_id, false).await });
        dispatch.wait_for_calls(callback, 1).await;
        let replacement_public_id = {
            let mut db = STATE.db.write().await;
            let mut replacement = lyra_db::tracks::get_by_id(&*db, track_id)?
                .ok_or_else(|| anyhow::anyhow!("original track missing"))?;
            replacement.db_id = None;
            replacement.id = "replacement-track-public-id".to_string();
            replacement.track_title = "Replacement Track".to_string();
            db.exec_mut(agdb::QueryBuilder::remove().ids(track_id).query())?;
            let replacement_id = db
                .exec_mut(agdb::QueryBuilder::insert().element(&replacement).query())?
                .ids()[0];
            assert_eq!(
                replacement_id, track_id,
                "test requires agdb to reuse the deleted track's numeric ID"
            );
            replacement.id
        };
        release.add_permits(1);
        running.await??;

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
        let dispatch = initialize_test_runtime().await?;
        let track_id =
            install_track_in_state_db("Title For Force", "Artist For Force", 200_000).await;
        let callback = dispatch.add(miss());
        install_handler_for_test("test_force", handler(callback)).await;

        dispatch_for_track(track_id, true).await?;
        let calls = dispatch.calls(callback);
        assert_eq!(calls.len(), 1);
        assert_eq!(
            calls[0]["force_refresh"],
            serde_json::Value::Bool(true),
            "handler must observe force_refresh = true"
        );
        Ok(())
    }

    #[tokio::test]
    async fn in_flight_dedupe_skips_concurrent_call_for_same_key() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        let dispatch = initialize_test_runtime().await?;
        let track_id =
            install_track_in_state_db("Title For Dedupe", "Artist For Dedupe", 200_000).await;
        let release = Arc::new(Semaphore::new(0));
        let callback = dispatch.add(FakeResponse::Held(release.clone(), Box::new(miss())));
        install_handler_for_test("test_dedupe", handler(callback)).await;

        let first = tokio::spawn(async move { dispatch_for_track(track_id, false).await });
        dispatch.wait_for_calls(callback, 1).await;
        dispatch_for_track(track_id, false).await?;
        let outcome = dispatch_for_track_identity(track_id, None, false).await?;
        assert_eq!(
            dispatch.calls(callback).len(),
            1,
            "concurrent dispatches must dedupe (handler called exactly once)"
        );
        assert_eq!(outcome.in_progress, vec![Arc::<str>::from("test_dedupe")]);
        assert!(outcome.failed.is_empty());

        release.add_permits(1);
        first.await.expect("join first")?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_provider_unregistered_after_enumeration_is_reported_as_failed() -> anyhow::Result<()>
    {
        let _guard = crate::services::testing::runtime_test_lock().await;
        let dispatch = initialize_test_runtime().await?;
        let track_id = install_track_in_state_db("Title", "Artist", 200_000).await;
        let release = Arc::new(Semaphore::new(0));
        let held = dispatch.add(FakeResponse::Held(release.clone(), Box::new(miss())));
        let vanishing = dispatch.add(miss());
        install_handler_for_test("a_held", handler(held)).await;
        install_handler_for_test("b_vanishing", handler(vanishing)).await;

        let run =
            tokio::spawn(async move { dispatch_for_track_identity(track_id, None, false).await });
        dispatch.wait_for_calls(held, 1).await;
        teardown_plugin_providers(&test_plugin_id()).await;
        release.add_permits(1);
        let outcome = run.await.expect("join")?;

        assert!(dispatch.calls(vanishing).is_empty());
        let failed: Vec<&str> = outcome
            .failed
            .iter()
            .map(|(provider_id, _)| provider_id.as_ref())
            .collect();
        assert_eq!(failed, ["b_vanishing"]);
        assert!(outcome.in_progress.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn cancellation_aborts_parked_dispatches() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        let dispatch = initialize_test_runtime().await?;

        for i in 0..100 {
            install_track_in_state_db(
                &format!("Cancel Title {i}"),
                &format!("Cancel Artist {i}"),
                180_000,
            )
            .await;
        }

        let callback = dispatch.add(FakeResponse::Stall);
        let mut spec = handler(callback);
        spec.timeout = Duration::from_secs(60);
        let cancel = install_handler_for_test("test_cancel", spec).await;

        let handle = tokio::spawn(async move { rescan(false, cancel).await });
        // Calls to one provider are serialized, so one handler call parks the rest.
        dispatch.wait_for_calls(callback, 1).await;
        let started_at_cancel = dispatch.calls(callback).len();
        teardown_plugin_providers(&test_plugin_id()).await;

        handle.await.expect("rescan join")?;
        let final_started = dispatch.calls(callback).len();
        assert!(
            final_started <= started_at_cancel + MAX_CONCURRENT_DISPATCHES,
            "after unregister at most N more handler calls may execute (started_at_cancel={started_at_cancel}, final={final_started}, N={MAX_CONCURRENT_DISPATCHES})"
        );
        Ok(())
    }

    #[tokio::test]
    async fn teardown_removes_and_cancels_only_the_plugin_handlers() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        let dispatch = initialize_test_runtime().await?;
        let torn_down_cancel =
            install_handler_for_test("test_torn_down", handler(dispatch.add(miss()))).await;
        let kept_cancel = install_plugin_handler_for_test(
            PluginId::new("other_plugin")?,
            "test_kept",
            handler(dispatch.add(miss())),
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
    async fn a_dispatch_waiting_its_turn_reaches_the_handler_registered_by_then()
    -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        let dispatch = initialize_test_runtime().await?;
        let track_id = install_track_in_state_db("Waiting", "Artist", 180_000).await;
        let old = dispatch.add(miss());
        let old_cancel = install_handler_for_test("test_waiting", handler(old)).await;
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(Semaphore::new(0));
        let running = tokio::spawn({
            let (entered, release) = (entered.clone(), release.clone());
            async move {
                with_provider_call(
                    "test_waiting",
                    ProviderCallStage::Lyrics,
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
        let waiting = tokio::spawn(async move { dispatch_for_track(track_id, false).await });
        tokio::task::yield_now().await;

        let new = dispatch.add(miss());
        provider_registry()
            .write()
            .await
            .set_lyrics_handler("test_waiting", handler(new));
        assert!(old_cancel.is_cancelled());
        release.add_permits(1);
        running.await.expect("join")?;
        waiting.await.expect("join")?;

        assert!(dispatch.calls(old).is_empty());
        assert_eq!(dispatch.calls(new).len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn reset_leaves_no_handlers_or_in_flight_dispatches() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        let dispatch = initialize_test_runtime().await?;
        let cancel = install_handler_for_test("test_reset", handler(dispatch.add(miss()))).await;
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
