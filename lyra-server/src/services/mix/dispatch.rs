// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashMap;

use lyra_db::Track;

use super::{
    DEFAULT_LIMIT,
    MAX_LIMIT,
    MIX_DISPATCH_BUDGET,
    MixError,
    MixHandlerSpec,
    MixRequest,
    MixRequirements,
    MixSeedType,
    MixShape,
    MixViewer,
    builtin,
    finalize_mixer_tracks,
    mix_registry,
    request::ResolvedSeeds,
};
use crate::services::{
    STATE,
    options::typed_options,
    plugin_dispatch::{
        MixHandlerRequest,
        MixHandlerTrack,
    },
};

/// A mixer able to serve a request, with the request's options for it. Its handler is looked up
/// when its turn comes, so a request running through a plugin reload reaches the current one.
pub(super) struct Candidate {
    mixer_id: String,
    options: serde_json::Map<String, serde_json::Value>,
}

/// The mixers able to serve a request, highest priority first.
pub(super) struct MixerChoice {
    pub(super) candidates: Vec<Candidate>,
    /// The specs of every mixer the request could go to, capable or not.
    considered: Vec<MixHandlerSpec>,
}

/// The enabled mixers whose handler for the request's shape covers it, or only the pinned one.
/// Fails for an option an enabled mixer declares with another type.
pub(super) async fn choose_mixers(
    request: &MixRequest,
    requirements: &MixRequirements,
) -> Result<MixerChoice, MixError> {
    let configs = {
        let db = STATE.db.read().await;
        lyra_db::mixers::get(&db)?
    };
    let registry = mix_registry().read_owned().await;
    let pinned = request.mixer.as_deref();
    if let Some(pinned) = pinned {
        if !registry.is_registered(pinned) {
            return Err(MixError::Invalid(format!("unknown mixer '{pinned}'")));
        }
        if configs
            .iter()
            .any(|config| config.mixer_id == pinned && !config.enabled)
        {
            return Err(MixError::Invalid(format!("mixer '{pinned}' is disabled")));
        }
    }

    let mut choice = MixerChoice {
        candidates: Vec::new(),
        considered: Vec::new(),
    };
    for config in configs {
        if !config.enabled {
            continue;
        }
        let options = typed_options(
            &config.mixer_id,
            registry.get_options(&config.mixer_id),
            &request.options,
        )
        .map_err(MixError::Invalid)?;
        if pinned.is_some_and(|pinned| pinned != config.mixer_id) {
            continue;
        }
        let Some(registration) = registry.handler(&config.mixer_id, request.shape) else {
            continue;
        };
        choice.considered.push(registration.spec.clone());
        if registration.spec.covers(requirements) {
            choice.candidates.push(Candidate {
                mixer_id: config.mixer_id,
                options,
            });
        }
    }
    Ok(choice)
}

/// Names what keeps the request from any mixer, the built-in one included when the request
/// could go to it: the features no mixer takes at all, or the combination when each is taken by
/// some mixer but none takes them all.
pub(super) fn unsupported(
    request: &MixRequest,
    requirements: &MixRequirements,
    choice: &MixerChoice,
) -> MixError {
    let subject = match &request.mixer {
        Some(pinned) => format!("mixer '{pinned}' can't serve this mix"),
        None => "no mixer can serve this mix".to_string(),
    };
    let builtin = (request.mixer.is_none() && request.shape == MixShape::Blend).then(builtin::spec);
    let specs = choice
        .considered
        .iter()
        .chain(builtin.as_ref())
        .collect::<Vec<_>>();
    if specs.is_empty() {
        return MixError::Unsupported(format!("{subject}: {}", request.shape.as_str()));
    }

    let mut features = requirements
        .seed_types
        .iter()
        .map(|&seed_type| Feature::SeedType(seed_type))
        .collect::<Vec<_>>();
    if request.shape == MixShape::Blend && requirements.seed_count > 1 {
        features.push(Feature::SeedCount(requirements.seed_count));
    }
    if requirements.exclude {
        features.push(Feature::Exclude);
    }
    if requirements.query {
        features.push(Feature::Query);
    }

    let missing = features
        .iter()
        .filter(|feature| !specs.iter().any(|spec| feature.served_by(spec)))
        .map(Feature::name)
        .collect::<Vec<_>>();
    if missing.is_empty() {
        let all = features.iter().map(Feature::name).collect::<Vec<_>>();
        MixError::Unsupported(format!(
            "{subject}: no single mixer takes {}",
            all.join(", ")
        ))
    } else {
        MixError::Unsupported(format!("{subject}: {}", missing.join(", ")))
    }
}

enum Feature {
    SeedType(MixSeedType),
    SeedCount(usize),
    Exclude,
    Query,
}

impl Feature {
    fn name(&self) -> String {
        match self {
            Self::SeedType(seed_type) => format!("{} seeds", seed_type.as_str()),
            Self::SeedCount(count) => format!("{count} seeds"),
            Self::Exclude => "excluded seeds".to_string(),
            Self::Query => "query".to_string(),
        }
    }

    fn served_by(&self, spec: &MixHandlerSpec) -> bool {
        match self {
            Self::SeedType(seed_type) => spec.seed_types.contains(seed_type),
            Self::SeedCount(count) => spec.max_seeds >= *count,
            Self::Exclude => spec.exclude,
            Self::Query => spec.query,
        }
    }
}

/// The visible tracks a mixer returned, in its order, and the scores it gave them by public
/// track id.
pub(super) struct Dispatched {
    pub(super) tracks: Vec<Track>,
    pub(super) scores: HashMap<String, f64>,
}

/// Tries `candidates` in order; the first with visible tracks wins. Each handler gets its spec's
/// timeout, cut to what remains of the request's budget, and the explicit seeds followed by as
/// many history seeds as its spec takes. A candidate whose current handler no longer covers the
/// request is passed over. `Ok(None)` when none produced tracks.
pub(super) async fn dispatch(
    candidates: &[Candidate],
    request: &MixRequest,
    requirements: &MixRequirements,
    seeds: &ResolvedSeeds,
    viewer: &MixViewer,
) -> anyhow::Result<Option<Dispatched>> {
    if candidates.is_empty() {
        return Ok(None);
    }
    let deadline = tokio::time::Instant::now() + MIX_DISPATCH_BUDGET;

    for (index, candidate) in candidates.iter().enumerate() {
        let now = tokio::time::Instant::now();
        if now >= deadline {
            tracing::warn!(
                remaining = candidates.len() - index,
                "mix dispatch budget exhausted"
            );
            break;
        }
        let mixer_id = &candidate.mixer_id;
        let registration = mix_registry()
            .read()
            .await
            .handler(mixer_id, request.shape)
            .cloned();
        let Some(registration) =
            registration.filter(|registration| registration.spec.covers(requirements))
        else {
            tracing::debug!(mixer = %mixer_id, "mixer has no handler for this mix now, trying next");
            continue;
        };
        let Some(runtime) = STATE.generation().plugin_dispatch.get() else {
            tracing::debug!(mixer = %mixer_id, "no plugin runtime to reach the mixer, trying next");
            continue;
        };
        let timeout = (deadline - now).min(registration.timeout);
        let handler_request = MixHandlerRequest {
            handler: registration.handler,
            seeds: seeds.for_handler(registration.spec.max_seeds),
            query: request.query().map(str::to_string),
            limit: request.limit,
            options: candidate.options.clone(),
        };
        let entries = match tokio::time::timeout(
            timeout,
            runtime.dispatch_mix_handler(handler_request),
        )
        .await
        {
            Ok(Ok(result)) => result.tracks,
            Ok(Err(err)) => {
                tracing::warn!(mixer = %mixer_id, error = %err, "mixer handler crashed, trying next");
                continue;
            }
            Err(_) => {
                tracing::warn!(
                    mixer = %mixer_id,
                    timeout_ms = timeout.as_millis() as u64,
                    "mixer handler timed out; trying next"
                );
                continue;
            }
        };
        let mut dispatched = tracks_from_mixer_result(entries).await?;
        dispatched.tracks = {
            let db = STATE.db.read().await;
            finalize_mixer_tracks(
                &db,
                dispatched.tracks,
                request.shape,
                viewer,
                request.limit.unwrap_or(DEFAULT_LIMIT),
            )?
        };
        if dispatched.tracks.is_empty() {
            tracing::debug!(mixer = %mixer_id, "mixer returned no accessible tracks, trying next");
        } else {
            return Ok(Some(dispatched));
        }
    }

    Ok(None)
}

/// The most track ids kept from a handler's result: a blend's largest limit, and the longest
/// path a mixer can return.
const MAX_MIXER_RESULT_IDS: usize = MAX_LIMIT;

/// The tracks a handler's result names that resolve, in its order, and the scores it gave by
/// public track id; a track named twice keeps the score of its first entry, or none.
async fn tracks_from_mixer_result(entries: Vec<MixHandlerTrack>) -> anyhow::Result<Dispatched> {
    let mut first_scores = HashMap::new();
    let track_ids = entries
        .into_iter()
        .take(MAX_MIXER_RESULT_IDS)
        .map(|entry| {
            first_scores
                .entry(entry.track_id.clone())
                .or_insert(entry.score);
            entry.track_id
        })
        .collect::<Vec<_>>();
    let scores = first_scores
        .into_iter()
        .filter_map(|(track_id, score)| Some((track_id, score?)))
        .collect();
    if track_ids.is_empty() {
        return Ok(Dispatched {
            tracks: Vec::new(),
            scores,
        });
    }

    let db = STATE.db.read().await;
    let mut ids = Vec::with_capacity(track_ids.len());
    for track_id in &track_ids {
        if let Some(id) = lyra_db::lookup::find_node_id_by_id(&*db, track_id)? {
            ids.push(id);
        }
    }
    let tracks_by_id = lyra_db::tracks::get_existing_by_ids(&db, &ids)?;
    let tracks = ids
        .iter()
        .filter_map(|id| tracks_by_id.get(id).cloned())
        .collect::<Vec<_>>();
    let dropped = track_ids.len() - tracks.len();
    if dropped > 0 {
        tracing::warn!(
            dropped,
            requested = track_ids.len(),
            "mixer returned track IDs that could not be resolved"
        );
    }
    Ok(Dispatched { tracks, scores })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::mix::MixHandlerRegistration;

    fn spec(seed_types: &[MixSeedType], max_seeds: usize, query: bool) -> MixHandlerSpec {
        MixHandlerSpec {
            seed_types: seed_types.to_vec(),
            max_seeds,
            exclude: false,
            query,
        }
    }

    fn message(
        request: &MixRequest,
        requirements: &MixRequirements,
        specs: Vec<MixHandlerSpec>,
    ) -> String {
        let choice = MixerChoice {
            candidates: Vec::new(),
            considered: specs,
        };
        match unsupported(request, requirements, &choice) {
            MixError::Unsupported(message) => message,
            other => panic!("expected an unsupported mix, got {other:?}"),
        }
    }

    async fn register_mixer(
        mixer_id: &str,
        handler: crate::services::plugin_dispatch::PluginCallbackHandle,
    ) -> anyhow::Result<()> {
        let registry = mix_registry();
        let mut registry = registry.write().await;
        registry.register(
            crate::services::plugin_scope::PluginId::new(format!("{mixer_id}_plugin").as_str())?,
            mixer_id.to_string(),
        )?;
        registry.set_handler(
            mixer_id,
            MixShape::Blend,
            MixHandlerRegistration {
                spec: spec(&[], 5, false),
                timeout: std::time::Duration::from_secs(5),
                handler,
            },
        )?;
        Ok(())
    }

    fn candidate(mixer_id: &str) -> Candidate {
        Candidate {
            mixer_id: mixer_id.to_string(),
            options: serde_json::Map::new(),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_reload_between_mixers_reaches_the_next_mixers_new_handler() -> anyhow::Result<()> {
        use crate::services::testing::{
            FakePluginDispatch,
            FakeResponse,
            init_default_test_state,
            runtime_test_lock,
        };

        let _guard = runtime_test_lock().await;
        init_default_test_state()?;
        let old = FakePluginDispatch::install();
        let gate = std::sync::Arc::new(tokio::sync::Semaphore::new(0));
        let first = old.add(FakeResponse::Held(
            gate.clone(),
            Box::new(FakeResponse::Value(serde_json::json!([]))),
        ));
        let old_second = old.add(FakeResponse::Value(serde_json::json!([])));
        register_mixer("first", first).await?;
        register_mixer("second", old_second).await?;
        let (track_public_id, seeds, viewer) = {
            let mut db = STATE.db.write().await;
            let track = lyra_db::fixtures::insert_track(&mut db, "Track")?;
            let viewer = MixViewer {
                principal: None,
                accessible_library_ids: None,
                playlists: Default::default(),
            };
            let seeds = ResolvedSeeds::resolve(&db, &MixRequest::default(), &viewer)
                .map_err(|err| anyhow::anyhow!("seeds: {err:?}"))?;
            let public_id = lyra_db::tracks::get_by_id(&db, track)?.expect("track").id;
            (public_id, seeds, viewer)
        };
        let requirements = MixRequirements {
            seed_types: Vec::new(),
            seed_count: 0,
            exclude: false,
            query: false,
        };

        let running = tokio::spawn(async move {
            dispatch(
                &[candidate("first"), candidate("second")],
                &MixRequest::default(),
                &requirements,
                &seeds,
                &viewer,
            )
            .await
            .map(|dispatched| dispatched.map(|dispatched| dispatched.tracks.len()))
        });
        old.wait_for_calls(first, 1).await;
        super::super::registry::teardown_plugin_mixers(
            &crate::services::plugin_scope::PluginId::new("second_plugin")?,
        )
        .await;
        let new = FakePluginDispatch::install_as(2);
        let new_second = new.add(FakeResponse::Value(serde_json::json!([track_public_id])));
        register_mixer("second", new_second).await?;
        gate.add_permits(1);

        assert_eq!(running.await??, Some(1));
        assert!(old.calls(old_second).is_empty());
        assert_eq!(new.calls(new_second).len(), 1);
        Ok(())
    }

    #[test]
    fn unsupported_mixes_name_what_no_mixer_takes() {
        let requirements = MixRequirements {
            seed_types: vec![MixSeedType::Track],
            seed_count: 3,
            exclude: true,
            query: false,
        };
        let blend = MixRequest::default();
        assert_eq!(
            message(
                &blend,
                &requirements,
                vec![spec(&[MixSeedType::Track], 5, false)]
            ),
            "no mixer can serve this mix: excluded seeds"
        );

        let path = MixRequest {
            shape: MixShape::Path,
            ..MixRequest::default()
        };
        let requirements = MixRequirements {
            seed_count: 2,
            exclude: false,
            ..requirements
        };
        assert_eq!(
            message(&path, &requirements, Vec::new()),
            "no mixer can serve this mix: path"
        );
    }

    #[test]
    fn unsupported_mixes_name_a_combination_no_single_mixer_takes() {
        let requirements = MixRequirements {
            seed_types: vec![MixSeedType::Playlist],
            seed_count: 2,
            exclude: false,
            query: true,
        };
        let specs = || vec![spec(&[MixSeedType::Track], 2, true)];
        assert_eq!(
            message(&MixRequest::default(), &requirements, specs()),
            "no mixer can serve this mix: no single mixer takes playlist seeds, 2 seeds, query",
            "the built-in mixer takes playlist seeds"
        );
        let pinned = MixRequest {
            mixer: Some("pinned".to_string()),
            ..MixRequest::default()
        };
        assert_eq!(
            message(&pinned, &requirements, specs()),
            "mixer 'pinned' can't serve this mix: playlist seeds"
        );
    }
}
