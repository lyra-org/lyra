// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::{
    HashMap,
    HashSet,
};

use agdb::{
    DbAny,
    DbId,
};
use serde::Deserialize;

use super::{
    MAX_LIMIT,
    MixRequirements,
    MixSeedType,
    MixShape,
    MixViewer,
    builtin::recent_listen_tracks,
};
use crate::services::{
    auth::access,
    options::check_passed_options,
};

/// Most seeds a request may name; no mixer takes more. History adds its own on top.
pub(crate) const MAX_SEEDS: usize = 50;
pub(crate) const MAX_QUERY_LEN: usize = 512;

/// A mix request as clients send it, seeds by public id.
#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "docgen", schemars(rename = "MixRequest"))]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MixRequestFields {
    /// Tracks, releases, artists, genres or playlists to mix from; the server tells their types
    /// apart. An excluded seed is one to mix away from.
    #[serde(default)]
    pub(crate) seeds: Vec<MixSeedField>,
    /// Adds the caller's recent listens as track seeds, most recent first, up to what the
    /// serving mixer takes beyond the explicit seeds.
    #[serde(default)]
    pub(crate) from_history: bool,
    /// A free-text description of the mix.
    pub(crate) q: Option<String>,
    pub(crate) shape: Option<MixShapeField>,
    /// Serves the mix with this mixer alone, with no fallback.
    pub(crate) mixer: Option<String>,
    /// Mixer options keyed `<mixer_id>.<name>`, each a value of the option's declared type.
    /// Each mixer receives only its own declared options.
    #[serde(default)]
    pub(crate) options: HashMap<String, serde_json::Value>,
    /// Leads a blend with its single track seed. When the seed is in a chain, only the chain's
    /// later tracks in the mix follow it; its earlier ones are left out.
    #[serde(default)]
    pub(crate) pin_seed: bool,
    /// For a blend, the most tracks returned (default 200). For a path, a hint for how many
    /// steps to take; a path is cut short of its end only past 2000 tracks.
    pub(crate) limit: Option<usize>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "docgen", schemars(rename = "MixSeed"))]
#[derive(Deserialize)]
#[serde(untagged)]
pub(crate) enum MixSeedField {
    Id(String),
    Entry(MixSeedEntryField),
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "docgen", schemars(rename = "MixSeedEntry"))]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MixSeedEntryField {
    pub(crate) id: String,
    /// Mixes away from the seed instead of toward it (default false).
    #[serde(default)]
    pub(crate) exclude: bool,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "docgen", schemars(rename = "MixShape"))]
#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum MixShapeField {
    /// Tracks like the seeds, or like what `q` describes.
    Blend,
    /// Tracks leading from the first of two seeds to the second, in play order.
    Path,
}

impl MixRequestFields {
    /// The request, a blend unless it names a shape.
    pub(crate) fn into_request(self) -> MixRequest {
        MixRequest {
            seeds: self
                .seeds
                .into_iter()
                .map(|seed| match seed {
                    MixSeedField::Id(id) => MixSeedRequest::new(id),
                    MixSeedField::Entry(seed) => MixSeedRequest {
                        id: seed.id,
                        exclude: seed.exclude,
                    },
                })
                .collect(),
            from_history: self.from_history,
            query: self.q,
            shape: match self.shape {
                None | Some(MixShapeField::Blend) => MixShape::Blend,
                Some(MixShapeField::Path) => MixShape::Path,
            },
            mixer: self.mixer,
            pin_seed: self.pin_seed,
            limit: self.limit,
            options: self.options,
        }
    }
}

/// A seed by public id. An excluded seed is one to mix away from.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct MixSeedRequest {
    pub(crate) id: String,
    pub(crate) exclude: bool,
}

impl MixSeedRequest {
    pub(crate) fn new(id: String) -> Self {
        Self { id, exclude: false }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct MixRequest {
    pub(crate) seeds: Vec<MixSeedRequest>,
    /// Adds the viewer's recent listens as track seeds.
    pub(crate) from_history: bool,
    pub(crate) query: Option<String>,
    pub(crate) shape: MixShape,
    /// Serves the mix with this mixer alone.
    pub(crate) mixer: Option<String>,
    /// Puts the single track seed first; only its chain's later tracks in the mix follow it.
    pub(crate) pin_seed: bool,
    /// For blends, the most tracks returned; for paths, a step hint for the mixer.
    pub(crate) limit: Option<usize>,
    /// Mixer options keyed `<mixer_id>.<name>`.
    pub(crate) options: HashMap<String, serde_json::Value>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum MixError {
    /// A seed that is missing, not a seed type, or hidden from the viewer.
    #[error("seed not found: {0}")]
    SeedNotFound(String),
    #[error("{0}")]
    Invalid(String),
    /// No mixer can serve the request.
    #[error("{0}")]
    Unsupported(String),
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

fn invalid(message: impl Into<String>) -> MixError {
    MixError::Invalid(message.into())
}

const PIN_SEED_NEEDS: &str =
    "pin_seed needs exactly one seed, a track that isn't excluded, without from_history";

impl MixRequest {
    pub(crate) fn query(&self) -> Option<&str> {
        self.query
            .as_deref()
            .map(str::trim)
            .filter(|query| !query.is_empty())
    }

    pub(super) fn validate(&self) -> Result<(), MixError> {
        if self.seeds.iter().all(|seed| seed.exclude)
            && !self.from_history
            && self.query().is_none()
        {
            return Err(invalid(
                "a mix needs a seed that isn't excluded, from_history, or q",
            ));
        }
        if self.seeds.len() > MAX_SEEDS {
            return Err(invalid(format!(
                "a mix takes at most {MAX_SEEDS} seeds, got {}",
                self.seeds.len()
            )));
        }
        let mut seen = HashSet::new();
        for seed in &self.seeds {
            if !seen.insert(seed.id.as_str()) {
                return Err(invalid(format!("seed {} is given twice", seed.id)));
            }
        }
        if let Some(query) = self.query()
            && query.chars().count() > MAX_QUERY_LEN
        {
            return Err(invalid(format!(
                "q must be at most {MAX_QUERY_LEN} characters"
            )));
        }
        match self.limit {
            Some(0) => return Err(invalid("limit must be > 0")),
            Some(limit) if limit > MAX_LIMIT => {
                return Err(invalid(format!(
                    "limit must be <= {MAX_LIMIT}, got {limit}"
                )));
            }
            _ => {}
        }
        check_passed_options(&self.options).map_err(MixError::Invalid)?;
        match self.shape {
            MixShape::Path => {
                if self.seeds.len() != 2 {
                    return Err(invalid(format!(
                        "a path takes exactly two seeds, its start and end, got {}",
                        self.seeds.len()
                    )));
                }
                if self.from_history {
                    return Err(invalid("a path can't take from_history"));
                }
                if self.seeds.iter().any(|seed| seed.exclude) {
                    return Err(invalid("a path can't exclude seeds"));
                }
                if self.pin_seed {
                    return Err(invalid("a path can't pin a seed"));
                }
            }
            MixShape::Blend => {
                if self.pin_seed
                    && (self.seeds.len() != 1 || self.seeds[0].exclude || self.from_history)
                {
                    return Err(invalid(PIN_SEED_NEEDS));
                }
            }
        }
        Ok(())
    }
}

/// A seed as mixers receive it, resolved under one guard; its `DbId` is valid only under that
/// guard, so handlers get its public `id`.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct MixSeed {
    pub(crate) db_id: DbId,
    pub(crate) id: String,
    pub(crate) seed_type: MixSeedType,
    /// Mixes away from the seed instead of toward it.
    pub(crate) exclude: bool,
}

/// The request's seeds resolved and checked under one guard.
pub(super) struct ResolvedSeeds {
    pub(super) explicit: Vec<MixSeed>,
    /// The viewer's recent listens, most recent first, without the explicit track seeds.
    history: Vec<MixSeed>,
}

impl ResolvedSeeds {
    pub(super) fn resolve(
        db: &DbAny,
        request: &MixRequest,
        viewer: &MixViewer,
    ) -> Result<Self, MixError> {
        let explicit = request
            .seeds
            .iter()
            .map(|seed| {
                resolve_visible(db, seed, viewer)?
                    .ok_or_else(|| MixError::SeedNotFound(seed.id.clone()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        if request.pin_seed && explicit[0].seed_type != MixSeedType::Track {
            return Err(invalid(PIN_SEED_NEEDS));
        }
        let mut history = if request.from_history {
            recent_listen_tracks(
                db,
                viewer.require(db)?,
                viewer.accessible_library_ids.as_ref(),
            )?
        } else {
            Vec::new()
        };
        history.retain(|(track, _)| {
            !explicit
                .iter()
                .any(|seed| seed.seed_type == MixSeedType::Track && seed.db_id == *track)
        });
        let history = history
            .into_iter()
            .map(|(db_id, id)| MixSeed {
                db_id,
                id,
                seed_type: MixSeedType::Track,
                exclude: false,
            })
            .collect();
        Ok(Self { explicit, history })
    }

    /// Fails unless every seed still resolves as it did, so a seed deleted, hidden, or whose
    /// `DbId` was recycled since is not mixed from.
    pub(super) fn verify(
        &self,
        db: &DbAny,
        request: &MixRequest,
        viewer: &MixViewer,
    ) -> Result<(), MixError> {
        for (seed, resolved) in request.seeds.iter().zip(&self.explicit) {
            if resolve_visible(db, seed, viewer)?.as_ref() != Some(resolved) {
                return Err(MixError::SeedNotFound(seed.id.clone()));
            }
        }
        Ok(())
    }

    pub(super) fn is_empty(&self) -> bool {
        self.explicit.is_empty() && self.history.is_empty()
    }

    /// The seeds a handler taking `max_seeds` receives: the explicit ones, then the most recent
    /// history tracks that fit.
    pub(super) fn for_handler(&self, max_seeds: usize) -> Vec<MixSeed> {
        let room = max_seeds.saturating_sub(self.explicit.len());
        self.explicit
            .iter()
            .chain(self.history.iter().take(room))
            .cloned()
            .collect()
    }
}

/// What a handler needs to take to serve `request`, whose explicit seeds resolved to `explicit`.
/// History only asks for track seeds, so whether a mixer serves a request doesn't hang on how
/// many listens the viewer has.
pub(super) fn requirements(request: &MixRequest, explicit: &[MixSeed]) -> MixRequirements {
    let mut seed_types = Vec::new();
    let history = request.from_history.then_some(MixSeedType::Track);
    for seed_type in explicit.iter().map(|seed| seed.seed_type).chain(history) {
        if !seed_types.contains(&seed_type) {
            seed_types.push(seed_type);
        }
    }
    MixRequirements {
        seed_types,
        seed_count: explicit.len(),
        exclude: explicit.iter().any(|seed| seed.exclude),
        query: request.query().is_some(),
    }
}

fn resolve_visible(
    db: &DbAny,
    seed: &MixSeedRequest,
    viewer: &MixViewer,
) -> anyhow::Result<Option<MixSeed>> {
    let Some(db_id) = lyra_db::lookup::find_node_id_by_id(db, &seed.id)? else {
        return Ok(None);
    };
    let principal = viewer.principal()?;
    let seed_type = if lyra_db::tracks::get_by_id(db, db_id)?.is_some() {
        access::entity_accessible(db, principal, db_id)?.then_some(MixSeedType::Track)
    } else if lyra_db::releases::get_by_id(db, db_id)?.is_some() {
        access::entity_accessible(db, principal, db_id)?.then_some(MixSeedType::Release)
    } else if lyra_db::artists::get_by_id(db, db_id)?.is_some() {
        access::entity_accessible(db, principal, db_id)?.then_some(MixSeedType::Artist)
    } else if lyra_db::genres::get_by_id(db, db_id)?.is_some() {
        Some(MixSeedType::Genre)
    } else if lyra_db::playlists::get_by_id(db, db_id)?.is_some() {
        let user_db_id = principal.require(db)?;
        viewer
            .playlists
            .sees(db, principal, user_db_id, db_id)?
            .then_some(MixSeedType::Playlist)
    } else {
        None
    };
    Ok(seed_type.map(|seed_type| MixSeed {
        db_id,
        id: seed.id.clone(),
        seed_type,
        exclude: seed.exclude,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seed(id: &str) -> MixSeedRequest {
        MixSeedRequest::new(id.to_string())
    }

    fn excluded(id: &str) -> MixSeedRequest {
        MixSeedRequest {
            id: id.to_string(),
            exclude: true,
        }
    }

    fn rejected(request: MixRequest) -> String {
        match request.validate() {
            Err(MixError::Invalid(message)) => message,
            other => panic!("expected an invalid request, got {other:?}"),
        }
    }

    #[test]
    fn requests_need_something_to_mix_from() {
        assert!(rejected(MixRequest::default()).contains("needs a seed"));
        assert!(
            rejected(MixRequest {
                query: Some("  ".to_string()),
                ..MixRequest::default()
            })
            .contains("needs a seed")
        );
        assert!(
            rejected(MixRequest {
                seeds: vec![excluded("a")],
                ..MixRequest::default()
            })
            .contains("needs a seed that isn't excluded")
        );
        assert!(
            MixRequest {
                seeds: vec![excluded("a")],
                query: Some("rain".to_string()),
                ..MixRequest::default()
            }
            .validate()
            .is_ok()
        );
        assert!(
            MixRequest {
                from_history: true,
                ..MixRequest::default()
            }
            .validate()
            .is_ok()
        );
    }

    #[test]
    fn seeds_need_distinct_ids() {
        let request = |seeds| MixRequest {
            seeds,
            ..MixRequest::default()
        };
        assert!(rejected(request(vec![seed("a"), excluded("a")])).contains("twice"));
        assert!(
            rejected(request(
                (0..=MAX_SEEDS).map(|n| seed(&n.to_string())).collect()
            ))
            .contains("at most")
        );
    }

    #[test]
    fn paths_take_an_included_start_and_end() {
        let path = |seeds, from_history, pin_seed| MixRequest {
            seeds,
            from_history,
            pin_seed,
            shape: MixShape::Path,
            ..MixRequest::default()
        };
        assert!(rejected(path(vec![seed("a")], false, false)).contains("exactly two"));
        assert!(rejected(path(vec![seed("a"), seed("b")], true, false)).contains("from_history"));
        assert!(rejected(path(vec![seed("a"), excluded("b")], false, false)).contains("exclude"));
        assert!(rejected(path(vec![seed("a"), seed("b")], false, true)).contains("pin"));
        assert!(
            path(vec![seed("a"), seed("b")], false, false)
                .validate()
                .is_ok()
        );
    }

    #[test]
    fn pinning_takes_a_single_seed_without_history() {
        let pinned = |seeds, from_history| MixRequest {
            seeds,
            from_history,
            pin_seed: true,
            ..MixRequest::default()
        };
        assert!(rejected(pinned(vec![seed("a"), seed("b")], false)).contains("pin_seed"));
        assert!(rejected(pinned(vec![seed("a")], true)).contains("pin_seed"));
        assert!(
            rejected(MixRequest {
                query: Some("rain".to_string()),
                ..pinned(vec![excluded("a")], false)
            })
            .contains("pin_seed")
        );
        assert!(pinned(vec![seed("a")], false).validate().is_ok());
    }

    #[test]
    fn limits_and_options_are_bounded() {
        let request = |limit, options: Vec<(&str, serde_json::Value)>| MixRequest {
            seeds: vec![seed("a")],
            limit,
            options: options
                .into_iter()
                .map(|(key, value)| (key.to_string(), value))
                .collect(),
            ..MixRequest::default()
        };
        assert!(rejected(request(Some(0), Vec::new())).contains("limit must be > 0"));
        assert!(rejected(request(Some(MAX_LIMIT + 1), Vec::new())).contains("limit must be <="));
        assert!(
            rejected(request(None, vec![("a.b", serde_json::json!([1]))])).contains("option 'a.b'")
        );
        assert!(
            request(Some(MAX_LIMIT), vec![("a.b", serde_json::json!(1))])
                .validate()
                .is_ok()
        );
    }

    #[test]
    fn history_fills_what_a_handler_takes_beyond_the_explicit_seeds() {
        let seed = |id, seed_type| MixSeed {
            db_id: DbId(id),
            id: id.to_string(),
            seed_type,
            exclude: false,
        };
        let seeds = ResolvedSeeds {
            explicit: vec![seed(1, MixSeedType::Release)],
            history: vec![
                seed(5, MixSeedType::Track),
                seed(4, MixSeedType::Track),
                seed(3, MixSeedType::Track),
            ],
        };
        let ids = |max_seeds| {
            seeds
                .for_handler(max_seeds)
                .into_iter()
                .map(|seed| seed.db_id.0)
                .collect::<Vec<_>>()
        };
        assert_eq!(ids(1), [1]);
        assert_eq!(ids(3), [1, 5, 4]);
        assert_eq!(ids(10), [1, 5, 4, 3]);

        let request = MixRequest {
            from_history: true,
            ..MixRequest::default()
        };
        assert_eq!(
            requirements(&request, &seeds.explicit),
            MixRequirements {
                seed_types: vec![MixSeedType::Release, MixSeedType::Track],
                seed_count: 1,
                exclude: false,
                query: false,
            }
        );
    }
}
