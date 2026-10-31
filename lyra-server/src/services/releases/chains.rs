// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Release chains as clients name them: their tracks by public id.

use std::collections::HashMap;

use agdb::{
    DbAny,
    DbId,
};
use lyra_db::chains::{
    ChainError,
    ReleaseChain,
};
use serde::Serialize;

use crate::services::auth::{
    Principal,
    access,
};

/// A release's chain: the public ids of its tracks in play order.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct PublicChain {
    pub(crate) id: String,
    pub(crate) track_ids: Vec<String>,
}

/// `chains` with their tracks' public ids.
pub(crate) fn public_chains(
    db: &DbAny,
    chains: Vec<ReleaseChain>,
) -> anyhow::Result<Vec<PublicChain>> {
    let track_db_ids = chains
        .iter()
        .flat_map(|chain| chain.track_db_ids.iter().copied())
        .collect::<Vec<_>>();
    let public_ids = lyra_db::lookup::find_ids_by_db_ids(db, &track_db_ids)?;
    Ok(chains
        .into_iter()
        .map(|chain| PublicChain {
            id: chain.id,
            track_ids: chain
                .track_db_ids
                .iter()
                .filter_map(|track| public_ids.get(track).cloned())
                .collect(),
        })
        .collect())
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ReplaceChainsError {
    #[error("release not found")]
    ReleaseNotFound,
    #[error("a chain needs at least two tracks")]
    TooShort,
    #[error("track {0} is listed more than once")]
    DuplicateTrack(String),
    #[error("track {0} is not on the release")]
    TrackNotOnRelease(String),
    #[error("track {0} is already in a chain on another release")]
    ChainedElsewhere(String),
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

/// Replaces every chain on the release with `chains`, each its tracks' public ids in play order,
/// and returns the stored chains. A track `principal` cannot see is treated as one that does not
/// exist, so the error names neither apart.
pub(crate) fn replace(
    db: &mut DbAny,
    principal: &Principal,
    release_db_id: DbId,
    chains: &[Vec<String>],
) -> Result<Vec<PublicChain>, ReplaceChainsError> {
    let mut track_ids = HashMap::new();
    let mut resolved = Vec::with_capacity(chains.len());
    for chain in chains {
        let mut track_db_ids = Vec::with_capacity(chain.len());
        for track_id in chain {
            let not_on_release = || ReplaceChainsError::TrackNotOnRelease(track_id.clone());
            let track_db_id =
                lyra_db::lookup::find_node_id_by_id(&*db, track_id)?.ok_or_else(not_on_release)?;
            if !access::entity_accessible(&*db, principal, track_db_id)? {
                return Err(not_on_release());
            }
            track_ids.insert(track_db_id, track_id.as_str());
            track_db_ids.push(track_db_id);
        }
        resolved.push(track_db_ids);
    }

    let track_id = |track_db_id: DbId| {
        track_ids
            .get(&track_db_id)
            .copied()
            .unwrap_or_default()
            .to_string()
    };
    let stored =
        lyra_db::chains::replace_for_release(db, release_db_id, &resolved)?.map_err(|error| {
            match error {
                ChainError::ReleaseNotFound => ReplaceChainsError::ReleaseNotFound,
                ChainError::TooShort => ReplaceChainsError::TooShort,
                ChainError::DuplicateTrack(track) => {
                    ReplaceChainsError::DuplicateTrack(track_id(track))
                }
                ChainError::TrackNotOnRelease(track) => {
                    ReplaceChainsError::TrackNotOnRelease(track_id(track))
                }
                ChainError::ChainedElsewhere(track) => {
                    ReplaceChainsError::ChainedElsewhere(track_id(track))
                }
            }
        })?;
    Ok(public_chains(db, stored)?)
}
