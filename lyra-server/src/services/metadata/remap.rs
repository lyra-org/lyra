// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Remapping a missing track onto the track whose file it now is: the missing track keeps its
//! identity and what users attached to it, and takes over the other's file and place.

use std::collections::HashSet;

use agdb::{
    DbAny,
    DbAnyTransactionMut,
    DbId,
};

use super::layers::MANUAL_SOURCE_ID;
use crate::services::{
    STATE,
    auth::Principal,
    libraries::{
        control,
        library_has_active_run,
        library_mutation_guard,
    },
};
use lyra_db::graph::RetargetLookup;

#[derive(Debug, thiserror::Error)]
pub(crate) enum RemapError {
    #[error("track not found: {0}")]
    NotFound(String),
    #[error("{0}")]
    Forbidden(String),
    #[error("{0}")]
    Conflict(String),
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

/// Remaps the unavailable track `missing_id` onto the available track `target_id`: the missing
/// track takes over the target's file, its place on its release and in its chain, and its tags'
/// metadata, keeping its own manual edits and filling them with the target's; favorites,
/// ratings, tags, listens, bookmarks, playlist entries, queues, lyrics and covers of the target
/// move to it, a user's own kept where both have one. The target is then removed. Both tracks'
/// libraries must be ones `principal` manages and runs none.
pub(crate) async fn remap(
    principal: &Principal,
    missing_id: &str,
    target_id: &str,
) -> Result<(), RemapError> {
    let _guard = library_mutation_guard().await;
    let libraries = {
        let db = STATE.db.read().await;
        let mut libraries = HashSet::new();
        for id in [missing_id, target_id] {
            libraries.extend(libraries_of(&db, principal, id)?);
        }
        libraries
    };
    for library in &libraries {
        if library_has_active_run(library).await? {
            return Err(RemapError::Conflict(
                "a sync, refresh or purge of the tracks' libraries is running".to_string(),
            ));
        }
    }

    let orphaned = {
        let mut db = STATE.db.write().await;
        let missing = track_db_id(&db, missing_id)?;
        let target = track_db_id(&db, target_id)?;
        if missing == target {
            return Err(RemapError::Conflict(
                "a track can't be remapped onto itself".to_string(),
            ));
        }
        if lyra_db::track_sources::track_available(&*db, missing)? {
            return Err(RemapError::Conflict(format!(
                "track {missing_id} can still play"
            )));
        }
        if !lyra_db::track_sources::track_available(&*db, target)? {
            return Err(RemapError::Conflict(format!(
                "track {target_id} can't play"
            )));
        }
        db.transaction_mut(|t| take_over(t, missing, target, missing_id, target_id))?
    };
    crate::services::covers::discard_unreferenced_cover_files(orphaned);
    Ok(())
}

/// The public ids of the libraries holding the track `id`, each one `principal` manages.
fn libraries_of(db: &DbAny, principal: &Principal, id: &str) -> Result<Vec<String>, RemapError> {
    let track = track_db_id(db, id)?;
    crate::services::auth::access::require_entity_accessible(db, principal, track, || {
        RemapError::NotFound(id.to_string())
    })?;
    let mut libraries = Vec::new();
    for release in lyra_db::graph::inbound_neighbor_ids(db, track, "Release")? {
        for library in lyra_db::libraries::get_by_release(db, release)? {
            control::managed_library(db, principal, &library.id).map_err(|err| match err {
                control::LibraryControlError::Internal(err) => RemapError::Internal(err),
                other => RemapError::Forbidden(other.to_string()),
            })?;
            libraries.push(library.id);
        }
    }
    Ok(libraries)
}

fn track_db_id(db: &DbAny, id: &str) -> Result<DbId, RemapError> {
    lyra_db::lookup::find_node_id_by_id(db, id)?
        .filter(|track| lyra_db::graph::is_kind(db, *track, "Track").unwrap_or(false))
        .ok_or_else(|| RemapError::NotFound(id.to_string()))
}

/// `missing` takes `target`'s place and file, and `target` goes. Returns the covers no remaining
/// owner shares.
fn take_over(
    t: &mut DbAnyTransactionMut<'_>,
    missing: DbId,
    target: DbId,
    missing_id: &str,
    target_id: &str,
) -> anyhow::Result<Vec<lyra_db::Cover>> {
    use lyra_db::graph::{
        inbound_neighbor_ids,
        neighbor_ids,
        remove_edges_between,
        retarget_inbound,
    };

    // The file: the missing track's sources go, the target's become its own.
    let lost = neighbor_ids(t, missing, "TrackSource")?;
    if !lost.is_empty() {
        t.exec_mut(agdb::QueryBuilder::remove().ids(&lost).query())?;
    }
    for source in neighbor_ids(t, target, "TrackSource")? {
        remove_edges_between(t, target, source)?;
        t.exec_mut(
            agdb::QueryBuilder::insert()
                .edges()
                .from(missing)
                .to(source)
                .query(),
        )?;
    }

    // The place: its chain slot and release, leaving its old chain slot and release.
    lyra_db::chains::hand_over(t, target, missing)?;
    let old_releases = inbound_neighbor_ids(t, missing, "Release")?;
    let new_releases = inbound_neighbor_ids(t, target, "Release")?;
    for &release in old_releases.iter().filter(|r| !new_releases.contains(r)) {
        lyra_db::releases::unlink_track(t, release, missing)?;
    }
    for &release in &new_releases {
        lyra_db::releases::link_track(t, release, missing)?;
    }

    // The metadata: tags follow the file, manual edits the identity.
    for layer in lyra_db::metadata::layers::get_for_entity(t, missing)? {
        if layer.source_id != MANUAL_SOURCE_ID {
            lyra_db::metadata::layers::remove_inside_tx(t, missing, &layer.source_id)?;
        }
    }
    super::cleanup::migrate_metadata(t, missing, target)?;

    // What users attached to the target.
    lyra_db::favorites::retarget(t, target, missing, RetargetLookup::Target)?;
    lyra_db::ratings::retarget(t, target, missing, RetargetLookup::Target)?;
    lyra_db::tags::retarget(t, target, missing, RetargetLookup::Target)?;
    lyra_db::bookmarks::retarget(t, target, missing)?;
    lyra_db::lyrics::retarget_missing(t, target, missing)?;
    for kind in ["Listen", "Playlist", "PlaybackSession"] {
        retarget_inbound(t, kind, target, missing)?;
    }
    lyra_db::playbacks::retarget_queue_track(t, target_id, missing_id)?;
    lyra_db::covers::adopt_missing(t, missing, target)?;

    let mut orphaned = lyra_db::metadata::cascade_remove_entities_in_txn(t, &[target])?;
    super::merging::apply_merged_metadata_to_entity_inside_tx(t, missing)?;
    for &release in old_releases.iter().filter(|r| !new_releases.contains(r)) {
        if neighbor_ids(t, release, "Track")?.is_empty() {
            orphaned.extend(lyra_db::metadata::cascade_remove_entities_in_txn(
                t,
                &[release],
            )?);
        }
    }
    Ok(orphaned)
}
