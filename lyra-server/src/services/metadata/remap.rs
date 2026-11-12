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

    {
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
        db.transaction_mut(|t| take_over(t, missing, target, missing_id, target_id))?;
        crate::services::covers::discard_listed_cover_files(&*db);
    }
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

/// `missing` takes `target`'s place and file, and `target` goes.
fn take_over(
    t: &mut DbAnyTransactionMut<'_>,
    missing: DbId,
    target: DbId,
    missing_id: &str,
    target_id: &str,
) -> anyhow::Result<()> {
    use lyra_db::graph::{
        inbound_neighbor_ids,
        neighbor_ids,
        remove_edges_between,
        retarget_inbound,
    };

    // The file: the missing track leaves its sources, the target's become its own.
    let lost = neighbor_ids(t, missing, "TrackSource")?;
    for &source in &lost {
        remove_edges_between(t, missing, source)?;
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
    let mut left = lyra_db::orphans::Candidates {
        track_sources: lost,
        ..Default::default()
    };
    for &release in old_releases.iter().filter(|r| !new_releases.contains(r)) {
        left.extend(lyra_db::releases::unlink_track(t, release, missing)?);
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
    for external_id in lyra_db::external_ids::get_for_entity_inside_tx(t, target)? {
        lyra_db::external_ids::merge_inside_tx(t, missing, &external_id)?;
    }

    // What users attached to the target.
    lyra_db::favorites::retarget(t, target, missing, RetargetLookup::Target)?;
    lyra_db::ratings::retarget(t, target, missing, RetargetLookup::Target)?;
    lyra_db::tags::retarget(t, target, missing, RetargetLookup::Target)?;
    lyra_db::bookmarks::retarget(t, target, missing)?;
    lyra_db::listens::retarget(t, target, missing)?;
    lyra_db::lyrics::retarget_missing(t, target, missing)?;
    for kind in ["Listen", "Playlist", "PlaybackSession"] {
        retarget_inbound(t, kind, target, missing)?;
    }
    lyra_db::playbacks::retarget_queue_track(t, target_id, missing_id)?;
    lyra_db::covers::adopt_missing(t, missing, target)?;

    lyra_db::metadata::cascade_remove_entities_in_txn(t, &[target])?;
    super::merging::apply_merged_metadata_to_entity_inside_tx(t, missing)?;
    lyra_db::orphans::collect(t, left)
}

#[cfg(test)]
mod tests {
    use lyra_db::fixtures::{
        insert_listen,
        insert_track,
    };
    use lyra_db::test_db::{
        insert_user,
        new_test_db,
    };

    use super::*;

    #[test]
    fn listens_of_the_target_count_for_the_track_that_took_over() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let missing = insert_track(&mut db, "Missing")?;
        let target = insert_track(&mut db, "Target")?;
        let both = insert_user(&mut db, "both")?;
        let target_only = insert_user(&mut db, "target-only")?;
        let missing_only = insert_user(&mut db, "missing-only")?;
        insert_listen(&mut db, both, missing, 5_000)?;
        insert_listen(&mut db, both, target, 9_000)?;
        insert_listen(&mut db, both, target, 2_000)?;
        insert_listen(&mut db, target_only, target, 0)?;
        insert_listen(&mut db, missing_only, missing, 7_000)?;
        let id = |db: &DbAny, track| {
            lyra_db::lookup::find_id_by_db_id(db, track).map(|id| id.expect("public id"))
        };
        let (missing_id, target_id) = (id(&db, missing)?, id(&db, target)?);

        db.transaction_mut(|t| take_over(t, missing, target, &missing_id, &target_id))?;

        for (user, count, last_played_at_ms) in [
            (both, 3, 9_000),
            (target_only, 1, 0),
            (missing_only, 1, 7_000),
        ] {
            assert_eq!(
                lyra_db::listens::total(&db, user, missing)?,
                Some(lyra_db::listens::ListenTotal {
                    count,
                    last_played_at_ms,
                })
            );
            assert_eq!(
                lyra_db::listens::totals(&db, user)?.len(),
                1,
                "the removed track keeps no total"
            );
        }
        Ok(())
    }

    #[test]
    fn taking_over_collects_the_file_and_release_the_missing_track_left() -> anyhow::Result<()> {
        use lyra_db::fixtures::{
            insert_file_entry,
            insert_release,
            link_track,
        };
        use lyra_db::orphans::testing::{
            assert_collected,
            assert_drained,
            drain_artists,
        };

        let mut db = new_test_db()?;
        let library = lyra_db::test_db::insert_library(&mut db, "Music", "/music")?;
        let placed = |db: &mut DbAny, name: &str| -> anyhow::Result<(DbId, DbId, DbId)> {
            let release = insert_release(db, name)?;
            let track = insert_track(db, name)?;
            link_track(db, release, track)?;
            let entry = insert_file_entry(db, library, name)?;
            let source = db.transaction_mut(|t| {
                lyra_db::track_sources::upsert(
                    t,
                    track,
                    entry,
                    lyra_db::track_sources::TrackSourceUpsert {
                        source_kind: "embedded_tags".to_string(),
                        source_key: name.to_string(),
                        is_primary: true,
                        start_ms: None,
                        end_ms: None,
                        format: Default::default(),
                    },
                    None,
                )
            })?;
            Ok((release, track, source))
        };
        let (old_release, missing, lost_source) = placed(&mut db, "gone.flac")?;
        let (new_release, target, kept_source) = placed(&mut db, "here.flac")?;
        let artist = lyra_db::artists::create(&mut db, "Target Artist", None)?;
        lyra_db::test_db::connect_artist(&mut db, target, artist)?;
        drain_artists(&mut db)?;
        let id = |db: &DbAny, track| {
            lyra_db::lookup::find_id_by_db_id(db, track).map(|id| id.expect("public id"))
        };
        let (missing_id, target_id) = (id(&db, missing)?, id(&db, target)?);

        db.transaction_mut(|t| take_over(t, missing, target, &missing_id, &target_id))?;

        assert!(lyra_db::tracks::get_by_id(&db, target)?.is_none());
        assert!(lyra_db::releases::get_by_id(&db, old_release)?.is_none());
        assert!(lyra_db::track_sources::get_by_id(&db, lost_source)?.is_none());
        assert_eq!(
            lyra_db::graph::neighbor_ids(&db, missing, "TrackSource")?,
            vec![kept_source]
        );
        assert_eq!(
            lyra_db::graph::inbound_neighbor_ids(&db, missing, "Release")?,
            vec![new_release]
        );
        assert!(lyra_db::artists::get_by_id(&db, artist)?.is_some());
        assert_collected(&db)?;

        assert_eq!(drain_artists(&mut db)?, 1);
        assert_drained(&db)
    }

    #[test]
    fn a_play_counted_for_the_target_is_a_duplicate_for_the_track_that_took_over()
    -> anyhow::Result<()> {
        use crate::services::listens::{
            Play,
            RecordOutcome,
            record,
        };

        const NOW_MS: u64 = 1_800_000_000_000;
        let mut db = new_test_db()?;
        let missing = insert_track(&mut db, "Missing")?;
        let target = insert_track(&mut db, "Target")?;
        let user = insert_user(&mut db, "listener")?;
        let play = |track_db_id, played_ms| Play {
            play_id: "play-remapped-0001",
            track_db_id,
            started_at_ms: NOW_MS - 600_000,
            played_ms,
        };
        let recorded = db.transaction_mut(|t| record(t, user, play(target, 40_000), NOW_MS))?;
        assert!(matches!(recorded, RecordOutcome::Recorded(_)));
        let id = |db: &DbAny, track| {
            lyra_db::lookup::find_id_by_db_id(db, track).map(|id| id.expect("public id"))
        };
        let (missing_id, target_id) = (id(&db, missing)?, id(&db, target)?);
        db.transaction_mut(|t| take_over(t, missing, target, &missing_id, &target_id))?;

        // The player reports the same play again, now under the track that took over.
        let again = db.transaction_mut(|t| record(t, user, play(missing, 90_000), NOW_MS))?;

        assert_eq!(again, RecordOutcome::Duplicate);
        assert_eq!(
            lyra_db::listens::total(&db, user, missing)?.map(|total| total.count),
            Some(1)
        );
        assert_eq!(
            lyra_db::listens::get_by_play_id(&db, user, "play-remapped-0001")?
                .and_then(|listen| listen.played_ms),
            Some(90_000),
            "the play still raises its listen's playing time"
        );
        Ok(())
    }
}
