// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use super::EntityType;

/// Enabled providers in position order, narrowed to `provider_filter` when set.
pub(crate) fn active_providers(
    db: &impl lyra_db::DbAccess,
    provider_filter: Option<&str>,
) -> anyhow::Result<Vec<lyra_db::ProviderConfig>> {
    let provider_filter = provider_filter
        .map(str::trim)
        .filter(|value| !value.is_empty());
    Ok(lyra_db::providers::get(db)?
        .into_iter()
        .filter(|provider| provider.enabled)
        .filter(|provider| provider_filter.is_none_or(|id| provider.provider_id == id))
        .collect())
}

mod admin;
mod dedup;
mod links;
mod refresh;
mod registry;
mod requirements;
mod schemes;
mod sync;

mod capture {
    use agdb::DbId;

    pub(crate) async fn run_capture(
        _library_db_id: DbId,
        _output_path: &str,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}

pub(crate) use admin::{
    EntityExternalIdRecord,
    ProviderAdminError,
    ProviderUpdate,
    SetEntityExternalIdRequest,
    inherit_entity_external_id,
    list_entity_external_ids,
    list_provider_configs,
    refresh_entity_by_id,
    set_entity_external_id,
    set_entity_locked,
    update_provider,
};
pub(crate) use capture::run_capture;
pub(crate) use links::{
    ID_LINK_BATCH_TIMEOUT,
    ID_LINK_CALL_BUDGET,
    IdLink,
    IdLinkCall,
    IdLinkCallError,
    IdLinkCallResult,
    IdLinkGenerators,
    IdLinkLocale,
    IdLinkTarget,
    resolve_id_links,
    validate_id_link_template,
};
pub(crate) use refresh::{
    LibraryRefreshOptions,
    refresh_entity_metadata,
    refresh_library_metadata_with_progress,
    refresh_release_metadata_for_scan_with_progress,
    release_refresh_provider_ids,
};
#[cfg(test)]
pub(crate) use registry::tests as registry_tests;
pub(crate) use registry::{
    DEFAULT_SIMILAR_RELEASES_HANDLER_TIMEOUT,
    MAX_SIMILAR_RELEASES_HANDLER_TIMEOUT,
    ProviderCallStage,
    ProviderCallbackHandle,
    ProviderCoverSpec,
    ProviderIdSpec,
    ProviderIdUrlGenerator,
    ProviderRegistries,
    ProviderRequireSpec,
    ProviderSimilarReleasesSpec,
    id_schemes,
    library_refresh_locks,
    provider_registry,
    sync_locks,
    teardown_plugin_providers,
    unique_artist_id_pairs,
    with_provider_call,
};
pub(crate) use requirements::{
    requirements_match,
    requirements_match_with,
};
#[cfg(feature = "docgen")]
pub(crate) use schemes::IDENTIFIERS_DOC;
pub(crate) use schemes::{
    IdSchemes,
    IdentifiersByScheme,
    validate_id_scheme,
};
pub(crate) use sync::{
    run_provider_sync,
    run_provider_sync_loop,
};

#[derive(Debug, thiserror::Error)]
pub(crate) enum ProviderServiceError {
    #[error("Entity not found: {0}")]
    EntityNotFound(i64),
    #[error("Library not found: {0}")]
    LibraryNotFound(i64),
    #[error("Sync already running for provider '{0}'")]
    SyncAlreadyRunning(String),
    #[error("Refresh already running for library {0}")]
    RefreshAlreadyRunning(i64),
    #[error("No refresh handler for provider '{0}'")]
    NoRefreshHandler(String),
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

pub(crate) struct EntityRefreshResult {
    pub(crate) entity_type: EntityType,
    pub(crate) providers_called: Vec<String>,
}

#[derive(Clone)]
pub(crate) enum EntityRefreshMode {
    MetadataOnly,
    WithReleaseArtifacts {
        replace_cover: bool,
        force_refresh: bool,
        options: std::collections::HashMap<String, String>,
    },
}

#[cfg(test)]
mod tests {
    use super::dedup::deduplicate_releases_by_external_id;
    use super::refresh::resolve_library_id_for_entity;
    use agdb::{
        DbAny,
        DbId,
    };
    use lyra_db::fixtures::{
        connect,
        insert_artist,
        insert_release,
        insert_track,
    };
    use lyra_db::test_db::{
        insert_library,
        new_test_db,
    };
    use lyra_db::{
        IdSource,
        external_ids,
        releases,
        tracks,
    };
    use std::collections::HashSet;

    fn release_ids_for_library(db: &DbAny, library_db_id: DbId) -> anyhow::Result<Vec<DbId>> {
        Ok(releases::get(db, library_db_id)?
            .into_iter()
            .filter_map(|release| release.db_id.map(Into::into))
            .collect())
    }

    #[test]
    fn active_providers_are_enabled_and_in_position_order() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        for provider_id in ["first", "off", "second"] {
            lyra_db::providers::register(&mut db, provider_id)?;
        }
        lyra_db::providers::move_to(&mut db, "second", 0)?;
        lyra_db::providers::set_enabled(&mut db, "off", false)?;

        let ids = |filter| -> anyhow::Result<Vec<String>> {
            Ok(super::active_providers(&db, filter)?
                .into_iter()
                .map(|provider| provider.provider_id)
                .collect())
        };
        assert_eq!(ids(None)?, ["second", "first"]);
        assert_eq!(ids(Some(" first "))?, ["first"]);
        assert!(ids(Some("off"))?.is_empty());
        Ok(())
    }

    #[test]
    fn resolve_library_id_for_release_track_and_artist() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let library_db_id = insert_library(&mut db, "Music", "/music")?;
        let release_db_id = insert_release(&mut db, "Release")?;
        let track_db_id = insert_track(&mut db, "Track")?;
        let artist_db_id = insert_artist(&mut db, "Artist")?;

        connect(&mut db, library_db_id, release_db_id)?;
        connect(&mut db, release_db_id, track_db_id)?;
        connect(&mut db, track_db_id, artist_db_id)?;
        connect(&mut db, release_db_id, artist_db_id)?;

        assert_eq!(
            resolve_library_id_for_entity(&db, release_db_id)?,
            Some(library_db_id)
        );
        assert_eq!(
            resolve_library_id_for_entity(&db, track_db_id)?,
            Some(library_db_id)
        );
        assert_eq!(
            resolve_library_id_for_entity(&db, artist_db_id)?,
            Some(library_db_id)
        );

        Ok(())
    }

    #[test]
    fn resolve_library_id_for_unknown_entity_returns_error() -> anyhow::Result<()> {
        let db = new_test_db()?;
        assert!(resolve_library_id_for_entity(&db, DbId(999_999)).is_err());
        Ok(())
    }

    #[test]
    fn resolve_library_id_for_entity_without_library_returns_none() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_db_id = insert_release(&mut db, "Release")?;
        assert_eq!(resolve_library_id_for_entity(&db, release_db_id)?, None);
        Ok(())
    }

    #[test]
    fn resolve_library_id_is_stable_when_release_is_in_multiple_libraries() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let library_a = insert_library(&mut db, "A", "/a")?;
        let library_b = insert_library(&mut db, "B", "/b")?;
        let release_db_id = insert_release(&mut db, "Release")?;

        connect(&mut db, library_a, release_db_id)?;
        connect(&mut db, library_b, release_db_id)?;

        let resolved = resolve_library_id_for_entity(&db, release_db_id)?
            .ok_or_else(|| anyhow::anyhow!("expected resolved library id"))?;
        assert_eq!(resolved.0, library_a.0.min(library_b.0));

        Ok(())
    }

    #[test]
    fn deduplicate_releases_by_external_id_merges_within_library() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let library_db_id = insert_library(&mut db, "Music", "/music")?;
        let release_a = insert_release(&mut db, "Release A")?;
        let release_b = insert_release(&mut db, "Release B")?;
        let track_a = insert_track(&mut db, "Track A")?;
        let track_b = insert_track(&mut db, "Track B")?;

        connect(&mut db, library_db_id, release_a)?;
        connect(&mut db, library_db_id, release_b)?;
        connect(&mut db, release_a, track_a)?;
        connect(&mut db, release_b, track_b)?;

        external_ids::upsert(
            &mut db,
            release_a,
            "musicbrainz",
            "release_id",
            "same-release",
            IdSource::Resolved,
        )?;
        external_ids::upsert(
            &mut db,
            release_b,
            "musicbrainz",
            "release_id",
            "same-release",
            IdSource::Resolved,
        )?;

        let unique_pairs = HashSet::from([("musicbrainz".to_string(), "release_id".to_string())]);
        let merged = deduplicate_releases_by_external_id(
            &mut db,
            library_db_id,
            &unique_pairs,
            &HashSet::new(),
            None,
        )?;
        assert_eq!(merged, 1);

        let releases = release_ids_for_library(&db, library_db_id)?;
        assert_eq!(releases.len(), 1);
        let merged_tracks = tracks::get(&db, releases[0])?;
        assert_eq!(merged_tracks.len(), 2);

        Ok(())
    }

    #[test]
    fn merging_releases_moves_the_losers_chains_to_the_winner() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let library_db_id = insert_library(&mut db, "Music", "/music")?;
        let mut chains = Vec::new();
        for title in ["Urn", "Urn (Disc 2)"] {
            let release = insert_release(&mut db, title)?;
            connect(&mut db, library_db_id, release)?;
            let mut chain = Vec::new();
            for part in ["Part I", "Part II"] {
                let track = insert_track(&mut db, &format!("{title} {part}"))?;
                connect(&mut db, release, track)?;
                chain.push(track);
            }
            lyra_db::chains::replace_for_release(&mut db, release, std::slice::from_ref(&chain))??;
            external_ids::upsert(
                &mut db,
                release,
                "musicbrainz",
                "release_id",
                "same-release",
                IdSource::Resolved,
            )?;
            chains.push((release, chain));
        }

        let unique_pairs = HashSet::from([("musicbrainz".to_string(), "release_id".to_string())]);
        assert_eq!(
            deduplicate_releases_by_external_id(
                &mut db,
                library_db_id,
                &unique_pairs,
                &HashSet::new(),
                None,
            )?,
            1
        );

        let releases = release_ids_for_library(&db, library_db_id)?;
        assert_eq!(releases.len(), 1);
        let mut stored = lyra_db::chains::get_for_release(&db, releases[0])?
            .into_iter()
            .map(|chain| chain.track_db_ids)
            .collect::<Vec<_>>();
        stored.sort();
        let mut expected = chains
            .iter()
            .map(|(_, chain)| chain.clone())
            .collect::<Vec<_>>();
        expected.sort();
        assert_eq!(
            stored, expected,
            "both chains, members in order, on the winner"
        );
        assert_eq!(
            lyra_db::graph::neighbor_ids(&db, "chains", "Chain")?.len(),
            2,
            "no chain node is left behind"
        );
        Ok(())
    }

    #[test]
    fn deduplicate_releases_by_external_id_does_not_merge_across_libraries() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let library_a = insert_library(&mut db, "A", "/a")?;
        let library_b = insert_library(&mut db, "B", "/b")?;
        let release_a = insert_release(&mut db, "Release A")?;
        let release_b = insert_release(&mut db, "Release B")?;

        connect(&mut db, library_a, release_a)?;
        connect(&mut db, library_b, release_b)?;

        external_ids::upsert(
            &mut db,
            release_a,
            "musicbrainz",
            "release_id",
            "shared-release",
            IdSource::Resolved,
        )?;
        external_ids::upsert(
            &mut db,
            release_b,
            "musicbrainz",
            "release_id",
            "shared-release",
            IdSource::Resolved,
        )?;

        let unique_pairs = HashSet::from([("musicbrainz".to_string(), "release_id".to_string())]);
        assert_eq!(
            deduplicate_releases_by_external_id(
                &mut db,
                library_a,
                &unique_pairs,
                &HashSet::new(),
                None,
            )?,
            0
        );
        assert_eq!(
            deduplicate_releases_by_external_id(
                &mut db,
                library_b,
                &unique_pairs,
                &HashSet::new(),
                None,
            )?,
            0
        );

        assert_eq!(release_ids_for_library(&db, library_a)?.len(), 1);
        assert_eq!(release_ids_for_library(&db, library_b)?.len(), 1);

        Ok(())
    }

    #[test]
    fn deduplicate_releases_skips_merge_when_tracks_have_overlapping_unique_ids()
    -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let library_db_id = insert_library(&mut db, "Music", "/music")?;
        let release_a = insert_release(&mut db, "Full Release")?;
        let release_b = insert_release(&mut db, "Bonus Disc")?;
        let track_a1 = insert_track(&mut db, "Track 1")?;
        let track_a2 = insert_track(&mut db, "Track 2")?;
        let track_b1 = insert_track(&mut db, "Track 2 copy")?;

        connect(&mut db, library_db_id, release_a)?;
        connect(&mut db, library_db_id, release_b)?;
        connect(&mut db, release_a, track_a1)?;
        connect(&mut db, release_a, track_a2)?;
        connect(&mut db, release_b, track_b1)?;

        for release in [release_a, release_b] {
            external_ids::upsert(
                &mut db,
                release,
                "musicbrainz",
                "release_id",
                "same-release",
                IdSource::Resolved,
            )?;
        }

        external_ids::upsert(
            &mut db,
            track_a1,
            "musicbrainz",
            "recording_id",
            "rec-1",
            IdSource::Resolved,
        )?;
        external_ids::upsert(
            &mut db,
            track_a2,
            "musicbrainz",
            "recording_id",
            "rec-2",
            IdSource::Resolved,
        )?;
        external_ids::upsert(
            &mut db,
            track_b1,
            "musicbrainz",
            "recording_id",
            "rec-2",
            IdSource::Resolved,
        )?;

        let release_pairs = HashSet::from([("musicbrainz".to_string(), "release_id".to_string())]);
        let track_pairs = HashSet::from([("musicbrainz".to_string(), "recording_id".to_string())]);
        let merged = deduplicate_releases_by_external_id(
            &mut db,
            library_db_id,
            &release_pairs,
            &track_pairs,
            None,
        )?;
        assert_eq!(merged, 0);
        assert_eq!(release_ids_for_library(&db, library_db_id)?.len(), 2);

        Ok(())
    }

    #[test]
    fn deduplicate_releases_merges_when_tracks_have_no_overlapping_unique_ids() -> anyhow::Result<()>
    {
        let mut db = new_test_db()?;
        let library_db_id = insert_library(&mut db, "Music", "/music")?;
        let release_a = insert_release(&mut db, "Release A")?;
        let release_b = insert_release(&mut db, "Release B")?;
        let track_a = insert_track(&mut db, "Track A")?;
        let track_b = insert_track(&mut db, "Track B")?;

        connect(&mut db, library_db_id, release_a)?;
        connect(&mut db, library_db_id, release_b)?;
        connect(&mut db, release_a, track_a)?;
        connect(&mut db, release_b, track_b)?;

        for release in [release_a, release_b] {
            external_ids::upsert(
                &mut db,
                release,
                "musicbrainz",
                "release_id",
                "same-release",
                IdSource::Resolved,
            )?;
        }

        external_ids::upsert(
            &mut db,
            track_a,
            "musicbrainz",
            "recording_id",
            "rec-1",
            IdSource::Resolved,
        )?;
        external_ids::upsert(
            &mut db,
            track_b,
            "musicbrainz",
            "recording_id",
            "rec-2",
            IdSource::Resolved,
        )?;

        let release_pairs = HashSet::from([("musicbrainz".to_string(), "release_id".to_string())]);
        let track_pairs = HashSet::from([("musicbrainz".to_string(), "recording_id".to_string())]);
        let merged = deduplicate_releases_by_external_id(
            &mut db,
            library_db_id,
            &release_pairs,
            &track_pairs,
            None,
        )?;
        assert_eq!(merged, 1);
        assert_eq!(release_ids_for_library(&db, library_db_id)?.len(), 1);

        Ok(())
    }

    /// Both orders in which the two linking providers' groups can run.
    const LINK_ORDERS: [(&str, &str); 2] =
        [("provider-a", "provider-b"), ("provider-b", "provider-a")];

    /// Three releases in one library, the middle one sharing a release ID from
    /// each linking provider with one neighbour, plus the release IDs
    /// `other_ids` lists by index; each release gets a track per recording ID.
    /// MusicBrainz ranks above `provider-a`, which ranks above `provider-b`.
    fn insert_bridged_releases(
        db: &mut DbAny,
        library_db_id: DbId,
        (first, second): (&str, &str),
        recordings: [&[&str]; 3],
        other_ids: &[(usize, &str, &str)],
    ) -> anyhow::Result<Vec<DbId>> {
        for (position, provider_id) in ["musicbrainz", "provider-a", "provider-b"]
            .into_iter()
            .enumerate()
        {
            lyra_db::providers::upsert(
                db,
                &lyra_db::ProviderConfig {
                    db_id: None,
                    provider_id: provider_id.to_string(),
                    position: position as u32,
                    enabled: true,
                },
            )?;
        }
        let mut releases = Vec::new();
        for (title, recordings) in ["Left", "Bridge", "Right"].into_iter().zip(recordings) {
            let release = insert_release(db, title)?;
            connect(db, library_db_id, release)?;
            for &recording in recordings {
                let track = insert_track(db, recording)?;
                connect(db, release, track)?;
                external_ids::upsert(
                    db,
                    track,
                    "musicbrainz",
                    "recording_id",
                    recording,
                    IdSource::Resolved,
                )?;
            }
            releases.push(release);
        }
        let links = [
            (0, first, "left"),
            (1, first, "left"),
            (1, second, "right"),
            (2, second, "right"),
        ];
        for &(index, provider_id, id_value) in links.iter().chain(other_ids) {
            external_ids::upsert(
                db,
                releases[index],
                provider_id,
                "release_id",
                id_value,
                IdSource::Resolved,
            )?;
        }
        Ok(releases)
    }

    fn deduplicate_bridged_releases(db: &mut DbAny, library_db_id: DbId) -> anyhow::Result<u32> {
        let release_pairs = ["musicbrainz", "provider-a", "provider-b"]
            .into_iter()
            .map(|provider_id| (provider_id.to_string(), "release_id".to_string()))
            .collect();
        let track_pairs = HashSet::from([("musicbrainz".to_string(), "recording_id".to_string())]);
        deduplicate_releases_by_external_id(db, library_db_id, &release_pairs, &track_pairs, None)
    }

    #[test]
    fn deduplicate_releases_merges_a_release_sharing_ids_with_two_others() -> anyhow::Result<()> {
        // The bridging release is merged away by whichever of its groups runs first.
        for order in LINK_ORDERS {
            let mut db = new_test_db()?;
            let library_db_id = insert_library(&mut db, "Music", "/music")?;
            insert_bridged_releases(
                &mut db,
                library_db_id,
                order,
                [&["r1"], &["r2"], &["r3"]],
                &[],
            )?;

            let merged = deduplicate_bridged_releases(&mut db, library_db_id)?;
            assert_eq!(merged, 2, "{order:?}");
            assert_eq!(release_ids_for_library(&db, library_db_id)?.len(), 1);
        }
        Ok(())
    }

    #[test]
    fn deduplicate_releases_checks_overlap_against_the_tracks_a_winner_absorbed()
    -> anyhow::Result<()> {
        for order in LINK_ORDERS {
            let mut db = new_test_db()?;
            let library_db_id = insert_library(&mut db, "Music", "/music")?;
            // The left release wins ties and absorbs the bridge first, whose
            // recording returns on the right one.
            insert_bridged_releases(
                &mut db,
                library_db_id,
                order,
                [&["r1"], &["r2"], &["r2"]],
                &[],
            )?;

            let merged = deduplicate_bridged_releases(&mut db, library_db_id)?;
            assert_eq!(merged, 1, "{order:?}");
            assert_eq!(release_ids_for_library(&db, library_db_id)?.len(), 2);
        }
        Ok(())
    }

    #[test]
    fn deduplicate_releases_merges_into_the_one_ranked_first_across_all_of_them()
    -> anyhow::Result<()> {
        // Merging the first pair alone would lift its winner above the right
        // release, which has the most tracks of all three.
        for order in LINK_ORDERS {
            let mut db = new_test_db()?;
            let library_db_id = insert_library(&mut db, "Music", "/music")?;
            let releases = insert_bridged_releases(
                &mut db,
                library_db_id,
                order,
                [&["l1", "l2"], &["b1", "b2"], &["r1", "r2", "r3"]],
                &[],
            )?;

            assert_eq!(deduplicate_bridged_releases(&mut db, library_db_id)?, 2);
            assert_eq!(
                release_ids_for_library(&db, library_db_id)?,
                [releases[2]],
                "{order:?}"
            );
        }
        Ok(())
    }

    #[test]
    fn deduplicate_releases_keeps_releases_with_other_musicbrainz_ids_apart() -> anyhow::Result<()>
    {
        for order in LINK_ORDERS {
            let mut db = new_test_db()?;
            let library_db_id = insert_library(&mut db, "Music", "/music")?;
            insert_bridged_releases(
                &mut db,
                library_db_id,
                order,
                [&["r1"], &["r2"], &["r3"]],
                &[
                    (0, "musicbrainz", "one"),
                    (1, "musicbrainz", "two"),
                    (2, "musicbrainz", "three"),
                ],
            )?;

            assert_eq!(deduplicate_bridged_releases(&mut db, library_db_id)?, 0);
            assert_eq!(release_ids_for_library(&db, library_db_id)?.len(), 3);
        }
        Ok(())
    }

    #[test]
    fn deduplicate_releases_does_not_merge_a_chain_across_a_conflict() -> anyhow::Result<()> {
        // The bridge may join either end, but never both ends together.
        for order in LINK_ORDERS {
            let mut db = new_test_db()?;
            let library_db_id = insert_library(&mut db, "Music", "/music")?;
            let releases = insert_bridged_releases(
                &mut db,
                library_db_id,
                order,
                [&["l1", "l2"], &["b1"], &["r1", "r2"]],
                &[(0, "musicbrainz", "one"), (2, "musicbrainz", "two")],
            )?;

            assert_eq!(
                deduplicate_bridged_releases(&mut db, library_db_id)?,
                1,
                "{order:?}"
            );
            let mut remaining = release_ids_for_library(&db, library_db_id)?;
            remaining.sort();
            assert_eq!(remaining, [releases[0], releases[2]], "{order:?}");
        }
        Ok(())
    }

    #[test]
    fn deduplicate_releases_merges_past_a_lower_ranked_conflicting_id() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let library_db_id = insert_library(&mut db, "Music", "/music")?;
        // The left pair shares a MusicBrainz ID; provider-a matched each to
        // its own album.
        insert_bridged_releases(
            &mut db,
            library_db_id,
            ("musicbrainz", "provider-b"),
            [&["r1"], &["r2"], &["r3"]],
            &[(0, "provider-a", "one"), (1, "provider-a", "two")],
        )?;

        assert_eq!(deduplicate_bridged_releases(&mut db, library_db_id)?, 2);
        let remaining = release_ids_for_library(&db, library_db_id)?;
        assert_eq!(remaining.len(), 1);
        let provider_a_ids = external_ids::get_for_entity(&db, remaining[0])?
            .into_iter()
            .filter(|id| id.provider_id == "provider-a")
            .count();
        assert_eq!(provider_a_ids, 1);
        Ok(())
    }

    #[test]
    fn a_scoped_dedup_merges_releases_that_conflict_only_with_a_third() -> anyhow::Result<()> {
        // The out-of-scope MusicBrainz IDs keep the odd release apart without
        // linking the other two. It takes every place, so it is first in its
        // group in one of them.
        for odd in 0..3 {
            let mut db = new_test_db()?;
            let library_db_id = insert_library(&mut db, "Music", "/music")?;
            for (position, provider_id) in ["musicbrainz", "provider"].into_iter().enumerate() {
                lyra_db::providers::upsert(
                    &mut db,
                    &lyra_db::ProviderConfig {
                        db_id: None,
                        provider_id: provider_id.to_string(),
                        position: position as u32,
                        enabled: true,
                    },
                )?;
            }
            let mut releases = Vec::new();
            for index in 0..3 {
                let release = insert_release(&mut db, "Release")?;
                connect(&mut db, library_db_id, release)?;
                let musicbrainz_id = if index == odd { "one" } else { "two" };
                for (provider_id, id_value) in
                    [("provider", "shared"), ("musicbrainz", musicbrainz_id)]
                {
                    external_ids::upsert(
                        &mut db,
                        release,
                        provider_id,
                        "release_id",
                        id_value,
                        IdSource::Resolved,
                    )?;
                }
                releases.push(release);
            }
            let release_pairs = ["musicbrainz", "provider"]
                .into_iter()
                .map(|provider_id| (provider_id.to_string(), "release_id".to_string()))
                .collect();
            let scope = HashSet::from(["provider".to_string()]);

            let merged = deduplicate_releases_by_external_id(
                &mut db,
                library_db_id,
                &release_pairs,
                &HashSet::new(),
                Some(&scope),
            )?;

            assert_eq!(merged, 1, "{odd}");
            let remaining = release_ids_for_library(&db, library_db_id)?;
            assert_eq!(remaining.len(), 2, "{odd}");
            assert!(remaining.contains(&releases[odd]), "{odd}");
        }
        Ok(())
    }
}
