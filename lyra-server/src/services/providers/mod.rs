// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use super::EntityType;

mod admin;
mod capture;
mod dedup;
mod refresh;
mod registry;
mod sync;

pub(crate) use admin::{
    EntityExternalIdRecord,
    ProviderAdminError,
    SetEntityExternalIdRequest,
    list_entity_external_ids,
    list_provider_configs,
    refresh_entity_by_id,
    set_entity_external_id,
    set_entity_locked,
    update_provider_priority,
};
pub(crate) use capture::run_capture;
pub(crate) use refresh::{
    LibraryRefreshOptions,
    refresh_entity_metadata,
    refresh_library_metadata,
};
pub(crate) use registry::{
    LIBRARY_REFRESH_LOCKS,
    PROVIDER_REGISTRY,
    ProviderCoverRequireSpec,
    ProviderCoverSpec,
    ProviderIdSpec,
    SYNC_LOCKS,
    reset_provider_registry_for_test,
    teardown_plugin_providers,
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
    use crate::db::test_db::{
        connect,
        insert_artist,
        insert_library,
        insert_release,
        insert_track,
        new_test_db,
    };
    use crate::db::{
        IdSource,
        external_ids,
        releases,
        tracks,
    };
    use agdb::{
        DbAny,
        DbId,
    };
    use std::collections::HashSet;

    fn release_ids_for_library(db: &DbAny, library_db_id: DbId) -> anyhow::Result<Vec<DbId>> {
        Ok(releases::get(db, library_db_id)?
            .into_iter()
            .filter_map(|release| release.db_id.map(Into::into))
            .collect())
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
            IdSource::Plugin,
        )?;
        external_ids::upsert(
            &mut db,
            release_b,
            "musicbrainz",
            "release_id",
            "same-release",
            IdSource::Plugin,
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
            IdSource::Plugin,
        )?;
        external_ids::upsert(
            &mut db,
            release_b,
            "musicbrainz",
            "release_id",
            "shared-release",
            IdSource::Plugin,
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
                IdSource::Plugin,
            )?;
        }

        external_ids::upsert(
            &mut db,
            track_a1,
            "musicbrainz",
            "recording_id",
            "rec-1",
            IdSource::Plugin,
        )?;
        external_ids::upsert(
            &mut db,
            track_a2,
            "musicbrainz",
            "recording_id",
            "rec-2",
            IdSource::Plugin,
        )?;
        external_ids::upsert(
            &mut db,
            track_b1,
            "musicbrainz",
            "recording_id",
            "rec-2",
            IdSource::Plugin,
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
                IdSource::Plugin,
            )?;
        }

        external_ids::upsert(
            &mut db,
            track_a,
            "musicbrainz",
            "recording_id",
            "rec-1",
            IdSource::Plugin,
        )?;
        external_ids::upsert(
            &mut db,
            track_b,
            "musicbrainz",
            "recording_id",
            "rec-2",
            IdSource::Plugin,
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
}
