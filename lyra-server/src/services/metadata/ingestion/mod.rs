// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

mod artists;
mod grouping;
mod releases;
mod tracks;

use std::{
    collections::BTreeMap,
    mem,
    path::{
        Path,
        PathBuf,
    },
};

use agdb::{
    DbAny,
    DbId,
};

pub(crate) use self::grouping::{
    MetadataGroup,
    SourceDirGroup,
    album_groups,
    albums_left_by,
    group_by_source_dir,
    metadata_groups,
};
use self::{
    releases::{
        ReleaseIngestResult,
        TrackIngest,
        persist_release,
    },
    tracks::build_existing_track_map,
};
use super::TrackMetadata;

#[derive(Debug)]
pub(crate) struct ParsedMetadataGroup {
    pub(crate) coalesce_group_key: usize,
    pub(crate) source_dir: PathBuf,
    pub(crate) metadata: Vec<TrackMetadata>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct MetadataApplyResult {
    pub(crate) releases: Vec<DbId>,
    pub(crate) tracks: Vec<DbId>,
}

impl MetadataApplyResult {
    fn push_release(&mut self, release_id: DbId) {
        if !self.releases.contains(&release_id) {
            self.releases.push(release_id);
        }
    }

    fn push_track(&mut self, track_id: DbId) {
        if !self.tracks.contains(&track_id) {
            self.tracks.push(track_id);
        }
    }

    fn extend_release(&mut self, result: ReleaseIngestResult) {
        self.push_release(result.release_db_id);
        for track_id in result.track_db_ids {
            self.push_track(track_id);
        }
    }
}

pub(crate) fn coalesce_disc_groups(groups: Vec<ParsedMetadataGroup>) -> Vec<Vec<TrackMetadata>> {
    lyra_metadata::coalesce_release_groups(
        groups
            .into_iter()
            .map(|group| lyra_metadata::ParsedReleaseGroup {
                coalesce_group_key: group.coalesce_group_key,
                source_dir: group.source_dir.to_string_lossy().to_string(),
                tracks: group.metadata,
            })
            .collect(),
    )
}

/// Coalesces one metadata group's parsed tracks into batches, comparing the folders
/// `candidate_dirs` puts them in. A track it doesn't name counts as in `source_dir`.
pub(crate) fn coalesce_group_tracks(
    source_dir: &Path,
    candidate_dirs: &BTreeMap<DbId, PathBuf>,
    metadata: Vec<TrackMetadata>,
) -> Vec<Vec<TrackMetadata>> {
    let mut metadata_by_dir: BTreeMap<PathBuf, Vec<TrackMetadata>> = BTreeMap::new();
    for track in metadata {
        let dir = candidate_dirs
            .get(&track.entry_db_id)
            .map_or(source_dir, PathBuf::as_path);
        metadata_by_dir
            .entry(dir.to_path_buf())
            .or_default()
            .push(track);
    }
    coalesce_disc_groups(
        metadata_by_dir
            .into_iter()
            .map(|(source_dir, metadata)| ParsedMetadataGroup {
                coalesce_group_key: 0,
                source_dir,
                metadata,
            })
            .collect(),
    )
}

pub(crate) fn apply_metadata(
    db: &mut DbAny,
    library_db_id: DbId,
    metadata: Vec<TrackMetadata>,
) -> anyhow::Result<MetadataApplyResult> {
    if metadata.is_empty() {
        return Ok(MetadataApplyResult::default());
    }

    let existing_tracks = build_existing_track_map(db, &metadata)?;

    let mut current_release = None;
    let mut release_tracks: Vec<TrackIngest> = Vec::new();
    let mut result = MetadataApplyResult::default();

    for track in metadata {
        let release = lyra_metadata::normalize_unicode_nfc(track.album.as_deref().unwrap_or(""));
        let track_db_id = existing_tracks.get(&track.source_key).copied();

        if current_release.as_ref() != Some(&release) {
            if !release_tracks.is_empty() {
                let release_result = persist_release(
                    db,
                    library_db_id,
                    current_release.as_deref().unwrap(),
                    mem::take(&mut release_tracks),
                )?;
                result.extend_release(release_result);
            }

            current_release = Some(release);
        }

        release_tracks.push(TrackIngest {
            meta: track,
            track_db_id,
        });
    }

    if !release_tracks.is_empty() {
        let release_result = persist_release(
            db,
            library_db_id,
            current_release.as_deref().unwrap(),
            release_tracks,
        )?;
        result.extend_release(release_result);
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::metadata::mapping::ensure_config;
    use crate::services::metadata::{
        TrackMetadata,
        cleanup::cleanup_orphaned_metadata,
        parse_metadata,
    };
    use agdb::QueryBuilder;
    use anyhow::anyhow;
    use lyra_db::roots::{
        self,
        Root,
    };
    use lyra_db::test_db::{
        self,
        TestDb,
        new_test_db,
    };
    use lyra_db::track_sources::SourceFormat;
    use lyra_db::{
        Artist,
        Entry,
        IdSource,
        Library,
        ProviderConfig,
        Release,
        Track,
    };
    use nanoid::nanoid;
    use std::collections::{
        HashMap,
        HashSet,
    };
    use std::fs;
    use std::path::{
        Path,
        PathBuf,
    };
    use std::time::{
        SystemTime,
        UNIX_EPOCH,
    };

    use crate::services::metadata::layers::{
        LOCAL_SOURCE_ID,
        MANUAL_SOURCE_ID,
    };
    use lyra_db::MetadataField;
    use lyra_db::external_ids::ExternalId;
    use lyra_db::metadata::layers::{
        FieldValue,
        MetadataLayer,
    };
    use std::collections::BTreeMap;

    fn insert_entry(db: &mut DbAny, path: &str) -> anyhow::Result<DbId> {
        insert_entry_with_kind(db, Path::new(path), lyra_db::entries::EntryKind::File)
    }

    fn insert_entry_with_kind(
        db: &mut DbAny,
        path: &Path,
        kind: lyra_db::entries::EntryKind,
    ) -> anyhow::Result<DbId> {
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .map(|name| name.to_string())
            .unwrap_or_else(|| path.to_string_lossy().to_string());
        let entry = Entry {
            db_id: None,
            id: nanoid!(),
            full_path: path.to_path_buf(),
            kind,
            file_kind: if kind == lyra_db::entries::EntryKind::File {
                lyra_db::entries::classify_file_kind(path)
                    .map(str::to_string)
                    .or_else(|| Some("audio".to_string()))
            } else {
                None
            },
            name,
            hash: None,
            size: 0,
            file_modified_at_ms: 0,
            file_created_at_ms: 0,
            synced_at_ms: None,
            missing_at_ms: None,
        };
        let qr = db.exec_mut(QueryBuilder::insert().element(&entry).query())?;
        Ok(qr.elements[0].id)
    }

    fn insert_track(db: &mut DbAny, title: &str) -> anyhow::Result<DbId> {
        let track = Track {
            db_id: None,
            id: nanoid!(),
            track_title: title.to_string(),
            sort_title: None,
            year: None,
            disc: None,
            disc_total: None,
            track: None,
            track_total: None,
            duration_ms: None,
            track_gain_db: None,
            album_gain_db: None,
            locked: Some(false),
            created_at_ms: None,
            file_created_at_ms: None,
        };
        let qr = db.exec_mut(QueryBuilder::insert().element(&track).query())?;
        Ok(qr.elements[0].id)
    }

    fn insert_release(db: &mut DbAny, title: &str) -> anyhow::Result<DbId> {
        let release = Release {
            db_id: None,
            id: nanoid!(),
            release_title: title.to_string(),
            sort_title: None,
            release_type: None,
            release_date: None,
            media_formats: None,
            barcode: None,
            description: None,
            locked: Some(false),
            created_at_ms: None,
            file_created_at_ms: None,
        };
        let qr = db.exec_mut(QueryBuilder::insert().element(&release).query())?;
        Ok(qr.elements[0].id)
    }

    fn insert_artist(db: &mut DbAny, name: &str) -> anyhow::Result<DbId> {
        let artist = Artist {
            db_id: None,
            id: nanoid!(),
            artist_name: name.to_string(),
            scan_name: name.to_string(),
            scan_aliases: Vec::new(),
            sort_name: None,
            artist_type: None,
            description: None,
            credited: false,
            locked: Some(false),
            created_at_ms: None,
        };
        let qr = db.exec_mut(QueryBuilder::insert().element(&artist).query())?;
        Ok(qr.elements[0].id)
    }

    fn connect(
        db: &mut DbAny,
        from: impl Into<agdb::QueryId>,
        to: impl Into<agdb::QueryId>,
    ) -> anyhow::Result<()> {
        db.exec_mut(QueryBuilder::insert().edges().from(from).to(to).query())?;
        Ok(())
    }

    fn connect_track_to_entry_source(
        db: &mut DbAny,
        track_db_id: DbId,
        entry_db_id: DbId,
    ) -> anyhow::Result<()> {
        let source_key = format!("entry:{}:embedded", entry_db_id.0);
        db.transaction_mut(|t| {
            lyra_db::track_sources::upsert(
                t,
                track_db_id,
                entry_db_id,
                lyra_db::track_sources::TrackSourceUpsert {
                    source_kind: "embedded_tags".to_string(),
                    source_key,
                    is_primary: true,
                    start_ms: None,
                    end_ms: None,
                    format: Default::default(),
                },
                None,
            )
        })?;
        Ok(())
    }

    fn fixture_path() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/assets/metadata/integration_track.flac")
    }

    fn multi_fixture_dir() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/assets/metadata/multi")
    }

    fn select_tracks(db: &DbAny) -> anyhow::Result<Vec<Track>> {
        let tracks: Vec<Track> = roots::members::<Track>(db, Root::Tracks)?;
        Ok(tracks)
    }

    fn select_releases(db: &DbAny) -> anyhow::Result<Vec<Release>> {
        let releases: Vec<Release> = roots::members::<Release>(db, Root::Releases)?;
        Ok(releases)
    }

    fn select_artists(db: &DbAny) -> anyhow::Result<Vec<Artist>> {
        let artists: Vec<Artist> = roots::members::<Artist>(db, Root::Artists)?;
        Ok(artists)
    }

    fn member_count(db: &DbAny, root: lyra_db::roots::Root) -> anyhow::Result<usize> {
        Ok(roots::member_ids(db, root)?.len())
    }

    fn parsed_group(source_dir: &str, metadata: Vec<TrackMetadata>) -> ParsedMetadataGroup {
        ParsedMetadataGroup {
            coalesce_group_key: 0,
            source_dir: PathBuf::from(source_dir),
            metadata,
        }
    }

    fn track_metadata(
        entry_db_id: i64,
        album: &str,
        artist: &str,
        year: Option<u32>,
        disc: Option<u32>,
        track: u32,
    ) -> TrackMetadata {
        let entry_db_id = DbId(entry_db_id);
        TrackMetadata {
            entry_db_id,
            format: Default::default(),
            album: Some(album.to_string()),
            album_artists: Some(vec![artist.to_string()]),
            date: None,
            year,
            title: Some(format!("Track {track}")),
            artists: Some(vec![artist.to_string()]),
            artist_relations: Vec::new(),
            disc,
            disc_total: None,
            track: Some(track),
            track_total: None,
            duration_ms: None,
            genres: None,
            label: None,
            catalog_number: None,
            media_formats: Vec::new(),
            barcode: None,
            source_kind: Some("embedded_tags".to_string()),
            source_key: format!("entry:{}:embedded", entry_db_id.0),
            segment_start_ms: None,
            segment_end_ms: None,
            cue_sheet_entry_id: None,
            cue_sheet_hash: None,
            cue_track_no: None,
            cue_audio_entry_id: None,
            cue_index00_frames: None,
            cue_index01_frames: None,
            track_gain_db: None,
            album_gain_db: None,
        }
    }

    fn insert_release_cover(db: &mut DbAny, release_db_id: DbId, id: &str) -> anyhow::Result<()> {
        db.transaction_mut(|t| {
            lyra_db::covers::upsert(
                t,
                release_db_id,
                lyra_db::covers::CoverSource::Local,
                lyra_db::covers::Cover {
                    db_id: None,
                    id: id.to_string(),
                    path: format!("/tmp/{id}.jpg"),
                    mime_type: "image/jpeg".to_string(),
                    hash: "a".repeat(64),
                    blurhash: None,
                    provider_id: None,
                },
            )?;
            Ok(())
        })
    }

    fn playlist_with_tracks(
        db: &mut DbAny,
        username: &str,
        track_db_ids: &[DbId],
    ) -> anyhow::Result<DbId> {
        let user_db_id = lyra_db::users::create(db, &lyra_db::fixtures::test_user(username)?)?;
        let playlist = lyra_db::playlists::Playlist {
            db_id: None,
            id: nanoid!(),
            name: "Ingestion Cover Regression".to_string(),
            description: None,
            is_public: None,
            created_at_ms: None,
            updated_at_ms: None,
        };
        let playlist_db_id = lyra_db::playlists::create(db, &playlist, user_db_id)?;
        db.transaction_mut(|t| -> anyhow::Result<()> {
            lyra_db::playlists::add_tracks(t, playlist_db_id, track_db_ids)?;
            Ok(())
        })?;
        Ok(playlist_db_id)
    }

    fn playlist_cover_winner(db: &DbAny, playlist_db_id: DbId) -> anyhow::Result<Option<DbId>> {
        use lyra_db::covers::display::{
            DisplayCoverScope,
            DisplayCoverTargetKind,
            DisplayCoverWinnerKind,
        };

        let playlist = lyra_db::playlists::get_by_id(db, playlist_db_id)?
            .ok_or_else(|| anyhow!("playlist missing"))?;
        let Some((_, profile)) = lyra_db::covers::display::get_profile(
            db,
            DisplayCoverScope::Instance,
            DisplayCoverTargetKind::Playlist,
            None,
            &playlist.id,
        )?
        else {
            return Ok(None);
        };
        Ok(
            lyra_db::covers::display::get_winner(db, &profile, DisplayCoverWinnerKind::Random)?
                .map(|winner| winner.release_db_id),
        )
    }

    async fn add_metadata(
        db: &mut DbAny,
        library: &Library,
        entries: Vec<DbId>,
    ) -> anyhow::Result<()> {
        let library_db_id = library
            .db_id
            .ok_or_else(|| anyhow::anyhow!("library missing db_id"))?;
        let roots = std::sync::Arc::new(crate::services::libraries::MediaRoots::around([library
            .path
            .clone()]));
        let groups = metadata_groups(
            &library.path,
            lyra_db::entries::load_existing(db, library_db_id)?,
            &Default::default(),
            &roots,
        )
        .await
        .into_iter()
        .filter(|group| {
            group
                .entries
                .iter()
                .any(|entry| entry.db_id.is_some_and(|id| entries.contains(&id)))
        });
        let mapping_config = ensure_config(db)?;

        for group in groups {
            let candidate_dirs = group.candidate_dirs();
            let (source_dir, entries) = (group.source_dir, group.entries);
            let parse_output =
                parse_metadata(&mapping_config, entries, &Default::default(), &roots).await?;
            if !parse_output.skipped.is_empty() {
                crate::services::metadata::log_skip_summary(&parse_output.skipped);
            }
            for metadata in
                coalesce_group_tracks(&source_dir, &candidate_dirs, parse_output.metadata)
            {
                apply_metadata(db, library_db_id, metadata)?;
            }
        }

        cleanup_orphaned_metadata(db)?;

        Ok(())
    }

    #[test]
    fn cleanup_removes_orphans_after_entry_delete() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let entry_db_id = insert_entry(&mut db, "/music/album/track1.flac")?;
        let track_db_id = insert_track(&mut db, "Track 1")?;
        let release_db_id = insert_release(&mut db, "Album 1")?;
        let artist_db_id = insert_artist(&mut db, "Artist 1")?;

        roots::link(&mut db, Root::Tracks, track_db_id)?;
        roots::link(&mut db, Root::Releases, release_db_id)?;
        roots::link(&mut db, Root::Artists, artist_db_id)?;
        connect(&mut db, release_db_id, track_db_id)?;
        lyra_db::test_db::connect_artist(&mut db, release_db_id, artist_db_id)?;
        connect_track_to_entry_source(&mut db, track_db_id, entry_db_id)?;
        lyra_db::test_db::connect_artist(&mut db, track_db_id, artist_db_id)?;

        db.exec_mut(QueryBuilder::remove().ids(entry_db_id).query())?;
        cleanup_orphaned_metadata(&mut db)?;

        assert!(select_tracks(&db)?.is_empty());
        assert!(select_releases(&db)?.is_empty());
        assert!(select_artists(&db)?.is_empty());
        assert_eq!(member_count(&db, Root::Tracks)?, 0);
        assert_eq!(member_count(&db, Root::Releases)?, 0);
        assert_eq!(member_count(&db, Root::Artists)?, 0);

        Ok(())
    }

    #[test]
    fn cleanup_removes_orphans_with_layer_and_ext_id() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let entry_db_id = insert_entry(&mut db, "/music/album/track_layered.flac")?;
        let track_db_id = insert_track(&mut db, "Layered Track")?;
        let release_db_id = insert_release(&mut db, "Album L")?;
        let artist_db_id = insert_artist(&mut db, "Artist L")?;

        roots::link(&mut db, Root::Tracks, track_db_id)?;
        roots::link(&mut db, Root::Releases, release_db_id)?;
        roots::link(&mut db, Root::Artists, artist_db_id)?;
        connect(&mut db, release_db_id, track_db_id)?;
        lyra_db::test_db::connect_artist(&mut db, release_db_id, artist_db_id)?;
        connect_track_to_entry_source(&mut db, track_db_id, entry_db_id)?;
        lyra_db::test_db::connect_artist(&mut db, track_db_id, artist_db_id)?;

        // Add a metadata layer attached to the track
        lyra_db::metadata::layers::upsert(
            &mut db,
            track_db_id,
            &MetadataLayer::new("test-provider", Default::default(), 0),
        )?;
        let layer_id = lyra_db::metadata::collect_layer_ids(&db, track_db_id)?[0];

        // Add an external ID attached to the track
        let ext = ExternalId {
            db_id: None,
            provider_id: "test-provider".to_string(),
            id_type: "track_db_id".to_string(),
            id_value: "abc123".to_string(),
            source: IdSource::Resolved,
            resolved_value: None,
        };
        let ext_qr = db.exec_mut(QueryBuilder::insert().element(&ext).query())?;
        let ext_id = ext_qr.elements[0].id;
        connect(&mut db, track_db_id, ext_id)?;

        // Delete the entry → track becomes orphan
        db.exec_mut(QueryBuilder::remove().ids(entry_db_id).query())?;
        cleanup_orphaned_metadata(&mut db)?;

        assert!(select_tracks(&db)?.is_empty());
        assert!(select_releases(&db)?.is_empty());
        assert!(select_artists(&db)?.is_empty());

        // Layer and external ID should also be gone (cascade)
        assert!(
            db.exec(QueryBuilder::select().ids(layer_id).query())
                .is_err(),
            "layer should be cascade-deleted"
        );
        assert!(
            db.exec(QueryBuilder::select().ids(ext_id).query()).is_err(),
            "external id should be cascade-deleted"
        );

        Ok(())
    }

    #[test]
    fn cleanup_preserves_connected_metadata() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let entry_db_id = insert_entry(&mut db, "/music/album/track2.flac")?;
        let track_db_id = insert_track(&mut db, "Track 2")?;
        let release_db_id = insert_release(&mut db, "Album 2")?;
        let artist_db_id = insert_artist(&mut db, "Artist 2")?;

        roots::link(&mut db, Root::Tracks, track_db_id)?;
        roots::link(&mut db, Root::Releases, release_db_id)?;
        roots::link(&mut db, Root::Artists, artist_db_id)?;
        connect(&mut db, release_db_id, track_db_id)?;
        lyra_db::test_db::connect_artist(&mut db, release_db_id, artist_db_id)?;
        connect_track_to_entry_source(&mut db, track_db_id, entry_db_id)?;

        cleanup_orphaned_metadata(&mut db)?;

        assert_eq!(select_tracks(&db)?.len(), 1);
        assert_eq!(select_releases(&db)?.len(), 1);
        assert_eq!(select_artists(&db)?.len(), 1);
        assert_eq!(member_count(&db, Root::Tracks)?, 1);
        assert_eq!(member_count(&db, Root::Releases)?, 1);
        assert_eq!(member_count(&db, Root::Artists)?, 1);

        Ok(())
    }

    fn provider_layer(db: &mut DbAny, node_id: DbId) -> anyhow::Result<DbId> {
        lyra_db::metadata::layers::upsert(
            db,
            node_id,
            &MetadataLayer::new("provider", Default::default(), 0),
        )?;
        lyra_db::metadata::layers::layer_id(db, node_id, "provider")?
            .ok_or_else(|| anyhow!("provider layer missing"))
    }

    #[test]
    fn cleanup_keeps_layer_referenced_artists_until_their_layers_go() -> anyhow::Result<()> {
        use lyra_db::metadata::layers::edges;

        let mut db = new_test_db()?;
        let entry_db_id = insert_entry(&mut db, "/music/album/track.flac")?;
        let track_db_id = insert_track(&mut db, "Track")?;
        let release_db_id = insert_release(&mut db, "Album")?;
        let empty_release_id = insert_release(&mut db, "Empty")?;
        let hidden_id = insert_artist(&mut db, "Hidden")?;
        let member_id = insert_artist(&mut db, "Member")?;
        roots::link(&mut db, Root::Tracks, track_db_id)?;
        roots::link(&mut db, Root::Releases, release_db_id)?;
        roots::link(&mut db, Root::Releases, empty_release_id)?;
        roots::link(&mut db, Root::Artists, hidden_id)?;
        roots::link(&mut db, Root::Artists, member_id)?;
        connect(&mut db, release_db_id, track_db_id)?;
        connect_track_to_entry_source(&mut db, track_db_id, entry_db_id)?;

        let track_layer = provider_layer(&mut db, track_db_id)?;
        let hidden_layer = provider_layer(&mut db, hidden_id)?;
        let empty_layer = provider_layer(&mut db, empty_release_id)?;
        let jazz = lyra_db::genres::resolve_by_name(&mut db, "Jazz")?;
        lyra_db::genres::link_to_release(&mut db, jazz, release_db_id)?;
        db.transaction_mut(|t| {
            edges::replace_credits(
                t,
                track_layer,
                lyra_db::CreditType::Artist,
                &[edges::LayerCreditInput {
                    artist_id: hidden_id,
                    detail: None,
                }],
            )?;
            edges::replace_relations(
                t,
                hidden_layer,
                lyra_db::ArtistRelationType::MemberOf,
                &[edges::LayerRelationInput {
                    artist_id: member_id,
                    attributes: None,
                }],
            )?;
            edges::replace_genres(t, empty_layer, &[jazz])
        })?;

        let alias_id = insert_artist(&mut db, "Alias")?;
        roots::link(&mut db, Root::Artists, alias_id)?;
        lyra_db::artists::relations::link(
            &mut db,
            hidden_id,
            alias_id,
            lyra_db::ArtistRelationType::VoiceActor,
            None,
        )?;

        cleanup_orphaned_metadata(&mut db)?;

        assert!(lyra_db::artists::get_by_id(&db, hidden_id)?.is_some());
        assert!(lyra_db::artists::get_by_id(&db, member_id)?.is_some());
        assert!(
            lyra_db::artists::get_by_id(&db, alias_id)?.is_some(),
            "a kept artist keeps its effective relation targets"
        );
        assert!(
            lyra_db::releases::get_by_id(&db, empty_release_id)?.is_none(),
            "a layer genre does not give a release tracks"
        );

        db.transaction_mut(|t| {
            lyra_db::metadata::layers::remove_inside_tx(t, track_db_id, "provider")
        })?;
        cleanup_orphaned_metadata(&mut db)?;

        assert!(lyra_db::artists::get_by_id(&db, hidden_id)?.is_none());
        assert!(lyra_db::artists::get_by_id(&db, member_id)?.is_none());
        assert!(lyra_db::artists::get_by_id(&db, alias_id)?.is_none());
        assert!(lyra_db::tracks::get_by_id(&db, track_db_id)?.is_some());
        Ok(())
    }

    #[test]
    fn cleanup_only_removes_orphaned_releases_and_artists() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let entry_db_id = insert_entry(&mut db, "/music/keep/track.flac")?;
        let track_db_id = insert_track(&mut db, "Keep Track")?;
        let release_keep_id = insert_release(&mut db, "Keep Album")?;
        let release_drop_id = insert_release(&mut db, "Drop Album")?;
        let artist_keep_id = insert_artist(&mut db, "Keep Artist")?;
        let artist_drop_id = insert_artist(&mut db, "Drop Artist")?;

        roots::link(&mut db, Root::Tracks, track_db_id)?;
        roots::link(&mut db, Root::Releases, release_keep_id)?;
        roots::link(&mut db, Root::Releases, release_drop_id)?;
        roots::link(&mut db, Root::Artists, artist_keep_id)?;
        roots::link(&mut db, Root::Artists, artist_drop_id)?;

        connect(&mut db, release_keep_id, track_db_id)?;
        lyra_db::test_db::connect_artist(&mut db, release_keep_id, artist_keep_id)?;
        connect_track_to_entry_source(&mut db, track_db_id, entry_db_id)?;

        lyra_db::test_db::connect_artist(&mut db, release_drop_id, artist_drop_id)?;

        cleanup_orphaned_metadata(&mut db)?;

        let releases = select_releases(&db)?;
        assert_eq!(releases.len(), 1);
        assert_eq!(releases[0].release_title, "Keep Album");

        let artists = select_artists(&db)?;
        assert_eq!(artists.len(), 1);
        assert_eq!(artists[0].artist_name, "Keep Artist");
        assert_eq!(member_count(&db, Root::Tracks)?, 1);
        assert_eq!(member_count(&db, Root::Releases)?, 1);
        assert_eq!(member_count(&db, Root::Artists)?, 1);

        Ok(())
    }

    #[tokio::test]
    async fn add_metadata_ingests_audio_fixture() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let fixture = fixture_path();
        assert!(fixture.is_file());
        let dir_path = fixture.parent().expect("fixture has parent").to_path_buf();

        let library = test_db::insert_test_library_node(&mut db, "Test Library", dir_path.clone())?;

        let dir_id = insert_entry_with_kind(&mut db, &dir_path, lyra_db::entries::EntryKind::Dir)?;
        let file_id = insert_entry_with_kind(&mut db, &fixture, lyra_db::entries::EntryKind::File)?;

        connect(&mut db, library.db_id.unwrap(), dir_id)?;
        connect(&mut db, dir_id, file_id)?;

        add_metadata(&mut db, &library, vec![dir_id, file_id]).await?;

        let releases = select_releases(&db)?;
        assert_eq!(releases.len(), 1);
        assert_eq!(releases[0].release_title, "Integration Album");

        let release_db_id: DbId = releases[0].db_id.clone().unwrap().into();
        let release_tracks = lyra_db::tracks::get(&db, release_db_id)?;
        assert_eq!(release_tracks.len(), 1);
        assert_eq!(release_tracks[0].track_title, "Integration Track");
        assert!(release_tracks[0].duration_ms.unwrap_or(0) > 0);
        let format = source_format_of(&db, &release_tracks[0])?;
        assert_eq!(format.sample_rate_hz, Some(44_100));
        assert_eq!(format.channel_count, Some(1));
        assert_eq!(format.bit_depth, Some(16));
        assert!(
            format.bitrate_bps.is_some_and(|bps| bps > 10_000),
            "scan should capture the source bitrate from the fixture (got {:?})",
            format.bitrate_bps
        );

        let album_artists = lyra_db::artists::get(&db, release_db_id)?;
        assert_eq!(album_artists.len(), 1);
        assert_eq!(album_artists[0].artist_name, "Integration Artist");

        let all_artists = select_artists(&db)?;
        assert_eq!(all_artists.len(), 1);

        let track_db_id: DbId = release_tracks[0].db_id.clone().unwrap().into();
        let linked_entries = lyra_db::entries::get_by_track(&db, track_db_id)?;
        assert_eq!(linked_entries.len(), 1);
        assert_eq!(linked_entries[0].full_path, fixture);

        Ok(())
    }

    #[tokio::test]
    async fn add_metadata_stores_scanned_edition_signals_on_release() -> anyhow::Result<()> {
        use lofty::{
            config::WriteOptions,
            file::AudioFile,
            flac::FlacFile,
            ogg::tag::VorbisComments,
            tag::TagExt,
        };

        let mut db = new_test_db()?;

        let dir_path = std::env::temp_dir().join(format!(
            "lyra-edition-signals-{}-{}",
            std::process::id(),
            nanoid!()
        ));
        std::fs::create_dir_all(&dir_path)?;
        let file_paths = vec![
            dir_path.join("multi_track_1.flac"),
            dir_path.join("multi_track_2.flac"),
        ];
        for path in &file_paths {
            std::fs::copy(multi_fixture_dir().join(path.file_name().unwrap()), path)?;
            let flac = FlacFile::read_from(
                &mut std::fs::File::open(path)?,
                lofty::config::ParseOptions::new(),
            )?;
            let mut comments = flac
                .vorbis_comments()
                .cloned()
                .unwrap_or_else(VorbisComments::default);
            comments.push("MEDIA".to_string(), "WEB".to_string());
            comments.push("UPC".to_string(), "0199957588430".to_string());
            comments.save_to_path(path, WriteOptions::default())?;
        }

        let library = test_db::insert_test_library_node(&mut db, "Test Library", dir_path.clone())?;
        let dir_id = insert_entry_with_kind(&mut db, &dir_path, lyra_db::entries::EntryKind::Dir)?;
        connect(&mut db, library.db_id.unwrap(), dir_id)?;
        let mut entry_ids = vec![dir_id];
        for path in &file_paths {
            let file_id = insert_entry_with_kind(&mut db, path, lyra_db::entries::EntryKind::File)?;
            connect(&mut db, dir_id, file_id)?;
            entry_ids.push(file_id);
        }

        add_metadata(&mut db, &library, entry_ids).await?;
        std::fs::remove_dir_all(&dir_path)?;

        let releases = select_releases(&db)?;
        assert_eq!(releases.len(), 1);
        assert_eq!(
            releases[0].media_formats.as_deref(),
            Some(&["digital".to_string()][..])
        );
        assert_eq!(releases[0].barcode.as_deref(), Some("00199957588430"));
        Ok(())
    }

    #[tokio::test]
    async fn add_metadata_ingests_multi_track_release_artist() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let dir_path = multi_fixture_dir();
        assert!(dir_path.is_dir());
        let file_paths = vec![
            dir_path.join("multi_track_1.flac"),
            dir_path.join("multi_track_2.flac"),
        ];
        for path in &file_paths {
            assert!(path.is_file());
        }

        let library = test_db::insert_test_library_node(&mut db, "Test Library", dir_path.clone())?;

        let dir_id = insert_entry_with_kind(&mut db, &dir_path, lyra_db::entries::EntryKind::Dir)?;
        connect(&mut db, library.db_id.unwrap(), dir_id)?;

        let mut entry_ids = vec![dir_id];
        for path in &file_paths {
            let file_id = insert_entry_with_kind(&mut db, path, lyra_db::entries::EntryKind::File)?;
            connect(&mut db, dir_id, file_id)?;
            entry_ids.push(file_id);
        }

        add_metadata(&mut db, &library, entry_ids).await?;

        let releases = select_releases(&db)?;
        assert_eq!(releases.len(), 1);
        assert_eq!(releases[0].release_title, "Integration Multi Album");

        let release_db_id: DbId = releases[0].db_id.clone().unwrap().into();
        let release_tracks = lyra_db::tracks::get(&db, release_db_id)?;
        assert_eq!(release_tracks.len(), 2);
        let titles: HashSet<&str> = release_tracks
            .iter()
            .map(|track| track.track_title.as_str())
            .collect();
        assert!(titles.contains("Multi Track 1"));
        assert!(titles.contains("Multi Track 2"));

        let album_artists = lyra_db::artists::get(&db, release_db_id)?;
        assert_eq!(album_artists.len(), 1);
        assert_eq!(album_artists[0].artist_name, "Integration Album Artist");

        for track in release_tracks {
            let track_db_id: DbId = track.db_id.clone().unwrap().into();
            let track_artists = lyra_db::artists::get(&db, track_db_id)?;
            assert_eq!(track_artists.len(), 1);
            match track.track_title.as_str() {
                "Multi Track 1" => {
                    assert_eq!(track_artists[0].artist_name, "Track Artist 1");
                }
                "Multi Track 2" => {
                    assert_eq!(track_artists[0].artist_name, "Track Artist 2");
                }
                _ => return Err(anyhow!("unexpected track title")),
            }
            let linked_entries: Vec<Entry> = lyra_db::entries::get_by_track(&db, track_db_id)?;
            assert_eq!(linked_entries.len(), 1);
            assert!(
                file_paths
                    .iter()
                    .any(|path| path == &linked_entries[0].full_path)
            );
        }

        Ok(())
    }

    #[test]
    fn apply_metadata_preserves_inferred_release_artist_order() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let library = test_db::insert_test_library_node(
            &mut db,
            "Test Library",
            PathBuf::from("/music/ordered"),
        )?;
        let library_db_id = library.db_id.expect("test library has db_id");

        let entry_db_id = insert_entry(&mut db, "/music/ordered/capsule.flac")?;
        connect(&mut db, library_db_id, entry_db_id)?;

        apply_metadata(
            &mut db,
            library_db_id,
            vec![TrackMetadata {
                entry_db_id,
                format: Default::default(),
                album: Some("Ordered Album".to_string()),
                album_artists: None,
                date: None,
                year: Some(2022),
                title: Some("CapSule".to_string()),
                artists: Some(vec!["Zulu Artist".to_string(), "Alpha Artist".to_string()]),
                artist_relations: Vec::new(),
                disc: Some(1),
                disc_total: Some(1),
                track: Some(1),
                track_total: Some(1),
                duration_ms: Some(174842),
                genres: Some(vec!["Jpop".to_string()]),
                label: None,
                catalog_number: None,
                media_formats: Vec::new(),
                barcode: None,
                source_kind: Some("embedded_tags".to_string()),
                source_key: format!("entry:{}:embedded", entry_db_id.0),
                segment_start_ms: None,
                segment_end_ms: None,
                cue_sheet_entry_id: None,
                cue_sheet_hash: None,
                cue_track_no: None,
                cue_audio_entry_id: None,
                cue_index00_frames: None,
                cue_index01_frames: None,
                track_gain_db: None,
                album_gain_db: None,
            }],
        )?;

        let releases = select_releases(&db)?;
        assert_eq!(releases.len(), 1);
        let release_db_id: DbId = releases[0].db_id.clone().unwrap().into();
        let album_artists = lyra_db::artists::get(&db, release_db_id)?;
        let names: Vec<&str> = album_artists
            .iter()
            .map(|artist| artist.artist_name.as_str())
            .collect();
        assert_eq!(names, vec!["Zulu Artist", "Alpha Artist"]);

        Ok(())
    }

    #[test]
    fn apply_metadata_persists_scanned_voice_actor_artist_relations() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let library =
            test_db::insert_test_library_node(&mut db, "CV Library", PathBuf::from("/music/cv"))?;
        let library_db_id = library.db_id.expect("test library has db_id");

        let entry_db_id = insert_entry(&mut db, "/music/cv/01.flac")?;
        connect(&mut db, library_db_id, entry_db_id)?;

        apply_metadata(
            &mut db,
            library_db_id,
            vec![TrackMetadata {
                entry_db_id,
                format: Default::default(),
                album: Some("瞬く日々へ".to_string()),
                album_artists: Some(vec!["井ノ華六花".to_string()]),
                date: None,
                year: Some(2026),
                title: Some("瞬く日々へ".to_string()),
                artists: Some(vec!["井ノ華六花".to_string()]),
                artist_relations: vec![
                    lyra_metadata::ArtistRelationMetadata {
                        source_artist: "瀬戸桃子".to_string(),
                        target_artist: "井ノ華六花".to_string(),
                        relation_type: lyra_metadata::ArtistRelationKind::VoiceActor,
                        source_artist_type: Some(lyra_metadata::ParsedArtistType::Person),
                        target_artist_type: Some(lyra_metadata::ParsedArtistType::Character),
                    },
                    lyra_metadata::ArtistRelationMetadata {
                        source_artist: "瀬戸桃子".to_string(),
                        target_artist: "井ノ華六花".to_string(),
                        relation_type: lyra_metadata::ArtistRelationKind::VoiceActor,
                        source_artist_type: Some(lyra_metadata::ParsedArtistType::Person),
                        target_artist_type: Some(lyra_metadata::ParsedArtistType::Character),
                    },
                ],
                disc: Some(1),
                disc_total: Some(1),
                track: Some(1),
                track_total: Some(1),
                duration_ms: Some(180_000),
                genres: None,
                label: None,
                catalog_number: None,
                media_formats: Vec::new(),
                barcode: None,
                source_kind: Some("embedded_tags".to_string()),
                source_key: format!("entry:{}:embedded", entry_db_id.0),
                segment_start_ms: None,
                segment_end_ms: None,
                cue_sheet_entry_id: None,
                cue_sheet_hash: None,
                cue_track_no: None,
                cue_audio_entry_id: None,
                cue_index00_frames: None,
                cue_index01_frames: None,
                track_gain_db: None,
                album_gain_db: None,
            }],
        )?;

        let release = select_releases(&db)?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("release missing"))?;
        let release_db_id: DbId = release
            .db_id
            .ok_or_else(|| anyhow!("release missing db_id"))?
            .into();
        let release_artists = lyra_db::artists::get(&db, release_db_id)?;
        assert_eq!(release_artists.len(), 1);
        assert_eq!(release_artists[0].artist_name, "井ノ華六花");
        assert_eq!(
            release_artists[0].artist_type,
            Some(lyra_db::ArtistType::Character)
        );

        let character_id =
            lyra_db::lookup::find_id_by_index(&db, "scan_name", "井ノ華六花", "Artist")?
                .ok_or_else(|| anyhow!("character artist missing"))?;
        let actor_id = lyra_db::lookup::find_id_by_index(&db, "scan_name", "瀬戸桃子", "Artist")?
            .ok_or_else(|| anyhow!("voice actor artist missing"))?;

        let actor =
            lyra_db::artists::get_by_id(&db, actor_id)?.ok_or_else(|| anyhow!("actor missing"))?;
        assert_eq!(actor.artist_type, Some(lyra_db::ArtistType::Person));

        let relations = lyra_db::artists::relations::get_relations_from(
            &db,
            actor_id,
            Some(lyra_db::ArtistRelationType::VoiceActor),
        )?;
        assert_eq!(relations.len(), 1);
        assert_eq!(relations[0].1, character_id);

        Ok(())
    }

    fn voiced_track(entry_db_id: DbId, album: &str, actor: &str) -> TrackMetadata {
        let mut metadata = track_metadata(entry_db_id.0, album, "Character", None, Some(1), 1);
        metadata.artist_relations = vec![lyra_metadata::ArtistRelationMetadata {
            source_artist: actor.to_string(),
            target_artist: "Character".to_string(),
            relation_type: lyra_metadata::ArtistRelationKind::VoiceActor,
            source_artist_type: Some(lyra_metadata::ParsedArtistType::Person),
            target_artist_type: Some(lyra_metadata::ParsedArtistType::Character),
        }];
        metadata
    }

    fn artist_named(db: &DbAny, name: &str) -> anyhow::Result<Option<DbId>> {
        lyra_db::lookup::find_id_by_index(db, "scan_name", name, "Artist")
    }

    fn voice_actors_of(db: &DbAny, character_id: DbId) -> anyhow::Result<Vec<String>> {
        let mut names = Vec::new();
        for (_, actor_id) in lyra_db::artists::relations::get_relations_to(
            db,
            character_id,
            Some(lyra_db::ArtistRelationType::VoiceActor),
        )? {
            names.push(
                lyra_db::artists::get_by_id(db, actor_id)?
                    .ok_or_else(|| anyhow!("actor missing"))?
                    .artist_name,
            );
        }
        names.sort();
        Ok(names)
    }

    #[test]
    fn scanned_relations_from_every_release_reach_the_characters_layer() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let library =
            test_db::insert_test_library_node(&mut db, "CV Library", PathBuf::from("/music/cv"))?;
        let library_db_id = library.db_id.expect("test library has db_id");
        let first_entry = insert_entry(&mut db, "/music/cv/a/01.flac")?;
        let second_entry = insert_entry(&mut db, "/music/cv/b/01.flac")?;
        connect(&mut db, library_db_id, first_entry)?;
        connect(&mut db, library_db_id, second_entry)?;

        apply_metadata(
            &mut db,
            library_db_id,
            vec![voiced_track(first_entry, "First Album", "First Actor")],
        )?;
        apply_metadata(
            &mut db,
            library_db_id,
            vec![voiced_track(second_entry, "Second Album", "Second Actor")],
        )?;

        let character_id = artist_named(&db, "Character")?.ok_or_else(|| anyhow!("character"))?;
        assert_eq!(
            voice_actors_of(&db, character_id)?,
            vec!["First Actor", "Second Actor"]
        );
        let local = local_layer(&db, character_id)?;
        assert_eq!(
            local.relations[&lyra_db::ArtistRelationType::VoiceActor].len(),
            2,
            "the character's local layer owns both relations"
        );
        Ok(())
    }

    #[test]
    fn a_layer_only_voice_actor_lives_as_long_as_the_relation() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let entry_db_id = insert_entry(&mut db, "/music/album/track.flac")?;
        let track_db_id = insert_track(&mut db, "Track")?;
        let character_id = insert_artist(&mut db, "Character")?;
        let actor_id = insert_artist(&mut db, "Actor")?;
        roots::link(&mut db, Root::Tracks, track_db_id)?;
        roots::link(&mut db, Root::Artists, character_id)?;
        roots::link(&mut db, Root::Artists, actor_id)?;
        connect_track_to_entry_source(&mut db, track_db_id, entry_db_id)?;
        lyra_db::test_db::connect_artist(&mut db, track_db_id, character_id)?;
        upsert_provider(&mut db, "provider", true)?;
        crate::services::metadata::layers::save_provider_layer(
            &mut db,
            character_id,
            "provider",
            &crate::services::metadata::layers::ProviderValues {
                relations: BTreeMap::from([(
                    lyra_db::ArtistRelationType::VoiceActor,
                    vec![lyra_db::metadata::layers::edges::LayerRelationInput {
                        artist_id: actor_id,
                        attributes: None,
                    }],
                )]),
                ..Default::default()
            },
            &HashMap::new(),
            &Default::default(),
        )?;

        cleanup_orphaned_metadata(&mut db)?;
        assert!(lyra_db::artists::get_by_id(&db, actor_id)?.is_some());
        assert_eq!(voice_actors_of(&db, character_id)?, vec!["Actor"]);

        crate::services::metadata::layers::clear_provider_layer(&mut db, character_id, "provider")?;
        assert!(voice_actors_of(&db, character_id)?.is_empty());
        cleanup_orphaned_metadata(&mut db)?;
        assert!(lyra_db::artists::get_by_id(&db, actor_id)?.is_none());
        assert!(lyra_db::artists::get_by_id(&db, character_id)?.is_some());
        Ok(())
    }

    #[test]
    fn a_partial_rescan_keeps_a_compilations_release_artist() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let library = test_db::insert_test_library_node(
            &mut db,
            "Compilation",
            PathBuf::from("/music/compilation"),
        )?;
        let library_id = library.db_id.unwrap();
        let mut tracks = Vec::new();
        for (number, artist) in [(1, "Main"), (2, "Main"), (3, "Guest")] {
            let entry = insert_entry(&mut db, &format!("/music/compilation/{number:02}.flac"))?;
            connect(&mut db, library_id, entry)?;
            let mut metadata =
                track_metadata(entry.0, "Compilation", artist, None, Some(1), number);
            metadata.album_artists = None;
            tracks.push(metadata);
        }
        apply_metadata(&mut db, library_id, tracks.clone())?;
        let release_id: DbId = select_releases(&db)?[0].db_id.clone().unwrap().into();
        let release_artists = |db: &DbAny| -> anyhow::Result<Vec<String>> {
            Ok(lyra_db::artists::get(db, release_id)?
                .into_iter()
                .map(|artist| artist.artist_name)
                .collect())
        };
        assert_eq!(release_artists(&db)?, vec!["Main"]);

        apply_metadata(&mut db, library_id, vec![tracks[2].clone()])?;

        assert_eq!(select_releases(&db)?.len(), 1);
        assert_eq!(
            release_artists(&db)?,
            vec!["Main"],
            "one rescanned track cannot outvote the rest of the release"
        );
        Ok(())
    }

    #[test]
    fn rescans_and_cleanup_keep_a_releases_chains() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let library =
            test_db::insert_test_library_node(&mut db, "Urn", PathBuf::from("/music/urn"))?;
        let library_id = library.db_id.unwrap();
        let mut tracks = Vec::new();
        for number in 1..=4 {
            let entry = insert_entry(&mut db, &format!("/music/urn/{number:02}.flac"))?;
            connect(&mut db, library_id, entry)?;
            tracks.push(track_metadata(
                entry.0,
                "Urn",
                "Ne Obliviscaris",
                Some(2017),
                Some(1),
                number,
            ));
        }
        apply_metadata(&mut db, library_id, tracks.clone())?;
        let release_id: DbId = select_releases(&db)?[0].db_id.clone().unwrap().into();
        let mut track_ids = lyra_db::tracks::get_direct(&db, release_id)?;
        track_ids.sort_by_key(|track| track.track);
        let track_ids = track_ids
            .into_iter()
            .map(|track| DbId::from(track.db_id.unwrap()))
            .collect::<Vec<_>>();
        let chains = lyra_db::chains::replace_for_release(
            &mut db,
            release_id,
            &[track_ids[1..3].to_vec()],
        )??;

        for (track, title) in tracks.iter_mut().zip([
            "Intra Venus",
            "Libera (Part I) – Saturnine Spheres",
            "Libera (Part II) – Ascent of Burning Moths",
            "Urn (Part I) – And Within the Void We Are Breathless",
        ]) {
            track.title = Some(title.to_string());
            track.album = Some("Urn (Remastered)".to_string());
            track.year = Some(2018);
        }
        apply_metadata(&mut db, library_id, tracks.clone())?;
        apply_metadata(&mut db, library_id, vec![tracks[2].clone()])?;
        cleanup_orphaned_metadata(&mut db)?;

        let releases = select_releases(&db)?;
        assert_eq!(releases.len(), 1);
        assert_eq!(DbId::from(releases[0].db_id.clone().unwrap()), release_id);
        assert_eq!(
            lyra_db::chains::get_for_release(&db, release_id)?,
            chains,
            "rescans keep the tracks and their release, so the chain stays"
        );
        Ok(())
    }

    fn two_disc_release(db: &mut DbAny) -> anyhow::Result<(DbId, Vec<TrackMetadata>)> {
        let library =
            test_db::insert_test_library_node(db, "Two Discs", PathBuf::from("/music/two-discs"))?;
        let library_id = library.db_id.unwrap();
        let mut tracks = Vec::new();
        for disc in [1, 2] {
            let entry = insert_entry(db, &format!("/music/two-discs/{disc}-01.flac"))?;
            connect(db, library_id, entry)?;
            let mut metadata = track_metadata(entry.0, "Two Discs", "Artist", None, Some(disc), 1);
            if disc == 1 {
                metadata.date = Some("2001-05-03".to_string());
                metadata.genres = Some(vec!["Rock".to_string()]);
                metadata.label = Some("Label".to_string());
                metadata.catalog_number = Some("CAT-1".to_string());
                metadata.barcode = Some("0123456789012".to_string());
            }
            tracks.push(metadata);
        }
        apply_metadata(db, library_id, tracks.clone())?;
        Ok((library_id, tracks))
    }

    type ReleaseScanFields = (Option<String>, Vec<String>, Vec<String>, Option<String>);

    fn release_scan_fields(db: &DbAny) -> anyhow::Result<ReleaseScanFields> {
        let release = select_releases(db)?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("release missing"))?;
        let release_id: DbId = release.db_id.clone().unwrap().into();
        let genres = lyra_db::genres::get_for_release(db, release_id)?
            .into_iter()
            .map(|genre| genre.name)
            .collect();
        let labels = lyra_db::labels::get_for_release(db, release_id)?
            .into_iter()
            .map(|label| format!("{} {:?}", label.label.name, label.catalog_number))
            .collect();
        Ok((release.release_date, genres, labels, release.barcode))
    }

    #[test]
    fn a_partial_rescan_keeps_release_fields_its_tracks_do_not_tag() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let (library_id, tracks) = two_disc_release(&mut db)?;
        let full = release_scan_fields(&db)?;
        assert_eq!(full.0.as_deref(), Some("2001-05-03"));
        assert_eq!(full.1, vec!["Rock"]);
        assert_eq!(full.2.len(), 1);
        assert!(full.3.is_some());

        apply_metadata(&mut db, library_id, vec![tracks[1].clone()])?;

        assert_eq!(select_releases(&db)?.len(), 1);
        assert_eq!(release_scan_fields(&db)?, full);
        Ok(())
    }

    #[test]
    fn a_full_rescan_clears_release_fields_removed_from_every_track() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let (library_id, mut tracks) = two_disc_release(&mut db)?;
        for track in &mut tracks {
            track.date = None;
            track.genres = None;
            track.label = None;
            track.catalog_number = None;
            track.barcode = None;
        }

        apply_metadata(&mut db, library_id, tracks)?;

        assert_eq!(
            release_scan_fields(&db)?,
            (None, Vec::new(), Vec::new(), None)
        );
        Ok(())
    }

    fn voice_actor(actor: &str, character: &str) -> lyra_metadata::ArtistRelationMetadata {
        lyra_metadata::ArtistRelationMetadata {
            source_artist: actor.to_string(),
            target_artist: character.to_string(),
            relation_type: lyra_metadata::ArtistRelationKind::VoiceActor,
            source_artist_type: Some(lyra_metadata::ParsedArtistType::Person),
            target_artist_type: Some(lyra_metadata::ParsedArtistType::Character),
        }
    }

    fn artist_type_of(db: &DbAny, name: &str) -> anyhow::Result<Option<lyra_db::ArtistType>> {
        let artist_id = artist_named(db, name)?.ok_or_else(|| anyhow!("{name} missing"))?;
        Ok(lyra_db::artists::get_by_id(db, artist_id)?
            .ok_or_else(|| anyhow!("{name} missing"))?
            .artist_type)
    }

    /// Scans each `(album, credited artist, relations)` as its own release, in order.
    fn scan_releases(
        db: &mut DbAny,
        releases: Vec<(&str, &str, Vec<lyra_metadata::ArtistRelationMetadata>)>,
    ) -> anyhow::Result<()> {
        let library =
            test_db::insert_test_library_node(db, "Types", PathBuf::from("/music/types"))?;
        let library_id = library.db_id.unwrap();
        for (album, artist, relations) in releases {
            let entry = insert_entry(db, &format!("/music/types/{album}.flac"))?;
            connect(db, library_id, entry)?;
            let mut track = track_metadata(entry.0, album, artist, None, Some(1), 1);
            track.artist_relations = relations;
            apply_metadata(db, library_id, vec![track])?;
        }
        Ok(())
    }

    #[test]
    fn scanning_another_release_keeps_a_characters_type() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        scan_releases(
            &mut db,
            vec![
                (
                    "Drama",
                    "Character",
                    vec![voice_actor("Actor", "Character")],
                ),
                ("Single", "Character", Vec::new()),
            ],
        )?;

        assert_eq!(
            artist_type_of(&db, "Character")?,
            Some(lyra_db::ArtistType::Character)
        );
        Ok(())
    }

    #[test]
    fn conflicting_scanned_artist_types_resolve_the_same_in_any_order() -> anyhow::Result<()> {
        let as_character = ("As Character", "Both", vec![voice_actor("Actor", "Both")]);
        let as_actor = ("As Actor", "Other", vec![voice_actor("Both", "Other")]);
        for releases in [
            vec![as_character.clone(), as_actor.clone()],
            vec![as_actor.clone(), as_character.clone()],
        ] {
            let mut db = new_test_db()?;
            scan_releases(&mut db, releases)?;
            assert_eq!(
                artist_type_of(&db, "Both")?,
                Some(lyra_db::ArtistType::Character)
            );
        }
        Ok(())
    }

    #[test]
    fn a_rescan_credits_the_artist_a_spelling_was_merged_into() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let library =
            test_db::insert_test_library_node(&mut db, "Merged", PathBuf::from("/music/merged"))?;
        let library_id = library.db_id.unwrap();
        let mut tracks = Vec::new();
        for (number, album, artist) in [(1, "Atomic Heart", "Mr.Children"), (2, "Q", "Mr Children")]
        {
            let entry = insert_entry(&mut db, &format!("/music/merged/{number:02}.flac"))?;
            connect(&mut db, library_id, entry)?;
            let metadata = track_metadata(entry.0, album, artist, None, Some(1), 1);
            apply_metadata(&mut db, library_id, vec![metadata.clone()])?;
            tracks.push(metadata);
        }
        let winner = artist_named(&db, "Mr.Children")?.ok_or_else(|| anyhow!("winner"))?;
        let loser = artist_named(&db, "Mr Children")?.ok_or_else(|| anyhow!("loser"))?;
        for artist in [winner, loser] {
            lyra_db::external_ids::upsert(
                &mut db,
                artist,
                "musicbrainz",
                "artist_id",
                "mbid-123",
                IdSource::Resolved,
            )?;
        }
        assert_eq!(
            crate::services::metadata::cleanup::deduplicate_artists_by_external_id(
                &mut db,
                &crate::services::metadata::id_links::DeclaredIds::unique([(
                    "musicbrainz",
                    "artist_id"
                )]),
            )?,
            1
        );

        apply_metadata(&mut db, library_id, vec![tracks[1].clone()])?;
        cleanup_orphaned_metadata(&mut db)?;

        assert_eq!(artist_named(&db, "Mr Children")?, None);
        assert_eq!(select_artists(&db)?.len(), 1);
        for release in select_releases(&db)? {
            let artists = lyra_db::artists::get(&db, DbId::from(release.db_id.unwrap()))?;
            assert_eq!(artists.len(), 1, "{}", release.release_title);
            assert_eq!(artists[0].db_id.clone().map(DbId::from), Some(winner));
        }
        Ok(())
    }

    #[test]
    fn a_scan_alias_spelling_never_renames_the_artist() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let library =
            test_db::insert_test_library_node(&mut db, "Aliases", PathBuf::from("/music/aliases"))?;
        let library_id = library.db_id.unwrap();
        let mut entries = Vec::new();
        for (number, album, artist) in [(1, "Atomic Heart", "Mr Children"), (2, "Q", "Mr.Children")]
        {
            let entry = insert_entry(&mut db, &format!("/music/aliases/{number:02}.flac"))?;
            connect(&mut db, library_id, entry)?;
            apply_metadata(
                &mut db,
                library_id,
                vec![track_metadata(
                    entry.0,
                    album,
                    artist,
                    None,
                    Some(1),
                    number,
                )],
            )?;
            entries.push(entry);
        }
        let winner = artist_named(&db, "Mr Children")?.ok_or_else(|| anyhow!("winner"))?;
        let loser = artist_named(&db, "Mr.Children")?.ok_or_else(|| anyhow!("loser"))?;
        for artist in [winner, loser] {
            lyra_db::external_ids::upsert(
                &mut db,
                artist,
                "musicbrainz",
                "artist_id",
                "mbid-123",
                IdSource::Resolved,
            )?;
        }
        crate::services::metadata::cleanup::deduplicate_artists_by_external_id(
            &mut db,
            &crate::services::metadata::id_links::DeclaredIds::unique([(
                "musicbrainz",
                "artist_id",
            )]),
        )?;

        let local_name = |db: &DbAny| -> anyhow::Result<Option<Option<FieldValue>>> {
            Ok(lyra_db::metadata::layers::get_for_entity(db, winner)?
                .into_iter()
                .find(|layer| layer.source_id == LOCAL_SOURCE_ID)
                .and_then(|layer| layer.fields.get(&MetadataField::ArtistName).cloned()))
        };
        let both = [
            (entries[0], "Mr Children", 1),
            (entries[1], "Mr.Children", 2),
        ]
        .map(|(entry, artist, number)| {
            track_metadata(entry.0, "Atomic Heart", artist, None, Some(1), number)
        });
        apply_metadata(&mut db, library_id, both.to_vec())?;
        assert_eq!(local_name(&db)?, Some(Some("Mr Children".into())));
        apply_metadata(&mut db, library_id, vec![both[1].clone()])?;
        assert_eq!(local_name(&db)?, Some(Some("Mr Children".into())));
        assert_eq!(
            lyra_db::artists::get_by_id(&db, winner)?
                .ok_or_else(|| anyhow!("winner"))?
                .artist_name,
            "Mr Children"
        );
        Ok(())
    }

    #[test]
    fn a_provider_split_wins_the_artist_role_until_unmatched() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let library = test_db::insert_test_library_node(
            &mut db,
            "Reconciliation",
            PathBuf::from("/music/combined"),
        )?;
        let library_id = library.db_id.unwrap();
        let entry = insert_entry(&mut db, "/music/combined/track.flac")?;
        connect(&mut db, library_id, entry)?;
        let metadata = track_metadata(entry.0, "Combined Album", "imase and なとり", None, None, 1);
        apply_metadata(&mut db, library_id, vec![metadata.clone()])?;
        let release_id: DbId = select_releases(&db)?[0].db_id.clone().unwrap().into();
        let track_id: DbId = lyra_db::tracks::get(&db, release_id)?[0]
            .db_id
            .clone()
            .unwrap()
            .into();
        let combined = artist_named(&db, "imase and なとり")?.ok_or_else(|| anyhow!("combined"))?;
        upsert_provider(&mut db, "test", true)?;
        let split = vec![
            lyra_db::fixtures::insert_artist(&mut db, "imase")?,
            lyra_db::fixtures::insert_artist(&mut db, "なとり")?,
        ];
        let credits = lyra_db::metadata::layers::LayerCredits::from([(
            lyra_db::CreditType::Artist,
            split
                .iter()
                .map(
                    |&artist_id| lyra_db::metadata::layers::edges::LayerCreditInput {
                        artist_id,
                        detail: None,
                    },
                )
                .collect(),
        )]);
        for owner in [release_id, track_id] {
            crate::services::metadata::layers::save_provider_layer(
                &mut db,
                owner,
                "test",
                &crate::services::metadata::layers::ProviderValues {
                    credits: credits.clone(),
                    ..Default::default()
                },
                &HashMap::new(),
                &Default::default(),
            )?;
        }
        let artists_of = |db: &DbAny, owner: DbId| -> anyhow::Result<Vec<DbId>> {
            Ok(lyra_db::artists::get(db, owner)?
                .into_iter()
                .map(|artist| DbId::from(artist.db_id.unwrap()))
                .collect())
        };
        for owner in [release_id, track_id] {
            assert_eq!(artists_of(&db, owner)?, split);
        }

        apply_metadata(&mut db, library_id, vec![metadata])?;
        cleanup_orphaned_metadata(&mut db)?;
        for owner in [release_id, track_id] {
            assert_eq!(artists_of(&db, owner)?, split, "a rescan keeps the split");
        }
        assert!(
            lyra_db::artists::get_by_id(&db, combined)?.is_some(),
            "the local layer still names the combined artist"
        );

        for owner in [release_id, track_id] {
            crate::services::metadata::layers::clear_provider_layer(&mut db, owner, "test")?;
            assert_eq!(artists_of(&db, owner)?, vec![combined]);
        }
        Ok(())
    }

    fn audio_properties(
        sample_rate_hz: Option<u32>,
        channel_count: Option<u32>,
        bit_depth: Option<u32>,
        bitrate_bps: Option<u32>,
    ) -> SourceFormat {
        SourceFormat {
            sample_rate_hz,
            channel_count,
            bit_depth,
            bitrate_bps,
            ..SourceFormat::default()
        }
    }

    fn source_format_of(db: &DbAny, track: &Track) -> anyhow::Result<SourceFormat> {
        let track_db_id: DbId = track.db_id.clone().expect("track has db_id").into();
        let sources = lyra_db::track_sources::get_by_track(db, track_db_id)?;
        assert_eq!(sources.len(), 1);
        Ok(sources[0].format())
    }

    fn track_metadata_with_audio_properties(
        entry_db_id: DbId,
        title: &str,
        format: SourceFormat,
    ) -> TrackMetadata {
        TrackMetadata {
            entry_db_id,
            format,
            album: Some("Audio Props Album".to_string()),
            album_artists: Some(vec!["Audio Props Artist".to_string()]),
            date: None,
            year: Some(2024),
            title: Some(title.to_string()),
            artists: Some(vec!["Audio Props Artist".to_string()]),
            artist_relations: Vec::new(),
            disc: Some(1),
            disc_total: Some(1),
            track: Some(1),
            track_total: Some(1),
            duration_ms: Some(60_000),
            genres: None,
            label: None,
            catalog_number: None,
            media_formats: Vec::new(),
            barcode: None,
            source_kind: Some("embedded_tags".to_string()),
            source_key: format!("entry:{}:embedded", entry_db_id.0),
            segment_start_ms: None,
            segment_end_ms: None,
            cue_sheet_entry_id: None,
            cue_sheet_hash: None,
            cue_track_no: None,
            cue_audio_entry_id: None,
            cue_index00_frames: None,
            cue_index01_frames: None,
            track_gain_db: None,
            album_gain_db: None,
        }
    }

    #[test]
    fn apply_metadata_propagates_multichannel_high_rate_24bit() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let library = test_db::insert_test_library_node(
            &mut db,
            "Audio Props Library",
            PathBuf::from("/music/audio_props"),
        )?;
        let library_db_id = library.db_id.expect("test library has db_id");

        let entry_db_id = insert_entry(&mut db, "/music/audio_props/surround.flac")?;
        connect(&mut db, library_db_id, entry_db_id)?;

        apply_metadata(
            &mut db,
            library_db_id,
            vec![track_metadata_with_audio_properties(
                entry_db_id,
                "Surround Track",
                audio_properties(Some(96_000), Some(6), Some(24), Some(4_500_000)),
            )],
        )?;

        let releases = select_releases(&db)?;
        let release_db_id: DbId = releases[0].db_id.clone().unwrap().into();
        let tracks = lyra_db::tracks::get(&db, release_db_id)?;
        assert_eq!(tracks.len(), 1);
        assert_eq!(
            source_format_of(&db, &tracks[0])?,
            audio_properties(Some(96_000), Some(6), Some(24), Some(4_500_000))
        );

        Ok(())
    }

    #[test]
    fn apply_metadata_preserves_none_bit_depth_for_lossy_sources() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let library = test_db::insert_test_library_node(
            &mut db,
            "Lossy Library",
            PathBuf::from("/music/lossy"),
        )?;
        let library_db_id = library.db_id.expect("test library has db_id");

        let entry_db_id = insert_entry(&mut db, "/music/lossy/song.mp3")?;
        connect(&mut db, library_db_id, entry_db_id)?;

        apply_metadata(
            &mut db,
            library_db_id,
            vec![track_metadata_with_audio_properties(
                entry_db_id,
                "Lossy Track",
                audio_properties(Some(44_100), Some(2), None, Some(128_000)),
            )],
        )?;

        let releases = select_releases(&db)?;
        let release_db_id: DbId = releases[0].db_id.clone().unwrap().into();
        let tracks = lyra_db::tracks::get(&db, release_db_id)?;
        assert_eq!(
            source_format_of(&db, &tracks[0])?,
            audio_properties(Some(44_100), Some(2), None, Some(128_000))
        );

        Ok(())
    }

    #[test]
    fn apply_metadata_refreshes_audio_properties_on_reingest() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let library = test_db::insert_test_library_node(
            &mut db,
            "Refresh Library",
            PathBuf::from("/music/refresh"),
        )?;
        let library_db_id = library.db_id.expect("test library has db_id");

        let entry_db_id = insert_entry(&mut db, "/music/refresh/track.flac")?;
        connect(&mut db, library_db_id, entry_db_id)?;

        apply_metadata(
            &mut db,
            library_db_id,
            vec![track_metadata_with_audio_properties(
                entry_db_id,
                "Refresh Track",
                audio_properties(Some(44_100), Some(2), Some(16), Some(900_000)),
            )],
        )?;

        let releases = select_releases(&db)?;
        let release_db_id: DbId = releases[0].db_id.clone().unwrap().into();
        let tracks = lyra_db::tracks::get(&db, release_db_id)?;
        assert_eq!(
            source_format_of(&db, &tracks[0])?,
            audio_properties(Some(44_100), Some(2), Some(16), Some(900_000))
        );

        apply_metadata(
            &mut db,
            library_db_id,
            vec![track_metadata_with_audio_properties(
                entry_db_id,
                "Refresh Track",
                audio_properties(Some(192_000), Some(8), Some(24), Some(6_000_000)),
            )],
        )?;

        let tracks_after = lyra_db::tracks::get(&db, release_db_id)?;
        assert_eq!(tracks_after.len(), 1);
        assert_eq!(
            source_format_of(&db, &tracks_after[0])?,
            audio_properties(Some(192_000), Some(8), Some(24), Some(6_000_000))
        );

        Ok(())
    }

    #[test]
    fn apply_metadata_replaces_gains_on_reingest() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let library = test_db::insert_test_library_node(
            &mut db,
            "Gain Library",
            PathBuf::from("/music/gain"),
        )?;
        let library_db_id = library.db_id.expect("test library has db_id");
        let entry_db_id = insert_entry(&mut db, "/music/gain/track.flac")?;
        connect(&mut db, library_db_id, entry_db_id)?;

        let with_gains = |track_gain_db, album_gain_db| TrackMetadata {
            track_gain_db,
            album_gain_db,
            ..track_metadata_with_audio_properties(
                entry_db_id,
                "Gain Track",
                audio_properties(None, None, None, None),
            )
        };

        apply_metadata(
            &mut db,
            library_db_id,
            vec![with_gains(Some(-6.5), Some(-7.25))],
        )?;
        let releases = select_releases(&db)?;
        let release_db_id: DbId = releases[0].db_id.clone().unwrap().into();
        let tracks = lyra_db::tracks::get(&db, release_db_id)?;
        assert_eq!(tracks[0].track_gain_db, Some(-6.5));
        assert_eq!(tracks[0].album_gain_db, Some(-7.25));

        apply_metadata(&mut db, library_db_id, vec![with_gains(None, None)])?;
        let tracks = lyra_db::tracks::get(&db, release_db_id)?;
        assert_eq!(tracks[0].track_gain_db, None);
        assert_eq!(tracks[0].album_gain_db, None);

        Ok(())
    }

    /// Cross-track label/cat# selection pulls both fields from the same
    /// track — never a Frankensteined pairing that exists in no source.
    #[test]
    fn apply_metadata_pulls_label_and_cat_number_from_same_track() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let library =
            test_db::insert_test_library_node(&mut db, "Test Library", PathBuf::from("/music"))?;
        let library_db_id = library
            .db_id
            .ok_or_else(|| anyhow!("library missing db_id"))?;

        let entry_a = insert_entry(&mut db, "/music/Split/01.flac")?;
        let entry_b = insert_entry(&mut db, "/music/Split/02.flac")?;
        connect(&mut db, library_db_id, entry_a)?;
        connect(&mut db, library_db_id, entry_b)?;

        let make_track =
            |entry_id: DbId, track_no: u32, label: Option<&str>, cat: Option<&str>| TrackMetadata {
                entry_db_id: entry_id,
                format: Default::default(),
                album: Some("Split".to_string()),
                album_artists: Some(vec!["Split Band".to_string()]),
                date: None,
                year: Some(2024),
                title: Some(format!("Track {track_no}")),
                artists: Some(vec!["Split Band".to_string()]),
                artist_relations: Vec::new(),
                disc: Some(1),
                disc_total: Some(1),
                track: Some(track_no),
                track_total: Some(2),
                duration_ms: Some(180_000),
                genres: None,
                label: label.map(str::to_string),
                catalog_number: cat.map(str::to_string),
                media_formats: Vec::new(),
                barcode: None,
                source_kind: Some("embedded_tags".to_string()),
                source_key: format!("entry:{}:embedded", entry_id.0),
                segment_start_ms: None,
                segment_end_ms: None,
                cue_sheet_entry_id: None,
                cue_sheet_hash: None,
                cue_track_no: None,
                cue_audio_entry_id: None,
                cue_index00_frames: None,
                cue_index01_frames: None,
                track_gain_db: None,
                album_gain_db: None,
            };

        apply_metadata(
            &mut db,
            library_db_id,
            vec![
                make_track(entry_a, 1, Some("Blue Note"), None),
                make_track(entry_b, 2, Some("Impulse!"), Some("A-77")),
            ],
        )?;

        let release = select_releases(&db)?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("release missing"))?;
        let release_db_id: DbId = release
            .db_id
            .ok_or_else(|| anyhow!("release missing db_id"))?
            .into();

        let labels = lyra_db::labels::get_for_release(&db, release_db_id)?;
        assert_eq!(labels.len(), 1);
        assert_eq!(labels[0].label.name, "Blue Note");
        assert!(
            labels[0].catalog_number.is_none(),
            "cat# must come from the same track as the label"
        );
        Ok(())
    }

    /// When the labeled track lacks a cat#, recovery scans other tracks
    /// with the same label (case-insensitive) for a usable one.
    #[test]
    fn apply_metadata_fills_cat_number_from_same_label_on_other_track() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let library =
            test_db::insert_test_library_node(&mut db, "Test Library", PathBuf::from("/music"))?;
        let library_db_id = library
            .db_id
            .ok_or_else(|| anyhow!("library missing db_id"))?;

        let entry_a = insert_entry(&mut db, "/music/BN/01.flac")?;
        let entry_b = insert_entry(&mut db, "/music/BN/02.flac")?;
        connect(&mut db, library_db_id, entry_a)?;
        connect(&mut db, library_db_id, entry_b)?;

        let make_track = |entry_id: DbId, track_no: u32, cat: Option<&str>| TrackMetadata {
            entry_db_id: entry_id,
            format: Default::default(),
            album: Some("BN".to_string()),
            album_artists: Some(vec!["Artist".to_string()]),
            date: None,
            year: Some(1957),
            title: Some(format!("Track {track_no}")),
            artists: Some(vec!["Artist".to_string()]),
            artist_relations: Vec::new(),
            disc: Some(1),
            disc_total: Some(1),
            track: Some(track_no),
            track_total: Some(2),
            duration_ms: Some(180_000),
            genres: None,
            label: Some("Blue Note".to_string()),
            catalog_number: cat.map(str::to_string),
            media_formats: Vec::new(),
            barcode: None,
            source_kind: Some("embedded_tags".to_string()),
            source_key: format!("entry:{}:embedded", entry_id.0),
            segment_start_ms: None,
            segment_end_ms: None,
            cue_sheet_entry_id: None,
            cue_sheet_hash: None,
            cue_track_no: None,
            cue_audio_entry_id: None,
            cue_index00_frames: None,
            cue_index01_frames: None,
            track_gain_db: None,
            album_gain_db: None,
        };

        apply_metadata(
            &mut db,
            library_db_id,
            vec![
                make_track(entry_a, 1, None),
                make_track(entry_b, 2, Some("BN-1577")),
            ],
        )?;

        let release = select_releases(&db)?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("release missing"))?;
        let release_db_id: DbId = release
            .db_id
            .ok_or_else(|| anyhow!("release missing db_id"))?
            .into();

        let labels = lyra_db::labels::get_for_release(&db, release_db_id)?;
        assert_eq!(labels.len(), 1);
        assert_eq!(labels[0].label.name, "Blue Note");
        assert_eq!(labels[0].catalog_number.as_deref(), Some("BN-1577"));
        Ok(())
    }

    /// Cross-track cat# match uses NFC + lowercase so non-ASCII case pairs
    /// (`Éditions Mego` vs `éditions mego`) converge — storage dedups the
    /// same way, so the ingestion-time match must agree.
    #[test]
    fn apply_metadata_cross_track_cat_number_match_is_unicode_aware() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let library =
            test_db::insert_test_library_node(&mut db, "Test Library", PathBuf::from("/music"))?;
        let library_db_id = library
            .db_id
            .ok_or_else(|| anyhow!("library missing db_id"))?;

        let entry_a = insert_entry(&mut db, "/music/EM/01.flac")?;
        let entry_b = insert_entry(&mut db, "/music/EM/02.flac")?;
        connect(&mut db, library_db_id, entry_a)?;
        connect(&mut db, library_db_id, entry_b)?;

        let make_track =
            |entry_id: DbId, track_no: u32, label: &str, cat: Option<&str>| TrackMetadata {
                entry_db_id: entry_id,
                format: Default::default(),
                album: Some("EM".to_string()),
                album_artists: Some(vec!["Artist".to_string()]),
                date: None,
                year: Some(2010),
                title: Some(format!("Track {track_no}")),
                artists: Some(vec!["Artist".to_string()]),
                artist_relations: Vec::new(),
                disc: Some(1),
                disc_total: Some(1),
                track: Some(track_no),
                track_total: Some(2),
                duration_ms: Some(180_000),
                genres: None,
                label: Some(label.to_string()),
                catalog_number: cat.map(str::to_string),
                media_formats: Vec::new(),
                barcode: None,
                source_kind: Some("embedded_tags".to_string()),
                source_key: format!("entry:{}:embedded", entry_id.0),
                segment_start_ms: None,
                segment_end_ms: None,
                cue_sheet_entry_id: None,
                cue_sheet_hash: None,
                cue_track_no: None,
                cue_audio_entry_id: None,
                cue_index00_frames: None,
                cue_index01_frames: None,
                track_gain_db: None,
                album_gain_db: None,
            };

        apply_metadata(
            &mut db,
            library_db_id,
            vec![
                make_track(entry_a, 1, "Éditions Mego", None),
                make_track(entry_b, 2, "éditions mego", Some("EMEGO-001")),
            ],
        )?;

        let release = select_releases(&db)?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("release missing"))?;
        let release_db_id: DbId = release
            .db_id
            .ok_or_else(|| anyhow!("release missing db_id"))?
            .into();

        let labels = lyra_db::labels::get_for_release(&db, release_db_id)?;
        assert_eq!(labels.len(), 1);
        assert_eq!(labels[0].catalog_number.as_deref(), Some("EMEGO-001"));
        Ok(())
    }

    #[tokio::test]
    async fn add_metadata_ingests_cue_virtual_tracks() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let dir_path =
            std::env::temp_dir().join(format!("lyra-cue-test-{}-{}", std::process::id(), nanos));
        fs::create_dir_all(&dir_path)?;

        let audio_path = dir_path.join("album.flac");
        let cue_path = dir_path.join("album.cue");
        fs::write(&audio_path, b"placeholder-audio")?;
        fs::write(
            &cue_path,
            r#"PERFORMER "Cue Artist"
TITLE "Cue Album"
FILE "album.flac" WAVE
  TRACK 01 AUDIO
    TITLE "Cue Track 1"
    INDEX 01 00:00:00
  TRACK 02 AUDIO
    TITLE "Cue Track 2"
    INDEX 01 02:00:00
"#,
        )?;

        let library = test_db::insert_test_library_node(&mut db, "Cue Library", dir_path.clone())?;

        let dir_id = insert_entry_with_kind(&mut db, &dir_path, lyra_db::entries::EntryKind::Dir)?;
        let audio_id =
            insert_entry_with_kind(&mut db, &audio_path, lyra_db::entries::EntryKind::File)?;
        let cue_id = insert_entry_with_kind(&mut db, &cue_path, lyra_db::entries::EntryKind::File)?;

        connect(&mut db, library.db_id.unwrap(), dir_id)?;
        connect(&mut db, dir_id, audio_id)?;
        connect(&mut db, dir_id, cue_id)?;

        add_metadata(&mut db, &library, vec![dir_id, audio_id, cue_id]).await?;

        let releases = select_releases(&db)?;
        assert_eq!(releases.len(), 1);
        assert_eq!(releases[0].release_title, "Cue Album");
        let release_db_id: DbId = releases[0].db_id.clone().unwrap().into();

        let mut tracks = lyra_db::tracks::get(&db, release_db_id)?;
        tracks.sort_by_key(|track| track.track.unwrap_or(0));
        assert_eq!(tracks.len(), 2);
        assert_eq!(tracks[0].track_title, "Cue Track 1");
        assert_eq!(tracks[1].track_title, "Cue Track 2");

        let track_one_id: DbId = tracks[0].db_id.clone().unwrap().into();
        let track_one_entries = lyra_db::entries::get_by_track(&db, track_one_id)?;
        assert_eq!(track_one_entries.len(), 1);
        assert_eq!(track_one_entries[0].full_path, audio_path);

        let track_one_sources = lyra_db::track_sources::get_by_track(&db, track_one_id)?;
        assert_eq!(track_one_sources.len(), 1);
        assert_eq!(track_one_sources[0].source_kind, "cue");
        assert_eq!(track_one_sources[0].start_ms, Some(0));
        assert_eq!(track_one_sources[0].end_ms, Some(120_000));

        let track_two_id: DbId = tracks[1].db_id.clone().unwrap().into();
        let track_two_sources = lyra_db::track_sources::get_by_track(&db, track_two_id)?;
        assert_eq!(track_two_sources.len(), 1);
        assert_eq!(track_two_sources[0].source_kind, "cue");
        assert_eq!(track_two_sources[0].start_ms, Some(120_000));
        assert_eq!(track_two_sources[0].end_ms, None);

        let cue_sheets: Vec<lyra_db::CueSheet> =
            roots::members::<lyra_db::CueSheet>(&db, Root::CueSheets)?;
        assert_eq!(cue_sheets.len(), 1);
        let cue_sheet_id = cue_sheets[0]
            .db_id
            .ok_or_else(|| anyhow!("cue sheet missing db_id"))?;
        assert!(lyra_db::graph::edge_exists(&db, cue_sheet_id, cue_id)?);

        let cue_tracks: Vec<lyra_db::CueTrack> =
            roots::members::<lyra_db::CueTrack>(&db, Root::CueTracks)?;
        assert_eq!(cue_tracks.len(), 2);
        let cue_track_numbers: HashSet<u32> =
            cue_tracks.iter().map(|track| track.track_no).collect();
        assert_eq!(cue_track_numbers, HashSet::from([1, 2]));
        for cue_track in cue_tracks {
            let cue_track_id = cue_track
                .db_id
                .ok_or_else(|| anyhow!("cue track missing db_id"))?;
            assert!(lyra_db::graph::edge_exists(
                &db,
                cue_sheet_id,
                cue_track_id
            )?);
            assert!(lyra_db::graph::edge_exists(&db, cue_track_id, audio_id)?);
        }

        let _ = fs::remove_dir_all(&dir_path);
        Ok(())
    }

    #[tokio::test]
    async fn add_metadata_ingests_bom_cue_with_mixed_unicode_file_refs() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let dir_path = std::env::temp_dir().join(format!(
            "lyra-cue-unicode-test-{}-{}",
            std::process::id(),
            nanos
        ));
        fs::create_dir_all(&dir_path)?;

        let audio_one = dir_path.join("01 alpha.flac");
        let audio_two = dir_path.join("02 beta.flac");
        let audio_three = dir_path.join("03 Cafe\u{301}.flac");
        let audio_four = dir_path.join("04 Pin\u{303}ata.flac");
        let cue_path = dir_path.join("Cafe\u{301} Album.cue");

        for path in [&audio_one, &audio_two, &audio_three, &audio_four] {
            fs::write(path, b"placeholder-audio")?;
        }

        let cue_text = "\u{feff}PERFORMER \"Cue Artist\"
TITLE \"Caf\u{e9} Album\"
FILE \"01 alpha.flac\" WAVE
  TRACK 01 AUDIO
    TITLE \"Cue Track 1\"
    INDEX 01 00:00:00
FILE \"02 beta.flac\" WAVE
  TRACK 02 AUDIO
    TITLE \"Cue Track 2\"
    INDEX 01 01:00:00
FILE \"03 Caf\u{e9}.flac\" WAVE
  TRACK 03 AUDIO
    TITLE \"Cue Track 3\"
    INDEX 01 02:00:00
FILE \"04 Pi\u{f1}ata.flac\" WAVE
  TRACK 04 AUDIO
    TITLE \"Cue Track 4\"
    INDEX 01 03:00:00
";
        fs::write(&cue_path, cue_text)?;

        let library =
            test_db::insert_test_library_node(&mut db, "Cue Unicode Library", dir_path.clone())?;

        let dir_id = insert_entry_with_kind(&mut db, &dir_path, lyra_db::entries::EntryKind::Dir)?;
        let audio_one_id =
            insert_entry_with_kind(&mut db, &audio_one, lyra_db::entries::EntryKind::File)?;
        let audio_two_id =
            insert_entry_with_kind(&mut db, &audio_two, lyra_db::entries::EntryKind::File)?;
        let audio_three_id =
            insert_entry_with_kind(&mut db, &audio_three, lyra_db::entries::EntryKind::File)?;
        let audio_four_id =
            insert_entry_with_kind(&mut db, &audio_four, lyra_db::entries::EntryKind::File)?;
        let cue_id = insert_entry_with_kind(&mut db, &cue_path, lyra_db::entries::EntryKind::File)?;

        connect(&mut db, library.db_id.unwrap(), dir_id)?;
        for id in [
            audio_one_id,
            audio_two_id,
            audio_three_id,
            audio_four_id,
            cue_id,
        ] {
            connect(&mut db, dir_id, id)?;
        }

        add_metadata(
            &mut db,
            &library,
            vec![
                dir_id,
                audio_one_id,
                audio_two_id,
                audio_three_id,
                audio_four_id,
                cue_id,
            ],
        )
        .await?;

        let releases = select_releases(&db)?;
        assert_eq!(releases.len(), 1);
        assert_eq!(releases[0].release_title, "Caf\u{e9} Album");
        let release_db_id: DbId = releases[0].db_id.clone().unwrap().into();

        let mut tracks = lyra_db::tracks::get(&db, release_db_id)?;
        tracks.sort_by_key(|track| track.track.unwrap_or(0));
        assert_eq!(tracks.len(), 4);
        let track_numbers: Vec<u32> = tracks.iter().filter_map(|track| track.track).collect();
        assert_eq!(track_numbers, vec![1, 2, 3, 4]);
        for track in tracks {
            let track_db_id: DbId = track.db_id.clone().unwrap().into();
            let sources = lyra_db::track_sources::get_by_track(&db, track_db_id)?;
            assert_eq!(sources.len(), 1);
            assert_eq!(sources[0].source_kind, "cue");
        }

        let _ = fs::remove_dir_all(&dir_path);
        Ok(())
    }

    #[test]
    fn apply_metadata_groups_unicode_equivalent_release_titles() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let library = test_db::insert_test_library_node(
            &mut db,
            "Unicode Grouping Library",
            PathBuf::from("/music"),
        )?;
        let library_db_id = library
            .db_id
            .ok_or_else(|| anyhow!("library missing db_id"))?;

        let entry_one = insert_entry(&mut db, "/music/unicode/01.flac")?;
        let entry_two = insert_entry(&mut db, "/music/unicode/02.flac")?;
        connect(&mut db, library_db_id, entry_one)?;
        connect(&mut db, library_db_id, entry_two)?;

        let metadata = vec![
            TrackMetadata {
                entry_db_id: entry_one,
                format: Default::default(),
                album: Some("Cafe\u{301} Album".to_string()),
                album_artists: Some(vec!["Artist".to_string()]),
                date: None,
                year: Some(2024),
                title: Some("Track 1".to_string()),
                artists: Some(vec!["Artist".to_string()]),
                artist_relations: Vec::new(),
                disc: Some(1),
                disc_total: Some(1),
                track: Some(1),
                track_total: Some(2),
                duration_ms: Some(60_000),
                genres: None,
                label: None,
                catalog_number: None,
                media_formats: Vec::new(),
                barcode: None,
                source_kind: Some("embedded_tags".to_string()),
                source_key: format!("entry:{}:embedded", entry_one.0),
                segment_start_ms: None,
                segment_end_ms: None,
                cue_sheet_entry_id: None,
                cue_sheet_hash: None,
                cue_track_no: None,
                cue_audio_entry_id: None,
                cue_index00_frames: None,
                cue_index01_frames: None,
                track_gain_db: None,
                album_gain_db: None,
            },
            TrackMetadata {
                entry_db_id: entry_two,
                format: Default::default(),
                album: Some("Caf\u{e9} Album".to_string()),
                album_artists: Some(vec!["Artist".to_string()]),
                date: None,
                year: Some(2024),
                title: Some("Track 2".to_string()),
                artists: Some(vec!["Artist".to_string()]),
                artist_relations: Vec::new(),
                disc: Some(1),
                disc_total: Some(1),
                track: Some(2),
                track_total: Some(2),
                duration_ms: Some(60_000),
                genres: None,
                label: None,
                catalog_number: None,
                media_formats: Vec::new(),
                barcode: None,
                source_kind: Some("embedded_tags".to_string()),
                source_key: format!("entry:{}:embedded", entry_two.0),
                segment_start_ms: None,
                segment_end_ms: None,
                cue_sheet_entry_id: None,
                cue_sheet_hash: None,
                cue_track_no: None,
                cue_audio_entry_id: None,
                cue_index00_frames: None,
                cue_index01_frames: None,
                track_gain_db: None,
                album_gain_db: None,
            },
        ];

        apply_metadata(&mut db, library_db_id, metadata)?;

        let releases = select_releases(&db)?;
        assert_eq!(releases.len(), 1);
        assert_eq!(releases[0].release_title, "Caf\u{e9} Album");

        let release_db_id: DbId = releases[0]
            .db_id
            .clone()
            .ok_or_else(|| anyhow!("release missing db_id"))?
            .into();
        assert_eq!(lyra_db::tracks::get(&db, release_db_id)?.len(), 2);

        Ok(())
    }

    #[test]
    fn apply_metadata_infers_disc_total_from_max_disc_when_missing() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let library = test_db::insert_test_library_node(
            &mut db,
            "Disc Total Inference Library",
            PathBuf::from("/music"),
        )?;
        let library_db_id = library
            .db_id
            .ok_or_else(|| anyhow!("library missing db_id"))?;

        let entry_one = insert_entry(&mut db, "/music/disc-test/disc1-track1.flac")?;
        let entry_two = insert_entry(&mut db, "/music/disc-test/disc2-track1.flac")?;
        connect(&mut db, library_db_id, entry_one)?;
        connect(&mut db, library_db_id, entry_two)?;

        let metadata = vec![
            TrackMetadata {
                entry_db_id: entry_one,
                format: Default::default(),
                album: Some("Disc Test".to_string()),
                album_artists: Some(vec!["Artist".to_string()]),
                date: None,
                year: Some(2024),
                title: Some("Disc 1 Track 1".to_string()),
                artists: Some(vec!["Artist".to_string()]),
                artist_relations: Vec::new(),
                disc: Some(1),
                disc_total: None,
                track: Some(1),
                track_total: Some(1),
                duration_ms: Some(60_000),
                genres: None,
                label: None,
                catalog_number: None,
                media_formats: Vec::new(),
                barcode: None,
                source_kind: Some("embedded_tags".to_string()),
                source_key: format!("entry:{}:embedded", entry_one.0),
                segment_start_ms: None,
                segment_end_ms: None,
                cue_sheet_entry_id: None,
                cue_sheet_hash: None,
                cue_track_no: None,
                cue_audio_entry_id: None,
                cue_index00_frames: None,
                cue_index01_frames: None,
                track_gain_db: None,
                album_gain_db: None,
            },
            TrackMetadata {
                entry_db_id: entry_two,
                format: Default::default(),
                album: Some("Disc Test".to_string()),
                album_artists: Some(vec!["Artist".to_string()]),
                date: None,
                year: Some(2024),
                title: Some("Disc 2 Track 1".to_string()),
                artists: Some(vec!["Artist".to_string()]),
                artist_relations: Vec::new(),
                disc: Some(2),
                disc_total: None,
                track: Some(1),
                track_total: Some(1),
                duration_ms: Some(60_000),
                genres: None,
                label: None,
                catalog_number: None,
                media_formats: Vec::new(),
                barcode: None,
                source_kind: Some("embedded_tags".to_string()),
                source_key: format!("entry:{}:embedded", entry_two.0),
                segment_start_ms: None,
                segment_end_ms: None,
                cue_sheet_entry_id: None,
                cue_sheet_hash: None,
                cue_track_no: None,
                cue_audio_entry_id: None,
                cue_index00_frames: None,
                cue_index01_frames: None,
                track_gain_db: None,
                album_gain_db: None,
            },
        ];

        apply_metadata(&mut db, library_db_id, metadata)?;

        let releases = select_releases(&db)?;
        assert_eq!(releases.len(), 1);
        let release_db_id: DbId = releases[0]
            .db_id
            .clone()
            .ok_or_else(|| anyhow!("release missing db_id"))?
            .into();

        let tracks = lyra_db::tracks::get(&db, release_db_id)?;
        assert_eq!(tracks.len(), 2);
        assert!(tracks.iter().all(|track| track.disc_total == Some(2)));

        Ok(())
    }

    #[test]
    fn apply_metadata_reinfers_disc_total_when_the_explicit_tag_is_removed() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let library = test_db::insert_test_library_node(
            &mut db,
            "Disc Total Preserve Library",
            PathBuf::from("/music"),
        )?;
        let library_db_id = library
            .db_id
            .ok_or_else(|| anyhow!("library missing db_id"))?;

        let entry_one = insert_entry(&mut db, "/music/disc-test-preserve/disc1-track1.flac")?;
        let entry_two = insert_entry(&mut db, "/music/disc-test-preserve/disc2-track1.flac")?;
        connect(&mut db, library_db_id, entry_one)?;
        connect(&mut db, library_db_id, entry_two)?;

        let first_scan = vec![
            TrackMetadata {
                entry_db_id: entry_one,
                format: Default::default(),
                album: Some("Disc Preserve".to_string()),
                album_artists: Some(vec!["Artist".to_string()]),
                date: None,
                year: Some(2024),
                title: Some("Disc 1 Track 1".to_string()),
                artists: Some(vec!["Artist".to_string()]),
                artist_relations: Vec::new(),
                disc: Some(1),
                disc_total: Some(5),
                track: Some(1),
                track_total: Some(1),
                duration_ms: Some(60_000),
                genres: None,
                label: None,
                catalog_number: None,
                media_formats: Vec::new(),
                barcode: None,
                source_kind: Some("embedded_tags".to_string()),
                source_key: format!("entry:{}:embedded", entry_one.0),
                segment_start_ms: None,
                segment_end_ms: None,
                cue_sheet_entry_id: None,
                cue_sheet_hash: None,
                cue_track_no: None,
                cue_audio_entry_id: None,
                cue_index00_frames: None,
                cue_index01_frames: None,
                track_gain_db: None,
                album_gain_db: None,
            },
            TrackMetadata {
                entry_db_id: entry_two,
                format: Default::default(),
                album: Some("Disc Preserve".to_string()),
                album_artists: Some(vec!["Artist".to_string()]),
                date: None,
                year: Some(2024),
                title: Some("Disc 2 Track 1".to_string()),
                artists: Some(vec!["Artist".to_string()]),
                artist_relations: Vec::new(),
                disc: Some(2),
                disc_total: Some(5),
                track: Some(1),
                track_total: Some(1),
                duration_ms: Some(60_000),
                genres: None,
                label: None,
                catalog_number: None,
                media_formats: Vec::new(),
                barcode: None,
                source_kind: Some("embedded_tags".to_string()),
                source_key: format!("entry:{}:embedded", entry_two.0),
                segment_start_ms: None,
                segment_end_ms: None,
                cue_sheet_entry_id: None,
                cue_sheet_hash: None,
                cue_track_no: None,
                cue_audio_entry_id: None,
                cue_index00_frames: None,
                cue_index01_frames: None,
                track_gain_db: None,
                album_gain_db: None,
            },
        ];
        apply_metadata(&mut db, library_db_id, first_scan)?;

        let second_scan = vec![
            TrackMetadata {
                entry_db_id: entry_one,
                format: Default::default(),
                album: Some("Disc Preserve".to_string()),
                album_artists: Some(vec!["Artist".to_string()]),
                date: None,
                year: Some(2024),
                title: Some("Disc 1 Track 1".to_string()),
                artists: Some(vec!["Artist".to_string()]),
                artist_relations: Vec::new(),
                disc: Some(1),
                disc_total: None,
                track: Some(1),
                track_total: Some(1),
                duration_ms: Some(60_000),
                genres: None,
                label: None,
                catalog_number: None,
                media_formats: Vec::new(),
                barcode: None,
                source_kind: Some("embedded_tags".to_string()),
                source_key: format!("entry:{}:embedded", entry_one.0),
                segment_start_ms: None,
                segment_end_ms: None,
                cue_sheet_entry_id: None,
                cue_sheet_hash: None,
                cue_track_no: None,
                cue_audio_entry_id: None,
                cue_index00_frames: None,
                cue_index01_frames: None,
                track_gain_db: None,
                album_gain_db: None,
            },
            TrackMetadata {
                entry_db_id: entry_two,
                format: Default::default(),
                album: Some("Disc Preserve".to_string()),
                album_artists: Some(vec!["Artist".to_string()]),
                date: None,
                year: Some(2024),
                title: Some("Disc 2 Track 1".to_string()),
                artists: Some(vec!["Artist".to_string()]),
                artist_relations: Vec::new(),
                disc: Some(2),
                disc_total: None,
                track: Some(1),
                track_total: Some(1),
                duration_ms: Some(60_000),
                genres: None,
                label: None,
                catalog_number: None,
                media_formats: Vec::new(),
                barcode: None,
                source_kind: Some("embedded_tags".to_string()),
                source_key: format!("entry:{}:embedded", entry_two.0),
                segment_start_ms: None,
                segment_end_ms: None,
                cue_sheet_entry_id: None,
                cue_sheet_hash: None,
                cue_track_no: None,
                cue_audio_entry_id: None,
                cue_index00_frames: None,
                cue_index01_frames: None,
                track_gain_db: None,
                album_gain_db: None,
            },
        ];
        apply_metadata(&mut db, library_db_id, second_scan)?;

        let releases = select_releases(&db)?;
        assert_eq!(releases.len(), 1);
        let release_db_id: DbId = releases[0]
            .db_id
            .clone()
            .ok_or_else(|| anyhow!("release missing db_id"))?
            .into();

        let tracks = lyra_db::tracks::get(&db, release_db_id)?;
        assert_eq!(tracks.len(), 2);
        assert!(tracks.iter().all(|track| track.disc_total == Some(2)));

        Ok(())
    }

    #[test]
    fn coalesce_disc_groups_merges_non_overlapping_discs_and_rewrites_release_title() {
        let groups = vec![
            parsed_group(
                "/music/release/DISC1",
                vec![track_metadata(
                    1,
                    "Release Title",
                    "Artist",
                    Some(2024),
                    Some(1),
                    1,
                )],
            ),
            parsed_group(
                "/music/release/DISC2",
                vec![track_metadata(
                    2,
                    "Bonus Disc Title",
                    "Artist",
                    Some(2024),
                    Some(2),
                    1,
                )],
            ),
        ];

        let coalesced = coalesce_disc_groups(groups);
        assert_eq!(coalesced.len(), 1);
        assert_eq!(coalesced[0].len(), 2);
        assert!(
            coalesced[0]
                .iter()
                .all(|track| track.album.as_deref() == Some("Release Title"))
        );
    }

    #[test]
    fn coalesce_disc_groups_vetoes_resolved_disc_track_overlap() {
        let groups = vec![
            parsed_group(
                "/music/release/DISC1",
                vec![track_metadata(
                    1,
                    "Disc One",
                    "Artist",
                    Some(2024),
                    Some(1),
                    1,
                )],
            ),
            parsed_group(
                "/music/release/DISC2",
                vec![track_metadata(
                    2,
                    "Disc Two",
                    "Artist",
                    Some(2024),
                    Some(1),
                    1,
                )],
            ),
        ];

        let coalesced = coalesce_disc_groups(groups);
        assert_eq!(coalesced.len(), 2);
        assert!(coalesced.iter().all(|batch| batch.len() == 1));
    }

    #[test]
    fn coalesce_disc_groups_uses_folder_fallback_for_missing_disc_overlap() {
        let groups = vec![
            parsed_group(
                "/music/release/DISC1",
                vec![track_metadata(
                    1,
                    "Release Title",
                    "Artist",
                    Some(2024),
                    None,
                    1,
                )],
            ),
            parsed_group(
                "/music/release/DISC2",
                vec![track_metadata(
                    2,
                    "Disc Two Title",
                    "Artist",
                    Some(2024),
                    None,
                    1,
                )],
            ),
        ];

        let coalesced = coalesce_disc_groups(groups);
        assert_eq!(coalesced.len(), 1);
        assert_eq!(coalesced[0].len(), 2);
        assert!(
            coalesced[0]
                .iter()
                .all(|track| track.album.as_deref() == Some("Release Title"))
        );
    }

    #[test]
    fn coalesce_disc_groups_rejects_ambiguous_fallback_when_disc_missing() {
        let groups = vec![
            parsed_group(
                "/music/release/ESCL-5394",
                vec![track_metadata(
                    1,
                    "Release Title",
                    "Artist",
                    Some(2024),
                    None,
                    1,
                )],
            ),
            parsed_group(
                "/music/release/ESCL-5395",
                vec![track_metadata(
                    2,
                    "Disc Two Title",
                    "Artist",
                    Some(2024),
                    None,
                    1,
                )],
            ),
        ];

        let coalesced = coalesce_disc_groups(groups);
        assert_eq!(coalesced.len(), 2);
        assert!(coalesced.iter().all(|batch| batch.len() == 1));
    }

    #[test]
    fn coalesce_disc_groups_splits_mixed_folder_titles_and_merges_matching_main_title() {
        let groups = vec![
            parsed_group(
                "/music/212",
                vec![
                    track_metadata(1, "212", "nameless; とあ", Some(2015), None, 1),
                    track_metadata(2, "212", "nameless; とあ", Some(2015), None, 2),
                ],
            ),
            parsed_group(
                "/music/212/Bonus",
                vec![
                    track_metadata(3, "212 ボカロver.CD", "nameless; とあ", Some(2015), None, 1),
                    track_metadata(4, "212 ボカロver.CD", "nameless; とあ", Some(2015), None, 2),
                    track_metadata(5, "212", "nameless; とあ", Some(2015), None, 13),
                ],
            ),
        ];

        let coalesced = coalesce_disc_groups(groups);
        assert_eq!(coalesced.len(), 2);

        let merged_main = coalesced
            .iter()
            .find(|batch| batch.iter().any(|track| track.track == Some(13)))
            .expect("expected a merged main-release batch containing track 13");
        let mut main_tracks: Vec<u32> =
            merged_main.iter().filter_map(|track| track.track).collect();
        main_tracks.sort_unstable();
        assert_eq!(main_tracks, vec![1, 2, 13]);
        assert!(
            merged_main
                .iter()
                .all(|track| track.album.as_deref() == Some("212"))
        );

        let bonus_only = coalesced
            .iter()
            .find(|batch| {
                batch
                    .iter()
                    .all(|track| track.album.as_deref() == Some("212 ボカロver.CD"))
            })
            .expect("expected separate bonus release batch");
        let mut bonus_tracks: Vec<u32> =
            bonus_only.iter().filter_map(|track| track.track).collect();
        bonus_tracks.sort_unstable();
        assert_eq!(bonus_tracks, vec![1, 2]);
    }

    #[test]
    fn a_rescan_keeps_provider_release_and_track_values() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let library =
            test_db::insert_test_library_node(&mut db, "Test Library", PathBuf::from("/music"))?;
        let library_db_id = library
            .db_id
            .ok_or_else(|| anyhow!("library missing db_id"))?;

        let entry_db_id = insert_entry(&mut db, "/music/Artist/Album/01 - Track.flac")?;
        connect(&mut db, library_db_id, entry_db_id)?;

        let first_scan = TrackMetadata {
            entry_db_id,
            format: Default::default(),
            album: Some("Album".to_string()),
            album_artists: Some(vec!["Artist".to_string()]),
            date: None,
            year: Some(2001),
            title: Some("Track".to_string()),
            artists: Some(vec!["Artist".to_string()]),
            artist_relations: Vec::new(),
            disc: Some(1),
            disc_total: Some(1),
            track: Some(1),
            track_total: Some(1),
            duration_ms: Some(180_000),
            genres: None,
            label: None,
            catalog_number: None,
            media_formats: Vec::new(),
            barcode: None,
            source_kind: Some("embedded_tags".to_string()),
            source_key: format!("entry:{}:embedded", entry_db_id.0),
            segment_start_ms: None,
            segment_end_ms: None,
            cue_sheet_entry_id: None,
            cue_sheet_hash: None,
            cue_track_no: None,
            cue_audio_entry_id: None,
            cue_index00_frames: None,
            cue_index01_frames: None,
            track_gain_db: None,
            album_gain_db: None,
        };
        apply_metadata(&mut db, library_db_id, vec![first_scan])?;

        let release = select_releases(&db)?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("release missing after first scan"))?;
        let release_db_id: DbId = release
            .db_id
            .ok_or_else(|| anyhow!("release missing db_id"))?
            .into();
        let track = lyra_db::tracks::get(&db, release_db_id)?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("track missing after first scan"))?;
        let track_db_id: DbId = track
            .db_id
            .ok_or_else(|| anyhow!("track missing db_id"))?
            .into();

        lyra_db::providers::upsert(
            &mut db,
            &ProviderConfig {
                db_id: None,
                provider_id: "test".to_string(),
                position: 0,
                enabled: true,
            },
        )?;

        lyra_db::metadata::layers::upsert(
            &mut db,
            track_db_id,
            &MetadataLayer {
                source_id: "test".to_string(),
                relations: Default::default(),
                credits: Default::default(),
                fields: crate::services::metadata::layers::json::fields_from_json(
                    serde_json::json!({
                        "track_title": "Provider Track",
                        "year": 1999,
                        "disc": 9,
                        "track": 12
                    }),
                ),
                updated_at_ms: 1,
            },
        )?;
        lyra_db::metadata::layers::upsert(
            &mut db,
            release_db_id,
            &MetadataLayer {
                source_id: "test".to_string(),
                relations: Default::default(),
                credits: Default::default(),
                fields: crate::services::metadata::layers::json::fields_from_json(
                    serde_json::json!({
                        "release_title": "Provider Release",
                        "release_date": "1988-08-08"
                    }),
                ),
                updated_at_ms: 1,
            },
        )?;

        crate::services::metadata::merging::apply_merged_metadata_to_entity(&mut db, track_db_id)?;
        crate::services::metadata::merging::apply_merged_metadata_to_entity(
            &mut db,
            release_db_id,
        )?;
        let track_after_provider = lyra_db::tracks::get_by_id(&db, track_db_id)?
            .ok_or_else(|| anyhow!("track missing"))?;
        let release_after_provider = lyra_db::releases::get_by_id(&db, release_db_id)?
            .ok_or_else(|| anyhow!("release missing"))?;
        assert_eq!(release_after_provider.release_title, "Provider Release");
        assert_eq!(
            release_after_provider.release_date.as_deref(),
            Some("1988-08-08")
        );
        assert_eq!(track_after_provider.track_title, "Provider Track");
        assert_eq!(track_after_provider.year, Some(1999));
        assert_eq!(track_after_provider.disc, Some(9));
        assert_eq!(track_after_provider.track, Some(12));

        let second_scan = TrackMetadata {
            entry_db_id,
            format: Default::default(),
            album: Some("Album".to_string()),
            album_artists: Some(vec!["Artist".to_string()]),
            date: None,
            year: Some(2005),
            title: Some("Track".to_string()),
            artists: Some(vec!["Artist".to_string()]),
            artist_relations: Vec::new(),
            disc: Some(1),
            disc_total: Some(1),
            track: Some(1),
            track_total: Some(1),
            duration_ms: Some(180_000),
            genres: None,
            label: None,
            catalog_number: None,
            media_formats: Vec::new(),
            barcode: None,
            source_kind: Some("embedded_tags".to_string()),
            source_key: format!("entry:{}:embedded", entry_db_id.0),
            segment_start_ms: None,
            segment_end_ms: None,
            cue_sheet_entry_id: None,
            cue_sheet_hash: None,
            cue_track_no: None,
            cue_audio_entry_id: None,
            cue_index00_frames: None,
            cue_index01_frames: None,
            track_gain_db: None,
            album_gain_db: None,
        };
        apply_metadata(&mut db, library_db_id, vec![second_scan])?;

        let track_after_rescan = lyra_db::tracks::get_by_id(&db, track_db_id)?
            .ok_or_else(|| anyhow!("track missing"))?;
        let release_after_rescan = lyra_db::releases::get_by_id(&db, release_db_id)?
            .ok_or_else(|| anyhow!("release missing"))?;
        assert_eq!(release_after_rescan.release_title, "Provider Release");
        assert_eq!(
            release_after_rescan.release_date.as_deref(),
            Some("1988-08-08")
        );
        assert_eq!(track_after_rescan.track_title, "Provider Track");
        assert_eq!(track_after_rescan.year, Some(1999));
        assert_eq!(track_after_rescan.disc, Some(9));
        assert_eq!(track_after_rescan.track, Some(12));

        Ok(())
    }

    /// Tags that vanish from the file must vanish from storage. agdb omits
    /// `None` fields from inserts, so a rescan that clears a field only takes
    /// effect if `update` removes the stored key.
    #[test]
    fn rescan_clears_release_and_track_fields_whose_tags_were_removed() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let library =
            test_db::insert_test_library_node(&mut db, "Test Library", PathBuf::from("/music"))?;
        let library_db_id = library
            .db_id
            .ok_or_else(|| anyhow!("library missing db_id"))?;

        let entry_db_id = insert_entry(&mut db, "/music/Artist/Album/01 - Track.flac")?;
        connect(&mut db, library_db_id, entry_db_id)?;

        let tagged = TrackMetadata {
            entry_db_id,
            format: Default::default(),
            album: Some("Album".to_string()),
            album_artists: Some(vec!["Artist".to_string()]),
            date: Some("1999-05-04".to_string()),
            year: Some(1999),
            title: Some("Track".to_string()),
            artists: Some(vec!["Artist".to_string()]),
            artist_relations: Vec::new(),
            disc: Some(1),
            disc_total: Some(1),
            track: Some(7),
            track_total: Some(12),
            duration_ms: Some(180_000),
            genres: None,
            label: None,
            catalog_number: None,
            media_formats: Vec::new(),
            barcode: None,
            source_kind: Some("embedded_tags".to_string()),
            source_key: format!("entry:{}:embedded", entry_db_id.0),
            segment_start_ms: None,
            segment_end_ms: None,
            cue_sheet_entry_id: None,
            cue_sheet_hash: None,
            cue_track_no: None,
            cue_audio_entry_id: None,
            cue_index00_frames: None,
            cue_index01_frames: None,
            track_gain_db: None,
            album_gain_db: None,
        };
        apply_metadata(&mut db, library_db_id, vec![tagged])?;

        let release = select_releases(&db)?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("release missing after first scan"))?;
        let release_db_id: DbId = release
            .db_id
            .ok_or_else(|| anyhow!("release missing db_id"))?
            .into();
        let track = lyra_db::tracks::get(&db, release_db_id)?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("track missing after first scan"))?;
        let track_db_id: DbId = track
            .db_id
            .ok_or_else(|| anyhow!("track missing db_id"))?
            .into();
        assert_eq!(track.year, Some(1999));
        assert_eq!(track.track, Some(7));
        assert_eq!(
            lyra_db::releases::get_by_id(&db, release_db_id)?
                .and_then(|release| release.release_date)
                .as_deref(),
            Some("1999-05-04")
        );

        let stripped = TrackMetadata {
            entry_db_id,
            format: Default::default(),
            album: Some("Album".to_string()),
            album_artists: Some(vec!["Artist".to_string()]),
            date: None,
            year: None,
            title: Some("Track".to_string()),
            artists: Some(vec!["Artist".to_string()]),
            artist_relations: Vec::new(),
            disc: None,
            disc_total: None,
            track: None,
            track_total: None,
            duration_ms: None,
            genres: None,
            label: None,
            catalog_number: None,
            media_formats: Vec::new(),
            barcode: None,
            source_kind: Some("embedded_tags".to_string()),
            source_key: format!("entry:{}:embedded", entry_db_id.0),
            segment_start_ms: None,
            segment_end_ms: None,
            cue_sheet_entry_id: None,
            cue_sheet_hash: None,
            cue_track_no: None,
            cue_audio_entry_id: None,
            cue_index00_frames: None,
            cue_index01_frames: None,
            track_gain_db: None,
            album_gain_db: None,
        };
        apply_metadata(&mut db, library_db_id, vec![stripped])?;

        let track_after = lyra_db::tracks::get_by_id(&db, track_db_id)?
            .ok_or_else(|| anyhow!("track missing"))?;
        assert_eq!(track_after.year, None);
        assert_eq!(track_after.track, None);
        assert_eq!(track_after.track_total, None);
        assert_eq!(track_after.track_title, "Track");

        // Duration is not a tag: an unresolved probe must not wipe the value a previous scan
        // established.
        assert_eq!(track_after.duration_ms, Some(180_000));
        assert!(
            track_after.file_created_at_ms.is_some(),
            "file_created_at_ms must survive a rescan"
        );

        let release_after = lyra_db::releases::get_by_id(&db, release_db_id)?
            .ok_or_else(|| anyhow!("release missing"))?;
        assert_eq!(release_after.release_date, None);
        assert_eq!(release_after.release_title, "Album");

        Ok(())
    }

    /// A rescan's tag labels rank below a provider's, so enriched labels
    /// survive every library scan.
    #[test]
    fn a_rescan_keeps_provider_labels() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let library =
            test_db::insert_test_library_node(&mut db, "Test Library", PathBuf::from("/music"))?;
        let library_db_id = library
            .db_id
            .ok_or_else(|| anyhow!("library missing db_id"))?;

        let entry_db_id = insert_entry(&mut db, "/music/Artist/Album/01.flac")?;
        connect(&mut db, library_db_id, entry_db_id)?;

        let first_scan = TrackMetadata {
            entry_db_id,
            format: Default::default(),
            album: Some("Album".to_string()),
            album_artists: Some(vec!["Artist".to_string()]),
            date: None,
            year: Some(2001),
            title: Some("Track".to_string()),
            artists: Some(vec!["Artist".to_string()]),
            artist_relations: Vec::new(),
            disc: Some(1),
            disc_total: Some(1),
            track: Some(1),
            track_total: Some(1),
            duration_ms: Some(180_000),
            genres: None,
            label: Some("Tag Label".to_string()),
            catalog_number: Some("TAG-001".to_string()),
            media_formats: Vec::new(),
            barcode: None,
            source_kind: Some("embedded_tags".to_string()),
            source_key: format!("entry:{}:embedded", entry_db_id.0),
            segment_start_ms: None,
            segment_end_ms: None,
            cue_sheet_entry_id: None,
            cue_sheet_hash: None,
            cue_track_no: None,
            cue_audio_entry_id: None,
            cue_index00_frames: None,
            cue_index01_frames: None,
            track_gain_db: None,
            album_gain_db: None,
        };
        apply_metadata(&mut db, library_db_id, vec![first_scan])?;

        let release = select_releases(&db)?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("release missing after first scan"))?;
        let release_db_id: DbId = release
            .db_id
            .ok_or_else(|| anyhow!("release missing db_id"))?
            .into();

        let tag_labels = lyra_db::labels::get_for_release(&db, release_db_id)?;
        assert_eq!(tag_labels.len(), 1);
        assert_eq!(tag_labels[0].label.name, "Tag Label");

        lyra_db::providers::upsert(
            &mut db,
            &ProviderConfig {
                db_id: None,
                provider_id: "test".to_string(),
                position: 0,
                enabled: true,
            },
        )?;
        let layer = MetadataLayer {
            source_id: "test".to_string(),
            relations: Default::default(),
            credits: Default::default(),
            fields: crate::services::metadata::layers::json::resolved_fields_from_json(
                &mut db,
                serde_json::json!({
                    "labels": [
                        {"name": "Provider Label", "catalog_number": "PRV-001"}
                    ]
                }),
            ),
            updated_at_ms: 1,
        };
        lyra_db::metadata::layers::upsert(&mut db, release_db_id, &layer)?;
        crate::services::metadata::merging::apply_merged_metadata_to_entity(
            &mut db,
            release_db_id,
        )?;

        let after_provider = lyra_db::labels::get_for_release(&db, release_db_id)?;
        assert_eq!(after_provider[0].label.name, "Provider Label");

        let second_scan = TrackMetadata {
            entry_db_id,
            format: Default::default(),
            album: Some("Album".to_string()),
            album_artists: Some(vec!["Artist".to_string()]),
            date: None,
            year: Some(2001),
            title: Some("Track".to_string()),
            artists: Some(vec!["Artist".to_string()]),
            artist_relations: Vec::new(),
            disc: Some(1),
            disc_total: Some(1),
            track: Some(1),
            track_total: Some(1),
            duration_ms: Some(180_000),
            genres: None,
            label: Some("Different Tag Label".to_string()),
            catalog_number: Some("TAG-999".to_string()),
            media_formats: Vec::new(),
            barcode: None,
            source_kind: Some("embedded_tags".to_string()),
            source_key: format!("entry:{}:embedded", entry_db_id.0),
            segment_start_ms: None,
            segment_end_ms: None,
            cue_sheet_entry_id: None,
            cue_sheet_hash: None,
            cue_track_no: None,
            cue_audio_entry_id: None,
            cue_index00_frames: None,
            cue_index01_frames: None,
            track_gain_db: None,
            album_gain_db: None,
        };
        apply_metadata(&mut db, library_db_id, vec![second_scan])?;

        let after_rescan = lyra_db::labels::get_for_release(&db, release_db_id)?;
        assert_eq!(after_rescan.len(), 1);
        assert_eq!(after_rescan[0].label.name, "Provider Label");
        assert_eq!(after_rescan[0].catalog_number.as_deref(), Some("PRV-001"));
        Ok(())
    }

    fn single_file_library(db: &mut DbAny, path: &str) -> anyhow::Result<(DbId, DbId)> {
        let library =
            test_db::insert_test_library_node(db, "Test Library", PathBuf::from("/music"))?;
        let library_db_id = library
            .db_id
            .ok_or_else(|| anyhow!("library missing db_id"))?;
        let entry_db_id = insert_entry(db, path)?;
        connect(db, library_db_id, entry_db_id)?;
        Ok((library_db_id, entry_db_id))
    }

    fn tagged_track(entry_db_id: DbId) -> TrackMetadata {
        let mut meta = track_metadata(entry_db_id.0, "Album", "Artist", Some(1999), Some(1), 7);
        meta.date = Some("1999-05-04".to_string());
        meta.track_total = Some(12);
        meta.genres = Some(vec!["Jazz".to_string()]);
        meta.label = Some("Blue Note".to_string());
        meta.catalog_number = Some("BN-1".to_string());
        meta
    }

    fn only_release(db: &DbAny) -> anyhow::Result<DbId> {
        Ok(select_releases(db)?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("release missing"))?
            .db_id
            .ok_or_else(|| anyhow!("release missing db_id"))?
            .into())
    }

    fn only_track(db: &DbAny, release_db_id: DbId) -> anyhow::Result<DbId> {
        Ok(lyra_db::tracks::get(db, release_db_id)?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("track missing"))?
            .db_id
            .ok_or_else(|| anyhow!("track missing db_id"))?
            .into())
    }

    fn local_layer(db: &DbAny, node_id: DbId) -> anyhow::Result<MetadataLayer> {
        lyra_db::metadata::layers::get_for_entity(db, node_id)?
            .into_iter()
            .find(|layer| layer.source_id == LOCAL_SOURCE_ID)
            .ok_or_else(|| anyhow!("local layer missing"))
    }

    fn upsert_provider(db: &mut DbAny, provider_id: &str, enabled: bool) -> anyhow::Result<()> {
        lyra_db::providers::upsert(
            db,
            &ProviderConfig {
                db_id: None,
                provider_id: provider_id.to_string(),
                position: 0,
                enabled,
            },
        )
        .map(|_| ())
    }

    #[test]
    fn import_without_providers_resolves_entities_from_the_local_layer() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let (library_db_id, entry_db_id) = single_file_library(&mut db, "/music/Album/01.flac")?;

        apply_metadata(&mut db, library_db_id, vec![tagged_track(entry_db_id)])?;

        let release_db_id = only_release(&db)?;
        let release = lyra_db::releases::get_by_id(&db, release_db_id)?
            .ok_or_else(|| anyhow!("release missing"))?;
        assert_eq!(release.release_title, "Album");
        assert_eq!(release.release_date.as_deref(), Some("1999-05-04"));

        let genres = lyra_db::genres::get_for_release(&db, release_db_id)?;
        assert_eq!(genres.len(), 1);
        assert_eq!(genres[0].name, "Jazz");
        let labels = lyra_db::labels::get_for_release(&db, release_db_id)?;
        assert_eq!(labels.len(), 1);
        assert_eq!(labels[0].label.name, "Blue Note");
        assert_eq!(labels[0].catalog_number.as_deref(), Some("BN-1"));

        let track_db_id = only_track(&db, release_db_id)?;
        let track =
            lyra_db::tracks::get_by_id(&db, track_db_id)?.ok_or_else(|| anyhow!("track"))?;
        assert_eq!(track.track_title, "Track 7");
        assert_eq!(track.year, Some(1999));
        assert_eq!(track.disc, Some(1));
        assert_eq!(track.disc_total, Some(1));
        assert_eq!(track.track, Some(7));
        assert_eq!(track.track_total, Some(12));

        let artists = lyra_db::artists::get(&db, release_db_id)?;
        assert_eq!(artists.len(), 1);
        assert_eq!(artists[0].artist_name, "Artist");

        let layer_fields = local_layer(&db, track_db_id)?.fields;
        assert_eq!(layer_fields[&MetadataField::Year], Some(1999.into()));
        assert!(
            !layer_fields.contains_key(&MetadataField::SortTitle),
            "an untagged field must stay absent instead of null"
        );
        Ok(())
    }

    #[test]
    fn disabling_a_provider_restores_the_local_value() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let (library_db_id, entry_db_id) = single_file_library(&mut db, "/music/Album/01.flac")?;
        apply_metadata(&mut db, library_db_id, vec![tagged_track(entry_db_id)])?;

        let release_db_id = only_release(&db)?;
        let track_db_id = only_track(&db, release_db_id)?;
        upsert_provider(&mut db, "test", true)?;
        lyra_db::metadata::layers::upsert(
            &mut db,
            track_db_id,
            &MetadataLayer {
                source_id: "test".to_string(),
                relations: Default::default(),
                credits: Default::default(),
                fields: crate::services::metadata::layers::json::fields_from_json(
                    serde_json::json!({"track_title": "Provider Track", "year": 2020}),
                ),
                updated_at_ms: 1,
            },
        )?;
        crate::services::metadata::merging::apply_merged_metadata_to_entity(&mut db, track_db_id)?;
        let track =
            lyra_db::tracks::get_by_id(&db, track_db_id)?.ok_or_else(|| anyhow!("track"))?;
        assert_eq!(track.track_title, "Provider Track");
        assert_eq!(track.year, Some(2020));

        upsert_provider(&mut db, "test", false)?;
        crate::services::metadata::merging::apply_merged_metadata_to_entity(&mut db, track_db_id)?;

        let track =
            lyra_db::tracks::get_by_id(&db, track_db_id)?.ok_or_else(|| anyhow!("track"))?;
        assert_eq!(track.track_title, "Track 7");
        assert_eq!(track.year, Some(1999));
        Ok(())
    }

    #[test]
    fn rescanning_an_unchanged_file_rewrites_nothing() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let (library_db_id, entry_db_id) = single_file_library(&mut db, "/music/Album/01.flac")?;
        apply_metadata(&mut db, library_db_id, vec![tagged_track(entry_db_id)])?;

        let release_db_id = only_release(&db)?;
        let track_db_id = only_track(&db, release_db_id)?;
        let release_layer = local_layer(&db, release_db_id)?;
        let track_layer = local_layer(&db, track_db_id)?;
        let track_before =
            lyra_db::tracks::get_by_id(&db, track_db_id)?.ok_or_else(|| anyhow!("t"))?;

        apply_metadata(&mut db, library_db_id, vec![tagged_track(entry_db_id)])?;

        let release_layer_after = local_layer(&db, release_db_id)?;
        let track_layer_after = local_layer(&db, track_db_id)?;
        assert_eq!(release_layer_after.fields, release_layer.fields);
        assert_eq!(
            release_layer_after.updated_at_ms,
            release_layer.updated_at_ms
        );
        assert_eq!(track_layer_after.fields, track_layer.fields);
        assert_eq!(track_layer_after.updated_at_ms, track_layer.updated_at_ms);
        assert_eq!(only_release(&db)?, release_db_id);
        assert_eq!(only_track(&db, release_db_id)?, track_db_id);
        let track_after =
            lyra_db::tracks::get_by_id(&db, track_db_id)?.ok_or_else(|| anyhow!("t"))?;
        assert_eq!(track_after.track_title, track_before.track_title);
        assert_eq!(track_after.year, track_before.year);
        assert_eq!(track_after.track, track_before.track);
        Ok(())
    }

    fn save_manual(db: &mut DbAny, node_id: DbId, fields: serde_json::Value) -> anyhow::Result<()> {
        let fields = crate::services::metadata::layers::json::resolved_fields_from_json(db, fields);
        lyra_db::metadata::layers::upsert(
            db,
            node_id,
            &lyra_db::MetadataLayer::new(MANUAL_SOURCE_ID, fields, 1),
        )?;
        Ok(())
    }

    #[test]
    fn a_manual_edit_survives_a_rescan() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let (library_db_id, entry_db_id) = single_file_library(&mut db, "/music/Album/01.flac")?;
        apply_metadata(&mut db, library_db_id, vec![tagged_track(entry_db_id)])?;

        let release_db_id = only_release(&db)?;
        let track_db_id = only_track(&db, release_db_id)?;
        save_manual(
            &mut db,
            track_db_id,
            serde_json::json!({"track_title": "Manual Title"}),
        )?;
        crate::services::metadata::merging::apply_merged_metadata_to_entity(&mut db, track_db_id)?;

        apply_metadata(&mut db, library_db_id, vec![tagged_track(entry_db_id)])?;

        let track =
            lyra_db::tracks::get_by_id(&db, track_db_id)?.ok_or_else(|| anyhow!("track"))?;
        assert_eq!(track.track_title, "Manual Title");
        Ok(())
    }

    #[test]
    fn dropping_a_manual_edit_falls_back_to_the_local_layer() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let (library_db_id, entry_db_id) = single_file_library(&mut db, "/music/Album/01.flac")?;
        apply_metadata(&mut db, library_db_id, vec![tagged_track(entry_db_id)])?;

        let release_db_id = only_release(&db)?;
        let track_db_id = only_track(&db, release_db_id)?;
        save_manual(&mut db, track_db_id, serde_json::json!({"year": 2020}))?;
        save_manual(
            &mut db,
            release_db_id,
            serde_json::json!({"genres": ["Manual Genre"]}),
        )?;
        crate::services::metadata::merging::apply_merged_metadata_to_entity(&mut db, track_db_id)?;
        crate::services::metadata::merging::apply_merged_metadata_to_entity(
            &mut db,
            release_db_id,
        )?;
        assert_eq!(
            lyra_db::tracks::get_by_id(&db, track_db_id)?
                .ok_or_else(|| anyhow!("track"))?
                .year,
            Some(2020)
        );

        for node_id in [track_db_id, release_db_id] {
            db.transaction_mut(|t| {
                lyra_db::metadata::layers::remove_inside_tx(t, node_id, MANUAL_SOURCE_ID)
            })?;
        }
        crate::services::metadata::merging::apply_merged_metadata_to_entity(&mut db, track_db_id)?;
        crate::services::metadata::merging::apply_merged_metadata_to_entity(
            &mut db,
            release_db_id,
        )?;

        assert_eq!(
            lyra_db::tracks::get_by_id(&db, track_db_id)?
                .ok_or_else(|| anyhow!("track"))?
                .year,
            Some(1999)
        );
        let genres = lyra_db::genres::get_for_release(&db, release_db_id)?;
        assert_eq!(genres.len(), 1);
        assert_eq!(genres[0].name, "Jazz");
        Ok(())
    }

    #[test]
    fn rescanning_one_disc_keeps_the_release_wide_disc_total() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let (library_db_id, disc_one_entry) =
            single_file_library(&mut db, "/music/Album/disc1/01.flac")?;
        let disc_two_entry = insert_entry(&mut db, "/music/Album/disc2/01.flac")?;
        connect(&mut db, library_db_id, disc_two_entry)?;

        apply_metadata(
            &mut db,
            library_db_id,
            vec![
                track_metadata(disc_one_entry.0, "Album", "Artist", Some(1999), Some(1), 1),
                track_metadata(disc_two_entry.0, "Album", "Artist", Some(1999), Some(2), 1),
            ],
        )?;
        let release_db_id = only_release(&db)?;
        assert!(
            lyra_db::tracks::get(&db, release_db_id)?
                .iter()
                .all(|track| track.disc_total == Some(2))
        );

        apply_metadata(
            &mut db,
            library_db_id,
            vec![track_metadata(
                disc_one_entry.0,
                "Album",
                "Artist",
                Some(1999),
                Some(1),
                1,
            )],
        )?;

        assert!(
            lyra_db::tracks::get(&db, release_db_id)?
                .iter()
                .all(|track| track.disc_total == Some(2))
        );
        Ok(())
    }

    /// Merged-metadata reapply must skip the labels sync when the
    /// release is locked — dedup-path layer migrations would otherwise
    /// wipe curated labels.
    #[test]
    fn apply_merged_metadata_to_entity_skips_labels_on_locked_release() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let library =
            test_db::insert_test_library_node(&mut db, "Test Library", PathBuf::from("/music"))?;
        let library_db_id = library
            .db_id
            .ok_or_else(|| anyhow!("library missing db_id"))?;

        let entry_db_id = insert_entry(&mut db, "/music/Locked/01.flac")?;
        connect(&mut db, library_db_id, entry_db_id)?;

        let scan = TrackMetadata {
            entry_db_id,
            format: Default::default(),
            album: Some("Album".to_string()),
            album_artists: Some(vec!["Artist".to_string()]),
            date: None,
            year: Some(2001),
            title: Some("Track".to_string()),
            artists: Some(vec!["Artist".to_string()]),
            artist_relations: Vec::new(),
            disc: Some(1),
            disc_total: Some(1),
            track: Some(1),
            track_total: Some(1),
            duration_ms: Some(180_000),
            genres: None,
            label: Some("Curated Label".to_string()),
            catalog_number: Some("CUR-001".to_string()),
            media_formats: Vec::new(),
            barcode: None,
            source_kind: Some("embedded_tags".to_string()),
            source_key: format!("entry:{}:embedded", entry_db_id.0),
            segment_start_ms: None,
            segment_end_ms: None,
            cue_sheet_entry_id: None,
            cue_sheet_hash: None,
            cue_track_no: None,
            cue_audio_entry_id: None,
            cue_index00_frames: None,
            cue_index01_frames: None,
            track_gain_db: None,
            album_gain_db: None,
        };
        apply_metadata(&mut db, library_db_id, vec![scan])?;

        let release = select_releases(&db)?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("release missing"))?;
        let release_db_id: DbId = release
            .db_id
            .clone()
            .ok_or_else(|| anyhow!("release missing db_id"))?
            .into();

        let mut locked_release = release;
        locked_release.locked = Some(true);
        lyra_db::releases::update(&mut db, &locked_release)?;

        lyra_db::providers::upsert(
            &mut db,
            &ProviderConfig {
                db_id: None,
                provider_id: "test".to_string(),
                position: 0,
                enabled: true,
            },
        )?;
        let layer = MetadataLayer {
            source_id: "test".to_string(),
            relations: Default::default(),
            credits: Default::default(),
            fields: crate::services::metadata::layers::json::resolved_fields_from_json(
                &mut db,
                serde_json::json!({
                    "labels": [
                        {"name": "Provider Override", "catalog_number": "PRV-999"}
                    ]
                }),
            ),
            updated_at_ms: 1,
        };
        lyra_db::metadata::layers::upsert(&mut db, release_db_id, &layer)?;
        crate::services::metadata::merging::apply_merged_metadata_to_entity(
            &mut db,
            release_db_id,
        )?;

        let labels = lyra_db::labels::get_for_release(&db, release_db_id)?;
        assert_eq!(labels.len(), 1);
        assert_eq!(labels[0].label.name, "Curated Label");
        assert_eq!(labels[0].catalog_number.as_deref(), Some("CUR-001"));
        Ok(())
    }

    #[test]
    fn unchanged_multi_track_rescan_skips_playlist_cover_recomputation() -> anyhow::Result<()> {
        let mut db = TestDb::initialized()?.into_inner();
        let library = test_db::insert_test_library_node(
            &mut db,
            "Unchanged Cover Library",
            PathBuf::from("/music"),
        )?;
        let library_db_id = library
            .db_id
            .ok_or_else(|| anyhow!("library missing db_id"))?;

        let mut metadata = Vec::new();
        for track_number in 1..=3 {
            let entry_db_id = insert_entry(
                &mut db,
                &format!("/music/unchanged/track-{track_number}.flac"),
            )?;
            connect(&mut db, library_db_id, entry_db_id)?;
            metadata.push(track_metadata(
                entry_db_id.0,
                "Unchanged Album",
                "Artist",
                Some(2026),
                Some(1),
                track_number,
            ));
        }

        let first = apply_metadata(&mut db, library_db_id, metadata.clone())?;
        let release_db_id = first.releases[0];
        let playlist_db_id = playlist_with_tracks(&mut db, "unchanged-covers", &first.tracks)?;
        insert_release_cover(&mut db, release_db_id, "unchanged-cover")?;
        lyra_db::covers::display::sync_playlist_cover(&mut db, playlist_db_id)?;
        assert_eq!(
            playlist_cover_winner(&db, playlist_db_id)?,
            Some(release_db_id)
        );

        lyra_db::covers::display::reset_playlist_cover_sync_count();
        let second = apply_metadata(&mut db, library_db_id, metadata)?;

        assert_eq!(second.releases, vec![release_db_id]);
        assert_eq!(
            lyra_db::covers::display::playlist_cover_sync_count(),
            0,
            "an unchanged logical release membership must not publish cover work"
        );
        assert_eq!(
            playlist_cover_winner(&db, playlist_db_id)?,
            Some(release_db_id)
        );
        Ok(())
    }

    #[test]
    fn moved_tracks_recompute_each_affected_playlist_once() -> anyhow::Result<()> {
        let mut db = TestDb::initialized()?.into_inner();
        let library = test_db::insert_test_library_node(
            &mut db,
            "Moved Cover Library",
            PathBuf::from("/music"),
        )?;
        let library_db_id = library
            .db_id
            .ok_or_else(|| anyhow!("library missing db_id"))?;

        let mut target_metadata = Vec::new();
        for track_number in 1..=3 {
            let entry_db_id =
                insert_entry(&mut db, &format!("/music/target/track-{track_number}.flac"))?;
            connect(&mut db, library_db_id, entry_db_id)?;
            target_metadata.push(track_metadata(
                entry_db_id.0,
                "Target Album",
                "Artist",
                Some(2026),
                Some(1),
                track_number,
            ));
        }
        let target = apply_metadata(&mut db, library_db_id, target_metadata.clone())?;
        let target_release_db_id = target.releases[0];

        let mut moved_metadata = Vec::new();
        for track_number in 1..=2 {
            let entry_db_id =
                insert_entry(&mut db, &format!("/music/source/track-{track_number}.flac"))?;
            connect(&mut db, library_db_id, entry_db_id)?;
            moved_metadata.push(track_metadata(
                entry_db_id.0,
                "Source Album",
                "Artist",
                Some(2026),
                Some(1),
                track_number,
            ));
        }
        let source = apply_metadata(&mut db, library_db_id, moved_metadata.clone())?;
        let source_release_db_id = source.releases[0];
        let playlist_db_id = playlist_with_tracks(&mut db, "moved-covers", &source.tracks)?;
        insert_release_cover(&mut db, source_release_db_id, "source-cover")?;
        insert_release_cover(&mut db, target_release_db_id, "target-cover")?;
        lyra_db::covers::display::sync_playlist_cover(&mut db, playlist_db_id)?;
        assert_eq!(
            playlist_cover_winner(&db, playlist_db_id)?,
            Some(source_release_db_id)
        );

        for (index, track) in moved_metadata.iter_mut().enumerate() {
            track.album = Some("Target Album".to_string());
            track.track = Some((index + 4) as u32);
        }
        target_metadata.extend(moved_metadata);

        lyra_db::covers::display::reset_playlist_cover_sync_count();
        let result = apply_metadata(&mut db, library_db_id, target_metadata)?;

        assert_eq!(result.releases, vec![target_release_db_id]);
        assert_eq!(
            lyra_db::covers::display::playlist_cover_sync_count(),
            1,
            "two moved tracks in one playlist must publish one final-state recomputation"
        );
        assert_eq!(
            playlist_cover_winner(&db, playlist_db_id)?,
            Some(target_release_db_id)
        );
        for track_db_id in source.tracks {
            let releases = lyra_db::releases::get_by_track(&db, track_db_id)?;
            assert_eq!(releases.len(), 1);
            assert_eq!(
                releases[0].db_id.clone().map(DbId::from),
                Some(target_release_db_id)
            );
        }
        Ok(())
    }
}
