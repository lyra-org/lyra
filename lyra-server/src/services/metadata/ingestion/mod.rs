// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

mod artists;
mod releases;
mod tracks;

use std::{
    collections::HashSet,
    mem,
    path::{
        Path,
        PathBuf,
    },
};

use agdb::{
    DbAny,
    DbId,
    QueryBuilder,
};

use self::{
    releases::{
        TrackIngest,
        persist_release,
    },
    tracks::build_existing_track_map,
};
use super::TrackMetadata;
use crate::db::Entry;

#[derive(Debug)]
pub(crate) struct ParsedMetadataGroup {
    pub(crate) coalesce_group_key: usize,
    pub(crate) source_dir: PathBuf,
    pub(crate) metadata: Vec<TrackMetadata>,
}

pub(crate) fn source_directory_for_group_entries(entries: &[Entry]) -> Option<PathBuf> {
    entries.iter().find_map(|entry| {
        if entry.kind != crate::db::entries::EntryKind::File {
            return None;
        }
        entry.full_path.parent().map(Path::to_path_buf)
    })
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

pub(crate) fn group_entries(
    db: &DbAny,
    library_db_id: DbId,
    entries: Vec<DbId>,
) -> anyhow::Result<Vec<Vec<Entry>>> {
    if entries.is_empty() {
        return Ok(Vec::new());
    }

    let groups: Vec<Entry> = db
        .exec(
            QueryBuilder::select()
                .elements::<Entry>()
                .search()
                .from(library_db_id)
                .where_()
                .ids(entries)
                .and()
                // group by directory
                .neighbor()
                .end_where()
                .query(),
        )?
        .try_into()?;

    let mut seen = HashSet::new();
    let mut grouped = Vec::new();
    for group in groups {
        let group_db_id = group
            .db_id
            .ok_or_else(|| anyhow::anyhow!("metadata group missing db_id"))?;
        if !seen.insert(group_db_id) {
            continue;
        }

        let entries: Vec<Entry> = db
            .exec(
                QueryBuilder::select()
                    .elements::<Entry>()
                    .search()
                    .from(group_db_id)
                    .query(),
            )?
            .try_into()?;
        grouped.push(entries);
    }

    Ok(grouped)
}

pub(crate) fn apply_metadata(
    db: &mut DbAny,
    library_db_id: DbId,
    metadata: Vec<TrackMetadata>,
) -> anyhow::Result<()> {
    if metadata.is_empty() {
        return Ok(());
    }

    let existing_tracks = build_existing_track_map(db, &metadata)?;

    let mut current_release = None;
    let mut release_tracks: Vec<TrackIngest> = Vec::new();

    for track in metadata {
        let release = lyra_metadata::normalize_unicode_nfc(track.album.as_deref().unwrap_or(""));
        let source_identity = track
            .source_key
            .clone()
            .unwrap_or_else(|| format!("entry:{}:embedded", track.entry_db_id.0));
        let track_db_id = existing_tracks.get(&source_identity).copied();

        if current_release.as_ref() != Some(&release) {
            if !release_tracks.is_empty() {
                persist_release(
                    db,
                    library_db_id,
                    current_release.as_deref().unwrap(),
                    mem::take(&mut release_tracks),
                )?;
            }

            current_release = Some(release);
        }

        release_tracks.push(TrackIngest {
            meta: track,
            track_db_id,
        });
    }

    if !release_tracks.is_empty() {
        persist_release(
            db,
            library_db_id,
            current_release.as_deref().unwrap(),
            release_tracks,
        )?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::test_db::{
        self,
        TestDb,
    };
    use crate::db::{
        self,
        Artist,
        IdSource,
        Library,
        ProviderConfig,
        Release,
        Track,
    };
    use crate::services::metadata::{
        TrackMetadata,
        cleanup::cleanup_orphaned_metadata,
        parse_metadata,
    };
    use agdb::{
        CountComparison,
        QueryBuilder,
    };
    use anyhow::anyhow;
    use nanoid::nanoid;
    use std::collections::HashSet;
    use std::fs;
    use std::path::{
        Path,
        PathBuf,
    };
    use std::time::{
        SystemTime,
        UNIX_EPOCH,
    };

    use crate::db::external_ids::ExternalId;
    use crate::db::metadata::layers::MetadataLayer;

    fn new_test_db() -> anyhow::Result<DbAny> {
        Ok(TestDb::with_root_aliases(&[
            "releases",
            "tracks",
            "artists",
            "credits",
            "providers",
            "entries",
            "track_sources",
            "cue_sheets",
            "cue_tracks",
            "genres",
            "labels",
            "release_labels",
        ])?
        .into_inner())
    }

    fn insert_entry(db: &mut DbAny, path: &str) -> anyhow::Result<DbId> {
        insert_entry_with_kind(db, Path::new(path), crate::db::entries::EntryKind::File)
    }

    fn insert_entry_with_kind(
        db: &mut DbAny,
        path: &Path,
        kind: crate::db::entries::EntryKind,
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
            file_kind: if kind == crate::db::entries::EntryKind::File {
                crate::db::entries::classify_file_kind(path)
                    .map(str::to_string)
                    .or_else(|| Some("audio".to_string()))
            } else {
                None
            },
            name,
            hash: None,
            size: 0,
            mtime: 0,
            ctime: 0,
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
            sample_rate_hz: None,
            channel_count: None,
            bit_depth: None,
            bitrate_bps: None,
            locked: Some(false),
            created_at: None,
            ctime: None,
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
            locked: Some(false),
            created_at: None,
            ctime: None,
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
            sort_name: None,
            artist_type: None,
            description: None,
            verified: false,
            locked: Some(false),
            created_at: None,
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

    fn connect_artist(db: &mut DbAny, owner: DbId, artist_id: DbId) -> anyhow::Result<()> {
        crate::db::test_db::connect_artist(db, owner, artist_id)
    }

    fn connect_track_to_entry_source(
        db: &mut DbAny,
        track_db_id: DbId,
        entry_db_id: DbId,
    ) -> anyhow::Result<()> {
        let source_key = format!("entry:{}:embedded", entry_db_id.0);
        db::track_sources::upsert(
            db,
            track_db_id,
            entry_db_id,
            db::track_sources::TrackSourceUpsert {
                source_kind: "embedded_tags".to_string(),
                source_key,
                is_primary: true,
                start_ms: None,
                end_ms: None,
            },
            None,
        )?;
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
        let tracks: Vec<Track> = db
            .exec(
                QueryBuilder::select()
                    .elements::<Track>()
                    .search()
                    .from("tracks")
                    .query(),
            )?
            .try_into()?;
        Ok(tracks)
    }

    fn select_releases(db: &DbAny) -> anyhow::Result<Vec<Release>> {
        let releases: Vec<Release> = db
            .exec(
                QueryBuilder::select()
                    .elements::<Release>()
                    .search()
                    .from("releases")
                    .query(),
            )?
            .try_into()?;
        Ok(releases)
    }

    fn select_artists(db: &DbAny) -> anyhow::Result<Vec<Artist>> {
        let artists: Vec<Artist> = db
            .exec(
                QueryBuilder::select()
                    .elements::<Artist>()
                    .search()
                    .from("artists")
                    .query(),
            )?
            .try_into()?;
        Ok(artists)
    }

    fn collection_edge_count(db: &DbAny, alias: &str) -> anyhow::Result<u64> {
        let from = db.exec(QueryBuilder::select().ids(alias).query())?.ids()[0];
        let qr = db.exec(
            QueryBuilder::search()
                .from(from)
                .where_()
                .edge()
                .and()
                .distance(CountComparison::Equal(1))
                .query(),
        )?;

        Ok(qr
            .elements
            .into_iter()
            .filter(|edge| edge.from == Some(from))
            .count() as u64)
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
            album: Some(album.to_string()),
            album_artists: Some(vec![artist.to_string()]),
            date: None,
            year,
            title: Some(format!("Track {track}")),
            artists: Some(vec![artist.to_string()]),
            disc,
            disc_total: None,
            track: Some(track),
            track_total: None,
            duration_ms: None,
            genres: None,
            label: None,
            catalog_number: None,
            source_kind: Some("embedded_tags".to_string()),
            source_key: Some(format!("entry:{}:embedded", entry_db_id.0)),
            segment_start_ms: None,
            segment_end_ms: None,
            cue_sheet_entry_id: None,
            cue_sheet_hash: None,
            cue_track_no: None,
            cue_audio_entry_id: None,
            cue_index00_frames: None,
            cue_index01_frames: None,
            sample_rate_hz: None,
            channel_count: None,
            bit_depth: None,
            bitrate_bps: None,
        }
    }

    async fn add_metadata(
        db: &mut DbAny,
        library: &Library,
        entries: Vec<DbId>,
    ) -> anyhow::Result<()> {
        let library_db_id = library
            .db_id
            .ok_or_else(|| anyhow::anyhow!("library missing db_id"))?;
        let groups = group_entries(db, library_db_id, entries)?;
        let mut parsed_groups = Vec::new();

        for (coalesce_group_key, entries) in groups.into_iter().enumerate() {
            tracing::debug!(entry_count = entries.len(), "metadata group entries");
            let source_dir = source_directory_for_group_entries(&entries)
                .unwrap_or_else(|| library.directory.clone());
            let entry_source_dirs: std::collections::BTreeMap<DbId, PathBuf> = entries
                .iter()
                .filter_map(|entry| {
                    if entry.kind != crate::db::entries::EntryKind::File {
                        return None;
                    }
                    let entry_db_id = entry.db_id?;
                    let parent = entry
                        .full_path
                        .parent()
                        .map(Path::to_path_buf)
                        .unwrap_or_else(|| source_dir.clone());
                    Some((entry_db_id, parent))
                })
                .collect();
            let parse_output = parse_metadata(entries).await?;
            if !parse_output.skipped.is_empty() {
                crate::services::metadata::log_skip_summary(&parse_output.skipped);
            }
            let metadata = parse_output.metadata;
            if metadata.is_empty() {
                continue;
            }

            let mut metadata_by_source_dir = std::collections::BTreeMap::new();
            for track in metadata {
                let track_source_dir = entry_source_dirs
                    .get(&track.entry_db_id)
                    .cloned()
                    .unwrap_or_else(|| source_dir.clone());
                metadata_by_source_dir
                    .entry(track_source_dir)
                    .or_insert_with(Vec::new)
                    .push(track);
            }

            for (source_dir, metadata) in metadata_by_source_dir {
                parsed_groups.push(ParsedMetadataGroup {
                    coalesce_group_key,
                    source_dir,
                    metadata,
                });
            }
        }

        for metadata in coalesce_disc_groups(parsed_groups) {
            if metadata.is_empty() {
                continue;
            }
            apply_metadata(db, library_db_id, metadata)?;
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

        connect(&mut db, "tracks", track_db_id)?;
        connect(&mut db, "releases", release_db_id)?;
        connect(&mut db, "artists", artist_db_id)?;
        connect(&mut db, release_db_id, track_db_id)?;
        connect_artist(&mut db, release_db_id, artist_db_id)?;
        connect_track_to_entry_source(&mut db, track_db_id, entry_db_id)?;
        connect_artist(&mut db, track_db_id, artist_db_id)?;

        db.exec_mut(QueryBuilder::remove().ids(entry_db_id).query())?;
        cleanup_orphaned_metadata(&mut db)?;

        assert!(select_tracks(&db)?.is_empty());
        assert!(select_releases(&db)?.is_empty());
        assert!(select_artists(&db)?.is_empty());
        assert_eq!(collection_edge_count(&db, "tracks")?, 0);
        assert_eq!(collection_edge_count(&db, "releases")?, 0);
        assert_eq!(collection_edge_count(&db, "artists")?, 0);

        Ok(())
    }

    #[test]
    fn cleanup_removes_orphans_with_layer_and_ext_id() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let entry_db_id = insert_entry(&mut db, "/music/album/track_layered.flac")?;
        let track_db_id = insert_track(&mut db, "Layered Track")?;
        let release_db_id = insert_release(&mut db, "Album L")?;
        let artist_db_id = insert_artist(&mut db, "Artist L")?;

        connect(&mut db, "tracks", track_db_id)?;
        connect(&mut db, "releases", release_db_id)?;
        connect(&mut db, "artists", artist_db_id)?;
        connect(&mut db, release_db_id, track_db_id)?;
        connect_artist(&mut db, release_db_id, artist_db_id)?;
        connect_track_to_entry_source(&mut db, track_db_id, entry_db_id)?;
        connect_artist(&mut db, track_db_id, artist_db_id)?;

        // Add a metadata layer attached to the track
        let layer = MetadataLayer {
            db_id: None,
            id: nanoid!(),
            provider_id: "test-provider".to_string(),
            fields: "{}".to_string(),
            updated_at: 0,
        };
        let layer_qr = db.exec_mut(QueryBuilder::insert().element(&layer).query())?;
        let layer_id = layer_qr.elements[0].id;
        connect(&mut db, track_db_id, layer_id)?;

        // Add an external ID attached to the track
        let ext = ExternalId {
            db_id: None,
            id: nanoid!(),
            provider_id: "test-provider".to_string(),
            id_type: "track_db_id".to_string(),
            id_value: "abc123".to_string(),
            source: IdSource::Plugin,
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

        connect(&mut db, "tracks", track_db_id)?;
        connect(&mut db, "releases", release_db_id)?;
        connect(&mut db, "artists", artist_db_id)?;
        connect(&mut db, release_db_id, track_db_id)?;
        connect_artist(&mut db, release_db_id, artist_db_id)?;
        connect_track_to_entry_source(&mut db, track_db_id, entry_db_id)?;

        cleanup_orphaned_metadata(&mut db)?;

        assert_eq!(select_tracks(&db)?.len(), 1);
        assert_eq!(select_releases(&db)?.len(), 1);
        assert_eq!(select_artists(&db)?.len(), 1);
        assert_eq!(collection_edge_count(&db, "tracks")?, 1);
        assert_eq!(collection_edge_count(&db, "releases")?, 1);
        assert_eq!(collection_edge_count(&db, "artists")?, 1);

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

        connect(&mut db, "tracks", track_db_id)?;
        connect(&mut db, "releases", release_keep_id)?;
        connect(&mut db, "releases", release_drop_id)?;
        connect(&mut db, "artists", artist_keep_id)?;
        connect(&mut db, "artists", artist_drop_id)?;

        connect(&mut db, release_keep_id, track_db_id)?;
        connect_artist(&mut db, release_keep_id, artist_keep_id)?;
        connect_track_to_entry_source(&mut db, track_db_id, entry_db_id)?;

        connect_artist(&mut db, release_drop_id, artist_drop_id)?;

        cleanup_orphaned_metadata(&mut db)?;

        let releases = select_releases(&db)?;
        assert_eq!(releases.len(), 1);
        assert_eq!(releases[0].release_title, "Keep Album");

        let artists = select_artists(&db)?;
        assert_eq!(artists.len(), 1);
        assert_eq!(artists[0].artist_name, "Keep Artist");
        assert_eq!(collection_edge_count(&db, "tracks")?, 1);
        assert_eq!(collection_edge_count(&db, "releases")?, 1);
        assert_eq!(collection_edge_count(&db, "artists")?, 1);

        Ok(())
    }

    #[tokio::test]
    async fn add_metadata_ingests_audio_fixture() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let fixture = fixture_path();
        assert!(fixture.is_file());
        let dir_path = fixture.parent().expect("fixture has parent").to_path_buf();

        let library = test_db::insert_test_library_node(&mut db, "Test Library", dir_path.clone())?;

        let dir_id =
            insert_entry_with_kind(&mut db, &dir_path, crate::db::entries::EntryKind::Dir)?;
        let file_id =
            insert_entry_with_kind(&mut db, &fixture, crate::db::entries::EntryKind::File)?;

        connect(&mut db, library.db_id.unwrap(), dir_id)?;
        connect(&mut db, dir_id, file_id)?;

        add_metadata(&mut db, &library, vec![dir_id, file_id]).await?;

        let releases = select_releases(&db)?;
        assert_eq!(releases.len(), 1);
        assert_eq!(releases[0].release_title, "Integration Album");

        let release_db_id: DbId = releases[0].db_id.clone().unwrap().into();
        let release_tracks = db::tracks::get(&db, release_db_id)?;
        assert_eq!(release_tracks.len(), 1);
        assert_eq!(release_tracks[0].track_title, "Integration Track");
        assert!(release_tracks[0].duration_ms.unwrap_or(0) > 0);
        assert_eq!(release_tracks[0].sample_rate_hz, Some(44_100));
        assert_eq!(release_tracks[0].channel_count, Some(1));
        assert_eq!(release_tracks[0].bit_depth, Some(16));
        assert!(
            release_tracks[0]
                .bitrate_bps
                .is_some_and(|bps| bps > 10_000),
            "scan should capture the source bitrate from the fixture (got {:?})",
            release_tracks[0].bitrate_bps
        );

        let album_artists = db::artists::get(&db, release_db_id)?;
        assert_eq!(album_artists.len(), 1);
        assert_eq!(album_artists[0].artist_name, "Integration Artist");

        let all_artists = select_artists(&db)?;
        assert_eq!(all_artists.len(), 1);

        let track_db_id: DbId = release_tracks[0].db_id.clone().unwrap().into();
        let linked_entries = db::entries::get_by_track(&db, track_db_id)?;
        assert_eq!(linked_entries.len(), 1);
        assert_eq!(linked_entries[0].full_path, fixture);

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

        let dir_id =
            insert_entry_with_kind(&mut db, &dir_path, crate::db::entries::EntryKind::Dir)?;
        connect(&mut db, library.db_id.unwrap(), dir_id)?;

        let mut entry_ids = vec![dir_id];
        for path in &file_paths {
            let file_id =
                insert_entry_with_kind(&mut db, path, crate::db::entries::EntryKind::File)?;
            connect(&mut db, dir_id, file_id)?;
            entry_ids.push(file_id);
        }

        add_metadata(&mut db, &library, entry_ids).await?;

        let releases = select_releases(&db)?;
        assert_eq!(releases.len(), 1);
        assert_eq!(releases[0].release_title, "Integration Multi Album");

        let release_db_id: DbId = releases[0].db_id.clone().unwrap().into();
        let release_tracks = db::tracks::get(&db, release_db_id)?;
        assert_eq!(release_tracks.len(), 2);
        let titles: HashSet<&str> = release_tracks
            .iter()
            .map(|track| track.track_title.as_str())
            .collect();
        assert!(titles.contains("Multi Track 1"));
        assert!(titles.contains("Multi Track 2"));

        let album_artists = db::artists::get(&db, release_db_id)?;
        assert_eq!(album_artists.len(), 1);
        assert_eq!(album_artists[0].artist_name, "Integration Album Artist");

        for track in release_tracks {
            let track_db_id: DbId = track.db_id.clone().unwrap().into();
            let track_artists = db::artists::get(&db, track_db_id)?;
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
            let linked_entries: Vec<Entry> = db::entries::get_by_track(&db, track_db_id)?;
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
                album: Some("Ordered Album".to_string()),
                album_artists: None,
                date: None,
                year: Some(2022),
                title: Some("CapSule".to_string()),
                artists: Some(vec!["Zulu Artist".to_string(), "Alpha Artist".to_string()]),
                disc: Some(1),
                disc_total: Some(1),
                track: Some(1),
                track_total: Some(1),
                duration_ms: Some(174842),
                genres: Some(vec!["Jpop".to_string()]),
                label: None,
                catalog_number: None,
                source_kind: Some("embedded_tags".to_string()),
                source_key: Some(format!("entry:{}:embedded", entry_db_id.0)),
                segment_start_ms: None,
                segment_end_ms: None,
                cue_sheet_entry_id: None,
                cue_sheet_hash: None,
                cue_track_no: None,
                cue_audio_entry_id: None,
                cue_index00_frames: None,
                cue_index01_frames: None,
                sample_rate_hz: None,
                channel_count: None,
                bit_depth: None,
                bitrate_bps: None,
            }],
        )?;

        let releases = select_releases(&db)?;
        assert_eq!(releases.len(), 1);
        let release_db_id: DbId = releases[0].db_id.clone().unwrap().into();
        let album_artists = db::artists::get(&db, release_db_id)?;
        let names: Vec<&str> = album_artists
            .iter()
            .map(|artist| artist.artist_name.as_str())
            .collect();
        assert_eq!(names, vec!["Zulu Artist", "Alpha Artist"]);

        Ok(())
    }

    fn track_metadata_with_audio_properties(
        entry_db_id: DbId,
        title: &str,
        sample_rate_hz: Option<u32>,
        channel_count: Option<u32>,
        bit_depth: Option<u32>,
        bitrate_bps: Option<u32>,
    ) -> TrackMetadata {
        TrackMetadata {
            entry_db_id,
            album: Some("Audio Props Album".to_string()),
            album_artists: Some(vec!["Audio Props Artist".to_string()]),
            date: None,
            year: Some(2024),
            title: Some(title.to_string()),
            artists: Some(vec!["Audio Props Artist".to_string()]),
            disc: Some(1),
            disc_total: Some(1),
            track: Some(1),
            track_total: Some(1),
            duration_ms: Some(60_000),
            genres: None,
            label: None,
            catalog_number: None,
            source_kind: Some("embedded_tags".to_string()),
            source_key: Some(format!("entry:{}:embedded", entry_db_id.0)),
            segment_start_ms: None,
            segment_end_ms: None,
            cue_sheet_entry_id: None,
            cue_sheet_hash: None,
            cue_track_no: None,
            cue_audio_entry_id: None,
            cue_index00_frames: None,
            cue_index01_frames: None,
            sample_rate_hz,
            channel_count,
            bit_depth,
            bitrate_bps,
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
                Some(96_000),
                Some(6),
                Some(24),
                Some(4_500_000),
            )],
        )?;

        let releases = select_releases(&db)?;
        let release_db_id: DbId = releases[0].db_id.clone().unwrap().into();
        let tracks = db::tracks::get(&db, release_db_id)?;
        assert_eq!(tracks.len(), 1);
        assert_eq!(tracks[0].sample_rate_hz, Some(96_000));
        assert_eq!(tracks[0].channel_count, Some(6));
        assert_eq!(tracks[0].bit_depth, Some(24));
        assert_eq!(tracks[0].bitrate_bps, Some(4_500_000));

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
                Some(44_100),
                Some(2),
                None,
                Some(128_000),
            )],
        )?;

        let releases = select_releases(&db)?;
        let release_db_id: DbId = releases[0].db_id.clone().unwrap().into();
        let tracks = db::tracks::get(&db, release_db_id)?;
        assert_eq!(tracks[0].sample_rate_hz, Some(44_100));
        assert_eq!(tracks[0].channel_count, Some(2));
        assert_eq!(tracks[0].bit_depth, None);
        assert_eq!(tracks[0].bitrate_bps, Some(128_000));

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
                Some(44_100),
                Some(2),
                Some(16),
                Some(900_000),
            )],
        )?;

        let releases = select_releases(&db)?;
        let release_db_id: DbId = releases[0].db_id.clone().unwrap().into();
        let tracks = db::tracks::get(&db, release_db_id)?;
        assert_eq!(tracks[0].sample_rate_hz, Some(44_100));
        assert_eq!(tracks[0].channel_count, Some(2));
        assert_eq!(tracks[0].bit_depth, Some(16));
        assert_eq!(tracks[0].bitrate_bps, Some(900_000));

        apply_metadata(
            &mut db,
            library_db_id,
            vec![track_metadata_with_audio_properties(
                entry_db_id,
                "Refresh Track",
                Some(192_000),
                Some(8),
                Some(24),
                Some(6_000_000),
            )],
        )?;

        let tracks_after = db::tracks::get(&db, release_db_id)?;
        assert_eq!(tracks_after.len(), 1);
        assert_eq!(tracks_after[0].sample_rate_hz, Some(192_000));
        assert_eq!(tracks_after[0].channel_count, Some(8));
        assert_eq!(tracks_after[0].bit_depth, Some(24));
        assert_eq!(tracks_after[0].bitrate_bps, Some(6_000_000));

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
                album: Some("Split".to_string()),
                album_artists: Some(vec!["Split Band".to_string()]),
                date: None,
                year: Some(2024),
                title: Some(format!("Track {track_no}")),
                artists: Some(vec!["Split Band".to_string()]),
                disc: Some(1),
                disc_total: Some(1),
                track: Some(track_no),
                track_total: Some(2),
                duration_ms: Some(180_000),
                genres: None,
                label: label.map(str::to_string),
                catalog_number: cat.map(str::to_string),
                source_kind: Some("embedded_tags".to_string()),
                source_key: Some(format!("entry:{}:embedded", entry_id.0)),
                segment_start_ms: None,
                segment_end_ms: None,
                cue_sheet_entry_id: None,
                cue_sheet_hash: None,
                cue_track_no: None,
                cue_audio_entry_id: None,
                cue_index00_frames: None,
                cue_index01_frames: None,
                sample_rate_hz: None,
                channel_count: None,
                bit_depth: None,
                bitrate_bps: None,
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

        let labels = db::labels::get_for_release(&db, release_db_id)?;
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
            album: Some("BN".to_string()),
            album_artists: Some(vec!["Artist".to_string()]),
            date: None,
            year: Some(1957),
            title: Some(format!("Track {track_no}")),
            artists: Some(vec!["Artist".to_string()]),
            disc: Some(1),
            disc_total: Some(1),
            track: Some(track_no),
            track_total: Some(2),
            duration_ms: Some(180_000),
            genres: None,
            label: Some("Blue Note".to_string()),
            catalog_number: cat.map(str::to_string),
            source_kind: Some("embedded_tags".to_string()),
            source_key: Some(format!("entry:{}:embedded", entry_id.0)),
            segment_start_ms: None,
            segment_end_ms: None,
            cue_sheet_entry_id: None,
            cue_sheet_hash: None,
            cue_track_no: None,
            cue_audio_entry_id: None,
            cue_index00_frames: None,
            cue_index01_frames: None,
            sample_rate_hz: None,
            channel_count: None,
            bit_depth: None,
            bitrate_bps: None,
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

        let labels = db::labels::get_for_release(&db, release_db_id)?;
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
                album: Some("EM".to_string()),
                album_artists: Some(vec!["Artist".to_string()]),
                date: None,
                year: Some(2010),
                title: Some(format!("Track {track_no}")),
                artists: Some(vec!["Artist".to_string()]),
                disc: Some(1),
                disc_total: Some(1),
                track: Some(track_no),
                track_total: Some(2),
                duration_ms: Some(180_000),
                genres: None,
                label: Some(label.to_string()),
                catalog_number: cat.map(str::to_string),
                source_kind: Some("embedded_tags".to_string()),
                source_key: Some(format!("entry:{}:embedded", entry_id.0)),
                segment_start_ms: None,
                segment_end_ms: None,
                cue_sheet_entry_id: None,
                cue_sheet_hash: None,
                cue_track_no: None,
                cue_audio_entry_id: None,
                cue_index00_frames: None,
                cue_index01_frames: None,
                sample_rate_hz: None,
                channel_count: None,
                bit_depth: None,
                bitrate_bps: None,
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

        let labels = db::labels::get_for_release(&db, release_db_id)?;
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

        let dir_id =
            insert_entry_with_kind(&mut db, &dir_path, crate::db::entries::EntryKind::Dir)?;
        let audio_id =
            insert_entry_with_kind(&mut db, &audio_path, crate::db::entries::EntryKind::File)?;
        let cue_id =
            insert_entry_with_kind(&mut db, &cue_path, crate::db::entries::EntryKind::File)?;

        connect(&mut db, library.db_id.unwrap(), dir_id)?;
        connect(&mut db, dir_id, audio_id)?;
        connect(&mut db, dir_id, cue_id)?;

        add_metadata(&mut db, &library, vec![dir_id, audio_id, cue_id]).await?;

        let releases = select_releases(&db)?;
        assert_eq!(releases.len(), 1);
        assert_eq!(releases[0].release_title, "Cue Album");
        let release_db_id: DbId = releases[0].db_id.clone().unwrap().into();

        let mut tracks = db::tracks::get(&db, release_db_id)?;
        tracks.sort_by_key(|track| track.track.unwrap_or(0));
        assert_eq!(tracks.len(), 2);
        assert_eq!(tracks[0].track_title, "Cue Track 1");
        assert_eq!(tracks[1].track_title, "Cue Track 2");

        let track_one_id: DbId = tracks[0].db_id.clone().unwrap().into();
        let track_one_entries = db::entries::get_by_track(&db, track_one_id)?;
        assert_eq!(track_one_entries.len(), 1);
        assert_eq!(track_one_entries[0].full_path, audio_path);

        let track_one_sources = db::track_sources::get_by_track(&db, track_one_id)?;
        assert_eq!(track_one_sources.len(), 1);
        assert_eq!(track_one_sources[0].source_kind, "cue");
        assert_eq!(track_one_sources[0].start_ms, Some(0));
        assert_eq!(track_one_sources[0].end_ms, Some(120_000));

        let track_two_id: DbId = tracks[1].db_id.clone().unwrap().into();
        let track_two_sources = db::track_sources::get_by_track(&db, track_two_id)?;
        assert_eq!(track_two_sources.len(), 1);
        assert_eq!(track_two_sources[0].source_kind, "cue");
        assert_eq!(track_two_sources[0].start_ms, Some(120_000));
        assert_eq!(track_two_sources[0].end_ms, None);

        let cue_sheets: Vec<db::CueSheet> = db
            .exec(
                QueryBuilder::select()
                    .elements::<db::CueSheet>()
                    .search()
                    .from("cue_sheets")
                    .query(),
            )?
            .try_into()?;
        assert_eq!(cue_sheets.len(), 1);
        let cue_sheet_id = cue_sheets[0]
            .db_id
            .ok_or_else(|| anyhow!("cue sheet missing db_id"))?;
        assert!(db::graph::edge_exists(&db, cue_sheet_id, cue_id)?);

        let cue_tracks: Vec<db::CueTrack> = db
            .exec(
                QueryBuilder::select()
                    .elements::<db::CueTrack>()
                    .search()
                    .from("cue_tracks")
                    .query(),
            )?
            .try_into()?;
        assert_eq!(cue_tracks.len(), 2);
        let cue_track_numbers: HashSet<u32> =
            cue_tracks.iter().map(|track| track.track_no).collect();
        assert_eq!(cue_track_numbers, HashSet::from([1, 2]));
        for cue_track in cue_tracks {
            let cue_track_id = cue_track
                .db_id
                .ok_or_else(|| anyhow!("cue track missing db_id"))?;
            assert!(db::graph::edge_exists(&db, cue_sheet_id, cue_track_id)?);
            assert!(db::graph::edge_exists(&db, cue_track_id, audio_id)?);
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

        let dir_id =
            insert_entry_with_kind(&mut db, &dir_path, crate::db::entries::EntryKind::Dir)?;
        let audio_one_id =
            insert_entry_with_kind(&mut db, &audio_one, crate::db::entries::EntryKind::File)?;
        let audio_two_id =
            insert_entry_with_kind(&mut db, &audio_two, crate::db::entries::EntryKind::File)?;
        let audio_three_id =
            insert_entry_with_kind(&mut db, &audio_three, crate::db::entries::EntryKind::File)?;
        let audio_four_id =
            insert_entry_with_kind(&mut db, &audio_four, crate::db::entries::EntryKind::File)?;
        let cue_id =
            insert_entry_with_kind(&mut db, &cue_path, crate::db::entries::EntryKind::File)?;

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

        let mut tracks = db::tracks::get(&db, release_db_id)?;
        tracks.sort_by_key(|track| track.track.unwrap_or(0));
        assert_eq!(tracks.len(), 4);
        let track_numbers: Vec<u32> = tracks.iter().filter_map(|track| track.track).collect();
        assert_eq!(track_numbers, vec![1, 2, 3, 4]);
        for track in tracks {
            let track_db_id: DbId = track.db_id.clone().unwrap().into();
            let sources = db::track_sources::get_by_track(&db, track_db_id)?;
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
                album: Some("Cafe\u{301} Album".to_string()),
                album_artists: Some(vec!["Artist".to_string()]),
                date: None,
                year: Some(2024),
                title: Some("Track 1".to_string()),
                artists: Some(vec!["Artist".to_string()]),
                disc: Some(1),
                disc_total: Some(1),
                track: Some(1),
                track_total: Some(2),
                duration_ms: Some(60_000),
                genres: None,
                label: None,
                catalog_number: None,
                source_kind: Some("embedded_tags".to_string()),
                source_key: Some(format!("entry:{}:embedded", entry_one.0)),
                segment_start_ms: None,
                segment_end_ms: None,
                cue_sheet_entry_id: None,
                cue_sheet_hash: None,
                cue_track_no: None,
                cue_audio_entry_id: None,
                cue_index00_frames: None,
                cue_index01_frames: None,
                sample_rate_hz: None,
                channel_count: None,
                bit_depth: None,
                bitrate_bps: None,
            },
            TrackMetadata {
                entry_db_id: entry_two,
                album: Some("Caf\u{e9} Album".to_string()),
                album_artists: Some(vec!["Artist".to_string()]),
                date: None,
                year: Some(2024),
                title: Some("Track 2".to_string()),
                artists: Some(vec!["Artist".to_string()]),
                disc: Some(1),
                disc_total: Some(1),
                track: Some(2),
                track_total: Some(2),
                duration_ms: Some(60_000),
                genres: None,
                label: None,
                catalog_number: None,
                source_kind: Some("embedded_tags".to_string()),
                source_key: Some(format!("entry:{}:embedded", entry_two.0)),
                segment_start_ms: None,
                segment_end_ms: None,
                cue_sheet_entry_id: None,
                cue_sheet_hash: None,
                cue_track_no: None,
                cue_audio_entry_id: None,
                cue_index00_frames: None,
                cue_index01_frames: None,
                sample_rate_hz: None,
                channel_count: None,
                bit_depth: None,
                bitrate_bps: None,
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
        assert_eq!(db::tracks::get(&db, release_db_id)?.len(), 2);

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
                album: Some("Disc Test".to_string()),
                album_artists: Some(vec!["Artist".to_string()]),
                date: None,
                year: Some(2024),
                title: Some("Disc 1 Track 1".to_string()),
                artists: Some(vec!["Artist".to_string()]),
                disc: Some(1),
                disc_total: None,
                track: Some(1),
                track_total: Some(1),
                duration_ms: Some(60_000),
                genres: None,
                label: None,
                catalog_number: None,
                source_kind: Some("embedded_tags".to_string()),
                source_key: Some(format!("entry:{}:embedded", entry_one.0)),
                segment_start_ms: None,
                segment_end_ms: None,
                cue_sheet_entry_id: None,
                cue_sheet_hash: None,
                cue_track_no: None,
                cue_audio_entry_id: None,
                cue_index00_frames: None,
                cue_index01_frames: None,
                sample_rate_hz: None,
                channel_count: None,
                bit_depth: None,
                bitrate_bps: None,
            },
            TrackMetadata {
                entry_db_id: entry_two,
                album: Some("Disc Test".to_string()),
                album_artists: Some(vec!["Artist".to_string()]),
                date: None,
                year: Some(2024),
                title: Some("Disc 2 Track 1".to_string()),
                artists: Some(vec!["Artist".to_string()]),
                disc: Some(2),
                disc_total: None,
                track: Some(1),
                track_total: Some(1),
                duration_ms: Some(60_000),
                genres: None,
                label: None,
                catalog_number: None,
                source_kind: Some("embedded_tags".to_string()),
                source_key: Some(format!("entry:{}:embedded", entry_two.0)),
                segment_start_ms: None,
                segment_end_ms: None,
                cue_sheet_entry_id: None,
                cue_sheet_hash: None,
                cue_track_no: None,
                cue_audio_entry_id: None,
                cue_index00_frames: None,
                cue_index01_frames: None,
                sample_rate_hz: None,
                channel_count: None,
                bit_depth: None,
                bitrate_bps: None,
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

        let tracks = db::tracks::get(&db, release_db_id)?;
        assert_eq!(tracks.len(), 2);
        assert!(tracks.iter().all(|track| track.disc_total == Some(2)));

        Ok(())
    }

    #[test]
    fn apply_metadata_disc_total_fallback_does_not_overwrite_existing_explicit_value()
    -> anyhow::Result<()> {
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
                album: Some("Disc Preserve".to_string()),
                album_artists: Some(vec!["Artist".to_string()]),
                date: None,
                year: Some(2024),
                title: Some("Disc 1 Track 1".to_string()),
                artists: Some(vec!["Artist".to_string()]),
                disc: Some(1),
                disc_total: Some(5),
                track: Some(1),
                track_total: Some(1),
                duration_ms: Some(60_000),
                genres: None,
                label: None,
                catalog_number: None,
                source_kind: Some("embedded_tags".to_string()),
                source_key: Some(format!("entry:{}:embedded", entry_one.0)),
                segment_start_ms: None,
                segment_end_ms: None,
                cue_sheet_entry_id: None,
                cue_sheet_hash: None,
                cue_track_no: None,
                cue_audio_entry_id: None,
                cue_index00_frames: None,
                cue_index01_frames: None,
                sample_rate_hz: None,
                channel_count: None,
                bit_depth: None,
                bitrate_bps: None,
            },
            TrackMetadata {
                entry_db_id: entry_two,
                album: Some("Disc Preserve".to_string()),
                album_artists: Some(vec!["Artist".to_string()]),
                date: None,
                year: Some(2024),
                title: Some("Disc 2 Track 1".to_string()),
                artists: Some(vec!["Artist".to_string()]),
                disc: Some(2),
                disc_total: Some(5),
                track: Some(1),
                track_total: Some(1),
                duration_ms: Some(60_000),
                genres: None,
                label: None,
                catalog_number: None,
                source_kind: Some("embedded_tags".to_string()),
                source_key: Some(format!("entry:{}:embedded", entry_two.0)),
                segment_start_ms: None,
                segment_end_ms: None,
                cue_sheet_entry_id: None,
                cue_sheet_hash: None,
                cue_track_no: None,
                cue_audio_entry_id: None,
                cue_index00_frames: None,
                cue_index01_frames: None,
                sample_rate_hz: None,
                channel_count: None,
                bit_depth: None,
                bitrate_bps: None,
            },
        ];
        apply_metadata(&mut db, library_db_id, first_scan)?;

        let second_scan = vec![
            TrackMetadata {
                entry_db_id: entry_one,
                album: Some("Disc Preserve".to_string()),
                album_artists: Some(vec!["Artist".to_string()]),
                date: None,
                year: Some(2024),
                title: Some("Disc 1 Track 1".to_string()),
                artists: Some(vec!["Artist".to_string()]),
                disc: Some(1),
                disc_total: None,
                track: Some(1),
                track_total: Some(1),
                duration_ms: Some(60_000),
                genres: None,
                label: None,
                catalog_number: None,
                source_kind: Some("embedded_tags".to_string()),
                source_key: Some(format!("entry:{}:embedded", entry_one.0)),
                segment_start_ms: None,
                segment_end_ms: None,
                cue_sheet_entry_id: None,
                cue_sheet_hash: None,
                cue_track_no: None,
                cue_audio_entry_id: None,
                cue_index00_frames: None,
                cue_index01_frames: None,
                sample_rate_hz: None,
                channel_count: None,
                bit_depth: None,
                bitrate_bps: None,
            },
            TrackMetadata {
                entry_db_id: entry_two,
                album: Some("Disc Preserve".to_string()),
                album_artists: Some(vec!["Artist".to_string()]),
                date: None,
                year: Some(2024),
                title: Some("Disc 2 Track 1".to_string()),
                artists: Some(vec!["Artist".to_string()]),
                disc: Some(2),
                disc_total: None,
                track: Some(1),
                track_total: Some(1),
                duration_ms: Some(60_000),
                genres: None,
                label: None,
                catalog_number: None,
                source_kind: Some("embedded_tags".to_string()),
                source_key: Some(format!("entry:{}:embedded", entry_two.0)),
                segment_start_ms: None,
                segment_end_ms: None,
                cue_sheet_entry_id: None,
                cue_sheet_hash: None,
                cue_track_no: None,
                cue_audio_entry_id: None,
                cue_index00_frames: None,
                cue_index01_frames: None,
                sample_rate_hz: None,
                channel_count: None,
                bit_depth: None,
                bitrate_bps: None,
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

        let tracks = db::tracks::get(&db, release_db_id)?;
        assert_eq!(tracks.len(), 2);
        assert!(tracks.iter().all(|track| track.disc_total == Some(5)));

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
    fn rescan_preserves_provider_owned_release_and_track_fields() -> anyhow::Result<()> {
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
            album: Some("Album".to_string()),
            album_artists: Some(vec!["Artist".to_string()]),
            date: None,
            year: Some(2001),
            title: Some("Track".to_string()),
            artists: Some(vec!["Artist".to_string()]),
            disc: Some(1),
            disc_total: Some(1),
            track: Some(1),
            track_total: Some(1),
            duration_ms: Some(180_000),
            genres: None,
            label: None,
            catalog_number: None,
            source_kind: Some("embedded_tags".to_string()),
            source_key: Some(format!("entry:{}:embedded", entry_db_id.0)),
            segment_start_ms: None,
            segment_end_ms: None,
            cue_sheet_entry_id: None,
            cue_sheet_hash: None,
            cue_track_no: None,
            cue_audio_entry_id: None,
            cue_index00_frames: None,
            cue_index01_frames: None,
            sample_rate_hz: None,
            channel_count: None,
            bit_depth: None,
            bitrate_bps: None,
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
        let track = db::tracks::get(&db, release_db_id)?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("track missing after first scan"))?;
        let track_db_id: DbId = track
            .db_id
            .ok_or_else(|| anyhow!("track missing db_id"))?
            .into();

        db::providers::upsert(
            &mut db,
            &ProviderConfig {
                db_id: None,
                id: nanoid!(),
                provider_id: "test".to_string(),
                display_name: "Test".to_string(),
                priority: 100,
                enabled: true,
            },
        )?;

        db::metadata::layers::upsert(
            &mut db,
            track_db_id,
            &MetadataLayer {
                db_id: None,
                id: nanoid!(),
                provider_id: "test".to_string(),
                fields: serde_json::json!({
                    "track_title": "Provider Track",
                    "year": 1999,
                    "disc": 9,
                    "track": 12
                })
                .to_string(),
                updated_at: 1,
            },
        )?;
        db::metadata::layers::upsert(
            &mut db,
            release_db_id,
            &MetadataLayer {
                db_id: None,
                id: nanoid!(),
                provider_id: "test".to_string(),
                fields: serde_json::json!({
                    "release_title": "Provider Release",
                    "release_date": "1988-08-08"
                })
                .to_string(),
                updated_at: 1,
            },
        )?;

        crate::services::metadata::merging::apply_merged_metadata_to_entity(&mut db, track_db_id)?;
        crate::services::metadata::merging::apply_merged_metadata_to_entity(
            &mut db,
            release_db_id,
        )?;
        let track_after_provider =
            db::tracks::get_by_id(&db, track_db_id)?.ok_or_else(|| anyhow!("track missing"))?;
        let release_after_provider = db::releases::get_by_id(&db, release_db_id)?
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
            album: Some("Album".to_string()),
            album_artists: Some(vec!["Artist".to_string()]),
            date: None,
            year: Some(2005),
            title: Some("Track".to_string()),
            artists: Some(vec!["Artist".to_string()]),
            disc: Some(1),
            disc_total: Some(1),
            track: Some(1),
            track_total: Some(1),
            duration_ms: Some(180_000),
            genres: None,
            label: None,
            catalog_number: None,
            source_kind: Some("embedded_tags".to_string()),
            source_key: Some(format!("entry:{}:embedded", entry_db_id.0)),
            segment_start_ms: None,
            segment_end_ms: None,
            cue_sheet_entry_id: None,
            cue_sheet_hash: None,
            cue_track_no: None,
            cue_audio_entry_id: None,
            cue_index00_frames: None,
            cue_index01_frames: None,
            sample_rate_hz: None,
            channel_count: None,
            bit_depth: None,
            bitrate_bps: None,
        };
        apply_metadata(&mut db, library_db_id, vec![second_scan])?;

        let track_after_rescan =
            db::tracks::get_by_id(&db, track_db_id)?.ok_or_else(|| anyhow!("track missing"))?;
        let release_after_rescan = db::releases::get_by_id(&db, release_db_id)?
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

    /// Rescans must skip the tag-path label sync when a provider owns
    /// the field. Regressing the gate would wipe enriched labels on
    /// every library scan.
    #[test]
    fn rescan_preserves_provider_owned_labels() -> anyhow::Result<()> {
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
            album: Some("Album".to_string()),
            album_artists: Some(vec!["Artist".to_string()]),
            date: None,
            year: Some(2001),
            title: Some("Track".to_string()),
            artists: Some(vec!["Artist".to_string()]),
            disc: Some(1),
            disc_total: Some(1),
            track: Some(1),
            track_total: Some(1),
            duration_ms: Some(180_000),
            genres: None,
            label: Some("Tag Label".to_string()),
            catalog_number: Some("TAG-001".to_string()),
            source_kind: Some("embedded_tags".to_string()),
            source_key: Some(format!("entry:{}:embedded", entry_db_id.0)),
            segment_start_ms: None,
            segment_end_ms: None,
            cue_sheet_entry_id: None,
            cue_sheet_hash: None,
            cue_track_no: None,
            cue_audio_entry_id: None,
            cue_index00_frames: None,
            cue_index01_frames: None,
            sample_rate_hz: None,
            channel_count: None,
            bit_depth: None,
            bitrate_bps: None,
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

        let tag_labels = db::labels::get_for_release(&db, release_db_id)?;
        assert_eq!(tag_labels.len(), 1);
        assert_eq!(tag_labels[0].label.name, "Tag Label");

        db::providers::upsert(
            &mut db,
            &ProviderConfig {
                db_id: None,
                id: nanoid!(),
                provider_id: "test".to_string(),
                display_name: "Test".to_string(),
                priority: 100,
                enabled: true,
            },
        )?;
        db::metadata::layers::upsert(
            &mut db,
            release_db_id,
            &MetadataLayer {
                db_id: None,
                id: nanoid!(),
                provider_id: "test".to_string(),
                fields: serde_json::json!({
                    "labels": [
                        {"name": "Provider Label", "catalog_number": "PRV-001"}
                    ]
                })
                .to_string(),
                updated_at: 1,
            },
        )?;
        crate::services::metadata::merging::apply_merged_metadata_to_entity(
            &mut db,
            release_db_id,
        )?;

        let after_provider = db::labels::get_for_release(&db, release_db_id)?;
        assert_eq!(after_provider[0].label.name, "Provider Label");

        let second_scan = TrackMetadata {
            entry_db_id,
            album: Some("Album".to_string()),
            album_artists: Some(vec!["Artist".to_string()]),
            date: None,
            year: Some(2001),
            title: Some("Track".to_string()),
            artists: Some(vec!["Artist".to_string()]),
            disc: Some(1),
            disc_total: Some(1),
            track: Some(1),
            track_total: Some(1),
            duration_ms: Some(180_000),
            genres: None,
            label: Some("Different Tag Label".to_string()),
            catalog_number: Some("TAG-999".to_string()),
            source_kind: Some("embedded_tags".to_string()),
            source_key: Some(format!("entry:{}:embedded", entry_db_id.0)),
            segment_start_ms: None,
            segment_end_ms: None,
            cue_sheet_entry_id: None,
            cue_sheet_hash: None,
            cue_track_no: None,
            cue_audio_entry_id: None,
            cue_index00_frames: None,
            cue_index01_frames: None,
            sample_rate_hz: None,
            channel_count: None,
            bit_depth: None,
            bitrate_bps: None,
        };
        apply_metadata(&mut db, library_db_id, vec![second_scan])?;

        let after_rescan = db::labels::get_for_release(&db, release_db_id)?;
        assert_eq!(after_rescan.len(), 1);
        assert_eq!(after_rescan[0].label.name, "Provider Label");
        assert_eq!(after_rescan[0].catalog_number.as_deref(), Some("PRV-001"));
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
            album: Some("Album".to_string()),
            album_artists: Some(vec!["Artist".to_string()]),
            date: None,
            year: Some(2001),
            title: Some("Track".to_string()),
            artists: Some(vec!["Artist".to_string()]),
            disc: Some(1),
            disc_total: Some(1),
            track: Some(1),
            track_total: Some(1),
            duration_ms: Some(180_000),
            genres: None,
            label: Some("Curated Label".to_string()),
            catalog_number: Some("CUR-001".to_string()),
            source_kind: Some("embedded_tags".to_string()),
            source_key: Some(format!("entry:{}:embedded", entry_db_id.0)),
            segment_start_ms: None,
            segment_end_ms: None,
            cue_sheet_entry_id: None,
            cue_sheet_hash: None,
            cue_track_no: None,
            cue_audio_entry_id: None,
            cue_index00_frames: None,
            cue_index01_frames: None,
            sample_rate_hz: None,
            channel_count: None,
            bit_depth: None,
            bitrate_bps: None,
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
        db::releases::update(&mut db, &locked_release)?;

        db::providers::upsert(
            &mut db,
            &ProviderConfig {
                db_id: None,
                id: nanoid!(),
                provider_id: "test".to_string(),
                display_name: "Test".to_string(),
                priority: 100,
                enabled: true,
            },
        )?;
        db::metadata::layers::upsert(
            &mut db,
            release_db_id,
            &MetadataLayer {
                db_id: None,
                id: nanoid!(),
                provider_id: "test".to_string(),
                fields: serde_json::json!({
                    "labels": [
                        {"name": "Provider Override", "catalog_number": "PRV-999"}
                    ]
                })
                .to_string(),
                updated_at: 1,
            },
        )?;
        crate::services::metadata::merging::apply_merged_metadata_to_entity(
            &mut db,
            release_db_id,
        )?;

        let labels = db::labels::get_for_release(&db, release_db_id)?;
        assert_eq!(labels.len(), 1);
        assert_eq!(labels[0].label.name, "Curated Label");
        assert_eq!(labels[0].catalog_number.as_deref(), Some("CUR-001"));
        Ok(())
    }
}
