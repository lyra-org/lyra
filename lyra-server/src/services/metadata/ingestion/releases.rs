// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::{
    BTreeMap,
    HashMap,
    HashSet,
};
use std::time::{
    SystemTime,
    UNIX_EPOCH,
};

use agdb::{
    DbAny,
    DbAnyTransactionMut,
    DbId,
    QueryBuilder,
};
use nanoid::nanoid;

use super::super::{
    TrackMetadata,
    layers::LocalLayer,
};
use super::artists::{
    resolve_artist_ids,
    sync_artist_edges,
};
use crate::db::{
    self,
    ArtistRelationType,
    ArtistType,
    CreditType,
    DbAccess,
    Release,
    Track,
    graph::ensure_owned_edge,
    indexes::ensure_index,
    metadata::{
        get_connected_artist_ids,
        manual_overrides::ManualMetadataField,
    },
};

pub(crate) struct TrackIngest {
    pub(crate) meta: TrackMetadata,
    pub(crate) track_db_id: Option<DbId>,
}

#[derive(Clone, Debug)]
pub(crate) struct ReleaseIngestResult {
    pub(crate) release_db_id: DbId,
    pub(crate) track_db_ids: Vec<DbId>,
}

fn select_release_id(db: &impl DbAccess, track_ids: &[DbId]) -> anyhow::Result<Option<DbId>> {
    let mut counts: HashMap<DbId, usize> = HashMap::new();
    for track_db_id in track_ids {
        let releases = db::releases::get_by_track(db, *track_db_id)?;
        for release in releases {
            if let Some(release_db_id) = release.db_id.map(Into::into) {
                *counts.entry(release_db_id).or_default() += 1;
            }
        }
    }

    Ok(counts
        .into_iter()
        .max_by_key(|(_, count)| *count)
        .map(|(id, _)| id))
}

fn infer_release_artists(release_tracks: &[TrackIngest]) -> Vec<String> {
    if let Some(explicit) = release_tracks
        .iter()
        .find_map(|track| track.meta.album_artists.clone())
    {
        return explicit;
    }

    let total = release_tracks.len();
    let mut counts: HashMap<&str, usize> = HashMap::new();
    let mut ordered = Vec::new();
    let mut seen = HashSet::new();

    for track in release_tracks {
        if let Some(artists) = track.meta.artists.as_deref() {
            for name in artists {
                *counts.entry(name.as_str()).or_default() += 1;
                if seen.insert(name.as_str()) {
                    ordered.push(name.clone());
                }
            }
        }
    }

    ordered
        .into_iter()
        .filter(|name| counts.get(name.as_str()).copied().unwrap_or(0) > total / 2)
        .collect()
}

fn parsed_artist_type_to_db(artist_type: lyra_metadata::ParsedArtistType) -> ArtistType {
    match artist_type {
        lyra_metadata::ParsedArtistType::Person => ArtistType::Person,
        lyra_metadata::ParsedArtistType::Character => ArtistType::Character,
    }
}

fn parsed_relation_type_to_db(
    relation_type: lyra_metadata::ArtistRelationKind,
) -> ArtistRelationType {
    match relation_type {
        lyra_metadata::ArtistRelationKind::VoiceActor => ArtistRelationType::VoiceActor,
    }
}

fn manual_relations_owned(
    db: &impl DbAccess,
    owned_by_artist: &mut HashMap<DbId, bool>,
    artist_id: DbId,
) -> anyhow::Result<bool> {
    if let Some(owned) = owned_by_artist.get(&artist_id) {
        return Ok(*owned);
    }
    let owned =
        db::metadata::manual_overrides::owns_field(db, artist_id, ManualMetadataField::Relations)?;
    owned_by_artist.insert(artist_id, owned);
    Ok(owned)
}

fn sync_scanned_artist_relations(
    db: &mut DbAnyTransactionMut<'_>,
    relations: &[lyra_metadata::ArtistRelationMetadata],
    cache: &mut BTreeMap<String, DbId>,
    relations_owned_by_artist: &mut HashMap<DbId, bool>,
    scanned_artist_types: &mut HashMap<DbId, ArtistType>,
) -> anyhow::Result<()> {
    for relation in relations {
        let source_ids =
            resolve_artist_ids(db, std::slice::from_ref(&relation.source_artist), cache)?;
        let target_ids =
            resolve_artist_ids(db, std::slice::from_ref(&relation.target_artist), cache)?;
        let Some(source_artist_id) = source_ids.first().copied() else {
            continue;
        };
        let Some(target_artist_id) = target_ids.first().copied() else {
            continue;
        };

        for (artist_id, artist_type) in [
            (source_artist_id, relation.source_artist_type),
            (target_artist_id, relation.target_artist_type),
        ] {
            if let Some(artist_type) = artist_type {
                scanned_artist_types.insert(artist_id, parsed_artist_type_to_db(artist_type));
            }
        }

        if manual_relations_owned(db, relations_owned_by_artist, source_artist_id)? {
            continue;
        }
        db::artists::relations::link(
            db,
            source_artist_id,
            target_artist_id,
            parsed_relation_type_to_db(relation.relation_type),
            None,
        )?;
    }

    Ok(())
}

/// Selects a catalog number only from a track whose normalized label matches
/// the chosen release label.
fn scanned_release_labels(release_tracks: &[TrackIngest]) -> Option<Vec<serde_json::Value>> {
    let name = release_tracks.iter().find_map(|track| {
        track
            .meta
            .label
            .as_deref()
            .map(str::trim)
            .filter(|label| !label.is_empty())
            .map(str::to_string)
    })?;

    let picked = db::labels::normalize_label_name(&name);
    let catalog_number = release_tracks.iter().find_map(|track| {
        let track_label = track.meta.label.as_deref()?;
        if db::labels::normalize_label_name(track_label) != picked {
            return None;
        }
        track
            .meta
            .catalog_number
            .as_deref()
            .map(str::trim)
            .filter(|catalog_number| !catalog_number.is_empty())
            .map(str::to_string)
    });

    let mut label = serde_json::Map::new();
    label.insert("name".to_string(), name.into());
    if let Some(catalog_number) = catalog_number {
        label.insert("catalog_number".to_string(), catalog_number.into());
    }
    Some(vec![label.into()])
}

fn release_date_from_track(track: &TrackMetadata) -> Option<String> {
    track
        .date
        .as_deref()
        .and_then(db::releases::normalize_release_date)
        .or_else(|| track.year.map(|year| format!("{year:04}")))
}

pub(crate) fn persist_release(
    db: &mut DbAny,
    library_db_id: DbId,
    release_title: &str,
    release_tracks: Vec<TrackIngest>,
) -> anyhow::Result<ReleaseIngestResult> {
    db.transaction_mut(|t| persist_release_inner(t, library_db_id, release_title, release_tracks))
}

fn persist_release_inner(
    db: &mut DbAnyTransactionMut<'_>,
    library_db_id: DbId,
    release_title: &str,
    release_tracks: Vec<TrackIngest>,
) -> anyhow::Result<ReleaseIngestResult> {
    ensure_index(db, "scan_name")?;

    let mut artist_cache: BTreeMap<String, DbId> = BTreeMap::new();
    let mut relations_owned_by_artist = HashMap::new();
    let mut scanned_artist_types: HashMap<DbId, ArtistType> = HashMap::new();
    let release_date = release_tracks
        .iter()
        .filter_map(|track| release_date_from_track(&track.meta))
        .max();
    let track_ids_for_release: Vec<DbId> = release_tracks
        .iter()
        .filter_map(|track| track.track_db_id)
        .collect();
    let existing_release_id = select_release_id(db, &track_ids_for_release)?;

    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .ok();

    // Propagate lookup failures: a swallowed error is indistinguishable from
    // "no ctime", and ctime drives date-added ordering.
    let mut earliest_ctime: Option<u64> = None;
    for track in release_tracks.iter() {
        if let Some(entry) = db::entries::get_by_id(db, track.meta.entry_db_id)? {
            earliest_ctime = Some(earliest_ctime.map_or(entry.ctime, |c| c.min(entry.ctime)));
        }
    }

    let release_db_id = if let Some(release_db_id) = existing_release_id {
        let mut release = db::releases::get_by_id(db, release_db_id)?.unwrap_or(Release {
            db_id: Some(release_db_id.into()),
            id: nanoid!(),
            release_title: release_title.to_string(),
            sort_title: None,
            release_type: None,
            release_date: None,
            locked: None,
            created_at: now_secs,
            ctime: earliest_ctime,
        });
        // Only overwrite when the scan actually resolved one; `None` here means
        // "not determined", never "cleared".
        if earliest_ctime.is_some() {
            release.ctime = earliest_ctime;
        }
        db::releases::update_in_transaction(db, &release)?;
        release_db_id
    } else {
        let release = Release {
            db_id: None,
            id: nanoid!(),
            release_title: release_title.to_string(),
            sort_title: None,
            release_type: None,
            release_date: None,
            locked: None,
            created_at: now_secs,
            ctime: earliest_ctime,
        };
        let insert_result = db.exec_mut(QueryBuilder::insert().element(&release).query())?;
        let release_db_id = insert_result.ids()[0];
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from("releases")
                .to(release_db_id)
                .query(),
        )?;
        release_db_id
    };

    ensure_owned_edge(db, library_db_id, release_db_id)?;

    let mut release_layer = LocalLayer::default();
    release_layer.supply(
        ManualMetadataField::ReleaseTitle,
        Some(release_title.to_string()),
    );
    release_layer.supply(ManualMetadataField::ReleaseDate, release_date);
    release_layer.supply(
        ManualMetadataField::Genres,
        release_tracks[0]
            .meta
            .genres
            .clone()
            .filter(|genres| !genres.is_empty()),
    );
    release_layer.supply(
        ManualMetadataField::Labels,
        scanned_release_labels(&release_tracks),
    );
    release_layer.save(db, release_db_id)?;

    // Derive release artists: explicit tag > majority track artists > empty (compilation)
    let release_artists = infer_release_artists(&release_tracks);
    // Reuse existing artist edges on rescan to prevent duplicates when plugins
    // have renamed the artist, causing the artist_name index to diverge from file tags.
    let release_artist_ids = if existing_release_id.is_some() {
        let existing = get_connected_artist_ids(db, release_db_id)?;
        if !existing.is_empty() {
            existing
        } else {
            resolve_artist_ids(db, &release_artists, &mut artist_cache)?
        }
    } else {
        resolve_artist_ids(db, &release_artists, &mut artist_cache)?
    };
    if !db::metadata::manual_overrides::owns_field(db, release_db_id, ManualMetadataField::Credits)?
    {
        sync_artist_edges(db, release_db_id, &release_artist_ids, CreditType::Artist)?;
    }

    // Infer across the whole release, not just this batch: an incremental scan
    // of one disc directory would otherwise shrink `disc_total` to that disc.
    let inferred_disc_total = release_tracks
        .iter()
        .filter_map(|track| track.meta.disc)
        .chain(
            db::tracks::get_direct(db, release_db_id)?
                .into_iter()
                .filter_map(|track| track.disc),
        )
        .max();

    let mut persisted_track_ids = Vec::new();
    let mut changed_track_ids = Vec::new();
    let mut changed_track_id_set = HashSet::new();
    let mut target_gained_track = false;

    for track in release_tracks {
        let is_existing_track = track.track_db_id.is_some();

        let TrackMetadata {
            entry_db_id,
            album: _,
            album_artists: _,
            date: _,
            year,
            title,
            artists,
            artist_relations,
            disc,
            disc_total,
            track: track_number,
            track_total,
            duration_ms,
            genres: _,
            label: _,
            catalog_number: _,
            source_kind,
            source_key,
            segment_start_ms,
            segment_end_ms,
            cue_sheet_entry_id,
            cue_sheet_hash,
            cue_track_no,
            cue_audio_entry_id,
            cue_index00_frames,
            cue_index01_frames,
            sample_rate_hz,
            channel_count,
            bit_depth,
            bitrate_bps,
        } = track.meta;

        let effective_disc_total = disc_total.or(inferred_disc_total);

        let entry_ctime = db::entries::get_by_id(db, entry_db_id)?.map(|e| e.ctime);

        let track_db_id = if let Some(track_db_id) = track.track_db_id {
            let mut existing = db::tracks::get_by_id(db, track_db_id)?.unwrap_or(Track {
                db_id: Some(track_db_id.into()),
                id: nanoid!(),
                track_title: title.clone().unwrap_or_default(),
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
                locked: None,
                created_at: now_secs,
                ctime: entry_ctime,
            });
            // Duration and audio properties are intrinsic to the file, not tags:
            // there is no "user removed it" case, so `None` only ever means the
            // probe could not determine the value. Keep what is stored.
            if let Some(duration_ms) = duration_ms {
                existing.set_duration_ms(duration_ms);
            }
            existing.sample_rate_hz = sample_rate_hz.or(existing.sample_rate_hz);
            existing.channel_count = channel_count.or(existing.channel_count);
            existing.bit_depth = bit_depth.or(existing.bit_depth);
            existing.bitrate_bps = bitrate_bps.or(existing.bitrate_bps);
            if entry_ctime.is_some() {
                existing.ctime = entry_ctime;
            }
            db::tracks::update_in_transaction(db, &existing)?;
            track_db_id
        } else {
            let track_db = Track {
                db_id: None,
                id: nanoid!(),
                track_title: title.clone().unwrap_or_default(),
                sort_title: None,
                year: None,
                disc: None,
                disc_total: None,
                track: None,
                track_total: None,
                duration_ms,
                sample_rate_hz,
                channel_count,
                bit_depth,
                bitrate_bps,
                locked: None,
                created_at: now_secs,
                ctime: entry_ctime,
            };
            let track_insert = db.exec_mut(QueryBuilder::insert().element(&track_db).query())?;
            let track_db_id = track_insert.ids()[0];
            db.exec_mut(
                QueryBuilder::insert()
                    .edges()
                    .from("tracks")
                    .to(track_db_id)
                    .query(),
            )?;
            track_db_id
        };

        let mut track_layer = LocalLayer::default();
        track_layer.supply(ManualMetadataField::TrackTitle, title);
        track_layer.supply(ManualMetadataField::Year, year);
        track_layer.supply(ManualMetadataField::Disc, disc);
        track_layer.supply(ManualMetadataField::DiscTotal, effective_disc_total);
        track_layer.supply(ManualMetadataField::Track, track_number);
        track_layer.supply(ManualMetadataField::TrackTotal, track_total);
        track_layer.save(db, track_db_id)?;

        let source_kind = source_kind.unwrap_or_else(|| "embedded_tags".to_string());
        let source_key = source_key.unwrap_or_else(|| format!("entry:{}:embedded", entry_db_id.0));

        let cue_track_id = if source_kind == "cue" {
            let cue_index01_frames = cue_index01_frames.or_else(|| {
                segment_start_ms.map(|start_ms| ((start_ms.saturating_mul(75)) / 1000) as u32)
            });
            let cue_audio_entry_id = cue_audio_entry_id.unwrap_or(entry_db_id);

            match (
                cue_sheet_entry_id,
                cue_sheet_hash.clone(),
                cue_track_no,
                cue_index01_frames,
            ) {
                (
                    Some(cue_sheet_entry_id),
                    Some(cue_sheet_hash),
                    Some(cue_track_no),
                    Some(cue_index01_frames),
                ) => {
                    let cue_sheet_id =
                        db::cue::sheets::upsert(db, cue_sheet_entry_id, &cue_sheet_hash)?;
                    Some(db::cue::tracks::upsert(
                        db,
                        cue_sheet_id,
                        cue_sheet_entry_id,
                        cue_track_no,
                        cue_audio_entry_id,
                        cue_index00_frames,
                        cue_index01_frames,
                    )?)
                }
                _ => {
                    tracing::warn!(
                        track_db_id = track_db_id.0,
                        "cue source metadata missing required provenance fields; cue linkage will be skipped"
                    );
                    None
                }
            }
        } else {
            None
        };

        db::track_sources::upsert(
            db,
            track_db_id,
            entry_db_id,
            db::track_sources::TrackSourceUpsert {
                source_kind,
                source_key,
                is_primary: true,
                start_ms: segment_start_ms,
                end_ms: segment_end_ms,
            },
            cue_track_id,
        )?;

        let track_artist_names = artists.unwrap_or_default();
        let track_artist_ids = if is_existing_track {
            let existing = get_connected_artist_ids(db, track_db_id)?;
            if !existing.is_empty() {
                existing
            } else {
                resolve_artist_ids(db, &track_artist_names, &mut artist_cache)?
            }
        } else {
            resolve_artist_ids(db, &track_artist_names, &mut artist_cache)?
        };
        if !db::metadata::manual_overrides::owns_field(
            db,
            track_db_id,
            ManualMetadataField::Credits,
        )? {
            sync_artist_edges(db, track_db_id, &track_artist_ids, CreditType::Artist)?;
        }
        sync_scanned_artist_relations(
            db,
            &artist_relations,
            &mut artist_cache,
            &mut relations_owned_by_artist,
            &mut scanned_artist_types,
        )?;

        let release_delta = db::releases::replace_track_release(db, release_db_id, track_db_id)?;
        if release_delta.membership_changed && changed_track_id_set.insert(track_db_id) {
            changed_track_ids.push(track_db_id);
        }
        target_gained_track |= release_delta.target_added;
        if !persisted_track_ids.contains(&track_db_id) {
            persisted_track_ids.push(track_db_id);
        }
    }

    for (scan_name, artist_db_id) in artist_cache {
        let mut artist_layer = LocalLayer::default();
        artist_layer.supply(ManualMetadataField::ArtistName, Some(scan_name));
        artist_layer.supply(
            ManualMetadataField::ArtistType,
            scanned_artist_types
                .get(&artist_db_id)
                .map(ArtistType::to_string),
        );
        artist_layer.save(db, artist_db_id)?;
    }

    if target_gained_track {
        db::covers::display::mark_genre_profiles_dirty_for_release(db, release_db_id)?;
    }
    let affected_playlist_ids =
        db::covers::display::playlist_ids_for_tracks(db, &changed_track_ids)?;
    db::covers::display::sync_playlist_covers(db, &affected_playlist_ids)?;

    Ok(ReleaseIngestResult {
        release_db_id,
        track_db_ids: persisted_track_ids,
    })
}
