// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::{
    HashMap,
    HashSet,
};
use std::time::{
    SystemTime,
    UNIX_EPOCH,
};

use agdb::{
    DbAny,
    DbId,
    QueryBuilder,
};
use nanoid::nanoid;

use super::super::{
    TrackMetadata,
    merging::merge_layers,
};
use super::artists::{
    resolve_artist_ids,
    sync_artist_edges,
};
use crate::db::{
    self,
    CreditType,
    DbAccess,
    Release,
    Track,
    graph::{
        ensure_owned_edge,
        remove_edges_between,
    },
    indexes::ensure_index,
    metadata::get_connected_artist_ids,
};

pub(crate) struct TrackIngest {
    pub(crate) meta: TrackMetadata,
    pub(crate) track_db_id: Option<DbId>,
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

fn provider_owned_fields_for_entity(
    db: &impl DbAccess,
    node_id: DbId,
    providers: &[db::ProviderConfig],
) -> anyhow::Result<HashSet<String>> {
    let layers = db::metadata::layers::get_for_entity(db, node_id)?;
    if layers.is_empty() || providers.is_empty() {
        return Ok(HashSet::new());
    }
    let merged = merge_layers(layers, providers);
    Ok(merged.provenance.into_keys().collect())
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
) -> anyhow::Result<()> {
    db.transaction_mut(|t| persist_release_inner(t, library_db_id, release_title, release_tracks))
}

fn persist_release_inner(
    db: &mut impl DbAccess,
    library_db_id: DbId,
    release_title: &str,
    release_tracks: Vec<TrackIngest>,
) -> anyhow::Result<()> {
    ensure_index(db, "scan_name")?;

    let mut artist_cache: HashMap<String, DbId> = HashMap::new();
    let first_track = release_tracks[0].meta.clone();
    let release_date = release_tracks
        .iter()
        .filter_map(|track| release_date_from_track(&track.meta))
        .max();
    let track_ids_for_release: Vec<DbId> = release_tracks
        .iter()
        .filter_map(|track| track.track_db_id)
        .collect();
    let existing_release_id = select_release_id(db, &track_ids_for_release)?;
    let providers = db::providers::get(db)?;

    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .ok();

    let earliest_ctime = release_tracks
        .iter()
        .filter_map(|t| {
            db::entries::get_by_id(db, t.meta.entry_db_id)
                .ok()
                .flatten()
        })
        .map(|e| e.ctime)
        .min();

    let (release_db_id, release_provider_fields) = if let Some(release_db_id) = existing_release_id
    {
        let release_provider_fields =
            provider_owned_fields_for_entity(db, release_db_id, &providers)?;
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
        if !release_provider_fields.contains("release_title") {
            release.release_title = release_title.to_string();
        }
        if !release_provider_fields.contains("release_date") {
            release.release_date = release_date;
        }
        release.ctime = earliest_ctime;
        db::releases::update(db, &release)?;
        (release_db_id, release_provider_fields)
    } else {
        let release = Release {
            db_id: None,
            id: nanoid!(),
            release_title: release_title.to_string(),
            sort_title: None,
            release_type: None,
            release_date,
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
        (release_db_id, HashSet::new())
    };

    ensure_owned_edge(db, library_db_id, release_db_id)?;

    if !release_provider_fields.contains("genres") {
        if let Some(genres) = &first_track.genres {
            if !genres.is_empty() {
                db::genres::sync_release_genres(db, release_db_id, genres)?;
            }
        }
    }

    // Tag-sourced absence ≡ "no labels, drop any stale entries."
    //
    // Two-pass: pick the label from the first track that tags one, then scan
    // for a catalog number only on tracks with the same (normalized) label
    // name. Prevents Frankensteining a (label, cat#) pair that no single
    // track actually carries — and cat# tagged on a later track with the
    // same label is still recovered.
    if !release_provider_fields.contains("labels") {
        let release_label_name = release_tracks.iter().find_map(|t| {
            t.meta
                .label
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
        });

        let mut inputs: Vec<db::labels::LabelInput> = Vec::new();
        if let Some(name) = release_label_name {
            // Compare via `normalize_label_name` (NFC + lowercase) so non-ASCII
            // case pairs ("Éditions Mego" vs "éditions mego") converge —
            // `eq_ignore_ascii_case` would drop the cat# match here.
            let picked = db::labels::normalize_label_name(&name);
            let catalog_number = release_tracks.iter().find_map(|t| {
                let track_label = t.meta.label.as_deref()?;
                if db::labels::normalize_label_name(track_label) != picked {
                    return None;
                }
                t.meta
                    .catalog_number
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string)
            });

            inputs.push(db::labels::LabelInput {
                name,
                catalog_number,
                external_id: None,
            });
        }
        db::labels::sync_release_labels_inside_tx(db, release_db_id, &inputs)?;
    }

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
    sync_artist_edges(db, release_db_id, &release_artist_ids, CreditType::Artist)?;

    let inferred_disc_total = release_tracks
        .iter()
        .filter_map(|track| track.meta.disc)
        .max();

    for track in release_tracks {
        let is_existing_track = track.track_db_id.is_some();
        let track_provider_fields = if let Some(track_db_id) = track.track_db_id {
            provider_owned_fields_for_entity(db, track_db_id, &providers)?
        } else {
            HashSet::new()
        };

        let TrackMetadata {
            entry_db_id,
            album: _,
            album_artists: _,
            date: _,
            year,
            title,
            artists,
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

        let entry_ctime = db::entries::get_by_id(db, entry_db_id)
            .ok()
            .flatten()
            .map(|e| e.ctime);

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
            if !track_provider_fields.contains("track_title") {
                existing.track_title = title.unwrap_or_default();
            }
            if !track_provider_fields.contains("year") {
                existing.year = year;
            }
            if !track_provider_fields.contains("disc") {
                existing.disc = disc;
            }
            if !track_provider_fields.contains("disc_total") {
                if let Some(explicit_disc_total) = disc_total {
                    existing.disc_total = Some(explicit_disc_total);
                } else if existing.disc_total.is_none() {
                    existing.disc_total = inferred_disc_total;
                }
            }
            if !track_provider_fields.contains("track") {
                existing.track = track_number;
            }
            if !track_provider_fields.contains("track_total") {
                existing.track_total = track_total;
            }
            existing.duration_ms = duration_ms;
            existing.sample_rate_hz = sample_rate_hz;
            existing.channel_count = channel_count;
            existing.bit_depth = bit_depth;
            existing.bitrate_bps = bitrate_bps;
            existing.ctime = entry_ctime;
            db::tracks::update(db, &existing)?;
            track_db_id
        } else {
            let track_db = Track {
                db_id: None,
                id: nanoid!(),
                track_title: title.unwrap_or_default(),
                sort_title: None,
                year,
                disc,
                disc_total: effective_disc_total,
                track: track_number,
                track_total,
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
        sync_artist_edges(db, track_db_id, &track_artist_ids, CreditType::Artist)?;

        let current_releases = db::releases::get_by_track(db, track_db_id)?;
        for release in current_releases {
            let Some(other_db_id) = release.db_id.map(Into::into) else {
                continue;
            };
            if other_db_id != release_db_id {
                remove_edges_between(db, other_db_id, track_db_id)?;
            }
        }

        remove_edges_between(db, release_db_id, track_db_id)?;
        ensure_owned_edge(db, release_db_id, track_db_id)?;
    }

    Ok(())
}
