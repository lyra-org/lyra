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
    layers::{
        LOCAL_SOURCE_ID,
        LocalLayer,
    },
};
use super::artists::resolve_artist_ids;
use lyra_db::{
    ArtistRelationType,
    ArtistType,
    DbAccess,
    MetadataField,
    MetadataLayer,
    Release,
    Track,
    graph::ensure_owned_edge,
    indexes::ensure_index,
    labels::LabelInput,
    metadata::layers::{
        FieldInput,
        edges::LayerRelationInput,
        field_value,
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
        let releases = lyra_db::releases::get_by_track(db, *track_db_id)?;
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

fn scanned_media_formats(release_tracks: &[TrackIngest]) -> Option<Vec<String>> {
    let formats = lyra_metadata::release_media_formats(
        release_tracks
            .iter()
            .map(|track| track.meta.media_formats.as_slice()),
    );
    (!formats.is_empty()).then(|| {
        formats
            .into_iter()
            .map(|format| format.as_str().to_string())
            .collect()
    })
}

/// The local layer a scan stored on an entity, if any.
fn stored_local_layer(db: &impl DbAccess, node_id: DbId) -> anyhow::Result<Option<MetadataLayer>> {
    Ok(lyra_db::metadata::layers::get_for_entity(db, node_id)?
        .into_iter()
        .find(|layer| layer.source_id == LOCAL_SOURCE_ID))
}

/// The release's tracks this batch does not hold.
fn other_track_ids(
    db: &impl DbAccess,
    release_db_id: Option<DbId>,
    release_tracks: &[TrackIngest],
) -> anyhow::Result<Vec<DbId>> {
    let Some(release_db_id) = release_db_id else {
        return Ok(Vec::new());
    };
    let batch_track_ids: HashSet<DbId> = release_tracks
        .iter()
        .filter_map(|track| track.track_db_id)
        .collect();
    Ok(lyra_db::tracks::get_direct(db, release_db_id)?
        .into_iter()
        .filter_map(|track| track.db_id.map(DbId::from))
        .filter(|track_id| !batch_track_ids.contains(track_id))
        .collect())
}

/// The release's artists: the batch's explicit album-artist tag, or else the
/// artists on more than half of the release's tracks, counting this batch's
/// tags and the local layers of its other tracks, so a partial scan of a
/// compilation keeps what the whole release says.
fn release_artist_ids(
    db: &mut DbAnyTransactionMut<'_>,
    other_track_ids: &[DbId],
    release_tracks: &[TrackIngest],
    cache: &mut BTreeMap<String, DbId>,
) -> anyhow::Result<Vec<DbId>> {
    if let Some(explicit) = release_tracks
        .iter()
        .find_map(|track| track.meta.album_artists.clone())
    {
        return resolve_artist_ids(db, &explicit, cache);
    }

    let mut track_artists = Vec::with_capacity(release_tracks.len());
    for track in release_tracks {
        let names = track.meta.artists.clone().unwrap_or_default();
        track_artists.push(resolve_artist_ids(db, &names, cache)?);
    }
    for &track_id in other_track_ids {
        let local_artists = stored_local_layer(db, track_id)?
            .and_then(|mut layer| layer.credits.remove(&lyra_db::CreditType::Artist))
            .unwrap_or_default();
        track_artists.push(
            local_artists
                .into_iter()
                .map(|credit| credit.artist_id)
                .collect(),
        );
    }

    let total = track_artists.len();
    let mut counts: HashMap<DbId, usize> = HashMap::new();
    let mut ordered = Vec::new();
    for artist_ids in &track_artists {
        for &artist_id in artist_ids {
            let count = counts.entry(artist_id).or_default();
            if *count == 0 {
                ordered.push(artist_id);
            }
            *count += 1;
        }
    }
    Ok(ordered
        .into_iter()
        .filter(|artist_id| counts[artist_id] > total / 2)
        .collect())
}

fn parsed_artist_type_to_db(artist_type: lyra_metadata::ParsedArtistType) -> ArtistType {
    match artist_type {
        lyra_metadata::ParsedArtistType::Person => ArtistType::Person,
        lyra_metadata::ParsedArtistType::Character => ArtistType::Character,
    }
}

/// Character evidence outranks person evidence, so an artist scanned as both
/// gets the same type whichever release is scanned first.
fn combine_scanned_artist_types(left: ArtistType, right: ArtistType) -> ArtistType {
    if right == ArtistType::Character {
        right
    } else {
        left
    }
}

fn parsed_relation_type_to_db(
    relation_type: lyra_metadata::ArtistRelationKind,
) -> ArtistRelationType {
    match relation_type {
        lyra_metadata::ArtistRelationKind::VoiceActor => ArtistRelationType::VoiceActor,
    }
}

/// Relations a scan found, keyed by the related-to artist whose layer owns them.
type ScannedRelations = HashMap<DbId, BTreeMap<ArtistRelationType, Vec<LayerRelationInput>>>;

fn collect_scanned_artist_relations(
    db: &mut DbAnyTransactionMut<'_>,
    relations: &[lyra_metadata::ArtistRelationMetadata],
    cache: &mut BTreeMap<String, DbId>,
    scanned_relations: &mut ScannedRelations,
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
            if let Some(artist_type) = artist_type.map(parsed_artist_type_to_db) {
                scanned_artist_types
                    .entry(artist_id)
                    .and_modify(|scanned| {
                        *scanned = combine_scanned_artist_types(*scanned, artist_type);
                    })
                    .or_insert(artist_type);
            }
        }

        let inputs = scanned_relations
            .entry(target_artist_id)
            .or_default()
            .entry(parsed_relation_type_to_db(relation.relation_type))
            .or_default();
        if source_artist_id != target_artist_id
            && !inputs
                .iter()
                .any(|input| input.artist_id == source_artist_id)
        {
            inputs.push(LayerRelationInput {
                artist_id: source_artist_id,
                attributes: None,
            });
        }
    }

    Ok(())
}

/// The name an artist's local layer supplies when a tag spelled it `scan_name`:
/// that spelling when it is the artist's own scan name, or else, when it
/// reached the artist through a scan alias, the name the layer already holds.
fn scanned_artist_name(
    db: &impl DbAccess,
    artist_db_id: DbId,
    stored: Option<&MetadataLayer>,
    scan_name: String,
) -> anyhow::Result<Option<String>> {
    if lyra_db::artists::get_by_id(db, artist_db_id)?
        .is_some_and(|artist| artist.scan_name == scan_name)
    {
        return Ok(Some(scan_name));
    }
    stored.map_or(Ok(None), |layer| {
        field_value(&layer.fields, MetadataField::ArtistName)
    })
}

/// The type an artist's local layer supplies after this scan: what earlier
/// scans recorded, combined with what this scan found.
fn scanned_artist_type(
    stored: Option<&MetadataLayer>,
    scanned: Option<ArtistType>,
) -> anyhow::Result<Option<ArtistType>> {
    let stored = stored.map_or(Ok(None), |layer| {
        field_value(&layer.fields, MetadataField::ArtistType)
    })?;
    Ok(match (stored, scanned) {
        (Some(stored), Some(scanned)) => Some(combine_scanned_artist_types(stored, scanned)),
        (stored, scanned) => scanned.or(stored),
    })
}

/// Selects a catalog number only from a track whose normalized label matches
/// the chosen release label.
fn scanned_release_labels(release_tracks: &[TrackIngest]) -> Option<Vec<LabelInput>> {
    let name = release_tracks.iter().find_map(|track| {
        track
            .meta
            .label
            .as_deref()
            .map(str::trim)
            .filter(|label| !label.is_empty())
            .map(str::to_string)
    })?;

    let picked = lyra_db::labels::normalize_label_name(&name);
    let catalog_number = release_tracks.iter().find_map(|track| {
        let track_label = track.meta.label.as_deref()?;
        if lyra_db::labels::normalize_label_name(track_label) != picked {
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

    Some(vec![LabelInput {
        name,
        catalog_number,
        external_id: None,
    }])
}

fn release_date_from_track(track: &TrackMetadata) -> Option<String> {
    track
        .date
        .as_deref()
        .and_then(lyra_db::releases::normalize_release_date)
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
    ensure_index(db, lyra_db::artists::SCAN_ALIAS_HOLDER)?;

    let mut artist_cache: BTreeMap<String, DbId> = BTreeMap::new();
    let mut scanned_relations = ScannedRelations::new();
    let mut scanned_artist_types: HashMap<DbId, ArtistType> = HashMap::new();
    let release_date = release_tracks
        .iter()
        .filter_map(|track| release_date_from_track(&track.meta))
        .max();
    let media_formats = scanned_media_formats(&release_tracks);
    let barcode =
        lyra_metadata::release_barcode(release_tracks.iter().map(|t| t.meta.barcode.as_deref()));
    let track_ids_for_release: Vec<DbId> = release_tracks
        .iter()
        .filter_map(|track| track.track_db_id)
        .collect();
    let existing_release_id = select_release_id(db, &track_ids_for_release)?;
    let other_track_ids = other_track_ids(db, existing_release_id, &release_tracks)?;
    // A batch holding every track of the release speaks for the whole release.
    // A partial one keeps what the release's other tracks contributed to a
    // release field the batch does not tag, as no per-track record of it exists.
    let partial = !other_track_ids.is_empty();

    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .ok();

    // Propagate lookup failures: a swallowed error is indistinguishable from
    // "no file creation time", and that time drives date-added ordering.
    let mut earliest_file_created_at_ms: Option<u64> = None;
    for track in release_tracks.iter() {
        if let Some(entry) = lyra_db::entries::get_by_id(db, track.meta.entry_db_id)? {
            earliest_file_created_at_ms = Some(
                earliest_file_created_at_ms.map_or(entry.file_created_at_ms, |c| {
                    c.min(entry.file_created_at_ms)
                }),
            );
        }
    }

    let release_db_id = if let Some(release_db_id) = existing_release_id {
        let mut release = lyra_db::releases::get_by_id(db, release_db_id)?.unwrap_or(Release {
            db_id: Some(release_db_id.into()),
            id: nanoid!(),
            release_title: release_title.to_string(),
            sort_title: None,
            release_type: None,
            release_date: None,
            media_formats: None,
            barcode: None,
            locked: None,
            created_at_ms: now_ms,
            file_created_at_ms: earliest_file_created_at_ms,
        });
        if media_formats.is_some() || !partial {
            release.media_formats = media_formats;
        }
        if barcode.is_some() || !partial {
            release.barcode = barcode;
        }
        // Only overwrite when the scan actually resolved one; `None` here means
        // "not determined", never "cleared".
        if earliest_file_created_at_ms.is_some() {
            release.file_created_at_ms = earliest_file_created_at_ms;
        }
        lyra_db::releases::update_in_transaction(db, &release)?;
        release_db_id
    } else {
        let release = Release {
            db_id: None,
            id: nanoid!(),
            release_title: release_title.to_string(),
            sort_title: None,
            release_type: None,
            release_date: None,
            media_formats,
            barcode,
            locked: None,
            created_at_ms: now_ms,
            file_created_at_ms: earliest_file_created_at_ms,
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

    let stored_release_fields = if partial {
        stored_local_layer(db, release_db_id)?
            .map(|layer| layer.fields)
            .unwrap_or_default()
    } else {
        Default::default()
    };
    let or_stored = |field: MetadataField, scanned: Option<FieldInput>| {
        scanned.map(Some).or_else(|| {
            stored_release_fields
                .get(&field)
                .cloned()
                .map(|value| value.map(FieldInput::Value))
        })
    };
    let mut release_layer = LocalLayer::default();
    release_layer.supply(MetadataField::ReleaseTitle, Some(release_title));
    release_layer.supply_input(
        MetadataField::ReleaseDate,
        or_stored(
            MetadataField::ReleaseDate,
            release_date.map(|date| FieldInput::Value(date.into())),
        ),
    );
    release_layer.supply_input(
        MetadataField::Genres,
        or_stored(
            MetadataField::Genres,
            release_tracks[0]
                .meta
                .genres
                .clone()
                .filter(|genres| !genres.is_empty())
                .map(FieldInput::Genres),
        ),
    );
    release_layer.supply_input(
        MetadataField::Labels,
        or_stored(
            MetadataField::Labels,
            scanned_release_labels(&release_tracks).map(FieldInput::Labels),
        ),
    );
    release_layer.supply_artists(&release_artist_ids(
        db,
        &other_track_ids,
        &release_tracks,
        &mut artist_cache,
    )?);
    release_layer.save(db, release_db_id)?;

    // Infer across the whole release, not just this batch: an incremental scan
    // of one disc directory would otherwise shrink `disc_total` to that disc.
    let inferred_disc_total = release_tracks
        .iter()
        .filter_map(|track| track.meta.disc)
        .chain(
            lyra_db::tracks::get_direct(db, release_db_id)?
                .into_iter()
                .filter_map(|track| track.disc),
        )
        .max();

    let mut persisted_track_ids = Vec::new();
    let mut changed_track_ids = Vec::new();
    let mut changed_track_id_set = HashSet::new();
    let mut target_gained_track = false;

    for track in release_tracks {
        let TrackMetadata {
            entry_db_id,
            format,
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
            media_formats: _,
            barcode: _,
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
            track_gain_db,
            album_gain_db,
        } = track.meta;

        let effective_disc_total = disc_total.or(inferred_disc_total);

        let entry_file_created_at_ms =
            lyra_db::entries::get_by_id(db, entry_db_id)?.map(|e| e.file_created_at_ms);

        let track_db_id = if let Some(track_db_id) = track.track_db_id {
            let mut existing = lyra_db::tracks::get_by_id(db, track_db_id)?.unwrap_or(Track {
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
                track_gain_db: None,
                album_gain_db: None,
                locked: None,
                created_at_ms: now_ms,
                file_created_at_ms: entry_file_created_at_ms,
            });
            // Duration and audio properties are intrinsic to the file, not tags:
            // there is no "user removed it" case, so `None` only ever means the
            // probe could not determine the value. Keep what is stored.
            if let Some(duration_ms) = duration_ms {
                existing.set_duration_ms(duration_ms);
            }
            existing.track_gain_db = track_gain_db;
            existing.album_gain_db = album_gain_db;
            if entry_file_created_at_ms.is_some() {
                existing.file_created_at_ms = entry_file_created_at_ms;
            }
            lyra_db::tracks::update_in_transaction(db, &existing)?;
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
                track_gain_db,
                album_gain_db,
                locked: None,
                created_at_ms: now_ms,
                file_created_at_ms: entry_file_created_at_ms,
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

        let track_artist_names = artists.unwrap_or_default();
        let track_artist_ids = resolve_artist_ids(db, &track_artist_names, &mut artist_cache)?;
        let mut track_layer = LocalLayer::default();
        track_layer.supply_artists(&track_artist_ids);
        track_layer.supply(MetadataField::TrackTitle, title);
        track_layer.supply(MetadataField::Year, year);
        track_layer.supply(MetadataField::Disc, disc);
        track_layer.supply(MetadataField::DiscTotal, effective_disc_total);
        track_layer.supply(MetadataField::Track, track_number);
        track_layer.supply(MetadataField::TrackTotal, track_total);
        track_layer.save(db, track_db_id)?;

        let source_kind = source_kind.unwrap_or_else(|| "embedded_tags".to_string());

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
                        lyra_db::cue::sheets::upsert(db, cue_sheet_entry_id, &cue_sheet_hash)?;
                    Some(lyra_db::cue::tracks::upsert(
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

        lyra_db::track_sources::upsert(
            db,
            track_db_id,
            entry_db_id,
            lyra_db::track_sources::TrackSourceUpsert {
                source_kind,
                source_key,
                is_primary: true,
                start_ms: segment_start_ms,
                end_ms: segment_end_ms,
                format,
            },
            cue_track_id,
        )?;

        collect_scanned_artist_relations(
            db,
            &artist_relations,
            &mut artist_cache,
            &mut scanned_relations,
            &mut scanned_artist_types,
        )?;

        let release_delta =
            lyra_db::releases::replace_track_release(db, release_db_id, track_db_id)?;
        if release_delta.membership_changed && changed_track_id_set.insert(track_db_id) {
            changed_track_ids.push(track_db_id);
        }
        target_gained_track |= release_delta.target_added;
        if !persisted_track_ids.contains(&track_db_id) {
            persisted_track_ids.push(track_db_id);
        }
    }

    for (scan_name, artist_db_id) in artist_cache {
        let stored = stored_local_layer(db, artist_db_id)?;
        let mut artist_layer = LocalLayer::default();
        artist_layer.supply(
            MetadataField::ArtistName,
            scanned_artist_name(db, artist_db_id, stored.as_ref(), scan_name)?,
        );
        artist_layer.supply(
            MetadataField::ArtistType,
            scanned_artist_type(
                stored.as_ref(),
                scanned_artist_types.get(&artist_db_id).copied(),
            )?,
        );
        if let Some(scanned) = scanned_relations.remove(&artist_db_id) {
            let stored = stored.map(|layer| layer.relations).unwrap_or_default();
            for (relation_type, inputs) in
                lyra_db::metadata::layers::union_relations(&stored, scanned)
            {
                artist_layer.supply_relations(relation_type, inputs);
            }
        }
        artist_layer.save(db, artist_db_id)?;
    }

    if target_gained_track {
        lyra_db::covers::display::mark_genre_profiles_dirty_for_release(db, release_db_id)?;
    }
    let affected_playlist_ids =
        lyra_db::covers::display::playlist_ids_for_tracks(db, &changed_track_ids)?;
    lyra_db::covers::display::sync_playlist_covers(db, &affected_playlist_ids)?;

    Ok(ReleaseIngestResult {
        release_db_id,
        track_db_ids: persisted_track_ids,
    })
}
