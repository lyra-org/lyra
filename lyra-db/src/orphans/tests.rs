// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::BTreeMap;

use agdb::{
    DbAny,
    DbId,
};

use super::{
    testing::{
        assert_artists_tracked,
        assert_collected,
        assert_drained,
        drain_artists,
        unreferenced_artists,
    },
    *,
};
use crate::{
    ArtistRelationType,
    CreditType,
    MetadataLayer,
    artists,
    credits::{
        self,
        CreditLinkInput,
    },
    fixtures::{
        insert_file_entry,
        insert_release,
        insert_track,
        link_track,
    },
    metadata::{
        cascade_remove_entities,
        layers::{
            self,
            edges::{
                LayerCreditInput,
                LayerRelationInput,
            },
        },
    },
    releases,
    test_db::{
        connect_artist,
        insert_library,
        new_test_db,
    },
    track_sources::{
        self,
        TrackSourceUpsert,
    },
    tracks,
};

fn source(key: &str) -> TrackSourceUpsert {
    TrackSourceUpsert {
        source_kind: "embedded_tags".to_string(),
        source_key: key.to_string(),
        is_primary: true,
        start_ms: None,
        end_ms: None,
        format: Default::default(),
    }
}

/// A track of `release` playing a file of its own in `library`. Returns the track and its
/// entry.
fn playable_track(
    db: &mut DbAny,
    library: DbId,
    release: DbId,
    name: &str,
) -> anyhow::Result<(DbId, DbId)> {
    let entry = insert_file_entry(db, library, name)?;
    let track = insert_track(db, name)?;
    link_track(db, release, track)?;
    db.transaction_mut(|t| track_sources::upsert(t, track, entry, source(name), None))?;
    Ok((track, entry))
}

/// A cue sheet in `library` whose segments of one audio file are the tracks of `release`.
/// Returns the tracks, the sheet and the cue tracks.
fn cue_tracks(
    db: &mut DbAny,
    library: DbId,
    release: DbId,
    count: u32,
) -> anyhow::Result<(Vec<DbId>, DbId, Vec<DbId>)> {
    let audio = insert_file_entry(db, library, "image.flac")?;
    let cue = insert_file_entry(db, library, "image.cue")?;
    let mut tracks = Vec::new();
    let mut cue_tracks = Vec::new();
    let mut sheet = DbId(0);
    for track_no in 1..=count {
        let track = insert_track(db, &format!("Segment {track_no}"))?;
        link_track(db, release, track)?;
        db.transaction_mut(|t| -> anyhow::Result<()> {
            sheet = crate::cue::sheets::upsert(t, cue, "hash")?;
            let cue_track =
                crate::cue::tracks::upsert(t, sheet, cue, track_no, audio, None, track_no * 100)?;
            track_sources::upsert(
                t,
                track,
                audio,
                source(&format!("cue:{track_no}")),
                Some(cue_track),
            )?;
            cue_tracks.push(cue_track);
            Ok(())
        })?;
        tracks.push(track);
    }
    Ok((tracks, sheet, cue_tracks))
}

fn exists(db: &DbAny, id: DbId, kind: &str) -> anyhow::Result<bool> {
    graph::is_kind(db, id, kind)
}

fn relating(artist_id: DbId) -> BTreeMap<ArtistRelationType, Vec<LayerRelationInput>> {
    BTreeMap::from([(
        ArtistRelationType::MemberOf,
        vec![LayerRelationInput {
            artist_id,
            attributes: None,
        }],
    )])
}

fn crediting(artist_id: DbId) -> BTreeMap<CreditType, Vec<LayerCreditInput>> {
    BTreeMap::from([(
        CreditType::Artist,
        vec![LayerCreditInput {
            artist_id,
            detail: None,
        }],
    )])
}

/// A band whose layer names `member` as one, which makes the member's relation to it.
fn band_of(db: &mut DbAny, member: DbId) -> anyhow::Result<DbId> {
    let band = artists::create(db, "Band", None)?;
    layers::upsert(
        db,
        band,
        &MetadataLayer::new("provider", Default::default(), 1).with_relations(relating(member)),
    )?;
    artists::relations::link(db, member, band, ArtistRelationType::MemberOf, None)?;
    Ok(band)
}

#[test]
fn removing_a_releases_last_track_collects_the_release_and_the_source() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let library = insert_library(&mut db, "Music", "/music")?;
    let emptied = insert_release(&mut db, "Emptied")?;
    let kept = insert_release(&mut db, "Kept")?;
    let (removed, _) = playable_track(&mut db, library, emptied, "one.flac")?;
    let (first, _) = playable_track(&mut db, library, kept, "two.flac")?;
    let (second, _) = playable_track(&mut db, library, kept, "three.flac")?;
    let removed_source = neighbor_ids(&db, removed, "TrackSource")?[0];
    assert_collected(&db)?;

    cascade_remove_entities(&mut db, &[removed, first])?;

    assert!(!exists(&db, emptied, "Release")?);
    assert!(!exists(&db, removed_source, "TrackSource")?);
    assert!(exists(&db, kept, "Release")?);
    assert!(exists(&db, second, "Track")?);
    assert_drained(&db)
}

#[test]
fn a_source_two_tracks_play_stays_until_both_are_gone() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let library = insert_library(&mut db, "Music", "/music")?;
    let release = insert_release(&mut db, "Release")?;
    let (first, entry) = playable_track(&mut db, library, release, "one.flac")?;
    let second = insert_track(&mut db, "Second")?;
    link_track(&mut db, release, second)?;
    db.transaction_mut(|t| track_sources::upsert(t, second, entry, source("one.flac"), None))?;
    let shared = neighbor_ids(&db, first, "TrackSource")?[0];

    cascade_remove_entities(&mut db, &[first])?;
    assert!(exists(&db, shared, "TrackSource")?);
    assert_collected(&db)?;

    cascade_remove_entities(&mut db, &[second])?;
    assert!(!exists(&db, shared, "TrackSource")?);
    assert!(!exists(&db, release, "Release")?);
    assert_drained(&db)
}

#[test]
fn a_cue_sheet_goes_with_its_last_cue_track() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let library = insert_library(&mut db, "Music", "/music")?;
    let release = insert_release(&mut db, "Release")?;
    let (tracks, sheet, cue_tracks) = cue_tracks(&mut db, library, release, 2)?;
    assert_collected(&db)?;

    cascade_remove_entities(&mut db, &[tracks[0]])?;
    assert!(!exists(&db, cue_tracks[0], "CueTrack")?);
    assert!(exists(&db, cue_tracks[1], "CueTrack")?);
    assert!(exists(&db, sheet, "CueSheet")?);
    assert_collected(&db)?;

    cascade_remove_entities(&mut db, &[tracks[1]])?;
    assert!(!exists(&db, cue_tracks[1], "CueTrack")?);
    assert!(!exists(&db, sheet, "CueSheet")?);
    assert_drained(&db)
}

#[test]
fn a_source_pointed_at_another_cue_track_collects_the_one_it_left() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let library = insert_library(&mut db, "Music", "/music")?;
    let release = insert_release(&mut db, "Release")?;
    let (tracks, sheet, cue_tracks) = cue_tracks(&mut db, library, release, 1)?;
    let audio = neighbor_ids(&db, cue_tracks[0], "Entry")?[0];
    let cue = neighbor_ids(&db, sheet, "Entry")?[0];

    let moved = db.transaction_mut(|t| -> anyhow::Result<DbId> {
        let cue_track = crate::cue::tracks::upsert(t, sheet, cue, 2, audio, None, 500)?;
        track_sources::upsert(t, tracks[0], audio, source("cue:1"), Some(cue_track))?;
        Ok(cue_track)
    })?;

    assert!(!exists(&db, cue_tracks[0], "CueTrack")?);
    assert!(exists(&db, moved, "CueTrack")?);
    assert!(exists(&db, sheet, "CueSheet")?);
    assert_collected(&db)
}

#[test]
fn collecting_leaves_what_something_still_holds() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let library = insert_library(&mut db, "Music", "/music")?;
    let release = insert_release(&mut db, "Release")?;
    let (track, _) = playable_track(&mut db, library, release, "one.flac")?;
    let track_source = neighbor_ids(&db, track, "TrackSource")?[0];
    let user = crate::test_db::insert_user(&mut db, "listener")?;

    db.transaction_mut(|t| {
        collect(
            t,
            Candidates {
                tracks: vec![track, release, user, DbId(999_999)],
                releases: vec![release, track],
                track_sources: vec![track_source, user],
                cue_tracks: vec![track_source],
                cue_sheets: vec![release],
            },
        )
    })?;

    assert!(exists(&db, track, "Track")?);
    assert!(exists(&db, release, "Release")?);
    assert!(exists(&db, track_source, "TrackSource")?);
    assert!(exists(&db, user, "User")?);
    Ok(())
}

#[test]
fn a_track_that_lost_its_file_goes_with_its_release() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let library = insert_library(&mut db, "Music", "/music")?;
    let release = insert_release(&mut db, "Release")?;
    let (track, _) = playable_track(&mut db, library, release, "one.flac")?;
    let track_source = neighbor_ids(&db, track, "TrackSource")?[0];
    let artist = artists::create(&mut db, "Artist", None)?;
    connect_artist(&mut db, track, artist)?;
    drain_artists(&mut db)?;
    assert!(!is_artist_candidate(&db, artist)?);

    db.transaction_mut(|t| -> anyhow::Result<()> {
        graph::remove_edges_between(t, track, track_source)?;
        collect(
            t,
            Candidates {
                tracks: vec![track],
                track_sources: vec![track_source],
                ..Default::default()
            },
        )?;
        Ok(())
    })?;

    assert!(!exists(&db, track, "Track")?);
    assert!(!exists(&db, release, "Release")?);
    assert!(!exists(&db, track_source, "TrackSource")?);
    assert!(
        exists(&db, artist, "Artist")?,
        "an artist waits for its collection"
    );
    assert!(is_artist_candidate(&db, artist)?);
    assert_collected(&db)?;

    assert_eq!(drain_artists(&mut db)?, 1);
    assert!(!exists(&db, artist, "Artist")?);
    assert_drained(&db)
}

#[test]
fn an_artist_nothing_ever_references_goes_at_the_next_collection() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let artist = artists::create(&mut db, "Ensured", None)?;
    layers::upsert(
        &mut db,
        artist,
        &MetadataLayer::new("provider", Default::default(), 1),
    )?;
    crate::external_ids::upsert(
        &mut db,
        artist,
        "provider",
        "artist_id",
        "a-1",
        crate::IdSource::Resolved,
    )?;

    assert!(exists(&db, artist, "Artist")?);
    assert!(is_artist_candidate(&db, artist)?);
    assert_collected(&db)?;

    assert_eq!(drain_artists(&mut db)?, 1);
    assert!(!exists(&db, artist, "Artist")?);
    assert_drained(&db)
}

#[test]
fn a_referenced_candidate_stays_and_stops_being_one() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let track = insert_track(&mut db, "Track")?;
    let credited = artists::create(&mut db, "Credited", None)?;
    let layered = artists::create(&mut db, "Layered", None)?;
    connect_artist(&mut db, track, credited)?;
    layers::upsert(
        &mut db,
        track,
        &MetadataLayer::new("provider", Default::default(), 1).with_credits(crediting(layered)),
    )?;

    assert_eq!(drain_artists(&mut db)?, 0);

    for artist in [credited, layered] {
        assert!(exists(&db, artist, "Artist")?);
        assert!(!is_artist_candidate(&db, artist)?);
    }
    assert!(unreferenced_artists(&db)?.is_empty());
    assert_artists_tracked(&db)
}

#[test]
fn an_artist_dropped_by_one_write_and_credited_by_a_later_one_survives() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let release = insert_release(&mut db, "Release")?;
    let track = insert_track(&mut db, "Track")?;
    let moved = artists::create(&mut db, "Moved", None)?;
    let dropped = artists::create(&mut db, "Dropped", None)?;
    let credit = |artist_id| CreditLinkInput {
        artist_id,
        credit_type: CreditType::Artist,
        detail: None,
    };
    db.transaction_mut(|t| {
        credits::replace_for_owner(t, release, &[credit(moved), credit(dropped)])
    })?;
    drain_artists(&mut db)?;
    assert!(!is_artist_candidate(&db, moved)?);

    db.transaction_mut(|t| credits::replace_for_owner(t, release, &[]))?;
    for artist in [moved, dropped] {
        assert!(
            exists(&db, artist, "Artist")?,
            "the write that drops an artist leaves it"
        );
        assert!(is_artist_candidate(&db, artist)?);
    }
    assert_artists_tracked(&db)?;

    db.transaction_mut(|t| credits::replace_for_owner(t, track, &[credit(moved)]))?;
    assert_eq!(drain_artists(&mut db)?, 1);

    assert!(exists(&db, moved, "Artist")?);
    assert!(!is_artist_candidate(&db, moved)?);
    assert!(!exists(&db, dropped, "Artist")?);
    assert!(unreferenced_artists(&db)?.is_empty());
    Ok(())
}

#[test]
fn a_layer_that_stops_naming_an_artist_makes_it_a_candidate() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let track = insert_track(&mut db, "Track")?;
    let named = artists::create(&mut db, "Named", None)?;
    let layer =
        |credits| MetadataLayer::new("provider", Default::default(), 1).with_credits(credits);
    layers::upsert(&mut db, track, &layer(crediting(named)))?;
    drain_artists(&mut db)?;
    assert!(exists(&db, named, "Artist")?);

    layers::upsert(
        &mut db,
        track,
        &layer(BTreeMap::from([(CreditType::Artist, Vec::new())])),
    )?;
    assert!(exists(&db, named, "Artist")?);
    assert!(is_artist_candidate(&db, named)?);
    assert_artists_tracked(&db)?;

    layers::upsert(&mut db, track, &layer(crediting(named)))?;
    db.transaction_mut(|t| layers::remove_inside_tx(t, track, "provider"))?;
    assert!(is_artist_candidate(&db, named)?);
    assert_artists_tracked(&db)?;

    assert_eq!(drain_artists(&mut db)?, 1);
    assert!(!exists(&db, named, "Artist")?);
    Ok(())
}

#[test]
fn artists_that_only_reference_each_other_go_together() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let track = insert_track(&mut db, "Track")?;
    let member = artists::create(&mut db, "Member", None)?;
    let band = band_of(&mut db, member)?;
    let alias = artists::create(&mut db, "Alias", None)?;
    artists::relations::link(&mut db, band, alias, ArtistRelationType::VoiceActor, None)?;
    connect_artist(&mut db, track, band)?;
    assert_eq!(drain_artists(&mut db)?, 0);
    assert!(!has_artist_candidates(&db)?);

    cascade_remove_entities(&mut db, &[track])?;

    assert!(is_artist_candidate(&db, band)?);
    assert!(!is_artist_candidate(&db, member)?);
    for artist in [band, member, alias] {
        assert!(exists(&db, artist, "Artist")?);
    }
    assert_artists_tracked(&db)?;

    assert_eq!(drain_artists(&mut db)?, 3);
    for artist in [band, member, alias] {
        assert!(!exists(&db, artist, "Artist")?);
    }
    assert_drained(&db)
}

#[test]
fn artists_that_reference_each_other_stay_while_one_is_referenced() -> anyhow::Result<()> {
    for by_layer in [false, true] {
        let mut db = new_test_db()?;
        let track = insert_track(&mut db, "Track")?;
        let other = insert_track(&mut db, "Other")?;
        let member = artists::create(&mut db, "Member", None)?;
        let band = band_of(&mut db, member)?;
        connect_artist(&mut db, track, band)?;
        if by_layer {
            layers::upsert(
                &mut db,
                other,
                &MetadataLayer::new("provider", Default::default(), 1)
                    .with_credits(crediting(member)),
            )?;
        } else {
            connect_artist(&mut db, other, member)?;
        }
        drain_artists(&mut db)?;

        cascade_remove_entities(&mut db, &[track])?;
        assert!(is_artist_candidate(&db, band)?);

        assert_eq!(drain_artists(&mut db)?, 0, "by layer: {by_layer}");
        for artist in [band, member] {
            assert!(exists(&db, artist, "Artist")?);
            assert!(!is_artist_candidate(&db, artist)?);
        }
        assert!(unreferenced_artists(&db)?.is_empty());
    }
    Ok(())
}

#[test]
fn removing_an_artist_makes_candidates_of_what_it_referenced() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let track = insert_track(&mut db, "Track")?;
    let member = artists::create(&mut db, "Member", None)?;
    let band = band_of(&mut db, member)?;
    let alias = artists::create(&mut db, "Alias", None)?;
    artists::relations::link(&mut db, band, alias, ArtistRelationType::VoiceActor, None)?;
    connect_artist(&mut db, track, band)?;
    drain_artists(&mut db)?;

    cascade_remove_entities(&mut db, &[band])?;

    assert!(is_artist_candidate(&db, member)?);
    assert!(is_artist_candidate(&db, alias)?);
    assert_artists_tracked(&db)?;
    assert_eq!(drain_artists(&mut db)?, 2);
    assert!(unreferenced_artists(&db)?.is_empty());
    Ok(())
}

#[test]
fn a_dropped_relation_makes_a_candidate_of_the_artist_it_led_to() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let track = insert_track(&mut db, "Track")?;
    let actor = artists::create(&mut db, "Actor", None)?;
    let character = artists::create(&mut db, "Character", None)?;
    connect_artist(&mut db, track, actor)?;
    artists::relations::link(
        &mut db,
        actor,
        character,
        ArtistRelationType::VoiceActor,
        None,
    )?;
    assert_eq!(drain_artists(&mut db)?, 0);

    db.transaction_mut(|t| {
        artists::relations::unlink(t, actor, character, ArtistRelationType::VoiceActor)
    })?;

    assert!(is_artist_candidate(&db, character)?);
    assert_artists_tracked(&db)?;
    assert_eq!(drain_artists(&mut db)?, 1);
    assert!(exists(&db, actor, "Artist")?);
    assert!(!exists(&db, character, "Artist")?);
    Ok(())
}

#[test]
fn a_collection_checks_a_batch_at_a_time_and_says_when_more_wait() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let track = insert_track(&mut db, "Track")?;
    let mut artists = Vec::new();
    for index in 0..5 {
        let artist = artists::create(&mut db, &format!("Artist {index}"), None)?;
        if index % 2 == 0 {
            connect_artist(&mut db, track, artist)?;
        }
        artists.push(artist);
    }

    let first = collect_artist_candidates(&mut db, 2)?;
    assert_eq!((first.removed, first.remaining), (1, true));
    let second = collect_artist_candidates(&mut db, 2)?;
    assert_eq!((second.removed, second.remaining), (1, true));
    let third = collect_artist_candidates(&mut db, 2)?;
    assert_eq!((third.removed, third.remaining), (0, false));

    for (index, artist) in artists.into_iter().enumerate() {
        assert_eq!(exists(&db, artist, "Artist")?, index % 2 == 0);
    }
    assert_eq!(
        roots::member_ids(&db, Root::Artists)?.len(),
        3,
        "the artists root follows"
    );
    Ok(())
}

#[test]
fn a_removed_candidates_reissued_id_is_no_candidate() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let artist = artists::create(&mut db, "Gone", None)?;
    cascade_remove_entities(&mut db, &[artist])?;

    let track = insert_track(&mut db, "Reissued")?;

    assert_eq!(track, artist, "agdb reuses the freed id");
    assert!(!has_artist_candidates(&db)?);
    assert_eq!(drain_artists(&mut db)?, 0);
    assert!(tracks::get_by_id(&db, track)?.is_some());
    Ok(())
}

#[test]
fn candidates_survive_reopening_the_database() -> anyhow::Result<()> {
    use crate::bootstrap::{
        DbKind,
        initialize,
        open,
    };

    let dir = tempfile::tempdir()?;
    let path = dir.path().join("lyra.db");
    let path = path.to_str().expect("utf-8 path");
    let (kept, dropped) = {
        let mut db = open(DbKind::File, path)?;
        initialize(&mut db)?;
        let track = insert_track(&mut db, "Track")?;
        let kept = artists::create(&mut db, "Kept", None)?;
        let dropped = artists::create(&mut db, "Dropped", None)?;
        connect_artist(&mut db, track, kept)?;
        (kept, dropped)
    };

    let mut db = open(DbKind::File, path)?;
    initialize(&mut db)?;

    assert!(is_artist_candidate(&db, kept)?);
    assert!(is_artist_candidate(&db, dropped)?);
    assert_eq!(drain_artists(&mut db)?, 1);
    assert!(exists(&db, kept, "Artist")?);
    assert!(!exists(&db, dropped, "Artist")?);
    Ok(())
}

#[test]
fn moving_a_releases_last_track_away_names_it_for_the_caller_to_collect() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let library = insert_library(&mut db, "Music", "/music")?;
    let old = insert_release(&mut db, "Old")?;
    let new = insert_release(&mut db, "New")?;
    let (track, _) = playable_track(&mut db, library, old, "one.flac")?;
    let (other, _) = playable_track(&mut db, library, new, "two.flac")?;

    db.transaction_mut(|t| -> anyhow::Result<()> {
        let unchanged = releases::replace_track_release(t, new, other)?;
        assert!(!unchanged.membership_changed);
        assert!(unchanged.left.releases.is_empty());
        let delta = releases::replace_track_release(t, new, track)?;
        assert_eq!(delta.left.releases, vec![old]);
        collect(t, delta.left)?;
        Ok(())
    })?;
    assert!(!exists(&db, old, "Release")?);

    db.transaction_mut(|t| -> anyhow::Result<()> {
        let mut left = releases::unlink_track(t, new, track)?;
        assert_eq!(left.releases, vec![new]);
        left.extend(releases::unlink_track(t, new, other)?);
        collect(t, left)?;
        Ok(())
    })?;
    assert!(!exists(&db, new, "Release")?);
    Ok(())
}

#[test]
fn an_artist_referencing_a_referenced_one_is_not_taken_for_referenced() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let track = insert_track(&mut db, "Track")?;
    let credited = artists::create(&mut db, "Credited", None)?;
    let through = artists::create(&mut db, "Through", None)?;
    let hanger_on = artists::create(&mut db, "Hanger-on", None)?;
    let reached = artists::create(&mut db, "Reached", None)?;
    connect_artist(&mut db, track, credited)?;
    // `reached` is referenced through `through`, which `credited` references; `hanger_on`
    // references `reached` and nothing references it.
    artists::relations::link(
        &mut db,
        credited,
        through,
        ArtistRelationType::MemberOf,
        None,
    )?;
    artists::relations::link(
        &mut db,
        through,
        reached,
        ArtistRelationType::MemberOf,
        None,
    )?;
    artists::relations::link(
        &mut db,
        hanger_on,
        reached,
        ArtistRelationType::VoiceActor,
        None,
    )?;

    // One pass walks them all, newest first: `reached` from `hanger_on` before `through`.
    let pass = collect_artist_candidates(&mut db, 16)?;

    assert_eq!(pass.removed, 1);
    assert!(!exists(&db, hanger_on, "Artist")?);
    assert_eq!(drain_artists(&mut db)?, 0);
    for artist in [credited, through, reached] {
        assert!(exists(&db, artist, "Artist")?);
    }
    assert!(unreferenced_artists(&db)?.is_empty());
    Ok(())
}

#[test]
fn a_pass_removes_no_more_artists_than_its_limit() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let track = insert_track(&mut db, "Track")?;
    let band = artists::create(&mut db, "Band", None)?;
    // Six members, each relating to the band, whose layer names them all.
    let mut members = Vec::new();
    for index in 0..6 {
        let member = artists::create(&mut db, &format!("Member {index}"), None)?;
        artists::relations::link(&mut db, member, band, ArtistRelationType::MemberOf, None)?;
        members.push(member);
    }
    layers::upsert(
        &mut db,
        band,
        &MetadataLayer::new("provider", Default::default(), 1).with_relations(BTreeMap::from([(
            ArtistRelationType::MemberOf,
            members
                .iter()
                .map(|&artist_id| LayerRelationInput {
                    artist_id,
                    attributes: None,
                })
                .collect(),
        )])),
    )?;
    connect_artist(&mut db, track, band)?;
    assert_eq!(drain_artists(&mut db)?, 0);
    cascade_remove_entities(&mut db, &[track])?;

    let first = collect_artist_candidates(&mut db, 3)?;
    assert_eq!((first.removed, first.remaining), (3, true));
    assert!(
        exists(&db, band, "Artist")?,
        "the candidate stays for the rest"
    );
    let mut left = 0;
    for &member in &members {
        left += usize::from(exists(&db, member, "Artist")?);
    }
    assert_eq!(left, 3);
    assert_artists_tracked(&db)?;

    let second = collect_artist_candidates(&mut db, 3)?;
    assert_eq!((second.removed, second.remaining), (3, true));
    let third = collect_artist_candidates(&mut db, 3)?;
    assert_eq!((third.removed, third.remaining), (1, false));
    assert_drained(&db)
}
