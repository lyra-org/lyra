// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! A viewer limited to some libraries must not learn anything about releases in the others:
//! not through filters, sort values, shuffles or lookups.

use std::collections::HashSet;

use agdb::{
    DbAny,
    DbId,
};

use super::{
    ArtistCredit,
    CreditRole,
    Direction,
    Query,
    Viewer,
    artists::{
        ArtistFilter,
        Artists,
    },
    lookups,
    order,
    page,
    pipeline::shuffle,
    releases::{
        ReleaseFilter,
        Releases,
    },
    tracks::{
        TrackFilter,
        TrackKey,
        Tracks,
    },
};
use crate::services::auth::Principal;
use lyra_db::{
    CreditType,
    fixtures::{
        connect,
        connect_credit,
        insert_artist,
        insert_release,
        insert_track,
    },
    test_db::{
        insert_library,
        insert_user,
        new_test_db,
    },
};

/// Track `a` sits on visible "Zulu", which credits no one, and on hidden "Alpha Secret", whose
/// chain is `[h, a]` and whose only artist is Aaron. Track `b` sits on visible "Middle", credited
/// to Moe, and credits Aaron itself. Track `h` is only on the hidden release.
struct Split {
    db: DbAny,
    restricted: Viewer,
    admin: Viewer,
    a: DbId,
    b: DbId,
    h: DbId,
    middle: DbId,
    secret: DbId,
    aaron: DbId,
}

fn split() -> anyhow::Result<Split> {
    let mut db = new_test_db()?;
    let shown = insert_library(&mut db, "Shown", "/visibility/shown")?;
    let hidden = insert_library(&mut db, "Hidden", "/visibility/hidden")?;
    let release = |db: &mut DbAny, title: &str, library: DbId| -> anyhow::Result<DbId> {
        let release = insert_release(db, title)?;
        connect(db, library, release)?;
        Ok(release)
    };
    let zulu = release(&mut db, "Zulu", shown)?;
    let middle = release(&mut db, "Middle", shown)?;
    let secret = release(&mut db, "Alpha Secret", hidden)?;
    let a = insert_track(&mut db, "A")?;
    let b = insert_track(&mut db, "B")?;
    let h = insert_track(&mut db, "H")?;
    for (release, track) in [(zulu, a), (secret, h), (secret, a), (middle, b)] {
        lyra_db::releases::link_track(&mut db, release, track)?;
    }
    let aaron = insert_artist(&mut db, "Aaron")?;
    let moe = insert_artist(&mut db, "Moe")?;
    connect_credit(&mut db, secret, aaron, CreditType::Artist, None, 0)?;
    connect_credit(&mut db, b, aaron, CreditType::Artist, None, 0)?;
    connect_credit(&mut db, middle, moe, CreditType::Artist, None, 0)?;
    lyra_db::chains::replace_for_release(&mut db, secret, &[vec![h, a]])??;

    let shown_id = lyra_db::libraries::get_by_id(&db, shown)?
        .expect("library exists")
        .id;
    let user = insert_user(&mut db, "listener")?;
    let restricted = Viewer::user(
        &db,
        Principal::for_user(&db, user, Vec::new(), HashSet::from([shown_id])),
    )?;
    let admin = Viewer::user(
        &db,
        Principal::for_user(&db, user, vec![lyra_db::Permission::Admin], HashSet::new()),
    )?;
    Ok(Split {
        db,
        restricted,
        admin,
        a,
        b,
        h,
        middle,
        secret,
        aaron,
    })
}

/// The ids a query finds, checked against the total its first page reports.
fn found<C: super::pipeline::Catalog>(
    db: &DbAny,
    viewer: &Viewer,
    query: &Query<C>,
) -> anyhow::Result<Vec<DbId>> {
    let ids = order(db, viewer, query)?;
    assert_eq!(page(db, viewer, query, 0, Some(1))?.total, ids.len() as u64);
    Ok(ids)
}

fn tracks_by(credit: ArtistCredit) -> Query<Tracks> {
    Query::new(TrackFilter {
        artists: Some(credit),
        ..TrackFilter::default()
    })
}

fn credit(artist: DbId, role: CreditRole) -> ArtistCredit {
    ArtistCredit {
        artists: vec![artist],
        role,
        excluding: None,
    }
}

#[test]
fn artist_filters_on_tracks_reach_only_visible_releases() -> anyhow::Result<()> {
    let split = split()?;
    let (db, viewer) = (&split.db, &split.restricted);
    for (role, expected) in [
        (CreditRole::Any, vec![split.b]),
        (CreditRole::Track, vec![split.b]),
        (CreditRole::Release, vec![]),
    ] {
        assert_eq!(
            found(db, viewer, &tracks_by(credit(split.aaron, role)))?,
            expected,
            "{role:?}"
        );
    }
    let excluding_release = ArtistCredit {
        excluding: Some(CreditRole::Release),
        ..credit(split.aaron, CreditRole::Track)
    };
    assert_eq!(found(db, viewer, &tracks_by(excluding_release))?, [split.b]);
    let without = Query::<Tracks>::new(TrackFilter {
        exclude_artists: vec![split.aaron],
        ..TrackFilter::default()
    });
    assert_eq!(found(db, viewer, &without)?, [split.a]);

    let admin = &split.admin;
    assert_eq!(
        found(
            db,
            admin,
            &tracks_by(credit(split.aaron, CreditRole::Release))
        )?,
        [split.a, split.h]
    );
    assert_eq!(found(db, admin, &without)?, Vec::<DbId>::new());
    Ok(())
}

#[test]
fn artist_filters_on_releases_and_release_scopes_on_artists_stay_visible() -> anyhow::Result<()> {
    let split = split()?;
    let db = &split.db;
    let releases_by = |role| {
        Query::<Releases>::new(ReleaseFilter {
            artists: Some(credit(split.aaron, role)),
            ..ReleaseFilter::default()
        })
    };
    assert_eq!(
        found(db, &split.restricted, &releases_by(CreditRole::Any))?,
        [split.middle]
    );
    assert_eq!(
        found(db, &split.restricted, &releases_by(CreditRole::Release))?,
        Vec::<DbId>::new()
    );
    assert_eq!(
        found(db, &split.admin, &releases_by(CreditRole::Release))?,
        [split.secret]
    );

    let artists_on = |filter| Query::<Artists>::new(filter);
    let on_secret = ArtistFilter {
        releases: Some(vec![split.secret]),
        ..ArtistFilter::default()
    };
    // A track without credits of its own takes those of its releases.
    let on_a = ArtistFilter {
        tracks: Some(vec![split.a]),
        credit_types: Some(vec![CreditType::Artist]),
        ..ArtistFilter::default()
    };
    for filter in [on_secret.clone(), on_a.clone()] {
        assert_eq!(
            found(db, &split.restricted, &artists_on(filter))?,
            Vec::<DbId>::new()
        );
    }
    for filter in [on_secret, on_a] {
        assert_eq!(found(db, &split.admin, &artists_on(filter))?, [split.aaron]);
    }
    Ok(())
}

#[test]
fn track_sort_values_come_from_visible_releases_only() -> anyhow::Result<()> {
    let split = split()?;
    let db = &split.db;
    let sorted = |viewer: &Viewer, key, direction| -> anyhow::Result<Vec<DbId>> {
        let mut query = Query::<Tracks>::new(TrackFilter {
            ids: Some(vec![split.a, split.b]),
            ..TrackFilter::default()
        });
        query.sort = vec![(key, direction)];
        let ids = order(db, viewer, &query)?;
        let mut paged = Vec::new();
        for offset in 0..ids.len() as u64 {
            paged.extend(
                page(db, viewer, &query, offset, Some(1))?
                    .items
                    .into_iter()
                    .filter_map(|track| track.item.db_id.map(DbId::from)),
            );
        }
        assert_eq!(paged, ids, "pages follow the order");
        Ok(ids)
    };
    let (a, b) = (split.a, split.b);
    for (viewer, key, ascending) in [
        (&split.restricted, TrackKey::ReleaseTitle, [b, a]),
        (&split.admin, TrackKey::ReleaseTitle, [a, b]),
        // Only the hidden release credits anyone for `a`, so it has no value and sorts last.
        (&split.restricted, TrackKey::ReleaseArtistName, [b, a]),
        (&split.admin, TrackKey::ReleaseArtistName, [a, b]),
    ] {
        assert_eq!(
            sorted(viewer, key, Direction::Ascending)?,
            ascending,
            "{key:?}"
        );
    }
    assert_eq!(
        sorted(
            &split.restricted,
            TrackKey::ReleaseArtistName,
            Direction::Descending
        )?,
        [b, a],
        "a missing value sorts last either way"
    );

    Ok(())
}

#[test]
fn hidden_chains_neither_group_tracks_nor_show_in_lookups() -> anyhow::Result<()> {
    let split = split()?;
    let db = &split.db;
    let places = lookups::chain_places(db, &split.restricted, &[split.a, split.b])?;
    assert!(places.is_empty(), "{places:?}");
    let admin_places = lookups::chain_places(db, &split.admin, &[split.a])?;
    assert_eq!(admin_places[&split.a].position, 1);
    let releases = lookups::releases_by_track(db, &split.restricted, &[split.a])?;
    let titles = releases[&split.a]
        .iter()
        .map(|release| release.release_title.as_str())
        .collect::<Vec<_>>();
    assert_eq!(titles, ["Zulu"]);

    let mut query = Query::<Tracks>::new(TrackFilter::default());
    query.sort = vec![(TrackKey::Random, Direction::Ascending)];
    let mut own = vec![split.a, split.b];
    for seed in 0..32 {
        query.seed = seed;
        own.sort_by_key(|id| (shuffle(seed, *id), id.0));
        assert_eq!(order(db, &split.restricted, &query)?, own, "seed {seed}");
        let admin = order(db, &split.admin, &query)?;
        let h = admin.iter().position(|id| *id == split.h);
        let a = admin.iter().position(|id| *id == split.a);
        assert_eq!(
            a,
            h.map(|h| h + 1),
            "the chain keeps h before a for an admin"
        );
    }
    Ok(())
}

/// Every catalog's results for a viewer limited to library `shown` and filtered to library
/// `hidden`, which holds "Shared" with `shown` when `shared` and nothing otherwise.
fn filtered_to_a_hidden_library(shared: bool, admin: bool) -> anyhow::Result<[Vec<DbId>; 4]> {
    use super::genres::{
        GenreFilter,
        Genres,
    };
    let mut db = new_test_db()?;
    let shown = insert_library(&mut db, "Shown", "/visibility/shown")?;
    let hidden = insert_library(&mut db, "Hidden", "/visibility/hidden")?;
    let release = insert_release(&mut db, "Shared")?;
    connect(&mut db, shown, release)?;
    if shared {
        connect(&mut db, hidden, release)?;
    }
    let track = insert_track(&mut db, "Song")?;
    lyra_db::releases::link_track(&mut db, release, track)?;
    let artist = insert_artist(&mut db, "Singer")?;
    connect_credit(&mut db, release, artist, CreditType::Artist, None, 0)?;
    lyra_db::genres::sync_release_genres(&mut db, release, &["Rock".to_string()])?;
    let shown_id = lyra_db::libraries::get_by_id(&db, shown)?
        .expect("library exists")
        .id;
    let user = insert_user(&mut db, "listener")?;
    let (permissions, libraries) = if admin {
        (vec![lyra_db::Permission::Admin], HashSet::new())
    } else {
        (Vec::new(), HashSet::from([shown_id]))
    };
    let viewer = Viewer::user(&db, Principal::for_user(&db, user, permissions, libraries))?;
    let library = Some(hidden);
    Ok([
        found(
            &db,
            &viewer,
            &Query::<Tracks>::new(TrackFilter {
                library,
                ..TrackFilter::default()
            }),
        )?,
        found(
            &db,
            &viewer,
            &Query::<Releases>::new(ReleaseFilter {
                library,
                ..ReleaseFilter::default()
            }),
        )?,
        found(
            &db,
            &viewer,
            &Query::<Artists>::new(ArtistFilter {
                library,
                ..ArtistFilter::default()
            }),
        )?,
        found(
            &db,
            &viewer,
            &Query::<Genres>::new(GenreFilter {
                library,
                ..GenreFilter::default()
            }),
        )?,
    ])
}

#[test]
fn a_library_filter_shows_nothing_of_a_library_the_viewer_lacks() -> anyhow::Result<()> {
    let empty: [Vec<DbId>; 4] = Default::default();
    assert_eq!(filtered_to_a_hidden_library(true, false)?, empty);
    assert_eq!(filtered_to_a_hidden_library(false, false)?, empty);
    assert!(
        filtered_to_a_hidden_library(true, true)?
            .iter()
            .all(|found| found.len() == 1),
        "an admin sees what the library holds"
    );
    assert_eq!(filtered_to_a_hidden_library(false, true)?, empty);
    Ok(())
}

#[test]
fn numbers_cover_only_visible_ids_and_numeric_keys() -> anyhow::Result<()> {
    use super::releases::ReleaseKey;
    let split = split()?;
    let db = &split.db;
    let counts = |viewer: &Viewer, ids: Vec<DbId>| {
        super::numbers::<Releases>(
            db,
            viewer,
            &ReleaseFilter::default(),
            ids,
            &[ReleaseKey::TrackCount, ReleaseKey::Name],
        )
    };
    assert_eq!(
        counts(&split.restricted, vec![split.secret, split.middle])?
            .into_iter()
            .collect::<Vec<_>>(),
        [(split.middle, vec![Some(1), None])],
        "the hidden release is left out and the name key has no number"
    );
    assert_eq!(
        counts(&split.admin, vec![split.secret])?[&split.secret],
        [Some(2), None]
    );

    let mut empty = new_test_db()?;
    let user = insert_user(&mut empty, "nobody")?;
    let nobody = Viewer::user(
        &empty,
        Principal::for_user(&empty, user, Vec::new(), HashSet::new()),
    )?;
    let release = insert_release(&mut empty, "Loose")?;
    assert!(
        super::numbers::<Releases>(
            &empty,
            &nobody,
            &ReleaseFilter::default(),
            vec![release],
            &[ReleaseKey::TrackCount],
        )?
        .is_empty()
    );

    // Name keys carry places in a name order that hidden entities shift, so none come back.
    let names = super::numbers::<Tracks>(
        db,
        &split.restricted,
        &TrackFilter::default(),
        vec![split.a],
        &[TrackKey::ReleaseTitle, TrackKey::Name, TrackKey::Duration],
    )?;
    assert_eq!(names[&split.a][..2], [None, None]);
    Ok(())
}
