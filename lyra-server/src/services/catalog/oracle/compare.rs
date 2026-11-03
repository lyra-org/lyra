// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Random catalogs queried through the index and through the scanning pipeline, which must agree.

use std::sync::Arc;

use super::*;
use crate::services::catalog::{
    index::Snapshot,
    pipeline::Context,
};
use crate::services::{
    auth::Principal,
    catalog::{
        self,
        NameRange,
    },
};
use lyra_db::{
    ArtistType,
    fixtures::{
        connect,
        connect_credit,
        insert_artist,
        insert_listen,
        insert_release,
        insert_track,
    },
    ratings::{
        RatingFilter,
        RatingValue,
    },
    test_db::{
        insert_library,
        insert_user,
        new_test_db,
    },
};

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, bound: usize) -> usize {
        (self.next() % bound as u64) as usize
    }

    fn chance(&mut self, percent: u64) -> bool {
        self.next() % 100 < percent
    }

    fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len())]
    }
}

const ARTIST_NAMES: &[&str] = &[
    "Beyoncé",
    "Beyonce Tribute",
    "AC/DC",
    "Guns N' Roses",
    "Sigur Rós",
    "東京事変",
    "!!!",
    "$uicideboy$",
    "Mötley Crüe",
    "The Blue Notes",
    "blue",
    "Ayumi",
];
const WORDS: &[&str] = &[
    "Blue",
    "Moon",
    "Rose",
    "Train",
    "Tokyo",
    "夜に駆ける",
    "Été",
    "Don’t",
    "Stop",
    "Rock",
    "Roll",
    "Live",
    "(Remix)",
    "AC",
    "DC",
    "Ayu",
    "Ayumi",
    "Zero",
];
const GENRES: &[&str] = &["Rock", "Jazz", "Acid Jazz", "Ambient", "J-Pop"];

struct Fixture {
    db: DbAny,
    user: DbId,
    libraries: Vec<(DbId, String)>,
    tracks: Vec<DbId>,
    releases: Vec<DbId>,
    artists: Vec<DbId>,
    genres: Vec<DbId>,
}

fn title(rng: &mut Rng) -> String {
    (0..1 + rng.below(3))
        .map(|_| *rng.pick(WORDS))
        .collect::<Vec<_>>()
        .join(" ")
}

fn fixture(seed: u64) -> anyhow::Result<Fixture> {
    let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
    let mut db = new_test_db()?;
    let user = insert_user(&mut db, "oracle")?;
    let mut libraries = Vec::new();
    for index in 0..3 {
        let library = insert_library(
            &mut db,
            &format!("Library {index}"),
            &format!("/oracle/{index}"),
        )?;
        let public = lyra_db::libraries::get_by_id(&db, library)?
            .expect("library exists")
            .id;
        libraries.push((library, public));
    }

    let mut artists = Vec::new();
    for name in ARTIST_NAMES {
        let artist_db_id = insert_artist(&mut db, name)?;
        let mut artist = lyra_db::artists::get_by_id(&db, artist_db_id)?.expect("artist exists");
        if rng.chance(30) {
            artist.sort_name = Some(format!("{} sort", rng.pick(WORDS)));
        }
        if rng.chance(50) {
            artist.artist_type = Some(*rng.pick(&[ArtistType::Person, ArtistType::Group]));
        }
        if rng.chance(20) {
            artist.scan_aliases = vec![rng.pick(WORDS).to_lowercase()];
        }
        lyra_db::artists::update(&mut db, &artist)?;
        artists.push(artist_db_id);
    }

    let credit = |db: &mut DbAny, rng: &mut Rng, owner: DbId, artists: &[DbId]| {
        for order in 0..rng.below(3) {
            let artist = *rng.pick(artists);
            let credit_type = *rng.pick(&[
                CreditType::Artist,
                CreditType::Artist,
                CreditType::Composer,
                CreditType::Producer,
            ]);
            connect_credit(db, owner, artist, credit_type, None, order as u64 % 2)?;
        }
        anyhow::Ok(())
    };

    let mut releases = Vec::new();
    for _ in 0..14 {
        let release_db_id = insert_release(&mut db, &title(&mut rng))?;
        let mut release =
            lyra_db::releases::get_by_id(&db, release_db_id)?.expect("release exists");
        if rng.chance(25) {
            release.sort_title = Some(title(&mut rng));
        }
        release.release_date = match rng.below(4) {
            0 => None,
            1 => Some(format!("{}", 1990 + rng.below(4))),
            2 => Some(format!("{}-0{}", 1990 + rng.below(4), 1 + rng.below(9))),
            _ => Some(format!(
                "{}-0{}-1{}",
                1990 + rng.below(4),
                1 + rng.below(9),
                rng.below(9)
            )),
        };
        release.file_created_at_ms = rng.chance(80).then(|| rng.below(5) as u64);
        lyra_db::releases::update(&mut db, &release)?;
        for (library, _) in &libraries {
            if rng.chance(40) {
                connect(&mut db, *library, release_db_id)?;
            }
        }
        let genres = (0..rng.below(3))
            .map(|_| rng.pick(GENRES).to_string())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        lyra_db::genres::sync_release_genres(&mut db, release_db_id, &genres)?;
        credit(&mut db, &mut rng, release_db_id, &artists)?;
        releases.push(release_db_id);
    }

    let mut tracks = Vec::new();
    for _ in 0..60 {
        let track_db_id = insert_track(&mut db, &title(&mut rng))?;
        let mut track = lyra_db::tracks::get_by_id(&db, track_db_id)?.expect("track exists");
        if rng.chance(20) {
            track.sort_title = Some(title(&mut rng));
        }
        track.year = rng.chance(70).then(|| 1990 + rng.below(4) as u32);
        track.duration_ms = rng.chance(90).then(|| 1000 * rng.below(5) as u64);
        track.disc = rng.chance(60).then(|| 1 + rng.below(2) as u32);
        track.track = rng.chance(80).then(|| 1 + rng.below(6) as u32);
        track.file_created_at_ms = rng.chance(80).then(|| rng.below(5) as u64);
        lyra_db::tracks::update(&mut db, &track)?;
        for _ in 0..rng.below(3) {
            let release = *rng.pick(&releases);
            if !lyra_db::graph::edge_exists(&db, release, track_db_id)? {
                lyra_db::releases::link_track(&mut db, release, track_db_id)?;
            }
        }
        credit(&mut db, &mut rng, track_db_id, &artists)?;
        tracks.push(track_db_id);
    }

    for release in &releases {
        let on = lyra_db::graph::neighbor_ids(&db, *release, "Track")?;
        if on.len() >= 2 && rng.chance(60) {
            // A track sits in one chain at most, so some of these are refused.
            let _ = lyra_db::chains::replace_for_release(&mut db, *release, &[on[..2].to_vec()])?;
        }
    }
    for track in &tracks {
        if rng.chance(30) {
            insert_listen(&mut db, user, *track, rng.below(1000) as u64)?;
        }
        if rng.chance(15) {
            lyra_db::favorites::add(
                &mut db,
                user,
                *track,
                FavoriteKind::Track,
                rng.below(9) as i64,
            )?;
        }
    }
    for (targets, kind) in [
        (&releases, FavoriteKind::Release),
        (&artists, FavoriteKind::Artist),
    ] {
        for target in targets {
            if rng.chance(25) {
                lyra_db::favorites::add(&mut db, user, *target, kind, rng.below(9) as i64)?;
            }
        }
    }
    for target in tracks.iter().chain(&releases).chain(&artists) {
        if rng.chance(25) {
            let value = RatingValue::new(1 + rng.below(5) as u8).expect("a rating");
            lyra_db::ratings::upsert(&mut db, user, *target, value, rng.below(9) as i64)?;
        }
    }
    let genres = GENRES
        .iter()
        .filter_map(|name| lyra_db::genres::find_by_name(&db, name).transpose())
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(Fixture {
        db,
        user,
        libraries,
        tracks,
        releases,
        artists,
        genres,
    })
}

impl Fixture {
    fn viewers(&self) -> Vec<(String, Viewer)> {
        let mut viewers = vec![("system".to_string(), Viewer::System)];
        let user = |permissions, libraries: &[usize]| {
            Viewer::user(
                &self.db,
                Principal::for_user(
                    &self.db,
                    self.user,
                    permissions,
                    libraries
                        .iter()
                        .map(|index| self.libraries[*index].1.clone())
                        .collect(),
                ),
            )
            .expect("a user")
        };
        viewers.push((
            "admin".to_string(),
            user(vec![lyra_db::Permission::Admin], &[]),
        ));
        for libraries in [&[][..], &[0], &[1, 2]] {
            viewers.push((
                format!("libraries {libraries:?}"),
                user(Vec::new(), libraries),
            ));
        }
        viewers
    }
}

const SEARCHES: &[&str] = &[
    "blue",
    "BEYONCÉ",
    "beyonce",
    "ac dc",
    "acdc",
    "guns n roses",
    "!!!",
    "駆け",
    "ro",
    "ete",
    "dont stop",
    "blue moon",
    "'",
    "ayu",
];

/// Names each query is also checked with, without a search and with one.
const NAMES: &[&str] = &["BLUE", "blue moon", "ac dc", "beyonce", "!!!", "'"];

fn check<C: Old>(
    fixture: &Fixture,
    kind: &str,
    filters: Vec<C::Filter>,
    sorts: &[SortSpec<C::Key>],
    count_keys: &[C::Key],
) -> anyhow::Result<()>
where
    C::Filter: Clone + std::fmt::Debug,
{
    let db = &fixture.db;
    // The catalog does not change while it is queried, so one snapshot serves every query.
    let index = Arc::new(Snapshot::build(db)?);
    let mut matched = 0;
    for (who, viewer) in fixture.viewers() {
        let cx = Context::with(Arc::clone(&index), db, &viewer);
        for filter in &filters {
            let mut reads = Reads::default();
            let unsorted = [
                Vec::new(),
                vec![(C::Key::NAME_ORDER, Direction::Descending)],
            ];
            let queries = sorts
                .iter()
                .map(|sort| (None, None, sort, 7))
                .chain(SEARCHES.iter().flat_map(|search| {
                    unsorted
                        .iter()
                        .map(move |sort| (None, Some(*search), sort, 0))
                }))
                .chain(NAMES.iter().flat_map(|name| {
                    sorts
                        .iter()
                        .map(move |sort| (Some(*name), None, sort, 7))
                        .chain(
                            unsorted
                                .iter()
                                .map(move |sort| (Some(*name), Some("blue"), sort, 0)),
                        )
                }));
            for (name, search, sort, seed) in queries {
                let mut query = Query::<C>::new(filter.clone());
                query.name = name.map(str::to_string);
                query.search = search.map(str::to_string);
                query.sort = sort.clone();
                query.seed = seed;
                if search.is_none() && sort.len() == 1 && sort[0].1 == Direction::Descending {
                    query.names = NameRange::new(Some("b".to_string()), None, None);
                }
                let expected = order(db, &viewer, &query, kind, &mut reads);
                let found = catalog::pipeline::leading_in(&cx, &query, None);
                match (&expected, &found) {
                    (Ok(expected), Ok(found)) => {
                        assert_eq!(
                            found, expected,
                            "{kind} for {who}, {filter:?}, name {name:?}, search {search:?}, sort {sort:?}"
                        );
                        matched += usize::from(!found.is_empty());
                    }
                    (Err(_), Err(_)) => {}
                    _ => panic!(
                        "{kind} for {who}, {filter:?}, name {name:?}, search {search:?}, sort {sort:?}: {:?} \
                         against {:?}",
                        found.as_ref().err(),
                        expected.as_ref().err()
                    ),
                }
                if let Ok(order) = &found {
                    for (offset, limit) in [(0, 3), (2, 5), (4, 0)] {
                        let page = catalog::pipeline::page_in(&cx, &query, offset, Some(limit))?;
                        assert_eq!(page.total, order.len() as u64);
                        let ids = order
                            .iter()
                            .skip(offset as usize)
                            .take(limit as usize)
                            .copied()
                            .collect::<Vec<_>>();
                        assert_eq!(page.items.len(), ids.len());
                    }
                }
            }
            let ids = C::old_candidates(db, &viewer, filter)
                .ok()
                .unwrap_or_default();
            if let (Ok(rows), Ok(numbers)) = (
                C::old_rows(db, &viewer, filter, ids.clone(), count_keys),
                catalog::pipeline::numbers_in::<C>(&cx, filter, ids, count_keys),
            ) {
                for row in rows {
                    let expected = row
                        .values
                        .iter()
                        .map(|value| match value {
                            Some(SortValue::Number(number)) => Some(*number),
                            _ => None,
                        })
                        .collect::<Vec<_>>();
                    assert_eq!(
                        numbers.get(&row.id),
                        Some(&expected),
                        "{kind} counts for {who}, {filter:?}"
                    );
                }
            }
        }
    }
    assert!(matched > 100, "{kind} queries found too little: {matched}");
    Ok(())
}

fn rating(min: u8, max: u8) -> RatingFilter {
    RatingFilter::new(None, RatingValue::new(min), RatingValue::new(max)).expect("a range")
}

/// Every rated, or every unrated, entity.
fn rated(rated: bool) -> RatingFilter {
    RatingFilter::new(Some(rated), None, None).expect("a filter")
}

fn sorts<K: SortKey>(keys: &[K]) -> Vec<SortSpec<K>> {
    let mut sorts = vec![Vec::new()];
    for key in keys {
        for direction in [Direction::Ascending, Direction::Descending] {
            sorts.push(vec![(*key, direction)]);
        }
    }
    sorts
}

const SEEDS: std::ops::Range<u64> = 1..3;

#[test]
fn tracks_match_the_scanning_pipeline() -> anyhow::Result<()> {
    for seed in SEEDS {
        let fixture = fixture(seed)?;
        let credit = |role, excluding| ArtistCredit {
            artists: fixture.artists[..4].to_vec(),
            role,
            excluding,
        };
        let filters = vec![
            TrackFilter::default(),
            TrackFilter {
                library: Some(fixture.libraries[0].0),
                ..Default::default()
            },
            TrackFilter {
                releases: Some(vec![fixture.releases[0]]),
                ..Default::default()
            },
            TrackFilter {
                ids: Some(fixture.tracks[..20].to_vec()),
                releases: Some(fixture.releases[..6].to_vec()),
                exclude_ids: fixture.tracks[..3].to_vec(),
                ..Default::default()
            },
            TrackFilter {
                ids: Some(fixture.tracks[10..40].to_vec()),
                genres: fixture.genres[..2].to_vec(),
                ..Default::default()
            },
            TrackFilter {
                artists: Some(credit(CreditRole::Any, None)),
                years: vec![1990, 1992],
                ..Default::default()
            },
            TrackFilter {
                library: Some(fixture.libraries[0].0),
                year_range: YearRange::new(Some(1991), None)?,
                ..Default::default()
            },
            TrackFilter {
                year_range: YearRange::new(None, Some(1991))?,
                ..Default::default()
            },
            TrackFilter {
                years: vec![1990, 1992],
                year_range: YearRange::new(Some(1991), Some(1992))?,
                ..Default::default()
            },
            TrackFilter {
                artists: Some(credit(CreditRole::Release, Some(CreditRole::Track))),
                exclude_artists: fixture.artists[5..7].to_vec(),
                ..Default::default()
            },
            TrackFilter {
                personal: PersonalFilter {
                    favorite: Some(false),
                    listened: Some(true),
                    ..PersonalFilter::default()
                },
                ..Default::default()
            },
            TrackFilter {
                personal: PersonalFilter {
                    favorite: Some(true),
                    rating: rating(2, 4),
                    ..PersonalFilter::default()
                },
                ..Default::default()
            },
            TrackFilter {
                personal: PersonalFilter {
                    rating: rated(false),
                    ..PersonalFilter::default()
                },
                ..Default::default()
            },
        ];
        use TrackKey::*;
        let mut sort_specs = sorts(&[
            Name,
            SortName,
            DateCreated,
            Year,
            Duration,
            Disc,
            Track,
            ReleaseTitle,
            ArtistName,
            ReleaseArtistName,
            ListenCount,
            LastPlayedAt,
            Rating,
            RatedAt,
            FavoritedAt,
            Random,
            Id,
        ]);
        sort_specs.push(vec![
            (Disc, Direction::Ascending),
            (Track, Direction::Descending),
            (SortName, Direction::Ascending),
        ]);
        sort_specs.push(vec![
            (Year, Direction::Descending),
            (Random, Direction::Ascending),
        ]);
        check::<Tracks>(
            &fixture,
            "Track",
            filters,
            &sort_specs,
            &[
                Duration,
                Year,
                ListenCount,
                LastPlayedAt,
                Rating,
                RatedAt,
                FavoritedAt,
            ],
        )?;
    }
    Ok(())
}

#[test]
fn releases_match_the_scanning_pipeline() -> anyhow::Result<()> {
    for seed in SEEDS {
        let fixture = fixture(seed)?;
        let filters = vec![
            ReleaseFilter::default(),
            ReleaseFilter {
                library: Some(fixture.libraries[1].0),
                genres: fixture.genres[..1].to_vec(),
                ..Default::default()
            },
            ReleaseFilter {
                ids: Some(fixture.releases[..8].to_vec()),
                exclude_ids: fixture.releases[..2].to_vec(),
                ..Default::default()
            },
            ReleaseFilter {
                artists: Some(ArtistCredit {
                    artists: fixture.artists[..5].to_vec(),
                    role: CreditRole::Track,
                    excluding: None,
                }),
                years: vec![1991],
                ..Default::default()
            },
            ReleaseFilter {
                exclude_artists: fixture.artists[..3].to_vec(),
                personal: PersonalFilter {
                    listened: Some(false),
                    ..PersonalFilter::default()
                },
                ..Default::default()
            },
            ReleaseFilter {
                personal: PersonalFilter {
                    favorite: Some(true),
                    listened: Some(true),
                    rating: rating(1, 3),
                },
                ..Default::default()
            },
            ReleaseFilter {
                personal: PersonalFilter {
                    rating: rated(true),
                    ..PersonalFilter::default()
                },
                ..Default::default()
            },
        ];
        use ReleaseKey::*;
        check::<Releases>(
            &fixture,
            "Release",
            filters,
            &sorts(&[
                Name,
                SortName,
                DateCreated,
                ReleaseDate,
                Year,
                TotalDuration,
                TrackCount,
                ArtistName,
                ListenCount,
                LastPlayedAt,
                Rating,
                RatedAt,
                FavoritedAt,
                Random,
                Id,
            ]),
            &[
                TrackCount,
                TotalDuration,
                ListenCount,
                LastPlayedAt,
                Rating,
                RatedAt,
                FavoritedAt,
            ],
        )?;
    }
    Ok(())
}

#[test]
fn artists_match_the_scanning_pipeline() -> anyhow::Result<()> {
    for seed in SEEDS {
        let fixture = fixture(seed)?;
        let filters = vec![
            ArtistFilter::default(),
            ArtistFilter {
                ids: Some(fixture.artists[..8].to_vec()),
                exclude_ids: fixture.artists[..1].to_vec(),
                ..Default::default()
            },
            ArtistFilter {
                library: Some(fixture.libraries[2].0),
                credit_role: Some(CreditRole::Release),
                ..Default::default()
            },
            ArtistFilter {
                releases: Some(fixture.releases[..5].to_vec()),
                exclude_credit_role: Some(CreditRole::Track),
                ..Default::default()
            },
            ArtistFilter {
                tracks: Some(fixture.tracks[..30].to_vec()),
                credit_types: Some(vec![CreditType::Artist, CreditType::Composer]),
                ..Default::default()
            },
            ArtistFilter {
                genres: fixture.genres[..2].to_vec(),
                exclude_credit_types: vec![CreditType::Producer],
                artist_type: Some(ArtistType::Group),
                ..Default::default()
            },
            ArtistFilter {
                ids: Some(fixture.artists.clone()),
                artist_type: Some(ArtistType::Person),
                ..Default::default()
            },
            ArtistFilter {
                personal: PersonalFilter {
                    favorite: Some(true),
                    ..PersonalFilter::default()
                },
                ..Default::default()
            },
            ArtistFilter {
                personal: PersonalFilter {
                    favorite: Some(false),
                    listened: Some(true),
                    rating: rating(3, 5),
                },
                ..Default::default()
            },
            ArtistFilter {
                personal: PersonalFilter {
                    favorite: Some(false),
                    rating: rated(false),
                    ..PersonalFilter::default()
                },
                ..Default::default()
            },
        ];
        use ArtistKey::*;
        check::<Artists>(
            &fixture,
            "Artist",
            filters,
            &sorts(&[
                Name,
                SortName,
                DateCreated,
                ReleaseCount,
                TrackCount,
                TotalDuration,
                ListenCount,
                LastPlayedAt,
                Rating,
                RatedAt,
                FavoritedAt,
                Random,
                Id,
            ]),
            &[
                ReleaseCount,
                TrackCount,
                TotalDuration,
                ListenCount,
                LastPlayedAt,
                Rating,
                RatedAt,
                FavoritedAt,
            ],
        )?;
    }
    Ok(())
}

#[test]
fn genres_match_the_scanning_pipeline() -> anyhow::Result<()> {
    for seed in SEEDS {
        let fixture = fixture(seed)?;
        let filters = vec![
            GenreFilter::default(),
            GenreFilter {
                library: Some(fixture.libraries[0].0),
                ..Default::default()
            },
            GenreFilter {
                library: Some(fixture.libraries[1].0),
                ..Default::default()
            },
            GenreFilter {
                ids: Some(fixture.genres[..3].to_vec()),
                releases: Some(fixture.releases[..7].to_vec()),
                exclude_ids: fixture.genres[..1].to_vec(),
                ..Default::default()
            },
        ];
        use GenreKey::*;
        check::<Genres>(
            &fixture,
            "Genre",
            filters,
            &sorts(&[
                Name,
                ReleaseCount,
                TrackCount,
                TotalDuration,
                ListenCount,
                LastPlayedAt,
                Random,
                Id,
            ]),
            &[
                ReleaseCount,
                TrackCount,
                TotalDuration,
                ListenCount,
                LastPlayedAt,
            ],
        )?;
    }
    Ok(())
}
