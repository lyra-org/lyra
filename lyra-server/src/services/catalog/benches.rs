// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Catalog query benches over a generated library, and a report of the costs that matter at
//! scale. Run the benches with
//! `cargo +nightly bench -p lyra-server --features nightly --lib -- catalog::benches` and the
//! report with
//! `cargo +nightly test --release -p lyra-server --features nightly --lib -- catalog::benches::report --ignored --nocapture`.

extern crate test;

use std::{
    collections::HashSet,
    hint::black_box,
    sync::Arc,
    time::{
        Duration,
        Instant,
    },
};

use agdb::{
    DbAny,
    DbId,
    QueryBuilder,
};
use nanoid::nanoid;
use test::Bencher;

use super::{
    Viewer,
    artists::Artists,
    index::Snapshot,
    pipeline::{
        Catalog,
        Context,
        Query,
        page_in,
    },
    releases::Releases,
    tracks::Tracks,
};
use crate::services::auth::Principal;
use lyra_db::roots::{
    self,
    Root,
};
use lyra_db::{
    Track,
    fixtures::{
        connect,
        connect_credit,
        insert_artist,
        insert_release,
    },
    test_db::{
        connect_artist,
        insert_library,
        new_test_db,
    },
};

const FIRST: &[&str] = &[
    "Silver",
    "Crimson",
    "Électrique",
    "Blåbær",
    "Northern",
    "Sólstafir",
    "Ñandú",
    "Hollow",
    "東京",
    "Neon",
    "Paper",
    "Glass",
    "Wild",
    "Café",
    "Midnight",
    "Static",
    "夜の",
    "Golden",
    "Quiet",
    "Ørsted",
];
const SECOND: &[&str] = &[
    "Moon",
    "River",
    "Garden",
    "Signal",
    "Mirror",
    "Ballad",
    "Ritual",
    "Échos",
    "Harbor",
    "季節",
    "Fever",
    "Lanterns",
    "Coast",
    "Machine",
    "Saga",
    "Weather",
    "Ghost",
    "Bloom",
    "Kaleidoscope",
    "Atlas",
];

/// A name from the word lists, unique by `index`; one in twenty carries the selective phrase
/// "Velvet Horizon".
fn name(index: usize, kind: &str) -> String {
    if index % 20 == 0 {
        return format!("Velvet Horizon {kind} {index:06}");
    }
    format!(
        "{} {} {kind} {index:06}",
        FIRST[index % FIRST.len()],
        SECOND[(index / FIRST.len()) % SECOND.len()],
    )
}

fn insert_track(db: &mut DbAny, index: usize, number: u32) -> anyhow::Result<DbId> {
    let track = Track {
        db_id: None,
        id: nanoid!(),
        track_title: name(index, "Track"),
        sort_title: (index % 10 == 3).then(|| name(index + 1, "Sort")),
        year: Some(1960 + (index % 60) as u32),
        disc: Some(1),
        disc_total: Some(1),
        track: Some(number),
        track_total: Some(10),
        duration_ms: Some(120_000 + (index as u64 * 7_919) % 240_000),
        track_gain_db: Some(-7.5),
        album_gain_db: Some(-8.0),
        locked: Some(false),
        created_at_ms: Some(1_700_000_000_000 + index as u64),
        file_created_at_ms: Some(1_700_000_000 + (index as u64 * 31) % 86_400),
    };
    let track_id = db
        .exec_mut(QueryBuilder::insert().element(&track).query())?
        .ids()[0];
    roots::link(db, Root::Tracks, track_id)?;
    Ok(track_id)
}

/// A library of `track_count` tracks, ten per release and five releases per artist, with
/// releases spread across `library_count` libraries the principal is granted, and one more
/// release, artist and track named "Quarantined" in a library only an admin sees. Tracks carry
/// a dozen stored values; one in ten items has a sort title, and names mix in non-ASCII words.
pub(crate) fn search_fixture(
    track_count: usize,
    library_count: usize,
    admin: bool,
) -> (DbAny, Principal) {
    let mut db = new_test_db().unwrap();
    let user_db_id = lyra_db::test_db::insert_user(&mut db, "search-bench-user").unwrap();
    let mut principal = Principal::for_user(
        &db,
        user_db_id,
        if admin {
            vec![lyra_db::Permission::Admin]
        } else {
            Vec::new()
        },
        HashSet::new(),
    );
    let libraries: Vec<_> = (0..library_count)
        .map(|i| {
            let id = insert_library(
                &mut db,
                &format!("Library {i}"),
                &format!("/search-bench/{i}"),
            )
            .unwrap();
            principal
                .accessible_library_ids
                .insert(lyra_db::libraries::get_by_id(&db, id).unwrap().unwrap().id);
            id
        })
        .collect();

    for artist_index in 0..track_count / 50 {
        let artist = insert_artist(&mut db, &name(artist_index, "Artist")).unwrap();
        for release_offset in 0..5 {
            let release_index = artist_index * 5 + release_offset;
            let release = insert_release(&mut db, &name(release_index, "Release")).unwrap();
            if release_index % 10 == 7 {
                let mut stored = lyra_db::releases::get_by_id(&db, release).unwrap().unwrap();
                stored.sort_title = Some(name(release_index + 1, "Sort"));
                stored.release_date = Some(format!("{}-05-01", 1960 + release_index % 60));
                lyra_db::releases::update(&mut db, &stored).unwrap();
            }
            connect(&mut db, libraries[release_index % library_count], release).unwrap();
            connect_artist(&mut db, release, artist).unwrap();
            for track_offset in 0..10 {
                let track_index = release_index * 10 + track_offset;
                let track = insert_track(&mut db, track_index, track_offset as u32 + 1).unwrap();
                connect(&mut db, release, track).unwrap();
                connect_credit(&mut db, track, artist, lyra_db::CreditType::Artist, None, 0)
                    .unwrap();
            }
        }
    }

    let hidden_library = insert_library(&mut db, "Hidden", "/search-bench/hidden").unwrap();
    let hidden_artist = insert_artist(&mut db, "Quarantined Artist").unwrap();
    let hidden_release = insert_release(&mut db, "Quarantined Release").unwrap();
    let hidden_track = insert_track(&mut db, usize::MAX / 2, 1).unwrap();
    let mut track = lyra_db::tracks::get_by_id(&db, hidden_track)
        .unwrap()
        .unwrap();
    track.track_title = "Quarantined Track".to_string();
    lyra_db::tracks::update(&mut db, &track).unwrap();
    connect(&mut db, hidden_library, hidden_release).unwrap();
    connect_artist(&mut db, hidden_release, hidden_artist).unwrap();
    connect(&mut db, hidden_release, hidden_track).unwrap();
    (db, principal)
}

struct Bench {
    db: DbAny,
    viewer: Viewer,
    index: Arc<Snapshot>,
}

impl Bench {
    fn new(track_count: usize, admin: bool) -> Self {
        let (db, principal) = search_fixture(track_count, 1, admin);
        let viewer = Viewer::user(&db, principal).unwrap();
        let index = Arc::new(Snapshot::build(&db).unwrap());
        Self { db, viewer, index }
    }

    fn page<C: Catalog>(&self, search: Option<&str>, offset: u64, limit: u64) -> u64
    where
        C::Filter: Default,
    {
        let cx = Context::with(Arc::clone(&self.index), &self.db, &self.viewer);
        let mut query = Query::<C>::new(C::Filter::default());
        query.search = search.map(str::to_string);
        let page = page_in(&cx, &query, offset, Some(limit)).unwrap();
        page.total + page.items.len() as u64
    }

    /// The first page of tracks, artists and releases each named as one item near `index` is, as
    /// a plugin looking an item up by name reads them. The names sort last, so a walk in name
    /// order finds them only at its end.
    fn named(&self, index: usize) -> u64 {
        let last = |index: usize| index - index % FIRST.len() + 8;
        fn first<C: Catalog>(bench: &Bench, name: String) -> u64
        where
            C::Filter: Default,
        {
            let cx = Context::with(Arc::clone(&bench.index), &bench.db, &bench.viewer);
            let mut query = Query::<C>::new(C::Filter::default());
            query.name = Some(name);
            let page = page_in(&cx, &query, 0, Some(20)).unwrap();
            page.total + page.items.len() as u64
        }
        first::<Tracks>(self, name(last(index), "Track"))
            + first::<Artists>(self, name(last(index / 50), "Artist"))
            + first::<Releases>(self, name(last(index / 10), "Release"))
    }

    /// A page of tracks sorted by their releases' titles, which a viewer limited to some
    /// libraries reads through its own scope.
    fn by_release_title(&self, offset: u64) -> u64 {
        let cx = Context::with(Arc::clone(&self.index), &self.db, &self.viewer);
        let mut query = Query::<Tracks>::new(Default::default());
        query.sort = vec![(
            super::tracks::TrackKey::ReleaseTitle,
            super::Direction::Ascending,
        )];
        page_in(&cx, &query, offset, Some(500)).unwrap().items.len() as u64
    }

    /// Searches tracks, artists and releases, as the search endpoint does.
    fn search(&self, term: &str) -> u64 {
        self.page::<Tracks>(Some(term), 0, 20)
            + self.page::<Artists>(Some(term), 0, 20)
            + self.page::<Releases>(Some(term), 0, 20)
    }

    /// Every track, 500 at a time.
    fn sync(&self, track_count: usize) -> u64 {
        let mut seen = 0;
        for offset in (0..track_count as u64 + 1).step_by(500) {
            let cx = Context::with(Arc::clone(&self.index), &self.db, &self.viewer);
            seen += page_in(
                &cx,
                &Query::<Tracks>::new(Default::default()),
                offset,
                Some(500),
            )
            .unwrap()
            .items
            .len() as u64;
        }
        seen
    }
}

fn checked(track_count: usize, admin: bool) -> Bench {
    let bench = Bench::new(track_count, admin);
    let hidden = u64::from(admin);
    let total = |search| {
        let cx = Context::with(Arc::clone(&bench.index), &bench.db, &bench.viewer);
        let mut query = Query::<Tracks>::new(Default::default());
        query.search = Some(search);
        page_in(&cx, &query, 0, Some(1)).unwrap().total
    };
    assert_eq!(total("quarantined".to_string()), hidden);
    assert!(total("velvet horizon".to_string()) >= track_count as u64 / 20);
    bench
}

macro_rules! access_cases {
    ($name:ident, $size:expr) => {
        mod $name {
            use super::*;

            #[bench]
            fn search_admin(b: &mut Bencher) {
                let bench = checked($size, true);
                b.iter(|| black_box(bench.search(black_box("velvet horizon"))));
            }

            #[bench]
            fn search_user(b: &mut Bencher) {
                let bench = checked($size, false);
                b.iter(|| black_box(bench.search(black_box("velvet horizon"))));
            }

            #[bench]
            fn search_user_broad(b: &mut Bencher) {
                let bench = checked($size, false);
                b.iter(|| black_box(bench.search(black_box("s"))));
            }

            #[bench]
            fn named_user(b: &mut Bencher) {
                let bench = checked($size, false);
                b.iter(|| black_box(bench.named(black_box($size / 2))));
            }

            #[bench]
            fn deep_page_user(b: &mut Bencher) {
                let bench = checked($size, false);
                b.iter(|| black_box(bench.page::<Tracks>(None, $size / 2, 500)));
            }

            #[bench]
            fn rebuild(b: &mut Bencher) {
                let bench = checked($size, true);
                b.iter(|| black_box(Snapshot::build(&bench.db).unwrap().tracks.len()));
            }
        }
    };
}

access_cases!(tracks_1k, 1_000);
access_cases!(tracks_10k, 10_000);

/// The median of `runs` timings of `work`.
fn median(runs: usize, mut work: impl FnMut() -> u64) -> Duration {
    black_box(work());
    let mut times = (0..runs)
        .map(|_| {
            let started = Instant::now();
            black_box(work());
            started.elapsed()
        })
        .collect::<Vec<_>>();
    times.sort();
    times[runs / 2]
}

fn ms(duration: Duration) -> String {
    format!("{:.2} ms", duration.as_secs_f64() * 1e3)
}

#[test]
#[ignore = "a report for release builds"]
fn report() {
    let track_count = std::env::var("CATALOG_REPORT_TRACKS")
        .ok()
        .and_then(|tracks| tracks.parse().ok())
        .unwrap_or(100_000);
    for admin in [true, false] {
        let mut bench = checked(track_count, admin);
        let who = if admin { "admin" } else { "one-library user" };
        println!("{track_count} tracks, {who}");
        println!(
            "  search \"velvet horizon\" (tracks, artists, releases): {}",
            ms(median(9, || bench.search("velvet horizon")))
        );
        println!(
            "  search \"s\" (tracks, artists, releases): {}",
            ms(median(9, || bench.search("s")))
        );
        assert_eq!(bench.named(track_count / 2), 6);
        println!(
            "  name lookup (tracks, artists, releases), first page of 20: {}",
            ms(median(51, || bench.named(track_count / 2)))
        );
        println!(
            "  track page of 500 by release title at offset {}: {}",
            track_count / 2,
            ms(median(9, || bench.by_release_title(track_count as u64 / 2)))
        );
        println!(
            "  track page of 500 at offset {}: {}",
            track_count / 2,
            ms(median(9, || bench.page::<Tracks>(
                None,
                track_count as u64 / 2,
                500
            )))
        );
        println!(
            "  full sync, 500 tracks a page: {}",
            ms(median(3, || bench.sync(track_count)))
        );
        if admin {
            println!(
                "  index rebuild: {}",
                ms(median(
                    5,
                    || Snapshot::build(&bench.db).unwrap().tracks.len() as u64
                ))
            );
        }
        if !admin {
            // A fresh snapshot has no scope for the viewer yet; the first query builds it and
            // the release title column and order it reads.
            let mut times = (0..5)
                .map(|_| {
                    bench.index = Arc::new(Snapshot::build(&bench.db).unwrap());
                    let started = Instant::now();
                    black_box(bench.search("s") + bench.by_release_title(track_count as u64 / 2));
                    started.elapsed()
                })
                .collect::<Vec<_>>();
            times.sort();
            println!(
                "  first search \"s\" and release title page on a new snapshot: {} (warm {})",
                ms(times[2]),
                ms(median(9, || bench.search("s")
                    + bench.by_release_title(track_count as u64 / 2)))
            );
        }
        println!(
            "  index memory after the queries above: {:.1} MB",
            bench.index.memory() as f64 / 1e6
        );
    }
}

/// Viewers each limited to a different set of `libraries`, cycling through `sets` of them; with
/// more sets than a snapshot keeps scopes for, every query rebuilds its scope.
#[test]
#[ignore = "a report for release builds"]
fn scope_cycling() {
    let track_count = std::env::var("CATALOG_REPORT_TRACKS")
        .ok()
        .and_then(|tracks| tracks.parse().ok())
        .unwrap_or(100_000);
    let (db, principal) = search_fixture(track_count, 8, false);
    let user = principal.require(&db).unwrap();
    let libraries = principal
        .accessible_library_ids
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    let viewers = (1..1u32 << libraries.len())
        .filter(|bits| bits.count_ones() >= 4)
        .map(|bits| {
            let set = libraries
                .iter()
                .enumerate()
                .filter(|(place, _)| bits & (1 << place) != 0)
                .map(|(_, id)| id.clone())
                .collect();
            Viewer::user(&db, Principal::for_user(&db, user, Vec::new(), set)).unwrap()
        })
        .collect::<Vec<_>>();
    let index = Arc::new(Snapshot::build(&db).unwrap());
    let release_title = |viewer: &Viewer| {
        let cx = Context::with(Arc::clone(&index), &db, viewer);
        let mut query = Query::<Tracks>::new(Default::default());
        query.sort = vec![(
            super::tracks::TrackKey::ReleaseTitle,
            super::Direction::Ascending,
        )];
        page_in(&cx, &query, 0, Some(500)).unwrap().items.len() as u64
    };
    let searched = |viewer: &Viewer| {
        let cx = Context::with(Arc::clone(&index), &db, viewer);
        let mut query = Query::<Tracks>::new(Default::default());
        query.search = Some("velvet horizon".to_string());
        page_in(&cx, &query, 0, Some(20)).unwrap().total
    };
    println!("{track_count} tracks, 8 libraries");
    for sets in [32, 40] {
        let cycle = &viewers[..sets];
        for (label, query) in [
            (
                "release title page",
                &release_title as &dyn Fn(&Viewer) -> u64,
            ),
            ("search", &searched as &dyn Fn(&Viewer) -> u64),
        ] {
            let round = || cycle.iter().map(|viewer| query(viewer)).sum::<u64>();
            let time = median(3, round);
            println!(
                "  {sets} sets round robin, {label}: {} a cycle, {} a query",
                ms(time),
                ms(time / sets as u32)
            );
        }
    }
    println!(
        "  index memory with the scopes kept: {:.1} MB",
        index.memory() as f64 / 1e6
    );
}
