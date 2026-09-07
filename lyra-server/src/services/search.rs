// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

#[cfg(test)]
use std::collections::HashSet;

use agdb::DbAny;

use crate::db::{
    self,
    ratings::RatingFilter,
};

const DEFAULT_LIMIT: u64 = 20;
const MAX_LIMIT: u64 = 50;

#[derive(Clone, Debug)]
pub(crate) struct SearchOptions {
    pub(crate) query: String,
    pub(crate) limit: u64,
    pub(crate) rating_filter: RatingFilter,
}

impl SearchOptions {
    pub(crate) fn new(query: String, limit: Option<u64>, rating_filter: RatingFilter) -> Self {
        let limit = limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
        Self {
            query,
            limit,
            rating_filter,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct TitleHit {
    pub(crate) id: String,
    pub(crate) title: String,
}

#[derive(Clone, Debug)]
pub(crate) struct ArtistHit {
    pub(crate) id: String,
    pub(crate) name: String,
}

#[derive(Clone, Debug)]
pub(crate) struct SearchResults {
    pub(crate) tracks: Vec<TitleHit>,
    pub(crate) artists: Vec<ArtistHit>,
    pub(crate) releases: Vec<TitleHit>,
}

pub(crate) fn search_accessible(
    db: &DbAny,
    principal: &crate::services::auth::Principal,
    options: &SearchOptions,
) -> anyhow::Result<SearchResults> {
    let library_ids = (!principal.permissions.contains(&db::Permission::Admin))
        .then_some(&principal.accessible_library_ids);
    let mut candidates = db::search::collect_candidates(db, library_ids)?;
    if !options.rating_filter.is_empty() {
        let matching =
            db::ratings::target_ids_matching(db, principal.user_db_id, options.rating_filter)?;
        candidates
            .tracks
            .retain(|candidate| matching.contains(&candidate.db_id));
        candidates
            .artists
            .retain(|candidate| matching.contains(&candidate.db_id));
        candidates
            .releases
            .retain(|candidate| matching.contains(&candidate.db_id));
    }
    let limit = options.limit as usize;
    let tracks = db::search::rank(&candidates.tracks, &options.query, limit)
        .into_iter()
        .map(|candidate| TitleHit {
            id: candidate.id,
            title: candidate.text,
        })
        .collect();
    let artists = db::search::rank(&candidates.artists, &options.query, limit)
        .into_iter()
        .map(|candidate| ArtistHit {
            id: candidate.id,
            name: candidate.text,
        })
        .collect();
    let releases = db::search::rank(&candidates.releases, &options.query, limit)
        .into_iter()
        .map(|candidate| TitleHit {
            id: candidate.id,
            title: candidate.text,
        })
        .collect();

    Ok(SearchResults {
        tracks,
        artists,
        releases,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::test_db::{
        insert_artist,
        insert_release,
        insert_track,
        new_test_db,
    };
    use crate::services::auth::Principal;
    use agdb::QueryBuilder;

    fn admin_principal(db: &mut DbAny) -> anyhow::Result<Principal> {
        let user_db_id = db
            .exec_mut(QueryBuilder::insert().nodes().count(1).query())?
            .ids()[0];
        Ok(Principal {
            user_db_id,
            user_public_id: "test-search-user".to_string(),
            username: "search-user".to_string(),
            permissions: vec![db::Permission::Admin],
            role_name: None,
            accessible_library_ids: HashSet::new(),
        })
    }

    #[test]
    fn search_ranks_best_matches_before_applying_limit() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let principal = admin_principal(&mut db)?;
        for title in ["Blue", "A b l u e", "Blue horizon", "B l u e"] {
            insert_track(&mut db, title)?;
            insert_artist(&mut db, title)?;
            insert_release(&mut db, title)?;
        }
        let options = SearchOptions::new("blue".to_string(), Some(1), RatingFilter::default());
        let results = search_accessible(&db, &principal, &options)?;
        assert_eq!(results.tracks[0].title, "Blue");
        assert_eq!(results.artists[0].name, "Blue");
        assert_eq!(results.releases[0].title, "Blue");
        Ok(())
    }

    #[test]
    fn search_scopes_and_deduplicates_shared_entities() -> anyhow::Result<()> {
        use crate::db::test_db::{
            connect,
            connect_artist,
            insert_library,
        };
        let mut db = new_test_db()?;
        let mut principal = admin_principal(&mut db)?;
        principal.permissions.clear();
        let first = insert_library(&mut db, "First", "/search-test/first")?;
        let second = insert_library(&mut db, "Second", "/search-test/second")?;
        let hidden = insert_library(&mut db, "Hidden", "/search-test/hidden")?;
        for library in [first, second] {
            principal
                .accessible_library_ids
                .insert(db::libraries::get_by_id(&db, library)?.unwrap().id);
        }
        principal
            .accessible_library_ids
            .insert("deleted-library".to_string());
        let artist = insert_artist(&mut db, "Blue Artist")?;
        let track = insert_track(&mut db, "Blue Track")?;
        let release = insert_release(&mut db, "Blue Release")?;
        for library in [first, second] {
            connect(&mut db, library, release)?;
        }
        connect(&mut db, release, track)?;
        connect_artist(&mut db, release, artist)?;
        connect_artist(&mut db, release, artist)?;
        let other_release = insert_release(&mut db, "Blue Other Release")?;
        connect(&mut db, second, other_release)?;
        connect(&mut db, other_release, track)?;
        connect_artist(&mut db, other_release, artist)?;
        let hidden_track = insert_track(&mut db, "Blue")?;
        let hidden_artist = insert_artist(&mut db, "Blue")?;
        let hidden_release = insert_release(&mut db, "Blue")?;
        connect(&mut db, hidden, hidden_release)?;
        connect(&mut db, hidden_release, hidden_track)?;
        connect_artist(&mut db, hidden_release, hidden_artist)?;
        let options = SearchOptions::new("blue".to_string(), Some(50), RatingFilter::default());
        let results = search_accessible(&db, &principal, &options)?;
        assert_eq!(results.tracks.len(), 1);
        assert_eq!(results.tracks[0].title, "Blue Track");
        assert_eq!(results.artists.len(), 1);
        assert_eq!(results.artists[0].name, "Blue Artist");
        assert_eq!(results.releases.len(), 2);
        assert!(
            results
                .releases
                .iter()
                .all(|release| release.title != "Blue")
        );
        principal.accessible_library_ids.clear();
        let results = search_accessible(&db, &principal, &options)?;
        assert!(
            results.tracks.is_empty() && results.artists.is_empty() && results.releases.is_empty()
        );
        Ok(())
    }

    #[test]
    fn search_returns_per_entity_hits_for_matching_query() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        insert_track(&mut db, "Blue in Green")?;
        insert_track(&mut db, "Red Clay")?;
        insert_artist(&mut db, "Bluegrass Trio")?;
        insert_artist(&mut db, "Mingus")?;
        insert_release(&mut db, "Kind of Blue")?;
        insert_release(&mut db, "Bitches Brew")?;

        let principal = admin_principal(&mut db)?;
        let options = SearchOptions::new("blue".to_string(), None, RatingFilter::default());
        let results = search_accessible(&db, &principal, &options)?;

        assert!(
            results
                .tracks
                .iter()
                .any(|hit| hit.title == "Blue in Green"),
            "expected blue-matching track"
        );
        assert!(
            results
                .artists
                .iter()
                .any(|hit| hit.name == "Bluegrass Trio"),
            "expected blue-matching artist"
        );
        assert!(
            results
                .releases
                .iter()
                .any(|hit| hit.title == "Kind of Blue"),
            "expected blue-matching release"
        );
        Ok(())
    }

    #[test]
    fn search_clamps_limit_within_bounds() {
        let zero = SearchOptions::new("q".to_string(), Some(0), RatingFilter::default());
        assert_eq!(zero.limit, 1);
        let huge = SearchOptions::new("q".to_string(), Some(9_999), RatingFilter::default());
        assert_eq!(huge.limit, MAX_LIMIT);
        let none = SearchOptions::new("q".to_string(), None, RatingFilter::default());
        assert_eq!(none.limit, DEFAULT_LIMIT);
    }

    #[test]
    fn search_respects_limit_per_entity() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        for i in 0..10 {
            insert_track(&mut db, &format!("Blue Track {i}"))?;
            insert_artist(&mut db, &format!("Blue Artist {i}"))?;
            insert_release(&mut db, &format!("Blue Release {i}"))?;
        }

        let principal = admin_principal(&mut db)?;
        let options = SearchOptions::new("blue".to_string(), Some(3), RatingFilter::default());
        let results = search_accessible(&db, &principal, &options)?;

        assert!(results.tracks.len() <= 3);
        assert!(results.artists.len() <= 3);
        assert!(results.releases.len() <= 3);
        Ok(())
    }

    #[test]
    fn search_filters_personal_rating_range_before_limit() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let principal = admin_principal(&mut db)?;
        insert_track(&mut db, "Blue")?;
        let rated_track = insert_track(&mut db, "A Distant Blue Track")?;
        let rated_artist = insert_artist(&mut db, "Blue Artist")?;
        let low_release = insert_release(&mut db, "Blue Low Release")?;
        let high_release = insert_release(&mut db, "Blue High Release")?;

        for (target, kind, value) in [
            (rated_track, db::ratings::RatingKind::Track, 4),
            (rated_artist, db::ratings::RatingKind::Artist, 3),
            (low_release, db::ratings::RatingKind::Release, 2),
            (high_release, db::ratings::RatingKind::Release, 5),
        ] {
            db::ratings::upsert(
                &mut db,
                principal.user_db_id,
                target,
                kind,
                db::ratings::RatingValue::new(value).unwrap(),
                100,
            )?;
        }

        let filter = RatingFilter::new(
            db::ratings::RatingValue::new(3),
            db::ratings::RatingValue::new(4),
        )
        .unwrap();
        let options = SearchOptions::new("blue".to_string(), Some(1), filter);
        let results = search_accessible(&db, &principal, &options)?;

        assert_eq!(results.tracks.len(), 1);
        assert_eq!(results.tracks[0].title, "A Distant Blue Track");
        assert_eq!(results.artists.len(), 1);
        assert_eq!(results.artists[0].name, "Blue Artist");
        assert!(results.releases.is_empty());
        Ok(())
    }
}

#[cfg(all(test, feature = "nightly"))]
mod benches {
    extern crate test;

    use std::hint::black_box;

    use test::Bencher;

    use super::*;
    use crate::{
        db::test_db::{
            connect,
            connect_artist,
            insert_artist,
            insert_library,
            insert_release,
            insert_track,
            new_test_db,
        },
        services::auth::Principal,
    };

    fn fixture(track_count: usize, library_count: usize, admin: bool) -> (DbAny, Principal) {
        let mut db = new_test_db().unwrap();
        let user_db_id = db
            .exec_mut(agdb::QueryBuilder::insert().nodes().count(1).query())
            .unwrap()
            .ids()[0];
        let mut principal = Principal {
            user_db_id,
            user_public_id: "search-bench-user".to_string(),
            username: "search-bench-user".to_string(),
            permissions: if admin {
                vec![db::Permission::Admin]
            } else {
                Vec::new()
            },
            role_name: None,
            accessible_library_ids: HashSet::new(),
        };
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
                    .insert(db::libraries::get_by_id(&db, id).unwrap().unwrap().id);
                id
            })
            .collect();

        // Ten tracks per release, five releases per artist; 5% share a selective phrase.
        for artist_idx in 0..track_count / 50 {
            let phrase = if artist_idx % 20 == 0 {
                "Velvet Horizon"
            } else {
                "Silver Moon"
            };
            let artist =
                insert_artist(&mut db, &format!("{phrase} Artist {artist_idx:05}")).unwrap();
            for release_idx in 0..5 {
                let release_number = artist_idx * 5 + release_idx;
                let release =
                    insert_release(&mut db, &format!("{phrase} Release {release_number:05}"))
                        .unwrap();
                connect(&mut db, libraries[release_number % library_count], release).unwrap();
                connect_artist(&mut db, release, artist).unwrap();
                for track_idx in 0..10 {
                    let track_number = release_number * 10 + track_idx;
                    let track = insert_track(&mut db, &format!("{phrase} Track {track_number:06}"))
                        .unwrap();
                    connect(&mut db, release, track).unwrap();
                }
            }
        }

        // Unreachable from the user's libraries, but visible through the admin roots.
        let hidden_library = insert_library(&mut db, "Hidden", "/search-bench/hidden").unwrap();
        let hidden_artist = insert_artist(&mut db, "Quarantined Artist").unwrap();
        let hidden_release = insert_release(&mut db, "Quarantined Release").unwrap();
        let hidden_track = insert_track(&mut db, "Quarantined Track").unwrap();
        connect(&mut db, hidden_library, hidden_release).unwrap();
        connect_artist(&mut db, hidden_release, hidden_artist).unwrap();
        connect(&mut db, hidden_release, hidden_track).unwrap();
        let hidden = search_accessible(
            &db,
            &principal,
            &SearchOptions::new("quarantined".to_string(), Some(5), RatingFilter::default()),
        )
        .unwrap();
        assert_eq!(
            (
                hidden.tracks.len(),
                hidden.artists.len(),
                hidden.releases.len()
            ),
            if admin { (1, 1, 1) } else { (0, 0, 0) }
        );
        (db, principal)
    }

    fn run(
        b: &mut Bencher,
        track_count: usize,
        library_count: usize,
        admin: bool,
        query: &str,
        limit: u64,
    ) {
        let (db, principal) = fixture(track_count, library_count, admin);
        let options = SearchOptions::new(query.to_string(), Some(limit), RatingFilter::default());
        let results = search_accessible(&db, &principal, &options).unwrap();
        let expected = match query {
            "a" => (limit as usize, limit as usize, limit as usize),
            "vlthrz" => (
                limit as usize,
                (track_count / 1_000).min(limit as usize),
                (track_count / 200).min(limit as usize),
            ),
            "zzzzzz" => (0, 0, 0),
            _ => unreachable!(),
        };
        assert_eq!(
            (
                results.tracks.len(),
                results.artists.len(),
                results.releases.len()
            ),
            expected
        );
        b.iter(|| {
            black_box(
                search_accessible(black_box(&db), black_box(&principal), black_box(&options))
                    .unwrap(),
            )
        });
    }

    fn collect(b: &mut Bencher, size: usize, libraries: usize, admin: bool) {
        let (db, principal) = fixture(size, libraries, admin);
        let library_ids = (!admin).then_some(&principal.accessible_library_ids);
        b.iter(|| black_box(db::search::collect_candidates(black_box(&db), library_ids).unwrap()));
    }

    fn ranking(b: &mut Bencher, query: &str) {
        let (db, _) = fixture(100_000, 1, true);
        let candidates = db::search::collect_candidates(&db, None).unwrap();
        b.iter(|| {
            black_box(db::search::rank(
                black_box(&candidates.tracks),
                black_box(query),
                5,
            ));
            black_box(db::search::rank(
                black_box(&candidates.artists),
                black_box(query),
                5,
            ));
            black_box(db::search::rank(
                black_box(&candidates.releases),
                black_box(query),
                5,
            ));
        });
    }

    #[bench]
    fn ranking_100k_broad(b: &mut Bencher) {
        ranking(b, "a");
    }
    #[bench]
    fn ranking_100k_selective(b: &mut Bencher) {
        ranking(b, "vlthrz");
    }
    #[bench]
    fn ranking_100k_no_match(b: &mut Bencher) {
        ranking(b, "zzzzzz");
    }

    macro_rules! queries {
        ($size:expr, $libraries:expr, $admin:expr) => {
            #[bench]
            fn collection(b: &mut Bencher) {
                collect(b, $size, $libraries, $admin);
            }
            #[bench]
            fn broad_limit_5(b: &mut Bencher) {
                run(b, $size, $libraries, $admin, "a", 5);
            }
            #[bench]
            fn broad_limit_20(b: &mut Bencher) {
                run(b, $size, $libraries, $admin, "a", 20);
            }
            #[bench]
            fn selective_limit_5(b: &mut Bencher) {
                run(b, $size, $libraries, $admin, "vlthrz", 5);
            }
            #[bench]
            fn selective_limit_20(b: &mut Bencher) {
                run(b, $size, $libraries, $admin, "vlthrz", 20);
            }
            #[bench]
            fn no_match_limit_5(b: &mut Bencher) {
                run(b, $size, $libraries, $admin, "zzzzzz", 5);
            }
            #[bench]
            fn no_match_limit_20(b: &mut Bencher) {
                run(b, $size, $libraries, $admin, "zzzzzz", 20);
            }
        };
    }

    macro_rules! access_cases {
        ($name:ident, $size:expr) => {
            mod $name {
                use super::*;
                mod admin {
                    use super::*;
                    queries!($size, 1, true);
                }
                mod user_one_library {
                    use super::*;
                    queries!($size, 1, false);
                }
                mod user_four_libraries {
                    use super::*;
                    queries!($size, 4, false);
                }
            }
        };
    }

    access_cases!(tracks_1k, 1_000);
    access_cases!(tracks_10k, 10_000);
    access_cases!(tracks_100k, 100_000);
}
