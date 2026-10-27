// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::cell::RefCell;
#[cfg(test)]
use std::collections::HashSet;

use agdb::DbAny;

use nucleo_matcher::{
    Config,
    Matcher,
    Utf32Str,
    pattern::{
        CaseMatching,
        Normalization,
        Pattern,
    },
};

use lyra_db::{
    ratings::RatingFilter,
    search::Candidate,
};

const DEFAULT_LIMIT: u64 = 20;
const MAX_LIMIT: u64 = 50;

thread_local! {
    static MATCHER: RefCell<Matcher> = RefCell::new(Matcher::new(Config::DEFAULT));
}

/// A blank `needle` keeps every entry; one with nothing to match, like `^^^`,
/// keeps none. `text_of` and `set_score` must not call back into this.
pub(crate) fn fuzzy_filter<T>(
    entries: &mut Vec<T>,
    needle: &str,
    text_of: impl Fn(&T) -> &str,
    set_score: impl Fn(&mut T, u32),
) {
    let trimmed = needle.trim();
    if trimmed.is_empty() {
        return;
    }

    let pattern = Pattern::parse(trimmed, CaseMatching::Ignore, Normalization::Smart);
    if pattern.atoms.is_empty() {
        entries.clear();
        return;
    }

    MATCHER.with_borrow_mut(|matcher| {
        let mut buf = Vec::new();
        entries.retain_mut(|entry| {
            let utf32 = Utf32Str::new(text_of(entry), &mut buf);
            match pattern.score(utf32, matcher) {
                Some(score) => {
                    set_score(entry, score);
                    true
                }
                None => false,
            }
        });
    });
}

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
    let library_ids = (!principal.permissions.contains(&lyra_db::Permission::Admin))
        .then_some(&principal.accessible_library_ids);
    let mut candidates = lyra_db::search::collect_candidates(db, library_ids)?;
    if !options.rating_filter.is_empty() {
        let matching = lyra_db::ratings::target_ids_matching(
            db,
            principal.require(db)?,
            options.rating_filter,
        )?;
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
    let tracks = rank(&candidates.tracks, &options.query, limit)
        .into_iter()
        .map(|candidate| TitleHit {
            id: candidate.id,
            title: candidate.text,
        })
        .collect();
    let artists = rank(&candidates.artists, &options.query, limit)
        .into_iter()
        .map(|candidate| ArtistHit {
            id: candidate.id,
            name: candidate.text,
        })
        .collect();
    let releases = rank(&candidates.releases, &options.query, limit)
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

fn rank(candidates: &[Candidate], query: &str, limit: usize) -> Vec<Candidate> {
    let mut scored: Vec<_> = candidates.iter().map(|candidate| (candidate, 0)).collect();
    fuzzy_filter(
        &mut scored,
        query,
        |entry| entry.0.text.as_str(),
        |entry, score| entry.1 = score,
    );
    let mut scored: Vec<_> = scored
        .into_iter()
        .map(|(candidate, score)| {
            let name = candidate.text.to_lowercase();
            (candidate, score, name)
        })
        .collect();
    let compare = |a: &(&Candidate, u32, String), b: &(&Candidate, u32, String)| {
        b.1.cmp(&a.1)
            .then_with(|| a.2.cmp(&b.2))
            .then_with(|| a.0.id.cmp(&b.0.id))
    };
    if limit == 0 {
        return Vec::new();
    }
    if scored.len() > limit {
        scored.select_nth_unstable_by(limit, compare);
        scored.truncate(limit);
    }
    scored.sort_unstable_by(compare);
    scored.into_iter().map(|entry| entry.0.clone()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::auth::Principal;
    use agdb::DbId;
    use lyra_db::fixtures::{
        insert_artist,
        insert_release,
        insert_track,
    };
    use lyra_db::test_db::{
        insert_credited_artist,
        new_test_db,
    };

    fn admin_principal(db: &mut DbAny) -> anyhow::Result<Principal> {
        let user_db_id = lyra_db::test_db::insert_user(db, "search-user")?;
        Ok(Principal::for_user(
            db,
            user_db_id,
            vec![lyra_db::Permission::Admin],
            HashSet::new(),
        ))
    }

    #[test]
    fn search_ranks_best_matches_before_applying_limit() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let principal = admin_principal(&mut db)?;
        for title in ["Blue", "A b l u e", "Blue horizon", "B l u e"] {
            insert_track(&mut db, title)?;
            insert_credited_artist(&mut db, title)?;
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
        use lyra_db::fixtures::connect;
        use lyra_db::test_db::{
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
                .insert(lyra_db::libraries::get_by_id(&db, library)?.unwrap().id);
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
        insert_credited_artist(&mut db, "Bluegrass Trio")?;
        insert_credited_artist(&mut db, "Mingus")?;
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
            insert_credited_artist(&mut db, &format!("Blue Artist {i}"))?;
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
        let rated_artist = insert_credited_artist(&mut db, "Blue Artist")?;
        let low_release = insert_release(&mut db, "Blue Low Release")?;
        let high_release = insert_release(&mut db, "Blue High Release")?;

        let user_db_id = principal.require(&db)?;
        for (target, kind, value) in [
            (rated_track, lyra_db::ratings::RatingKind::Track, 4),
            (rated_artist, lyra_db::ratings::RatingKind::Artist, 3),
            (low_release, lyra_db::ratings::RatingKind::Release, 2),
            (high_release, lyra_db::ratings::RatingKind::Release, 5),
        ] {
            lyra_db::ratings::upsert(
                &mut db,
                user_db_id,
                target,
                kind,
                lyra_db::ratings::RatingValue::new(value).unwrap(),
                100,
            )?;
        }

        let filter = RatingFilter::new(
            lyra_db::ratings::RatingValue::new(3),
            lyra_db::ratings::RatingValue::new(4),
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

    fn run(needle: &str, mut entries: Vec<&'static str>) -> Vec<&'static str> {
        fuzzy_filter(&mut entries, needle, |s| *s, |_, _| {});
        entries
    }

    #[test]
    fn ranking_ties_are_stable_across_candidate_order() {
        let mut candidates: Vec<_> = ["c", "a", "b"]
            .into_iter()
            .enumerate()
            .map(|(index, id)| Candidate {
                db_id: DbId(index as i64 + 1),
                id: id.to_string(),
                text: "Blue".to_string(),
            })
            .collect();
        for _ in 0..2 {
            let hits = rank(&candidates, "blue", 2);
            assert_eq!(
                hits.iter().map(|hit| hit.id.as_str()).collect::<Vec<_>>(),
                vec!["a", "b"]
            );
            candidates.reverse();
        }
        assert!(rank(&candidates, "^^^", 2).is_empty());
        assert!(rank(&candidates, "blue", 0).is_empty());
    }

    #[test]
    fn ranking_preserves_query_modifiers() {
        let candidates: Vec<_> = ["Blue Train", "Blue Moon", "Kind of Blue"]
            .into_iter()
            .enumerate()
            .map(|(index, text)| Candidate {
                db_id: DbId(index as i64 + 1),
                id: index.to_string(),
                text: text.to_string(),
            })
            .collect();
        let hits = rank(&candidates, "^blue !moon", 5);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].text, "Blue Train");
    }

    #[test]
    fn empty_needle_is_a_noop() {
        let result = run("", vec!["Alpha", "Beta"]);
        assert_eq!(result, vec!["Alpha", "Beta"]);
    }

    #[test]
    fn whitespace_needle_is_a_noop() {
        let result = run("   ", vec!["Alpha", "Beta"]);
        assert_eq!(result, vec!["Alpha", "Beta"]);
    }

    #[test]
    fn non_empty_needle_with_zero_atoms_clears_entries() {
        let result = run("^^^", vec!["Alpha", "Beta"]);
        assert!(
            result.is_empty(),
            "garbage needle must not behave like empty needle"
        );
    }

    #[test]
    fn case_matching_is_case_insensitive() {
        let lower = run("blue", vec!["Blue Train", "Red Album"]);
        let upper = run("BLUE", vec!["Blue Train", "Red Album"]);
        assert_eq!(lower, upper);
        assert_eq!(lower, vec!["Blue Train"]);
    }
}

#[cfg(all(test, feature = "nightly"))]
mod benches {
    extern crate test;

    use std::hint::black_box;

    use test::Bencher;

    use super::*;
    use crate::services::auth::Principal;
    use lyra_db::fixtures::{
        connect,
        insert_artist,
        insert_release,
        insert_track,
    };
    use lyra_db::test_db::{
        connect_artist,
        insert_library,
        new_test_db,
    };

    fn fixture(track_count: usize, library_count: usize, admin: bool) -> (DbAny, Principal) {
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
        b.iter(|| {
            black_box(lyra_db::search::collect_candidates(black_box(&db), library_ids).unwrap())
        });
    }

    fn ranking(b: &mut Bencher, query: &str) {
        let (db, _) = fixture(100_000, 1, true);
        let candidates = lyra_db::search::collect_candidates(&db, None).unwrap();
        b.iter(|| {
            black_box(rank(black_box(&candidates.tracks), black_box(query), 5));
            black_box(rank(black_box(&candidates.artists), black_box(query), 5));
            black_box(rank(black_box(&candidates.releases), black_box(query), 5));
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
