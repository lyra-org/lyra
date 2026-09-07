// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    cell::RefCell,
    collections::HashSet,
};

use agdb::{
    DbAny,
    DbElement,
    DbId,
    QueryBuilder,
    QueryId,
};

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

thread_local! {
    static MATCHER: RefCell<Matcher> = RefCell::new(Matcher::new(Config::DEFAULT));
}

/// Empty/whitespace `needle` is a no-op; a non-empty `needle` that parses to
/// zero atoms (e.g. `^^^`) clears `entries` rather than leaking the unfiltered
/// list. Not reentrant: the matcher is held across `retain_mut`, so `text_of`
/// and `set_score` must not call back in, and no `.await` may be added inside.
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
pub(crate) struct Candidate {
    pub(crate) db_id: DbId,
    pub(crate) id: String,
    pub(crate) text: String,
}

#[derive(Default)]
pub(crate) struct Candidates {
    pub(crate) tracks: Vec<Candidate>,
    pub(crate) artists: Vec<Candidate>,
    pub(crate) releases: Vec<Candidate>,
}

fn string_value(element: &mut DbElement, key: &str) -> anyhow::Result<String> {
    let value = element
        .values
        .iter_mut()
        .find(|value| value.key.string().is_ok_and(|stored| stored == key))
        .ok_or_else(|| anyhow::anyhow!("search candidate {} is missing {key}", element.id.0))?;
    Ok(String::try_from(std::mem::take(&mut value.value))?)
}

fn neighbor_types(db: &DbAny, from: impl Into<QueryId>) -> anyhow::Result<Vec<DbElement>> {
    Ok(db
        .exec(
            QueryBuilder::select()
                .values(["db_element_id"])
                .search()
                .from(from)
                .where_()
                .neighbor()
                .query(),
        )?
        .elements)
}

fn typed_neighbors(db: &DbAny, from: impl Into<QueryId>, kind: &str) -> anyhow::Result<Vec<DbId>> {
    Ok(neighbor_types(db, from)?
        .into_iter()
        .filter(|element| super::graph::is_element_type(element, kind))
        .map(|element| element.id)
        .collect())
}

fn candidates(elements: Vec<DbElement>, text_key: &str) -> anyhow::Result<Vec<Candidate>> {
    elements
        .into_iter()
        .map(|mut element| {
            Ok(Candidate {
                db_id: element.id,
                id: string_value(&mut element, "id")?,
                text: string_value(&mut element, text_key)?,
            })
        })
        .collect()
}

fn fetch(db: &DbAny, ids: HashSet<DbId>, text_key: &str) -> anyhow::Result<Vec<Candidate>> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let mut ids: Vec<_> = ids.into_iter().collect();
    ids.sort_unstable();
    let elements = db
        .exec(
            QueryBuilder::select()
                .values(["id", text_key])
                .ids(ids)
                .query(),
        )?
        .elements;
    candidates(elements, text_key)
}

fn from_root(db: &DbAny, root: &str, kind: &str, text_key: &str) -> anyhow::Result<Vec<Candidate>> {
    let elements = db
        .exec(
            QueryBuilder::select()
                .values(["id", text_key])
                .search()
                .from(root)
                .where_()
                .neighbor()
                .and()
                .key("db_element_id")
                .value(kind)
                .query(),
        )?
        .elements;
    candidates(elements, text_key)
}

pub(crate) fn collect_candidates(
    db: &DbAny,
    library_ids: Option<&HashSet<String>>,
) -> anyhow::Result<Candidates> {
    let Some(library_ids) = library_ids else {
        return Ok(Candidates {
            tracks: from_root(db, "tracks", "Track", "track_title")?,
            artists: from_root(db, "artists", "Artist", "artist_name")?,
            releases: from_root(db, "releases", "Release", "release_title")?,
        });
    };
    let (mut tracks, mut artists, mut releases) = (HashSet::new(), HashSet::new(), HashSet::new());
    for public_id in library_ids {
        let Some(library_id) = super::lookup::find_node_id_by_id(db, public_id)? else {
            continue;
        };
        if super::libraries::get_by_id(db, library_id)?.is_some() {
            releases.extend(typed_neighbors(db, library_id, "Release")?);
        }
    }
    let mut credits = HashSet::new();
    // Walk each distinct release once, collecting IDs before fetching search fields.
    for release in &releases {
        for element in neighbor_types(db, *release)? {
            if super::graph::is_element_type(&element, "Track") {
                tracks.insert(element.id);
            } else if super::graph::is_element_type(&element, "Artist") {
                artists.insert(element.id);
            } else if super::graph::is_element_type(&element, "Credit") {
                credits.insert(element.id);
            }
        }
    }
    for credit in credits {
        artists.extend(typed_neighbors(db, credit, "Artist")?);
    }
    Ok(Candidates {
        tracks: fetch(db, tracks, "track_title")?,
        artists: fetch(db, artists, "artist_name")?,
        releases: fetch(db, releases, "release_title")?,
    })
}

pub(crate) fn rank(candidates: &[Candidate], query: &str, limit: usize) -> Vec<Candidate> {
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
