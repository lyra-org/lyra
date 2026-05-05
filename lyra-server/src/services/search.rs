// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::{
    DbAny,
    QueryId,
};

use crate::db::{
    self,
    ListOptions,
};

const DEFAULT_LIMIT: u64 = 20;
const MAX_LIMIT: u64 = 50;

#[derive(Clone, Debug)]
pub(crate) struct SearchOptions {
    pub(crate) query: String,
    pub(crate) limit: u64,
}

impl SearchOptions {
    pub(crate) fn new(query: String, limit: Option<u64>) -> Self {
        let limit = limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
        Self { query, limit }
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

pub(crate) fn search(db: &DbAny, options: &SearchOptions) -> anyhow::Result<SearchResults> {
    let list_options = ListOptions {
        sort: Vec::new(),
        offset: None,
        limit: Some(options.limit),
        search_term: Some(options.query.clone()),
    };

    let tracks = db::tracks::query(db, "tracks", &list_options)?
        .entries
        .into_iter()
        .map(|track| TitleHit {
            id: track.id,
            title: track.track_title,
        })
        .collect();

    let artists = db::artists::query(db, "artists", &list_options, None)?
        .entries
        .into_iter()
        .map(|artist| ArtistHit {
            id: artist.id,
            name: artist.artist_name,
        })
        .collect();

    let releases = db::releases::query(
        db,
        QueryId::Alias("releases".to_string()),
        &list_options,
        &db::releases::ReleaseQueryFilters::default(),
    )?
    .entries
    .into_iter()
    .map(|release| TitleHit {
        id: release.id,
        title: release.release_title,
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

    #[test]
    fn search_returns_per_entity_hits_for_matching_query() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        insert_track(&mut db, "Blue in Green")?;
        insert_track(&mut db, "Red Clay")?;
        insert_artist(&mut db, "Bluegrass Trio")?;
        insert_artist(&mut db, "Mingus")?;
        insert_release(&mut db, "Kind of Blue")?;
        insert_release(&mut db, "Bitches Brew")?;

        let options = SearchOptions::new("blue".to_string(), None);
        let results = search(&db, &options)?;

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
        let zero = SearchOptions::new("q".to_string(), Some(0));
        assert_eq!(zero.limit, 1);
        let huge = SearchOptions::new("q".to_string(), Some(9_999));
        assert_eq!(huge.limit, MAX_LIMIT);
        let none = SearchOptions::new("q".to_string(), None);
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

        let options = SearchOptions::new("blue".to_string(), Some(3));
        let results = search(&db, &options)?;

        assert!(results.tracks.len() <= 3);
        assert!(results.artists.len() <= 3);
        assert!(results.releases.len() <= 3);
        Ok(())
    }
}
