// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashSet;

use agdb::{
    DbAny,
    DbId,
};

use crate::db::{
    self,
    ListOptions,
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
    let (mut tracks, mut artists, mut releases) =
        if principal.permissions.contains(&db::Permission::Admin) {
            (
                db::tracks::get(db, "tracks")?,
                db::artists::get(db, "artists")?,
                db::releases::get_direct(db, "releases")?,
            )
        } else {
            let mut library_db_ids = Vec::new();
            for library_id in &principal.accessible_library_ids {
                let Some(library_db_id) = db::lookup::find_node_id_by_id(db, library_id)? else {
                    continue;
                };
                if db::libraries::get_by_id(db, library_db_id)?.is_some() {
                    library_db_ids.push(library_db_id);
                }
            }

            let mut tracks = Vec::new();
            let mut artists = Vec::new();
            let mut releases = Vec::new();
            let mut seen_tracks = HashSet::<DbId>::new();
            let mut seen_artists = HashSet::<DbId>::new();
            let mut seen_releases = HashSet::<DbId>::new();

            for library_db_id in library_db_ids {
                for track in db::tracks::get_by_library(db, library_db_id)? {
                    if track
                        .db_id
                        .clone()
                        .map(DbId::from)
                        .is_some_and(|id| seen_tracks.insert(id))
                    {
                        tracks.push(track);
                    }
                }
                for artist in db::artists::get_by_library(db, library_db_id)? {
                    if artist
                        .db_id
                        .clone()
                        .map(DbId::from)
                        .is_some_and(|id| seen_artists.insert(id))
                    {
                        artists.push(artist);
                    }
                }
                for release in db::releases::get_direct(db, library_db_id)? {
                    if release
                        .db_id
                        .clone()
                        .map(DbId::from)
                        .is_some_and(|id| seen_releases.insert(id))
                    {
                        releases.push(release);
                    }
                }
            }

            (tracks, artists, releases)
        };

    if !options.rating_filter.is_empty() {
        let matching_target_ids =
            db::ratings::target_ids_matching(db, principal.user_db_id, options.rating_filter)?;
        tracks.retain(|track| {
            track
                .db_id
                .clone()
                .map(DbId::from)
                .is_some_and(|id| matching_target_ids.contains(&id))
        });
        artists.retain(|artist| {
            artist
                .db_id
                .clone()
                .map(DbId::from)
                .is_some_and(|id| matching_target_ids.contains(&id))
        });
        releases.retain(|release| {
            release
                .db_id
                .clone()
                .map(DbId::from)
                .is_some_and(|id| matching_target_ids.contains(&id))
        });
    }

    let list_options = ListOptions {
        sort: Vec::new(),
        offset: None,
        limit: Some(options.limit),
        search_term: Some(options.query.clone()),
    };

    let tracks = db::tracks::query_items(tracks, &list_options)
        .entries
        .into_iter()
        .map(|track| TitleHit {
            id: track.id,
            title: track.track_title,
        })
        .collect();

    let artists = db::artists::query_items(artists, &list_options)
        .entries
        .into_iter()
        .map(|artist| ArtistHit {
            id: artist.id,
            name: artist.artist_name,
        })
        .collect();

    let releases = db::releases::query_items(releases, &list_options)
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
