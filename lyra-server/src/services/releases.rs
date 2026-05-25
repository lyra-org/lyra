// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashMap;

use agdb::{
    DbAny,
    DbId,
    QueryId,
};

use crate::db::{
    self,
    Entry,
    ListOptions,
    PagedResult,
    Release,
    Track,
};

use super::entities::{
    ResolvedCreditedArtist,
    relations,
};

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ReleaseIncludes {
    pub(crate) artists: bool,
    pub(crate) tracks: bool,
    pub(crate) track_artists: bool,
    pub(crate) entries: bool,
}

pub(crate) struct ReleaseDetails {
    pub(crate) release_db_id: DbId,
    pub(crate) release: Release,
    pub(crate) artists: Option<Vec<ResolvedCreditedArtist>>,
    pub(crate) tracks: Option<Vec<Track>>,
    pub(crate) track_artists: Option<HashMap<DbId, Vec<ResolvedCreditedArtist>>>,
    pub(crate) entries: Option<Vec<Entry>>,
}

pub(crate) fn get(db: &DbAny, id: Option<QueryId>) -> anyhow::Result<Vec<Release>> {
    match id {
        None => db::releases::get(db, "releases"),
        Some(query_id) => match query_id {
            QueryId::Id(node_id) => {
                if db::tracks::get_by_id(db, node_id)?.is_some() {
                    db::releases::get_by_track(db, node_id)
                } else {
                    db::releases::get(db, QueryId::Id(node_id))
                }
            }
            other => db::releases::get(db, other),
        },
    }
}

pub(crate) fn get_many_by_track(
    db: &DbAny,
    track_ids: &[DbId],
) -> anyhow::Result<HashMap<DbId, Vec<Release>>> {
    db::releases::get_by_tracks(db, track_ids)
}

pub(crate) fn query(
    db: &DbAny,
    scope: Option<QueryId>,
    list_options: &ListOptions,
) -> anyhow::Result<PagedResult<Release>> {
    let from = match scope {
        None => QueryId::Alias("releases".to_string()),
        Some(query_id) => query_id,
    };

    match from {
        QueryId::Id(node_id) if db::tracks::get_by_id(db, node_id)?.is_some() => {
            // Preserve releases-by-track behavior with manual search/pagination.
            let mut releases = db::releases::get_by_track(db, node_id)?;
            if let Some(ref term) = list_options.search_term {
                db::search::fuzzy_filter(
                    &mut releases,
                    term,
                    |release| release.release_title.as_str(),
                    |_, _| {},
                );
            }
            let total_count = releases.len() as u64;
            let offset = list_options.offset.unwrap_or(0).min(total_count);
            let entries = match list_options.limit {
                Some(limit) => releases
                    .into_iter()
                    .skip(offset as usize)
                    .take(limit as usize)
                    .collect(),
                None => releases.into_iter().skip(offset as usize).collect(),
            };

            Ok(PagedResult {
                entries,
                total_count,
                offset,
            })
        }
        other => db::releases::query(
            db,
            other,
            list_options,
            &db::releases::ReleaseQueryFilters::default(),
        ),
    }
}

pub(crate) fn query_by_artists(
    db: &DbAny,
    artist_ids: &[DbId],
    scope: Option<QueryId>,
    list_options: &ListOptions,
) -> anyhow::Result<PagedResult<Release>> {
    db::releases::query_by_artists(
        db,
        artist_ids,
        scope,
        list_options,
        &db::releases::ReleaseQueryFilters::default(),
    )
}

pub(crate) fn get_appearances(db: &DbAny, artist_id: DbId) -> anyhow::Result<Vec<Release>> {
    db::releases::get_appearances(db, artist_id)
}

pub(crate) fn list_details_for_releases(
    db: &DbAny,
    includes: ReleaseIncludes,
    releases: Vec<Release>,
) -> anyhow::Result<Vec<ReleaseDetails>> {
    let release_ids = relations::db_ids_from_releases(&releases);
    let needs_release_tracks = includes.tracks || includes.track_artists || includes.entries;
    let needs_release_artists = includes.artists || includes.track_artists;

    let mut tracks_by_release = if needs_release_tracks {
        relations::release_tracks_by_release(db, &release_ids)?
    } else {
        HashMap::new()
    };
    let artists_by_release = if needs_release_artists {
        Some(relations::release_credited_artists_by_release(
            db,
            &release_ids,
        )?)
    } else {
        None
    };
    let mut details = Vec::with_capacity(releases.len());
    for release in releases {
        let release_db_id = release
            .db_id
            .clone()
            .map(DbId::from)
            .ok_or_else(|| anyhow::anyhow!("release missing db id"))?;

        let release_tracks = if needs_release_tracks {
            tracks_by_release.remove(&release_db_id).unwrap_or_default()
        } else {
            Vec::new()
        };
        let artists = artists_by_release
            .as_ref()
            .filter(|_| includes.artists)
            .map(|m| m.get(&release_db_id).cloned().unwrap_or_default());
        let track_artists = if includes.track_artists {
            let track_ids = relations::db_ids_from_tracks(&release_tracks);
            Some(relations::track_credited_artists_by_track(
                db,
                &track_ids,
                None,
                artists_by_release.as_ref(),
                Some(release_db_id),
            )?)
        } else {
            None
        };
        let entries = if includes.entries {
            Some(relations::unique_entries_for_tracks(db, &release_tracks)?)
        } else {
            None
        };
        let tracks = if includes.tracks {
            Some(release_tracks)
        } else {
            None
        };

        details.push(ReleaseDetails {
            release_db_id,
            release,
            artists,
            tracks,
            track_artists,
            entries,
        });
    }

    Ok(details)
}

pub(crate) fn get_details(
    db: &DbAny,
    release_db_id: DbId,
    includes: ReleaseIncludes,
) -> anyhow::Result<Option<ReleaseDetails>> {
    let Some(release) = db::releases::get_by_id(db, release_db_id)? else {
        return Ok(None);
    };

    Ok(list_details_for_releases(db, includes, vec![release])?
        .into_iter()
        .next())
}

#[cfg(test)]
mod tests {
    use agdb::{
        DbAny,
        DbId,
        QueryBuilder,
    };

    use super::*;
    use crate::db::test_db::{
        connect_artist,
        new_test_db,
    };
    use crate::db::{
        Artist,
        SortDirection,
        SortKey,
        SortSpec,
    };
    use crate::services::entities::ArtistCreditSource;
    use nanoid::nanoid;

    #[derive(Clone, Debug, Default)]
    struct ReleaseListFilters {
        year: Option<u32>,
        genres: Vec<String>,
    }

    fn list_details_with_options(
        db: &DbAny,
        includes: ReleaseIncludes,
        list_options: ListOptions,
        filters: ReleaseListFilters,
    ) -> anyhow::Result<Vec<ReleaseDetails>> {
        let ids = if !filters.genres.is_empty() {
            Some(db::genres::release_ids_matching_genres(
                db,
                &filters.genres,
            )?)
        } else {
            None
        };

        let query_filters = db::releases::ReleaseQueryFilters {
            year: filters.year,
            ids,
        };
        let releases = db::releases::query(
            db,
            QueryId::Alias("releases".to_string()),
            &list_options,
            &query_filters,
        )?
        .entries;
        list_details_for_releases(db, includes, releases)
    }

    fn insert_release(
        db: &mut DbAny,
        title: &str,
        sort_title: Option<&str>,
        year: Option<u32>,
        genres: Option<Vec<&str>>,
    ) -> anyhow::Result<DbId> {
        let genre_strings: Option<Vec<String>> =
            genres.map(|values| values.into_iter().map(ToString::to_string).collect());
        let release = Release {
            db_id: None,
            id: nanoid!(),
            release_title: title.to_string(),
            sort_title: sort_title.map(ToString::to_string),
            release_type: None,
            release_date: year.map(|year| format!("{year:04}")),
            locked: None,
            created_at: None,
            ctime: None,
        };

        let result = db.exec_mut(QueryBuilder::insert().element(&release).query())?;
        let release_db_id = result
            .ids()
            .first()
            .copied()
            .ok_or_else(|| anyhow::anyhow!("release insert returned no id"))?;
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from("releases")
                .to(release_db_id)
                .query(),
        )?;

        if let Some(genre_names) = &genre_strings {
            db::genres::sync_release_genres(db, release_db_id, genre_names)?;
        }

        Ok(release_db_id)
    }

    #[test]
    fn list_details_with_options_filters_by_query_year_and_genre() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        insert_release(
            &mut db,
            "Blue Train",
            Some("Blue Train"),
            Some(1957),
            Some(vec!["Jazz"]),
        )?;
        insert_release(
            &mut db,
            "Blue Sky Noise",
            Some("Blue Sky Noise"),
            Some(2010),
            Some(vec!["Rock"]),
        )?;
        insert_release(
            &mut db,
            "Red Release",
            Some("Red Release"),
            Some(2010),
            Some(vec!["Rock"]),
        )?;

        let list_options = ListOptions {
            sort: Vec::new(),
            offset: None,
            limit: None,
            search_term: Some("blue".to_string()),
        };
        let filters = ReleaseListFilters {
            year: Some(2010),
            genres: vec!["rock".to_string()],
        };
        let details =
            list_details_with_options(&db, ReleaseIncludes::default(), list_options, filters)?;

        assert_eq!(details.len(), 1);
        assert_eq!(details[0].release.release_title, "Blue Sky Noise");
        Ok(())
    }

    #[test]
    fn get_details_returns_none_for_missing_release() -> anyhow::Result<()> {
        let db = new_test_db()?;
        let result = get_details(&db, DbId(999_999), ReleaseIncludes::default())?;
        assert!(result.is_none());
        Ok(())
    }

    #[test]
    fn get_details_hydrates_tracks_and_artists() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_id = insert_release(&mut db, "Kind of Blue", None, Some(1959), None)?;

        let track = Track {
            db_id: None,
            id: nanoid!(),
            track_title: "So What".to_string(),
            sort_title: None,
            year: None,
            disc: Some(1),
            disc_total: None,
            track: Some(1),
            track_total: None,
            duration_ms: Some(540_000),
            sample_rate_hz: None,
            channel_count: None,
            bit_depth: None,
            bitrate_bps: None,
            locked: None,
            created_at: None,
            ctime: None,
        };
        let qr = db.exec_mut(QueryBuilder::insert().element(&track).query())?;
        let track_id = *qr.ids().first().unwrap();
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from("tracks")
                .to(track_id)
                .query(),
        )?;
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(release_id)
                .to(track_id)
                .query(),
        )?;

        let artist = Artist {
            db_id: None,
            id: nanoid!(),
            artist_name: "Miles Davis".to_string(),
            scan_name: "miles davis".to_string(),
            sort_name: None,
            artist_type: None,
            description: None,
            verified: false,
            locked: None,
            created_at: None,
        };
        let qr = db.exec_mut(QueryBuilder::insert().element(&artist).query())?;
        let artist_id = *qr.ids().first().unwrap();
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from("artists")
                .to(artist_id)
                .query(),
        )?;
        connect_artist(&mut db, release_id, artist_id)?;

        let includes = ReleaseIncludes {
            artists: true,
            tracks: true,
            track_artists: false,
            entries: false,
        };
        let details = get_details(&db, release_id, includes)?.expect("release should exist");

        assert_eq!(details.release.release_title, "Kind of Blue");
        assert_eq!(details.release_db_id, release_id);

        let tracks = details.tracks.expect("tracks should be included");
        assert_eq!(tracks.len(), 1);
        assert_eq!(tracks[0].track_title, "So What");

        let artists = details.artists.expect("artists should be included");
        assert_eq!(artists.len(), 1);
        assert_eq!(artists[0].artist.artist_name, "Miles Davis");
        assert_eq!(artists[0].credit.credit_type, db::CreditType::Artist);
        assert_eq!(artists[0].source, ArtistCreditSource::Release);

        assert!(details.entries.is_none());
        Ok(())
    }

    #[test]
    fn get_details_omits_includes_when_disabled() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_id = insert_release(&mut db, "Test Release", None, None, None)?;

        let includes = ReleaseIncludes {
            artists: false,
            tracks: false,
            track_artists: false,
            entries: false,
        };
        let details = get_details(&db, release_id, includes)?.expect("release should exist");
        assert!(details.artists.is_none());
        assert!(details.tracks.is_none());
        assert!(details.entries.is_none());
        Ok(())
    }

    #[test]
    fn list_details_with_options_applies_sorting() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        insert_release(&mut db, "First", Some("alpha"), None, None)?;
        insert_release(&mut db, "Second", Some("charlie"), None, None)?;
        insert_release(&mut db, "Third", Some("bravo"), None, None)?;

        let list_options = ListOptions {
            sort: vec![SortSpec {
                key: SortKey::SortName,
                direction: SortDirection::Descending,
            }],
            offset: None,
            limit: None,
            search_term: None,
        };
        let details = list_details_with_options(
            &db,
            ReleaseIncludes::default(),
            list_options,
            ReleaseListFilters::default(),
        )?;
        let titles: Vec<String> = details
            .into_iter()
            .map(|detail| detail.release.release_title)
            .collect();

        assert_eq!(titles, vec!["Second", "Third", "First"]);
        Ok(())
    }
}
