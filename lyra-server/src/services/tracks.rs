// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::{
    DbAny,
    DbId,
};

use super::entities::{
    ResolvedCreditedArtist,
    resolve_track_credited_artists,
};
use crate::db::{
    self,
    ListOptions,
    Release,
    Track,
};

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct TrackIncludes {
    pub(crate) releases: bool,
    pub(crate) artists: bool,
}

pub(crate) struct TrackDetails {
    pub(crate) track: Track,
    pub(crate) releases: Option<Vec<Release>>,
    pub(crate) artists: Option<Vec<ResolvedCreditedArtist>>,
}

pub(crate) fn list_details(
    db: &DbAny,
    includes: TrackIncludes,
    options: &ListOptions,
) -> anyhow::Result<Vec<TrackDetails>> {
    let tracks = db::tracks::query(db, "tracks", options)?.entries;

    let track_ids: Vec<DbId> = tracks
        .iter()
        .filter_map(|track| track.db_id.clone().map(DbId::from))
        .collect();

    let releases_by_track = if includes.releases {
        Some(db::releases::get_by_tracks(db, &track_ids)?)
    } else {
        None
    };
    let artists_by_track = if includes.artists {
        let ctx = super::entities::TrackCreditedArtistContext {
            releases_by_track: releases_by_track.as_ref(),
            credited_artists_by_release: None,
            scope_release_id: None,
        };
        Some(super::entities::resolve_track_credited_artists_with_context(db, &track_ids, &ctx)?)
    } else {
        None
    };

    let mut details = Vec::with_capacity(tracks.len());
    for track in tracks {
        let track_db_id = track
            .db_id
            .clone()
            .map(DbId::from)
            .ok_or_else(|| anyhow::anyhow!("track missing db id"))?;
        let releases = releases_by_track
            .as_ref()
            .map(|m| m.get(&track_db_id).cloned().unwrap_or_default());
        let artists = artists_by_track
            .as_ref()
            .map(|m| m.get(&track_db_id).cloned().unwrap_or_default());

        details.push(TrackDetails {
            track,
            releases,
            artists,
        });
    }

    Ok(details)
}

pub(crate) fn get_details(
    db: &DbAny,
    track_db_id: DbId,
    includes: TrackIncludes,
) -> anyhow::Result<Option<TrackDetails>> {
    let Some(track) = db::tracks::get_by_id(db, track_db_id)? else {
        return Ok(None);
    };

    let releases = if includes.releases {
        Some(db::releases::get_by_track(db, track_db_id)?)
    } else {
        None
    };
    let artists = if includes.artists {
        Some(resolve_track_credited_artists(db, track_db_id)?)
    } else {
        None
    };

    Ok(Some(TrackDetails {
        track,
        releases,
        artists,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::test_db::{
        connect,
        connect_artist,
        insert_artist,
        insert_release,
        insert_track,
        new_test_db,
    };
    use crate::services::entities::ArtistCreditSource;

    fn default_options() -> ListOptions {
        ListOptions {
            sort: Vec::new(),
            offset: None,
            limit: None,
            search_term: None,
        }
    }

    #[test]
    fn list_details_returns_tracks_with_releases_and_artists() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_id = insert_release(&mut db, "Blue Train")?;
        let track_id = insert_track(&mut db, "Blue Train")?;
        let artist_id = insert_artist(&mut db, "Coltrane")?;

        connect(&mut db, release_id, track_id)?;
        connect_artist(&mut db, track_id, artist_id)?;

        let includes = TrackIncludes {
            releases: true,
            artists: true,
        };
        let details = list_details(&db, includes, &default_options())?;

        assert_eq!(details.len(), 1);
        assert_eq!(details[0].track.track_title, "Blue Train");
        assert_eq!(
            details[0].track.db_id.clone().map(DbId::from),
            Some(track_id)
        );

        let releases = details[0].releases.as_ref().expect("releases included");
        assert_eq!(releases.len(), 1);
        assert_eq!(releases[0].release_title, "Blue Train");

        let artists = details[0].artists.as_ref().expect("artists included");
        assert_eq!(artists.len(), 1);
        assert_eq!(artists[0].artist.artist_name, "Coltrane");
        assert_eq!(artists[0].credit.credit_type, db::CreditType::Artist);
        assert_eq!(artists[0].source, ArtistCreditSource::Track);

        Ok(())
    }

    #[test]
    fn list_details_omits_includes_when_disabled() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        insert_track(&mut db, "Lone Track")?;

        let includes = TrackIncludes {
            releases: false,
            artists: false,
        };
        let details = list_details(&db, includes, &default_options())?;

        assert_eq!(details.len(), 1);
        assert!(details[0].releases.is_none());
        assert!(details[0].artists.is_none());
        Ok(())
    }

    #[test]
    fn get_details_returns_none_for_missing_track() -> anyhow::Result<()> {
        let db = new_test_db()?;
        let result = get_details(&db, DbId(999_999), TrackIncludes::default())?;
        assert!(result.is_none());
        Ok(())
    }

    #[test]
    fn get_details_hydrates_releases_and_artists() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_id = insert_release(&mut db, "Mingus Ah Um")?;
        let track_id = insert_track(&mut db, "Goodbye Pork Pie Hat")?;
        let artist_id = insert_artist(&mut db, "Mingus")?;

        connect(&mut db, release_id, track_id)?;
        connect_artist(&mut db, track_id, artist_id)?;

        let includes = TrackIncludes {
            releases: true,
            artists: true,
        };
        let details = get_details(&db, track_id, includes)?.expect("track should exist");

        assert_eq!(details.track.track_title, "Goodbye Pork Pie Hat");

        let releases = details.releases.expect("releases included");
        assert_eq!(releases.len(), 1);
        assert_eq!(releases[0].release_title, "Mingus Ah Um");

        let artists = details.artists.expect("artists included");
        assert_eq!(artists.len(), 1);
        assert_eq!(artists[0].artist.artist_name, "Mingus");
        assert_eq!(artists[0].credit.credit_type, db::CreditType::Artist);
        assert_eq!(artists[0].source, ArtistCreditSource::Track);

        Ok(())
    }

    #[test]
    fn get_details_resolves_artists_from_release_when_track_has_none() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_id = insert_release(&mut db, "Release")?;
        let track_id = insert_track(&mut db, "Track")?;
        let artist_id = insert_artist(&mut db, "Release Artist")?;

        connect(&mut db, release_id, track_id)?;
        connect_artist(&mut db, release_id, artist_id)?;
        // No direct track -> artist edge

        let includes = TrackIncludes {
            releases: false,
            artists: true,
        };
        let details = get_details(&db, track_id, includes)?.expect("track should exist");

        let artists = details.artists.expect("artists included");
        assert_eq!(artists.len(), 1);
        assert_eq!(artists[0].artist.artist_name, "Release Artist");
        assert_eq!(artists[0].credit.credit_type, db::CreditType::Artist);
        assert_eq!(artists[0].source, ArtistCreditSource::Release);

        Ok(())
    }
}
