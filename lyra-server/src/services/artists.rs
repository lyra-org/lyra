// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::{
    DbAny,
    DbId,
};

use crate::db::{
    self,
    Artist,
    Release,
    Track,
};

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ArtistIncludes {
    pub(crate) releases: bool,
    pub(crate) tracks: bool,
}

pub(crate) struct ArtistDetails {
    pub(crate) artist: Artist,
    pub(crate) releases: Option<Vec<Release>>,
    pub(crate) tracks: Option<Vec<Track>>,
}

pub(crate) fn list_details(
    db: &DbAny,
    includes: ArtistIncludes,
) -> anyhow::Result<Vec<ArtistDetails>> {
    let artists = db::artists::get(db, "artists")?;
    let mut details = Vec::with_capacity(artists.len());

    for artist in artists {
        let artist_db_id = artist
            .db_id
            .clone()
            .map(DbId::from)
            .ok_or_else(|| anyhow::anyhow!("artist missing db id"))?;
        let releases = if includes.releases {
            Some(db::releases::get_by_artist(db, artist_db_id)?)
        } else {
            None
        };
        let tracks = if includes.tracks {
            Some(db::tracks::get_by_artist(db, artist_db_id)?)
        } else {
            None
        };

        details.push(ArtistDetails {
            artist: artist,
            releases,
            tracks,
        });
    }

    Ok(details)
}

pub(crate) fn get_details(
    db: &DbAny,
    artist_db_id: DbId,
    includes: ArtistIncludes,
) -> anyhow::Result<Option<ArtistDetails>> {
    let Some(artist) = db::artists::get_by_id(db, artist_db_id)? else {
        return Ok(None);
    };

    let releases = if includes.releases {
        Some(db::releases::get_by_artist(db, artist_db_id)?)
    } else {
        None
    };
    let tracks = if includes.tracks {
        Some(db::tracks::get_by_artist(db, artist_db_id)?)
    } else {
        None
    };

    Ok(Some(ArtistDetails {
        artist: artist,
        releases,
        tracks,
    }))
}

pub(crate) fn update(
    db: &mut DbAny,
    artist_db_id: DbId,
    update_name: Option<String>,
    update_sort_name: Option<Option<String>>,
    update_description: Option<Option<String>>,
) -> anyhow::Result<Option<Artist>> {
    let Some(artist_entity) = db::artists::get_by_id(db, artist_db_id)? else {
        return Ok(None);
    };

    let Artist {
        id: entity_id,
        artist_name,
        scan_name,
        sort_name: artist_sort_name,
        description: artist_description,
        verified,
        created_at,
        ..
    } = artist_entity;
    let updated_name = update_name.unwrap_or(artist_name);
    let mut updated_sort_name = artist_sort_name;
    let mut updated_description = artist_description;

    let mut clear_sort_name = false;
    if let Some(sort_name) = update_sort_name {
        match sort_name {
            Some(value) => updated_sort_name = Some(value),
            None => {
                updated_sort_name = None;
                clear_sort_name = true;
            }
        }
    }

    let mut clear_description = false;
    if let Some(description) = update_description {
        match description {
            Some(value) => updated_description = Some(value),
            None => {
                updated_description = None;
                clear_description = true;
            }
        }
    }

    let updated = Artist {
        db_id: Some(artist_db_id.into()),
        id: entity_id,
        artist_name: updated_name,
        scan_name,
        sort_name: updated_sort_name,
        artist_type: None,
        description: updated_description,
        verified,
        locked: None,
        created_at,
    };

    db::artists::update_with_clears(db, &updated, clear_sort_name, clear_description)?;
    Ok(Some(updated))
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

    #[test]
    fn list_details_returns_artists_with_releases_and_tracks() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let artist_id = insert_artist(&mut db, "Coltrane")?;
        let release_id = insert_release(&mut db, "A Love Supreme")?;
        let track_id = insert_track(&mut db, "Acknowledgement")?;

        connect_artist(&mut db, release_id, artist_id)?;
        connect(&mut db, release_id, track_id)?;
        connect_artist(&mut db, track_id, artist_id)?;

        let includes = ArtistIncludes {
            releases: true,
            tracks: true,
        };
        let details = list_details(&db, includes)?;

        assert_eq!(details.len(), 1);
        assert_eq!(details[0].artist.artist_name, "Coltrane");
        assert_eq!(
            details[0].artist.db_id.clone().map(DbId::from),
            Some(artist_id)
        );

        let releases = details[0].releases.as_ref().expect("releases included");
        assert_eq!(releases.len(), 1);
        assert_eq!(releases[0].release_title, "A Love Supreme");

        let tracks = details[0].tracks.as_ref().expect("tracks included");
        assert_eq!(tracks.len(), 1);
        assert_eq!(tracks[0].track_title, "Acknowledgement");

        Ok(())
    }

    #[test]
    fn list_details_omits_includes_when_disabled() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        insert_artist(&mut db, "Solo Artist")?;

        let includes = ArtistIncludes {
            releases: false,
            tracks: false,
        };
        let details = list_details(&db, includes)?;

        assert_eq!(details.len(), 1);
        assert!(details[0].releases.is_none());
        assert!(details[0].tracks.is_none());
        Ok(())
    }

    #[test]
    fn get_details_returns_none_for_missing_artist() -> anyhow::Result<()> {
        let db = new_test_db()?;
        let result = get_details(&db, DbId(999_999), ArtistIncludes::default())?;
        assert!(result.is_none());
        Ok(())
    }

    #[test]
    fn get_details_hydrates_releases_and_tracks() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let artist_id = insert_artist(&mut db, "Mingus")?;
        let release_id = insert_release(&mut db, "The Black Saint")?;
        let track_id = insert_track(&mut db, "Solo Dancer")?;

        connect_artist(&mut db, release_id, artist_id)?;
        connect(&mut db, release_id, track_id)?;
        connect_artist(&mut db, track_id, artist_id)?;

        let includes = ArtistIncludes {
            releases: true,
            tracks: true,
        };
        let details = get_details(&db, artist_id, includes)?.expect("artist should exist");

        assert_eq!(details.artist.artist_name, "Mingus");

        let releases = details.releases.expect("releases included");
        assert_eq!(releases.len(), 1);
        assert_eq!(releases[0].release_title, "The Black Saint");

        let tracks = details.tracks.expect("tracks included");
        assert_eq!(tracks.len(), 1);
        assert_eq!(tracks[0].track_title, "Solo Dancer");

        Ok(())
    }
}
