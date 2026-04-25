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
    Artist,
    Entry,
    Release,
    Track,
};

#[derive(Clone, Copy)]
pub(crate) struct EntryIncludes {
    pub(crate) tracks: bool,
    pub(crate) releases: bool,
    pub(crate) artists: bool,
}

pub(crate) struct EntryDetails {
    pub(crate) entry: Entry,
    pub(crate) tracks: Option<Vec<Track>>,
    pub(crate) releases: Option<Vec<Release>>,
    pub(crate) artists: Option<Vec<Artist>>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum EntryServiceError {
    #[error("not found: {0}")]
    NotFound(String),
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

fn collect_releases(db: &DbAny, tracks: &[Track]) -> anyhow::Result<Vec<Release>> {
    let mut releases = Vec::new();
    let mut seen = HashSet::new();

    for track in tracks {
        let Some(track_db_id) = track.db_id.clone() else {
            continue;
        };
        let track_releases = db::releases::get_by_track(db, DbId::from(track_db_id))?;
        for release in track_releases {
            let Some(release_db_id) = release.db_id.clone() else {
                releases.push(release);
                continue;
            };
            if seen.insert(DbId::from(release_db_id)) {
                releases.push(release);
            }
        }
    }

    Ok(releases)
}

fn collect_artists(db: &DbAny, releases: &[Release]) -> anyhow::Result<Vec<Artist>> {
    let mut artists = Vec::new();
    let mut seen = HashSet::new();

    for release in releases {
        let Some(release_db_id) = release.db_id.clone() else {
            continue;
        };
        let release_artists = db::artists::get(db, DbId::from(release_db_id))?;
        for artist in release_artists {
            let Some(artist_db_id) = artist.db_id.clone() else {
                artists.push(artist);
                continue;
            };
            if seen.insert(DbId::from(artist_db_id)) {
                artists.push(artist);
            }
        }
    }

    Ok(artists)
}

fn build_entry_details(
    db: &DbAny,
    entry: Entry,
    include: EntryIncludes,
) -> anyhow::Result<EntryDetails> {
    let entry_db_id = entry
        .db_id
        .ok_or_else(|| anyhow::anyhow!("entry missing db id"))?;
    let need_tracks = include.tracks || include.releases || include.artists;
    let tracks = if need_tracks {
        db::tracks::get_by_entry(db, entry_db_id)?
    } else {
        Vec::new()
    };
    let releases = if include.releases || include.artists {
        collect_releases(db, &tracks)?
    } else {
        Vec::new()
    };
    let artists = if include.artists {
        Some(collect_artists(db, &releases)?)
    } else {
        None
    };

    Ok(EntryDetails {
        entry,
        tracks: include.tracks.then_some(tracks),
        releases: include.releases.then_some(releases),
        artists,
    })
}

pub(crate) fn list_details(
    db: &DbAny,
    include: EntryIncludes,
) -> Result<Vec<EntryDetails>, EntryServiceError> {
    let entries = db::entries::get(db, "libraries")?;
    let mut details = Vec::with_capacity(entries.len());
    for entry in entries {
        details.push(build_entry_details(db, entry, include)?);
    }
    Ok(details)
}

pub(crate) fn get_details(
    db: &DbAny,
    id: &str,
    include: EntryIncludes,
) -> Result<EntryDetails, EntryServiceError> {
    let entry_db_id = db::lookup::find_node_id_by_id(db, id)?
        .ok_or_else(|| EntryServiceError::NotFound(id.to_string()))?;
    let entry = db::entries::get_by_id(db, entry_db_id)?
        .ok_or_else(|| EntryServiceError::NotFound(format!("Entry not found: {id}")))?;

    Ok(build_entry_details(db, entry, include)?)
}
