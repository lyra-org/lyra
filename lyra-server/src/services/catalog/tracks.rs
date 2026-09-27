// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::{
    HashMap,
    HashSet,
};

use agdb::{
    DbAny,
    DbId,
};

use super::{
    ArtistCredit,
    Candidates,
    CatalogError,
    CreditRole,
    Direction,
    SortSpec,
    Viewer,
    credited_owners,
    pipeline::{
        Catalog,
        KeyKind,
        Row,
        SortKey,
        SortValue,
    },
    tracks_of_releases,
};
use crate::db::{
    self,
    Track,
    favorites::FavoriteKind,
    ratings::RatingFilter,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TrackKey {
    Name,
    SortName,
    DateCreated,
    Year,
    Duration,
    Disc,
    Track,
    /// The title of the one release the query is scoped to.
    ReleaseTitle,
    /// The first artist credited on the track.
    ArtistName,
    /// The first artist credited on the track's release.
    ReleaseArtistName,
    ListenCount,
    LastPlayedAt,
    Relevance,
    Random,
    Id,
}

impl SortKey for TrackKey {
    const KEYS: &'static [(&'static str, Self)] = &[
        ("name", Self::Name),
        ("sort_name", Self::SortName),
        ("date_created", Self::DateCreated),
        ("year", Self::Year),
        ("duration", Self::Duration),
        ("disc", Self::Disc),
        ("track", Self::Track),
        ("release_title", Self::ReleaseTitle),
        ("artist_name", Self::ArtistName),
        ("release_artist_name", Self::ReleaseArtistName),
        ("listen_count", Self::ListenCount),
        ("last_played_at", Self::LastPlayedAt),
        ("relevance", Self::Relevance),
        ("random", Self::Random),
        ("id", Self::Id),
    ];
    const RELEVANCE: Self = Self::Relevance;

    fn kind(self) -> KeyKind {
        match self {
            Self::Relevance => KeyKind::Relevance,
            Self::Random => KeyKind::Random,
            Self::Id => KeyKind::Id,
            _ => KeyKind::Field,
        }
    }

    fn missing_first(self) -> bool {
        matches!(self, Self::Disc | Self::Track)
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct TrackFilter {
    pub(crate) ids: Option<Vec<DbId>>,
    pub(crate) exclude_ids: Vec<DbId>,
    pub(crate) library: Option<DbId>,
    pub(crate) releases: Option<Vec<DbId>>,
    pub(crate) artists: Option<ArtistCredit>,
    /// Tracks on a release in any of these genres.
    pub(crate) genres: Vec<DbId>,
    pub(crate) years: Vec<u32>,
    pub(crate) favorite: Option<bool>,
    pub(crate) listened: Option<bool>,
    pub(crate) rating: RatingFilter,
}

impl TrackFilter {
    fn single_release(&self) -> Option<DbId> {
        match self.releases.as_deref() {
            Some([release]) => Some(*release),
            _ => None,
        }
    }
}

pub(crate) struct Tracks;

impl Catalog for Tracks {
    type Key = TrackKey;
    type Filter = TrackFilter;
    type Item = Track;

    fn default_sort(filter: &TrackFilter) -> SortSpec<TrackKey> {
        if filter.single_release().is_some() {
            vec![
                (TrackKey::Disc, Direction::Ascending),
                (TrackKey::Track, Direction::Ascending),
                (TrackKey::SortName, Direction::Ascending),
            ]
        } else {
            vec![(TrackKey::SortName, Direction::Ascending)]
        }
    }

    fn check_sort(filter: &TrackFilter, sort: &SortSpec<TrackKey>) -> Result<(), CatalogError> {
        if filter.single_release().is_none()
            && sort.iter().any(|(key, _)| *key == TrackKey::ReleaseTitle)
        {
            return Err(CatalogError::Invalid(
                "the release_title sort key needs a query scoped to one release".to_string(),
            ));
        }
        Ok(())
    }

    fn candidates(
        db: &DbAny,
        viewer: &Viewer,
        filter: &TrackFilter,
    ) -> Result<Vec<DbId>, CatalogError> {
        let mut releases = Candidates::default();
        if let Some(visible) = viewer.visible_releases(db)? {
            releases.restrict(visible);
        }
        if let Some(library) = filter.library {
            let mut library_releases = Vec::new();
            for library in db::graph::existing_ids(db, &[library], "Library")? {
                library_releases.extend(db::graph::neighbor_ids(db, library, "Release")?);
            }
            releases.restrict(library_releases);
        }
        if let Some(ids) = &filter.releases {
            releases.restrict(db::graph::existing_ids(db, ids, "Release")?);
        }
        if !filter.genres.is_empty() {
            let genres = db::graph::existing_ids(db, &filter.genres, "Genre")?;
            releases.restrict(db::genres::release_ids_matching_genre_ids(db, &genres)?);
        }

        let mut tracks = Candidates::default();
        if let Some(releases) = releases.ids() {
            tracks.restrict(tracks_of_releases(db, releases.iter().copied())?);
        }
        if let Some(ids) = &filter.ids {
            tracks.restrict(db::graph::existing_ids(db, ids, "Track")?);
        }
        if let Some(credit) = &filter.artists {
            tracks.restrict(credit.matching(db, |artists, role| match role {
                CreditRole::Release => {
                    tracks_of_releases(db, credited_owners::<db::Release>(db, artists)?)
                }
                _ => credited_owners::<Track>(db, artists),
            })?);
        }
        if !filter.years.is_empty() {
            let mut matching = HashSet::new();
            for year in &filter.years {
                matching.extend(db::tracks::ids_with_year(db, *year)?);
            }
            tracks.restrict(matching);
        }
        if let Some(favorite) = filter.favorite {
            let user = viewer.user_db_id("the favorite filter")?;
            let favorites = db::favorites::target_ids(db, user, FavoriteKind::Track)?;
            tracks.restrict_membership(favorites, favorite, || all_tracks(db))?;
        }
        if let Some(listened) = filter.listened {
            let user = viewer.user_db_id("the listened filter")?;
            let listened_ids = db::listens::listened_track_ids(db, user)?;
            tracks.restrict_membership(listened_ids, listened, || all_tracks(db))?;
        }
        if !filter.rating.is_empty() {
            let user = viewer.user_db_id("the rating filter")?;
            tracks.restrict(db::ratings::target_ids_matching(db, user, filter.rating)?);
        }

        Ok(tracks.resolve(&filter.exclude_ids, || all_tracks(db))?)
    }

    fn rows(
        db: &DbAny,
        viewer: &Viewer,
        filter: &TrackFilter,
        ids: Vec<DbId>,
        keys: &[TrackKey],
    ) -> Result<Vec<Row>, CatalogError> {
        let fields = db::graph::select_fields(db, &ids, &track_fields(keys))?;
        let listens = if keys
            .iter()
            .any(|key| matches!(key, TrackKey::ListenCount | TrackKey::LastPlayedAt))
        {
            let user = viewer.user_db_id("listen sort keys")?;
            db::listens::get_stats(db, &ids, user)?
                .into_iter()
                .map(|stats| (stats.db_id, stats))
                .collect()
        } else {
            HashMap::new()
        };
        let artist_names = if keys.contains(&TrackKey::ArtistName) {
            db::artists::first_credited_names(db, &ids)?
        } else {
            HashMap::new()
        };
        let release_artist_names = if keys.contains(&TrackKey::ReleaseArtistName) {
            release_artist_names(db, &ids)?
        } else {
            HashMap::new()
        };
        let release_title = match filter.single_release() {
            Some(release) if keys.contains(&TrackKey::ReleaseTitle) => {
                db::releases::get_by_id(db, release)?
                    .map(|release| release.release_title.to_lowercase())
            }
            _ => None,
        };

        Ok(ids
            .into_iter()
            .filter_map(|id| {
                let track = fields.get(&id)?;
                let name = track.text("track_title")?;
                let sort_name = track.text("sort_title").unwrap_or(name).to_lowercase();
                let number = |key| track.number(key).map(SortValue::Number);
                let values = keys
                    .iter()
                    .map(|key| match key {
                        TrackKey::Name => Some(SortValue::Text(name.to_lowercase())),
                        TrackKey::SortName => Some(SortValue::Text(sort_name.clone())),
                        TrackKey::DateCreated => number("ctime").or_else(|| number("created_at")),
                        TrackKey::Year => number("year"),
                        TrackKey::Duration => number("duration_ms"),
                        TrackKey::Disc => number("disc"),
                        TrackKey::Track => number("track"),
                        TrackKey::ReleaseTitle => release_title.clone().map(SortValue::Text),
                        TrackKey::ArtistName => artist_names
                            .get(&id)
                            .map(|name| SortValue::Text(name.to_lowercase())),
                        TrackKey::ReleaseArtistName => release_artist_names
                            .get(&id)
                            .map(|name| SortValue::Text(name.to_lowercase())),
                        TrackKey::ListenCount => Some(SortValue::Number(
                            listens.get(&id).map_or(0, |stats| stats.count),
                        )),
                        TrackKey::LastPlayedAt => listens
                            .get(&id)
                            .and_then(|stats| stats.last_played)
                            .map(SortValue::Number),
                        TrackKey::Relevance | TrackKey::Random | TrackKey::Id => None,
                    })
                    .collect();
                Some(Row {
                    id,
                    name: name.to_string(),
                    sort_name,
                    values,
                })
            })
            .collect())
    }

    fn hydrate(db: &DbAny, ids: &[DbId]) -> anyhow::Result<Vec<Track>> {
        let mut tracks = db::tracks::get_by_ids(db, ids)?;
        Ok(ids.iter().filter_map(|id| tracks.remove(id)).collect())
    }
}

/// The stored track fields that the names and `keys` read.
fn track_fields(keys: &[TrackKey]) -> Vec<&'static str> {
    let mut fields = vec!["track_title", "sort_title"];
    for key in keys {
        fields.extend_from_slice(match key {
            TrackKey::DateCreated => &["ctime", "created_at"],
            TrackKey::Year => &["year"],
            TrackKey::Duration => &["duration_ms"],
            TrackKey::Disc => &["disc"],
            TrackKey::Track => &["track"],
            _ => &[],
        });
    }
    fields
}

/// The name each track's release credits first; the least such name when a track is on several.
fn release_artist_names(db: &DbAny, tracks: &[DbId]) -> anyhow::Result<HashMap<DbId, String>> {
    let mut releases_by_track = HashMap::new();
    for track in tracks {
        releases_by_track.insert(
            *track,
            db::graph::inbound_neighbor_ids(db, *track, "Release")?,
        );
    }
    let release_ids = releases_by_track
        .values()
        .flatten()
        .copied()
        .collect::<Vec<_>>();
    let names = db::artists::first_credited_names(db, &release_ids)?;
    Ok(releases_by_track
        .into_iter()
        .filter_map(|(track, releases)| {
            let name = releases
                .iter()
                .filter_map(|release| names.get(release))
                .min_by_key(|name| name.to_lowercase())?;
            Some((track, name.clone()))
        })
        .collect())
}

fn all_tracks(db: &DbAny) -> anyhow::Result<Vec<DbId>> {
    db::graph::neighbor_ids(db, "tracks", "Track")
}
