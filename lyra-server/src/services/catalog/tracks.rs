// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashMap;

use agdb::{
    DbAny,
    DbId,
};

use super::{
    ArtistCredit,
    CatalogError,
    CreditRole,
    Direction,
    PersonalData,
    PersonalFilter,
    SortSpec,
    favorited_at_column,
    index::{
        Snapshot,
        Table,
        bits::Bits,
    },
    pipeline::{
        Catalog,
        Column,
        Context,
        KeyKind,
        ShuffleUnit,
        SortKey,
    },
    ratings_column,
    scoped_releases,
};
use crate::services::search::Word;
use lyra_db::{
    Track,
    favorites::FavoriteKind,
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
    /// The title of the track's release.
    ReleaseTitle,
    /// The first artist credited on the track.
    ArtistName,
    /// The first artist credited on the track's release.
    ReleaseArtistName,
    ListenCount,
    LastPlayedAt,
    /// The viewer's rating.
    Rating,
    /// When the viewer last set their rating.
    RatedAt,
    /// When the viewer first favorited the track.
    FavoritedAt,
    /// When the viewer last set their bookmark of the track.
    BookmarkedAt,
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
        ("rating", Self::Rating),
        ("rated_at", Self::RatedAt),
        ("favorited_at", Self::FavoritedAt),
        ("bookmarked_at", Self::BookmarkedAt),
        ("relevance", Self::Relevance),
        ("random", Self::Random),
        ("id", Self::Id),
    ];
    const RELEVANCE: Self = Self::Relevance;
    const NAME_ORDER: Self = Self::SortName;

    fn ranked(self) -> bool {
        matches!(
            self,
            Self::Name
                | Self::SortName
                | Self::ReleaseTitle
                | Self::ArtistName
                | Self::ReleaseArtistName
        )
    }
    fn kind(self) -> KeyKind {
        match self {
            Self::Relevance => KeyKind::Relevance,
            Self::Random => KeyKind::Random,
            Self::Id => KeyKind::Id,
            _ => KeyKind::Field,
        }
    }

    fn personal(self) -> Option<PersonalData> {
        match self {
            Self::ListenCount | Self::LastPlayedAt => Some(PersonalData::Listens),
            Self::Rating | Self::RatedAt => Some(PersonalData::Ratings),
            Self::FavoritedAt => Some(PersonalData::Favorites),
            Self::BookmarkedAt => Some(PersonalData::Bookmarks),
            _ => None,
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
    /// Leaves out tracks credited to these artists in either role.
    pub(crate) exclude_artists: Vec<DbId>,
    pub(crate) library: Option<DbId>,
    pub(crate) releases: Option<Vec<DbId>>,
    pub(crate) artists: Option<ArtistCredit>,
    /// Tracks on a release in any of these genres.
    pub(crate) genres: Vec<DbId>,
    pub(crate) years: Vec<u32>,
    pub(crate) year_range: YearRange,
    pub(crate) personal: PersonalFilter,
    /// Keeps tracks the viewer bookmarked (`true`) or did not (`false`).
    pub(crate) bookmarked: Option<bool>,
}

/// Inclusive bounds on a track's own year, either of them open. Any bound leaves out tracks
/// without a year.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct YearRange {
    min: Option<u32>,
    max: Option<u32>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("the minimum year exceeds the maximum")]
pub(crate) struct InvertedYearRange;

impl YearRange {
    pub(crate) fn new(min: Option<u32>, max: Option<u32>) -> Result<Self, InvertedYearRange> {
        if let (Some(min), Some(max)) = (min, max)
            && min > max
        {
            return Err(InvertedYearRange);
        }
        Ok(Self { min, max })
    }

    pub(crate) fn is_unbounded(self) -> bool {
        self.min.is_none() && self.max.is_none()
    }

    pub(crate) fn contains(self, year: Option<u64>) -> bool {
        year.is_some_and(|year| {
            self.min.is_none_or(|min| year >= u64::from(min))
                && self.max.is_none_or(|max| year <= u64::from(max))
        })
    }
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

    fn personal(filter: &TrackFilter) -> Vec<PersonalData> {
        let mut reads = filter.personal.reads();
        reads.extend(filter.bookmarked.map(|_| PersonalData::Bookmarks));
        reads
    }

    fn id_filter(ids: Vec<DbId>) -> TrackFilter {
        TrackFilter {
            ids: Some(ids),
            ..TrackFilter::default()
        }
    }

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

    fn table(index: &Snapshot) -> &Table {
        &index.tracks
    }

    fn candidates(cx: &Context<'_>, filter: &TrackFilter) -> Result<Bits, CatalogError> {
        let index = &cx.index;
        let links = cx.links();
        let len = index.tracks.len();
        let releases = scoped_releases(
            cx,
            filter.library,
            filter.releases.as_deref(),
            &filter.genres,
        );
        let mut tracks = match (&filter.ids, &releases) {
            (Some(ids), Some(releases)) => Bits::of(
                len,
                index.tracks.slots(ids).iter().filter(|track| {
                    links
                        .releases_of_track(*track)
                        .any(|release| releases.contains(release))
                }),
            ),
            (Some(ids), None) => index.tracks.slots(ids),
            (None, Some(releases)) => links.tracks_of_releases(releases),
            (None, None) => Bits::full(len),
        };
        let reach = |artists: &Bits, role| match role {
            CreditRole::Release => links.tracks_of_releases(&links.releases_of_artists(artists)),
            _ => links.tracks_of_artists(artists),
        };
        if let Some(credit) = &filter.artists {
            tracks.and(&credit.reach(index, reach));
        }
        if !filter.years.is_empty() {
            let years = filter
                .years
                .iter()
                .map(|year| u64::from(*year))
                .collect::<Vec<_>>();
            tracks = Bits::of(
                len,
                tracks
                    .iter()
                    .filter(|track| years.contains(&index.track.year[*track as usize])),
            );
        }
        if !filter.year_range.is_unbounded() {
            tracks = Bits::of(
                len,
                tracks.iter().filter(|track| {
                    let year = index.track.year[*track as usize];
                    filter
                        .year_range
                        .contains((year != super::index::MISSING).then_some(year))
                }),
            );
        }
        let mut personal = Some(tracks);
        filter.personal.restrict(
            cx,
            &index.tracks,
            &mut personal,
            &Bits::full(len),
            FavoriteKind::Track,
            |listened| index.tracks.slots(listened),
        )?;
        let mut tracks = personal.unwrap_or_else(|| Bits::full(len));
        if let Some(bookmarked) = filter.bookmarked {
            let user = cx.viewer.user_db_id("the bookmarked filter")?;
            let members = index
                .tracks
                .slots(&lyra_db::bookmarks::target_ids(cx.db, user)?);
            if bookmarked {
                tracks.and(&members);
            } else {
                tracks.and_not(&members);
            }
        }

        tracks.and_not(&index.tracks.slots(&filter.exclude_ids));
        if !filter.exclude_artists.is_empty() {
            tracks.and_not(&ArtistCredit::any(filter.exclude_artists.clone()).reach(index, reach));
        }
        Ok(tracks)
    }

    fn column<'a>(
        cx: &'a Context<'_>,
        _filter: &TrackFilter,
        key: TrackKey,
        slots: &[u32],
    ) -> Result<Column<'a>, CatalogError> {
        let index = &*cx.index;
        let links = cx.links();
        let tracks = &index.tracks;
        let stored = |compute: &dyn Fn() -> Vec<u64>| Column::stored(&tracks.cache, key, compute);
        // Values read through other entities depend on which of them the viewer sees.
        let linked = |compute: &dyn Fn() -> Vec<u64>| cx.linked_column(tracks, key, compute);
        // The least of a value over each track's visible releases.
        let least_over_releases = |value: &dyn Fn(u32) -> Option<u64>| {
            (0..tracks.len() as u32)
                .map(|track| {
                    links
                        .releases_of_track(track)
                        .filter_map(value)
                        .min()
                        .unwrap_or(super::index::MISSING)
                })
                .collect()
        };
        let artist_name = |artist: Option<u32>| {
            artist.map(|artist| u64::from(index.artists.name_rank[artist as usize]))
        };
        Ok(match key {
            TrackKey::Name => stored(&|| {
                tracks
                    .name_rank
                    .iter()
                    .map(|rank| u64::from(*rank))
                    .collect()
            }),
            TrackKey::SortName => stored(&|| {
                tracks
                    .sort_name_rank
                    .iter()
                    .map(|rank| u64::from(*rank))
                    .collect()
            }),
            TrackKey::DateCreated => Column::Fields(&index.track.created),
            TrackKey::Year => Column::Fields(&index.track.year),
            TrackKey::Duration => Column::Fields(&index.track.duration),
            TrackKey::Disc => Column::Fields(&index.track.disc),
            TrackKey::Track => Column::Fields(&index.track.track),
            TrackKey::ReleaseTitle => linked(&|| {
                least_over_releases(&|release| {
                    Some(u64::from(index.releases.name_rank[release as usize]))
                })
            }),
            TrackKey::ArtistName => linked(&|| {
                (0..tracks.len() as u32)
                    .map(|track| {
                        artist_name(links.track_lead(track)).unwrap_or(super::index::MISSING)
                    })
                    .collect()
            }),
            TrackKey::ReleaseArtistName => {
                linked(&|| least_over_releases(&|release| artist_name(links.release_lead(release))))
            }
            TrackKey::ListenCount | TrackKey::LastPlayedAt => {
                let user = cx.viewer.user_db_id("listen sort keys")?;
                let ids = slots
                    .iter()
                    .map(|slot| tracks.id(*slot))
                    .collect::<Vec<_>>();
                let listens = lyra_db::listens::get_stats(cx.db, &ids, user)?
                    .into_iter()
                    .filter_map(|stats| Some((tracks.slot(stats.db_id)?, stats)))
                    .collect::<HashMap<_, _>>();
                Column::computed(tracks.len(), slots, |slot| match key {
                    TrackKey::ListenCount => {
                        Some(listens.get(&slot).map_or(0, |stats| stats.count))
                    }
                    _ => listens.get(&slot).and_then(|stats| stats.last_played),
                })
            }
            TrackKey::Rating => ratings_column(cx, tracks, slots, false)?,
            TrackKey::RatedAt => ratings_column(cx, tracks, slots, true)?,
            TrackKey::FavoritedAt => favorited_at_column(cx, tracks, slots, FavoriteKind::Track)?,
            TrackKey::BookmarkedAt => {
                let user = cx.viewer.user_db_id("the bookmarked_at sort key")?;
                let bookmarked = if slots.is_empty() {
                    HashMap::new()
                } else {
                    lyra_db::bookmarks::updated_at(cx.db, user)?
                        .into_iter()
                        .filter_map(|(id, ms)| Some((tracks.slot(id)?, u64::try_from(ms).ok()?)))
                        .collect()
                };
                Column::computed(tracks.len(), slots, |slot| bookmarked.get(&slot).copied())
            }
            TrackKey::Relevance | TrackKey::Random | TrackKey::Id => {
                unreachable!("{key:?} is not a field key")
            }
        })
    }

    /// A track also matches through its artists, its visible releases and their artists.
    fn related(cx: &Context<'_>, word: &Word) -> Option<Bits> {
        let index = &cx.index;
        let links = cx.links();
        let artists = links.artists_seen(index.artists.matching(word));
        let mut releases = links.releases_of_artists(&artists);
        releases.or(&links.releases_seen(index.releases.matching(word)));
        let mut tracks = links.tracks_of_artists(&artists);
        tracks.or(&links.tracks_of_releases(&releases));
        Some(tracks)
    }

    fn hydrate(db: &DbAny, ids: &[DbId]) -> anyhow::Result<Vec<Track>> {
        let mut tracks = lyra_db::tracks::get_by_ids(db, ids)?;
        Ok(ids.iter().filter_map(|id| tracks.remove(id)).collect())
    }

    /// A chain on a release the viewer sees shuffles as one, its tracks in chain order.
    fn shuffle_units(cx: &Context<'_>, ids: &[DbId]) -> anyhow::Result<HashMap<DbId, ShuffleUnit>> {
        let links = cx.links();
        Ok(lyra_db::chains::links_for_tracks(cx.db, ids)?
            .into_iter()
            .filter(|(_, link)| links.sees_release(link.release_db_id))
            .map(|(track, link)| {
                (
                    track,
                    ShuffleUnit {
                        id: link.chain_db_id,
                        place: link.position,
                    },
                )
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_year_range_keeps_dated_tracks_within_its_inclusive_bounds() -> anyhow::Result<()> {
        let range = YearRange::new(Some(1990), Some(1992))?;
        assert!(range.contains(Some(1990)) && range.contains(Some(1992)));
        assert!(!range.contains(Some(1989)) && !range.contains(Some(1993)));
        assert!(!range.contains(None));
        let from = YearRange::new(Some(1990), None)?;
        assert!(from.contains(Some(3000)) && !from.contains(None));
        let to = YearRange::new(None, Some(1990))?;
        assert!(to.contains(Some(0)) && !to.contains(Some(1991)));
        assert!(YearRange::new(None, None)?.is_unbounded());
        assert!(YearRange::new(Some(1991), Some(1991))?.contains(Some(1991)));
        Ok(())
    }

    #[test]
    fn an_inverted_year_range_is_refused() {
        assert_eq!(
            YearRange::new(Some(1993), Some(1992)),
            Err(InvertedYearRange)
        );
    }
}
