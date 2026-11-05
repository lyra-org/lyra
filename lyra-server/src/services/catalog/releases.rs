// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

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
        SortKey,
    },
    ratings_column,
    scoped_releases,
    totals_column,
};
use crate::services::search::Word;
use lyra_db::{
    Release,
    favorites::FavoriteKind,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ReleaseKey {
    Name,
    SortName,
    DateCreated,
    /// A partial date orders before the fuller dates it starts: 2004 < 2004-05 < 2004-05-06.
    ReleaseDate,
    Year,
    TotalDuration,
    TrackCount,
    /// The first artist credited on the release.
    ArtistName,
    ListenCount,
    LastPlayedAt,
    /// The viewer's rating.
    Rating,
    /// When the viewer last set their rating.
    RatedAt,
    /// When the viewer first favorited the release.
    FavoritedAt,
    Relevance,
    Random,
    Id,
}

impl SortKey for ReleaseKey {
    const KEYS: &'static [(&'static str, Self)] = &[
        ("name", Self::Name),
        ("sort_name", Self::SortName),
        ("date_created", Self::DateCreated),
        ("release_date", Self::ReleaseDate),
        ("year", Self::Year),
        ("total_duration", Self::TotalDuration),
        ("track_count", Self::TrackCount),
        ("artist_name", Self::ArtistName),
        ("listen_count", Self::ListenCount),
        ("last_played_at", Self::LastPlayedAt),
        ("rating", Self::Rating),
        ("rated_at", Self::RatedAt),
        ("favorited_at", Self::FavoritedAt),
        ("relevance", Self::Relevance),
        ("random", Self::Random),
        ("id", Self::Id),
    ];
    const RELEVANCE: Self = Self::Relevance;
    const NAME_ORDER: Self = Self::SortName;

    fn ranked(self) -> bool {
        matches!(self, Self::Name | Self::SortName | Self::ArtistName)
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
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ReleaseFilter {
    /// Keeps only available entities (`true`) or only unavailable ones (`false`), narrowing
    /// what the query counts as a scope does.
    pub(crate) available: Option<bool>,
    pub(crate) ids: Option<Vec<DbId>>,
    pub(crate) exclude_ids: Vec<DbId>,
    /// Leaves out releases credited to these artists in either role.
    pub(crate) exclude_artists: Vec<DbId>,
    pub(crate) library: Option<DbId>,
    pub(crate) artists: Option<ArtistCredit>,
    pub(crate) genres: Vec<DbId>,
    pub(crate) years: Vec<u32>,
    /// `listened` keeps releases whose every track the user has listened to.
    pub(crate) personal: PersonalFilter,
}

pub(crate) struct Releases;

impl Catalog for Releases {
    type Key = ReleaseKey;
    type Filter = ReleaseFilter;
    type Item = super::Listed<Release>;

    fn personal(filter: &ReleaseFilter) -> Vec<PersonalData> {
        filter.personal.reads()
    }

    fn available(filter: &ReleaseFilter) -> Option<bool> {
        filter.available
    }

    fn available_mut(filter: &mut ReleaseFilter) -> &mut Option<bool> {
        &mut filter.available
    }

    fn id_filter(ids: Vec<DbId>) -> ReleaseFilter {
        ReleaseFilter {
            ids: Some(ids),
            ..ReleaseFilter::default()
        }
    }

    fn default_sort(_filter: &ReleaseFilter) -> SortSpec<ReleaseKey> {
        vec![(ReleaseKey::SortName, Direction::Ascending)]
    }

    fn table(index: &Snapshot) -> &Table {
        &index.releases
    }

    fn scoped(scope: &super::index::Scope) -> &Bits {
        &scope.releases
    }

    fn candidates(cx: &Context<'_>, filter: &ReleaseFilter) -> Result<Bits, CatalogError> {
        let index = &cx.index;
        let links = cx.links();
        let len = index.releases.len();
        let mut releases =
            scoped_releases(cx, filter.library, filter.ids.as_deref(), &filter.genres)
                .unwrap_or_else(|| Bits::full(len));
        let reach = |artists: &Bits, role| match role {
            CreditRole::Track => links.releases_of_tracks(&links.tracks_of_artists(artists)),
            _ => links.releases_of_artists(artists),
        };
        if let Some(credit) = &filter.artists {
            releases.and(&credit.reach(index, reach));
        }
        if !filter.years.is_empty() {
            let years = filter
                .years
                .iter()
                .map(|year| u64::from(*year))
                .collect::<Vec<_>>();
            releases = Bits::of(
                len,
                releases
                    .iter()
                    .filter(|release| years.contains(&index.release.year[*release as usize])),
            );
        }
        let mut personal = Some(releases);
        filter.personal.restrict(
            cx,
            &index.releases,
            &mut personal,
            &Bits::full(len),
            FavoriteKind::Release,
            |listened| {
                let tracks = index.tracks.slots(listened);
                Bits::of(
                    len,
                    links.releases_of_tracks(&tracks).iter().filter(|release| {
                        links
                            .tracks_of_release(*release)
                            .all(|track| tracks.contains(track))
                    }),
                )
            },
        )?;
        let mut releases = personal.unwrap_or_else(|| Bits::full(len));

        releases.and_not(&index.releases.slots(&filter.exclude_ids));
        if !filter.exclude_artists.is_empty() {
            releases
                .and_not(&ArtistCredit::any(filter.exclude_artists.clone()).reach(index, reach));
        }
        Ok(releases)
    }

    fn column<'a>(
        cx: &'a Context<'_>,
        _filter: &ReleaseFilter,
        key: ReleaseKey,
        slots: &[u32],
    ) -> Result<Column<'a>, CatalogError> {
        let index = &*cx.index;
        let links = cx.links();
        let releases = &index.releases;
        let stored = |compute: &dyn Fn() -> Vec<u64>| Column::stored(&releases.cache, key, compute);
        // Values read through other entities depend on which of them the viewer sees.
        let linked = |compute: &dyn Fn() -> Vec<u64>| cx.linked_column(releases, key, compute);
        let tracks_of = |release: u32| links.tracks_of_release(release).collect::<Vec<_>>();
        Ok(match key {
            ReleaseKey::Name => stored(&|| {
                releases
                    .name_rank
                    .iter()
                    .map(|rank| u64::from(*rank))
                    .collect()
            }),
            ReleaseKey::SortName => stored(&|| {
                releases
                    .sort_name_rank
                    .iter()
                    .map(|rank| u64::from(*rank))
                    .collect()
            }),
            ReleaseKey::DateCreated => Column::Fields(&index.release.created),
            ReleaseKey::ReleaseDate => Column::Fields(&index.release.date),
            ReleaseKey::Year => Column::Fields(&index.release.year),
            // Narrowed to the available tracks or the unavailable ones, a release holds fewer.
            ReleaseKey::TotalDuration if cx.scope.available.is_some() => linked(&|| {
                (0..releases.len() as u32)
                    .map(|release| {
                        tracks_of(release)
                            .into_iter()
                            .map(|track| index.track.duration[track as usize])
                            .filter(|duration| *duration != super::index::MISSING)
                            .fold(0u64, u64::saturating_add)
                    })
                    .collect()
            }),
            ReleaseKey::TrackCount if cx.scope.available.is_some() => linked(&|| {
                (0..releases.len() as u32)
                    .map(|release| tracks_of(release).len() as u64)
                    .collect()
            }),
            ReleaseKey::TotalDuration => Column::Fields(&index.release.total_duration),
            ReleaseKey::TrackCount => Column::Fields(&index.release.track_count),
            ReleaseKey::ArtistName => linked(&|| {
                (0..releases.len() as u32)
                    .map(|release| {
                        links
                            .release_lead(release)
                            .map_or(super::index::MISSING, |artist| {
                                u64::from(index.artists.name_rank[artist as usize])
                            })
                    })
                    .collect()
            }),
            ReleaseKey::ListenCount => {
                totals_column(cx, releases, slots, true, tracks_of, |totals| {
                    Some(totals.listen_count)
                })?
            }
            ReleaseKey::LastPlayedAt => {
                totals_column(cx, releases, slots, true, tracks_of, |totals| {
                    totals.last_played_at
                })?
            }
            ReleaseKey::Rating => ratings_column(cx, releases, slots, false)?,
            ReleaseKey::RatedAt => ratings_column(cx, releases, slots, true)?,
            ReleaseKey::FavoritedAt => {
                favorited_at_column(cx, releases, slots, FavoriteKind::Release)?
            }
            ReleaseKey::Relevance | ReleaseKey::Random | ReleaseKey::Id => {
                unreachable!("{key:?} is not a field key")
            }
        })
    }

    /// A release also matches through its artists.
    fn related(cx: &Context<'_>, word: &Word) -> Option<Bits> {
        let links = cx.links();
        Some(links.releases_of_artists(&links.artists_seen(cx.index.artists.matching(word))))
    }

    fn hydrate(db: &DbAny, ids: &[DbId]) -> anyhow::Result<Vec<super::Listed<Release>>> {
        let releases = lyra_db::graph::bulk_fetch_typed(db, ids.to_vec(), "Release")?;
        super::listed(db, ids, releases, super::index::Snapshot::release_available)
    }
}

/// `YYYY`, `YYYY-MM` or `YYYY-MM-DD` as `YYYYMMDD`, with missing parts as zero.
pub(crate) fn date_order(date: &str) -> Option<u64> {
    let mut parts = date.splitn(3, '-').map(str::parse::<u64>);
    let year = parts.next()?.ok()?;
    let month = parts.next().transpose().ok()?.unwrap_or(0);
    let day = parts.next().transpose().ok()?.unwrap_or(0);
    Some(year * 10_000 + month * 100 + day)
}
