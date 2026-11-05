// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::{
    DbAny,
    DbId,
};

use super::{
    CatalogError,
    Direction,
    PersonalData,
    SortSpec,
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
    scoped_releases,
    totals_column,
};
use lyra_db::genres::Genre;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GenreKey {
    Name,
    /// The visible releases in the genre.
    ReleaseCount,
    /// The tracks on the visible releases in the genre.
    TrackCount,
    TotalDuration,
    ListenCount,
    LastPlayedAt,
    Relevance,
    Random,
    Id,
}

impl SortKey for GenreKey {
    const KEYS: &'static [(&'static str, Self)] = &[
        ("name", Self::Name),
        ("release_count", Self::ReleaseCount),
        ("track_count", Self::TrackCount),
        ("total_duration", Self::TotalDuration),
        ("listen_count", Self::ListenCount),
        ("last_played_at", Self::LastPlayedAt),
        ("relevance", Self::Relevance),
        ("random", Self::Random),
        ("id", Self::Id),
    ];
    const RELEVANCE: Self = Self::Relevance;
    const NAME_ORDER: Self = Self::Name;

    fn ranked(self) -> bool {
        matches!(self, Self::Name)
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
            _ => None,
        }
    }
}

/// Genres are reached through the releases in them.
#[derive(Clone, Debug, Default)]
pub(crate) struct GenreFilter {
    /// Keeps only available entities (`true`) or only unavailable ones (`false`), narrowing
    /// what the query counts as a scope does.
    pub(crate) available: Option<bool>,
    pub(crate) ids: Option<Vec<DbId>>,
    pub(crate) exclude_ids: Vec<DbId>,
    pub(crate) library: Option<DbId>,
    /// Genres of these releases.
    pub(crate) releases: Option<Vec<DbId>>,
}

pub(crate) struct Genres;

impl Catalog for Genres {
    type Key = GenreKey;
    type Filter = GenreFilter;
    type Item = super::Listed<Genre>;

    fn available(filter: &GenreFilter) -> Option<bool> {
        filter.available
    }

    fn available_mut(filter: &mut GenreFilter) -> &mut Option<bool> {
        &mut filter.available
    }

    fn id_filter(ids: Vec<DbId>) -> GenreFilter {
        GenreFilter {
            ids: Some(ids),
            ..GenreFilter::default()
        }
    }

    fn default_sort(_filter: &GenreFilter) -> SortSpec<GenreKey> {
        vec![(GenreKey::Name, Direction::Ascending)]
    }

    fn table(index: &Snapshot) -> &Table {
        &index.genres
    }

    fn scoped(scope: &super::index::Scope) -> &Bits {
        &scope.genres
    }

    fn candidates(cx: &Context<'_>, filter: &GenreFilter) -> Result<Bits, CatalogError> {
        let index = &cx.index;
        let mut genres = if filter.library.is_none() && filter.releases.is_none() {
            cx.scope.genres.clone()
        } else {
            cx.links()
                .genres_of_releases(&scoped_genre_releases(cx, filter))
        };
        if let Some(ids) = &filter.ids {
            genres.and(&index.genres.slots(ids));
        }
        genres.and_not(&index.genres.slots(&filter.exclude_ids));
        Ok(genres)
    }

    fn column<'a>(
        cx: &'a Context<'_>,
        filter: &GenreFilter,
        key: GenreKey,
        slots: &[u32],
    ) -> Result<Column<'a>, CatalogError> {
        let index = &*cx.index;
        let genres = &index.genres;
        if key == GenreKey::Name {
            return Ok(Column::stored(&genres.cache, key, || {
                genres
                    .name_rank
                    .iter()
                    .map(|rank| u64::from(*rank))
                    .collect()
            }));
        }
        let links = cx.links();
        let scoped = scoped_genre_releases(cx, filter);
        let releases_of = |genre: u32| {
            links
                .releases_of_genre(genre)
                .filter(|release| scoped.contains(*release))
        };
        let tracks_of = |genre: u32| {
            let mut tracks = releases_of(genre)
                .flat_map(|release| links.tracks_of_release(release))
                .collect::<Vec<_>>();
            tracks.sort_unstable();
            tracks.dedup();
            tracks
        };
        let totals = |listens, value: fn(&super::TrackTotals) -> Option<u64>| {
            totals_column(cx, genres, slots, listens, tracks_of, value)
        };
        Ok(match key {
            GenreKey::ReleaseCount => Column::computed(genres.len(), slots, |genre| {
                Some(releases_of(genre).count() as u64)
            }),
            GenreKey::TrackCount => totals(false, |totals| Some(totals.track_count))?,
            GenreKey::TotalDuration => totals(false, |totals| Some(totals.total_duration))?,
            GenreKey::ListenCount => totals(true, |totals| Some(totals.listen_count))?,
            GenreKey::LastPlayedAt => totals(true, |totals| totals.last_played_at)?,
            GenreKey::Name | GenreKey::Relevance | GenreKey::Random | GenreKey::Id => {
                unreachable!("{key:?} is not a computed field key")
            }
        })
    }

    fn hydrate(db: &DbAny, ids: &[DbId]) -> anyhow::Result<Vec<super::Listed<Genre>>> {
        let genres = lyra_db::graph::bulk_fetch_typed(db, ids.to_vec(), "Genre")?;
        super::listed(db, ids, genres, super::index::Snapshot::genre_available)
    }
}

/// The release slots the viewer can see that the filter scopes genres to.
fn scoped_genre_releases(cx: &Context<'_>, filter: &GenreFilter) -> Bits {
    scoped_releases(cx, filter.library, filter.releases.as_deref(), &[])
        .unwrap_or_else(|| Bits::full(cx.index.releases.len()))
}
