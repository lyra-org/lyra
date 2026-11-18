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
        Match,
        SortKey,
        nested_in,
    },
    releases::Releases,
    totals_column,
    tracks::Tracks,
};
use lyra_db::genres::Genre;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GenreKey {
    Name,
    DateCreated,
    /// The visible releases in the genre that the filter's library and nested queries keep.
    ReleaseCount,
    /// The tracks on those releases, only those `tracks` matches when it is set.
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
        ("date_created", Self::DateCreated),
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

/// Genres are reached through the releases in them. `library`, `releases` and `tracks` narrow
/// those releases, together, and the counts and totals count only within them.
#[derive(Clone, Debug, Default)]
pub(crate) struct GenreFilter {
    /// Keeps only available entities (`true`) or only unavailable ones (`false`), narrowing
    /// what the query counts as a scope does.
    pub(crate) available: Option<bool>,
    pub(crate) ids: Option<Vec<DbId>>,
    pub(crate) exclude_ids: Vec<DbId>,
    pub(crate) library: Option<DbId>,
    /// Genres of the releases this matches.
    pub(crate) releases: Option<Match<Releases>>,
    /// Genres of the releases the tracks this matches are kept through: their visible releases
    /// within its own `library`, `releases` and `genres`. Only these tracks count as the genres'
    /// tracks.
    pub(crate) tracks: Option<Match<Tracks>>,
}

/// What a genre filter's release scope and nested queries keep.
#[derive(Default)]
pub(crate) struct GenreScope {
    /// The releases that count, or `None` for every release the viewer sees.
    releases: Option<Bits>,
    /// The tracks that count, or `None` for every track on the releases that count.
    tracks: Option<Bits>,
}

impl GenreScope {
    fn counts_release(&self, release: u32) -> bool {
        self.releases
            .as_ref()
            .is_none_or(|releases| releases.contains(release))
    }

    fn counts_track(&self, track: u32) -> bool {
        self.tracks
            .as_ref()
            .is_none_or(|tracks| tracks.contains(track))
    }
}

pub(crate) struct Genres;

impl Catalog for Genres {
    type Key = GenreKey;
    type Filter = GenreFilter;
    type Item = super::Listed<Genre>;
    type Prepared = GenreScope;

    fn prepare(cx: &Context<'_>, filter: &GenreFilter) -> Result<GenreScope, CatalogError> {
        let links = cx.links();
        let mut releases = filter
            .library
            .map(|library| links.releases_of_library(library));
        let mut restrict = |bits: Bits| match &mut releases {
            Some(releases) => releases.and(&bits),
            None => releases = Some(bits),
        };
        if let Some(matching) = &filter.releases {
            restrict(nested_in(cx, matching)?);
        }
        let mut tracks = None;
        if let Some(matching) = &filter.tracks {
            let matched = nested_in(cx, matching)?;
            // A genre filter on the tracks keeps a track only through these releases.
            let mut reached = links.releases_of_tracks(&matched);
            if let Some(kept) = matching.filter.releases_kept(cx) {
                reached.and(&kept);
            }
            restrict(reached);
            tracks = Some(matched);
        }
        Ok(GenreScope { releases, tracks })
    }

    fn personal(filter: &GenreFilter) -> Vec<PersonalData> {
        let mut reads = filter
            .releases
            .as_ref()
            .map(Match::personal)
            .unwrap_or_default();
        reads.extend(filter.tracks.iter().flat_map(Match::personal));
        reads
    }

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

    fn candidates(
        cx: &Context<'_>,
        filter: &GenreFilter,
        scope: &GenreScope,
    ) -> Result<Bits, CatalogError> {
        let index = &cx.index;
        let mut genres = match &scope.releases {
            Some(releases) => cx.links().genres_of_releases(releases),
            None => cx.scope.genres.clone(),
        };
        if let Some(ids) = &filter.ids {
            genres.and(&index.genres.slots(ids));
        }
        genres.and_not(&index.genres.slots(&filter.exclude_ids));
        Ok(genres)
    }

    fn column<'a>(
        cx: &'a Context<'_>,
        _filter: &GenreFilter,
        scope: &GenreScope,
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
        if key == GenreKey::DateCreated {
            return Ok(Column::Fields(&index.genre.created));
        }
        let links = cx.links();
        let releases_of = |genre: u32| {
            links
                .releases_of_genre(genre)
                .filter(|release| scope.counts_release(*release))
        };
        let tracks_of = |genre: u32| {
            let mut tracks = releases_of(genre)
                .flat_map(|release| links.tracks_of_release(release))
                .filter(|track| scope.counts_track(*track))
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
            GenreKey::Name
            | GenreKey::DateCreated
            | GenreKey::Relevance
            | GenreKey::Random
            | GenreKey::Id => {
                unreachable!("{key:?} is not a computed field key")
            }
        })
    }

    fn hydrate(db: &DbAny, ids: &[DbId]) -> anyhow::Result<Vec<super::Listed<Genre>>> {
        let genres = lyra_db::graph::bulk_fetch_typed(db, ids.to_vec(), "Genre")?;
        super::listed(db, ids, genres, super::index::Snapshot::genre_available)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::services::{
        auth::Principal,
        catalog::{
            PersonalFilter,
            Viewer,
            numbers,
            releases::ReleaseFilter,
            tracks::TrackFilter,
        },
    };
    use lyra_db::{
        favorites::FavoriteKind,
        fixtures::{
            insert_release,
            insert_track,
            link_track,
        },
        test_db::{
            insert_user,
            new_test_db,
        },
    };

    #[test]
    fn a_track_match_counts_only_the_tracks_it_keeps() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = insert_user(&mut db, "listener")?;
        let album = insert_release(&mut db, "Album")?;
        lyra_db::genres::sync_release_genres(&mut db, album, &["Rock".to_string()])?;
        let rock = lyra_db::genres::find_by_name(&db, "Rock")?.expect("the genre exists");
        let mut tracks = Vec::new();
        for number in 0..10 {
            let track = insert_track(&mut db, &format!("Track {number}"))?;
            link_track(&mut db, album, track)?;
            tracks.push(track);
        }
        lyra_db::favorites::add(&mut db, user, tracks[3], FavoriteKind::Track, 1)?;
        let viewer = Viewer::user(
            &db,
            Principal::for_user(&db, user, vec![lyra_db::Permission::Admin], HashSet::new()),
        )?;
        let counts = |filter: GenreFilter| {
            numbers::<Genres>(
                &db,
                &viewer,
                &filter,
                vec![rock],
                &[GenreKey::ReleaseCount, GenreKey::TrackCount],
            )
        };
        let favorites = GenreFilter {
            tracks: Some(Match::new(TrackFilter {
                personal: PersonalFilter {
                    favorite: Some(true),
                    ..PersonalFilter::default()
                },
                ..TrackFilter::default()
            })),
            ..GenreFilter::default()
        };
        assert_eq!(counts(favorites)?[&rock], [Some(1), Some(1)]);
        assert_eq!(counts(GenreFilter::default())?[&rock], [Some(1), Some(10)]);
        let others = GenreFilter {
            tracks: Some(Match::new(TrackFilter {
                ids: Some(vec![tracks[0], tracks[1]]),
                ..TrackFilter::default()
            })),
            releases: Some(Match::new(ReleaseFilter {
                ids: Some(vec![album]),
                ..ReleaseFilter::default()
            })),
            ..GenreFilter::default()
        };
        assert_eq!(counts(others)?[&rock], [Some(1), Some(2)]);
        Ok(())
    }
}
