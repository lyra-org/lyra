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
    Candidates,
    CatalogError,
    Direction,
    SortSpec,
    Viewer,
    all_releases,
    pipeline::{
        Catalog,
        KeyKind,
        Row,
        SortKey,
        SortValue,
    },
    scoped_releases,
    track_totals,
};
use crate::db::{
    self,
    genres::Genre,
};

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

    fn kind(self) -> KeyKind {
        match self {
            Self::Relevance => KeyKind::Relevance,
            Self::Random => KeyKind::Random,
            Self::Id => KeyKind::Id,
            _ => KeyKind::Field,
        }
    }
}

/// Genres are reached through the releases in them.
#[derive(Clone, Debug, Default)]
pub(crate) struct GenreFilter {
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
    type Item = Genre;

    fn default_sort(_filter: &GenreFilter) -> SortSpec<GenreKey> {
        vec![(GenreKey::Name, Direction::Ascending)]
    }

    fn candidates(
        db: &DbAny,
        viewer: &Viewer,
        filter: &GenreFilter,
    ) -> Result<Vec<DbId>, CatalogError> {
        let releases = scoped_genre_releases(db, viewer, filter)?;
        let mut genres = Candidates::default();
        genres.restrict(
            db::genres::get_for_releases_many(db, &releases)?
                .into_values()
                .flatten()
                .filter_map(|genre| genre.db_id.map(DbId::from)),
        );
        if let Some(ids) = &filter.ids {
            genres.restrict(db::graph::existing_ids(db, ids, "Genre")?);
        }
        Ok(genres.resolve(&filter.exclude_ids, || Ok(Vec::new()))?)
    }

    fn rows(
        db: &DbAny,
        viewer: &Viewer,
        filter: &GenreFilter,
        ids: Vec<DbId>,
        keys: &[GenreKey],
    ) -> Result<Vec<Row>, CatalogError> {
        let fields = db::graph::select_fields(db, &ids, &["name"])?;
        let needs = |wanted: &[GenreKey]| keys.iter().any(|key| wanted.contains(key));
        let mut release_counts = HashMap::new();
        let mut tracks_by_genre = HashMap::new();
        if needs(&[
            GenreKey::ReleaseCount,
            GenreKey::TrackCount,
            GenreKey::TotalDuration,
            GenreKey::ListenCount,
            GenreKey::LastPlayedAt,
        ]) {
            let visible = scoped_genre_releases(db, viewer, filter)?
                .into_iter()
                .collect::<HashSet<_>>();
            for (genre, releases) in db::genres::get_releases_many(db, &ids)? {
                let releases = releases
                    .into_iter()
                    .filter(|release| visible.contains(release))
                    .collect::<Vec<_>>();
                release_counts.insert(genre, releases.len() as u64);
                tracks_by_genre.insert(genre, super::tracks_of_releases(db, releases)?);
            }
        }
        let totals = track_totals(
            db,
            viewer,
            &tracks_by_genre,
            needs(&[GenreKey::TotalDuration]),
            needs(&[GenreKey::ListenCount, GenreKey::LastPlayedAt]),
        )?;

        Ok(ids
            .into_iter()
            .filter_map(|id| {
                let name = fields.get(&id)?.text("name")?;
                let totals = totals.get(&id).copied().unwrap_or_default();
                let values = keys
                    .iter()
                    .map(|key| match key {
                        GenreKey::Name => Some(SortValue::Text(name.to_lowercase())),
                        GenreKey::ReleaseCount => Some(SortValue::Number(
                            release_counts.get(&id).copied().unwrap_or(0),
                        )),
                        GenreKey::TrackCount => Some(SortValue::Number(totals.track_count)),
                        GenreKey::TotalDuration => Some(SortValue::Number(totals.total_duration)),
                        GenreKey::ListenCount => Some(SortValue::Number(totals.listen_count)),
                        GenreKey::LastPlayedAt => totals.last_played_at.map(SortValue::Number),
                        GenreKey::Relevance | GenreKey::Random | GenreKey::Id => None,
                    })
                    .collect();
                Some(Row {
                    id,
                    name: name.to_string(),
                    sort_name: name.to_lowercase(),
                    values,
                })
            })
            .collect())
    }

    fn hydrate(db: &DbAny, ids: &[DbId]) -> anyhow::Result<Vec<Genre>> {
        let mut genres = db::graph::bulk_fetch_typed(db, ids.to_vec(), "Genre")?;
        Ok(ids.iter().filter_map(|id| genres.remove(id)).collect())
    }
}

/// The releases the viewer can see that the filter scopes genres to.
fn scoped_genre_releases(
    db: &DbAny,
    viewer: &Viewer,
    filter: &GenreFilter,
) -> anyhow::Result<Vec<DbId>> {
    scoped_releases(db, viewer, filter.library, filter.releases.as_deref(), &[])?
        .resolve(&[], || all_releases(db))
}
