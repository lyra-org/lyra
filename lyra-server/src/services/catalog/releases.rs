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
    CatalogError,
    CreditRole,
    Direction,
    SortSpec,
    Viewer,
    all_releases,
    credited_owners,
    pipeline::{
        Catalog,
        KeyKind,
        Row,
        SortKey,
        SortValue,
    },
    releases_of_tracks,
    scoped_releases,
    track_totals,
};
use crate::db::{
    self,
    Release,
    Track,
    favorites::FavoriteKind,
    ratings::RatingFilter,
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

#[derive(Clone, Debug, Default)]
pub(crate) struct ReleaseFilter {
    pub(crate) ids: Option<Vec<DbId>>,
    pub(crate) exclude_ids: Vec<DbId>,
    /// Leaves out releases credited to these artists in either role.
    pub(crate) exclude_artists: Vec<DbId>,
    pub(crate) library: Option<DbId>,
    pub(crate) artists: Option<ArtistCredit>,
    pub(crate) genres: Vec<DbId>,
    pub(crate) years: Vec<u32>,
    pub(crate) favorite: Option<bool>,
    /// Releases whose every track the user has listened to.
    pub(crate) listened: Option<bool>,
    pub(crate) rating: RatingFilter,
}

pub(crate) struct Releases;

impl Catalog for Releases {
    type Key = ReleaseKey;
    type Filter = ReleaseFilter;
    type Item = Release;

    fn id_filter(ids: Vec<DbId>) -> ReleaseFilter {
        ReleaseFilter {
            ids: Some(ids),
            ..ReleaseFilter::default()
        }
    }

    fn default_sort(_filter: &ReleaseFilter) -> SortSpec<ReleaseKey> {
        vec![(ReleaseKey::SortName, Direction::Ascending)]
    }

    fn candidates(
        db: &DbAny,
        viewer: &Viewer,
        filter: &ReleaseFilter,
    ) -> Result<Vec<DbId>, CatalogError> {
        let mut releases = scoped_releases(
            db,
            viewer,
            filter.library,
            filter.ids.as_deref(),
            &filter.genres,
        )?;
        let reach = |artists: &[DbId], role| match role {
            CreditRole::Track => releases_of_tracks(db, credited_owners::<Track>(db, artists)?),
            _ => credited_owners::<Release>(db, artists),
        };
        if let Some(credit) = &filter.artists {
            releases.restrict(credit.matching(db, reach)?);
        }
        if !filter.years.is_empty() {
            let mut matching = HashSet::new();
            for year in &filter.years {
                matching.extend(db::releases::ids_with_year(db, *year)?);
            }
            releases.restrict(matching);
        }
        if let Some(favorite) = filter.favorite {
            let user = viewer.user_db_id("the favorite filter")?;
            let favorites = db::favorites::target_ids(db, user, FavoriteKind::Release)?;
            releases.restrict_membership(favorites, favorite, || all_releases(db))?;
        }
        if let Some(listened) = filter.listened {
            let user = viewer.user_db_id("the listened filter")?;
            let listened_tracks = db::listens::listened_track_ids(db, user)?;
            let mut listened_ids = HashSet::new();
            for release in releases_of_tracks(db, listened_tracks.iter().copied())? {
                if db::graph::neighbor_ids(db, release, "Track")?
                    .iter()
                    .all(|track| listened_tracks.contains(track))
                {
                    listened_ids.insert(release);
                }
            }
            releases.restrict_membership(listened_ids, listened, || all_releases(db))?;
        }
        if !filter.rating.is_empty() {
            let user = viewer.user_db_id("the rating filter")?;
            releases.restrict(db::ratings::target_ids_matching(db, user, filter.rating)?);
        }

        let mut excluded = filter.exclude_ids.clone();
        if !filter.exclude_artists.is_empty() {
            excluded.extend(ArtistCredit::any(filter.exclude_artists.clone()).matching(db, reach)?);
        }
        Ok(releases.resolve(&excluded, || all_releases(db))?)
    }

    fn rows(
        db: &DbAny,
        viewer: &Viewer,
        _filter: &ReleaseFilter,
        ids: Vec<DbId>,
        keys: &[ReleaseKey],
    ) -> Result<Vec<Row>, CatalogError> {
        let fields = db::graph::select_fields(db, &ids, &release_fields(keys))?;
        let needs = |wanted: &[ReleaseKey]| keys.iter().any(|key| wanted.contains(key));
        let totals = if needs(&[
            ReleaseKey::TotalDuration,
            ReleaseKey::TrackCount,
            ReleaseKey::ListenCount,
            ReleaseKey::LastPlayedAt,
        ]) {
            let mut tracks_by_release = HashMap::new();
            for id in &ids {
                tracks_by_release.insert(
                    *id,
                    db::graph::neighbor_ids(db, *id, "Track")?
                        .into_iter()
                        .collect::<HashSet<_>>(),
                );
            }
            track_totals(
                db,
                viewer,
                &tracks_by_release,
                needs(&[ReleaseKey::TotalDuration]),
                needs(&[ReleaseKey::ListenCount, ReleaseKey::LastPlayedAt]),
            )?
        } else {
            HashMap::new()
        };
        let artist_names = if needs(&[ReleaseKey::ArtistName]) {
            db::artists::first_credited_names(db, &ids)?
        } else {
            HashMap::new()
        };

        Ok(ids
            .into_iter()
            .filter_map(|id| {
                let release = fields.get(&id)?;
                let name = release.text("release_title")?;
                let sort_name = release.text("sort_title").unwrap_or(name).to_lowercase();
                let number = |key| release.number(key).map(SortValue::Number);
                let totals = totals.get(&id).copied().unwrap_or_default();
                let values = keys
                    .iter()
                    .map(|key| match key {
                        ReleaseKey::Name => Some(SortValue::Text(name.to_lowercase())),
                        ReleaseKey::SortName => Some(SortValue::Text(sort_name.clone())),
                        ReleaseKey::DateCreated => number("ctime").or_else(|| number("created_at")),
                        ReleaseKey::ReleaseDate => release
                            .text("release_date")
                            .and_then(date_order)
                            .map(SortValue::Number),
                        ReleaseKey::Year => {
                            db::releases::release_year(release.text("release_date"))
                                .map(|year| SortValue::Number(year.into()))
                        }
                        ReleaseKey::TotalDuration => Some(SortValue::Number(totals.total_duration)),
                        ReleaseKey::TrackCount => Some(SortValue::Number(totals.track_count)),
                        ReleaseKey::ArtistName => artist_names
                            .get(&id)
                            .map(|name| SortValue::Text(name.to_lowercase())),
                        ReleaseKey::ListenCount => Some(SortValue::Number(totals.listen_count)),
                        ReleaseKey::LastPlayedAt => totals.last_played_at.map(SortValue::Number),
                        ReleaseKey::Relevance | ReleaseKey::Random | ReleaseKey::Id => None,
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

    fn hydrate(db: &DbAny, ids: &[DbId]) -> anyhow::Result<Vec<Release>> {
        let mut releases = db::graph::bulk_fetch_typed(db, ids.to_vec(), "Release")?;
        Ok(ids.iter().filter_map(|id| releases.remove(id)).collect())
    }
}

/// The stored release fields that the names and `keys` read.
fn release_fields(keys: &[ReleaseKey]) -> Vec<&'static str> {
    let mut fields = vec!["release_title", "sort_title"];
    for key in keys {
        fields.extend_from_slice(match key {
            ReleaseKey::DateCreated => &["ctime", "created_at"],
            ReleaseKey::ReleaseDate | ReleaseKey::Year => &["release_date"],
            _ => &[],
        });
    }
    fields
}

/// `YYYY`, `YYYY-MM` or `YYYY-MM-DD` as `YYYYMMDD`, with missing parts as zero.
fn date_order(date: &str) -> Option<u64> {
    let mut parts = date.splitn(3, '-').map(str::parse::<u64>);
    let year = parts.next()?.ok()?;
    let month = parts.next().transpose().ok()?.unwrap_or(0);
    let day = parts.next().transpose().ok()?.unwrap_or(0);
    Some(year * 10_000 + month * 100 + day)
}
