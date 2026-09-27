// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Catalog queries: every catalog entity is filtered, sorted and paged by [`pipeline`].

pub(crate) mod artists;
pub(crate) mod genres;
pub(crate) mod lookups;
pub(crate) mod pipeline;
pub(crate) mod releases;
pub(crate) mod tracks;

use std::{
    cmp::Ordering,
    collections::{
        HashMap,
        HashSet,
    },
};

use agdb::{
    DbAny,
    DbId,
};

use crate::{
    db,
    services::auth::{
        AuthError,
        Principal,
    },
};

pub(crate) use pipeline::{
    NameRange,
    Query,
    counts,
    order,
    page,
};

/// Who a catalog query runs for.
#[derive(Clone, Debug)]
pub(crate) enum Viewer {
    /// Server-side work, which sees every library and has no user.
    System,
    User {
        principal: Principal,
        user_db_id: DbId,
    },
}

impl Viewer {
    /// A viewer for `principal`, verified under the caller's guard.
    pub(crate) fn user(db: &impl db::DbAccess, principal: Principal) -> Result<Self, AuthError> {
        let user_db_id = principal.require(db)?;
        Ok(Self::User {
            principal,
            user_db_id,
        })
    }

    /// The user a user-dependent filter or key reads, which a system query does not have.
    pub(crate) fn user_db_id(&self, needed_by: &str) -> Result<DbId, CatalogError> {
        match self {
            Self::User { user_db_id, .. } => Ok(*user_db_id),
            Self::System => Err(CatalogError::Invalid(format!(
                "{needed_by} needs a user; this query runs without one"
            ))),
        }
    }

    /// The releases in the viewer's libraries, or `None` when the viewer sees every release.
    pub(crate) fn visible_releases(&self, db: &DbAny) -> anyhow::Result<Option<HashSet<DbId>>> {
        let Self::User { principal, .. } = self else {
            return Ok(None);
        };
        if principal.permissions.contains(&db::Permission::Admin) {
            return Ok(None);
        }
        let mut releases = HashSet::new();
        for library in db::libraries::db_ids_for_public_ids(db, &principal.accessible_library_ids)?
        {
            releases.extend(db::graph::neighbor_ids(db, library, "Release")?);
        }
        Ok(Some(releases))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Direction {
    Ascending,
    Descending,
}

impl Direction {
    pub(crate) fn parse(raw: &str) -> Result<Self, CatalogError> {
        if raw.eq_ignore_ascii_case("ascending") {
            Ok(Self::Ascending)
        } else if raw.eq_ignore_ascii_case("descending") {
            Ok(Self::Descending)
        } else {
            Err(CatalogError::Invalid(format!(
                "unsupported sort order: {raw}. Supported orders: ascending, descending"
            )))
        }
    }

    pub(crate) fn apply(self, ordering: Ordering) -> Ordering {
        match self {
            Self::Ascending => ordering,
            Self::Descending => ordering.reverse(),
        }
    }
}

/// Sort keys in priority order, each with its own direction.
pub(crate) type SortSpec<K> = Vec<(K, Direction)>;

pub(crate) struct Page<T> {
    pub(crate) items: Vec<T>,
    pub(crate) total: u64,
    pub(crate) offset: u64,
}

/// The kind of owner an artist credit is on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CreditRole {
    /// A credit on a track.
    Track,
    /// A credit on a release.
    Release,
    /// A credit on either.
    Any,
}

impl CreditRole {
    /// Every role and the token that names it.
    pub(crate) const KEYS: &'static [(&'static str, Self)] = &[
        ("track", Self::Track),
        ("release", Self::Release),
        ("any", Self::Any),
    ];

    pub(crate) fn parse(raw: &str) -> Result<Self, CatalogError> {
        Self::KEYS
            .iter()
            .find(|(token, _)| *token == raw)
            .map(|(_, role)| *role)
            .ok_or_else(|| {
                let supported = Self::KEYS
                    .iter()
                    .map(|(token, _)| *token)
                    .collect::<Vec<_>>()
                    .join(", ");
                CatalogError::Invalid(format!(
                    "unsupported credit role: {raw}. Supported roles: {supported}"
                ))
            })
    }
}

/// Entities credited to any of `artists` in `role`, less those credited to them in `excluding`.
#[derive(Clone, Debug)]
pub(crate) struct ArtistCredit {
    pub(crate) artists: Vec<DbId>,
    pub(crate) role: CreditRole,
    pub(crate) excluding: Option<CreditRole>,
}

impl ArtistCredit {
    /// Entities credited to any of `artists` in either role.
    pub(crate) fn any(artists: Vec<DbId>) -> Self {
        Self {
            artists,
            role: CreditRole::Any,
            excluding: None,
        }
    }

    /// The entities the filter keeps, given the entities each single role reaches from a set of
    /// existing artists.
    pub(crate) fn matching(
        &self,
        db: &DbAny,
        reach: impl Fn(&[DbId], CreditRole) -> anyhow::Result<HashSet<DbId>>,
    ) -> anyhow::Result<HashSet<DbId>> {
        let artists = db::graph::existing_ids(db, &self.artists, "Artist")?;
        let reach_role = |role| -> anyhow::Result<HashSet<DbId>> {
            match role {
                CreditRole::Any => {
                    let mut reached = reach(&artists, CreditRole::Track)?;
                    reached.extend(reach(&artists, CreditRole::Release)?);
                    Ok(reached)
                }
                role => reach(&artists, role),
            }
        };
        let mut matching = reach_role(self.role)?;
        if let Some(excluding) = self.excluding {
            let excluded = reach_role(excluding)?;
            matching.retain(|id| !excluded.contains(id));
        }
        Ok(matching)
    }
}

/// The releases the viewer can see, narrowed to `library`, to `releases` and to releases in any
/// of `genres` when each is given. Ids that name nothing are dropped.
pub(crate) fn scoped_releases(
    db: &DbAny,
    viewer: &Viewer,
    library: Option<DbId>,
    releases: Option<&[DbId]>,
    genres: &[DbId],
) -> anyhow::Result<Candidates> {
    let mut scoped = Candidates::default();
    if let Some(visible) = viewer.visible_releases(db)? {
        scoped.restrict(visible);
    }
    if let Some(library) = library {
        let mut library_releases = Vec::new();
        for library in db::graph::existing_ids(db, &[library], "Library")? {
            library_releases.extend(db::graph::neighbor_ids(db, library, "Release")?);
        }
        scoped.restrict(library_releases);
    }
    if let Some(releases) = releases {
        scoped.restrict(db::graph::existing_ids(db, releases, "Release")?);
    }
    if !genres.is_empty() {
        let genres = db::graph::existing_ids(db, genres, "Genre")?;
        scoped.restrict(db::genres::release_ids_matching_genre_ids(db, &genres)?);
    }
    Ok(scoped)
}

pub(crate) fn all_releases(db: &DbAny) -> anyhow::Result<Vec<DbId>> {
    db::graph::neighbor_ids(db, "releases", "Release")
}

/// The owners of kind `Owner` that credit any of `artists`.
pub(crate) fn credited_owners<Owner: agdb::DbType>(
    db: &DbAny,
    artists: &[DbId],
) -> anyhow::Result<HashSet<DbId>> {
    let mut owners = HashSet::new();
    for artist in artists {
        owners.extend(db::credits::owner_ids_by_artist::<Owner>(
            db, *artist, 0, 0,
        )?);
    }
    Ok(owners)
}

pub(crate) fn tracks_of_releases(
    db: &DbAny,
    releases: impl IntoIterator<Item = DbId>,
) -> anyhow::Result<HashSet<DbId>> {
    let mut tracks = HashSet::new();
    for release in releases {
        tracks.extend(db::graph::neighbor_ids(db, release, "Track")?);
    }
    Ok(tracks)
}

pub(crate) fn releases_of_tracks(
    db: &DbAny,
    tracks: impl IntoIterator<Item = DbId>,
) -> anyhow::Result<HashSet<DbId>> {
    let mut releases = HashSet::new();
    for track in tracks {
        releases.extend(db::graph::inbound_neighbor_ids(db, track, "Release")?);
    }
    Ok(releases)
}

/// What an entity's tracks add up to.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct TrackTotals {
    pub(crate) track_count: u64,
    pub(crate) total_duration: u64,
    pub(crate) listen_count: u64,
    pub(crate) last_played_at: Option<u64>,
}

/// Totals over each owner's tracks. Durations and listens are read only when asked for.
pub(crate) fn track_totals(
    db: &DbAny,
    viewer: &Viewer,
    tracks_by_owner: &HashMap<DbId, HashSet<DbId>>,
    durations: bool,
    listens: bool,
) -> Result<HashMap<DbId, TrackTotals>, CatalogError> {
    let tracks = tracks_by_owner
        .values()
        .flatten()
        .copied()
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let durations = if durations {
        db::graph::select_fields(db, &tracks, &["duration_ms"])?
            .into_iter()
            .filter_map(|(id, fields)| Some((id, fields.number("duration_ms")?)))
            .collect()
    } else {
        HashMap::new()
    };
    let listens = if listens {
        let user = viewer.user_db_id("listen sort keys")?;
        db::listens::get_stats(db, &tracks, user)?
            .into_iter()
            .map(|stats| (stats.db_id, stats))
            .collect()
    } else {
        HashMap::new()
    };
    Ok(tracks_by_owner
        .iter()
        .map(|(owner, tracks)| {
            let mut totals = TrackTotals {
                track_count: tracks.len() as u64,
                ..TrackTotals::default()
            };
            for track in tracks {
                if let Some(duration) = durations.get(track) {
                    totals.total_duration = totals.total_duration.saturating_add(*duration);
                }
                if let Some(stats) = listens.get(track) {
                    totals.listen_count = totals.listen_count.saturating_add(stats.count);
                    totals.last_played_at = totals.last_played_at.max(stats.last_played);
                }
            }
            (*owner, totals)
        })
        .collect())
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum CatalogError {
    #[error("{0}")]
    Invalid(String),
    #[error(transparent)]
    Auth(#[from] AuthError),
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl From<agdb::DbError> for CatalogError {
    fn from(error: agdb::DbError) -> Self {
        Self::Internal(error.into())
    }
}

/// A candidate id set built by intersection, where `None` stands for every entity.
#[derive(Default)]
pub(crate) struct Candidates(Option<HashSet<DbId>>);

impl Candidates {
    pub(crate) fn restrict(&mut self, ids: impl IntoIterator<Item = DbId>) {
        let ids = ids.into_iter();
        self.0 = Some(match self.0.take() {
            Some(current) => {
                let ids = ids.collect::<HashSet<_>>();
                current.into_iter().filter(|id| ids.contains(id)).collect()
            }
            None => ids.collect(),
        });
    }

    pub(crate) fn ids(&self) -> Option<&HashSet<DbId>> {
        self.0.as_ref()
    }

    /// Keeps the ids in `ids` when `wanted` is true, and the others when it is false.
    pub(crate) fn restrict_membership(
        &mut self,
        ids: HashSet<DbId>,
        wanted: bool,
        all: impl FnOnce() -> anyhow::Result<Vec<DbId>>,
    ) -> anyhow::Result<()> {
        if wanted {
            self.restrict(ids);
            return Ok(());
        }
        let remaining = match &self.0 {
            Some(current) => current
                .iter()
                .copied()
                .filter(|id| !ids.contains(id))
                .collect(),
            None => all()?.into_iter().filter(|id| !ids.contains(id)).collect(),
        };
        self.0 = Some(remaining);
        Ok(())
    }

    /// The final ids, reading every entity through `all` when nothing restricted them.
    pub(crate) fn resolve(
        self,
        exclude: &[DbId],
        all: impl FnOnce() -> anyhow::Result<Vec<DbId>>,
    ) -> anyhow::Result<Vec<DbId>> {
        let exclude = exclude.iter().copied().collect::<HashSet<_>>();
        let ids = match self.0 {
            Some(ids) => ids.into_iter().collect(),
            None => all()?,
        };
        Ok(ids.into_iter().filter(|id| !exclude.contains(id)).collect())
    }
}
