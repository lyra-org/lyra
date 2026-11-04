// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Catalog queries: every catalog entity is filtered, sorted and paged by [`pipeline`] over the
//! catalog [`index`].

pub(crate) mod artists;
#[cfg(all(test, feature = "nightly"))]
mod benches;
pub(crate) mod genres;
pub(crate) mod index;
pub(crate) mod lookups;
#[cfg(test)]
mod oracle;
pub(crate) mod pipeline;
pub(crate) mod releases;
pub(crate) mod tracks;
#[cfg(test)]
mod visibility;

use std::{
    cmp::Ordering,
    collections::HashSet,
};

use agdb::{
    DbAny,
    DbId,
};

use crate::services::auth::{
    AuthError,
    Principal,
    access::PlaylistView,
};

use self::{
    index::{
        Snapshot,
        Table,
        bits::Bits,
    },
    pipeline::{
        Column,
        Context,
    },
};
pub(crate) use pipeline::{
    NameRange,
    PersonalData,
    PersonalFilter,
    Query,
    counts,
    leading,
    numbers,
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
        playlists: PlaylistView,
    },
}

impl Viewer {
    /// A viewer for `principal`, verified under the caller's guard.
    pub(crate) fn user(
        db: &impl lyra_db::DbAccess,
        principal: Principal,
    ) -> Result<Self, AuthError> {
        let user_db_id = principal.require(db)?;
        Ok(Self::User {
            principal,
            user_db_id,
            playlists: PlaylistView::Role,
        })
    }

    /// [`Viewer::user`] seeing playlists as a listener whatever its role, as plugins do.
    pub(crate) fn listener(
        db: &impl lyra_db::DbAccess,
        principal: Principal,
    ) -> Result<Self, AuthError> {
        let user_db_id = principal.require(db)?;
        Ok(Self::User {
            principal,
            user_db_id,
            playlists: PlaylistView::Listener,
        })
    }

    /// A viewer for `principal`, or the system without one.
    pub(crate) fn of(
        db: &impl lyra_db::DbAccess,
        principal: Option<Principal>,
    ) -> Result<Self, AuthError> {
        principal.map_or(Ok(Self::System), |principal| Self::user(db, principal))
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

    /// The user principal whose libraries limit what the viewer sees, or `None` when the viewer
    /// sees every library.
    pub(crate) fn restricted(&self) -> Option<&Principal> {
        match self {
            Self::User { principal, .. }
                if !principal.permissions.contains(&lyra_db::Permission::Admin) =>
            {
                Some(principal)
            }
            _ => None,
        }
    }

    /// Whether the viewer sees every release.
    pub(crate) fn sees_everything(&self) -> bool {
        self.restricted().is_none()
    }

    /// The viewer's libraries, or `None` when the viewer sees every library.
    pub(crate) fn visible_libraries(
        &self,
        db: &impl lyra_db::DbAccess,
    ) -> anyhow::Result<Option<Vec<DbId>>> {
        let Some(principal) = self.restricted() else {
            return Ok(None);
        };
        Ok(Some(lyra_db::libraries::db_ids_for_public_ids(
            db,
            &principal.accessible_library_ids,
        )?))
    }

    /// Whether the viewer can see `entity_db_id`, which is visible through the libraries it
    /// belongs to.
    pub(crate) fn sees_entity(
        &self,
        db: &impl lyra_db::DbAccess,
        entity_db_id: DbId,
    ) -> anyhow::Result<bool> {
        match self.restricted() {
            None => Ok(true),
            Some(principal) => {
                crate::services::auth::access::entity_accessible(db, principal, entity_db_id)
            }
        }
    }

    /// Whether the viewer can see `node_db_id`, by the rule for its type. A missing node is
    /// unseen; a graph search from it would fail.
    pub(crate) fn sees(&self, db: &DbAny, node_db_id: DbId) -> anyhow::Result<bool> {
        match lyra_db::graph::element_kind(db, node_db_id)?.as_deref() {
            None => Ok(false),
            Some("Artist") => self.sees_artist(db, node_db_id),
            Some("Playlist") => self.sees_playlist(db, node_db_id),
            Some("Entry") => {
                Ok(crate::services::entries::visible_entry(db, self, node_db_id)?.is_some())
            }
            Some("Genre") => Ok(self.sees_everything()
                || lookups::visible::<genres::Genres>(db, self, &[node_db_id])?
                    .contains(&node_db_id)),
            Some(_) => self.sees_entity(db, node_db_id),
        }
    }

    /// The nodes of `node_db_ids` the viewer can see, in order.
    pub(crate) fn visible(
        &self,
        db: &DbAny,
        node_db_ids: impl IntoIterator<Item = DbId>,
    ) -> anyhow::Result<Vec<DbId>> {
        let mut visible = Vec::new();
        for node_db_id in node_db_ids {
            if self.sees(db, node_db_id)? {
                visible.push(node_db_id);
            }
        }
        Ok(visible)
    }

    /// Whether the viewer can see `playlist_db_id`, by its [`PlaylistView`].
    pub(crate) fn sees_playlist(
        &self,
        db: &impl lyra_db::DbAccess,
        playlist_db_id: DbId,
    ) -> anyhow::Result<bool> {
        match self {
            Self::System => Ok(true),
            Self::User {
                principal,
                user_db_id,
                playlists,
            } => playlists.sees(db, principal, *user_db_id, playlist_db_id),
        }
    }

    /// Whether the viewer can see `artist_db_id`, which is visible through its releases.
    pub(crate) fn sees_artist(
        &self,
        db: &impl lyra_db::DbAccess,
        artist_db_id: DbId,
    ) -> anyhow::Result<bool> {
        match self.restricted() {
            None => Ok(true),
            Some(principal) => {
                crate::services::auth::access::artist_accessible(db, principal, artist_db_id)
            }
        }
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

    /// The slots of `len` the filter keeps, given the slots each single role reaches from a set
    /// of artist slots.
    pub(crate) fn reach(
        &self,
        index: &Snapshot,
        reach: impl Fn(&Bits, CreditRole) -> Bits,
    ) -> Bits {
        let artists = index.artists.slots(&self.artists);
        let reach_role = |role| match role {
            CreditRole::Any => {
                let mut reached = reach(&artists, CreditRole::Track);
                reached.or(&reach(&artists, CreditRole::Release));
                reached
            }
            role => reach(&artists, role),
        };
        let mut matching = reach_role(self.role);
        if let Some(excluding) = self.excluding {
            matching.and_not(&reach_role(excluding));
        }
        matching
    }
}

/// The release slots the viewer can see, narrowed to `library`, to `releases` and to releases in
/// any of `genres` when each is given, or `None` for every release. Ids that name nothing are
/// dropped.
pub(crate) fn scoped_releases(
    cx: &Context<'_>,
    library: Option<DbId>,
    releases: Option<&[DbId]>,
    genres: &[DbId],
) -> Option<Bits> {
    let index = &cx.index;
    let links = cx.links();
    let mut scoped = cx.scope.restricted.then(|| cx.scope.releases.clone());
    let mut restrict = |bits: Bits| match &mut scoped {
        Some(scoped) => scoped.and(&bits),
        None => scoped = Some(bits),
    };
    if let Some(library) = library {
        restrict(links.releases_of_library(library));
    }
    if let Some(releases) = releases {
        restrict(index.releases.slots(releases));
    }
    if !genres.is_empty() {
        restrict(links.releases_of_genres(&index.genres.slots(genres)));
    }
    scoped
}

/// What an entity's tracks add up to.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct TrackTotals {
    pub(crate) track_count: u64,
    pub(crate) total_duration: u64,
    pub(crate) listen_count: u64,
    pub(crate) last_played_at: Option<u64>,
}

/// Totals over each owner's track slots, by owner. Listens are read only when asked for.
pub(crate) fn track_totals(
    cx: &Context<'_>,
    tracks_by_owner: &[(u32, Vec<u32>)],
    listens: bool,
) -> Result<std::collections::HashMap<u32, TrackTotals>, CatalogError> {
    let index = &cx.index;
    let listens = if listens {
        let user = cx.viewer.user_db_id("listen sort keys")?;
        let tracks = tracks_by_owner
            .iter()
            .flat_map(|(_, tracks)| tracks)
            .copied()
            .collect::<HashSet<_>>()
            .into_iter()
            .map(|slot| index.tracks.id(slot))
            .collect::<Vec<_>>();
        lyra_db::listens::get_stats(cx.db, &tracks, user)?
            .into_iter()
            .filter_map(|stats| Some((index.tracks.slot(stats.db_id)?, stats)))
            .collect()
    } else {
        std::collections::HashMap::new()
    };
    Ok(tracks_by_owner
        .iter()
        .map(|(owner, tracks)| {
            let mut totals = TrackTotals {
                track_count: tracks.len() as u64,
                ..TrackTotals::default()
            };
            for track in tracks {
                let duration = index.track.duration[*track as usize];
                if duration != index::MISSING {
                    totals.total_duration = totals.total_duration.saturating_add(duration);
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

/// A column of one total for each of `slots`, computing the totals over the tracks `tracks_of`
/// gives each.
pub(crate) fn totals_column<'a>(
    cx: &Context<'_>,
    table: &Table,
    slots: &[u32],
    listens: bool,
    tracks_of: impl Fn(u32) -> Vec<u32>,
    value: impl Fn(&TrackTotals) -> Option<u64>,
) -> Result<Column<'a>, CatalogError> {
    let owners = slots
        .iter()
        .map(|slot| (*slot, tracks_of(*slot)))
        .collect::<Vec<_>>();
    let totals = track_totals(cx, &owners, listens)?;
    Ok(Column::computed(table.len(), slots, |slot| {
        value(&totals.get(&slot).copied().unwrap_or_default())
    }))
}

/// The viewer's rating of each of `slots`, or with `rated_at` when they last set it.
pub(crate) fn ratings_column<'a>(
    cx: &Context<'_>,
    table: &Table,
    slots: &[u32],
    rated_at: bool,
) -> Result<Column<'a>, CatalogError> {
    let user = cx.viewer.user_db_id("rating sort keys")?;
    let ids = slots.iter().map(|slot| table.id(*slot)).collect::<Vec<_>>();
    let ratings = lyra_db::ratings::values_for_targets(cx.db, user, &ids)?
        .into_iter()
        .filter_map(|(id, edge)| {
            let value = if rated_at {
                u64::try_from(edge.updated_at_ms).ok()?
            } else {
                u64::from(edge.value.get())
            };
            Some((table.slot(id)?, value))
        })
        .collect::<std::collections::HashMap<_, _>>();
    Ok(Column::computed(table.len(), slots, |slot| {
        ratings.get(&slot).copied()
    }))
}

/// When the viewer first favorited each of `slots`, a favorite of `kind`.
pub(crate) fn favorited_at_column<'a>(
    cx: &Context<'_>,
    table: &Table,
    slots: &[u32],
    kind: lyra_db::favorites::FavoriteKind,
) -> Result<Column<'a>, CatalogError> {
    let user = cx.viewer.user_db_id("the favorited_at sort key")?;
    let favorited = if slots.is_empty() {
        std::collections::HashMap::new()
    } else {
        lyra_db::favorites::first_favorited_at(cx.db, user, kind)?
            .into_iter()
            .filter_map(|(id, ms)| Some((table.slot(id)?, u64::try_from(ms).ok()?)))
            .collect()
    };
    Ok(Column::computed(table.len(), slots, |slot| {
        favorited.get(&slot).copied()
    }))
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
