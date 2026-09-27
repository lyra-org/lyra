// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Catalog queries: every catalog entity is filtered, sorted and paged by [`pipeline`].

pub(crate) mod pipeline;
pub(crate) mod tracks;

use std::{
    cmp::Ordering,
    collections::HashSet,
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

    fn apply(self, ordering: Ordering) -> Ordering {
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
