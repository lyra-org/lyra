// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

pub(crate) mod genres;
pub(crate) mod playlists;

use agdb::{
    DbAny,
    DbId,
};

use lyra_db::covers::display::{
    DisplayCoverProfile,
    DisplayCoverWinner,
};

use super::ResolvedCover;
use crate::services::catalog::Viewer;

/// Whose listens pick a genre's cover.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Listens {
    /// The viewing user's own listens first, then the instance's. The system has none of its own.
    Viewer,
    /// Only the instance's listens, so every viewer gets the same pick.
    Instance,
}

fn ceil_div(value: u64, divisor: u64) -> u64 {
    if divisor == 0 {
        return 0;
    }
    value.div_ceil(divisor)
}

fn required_user_listens(instance_profile: Option<&DisplayCoverProfile>) -> u64 {
    let Some(instance_profile) = instance_profile else {
        return 3;
    };
    if instance_profile.active_listener_count < 3 {
        return 3;
    }
    ceil_div(
        instance_profile.instance_total_listens,
        instance_profile.active_listener_count.saturating_mul(4),
    )
    .clamp(3, 20)
}

fn user_signal_is_enough(
    user_profile: &DisplayCoverProfile,
    personal_winner: &DisplayCoverWinner,
    instance_profile: Option<&DisplayCoverProfile>,
) -> bool {
    let required = required_user_listens(instance_profile);
    user_profile.user_total_listens >= required
        && personal_winner.listen_count >= 2.max(ceil_div(required, 2))
}

fn instance_signal_is_enough(instance_profile: &DisplayCoverProfile) -> bool {
    instance_profile.active_listener_count >= 3
        && instance_profile.instance_total_listens >= 10
        && instance_profile.instance_total_listens
            >= instance_profile.active_listener_count.saturating_mul(3)
}

fn profile_is_current(profile: &DisplayCoverProfile) -> bool {
    lyra_db::covers::display::profile_is_clean(profile)
}

/// A check a release's cover must pass to be shown, given the release and its cover.
pub(crate) type CoverCheck<'a> = &'a mut dyn FnMut(DbId, &lyra_db::Cover) -> bool;

/// The releases a display cover may come from: those the viewer sees, only in `library` when
/// set, whose cover passes the check. Every step of a selection skips the releases it rejects.
pub(crate) struct CoverFilter<'a> {
    viewer: &'a Viewer,
    library: Option<DbId>,
    check: Option<CoverCheck<'a>>,
}

impl<'a> CoverFilter<'a> {
    pub(crate) fn new(viewer: &'a Viewer) -> Self {
        Self {
            viewer,
            library: None,
            check: None,
        }
    }

    pub(crate) fn in_library(self, library: Option<DbId>) -> Self {
        Self { library, ..self }
    }

    pub(crate) fn checked(self, check: CoverCheck<'a>) -> Self {
        Self {
            check: Some(check),
            ..self
        }
    }

    pub(crate) fn viewer(&self) -> &'a Viewer {
        self.viewer
    }

    /// The cover of `release_db_id` when the filter lets it through.
    fn release_cover(
        &mut self,
        db: &DbAny,
        release_db_id: DbId,
    ) -> anyhow::Result<Option<ResolvedCover>> {
        if let Some(library) = self.library
            && !lyra_db::graph::edge_exists(db, library, release_db_id)?
        {
            return Ok(None);
        }
        if !self.viewer.sees_entity(db, release_db_id)? {
            return Ok(None);
        }
        let Some(cover) = lyra_db::covers::get(db, release_db_id)? else {
            return Ok(None);
        };
        if let Some(check) = self.check.as_mut()
            && !check(release_db_id, &cover)
        {
            return Ok(None);
        }
        Ok(Some(ResolvedCover {
            owner_db_id: release_db_id,
            cover,
        }))
    }

    /// The cover `owner_db_id` holds itself, when it passes the check. The caller has already
    /// established that the viewer sees the owner.
    fn owned_cover(
        &mut self,
        db: &DbAny,
        owner_db_id: DbId,
    ) -> anyhow::Result<Option<ResolvedCover>> {
        let Some(cover) = lyra_db::covers::get(db, owner_db_id)? else {
            return Ok(None);
        };
        if let Some(check) = self.check.as_mut()
            && !check(owner_db_id, &cover)
        {
            return Ok(None);
        }
        Ok(Some(ResolvedCover { owner_db_id, cover }))
    }

    /// The first of `release_db_ids` the filter lets through.
    fn first_release_cover(
        &mut self,
        db: &DbAny,
        release_db_ids: impl IntoIterator<Item = DbId>,
    ) -> anyhow::Result<Option<ResolvedCover>> {
        for release_db_id in release_db_ids {
            if let Some(cover) = self.release_cover(db, release_db_id)? {
                return Ok(Some(cover));
            }
        }
        Ok(None)
    }
}
