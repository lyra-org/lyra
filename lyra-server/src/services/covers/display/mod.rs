// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

pub(crate) mod genres;
pub(crate) mod playlists;

use agdb::DbId;

use crate::db::{
    self,
    covers::display::{
        DisplayCoverProfile,
        DisplayCoverWinner,
    },
};

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
    db::covers::display::profile_is_clean(profile)
}

fn release_is_visible(
    release_db_id: DbId,
    visible_release_ids: Option<&std::collections::HashSet<DbId>>,
) -> bool {
    visible_release_ids.is_none_or(|ids| ids.contains(&release_db_id))
}

fn cover_for_release(
    db: &agdb::DbAny,
    release_db_id: DbId,
    visible_release_ids: Option<&std::collections::HashSet<DbId>>,
) -> anyhow::Result<Option<db::Cover>> {
    if !release_is_visible(release_db_id, visible_release_ids) {
        return Ok(None);
    }
    db::covers::get(db, release_db_id)
}

/// Per-release visibility check, for callers that would otherwise have to
/// enumerate every accessible release just to filter a handful of winners.
fn cover_for_accessible_release(
    db: &agdb::DbAny,
    principal: &crate::services::auth::Principal,
    release_db_id: DbId,
) -> anyhow::Result<Option<db::Cover>> {
    if !crate::services::auth::access::entity_accessible(db, principal, release_db_id)? {
        return Ok(None);
    }
    db::covers::get(db, release_db_id)
}
