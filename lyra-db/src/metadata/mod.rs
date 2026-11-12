// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

pub mod custom_fields;
pub(crate) mod fields;
pub mod layers;
pub mod mapping_config;

use agdb::{
    DbAny,
    DbId,
    QueryBuilder,
};

use self::custom_fields::ProviderCustomFields;
use super::DbAccess;

pub fn collect_layer_ids(db: &impl DbAccess, track_db_id: DbId) -> anyhow::Result<Vec<DbId>> {
    layers::ids_for_entity(db, track_db_id)
}

pub(crate) fn collect_custom_field_ids(
    db: &impl DbAccess,
    track_db_id: DbId,
) -> anyhow::Result<Vec<DbId>> {
    let rows: Vec<ProviderCustomFields> = db
        .exec(
            QueryBuilder::select()
                .elements::<ProviderCustomFields>()
                .search()
                .from(track_db_id)
                .where_()
                .neighbor()
                .end_where()
                .query(),
        )?
        .try_into()?;

    Ok(rows
        .into_iter()
        .filter_map(|row| row.db_id.map(Into::into))
        .collect())
}

/// Cascade-remove entity nodes and their inbound favorite, rating, and tag edges, and detach
/// their covers, then collect what they leave unreferenced: a removed track's emptied release
/// and its sources, with their cue records. Artists they referenced become candidates, and the
/// files of the covers no remaining owner shares are listed for discarding.
/// Callers already inside a `transaction_mut` must use [`cascade_remove_entities_in_txn`]
/// — agdb transactions are not reentrant.
pub fn cascade_remove_entities(db: &mut DbAny, node_ids: &[DbId]) -> anyhow::Result<()> {
    if node_ids.is_empty() {
        return Ok(());
    }
    db.transaction_mut(|t| cascade_remove_entities_in_txn(t, node_ids))
}

/// [`cascade_remove_entities`] inside the caller's transaction.
pub fn cascade_remove_entities_in_txn(
    db: &mut impl DbAccess,
    node_ids: &[DbId],
) -> anyhow::Result<()> {
    if node_ids.is_empty() {
        return Ok(());
    }
    let left = super::orphans::Candidates::around(db, node_ids)?;
    remove_entities(db, node_ids)?;
    super::orphans::collect(db, left)
}

/// Removes the entities with what is attached to them, leaving what they referenced as it is
/// but for the artists, which become candidates.
pub(crate) fn remove_entities(db: &mut impl DbAccess, node_ids: &[DbId]) -> anyhow::Result<()> {
    if node_ids.is_empty() {
        return Ok(());
    }
    let playlist_db_ids = super::covers::display::playlist_ids_for_tracks(db, node_ids)?;
    cascade_remove_entities_pre_favorites(db, node_ids)?;
    cascade_remove_favorites_and_nodes(db, node_ids)?;
    super::covers::display::sync_playlist_covers(db, &playlist_db_ids)
}

fn cascade_remove_entities_pre_favorites(
    db: &mut impl DbAccess,
    node_ids: &[DbId],
) -> anyhow::Result<()> {
    for &id in node_ids {
        super::covers::remove_all(db, id)?;
        layers::remove_by_ids(db, &collect_layer_ids(db, id)?)?;
        for custom_field_id in collect_custom_field_ids(db, id)? {
            db.exec_mut(QueryBuilder::remove().ids(custom_field_id).query())?;
        }
        super::external_ids::remove_all_for_owner(db, id)?;
        // agdb cascades the Release's label edges, but a Label left without
        // releases must be collected.
        super::labels::unlink_release(db, id)?;
        // A chain goes whole with its release or any of its tracks.
        super::chains::remove_for_entity(db, id)?;
        // Lyrics + their LyricLine/LyricWord children sit at distance ≥ 2 from
        // the track; agdb's direct-edge cascade misses them. No-op for non-tracks.
        super::lyrics::delete_for_track_in_txn(db, id)?;
    }
    Ok(())
}

fn cascade_remove_favorites_and_nodes(
    db: &mut impl DbAccess,
    node_ids: &[DbId],
) -> anyhow::Result<()> {
    for &id in node_ids {
        super::favorites::remove_inbound_for_target(db, id)?;
        super::ratings::remove_inbound_for_target(db, id)?;
        super::bookmarks::remove_inbound_for_target(db, id)?;
    }
    let credited = super::credits::credited_artist_ids(db, node_ids)?;
    let mut referenced = Vec::new();
    for &id in node_ids {
        referenced.extend(super::graph::neighbor_ids(db, id, "Artist")?);
    }
    db.exec_mut(QueryBuilder::remove().ids(node_ids).query())?;
    super::orphans::mark_artists(db, &referenced)?;
    super::credits::recompute_credited(db, &credited)
}
