// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

pub(crate) mod layers;
pub(crate) mod mapping_config;

use agdb::{
    DbAny,
    DbId,
    QueryBuilder,
};

use self::layers::MetadataLayer;
use super::DbAccess;
use super::external_ids::ExternalId;

pub(crate) fn collect_external_id_ids(
    db: &impl DbAccess,
    track_db_id: DbId,
) -> anyhow::Result<Vec<DbId>> {
    let ids: Vec<ExternalId> = db
        .exec(
            QueryBuilder::select()
                .elements::<ExternalId>()
                .search()
                .from(track_db_id)
                .where_()
                .neighbor()
                .end_where()
                .query(),
        )?
        .try_into()?;

    Ok(ids
        .into_iter()
        .filter_map(|id| id.db_id.map(Into::into))
        .collect())
}

pub(crate) fn collect_layer_ids(
    db: &impl DbAccess,
    track_db_id: DbId,
) -> anyhow::Result<Vec<DbId>> {
    let layers: Vec<MetadataLayer> = db
        .exec(
            QueryBuilder::select()
                .elements::<MetadataLayer>()
                .search()
                .from(track_db_id)
                .where_()
                .neighbor()
                .end_where()
                .query(),
        )?
        .try_into()?;

    Ok(layers
        .into_iter()
        .filter_map(|layer| layer.db_id.map(Into::into))
        .collect())
}

/// Cascade-remove entity nodes, their inbound favorite + tag edges, and orphan tags.
/// Callers already inside a `transaction_mut` must use [`cascade_remove_entities_in_txn`]
/// — agdb transactions are not reentrant.
pub(crate) fn cascade_remove_entities(db: &mut DbAny, node_ids: &[DbId]) -> anyhow::Result<()> {
    if node_ids.is_empty() {
        return Ok(());
    }
    cascade_remove_entities_pre_favorites(db, node_ids)?;
    db.transaction_mut(|t| -> anyhow::Result<()> {
        cascade_remove_favorites_and_nodes(t, node_ids)
    })
}

pub(crate) fn cascade_remove_entities_in_txn(
    db: &mut impl DbAccess,
    node_ids: &[DbId],
) -> anyhow::Result<()> {
    if node_ids.is_empty() {
        return Ok(());
    }
    cascade_remove_entities_pre_favorites(db, node_ids)?;
    cascade_remove_favorites_and_nodes(db, node_ids)
}

fn cascade_remove_entities_pre_favorites(
    db: &mut impl DbAccess,
    node_ids: &[DbId],
) -> anyhow::Result<()> {
    for &id in node_ids {
        for layer_id in collect_layer_ids(db, id)? {
            db.exec_mut(QueryBuilder::remove().ids(layer_id).query())?;
        }
        for ext_id in collect_external_id_ids(db, id)? {
            db.exec_mut(QueryBuilder::remove().ids(ext_id).query())?;
        }
        // Remove Credit child nodes (owner → Credit → Artist).
        let credits: Vec<super::Credit> = db
            .exec(
                QueryBuilder::select()
                    .elements::<super::Credit>()
                    .search()
                    .from(id)
                    .where_()
                    .neighbor()
                    .end_where()
                    .query(),
            )?
            .try_into()?;
        let credit_ids: Vec<DbId> = credits
            .iter()
            .filter_map(|c| c.db_id.clone().map(DbId::from))
            .collect();
        if !credit_ids.is_empty() {
            db.exec_mut(QueryBuilder::remove().ids(credit_ids).query())?;
        }
        // agdb cascades the Release's outgoing edges, but the RL node and
        // its linked Label would leak without this explicit walk.
        super::labels::cascade_remove_release_labels_for_owner(db, id)?;
    }
    Ok(())
}

fn cascade_remove_favorites_and_nodes(
    db: &mut impl DbAccess,
    node_ids: &[DbId],
) -> anyhow::Result<()> {
    for &id in node_ids {
        super::favorites::remove_inbound_for_target(db, id)?;
    }
    super::tags::remove_inbound_for_target_with_orphan_cleanup(db, node_ids)?;
    db.exec_mut(QueryBuilder::remove().ids(node_ids).query())?;
    Ok(())
}

pub(crate) fn get_connected_artist_ids(
    db: &impl super::DbAccess,
    owner_db_id: DbId,
) -> anyhow::Result<Vec<DbId>> {
    let artists = super::artists::get(db, owner_db_id)?;
    Ok(artists
        .into_iter()
        .filter_map(|a| a.db_id.map(Into::into))
        .collect())
}
