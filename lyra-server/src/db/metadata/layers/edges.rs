// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashSet;

use agdb::{
    CountComparison,
    DbAnyTransactionMut,
    DbElement,
    DbId,
    DbType,
    QueryBuilder,
};

use super::super::super::{
    Artist,
    DbAccess,
    artists::relations::ArtistRelationType,
    credits::CreditType,
    graph,
    labels::LabelLinkInput,
};

/// A layer's credit of an artist in one role, ordered within that role.
#[derive(DbElement, Clone, Debug, PartialEq, Eq)]
pub(crate) struct LayerCredit {
    pub(crate) credit_type: CreditType,
    pub(crate) detail: Option<String>,
    pub(crate) artist_order: u64,
}

impl LayerCredit {
    fn role(&self) -> (CreditType, Option<&str>) {
        (self.credit_type, self.detail.as_deref())
    }
}

/// An artist layer's relation to another artist.
#[derive(DbElement, Clone, Debug, PartialEq, Eq)]
pub(crate) struct LayerRelation {
    pub(crate) relation_type: ArtistRelationType,
    pub(crate) attributes: Option<String>,
}

#[derive(DbElement, Clone, Debug, PartialEq, Eq)]
pub(crate) struct LayerGenre {
    pub(crate) genre_order: u64,
}

#[derive(DbElement, Clone, Debug, PartialEq, Eq)]
pub(crate) struct LayerLabel {
    pub(crate) label_order: u64,
    pub(crate) catalog_number: Option<String>,
}

/// A stored relationship edge between a layer and the entity it references.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LayerLink<T> {
    pub(crate) edge_id: DbId,
    pub(crate) layer_id: DbId,
    pub(crate) target_id: DbId,
    pub(crate) edge: T,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LayerCreditInput {
    pub(crate) artist_id: DbId,
    pub(crate) detail: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LayerRelationInput {
    pub(crate) artist_id: DbId,
    pub(crate) attributes: Option<String>,
}

const RELEASE_LAYER: &str = "ReleaseLayer";
const TRACK_LAYER: &str = "TrackLayer";
const ARTIST_LAYER: &str = "ArtistLayer";

/// Every relationship edge points from the referenced entity into its layer,
/// so a layer has no outgoing edges and traversals cannot pass through it.
fn insert(
    db: &mut impl DbAccess,
    layer_id: DbId,
    target_id: DbId,
    edge: &impl DbType,
) -> anyhow::Result<()> {
    db.exec_mut(
        QueryBuilder::insert()
            .edges()
            .from(target_id)
            .to(layer_id)
            .values_uniform(edge.to_db_values())
            .query(),
    )?;
    Ok(())
}

fn edges_only(result: agdb::QueryResult) -> impl Iterator<Item = agdb::DbElement> {
    result
        .elements
        .into_iter()
        .filter(|element| element.id.0 < 0)
}

fn decode<T: DbType<ValueType = T>>(element: &agdb::DbElement) -> anyhow::Result<LayerLink<T>> {
    Ok(LayerLink {
        edge_id: element.id,
        layer_id: element.to,
        target_id: element.from,
        edge: T::from_db_element(element)?,
    })
}

fn of_layer<T: DbType<ValueType = T>>(
    db: &impl DbAccess,
    layer_id: DbId,
) -> anyhow::Result<Vec<LayerLink<T>>> {
    edges_only(
        db.exec(
            QueryBuilder::select()
                .search()
                .to(layer_id)
                .where_()
                .distance(CountComparison::Equal(1))
                .and()
                .element::<T>()
                .query(),
        )?,
    )
    .map(|element| decode(&element))
    .collect()
}

fn of_target<T: DbType<ValueType = T>>(
    db: &impl DbAccess,
    target_id: DbId,
) -> anyhow::Result<Vec<LayerLink<T>>> {
    edges_only(
        db.exec(
            QueryBuilder::select()
                .search()
                .from(target_id)
                .where_()
                .distance(CountComparison::Equal(1))
                .and()
                .element::<T>()
                .query(),
        )?,
    )
    .map(|element| decode(&element))
    .collect()
}

fn remove(db: &mut impl DbAccess, edge_ids: Vec<DbId>) -> anyhow::Result<()> {
    if !edge_ids.is_empty() {
        db.exec_mut(QueryBuilder::remove().ids(edge_ids).query())?;
    }
    Ok(())
}

fn ensure_layer(db: &impl DbAccess, layer_id: DbId, kinds: &[&str]) -> anyhow::Result<()> {
    let result = db.exec(QueryBuilder::select().ids(layer_id).query())?;
    anyhow::ensure!(
        result.elements.first().is_some_and(|element| kinds
            .iter()
            .any(|kind| graph::is_element_type(element, kind))),
        "{} is not a {} layer",
        layer_id.0,
        kinds.join(" or ")
    );
    Ok(())
}

fn ensure_targets(db: &impl DbAccess, target_ids: &[DbId], kind: &str) -> anyhow::Result<()> {
    for &target_id in target_ids {
        let result = db.exec(QueryBuilder::select().ids(target_id).query())?;
        anyhow::ensure!(
            result
                .elements
                .first()
                .is_some_and(|element| graph::is_element_type(element, kind)),
            "{} is not a {kind}",
            target_id.0
        );
    }
    Ok(())
}

/// The layer's credits, ordered within each role.
pub(crate) fn credits(
    db: &impl DbAccess,
    layer_id: DbId,
) -> anyhow::Result<Vec<LayerLink<LayerCredit>>> {
    let mut links = of_layer::<LayerCredit>(db, layer_id)?;
    links.sort_by(|a, b| {
        (a.edge.credit_type.to_string(), a.edge.artist_order)
            .cmp(&(b.edge.credit_type.to_string(), b.edge.artist_order))
    });
    Ok(links)
}

/// Replaces the layer's credits in `credit_type`, ordered as given; other roles
/// are kept. Like effective credits, an artist holds a role once per detail, so
/// a repeated artist and detail keeps its first place.
pub(crate) fn replace_credits(
    db: &mut DbAnyTransactionMut<'_>,
    layer_id: DbId,
    credit_type: CreditType,
    desired: &[LayerCreditInput],
) -> anyhow::Result<bool> {
    ensure_layer(db, layer_id, &[RELEASE_LAYER, TRACK_LAYER])?;
    let artist_ids: Vec<DbId> = desired.iter().map(|input| input.artist_id).collect();
    ensure_targets(db, &artist_ids, "Artist")?;
    let mut seen = HashSet::new();
    let desired: Vec<&LayerCreditInput> = desired
        .iter()
        .filter(|input| seen.insert((input.artist_id, input.detail.as_deref())))
        .collect();
    let previous: Vec<LayerLink<LayerCredit>> = credits(db, layer_id)?
        .into_iter()
        .filter(|link| link.edge.credit_type == credit_type)
        .collect();
    if previous
        .iter()
        .map(|link| (link.target_id, link.edge.detail.as_deref()))
        .eq(desired
            .iter()
            .map(|input| (input.artist_id, input.detail.as_deref())))
    {
        return Ok(false);
    }
    remove(db, previous.into_iter().map(|link| link.edge_id).collect())?;
    for (artist_order, input) in desired.into_iter().enumerate() {
        insert(
            db,
            layer_id,
            input.artist_id,
            &LayerCredit {
                credit_type,
                detail: input.detail.clone(),
                artist_order: artist_order as u64,
            },
        )?;
    }
    Ok(true)
}

/// The layer's relations, by relation type and then target.
pub(crate) fn relations(
    db: &impl DbAccess,
    layer_id: DbId,
) -> anyhow::Result<Vec<LayerLink<LayerRelation>>> {
    let mut links = of_layer::<LayerRelation>(db, layer_id)?;
    links.sort_by(|a, b| {
        (a.edge.relation_type.to_string(), a.target_id)
            .cmp(&(b.edge.relation_type.to_string(), b.target_id))
    });
    Ok(links)
}

/// Replaces the layer's relations of `relation_type`; other relation types are
/// kept. A repeated artist keeps its first entry. Returns false when the layer
/// already held exactly these.
pub(crate) fn replace_relations(
    db: &mut DbAnyTransactionMut<'_>,
    layer_id: DbId,
    relation_type: ArtistRelationType,
    desired: &[LayerRelationInput],
) -> anyhow::Result<bool> {
    ensure_layer(db, layer_id, &[ARTIST_LAYER])?;
    let artist_ids: Vec<DbId> = desired.iter().map(|input| input.artist_id).collect();
    ensure_targets(db, &artist_ids, "Artist")?;
    let owner = super::owner_id(db, layer_id)?;
    anyhow::ensure!(
        owner.is_none_or(|owner| !artist_ids.contains(&owner)),
        "an artist cannot relate to itself"
    );
    let mut desired_by_artist = std::collections::BTreeMap::new();
    for input in desired {
        desired_by_artist
            .entry(input.artist_id)
            .or_insert_with(|| input.attributes.clone());
    }
    let previous: Vec<LayerLink<LayerRelation>> = relations(db, layer_id)?
        .into_iter()
        .filter(|link| link.edge.relation_type == relation_type)
        .collect();
    if previous
        .iter()
        .map(|link| (link.target_id, link.edge.attributes.clone()))
        .eq(desired_by_artist
            .iter()
            .map(|(id, attributes)| (*id, attributes.clone())))
    {
        return Ok(false);
    }
    remove(db, previous.into_iter().map(|link| link.edge_id).collect())?;
    for (artist_id, attributes) in desired_by_artist {
        insert(
            db,
            layer_id,
            artist_id,
            &LayerRelation {
                relation_type,
                attributes,
            },
        )?;
    }
    Ok(true)
}

/// The layer's genres, in order.
pub(crate) fn genres(
    db: &impl DbAccess,
    layer_id: DbId,
) -> anyhow::Result<Vec<LayerLink<LayerGenre>>> {
    let mut links = of_layer::<LayerGenre>(db, layer_id)?;
    links.sort_by_key(|link| link.edge.genre_order);
    Ok(links)
}

/// Replaces the layer's genres, ordered as given; a repeated genre keeps its
/// first place. Returns false when the layer already held exactly these.
pub(crate) fn replace_genres(
    db: &mut DbAnyTransactionMut<'_>,
    layer_id: DbId,
    genre_ids: &[DbId],
) -> anyhow::Result<bool> {
    ensure_layer(db, layer_id, &[RELEASE_LAYER])?;
    ensure_targets(db, genre_ids, "Genre")?;
    let mut seen = HashSet::new();
    let desired: Vec<DbId> = genre_ids
        .iter()
        .copied()
        .filter(|genre_id| seen.insert(*genre_id))
        .collect();
    let previous = genres(db, layer_id)?;
    if previous
        .iter()
        .map(|link| link.target_id)
        .eq(desired.iter().copied())
    {
        return Ok(false);
    }
    remove(db, previous.into_iter().map(|link| link.edge_id).collect())?;
    for (genre_order, genre_id) in desired.into_iter().enumerate() {
        insert(
            db,
            layer_id,
            genre_id,
            &LayerGenre {
                genre_order: genre_order as u64,
            },
        )?;
    }
    Ok(true)
}

/// The layer's labels, in order.
pub(crate) fn labels(
    db: &impl DbAccess,
    layer_id: DbId,
) -> anyhow::Result<Vec<LayerLink<LayerLabel>>> {
    let mut links = of_layer::<LayerLabel>(db, layer_id)?;
    links.sort_by_key(|link| link.edge.label_order);
    Ok(links)
}

/// Replaces the layer's labels, ordered as given; a repeated label keeps its
/// first entry. Labels no longer referenced by any release or layer are
/// collected. Returns false when the layer already held exactly these.
pub(crate) fn replace_labels(
    db: &mut DbAnyTransactionMut<'_>,
    layer_id: DbId,
    desired: &[LabelLinkInput],
) -> anyhow::Result<bool> {
    ensure_layer(db, layer_id, &[RELEASE_LAYER])?;
    let label_ids: Vec<DbId> = desired.iter().map(|input| input.label_id).collect();
    ensure_targets(db, &label_ids, "Label")?;
    let mut seen = HashSet::new();
    let desired: Vec<(DbId, Option<String>)> = desired
        .iter()
        .filter(|input| seen.insert(input.label_id))
        .map(|input| {
            let catalog_number = input
                .catalog_number
                .as_deref()
                .map(str::trim)
                .filter(|catalog_number| !catalog_number.is_empty())
                .map(str::to_string);
            (input.label_id, catalog_number)
        })
        .collect();
    let previous = labels(db, layer_id)?;
    if previous
        .iter()
        .map(|link| (link.target_id, link.edge.catalog_number.clone()))
        .eq(desired.iter().cloned())
    {
        return Ok(false);
    }
    let previous_label_ids: Vec<DbId> = previous.iter().map(|link| link.target_id).collect();
    remove(db, previous.into_iter().map(|link| link.edge_id).collect())?;
    for (label_order, (label_id, catalog_number)) in desired.into_iter().enumerate() {
        insert(
            db,
            layer_id,
            label_id,
            &LayerLabel {
                label_order: label_order as u64,
                catalog_number,
            },
        )?;
    }
    super::super::super::labels::collect_unreferenced(db, &previous_label_ids)?;
    Ok(true)
}

/// Whether any layer references `target_id` through a relationship edge.
pub(crate) fn is_referenced(db: &impl DbAccess, target_id: DbId) -> anyhow::Result<bool> {
    Ok(!of_target::<LayerCredit>(db, target_id)?.is_empty()
        || !of_target::<LayerRelation>(db, target_id)?.is_empty()
        || !of_target::<LayerGenre>(db, target_id)?.is_empty()
        || !of_target::<LayerLabel>(db, target_id)?.is_empty())
}

/// How many layer edges reference `target_id`.
pub(crate) fn reference_count(db: &impl DbAccess, target_id: DbId) -> anyhow::Result<u64> {
    Ok((of_target::<LayerCredit>(db, target_id)?.len()
        + of_target::<LayerRelation>(db, target_id)?.len()
        + of_target::<LayerGenre>(db, target_id)?.len()
        + of_target::<LayerLabel>(db, target_id)?.len()) as u64)
}

/// The artists among `removing` that must stay: a layer of an entity that
/// stays references them, or a walk from an artist that stays reaches them, as
/// the walk from a surviving root reaches its artists' relation targets.
pub(crate) fn spared_artists(
    db: &impl DbAccess,
    removing: &HashSet<DbId>,
) -> anyhow::Result<HashSet<DbId>> {
    let mut spared = HashSet::new();
    loop {
        let mut newly_spared = Vec::new();
        for &artist_id in removing {
            if spared.contains(&artist_id) {
                continue;
            }
            let layer_ids = of_target::<LayerCredit>(db, artist_id)?
                .into_iter()
                .map(|link| link.layer_id)
                .chain(
                    of_target::<LayerRelation>(db, artist_id)?
                        .into_iter()
                        .map(|link| link.layer_id),
                );
            for layer_id in layer_ids {
                if super::owner_id(db, layer_id)?
                    .is_some_and(|owner| !removing.contains(&owner) || spared.contains(&owner))
                {
                    newly_spared.push(artist_id);
                    break;
                }
            }
        }
        if newly_spared.is_empty() {
            return Ok(spared);
        }
        for artist_id in newly_spared {
            if !spared.insert(artist_id) {
                continue;
            }
            let reachable: Vec<Artist> = db
                .exec(
                    QueryBuilder::select()
                        .elements::<Artist>()
                        .search()
                        .from(artist_id)
                        .query(),
                )?
                .try_into()?;
            spared.extend(
                reachable
                    .into_iter()
                    .filter_map(|artist| artist.db_id.map(DbId::from))
                    .filter(|id| removing.contains(id)),
            );
        }
    }
}

/// Labels the given layers reference, for collection once the layers are gone.
pub(crate) fn label_ids_of(db: &impl DbAccess, layer_ids: &[DbId]) -> anyhow::Result<Vec<DbId>> {
    let mut label_ids = Vec::new();
    for &layer_id in layer_ids {
        label_ids.extend(
            of_layer::<LayerLabel>(db, layer_id)?
                .into_iter()
                .map(|link| link.target_id),
        );
    }
    Ok(label_ids)
}

/// Points every layer edge referencing artist `loser` at `winner` instead,
/// keeping its properties. An edge the layer already holds for `winner` in the
/// same role or relation type is dropped instead of repeated.
pub(crate) fn retarget_artist(
    db: &mut impl DbAccess,
    winner: DbId,
    loser: DbId,
) -> anyhow::Result<()> {
    if winner == loser {
        return Ok(());
    }
    let mut winner_credits: HashSet<(DbId, CreditType, Option<String>)> =
        of_target::<LayerCredit>(db, winner)?
            .into_iter()
            .map(|link| (link.layer_id, link.edge.credit_type, link.edge.detail))
            .collect();
    for link in of_target::<LayerCredit>(db, loser)? {
        let (credit_type, detail) = link.edge.role();
        if winner_credits.insert((link.layer_id, credit_type, detail.map(str::to_string))) {
            insert(db, link.layer_id, winner, &link.edge)?;
        }
        remove(db, vec![link.edge_id])?;
    }

    let mut winner_relations: HashSet<(DbId, ArtistRelationType)> =
        of_target::<LayerRelation>(db, winner)?
            .into_iter()
            .map(|link| (link.layer_id, link.edge.relation_type))
            .collect();
    for link in of_target::<LayerRelation>(db, loser)? {
        if winner_relations.insert((link.layer_id, link.edge.relation_type)) {
            insert(db, link.layer_id, winner, &link.edge)?;
        }
        remove(db, vec![link.edge_id])?;
    }
    // The winner's own layers cannot relate it to itself.
    let winner_layers = super::ids_for_entity(db, winner)?;
    remove(
        db,
        of_target::<LayerRelation>(db, winner)?
            .into_iter()
            .filter(|link| winner_layers.contains(&link.layer_id))
            .map(|link| link.edge_id)
            .collect(),
    )
}

/// Replaces `to_layer`'s relationship edges with copies of `from_layer`'s.
pub(crate) fn copy_all(
    db: &mut impl DbAccess,
    from_layer: DbId,
    to_layer: DbId,
) -> anyhow::Result<()> {
    let mut stale = Vec::new();
    stale.extend(
        of_layer::<LayerCredit>(db, to_layer)?
            .into_iter()
            .map(|link| link.edge_id),
    );
    stale.extend(
        of_layer::<LayerRelation>(db, to_layer)?
            .into_iter()
            .map(|link| link.edge_id),
    );
    stale.extend(
        of_layer::<LayerGenre>(db, to_layer)?
            .into_iter()
            .map(|link| link.edge_id),
    );
    let stale_labels = of_layer::<LayerLabel>(db, to_layer)?;
    let stale_label_ids: Vec<DbId> = stale_labels.iter().map(|link| link.target_id).collect();
    stale.extend(stale_labels.into_iter().map(|link| link.edge_id));
    remove(db, stale)?;

    for link in of_layer::<LayerCredit>(db, from_layer)? {
        insert(db, to_layer, link.target_id, &link.edge)?;
    }
    for link in of_layer::<LayerRelation>(db, from_layer)? {
        insert(db, to_layer, link.target_id, &link.edge)?;
    }
    for link in of_layer::<LayerGenre>(db, from_layer)? {
        insert(db, to_layer, link.target_id, &link.edge)?;
    }
    for link in of_layer::<LayerLabel>(db, from_layer)? {
        insert(db, to_layer, link.target_id, &link.edge)?;
    }
    super::super::super::labels::collect_unreferenced(db, &stale_label_ids)
}

#[cfg(test)]
mod tests {
    use agdb::DbAny;

    use super::*;
    use crate::db::{
        MetadataLayer,
        labels::{
            ResolveLabel,
            get_by_id as get_label,
        },
        metadata::{
            cascade_remove_entities,
            layers::{
                layer_id,
                upsert,
            },
        },
        test_db::{
            insert_artist,
            insert_release,
            insert_track,
            new_test_db,
        },
    };

    fn layer_of(db: &mut DbAny, node_id: DbId, source_id: &str) -> anyhow::Result<DbId> {
        upsert(
            db,
            node_id,
            &MetadataLayer::new(source_id, Default::default(), 1),
        )?;
        layer_id(db, node_id, source_id)?.ok_or_else(|| anyhow::anyhow!("layer missing"))
    }

    fn credit(artist_id: DbId) -> LayerCreditInput {
        LayerCreditInput {
            artist_id,
            detail: None,
        }
    }

    fn label(db: &mut DbAny, name: &str) -> anyhow::Result<DbId> {
        crate::db::labels::resolve(
            db,
            &ResolveLabel {
                name,
                external_id: None,
            },
        )
    }

    fn credited(db: &DbAny, layer_id: DbId) -> anyhow::Result<Vec<(CreditType, DbId, u64)>> {
        Ok(credits(db, layer_id)?
            .into_iter()
            .map(|link| {
                (
                    link.edge.credit_type,
                    link.target_id,
                    link.edge.artist_order,
                )
            })
            .collect())
    }

    #[test]
    fn credits_replace_one_role_and_keep_the_others() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let track_id = insert_track(&mut db, "Track")?;
        let [a, b, c] = [
            insert_artist(&mut db, "A")?,
            insert_artist(&mut db, "B")?,
            insert_artist(&mut db, "C")?,
        ];
        let layer = layer_of(&mut db, track_id, "provider")?;

        db.transaction_mut(|t| {
            replace_credits(t, layer, CreditType::Artist, &[credit(a), credit(b)])?;
            replace_credits(
                t,
                layer,
                CreditType::Instrumentalist,
                &[LayerCreditInput {
                    artist_id: c,
                    detail: Some("piano".to_string()),
                }],
            )
        })?;
        db.transaction_mut(|t| replace_credits(t, layer, CreditType::Artist, &[credit(b)]))?;

        assert_eq!(
            credited(&db, layer)?,
            vec![
                (CreditType::Artist, b, 0),
                (CreditType::Instrumentalist, c, 0),
            ]
        );
        let instrumentalist = &credits(&db, layer)?[1].edge;
        assert_eq!(instrumentalist.detail.as_deref(), Some("piano"));
        assert!(crate::db::graph::direct_edges_from(&db, layer)?.is_empty());
        Ok(())
    }

    #[test]
    fn replacing_credits_reorders_them() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_id = insert_release(&mut db, "Release")?;
        let [a, b, c] = [
            insert_artist(&mut db, "A")?,
            insert_artist(&mut db, "B")?,
            insert_artist(&mut db, "C")?,
        ];
        let layer = layer_of(&mut db, release_id, "provider")?;

        db.transaction_mut(|t| {
            replace_credits(
                t,
                layer,
                CreditType::Artist,
                &[credit(a), credit(b), credit(c)],
            )
        })?;
        db.transaction_mut(|t| {
            replace_credits(
                t,
                layer,
                CreditType::Artist,
                &[credit(c), credit(a), credit(b)],
            )
        })?;

        assert_eq!(
            credited(&db, layer)?,
            vec![
                (CreditType::Artist, c, 0),
                (CreditType::Artist, a, 1),
                (CreditType::Artist, b, 2),
            ]
        );
        Ok(())
    }

    #[test]
    fn an_entitys_layers_exclude_layers_its_edges_point_into() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let member = insert_artist(&mut db, "Member")?;
        let band = insert_artist(&mut db, "Band")?;
        let track_id = insert_track(&mut db, "Track")?;
        let member_layer = layer_of(&mut db, member, "provider")?;
        let track_layer = layer_of(&mut db, track_id, "provider")?;
        db.transaction_mut(|t| {
            replace_relations(
                t,
                member_layer,
                ArtistRelationType::MemberOf,
                &[LayerRelationInput {
                    artist_id: band,
                    attributes: None,
                }],
            )?;
            replace_credits(t, track_layer, CreditType::Artist, &[credit(band)])
        })?;

        assert!(super::super::ids_for_entity(&db, band)?.is_empty());
        assert!(super::super::get_for_entity(&db, band)?.is_empty());
        assert_eq!(layer_id(&db, band, "provider")?, None);
        assert_eq!(super::super::entity_ids_for_source(&db, "provider")?, {
            let mut owners = vec![member, track_id];
            owners.sort_unstable();
            owners
        });

        cascade_remove_entities(&mut db, &[band])?;
        assert_eq!(
            super::super::ids_for_entity(&db, member)?,
            vec![member_layer]
        );
        assert_eq!(
            super::super::ids_for_entity(&db, track_id)?,
            vec![track_layer]
        );
        Ok(())
    }

    #[test]
    fn an_artist_holds_a_role_once_per_detail() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let track_id = insert_track(&mut db, "Track")?;
        let winner = insert_artist(&mut db, "Winner")?;
        let loser = insert_artist(&mut db, "Loser")?;
        let layer = layer_of(&mut db, track_id, "provider")?;
        let played = |artist_id, detail: &str| LayerCreditInput {
            artist_id,
            detail: Some(detail.to_string()),
        };

        db.transaction_mut(|t| {
            replace_credits(
                t,
                layer,
                CreditType::Instrumentalist,
                &[
                    played(winner, "guitar"),
                    played(loser, "guitar"),
                    played(loser, "bass"),
                    played(winner, "guitar"),
                ],
            )
        })?;
        assert_eq!(credits(&db, layer)?.len(), 3);

        db.transaction_mut(|t| retarget_artist(t, winner, loser))?;

        let roles: Vec<(DbId, Option<String>, u64)> = credits(&db, layer)?
            .into_iter()
            .map(|link| (link.target_id, link.edge.detail, link.edge.artist_order))
            .collect();
        assert_eq!(
            roles,
            vec![
                (winner, Some("guitar".to_string()), 0),
                (winner, Some("bass".to_string()), 2),
            ]
        );
        Ok(())
    }

    #[test]
    fn replacement_rejects_the_wrong_layer_or_target_kind() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let artist_id = insert_artist(&mut db, "Artist")?;
        let release_id = insert_release(&mut db, "Release")?;
        let artist_layer = layer_of(&mut db, artist_id, "provider")?;
        let release_layer = layer_of(&mut db, release_id, "provider")?;

        assert!(
            db.transaction_mut(|t| replace_credits(
                t,
                artist_layer,
                CreditType::Artist,
                &[credit(artist_id)]
            ))
            .is_err()
        );
        assert!(
            db.transaction_mut(|t| replace_credits(
                t,
                release_layer,
                CreditType::Artist,
                &[credit(release_id)]
            ))
            .is_err()
        );
        assert!(
            db.transaction_mut(|t| replace_genres(t, release_layer, &[artist_id]))
                .is_err()
        );
        assert!(
            db.transaction_mut(|t| replace_relations(
                t,
                release_layer,
                ArtistRelationType::MemberOf,
                &[]
            ))
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn genres_and_labels_keep_their_order_and_drop_repeats() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_id = insert_release(&mut db, "Release")?;
        let layer = layer_of(&mut db, release_id, "provider")?;
        let jazz = crate::db::genres::resolve_by_name(&mut db, "Jazz")?;
        let bebop = crate::db::genres::resolve_by_name(&mut db, "Bebop")?;
        let blue_note = label(&mut db, "Blue Note")?;
        let impulse = label(&mut db, "Impulse!")?;

        db.transaction_mut(|t| {
            replace_genres(t, layer, &[bebop, jazz, bebop])?;
            replace_labels(
                t,
                layer,
                &[
                    LabelLinkInput {
                        label_id: impulse,
                        catalog_number: Some(" A-77 ".to_string()),
                    },
                    LabelLinkInput {
                        label_id: blue_note,
                        catalog_number: None,
                    },
                ],
            )
        })?;

        let genre_ids: Vec<DbId> = genres(&db, layer)?
            .into_iter()
            .map(|link| link.target_id)
            .collect();
        assert_eq!(genre_ids, vec![bebop, jazz]);
        let labels: Vec<(DbId, Option<String>)> = labels(&db, layer)?
            .into_iter()
            .map(|link| (link.target_id, link.edge.catalog_number))
            .collect();
        assert_eq!(
            labels,
            vec![(impulse, Some("A-77".to_string())), (blue_note, None)]
        );
        Ok(())
    }

    #[test]
    fn a_label_survives_while_a_layer_references_it() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_id = insert_release(&mut db, "Release")?;
        let layer = layer_of(&mut db, release_id, "provider")?;
        let blue_note = label(&mut db, "Blue Note")?;
        crate::db::labels::sync_release_labels(
            &mut db,
            release_id,
            &[crate::db::labels::LabelInput {
                name: "Blue Note".to_string(),
                catalog_number: None,
                external_id: None,
            }],
        )?;
        db.transaction_mut(|t| {
            replace_labels(
                t,
                layer,
                &[LabelLinkInput {
                    label_id: blue_note,
                    catalog_number: None,
                }],
            )
        })?;

        crate::db::labels::sync_release_labels(&mut db, release_id, &[])?;
        assert!(
            get_label(&db, blue_note)?.is_some(),
            "the layer still names it"
        );

        db.transaction_mut(|t| replace_labels(t, layer, &[]))?;
        assert!(get_label(&db, blue_note)?.is_none(), "nothing names it now");
        Ok(())
    }

    #[test]
    fn removing_a_layer_or_its_entity_removes_its_edges() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_id = insert_release(&mut db, "Release")?;
        let artist_id = insert_artist(&mut db, "Artist")?;
        let blue_note = label(&mut db, "Blue Note")?;
        let layer = layer_of(&mut db, release_id, "provider")?;
        db.transaction_mut(|t| {
            replace_credits(t, layer, CreditType::Artist, &[credit(artist_id)])?;
            replace_labels(
                t,
                layer,
                &[LabelLinkInput {
                    label_id: blue_note,
                    catalog_number: None,
                }],
            )
        })?;

        db.transaction_mut(|t| super::super::remove_inside_tx(t, release_id, "provider"))?;
        assert!(!is_referenced(&db, artist_id)?);
        assert!(get_label(&db, blue_note)?.is_none());

        let layer = layer_of(&mut db, release_id, "provider")?;
        db.transaction_mut(|t| {
            replace_credits(t, layer, CreditType::Artist, &[credit(artist_id)])
        })?;
        assert!(is_referenced(&db, artist_id)?);
        cascade_remove_entities(&mut db, &[release_id])?;
        assert!(!is_referenced(&db, artist_id)?);
        assert!(db.exec(QueryBuilder::select().ids(layer).query()).is_err());
        Ok(())
    }

    #[test]
    fn retargeting_an_artist_moves_edges_without_repeating_them() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let track_id = insert_track(&mut db, "Track")?;
        let winner = insert_artist(&mut db, "Winner")?;
        let loser = insert_artist(&mut db, "Loser")?;
        let other = insert_artist(&mut db, "Other")?;
        let first = layer_of(&mut db, track_id, "first")?;
        let second = layer_of(&mut db, track_id, "second")?;
        let other_layer = layer_of(&mut db, other, "provider")?;
        let winner_layer = layer_of(&mut db, winner, "provider")?;
        db.transaction_mut(|t| {
            replace_credits(
                t,
                first,
                CreditType::Artist,
                &[credit(other), credit(loser)],
            )?;
            replace_credits(
                t,
                second,
                CreditType::Artist,
                &[credit(winner), credit(loser)],
            )?;
            replace_relations(
                t,
                other_layer,
                ArtistRelationType::MemberOf,
                &[LayerRelationInput {
                    artist_id: loser,
                    attributes: Some("bass".to_string()),
                }],
            )?;
            replace_relations(
                t,
                winner_layer,
                ArtistRelationType::MemberOf,
                &[LayerRelationInput {
                    artist_id: loser,
                    attributes: None,
                }],
            )
        })?;

        db.transaction_mut(|t| retarget_artist(t, winner, loser))?;

        assert_eq!(
            credited(&db, first)?,
            vec![
                (CreditType::Artist, other, 0),
                (CreditType::Artist, winner, 1)
            ]
        );
        assert_eq!(
            credited(&db, second)?,
            vec![(CreditType::Artist, winner, 0)]
        );
        let moved = relations(&db, other_layer)?;
        assert_eq!(moved.len(), 1);
        assert_eq!(moved[0].target_id, winner);
        assert_eq!(moved[0].edge.attributes.as_deref(), Some("bass"));
        assert!(relations(&db, winner_layer)?.is_empty(), "no self relation");
        assert!(!is_referenced(&db, loser)?);
        Ok(())
    }

    #[test]
    fn copying_replaces_the_target_layers_edges() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let from_release = insert_release(&mut db, "From")?;
        let to_release = insert_release(&mut db, "To")?;
        let artist_id = insert_artist(&mut db, "Artist")?;
        let stale_artist = insert_artist(&mut db, "Stale")?;
        let jazz = crate::db::genres::resolve_by_name(&mut db, "Jazz")?;
        let from_layer = layer_of(&mut db, from_release, "provider")?;
        let to_layer = layer_of(&mut db, to_release, "provider")?;
        db.transaction_mut(|t| {
            replace_credits(t, from_layer, CreditType::Artist, &[credit(artist_id)])?;
            replace_genres(t, from_layer, &[jazz])?;
            replace_credits(t, to_layer, CreditType::Producer, &[credit(stale_artist)])
        })?;

        db.transaction_mut(|t| copy_all(t, from_layer, to_layer))?;

        assert_eq!(
            credited(&db, to_layer)?,
            vec![(CreditType::Artist, artist_id, 0)]
        );
        assert_eq!(genres(&db, to_layer)?[0].target_id, jazz);
        assert!(!is_referenced(&db, stale_artist)?);
        Ok(())
    }
}
