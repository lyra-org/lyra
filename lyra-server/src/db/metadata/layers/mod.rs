// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

pub(crate) mod edges;
mod elements;

use std::collections::BTreeMap;

use agdb::{
    CountComparison,
    DbAny,
    DbAnyTransactionMut,
    DbId,
    QueryBuilder,
};
use anyhow::Context;
use serde::de::DeserializeOwned;
use serde_json::Value;

use self::elements::{
    ArtistLayer,
    LayerElement,
    ReleaseLayer,
    TrackLayer,
};
use super::super::{
    DbAccess,
    MetadataField,
    artists::{
        ArtistType,
        relations::ArtistRelationType,
    },
    credits::CreditType,
    entities::MetadataEntityType,
    labels::{
        LabelExternalIdInput,
        LabelInput,
        LabelLinkInput,
    },
    releases::{
        ReleaseType,
        normalize_release_date,
    },
};

/// Layer values keyed by field; `null` explicitly clears a field.
pub(crate) type LayerFields = BTreeMap<MetadataField, Value>;

/// An artist layer's incoming relations by type: the artists that relate to
/// the layer's artist. A listed type is supplied; an empty list clears it.
pub(crate) type LayerRelations = BTreeMap<ArtistRelationType, Vec<edges::LayerRelationInput>>;

/// A release or track layer's credits by role, each in credit order. A listed
/// role is supplied; an empty list clears it.
pub(crate) type LayerCredits = BTreeMap<CreditType, Vec<edges::LayerCreditInput>>;

/// One source's opinion on an entity's fields; stored as the entity's
/// [`ReleaseLayer`], [`TrackLayer`], or [`ArtistLayer`].
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct MetadataLayer {
    pub(crate) source_id: String,
    pub(crate) fields: LayerFields,
    pub(crate) relations: LayerRelations,
    pub(crate) credits: LayerCredits,
    pub(crate) updated_at: u64,
}

/// For each type `incoming` lists, `stored`'s artists followed by the incoming
/// ones it lacks.
pub(crate) fn union_relations(stored: &LayerRelations, incoming: LayerRelations) -> LayerRelations {
    incoming
        .into_iter()
        .map(|(relation_type, found)| {
            let mut combined = stored.get(&relation_type).cloned().unwrap_or_default();
            for input in found {
                if !combined
                    .iter()
                    .any(|existing| existing.artist_id == input.artist_id)
                {
                    combined.push(input);
                }
            }
            (relation_type, combined)
        })
        .collect()
}

impl MetadataLayer {
    pub(crate) fn new(source_id: impl Into<String>, fields: LayerFields, updated_at: u64) -> Self {
        Self {
            source_id: source_id.into(),
            fields,
            relations: LayerRelations::new(),
            credits: LayerCredits::new(),
            updated_at,
        }
    }

    pub(crate) fn with_relations(mut self, relations: LayerRelations) -> Self {
        self.relations = relations;
        self
    }

    pub(crate) fn with_credits(mut self, credits: LayerCredits) -> Self {
        self.credits = credits;
        self
    }
}

/// Validates a layer value and returns its stored form.
pub(crate) fn normalize_value(field: MetadataField, value: Value) -> anyhow::Result<Value> {
    anyhow::ensure!(
        field.is_layered(),
        "metadata layers supply '{field}' per role or relation type, not as one value"
    );
    if value.is_null() {
        anyhow::ensure!(!field.is_required(), "'{field}' cannot be cleared");
        return Ok(value);
    }
    match field {
        MetadataField::ReleaseTitle | MetadataField::TrackTitle | MetadataField::ArtistName => {
            let title: String = decode(field, &value)?;
            anyhow::ensure!(!title.trim().is_empty(), "'{field}' cannot be empty");
        }
        MetadataField::SortTitle | MetadataField::SortName | MetadataField::Description => {
            decode::<String>(field, &value)?;
        }
        MetadataField::ReleaseType => {
            decode::<ReleaseType>(field, &value)?;
        }
        MetadataField::ArtistType => {
            decode::<ArtistType>(field, &value)?;
        }
        MetadataField::ReleaseDate => {
            let date: String = decode(field, &value)?;
            let normalized = normalize_release_date(&date).with_context(|| {
                format!("'{field}' must use YYYY, YYYY-MM, or YYYY-MM-DD, got '{date}'")
            })?;
            return Ok(Value::String(normalized));
        }
        MetadataField::Year
        | MetadataField::Disc
        | MetadataField::DiscTotal
        | MetadataField::Track
        | MetadataField::TrackTotal => {
            decode::<u32>(field, &value)?;
        }
        MetadataField::Genres => {
            decode::<Vec<String>>(field, &value)?;
        }
        MetadataField::Labels => {
            label_inputs(&value)?;
        }
        MetadataField::Credits | MetadataField::Relations => {
            unreachable!("graph-only fields are rejected above")
        }
    }
    Ok(value)
}

pub(crate) fn decode<T: DeserializeOwned>(
    field: MetadataField,
    value: &Value,
) -> anyhow::Result<T> {
    serde_json::from_value(value.clone())
        .with_context(|| format!("invalid value for '{field}': {value}"))
}

/// Decodes a `labels` value: objects with a required `name`, an optional
/// `catalog_number`, and an optional complete `external_id`.
pub(crate) fn label_inputs(value: &Value) -> anyhow::Result<Vec<LabelInput>> {
    let entries = value
        .as_array()
        .with_context(|| format!("'labels' must be an array, got {value}"))?;
    entries.iter().map(label_input).collect()
}

fn label_input(entry: &Value) -> anyhow::Result<LabelInput> {
    let object = entry
        .as_object()
        .with_context(|| format!("label entries must be objects, got {entry}"))?;
    let name = object
        .get("name")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .with_context(|| format!("label entry needs a non-empty 'name': {entry}"))?;
    let catalog_number = match object.get("catalog_number") {
        None | Some(Value::Null) => None,
        Some(Value::String(catalog_number)) => {
            Some(catalog_number.trim()).filter(|catalog_number| !catalog_number.is_empty())
        }
        Some(other) => anyhow::bail!("label 'catalog_number' must be a string, got {other}"),
    };
    let external_id = match object.get("external_id") {
        None | Some(Value::Null) => None,
        Some(external_id) => Some(label_external_id(external_id)?),
    };
    Ok(LabelInput {
        name: name.to_string(),
        catalog_number: catalog_number.map(str::to_string),
        external_id,
    })
}

fn label_external_id(value: &Value) -> anyhow::Result<LabelExternalIdInput> {
    let part = |key: &str| {
        value
            .get(key)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|part| !part.is_empty())
            .map(str::to_string)
            .with_context(|| format!("label 'external_id' needs a non-empty '{key}': {value}"))
    };
    Ok(LabelExternalIdInput {
        provider_id: part("provider_id")?,
        id_type: part("id_type")?,
        id_value: part("id_value")?,
    })
}

fn entity_type(db: &impl DbAccess, node_id: DbId) -> anyhow::Result<MetadataEntityType> {
    super::super::entities::metadata_entity_type(db, node_id)?.with_context(|| {
        format!(
            "metadata layers belong to releases, tracks, and artists, not {}",
            node_id.0
        )
    })
}

/// The entity's layers; a stored layer that no longer validates is skipped
/// until its source replaces it.
pub(crate) fn get_for_entity(
    db: &impl DbAccess,
    node_id: DbId,
) -> anyhow::Result<Vec<MetadataLayer>> {
    match super::super::entities::metadata_entity_type(db, node_id)? {
        Some(MetadataEntityType::Release) => load::<ReleaseLayer>(db, node_id),
        Some(MetadataEntityType::Track) => load::<TrackLayer>(db, node_id),
        Some(MetadataEntityType::Artist) => load::<ArtistLayer>(db, node_id),
        None => Ok(Vec::new()),
    }
}

// An entity's own layers hang off untyped edges. Its typed relationship edges
// lead into other entities' layers, so lookups never cross them.
fn load<T: LayerElement>(db: &impl DbAccess, node_id: DbId) -> anyhow::Result<Vec<MetadataLayer>> {
    let result = db.exec(
        QueryBuilder::select()
            .search()
            .from(node_id)
            .where_()
            .neighbor()
            .and()
            .element::<T>()
            .and()
            .not_beyond()
            .where_()
            .edge()
            .and()
            .keys("db_element_id")
            .end_where()
            .end_where()
            .query(),
    )?;
    Ok(result
        .elements
        .iter()
        .filter_map(|element| {
            T::from_db_element(element)
                .map_err(anyhow::Error::from)
                .and_then(|layer| layer.to_layer(db))
                .inspect_err(|err| {
                    tracing::warn!(
                        layer_id = element.id.0,
                        error = %format!("{err:#}"),
                        "skipping metadata layer that no longer validates"
                    );
                })
                .ok()
        })
        .collect())
}

#[cfg(test)]
pub(crate) fn fields_from_json(value: Value) -> LayerFields {
    serde_json::from_value(value).expect("test layer fields deserialize")
}

/// Ids of every layer attached to an entity, valid or not.
pub(crate) fn ids_for_entity(db: &impl DbAccess, node_id: DbId) -> anyhow::Result<Vec<DbId>> {
    Ok(db
        .exec(
            QueryBuilder::search()
                .from(node_id)
                .where_()
                .neighbor()
                .and()
                .where_()
                .element::<ReleaseLayer>()
                .or()
                .element::<TrackLayer>()
                .or()
                .element::<ArtistLayer>()
                .end_where()
                .and()
                .not_beyond()
                .where_()
                .edge()
                .and()
                .keys("db_element_id")
                .end_where()
                .end_where()
                .query(),
        )?
        .ids())
}

fn find_id<T: LayerElement>(
    db: &impl DbAccess,
    node_id: DbId,
    source_id: &str,
) -> anyhow::Result<Option<DbId>> {
    Ok(db
        .exec(
            QueryBuilder::search()
                .from(node_id)
                .where_()
                .neighbor()
                .and()
                .element::<T>()
                .and()
                .key("source_id")
                .value(source_id)
                .and()
                .not_beyond()
                .where_()
                .edge()
                .and()
                .keys("db_element_id")
                .end_where()
                .end_where()
                .query(),
        )?
        .ids()
        .first()
        .copied())
}

pub(crate) fn entity_ids_for_source(
    db: &impl DbAccess,
    source_id: &str,
) -> anyhow::Result<Vec<DbId>> {
    let layer_ids = super::super::indexes::node_ids(db, "source_id", source_id)?;
    let mut entity_ids = Vec::with_capacity(layer_ids.len());
    for layer_id in layer_ids {
        entity_ids.extend(owner_id(db, layer_id)?);
    }
    entity_ids.sort_unstable();
    entity_ids.dedup();
    Ok(entity_ids)
}

/// Saves `layer` as its source's layer on the entity, replacing any previous
/// one. Returns false when the stored layer already supplied the same values.
pub(crate) fn upsert(db: &mut DbAny, node_id: DbId, layer: &MetadataLayer) -> anyhow::Result<bool> {
    db.transaction_mut(|t| upsert_inside_tx(t, node_id, layer))
}

pub(crate) fn upsert_inside_tx(
    db: &mut DbAnyTransactionMut<'_>,
    node_id: DbId,
    layer: &MetadataLayer,
) -> anyhow::Result<bool> {
    // A save replaces only the relation types and credit roles it supplies;
    // the source's others stay as stored.
    let mut layer = layer.clone();
    if let Some(stored) = get_for_entity(db, node_id)?
        .into_iter()
        .find(|stored| stored.source_id == layer.source_id)
    {
        for (relation_type, inputs) in stored.relations {
            layer.relations.entry(relation_type).or_insert(inputs);
        }
        for (credit_type, inputs) in stored.credits {
            layer.credits.entry(credit_type).or_insert(inputs);
        }
    }
    save_any(db, node_id, &layer, None)
}

/// Saves `layer` as its source's whole layer: unlike [`upsert_inside_tx`], the
/// relation types and credit roles it does not supply go, and a layer that
/// supplies nothing is removed. A supplied `labels` field links `labels` by id
/// when given, rather than resolving the field's names.
pub(crate) fn replace_inside_tx(
    db: &mut DbAnyTransactionMut<'_>,
    node_id: DbId,
    layer: &MetadataLayer,
    labels: Option<&[LabelLinkInput]>,
) -> anyhow::Result<()> {
    if layer.fields.is_empty() && layer.relations.is_empty() && layer.credits.is_empty() {
        remove_inside_tx(db, node_id, &layer.source_id)?;
        return Ok(());
    }
    save_any(db, node_id, layer, labels)?;
    Ok(())
}

fn save_any(
    db: &mut DbAnyTransactionMut<'_>,
    node_id: DbId,
    layer: &MetadataLayer,
    labels: Option<&[LabelLinkInput]>,
) -> anyhow::Result<bool> {
    match entity_type(db, node_id)? {
        MetadataEntityType::Release => save::<ReleaseLayer>(db, node_id, layer, labels),
        MetadataEntityType::Track => save::<TrackLayer>(db, node_id, layer, labels),
        MetadataEntityType::Artist => save::<ArtistLayer>(db, node_id, layer, labels),
    }
    .with_context(|| format!("invalid '{}' metadata layer", layer.source_id))
}

fn save<T: LayerElement>(
    db: &mut DbAnyTransactionMut<'_>,
    node_id: DbId,
    layer: &MetadataLayer,
    labels: Option<&[LabelLinkInput]>,
) -> anyhow::Result<bool> {
    let (layer_id, values_changed) = store(db, node_id, T::from_layer(layer)?)?;
    let edges_changed = T::save_edges(db, layer_id, layer, labels)?;
    if edges_changed && !values_changed {
        set_updated_at(db, layer_id, layer.updated_at)?;
    }
    Ok(values_changed || edges_changed)
}

fn set_updated_at(db: &mut impl DbAccess, layer_id: DbId, updated_at: u64) -> anyhow::Result<()> {
    db.exec_mut(
        QueryBuilder::insert()
            .values_uniform([("updated_at", updated_at).into()])
            .ids(layer_id)
            .query(),
    )?;
    Ok(())
}

/// The layer `source_id` contributed to an entity; unlike [`get_for_entity`],
/// a stored layer that no longer validates is an error.
pub(crate) fn find_valid(
    db: &impl DbAccess,
    node_id: DbId,
    source_id: &str,
) -> anyhow::Result<Option<MetadataLayer>> {
    match super::super::entities::metadata_entity_type(db, node_id)? {
        Some(MetadataEntityType::Release) => load_one::<ReleaseLayer>(db, node_id, source_id),
        Some(MetadataEntityType::Track) => load_one::<TrackLayer>(db, node_id, source_id),
        Some(MetadataEntityType::Artist) => load_one::<ArtistLayer>(db, node_id, source_id),
        None => Ok(None),
    }
}

fn load_one<T: LayerElement>(
    db: &impl DbAccess,
    node_id: DbId,
    source_id: &str,
) -> anyhow::Result<Option<MetadataLayer>> {
    let Some(layer_id) = find_id::<T>(db, node_id, source_id)? else {
        return Ok(None);
    };
    let stored = db.exec(QueryBuilder::select().ids(layer_id).query())?;
    let element = stored.elements.first().context("metadata layer is gone")?;
    T::from_db_element(element)
        .map_err(anyhow::Error::from)
        .and_then(|layer| layer.to_layer(db))
        .map(Some)
        .with_context(|| format!("the stored '{source_id}' metadata layer is invalid"))
}

/// The labels a layer links, by id, in order.
pub(crate) fn label_links(
    db: &impl DbAccess,
    layer_id: DbId,
) -> anyhow::Result<Vec<LabelLinkInput>> {
    Ok(edges::labels(db, layer_id)?
        .into_iter()
        .map(|link| LabelLinkInput {
            label_id: link.target_id,
            catalog_number: link.edge.catalog_number,
        })
        .collect())
}

/// Fills the keys `to_node`'s layer from `source_id` does not supply with
/// `from_node`'s, or copies `from_node`'s layer when `to_node` has none. A
/// filled layer is as recent as the newer of the two. Either layer failing to
/// validate is an error.
pub(crate) fn fill_inside_tx(
    db: &mut DbAnyTransactionMut<'_>,
    from_node: DbId,
    to_node: DbId,
    source_id: &str,
) -> anyhow::Result<()> {
    let Some(from) = find_valid(db, from_node, source_id)? else {
        return Ok(());
    };
    let from_id = layer_id(db, from_node, source_id)?.context("filled layer is gone")?;
    let Some(mut to) = find_valid(db, to_node, source_id)? else {
        return copy_inside_tx(db, from_node, to_node, source_id);
    };
    let to_id = layer_id(db, to_node, source_id)?.context("filling layer is gone")?;
    to.updated_at = to.updated_at.max(from.updated_at);
    let labels_from = if to.fields.contains_key(&MetadataField::Labels) {
        to_id
    } else {
        from_id
    };
    for (field, value) in from.fields {
        to.fields.entry(field).or_insert(value);
    }
    for (relation_type, mut inputs) in from.relations {
        // A type that only related to `to_node` has nothing left to say.
        let supplied_empty = inputs.is_empty();
        inputs.retain(|input| input.artist_id != to_node);
        if supplied_empty || !inputs.is_empty() {
            to.relations.entry(relation_type).or_insert(inputs);
        }
    }
    for (credit_type, inputs) in from.credits {
        to.credits.entry(credit_type).or_insert(inputs);
    }
    let labels = label_links(db, labels_from)?;
    replace_inside_tx(db, to_node, &to, Some(&labels))
}

/// Stores `element` as its source's layer on the entity. Returns the layer's id
/// and whether its values changed.
fn store<T: LayerElement>(
    db: &mut DbAnyTransactionMut<'_>,
    node_id: DbId,
    mut element: T,
) -> anyhow::Result<(DbId, bool)> {
    let Some(layer_id) = find_id::<T>(db, node_id, element.source_id())? else {
        element.set_db_id(None);
        let result = db.exec_mut(QueryBuilder::insert().element(&element).query())?;
        let layer_id = result
            .elements
            .first()
            .map(|inserted| inserted.id)
            .context("metadata layer insert returned no id")?;
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(node_id)
                .to(layer_id)
                .query(),
        )?;
        return Ok((layer_id, true));
    };

    let stored = db.exec(QueryBuilder::select().ids(layer_id).query())?;
    if let Some(existing) = stored.elements.first()
        && T::from_db_element(existing).is_ok_and(|existing| existing.same_values(&element))
    {
        return Ok((layer_id, false));
    }
    element.set_db_id(Some(layer_id));
    super::super::replace_element_in_transaction(db, layer_id, element.optional_keys(), &element)?;
    Ok((layer_id, true))
}

/// The id of the layer `source_id` contributed to an entity, if any.
pub(crate) fn layer_id(
    db: &impl DbAccess,
    node_id: DbId,
    source_id: &str,
) -> anyhow::Result<Option<DbId>> {
    match super::super::entities::metadata_entity_type(db, node_id)? {
        Some(MetadataEntityType::Release) => find_id::<ReleaseLayer>(db, node_id, source_id),
        Some(MetadataEntityType::Track) => find_id::<TrackLayer>(db, node_id, source_id),
        Some(MetadataEntityType::Artist) => find_id::<ArtistLayer>(db, node_id, source_id),
        None => Ok(None),
    }
}

/// The entity a layer belongs to: the only untyped edge into the layer, since
/// its relationship edges are typed.
pub(crate) fn owner_id(db: &impl DbAccess, layer_id: DbId) -> anyhow::Result<Option<DbId>> {
    Ok(db
        .exec(
            QueryBuilder::search()
                .to(layer_id)
                .where_()
                .edge()
                .and()
                .distance(CountComparison::Equal(1))
                .and()
                .not()
                .keys("db_element_id")
                .query(),
        )?
        .elements
        .first()
        .map(|edge| edge.from))
}

/// Copies the layer `source_id` contributed to `from_node` onto `to_node`, as
/// stored and with its relationship edges and update time, replacing that
/// source's layer there.
pub(crate) fn copy_inside_tx(
    db: &mut DbAnyTransactionMut<'_>,
    from_node: DbId,
    to_node: DbId,
    source_id: &str,
) -> anyhow::Result<()> {
    let Some(from_layer) = layer_id(db, from_node, source_id)? else {
        return Ok(());
    };
    let (to_layer, updated_at) = match entity_type(db, to_node)? {
        MetadataEntityType::Release => copy_element::<ReleaseLayer>(db, from_layer, to_node)?,
        MetadataEntityType::Track => copy_element::<TrackLayer>(db, from_layer, to_node)?,
        MetadataEntityType::Artist => copy_element::<ArtistLayer>(db, from_layer, to_node)?,
    };
    edges::copy_all(db, from_layer, to_layer)?;
    set_updated_at(db, to_layer, updated_at)
}

/// Stores `from_layer`'s element on `to_node`. Returns the stored layer's id
/// and the copied update time.
fn copy_element<T: LayerElement>(
    db: &mut DbAnyTransactionMut<'_>,
    from_layer: DbId,
    to_node: DbId,
) -> anyhow::Result<(DbId, u64)> {
    let stored = db.exec(QueryBuilder::select().ids(from_layer).query())?;
    let element = T::from_db_element(
        stored
            .elements
            .first()
            .context("copied metadata layer is gone")?,
    )?;
    let updated_at = element.updated_at();
    Ok((store(db, to_node, element)?.0, updated_at))
}

/// Removes the layer `source_id` contributed to an entity. Returns false when
/// there was none.
pub(crate) fn remove_inside_tx(
    db: &mut impl DbAccess,
    node_id: DbId,
    source_id: &str,
) -> anyhow::Result<bool> {
    let Some(layer_id) = layer_id(db, node_id, source_id)? else {
        return Ok(false);
    };
    remove_by_ids(db, &[layer_id])?;
    Ok(true)
}

/// Removes layers with their relationship edges, collecting labels only they referenced.
pub(crate) fn remove_by_ids(db: &mut impl DbAccess, layer_ids: &[DbId]) -> anyhow::Result<()> {
    if layer_ids.is_empty() {
        return Ok(());
    }
    let label_ids = edges::label_ids_of(db, layer_ids)?;
    db.exec_mut(QueryBuilder::remove().ids(layer_ids).query())?;
    super::super::labels::collect_unreferenced(db, &label_ids)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use serde_json::json;

    use super::*;
    use crate::db::test_db::{
        connect,
        insert_artist,
        insert_release,
        insert_track,
        new_test_db,
        stored_keys,
    };

    fn layer(source_id: &str, fields: Value, updated_at: u64) -> MetadataLayer {
        MetadataLayer::new(source_id, fields_from_json(fields), updated_at)
    }

    fn only_layer(db: &DbAny, node_id: DbId) -> anyhow::Result<MetadataLayer> {
        let mut layers = get_for_entity(db, node_id)?;
        anyhow::ensure!(layers.len() == 1, "expected one layer, got {layers:?}");
        Ok(layers.remove(0))
    }

    fn only_layer_keys(db: &DbAny, node_id: DbId) -> anyhow::Result<HashSet<String>> {
        let ids = ids_for_entity(db, node_id)?;
        anyhow::ensure!(ids.len() == 1, "expected one layer, got {ids:?}");
        stored_keys(db, ids[0])
    }

    /// Attaches a hand-written stored layer, bypassing write validation.
    fn insert_raw_layer(
        db: &mut DbAny,
        node_id: DbId,
        values: Vec<agdb::DbKeyValue>,
    ) -> anyhow::Result<()> {
        let layer_id = db
            .exec_mut(QueryBuilder::insert().nodes().values([values]).query())?
            .elements[0]
            .id;
        connect(db, node_id, layer_id)
    }

    #[test]
    fn upsert_replaces_the_sources_layer_in_place() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_id = insert_release(&mut db, "Release")?;

        assert!(upsert(
            &mut db,
            release_id,
            &layer("musicbrainz", json!({"release_title": "first"}), 100),
        )?);
        let first_ids = ids_for_entity(&db, release_id)?;
        let second = layer("musicbrainz", json!({"release_title": "second"}), 200);
        assert!(upsert(&mut db, release_id, &second)?);

        assert_eq!(ids_for_entity(&db, release_id)?, first_ids);
        assert_eq!(only_layer(&db, release_id)?, second);
        Ok(())
    }

    #[test]
    fn upsert_with_unchanged_values_is_a_noop() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let track_id = insert_track(&mut db, "Track")?;
        let first = layer(
            "musicbrainz",
            json!({"track_title": "same", "disc": 1}),
            100,
        );

        assert!(upsert(&mut db, track_id, &first)?);
        assert!(!upsert(
            &mut db,
            track_id,
            &layer(
                "musicbrainz",
                json!({"track_title": "same", "disc": 1}),
                200
            ),
        )?);

        assert_eq!(only_layer(&db, track_id)?, first);
        Ok(())
    }

    #[test]
    fn a_genre_or_label_only_change_moves_updated_at() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_id = insert_release(&mut db, "Release")?;
        let release = |genre: &str, label: &str, updated_at: u64| {
            layer(
                "provider",
                json!({"genres": [genre], "labels": [{"name": label}]}),
                updated_at,
            )
        };
        upsert(&mut db, release_id, &release("Jazz", "A", 1))?;

        for change in [release("Rock", "A", 2), release("Rock", "B", 3)] {
            assert!(upsert(&mut db, release_id, &change)?);
            assert_eq!(only_layer(&db, release_id)?.updated_at, change.updated_at);
        }
        assert!(!upsert(&mut db, release_id, &release("Rock", "B", 4))?);
        assert_eq!(
            only_layer(&db, release_id)?.updated_at,
            3,
            "a no-op save keeps the stored time"
        );
        Ok(())
    }

    #[test]
    fn a_copied_layer_carries_its_sources_updated_at() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let from = insert_release(&mut db, "From")?;
        let to = insert_release(&mut db, "To")?;
        upsert(
            &mut db,
            from,
            &layer("provider", json!({"genres": ["Rock"]}), 9),
        )?;
        upsert(
            &mut db,
            to,
            &layer("provider", json!({"genres": ["Jazz"]}), 1),
        )?;

        db.transaction_mut(|t| copy_inside_tx(t, from, to, "provider"))?;

        assert_eq!(only_layer(&db, to)?, only_layer(&db, from)?);
        Ok(())
    }

    #[test]
    fn filling_a_layer_takes_the_newer_updated_at() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let from = insert_release(&mut db, "From")?;
        let to = insert_release(&mut db, "To")?;
        upsert(
            &mut db,
            from,
            &layer("manual", json!({"genres": ["Rock"]}), 9),
        )?;
        upsert(
            &mut db,
            to,
            &layer("manual", json!({"release_title": "To"}), 1),
        )?;

        db.transaction_mut(|t| fill_inside_tx(t, from, to, "manual"))?;

        let filled = only_layer(&db, to)?;
        assert_eq!(filled.fields[&MetadataField::Genres], json!(["Rock"]));
        assert_eq!(filled.updated_at, 9);
        Ok(())
    }

    #[test]
    fn replacing_only_a_layers_edges_moves_updated_at() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_id = insert_release(&mut db, "Release")?;
        upsert(
            &mut db,
            release_id,
            &layer("manual", json!({"genres": ["Jazz"]}), 1),
        )?;

        let replaced = layer("manual", json!({"genres": ["Rock"]}), 2);
        db.transaction_mut(|t| replace_inside_tx(t, release_id, &replaced, None))?;

        assert_eq!(only_layer(&db, release_id)?, replaced);
        Ok(())
    }

    #[test]
    fn supplied_distinguishes_a_clear_from_no_opinion() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let artist_id = insert_artist(&mut db, "Artist")?;
        let saved = layer(
            "provider",
            json!({"artist_name": "Artist", "description": null}),
            1,
        );
        upsert(&mut db, artist_id, &saved)?;

        let keys = only_layer_keys(&db, artist_id)?;
        assert!(keys.contains("artist_name"));
        assert!(!keys.contains("description"), "a clear stores no property");
        assert!(!keys.contains("sort_name"));
        let loaded = only_layer(&db, artist_id)?;
        assert_eq!(loaded, saved);
        assert_eq!(loaded.fields[&MetadataField::Description], Value::Null);
        assert!(!loaded.fields.contains_key(&MetadataField::SortName));
        Ok(())
    }

    #[test]
    fn replacing_a_layer_removes_properties_it_no_longer_supplies() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_id = insert_release(&mut db, "Release")?;
        upsert(
            &mut db,
            release_id,
            &layer(
                "provider",
                json!({
                    "release_date": "1999",
                    "genres": ["Jazz"],
                    "labels": [{"name": "Blue Note"}],
                }),
                1,
            ),
        )?;
        let replacement = layer("provider", json!({"release_type": "ep"}), 2);
        upsert(&mut db, release_id, &replacement)?;

        let keys = only_layer_keys(&db, release_id)?;
        for removed in [
            "release_date",
            "genres",
            "label_names",
            "label_catalog_numbers",
        ] {
            assert!(!keys.contains(removed), "'{removed}' outlived its layer");
        }
        assert!(keys.contains("release_type"));
        assert_eq!(only_layer(&db, release_id)?, replacement);
        Ok(())
    }

    #[test]
    fn genres_and_labels_are_stored_as_edges() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_id = insert_release(&mut db, "Release")?;
        let fields = json!({
            "genres": ["Jazz", "Bebop", "jazz"],
            "labels": [
                {
                    "name": "Blue Note",
                    "catalog_number": "BN-1577",
                    "external_id": {
                        "provider_id": "mb",
                        "id_type": "label_id",
                        "id_value": "bn",
                    },
                },
                {"name": "Impulse!"},
            ],
        });
        assert!(upsert(
            &mut db,
            release_id,
            &layer("provider", fields.clone(), 1)
        )?);
        assert!(
            !upsert(&mut db, release_id, &layer("provider", fields, 2))?,
            "the same lists change nothing"
        );

        let layer_id = layer_id(&db, release_id, "provider")?.expect("layer saved");
        let keys = only_layer_keys(&db, release_id)?;
        assert!(!keys.contains("genres") && !keys.contains("label_names"));
        let genre_ids: Vec<DbId> = edges::genres(&db, layer_id)?
            .into_iter()
            .map(|link| link.target_id)
            .collect();
        assert_eq!(
            genre_ids,
            vec![
                crate::db::genres::find_by_name(&db, "Jazz")?.expect("jazz"),
                crate::db::genres::find_by_name(&db, "Bebop")?.expect("bebop"),
            ]
        );
        let blue_note = crate::db::labels::find_by_external_id(&db, "mb", "label_id", "bn")?
            .expect("label resolved with its external id");
        let impulse = crate::db::labels::find_by_name(&db, "Impulse!")?.expect("impulse");
        let labels: Vec<(DbId, Option<String>)> = edges::labels(&db, layer_id)?
            .into_iter()
            .map(|link| (link.target_id, link.edge.catalog_number))
            .collect();
        assert_eq!(
            labels,
            vec![(blue_note, Some("BN-1577".to_string())), (impulse, None)]
        );
        assert_eq!(
            only_layer(&db, release_id)?.fields,
            fields_from_json(json!({
                "genres": ["Jazz", "Bebop"],
                "labels": [
                    {"name": "Blue Note", "catalog_number": "BN-1577"},
                    {"name": "Impulse!"},
                ],
            }))
        );

        assert!(upsert(
            &mut db,
            release_id,
            &layer("provider", json!({"genres": [], "labels": null}), 3),
        )?);
        assert!(edges::genres(&db, layer_id)?.is_empty());
        assert!(edges::labels(&db, layer_id)?.is_empty());
        assert!(crate::db::labels::get_by_id(&db, impulse)?.is_none());
        assert_eq!(
            only_layer(&db, release_id)?.fields,
            fields_from_json(json!({"genres": null, "labels": null})),
            "an empty list clears like null"
        );
        Ok(())
    }

    #[test]
    fn edges_without_supplied_fields_do_not_validate() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_id = insert_release(&mut db, "Release")?;
        upsert(&mut db, release_id, &layer("provider", json!({}), 1))?;
        let layer_id = layer_id(&db, release_id, "provider")?.expect("layer saved");
        let jazz = crate::db::genres::resolve_by_name(&mut db, "Jazz")?;
        db.transaction_mut(|t| edges::replace_genres(t, layer_id, &[jazz]))?;

        assert!(get_for_entity(&db, release_id)?.is_empty());
        Ok(())
    }

    #[test]
    fn upsert_rejects_fields_the_entity_cannot_hold_or_clear() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let track_id = insert_track(&mut db, "Track")?;

        for fields in [
            json!({"genres": ["Jazz"]}),
            json!({"artist_name": "Artist"}),
            json!({"track_title": null}),
        ] {
            assert!(
                upsert(&mut db, track_id, &layer("provider", fields.clone(), 1)).is_err(),
                "accepted {fields}"
            );
        }
        assert!(ids_for_entity(&db, track_id)?.is_empty());
        Ok(())
    }

    #[test]
    fn get_for_entity_skips_a_stored_layer_that_no_longer_validates() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let track_id = insert_track(&mut db, "Track")?;
        let base = |source_id: &str, supplied: Vec<&str>| -> Vec<agdb::DbKeyValue> {
            vec![
                ("db_element_id", "TrackLayer").into(),
                ("source_id", source_id).into(),
                ("updated_at", 1_u64).into(),
                (
                    "supplied",
                    supplied.into_iter().map(String::from).collect::<Vec<_>>(),
                )
                    .into(),
            ]
        };
        insert_raw_layer(&mut db, track_id, base("unknown", vec!["work_id"]))?;
        let mut unsupplied = base("unsupplied", Vec::new());
        unsupplied.push(("track_title", "Title").into());
        insert_raw_layer(&mut db, track_id, unsupplied)?;
        insert_raw_layer(&mut db, track_id, base("cleared", vec!["track_title"]))?;
        insert_raw_layer(&mut db, track_id, base("foreign", vec!["genres"]))?;
        upsert(
            &mut db,
            track_id,
            &layer("valid", json!({"track_title": "Title"}), 1),
        )?;

        let sources: Vec<String> = get_for_entity(&db, track_id)?
            .into_iter()
            .map(|layer| layer.source_id)
            .collect();
        assert_eq!(sources, vec!["valid".to_string()]);

        upsert(
            &mut db,
            track_id,
            &layer("unknown", json!({"track_title": "Fixed"}), 2),
        )?;
        assert_eq!(get_for_entity(&db, track_id)?.len(), 2);
        assert_eq!(ids_for_entity(&db, track_id)?.len(), 5);
        Ok(())
    }

    #[test]
    fn layer_lookups_match_element_types_exactly() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let track_id = insert_track(&mut db, "Track")?;
        insert_raw_layer(
            &mut db,
            track_id,
            vec![
                ("db_element_id", "TrackLayerHistory").into(),
                ("source_id", "provider").into(),
            ],
        )?;

        assert!(ids_for_entity(&db, track_id)?.is_empty());
        assert!(!db.transaction_mut(|t| remove_inside_tx(t, track_id, "provider"))?);
        upsert(&mut db, track_id, &layer("provider", json!({}), 1))?;
        assert_eq!(ids_for_entity(&db, track_id)?.len(), 1);
        Ok(())
    }

    #[test]
    fn remove_drops_only_the_sources_layer() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_id = insert_release(&mut db, "Release")?;
        upsert(&mut db, release_id, &layer("first", json!({}), 1))?;
        upsert(&mut db, release_id, &layer("second", json!({}), 1))?;

        assert!(db.transaction_mut(|t| remove_inside_tx(t, release_id, "first"))?);
        assert!(!db.transaction_mut(|t| remove_inside_tx(t, release_id, "first"))?);

        assert_eq!(only_layer(&db, release_id)?.source_id, "second");
        Ok(())
    }

    #[test]
    fn a_relation_only_change_moves_updated_at() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let character_id = insert_artist(&mut db, "Character")?;
        let first = insert_artist(&mut db, "First")?;
        let second = insert_artist(&mut db, "Second")?;
        let voiced_by = |actor_id: DbId, updated_at: u64| {
            layer("provider", json!({}), updated_at).with_relations(LayerRelations::from([(
                ArtistRelationType::VoiceActor,
                vec![edges::LayerRelationInput {
                    artist_id: actor_id,
                    attributes: None,
                }],
            )]))
        };
        upsert(&mut db, character_id, &voiced_by(first, 1))?;

        assert!(upsert(&mut db, character_id, &voiced_by(second, 2))?);

        assert_eq!(only_layer(&db, character_id)?.updated_at, 2);
        Ok(())
    }

    #[test]
    fn normalize_value_rejects_values_the_field_cannot_hold() {
        use serde_json::json;

        for (field, value) in [
            (MetadataField::Credits, json!([])),
            (MetadataField::Relations, json!([])),
            (MetadataField::TrackTitle, Value::Null),
            (MetadataField::ReleaseTitle, json!("  ")),
            (MetadataField::Disc, json!("1")),
            (MetadataField::Year, json!(-1)),
            (MetadataField::ReleaseType, json!("mixtape")),
            (MetadataField::ArtistType, json!("Person")),
            (MetadataField::ReleaseDate, json!("1999-13")),
            (MetadataField::Genres, json!("Jazz")),
            (MetadataField::Genres, json!([1])),
        ] {
            assert!(
                normalize_value(field, value.clone()).is_err(),
                "{field} accepted {value}"
            );
        }
    }

    #[test]
    fn normalize_value_accepts_clears_and_normalizes_dates() -> anyhow::Result<()> {
        use serde_json::json;

        assert_eq!(
            normalize_value(MetadataField::SortTitle, Value::Null)?,
            Value::Null
        );
        assert_eq!(
            normalize_value(MetadataField::ReleaseDate, json!(" 1999-02 "))?,
            json!("1999-02")
        );
        assert_eq!(
            normalize_value(MetadataField::ReleaseType, json!("ep"))?,
            json!("ep")
        );
        Ok(())
    }

    #[test]
    fn label_inputs_parse_the_full_shape() -> anyhow::Result<()> {
        let inputs = label_inputs(&serde_json::json!([
            {
                "name": "Blue Note",
                "catalog_number": " BN-1577 ",
                "external_id": {
                    "provider_id": "  mb  ",
                    "id_type": " label_id ",
                    "id_value": " bn-001 "
                }
            },
            {"name": "Impulse!", "catalog_number": "  ", "external_id": null}
        ]))?;

        assert_eq!(inputs.len(), 2);
        assert_eq!(inputs[0].name, "Blue Note");
        assert_eq!(inputs[0].catalog_number.as_deref(), Some("BN-1577"));
        let external_id = inputs[0].external_id.as_ref().expect("external_id present");
        assert_eq!(external_id.provider_id, "mb");
        assert_eq!(external_id.id_type, "label_id");
        assert_eq!(external_id.id_value, "bn-001");
        assert_eq!(inputs[1].name, "Impulse!");
        assert!(inputs[1].catalog_number.is_none());
        assert!(inputs[1].external_id.is_none());
        assert!(label_inputs(&serde_json::json!([]))?.is_empty());
        Ok(())
    }

    #[test]
    fn label_inputs_reject_malformed_entries() {
        use serde_json::json;

        for value in [
            json!("Blue Note"),
            json!({"name": "Blue Note"}),
            json!(["Blue Note"]),
            json!([{"catalog_number": "BN-1"}]),
            json!([{"name": "   "}]),
            json!([{"name": "Blue Note", "catalog_number": 12345}]),
            json!([{"name": "Blue Note", "external_id": "mb"}]),
            json!([{
                "name": "Blue Note",
                "external_id": {"provider_id": "mb", "id_type": "label_id"}
            }]),
            json!([{
                "name": "Blue Note",
                "external_id": {"provider_id": "mb", "id_type": "label_id", "id_value": "   "}
            }]),
        ] {
            assert!(label_inputs(&value).is_err(), "accepted {value}");
        }
    }

    #[test]
    fn entity_ids_for_source_returns_layer_owners() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let first = insert_release(&mut db, "First")?;
        let second = insert_track(&mut db, "Second")?;
        let other = insert_artist(&mut db, "Other")?;

        for (node_id, source_id) in [
            (first, "musicbrainz"),
            (second, "musicbrainz"),
            (other, "wikidata"),
        ] {
            upsert(&mut db, node_id, &layer(source_id, json!({}), 1))?;
        }

        assert_eq!(
            entity_ids_for_source(&db, "musicbrainz")?,
            vec![first, second]
        );
        assert!(entity_ids_for_source(&db, "missing")?.is_empty());
        Ok(())
    }

    #[test]
    fn a_credit_only_change_moves_updated_at() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_id = insert_release(&mut db, "Release")?;
        let first = insert_artist(&mut db, "First")?;
        let second = insert_artist(&mut db, "Second")?;
        let credited = |artist_id: DbId, updated_at: u64| {
            layer("provider", json!({}), updated_at).with_credits(LayerCredits::from([(
                CreditType::Artist,
                vec![edges::LayerCreditInput {
                    artist_id,
                    detail: None,
                }],
            )]))
        };
        upsert(&mut db, release_id, &credited(first, 1))?;

        assert!(upsert(&mut db, release_id, &credited(second, 2))?);

        assert_eq!(only_layer(&db, release_id)?.updated_at, 2);
        Ok(())
    }
}
