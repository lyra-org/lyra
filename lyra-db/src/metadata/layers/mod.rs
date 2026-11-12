pub mod edges;
mod elements;
mod values;

use std::collections::BTreeMap;

use agdb::{
    CountComparison,
    DbAnyTransactionMut,
    DbId,
    QueryBuilder,
};
use anyhow::Context;

use self::elements::{
    ArtistLayer,
    LayerElement,
    ReleaseLayer,
    TrackLayer,
};
pub use self::values::{
    FieldInput,
    FieldInputs,
    FieldType,
    FieldValue,
    LayerFields,
    field_value,
    normalize_value,
    resolve_inputs,
};
use super::super::{
    DbAccess,
    artists::relations::ArtistRelationType,
    credits::CreditType,
    entities::MetadataEntityType,
    roots::{
        self,
        Root,
    },
};

/// An artist layer's incoming relations by type: the artists that relate to
/// the layer's artist. A listed type is supplied; an empty list clears it.
pub type LayerRelations = BTreeMap<ArtistRelationType, Vec<edges::LayerRelationInput>>;

/// A release or track layer's credits by role, each in credit order. A listed
/// role is supplied; an empty list clears it.
pub type LayerCredits = BTreeMap<CreditType, Vec<edges::LayerCreditInput>>;

/// One source's opinion on an entity's fields; stored as the entity's
/// [`ReleaseLayer`], [`TrackLayer`], or [`ArtistLayer`].
#[derive(Clone, Debug, PartialEq)]
pub struct MetadataLayer {
    pub source_id: String,
    pub fields: LayerFields,
    pub relations: LayerRelations,
    pub credits: LayerCredits,
    pub updated_at_ms: u64,
}

/// For each type `incoming` lists, `stored`'s artists followed by the incoming
/// ones it lacks.
pub fn union_relations(stored: &LayerRelations, incoming: LayerRelations) -> LayerRelations {
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
    pub fn new(source_id: impl Into<String>, fields: LayerFields, updated_at_ms: u64) -> Self {
        Self {
            source_id: source_id.into(),
            fields,
            relations: LayerRelations::new(),
            credits: LayerCredits::new(),
            updated_at_ms,
        }
    }

    pub fn with_relations(mut self, relations: LayerRelations) -> Self {
        self.relations = relations;
        self
    }

    pub fn with_credits(mut self, credits: LayerCredits) -> Self {
        self.credits = credits;
        self
    }
}

/// Evaluates `$body` with `$element` naming the element type that stores
/// `$entity`'s layers.
macro_rules! with_element {
    ($entity:expr, $element:ident => $body:expr) => {
        match $entity {
            MetadataEntityType::Release => {
                type $element = ReleaseLayer;
                $body
            }
            MetadataEntityType::Track => {
                type $element = TrackLayer;
                $body
            }
            MetadataEntityType::Artist => {
                type $element = ArtistLayer;
                $body
            }
        }
    };
}

fn entity_type(db: &impl DbAccess, node_id: DbId) -> anyhow::Result<MetadataEntityType> {
    super::super::entities::metadata_entity_type(db, node_id)?.with_context(|| {
        format!(
            "metadata layers belong to releases, tracks, and artists, not {}",
            node_id.0
        )
    })
}

/// The entity's layers.
pub fn get_for_entity(db: &impl DbAccess, node_id: DbId) -> anyhow::Result<Vec<MetadataLayer>> {
    let Some(entity) = super::super::entities::metadata_entity_type(db, node_id)? else {
        return Ok(Vec::new());
    };
    with_element!(entity, T => load::<T>(db, node_id))
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
    result
        .elements
        .iter()
        .map(|element| {
            read::<T>(db, element)
                .with_context(|| format!("stored metadata layer {} is invalid", element.id.0))
        })
        .collect()
}

fn read<T: LayerElement>(
    db: &impl DbAccess,
    element: &agdb::DbElement,
) -> anyhow::Result<MetadataLayer> {
    T::from_db_element(element)?.to_layer(db)
}

/// Ids of every layer attached to an entity.
pub fn ids_for_entity(db: &impl DbAccess, node_id: DbId) -> anyhow::Result<Vec<DbId>> {
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

pub fn entity_ids_for_source(db: &impl DbAccess, source_id: &str) -> anyhow::Result<Vec<DbId>> {
    let mut entity_ids = Vec::new();
    for layer_id in layer_ids_for_source::<ReleaseLayer>(db, Root::Releases, source_id)?
        .into_iter()
        .chain(layer_ids_for_source::<TrackLayer>(
            db,
            Root::Tracks,
            source_id,
        )?)
        .chain(layer_ids_for_source::<ArtistLayer>(
            db,
            Root::Artists,
            source_id,
        )?)
    {
        entity_ids.extend(owner_id(db, layer_id)?);
    }
    entity_ids.sort_unstable();
    entity_ids.dedup();
    Ok(entity_ids)
}

/// The `T` layers that `source_id` contributed to members of `root`.
fn layer_ids_for_source<T: LayerElement>(
    db: &impl DbAccess,
    root: Root,
    source_id: &str,
) -> anyhow::Result<Vec<DbId>> {
    Ok(db
        .exec(
            QueryBuilder::search()
                .from(root.alias())
                .where_()
                .distance(roots::MEMBER_CHILDREN)
                .and()
                .element::<T>()
                .and()
                .key("source_id")
                .value(source_id)
                .query(),
        )?
        .ids())
}

/// Saves `layer` as its source's layer on the entity, replacing any previous
/// one. Returns false when the stored layer already supplied the same values.
#[cfg(any(test, feature = "testing"))]
pub fn upsert(db: &mut agdb::DbAny, node_id: DbId, layer: &MetadataLayer) -> anyhow::Result<bool> {
    db.transaction_mut(|t| upsert_inside_tx(t, node_id, layer))
}

pub fn upsert_inside_tx(
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
    save_any(db, node_id, &layer)
}

/// Saves `layer` as its source's whole layer: unlike [`upsert_inside_tx`], the
/// relation types and credit roles it does not supply go, and a layer that
/// supplies nothing is removed.
pub fn replace_inside_tx(
    db: &mut DbAnyTransactionMut<'_>,
    node_id: DbId,
    layer: &MetadataLayer,
) -> anyhow::Result<()> {
    if layer.fields.is_empty() && layer.relations.is_empty() && layer.credits.is_empty() {
        remove_inside_tx(db, node_id, &layer.source_id)?;
        return Ok(());
    }
    save_any(db, node_id, layer)?;
    Ok(())
}

fn save_any(
    db: &mut DbAnyTransactionMut<'_>,
    node_id: DbId,
    layer: &MetadataLayer,
) -> anyhow::Result<bool> {
    with_element!(entity_type(db, node_id)?, T => save::<T>(db, node_id, layer))
        .with_context(|| format!("invalid '{}' metadata layer", layer.source_id))
}

fn save<T: LayerElement>(
    db: &mut DbAnyTransactionMut<'_>,
    node_id: DbId,
    layer: &MetadataLayer,
) -> anyhow::Result<bool> {
    let layer = elements::validated(T::ENTITY, layer)?;
    let (layer_id, values_changed) = store(db, node_id, T::from_layer(&layer)?)?;
    let edges_changed = T::save_edges(db, layer_id, &layer)?;
    if edges_changed && !values_changed {
        set_updated_at(db, layer_id, layer.updated_at_ms)?;
    }
    Ok(values_changed || edges_changed)
}

fn set_updated_at(
    db: &mut impl DbAccess,
    layer_id: DbId,
    updated_at_ms: u64,
) -> anyhow::Result<()> {
    db.exec_mut(
        QueryBuilder::insert()
            .values_uniform([("updated_at_ms", updated_at_ms).into()])
            .ids(layer_id)
            .query(),
    )?;
    Ok(())
}

/// The layer `source_id` contributed to an entity, if any.
pub fn find(
    db: &impl DbAccess,
    node_id: DbId,
    source_id: &str,
) -> anyhow::Result<Option<MetadataLayer>> {
    let Some(entity) = super::super::entities::metadata_entity_type(db, node_id)? else {
        return Ok(None);
    };
    with_element!(entity, T => load_one::<T>(db, node_id, source_id))
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
    read::<T>(db, element)
        .map(Some)
        .with_context(|| format!("the stored '{source_id}' metadata layer is invalid"))
}

/// Fills the keys `to_node`'s layer from `source_id` does not supply with
/// `from_node`'s, or copies `from_node`'s layer when `to_node` has none. A
/// filled layer is as recent as the newer of the two.
pub fn fill_inside_tx(
    db: &mut DbAnyTransactionMut<'_>,
    from_node: DbId,
    to_node: DbId,
    source_id: &str,
) -> anyhow::Result<()> {
    let Some(from) = find(db, from_node, source_id)? else {
        return Ok(());
    };
    let Some(mut to) = find(db, to_node, source_id)? else {
        return copy_inside_tx(db, from_node, to_node, source_id);
    };
    to.updated_at_ms = to.updated_at_ms.max(from.updated_at_ms);
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
    replace_inside_tx(db, to_node, &to)
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
pub fn layer_id(
    db: &impl DbAccess,
    node_id: DbId,
    source_id: &str,
) -> anyhow::Result<Option<DbId>> {
    let Some(entity) = super::super::entities::metadata_entity_type(db, node_id)? else {
        return Ok(None);
    };
    with_element!(entity, T => find_id::<T>(db, node_id, source_id))
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
pub fn copy_inside_tx(
    db: &mut DbAnyTransactionMut<'_>,
    from_node: DbId,
    to_node: DbId,
    source_id: &str,
) -> anyhow::Result<()> {
    let Some(from_layer) = layer_id(db, from_node, source_id)? else {
        return Ok(());
    };
    let (to_layer, updated_at_ms) =
        with_element!(entity_type(db, to_node)?, T => copy_element::<T>(db, from_layer, to_node)?);
    edges::copy_all(db, from_layer, to_layer)?;
    set_updated_at(db, to_layer, updated_at_ms)
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
    let updated_at_ms = element.updated_at_ms();
    Ok((store(db, to_node, element)?.0, updated_at_ms))
}

/// Removes the layer `source_id` contributed to an entity. Returns false when
/// there was none.
pub fn remove_inside_tx(
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

/// Removes layers with their relationship edges, collecting labels only they referenced. The
/// artists they referenced become candidates.
pub(crate) fn remove_by_ids(db: &mut impl DbAccess, layer_ids: &[DbId]) -> anyhow::Result<()> {
    if layer_ids.is_empty() {
        return Ok(());
    }
    let label_ids = edges::label_ids_of(db, layer_ids)?;
    let artist_ids = edges::artists_referenced_by(db, layer_ids)?;
    db.exec_mut(QueryBuilder::remove().ids(layer_ids).query())?;
    crate::orphans::mark_artists(db, &artist_ids)?;
    super::super::labels::collect_unreferenced(db, &label_ids)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use agdb::DbAny;
    use serde_json::{
        Value,
        json,
    };

    use super::*;
    use crate::MetadataField;
    use crate::fixtures::{
        connect,
        insert_artist,
        insert_release,
        insert_track,
    };
    use crate::test_db::{
        new_test_db,
        stored_keys,
    };

    #[derive(serde::Deserialize)]
    struct TestLabel {
        name: String,
        catalog_number: Option<String>,
        external_id: Option<TestExternalId>,
    }

    #[derive(serde::Deserialize)]
    struct TestExternalId {
        provider_id: String,
        id_type: String,
        id_value: String,
    }

    fn parse<T: serde::de::DeserializeOwned>(value: Value) -> T {
        serde_json::from_value(value).expect("test field value")
    }

    /// Field inputs from a JSON object of field names to values.
    fn inputs(value: Value) -> FieldInputs {
        let Value::Object(object) = value else {
            panic!("test layer fields are a JSON object, got {value}");
        };
        object
            .into_iter()
            .map(|(name, value)| {
                let field = MetadataField::from_name(&name).expect("known test field");
                let input = match (field, value) {
                    (_, Value::Null) => None,
                    (MetadataField::Genres, value) => Some(FieldInput::Genres(parse(value))),
                    (MetadataField::Labels, value) => {
                        let labels: Vec<TestLabel> = parse(value);
                        Some(FieldInput::Labels(
                            labels
                                .into_iter()
                                .map(|label| crate::labels::LabelInput {
                                    name: label.name,
                                    catalog_number: label.catalog_number,
                                    external_id: label.external_id.map(|id| {
                                        crate::labels::LabelExternalIdInput {
                                            provider_id: id.provider_id,
                                            id_type: id.id_type,
                                            id_value: id.id_value,
                                        }
                                    }),
                                })
                                .collect(),
                        ))
                    }
                    (MetadataField::ReleaseType, value) => {
                        Some(FieldInput::Value(FieldValue::ReleaseType(parse(value))))
                    }
                    (_, Value::String(text)) => Some(FieldInput::Value(FieldValue::Text(text))),
                    (_, value) => Some(FieldInput::Value(FieldValue::Number(parse(value)))),
                };
                (field, input)
            })
            .collect()
    }

    fn layer(source_id: &str, fields: Value, updated_at_ms: u64) -> MetadataLayer {
        let fields = inputs(fields)
            .into_iter()
            .map(|(field, input)| match input {
                Some(FieldInput::Value(value)) => (field, Some(value)),
                None => (field, None),
                Some(_) => panic!("'{field}' needs a database to resolve"),
            })
            .collect();
        MetadataLayer::new(source_id, fields, updated_at_ms)
    }

    /// A layer whose genres and labels resolve by name.
    fn resolved(
        db: &mut DbAny,
        source_id: &str,
        fields: Value,
        updated_at_ms: u64,
    ) -> MetadataLayer {
        let fields = resolve_inputs(db, inputs(fields)).expect("test layer fields resolve");
        MetadataLayer::new(source_id, fields, updated_at_ms)
    }

    fn genre(db: &DbAny, name: &str) -> anyhow::Result<DbId> {
        crate::genres::find_by_name(db, name)?.context("genre exists")
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
        let release = |db: &mut DbAny, genre: &str, label: &str, updated_at_ms: u64| {
            resolved(
                db,
                "provider",
                json!({"genres": [genre], "labels": [{"name": label}]}),
                updated_at_ms,
            )
        };
        let first = release(&mut db, "Jazz", "A", 1);
        upsert(&mut db, release_id, &first)?;

        for (genre, label, updated_at_ms) in [("Rock", "A", 2), ("Rock", "B", 3)] {
            let change = release(&mut db, genre, label, updated_at_ms);
            assert!(upsert(&mut db, release_id, &change)?);
            assert_eq!(
                only_layer(&db, release_id)?.updated_at_ms,
                change.updated_at_ms
            );
        }
        let unchanged = release(&mut db, "Rock", "B", 4);
        assert!(!upsert(&mut db, release_id, &unchanged)?);
        assert_eq!(
            only_layer(&db, release_id)?.updated_at_ms,
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
        let rock = resolved(&mut db, "provider", json!({"genres": ["Rock"]}), 9);
        upsert(&mut db, from, &rock)?;
        let jazz = resolved(&mut db, "provider", json!({"genres": ["Jazz"]}), 1);
        upsert(&mut db, to, &jazz)?;

        db.transaction_mut(|t| copy_inside_tx(t, from, to, "provider"))?;

        assert_eq!(only_layer(&db, to)?, only_layer(&db, from)?);
        Ok(())
    }

    #[test]
    fn filling_a_layer_takes_the_newer_updated_at() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let from = insert_release(&mut db, "From")?;
        let to = insert_release(&mut db, "To")?;
        let rock = resolved(&mut db, "manual", json!({"genres": ["Rock"]}), 9);
        upsert(&mut db, from, &rock)?;
        upsert(
            &mut db,
            to,
            &layer("manual", json!({"release_title": "To"}), 1),
        )?;

        db.transaction_mut(|t| fill_inside_tx(t, from, to, "manual"))?;

        let filled = only_layer(&db, to)?;
        assert_eq!(
            filled.fields,
            LayerFields::from([
                (MetadataField::ReleaseTitle, Some("To".into())),
                (
                    MetadataField::Genres,
                    Some(FieldValue::Genres(vec![genre(&db, "Rock")?]))
                ),
            ])
        );
        assert_eq!(filled.updated_at_ms, 9);
        Ok(())
    }

    #[test]
    fn replacing_only_a_layers_edges_moves_updated_at() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_id = insert_release(&mut db, "Release")?;
        let jazz = resolved(&mut db, "manual", json!({"genres": ["Jazz"]}), 1);
        upsert(&mut db, release_id, &jazz)?;

        let replaced = resolved(&mut db, "manual", json!({"genres": ["Rock"]}), 2);
        db.transaction_mut(|t| replace_inside_tx(t, release_id, &replaced))?;

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
        assert_eq!(loaded.fields[&MetadataField::Description], None);
        assert!(!loaded.fields.contains_key(&MetadataField::SortName));
        Ok(())
    }

    #[test]
    fn a_release_layer_stores_its_description() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_id = insert_release(&mut db, "Release")?;
        let saved = layer(
            "provider",
            json!({"release_title": "Release", "description": "About the release"}),
            1,
        );
        upsert(&mut db, release_id, &saved)?;

        assert!(only_layer_keys(&db, release_id)?.contains("description"));
        assert_eq!(only_layer(&db, release_id)?, saved);
        Ok(())
    }

    #[test]
    fn replacing_a_layer_removes_properties_it_no_longer_supplies() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_id = insert_release(&mut db, "Release")?;
        let first = resolved(
            &mut db,
            "provider",
            json!({
                "release_date": "1999",
                "genres": ["Jazz"],
                "labels": [{"name": "Blue Note"}],
            }),
            1,
        );
        upsert(&mut db, release_id, &first)?;
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
        let first = resolved(&mut db, "provider", fields.clone(), 1);
        assert!(upsert(&mut db, release_id, &first)?);
        let second = resolved(&mut db, "provider", fields, 2);
        assert!(
            !upsert(&mut db, release_id, &second)?,
            "the same lists change nothing"
        );

        let layer_id = layer_id(&db, release_id, "provider")?.expect("layer saved");
        let keys = only_layer_keys(&db, release_id)?;
        assert!(!keys.contains("genres") && !keys.contains("label_names"));
        let genre_ids: Vec<DbId> = edges::genres(&db, layer_id)?
            .into_iter()
            .map(|link| link.target_id)
            .collect();
        assert_eq!(genre_ids, vec![genre(&db, "Jazz")?, genre(&db, "Bebop")?]);
        let blue_note = crate::labels::find_by_external_id(&db, "mb", "label_id", "bn")?
            .expect("label resolved with its external id");
        let impulse = crate::labels::find_by_name(&db, "Impulse!")?.expect("impulse");
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
            LayerFields::from([
                (MetadataField::Genres, Some(FieldValue::Genres(genre_ids))),
                (
                    MetadataField::Labels,
                    Some(FieldValue::Labels(
                        labels
                            .into_iter()
                            .map(|(label_id, catalog_number)| crate::labels::LabelLinkInput {
                                label_id,
                                catalog_number,
                            })
                            .collect()
                    ))
                ),
            ])
        );

        let cleared = resolved(
            &mut db,
            "provider",
            json!({"genres": [], "labels": null}),
            3,
        );
        assert!(upsert(&mut db, release_id, &cleared)?);
        assert!(edges::genres(&db, layer_id)?.is_empty());
        assert!(edges::labels(&db, layer_id)?.is_empty());
        assert!(crate::labels::get_by_id(&db, impulse)?.is_none());
        assert_eq!(
            only_layer(&db, release_id)?.fields,
            LayerFields::from([(MetadataField::Genres, None), (MetadataField::Labels, None)]),
            "an empty list clears like null"
        );
        Ok(())
    }

    #[test]
    fn edges_of_an_unsupplied_field_are_not_read() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_id = insert_release(&mut db, "Release")?;
        upsert(&mut db, release_id, &layer("provider", json!({}), 1))?;
        let layer_id = layer_id(&db, release_id, "provider")?.expect("layer saved");
        let jazz = crate::genres::resolve_by_name(&mut db, "Jazz")?;
        db.transaction_mut(|t| edges::replace_genres(t, layer_id, &[jazz]))?;

        assert!(only_layer(&db, release_id)?.fields.is_empty());
        Ok(())
    }

    #[test]
    fn upsert_rejects_fields_the_entity_cannot_hold_or_clear() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let track_id = insert_track(&mut db, "Track")?;
        let jazz = crate::genres::resolve_by_name(&mut db, "Jazz")?;

        for fields in [
            LayerFields::from([(MetadataField::Genres, Some(FieldValue::Genres(vec![jazz])))]),
            LayerFields::from([(MetadataField::ArtistName, Some("Artist".into()))]),
            LayerFields::from([(MetadataField::TrackTitle, None)]),
            LayerFields::from([(MetadataField::Year, Some("1999".into()))]),
        ] {
            assert!(
                upsert(
                    &mut db,
                    track_id,
                    &MetadataLayer::new("provider", fields.clone(), 1)
                )
                .is_err(),
                "accepted {fields:?}"
            );
        }
        assert!(ids_for_entity(&db, track_id)?.is_empty());
        Ok(())
    }

    #[test]
    fn a_stored_layer_that_does_not_decode_is_an_error() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let track_id = insert_track(&mut db, "Track")?;
        upsert(
            &mut db,
            track_id,
            &layer("valid", json!({"track_title": "Title"}), 1),
        )?;
        insert_raw_layer(
            &mut db,
            track_id,
            vec![
                ("db_element_id", "TrackLayer").into(),
                ("source_id", "corrupt").into(),
                ("updated_at_ms", 1_u64).into(),
                ("supplied", vec!["year".to_string()]).into(),
                ("supplied_credits", Vec::<String>::new()).into(),
                ("year", "1999").into(),
            ],
        )?;

        assert!(get_for_entity(&db, track_id).is_err());
        assert!(find(&db, track_id, "corrupt").is_err());
        assert!(find(&db, track_id, "valid")?.is_some());
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
        let voiced_by = |actor_id: DbId, updated_at_ms: u64| {
            layer("provider", json!({}), updated_at_ms).with_relations(LayerRelations::from([(
                ArtistRelationType::VoiceActor,
                vec![edges::LayerRelationInput {
                    artist_id: actor_id,
                    attributes: None,
                }],
            )]))
        };
        upsert(&mut db, character_id, &voiced_by(first, 1))?;

        assert!(upsert(&mut db, character_id, &voiced_by(second, 2))?);

        assert_eq!(only_layer(&db, character_id)?.updated_at_ms, 2);
        Ok(())
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
        let credited = |artist_id: DbId, updated_at_ms: u64| {
            layer("provider", json!({}), updated_at_ms).with_credits(LayerCredits::from([(
                CreditType::Artist,
                vec![edges::LayerCreditInput {
                    artist_id,
                    detail: None,
                }],
            )]))
        };
        upsert(&mut db, release_id, &credited(first, 1))?;

        assert!(upsert(&mut db, release_id, &credited(second, 2))?);

        assert_eq!(only_layer(&db, release_id)?.updated_at_ms, 2);
        Ok(())
    }
}
