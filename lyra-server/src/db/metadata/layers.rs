// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::{
    DbAny,
    DbElement,
    DbId,
    QueryBuilder,
};
use schemars::JsonSchema;
use serde::Serialize;

use super::super::NodeId;

#[derive(DbElement, Serialize, Clone, Debug, JsonSchema)]
pub(crate) struct MetadataLayer {
    #[serde(skip)]
    pub(crate) db_id: Option<NodeId>,
    pub(crate) id: String,
    pub(crate) provider_id: String,
    pub(crate) fields: String,
    pub(crate) updated_at: u64,
}

pub(crate) fn get_for_entity(
    db: &impl super::super::DbAccess,
    node_id: DbId,
) -> anyhow::Result<Vec<MetadataLayer>> {
    let layers: Vec<MetadataLayer> = db
        .exec(
            QueryBuilder::select()
                .elements::<MetadataLayer>()
                .search()
                .from(node_id)
                .where_()
                .neighbor()
                .end_where()
                .query(),
        )?
        .try_into()?;

    Ok(layers)
}

pub(crate) fn upsert(db: &mut DbAny, node_id: DbId, layer: &MetadataLayer) -> anyhow::Result<DbId> {
    db.transaction_mut(|t| -> anyhow::Result<DbId> {
        let layers: Vec<MetadataLayer> = t
            .exec(
                QueryBuilder::select()
                    .elements::<MetadataLayer>()
                    .search()
                    .from(node_id)
                    .where_()
                    .neighbor()
                    .end_where()
                    .query(),
            )?
            .try_into()?;
        let existing = layers
            .into_iter()
            .find(|existing| existing.provider_id == layer.provider_id);

        // Avoid no-op rewrites when provider fields are unchanged.
        if let Some(existing_layer) = &existing
            && existing_layer.fields == layer.fields
            && let Some(db_id) = existing_layer.db_id.clone()
        {
            return Ok(db_id.into());
        }

        let mut layer_to_save = layer.clone();
        if let Some(existing_layer) = &existing {
            layer_to_save.db_id = existing_layer.db_id.clone();
        }

        let result = t.exec_mut(QueryBuilder::insert().element(&layer_to_save).query())?;
        let layer_db_id = existing
            .as_ref()
            .and_then(|existing_layer| existing_layer.db_id.clone())
            .map(DbId::from)
            .or_else(|| result.elements.first().map(|element| element.id))
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "upsert metadata layer returned no id (node_id={}, provider_id='{}')",
                    node_id.0,
                    layer.provider_id
                )
            })?;

        if existing.is_none() {
            t.exec_mut(
                QueryBuilder::insert()
                    .edges()
                    .from(node_id)
                    .to(layer_db_id)
                    .query(),
            )?;
        }

        Ok(layer_db_id)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::test_db::TestDb;
    use agdb::QueryBuilder;
    use anyhow::anyhow;
    use nanoid::nanoid;

    fn new_test_db() -> anyhow::Result<DbAny> {
        Ok(TestDb::new()?.into_inner())
    }

    fn insert_entity(db: &mut DbAny) -> anyhow::Result<DbId> {
        let result = db.exec_mut(QueryBuilder::insert().nodes().count(1).query())?;
        result
            .elements
            .first()
            .map(|element| element.id)
            .ok_or_else(|| anyhow!("entity insert did not return an id"))
    }

    #[test]
    fn upsert_layer_update_reuses_node_and_updates_values() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let node_id = insert_entity(&mut db)?;

        let first = MetadataLayer {
            db_id: None,
            id: nanoid!(),
            provider_id: "musicbrainz".to_string(),
            fields: r#"{"release_title":"first"}"#.to_string(),
            updated_at: 100,
        };
        let second = MetadataLayer {
            db_id: None,
            id: nanoid!(),
            provider_id: "musicbrainz".to_string(),
            fields: r#"{"release_title":"second"}"#.to_string(),
            updated_at: 200,
        };

        let first_id = upsert(&mut db, node_id, &first)?;
        let second_id = upsert(&mut db, node_id, &second)?;

        assert_eq!(first_id, second_id);

        let layers = get_for_entity(&db, node_id)?;
        assert_eq!(layers.len(), 1);
        assert_eq!(layers[0].provider_id, "musicbrainz");
        assert_eq!(layers[0].fields, second.fields);
        assert_eq!(layers[0].updated_at, second.updated_at);

        Ok(())
    }

    #[test]
    fn upsert_layer_unchanged_fields_is_noop() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let node_id = insert_entity(&mut db)?;

        let first = MetadataLayer {
            db_id: None,
            id: nanoid!(),
            provider_id: "musicbrainz".to_string(),
            fields: r#"{"release_title":"same"}"#.to_string(),
            updated_at: 100,
        };
        let second = MetadataLayer {
            db_id: None,
            id: nanoid!(),
            provider_id: "musicbrainz".to_string(),
            fields: first.fields.clone(),
            updated_at: 200,
        };

        let first_id = upsert(&mut db, node_id, &first)?;
        let second_id = upsert(&mut db, node_id, &second)?;

        assert_eq!(first_id, second_id);

        let layers = get_for_entity(&db, node_id)?;
        assert_eq!(layers.len(), 1);
        assert_eq!(layers[0].fields, first.fields);
        assert_eq!(layers[0].updated_at, first.updated_at);

        Ok(())
    }
}
