// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

pub(crate) mod external_ids;

use agdb::{
    DbAny,
    DbElement,
    DbId,
    QueryBuilder,
};
use schemars::JsonSchema;
use serde::Serialize;

use super::NodeId;

#[derive(DbElement, Serialize, Clone, Debug, JsonSchema)]
pub(crate) struct ProviderConfig {
    #[serde(skip)]
    pub(crate) db_id: Option<NodeId>,
    pub(crate) id: String,
    pub(crate) provider_id: String,
    pub(crate) display_name: String,
    pub(crate) priority: u32,
    pub(crate) enabled: bool,
}

pub(crate) fn get(db: &impl super::DbAccess) -> anyhow::Result<Vec<ProviderConfig>> {
    let providers: Vec<ProviderConfig> = db
        .exec(
            QueryBuilder::select()
                .elements::<ProviderConfig>()
                .search()
                .from("providers")
                .where_()
                .neighbor()
                .query(),
        )?
        .try_into()?;

    Ok(providers)
}

pub(crate) fn get_by_provider_id(
    db: &DbAny,
    provider_id: &str,
) -> anyhow::Result<Option<ProviderConfig>> {
    let providers: Vec<ProviderConfig> = db
        .exec(
            QueryBuilder::select()
                .elements::<ProviderConfig>()
                .search()
                .from("providers")
                .where_()
                .key("provider_id")
                .value(provider_id)
                .query(),
        )?
        .try_into()?;

    Ok(providers.into_iter().next())
}

pub(crate) fn upsert(db: &mut DbAny, provider: &ProviderConfig) -> anyhow::Result<DbId> {
    let existing = get_by_provider_id(db, &provider.provider_id)?;
    let mut to_save = provider.clone();
    if let Some(ref e) = existing {
        to_save.db_id = e.db_id.clone();
    }

    let result = db.exec_mut(QueryBuilder::insert().element(&to_save).query())?;
    let id = existing
        .as_ref()
        .and_then(|e| e.db_id.clone())
        .map(DbId::from)
        .or_else(|| result.elements.first().map(|e| e.id))
        .ok_or_else(|| anyhow::anyhow!("upsert provider returned no id"))?;

    if existing.is_none() {
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from("providers")
                .to(id)
                .query(),
        )?;
    }

    Ok(id)
}

pub(crate) fn update_priority(
    db: &mut DbAny,
    provider_id: &str,
    priority: u32,
) -> anyhow::Result<()> {
    let providers: Vec<ProviderConfig> = db
        .exec(
            QueryBuilder::select()
                .elements::<ProviderConfig>()
                .search()
                .from("providers")
                .where_()
                .key("provider_id")
                .value(provider_id)
                .query(),
        )?
        .try_into()?;

    if let Some(mut provider) = providers.into_iter().next() {
        provider.priority = priority;
        db.exec_mut(QueryBuilder::insert().element(&provider).query())?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::test_db::TestDb;
    use agdb::CountComparison;
    use nanoid::nanoid;

    fn new_test_db() -> anyhow::Result<DbAny> {
        Ok(TestDb::with_root_aliases(&["providers"])?.into_inner())
    }

    fn make_provider(id: &str) -> ProviderConfig {
        ProviderConfig {
            db_id: None,
            id: nanoid!(),
            provider_id: id.to_string(),
            display_name: id.to_string(),
            priority: 0,
            enabled: true,
        }
    }

    fn outgoing_edge_count(db: &DbAny, alias: &str) -> anyhow::Result<u64> {
        let from = db.exec(QueryBuilder::select().ids(alias).query())?.ids()[0];
        let qr = db.exec(
            QueryBuilder::search()
                .from(from)
                .where_()
                .edge()
                .and()
                .distance(CountComparison::Equal(1))
                .query(),
        )?;

        Ok(qr
            .elements
            .into_iter()
            .filter(|edge| edge.from == Some(from))
            .count() as u64)
    }

    #[test]
    fn upsert_provider_is_idempotent() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let provider = make_provider("test-provider");

        let id1 = upsert(&mut db, &provider)?;
        let id2 = upsert(&mut db, &provider)?;

        assert_eq!(id1, id2);

        let providers = get(&db)?;
        assert_eq!(providers.len(), 1);

        // Only one edge from "providers" alias
        assert_eq!(outgoing_edge_count(&db, "providers")?, 1);

        Ok(())
    }

    #[test]
    fn upsert_provider_updates_fields() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let mut provider = make_provider("test-provider");
        provider.display_name = "Original".to_string();
        upsert(&mut db, &provider)?;

        provider.display_name = "Updated".to_string();
        provider.priority = 5;
        upsert(&mut db, &provider)?;

        let providers = get(&db)?;
        assert_eq!(providers.len(), 1);
        assert_eq!(providers[0].display_name, "Updated");
        assert_eq!(providers[0].priority, 5);

        Ok(())
    }
}
