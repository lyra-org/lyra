// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

pub mod external_ids;

use agdb::{
    DbAny,
    DbElement,
    DbId,
    QueryBuilder,
};

use crate::roots::MEMBERS;
use serde::Serialize;

use super::NodeId;

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(DbElement, Serialize, Clone, Debug)]
pub struct ProviderConfig {
    #[serde(skip)]
    pub db_id: Option<NodeId>,
    pub provider_id: String,
    /// Zero-based rank; lower positions take precedence.
    pub position: u32,
    pub enabled: bool,
}

/// The one rule for provider ids: non-empty ASCII with no whitespace.
pub fn validate_provider_id(provider_id: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        !provider_id.is_empty()
            && provider_id.is_ascii()
            && !provider_id.chars().any(|c| c.is_whitespace()),
        "provider_id must be a non-empty ASCII string with no whitespace; got {provider_id:?}"
    );
    Ok(())
}

pub fn get(db: &impl super::DbAccess) -> anyhow::Result<Vec<ProviderConfig>> {
    let mut providers: Vec<ProviderConfig> = db
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
    sort(&mut providers);

    Ok(providers)
}

pub fn get_by_provider_id(db: &DbAny, provider_id: &str) -> anyhow::Result<Option<ProviderConfig>> {
    let providers: Vec<ProviderConfig> = db
        .exec(
            QueryBuilder::select()
                .elements::<ProviderConfig>()
                .search()
                .from("providers")
                .where_()
                .distance(MEMBERS)
                .and()
                .key("provider_id")
                .value(provider_id)
                .query(),
        )?
        .try_into()?;

    Ok(providers.into_iter().next())
}

pub fn upsert(db: &mut DbAny, provider: &ProviderConfig) -> anyhow::Result<DbId> {
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

/// Registers a provider at the end of the order unless it is already stored.
pub fn register(db: &mut DbAny, provider_id: &str) -> anyhow::Result<()> {
    if get_by_provider_id(db, provider_id)?.is_some() {
        return Ok(());
    }
    let position = u32::try_from(get(db)?.len())?;
    upsert(
        db,
        &ProviderConfig {
            db_id: None,
            provider_id: provider_id.to_string(),
            position,
            enabled: true,
        },
    )?;
    Ok(())
}

/// Moves a provider to `position`, clamped to the end, and renumbers the rest
/// without gaps. Returns `false` when the provider does not exist.
pub fn move_to(
    db: &mut impl super::DbAccess,
    provider_id: &str,
    position: u32,
) -> anyhow::Result<bool> {
    let mut providers = get(db)?;
    let Some(current) = providers
        .iter()
        .position(|provider| provider.provider_id == provider_id)
    else {
        return Ok(false);
    };
    let provider = providers.remove(current);
    let insert_at = usize::try_from(position)
        .unwrap_or(usize::MAX)
        .min(providers.len());
    providers.insert(insert_at, provider);

    for (index, mut provider) in providers.into_iter().enumerate() {
        let index = u32::try_from(index)?;
        if provider.position != index {
            provider.position = index;
            db.exec_mut(QueryBuilder::insert().element(&provider).query())?;
        }
    }
    Ok(true)
}

/// Returns `false` when the provider does not exist.
pub fn set_enabled(
    db: &mut impl super::DbAccess,
    provider_id: &str,
    enabled: bool,
) -> anyhow::Result<bool> {
    let Some(mut provider) = get(db)?
        .into_iter()
        .find(|provider| provider.provider_id == provider_id)
    else {
        return Ok(false);
    };
    if provider.enabled != enabled {
        provider.enabled = enabled;
        db.exec_mut(QueryBuilder::insert().element(&provider).query())?;
    }
    Ok(true)
}

fn sort(providers: &mut [ProviderConfig]) {
    providers.sort_by(|a, b| {
        a.position
            .cmp(&b.position)
            .then_with(|| a.provider_id.cmp(&b.provider_id))
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_db::TestDb;
    use agdb::CountComparison;

    fn new_test_db() -> anyhow::Result<DbAny> {
        Ok(TestDb::with_root_aliases(&["providers"])?.into_inner())
    }

    fn make_provider(id: &str) -> ProviderConfig {
        ProviderConfig {
            db_id: None,
            provider_id: id.to_string(),
            position: 0,
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
            .filter(|edge| edge.from == from)
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
        provider.position = 1;
        upsert(&mut db, &provider)?;

        provider.enabled = false;
        provider.position = 5;
        upsert(&mut db, &provider)?;

        let providers = get(&db)?;
        assert_eq!(providers.len(), 1);
        assert!(!providers[0].enabled);
        assert_eq!(providers[0].position, 5);

        Ok(())
    }

    fn order(db: &DbAny) -> anyhow::Result<Vec<(String, u32)>> {
        Ok(get(db)?
            .into_iter()
            .map(|provider| (provider.provider_id, provider.position))
            .collect())
    }

    #[test]
    fn register_appends_new_providers_and_keeps_existing_ones() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        register(&mut db, "a")?;
        register(&mut db, "b")?;
        move_to(&mut db, "b", 0)?;
        register(&mut db, "a")?;
        register(&mut db, "c")?;

        assert_eq!(
            order(&db)?,
            [("b".into(), 0), ("a".into(), 1), ("c".into(), 2)]
        );
        Ok(())
    }

    #[test]
    fn move_to_renumbers_without_gaps_and_clamps_to_the_end() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        for provider_id in ["a", "b", "c"] {
            register(&mut db, provider_id)?;
        }

        assert!(move_to(&mut db, "a", 1)?);
        assert_eq!(
            order(&db)?,
            [("b".into(), 0), ("a".into(), 1), ("c".into(), 2)]
        );

        assert!(move_to(&mut db, "b", 99)?);
        assert_eq!(
            order(&db)?,
            [("a".into(), 0), ("c".into(), 1), ("b".into(), 2)]
        );

        assert!(!move_to(&mut db, "missing", 0)?);
        Ok(())
    }

    #[test]
    fn set_enabled_reports_missing_providers() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        register(&mut db, "a")?;

        assert!(set_enabled(&mut db, "a", false)?);
        assert!(!get(&db)?[0].enabled);
        assert!(!set_enabled(&mut db, "missing", false)?);
        Ok(())
    }
}
