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
use anyhow::{
    Context,
    anyhow,
};
use nanoid::nanoid;
use serde::Serialize;

use crate::{
    db::{
        NodeId,
        server,
    },
    services::metadata::mapping::{
        MetadataMappingConfig,
        default_config,
        resolve_item_key,
    },
};

/// Rules are JSON, not parallel `VecString`s, so additive schema
/// changes don't require DB migrations.
#[derive(DbElement, Serialize, Clone, Debug)]
pub(crate) struct MetadataMappingConfigNode {
    #[serde(skip)]
    pub(crate) db_id: Option<NodeId>,
    pub(crate) id: String,
    pub(crate) rules_json: String,
    pub(crate) version: u64,
}

pub(crate) fn ensure(db: &mut DbAny) -> anyhow::Result<MetadataMappingConfig> {
    if let Some(config) = get(db)? {
        return Ok(config);
    }

    let server_info = server::ensure(db)?;
    let server_db_id: DbId = server_info
        .db_id
        .ok_or_else(|| anyhow!("server info missing db_id after ensure"))?;

    let default = default_config();
    let rules_json =
        serde_json::to_string(&default.rules).context("serialising default mapping rules")?;

    db.transaction_mut(|t| -> anyhow::Result<()> {
        // Re-check inside the transaction; otherwise two concurrent
        // `ensure` callers both pass the outer `get` and both insert,
        // leaving two config nodes edged off ServerInfo.
        let existing: Vec<MetadataMappingConfigNode> = t
            .exec(
                QueryBuilder::select()
                    .elements::<MetadataMappingConfigNode>()
                    .search()
                    .from(server_db_id)
                    .where_()
                    .neighbor()
                    .end_where()
                    .query(),
            )?
            .try_into()?;
        if !existing.is_empty() {
            return Ok(());
        }
        let node = MetadataMappingConfigNode {
            db_id: None,
            id: nanoid!(),
            rules_json,
            version: default.version,
        };
        let result = t.exec_mut(QueryBuilder::insert().element(&node).query())?;
        let inserted_id = result.ids()[0];
        t.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(server_db_id)
                .to(inserted_id)
                .query(),
        )?;
        Ok(())
    })?;

    get(db)?.ok_or_else(|| anyhow!("metadata mapping config missing after ensure"))
}

pub(crate) fn get(db: &DbAny) -> anyhow::Result<Option<MetadataMappingConfig>> {
    let Some(node) = find_node(db)? else {
        return Ok(None);
    };
    decode(&node).map(Some)
}

/// Persist `config`, asserting its version strictly exceeds the
/// currently stored one. Every rule's `source_key` must resolve to a
/// known [`ItemKey`] variant; unknown keys fail the write at the DB
/// boundary so scripts and repair tools can't bypass the route
/// validator.
pub(crate) fn update(db: &mut DbAny, config: &MetadataMappingConfig) -> anyhow::Result<()> {
    for rule in &config.rules {
        if resolve_item_key(&rule.source_key).is_none() {
            anyhow::bail!(
                "unsupported source_key '{}': not a recognised ItemKey variant",
                rule.source_key
            );
        }
    }

    let rules_json = serde_json::to_string(&config.rules).context("serialising mapping rules")?;

    db.transaction_mut(|t| -> anyhow::Result<()> {
        let server_infos: Vec<server::ServerInfo> = t
            .exec(
                QueryBuilder::select()
                    .elements::<server::ServerInfo>()
                    .search()
                    .from("server")
                    .where_()
                    .neighbor()
                    .end_where()
                    .query(),
            )?
            .try_into()?;
        let server_db_id: DbId = server_infos
            .into_iter()
            .next()
            .and_then(|s| s.db_id.map(DbId::from))
            .ok_or_else(|| anyhow!("server info missing"))?;

        let nodes: Vec<MetadataMappingConfigNode> = t
            .exec(
                QueryBuilder::select()
                    .elements::<MetadataMappingConfigNode>()
                    .search()
                    .from(server_db_id)
                    .where_()
                    .neighbor()
                    .end_where()
                    .query(),
            )?
            .try_into()?;
        let existing = nodes
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("metadata mapping config node missing; call ensure() first"))?;
        if config.version <= existing.version {
            anyhow::bail!(
                "metadata mapping version must strictly increase: existing {}, new {}",
                existing.version,
                config.version
            );
        }
        let node_id: DbId = existing
            .db_id
            .map(DbId::from)
            .ok_or_else(|| anyhow!("existing metadata mapping node missing db_id"))?;

        t.exec_mut(
            QueryBuilder::insert()
                .values_uniform([
                    ("rules_json", rules_json.as_str()).into(),
                    ("version", config.version).into(),
                ])
                .ids(node_id)
                .query(),
        )?;
        Ok(())
    })
}

/// Restore a previously-snapshotted config without enforcing
/// monotonicity. Reserved for the reingest failure path so a
/// bumped-then-failed commit doesn't leave users staring at a
/// version number that claims a state the data never reached.
///
/// Does not undo per-track ingestion that already ran under the
/// bumped rules; without a per-entry version stamp the inconsistent
/// window is not directly observable. Subsequent scans converge.
pub(crate) fn rollback_to(db: &mut DbAny, config: &MetadataMappingConfig) -> anyhow::Result<()> {
    let rules_json = serde_json::to_string(&config.rules).context("serialising mapping rules")?;

    db.transaction_mut(|t| -> anyhow::Result<()> {
        let server_infos: Vec<server::ServerInfo> = t
            .exec(
                QueryBuilder::select()
                    .elements::<server::ServerInfo>()
                    .search()
                    .from("server")
                    .where_()
                    .neighbor()
                    .end_where()
                    .query(),
            )?
            .try_into()?;
        let server_db_id: DbId = server_infos
            .into_iter()
            .next()
            .and_then(|s| s.db_id.map(DbId::from))
            .ok_or_else(|| anyhow!("server info missing"))?;
        let nodes: Vec<MetadataMappingConfigNode> = t
            .exec(
                QueryBuilder::select()
                    .elements::<MetadataMappingConfigNode>()
                    .search()
                    .from(server_db_id)
                    .where_()
                    .neighbor()
                    .end_where()
                    .query(),
            )?
            .try_into()?;
        let node_id: DbId = nodes
            .into_iter()
            .next()
            .and_then(|n| n.db_id.map(DbId::from))
            .ok_or_else(|| anyhow!("metadata mapping config node missing"))?;
        t.exec_mut(
            QueryBuilder::insert()
                .values_uniform([
                    ("rules_json", rules_json.as_str()).into(),
                    ("version", config.version).into(),
                ])
                .ids(node_id)
                .query(),
        )?;
        Ok(())
    })
}

fn find_node(db: &DbAny) -> anyhow::Result<Option<MetadataMappingConfigNode>> {
    let Some(server_info) = server::get(db)? else {
        return Ok(None);
    };
    let server_db_id: DbId = server_info
        .db_id
        .ok_or_else(|| anyhow!("server info missing db_id"))?;

    let mut nodes: Vec<MetadataMappingConfigNode> = db
        .exec(
            QueryBuilder::select()
                .elements::<MetadataMappingConfigNode>()
                .search()
                .from(server_db_id)
                .where_()
                .neighbor()
                .end_where()
                .query(),
        )?
        .try_into()?;
    Ok(nodes.pop())
}

fn decode(node: &MetadataMappingConfigNode) -> anyhow::Result<MetadataMappingConfig> {
    let rules =
        serde_json::from_str(&node.rules_json).context("deserialising mapping rules from node")?;
    Ok(MetadataMappingConfig {
        rules,
        version: node.version,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::test_db::new_test_db;

    #[test]
    fn ensure_seeds_default_and_is_idempotent() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let first = ensure(&mut db)?;
        let second = ensure(&mut db)?;
        assert_eq!(first.version, second.version);
        assert_eq!(first.rules.len(), second.rules.len());
        assert!(!first.rules.is_empty());
        Ok(())
    }

    #[test]
    fn update_bumps_version_and_replaces_rules() -> anyhow::Result<()> {
        use crate::services::metadata::mapping::{
            FieldName,
            MappingRule,
        };

        let mut db = new_test_db()?;
        let original = ensure(&mut db)?;
        let new_config = MetadataMappingConfig {
            version: original.version + 1,
            rules: vec![MappingRule {
                source_key: "AlbumTitle".to_string(),
                destination: FieldName::Album,
            }],
        };
        update(&mut db, &new_config)?;

        let loaded = get(&db)?.expect("config should exist");
        assert_eq!(loaded.version, original.version + 1);
        assert_eq!(loaded.rules.len(), 1);
        assert_eq!(loaded.rules[0].source_key, "AlbumTitle");
        Ok(())
    }

    #[test]
    fn update_rejects_non_monotonic_version() -> anyhow::Result<()> {
        use crate::services::metadata::mapping::{
            FieldName,
            MappingRule,
        };

        let mut db = new_test_db()?;
        let original = ensure(&mut db)?;
        let stale = MetadataMappingConfig {
            version: original.version,
            rules: vec![MappingRule {
                source_key: "AlbumTitle".to_string(),
                destination: FieldName::Album,
            }],
        };
        let err = update(&mut db, &stale).unwrap_err();
        assert!(format!("{err}").contains("strictly increase"));
        Ok(())
    }

    #[test]
    fn update_rejects_unknown_source_key() -> anyhow::Result<()> {
        use crate::services::metadata::mapping::{
            FieldName,
            MappingRule,
        };

        let mut db = new_test_db()?;
        let original = ensure(&mut db)?;
        let bogus = MetadataMappingConfig {
            version: original.version + 1,
            rules: vec![MappingRule {
                source_key: "NotARealKey".to_string(),
                destination: FieldName::Album,
            }],
        };
        let err = update(&mut db, &bogus).unwrap_err();
        assert!(format!("{err}").contains("unsupported source_key"));
        Ok(())
    }
}
