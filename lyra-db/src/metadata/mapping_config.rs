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
use serde::{
    Deserialize,
    Serialize,
};

use crate::{
    NodeId,
    server,
};

/// Mapping rule destinations. Numbering and duration are read directly.
#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldName {
    Album,
    Title,
    AlbumArtists,
    Artists,
    Date,
    Copyright,
    Genre,
    Label,
    CatalogNumber,
}

/// Rules sharing a `destination` form a fallback chain: first
/// non-empty value wins; empty values never overwrite.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MappingRule {
    pub source_key: String,
    pub destination: FieldName,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataMappingConfig {
    pub rules: Vec<MappingRule>,
}

/// Rules are JSON, not parallel `VecString`s, so additive schema
/// changes don't require DB migrations.
#[derive(DbElement, Serialize, Clone, Debug)]
pub(crate) struct MetadataMappingConfigNode {
    #[serde(skip)]
    pub(crate) db_id: Option<NodeId>,
    pub(crate) id: String,
    pub(crate) rules_json: String,
}

pub fn ensure(
    db: &mut DbAny,
    default_config: impl FnOnce() -> MetadataMappingConfig,
) -> anyhow::Result<MetadataMappingConfig> {
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

pub fn get(db: &DbAny) -> anyhow::Result<Option<MetadataMappingConfig>> {
    let Some(node) = find_node(db)? else {
        return Ok(None);
    };
    decode(&node).map(Some)
}

/// Replaces the stored rules; callers validate them first.
pub fn update(db: &mut DbAny, config: &MetadataMappingConfig) -> anyhow::Result<()> {
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
            .and_then(|s| s.db_id)
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
        let node_id: DbId = existing
            .db_id
            .map(DbId::from)
            .ok_or_else(|| anyhow!("existing metadata mapping node missing db_id"))?;

        t.exec_mut(
            QueryBuilder::insert()
                .values_uniform([("rules_json", rules_json.as_str()).into()])
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
    Ok(MetadataMappingConfig { rules })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_db::new_test_db;

    fn album_rule_config() -> MetadataMappingConfig {
        MetadataMappingConfig {
            rules: vec![MappingRule {
                source_key: "AlbumTitle".to_string(),
                destination: FieldName::Album,
            }],
        }
    }

    #[test]
    fn ensure_seeds_default_and_is_idempotent() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let first = ensure(&mut db, album_rule_config)?;
        let second = ensure(&mut db, || unreachable!("config already seeded"))?;
        assert_eq!(first.rules.len(), second.rules.len());
        assert!(!first.rules.is_empty());
        Ok(())
    }

    #[test]
    fn update_replaces_rules() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        ensure(&mut db, || MetadataMappingConfig { rules: Vec::new() })?;
        update(&mut db, &album_rule_config())?;

        let loaded = get(&db)?.expect("config should exist");
        assert_eq!(loaded.rules.len(), 1);
        assert_eq!(loaded.rules[0].source_key, "AlbumTitle");
        Ok(())
    }
}
