// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

#[cfg(feature = "docgen")]
use std::borrow::Cow;

use agdb::{
    DbId,
    QueryId,
};
use harmony_luau::{
    LuauType,
    LuauTypeInfo,
};
use serde::{
    Deserialize,
    Serialize,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NodeId(DbId);

impl From<DbId> for NodeId {
    fn from(db_id: DbId) -> Self {
        NodeId(db_id)
    }
}

impl From<NodeId> for DbId {
    fn from(node_id: NodeId) -> Self {
        node_id.0
    }
}

impl From<NodeId> for QueryId {
    fn from(value: NodeId) -> Self {
        QueryId::Id(value.0)
    }
}

#[cfg(feature = "docgen")]
impl schemars::JsonSchema for NodeId {
    fn schema_name() -> Cow<'static, str> {
        "NodeId".into()
    }

    fn json_schema(schema_gen: &mut schemars::SchemaGenerator) -> schemars::Schema {
        <i64 as schemars::JsonSchema>::json_schema(schema_gen)
    }
}

impl LuauTypeInfo for NodeId {
    fn luau_type() -> LuauType {
        f64::luau_type()
    }
}

#[derive(Clone, Debug)]
pub enum ResolveId {
    DbId(DbId),
    Alias(String),
    Nanoid(String),
}

impl<'de> Deserialize<'de> for ResolveId {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        match value {
            serde_json::Value::Number(n) => {
                let i = n
                    .as_i64()
                    .ok_or_else(|| serde::de::Error::custom("expected i64"))?;
                Ok(ResolveId::DbId(DbId(i)))
            }
            serde_json::Value::String(s) => {
                if super::bootstrap::ROOT_COLLECTION_ALIASES.contains(&s.as_str()) {
                    Ok(ResolveId::Alias(s))
                } else {
                    Ok(ResolveId::Nanoid(s))
                }
            }
            _ => Err(serde::de::Error::custom("expected integer or string")),
        }
    }
}

impl ResolveId {
    /// Resolve this identifier to a `QueryId` suitable for agdb queries.
    /// Nanoid variants require a DB lookup to find the corresponding `DbId`.
    pub fn to_query_id(&self, db: &agdb::DbAny) -> anyhow::Result<Option<QueryId>> {
        match self {
            ResolveId::DbId(id) => Ok(Some(QueryId::Id(*id))),
            ResolveId::Alias(alias) => Ok(Some(QueryId::Alias(alias.clone()))),
            ResolveId::Nanoid(nanoid) => {
                let db_id = super::lookup::find_node_id_by_id(db, nanoid)?;
                Ok(db_id.map(QueryId::Id))
            }
        }
    }

    /// Resolve this identifier to a `DbId`. All variants are resolved to a
    /// concrete node ID — aliases are looked up via agdb, nanoids via the
    /// id→node lookup index.
    pub fn to_db_id(&self, db: &agdb::DbAny) -> anyhow::Result<Option<DbId>> {
        match self {
            ResolveId::DbId(id) => Ok(Some(*id)),
            ResolveId::Alias(alias) => {
                let result = db.exec(agdb::QueryBuilder::select().ids(alias.as_str()).query());
                match result {
                    Ok(r) => Ok(r.ids().first().copied()),
                    Err(_) => Ok(None),
                }
            }
            ResolveId::Nanoid(nanoid) => super::lookup::find_node_id_by_id(db, nanoid),
        }
    }
}

impl From<QueryId> for ResolveId {
    fn from(query_id: QueryId) -> Self {
        match query_id {
            QueryId::Id(id) => ResolveId::DbId(id),
            QueryId::Alias(alias) => ResolveId::Alias(alias),
        }
    }
}

impl From<DbId> for ResolveId {
    fn from(db_id: DbId) -> Self {
        ResolveId::DbId(db_id)
    }
}

impl ResolveId {
    /// Creates a ResolveId for a known root collection alias.
    pub fn alias(name: &str) -> Self {
        ResolveId::Alias(name.to_string())
    }
}

impl LuauTypeInfo for ResolveId {
    fn luau_type() -> LuauType {
        let mut variants: Vec<LuauType> = super::bootstrap::ROOT_COLLECTION_ALIASES
            .iter()
            .map(|alias| {
                let quoted: &'static str = Box::leak(format!("\"{alias}\"").into_boxed_str());
                LuauType::literal(quoted)
            })
            .collect();
        variants.push(f64::luau_type());
        variants.push(String::luau_type());
        LuauType::union(variants)
    }
}
