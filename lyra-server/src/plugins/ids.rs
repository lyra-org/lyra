// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

#[cfg(feature = "docgen")]
use std::collections::HashMap;
use std::collections::HashSet;

use agdb::DbId;
use harmony_luau as luau;
use harmony_luau::{
    LuauType,
    LuauTypeInfo,
};

use crate::plugins::{
    args,
    args::UniqueIds,
    db::OptionalDb,
};

pub(crate) struct IdsModule;

/// An array of strings, trimmed, without blanks or repeats.
struct PublicIds(Vec<String>);

impl<'vm> luau::FromLuau<'vm> for PublicIds {
    fn read(reader: &mut luau::ArgReader<'vm>) -> luau::runtime::Result<Self> {
        let table: luau::Table = reader.read()?;
        parse_strings(&table).map(Self)
    }
}

impl LuauTypeInfo for PublicIds {
    fn luau_type() -> LuauType {
        Vec::<String>::luau_type()
    }
}

#[harmony_macros::module(id = "lyra/ids", docs_feature = "docgen")]
impl IdsModule {
    #[harmony(
        description = "Returns the public nanoid string for a given numeric database ID, or nil if not found."
    )]
    async fn get_id(
        db_id: i64,
        #[context] db: OptionalDb,
    ) -> luau::runtime::Result<Option<String>> {
        find_id_by_db_id(db.0, DbId(db_id)).await
    }

    #[harmony(
        description = "Returns public nanoid strings for many numeric database IDs.",
        returns(HashMap<u64, Option<String>>)
    )]
    async fn get_ids(
        db_ids: UniqueIds,
        #[context] db: OptionalDb,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let ids = db_ids.0;
        let resolved = find_ids_by_db_ids(db.0, &ids).await?;

        let table = luau::OwnedTable::from_entries(ids.into_iter().map(|id| {
            (
                id.0,
                resolved
                    .get(&id)
                    .map(|value| luau::Value::String(value.clone().into_bytes()))
                    .unwrap_or(luau::Value::Nil),
            )
        }));
        Ok(table)
    }

    #[harmony(
        description = "Returns the numeric database ID for a given public nanoid string, or nil if not found."
    )]
    async fn get_db_id(
        id: String,
        #[context] db: OptionalDb,
    ) -> luau::runtime::Result<Option<i64>> {
        Ok(find_node_id_by_id(db.0, &id).await?.map(|id| id.0))
    }

    #[harmony(
        description = "Returns numeric database IDs for many public nanoid strings.",
        returns(HashMap<String, Option<u64>>)
    )]
    async fn get_db_ids(
        ids: PublicIds,
        #[context] db: OptionalDb,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let ids = ids.0;
        let str_refs = ids.iter().map(String::as_str).collect::<Vec<_>>();
        let resolved = find_node_ids_by_ids(db.0, &str_refs).await?;

        let mut table = luau::OwnedTable::with_capacity(0, ids.len());
        for id in ids {
            table.set_field(
                id.as_str(),
                resolved
                    .get(&id)
                    .map(|db_id| luau::Value::from(db_id.0))
                    .unwrap_or(luau::Value::Nil),
            );
        }
        Ok(table)
    }
}

async fn find_id_by_db_id(
    db: Option<lyra_db::DbAsync>,
    db_id: DbId,
) -> luau::runtime::Result<Option<String>> {
    let Some(db) = db else {
        return Ok(None);
    };
    let db = db.read().await;
    lyra_db::lookup::find_id_by_db_id(&*db, db_id).map_err(crate::plugins::runtime_error)
}

async fn find_ids_by_db_ids(
    db: Option<lyra_db::DbAsync>,
    ids: &[DbId],
) -> luau::runtime::Result<std::collections::HashMap<DbId, String>> {
    let Some(db) = db else {
        return Ok(std::collections::HashMap::new());
    };
    let db = db.read().await;
    lyra_db::lookup::find_ids_by_db_ids(&*db, ids).map_err(crate::plugins::runtime_error)
}

async fn find_node_id_by_id(
    db: Option<lyra_db::DbAsync>,
    id: &str,
) -> luau::runtime::Result<Option<DbId>> {
    let Some(db) = db else {
        return Ok(None);
    };
    let db = db.read().await;
    lyra_db::lookup::find_node_id_by_id(&*db, id).map_err(crate::plugins::runtime_error)
}

async fn find_node_ids_by_ids(
    db: Option<lyra_db::DbAsync>,
    ids: &[&str],
) -> luau::runtime::Result<std::collections::HashMap<String, DbId>> {
    let Some(db) = db else {
        return Ok(std::collections::HashMap::new());
    };
    let db = db.read().await;
    lyra_db::lookup::find_node_ids_by_ids(&*db, ids).map_err(crate::plugins::runtime_error)
}
fn parse_strings(table: &luau::Table) -> luau::runtime::Result<Vec<String>> {
    let mut seen = HashSet::new();
    Ok(args::strings(table)?
        .into_iter()
        .map(|id| id.trim().to_string())
        .filter(|id| !id.is_empty() && seen.insert(id.clone()))
        .collect())
}

#[cfg(feature = "docgen")]
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    use harmony_luau::DescribeModule;

    harmony_luau::render_definition_file_with_support(
        &IdsModule::module_descriptor(),
        &[],
        &[],
        &[],
    )
}
