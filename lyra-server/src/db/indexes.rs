// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashSet;

use agdb::{
    QueryBuilder,
    QueryResult,
};

use super::DbAccess;

fn query_result_has_index(result: &QueryResult, name: &str) -> bool {
    result.elements.first().is_some_and(|element| {
        element
            .values
            .iter()
            .any(|kv| matches!(&kv.key, agdb::DbValue::String(key) if key == name))
    })
}

pub(crate) fn has_index(db: &impl DbAccess, name: &str) -> anyhow::Result<bool> {
    let result = db.exec(QueryBuilder::select().indexes().query())?;
    Ok(query_result_has_index(&result, name))
}

pub(crate) fn ensure_index(db: &mut impl DbAccess, name: &str) -> anyhow::Result<()> {
    if !has_index(db, name)? {
        db.exec_mut(QueryBuilder::insert().index(name).query())?;
    }

    Ok(())
}

pub(crate) fn ensure_indexes(db: &mut impl DbAccess, names: &[&str]) -> anyhow::Result<()> {
    if names.is_empty() {
        return Ok(());
    }

    let result = db.exec(QueryBuilder::select().indexes().query())?;
    let mut seen = HashSet::new();
    for name in names {
        if !seen.insert(*name) || query_result_has_index(&result, name) {
            continue;
        }
        db.exec_mut(QueryBuilder::insert().index(*name).query())?;
    }

    Ok(())
}
