// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::path::PathBuf;

use agdb::{
    DbAny,
    DbElement,
    DbId,
    QueryBuilder,
};
use anyhow::anyhow;

#[derive(DbElement, Clone, Debug)]
pub(crate) struct Library {
    pub(crate) db_id: Option<DbId>,
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) directory: PathBuf,
    pub(crate) language: Option<String>,
    pub(crate) country: Option<String>,
}

impl mlua::IntoLua for Library {
    fn into_lua(self, lua: &mlua::Lua) -> mlua::Result<mlua::Value> {
        let table = lua.create_table()?;
        if let Some(db_id) = self.db_id {
            table.set("db_id", db_id.0)?;
        }
        table.set("id", self.id)?;
        table.set("name", self.name)?;
        table.set("directory", self.directory.to_string_lossy().to_string())?;
        if let Some(language) = self.language {
            table.set("language", language)?;
        }
        if let Some(country) = self.country {
            table.set("country", country)?;
        }
        Ok(mlua::Value::Table(table))
    }
}

pub(crate) fn get(db: &DbAny) -> anyhow::Result<Vec<Library>> {
    let libraries: Vec<Library> = db
        .exec(
            QueryBuilder::select()
                .elements::<Library>()
                .search()
                .from("libraries")
                .query(),
        )?
        .try_into()?;

    Ok(libraries)
}

pub(crate) fn get_by_id(
    db: &impl super::DbAccess,
    library_db_id: DbId,
) -> anyhow::Result<Option<Library>> {
    super::graph::fetch_typed_by_id(db, library_db_id, "Library")
}

pub(crate) fn get_by_alias(db: &DbAny, alias: &str) -> anyhow::Result<Vec<Library>> {
    let libraries: Vec<Library> = db
        .exec(
            QueryBuilder::select()
                .elements::<Library>()
                .search()
                .from(alias)
                .query(),
        )?
        .try_into()?;

    Ok(libraries)
}

pub(crate) fn get_for_entity(db: &DbAny, node_id: DbId) -> anyhow::Result<Vec<Library>> {
    let libraries: Vec<Library> = db
        .exec(
            QueryBuilder::select()
                .elements::<Library>()
                .search()
                .to(node_id)
                .where_()
                .not_beyond()
                .key("db_element_id")
                .value("Library")
                .query(),
        )?
        .try_into()?;
    Ok(libraries)
}

pub(crate) fn get_by_release(db: &DbAny, release_db_id: DbId) -> anyhow::Result<Vec<Library>> {
    let libraries: Vec<Library> = db
        .exec(
            QueryBuilder::select()
                .elements::<Library>()
                .search()
                .to(release_db_id)
                .where_()
                .neighbor()
                .end_where()
                .query(),
        )?
        .try_into()?;

    Ok(libraries)
}

/// Resolves the owning library for each entity, caching intermediate results.
pub(crate) fn get_for_entities(
    db: &DbAny,
    entity_ids: &[DbId],
) -> anyhow::Result<std::collections::HashMap<DbId, Library>> {
    use std::collections::{
        HashMap,
        HashSet,
    };

    let unique_ids = super::dedup_positive_ids(entity_ids);
    if unique_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let all_libraries = get(db)?;
    if all_libraries.is_empty() {
        return Ok(HashMap::new());
    }

    let library_id_set: HashSet<DbId> = all_libraries.iter().filter_map(|lib| lib.db_id).collect();
    let libraries_by_id: HashMap<DbId, &Library> = all_libraries
        .iter()
        .filter_map(|lib| lib.db_id.map(|id| (id, lib)))
        .collect();

    let mut resolved_cache: HashMap<DbId, DbId> = HashMap::new();
    let mut result = HashMap::new();

    for entity_id in unique_ids {
        if library_id_set.contains(&entity_id) {
            if let Some(&lib) = libraries_by_id.get(&entity_id) {
                result.insert(entity_id, lib.clone());
            }
            continue;
        }

        if let Some(&lib_id) = resolved_cache.get(&entity_id) {
            if let Some(&lib) = libraries_by_id.get(&lib_id) {
                result.insert(entity_id, lib.clone());
            }
            continue;
        }

        let ancestors = db.exec(
            QueryBuilder::search()
                .to(entity_id)
                .where_()
                .node()
                .and()
                .not_beyond()
                .key("db_element_id")
                .value("Library")
                .query(),
        )?;

        for ancestor in &ancestors.elements {
            if ancestor.id.0 > 0 && library_id_set.contains(&ancestor.id) {
                if let Some(&lib) = libraries_by_id.get(&ancestor.id) {
                    result.insert(entity_id, lib.clone());
                    for node in &ancestors.elements {
                        if node.id.0 > 0 && node.id != ancestor.id {
                            resolved_cache.insert(node.id, ancestor.id);
                        }
                    }
                    resolved_cache.insert(entity_id, ancestor.id);
                }
                break;
            }
        }
    }

    Ok(result)
}

pub(crate) fn create(db: &mut DbAny, library: &Library) -> anyhow::Result<Library> {
    let mut created = library.clone();
    db.transaction_mut(|t| -> anyhow::Result<()> {
        let qr = t.exec_mut(QueryBuilder::insert().element(&created).query())?;
        let library_db_id = qr
            .ids()
            .first()
            .copied()
            .ok_or_else(|| anyhow!("library insert missing id"))?;
        created.db_id = Some(library_db_id);
        t.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from("libraries")
                .to(library_db_id)
                .query(),
        )?;
        Ok(())
    })?;

    Ok(created)
}

pub(crate) fn update(
    db: &mut DbAny,
    library: &Library,
    clear_language: bool,
    clear_country: bool,
) -> anyhow::Result<()> {
    let library_db_id = library
        .db_id
        .ok_or_else(|| anyhow!("library update missing db_id"))?;
    db.transaction_mut(|t| -> anyhow::Result<()> {
        if clear_language {
            t.exec_mut(
                QueryBuilder::remove()
                    .values(["language".to_string()])
                    .ids(library_db_id)
                    .query(),
            )?;
        }
        if clear_country {
            t.exec_mut(
                QueryBuilder::remove()
                    .values(["country".to_string()])
                    .ids(library_db_id)
                    .query(),
            )?;
        }
        t.exec_mut(QueryBuilder::insert().element(library).query())?;
        Ok(())
    })?;

    Ok(())
}
