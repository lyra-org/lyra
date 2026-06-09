// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::{
    DbElement,
    DbId,
    QueryBuilder,
};

/// A subscribed plugin repository. `origin` is the canonical repository
/// URL and is unique across subscriptions.
#[derive(DbElement, Clone, Debug)]
pub(crate) struct PluginRepository {
    pub(crate) db_id: Option<DbId>,
    pub(crate) id: String,
    pub(crate) origin: String,
    pub(crate) name: String,
    pub(crate) description: String,
    pub(crate) git_ref: Option<String>,
    pub(crate) last_commit: Option<String>,
    pub(crate) refreshed_at_ms: u64,
}

pub(crate) fn create(
    db: &mut impl super::DbAccess,
    record: &PluginRepository,
) -> anyhow::Result<DbId> {
    if get_by_origin(db, &record.origin)?.is_some() {
        anyhow::bail!("repository already added: {}", record.origin);
    }

    let result = db.exec_mut(QueryBuilder::insert().element(record).query())?;
    let repo_db_id = result
        .ids()
        .first()
        .copied()
        .ok_or_else(|| anyhow::anyhow!("plugin repository insert returned no id"))?;
    db.exec_mut(
        QueryBuilder::insert()
            .edges()
            .from("plugin_repositories")
            .to(repo_db_id)
            .query(),
    )?;
    Ok(repo_db_id)
}

pub(crate) fn update(
    db: &mut impl super::DbAccess,
    record: &PluginRepository,
) -> anyhow::Result<()> {
    if record.db_id.is_none() {
        anyhow::bail!("cannot update plugin repository without db_id");
    }
    db.exec_mut(QueryBuilder::insert().element(record).query())?;
    Ok(())
}

pub(crate) fn list(db: &impl super::DbAccess) -> anyhow::Result<Vec<PluginRepository>> {
    let mut repositories: Vec<PluginRepository> = db
        .exec(
            QueryBuilder::select()
                .elements::<PluginRepository>()
                .search()
                .from("plugin_repositories")
                .query(),
        )?
        .try_into()?;
    repositories.sort_by(|a, b| a.name.cmp(&b.name).then(a.origin.cmp(&b.origin)));
    Ok(repositories)
}

pub(crate) fn get_by_id(
    db: &impl super::DbAccess,
    id: &str,
) -> anyhow::Result<Option<PluginRepository>> {
    let repositories: Vec<PluginRepository> = db
        .exec(
            QueryBuilder::select()
                .elements::<PluginRepository>()
                .search()
                .from("plugin_repositories")
                .where_()
                .key("id")
                .value(id)
                .end_where()
                .query(),
        )?
        .try_into()?;
    Ok(repositories.into_iter().next())
}

pub(crate) fn get_by_origin(
    db: &impl super::DbAccess,
    origin: &str,
) -> anyhow::Result<Option<PluginRepository>> {
    let repositories: Vec<PluginRepository> = db
        .exec(
            QueryBuilder::select()
                .elements::<PluginRepository>()
                .search()
                .from("plugin_repositories")
                .where_()
                .key("origin")
                .value(origin)
                .end_where()
                .query(),
        )?
        .try_into()?;
    Ok(repositories.into_iter().next())
}

pub(crate) fn remove(db: &mut impl super::DbAccess, db_id: DbId) -> anyhow::Result<()> {
    db.exec_mut(QueryBuilder::remove().ids(db_id).query())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::test_db::new_test_db;

    fn repository(origin: &str) -> PluginRepository {
        PluginRepository {
            db_id: None,
            id: format!("repo-{origin}"),
            origin: origin.to_string(),
            name: "Test Plugins".to_string(),
            description: String::new(),
            git_ref: None,
            last_commit: None,
            refreshed_at_ms: 0,
        }
    }

    #[test]
    fn create_list_update_and_remove_repositories() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let mut record = repository("https://github.com/lyra/plugins");
        let db_id = create(&mut db, &record)?;
        record.db_id = Some(db_id);

        record.name = "Renamed".to_string();
        record.last_commit = Some("a".repeat(40));
        record.refreshed_at_ms = 42;
        update(&mut db, &record)?;

        let listed = list(&db)?;
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].name, "Renamed");
        assert_eq!(listed[0].last_commit.as_deref(), Some(&*"a".repeat(40)));

        let fetched = get_by_id(&db, &record.id)?.expect("repository exists");
        assert_eq!(fetched.db_id, Some(db_id));
        assert!(get_by_origin(&db, "https://github.com/lyra/plugins")?.is_some());

        remove(&mut db, db_id)?;
        assert!(list(&db)?.is_empty());
        Ok(())
    }

    #[test]
    fn create_rejects_duplicate_origins() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        create(&mut db, &repository("https://github.com/lyra/plugins"))?;
        let err = create(&mut db, &repository("https://github.com/lyra/plugins")).unwrap_err();

        assert!(err.to_string().contains("already added"), "{err}");
        Ok(())
    }
}
