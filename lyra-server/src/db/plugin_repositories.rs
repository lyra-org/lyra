// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::{
    DbAny,
    DbAnyTransactionMut,
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

const DEFAULT_ORIGIN: &str = "https://git.lyra.pub/lyra/lyra?forge=gitlab";
const DEFAULT_SEEDED_KEY: &str = "default_repository_seeded";

pub(crate) fn seed_default(db: &mut DbAny) -> anyhow::Result<()> {
    db.transaction_mut(|t| {
        let root = t.exec(QueryBuilder::select().ids("plugin_repositories").query())?;
        if root.elements[0]
            .values
            .iter()
            .any(|kv| kv.key == DEFAULT_SEEDED_KEY.into())
        {
            return Ok(());
        }
        if get_by_origin(t, DEFAULT_ORIGIN)?.is_none() {
            let manifest = harmony_repository::RepositoryManifest::parse(
                include_str!("../../../repository.json"),
                None,
            )?;
            let release_tag = env!("LYRA_RELEASE_TAG");
            create(
                t,
                &PluginRepository {
                    db_id: None,
                    id: nanoid::nanoid!(),
                    origin: DEFAULT_ORIGIN.to_string(),
                    name: manifest.name,
                    description: manifest.description,
                    git_ref: (!release_tag.is_empty()).then(|| release_tag.to_string()),
                    last_commit: None,
                    refreshed_at_ms: 0,
                },
            )?;
        }
        t.exec_mut(
            QueryBuilder::insert()
                .values_uniform([(DEFAULT_SEEDED_KEY, true).into()])
                .ids("plugin_repositories")
                .query(),
        )?;
        Ok(())
    })
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

/// Atomically aligns the stored row to `record`.
pub(crate) fn update(db: &mut DbAny, record: &PluginRepository) -> anyhow::Result<()> {
    db.transaction_mut(|t| update_in_transaction(t, record))
}

pub(crate) fn update_in_transaction(
    db: &mut DbAnyTransactionMut<'_>,
    record: &PluginRepository,
) -> anyhow::Result<()> {
    let repo_db_id = record
        .db_id
        .ok_or_else(|| anyhow::anyhow!("cannot update plugin repository without db_id"))?;
    super::replace_element_in_transaction(
        db,
        repo_db_id,
        [
            ("git_ref", record.git_ref.is_none()),
            ("last_commit", record.last_commit.is_none()),
        ],
        record,
    )
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

    #[test]
    fn default_repository_is_seeded_once_and_removal_survives_reopening() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("db.agdb");
        let config = crate::config::DbConfig {
            path,
            ..Default::default()
        };
        let created = super::super::bootstrap::create(&config)?;
        let id = {
            let mut db = created.db.try_write()?;
            seed_default(&mut db)?;
            let repositories = list(&*db)?;
            assert_eq!(repositories.len(), 1);
            let record = &repositories[0];
            assert_eq!(record.origin, DEFAULT_ORIGIN);
            let tag = env!("LYRA_RELEASE_TAG");
            assert_eq!(record.git_ref.as_deref(), (!tag.is_empty()).then_some(tag));
            assert_eq!(record.last_commit, None);
            assert_eq!(record.refreshed_at_ms, 0);
            record.db_id.unwrap()
        };
        remove(&mut *created.db.try_write()?, id)?;
        drop(created);
        let reopened = super::super::bootstrap::create(&config)?;
        assert!(list(&*reopened.db.try_read()?)?.is_empty());
        Ok(())
    }

    #[test]
    fn seed_preserves_existing_subscription() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let mut existing = repository(DEFAULT_ORIGIN);
        existing.git_ref = Some("custom-branch".into());
        create(&mut db, &existing)?;
        seed_default(&mut db)?;
        let rows = list(&db)?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].git_ref.as_deref(), Some("custom-branch"));
        Ok(())
    }

    #[test]
    fn default_catalog_lists_all_shipped_plugin_manifests() -> anyhow::Result<()> {
        use std::collections::BTreeSet;
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap();
        let catalog = harmony_repository::RepositoryManifest::parse(
            include_str!("../../../repository.json"),
            None,
        )?;
        let listed = catalog
            .entries
            .into_iter()
            .map(|entry| {
                let harmony_repository::RepositoryEntry::Path { path } = entry else {
                    panic!("official plugins should live in this repository");
                };
                assert!(root.join(&path).join("plugin.json").is_file());
                path
            })
            .collect::<BTreeSet<_>>();
        let shipped = std::fs::read_dir(root.join("plugins"))?
            .map(|entry| entry.map(|entry| entry.path()))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .filter(|path| path.join("plugin.json").is_file())
            .map(|path| {
                path.strip_prefix(root)
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_string()
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(listed, shipped);
        Ok(())
    }

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
    fn update_clears_a_dropped_last_commit() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let mut record = repository("https://github.com/lyra/plugins");
        record.db_id = Some(create(&mut db, &record)?);
        record.last_commit = Some("a".repeat(40));
        update(&mut db, &record)?;

        // A later refresh where the forge commit API call failed.
        record.last_commit = None;
        update(&mut db, &record)?;

        let fetched = get_by_id(&db, &record.id)?.expect("repository exists");
        assert_eq!(
            fetched.last_commit, None,
            "an unresolved commit must not leave a stale SHA driving update checks"
        );
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
