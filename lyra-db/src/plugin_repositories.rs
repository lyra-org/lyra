// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::{
    CountComparison,
    DbAny,
    DbAnyTransactionMut,
    DbElement,
    DbId,
    DbSerialize,
    DbTypeMarker,
    DbValue,
    QueryBuilder,
};

use crate::roots::MEMBERS;

/// A subscribed plugin repository. `origin` is the canonical repository
/// URL and is unique across subscriptions.
#[derive(DbElement, Clone, Debug)]
pub struct PluginRepository {
    pub db_id: Option<DbId>,
    pub id: String,
    pub origin: String,
    pub name: String,
    pub description: String,
    pub git_ref: Option<String>,
    /// The branch, tag, or commit the subscription resolved to at its last
    /// refresh.
    pub resolved_ref: Option<String>,
    pub commit: Option<String>,
    /// Unset until the first refresh.
    pub refreshed_at_ms: Option<u64>,
}

/// One plugin a subscribed repository offers, as of its last refresh or
/// update check. Linked from its repository.
#[derive(DbElement, Clone, Debug, PartialEq, Eq)]
pub struct PluginRepositoryEntry {
    pub db_id: Option<DbId>,
    pub plugin_id: String,
    /// Directory within the repository; unset for a root-level plugin.
    pub subpath: Option<String>,
    /// The repository holding the plugin, when the subscription's index
    /// references it there rather than holding it itself.
    pub source_origin: Option<String>,
    /// The plugin's version at the release the server would install;
    /// unset when there is none.
    pub version: Option<String>,
    /// The plugin's name and description at that release.
    pub name: Option<String>,
    pub description: Option<String>,
    /// That release's commit.
    pub commit: Option<String>,
    /// The scopes the plugin declares at that release.
    pub scopes: Option<Vec<String>>,
    /// The server requirement that excludes the server, when no release
    /// runs on it.
    pub requires: Option<String>,
    /// Why there is no release the server would install, when there is
    /// none.
    pub error: Option<String>,
    /// The plugins the plugin depends on at that release.
    pub dependencies: Option<Vec<PluginDependency>>,
    /// What the plugin's author says some of its scopes are for at that
    /// release; unset on entries stored before reasons were.
    pub scope_reasons: Option<Vec<PluginScopeReason>>,
    /// Where the listing placed the plugin, assigned on storing; unset on
    /// entries stored before positions were.
    pub position: Option<u64>,
}

/// A plugin a release depends on, as its `plugin.json` declares it.
#[derive(DbTypeMarker, DbSerialize, DbValue, Clone, Debug, PartialEq, Eq)]
pub struct PluginDependency {
    pub plugin_id: String,
    /// The version requirement on it.
    pub version: String,
    /// Whether the plugin loads without it.
    pub optional: bool,
}

/// Why a release's author says it needs a scope, as its `plugin.json` gives
/// it.
#[derive(DbTypeMarker, DbSerialize, DbValue, Clone, Debug, PartialEq, Eq)]
pub struct PluginScopeReason {
    pub scope: String,
    pub reason: String,
}

const DEFAULT_SEEDED_KEY: &str = "default_repository_seeded";

/// Subscribes `default` once per database, unless its origin is already
/// subscribed. Later calls are no-ops, so removing it survives restarts.
pub fn seed_default(db: &mut DbAny, default: &PluginRepository) -> anyhow::Result<()> {
    db.transaction_mut(|t| {
        let root = t.exec(QueryBuilder::select().ids("plugin_repositories").query())?;
        if root.elements[0]
            .values
            .iter()
            .any(|kv| kv.key == DEFAULT_SEEDED_KEY.into())
        {
            return Ok(());
        }
        if get_by_origin(t, &default.origin)?.is_none() {
            create(t, default, &[])?;
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

/// Stores a new subscription with its plugin entries. Atomic when `db` is
/// a transaction.
pub fn create(
    db: &mut impl super::DbAccess,
    record: &PluginRepository,
    entries: &[PluginRepositoryEntry],
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
    insert_entries(db, repo_db_id, entries)?;
    Ok(repo_db_id)
}

/// Atomically aligns the stored row to `record` and replaces its plugin
/// entries with `entries`. Returns `false`, changing nothing, when the
/// subscription `record` was read from is gone, so its `db_id` may name
/// another element now.
pub fn update(
    db: &mut DbAny,
    record: &PluginRepository,
    entries: &[PluginRepositoryEntry],
) -> anyhow::Result<bool> {
    let repo_db_id = record
        .db_id
        .ok_or_else(|| anyhow::anyhow!("cannot update plugin repository without db_id"))?;
    db.transaction_mut(|t| {
        if get_by_id(t, &record.id)?.and_then(|stored| stored.db_id) != Some(repo_db_id) {
            return Ok(false);
        }
        update_in_transaction(t, record)?;
        remove_entries(t, repo_db_id)?;
        insert_entries(t, repo_db_id, entries)?;
        Ok(true)
    })
}

/// The plugin entries stored on a subscription, in the order they were
/// stored; those without a position last.
pub fn entries(
    db: &impl super::DbAccess,
    repo_db_id: DbId,
) -> anyhow::Result<Vec<PluginRepositoryEntry>> {
    let mut entries: Vec<PluginRepositoryEntry> = db
        .exec(
            QueryBuilder::select()
                .elements::<PluginRepositoryEntry>()
                .search()
                .from(repo_db_id)
                .where_()
                .distance(CountComparison::Equal(2))
                .end_where()
                .query(),
        )?
        .try_into()?;
    entries.sort_by_key(|entry| (entry.position.is_none(), entry.position));
    Ok(entries)
}

fn insert_entries(
    db: &mut impl super::DbAccess,
    repo_db_id: DbId,
    entries: &[PluginRepositoryEntry],
) -> anyhow::Result<()> {
    if entries.is_empty() {
        return Ok(());
    }
    let entries: Vec<PluginRepositoryEntry> = entries
        .iter()
        .zip(0..)
        .map(|(entry, position)| PluginRepositoryEntry {
            db_id: None,
            position: Some(position),
            ..entry.clone()
        })
        .collect();
    let entry_ids = db
        .exec_mut(QueryBuilder::insert().elements(&entries).query())?
        .ids();
    db.exec_mut(
        QueryBuilder::insert()
            .edges()
            .from(repo_db_id)
            .to(entry_ids)
            .query(),
    )?;
    Ok(())
}

fn remove_entries(db: &mut impl super::DbAccess, repo_db_id: DbId) -> anyhow::Result<()> {
    db.exec_mut(
        QueryBuilder::remove()
            .search()
            .from(repo_db_id)
            .where_()
            .distance(CountComparison::Equal(2))
            .and()
            .key("db_element_id")
            .value("PluginRepositoryEntry")
            .end_where()
            .query(),
    )?;
    Ok(())
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
            ("resolved_ref", record.resolved_ref.is_none()),
            ("commit", record.commit.is_none()),
            ("refreshed_at_ms", record.refreshed_at_ms.is_none()),
        ],
        record,
    )
}

pub fn list(db: &impl super::DbAccess) -> anyhow::Result<Vec<PluginRepository>> {
    let mut repositories: Vec<PluginRepository> = db
        .exec(
            QueryBuilder::select()
                .elements::<PluginRepository>()
                .search()
                .from("plugin_repositories")
                .where_()
                .distance(MEMBERS)
                .query(),
        )?
        .try_into()?;
    repositories.sort_by(|a, b| a.name.cmp(&b.name).then(a.origin.cmp(&b.origin)));
    Ok(repositories)
}

pub fn get_by_id(db: &impl super::DbAccess, id: &str) -> anyhow::Result<Option<PluginRepository>> {
    let repositories: Vec<PluginRepository> = db
        .exec(
            QueryBuilder::select()
                .elements::<PluginRepository>()
                .search()
                .from("plugin_repositories")
                .where_()
                .distance(MEMBERS)
                .and()
                .key("id")
                .value(id)
                .end_where()
                .query(),
        )?
        .try_into()?;
    Ok(repositories.into_iter().next())
}

pub fn get_by_origin(
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
                .distance(MEMBERS)
                .and()
                .key("origin")
                .value(origin)
                .end_where()
                .query(),
        )?
        .try_into()?;
    Ok(repositories.into_iter().next())
}

/// Removes a subscription and its plugin entries.
pub fn remove(db: &mut DbAny, db_id: DbId) -> anyhow::Result<()> {
    db.transaction_mut(|t| {
        remove_entries(t, db_id)?;
        t.exec_mut(QueryBuilder::remove().ids(db_id).query())?;
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_db::new_test_db;

    #[test]
    fn default_repository_is_seeded_once_and_removal_survives_reopening() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("db.agdb");
        let config = crate::bootstrap::DbConfig {
            path,
            ..Default::default()
        };
        let default = repository("https://example.com/plugins");
        let created = super::super::bootstrap::create(&config)?;
        let id = {
            let mut db = created.db.try_write()?;
            seed_default(&mut db, &default)?;
            let repositories = list(&*db)?;
            assert_eq!(repositories.len(), 1);
            assert_eq!(repositories[0].origin, default.origin);
            repositories[0].db_id.unwrap()
        };
        remove(&mut *created.db.try_write()?, id)?;
        drop(created);
        let reopened = super::super::bootstrap::create(&config)?;
        seed_default(&mut *reopened.db.try_write()?, &default)?;
        assert!(list(&*reopened.db.try_read()?)?.is_empty());
        Ok(())
    }

    #[test]
    fn seed_preserves_existing_subscription() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let default = repository("https://example.com/plugins");
        let mut existing = repository(&default.origin);
        existing.git_ref = Some("custom-branch".into());
        create(&mut db, &existing, &[])?;
        seed_default(&mut db, &default)?;
        let rows = list(&db)?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].git_ref.as_deref(), Some("custom-branch"));
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
            resolved_ref: None,
            commit: None,
            refreshed_at_ms: None,
        }
    }

    #[test]
    fn create_list_update_and_remove_repositories() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let mut record = repository("https://github.com/lyra/plugins");
        let db_id = create(&mut db, &record, &[])?;
        record.db_id = Some(db_id);

        record.name = "Renamed".to_string();
        record.commit = Some("a".repeat(40));
        record.refreshed_at_ms = Some(42);
        update(&mut db, &record, &[])?;

        let listed = list(&db)?;
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].name, "Renamed");
        assert_eq!(listed[0].commit.as_deref(), Some(&*"a".repeat(40)));
        assert_eq!(listed[0].refreshed_at_ms, Some(42));

        let fetched = get_by_id(&db, &record.id)?.expect("repository exists");
        assert_eq!(fetched.db_id, Some(db_id));
        assert!(get_by_origin(&db, "https://github.com/lyra/plugins")?.is_some());

        remove(&mut db, db_id)?;
        assert!(list(&db)?.is_empty());
        Ok(())
    }

    #[test]
    fn update_leaves_a_removed_subscription_and_what_took_its_id_alone() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let mut record = repository("https://github.com/lyra/plugins");
        record.db_id = Some(create(&mut db, &record, &[])?);
        remove(&mut db, record.db_id.unwrap())?;
        let mut other = repository("https://github.com/lyra/other");
        other.db_id = Some(create(&mut db, &other, &[entry("b", "t1")])?);

        assert!(!update(&mut db, &record, &[entry("a", "t2")])?);
        // A removed subscription whose id went to another one is no update of
        // it either.
        let stale = PluginRepository {
            db_id: other.db_id,
            ..record.clone()
        };
        assert!(!update(&mut db, &stale, &[entry("a", "t2")])?);

        let listed = list(&db)?;
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].origin, other.origin);
        assert_eq!(
            entries(&db, other.db_id.unwrap())?
                .into_iter()
                .map(|entry| entry.plugin_id)
                .collect::<Vec<_>>(),
            ["b"]
        );
        Ok(())
    }

    #[test]
    fn update_clears_a_dropped_commit() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let mut record = repository("https://github.com/lyra/plugins");
        record.db_id = Some(create(&mut db, &record, &[])?);
        record.commit = Some("a".repeat(40));
        update(&mut db, &record, &[])?;

        // A later refresh where the forge commit API call failed.
        record.commit = None;
        update(&mut db, &record, &[])?;

        let fetched = get_by_id(&db, &record.id)?.expect("repository exists");
        assert_eq!(
            fetched.commit, None,
            "an unresolved commit must not leave a stale SHA driving update checks"
        );
        Ok(())
    }

    #[test]
    fn create_rejects_duplicate_origins() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        create(&mut db, &repository("https://github.com/lyra/plugins"), &[])?;
        let err = create(&mut db, &repository("https://github.com/lyra/plugins"), &[]).unwrap_err();

        assert!(err.to_string().contains("already added"), "{err}");
        Ok(())
    }

    fn entry(plugin_id: &str, version: &str) -> PluginRepositoryEntry {
        PluginRepositoryEntry {
            db_id: None,
            plugin_id: plugin_id.to_string(),
            subpath: Some(format!("plugins/{plugin_id}")),
            source_origin: None,
            version: Some(version.to_string()),
            name: None,
            description: None,
            commit: None,
            scopes: None,
            scope_reasons: None,
            dependencies: None,
            position: None,
            requires: None,
            error: None,
        }
    }

    fn sorted_entries(db: &DbAny, repo_db_id: DbId) -> anyhow::Result<Vec<(String, String)>> {
        let mut listed: Vec<_> = entries(db, repo_db_id)?
            .into_iter()
            .map(|entry| (entry.plugin_id, entry.version.unwrap_or_default()))
            .collect();
        listed.sort();
        Ok(listed)
    }

    #[test]
    fn entries_are_stored_replaced_and_removed_with_their_repository() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let mut first = repository("https://github.com/lyra/plugins");
        first.db_id = Some(create(
            &mut db,
            &first,
            &[entry("a", "t1"), entry("b", "t2")],
        )?);
        let mut second = repository("https://github.com/lyra/other");
        second.db_id = Some(create(&mut db, &second, &[entry("c", "t3")])?);

        assert_eq!(
            sorted_entries(&db, first.db_id.unwrap())?,
            [("a".into(), "t1".into()), ("b".into(), "t2".into())]
        );

        update(&mut db, &first, &[entry("a", "t4")])?;
        assert_eq!(
            sorted_entries(&db, first.db_id.unwrap())?,
            [("a".into(), "t4".into())]
        );
        assert_eq!(
            sorted_entries(&db, second.db_id.unwrap())?,
            [("c".into(), "t3".into())]
        );

        let entry_count = |db: &DbAny| -> anyhow::Result<usize> {
            Ok(db
                .exec(
                    QueryBuilder::select()
                        .elements::<PluginRepositoryEntry>()
                        .search()
                        .elements()
                        .query(),
                )?
                .elements
                .len())
        };
        assert_eq!(entry_count(&db)?, 2);
        remove(&mut db, first.db_id.unwrap())?;
        assert_eq!(entry_count(&db)?, 1);
        Ok(())
    }

    #[test]
    fn entries_keep_their_release_dependencies_and_listing() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let dependencies = vec![
            PluginDependency {
                plugin_id: "shared".into(),
                version: "^0.2".into(),
                optional: false,
            },
            PluginDependency {
                plugin_id: "fanart".into(),
                version: ">=0.1, <0.3".into(),
                optional: true,
            },
        ];
        let stored = PluginRepositoryEntry {
            dependencies: Some(dependencies),
            source_origin: Some("https://github.com/lyra/a".into()),
            name: Some("A".into()),
            description: Some("Does a".into()),
            error: Some("no releases".into()),
            ..entry("a", "t1")
        };
        let db_id = create(
            &mut db,
            &repository("https://github.com/lyra/plugins"),
            std::slice::from_ref(&stored),
        )?;

        let [loaded] = &entries(&db, db_id)?[..] else {
            panic!("expected one entry");
        };
        assert_eq!(
            PluginRepositoryEntry {
                db_id: None,
                ..loaded.clone()
            },
            PluginRepositoryEntry {
                position: Some(0),
                ..stored
            }
        );
        Ok(())
    }

    #[test]
    fn entries_stored_before_scope_reasons_read_as_having_none() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let stored = PluginRepositoryEntry {
            scope_reasons: Some(vec![PluginScopeReason {
                scope: "harmony.http".into(),
                reason: "Fetches things.".into(),
            }]),
            ..entry("a", "t1")
        };
        let db_id = create(
            &mut db,
            &repository("https://github.com/lyra/plugins"),
            std::slice::from_ref(&stored),
        )?;
        let [read] = &entries(&db, db_id)?[..] else {
            panic!("expected one entry");
        };
        assert_eq!(read.scope_reasons, stored.scope_reasons);
        let entry_id = read.db_id.expect("stored entry has an id");

        db.exec_mut(
            QueryBuilder::remove()
                .values(["scope_reasons"])
                .ids(entry_id)
                .query(),
        )?;
        let keys = db.exec(QueryBuilder::select().keys().ids(entry_id).query())?;
        assert!(
            !keys.elements[0]
                .values
                .iter()
                .any(|kv| kv.key == "scope_reasons".into()),
            "the old shape has no such key"
        );

        let [read] = &entries(&db, db_id)?[..] else {
            panic!("expected one entry");
        };
        assert_eq!(read.scope_reasons, None);
        Ok(())
    }

    #[test]
    fn entries_keep_the_order_they_were_stored_in() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let ids = |db: &DbAny, repo_db_id| -> anyhow::Result<Vec<String>> {
            Ok(entries(db, repo_db_id)?
                .into_iter()
                .map(|entry| entry.plugin_id)
                .collect())
        };
        let mut record = repository("https://github.com/lyra/plugins");
        let listed = ["zeta", "alpha", "mid", "beta"].map(|id| entry(id, "1"));
        let db_id = create(&mut db, &record, &listed)?;
        record.db_id = Some(db_id);
        assert_eq!(ids(&db, db_id)?, ["zeta", "alpha", "mid", "beta"]);

        update(
            &mut db,
            &record,
            &["mid", "omega", "zeta"].map(|id| entry(id, "2")),
        )?;
        assert_eq!(ids(&db, db_id)?, ["mid", "omega", "zeta"]);
        Ok(())
    }
}
