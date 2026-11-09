// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashSet;

use agdb::{
    DbAny,
    DbAnyTransactionMut,
    DbElement,
    DbId,
    QueryBuilder,
};

use crate::roots::MEMBERS;

#[derive(DbElement, Clone, Debug)]
pub struct SyncRunRecord {
    pub db_id: Option<DbId>,
    pub id: String,
    pub library_id: String,
    pub kind: String,
    pub status: String,
    pub started_at_ms: u64,
    pub updated_at_ms: u64,
    pub finished_at_ms: Option<u64>,
    pub error: Option<String>,
    pub cancellation_requested: bool,
    pub sequence: u64,
    pub progress_mode: String,
    pub total_state: String,
    pub completed_units: u64,
    pub failed_units: u64,
    pub skipped_units: u64,
    pub total_units: u64,
    pub current_stage: Option<String>,
    pub current_subject: Option<String>,
    pub active_units: u64,
    pub failure_count: u64,
}

/// One failed unit of a run's work, an outgoing neighbor of the run's node.
#[derive(DbElement, Clone, Debug)]
pub struct SyncRunFailureRecord {
    pub db_id: Option<DbId>,
    /// The run's failure count when this failure happened.
    pub ordinal: u64,
    pub stage: String,
    pub work_type: String,
    pub entity_type: Option<String>,
    pub entity_id: Option<String>,
    pub entity_title: Option<String>,
    pub provider_id: Option<String>,
    pub source_dir: Option<String>,
    pub message: String,
    pub failed_at_ms: u64,
}

pub fn create(db: &mut DbAny, record: &SyncRunRecord) -> anyhow::Result<DbId> {
    db.transaction_mut(|t| create_in_transaction(t, record))
}

pub(crate) fn create_in_transaction(
    db: &mut DbAnyTransactionMut<'_>,
    record: &SyncRunRecord,
) -> anyhow::Result<DbId> {
    let result = db.exec_mut(QueryBuilder::insert().element(record).query())?;
    let run_db_id = result
        .ids()
        .first()
        .copied()
        .ok_or_else(|| anyhow::anyhow!("sync run insert returned no id"))?;
    db.exec_mut(
        QueryBuilder::insert()
            .edges()
            .from("sync_runs")
            .to(run_db_id)
            .query(),
    )?;
    Ok(run_db_id)
}

/// Atomically aligns the stored row to `record`.
pub fn update(db: &mut DbAny, record: &SyncRunRecord) -> anyhow::Result<()> {
    db.transaction_mut(|t| update_in_transaction(t, record))
}

pub(crate) fn update_in_transaction(
    db: &mut DbAnyTransactionMut<'_>,
    record: &SyncRunRecord,
) -> anyhow::Result<()> {
    let Some(record_db_id) = record.db_id else {
        anyhow::bail!("cannot update sync run without db_id");
    };
    super::replace_element_in_transaction(
        db,
        record_db_id,
        [
            ("finished_at_ms", record.finished_at_ms.is_none()),
            ("error", record.error.is_none()),
            ("current_stage", record.current_stage.is_none()),
            ("current_subject", record.current_subject.is_none()),
        ],
        record,
    )
}

/// Stores `record` unless a newer one of its run is, and `failure` under the run either way, in
/// one transaction. Writes nothing once the run's library is gone.
pub fn persist(
    db: &mut DbAny,
    record: &SyncRunRecord,
    failure: Option<&SyncRunFailureRecord>,
) -> anyhow::Result<()> {
    db.transaction_mut(|t| persist_in_transaction(t, record, failure))
}

fn persist_in_transaction(
    db: &mut DbAnyTransactionMut<'_>,
    record: &SyncRunRecord,
    failure: Option<&SyncRunFailureRecord>,
) -> anyhow::Result<()> {
    if super::lookup::find_node_id_by_id(db, &record.library_id)?.is_none() {
        return Ok(());
    }
    let run_db_id = match get_by_id(db, &record.id)? {
        Some(stored) => {
            let run_db_id = stored
                .db_id
                .ok_or_else(|| anyhow::anyhow!("stored sync run has no db_id"))?;
            // A write that lost the race to the lock leaves the newer record in place.
            if stored.sequence < record.sequence {
                update_in_transaction(
                    db,
                    &SyncRunRecord {
                        db_id: Some(run_db_id),
                        ..record.clone()
                    },
                )?;
            }
            run_db_id
        }
        None => create_in_transaction(db, record)?,
    };
    if let Some(failure) = failure {
        insert_failure_in_transaction(db, run_db_id, failure)?;
    }
    Ok(())
}

fn insert_failure_in_transaction(
    db: &mut DbAnyTransactionMut<'_>,
    run_db_id: DbId,
    failure: &SyncRunFailureRecord,
) -> anyhow::Result<()> {
    let result = db.exec_mut(QueryBuilder::insert().element(failure).query())?;
    let failure_db_id = result
        .ids()
        .first()
        .copied()
        .ok_or_else(|| anyhow::anyhow!("sync run failure insert returned no id"))?;
    db.exec_mut(
        QueryBuilder::insert()
            .edges()
            .from(run_db_id)
            .to(failure_db_id)
            .query(),
    )?;
    Ok(())
}

/// The stored failures of the run `run_id` names, oldest first.
pub fn failures_for_run(
    db: &impl super::DbAccess,
    run_id: &str,
) -> anyhow::Result<Vec<SyncRunFailureRecord>> {
    let Some(run_db_id) = get_by_id(db, run_id)?.and_then(|run| run.db_id) else {
        return Ok(Vec::new());
    };
    let mut failures: Vec<SyncRunFailureRecord> = db
        .exec(
            QueryBuilder::select()
                .elements::<SyncRunFailureRecord>()
                .search()
                .from(run_db_id)
                .where_()
                .neighbor()
                .query(),
        )?
        .try_into()?;
    failures.sort_by_key(|failure| failure.ordinal);
    Ok(failures)
}

pub(crate) fn failure_ids(db: &impl super::DbAccess, run_db_id: DbId) -> anyhow::Result<Vec<DbId>> {
    Ok(db
        .exec(
            QueryBuilder::search()
                .from(run_db_id)
                .where_()
                .neighbor()
                .query(),
        )?
        .ids())
}

/// Removes the library's runs past its newest `keep`, except the latest finished run of each
/// kind, along with their failures.
pub fn prune_for_library(db: &mut DbAny, library_id: &str, keep: usize) -> anyhow::Result<()> {
    db.transaction_mut(|t| {
        let mut finished_kinds = HashSet::new();
        let mut removed = Vec::new();
        for (index, run) in list_for_library(t, library_id)?.iter().enumerate() {
            let latest_finished = !is_active(run) && finished_kinds.insert(run.kind.clone());
            if index < keep || latest_finished {
                continue;
            }
            if let Some(run_db_id) = run.db_id {
                removed.extend(failure_ids(t, run_db_id)?);
                removed.push(run_db_id);
            }
        }
        if !removed.is_empty() {
            t.exec_mut(QueryBuilder::remove().ids(removed).query())?;
        }
        Ok(())
    })
}

pub fn list(db: &impl super::DbAccess) -> anyhow::Result<Vec<SyncRunRecord>> {
    let runs: Vec<SyncRunRecord> = db
        .exec(
            QueryBuilder::select()
                .elements::<SyncRunRecord>()
                .search()
                .from("sync_runs")
                .where_()
                .distance(MEMBERS)
                .query(),
        )?
        .try_into()?;
    Ok(runs)
}

pub fn get_by_id(db: &impl super::DbAccess, id: &str) -> anyhow::Result<Option<SyncRunRecord>> {
    let runs: Vec<SyncRunRecord> = db
        .exec(
            QueryBuilder::select()
                .elements::<SyncRunRecord>()
                .search()
                .from("sync_runs")
                .where_()
                .distance(MEMBERS)
                .and()
                .key("id")
                .value(id)
                .end_where()
                .query(),
        )?
        .try_into()?;
    Ok(runs.into_iter().next())
}

/// The library's runs, latest first: by sequence, which grows across a library's runs, then by
/// start, for runs recorded before it did.
pub fn list_for_library(
    db: &impl super::DbAccess,
    library_id: &str,
) -> anyhow::Result<Vec<SyncRunRecord>> {
    let mut runs: Vec<SyncRunRecord> = db
        .exec(
            QueryBuilder::select()
                .elements::<SyncRunRecord>()
                .search()
                .from("sync_runs")
                .where_()
                .distance(MEMBERS)
                .and()
                .key("library_id")
                .value(library_id)
                .end_where()
                .query(),
        )?
        .try_into()?;
    runs.sort_by(|a, b| {
        b.sequence
            .cmp(&a.sequence)
            .then(b.started_at_ms.cmp(&a.started_at_ms))
            .then(b.updated_at_ms.cmp(&a.updated_at_ms))
            .then(a.id.cmp(&b.id))
    });
    Ok(runs)
}

pub fn latest_for_library(
    db: &impl super::DbAccess,
    library_id: &str,
) -> anyhow::Result<Option<SyncRunRecord>> {
    Ok(list_for_library(db, library_id)?.into_iter().next())
}

pub fn active_for_library(
    db: &impl super::DbAccess,
    library_id: &str,
) -> anyhow::Result<Option<SyncRunRecord>> {
    Ok(list_for_library(db, library_id)?
        .into_iter()
        .find(is_active))
}

fn is_active(run: &SyncRunRecord) -> bool {
    matches!(
        run.status.as_str(),
        "queued" | "planning" | "running" | "cancelling"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_db::{
        insert_library,
        new_test_db,
    };

    /// A finished run of `library_id`, numbered by `sequence`.
    fn finished_run(library_id: &str, id: &str, kind: &str, sequence: u64) -> SyncRunRecord {
        SyncRunRecord {
            db_id: None,
            id: id.to_string(),
            library_id: library_id.to_string(),
            kind: kind.to_string(),
            status: "succeeded".to_string(),
            started_at_ms: sequence,
            updated_at_ms: sequence,
            finished_at_ms: Some(sequence),
            error: None,
            cancellation_requested: false,
            sequence,
            progress_mode: "determinate".to_string(),
            total_state: "final".to_string(),
            completed_units: 0,
            failed_units: 0,
            skipped_units: 0,
            total_units: 0,
            current_stage: None,
            current_subject: None,
            active_units: 0,
            failure_count: 0,
        }
    }

    fn failure(ordinal: u64) -> SyncRunFailureRecord {
        SyncRunFailureRecord {
            db_id: None,
            ordinal,
            stage: "metadata_parse".to_string(),
            work_type: "metadata_parse_group".to_string(),
            entity_type: None,
            entity_id: None,
            entity_title: None,
            provider_id: None,
            source_dir: Some(format!("/music/{ordinal}")),
            message: format!("failure {ordinal}"),
            failed_at_ms: ordinal,
        }
    }

    fn library_public_id(db: &DbAny, library: DbId) -> anyhow::Result<String> {
        Ok(crate::libraries::get_by_id(db, library)?
            .expect("library exists")
            .id)
    }

    fn ordinals(db: &DbAny, run_id: &str) -> anyhow::Result<Vec<u64>> {
        Ok(failures_for_run(db, run_id)?
            .into_iter()
            .map(|failure| failure.ordinal)
            .collect())
    }

    /// Every failure node in the database, reachable from a run or not.
    fn failure_node_count(db: &DbAny) -> anyhow::Result<usize> {
        Ok(db
            .exec(
                QueryBuilder::search()
                    .elements()
                    .where_()
                    .key("db_element_id")
                    .value("SyncRunFailureRecord")
                    .query(),
            )?
            .elements
            .len())
    }

    #[test]
    fn failures_read_back_in_ordinal_order_whatever_order_they_arrived() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let library = insert_library(&mut db, "Music", "/runs")?;
        let library_id = library_public_id(&db, library)?;
        let run = finished_run(&library_id, "run", "library_sync", 1);

        for (sequence, ordinal) in [(2, 3), (3, 1), (4, 2)] {
            persist(
                &mut db,
                &SyncRunRecord {
                    sequence,
                    ..run.clone()
                },
                Some(&failure(ordinal)),
            )?;
        }

        let stored = failures_for_run(&db, "run")?;
        assert_eq!(
            stored
                .iter()
                .map(|failure| failure.ordinal)
                .collect::<Vec<_>>(),
            [1, 2, 3]
        );
        assert_eq!(stored[0].source_dir.as_deref(), Some("/music/1"));
        assert_eq!(stored[0].message, "failure 1");
        assert_eq!(get_by_id(&db, "run")?.expect("run").sequence, 4);
        assert!(failures_for_run(&db, "missing")?.is_empty());
        Ok(())
    }

    #[test]
    fn an_out_of_date_write_keeps_the_record_and_stores_the_failure() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let library = insert_library(&mut db, "Music", "/runs")?;
        let library_id = library_public_id(&db, library)?;
        let newest = SyncRunRecord {
            status: "running".to_string(),
            ..finished_run(&library_id, "run", "library_sync", 9)
        };
        persist(&mut db, &newest, None)?;

        let late = SyncRunRecord {
            status: "planning".to_string(),
            sequence: 5,
            ..newest.clone()
        };
        persist(&mut db, &late, Some(&failure(1)))?;

        let stored = get_by_id(&db, "run")?.expect("run");
        assert_eq!((stored.status.as_str(), stored.sequence), ("running", 9));
        assert_eq!(ordinals(&db, "run")?, [1]);
        Ok(())
    }

    #[test]
    fn nothing_is_written_once_the_library_is_gone() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let run = finished_run("gone", "run", "library_sync", 1);

        persist(&mut db, &run, Some(&failure(1)))?;

        assert!(get_by_id(&db, "run")?.is_none());
        assert_eq!(failure_node_count(&db)?, 0);
        Ok(())
    }

    #[test]
    fn a_failed_persist_leaves_neither_the_run_write_nor_its_failure() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let library = insert_library(&mut db, "Music", "/runs")?;
        let library_id = library_public_id(&db, library)?;
        let run = finished_run(&library_id, "run", "library_sync", 1);
        persist(&mut db, &run, None)?;

        let result = db.transaction_mut(|t| -> anyhow::Result<()> {
            let newer = SyncRunRecord {
                status: "failed".to_string(),
                sequence: 2,
                ..run.clone()
            };
            persist_in_transaction(t, &newer, Some(&failure(1)))?;
            anyhow::bail!("the transaction fails after both writes")
        });

        assert!(result.is_err());
        let stored = get_by_id(&db, "run")?.expect("run");
        assert_eq!((stored.status.as_str(), stored.sequence), ("succeeded", 1));
        assert_eq!(failure_node_count(&db)?, 0);
        Ok(())
    }

    #[test]
    fn root_searches_stop_at_the_run_nodes() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let library = insert_library(&mut db, "Music", "/runs")?;
        let library_id = library_public_id(&db, library)?;
        let run = finished_run(&library_id, "run", "library_sync", 1);
        persist(&mut db, &run, Some(&failure(1)))?;
        persist(&mut db, &run, Some(&failure(2)))?;

        let at_members = |db: &DbAny, key: &str| -> anyhow::Result<usize> {
            Ok(db
                .exec(
                    QueryBuilder::search()
                        .from("sync_runs")
                        .where_()
                        .distance(MEMBERS)
                        .and()
                        .keys(key)
                        .query(),
                )?
                .elements
                .len())
        };
        assert_eq!(at_members(&db, "library_id")?, 1);
        assert_eq!(
            at_members(&db, "ordinal")?,
            0,
            "a search at the root's members never reaches a failure"
        );
        assert_eq!(list(&db)?.len(), 1);
        assert_eq!(list_for_library(&db, &library_id)?.len(), 1);
        assert_eq!(
            db.exec(
                QueryBuilder::search()
                    .from("sync_runs")
                    .where_()
                    .distance(agdb::CountComparison::Equal(4))
                    .query(),
            )?
            .elements
            .len(),
            2,
            "the failures are one hop past the members"
        );
        Ok(())
    }

    #[test]
    fn pruning_keeps_the_newest_and_each_kinds_latest_finished_run() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let shown = insert_library(&mut db, "Music", "/runs")?;
        let shown_id = library_public_id(&db, shown)?;
        let other = insert_library(&mut db, "Other", "/other")?;
        let other_id = library_public_id(&db, other)?;

        for sequence in 1..=2 {
            let kind = if sequence == 1 {
                "library_purge"
            } else {
                "library_refresh"
            };
            persist(
                &mut db,
                &finished_run(&shown_id, &format!("old-{kind}"), kind, sequence),
                Some(&failure(sequence)),
            )?;
        }
        for sequence in 3..=8 {
            let id = format!("run-{sequence}");
            persist(
                &mut db,
                &finished_run(&shown_id, &id, "library_sync", sequence),
                Some(&failure(1)),
            )?;
        }
        persist(
            &mut db,
            &finished_run(&other_id, "untouched", "library_sync", 1),
            Some(&failure(1)),
        )?;

        prune_for_library(&mut db, &shown_id, 3)?;

        let kept: Vec<String> = list_for_library(&db, &shown_id)?
            .into_iter()
            .map(|run| run.id)
            .collect();
        assert_eq!(
            kept,
            [
                "run-8",
                "run-7",
                "run-6",
                "old-library_refresh",
                "old-library_purge"
            ]
        );
        assert!(failures_for_run(&db, "run-3")?.is_empty());
        assert_eq!(ordinals(&db, "old-library_purge")?, [1]);
        assert_eq!(ordinals(&db, "run-8")?, [1]);
        assert_eq!(list_for_library(&db, &other_id)?.len(), 1);
        assert_eq!(ordinals(&db, "untouched")?, [1]);
        assert_eq!(failure_node_count(&db)?, 6, "pruned runs took theirs along");
        Ok(())
    }

    #[test]
    fn deleting_a_library_removes_its_runs_and_failures() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let library = insert_library(&mut db, "Music", "/runs")?;
        let library_id = library_public_id(&db, library)?;
        persist(
            &mut db,
            &finished_run(&library_id, "run", "library_sync", 1),
            Some(&failure(1)),
        )?;

        let library = crate::libraries::get_by_id(&db, library)?.expect("library exists");
        db.transaction_mut(|t| crate::libraries::delete(t, &library))?;

        assert!(list(&db)?.is_empty());
        assert_eq!(failure_node_count(&db)?, 0);
        Ok(())
    }

    #[test]
    fn create_update_and_lookup_sync_run() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let mut record = SyncRunRecord {
            db_id: None,
            id: "run-a".to_string(),
            library_id: "lib-a".to_string(),
            kind: "library_sync".to_string(),
            status: "running".to_string(),
            started_at_ms: 10,
            updated_at_ms: 10,
            finished_at_ms: None,
            error: None,
            cancellation_requested: false,
            sequence: 0,
            progress_mode: "determinate".to_string(),
            total_state: "final".to_string(),
            completed_units: 1,
            failed_units: 0,
            skipped_units: 0,
            total_units: 4,
            current_stage: Some("provider_refresh".to_string()),
            current_subject: Some("provider-a".to_string()),
            active_units: 1,
            failure_count: 0,
        };

        let db_id = create(&mut db, &record)?;
        record.db_id = Some(db_id);
        record.status = "succeeded".to_string();
        record.updated_at_ms = 20;
        record.finished_at_ms = Some(20);
        update(&mut db, &record)?;

        let fetched = get_by_id(&db, "run-a")?.expect("run should exist");
        assert_eq!(fetched.db_id, Some(db_id));
        assert_eq!(fetched.status, "succeeded");
        assert_eq!(fetched.completed_units, 1);
        assert_eq!(fetched.current_stage.as_deref(), Some("provider_refresh"));
        assert_eq!(
            latest_for_library(&db, "lib-a")?.map(|run| run.id),
            Some("run-a".to_string())
        );
        assert!(active_for_library(&db, "lib-a")?.is_none());
        Ok(())
    }

    #[test]
    fn a_librarys_latest_run_is_its_highest_sequence() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let run = |id: &str, status: &str, started_at_ms: u64, sequence: u64| SyncRunRecord {
            db_id: None,
            id: id.to_string(),
            library_id: "lib".to_string(),
            kind: "library_sync".to_string(),
            status: status.to_string(),
            started_at_ms,
            updated_at_ms: started_at_ms,
            finished_at_ms: None,
            error: None,
            cancellation_requested: false,
            sequence,
            progress_mode: "indeterminate".to_string(),
            total_state: "discovering".to_string(),
            completed_units: 0,
            failed_units: 0,
            skipped_units: 0,
            total_units: 0,
            current_stage: None,
            current_subject: None,
            active_units: 0,
            failure_count: 0,
        };
        for record in [
            run("tied-earlier", "succeeded", 50, 3),
            run("tied-later", "succeeded", 60, 3),
            run("before-step", "succeeded", 200, 7),
            run("after-step", "running", 100, 8),
        ] {
            create(&mut db, &record)?;
        }

        let order: Vec<String> = list_for_library(&db, "lib")?
            .into_iter()
            .map(|run| run.id)
            .collect();
        assert_eq!(
            order,
            ["after-step", "before-step", "tied-later", "tied-earlier"],
            "a clock step back doesn't reorder runs; equal sequences fall back to the start"
        );
        assert_eq!(
            latest_for_library(&db, "lib")?.map(|run| run.id).as_deref(),
            Some("after-step")
        );
        assert_eq!(
            active_for_library(&db, "lib")?.map(|run| run.id).as_deref(),
            Some("after-step")
        );
        Ok(())
    }

    /// Guards the hand-maintained clear list in `update` against struct drift.
    #[test]
    fn update_clears_every_optional_key() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let populated = SyncRunRecord {
            db_id: None,
            id: "drift-guard".to_string(),
            library_id: "lib".to_string(),
            kind: "library_sync".to_string(),
            status: "running".to_string(),
            started_at_ms: 1,
            updated_at_ms: 2,
            finished_at_ms: Some(3),
            error: Some("e".to_string()),
            cancellation_requested: false,
            sequence: 0,
            progress_mode: "determinate".to_string(),
            total_state: "final".to_string(),
            completed_units: 0,
            failed_units: 0,
            skipped_units: 0,
            total_units: 0,
            current_stage: Some("provider_refresh".to_string()),
            current_subject: Some("s".to_string()),
            active_units: 0,
            failure_count: 0,
        };
        let record_db_id = create(&mut db, &populated)?;
        update(
            &mut db,
            &SyncRunRecord {
                db_id: Some(record_db_id),
                ..populated.clone()
            },
        )?;

        update(
            &mut db,
            &SyncRunRecord {
                db_id: Some(record_db_id),
                finished_at_ms: None,
                error: None,
                current_stage: None,
                current_subject: None,
                ..populated
            },
        )?;

        let keys = crate::test_db::stored_keys(&db, record_db_id)?;
        assert_eq!(
            keys,
            [
                "db_element_id",
                "id",
                "library_id",
                "kind",
                "status",
                "started_at_ms",
                "updated_at_ms",
                "cancellation_requested",
                "sequence",
                "progress_mode",
                "total_state",
                "completed_units",
                "failed_units",
                "skipped_units",
                "total_units",
                "active_units",
                "failure_count",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
            "only non-Option keys may remain after an all-None update"
        );
        Ok(())
    }

    #[test]
    fn update_clears_optional_fields_set_to_none() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let mut record = SyncRunRecord {
            db_id: None,
            id: "run-b".to_string(),
            library_id: "lib-b".to_string(),
            kind: "library_sync".to_string(),
            status: "running".to_string(),
            started_at_ms: 10,
            updated_at_ms: 10,
            finished_at_ms: None,
            error: None,
            cancellation_requested: false,
            sequence: 0,
            progress_mode: "determinate".to_string(),
            total_state: "final".to_string(),
            completed_units: 1,
            failed_units: 0,
            skipped_units: 0,
            total_units: 4,
            current_stage: Some("provider_refresh".to_string()),
            current_subject: Some("provider-a".to_string()),
            active_units: 1,
            failure_count: 0,
        };

        record.db_id = Some(create(&mut db, &record)?);
        record.status = "succeeded".to_string();
        record.finished_at_ms = Some(20);
        record.current_stage = None;
        record.current_subject = None;
        record.active_units = 0;
        update(&mut db, &record)?;

        let fetched = get_by_id(&db, "run-b")?.expect("run should exist");
        assert_eq!(fetched.status, "succeeded");
        assert_eq!(fetched.current_stage, None);
        assert_eq!(fetched.current_subject, None);
        assert_eq!(fetched.finished_at_ms, Some(20));
        Ok(())
    }
}
