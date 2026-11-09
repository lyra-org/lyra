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

/// Stores `record` unless a newer one of its run is, in one transaction. Writes nothing once the
/// run's library is gone.
pub fn persist(db: &mut DbAny, record: &SyncRunRecord) -> anyhow::Result<()> {
    db.transaction_mut(|t| persist_in_transaction(t, record))
}

fn persist_in_transaction(
    db: &mut DbAnyTransactionMut<'_>,
    record: &SyncRunRecord,
) -> anyhow::Result<()> {
    if super::lookup::find_node_id_by_id(db, &record.library_id)?.is_none() {
        return Ok(());
    }
    match get_by_id(db, &record.id)? {
        // A write that lost the race to the lock leaves the newer record in place.
        Some(stored) if stored.sequence >= record.sequence => {}
        Some(stored) => update_in_transaction(
            db,
            &SyncRunRecord {
                db_id: stored.db_id,
                ..record.clone()
            },
        )?,
        None => {
            create_in_transaction(db, record)?;
        }
    }
    Ok(())
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
    Ok(list_for_library(db, library_id)?.into_iter().find(|run| {
        matches!(
            run.status.as_str(),
            "queued" | "planning" | "running" | "cancelling"
        )
    }))
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

    fn library_public_id(db: &DbAny, library: DbId) -> anyhow::Result<String> {
        Ok(crate::libraries::get_by_id(db, library)?
            .expect("library exists")
            .id)
    }

    #[test]
    fn an_out_of_date_write_keeps_the_newer_record() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let library = insert_library(&mut db, "Music", "/runs")?;
        let library_id = library_public_id(&db, library)?;
        let newest = SyncRunRecord {
            status: "running".to_string(),
            ..finished_run(&library_id, "run", "library_sync", 9)
        };
        persist(&mut db, &newest)?;

        let late = SyncRunRecord {
            status: "planning".to_string(),
            sequence: 5,
            ..newest.clone()
        };
        persist(&mut db, &late)?;

        let stored = get_by_id(&db, "run")?.expect("run");
        assert_eq!((stored.status.as_str(), stored.sequence), ("running", 9));
        Ok(())
    }

    #[test]
    fn nothing_is_written_once_the_library_is_gone() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let run = finished_run("gone", "run", "library_sync", 1);

        persist(&mut db, &run)?;

        assert!(get_by_id(&db, "run")?.is_none());
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
