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

#[derive(DbElement, Clone, Debug)]
pub(crate) struct SyncRunRecord {
    pub(crate) db_id: Option<DbId>,
    pub(crate) id: String,
    pub(crate) library_id: String,
    pub(crate) kind: String,
    pub(crate) status: String,
    pub(crate) started_at_ms: u64,
    pub(crate) updated_at_ms: u64,
    pub(crate) finished_at_ms: Option<u64>,
    pub(crate) error: Option<String>,
    pub(crate) cancellation_requested: bool,
    pub(crate) sequence: u64,
    pub(crate) progress_mode: String,
    pub(crate) total_state: String,
    pub(crate) completed_units: u64,
    pub(crate) failed_units: u64,
    pub(crate) skipped_units: u64,
    pub(crate) total_units: u64,
    pub(crate) current_stage: Option<String>,
    pub(crate) current_subject: Option<String>,
    pub(crate) active_units: u64,
    pub(crate) failure_count: u64,
}

pub(crate) fn create(
    db: &mut impl super::DbAccess,
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
pub(crate) fn update(db: &mut DbAny, record: &SyncRunRecord) -> anyhow::Result<()> {
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

pub(crate) fn list(db: &impl super::DbAccess) -> anyhow::Result<Vec<SyncRunRecord>> {
    let runs: Vec<SyncRunRecord> = db
        .exec(
            QueryBuilder::select()
                .elements::<SyncRunRecord>()
                .search()
                .from("sync_runs")
                .query(),
        )?
        .try_into()?;
    Ok(runs)
}

pub(crate) fn get_by_id(
    db: &impl super::DbAccess,
    id: &str,
) -> anyhow::Result<Option<SyncRunRecord>> {
    let runs: Vec<SyncRunRecord> = db
        .exec(
            QueryBuilder::select()
                .elements::<SyncRunRecord>()
                .search()
                .from("sync_runs")
                .where_()
                .key("id")
                .value(id)
                .end_where()
                .query(),
        )?
        .try_into()?;
    Ok(runs.into_iter().next())
}

pub(crate) fn list_for_library(
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
                .key("library_id")
                .value(library_id)
                .end_where()
                .query(),
        )?
        .try_into()?;
    runs.sort_by(|a, b| {
        b.started_at_ms
            .cmp(&a.started_at_ms)
            .then(b.updated_at_ms.cmp(&a.updated_at_ms))
            .then(a.id.cmp(&b.id))
    });
    Ok(runs)
}

pub(crate) fn latest_for_library(
    db: &impl super::DbAccess,
    library_id: &str,
) -> anyhow::Result<Option<SyncRunRecord>> {
    Ok(list_for_library(db, library_id)?.into_iter().next())
}

pub(crate) fn active_for_library(
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
    use crate::db::test_db::new_test_db;

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

        let keys = crate::db::test_db::stored_keys(&db, record_db_id)?;
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
