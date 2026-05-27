// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::{
    DbElement,
    DbId,
    DbValue,
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

pub(crate) fn update(db: &mut impl super::DbAccess, record: &SyncRunRecord) -> anyhow::Result<()> {
    if record.db_id.is_none() {
        anyhow::bail!("cannot update sync run without db_id");
    }
    db.exec_mut(QueryBuilder::insert().element(record).query())?;
    Ok(())
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

pub(crate) fn delete_records_missing_summary_fields(
    db: &mut impl super::DbAccess,
) -> anyhow::Result<usize> {
    let type_key = DbValue::from("db_element_id");
    let type_value = DbValue::from("SyncRunRecord");
    let started_at_key = DbValue::from("started_at_ms");
    let result = db.exec(QueryBuilder::select().search().from("sync_runs").query())?;
    let legacy_ids = result
        .elements
        .into_iter()
        .filter(|element| element.id.0 > 0)
        .filter(|element| {
            element
                .values
                .iter()
                .any(|kv| kv.key == type_key && kv.value == type_value)
        })
        .filter(|element| !element.values.iter().any(|kv| kv.key == started_at_key))
        .map(|element| element.id)
        .collect::<Vec<_>>();
    if legacy_ids.is_empty() {
        return Ok(0);
    }
    let removed = legacy_ids.len();
    db.exec_mut(QueryBuilder::remove().ids(legacy_ids).query())?;
    Ok(removed)
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
}
