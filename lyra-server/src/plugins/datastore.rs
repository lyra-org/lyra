// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::time::{
    SystemTime,
    UNIX_EPOCH,
};

use harmony_core::UserDataBinding;
use harmony_luau as luau;
#[cfg(feature = "docgen")]
use harmony_luau::{
    DescribeTypeAlias,
    DescribeUserData,
    FieldDescriptor,
    InterfaceDescriptor,
};
use harmony_luau::{
    JsonValue,
    LuauType,
    LuauTypeInfo,
};

use crate::plugins::args::{
    self,
    JsonArg,
};
use crate::plugins::db::Db;
use lyra_db::DataStore;
use lyra_db::DbAsync;
use lyra_db::datastore::EntryWrite;

pub(crate) struct DataStoreModule;

#[harmony_macros::userdata(name = "DataStore", description = "A named persistent JSON store.")]
#[derive(Clone)]
struct DataStoreHandle {
    db: DbAsync,
    datastore_id: agdb::DbId,
}

#[harmony_macros::userdata_methods]
impl DataStoreHandle {
    #[harmony(
        description = "Gets a JSON value from this store by key, or `nil` when the key is missing or its entry expired.",
        returns(Option<JsonValue>)
    )]
    async fn get(&self, key: String) -> luau::runtime::Result<luau::Value> {
        read_value(&self.db, self.datastore_id, key).await
    }

    #[harmony(
        description = "Sets a JSON value in this store by key, replacing any earlier value and its expiry. Without `ttl_ms` the entry never expires."
    )]
    async fn set(
        &self,
        key: String,
        value: JsonArg,
        options: Option<SetOptions>,
    ) -> luau::runtime::Result<()> {
        let ttl_ms = options.and_then(|options| options.ttl_ms);
        write_values(&self.db, self.datastore_id, vec![(key, value.0, ttl_ms)]).await
    }

    #[harmony(
        description = "Removes an entry from this store by key. Returns whether a value was removed; an expired entry counts as absent."
    )]
    async fn remove(&self, key: String) -> luau::runtime::Result<bool> {
        remove_value(&self.db, self.datastore_id, key).await
    }

    #[harmony(
        description = "Gets multiple JSON values from this store under one read lock, in key order, with `nil` for missing or expired entries.",
        args(keys: Vec<String>),
        returns(Vec<Option<JsonValue>>)
    )]
    fn get_many(&self, keys: luau::Table) -> luau::runtime::Result<luau::ScheduledFuture> {
        let keys = read_string_array(&keys)?;
        let handle = self.clone();
        Ok(luau::ScheduledFuture::new(async move {
            read_values(&handle.db, handle.datastore_id, keys).await
        }))
    }

    #[harmony(
        description = "Writes multiple records to this store, keyed by key, in one transaction, each as `set` would.",
        args(entries: std::collections::BTreeMap<String, DataStoreRecord>)
    )]
    fn set_many(&self, entries: luau::Table) -> luau::runtime::Result<luau::ScheduledFuture> {
        let entries = read_records(&entries)?;
        let handle = self.clone();
        Ok(luau::ScheduledFuture::new(async move {
            write_values(&handle.db, handle.datastore_id, entries).await
        }))
    }

    #[harmony(
        description = "Removes every entry from this store. Returns the number removed, leaving out expired entries."
    )]
    async fn clear(&self) -> luau::runtime::Result<i64> {
        clear_values(&self.db, self.datastore_id)
            .await
            .map(|removed| removed as i64)
    }
}

#[harmony_macros::module(
    id = "lyra/datastore",
    name = "DataStore",
    docs_feature = "docgen",
    userdata(DataStoreHandle)
)]
impl DataStoreModule {
    #[harmony(
        description = "Returns a named data store, creating it if needed.",
        returns(DataStoreHandle)
    )]
    async fn get_or_create(
        name: String,
        #[context] db: Db,
        #[context] vm: luau::Vm,
        #[context] origin: luau::ChunkOrigin,
    ) -> luau::runtime::Result<luau::UserData> {
        let db = db.clone();
        let datastore_id = get_or_create(&db, name).await?;
        let userdata =
            DataStoreHandle::class().create(&vm, &origin, DataStoreHandle { db, datastore_id })?;
        Ok(userdata)
    }
}

/// The `options` argument of `set`.
struct SetOptions {
    ttl_ms: Option<u64>,
}

impl<'vm> luau::FromLuau<'vm> for SetOptions {
    fn read(reader: &mut luau::ArgReader<'vm>) -> luau::runtime::Result<Self> {
        let options: luau::Table = reader.read()?;
        args::reject_unknown_fields(&options, "options", &["ttl_ms"])?;
        Ok(Self {
            ttl_ms: read_ttl_ms(&options, "options")?,
        })
    }
}

impl LuauTypeInfo for SetOptions {
    fn luau_type() -> LuauType {
        LuauType::named("DataStoreSetOptions")
    }
}

/// A record of `set_many`.
struct DataStoreRecord;

impl LuauTypeInfo for DataStoreRecord {
    fn luau_type() -> LuauType {
        LuauType::named("DataStoreRecord")
    }
}

/// `table.ttl_ms` as whole milliseconds, rounded up; `owner` names the table in errors.
fn read_ttl_ms(table: &luau::Table, owner: &str) -> luau::runtime::Result<Option<u64>> {
    let ttl_ms = match table.get("ttl_ms")? {
        luau::Value::Nil => return Ok(None),
        luau::Value::Integer(value) => value as f64,
        luau::Value::Number(value) => value,
        other => {
            return Err(crate::plugins::runtime_error(format!(
                "{owner}.ttl_ms must be a number, got {}",
                args::describe(&other)
            )));
        }
    };
    if !ttl_ms.is_finite() || ttl_ms <= 0.0 {
        return Err(crate::plugins::runtime_error(format!(
            "{owner}.ttl_ms must be a positive finite number, got {ttl_ms}"
        )));
    }
    Ok(Some(ttl_ms.ceil() as u64))
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or(0)
}

async fn get_or_create(db: &DbAsync, name: String) -> luau::runtime::Result<agdb::DbId> {
    {
        let db = db.read().await;
        if let Some(existing) = lyra_db::datastore::find_by_name(&db, &name)? {
            return datastore_db_id(existing);
        }
    }

    // Writes the plugin's datastore and its entries only.
    let mut db = db.write_outside_catalog().await;
    let datastore = lyra_db::datastore::get_or_create(&mut db, name)?;
    datastore_db_id(datastore)
}

fn decode(stored_value: Option<String>) -> luau::runtime::Result<luau::Value> {
    let Some(stored_value) = stored_value else {
        return Ok(luau::Value::Nil);
    };
    let json: serde_json::Value =
        serde_json::from_str(&stored_value).map_err(crate::plugins::runtime_error)?;
    harmony_serde::json_to_luau_owned(json, 0)
}

async fn read_value(
    db: &DbAsync,
    datastore_id: agdb::DbId,
    key: String,
) -> luau::runtime::Result<luau::Value> {
    let stored_value = {
        let db = db.read().await;
        lyra_db::datastore::get_entry(&db, datastore_id, &key, now_ms())?.map(|entry| entry.value)
    };
    decode(stored_value)
}

async fn read_values(
    db: &DbAsync,
    datastore_id: agdb::DbId,
    keys: Vec<String>,
) -> luau::runtime::Result<luau::OwnedTable> {
    let stored_values = {
        let db = db.read().await;
        let now_ms = now_ms();
        let mut out = Vec::with_capacity(keys.len());
        for key in &keys {
            out.push(
                lyra_db::datastore::get_entry(&db, datastore_id, key, now_ms)?
                    .map(|entry| entry.value),
            );
        }
        out
    };

    let mut table = luau::OwnedTable::with_capacity(stored_values.len(), 0);
    for stored_value in stored_values {
        table.push_array(decode(stored_value)?);
    }
    Ok(table)
}

async fn write_values(
    db: &DbAsync,
    datastore_id: agdb::DbId,
    entries: Vec<(String, serde_json::Value, Option<u64>)>,
) -> luau::runtime::Result<()> {
    let mut serialized = Vec::with_capacity(entries.len());
    for (key, value, ttl_ms) in entries {
        let value = serde_json::to_string(&value).map_err(crate::plugins::runtime_error)?;
        serialized.push((key, value, ttl_ms));
    }
    // Writes the plugin's datastore and its entries only.
    let mut db = db.write_outside_catalog().await;
    // Expiry counts from when the write lands, not from when it began waiting.
    let now_ms = now_ms();
    let writes = serialized
        .into_iter()
        .map(|(key, value, ttl_ms)| EntryWrite {
            key,
            value,
            expires_at_ms: ttl_ms.map(|ttl_ms| now_ms.saturating_add(ttl_ms)),
        })
        .collect();
    lyra_db::datastore::upsert_many(&mut db, datastore_id, writes)
        .map_err(crate::plugins::runtime_error)
}

async fn remove_value(
    db: &DbAsync,
    datastore_id: agdb::DbId,
    key: String,
) -> luau::runtime::Result<bool> {
    // Writes the plugin's datastore and its entries only.
    let mut db = db.write_outside_catalog().await;
    lyra_db::datastore::remove_entry(&mut db, datastore_id, &key, now_ms())
        .map_err(crate::plugins::runtime_error)
}

async fn clear_values(db: &DbAsync, datastore_id: agdb::DbId) -> luau::runtime::Result<u64> {
    // Writes the plugin's datastore and its entries only.
    let mut db = db.write_outside_catalog().await;
    lyra_db::datastore::clear_entries(&mut db, datastore_id, now_ms())
        .map(|removed| removed as u64)
        .map_err(crate::plugins::runtime_error)
}

fn datastore_db_id(datastore: DataStore) -> luau::runtime::Result<agdb::DbId> {
    datastore
        .db_id
        .map(Into::into)
        .ok_or_else(|| luau::Error::Runtime("datastore missing db_id".to_string()))
}

fn read_string_array(table: &luau::Table) -> luau::runtime::Result<Vec<String>> {
    let len = i32::try_from(table.len()?).map_err(|_| {
        luau::Error::Runtime("table array part exceeded supported integer indexes".to_string())
    })?;
    let mut values = Vec::with_capacity(len as usize);
    for index in 1..=len {
        let value = table.get_integer(index)?;
        let luau::Value::String(value) = value else {
            return Err(luau::Error::Runtime(
                "datastore key arrays must contain only strings".to_string(),
            ));
        };
        values.push(String::from_utf8(value).map_err(crate::plugins::runtime_error)?);
    }
    Ok(values)
}

fn read_records(
    table: &luau::Table,
) -> luau::runtime::Result<Vec<(String, serde_json::Value, Option<u64>)>> {
    let mut entries = Vec::new();
    for (key, record) in table.pairs()? {
        let luau::Value::String(key) = key else {
            return Err(luau::Error::Runtime(
                "datastore entry keys must be strings".to_string(),
            ));
        };
        let key = String::from_utf8(key).map_err(crate::plugins::runtime_error)?;
        let luau::Value::Table(record) = record else {
            return Err(crate::plugins::runtime_error(format!(
                "entries['{key}'] must be a record table, got {}",
                record.type_name()
            )));
        };
        let owner = format!("entries['{key}']");
        args::reject_unknown_fields(&record, &owner, &["value", "ttl_ms"])?;
        let value = harmony_serde::luau_to_json(&record.get("value")?)?;
        let ttl_ms = read_ttl_ms(&record, &owner)?;
        entries.push((key, value, ttl_ms));
    }
    Ok(entries)
}

#[cfg(feature = "docgen")]
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    use harmony_luau::DescribeModule;

    let ttl_ms = || FieldDescriptor {
        name: "ttl_ms",
        ty: Option::<f64>::luau_type(),
        description: Some(
            "Milliseconds until the entry expires and reads as absent; a positive number, rounded up. Default none: the entry never expires.",
        ),
    };
    let mut set_options = InterfaceDescriptor::new("DataStoreSetOptions", None);
    set_options.fields.push(ttl_ms());
    let mut record = InterfaceDescriptor::new("DataStoreRecord", None);
    record.fields.extend([
        FieldDescriptor {
            name: "value",
            ty: JsonValue::luau_type(),
            description: None,
        },
        ttl_ms(),
    ]);

    harmony_luau::render_definition_file_with_support(
        &DataStoreModule::module_descriptor(),
        &[JsonValue::type_alias_descriptor()],
        &[set_options, record],
        &[DataStoreHandle::class_descriptor()],
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::services::{
        STATE,
        catalog::index::snapshot,
        testing::{
            init_default_test_state,
            runtime_test_lock,
        },
    };

    #[tokio::test]
    async fn datastore_writes_keep_the_catalog_index() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init_default_test_state()?;
        lyra_db::fixtures::insert_release(&mut *STATE.db.write().await, "Kept")?;
        let before = snapshot(&*STATE.db.read().await)?;

        let db = STATE.db.get();
        let store = get_or_create(&db, "plugin".to_string()).await?;
        write_values(
            &db,
            store,
            vec![
                ("key".to_string(), serde_json::json!(1), None),
                ("other".to_string(), serde_json::json!(2), Some(1000)),
            ],
        )
        .await?;
        remove_value(&db, store, "key".to_string()).await?;
        clear_values(&db, store).await?;
        assert!(Arc::ptr_eq(&before, &snapshot(&*STATE.db.read().await)?));
        Ok(())
    }

    #[tokio::test]
    async fn a_write_waiting_for_the_lock_keeps_its_whole_ttl() -> anyhow::Result<()> {
        let db = DbAsync::new(lyra_db::test_db::new_test_db()?);
        let store = get_or_create(&db, "contended".to_string()).await?;

        let held = db.write().await;
        let writer = tokio::spawn({
            let db = db.clone();
            async move {
                write_values(
                    &db,
                    store,
                    vec![("k".to_string(), serde_json::json!(1), Some(300))],
                )
                .await
            }
        });
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
        drop(held);
        writer.await??;

        let value = read_value(&db, store, "k".to_string()).await?;
        assert!(matches!(value, luau::Value::Number(n) if n == 1.0));
        Ok(())
    }
}
