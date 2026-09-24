// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use harmony_core::{
    FunctionSpec,
    ModuleExport,
    ModuleSpec,
};
use harmony_luau as luau;
use harmony_luau::JsonValue;
#[cfg(feature = "docgen")]
use harmony_luau::{
    DescribeTypeAlias,
    DescribeUserData,
    LuauType,
    LuauTypeInfo,
    ModuleDescriptor,
    ModuleFunctionDescriptor,
    ParameterDescriptor,
    render_definition_file_with_support,
};

use crate::plugins::db::DbAsync;
use crate::plugins::db::{
    self,
    DataStore,
};
struct PluginCaller;

#[cfg(feature = "docgen")]
fn param(name: &'static str, ty: LuauType) -> ParameterDescriptor {
    ParameterDescriptor {
        name,
        ty,
        description: None,
        variadic: false,
    }
}

struct DataStoreModule;

#[harmony_macros::userdata(name = "DataStore", description = "A named persistent JSON store.")]
#[derive(Clone)]
struct DataStoreHandle {
    store: DataStoreModuleStore,
    datastore_id: agdb::DbId,
}

#[harmony_macros::userdata_methods]
impl DataStoreHandle {
    #[harmony(
        description = "Gets a JSON value from this store by key.",
        args(key: String),
        returns(Option<JsonValue>)
    )]
    async fn get(&self, key: String) -> luau::runtime::Result<luau::Value> {
        self.store.get(self.datastore_id, key).await
    }

    #[harmony(
        description = "Sets a JSON value in this store by key.",
        args(key: String, value: JsonValue)
    )]
    fn set(
        &self,
        vm: &luau::Vm,
        key: String,
        value: luau::Value,
    ) -> luau::runtime::Result<luau::ScheduledFuture> {
        let json = harmony_serde::luau_to_json(vm, &value, 0)?;
        let handle = self.clone();
        Ok(luau::ScheduledFuture::new(async move {
            handle.store.set(handle.datastore_id, key, json).await
        }))
    }

    #[harmony(
        description = "Removes an entry from this store by key. Returns whether a value was removed."
    )]
    async fn remove(&self, key: String) -> luau::runtime::Result<bool> {
        self.store.remove(self.datastore_id, key).await
    }

    #[harmony(
        description = "Gets multiple JSON values from this store under one read lock.",
        args(keys: Vec<String>),
        returns(Vec<Option<JsonValue>>)
    )]
    fn get_many(
        &self,
        vm: &luau::Vm,
        keys: luau::Table,
    ) -> luau::runtime::Result<luau::ScheduledFuture> {
        let keys = read_string_array(vm, &keys)?;
        let handle = self.clone();
        Ok(luau::ScheduledFuture::new(async move {
            handle.store.get_many(handle.datastore_id, keys).await
        }))
    }

    #[harmony(
        description = "Writes multiple JSON values to this store under one write lock.",
        args(entries: std::collections::BTreeMap<String, JsonValue>)
    )]
    fn set_many(
        &self,
        vm: &luau::Vm,
        entries: luau::Table,
    ) -> luau::runtime::Result<luau::ScheduledFuture> {
        let entries = read_json_entries(vm, &entries)?;
        let handle = self.clone();
        Ok(luau::ScheduledFuture::new(async move {
            handle.store.set_many(handle.datastore_id, entries).await
        }))
    }

    #[harmony(description = "Removes every entry from this store. Returns the number removed.")]
    async fn clear(&self) -> luau::runtime::Result<i64> {
        self.store
            .clear(self.datastore_id)
            .await
            .map(|removed| removed as i64)
    }
}

pub(crate) fn module_spec() -> ModuleSpec {
    ModuleSpec::new("lyra/datastore")
        .capability("lyra.datastore")
        .function(get_or_create_spec())
        .userdata(DataStoreHandle::_harmony_userdata_spec())
        .install(|_| Ok(ModuleExport::new(DataStoreModule)))
}

fn get_or_create_spec() -> FunctionSpec {
    let spec = FunctionSpec::async_fn("get_or_create")
        .context::<PluginCaller>()
        .named_arg::<String>("name")
        .returns::<DataStoreHandle>();
    spec.call_async(std::sync::Arc::new(get_or_create_callback))
}

fn get_or_create_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let name: String = frame.args.read_named("name")?;
    let store = frame
        .vm
        .data()
        .get::<DataStoreModuleStore>()?
        .as_ref()
        .clone();
    let vm = frame.vm.clone();
    let origin = frame.context.origin.clone();
    Ok(luau::ScheduledFuture::new(async move {
        let datastore_id = store.get_or_create(name).await?;
        let userdata = DataStoreHandle::_harmony_userdata_class().create(
            &vm,
            &origin,
            DataStoreHandle {
                store,
                datastore_id,
            },
        )?;
        Ok(userdata)
    }))
}

#[derive(Clone, Default)]
pub(crate) struct DataStoreModuleStore {
    db: Option<DbAsync>,
}
impl DataStoreModuleStore {
    #[cfg(test)]
    pub(crate) fn empty() -> Self {
        Self { db: None }
    }

    pub(crate) fn with_db(db: DbAsync) -> Self {
        Self { db: Some(db) }
    }

    async fn get_or_create(&self, name: String) -> luau::runtime::Result<agdb::DbId> {
        let db = self.db()?;
        {
            let db = db.read().await;
            if let Some(existing) =
                db::datastore::find_by_name(&db, &name).map_err(crate::plugins::runtime_error)?
            {
                return datastore_db_id(existing);
            }
        }

        let mut db = db.write().await;
        let datastore =
            db::datastore::get_or_create(&mut db, name).map_err(crate::plugins::runtime_error)?;
        datastore_db_id(datastore)
    }

    async fn get(
        &self,
        datastore_id: agdb::DbId,
        key: String,
    ) -> luau::runtime::Result<luau::Value> {
        let stored_value = {
            let db = self.db()?;
            let db = db.read().await;
            db::datastore::get_entry(&db, datastore_id, &key)
                .map_err(crate::plugins::runtime_error)?
                .map(|entry| entry.value)
        };
        let Some(stored_value) = stored_value else {
            return Ok(luau::Value::Nil);
        };
        let json: serde_json::Value =
            serde_json::from_str(&stored_value).map_err(crate::plugins::runtime_error)?;
        harmony_serde::json_to_luau_owned(json, 0)
    }

    async fn set(
        &self,
        datastore_id: agdb::DbId,
        key: String,
        json: serde_json::Value,
    ) -> luau::runtime::Result<()> {
        let json = serde_json::to_string(&json).map_err(crate::plugins::runtime_error)?;
        let db = self.db()?;
        let mut db = db.write().await;
        db::datastore::upsert_entry(&mut db, datastore_id, key, json)
            .map(|_| ())
            .map_err(crate::plugins::runtime_error)
    }

    async fn remove(&self, datastore_id: agdb::DbId, key: String) -> luau::runtime::Result<bool> {
        let db = self.db()?;
        let mut db = db.write().await;
        db::datastore::remove_entry(&mut db, datastore_id, &key)
            .map_err(crate::plugins::runtime_error)
    }

    async fn get_many(
        &self,
        datastore_id: agdb::DbId,
        keys: Vec<String>,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let stored_values = {
            let db = self.db()?;
            let db = db.read().await;
            let mut out = Vec::with_capacity(keys.len());
            for key in &keys {
                out.push(
                    db::datastore::get_entry(&db, datastore_id, key)
                        .map_err(crate::plugins::runtime_error)?
                        .map(|entry| entry.value),
                );
            }
            out
        };

        let mut table = luau::OwnedTable::with_capacity(stored_values.len(), 0);
        for stored_value in stored_values {
            let value = match stored_value {
                Some(stored_value) => {
                    let json: serde_json::Value = serde_json::from_str(&stored_value)
                        .map_err(crate::plugins::runtime_error)?;
                    harmony_serde::json_to_luau_owned(json, 0)?
                }
                None => luau::Value::Nil,
            };
            table.push_array(value);
        }
        Ok(table)
    }

    async fn set_many(
        &self,
        datastore_id: agdb::DbId,
        entries: Vec<(String, serde_json::Value)>,
    ) -> luau::runtime::Result<()> {
        let mut prepared = Vec::with_capacity(entries.len());
        for (key, value) in entries {
            prepared.push((
                key,
                serde_json::to_string(&value).map_err(crate::plugins::runtime_error)?,
            ));
        }

        let db = self.db()?;
        let mut db = db.write().await;
        for (key, value) in prepared {
            db::datastore::upsert_entry(&mut db, datastore_id, key, value)
                .map_err(crate::plugins::runtime_error)?;
        }
        Ok(())
    }

    async fn clear(&self, datastore_id: agdb::DbId) -> luau::runtime::Result<u64> {
        let db = self.db()?;
        let mut db = db.write().await;
        db::datastore::clear_entries(&mut db, datastore_id)
            .map(|removed| removed as u64)
            .map_err(crate::plugins::runtime_error)
    }

    fn db(&self) -> luau::runtime::Result<DbAsync> {
        self.db.clone().ok_or_else(|| {
            luau::Error::Runtime("lyra/datastore database is unavailable".to_string())
        })
    }
}
fn datastore_db_id(datastore: DataStore) -> luau::runtime::Result<agdb::DbId> {
    datastore
        .db_id
        .map(Into::into)
        .ok_or_else(|| luau::Error::Runtime("datastore missing db_id".to_string()))
}

fn read_string_array(vm: &luau::Vm, table: &luau::Table) -> luau::runtime::Result<Vec<String>> {
    let entries = table.reader(vm)?.array_values_raw()?;
    let mut values = Vec::with_capacity(entries.len());
    for value in entries {
        let luau::Value::String(value) = value else {
            return Err(luau::Error::Runtime(
                "datastore key arrays must contain only strings".to_string(),
            ));
        };
        values.push(String::from_utf8(value).map_err(crate::plugins::runtime_error)?);
    }
    Ok(values)
}

fn read_json_entries(
    vm: &luau::Vm,
    table: &luau::Table,
) -> luau::runtime::Result<Vec<(String, serde_json::Value)>> {
    let mut entries = Vec::new();
    for (key, value) in table.pairs_raw(vm)? {
        let luau::Value::String(key) = key else {
            return Err(luau::Error::Runtime(
                "datastore entry keys must be strings".to_string(),
            ));
        };
        let key = String::from_utf8(key).map_err(crate::plugins::runtime_error)?;
        let value = harmony_serde::luau_to_json(vm, &value, 0)?;
        entries.push((key, value));
    }
    Ok(entries)
}
#[cfg(feature = "docgen")]
fn module_descriptor() -> ModuleDescriptor {
    ModuleDescriptor {
        name: "DataStore",
        local_name: "datastore",
        description: None,
        fields: Vec::new(),
        functions: vec![ModuleFunctionDescriptor {
            path: vec!["get_or_create"],
            description: Some("Returns a named data store, creating it if needed."),
            params: vec![param("name", String::luau_type())],
            returns: vec![DataStoreHandle::luau_type()],
            yields: true,
        }],
    }
}

#[cfg(feature = "docgen")]
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    render_definition_file_with_support(
        &module_descriptor(),
        &[JsonValue::type_alias_descriptor()],
        &[],
        &[DataStoreHandle::class_descriptor()],
    )
}
