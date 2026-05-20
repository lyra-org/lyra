// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::sync::Arc;

use harmony_core::{
    FunctionSpec,
    ModuleExport,
    ModuleSpec,
    UserDataSpec,
};
use harmony_luau as luau;
use harmony_luau::{
    ClassDescriptor,
    DescribeTypeAlias,
    DescribeUserData,
    JsonValue,
    LuauType,
    LuauTypeInfo,
    MethodDescriptor,
    MethodKind,
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

impl LuauTypeInfo for DataStore {
    fn luau_type() -> LuauType {
        LuauType::literal("DataStore")
    }
}

fn param(name: &'static str, ty: LuauType) -> ParameterDescriptor {
    ParameterDescriptor {
        name,
        ty,
        description: None,
        variadic: false,
    }
}

impl DescribeUserData for DataStore {
    fn class_descriptor() -> ClassDescriptor {
        let mut descriptor = ClassDescriptor::new("DataStore", None);
        descriptor.methods.extend([
            MethodDescriptor {
                name: "get",
                description: Some("Gets a JSON value from this store by key."),
                params: vec![param("key", String::luau_type())],
                returns: vec![Option::<JsonValue>::luau_type()],
                yields: true,
                kind: MethodKind::Instance,
            },
            MethodDescriptor {
                name: "set",
                description: Some("Sets a JSON value in this store by key."),
                params: vec![
                    param("key", String::luau_type()),
                    param("value", JsonValue::luau_type()),
                ],
                returns: vec![],
                yields: true,
                kind: MethodKind::Instance,
            },
            MethodDescriptor {
                name: "remove",
                description: Some(
                    "Removes an entry from this store by key. Returns whether a value was removed.",
                ),
                params: vec![param("key", String::luau_type())],
                returns: vec![bool::luau_type()],
                yields: true,
                kind: MethodKind::Instance,
            },
            MethodDescriptor {
                name: "get_many",
                description: Some("Gets multiple JSON values from this store under one read lock."),
                params: vec![param("keys", Vec::<String>::luau_type())],
                returns: vec![Vec::<Option<JsonValue>>::luau_type()],
                yields: true,
                kind: MethodKind::Instance,
            },
            MethodDescriptor {
                name: "set_many",
                description: Some(
                    "Writes multiple JSON values to this store under one write lock.",
                ),
                params: vec![param(
                    "entries",
                    LuauType::map(String::luau_type(), JsonValue::luau_type()),
                )],
                returns: vec![],
                yields: true,
                kind: MethodKind::Instance,
            },
            MethodDescriptor {
                name: "clear",
                description: Some(
                    "Removes every entry from this store. Returns the number removed.",
                ),
                params: vec![],
                returns: vec![u64::luau_type()],
                yields: true,
                kind: MethodKind::Instance,
            },
        ]);
        descriptor
    }
}

struct DataStoreModule;

pub(crate) fn module_spec() -> ModuleSpec {
    ModuleSpec::new("lyra/datastore")
        .capability("lyra.datastore")
        .function(get_or_create_spec())
        .userdata(
            UserDataSpec::new("DataStore")
                .method(
                    FunctionSpec::async_fn("get")
                        .context::<PluginCaller>()
                        .args::<String>()
                        .returns::<Option<JsonValue>>(),
                )
                .method(
                    FunctionSpec::async_fn("set")
                        .context::<PluginCaller>()
                        .args::<String>()
                        .args::<JsonValue>(),
                )
                .method(
                    FunctionSpec::async_fn("remove")
                        .context::<PluginCaller>()
                        .args::<String>()
                        .returns::<bool>(),
                )
                .method(
                    FunctionSpec::async_fn("get_many")
                        .context::<PluginCaller>()
                        .args::<Vec<String>>()
                        .returns::<Vec<Option<JsonValue>>>(),
                )
                .method(
                    FunctionSpec::async_fn("set_many")
                        .context::<PluginCaller>()
                        .args::<std::collections::BTreeMap<String, JsonValue>>(),
                )
                .method(
                    FunctionSpec::async_fn("clear")
                        .context::<PluginCaller>()
                        .returns::<u64>(),
                ),
        )
        .install(|_| Ok(ModuleExport::new(DataStoreModule)))
}

fn get_or_create_spec() -> FunctionSpec {
    let spec = FunctionSpec::async_fn("get_or_create")
        .context::<PluginCaller>()
        .named_arg::<String>("name")
        .returns::<DataStore>();
    spec.call(get_or_create_callback)
}

fn get_or_create_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let name: String = frame.args.read_named("name")?;
    let store = frame
        .vm
        .data()
        .get::<DataStoreModuleStore>()?
        .as_ref()
        .clone();
    let datastore_id = store.get_or_create(name)?;
    let table = datastore_table(frame.context.origin.clone(), store, datastore_id);
    frame.returns.write(table)?;
    Ok(())
}
#[derive(Clone, Default)]
pub(crate) struct DataStoreModuleStore {
    db: Option<DbAsync>,
}
impl DataStoreModuleStore {
    pub(crate) fn empty() -> Self {
        Self { db: None }
    }

    pub(crate) fn with_db(db: DbAsync) -> Self {
        Self { db: Some(db) }
    }

    fn get_or_create(&self, name: String) -> luau::runtime::Result<agdb::DbId> {
        let db = self.db()?;
        {
            let db = futures::executor::block_on(db.read());
            if let Some(existing) =
                db::datastore::find_by_name(&db, &name).map_err(crate::plugins::runtime_error)?
            {
                return datastore_db_id(existing);
            }
        }

        let mut db = futures::executor::block_on(db.write());
        let datastore =
            db::datastore::get_or_create(&mut db, name).map_err(crate::plugins::runtime_error)?;
        datastore_db_id(datastore)
    }

    fn get(
        &self,
        vm: &luau::Vm,
        datastore_id: agdb::DbId,
        key: String,
    ) -> luau::runtime::Result<luau::Value> {
        let stored_value = {
            let db = self.db()?;
            let db = futures::executor::block_on(db.read());
            db::datastore::get_entry(&db, datastore_id, &key)
                .map_err(crate::plugins::runtime_error)?
                .map(|entry| entry.value)
        };
        let Some(stored_value) = stored_value else {
            return Ok(luau::Value::Nil);
        };
        let json: serde_json::Value =
            serde_json::from_str(&stored_value).map_err(crate::plugins::runtime_error)?;
        harmony_json::json_to_luau(vm, json, 0)
    }

    fn set(
        &self,
        datastore_id: agdb::DbId,
        key: String,
        json: serde_json::Value,
    ) -> luau::runtime::Result<()> {
        let json = serde_json::to_string(&json).map_err(crate::plugins::runtime_error)?;
        let db = self.db()?;
        let mut db = futures::executor::block_on(db.write());
        db::datastore::upsert_entry(&mut db, datastore_id, key, json)
            .map(|_| ())
            .map_err(crate::plugins::runtime_error)
    }

    fn remove(&self, datastore_id: agdb::DbId, key: String) -> luau::runtime::Result<bool> {
        let db = self.db()?;
        let mut db = futures::executor::block_on(db.write());
        db::datastore::remove_entry(&mut db, datastore_id, &key)
            .map_err(crate::plugins::runtime_error)
    }

    fn get_many(
        &self,
        vm: &luau::Vm,
        datastore_id: agdb::DbId,
        keys: Vec<String>,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let stored_values = {
            let db = self.db()?;
            let db = futures::executor::block_on(db.read());
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
                    harmony_json::json_to_luau(vm, json, 0)?
                }
                None => luau::Value::Nil,
            };
            table.push_array(value);
        }
        Ok(table)
    }

    fn set_many(
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
        let mut db = futures::executor::block_on(db.write());
        for (key, value) in prepared {
            db::datastore::upsert_entry(&mut db, datastore_id, key, value)
                .map_err(crate::plugins::runtime_error)?;
        }
        Ok(())
    }

    fn clear(&self, datastore_id: agdb::DbId) -> luau::runtime::Result<u64> {
        let db = self.db()?;
        let mut db = futures::executor::block_on(db.write());
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
fn datastore_table(
    origin: luau::ChunkOrigin,
    store: DataStoreModuleStore,
    datastore_id: agdb::DbId,
) -> luau::OwnedTable {
    let mut table = luau::OwnedTable::with_capacity(0, 6);
    table.set_field(
        "get",
        datastore_method_callback(&origin, "get", ["self", "key"], {
            let store = store.clone();
            move |mut frame| {
                read_self(&mut frame.args)?;
                let key: String = frame.args.read_named("key")?;
                let value = store.get(frame.vm, datastore_id, key)?;
                frame.returns.write(value)
            }
        }),
    );
    table.set_field(
        "set",
        datastore_method_callback(&origin, "set", ["self", "key", "value"], {
            let store = store.clone();
            move |mut frame| {
                read_self(&mut frame.args)?;
                let key: String = frame.args.read_named("key")?;
                let value: luau::Value = frame.args.read_named("value")?;
                let json = harmony_json::luau_to_json(frame.vm, &value, 0)?;
                store.set(datastore_id, key, json)?;
                Ok(())
            }
        }),
    );
    table.set_field(
        "remove",
        datastore_method_callback(&origin, "remove", ["self", "key"], {
            let store = store.clone();
            move |mut frame| {
                read_self(&mut frame.args)?;
                let key: String = frame.args.read_named("key")?;
                frame.returns.write(store.remove(datastore_id, key)?)
            }
        }),
    );
    table.set_field(
        "get_many",
        datastore_method_callback(&origin, "get_many", ["self", "keys"], {
            let store = store.clone();
            move |mut frame| {
                read_self(&mut frame.args)?;
                let keys: luau::Table = frame.args.read_named("keys")?;
                let keys = read_string_array(frame.vm, &keys)?;
                let values = store.get_many(frame.vm, datastore_id, keys)?;
                frame.returns.write(values)
            }
        }),
    );
    table.set_field(
        "set_many",
        datastore_method_callback(&origin, "set_many", ["self", "entries"], {
            let store = store.clone();
            move |mut frame| {
                read_self(&mut frame.args)?;
                let entries: luau::Table = frame.args.read_named("entries")?;
                let entries = read_json_entries(frame.vm, &entries)?;
                store.set_many(datastore_id, entries)?;
                Ok(())
            }
        }),
    );
    table.set_field(
        "clear",
        datastore_method_callback(&origin, "clear", ["self"], move |mut frame| {
            read_self(&mut frame.args)?;
            frame.returns.write(store.clear(datastore_id)? as i64)
        }),
    );
    table
}
fn datastore_method_callback(
    origin: &luau::ChunkOrigin,
    name: &'static str,
    args: impl IntoIterator<Item = &'static str>,
    callback: impl for<'vm> Fn(luau::CallFrame<'vm>) -> luau::runtime::Result<()>
    + Send
    + Sync
    + 'static,
) -> luau::Value {
    let options = luau::NativeFunctionOptions::new(origin.clone())
        .function_name(format!("DataStore.{name}"))
        .argument_names(args.into_iter().map(Arc::<str>::from));
    luau::Value::NativeFunction(luau::NativeFunctionValue::new(options, Arc::new(callback)))
}
fn read_self(args: &mut luau::ArgReader<'_>) -> luau::runtime::Result<()> {
    let _: luau::Table = args.read_named("self")?;
    Ok(())
}
fn read_string_array(vm: &luau::Vm, table: &luau::Table) -> luau::runtime::Result<Vec<String>> {
    let mut entries = table
        .pairs_raw(vm)?
        .into_iter()
        .filter_map(|(key, value)| Some((sequence_index(key)?, value)))
        .collect::<Vec<_>>();
    entries.sort_by_key(|(index, _)| *index);

    let mut values = Vec::with_capacity(entries.len());
    for (_, value) in entries {
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
        let value = harmony_json::luau_to_json(vm, &value, 0)?;
        entries.push((key, value));
    }
    Ok(entries)
}
fn sequence_index(value: luau::Value) -> Option<i64> {
    match value {
        luau::Value::Integer(value) if value > 0 => Some(value),
        luau::Value::Number(value) if value.is_finite() && value.fract() == 0.0 && value > 0.0 => {
            Some(value as i64)
        }
        _ => None,
    }
}

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
            returns: vec![DataStore::luau_type()],
            yields: true,
        }],
    }
}

pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    render_definition_file_with_support(
        &module_descriptor(),
        &[JsonValue::type_alias_descriptor()],
        &[],
        &[DataStore::class_descriptor()],
    )
}

#[cfg(test)]
mod spec_tests {
    use super::*;

    #[test]
    fn exposes_handwritten_module_spec_with_userdata() {
        let spec = module_spec();

        assert_eq!(spec.id.0.as_ref(), "lyra/datastore");
        assert_eq!(
            spec.capability.as_ref().unwrap().0.as_ref(),
            "lyra.datastore"
        );
        assert_eq!(spec.functions.len(), 1);
        assert_eq!(spec.functions[0].name.as_ref(), "get_or_create");
        assert!(
            spec.functions[0]
                .context_type
                .is_some_and(|name| name.contains("PluginCaller"))
        );
        assert_eq!(spec.userdata.len(), 1);
        assert_eq!(spec.userdata[0].name.as_ref(), "DataStore");
        assert_eq!(spec.userdata[0].methods.len(), 6);
        assert!(spec.userdata[0].methods.iter().all(|method| method.yields));
    }

    #[test]
    fn renders_datastore_module_definition() {
        let rendered = render_luau_definition().expect("render lyra/datastore docs");

        assert!(rendered.contains("@class DataStore"));
        assert!(rendered.contains("@type JsonValue"));
        assert!(rendered.contains("function datastore.get_or_create(name: string): DataStore"));
        assert!(rendered.contains("get: (self: DataStore, key: string) -> JsonValue?"));
        assert!(rendered.contains("set: (self: DataStore, key: string, value: JsonValue)"));
        assert!(rendered.contains("clear: (self: DataStore) -> number"));
    }
}
