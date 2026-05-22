// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::time::Duration;

use agdb::DbId;
use harmony_core::{
    FunctionSpec,
    ModuleExport,
    ModuleSpec,
};
use harmony_luau as luau;
use harmony_luau::{
    DescribeInterface,
    FieldDescriptor,
    InterfaceDescriptor,
    LuauType,
    LuauTypeInfo,
    ModuleDescriptor,
    ModuleFunctionDescriptor,
    ParameterDescriptor,
    render_definition_file_with_support,
};

use crate::plugins::db::{
    self,
    DbAsync,
};

const DECODE_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Default)]
pub(crate) struct ChromaprintModuleStore {
    db: Option<DbAsync>,
}

impl ChromaprintModuleStore {
    pub(crate) fn empty() -> Self {
        Self { db: None }
    }

    pub(crate) fn with_db(db: DbAsync) -> Self {
        Self { db: Some(db) }
    }

    fn db(&self) -> luau::runtime::Result<DbAsync> {
        self.db.clone().ok_or_else(|| {
            crate::plugins::runtime_error(
                "lyra/chromaprint requires a database-backed plugin executor",
            )
        })
    }
}

struct ChromaprintModule;

struct ChromaprintResult;

pub(crate) fn module_spec() -> ModuleSpec {
    ModuleSpec::new("lyra/chromaprint")
        .capability("lyra.chromaprint")
        .function(compute_spec())
        .install(|_| Ok(ModuleExport::new(ChromaprintModule)))
}

fn compute_spec() -> FunctionSpec {
    FunctionSpec::async_fn("compute")
        .arg_name("entry_id")
        .args::<i64>()
        .returns::<luau::Table>()
        .call_async(std::sync::Arc::new(compute_callback))
}

fn compute_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let entry_id: i64 = frame.args.read_named("entry_id")?;
    if entry_id <= 0 {
        return Err(crate::plugins::runtime_error("entry not found"));
    }
    let store = frame
        .vm
        .data()
        .get::<ChromaprintModuleStore>()?
        .as_ref()
        .clone();
    let db = store.db()?;

    Ok(luau::ScheduledFuture::new(async move {
        let path = {
            let db = db.read().await;
            let entry = db::entries::get_by_id(&db, DbId(entry_id))
                .map_err(crate::plugins::runtime_error)?
                .ok_or_else(|| crate::plugins::runtime_error("entry not found"))?;
            entry.full_path
        };
        let (fingerprint, duration) =
            lyra_chromaprint::compute_fingerprint_from_file(&path, None, Some(DECODE_TIMEOUT))
                .map_err(crate::plugins::runtime_error)?;
        let mut table = luau::OwnedTable::with_capacity(0, 2);
        table.set_field("fingerprint", luau::Value::String(fingerprint.into_bytes()));
        table.set_field("duration", luau::Value::Number(f64::from(duration)));
        Ok(luau::Value::TableData(table))
    }))
}

impl LuauTypeInfo for ChromaprintResult {
    fn luau_type() -> LuauType {
        LuauType::named("ChromaprintResult")
    }
}

impl DescribeInterface for ChromaprintResult {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("ChromaprintResult", None);
        descriptor.fields.extend([
            FieldDescriptor {
                name: "fingerprint",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "duration",
                ty: f64::luau_type(),
                description: None,
            },
        ]);
        descriptor
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

fn module_descriptor() -> ModuleDescriptor {
    ModuleDescriptor {
        name: "Chromaprint",
        local_name: "chromaprint",
        description: None,
        fields: Vec::new(),
        functions: vec![ModuleFunctionDescriptor {
            path: vec!["compute"],
            description: None,
            params: vec![param("entry_id", i64::luau_type())],
            returns: vec![ChromaprintResult::luau_type()],
            yields: true,
        }],
    }
}

pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    render_definition_file_with_support(
        &module_descriptor(),
        &[],
        &[ChromaprintResult::interface_descriptor()],
        &[],
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_chromaprint_module_definition() {
        let rendered = render_luau_definition().expect("render lyra/chromaprint docs");

        assert!(rendered.contains("@interface ChromaprintResult"));
        assert!(rendered.contains("fingerprint: string"));
        assert!(rendered.contains("duration: number"));
        assert!(rendered.contains("@class Chromaprint"));
        assert!(rendered.contains("@yields"));
        assert!(
            rendered.contains("function chromaprint.compute(entry_id: number): ChromaprintResult")
        );
    }
}
