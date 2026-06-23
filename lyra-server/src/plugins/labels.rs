// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

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
};
#[cfg(feature = "docgen")]
use harmony_luau::{
    ModuleDescriptor,
    ModuleFunctionDescriptor,
    ParameterDescriptor,
    render_definition_file_with_support,
};

use crate::{
    STATE,
    plugins::db::{
        self,
        Permission,
        labels::{
            LabelExternalIdInput,
            LabelInput,
            ResolveExternalId,
            ResolveLabel,
        },
    },
    services::auth::Principal,
};

struct LabelsModule;
#[cfg(feature = "docgen")]
struct LabelAddRequest;
#[cfg(feature = "docgen")]
struct LabelResolveRequest;
#[cfg(feature = "docgen")]
struct LabelExternalId;
struct LabelInfo;
#[cfg(feature = "docgen")]
struct LabelForReleaseInfo;

pub(crate) fn module_spec() -> ModuleSpec {
    ModuleSpec::new("lyra/labels")
        .capability("lyra.labels")
        .function(add_spec())
        .function(resolve_spec())
        .function(sync_for_release_spec())
        .function(get_by_id_spec())
        .function(get_for_release_spec())
        .function(get_for_releases_many_spec())
        .function(get_releases_spec())
        .function(get_releases_many_spec())
        .install(|_| Ok(ModuleExport::new(LabelsModule)))
}

fn add_spec() -> FunctionSpec {
    FunctionSpec::sync_fn("add")
        .arg_name("release_id")
        .args::<i64>()
        .arg_name("request")
        .args::<luau::Table>()
        .returns::<i64>()
        .call(add_callback)
}

fn resolve_spec() -> FunctionSpec {
    FunctionSpec::sync_fn("resolve")
        .arg_name("request")
        .args::<luau::Table>()
        .returns::<i64>()
        .call(resolve_callback)
}

fn sync_for_release_spec() -> FunctionSpec {
    FunctionSpec::sync_fn("sync_for_release")
        .arg_name("release_id")
        .args::<i64>()
        .arg_name("requests")
        .args::<luau::Table>()
        .call(sync_for_release_callback)
}

fn get_by_id_spec() -> FunctionSpec {
    FunctionSpec::sync_fn("get_by_id")
        .arg_name("label_id")
        .args::<i64>()
        .returns::<Option<LabelInfo>>()
        .call(get_by_id_callback)
}

fn get_for_release_spec() -> FunctionSpec {
    FunctionSpec::sync_fn("get_for_release")
        .arg_name("release_id")
        .args::<i64>()
        .returns::<luau::Table>()
        .call(get_for_release_callback)
}

fn get_for_releases_many_spec() -> FunctionSpec {
    FunctionSpec::sync_fn("get_for_releases_many")
        .arg_name("release_ids")
        .args::<luau::Table>()
        .returns::<luau::Table>()
        .call(get_for_releases_many_callback)
}

fn get_releases_spec() -> FunctionSpec {
    FunctionSpec::sync_fn("get_releases")
        .arg_name("label_id")
        .args::<i64>()
        .returns::<luau::Table>()
        .call(get_releases_callback)
}

fn get_releases_many_spec() -> FunctionSpec {
    FunctionSpec::sync_fn("get_releases_many")
        .arg_name("label_ids")
        .args::<luau::Table>()
        .returns::<luau::Table>()
        .call(get_releases_many_callback)
}

fn add_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let release_id = DbId(require_positive_id(
        frame.args.read_named("release_id")?,
        "release_id",
    )?);
    let request: luau::Table = frame.args.read_named("request")?;
    let request = parse_label_add_request(frame.vm, &request)?;
    let principal = caller_principal(&frame.context);

    let label_id = futures::executor::block_on(async {
        let mut db = STATE.db.write().await;
        if !can_mutate_release(&db, principal.as_ref(), release_id)? {
            return Ok(DbId(0));
        }
        if db::releases::get_by_id(&db, release_id)
            .map_err(crate::plugins::runtime_error)?
            .is_some_and(|release| release.locked.unwrap_or(false))
        {
            return Ok(DbId(0));
        }
        db::labels::add_label_to_release(
            &mut db,
            release_id,
            &ResolveLabel {
                name: &request.name,
                external_id: request.external_id_ref(),
            },
            request.catalog_number.as_deref(),
        )
        .map_err(crate::plugins::runtime_error)
    })?;

    frame.returns.write(label_id.0)
}

fn resolve_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let request: luau::Table = frame.args.read_named("request")?;
    let request = parse_label_resolve_request(frame.vm, &request)?;
    let principal = caller_principal(&frame.context);
    if !can_mutate_global(principal.as_ref()) {
        frame.returns.write(0_i64)?;
        return Ok(());
    }

    let label_id = futures::executor::block_on(async {
        let mut db = STATE.db.write().await;
        db::labels::resolve(
            &mut db,
            &ResolveLabel {
                name: &request.name,
                external_id: request.external_id_ref(),
            },
        )
        .map_err(crate::plugins::runtime_error)
    })?;

    frame.returns.write(label_id.0)
}

fn sync_for_release_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let release_id = DbId(require_positive_id(
        frame.args.read_named("release_id")?,
        "release_id",
    )?);
    let requests: luau::Table = frame.args.read_named("requests")?;
    let requests = parse_label_add_requests(frame.vm, &requests)?;
    let principal = caller_principal(&frame.context);

    futures::executor::block_on(async {
        let mut db = STATE.db.write().await;
        if !can_mutate_release(&db, principal.as_ref(), release_id)? {
            return Ok(());
        }
        if db::releases::get_by_id(&db, release_id)
            .map_err(crate::plugins::runtime_error)?
            .is_some_and(|release| release.locked.unwrap_or(false))
        {
            return Err(crate::plugins::runtime_error(
                "cannot sync labels for a locked release",
            ));
        }
        let inputs = requests
            .iter()
            .map(LabelAddRequestData::to_label_input)
            .collect::<Vec<_>>();
        db::labels::sync_release_labels(&mut db, release_id, &inputs)
            .map_err(crate::plugins::runtime_error)
    })
}

fn get_by_id_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let label_id = DbId(require_positive_id(
        frame.args.read_named("label_id")?,
        "label_id",
    )?);
    let label = futures::executor::block_on(async {
        let db = STATE.db.read().await;
        db::labels::get_by_id(&db, label_id).map_err(crate::plugins::runtime_error)
    })?;
    frame.returns.write(
        label
            .map(|label| luau::Value::TableData(label_info_table(label)))
            .unwrap_or(luau::Value::Nil),
    )
}

fn get_for_release_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let release_id = DbId(require_positive_id(
        frame.args.read_named("release_id")?,
        "release_id",
    )?);
    let principal = caller_principal(&frame.context);
    let labels = futures::executor::block_on(async {
        let db = STATE.db.read().await;
        if !can_read_entity(&db, principal.as_ref(), release_id)? {
            return Ok(Vec::new());
        }
        db::labels::get_for_release(&db, release_id).map_err(crate::plugins::runtime_error)
    })?;
    frame
        .returns
        .write(luau::Value::TableData(label_release_array(labels)))
}

fn get_for_releases_many_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let ids_table: luau::Table = frame.args.read_named("release_ids")?;
    let ids = parse_db_ids(frame.vm, &ids_table)?;
    let principal = caller_principal(&frame.context);
    let labels = futures::executor::block_on(async {
        let db = STATE.db.read().await;
        let readable = ids
            .into_iter()
            .filter(|id| can_read_entity(&db, principal.as_ref(), *id).unwrap_or(false))
            .collect::<Vec<_>>();
        db::labels::get_for_releases_many(&db, &readable).map_err(crate::plugins::runtime_error)
    })?;
    let mut table = luau::OwnedTable::with_capacity(0, labels.len());
    for (release_id, labels) in labels {
        table.set_key(
            luau::Value::Integer(release_id.0),
            luau::Value::TableData(label_release_array(labels)),
        );
    }
    frame.returns.write(luau::Value::TableData(table))
}

fn get_releases_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let label_id = DbId(require_positive_id(
        frame.args.read_named("label_id")?,
        "label_id",
    )?);
    let release_ids = futures::executor::block_on(async {
        let db = STATE.db.read().await;
        db::labels::get_releases(&db, label_id).map_err(crate::plugins::runtime_error)
    })?;
    frame
        .returns
        .write(luau::Value::TableData(id_array(release_ids)))
}

fn get_releases_many_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let ids_table: luau::Table = frame.args.read_named("label_ids")?;
    let ids = parse_db_ids(frame.vm, &ids_table)?;
    let release_ids = futures::executor::block_on(async {
        let db = STATE.db.read().await;
        db::labels::get_releases_many(&db, &ids).map_err(crate::plugins::runtime_error)
    })?;
    let mut table = luau::OwnedTable::with_capacity(0, release_ids.len());
    for (label_id, ids) in release_ids {
        table.set_key(
            luau::Value::Integer(label_id.0),
            luau::Value::TableData(id_array(ids)),
        );
    }
    frame.returns.write(luau::Value::TableData(table))
}

#[derive(Clone)]
struct LabelExternalIdData {
    provider_id: String,
    id_type: String,
    id_value: String,
}

#[derive(Clone)]
struct LabelAddRequestData {
    name: String,
    catalog_number: Option<String>,
    external_id: Option<LabelExternalIdData>,
}

struct LabelResolveRequestData {
    name: String,
    external_id: Option<LabelExternalIdData>,
}

impl LabelAddRequestData {
    fn external_id_ref(&self) -> Option<ResolveExternalId<'_>> {
        self.external_id.as_ref().map(LabelExternalIdData::as_ref)
    }

    fn to_label_input(&self) -> LabelInput {
        LabelInput {
            name: self.name.clone(),
            catalog_number: self.catalog_number.clone(),
            external_id: self
                .external_id
                .as_ref()
                .map(|external_id| LabelExternalIdInput {
                    provider_id: external_id.provider_id.clone(),
                    id_type: external_id.id_type.clone(),
                    id_value: external_id.id_value.clone(),
                }),
        }
    }
}

impl LabelResolveRequestData {
    fn external_id_ref(&self) -> Option<ResolveExternalId<'_>> {
        self.external_id.as_ref().map(LabelExternalIdData::as_ref)
    }
}

impl LabelExternalIdData {
    fn as_ref(&self) -> ResolveExternalId<'_> {
        ResolveExternalId {
            provider_id: &self.provider_id,
            id_type: &self.id_type,
            id_value: &self.id_value,
        }
    }
}

fn parse_label_add_request(
    vm: &luau::Vm,
    table: &luau::Table,
) -> luau::runtime::Result<LabelAddRequestData> {
    Ok(LabelAddRequestData {
        name: required_string(vm, table, "name")?,
        catalog_number: optional_string(vm, table, "catalog_number")?,
        external_id: optional_external_id(vm, table)?,
    })
}

fn parse_label_resolve_request(
    vm: &luau::Vm,
    table: &luau::Table,
) -> luau::runtime::Result<LabelResolveRequestData> {
    Ok(LabelResolveRequestData {
        name: required_string(vm, table, "name")?,
        external_id: optional_external_id(vm, table)?,
    })
}

fn parse_label_add_requests(
    vm: &luau::Vm,
    table: &luau::Table,
) -> luau::runtime::Result<Vec<LabelAddRequestData>> {
    let mut parsed = Vec::new();
    for (_, value) in ordered_array_values(vm, table)? {
        let luau::Value::Table(request) = value else {
            return Err(crate::plugins::runtime_error(
                "label requests must be tables",
            ));
        };
        parsed.push(parse_label_add_request(vm, &request)?);
    }
    Ok(parsed)
}

fn optional_external_id(
    vm: &luau::Vm,
    table: &luau::Table,
) -> luau::runtime::Result<Option<LabelExternalIdData>> {
    match table.get_raw(vm, "external_id")? {
        luau::Value::Nil => Ok(None),
        luau::Value::Table(table) => Ok(Some(LabelExternalIdData {
            provider_id: required_string(vm, &table, "provider_id")?,
            id_type: required_string(vm, &table, "id_type")?,
            id_value: required_string(vm, &table, "id_value")?,
        })),
        other => Err(crate::plugins::runtime_error(format!(
            "external_id must be a table, got {}",
            other.type_name()
        ))),
    }
}

fn required_string(vm: &luau::Vm, table: &luau::Table, key: &str) -> luau::runtime::Result<String> {
    optional_string(vm, table, key)?
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| crate::plugins::runtime_error(format!("{key} must be a non-empty string")))
}

fn optional_string(
    vm: &luau::Vm,
    table: &luau::Table,
    key: &str,
) -> luau::runtime::Result<Option<String>> {
    match table.get_raw(vm, key)? {
        luau::Value::Nil => Ok(None),
        luau::Value::String(bytes) => String::from_utf8(bytes)
            .map(Some)
            .map_err(crate::plugins::runtime_error),
        other => Err(crate::plugins::runtime_error(format!(
            "{key} must be a string, got {}",
            other.type_name()
        ))),
    }
}

fn ordered_array_values(
    vm: &luau::Vm,
    table: &luau::Table,
) -> luau::runtime::Result<Vec<(i64, luau::Value)>> {
    let mut values = Vec::new();
    for (key, value) in table.pairs_raw(vm)? {
        let Some(index) = sequence_index(key) else {
            continue;
        };
        values.push((index, value));
    }
    values.sort_by_key(|(index, _)| *index);
    Ok(values)
}

fn parse_db_ids(vm: &luau::Vm, table: &luau::Table) -> luau::runtime::Result<Vec<DbId>> {
    let mut ids = Vec::new();
    for (_, value) in ordered_array_values(vm, table)? {
        if let Some(id) = db_id_from_value(value)? {
            ids.push(id);
        }
    }
    ids.sort_by_key(|id| id.0);
    ids.dedup();
    Ok(ids)
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

fn db_id_from_value(value: luau::Value) -> luau::runtime::Result<Option<DbId>> {
    match value {
        luau::Value::Integer(value) if value > 0 => Ok(Some(DbId(value))),
        luau::Value::Number(value) if value.is_finite() && value.fract() == 0.0 && value > 0.0 => {
            Ok(Some(DbId(value as i64)))
        }
        luau::Value::Integer(_) | luau::Value::Number(_) => Ok(None),
        other => Err(crate::plugins::runtime_error(format!(
            "id entries must be positive integers, got {}",
            other.type_name()
        ))),
    }
}

fn require_positive_id(value: i64, name: &str) -> luau::runtime::Result<i64> {
    if value > 0 {
        Ok(value)
    } else {
        Err(crate::plugins::runtime_error(format!(
            "{name} must be positive"
        )))
    }
}

fn caller_principal(context: &luau::CallContext) -> Option<Principal> {
    crate::plugins::auth::dispatch_principal(context)
}

fn can_read_entity(
    db: &impl db::DbAccess,
    principal: Option<&Principal>,
    entity_db_id: DbId,
) -> luau::runtime::Result<bool> {
    match principal {
        Some(principal) => {
            crate::routes::entity_accessible_to_principal(db, principal, entity_db_id)
                .map_err(crate::plugins::runtime_error)
        }
        None => Ok(true),
    }
}

fn can_mutate_release(
    db: &impl db::DbAccess,
    principal: Option<&Principal>,
    release_db_id: DbId,
) -> luau::runtime::Result<bool> {
    match principal {
        Some(principal) => Ok(can_mutate_global(Some(principal))
            && crate::routes::entity_accessible_to_principal(db, principal, release_db_id)
                .map_err(crate::plugins::runtime_error)?),
        None => Ok(true),
    }
}

fn can_mutate_global(principal: Option<&Principal>) -> bool {
    principal.is_none_or(|principal| {
        db::roles::has_permission(&principal.permissions, Permission::ManageLibraries)
    })
}

fn label_info_table(label: db::labels::Label) -> luau::OwnedTable {
    let mut table = luau::OwnedTable::with_capacity(0, 3);
    let db_id = label.db_id.map(DbId::from);
    table.set_field(
        "db_id",
        db_id
            .map(|id| luau::Value::Integer(id.0))
            .unwrap_or(luau::Value::Nil),
    );
    table.set_field("id", luau::Value::String(label.id.into_bytes()));
    table.set_field("name", luau::Value::String(label.name.into_bytes()));
    table
}

fn label_for_release_table(label: db::labels::LabelForRelease) -> luau::OwnedTable {
    let mut table = luau::OwnedTable::with_capacity(0, 2);
    table.set_field(
        "label",
        luau::Value::TableData(label_info_table(label.label)),
    );
    table.set_field(
        "catalog_number",
        label
            .catalog_number
            .map(|value| luau::Value::String(value.into_bytes()))
            .unwrap_or(luau::Value::Nil),
    );
    table
}

fn label_release_array(labels: Vec<db::labels::LabelForRelease>) -> luau::OwnedTable {
    let mut table = luau::OwnedTable::with_entry_capacity(0, 0, labels.len());
    for (index, label) in labels.into_iter().enumerate() {
        table.set_key(
            luau::Value::Integer(index as i64 + 1),
            luau::Value::TableData(label_for_release_table(label)),
        );
    }
    table
}

fn id_array(ids: Vec<DbId>) -> luau::OwnedTable {
    let mut table = luau::OwnedTable::with_entry_capacity(0, 0, ids.len());
    for (index, id) in ids.into_iter().enumerate() {
        table.set_key(
            luau::Value::Integer(index as i64 + 1),
            luau::Value::Integer(id.0),
        );
    }
    table
}

#[cfg(feature = "docgen")]
impl LuauTypeInfo for LabelExternalId {
    fn luau_type() -> LuauType {
        LuauType::literal("LabelExternalId")
    }
}

#[cfg(feature = "docgen")]
impl LuauTypeInfo for LabelAddRequest {
    fn luau_type() -> LuauType {
        LuauType::literal("LabelAddRequest")
    }
}

#[cfg(feature = "docgen")]
impl LuauTypeInfo for LabelResolveRequest {
    fn luau_type() -> LuauType {
        LuauType::literal("LabelResolveRequest")
    }
}

impl LuauTypeInfo for LabelInfo {
    fn luau_type() -> LuauType {
        LuauType::literal("LabelInfo")
    }
}

#[cfg(feature = "docgen")]
impl LuauTypeInfo for LabelForReleaseInfo {
    fn luau_type() -> LuauType {
        LuauType::literal("LabelForReleaseInfo")
    }
}

fn field(name: &'static str, ty: LuauType) -> FieldDescriptor {
    FieldDescriptor {
        name,
        ty,
        description: None,
    }
}

#[cfg(feature = "docgen")]
fn param(name: &'static str, ty: LuauType) -> ParameterDescriptor {
    ParameterDescriptor {
        name,
        ty,
        description: None,
        variadic: false,
    }
}

#[cfg(feature = "docgen")]
impl DescribeInterface for LabelExternalId {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("LabelExternalId", None);
        descriptor.fields.extend([
            field("provider_id", String::luau_type()),
            field("id_type", String::luau_type()),
            field("id_value", String::luau_type()),
        ]);
        descriptor
    }
}

#[cfg(feature = "docgen")]
impl DescribeInterface for LabelAddRequest {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("LabelAddRequest", None);
        descriptor.fields.extend([
            field("name", String::luau_type()),
            field("catalog_number", Option::<String>::luau_type()),
            field("external_id", Option::<LabelExternalId>::luau_type()),
        ]);
        descriptor
    }
}

#[cfg(feature = "docgen")]
impl DescribeInterface for LabelResolveRequest {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("LabelResolveRequest", None);
        descriptor.fields.extend([
            field("name", String::luau_type()),
            field("external_id", Option::<LabelExternalId>::luau_type()),
        ]);
        descriptor
    }
}

impl DescribeInterface for LabelInfo {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("LabelInfo", None);
        descriptor.fields.extend([
            field("db_id", Option::<i64>::luau_type()),
            field("id", String::luau_type()),
            field("name", String::luau_type()),
        ]);
        descriptor
    }
}

#[cfg(feature = "docgen")]
impl DescribeInterface for LabelForReleaseInfo {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("LabelForReleaseInfo", None);
        descriptor.fields.extend([
            field("label", LabelInfo::luau_type()),
            field("catalog_number", Option::<String>::luau_type()),
        ]);
        descriptor
    }
}

#[cfg(feature = "docgen")]
fn module_descriptor() -> ModuleDescriptor {
    ModuleDescriptor {
        name: "Labels",
        local_name: "labels",
        description: Some("Read and mutate release label metadata."),
        fields: Vec::new(),
        functions: vec![
            ModuleFunctionDescriptor {
                path: vec!["add"],
                description: None,
                params: vec![
                    param("release_id", i64::luau_type()),
                    param("request", LabelAddRequest::luau_type()),
                ],
                returns: vec![i64::luau_type()],
                yields: false,
            },
            ModuleFunctionDescriptor {
                path: vec!["resolve"],
                description: None,
                params: vec![param("request", LabelResolveRequest::luau_type())],
                returns: vec![i64::luau_type()],
                yields: false,
            },
            ModuleFunctionDescriptor {
                path: vec!["sync_for_release"],
                description: None,
                params: vec![
                    param("release_id", i64::luau_type()),
                    param("requests", Vec::<LabelAddRequest>::luau_type()),
                ],
                returns: Vec::new(),
                yields: false,
            },
            ModuleFunctionDescriptor {
                path: vec!["get_by_id"],
                description: None,
                params: vec![param("label_id", i64::luau_type())],
                returns: vec![Option::<LabelInfo>::luau_type()],
                yields: false,
            },
            ModuleFunctionDescriptor {
                path: vec!["get_for_release"],
                description: None,
                params: vec![param("release_id", i64::luau_type())],
                returns: vec![Vec::<LabelForReleaseInfo>::luau_type()],
                yields: false,
            },
            ModuleFunctionDescriptor {
                path: vec!["get_for_releases_many"],
                description: None,
                params: vec![param("release_ids", Vec::<i64>::luau_type())],
                returns: vec![LuauType::map(
                    i64::luau_type(),
                    Vec::<LabelForReleaseInfo>::luau_type(),
                )],
                yields: false,
            },
            ModuleFunctionDescriptor {
                path: vec!["get_releases"],
                description: None,
                params: vec![param("label_id", i64::luau_type())],
                returns: vec![Vec::<i64>::luau_type()],
                yields: false,
            },
            ModuleFunctionDescriptor {
                path: vec!["get_releases_many"],
                description: None,
                params: vec![param("label_ids", Vec::<i64>::luau_type())],
                returns: vec![LuauType::map(i64::luau_type(), Vec::<i64>::luau_type())],
                yields: false,
            },
        ],
    }
}

#[cfg(feature = "docgen")]
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    render_definition_file_with_support(
        &module_descriptor(),
        &[],
        &[
            LabelExternalId::interface_descriptor(),
            LabelAddRequest::interface_descriptor(),
            LabelResolveRequest::interface_descriptor(),
            LabelInfo::interface_descriptor(),
            LabelForReleaseInfo::interface_descriptor(),
        ],
        &[],
    )
}
