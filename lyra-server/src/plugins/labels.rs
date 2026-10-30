// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::DbId;
use harmony_luau as luau;
#[cfg(feature = "docgen")]
use harmony_luau::{
    DescribeInterface,
    FieldDescriptor,
    InterfaceDescriptor,
    LuauType,
    LuauTypeInfo,
};
#[cfg(feature = "docgen")]
use harmony_luau::{
    DescribeModule,
    render_definition_file_with_support,
};
#[cfg(feature = "docgen")]
use std::collections::HashMap;

use crate::plugins::args::{
    PositiveId,
    UniqueIds,
};
use crate::{
    plugins::{
        auth::DispatchPrincipal,
        db::{
            Db,
            ViewerDb,
        },
    },
    services::auth::Principal,
};
use lyra_db::Permission;
use lyra_db::labels::{
    ResolveExternalId,
    ResolveLabel,
};

pub(crate) struct LabelsModule;
#[cfg(feature = "docgen")]
struct LabelResolveRequest;
#[cfg(feature = "docgen")]
struct LabelExternalId;
#[cfg(feature = "docgen")]
struct LabelInfo;
#[cfg(feature = "docgen")]
struct LabelForReleaseInfo;

impl<'vm> luau::FromLuau<'vm> for LabelResolveRequestData {
    fn read(reader: &mut luau::ArgReader<'vm>) -> luau::runtime::Result<Self> {
        let request: luau::Table = reader.read()?;
        parse_label_resolve_request(&request)
    }
}

#[harmony_macros::module(
    id = "lyra/labels",
    description = "Read and mutate release label metadata.",
    docs_feature = "docgen"
)]
impl LabelsModule {
    #[harmony(args(request: LabelResolveRequest))]
    async fn resolve(
        request: LabelResolveRequestData,
        #[context] caller: DispatchPrincipal,
        #[context] vm: luau::Vm,
    ) -> luau::runtime::Result<i64> {
        if !can_mutate_global(caller.0.as_ref()) {
            return Ok(0);
        }
        let db = crate::plugins::db::get(&vm)?;
        let label_id = {
            let mut db = db.write().await;
            lyra_db::labels::resolve(
                &mut db,
                &ResolveLabel {
                    name: &request.name,
                    external_id: request.external_id_ref(),
                },
            )
            .map_err(crate::plugins::runtime_error)
        }?;

        Ok(label_id.0)
    }

    #[harmony(returns(Option<LabelInfo>))]
    async fn get_by_id(
        label_id: PositiveId,
        #[context] db: Db,
    ) -> luau::runtime::Result<Option<luau::OwnedTable>> {
        let label = {
            let db = db.read().await;
            lyra_db::labels::get_by_id(&db, label_id.0).map_err(crate::plugins::runtime_error)
        }?;
        Ok(label.map(label_info_table))
    }

    #[harmony(returns(Vec<LabelForReleaseInfo>))]
    async fn get_for_release(
        release_id: PositiveId,
        #[context] db: ViewerDb,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let release_id = release_id.0;
        let labels = {
            let (db, viewer) = db.read().await?;
            if viewer.sees_entity(&*db, release_id)? {
                lyra_db::labels::get_for_release(&db, release_id)?
            } else {
                Vec::new()
            }
        };
        Ok(label_release_array(labels))
    }

    #[harmony(
        returns(HashMap<i64, Vec<LabelForReleaseInfo>>)
    )]
    async fn get_for_releases_many(
        release_ids: UniqueIds,
        #[context] db: ViewerDb,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let labels = {
            let (db, viewer) = db.read().await?;
            let readable = viewer.visible(&db, release_ids.0)?;
            lyra_db::labels::get_for_releases_many(&db, &readable)
                .map_err(crate::plugins::runtime_error)
        }?;
        Ok(luau::OwnedTable::from_entries(labels.into_iter().map(
            |(release_id, labels)| (release_id.0, label_release_array(labels)),
        )))
    }

    #[harmony(returns(Vec<i64>))]
    async fn get_releases(
        label_id: PositiveId,
        #[context] db: ViewerDb,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let label_id = label_id.0;
        let release_ids = {
            let (db, viewer) = db.read().await?;
            let release_ids = lyra_db::labels::get_releases(&db, label_id)?;
            viewer
                .visible(&db, release_ids)
                .map_err(crate::plugins::runtime_error)
        }?;
        Ok(id_array(release_ids))
    }

    #[harmony(returns(HashMap<i64, Vec<i64>>))]
    async fn get_releases_many(
        label_ids: UniqueIds,
        #[context] db: ViewerDb,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let ids = label_ids.0;
        let release_ids = {
            let (db, viewer) = db.read().await?;
            let mut visible = Vec::new();
            for (label_id, release_ids) in lyra_db::labels::get_releases_many(&db, &ids)? {
                let release_ids = viewer.visible(&db, release_ids)?;
                visible.push((label_id, release_ids));
            }
            Ok::<_, luau::runtime::Error>(visible)
        }?;
        Ok(luau::OwnedTable::from_entries(
            release_ids
                .into_iter()
                .map(|(label_id, ids)| (label_id.0, id_array(ids))),
        ))
    }
}

#[derive(Clone)]
struct LabelExternalIdData {
    provider_id: String,
    id_type: String,
    id_value: String,
}

struct LabelResolveRequestData {
    name: String,
    external_id: Option<LabelExternalIdData>,
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

fn parse_label_resolve_request(
    table: &luau::Table,
) -> luau::runtime::Result<LabelResolveRequestData> {
    Ok(LabelResolveRequestData {
        name: required_string(table, "name")?,
        external_id: optional_external_id(table)?,
    })
}

fn optional_external_id(table: &luau::Table) -> luau::runtime::Result<Option<LabelExternalIdData>> {
    match table.get("external_id")? {
        luau::Value::Nil => Ok(None),
        luau::Value::Table(table) => Ok(Some(LabelExternalIdData {
            provider_id: required_string(&table, "provider_id")?,
            id_type: required_string(&table, "id_type")?,
            id_value: required_string(&table, "id_value")?,
        })),
        other => Err(crate::plugins::runtime_error(format!(
            "external_id must be a table, got {}",
            other.type_name()
        ))),
    }
}

fn required_string(table: &luau::Table, key: &str) -> luau::runtime::Result<String> {
    table
        .optional_string_field(key)?
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| crate::plugins::runtime_error(format!("{key} must be a non-empty string")))
}

fn can_mutate_global(principal: Option<&Principal>) -> bool {
    principal.is_none_or(|principal| {
        lyra_db::roles::has_permission(&principal.permissions, Permission::ManageLibraries)
    })
}

fn label_info_table(label: lyra_db::labels::Label) -> luau::OwnedTable {
    let mut table = luau::OwnedTable::with_capacity(0, 3);
    table.set_field("db_id", label.db_id.map(|id| DbId::from(id).0));
    table.set_field("id", label.id);
    table.set_field("name", label.name);
    table
}

fn label_for_release_table(label: lyra_db::labels::LabelForRelease) -> luau::OwnedTable {
    let mut table = luau::OwnedTable::with_capacity(0, 2);
    table.set_field("label", label_info_table(label.label));
    table.set_field("catalog_number", label.catalog_number);
    table
}

fn label_release_array(labels: Vec<lyra_db::labels::LabelForRelease>) -> luau::OwnedTable {
    luau::OwnedTable::from_entries(
        labels
            .into_iter()
            .enumerate()
            .map(|(index, label)| (index as i64 + 1, label_for_release_table(label))),
    )
}

fn id_array(ids: Vec<DbId>) -> luau::OwnedTable {
    luau::OwnedTable::from_entries(
        ids.into_iter()
            .enumerate()
            .map(|(index, id)| (index as i64 + 1, id.0)),
    )
}

#[cfg(feature = "docgen")]
impl LuauTypeInfo for LabelExternalId {
    fn luau_type() -> LuauType {
        LuauType::literal("LabelExternalId")
    }
}

#[cfg(feature = "docgen")]
impl LuauTypeInfo for LabelResolveRequest {
    fn luau_type() -> LuauType {
        LuauType::literal("LabelResolveRequest")
    }
}

#[cfg(feature = "docgen")]
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

#[cfg(feature = "docgen")]
fn field(name: &'static str, ty: LuauType) -> FieldDescriptor {
    FieldDescriptor {
        name,
        ty,
        description: None,
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

#[cfg(feature = "docgen")]
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
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    render_definition_file_with_support(
        &LabelsModule::module_descriptor(),
        &[],
        &[
            LabelExternalId::interface_descriptor(),
            LabelResolveRequest::interface_descriptor(),
            LabelInfo::interface_descriptor(),
            LabelForReleaseInfo::interface_descriptor(),
        ],
        &[],
    )
}
