// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

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

use crate::plugins::args::UniqueIds;
use crate::{
    plugins::{
        auth::DispatchPrincipal,
        db::{
            Db,
            ViewerDb,
        },
        ids::{
            self,
            Resolved,
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
    #[harmony(
        args(request: LabelResolveRequest),
        description = "The id of the label matching the request, created when there is none. Returns nil when the caller cannot manage libraries. Needs the `lyra.labels.manage` scope too."
    )]
    async fn resolve(
        request: LabelResolveRequestData,
        #[context] caller: DispatchPrincipal,
        #[context] vm: luau::Vm,
    ) -> luau::runtime::Result<Option<String>> {
        if !can_mutate_global(caller.0.as_ref()) {
            return Ok(None);
        }
        let db = crate::plugins::db::get(&vm)?;
        let mut db = db.write().await;
        let label_id = lyra_db::labels::resolve(
            &mut db,
            &ResolveLabel {
                name: &request.name,
                external_id: request.external_id_ref(),
            },
        )
        .map_err(crate::plugins::runtime_error)?;
        ids::public_id(&*db, label_id).map(Some)
    }

    #[harmony(returns(Option<LabelInfo>))]
    async fn get_by_id(
        label_id: String,
        #[context] db: Db,
    ) -> luau::runtime::Result<Option<luau::OwnedTable>> {
        let db = db.read().await;
        let Some(label_id) = ids::db_id(&*db, &label_id)? else {
            return Ok(None);
        };
        let label =
            lyra_db::labels::get_by_id(&db, label_id).map_err(crate::plugins::runtime_error)?;
        Ok(label.map(label_info_table))
    }

    #[harmony(returns(Vec<LabelForReleaseInfo>))]
    async fn get_for_release(
        release_id: String,
        #[context] db: ViewerDb,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let labels = {
            let (db, viewer) = db.read().await?;
            match ids::db_id(&*db, &release_id)? {
                Some(release_id) if viewer.sees_entity(&*db, release_id)? => {
                    lyra_db::labels::get_for_release(&db, release_id)?
                }
                _ => Vec::new(),
            }
        };
        Ok(label_release_array(labels))
    }

    #[harmony(
        description = "The labels of each of these releases that the caller can see, keyed by release id.",
        returns(HashMap<String, Vec<LabelForReleaseInfo>>)
    )]
    async fn get_for_releases_many(
        release_ids: UniqueIds,
        #[context] db: ViewerDb,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let (db, viewer) = db.read().await?;
        let releases = Resolved::of(&*db, release_ids.0)?;
        let readable = viewer.visible(&db, releases.db_ids().to_vec())?;
        let labels = lyra_db::labels::get_for_releases_many(&db, &readable)
            .map_err(crate::plugins::runtime_error)?;
        Ok(releases.keyed(
            labels
                .into_iter()
                .map(|(release_id, labels)| (release_id, label_release_array(labels))),
        ))
    }

    #[harmony(
        description = "The ids of the label's releases that the caller can see.",
        returns(Vec<String>)
    )]
    async fn get_releases(
        label_id: String,
        #[context] db: ViewerDb,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let (db, viewer) = db.read().await?;
        let Some(label_id) = ids::db_id(&*db, &label_id)? else {
            return Ok(luau::OwnedTable::new());
        };
        let release_ids = lyra_db::labels::get_releases(&db, label_id)?;
        let release_ids = viewer
            .visible(&db, release_ids)
            .map_err(crate::plugins::runtime_error)?;
        ids::public_id_list(&*db, &release_ids)
    }

    #[harmony(
        description = "The ids of each label's releases that the caller can see, keyed by label id.",
        returns(HashMap<String, Vec<String>>)
    )]
    async fn get_releases_many(
        label_ids: UniqueIds,
        #[context] db: ViewerDb,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let (db, viewer) = db.read().await?;
        let labels = Resolved::of(&*db, label_ids.0)?;
        let mut releases = Vec::new();
        for (label_id, release_ids) in lyra_db::labels::get_releases_many(&db, labels.db_ids())? {
            let release_ids = viewer.visible(&db, release_ids)?;
            releases.push((label_id, ids::public_id_list(&*db, &release_ids)?));
        }
        Ok(labels.keyed(releases))
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
    let mut table = luau::OwnedTable::with_capacity(0, 2);
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
