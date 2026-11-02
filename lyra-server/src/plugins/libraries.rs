// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

#[cfg(feature = "docgen")]
use std::collections::HashMap;

use harmony_luau as luau;
#[cfg(feature = "docgen")]
use harmony_luau::{
    DescribeInterface,
    FieldDescriptor,
    InterfaceDescriptor,
};
use harmony_luau::{
    LuauType,
    LuauTypeInfo,
};
use lyra_db::{
    Library,
    Permission,
};

use crate::{
    plugins::{
        args::UniqueIds,
        auth::DispatchPrincipal,
        db::Db,
        ids::Resolved,
    },
    services::{
        auth::{
            Principal,
            access,
        },
        libraries::system_context,
    },
};

pub(crate) struct LibrariesModule;

/// Who reads the libraries: a user, who sees the libraries granted to them and their paths with
/// `ManageLibraries`, or the system, which sees every library and its path.
struct Reader(Option<Principal>);

impl Reader {
    fn libraries(&self, db: &impl lyra_db::DbAccess) -> luau::runtime::Result<Vec<Library>> {
        let libraries = match &self.0 {
            Some(principal) => access::libraries(db, principal),
            None => access::system_libraries(db, &system_context()),
        };
        libraries.map_err(crate::plugins::runtime_error)
    }

    fn libraries_for_entity(
        &self,
        db: &impl lyra_db::DbAccess,
        entity_db_id: agdb::DbId,
    ) -> luau::runtime::Result<Vec<Library>> {
        let libraries = match &self.0 {
            Some(principal) => access::libraries_for_entity(db, principal, entity_db_id),
            None => access::system_libraries_for_entity(db, &system_context(), entity_db_id),
        };
        libraries.map_err(crate::plugins::runtime_error)
    }

    fn record(&self, library: Library) -> luau::runtime::Result<luau::Value> {
        let shows_path = self.0.as_ref().is_none_or(|principal| {
            lyra_db::roles::has_permission(&principal.permissions, Permission::ManageLibraries)
        });
        harmony_luau::serializable_to_luau_owned(LibraryRecord {
            path: shows_path.then(|| library.path.to_string_lossy().into_owned()),
            id: library.id,
            name: library.name,
            language: library.language,
            country: library.country,
        })
    }
}

#[harmony_macros::module(
    id = "lyra/libraries",
    description = "The libraries the caller can see.",
    docs_feature = "docgen"
)]
impl LibrariesModule {
    #[harmony(
        description = "The libraries the caller can see, in the order they were created.",
        returns(Vec<LibraryRecord>)
    )]
    async fn list(
        #[context] db: Db,
        #[context] caller: DispatchPrincipal,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let reader = Reader(caller.0);
        let db = db.read().await;
        let mut list = luau::OwnedTable::new();
        for library in reader.libraries(&*db)? {
            list.push_array(reader.record(library)?);
        }
        Ok(list)
    }

    #[harmony(
        description = "The libraries with these ids, keyed by id. Libraries that are missing or hidden from the caller are absent.",
        returns(HashMap<String, LibraryRecord>)
    )]
    async fn get(
        library_ids: UniqueIds,
        #[context] db: Db,
        #[context] caller: DispatchPrincipal,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let reader = Reader(caller.0);
        let db = db.read().await;
        let mut libraries = luau::OwnedTable::new();
        for library in reader.libraries(&*db)? {
            if library_ids.0.contains(&library.id) {
                let id = library.id.clone();
                libraries.set_field(id, reader.record(library)?);
            }
        }
        Ok(libraries)
    }

    #[harmony(
        description = "The libraries holding each of these entities that the caller can see, keyed by entity id, each list starting with the one the server picks for the entity. A library holds itself. Entities in no library the caller can see are absent.",
        returns(HashMap<String, Vec<LibraryRecord>>)
    )]
    async fn by_entity(
        entity_ids: UniqueIds,
        #[context] db: Db,
        #[context] caller: DispatchPrincipal,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let reader = Reader(caller.0);
        let db = db.read().await;
        let entities = Resolved::of(&*db, entity_ids.0)?;
        let mut values = Vec::with_capacity(entities.db_ids().len());
        for &entity_db_id in entities.db_ids() {
            let libraries = reader.libraries_for_entity(&*db, entity_db_id)?;
            if libraries.is_empty() {
                continue;
            }
            let mut list = luau::OwnedTable::with_capacity(libraries.len(), 0);
            for library in libraries {
                list.push_array(reader.record(library)?);
            }
            values.push((entity_db_id, list));
        }
        Ok(entities.keyed(values))
    }
}

#[derive(serde::Serialize)]
struct LibraryRecord {
    id: String,
    name: String,
    path: Option<String>,
    language: Option<String>,
    country: Option<String>,
}

impl LuauTypeInfo for LibraryRecord {
    fn luau_type() -> LuauType {
        LuauType::named("Library")
    }
}

#[cfg(feature = "docgen")]
impl DescribeInterface for LibraryRecord {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("Library", None);
        descriptor.fields.extend([
            FieldDescriptor {
                name: "id",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "name",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "path",
                ty: Option::<String>::luau_type(),
                description: Some(
                    "The directory the library scans. Nil unless the caller has the manage_libraries permission, or the call runs without a user.",
                ),
            },
            FieldDescriptor {
                name: "language",
                ty: Option::<String>::luau_type(),
                description: Some("The ISO 639 language code of the library's metadata, if set."),
            },
            FieldDescriptor {
                name: "country",
                ty: Option::<String>::luau_type(),
                description: Some("The ISO 3166 country code of the library's metadata, if set."),
            },
        ]);
        descriptor
    }
}

#[cfg(feature = "docgen")]
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    use harmony_luau::DescribeModule;

    harmony_luau::render_definition_file_with_support(
        &LibrariesModule::module_descriptor(),
        &[],
        &[LibraryRecord::interface_descriptor()],
        &[],
    )
}
