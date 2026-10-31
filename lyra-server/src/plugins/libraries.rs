// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashMap;

use agdb::DbId;
use harmony_luau as luau;
use harmony_luau::{
    DescribeInterface,
    FieldDescriptor,
    InterfaceDescriptor,
    LuauType,
    LuauTypeInfo,
};
use serde::Serialize;

use crate::{
    plugins::{
        args::UniqueIds,
        auth::DispatchPrincipal,
        db::Db,
        ids::{
            self,
            Resolved,
        },
    },
    services::auth::access::{
        self,
        LibraryFull,
        LibraryView,
    },
};

pub(crate) struct LibrariesModule;

#[harmony_macros::module(id = "lyra/libraries", docs_feature = "docgen")]
impl LibrariesModule {
    #[harmony(
        description = "The libraries the caller can see, or only the one with `id`. A library the caller cannot see, or an unknown id, gives an empty list.",
        returns(Vec<LibraryRecord>)
    )]
    async fn list(
        id: Option<String>,
        #[context] db: Db,
        #[context] caller: DispatchPrincipal,
    ) -> luau::runtime::Result<luau::Value> {
        let principal = caller.0;
        let db = db.read().await;
        let libraries: Vec<LibraryRecord> = match (principal.as_ref(), id) {
            (Some(principal), None) => access::libraries(&db, principal)?
                .into_iter()
                .map(LibraryRecord::from)
                .collect::<Vec<_>>(),
            (None, None) => {
                access::system_libraries(&db, &crate::services::libraries::system_context())?
                    .into_iter()
                    .map(LibraryRecord::from)
                    .collect::<Vec<_>>()
            }
            (principal, Some(id)) => match (principal, ids::db_id(&*db, &id)?) {
                (_, None) => Vec::new(),
                (Some(principal), Some(id)) => access::library_by_id(&db, principal, id)?
                    .into_iter()
                    .map(LibraryRecord::from)
                    .collect::<Vec<_>>(),
                (None, Some(id)) => access::system_library_by_id(
                    &db,
                    &crate::services::libraries::system_context(),
                    id,
                )?
                .into_iter()
                .map(LibraryRecord::from)
                .collect::<Vec<_>>(),
            },
        };
        harmony_luau::serializable_to_luau_owned(libraries)
    }

    #[harmony(returns(Vec<LibraryRecord>))]
    async fn get_for_entity(
        entity_id: String,
        #[context] db: Db,
        #[context] caller: DispatchPrincipal,
    ) -> luau::runtime::Result<luau::Value> {
        let db = db.read().await;
        let Some(entity_id) = ids::db_id(&*db, &entity_id)? else {
            return harmony_luau::serializable_to_luau_owned(Vec::<LibraryRecord>::new());
        };
        let libraries = if let Some(principal) = caller.0 {
            access::libraries_for_entity(&db, &principal, entity_id)?
                .into_iter()
                .map(LibraryRecord::from)
                .collect::<Vec<_>>()
        } else {
            access::system_libraries_for_entity(
                &db,
                &crate::services::libraries::system_context(),
                entity_id,
            )?
            .into_iter()
            .map(LibraryRecord::from)
            .collect::<Vec<_>>()
        };
        harmony_luau::serializable_to_luau_owned(libraries)
    }

    #[harmony(
        description = "The library of each of these entities, keyed by entity id. Entities in no library the caller can see are absent.",
        returns(HashMap<String, LibraryRecord>)
    )]
    async fn get_for_entities(
        entity_ids: UniqueIds,
        #[context] db: Db,
        #[context] caller: DispatchPrincipal,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let db = db.read().await;
        let entities = Resolved::of(&*db, entity_ids.0)?;
        let libraries: HashMap<DbId, LibraryRecord> = if let Some(principal) = caller.0 {
            access::libraries_for_entities(&db, &principal, entities.db_ids())?
                .into_iter()
                .map(|(id, library)| (id, LibraryRecord::from(library)))
                .collect::<HashMap<_, _>>()
        } else {
            access::system_libraries_for_entities(
                &db,
                &crate::services::libraries::system_context(),
                entities.db_ids(),
            )?
            .into_iter()
            .map(|(id, library)| (id, LibraryRecord::from(library)))
            .collect::<HashMap<_, _>>()
        };
        let mut values = Vec::with_capacity(libraries.len());
        for (id, library) in libraries {
            values.push((id, harmony_luau::serializable_to_luau_owned(library)?));
        }
        Ok(entities.keyed(values))
    }
}

#[derive(Clone, Serialize)]
pub(crate) struct LibraryRecord {
    id: String,
    name: String,
    path: Option<String>,
    language: Option<String>,
    country: Option<String>,
}

impl From<lyra_db::libraries::Library> for LibraryRecord {
    fn from(library: lyra_db::libraries::Library) -> Self {
        Self {
            id: library.id,
            name: library.name,
            path: Some(library.path.to_string_lossy().to_string()),
            language: library.language,
            country: library.country,
        }
    }
}

impl From<LibraryView> for LibraryRecord {
    fn from(library: LibraryView) -> Self {
        Self {
            id: library.id,
            name: library.name,
            path: None,
            language: library.language,
            country: library.country,
        }
    }
}

impl From<LibraryFull> for LibraryRecord {
    fn from(library: LibraryFull) -> Self {
        Self {
            id: library.id,
            name: library.name,
            path: Some(library.path.to_string_lossy().to_string()),
            language: library.language,
            country: library.country,
        }
    }
}

impl LuauTypeInfo for LibraryRecord {
    fn luau_type() -> LuauType {
        LuauType::named("Library")
    }
}

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
                description: None,
            },
            FieldDescriptor {
                name: "language",
                ty: Option::<String>::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "country",
                ty: Option::<String>::luau_type(),
                description: None,
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
