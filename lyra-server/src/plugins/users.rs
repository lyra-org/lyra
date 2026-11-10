// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

#[cfg(feature = "docgen")]
use std::collections::HashMap;

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
        args::{
            self,
            UniqueIds,
        },
        auth::permission_names,
        db::Db,
        types::LuauPermission,
    },
    services::auth::{
        accounts,
        resolve_principal,
    },
};

/// A user as plugins see it.
#[derive(Serialize)]
struct UserRecord {
    id: String,
    username: String,
    role: Option<String>,
    permissions: Vec<&'static str>,
    library_ids: Vec<String>,
}

impl UserRecord {
    /// `user` with the libraries it can see among `libraries`, in their order.
    fn of(db: &agdb::DbAny, libraries: &[lyra_db::Library], user: lyra_db::User) -> Option<Self> {
        let principal = resolve_principal(db, user.db_id?, user);
        Some(Self {
            library_ids: libraries
                .iter()
                .filter(|library| principal.accessible_library_ids.contains(&library.id))
                .map(|library| library.id.clone())
                .collect(),
            permissions: permission_names(&principal.permissions),
            id: principal.user_public_id,
            username: principal.username,
            role: principal.role_name,
        })
    }
}

#[derive(Default)]
struct ListOptions {
    username: Option<String>,
}

impl<'vm> luau::FromLuau<'vm> for ListOptions {
    fn read(reader: &mut luau::ArgReader<'vm>) -> luau::runtime::Result<Self> {
        let options: luau::Table = reader.read()?;
        args::reject_unknown_fields(&options, "options", &["username"])?;
        Ok(Self {
            username: options.optional_string_field("username")?,
        })
    }
}

impl LuauTypeInfo for ListOptions {
    fn luau_type() -> LuauType {
        LuauType::named("UserListOptions")
    }
}

pub(crate) struct UsersModule;

#[harmony_macros::module(id = "lyra/users", docs_feature = "docgen")]
impl UsersModule {
    #[harmony(
        description = "Lists the users, whoever the caller is. `options` narrows them. The default user is left out while authentication is enabled.",
        returns(Vec<UserRecord>)
    )]
    async fn list(
        options: Option<ListOptions>,
        #[context] db: Db,
    ) -> luau::runtime::Result<luau::Value> {
        let options = options.unwrap_or_default();
        let db = db.read().await;
        let users = match options.username {
            Some(username) => accounts::get_by_username(&*db, &username)?
                .into_iter()
                .collect(),
            None => accounts::list(&*db)?,
        };
        let libraries = lyra_db::libraries::get(&*db)?;
        let users = users
            .into_iter()
            .filter_map(|user| UserRecord::of(&db, &libraries, user))
            .collect::<Vec<_>>();
        harmony_luau::serializable_to_luau_owned(users)
    }

    #[harmony(
        description = "The users, keyed by id, whoever the caller is. Missing users are absent, as is the default user while authentication is enabled.",
        returns(HashMap<String, UserRecord>)
    )]
    async fn get(ids: UniqueIds, #[context] db: Db) -> luau::runtime::Result<luau::OwnedTable> {
        let db = db.read().await;
        let libraries = lyra_db::libraries::get(&*db)?;
        let mut table = luau::OwnedTable::with_capacity(0, ids.0.len());
        for id in ids.0 {
            let Some(user) = accounts::get_by_public_id(&*db, &id)? else {
                continue;
            };
            if let Some(user) = UserRecord::of(&db, &libraries, user) {
                table.set_field(id, harmony_luau::serializable_to_luau_owned(user)?);
            }
        }
        Ok(table)
    }
}

impl LuauTypeInfo for UserRecord {
    fn luau_type() -> LuauType {
        LuauType::named("User")
    }
}

impl DescribeInterface for UserRecord {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("User", None);
        descriptor.fields.extend([
            FieldDescriptor {
                name: "id",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "username",
                ty: String::luau_type(),
                description: Some("Unique among users, whatever its case."),
            },
            FieldDescriptor {
                name: "role",
                ty: Option::<String>::luau_type(),
                description: Some("The name of the user's role; nil without one."),
            },
            FieldDescriptor {
                name: "permissions",
                ty: Vec::<LuauPermission>::luau_type(),
                description: Some("What the user's role grants; empty without one."),
            },
            FieldDescriptor {
                name: "library_ids",
                ty: Vec::<String>::luau_type(),
                description: Some(
                    "The libraries the user can see, in creation order; every library for admins.",
                ),
            },
        ]);
        descriptor
    }
}

#[cfg(feature = "docgen")]
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    use harmony_luau::{
        DescribeModule,
        DescribeTypeAlias,
    };

    let mut list_options = InterfaceDescriptor::new("UserListOptions", None);
    list_options.fields.push(FieldDescriptor {
        name: "username",
        ty: Option::<String>::luau_type(),
        description: Some(
            "List only the user with this username, compared as sign-in compares it, ignoring case. Default every user.",
        ),
    });
    harmony_luau::render_definition_file_with_support(
        &UsersModule::module_descriptor(),
        &[LuauPermission::type_alias_descriptor()],
        &[UserRecord::interface_descriptor(), list_options],
        &[],
    )
}
