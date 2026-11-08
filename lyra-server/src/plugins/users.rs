// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

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
    plugins::db::Db,
    services::auth::{
        accounts,
        resolve_principal,
    },
};

#[derive(Clone, Debug, Serialize)]
struct PublicUser {
    user_id: String,
    username: String,
    role: Option<String>,
    permissions: Vec<lyra_db::Permission>,
    library_ids: Vec<String>,
}

pub(crate) struct UsersModule;

#[harmony_macros::module(id = "lyra/users", docs_feature = "docgen")]
impl UsersModule {
    #[harmony(
        description = "Lists the users. The default user is left out while authentication is enabled.",
        returns(Vec<PublicUser>)
    )]
    async fn list(#[context] db: Db) -> luau::runtime::Result<luau::Value> {
        let db = db.read().await;
        let libraries = lyra_db::libraries::get(&*db)?;
        let users = accounts::list(&*db)?
            .into_iter()
            .filter_map(|user| {
                let user_db_id = user.db_id?;
                let principal = resolve_principal(&db, user_db_id, user);
                Some(PublicUser {
                    library_ids: libraries
                        .iter()
                        .filter(|library| principal.accessible_library_ids.contains(&library.id))
                        .map(|library| library.id.clone())
                        .collect(),
                    user_id: principal.user_public_id,
                    username: principal.username,
                    permissions: principal.permissions,
                    role: principal.role_name,
                })
            })
            .collect::<Vec<_>>();
        harmony_luau::serializable_to_luau_owned(users)
    }
}

impl LuauTypeInfo for PublicUser {
    fn luau_type() -> LuauType {
        LuauType::named("PublicUser")
    }
}

impl DescribeInterface for PublicUser {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("PublicUser", None);
        descriptor.fields.extend([
            FieldDescriptor {
                name: "user_id",
                ty: String::luau_type(),
                description: Some("The user's public id."),
            },
            FieldDescriptor {
                name: "username",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "role",
                ty: Option::<String>::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "permissions",
                ty: Vec::<String>::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "library_ids",
                ty: Vec::<String>::luau_type(),
                description: Some(
                    "Public ids of the libraries the user can see, in creation order; every library for admins.",
                ),
            },
        ]);
        descriptor
    }
}

#[cfg(feature = "docgen")]
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    use harmony_luau::DescribeModule;

    harmony_luau::render_definition_file_with_support(
        &UsersModule::module_descriptor(),
        &[],
        &[PublicUser::interface_descriptor()],
        &[],
    )
}
