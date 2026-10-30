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

use crate::plugins::db::Db;

#[derive(Clone, Debug, Serialize)]
struct PublicUser {
    user_id: String,
    username: String,
    role: Option<String>,
    permissions: Vec<lyra_db::Permission>,
}

pub(crate) struct UsersModule;

#[harmony_macros::module(id = "lyra/users", docs_feature = "docgen")]
impl UsersModule {
    #[harmony(returns(Vec<PublicUser>))]
    async fn list(#[context] db: Db) -> luau::runtime::Result<luau::Value> {
        let db = db.read().await;
        let users = lyra_db::users::get(&db)?
            .into_iter()
            .filter_map(|user| {
                let user_db_id = user.db_id?;
                let role = lyra_db::roles::get_role_for_user(&db, user_db_id)
                    .ok()
                    .flatten();
                Some(PublicUser {
                    user_id: user.id,
                    username: user.username,
                    permissions: role
                        .as_ref()
                        .map(|role| role.permissions.clone())
                        .unwrap_or_default(),
                    role: role.map(|role| role.name),
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
