// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

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
    render_definition_file_with_support,
};
use serde::Serialize;

use crate::plugins::db::{
    self,
    DbAsync,
};

#[derive(Clone, Debug, Serialize)]
struct PublicUser {
    user_id: i64,
    username: String,
    role: Option<String>,
}

#[derive(Clone, Default)]
pub(crate) struct UsersModuleStore {
    db: Option<DbAsync>,
}

impl UsersModuleStore {
    pub(crate) fn empty() -> Self {
        Self { db: None }
    }

    pub(crate) fn with_db(db: DbAsync) -> Self {
        Self { db: Some(db) }
    }

    fn db(&self) -> luau::runtime::Result<DbAsync> {
        self.db.clone().ok_or_else(|| {
            crate::plugins::runtime_error("lyra/users requires a database-backed plugin executor")
        })
    }
}

struct UsersModule;

pub(crate) fn module_spec() -> ModuleSpec {
    ModuleSpec::new("lyra/users")
        .capability("lyra.users")
        .function(list_spec())
        .install(|_| Ok(ModuleExport::new(UsersModule)))
}

fn list_spec() -> FunctionSpec {
    FunctionSpec::async_fn("list")
        .returns::<Vec<PublicUser>>()
        .call_async_native(std::sync::Arc::new(list_callback))
}

fn list_callback(frame: luau::CallFrame<'_>) -> luau::runtime::Result<luau::ScheduledFuture> {
    let store = frame.vm.data().get::<UsersModuleStore>()?.as_ref().clone();
    let db = store.db()?;

    Ok(Box::pin(async move {
        let db = db.read().await;
        let users = db::users::get(&db)
            .map_err(crate::plugins::runtime_error)?
            .into_iter()
            .filter_map(|user| {
                let user_id = user.db_id?;
                let role = db::roles::get_role_for_user(&db, user_id)
                    .ok()
                    .flatten()
                    .map(|role| role.name);
                Some(PublicUser {
                    user_id: user_id.0,
                    username: user.username,
                    role,
                })
            })
            .collect::<Vec<_>>();
        Ok(vec![crate::plugins::serializable_to_luau_owned(users)?])
    }))
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
                ty: i64::luau_type(),
                description: None,
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
        ]);
        descriptor
    }
}

fn module_descriptor() -> ModuleDescriptor {
    ModuleDescriptor {
        name: "Users",
        local_name: "users",
        description: None,
        fields: Vec::new(),
        functions: vec![ModuleFunctionDescriptor {
            path: vec!["list"],
            description: None,
            params: Vec::new(),
            returns: vec![Vec::<PublicUser>::luau_type()],
            yields: true,
        }],
    }
}

pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    render_definition_file_with_support(
        &module_descriptor(),
        &[],
        &[PublicUser::interface_descriptor()],
        &[],
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_users_module_definition() {
        let rendered = render_luau_definition().expect("render lyra/users docs");

        assert!(rendered.contains("@interface PublicUser"));
        assert!(rendered.contains("user_id: number"));
        assert!(rendered.contains("username: string"));
        assert!(rendered.contains("role: string?"));
        assert!(rendered.contains("@class Users"));
        assert!(rendered.contains("function users.list(): {PublicUser}"));
    }
}
