// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::fmt;
use std::future::Future;
use std::sync::Arc;

use anyhow::{
    Result,
    bail,
};
use harmony_luau::{
    ClassDescriptor,
    DescribeInterface,
    DescribeTypeAlias,
    DescribeUserData,
    InterfaceDescriptor,
    ModuleDefinition,
    ModuleFunctionDescriptor,
    TypeAliasDescriptor,
};
use mlua::{
    FromLuaMulti,
    IntoLua,
    IntoLuaMulti,
    Lua,
    Table,
    Value,
};
use mlua_scheduler::MaybeSync;

use crate::{
    Danger,
    Module,
    ModuleContext,
    ModuleSetup,
    Scope,
    r#async::LuaAsyncExt,
    resolve_caller,
};

type ModuleInstall = dyn Fn(&Lua, &Table) -> Result<()> + Send + Sync + 'static;

#[derive(Clone)]
pub struct ModuleExport {
    module: Module,
    definition: ModuleDefinition,
}

impl ModuleExport {
    pub fn module(&self) -> &Module {
        &self.module
    }

    pub fn definition(&self) -> &ModuleDefinition {
        &self.definition
    }

    pub fn render_luau_definition(&self) -> Result<String, fmt::Error> {
        self.definition.render_definition_file()
    }

    pub fn into_module(self) -> Module {
        self.module
    }
}

impl From<ModuleExport> for Module {
    fn from(export: ModuleExport) -> Self {
        export.into_module()
    }
}

pub struct ModuleBuilder {
    path: Arc<str>,
    setup: Arc<ModuleSetup>,
    installs: Vec<Arc<ModuleInstall>>,
    scope: Scope,
    definition: ModuleDefinition,
}

impl ModuleBuilder {
    pub fn new<F>(
        path: impl Into<Arc<str>>,
        name: &'static str,
        local_name: &'static str,
        setup: F,
    ) -> Self
    where
        F: Fn(&Lua) -> Result<Table> + Send + Sync + 'static,
    {
        let path = path.into();
        Self {
            scope: Scope::generated(path.as_ref()),
            definition: ModuleDefinition::new(name, local_name, None),
            setup: Arc::new(setup),
            installs: Vec::new(),
            path,
        }
    }

    pub fn empty(path: impl Into<Arc<str>>, name: &'static str, local_name: &'static str) -> Self {
        Self::new(path, name, local_name, |lua| Ok(lua.create_table()?))
    }

    pub fn description(mut self, description: &'static str) -> Self {
        self.definition = self.definition.description(description);
        self
    }

    pub fn scope(mut self, scope: Scope) -> Self {
        self.scope = scope;
        self
    }

    pub fn scope_id(
        self,
        id: impl Into<Arc<str>>,
        description: &'static str,
        danger: Danger,
    ) -> Self {
        self.scope(Scope::new(id, description, danger))
    }

    pub fn function(mut self, function: ModuleFunctionDescriptor) -> Self {
        self.definition = self.definition.function(function);
        self
    }

    pub fn sync_function<A, F, R>(mut self, function: ModuleFunctionDescriptor, callback: F) -> Self
    where
        A: FromLuaMulti + 'static,
        F: Fn(&Lua, A) -> mlua::Result<R> + Send + Sync + 'static,
        R: IntoLuaMulti + 'static,
    {
        let path = function.path.clone();
        let callback = Arc::new(callback);
        self.installs.push(Arc::new(move |lua, table| {
            let callback = Arc::clone(&callback);
            let function = lua.create_function(move |lua, args| callback(lua, args))?;
            install_at_path(lua, table, &path, function)
        }));
        self.function(function)
    }

    pub fn async_function<A, F, R, FR>(
        mut self,
        function: ModuleFunctionDescriptor,
        callback: F,
    ) -> Self
    where
        A: FromLuaMulti + mlua::MaybeSend + MaybeSync + 'static,
        F: Fn(Lua, A) -> FR + Send + Sync + 'static,
        R: IntoLuaMulti + mlua::MaybeSend + MaybeSync + 'static,
        FR: Future<Output = mlua::Result<R>> + mlua::MaybeSend + MaybeSync + 'static,
    {
        let path = function.path.clone();
        let callback = Arc::new(callback);
        self.installs.push(Arc::new(move |lua, table| {
            let callback = Arc::clone(&callback);
            let function = lua.create_async_function(move |lua, args| {
                let callback = Arc::clone(&callback);
                callback(lua, args)
            })?;
            install_at_path(lua, table, &path, function)
        }));
        self.function(function)
    }

    pub fn async_function_with_prelude<P, PFn, A, F, R, FR>(
        mut self,
        function: ModuleFunctionDescriptor,
        prelude: PFn,
        callback: F,
    ) -> Self
    where
        P: mlua::MaybeSend + MaybeSync + 'static,
        PFn: Fn(&Lua) -> P + Send + Sync + 'static,
        A: FromLuaMulti + mlua::MaybeSend + MaybeSync + 'static,
        F: Fn(Lua, P, A) -> FR + Send + Sync + 'static,
        R: IntoLuaMulti + mlua::MaybeSend + MaybeSync + 'static,
        FR: Future<Output = mlua::Result<R>> + mlua::MaybeSend + MaybeSync + 'static,
    {
        let path = function.path.clone();
        let prelude = Arc::new(prelude);
        let callback = Arc::new(callback);
        self.installs.push(Arc::new(move |lua, table| {
            let prelude = Arc::clone(&prelude);
            let callback = Arc::clone(&callback);
            let function = lua.create_async_function_with_prelude(
                move |lua| prelude(lua),
                move |lua, prelude_result, args| {
                    let callback = Arc::clone(&callback);
                    callback(lua, prelude_result, args)
                },
            )?;
            install_at_path(lua, table, &path, function)
        }));
        self.function(function)
    }

    pub fn sync_caller_function<A, F, R>(
        mut self,
        function: ModuleFunctionDescriptor,
        callback: F,
    ) -> Self
    where
        A: FromLuaMulti + 'static,
        F: Fn(&Lua, Option<Arc<str>>, A) -> mlua::Result<R> + Send + Sync + 'static,
        R: IntoLuaMulti + 'static,
    {
        let path = function.path.clone();
        let callback = Arc::new(callback);
        self.installs.push(Arc::new(move |lua, table| {
            let callback = Arc::clone(&callback);
            let function = lua.create_function(move |lua, args| {
                let plugin_id = resolve_caller(lua);
                callback(lua, plugin_id, args)
            })?;
            install_at_path(lua, table, &path, function)
        }));
        self.function(function)
    }

    pub fn sync_context_function<C, A, F, R>(
        mut self,
        function: ModuleFunctionDescriptor,
        callback: F,
    ) -> Self
    where
        C: ModuleContext + 'static,
        A: FromLuaMulti + 'static,
        F: Fn(&Lua, C, A) -> mlua::Result<R> + Send + Sync + 'static,
        R: IntoLuaMulti + 'static,
    {
        let path = function.path.clone();
        let callback = Arc::new(callback);
        self.installs.push(Arc::new(move |lua, table| {
            let callback = Arc::clone(&callback);
            let function = lua.create_function(move |lua, args| {
                let plugin_id = resolve_caller(lua);
                let context = C::from_lua_plugin_id(lua, plugin_id)?;
                callback(lua, context, args)
            })?;
            install_at_path(lua, table, &path, function)
        }));
        self.function(function)
    }

    pub fn async_caller_function<A, F, R, FR>(
        self,
        function: ModuleFunctionDescriptor,
        callback: F,
    ) -> Self
    where
        A: FromLuaMulti + mlua::MaybeSend + MaybeSync + 'static,
        F: Fn(Lua, Option<Arc<str>>, A) -> FR + Send + Sync + 'static,
        R: IntoLuaMulti + mlua::MaybeSend + MaybeSync + 'static,
        FR: Future<Output = mlua::Result<R>> + mlua::MaybeSend + MaybeSync + 'static,
    {
        self.async_function_with_prelude(function, resolve_caller, callback)
    }

    pub fn async_context_function<C, A, F, R, FR>(
        mut self,
        function: ModuleFunctionDescriptor,
        callback: F,
    ) -> Self
    where
        C: ModuleContext + mlua::MaybeSend + MaybeSync + 'static,
        A: FromLuaMulti + mlua::MaybeSend + MaybeSync + 'static,
        F: Fn(Lua, C, A) -> FR + Send + Sync + 'static,
        R: IntoLuaMulti + mlua::MaybeSend + MaybeSync + 'static,
        FR: Future<Output = mlua::Result<R>> + mlua::MaybeSend + MaybeSync + 'static,
    {
        let path = function.path.clone();
        let callback = Arc::new(callback);
        self.installs.push(Arc::new(move |lua, table| {
            let callback = Arc::clone(&callback);
            let function = lua.create_async_function_with_prelude(
                resolve_caller,
                move |lua, plugin_id, args| {
                    let callback = Arc::clone(&callback);
                    async move {
                        let context = C::from_lua_plugin_id(&lua, plugin_id)?;
                        callback(lua, context, args).await
                    }
                },
            )?;
            install_at_path(lua, table, &path, function)
        }));
        self.function(function)
    }

    pub fn with_alias(mut self, alias: TypeAliasDescriptor) -> Self {
        self.definition = self.definition.with_alias(alias);
        self
    }

    pub fn type_alias<T>(self) -> Self
    where
        T: DescribeTypeAlias,
    {
        self.with_alias(T::type_alias_descriptor())
    }

    pub fn with_interface(mut self, interface: InterfaceDescriptor) -> Self {
        self.definition = self.definition.with_interface(interface);
        self
    }

    pub fn interface<T>(self) -> Self
    where
        T: DescribeInterface,
    {
        self.with_interface(T::interface_descriptor())
    }

    pub fn with_class(mut self, class: ClassDescriptor) -> Self {
        self.definition = self.definition.with_class(class);
        self
    }

    pub fn class<T>(self) -> Self
    where
        T: DescribeUserData,
    {
        self.with_class(T::class_descriptor())
    }

    pub fn build(self) -> ModuleExport {
        let setup = self.setup;
        let installs = self.installs;
        let setup = Arc::new(move |lua: &Lua| -> Result<Table> {
            let table = setup(lua)?;
            for install in &installs {
                install(lua, &table)?;
            }
            Ok(table)
        });

        ModuleExport {
            module: Module {
                path: self.path,
                setup,
                scope: self.scope,
            },
            definition: self.definition,
        }
    }
}

fn install_at_path(
    lua: &Lua,
    root: &Table,
    path: &[&'static str],
    value: impl IntoLua,
) -> Result<()> {
    let Some((leaf, parents)) = path.split_last() else {
        bail!("module function path must contain at least one segment");
    };

    let mut table = root.clone();
    for segment in parents {
        match table.get::<Value>(*segment)? {
            Value::Nil => {
                let child = lua.create_table()?;
                table.set(*segment, child.clone())?;
                table = child;
            }
            Value::Table(child) => {
                table = child;
            }
            _ => bail!("module path segment '{segment}' already exists and is not a table"),
        }
    }

    table.set(*leaf, value)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use harmony_luau::{
        FieldDescriptor,
        JsonValue,
        LuauType,
        ModuleFunctionDescriptor,
        ParameterDescriptor,
    };

    use super::*;

    #[derive(Clone)]
    struct TestContext(Option<Arc<str>>);

    impl ModuleContext for TestContext {
        fn from_lua_plugin_id(_lua: &Lua, plugin_id: Option<Arc<str>>) -> mlua::Result<Self> {
            Ok(Self(plugin_id))
        }
    }

    #[test]
    fn builder_produces_module_and_luau_definition() {
        let export = ModuleBuilder::empty("harmony/demo", "Demo", "demo")
            .description("Demo module.")
            .scope_id("harmony.demo", "Demo capability.", Danger::Low)
            .type_alias::<JsonValue>()
            .with_interface(
                InterfaceDescriptor::new("DemoOptions", Some("Demo options."))
                    .field(FieldDescriptor::of::<String>("name")),
            )
            .sync_function(
                ModuleFunctionDescriptor::new("util.encode")
                    .description("Encodes a value.")
                    .param(ParameterDescriptor::new(
                        "value",
                        LuauType::literal("JsonValue"),
                    ))
                    .returns::<String>()
                    .yields(),
                |_, value: String| Ok(value),
            )
            .build();

        let module = export.module();
        assert_eq!(module.path.as_ref(), "harmony/demo");
        assert_eq!(module.scope.id.as_ref(), "harmony.demo");
        assert_eq!(module.scope.description, "Demo capability.");
        assert_eq!(module.scope.danger, Danger::Low);

        let rendered = export
            .render_luau_definition()
            .expect("render luau definition");
        assert!(rendered.contains("@class Demo"));
        assert!(rendered.contains("@type JsonValue"));
        assert!(rendered.contains("export type DemoOptions = {"));
        assert!(rendered.contains("demo.util = {}"));
        assert!(rendered.contains("function demo.util.encode(value: JsonValue): string"));
        assert!(rendered.contains("@yields"));

        let lua = Lua::new();
        let table = (module.setup)(&lua).expect("install module");
        let util = table.get::<Table>("util").expect("get util table");
        let encode = util.get::<mlua::Function>("encode").expect("get function");
        let value = encode.call::<String>("ok").expect("call function");
        assert_eq!(value, "ok");
    }

    #[test]
    fn builder_defaults_scope_from_module_path() {
        let export =
            ModuleBuilder::new(
                "harmony/demo",
                "Demo",
                "demo",
                |lua| Ok(lua.create_table()?),
            )
            .build();

        assert_eq!(export.module().scope.id.as_ref(), "harmony.demo");
        assert_eq!(export.module().scope.description, "");
        assert_eq!(export.module().scope.danger, Danger::Negligible);
    }

    #[test]
    fn builder_installs_async_function() {
        let export = ModuleBuilder::empty("harmony/demo", "Demo", "demo")
            .async_function(
                ModuleFunctionDescriptor::new("echo")
                    .description("Echoes a value.")
                    .param_type::<String>("value")
                    .returns::<String>()
                    .yields(),
                |_, value: String| async move { Ok(value) },
            )
            .build();

        let rendered = export
            .render_luau_definition()
            .expect("render luau definition");
        assert!(rendered.contains("@yields"));
        assert!(rendered.contains("function demo.echo(value: string): string"));

        let lua = Lua::new();
        let table = (export.module().setup)(&lua).expect("install module");
        let _: mlua::Function = table.get("echo").expect("get async function");
    }

    #[test]
    fn builder_installs_async_function_with_prelude() {
        let export = ModuleBuilder::empty("harmony/demo", "Demo", "demo")
            .async_function_with_prelude(
                ModuleFunctionDescriptor::new("scoped_echo")
                    .description("Echoes a scoped value.")
                    .param_type::<String>("value")
                    .returns::<String>()
                    .yields(),
                |_| "scope".to_string(),
                |_, scope, value: String| async move { Ok(format!("{scope}:{value}")) },
            )
            .build();

        let lua = Lua::new();
        let table = (export.module().setup)(&lua).expect("install module");
        let _: mlua::Function = table.get("scoped_echo").expect("get async function");
    }

    #[test]
    fn builder_installs_sync_context_function() {
        let export = ModuleBuilder::empty("harmony/demo", "Demo", "demo")
            .sync_context_function::<TestContext, String, _, _>(
                ModuleFunctionDescriptor::new("context_echo")
                    .description("Echoes a context value.")
                    .param_type::<String>("value")
                    .returns::<String>(),
                |_, context, value| {
                    let caller = context.0.as_deref().unwrap_or("anonymous");
                    Ok(format!("{caller}:{value}"))
                },
            )
            .build();

        let lua = Lua::new();
        let table = (export.module().setup)(&lua).expect("install module");
        let echo = table
            .get::<mlua::Function>("context_echo")
            .expect("get function");
        let value = echo.call::<String>("ok").expect("call function");
        assert_eq!(value, "anonymous:ok");
    }

    #[test]
    fn builder_installs_async_context_function() {
        let export = ModuleBuilder::empty("harmony/demo", "Demo", "demo")
            .async_context_function::<TestContext, String, _, String, _>(
                ModuleFunctionDescriptor::new("context_echo")
                    .description("Echoes a context value.")
                    .param_type::<String>("value")
                    .returns::<String>()
                    .yields(),
                |_, context, value| async move {
                    let caller = context.0.as_deref().unwrap_or("anonymous");
                    Ok(format!("{caller}:{value}"))
                },
            )
            .build();

        let lua = Lua::new();
        let table = (export.module().setup)(&lua).expect("install module");
        let _: mlua::Function = table.get("context_echo").expect("get async function");
    }
}
