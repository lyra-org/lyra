// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::sync::Arc;

use harmony_core::{
    CallContext,
    ChunkOrigin,
    LocalScheduler,
    TokioRuntimeContext,
};
use harmony_luau::{
    self as luau,
    DescribeModule,
    LuauType,
};

/// The caller's greeting, which the host puts in the call context.
struct Greeting(String);

impl luau::FromCallFrame for Greeting {
    fn from_call_frame(_vm: &luau::Vm, context: &luau::CallContext) -> luau::runtime::Result<Self> {
        Ok(Self(context.caller.get::<String>()?.as_ref().clone()))
    }
}

struct DemoModule;

#[harmony_macros::module(id = "demo/greetings", description = "Greets people.")]
impl DemoModule {
    #[harmony(description = "Adds one.")]
    fn add_one(value: i64) -> i64 {
        value + 1
    }

    #[harmony(name = "text.shout")]
    fn shout(text: String) -> luau::runtime::Result<String> {
        if text.is_empty() {
            return Err(luau::Error::Runtime("nothing to shout".to_string()));
        }
        Ok(text.to_uppercase())
    }

    async fn greet(#[context] greeting: Greeting, name: Option<String>) -> String {
        format!("{}, {}", greeting.0, name.as_deref().unwrap_or("stranger"))
    }
}

fn run(source: &str, context: CallContext) -> luau::runtime::Result<Vec<luau::Value>> {
    let vm = luau::Vm::new()?;
    vm.open_standard_libraries(luau::StandardLibraries::all_supported())?;
    vm.data().insert(LocalScheduler::new())?;
    let scheduler = vm.data().get::<LocalScheduler>()?;
    let table = harmony_core::luau::install_module(
        &vm,
        &ChunkOrigin::default(),
        &DemoModule::module_spec(),
    )?;
    vm.set_global_table("demo", &table)?;
    let root = vm.load_chunk(&luau::Chunk::new(
        Arc::<[u8]>::from(format!("return pcall(function() {source} end)").as_bytes()),
        luau::ChunkOrigin::default(),
    ))?;
    let thread = vm.create_thread(&root)?;
    scheduler.spawn_luau_thread(context, vm.clone(), thread.clone(), vec![]);
    let tokio = TokioRuntimeContext::new().expect("tokio runtime");
    Ok(
        harmony_core::luau::drive_thread(&tokio, &scheduler, &thread, Default::default())
            .expect("the chunk finished"),
    )
}

fn string(value: &str) -> luau::Value {
    luau::Value::String(value.as_bytes().to_vec())
}

#[test]
fn module_functions_read_their_arguments_by_name() -> luau::runtime::Result<()> {
    assert_eq!(
        run("return demo.add_one(41)", CallContext::default())?,
        vec![luau::Value::Boolean(true), luau::Value::Number(42.0)]
    );
    assert_eq!(
        run("return demo.text.shout('hi')", CallContext::default())?,
        vec![luau::Value::Boolean(true), string("HI")]
    );

    let failed = run("return demo.add_one('x')", CallContext::default())?;
    assert_eq!(failed[0], luau::Value::Boolean(false));
    let luau::Value::String(message) = &failed[1] else {
        panic!("expected an error message, got {:?}", failed[1]);
    };
    assert!(String::from_utf8_lossy(message).contains("argument `value` expected number"));

    let failed = run("return demo.text.shout('')", CallContext::default())?;
    assert_eq!(failed[0], luau::Value::Boolean(false));
    Ok(())
}

#[test]
fn async_module_functions_take_context_from_the_call() -> luau::runtime::Result<()> {
    let mut context = CallContext::default();
    context.caller.insert("Hello".to_string());
    assert_eq!(
        run("return demo.greet('Ada')", context.clone())?,
        vec![luau::Value::Boolean(true), string("Hello, Ada")]
    );
    assert_eq!(
        run("return demo.greet()", context)?,
        vec![luau::Value::Boolean(true), string("Hello, stranger")]
    );

    let failed = run("return demo.greet('Ada')", CallContext::default())?;
    assert_eq!(failed[0], luau::Value::Boolean(false));
    Ok(())
}

#[test]
fn module_descriptor_documents_functions_from_their_signatures() {
    let descriptor = DemoModule::module_descriptor();
    assert_eq!(descriptor.name, "Demo");
    assert_eq!(descriptor.local_name, "greetings");
    assert_eq!(descriptor.description, Some("Greets people."));

    let functions = descriptor
        .functions
        .iter()
        .map(|function| {
            (
                function.path.join("."),
                function
                    .params
                    .iter()
                    .map(|param| (param.name, param.ty.clone()))
                    .collect::<Vec<_>>(),
                function.returns.clone(),
                function.yields,
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        functions,
        vec![
            (
                "add_one".to_string(),
                vec![("value", LuauType::literal("number"))],
                vec![LuauType::literal("number")],
                false,
            ),
            (
                "text.shout".to_string(),
                vec![("text", LuauType::literal("string"))],
                vec![LuauType::literal("string")],
                false,
            ),
            (
                "greet".to_string(),
                vec![("name", LuauType::optional(LuauType::literal("string")))],
                vec![LuauType::literal("string")],
                true,
            ),
        ]
    );
    assert_eq!(descriptor.functions[0].description, Some("Adds one."));
}
