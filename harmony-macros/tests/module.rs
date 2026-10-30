// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::sync::Arc;

use harmony_core::{
    CallContext,
    ChunkOrigin,
    LocalScheduler,
    ModuleSpec,
    TokioRuntimeContext,
};
use harmony_luau::{
    self as luau,
    DescribeModule,
    LuauType,
    ModuleDescriptor,
    ModuleFieldDescriptor,
};

/// The caller's greeting, which the host puts in the call context.
struct Greeting(String);

impl luau::FromCallFrame for Greeting {
    fn from_call_frame(_vm: &luau::Vm, context: &luau::CallContext) -> luau::runtime::Result<Self> {
        Ok(Self(context.caller.get::<String>()?.as_ref().clone()))
    }
}

struct DemoModule;

fn install_greeting_count(spec: ModuleSpec) -> ModuleSpec {
    spec.initializer(|_vm, _origin, table| table.set("greeting_count", 3_i64))
}

fn describe_greeting_count(descriptor: &mut ModuleDescriptor) {
    descriptor.fields.push(ModuleFieldDescriptor {
        path: vec!["greeting_count"],
        description: None,
        ty: LuauType::literal("number"),
    });
}

#[harmony_macros::module(
    id = "demo/greetings",
    description = "Greets people.",
    userdata(Mood),
    extend = install_greeting_count,
    describe = describe_greeting_count
)]
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

    fn ignore(_unused: i64) -> bool {
        true
    }

    fn borrowed_plugin(
        vm: &luau::Vm,
        context: &CallContext,
        origin: &ChunkOrigin,
    ) -> luau::runtime::Result<Option<String>> {
        vm.create_table()?;
        assert_eq!(origin, &context.origin);
        Ok(origin.plugin.as_deref().map(str::to_string))
    }

    async fn owned_plugin(
        #[context] vm: luau::Vm,
        #[context] context: CallContext,
        #[context] origin: ChunkOrigin,
    ) -> luau::runtime::Result<Option<String>> {
        vm.create_table()?;
        assert_eq!(origin, context.origin);
        Ok(origin.plugin.as_deref().map(str::to_string))
    }
}

#[harmony_macros::userdata]
#[derive(Clone, Copy, PartialEq, Eq)]
enum Mood {
    Happy,
}

fn run(source: &str, context: CallContext) -> luau::runtime::Result<Vec<luau::Value>> {
    let vm = luau::Vm::new()?;
    vm.open_standard_libraries(luau::StandardLibraries::all_supported())?;
    vm.data().insert(LocalScheduler::new())?;
    let scheduler = vm.data().get::<LocalScheduler>()?;
    let table =
        harmony_core::luau::install_module(&vm, &context.origin, &DemoModule::module_spec())?;
    vm.set_global_table("demo", &table)?;
    let root = vm.load_chunk(&luau::Chunk::new(
        Arc::<[u8]>::from(format!("return pcall(function() {source} end)").as_bytes()),
        context.origin.clone(),
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
fn modules_extend_their_spec_and_descriptor() -> luau::runtime::Result<()> {
    assert_eq!(
        run("return demo.greeting_count", CallContext::default())?,
        vec![luau::Value::Boolean(true), luau::Value::Number(3.0)]
    );
    assert_eq!(
        DemoModule::module_descriptor()
            .fields
            .iter()
            .map(|field| field.path.clone())
            .collect::<Vec<_>>(),
        vec![vec!["greeting_count"]]
    );
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
        functions[..3],
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

#[test]
fn arguments_take_their_names_without_a_leading_underscore() -> luau::runtime::Result<()> {
    let failed = run("return demo.ignore('x')", CallContext::default())?;
    assert_eq!(failed[0], luau::Value::Boolean(false));
    let luau::Value::String(message) = &failed[1] else {
        panic!("expected an error message, got {:?}", failed[1]);
    };
    assert!(String::from_utf8_lossy(message).contains("argument `unused` expected number"));

    let descriptor = DemoModule::module_descriptor();
    let ignore = descriptor
        .functions
        .iter()
        .find(|function| function.path == ["ignore"])
        .expect("ignore is documented");
    assert_eq!(ignore.params[0].name, "unused");
    Ok(())
}

#[test]
fn functions_borrow_or_own_the_vm_call_context_and_origin() -> luau::runtime::Result<()> {
    let mut context = CallContext::default();
    context.origin.plugin = Some("demo-plugin".into());
    for function in ["borrowed_plugin", "owned_plugin"] {
        assert_eq!(
            run(&format!("return demo.{function}()"), context.clone())?,
            vec![luau::Value::Boolean(true), string("demo-plugin")]
        );
    }
    Ok(())
}

#[test]
fn modules_register_their_userdata() -> luau::runtime::Result<()> {
    assert_eq!(
        run(
            "return demo.Mood.Happy == demo.Mood.Happy",
            CallContext::default()
        )?,
        vec![luau::Value::Boolean(true), luau::Value::Boolean(true)]
    );
    Ok(())
}

#[harmony_macros::userdata(name = "Counter")]
#[derive(Clone)]
struct Counter;

#[harmony_macros::userdata_methods]
impl Counter {
    fn add(&self, amount: i64, #[context] _greeting: Greeting) -> i64 {
        amount
    }

    fn reset(&self) {}
}

#[test]
fn userdata_specs_list_their_methods_and_arguments() {
    let spec = Counter::_harmony_userdata_spec();
    let methods: Vec<(&str, Vec<&str>)> = spec
        .methods
        .iter()
        .map(|method| {
            (
                &*method.name,
                method.arg_names.iter().map(|name| &**name).collect(),
            )
        })
        .collect();
    assert_eq!(&*spec.name, "Counter");
    assert_eq!(methods, [("add", vec!["amount"]), ("reset", vec![])]);
    assert!(spec.methods.iter().all(|method| method.callback.is_none()));
    Counter.reset();
    assert_eq!(Counter.add(2, Greeting(String::new())), 2);
}
