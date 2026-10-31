// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::sync::Arc;

use harmony_luau::{
    self as luau,
    DescribeModule,
    DescribeTypeAlias,
    LuauType,
    LuauTypeInfo,
    StringEnum,
};

#[harmony_macros::string_enum(description = "How a track is played.")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PlayMode {
    Shuffle,
    RepeatOne,
    #[harmony(rename = "xxh3_64")]
    Xxh364,
}

#[harmony_macros::string_enum(name = "Verb", rename_all = "UPPERCASE")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HttpVerb {
    Get,
    Delete,
}

struct EnumModule;

#[harmony_macros::module(id = "demo/enums")]
impl EnumModule {
    fn echo(mode: PlayMode) -> PlayMode {
        mode
    }

    fn verb(verb: Option<HttpVerb>) -> &'static str {
        verb.map_or("none", |verb| verb.as_str())
    }
}

fn run(source: &str) -> luau::runtime::Result<Vec<luau::Value>> {
    let vm = luau::Vm::new()?;
    vm.open_standard_libraries(luau::StandardLibraries::all_supported())?;
    let table = harmony_core::luau::install_module(
        &vm,
        &luau::ChunkOrigin::default(),
        &EnumModule::module_spec(),
    )?;
    vm.set_global_table("enums", &table)?;
    vm.eval(
        Arc::<[u8]>::from(format!("return pcall(function() {source} end)").as_bytes()),
        luau::ChunkOrigin::default(),
    )
}

fn string(value: &str) -> luau::Value {
    luau::Value::String(value.as_bytes().to_vec())
}

#[test]
fn variants_are_spelled_by_rule_or_rename() {
    assert_eq!(PlayMode::VALUES, ["shuffle", "repeat_one", "xxh3_64"]);
    assert_eq!(PlayMode::RepeatOne.as_str(), "repeat_one");
    assert_eq!(PlayMode::from_str("xxh3_64"), Some(PlayMode::Xxh364));
    assert_eq!(PlayMode::from_str("RepeatOne"), None);
    assert_eq!(HttpVerb::VALUES, ["GET", "DELETE"]);
}

#[test]
fn values_cross_both_ways_as_strings() -> luau::runtime::Result<()> {
    assert_eq!(
        run("return enums.echo('repeat_one')")?,
        vec![luau::Value::Boolean(true), string("repeat_one")]
    );
    assert_eq!(
        run("return enums.verb('DELETE')")?,
        vec![luau::Value::Boolean(true), string("DELETE")]
    );
    assert_eq!(
        run("return enums.verb(nil)")?,
        vec![luau::Value::Boolean(true), string("none")]
    );
    assert_eq!(luau::Value::from(PlayMode::Shuffle), string("shuffle"));
    Ok(())
}

#[test]
fn unknown_values_fail_with_the_accepted_ones() -> luau::runtime::Result<()> {
    let failed = run("return enums.echo('Shuffle')")?;
    assert_eq!(failed[0], luau::Value::Boolean(false));
    let luau::Value::String(message) = &failed[1] else {
        panic!("expected an error message, got {:?}", failed[1]);
    };
    assert!(
        String::from_utf8_lossy(message).contains(
            r#"argument `mode` expected one of "shuffle", "repeat_one", "xxh3_64", got "Shuffle""#
        ),
        "{}",
        String::from_utf8_lossy(message)
    );

    let failed = run("return enums.verb(1)")?;
    let luau::Value::String(message) = &failed[1] else {
        panic!("expected an error message, got {:?}", failed[1]);
    };
    assert!(
        String::from_utf8_lossy(message).contains("argument `verb` expected string, got number")
    );
    Ok(())
}

#[test]
fn enums_are_described_as_string_unions() {
    assert_eq!(PlayMode::luau_type(), LuauType::named("PlayMode"));
    assert_eq!(HttpVerb::luau_type(), LuauType::named("Verb"));

    let rendered = luau::render_definition_file_with_support(
        &EnumModule::module_descriptor(),
        &[
            PlayMode::type_alias_descriptor(),
            HttpVerb::type_alias_descriptor(),
        ],
        &[],
        &[],
    )
    .expect("render definition file");
    assert!(
        rendered.contains("export type PlayMode = \"shuffle\" | \"repeat_one\" | \"xxh3_64\"\n"),
        "{rendered}"
    );
    assert!(rendered.contains("How a track is played."), "{rendered}");
    assert!(
        rendered.contains("export type Verb = \"GET\" | \"DELETE\"\n"),
        "{rendered}"
    );
    assert!(
        rendered.contains("function enums.echo(mode: PlayMode): PlayMode"),
        "{rendered}"
    );
    assert!(
        rendered.contains("function enums.verb(verb: Verb?): string"),
        "{rendered}"
    );
}
