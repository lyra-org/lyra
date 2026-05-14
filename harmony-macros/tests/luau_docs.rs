// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

#![allow(dead_code)]

use harmony_luau::{
    DescribeInterface,
    DescribeTypeAlias,
    LuauType,
    TypeAliasDescriptor,
};

#[derive(Clone, Default)]
#[harmony_macros::structure]
/// Player object exposed to Lua.
struct Player {
    /// Display name.
    name: String,
    /// Optional score.
    score: Option<u32>,
    /// Attached tags.
    tags: Vec<String>,
}

#[harmony_macros::implementation]
impl Player {
    /// Creates a new player.
    pub fn new(name: String) -> Self {
        Self {
            name,
            score: None,
            tags: Vec::new(),
        }
    }

    /// Renames the player.
    pub fn rename(&mut self, name: String) {
        self.name = name;
    }

    /// Sets a structured status value.
    #[harmony(args(status: DemoStatus))]
    pub fn set_status(&mut self, status: String) {
        self.name = status;
    }

    /// Returns the structured status value.
    #[harmony(returns(DemoStatus))]
    pub fn status(&self) -> String {
        self.name.clone()
    }

    #[harmony(skip)]
    pub fn internal_only(&self) -> bool {
        !self.tags.is_empty()
    }
}

harmony_macros::compile!(type_path = Player, fields = true, methods = true);

#[harmony_macros::interface]
/// Extra request metadata.
struct RequestMeta {
    /// Optional trace identifier.
    trace_id: Option<String>,
}

struct DemoStatus;

impl harmony_luau::LuauTypeInfo for DemoStatus {
    fn luau_type() -> LuauType {
        LuauType::literal("DemoStatus")
    }
}

impl DescribeTypeAlias for DemoStatus {
    fn type_alias_descriptor() -> TypeAliasDescriptor {
        TypeAliasDescriptor::new(
            "DemoStatus",
            LuauType::union(vec![
                LuauType::literal("\"ok\""),
                LuauType::literal("\"error\""),
            ]),
            Some("Demo status values."),
        )
    }
}

struct DemoModule;

#[harmony_macros::module(
    name = "Demo",
    local = "demo",
    aliases(DemoStatus),
    interfaces(RequestMeta),
    classes(Player)
)]
impl DemoModule {
    /// Makes a player from a raw name.
    pub fn make_player(name: String) -> Player {
        Player::new(name)
    }

    #[harmony(path = "util.encode")]
    /// Encodes a value for transport.
    pub async fn encode(value: String) -> String {
        value
    }

    #[harmony(skip)]
    pub fn internal() -> bool {
        false
    }
}

#[test]
fn renders_luau_definition_output() {
    let rendered = DemoModule::render_luau_definition().expect("render luau definition file");

    assert!(rendered.contains("@class Demo"));
    assert!(rendered.contains("@type DemoStatus"));
    assert!(rendered.contains("export type RequestMeta = {"));
    assert!(rendered.contains("local demo = {}"));
    assert!(rendered.contains("demo.util = {}"));
    assert!(rendered.contains("function demo.util.encode(value: string): string"));
    assert!(rendered.contains("@interface Player"));
    assert!(rendered.contains("score: number?"));
    assert!(rendered.contains("tags: {string}"));
    assert!(rendered.contains("rename: (self: Player, name: string) -> ()"));
    assert!(rendered.contains("set_status: (self: Player, status: DemoStatus) -> ()"));
    assert!(rendered.contains("status: (self: Player) -> DemoStatus"));
    assert!(!rendered.contains("function Player.rename(self: Player, name: string)"));
    assert!(rendered.contains("function Player.new(name: string): Player"));
    assert!(rendered.contains("@yields"));
    assert!(!rendered.contains("internal_only"));
    assert!(!rendered.contains("function demo.internal("));
}

#[test]
fn interface_macro_emits_interface_descriptor() {
    let descriptor = <RequestMeta as DescribeInterface>::interface_descriptor();

    assert_eq!(descriptor.name, "RequestMeta");
    assert_eq!(descriptor.fields.len(), 1);
    assert_eq!(descriptor.fields[0].name, "trace_id");
    assert_eq!(descriptor.fields[0].ty.to_string(), "string?");
}
