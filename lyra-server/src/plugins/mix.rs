// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use crate::plugins::lifecycle::{
    PluginFunctionHandle,
    PluginId,
};
use std::sync::Arc;

use harmony_core::{
    LuaUserDataAsyncExt,
    Module,
};
use harmony_luau::{
    ClassDescriptor,
    DescribeInterface,
    DescribeModule,
    LuauType,
    LuauTypeInfo,
    ModuleDescriptor,
    ModuleFunctionDescriptor,
    ParameterDescriptor,
};
use mlua::{
    Function,
    Lua,
    Result,
    Table,
};
use nanoid::nanoid;

use crate::STATE;
use crate::db::{
    self,
    mixers::MixerConfig,
};
use crate::services::mix::{
    MIX_REGISTRY,
    MixSeedType,
};
use crate::services::options::{
    OptionDeclaration,
    OptionType,
};

use super::OptionConfig;

struct MixHandler;

impl LuauTypeInfo for MixHandler {
    fn luau_type() -> LuauType {
        LuauType::function(
            vec![harmony_luau::FunctionParameter {
                name: Some("ctx"),
                ty: LuauType::literal("MixContext"),
                variadic: false,
            }],
            vec![LuauType::literal("MixResult")],
        )
    }
}

struct MixRecentListensHandler;

impl LuauTypeInfo for MixRecentListensHandler {
    fn luau_type() -> LuauType {
        LuauType::function(
            vec![harmony_luau::FunctionParameter {
                name: Some("ctx"),
                ty: LuauType::literal("MixRecentListensContext"),
                variadic: false,
            }],
            vec![LuauType::literal("MixResult")],
        )
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Mixer {
    plugin_id: PluginId,
    id: String,
}

impl Mixer {
    async fn wrap_handler(&self, func: Function) -> mlua::Result<PluginFunctionHandle> {
        let counter = STATE
            .plugin_registries
            .inflight_counter(&self.plugin_id)
            .await;
        Ok(PluginFunctionHandle::new(
            self.plugin_id.clone(),
            counter,
            func,
        ))
    }

    /// Reject cross-plugin access. All plugins share one Lua, so a
    /// Mixer userdata stashed in `_G` can be picked up by another
    /// plugin; without this check, handlers would register under the
    /// owner's counter and bucket and get wiped on the owner's
    /// teardown.
    fn ensure_owner(&self, caller: Option<&PluginId>) -> mlua::Result<()> {
        match caller {
            Some(id) if id == &self.plugin_id => Ok(()),
            _ => Err(mlua::Error::runtime(format!(
                "mixer '{}' method must be called by owning plugin '{}'",
                self.id, self.plugin_id
            ))),
        }
    }
}

#[harmony_macros::implementation(plugin_scoped)]
impl Mixer {
    pub async fn new(plugin_id: Option<Arc<str>>, id: String) -> Result<Self> {
        let plugin_id = plugin_id
            .map(|raw| PluginId::new(raw).map_err(mlua::Error::external))
            .transpose()?;
        let plugin_id = plugin_id.ok_or_else(|| {
            mlua::Error::runtime("mix.Mixer.new must be called from plugin Lua code")
        })?;

        {
            let _registration = STATE
                .plugin_registries
                .ensure_registrations_open(&plugin_id)
                .await?;
            let mut registry = MIX_REGISTRY.write().await;
            registry
                .register(plugin_id.clone(), id.clone())
                .map_err(|err| mlua::Error::runtime(err.to_string()))?;
        }

        let db = &STATE.db;
        let mut db_write = db.write().await;

        if db::mixers::get_by_mixer_id(&db_write, &id)?.is_none() {
            let mixer_config = MixerConfig {
                db_id: None,
                id: nanoid!(),
                mixer_id: id.clone(),
                display_name: id.clone(),
                priority: 50,
                enabled: true,
            };
            db::mixers::upsert(&mut db_write, &mixer_config)?;
        }

        Ok(Mixer { plugin_id, id })
    }

    /// Registers a handler for generating a mix from a seed track.
    #[harmony(args(handler: MixHandler))]
    pub(crate) async fn from_track(
        &self,
        plugin_id: Option<Arc<str>>,
        handler: Function,
    ) -> Result<()> {
        let plugin_id = plugin_id
            .map(|raw| PluginId::new(raw).map_err(mlua::Error::external))
            .transpose()?;
        self.ensure_owner(plugin_id.as_ref())?;
        let _registration = STATE
            .plugin_registries
            .ensure_registrations_open(&self.plugin_id)
            .await?;
        let handle = self.wrap_handler(handler).await?;
        let mut registry = MIX_REGISTRY.write().await;
        registry.set_handler(&self.id, MixSeedType::Track, handle);
        Ok(())
    }

    /// Registers a handler for generating a mix from a seed release.
    #[harmony(args(handler: MixHandler))]
    pub(crate) async fn from_release(
        &self,
        plugin_id: Option<Arc<str>>,
        handler: Function,
    ) -> Result<()> {
        let plugin_id = plugin_id
            .map(|raw| PluginId::new(raw).map_err(mlua::Error::external))
            .transpose()?;
        self.ensure_owner(plugin_id.as_ref())?;
        let _registration = STATE
            .plugin_registries
            .ensure_registrations_open(&self.plugin_id)
            .await?;
        let handle = self.wrap_handler(handler).await?;
        let mut registry = MIX_REGISTRY.write().await;
        registry.set_handler(&self.id, MixSeedType::Release, handle);
        Ok(())
    }

    /// Registers a handler for generating a mix from a seed artist.
    #[harmony(args(handler: MixHandler))]
    pub(crate) async fn from_artist(
        &self,
        plugin_id: Option<Arc<str>>,
        handler: Function,
    ) -> Result<()> {
        let plugin_id = plugin_id
            .map(|raw| PluginId::new(raw).map_err(mlua::Error::external))
            .transpose()?;
        self.ensure_owner(plugin_id.as_ref())?;
        let _registration = STATE
            .plugin_registries
            .ensure_registrations_open(&self.plugin_id)
            .await?;
        let handle = self.wrap_handler(handler).await?;
        let mut registry = MIX_REGISTRY.write().await;
        registry.set_handler(&self.id, MixSeedType::Artist, handle);
        Ok(())
    }

    /// Registers a handler for generating a mix from a user's recent listens.
    #[harmony(args(handler: MixRecentListensHandler))]
    pub(crate) async fn from_recent_listens(
        &self,
        plugin_id: Option<Arc<str>>,
        handler: Function,
    ) -> Result<()> {
        let plugin_id = plugin_id
            .map(|raw| PluginId::new(raw).map_err(mlua::Error::external))
            .transpose()?;
        self.ensure_owner(plugin_id.as_ref())?;
        let _registration = STATE
            .plugin_registries
            .ensure_registrations_open(&self.plugin_id)
            .await?;
        let handle = self.wrap_handler(handler).await?;
        let mut registry = MIX_REGISTRY.write().await;
        registry.set_handler(&self.id, MixSeedType::RecentListens, handle);
        Ok(())
    }

    /// Declares an option that clients can toggle when requesting a mix.
    #[harmony(args(config: OptionConfig))]
    pub(crate) async fn declare_option(
        &self,
        plugin_id: Option<Arc<str>>,
        config: Table,
    ) -> Result<()> {
        let plugin_id = plugin_id
            .map(|raw| PluginId::new(raw).map_err(mlua::Error::external))
            .transpose()?;
        self.ensure_owner(plugin_id.as_ref())?;
        let _registration = STATE
            .plugin_registries
            .ensure_registrations_open(&self.plugin_id)
            .await?;
        let name: String = config
            .get("name")
            .ok()
            .filter(|s: &String| !s.trim().is_empty())
            .ok_or_else(|| mlua::Error::runtime("declare_option requires a non-empty 'name'"))?;
        let label: String = config
            .get("label")
            .ok()
            .filter(|s: &String| !s.trim().is_empty())
            .ok_or_else(|| mlua::Error::runtime("declare_option requires a non-empty 'label'"))?;
        let option_type_str: String = config
            .get("type")
            .ok()
            .filter(|s: &String| !s.trim().is_empty())
            .ok_or_else(|| mlua::Error::runtime("declare_option requires a non-empty 'type'"))?;
        let option_type = match option_type_str.as_str() {
            "boolean" => OptionType::Boolean,
            "string" => OptionType::String,
            "number" => OptionType::Number,
            other => {
                return Err(mlua::Error::runtime(format!(
                    "declare_option: unsupported type '{other}', expected 'boolean', 'string', or 'number'"
                )));
            }
        };
        let default: mlua::Value = config.get("default").unwrap_or(mlua::Value::Nil);
        let default_json = match &default {
            mlua::Value::Boolean(b) => serde_json::Value::Bool(*b),
            mlua::Value::Integer(n) => serde_json::json!(*n),
            mlua::Value::Number(n) => serde_json::json!(*n),
            mlua::Value::String(s) => serde_json::Value::String(s.to_str()?.to_string()),
            _ => serde_json::Value::Null,
        };
        let requires_settings: Vec<String> = config
            .get::<Vec<String>>("requires_settings")
            .unwrap_or_default();

        let mut registry = MIX_REGISTRY.write().await;
        registry
            .declare_option(
                &self.id,
                OptionDeclaration {
                    name,
                    label,
                    option_type,
                    default: default_json,
                    requires_settings,
                },
            )
            .map_err(mlua::Error::runtime)?;
        Ok(())
    }
}

harmony_macros::compile!(type_path = Mixer, fields = false, methods = true);

struct MixModule;

impl MixModule {
    fn support_aliases() -> Vec<harmony_luau::TypeAliasDescriptor> {
        vec![
            harmony_luau::TypeAliasDescriptor {
                name: "MixHandler",
                description: Some(
                    "A handler function that receives a context table and returns a result table with a `tracks` field. Returning an empty `tracks` array is a terminal success — no further mixers or the built-in algorithm will be tried. Raise an error to signal failure and allow fallthrough to the next mixer.",
                ),
                ty: MixHandler::luau_type(),
            },
            harmony_luau::TypeAliasDescriptor {
                name: "MixRecentListensHandler",
                description: Some(
                    "A handler function for recent-listens mixes. Context includes pre-resolved recent track IDs. Returning an empty `tracks` array is a terminal success — raise an error to allow fallthrough.",
                ),
                ty: MixRecentListensHandler::luau_type(),
            },
        ]
    }

    fn support_interfaces() -> Vec<harmony_luau::InterfaceDescriptor> {
        vec![
            OptionConfig::interface_descriptor(),
            harmony_luau::InterfaceDescriptor {
                name: "MixContext",
                description: Some("Context passed to a mix handler."),
                fields: vec![
                    harmony_luau::FieldDescriptor {
                        name: "seed_id",
                        ty: f64::luau_type(),
                        description: Some(
                            "The database ID of the seed entity (track, release, or artist).",
                        ),
                    },
                    harmony_luau::FieldDescriptor {
                        name: "limit",
                        ty: LuauType::Optional(Box::new(f64::luau_type())),
                        description: Some("Maximum number of tracks to return."),
                    },
                    harmony_luau::FieldDescriptor {
                        name: "user_id",
                        ty: LuauType::Optional(Box::new(f64::luau_type())),
                        description: Some(
                            "The database ID of the requesting user, if authenticated.",
                        ),
                    },
                    harmony_luau::FieldDescriptor {
                        name: "options",
                        ty: LuauType::Optional(Box::new(LuauType::literal(
                            "{ [string]: boolean | string | number }",
                        ))),
                        description: Some(
                            "Typed options declared by the mixer via declare_option, coerced from query parameters.",
                        ),
                    },
                ],
            },
            harmony_luau::InterfaceDescriptor {
                name: "MixRecentListensContext",
                description: Some("Context passed to a recent-listens mix handler."),
                fields: vec![
                    harmony_luau::FieldDescriptor {
                        name: "user_id",
                        ty: f64::luau_type(),
                        description: Some("The database ID of the requesting user."),
                    },
                    harmony_luau::FieldDescriptor {
                        name: "limit",
                        ty: LuauType::Optional(Box::new(f64::luau_type())),
                        description: Some("Maximum number of tracks to return."),
                    },
                    harmony_luau::FieldDescriptor {
                        name: "recent_track_ids",
                        ty: LuauType::literal("{ number }"),
                        description: Some("Pre-resolved database IDs of recently listened tracks."),
                    },
                    harmony_luau::FieldDescriptor {
                        name: "options",
                        ty: LuauType::Optional(Box::new(LuauType::literal(
                            "{ [string]: boolean | string | number }",
                        ))),
                        description: Some(
                            "Typed options declared by the mixer via declare_option, coerced from query parameters.",
                        ),
                    },
                ],
            },
            harmony_luau::InterfaceDescriptor {
                name: "MixResultTrack",
                description: Some("A track entry in a mix result."),
                fields: vec![harmony_luau::FieldDescriptor {
                    name: "track_id",
                    ty: f64::luau_type(),
                    description: Some("The database ID of the track."),
                }],
            },
            harmony_luau::InterfaceDescriptor {
                name: "MixResult",
                description: Some("The result returned by a mix handler."),
                fields: vec![harmony_luau::FieldDescriptor {
                    name: "tracks",
                    ty: LuauType::literal("{ MixResultTrack }"),
                    description: Some("Ordered list of tracks for the mix."),
                }],
            },
        ]
    }

    fn support_classes() -> Vec<ClassDescriptor> {
        use harmony_luau::DescribeUserData;
        let mut mixer = <Mixer as DescribeUserData>::class_descriptor();
        mixer.methods.retain(|method| {
            !(method.kind == harmony_luau::MethodKind::Static && method.name == "new")
        });
        vec![mixer]
    }

    fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
        harmony_luau::render_definition_file_with_support(
            &<Self as DescribeModule>::module_descriptor(),
            &Self::support_aliases(),
            &Self::support_interfaces(),
            &Self::support_classes(),
        )
    }
}

impl DescribeModule for MixModule {
    fn module_descriptor() -> ModuleDescriptor {
        let mut descriptor = ModuleDescriptor::new("Mix", "mix", None);
        descriptor.functions.extend(vec![ModuleFunctionDescriptor {
            path: vec!["Mixer", "new"],
            description: Some("Creates or loads a mixer."),
            params: vec![ParameterDescriptor {
                name: "id",
                ty: String::luau_type(),
                description: None,
                variadic: false,
            }],
            returns: vec![LuauType::literal("Mixer")],
            yields: true,
        }]);
        descriptor
    }
}

pub(crate) fn get_module() -> Module {
    Module {
        path: "lyra/mix".into(),
        setup: Arc::new(|lua: &Lua| -> anyhow::Result<mlua::Table> {
            let table = lua.create_table()?;
            table.set("Mixer", lua.create_proxy::<Mixer>()?)?;
            Ok(table)
        }),
        scope: harmony_core::Scope {
            id: "lyra.mix".into(),
            description: "Register a mix generator.",
            danger: harmony_core::Danger::Medium,
        },
    }
}

pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    MixModule::render_luau_definition()
}
