// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::sync::LazyLock;

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
    ParameterDescriptor,
    render_definition_file_with_support,
};
use serde::Serialize;

use crate::{
    STATE,
    plugins::db,
};

static HOSTNAME: LazyLock<String> =
    LazyLock::new(|| gethostname::gethostname().to_string_lossy().into_owned());

#[derive(Clone, Debug, Serialize)]
pub(crate) struct ServerInfo {
    pub(crate) id: String,
    pub(crate) version: String,
    pub(crate) commit_hash: String,
    pub(crate) hostname: String,
    pub(crate) port: u16,
    pub(crate) published_url: Option<String>,
    pub(crate) setup_complete: bool,
}

struct ServerModule;

pub(crate) fn module_spec() -> ModuleSpec {
    ModuleSpec::new("lyra/server")
        .capability("lyra.server")
        .function(server_info_spec())
        .install(|_| Ok(ModuleExport::new(ServerModule)))
}

fn server_info_spec() -> FunctionSpec {
    FunctionSpec::sync_fn("info")
        .returns::<ServerInfo>()
        .call(server_info_callback)
}

pub(crate) async fn load_server_info() -> anyhow::Result<ServerInfo> {
    let db = STATE.db.read().await;
    let info = db::server::get(&db)
        .map_err(|error| anyhow::anyhow!(error.to_string()))?
        .ok_or_else(|| anyhow::anyhow!("server info not initialized"))?;

    let default_username = &STATE.config.get().auth.default_username;
    let setup_complete = db::roles::has_non_default_admin(&db, default_username)
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;

    let config = STATE.config.get();

    Ok(ServerInfo {
        id: info.id,
        version: env!("CARGO_PKG_VERSION").to_string(),
        commit_hash: env!("LYRA_GIT_HASH").to_string(),
        hostname: HOSTNAME.clone(),
        port: config.port,
        published_url: config.published_url.clone(),
        setup_complete,
    })
}
#[derive(Clone)]
pub(crate) struct ServerInfoModuleStore {
    info: ServerInfo,
}
impl ServerInfoModuleStore {
    pub(crate) fn new(info: ServerInfo) -> Self {
        Self { info }
    }
}
fn server_info_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let info = frame.vm.data().get::<ServerInfoModuleStore>()?.info.clone();
    frame
        .returns
        .write(luau::Value::TableData(server_info_table(&info)))
}
fn server_info_table(info: &ServerInfo) -> luau::OwnedTable {
    let mut table = luau::OwnedTable::with_capacity(0, 7);
    table.set_field("id", luau::Value::String(info.id.clone().into_bytes()));
    table.set_field(
        "version",
        luau::Value::String(info.version.clone().into_bytes()),
    );
    table.set_field(
        "commit_hash",
        luau::Value::String(info.commit_hash.clone().into_bytes()),
    );
    table.set_field(
        "hostname",
        luau::Value::String(info.hostname.clone().into_bytes()),
    );
    table.set_field("port", luau::Value::Integer(i64::from(info.port)));
    table.set_field(
        "published_url",
        info.published_url
            .as_ref()
            .map(|url| luau::Value::String(url.clone().into_bytes()))
            .unwrap_or(luau::Value::Nil),
    );
    table.set_field("setup_complete", luau::Value::Boolean(info.setup_complete));
    table
}

impl LuauTypeInfo for ServerInfo {
    fn luau_type() -> LuauType {
        LuauType::literal("ServerInfo")
    }
}

impl DescribeInterface for ServerInfo {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("ServerInfo", None);
        descriptor.fields.extend([
            FieldDescriptor {
                name: "id",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "version",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "commit_hash",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "hostname",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "port",
                ty: u16::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "published_url",
                ty: Option::<String>::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "setup_complete",
                ty: bool::luau_type(),
                description: None,
            },
        ]);
        descriptor
    }
}

fn module_descriptor() -> ModuleDescriptor {
    ModuleDescriptor {
        name: "Server",
        local_name: "server",
        description: None,
        fields: Vec::new(),
        functions: vec![ModuleFunctionDescriptor {
            path: vec!["info"],
            description: Some("Returns information about the running server."),
            params: Vec::<ParameterDescriptor>::new(),
            returns: vec![ServerInfo::luau_type()],
            yields: false,
        }],
    }
}

pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    render_definition_file_with_support(
        &module_descriptor(),
        &[],
        &[ServerInfo::interface_descriptor()],
        &[],
    )
}

#[cfg(test)]
mod spec_tests {
    use super::*;

    #[test]
    fn exposes_handwritten_module_spec() {
        let spec = module_spec();

        assert_eq!(spec.id.0.as_ref(), "lyra/server");
        assert_eq!(spec.capability.as_ref().unwrap().0.as_ref(), "lyra.server");
        assert_eq!(spec.functions.len(), 1);
        assert_eq!(spec.functions[0].name.as_ref(), "info");
        assert!(!spec.functions[0].yields);
        assert!(
            spec.functions[0]
                .return_types
                .iter()
                .any(|name| name.contains("ServerInfo"))
        );
    }

    #[test]
    fn renders_server_module_definition() {
        let rendered = render_luau_definition().expect("render lyra/server docs");

        assert!(rendered.contains("@class Server"));
        assert!(rendered.contains("@interface ServerInfo"));
        assert!(rendered.contains("function server.info(): ServerInfo"));
    }
}
