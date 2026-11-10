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

use crate::services::STATE;

#[derive(Clone, Debug, Serialize)]
pub(crate) struct ServerInfo {
    pub(crate) id: String,
    pub(crate) version: String,
    pub(crate) commit_hash: String,
    pub(crate) port: u16,
    pub(crate) tls: bool,
    pub(crate) published_url: Option<String>,
    pub(crate) setup: crate::services::setup::Status,
}

pub(crate) struct ServerModule;

#[harmony_macros::module(id = "lyra/server", docs_feature = "docgen")]
impl ServerModule {
    #[harmony(
        description = "Returns information about the running server.",
        returns(ServerInfo)
    )]
    fn info(vm: &luau::Vm) -> luau::runtime::Result<luau::OwnedTable> {
        let store = vm.data().get::<ServerInfoModuleStore>()?;
        let mut info = store.info.clone();
        if let Some(settings) = &store.settings {
            info.published_url = settings.get().config.published_url.clone();
        }
        Ok(server_info_table(&info))
    }
}

pub(crate) async fn load_server_info() -> anyhow::Result<ServerInfo> {
    let db = STATE.db.read().await;
    let info = lyra_db::server::get(&db)
        .map_err(|error| anyhow::anyhow!(error.to_string()))?
        .ok_or_else(|| anyhow::anyhow!("server info not initialized"))?;

    drop(db);
    let setup = crate::services::setup::status().await?;

    let config = STATE.config();

    Ok(ServerInfo {
        id: info.id,
        version: env!("CARGO_PKG_VERSION").to_string(),
        commit_hash: STATE.boot.get().build.commit_hash.clone(),
        port: STATE.boot.get().port,
        tls: STATE.boot.get().tls.is_some(),
        published_url: config.published_url.clone(),
        setup,
    })
}
#[derive(Clone)]
pub(crate) struct ServerInfoModuleStore {
    info: ServerInfo,
    settings: Option<crate::services::state::SettingsHandle>,
}
impl ServerInfoModuleStore {
    pub(crate) fn new(
        info: ServerInfo,
        settings: Option<crate::services::state::SettingsHandle>,
    ) -> Self {
        Self { info, settings }
    }
}
fn server_info_table(info: &ServerInfo) -> luau::OwnedTable {
    let mut table = luau::OwnedTable::with_capacity(0, 7);
    table.set_field("id", info.id.clone());
    table.set_field("version", info.version.clone());
    table.set_field("commit_hash", info.commit_hash.clone());
    table.set_field("port", i64::from(info.port));
    table.set_field("tls", info.tls);
    table.set_field("published_url", info.published_url.clone());
    let mut setup = luau::OwnedTable::with_capacity(0, 3);
    setup.set_field("account_required", info.setup.account_required);
    setup.set_field("setup_code_required", info.setup.setup_code_required);
    setup.set_field(
        "plugin_selection_required",
        info.setup.plugin_selection_required,
    );
    table.set_field("setup", setup);
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
                name: "port",
                ty: u16::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "tls",
                ty: bool::luau_type(),
                description: Some(
                    "Whether the port serves HTTPS itself; a reverse proxy in front of the server doesn't count.",
                ),
            },
            FieldDescriptor {
                name: "published_url",
                ty: Option::<String>::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "setup",
                ty: LuauType::Object(vec![
                    FieldDescriptor {
                        name: "account_required",
                        ty: bool::luau_type(),
                        description: None,
                    },
                    FieldDescriptor {
                        name: "setup_code_required",
                        ty: bool::luau_type(),
                        description: Some(
                            "Whether creating that account takes the setup code the server logged at startup.",
                        ),
                    },
                    FieldDescriptor {
                        name: "plugin_selection_required",
                        ty: bool::luau_type(),
                        description: None,
                    },
                ]),
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
        &ServerModule::module_descriptor(),
        &[],
        &[ServerInfo::interface_descriptor()],
        &[],
    )
}
