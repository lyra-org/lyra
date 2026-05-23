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
#[cfg(any(feature = "docgen", test))]
use harmony_luau::render_definition_file_with_support;
use harmony_luau::{
    DescribeInterface,
    FieldDescriptor,
    InterfaceDescriptor,
    LuauType,
    LuauTypeInfo,
};
#[cfg(any(feature = "docgen", test))]
use harmony_luau::{
    ModuleDescriptor,
    ModuleFunctionDescriptor,
    ParameterDescriptor,
};
use serde::Serialize;

use crate::services::auth::{
    AuthCredential as ServiceAuthCredential,
    Principal as ServicePrincipal,
    ResolvedAuth as ServiceResolvedAuth,
};

#[derive(Serialize)]
pub(crate) struct Principal {
    pub(crate) user_id: i64,
    pub(crate) username: String,
    pub(crate) role: Option<String>,
    pub(crate) permissions: Vec<String>,
}

#[derive(Serialize)]
pub(crate) struct AuthCredential {
    pub(crate) session_id: Option<i64>,
    pub(crate) api_key_id: Option<i64>,
    pub(crate) api_key_name: Option<String>,
}

#[derive(Serialize)]
pub(crate) struct ResolvedAuth {
    pub(crate) principal: Principal,
    pub(crate) credential: AuthCredential,
}

#[derive(Serialize)]
#[cfg(any(feature = "docgen", test))]
struct LoginResult {
    principal: Principal,
    token: String,
}

#[derive(Clone, Serialize)]
pub(crate) struct AuthCapabilities {
    pub(crate) enabled: bool,
    pub(crate) allow_default_login_when_disabled: bool,
    pub(crate) default_username: String,
}

pub(crate) fn to_plugin_principal(principal: ServicePrincipal) -> Principal {
    Principal {
        user_id: principal.user_db_id.0,
        username: principal.username,
        role: principal.role_name,
        permissions: principal
            .permissions
            .iter()
            .filter_map(|p| {
                serde_json::to_value(p)
                    .ok()
                    .and_then(|v| v.as_str().map(String::from))
            })
            .collect(),
    }
}

pub(crate) fn to_plugin_credential(credential: ServiceAuthCredential) -> AuthCredential {
    match credential {
        ServiceAuthCredential::Session { session_id } => AuthCredential {
            session_id: Some(session_id.0),
            api_key_id: None,
            api_key_name: None,
        },
        ServiceAuthCredential::ApiKey { api_key_id, name } => AuthCredential {
            session_id: None,
            api_key_id: Some(api_key_id.0),
            api_key_name: Some(name),
        },
        ServiceAuthCredential::Default => AuthCredential {
            session_id: None,
            api_key_id: None,
            api_key_name: None,
        },
    }
}

pub(crate) fn to_plugin_auth(auth: ServiceResolvedAuth) -> ResolvedAuth {
    ResolvedAuth {
        principal: to_plugin_principal(auth.principal),
        credential: to_plugin_credential(auth.credential),
    }
}

struct AuthModule;

pub(crate) fn module_spec() -> ModuleSpec {
    let spec = ModuleSpec::new("lyra/auth")
        .capability("lyra.auth")
        .function(auth_capabilities_spec())
        .install(|_| Ok(ModuleExport::new(AuthModule)));
    spec
}

fn auth_capabilities_spec() -> FunctionSpec {
    let spec = FunctionSpec::sync_fn("capabilities").returns::<AuthCapabilities>();
    spec.call(auth_capabilities_callback)
}
#[derive(Clone)]
pub(crate) struct AuthCapabilitiesModuleStore {
    capabilities: AuthCapabilities,
}
impl AuthCapabilitiesModuleStore {
    pub(crate) fn new(capabilities: AuthCapabilities) -> Self {
        Self { capabilities }
    }
}
fn auth_capabilities_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let capabilities = frame
        .vm
        .data()
        .get::<AuthCapabilitiesModuleStore>()?
        .capabilities
        .clone();
    frame
        .returns
        .write(auth_capabilities_table(&capabilities))?;
    Ok(())
}
fn auth_capabilities_table(capabilities: &AuthCapabilities) -> luau::OwnedTable {
    let mut table = luau::OwnedTable::with_capacity(0, 3);
    table.set_field("enabled", luau::Value::Boolean(capabilities.enabled));
    table.set_field(
        "allow_default_login_when_disabled",
        luau::Value::Boolean(capabilities.allow_default_login_when_disabled),
    );
    table.set_field(
        "default_username",
        luau::Value::String(capabilities.default_username.clone().into_bytes()),
    );
    table
}

impl LuauTypeInfo for Principal {
    fn luau_type() -> LuauType {
        LuauType::literal("Principal")
    }
}

impl DescribeInterface for Principal {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("Principal", None);
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
            FieldDescriptor {
                name: "permissions",
                ty: Vec::<String>::luau_type(),
                description: None,
            },
        ]);
        descriptor
    }
}

impl LuauTypeInfo for AuthCredential {
    fn luau_type() -> LuauType {
        LuauType::literal("AuthCredential")
    }
}

impl DescribeInterface for AuthCredential {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("AuthCredential", None);
        descriptor.fields.extend([
            FieldDescriptor {
                name: "session_id",
                ty: Option::<i64>::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "api_key_id",
                ty: Option::<i64>::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "api_key_name",
                ty: Option::<String>::luau_type(),
                description: None,
            },
        ]);
        descriptor
    }
}

impl LuauTypeInfo for ResolvedAuth {
    fn luau_type() -> LuauType {
        LuauType::literal("ResolvedAuth")
    }
}

impl DescribeInterface for ResolvedAuth {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("ResolvedAuth", None);
        descriptor.fields.extend([
            FieldDescriptor {
                name: "principal",
                ty: Principal::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "credential",
                ty: AuthCredential::luau_type(),
                description: None,
            },
        ]);
        descriptor
    }
}

#[cfg(any(feature = "docgen", test))]
impl LuauTypeInfo for LoginResult {
    fn luau_type() -> LuauType {
        LuauType::literal("LoginResult")
    }
}

#[cfg(any(feature = "docgen", test))]
impl DescribeInterface for LoginResult {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("LoginResult", None);
        descriptor.fields.extend([
            FieldDescriptor {
                name: "principal",
                ty: Principal::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "token",
                ty: String::luau_type(),
                description: None,
            },
        ]);
        descriptor
    }
}

impl LuauTypeInfo for AuthCapabilities {
    fn luau_type() -> LuauType {
        LuauType::literal("AuthCapabilities")
    }
}

impl DescribeInterface for AuthCapabilities {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("AuthCapabilities", None);
        descriptor.fields.extend([
            FieldDescriptor {
                name: "enabled",
                ty: bool::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "allow_default_login_when_disabled",
                ty: bool::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "default_username",
                ty: String::luau_type(),
                description: None,
            },
        ]);
        descriptor
    }
}

#[cfg(any(feature = "docgen", test))]
fn param(name: &'static str, ty: LuauType) -> ParameterDescriptor {
    ParameterDescriptor {
        name,
        ty,
        description: None,
        variadic: false,
    }
}

#[cfg(any(feature = "docgen", test))]
fn module_descriptor() -> ModuleDescriptor {
    ModuleDescriptor {
        name: "Auth",
        local_name: "auth",
        description: None,
        fields: Vec::new(),
        functions: vec![
            ModuleFunctionDescriptor {
                path: vec!["resolve_auth"],
                description: Some(
                    "Resolves a bearer credential to the authenticated principal and credential metadata.",
                ),
                params: vec![param("bearer", Option::<String>::luau_type())],
                returns: vec![Option::<ResolvedAuth>::luau_type()],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["logout_session"],
                description: Some("Revokes the session identified by the provided token."),
                params: vec![param("token", Option::<String>::luau_type())],
                returns: vec![bool::luau_type()],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["login"],
                description: Some("Attempts to log in and returns a principal plus session token."),
                params: vec![
                    param("username", String::luau_type()),
                    param("password", Option::<String>::luau_type()),
                    param("user_agent", Option::<String>::luau_type()),
                    param("client_name", Option::<String>::luau_type()),
                ],
                returns: vec![Option::<LoginResult>::luau_type()],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["capabilities"],
                description: Some("Returns the current authentication capabilities."),
                params: Vec::new(),
                returns: vec![AuthCapabilities::luau_type()],
                yields: false,
            },
        ],
    }
}

#[cfg(any(feature = "docgen", test))]
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    render_definition_file_with_support(
        &module_descriptor(),
        &[],
        &[
            AuthCapabilities::interface_descriptor(),
            Principal::interface_descriptor(),
            AuthCredential::interface_descriptor(),
            ResolvedAuth::interface_descriptor(),
            LoginResult::interface_descriptor(),
        ],
        &[],
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exposes_handwritten_module_spec() {
        let spec = module_spec();

        assert_eq!(spec.id.0.as_ref(), "lyra/auth");
        assert_eq!(spec.capability.as_ref().unwrap().0.as_ref(), "lyra.auth");
        {
            assert_eq!(spec.functions.len(), 1);
            assert_eq!(spec.functions[0].name.as_ref(), "capabilities");
            assert!(!spec.functions[0].yields);
        }
    }

    #[test]
    fn renders_auth_module_definition() {
        let rendered = render_luau_definition().expect("render lyra/auth docs");

        assert!(rendered.contains("@class Auth"));
        assert!(rendered.contains("@interface Principal"));
        assert!(rendered.contains("@interface AuthCredential"));
        assert!(rendered.contains("@interface ResolvedAuth"));
        assert!(rendered.contains("@interface LoginResult"));
        assert!(rendered.contains("@interface AuthCapabilities"));
        assert!(rendered.contains("function auth.capabilities(): AuthCapabilities"));
    }
}
