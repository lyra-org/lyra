// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::fmt;
use std::sync::Arc;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use harmony_core::Module;
use harmony_luau::{
    DescribeModule,
    DescribeTypeAlias,
    LuauType,
    LuauTypeInfo,
    ModuleDescriptor,
    ModuleFunctionDescriptor,
    ParameterDescriptor,
    render_definition_file_with_support,
};
use mlua::{
    ExternalResult,
    Lua,
    Result,
    Value,
};
use sha2::{
    Digest,
    Sha256,
    Sha384,
    Sha512,
};

fn hash(_lua: &Lua, (algorithm, data): (String, mlua::String)) -> Result<String> {
    let bytes: &[u8] = &data.as_bytes();
    match algorithm.as_str() {
        "md5" => {
            let digest = md5::compute(bytes);
            Ok(format!("{digest:x}"))
        }
        "sha1" => {
            let digest = sha1::Sha1::digest(bytes);
            Ok(format!("{digest:x}"))
        }
        "sha256" => {
            let digest = Sha256::digest(bytes);
            Ok(format!("{digest:x}"))
        }
        "sha384" => {
            let digest = Sha384::digest(bytes);
            Ok(format!("{digest:x}"))
        }
        "sha512" => {
            let digest = Sha512::digest(bytes);
            Ok(format!("{digest:x}"))
        }
        "xxh3_64" => {
            let hash = xxh3::hash64_with_seed(bytes, 0);
            Ok(format!("{hash:016x}"))
        }
        "xxh3_128" => {
            let hash = xxh3::hash128_with_seed(bytes, 0);
            Ok(format!("{hash:032x}"))
        }
        _ => Err(mlua::Error::runtime(format!(
            "unsupported hash algorithm '{algorithm}', expected one of: md5, sha1, sha256, sha384, sha512, xxh3_64, xxh3_128"
        ))),
    }
}

fn base64_encode(_lua: &Lua, data: mlua::String) -> Result<String> {
    Ok(BASE64_STANDARD.encode(data.as_bytes()))
}

fn base64_decode(lua: &Lua, data: String) -> Result<Value> {
    let bytes = BASE64_STANDARD.decode(data).into_lua_err()?;
    let lua_string = lua.create_string(&bytes)?;
    Ok(Value::String(lua_string))
}

fn random(_lua: &Lua, size: u32) -> Result<String> {
    if size > 1024 {
        return Err(mlua::Error::runtime("random size cannot exceed 1024 bytes"));
    }
    let mut buf = vec![0u8; size as usize];
    rand::fill(&mut buf[..]);

    Ok(buf.iter().map(|b| format!("{b:02x}")).collect())
}

pub fn get_module() -> Module {
    Module {
        path: "harmony/crypt".into(),
        setup: Arc::new(|lua: &Lua| -> anyhow::Result<mlua::Table> {
            let table = lua.create_table()?;

            table.set("hash", lua.create_function(hash)?)?;
            table.set("random", lua.create_function(random)?)?;

            let base64_table = lua.create_table()?;
            base64_table.set("encode", lua.create_function(base64_encode)?)?;
            base64_table.set("decode", lua.create_function(base64_decode)?)?;
            table.set("base64", base64_table)?;

            Ok(table)
        }),
        scope: harmony_core::Scope {
            id: "harmony.crypt".into(),
            description: "Hashing, random bytes, and base64 encoding utilities.",
            danger: harmony_core::Danger::Negligible,
        },
    }
}

struct HashAlgorithm;

impl LuauTypeInfo for HashAlgorithm {
    fn luau_type() -> LuauType {
        LuauType::literal("HashAlgorithm")
    }
}

impl DescribeTypeAlias for HashAlgorithm {
    fn type_alias_descriptor() -> harmony_luau::TypeAliasDescriptor {
        harmony_luau::TypeAliasDescriptor::new(
            "HashAlgorithm",
            LuauType::union(vec![
                LuauType::literal("\"md5\""),
                LuauType::literal("\"sha1\""),
                LuauType::literal("\"sha256\""),
                LuauType::literal("\"sha384\""),
                LuauType::literal("\"sha512\""),
                LuauType::literal("\"xxh3_64\""),
                LuauType::literal("\"xxh3_128\""),
            ]),
            Some("Hash algorithm accepted by `crypt.hash`."),
        )
    }
}

struct CryptModuleDocs;

pub fn render_luau_definition() -> std::result::Result<String, fmt::Error> {
    render_definition_file_with_support(
        &CryptModuleDocs::module_descriptor(),
        &[HashAlgorithm::type_alias_descriptor()],
        &[],
        &[],
    )
}

impl DescribeModule for CryptModuleDocs {
    fn module_descriptor() -> ModuleDescriptor {
        ModuleDescriptor {
            name: "Crypt",
            local_name: "crypt",
            description: Some(
                "Cryptographic hashing, encoding, and random byte generation helpers.",
            ),
            functions: vec![
                ModuleFunctionDescriptor {
                    path: vec!["hash"],
                    description: Some(
                        "Hashes data with the specified algorithm and returns the hex-encoded digest.\n\nSupported algorithms: `\"md5\"`, `\"sha1\"`, `\"sha256\"`, `\"sha384\"`, `\"sha512\"`, `\"xxh3_64\"`, `\"xxh3_128\"`.",
                    ),
                    params: vec![
                        ParameterDescriptor {
                            name: "algorithm",
                            ty: HashAlgorithm::luau_type(),
                            description: None,
                            variadic: false,
                        },
                        ParameterDescriptor {
                            name: "data",
                            ty: String::luau_type(),
                            description: None,
                            variadic: false,
                        },
                    ],
                    returns: vec![String::luau_type()],
                    yields: false,
                },
                ModuleFunctionDescriptor {
                    path: vec!["base64", "encode"],
                    description: Some("Encodes data as a base64 string."),
                    params: vec![ParameterDescriptor {
                        name: "data",
                        ty: String::luau_type(),
                        description: None,
                        variadic: false,
                    }],
                    returns: vec![String::luau_type()],
                    yields: false,
                },
                ModuleFunctionDescriptor {
                    path: vec!["base64", "decode"],
                    description: Some("Decodes a base64 string back into raw bytes."),
                    params: vec![ParameterDescriptor {
                        name: "data",
                        ty: String::luau_type(),
                        description: None,
                        variadic: false,
                    }],
                    returns: vec![String::luau_type()],
                    yields: false,
                },
                ModuleFunctionDescriptor {
                    path: vec!["random"],
                    description: Some(
                        "Generates a hex-encoded string of random bytes. Size cannot exceed 1024.",
                    ),
                    params: vec![ParameterDescriptor {
                        name: "size",
                        ty: f64::luau_type(),
                        description: None,
                        variadic: false,
                    }],
                    returns: vec![String::luau_type()],
                    yields: false,
                },
            ],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::render_luau_definition;

    #[test]
    fn renders_crypt_module_definition() {
        let rendered = render_luau_definition().expect("render harmony/crypt docs");

        assert!(rendered.contains("@class Crypt"));
        assert!(rendered.contains("export type HashAlgorithm = \"md5\" | \"sha1\" | \"sha256\" | \"sha384\" | \"sha512\" | \"xxh3_64\" | \"xxh3_128\""));
        assert!(
            rendered
                .contains("function crypt.hash(algorithm: HashAlgorithm, data: string): string")
        );
        assert!(rendered.contains("function crypt.base64.encode(data: string): string"));
        assert!(rendered.contains("function crypt.base64.decode(data: string): string"));
        assert!(rendered.contains("function crypt.random(size: number): string"));
        assert!(rendered.contains("crypt.base64 = {}"));
    }
}
