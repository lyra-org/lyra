// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::fmt;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use harmony_core::{
    FunctionSpec,
    ModuleExport,
    ModuleSpec,
};
use harmony_luau as luau;
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
use sha2::{
    Digest,
    Sha256,
    Sha384,
    Sha512,
};

struct CryptModule;

pub fn module_spec() -> ModuleSpec {
    ModuleSpec::new("harmony/crypt")
        .capability("harmony.crypt")
        .function(hash_spec())
        .function(base64_encode_spec())
        .function(base64_decode_spec())
        .function(random_spec())
        .install(|_| Ok(ModuleExport::new(CryptModule)))
}

fn hash_spec() -> FunctionSpec {
    let spec = FunctionSpec::sync_fn("hash")
        .named_arg::<HashAlgorithm>("algorithm")
        .named_arg::<String>("data")
        .returns::<String>();
    spec.call(hash_callback)
}

fn base64_encode_spec() -> FunctionSpec {
    let spec = FunctionSpec::sync_fn("base64.encode")
        .named_arg::<String>("data")
        .returns::<String>();
    spec.call(base64_encode_callback)
}

fn base64_decode_spec() -> FunctionSpec {
    let spec = FunctionSpec::sync_fn("base64.decode")
        .named_arg::<String>("data")
        .returns::<String>();
    spec.call(base64_decode_callback)
}

fn random_spec() -> FunctionSpec {
    let spec = FunctionSpec::sync_fn("random")
        .named_arg::<f64>("size")
        .returns::<String>();
    spec.call(random_callback)
}

fn hash_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let algorithm: String = frame.args.read_named("algorithm")?;
    let data = read_string_bytes(&mut frame.args, "data")?;
    frame.returns.write(hash_bytes(&algorithm, &data)?)?;
    Ok(())
}

fn base64_encode_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let data = read_string_bytes(&mut frame.args, "data")?;
    frame.returns.write(BASE64_STANDARD.encode(data))?;
    Ok(())
}

fn base64_decode_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let data: String = frame.args.read_named("data")?;
    let bytes = BASE64_STANDARD
        .decode(data)
        .map_err(|error| luau::Error::Runtime(format!("base64 decode failed: {error}")))?;
    frame.returns.write(luau::Value::String(bytes))?;
    Ok(())
}

fn random_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let size: f64 = frame.args.read_named("size")?;
    if !size.is_finite() || size.fract() != 0.0 || size < 0.0 {
        return Err(luau::Error::Runtime(
            "random size must be a non-negative integer".into(),
        ));
    }
    if size > 1024.0 {
        return Err(luau::Error::Runtime(
            "random size cannot exceed 1024 bytes".into(),
        ));
    }
    let mut buf = vec![0u8; size as usize];
    rand::fill(&mut buf[..]);
    frame.returns.write(hex_bytes(&buf))?;
    Ok(())
}

fn read_string_bytes(
    args: &mut luau::ArgReader<'_>,
    name: &'static str,
) -> luau::runtime::Result<Vec<u8>> {
    let bytes: luau::ByteString = args.read_named(name)?;
    Ok(bytes.0)
}

fn hash_bytes(algorithm: &str, bytes: &[u8]) -> luau::runtime::Result<String> {
    match algorithm {
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
        _ => Err(luau::Error::Runtime(format!(
            "unsupported hash algorithm '{algorithm}', expected one of: md5, sha1, sha256, sha384, sha512, xxh3_64, xxh3_128"
        ))),
    }
}

fn hex_bytes(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
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
            fields: Vec::new(),
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
    use super::{
        module_spec,
        render_luau_definition,
    };

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

    #[test]
    fn exposes_handwritten_module_spec() {
        let spec = module_spec();

        assert_eq!(spec.id.0.as_ref(), "harmony/crypt");
        assert_eq!(
            spec.capability.as_ref().unwrap().0.as_ref(),
            "harmony.crypt"
        );
        assert_eq!(spec.functions.len(), 4);
        assert!(spec.functions.iter().all(|function| !function.yields));
        assert_eq!(spec.functions[0].name.as_ref(), "hash");
        assert_eq!(spec.functions[1].name.as_ref(), "base64.encode");
        assert_eq!(spec.functions[2].name.as_ref(), "base64.decode");
        assert_eq!(spec.functions[3].name.as_ref(), "random");
    }

    #[test]
    fn luau_module_hashes_and_base64_encodes() -> harmony_luau::runtime::Result<()> {
        let vm = harmony_luau::Vm::new()?;
        let spec = module_spec();
        let table =
            harmony_core::install_luau_module(&vm, &harmony_core::ChunkOrigin::default(), &spec)?;
        vm.set_global_table("crypt", &table)?;

        let values = vm.eval(
            std::sync::Arc::<[u8]>::from(
                &br#"
                local encoded = crypt.base64.encode("lyra")
                local decoded = crypt.base64.decode(encoded)
                return crypt.hash("sha256", "lyra"), encoded, decoded
                "#[..],
            ),
            harmony_luau::ChunkOrigin::default(),
        )?;

        assert_eq!(
            values,
            vec![
                harmony_luau::Value::String(
                    b"c4ddeffba8c2336a2af52d753c6079645d69db148800e2a79048e28196181b6e".to_vec()
                ),
                harmony_luau::Value::String(b"bHlyYQ==".to_vec()),
                harmony_luau::Value::String(b"lyra".to_vec()),
            ]
        );
        Ok(())
    }

    #[test]
    fn luau_module_random_validates_size() -> harmony_luau::runtime::Result<()> {
        let vm = harmony_luau::Vm::new()?;
        let spec = module_spec();
        let table =
            harmony_core::install_luau_module(&vm, &harmony_core::ChunkOrigin::default(), &spec)?;
        vm.set_global_table("crypt", &table)?;

        let values = vm.eval(
            std::sync::Arc::<[u8]>::from(&b"return #crypt.random(16)"[..]),
            harmony_luau::ChunkOrigin::default(),
        )?;
        assert_eq!(values, vec![harmony_luau::Value::Number(32.0)]);

        assert!(matches!(
            vm.eval(
                std::sync::Arc::<[u8]>::from(&b"return crypt.random(1025)"[..]),
                harmony_luau::ChunkOrigin::default(),
            ),
            Err(harmony_luau::Error::Runtime(message))
                if message.contains("random size cannot exceed 1024 bytes")
        ));
        Ok(())
    }
}
