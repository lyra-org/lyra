// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

mod md5;

use std::fmt;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use harmony_luau as luau;
use harmony_luau::{
    DescribeModule,
    DescribeTypeAlias,
    render_definition_file_with_support,
};
use sha2::{
    Digest,
    Sha256,
    Sha384,
    Sha512,
};

pub struct CryptModule;

/// The capability hosts conventionally gate this module behind.
pub const SCOPE: &str = "harmony.crypt";

#[harmony_macros::module(
    id = "harmony/crypt",
    name = "Crypt",
    local = "crypt",
    description = "Cryptographic hashing, encoding, and random byte generation helpers."
)]
impl CryptModule {
    #[harmony(
        description = "Hashes data with the specified algorithm and returns the hex-encoded digest.\n\nSupported algorithms: `\"md5\"`, `\"sha1\"`, `\"sha256\"`, `\"sha384\"`, `\"sha512\"`, `\"xxh3_64\"`, `\"xxh3_128\"`.",
        args(algorithm: HashAlgorithm, data: String)
    )]
    fn hash(algorithm: HashAlgorithm, data: luau::ByteString) -> String {
        hash_bytes(algorithm, &data.0)
    }

    #[harmony(
        name = "base64.encode",
        description = "Encodes data as a base64 string.",
        args(data: String)
    )]
    fn base64_encode(data: luau::ByteString) -> String {
        BASE64_STANDARD.encode(data.0)
    }

    #[harmony(
        name = "base64.decode",
        description = "Decodes a base64 string back into raw bytes.",
        returns(String)
    )]
    fn base64_decode(data: String) -> luau::runtime::Result<luau::Value> {
        let bytes = BASE64_STANDARD
            .decode(data)
            .map_err(|error| luau::Error::Runtime(format!("base64 decode failed: {error}")))?;
        Ok(luau::Value::String(bytes))
    }

    #[harmony(
        description = "Generates a hex-encoded string of random bytes. Size cannot exceed 1024."
    )]
    fn random(size: f64) -> luau::runtime::Result<String> {
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
        Ok(hex_bytes(&buf))
    }
}

fn hash_bytes(algorithm: HashAlgorithm, bytes: &[u8]) -> String {
    match algorithm {
        HashAlgorithm::Md5 => hex_bytes(&md5::digest(bytes)),
        HashAlgorithm::Sha1 => format!("{:x}", sha1::Sha1::digest(bytes)),
        HashAlgorithm::Sha256 => format!("{:x}", Sha256::digest(bytes)),
        HashAlgorithm::Sha384 => format!("{:x}", Sha384::digest(bytes)),
        HashAlgorithm::Sha512 => format!("{:x}", Sha512::digest(bytes)),
        HashAlgorithm::Xxh3_64 => format!("{:016x}", xxh3::hash64_with_seed(bytes, 0)),
        HashAlgorithm::Xxh3_128 => format!("{:032x}", xxh3::hash128_with_seed(bytes, 0)),
    }
}

fn hex_bytes(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[harmony_macros::string_enum(description = "Hash algorithm accepted by `crypt.hash`.")]
#[derive(Clone, Copy)]
enum HashAlgorithm {
    Md5,
    Sha1,
    Sha256,
    Sha384,
    Sha512,
    Xxh3_64,
    Xxh3_128,
}

pub fn render_luau_definition() -> std::result::Result<String, fmt::Error> {
    render_definition_file_with_support(
        &CryptModule::module_descriptor(),
        &[HashAlgorithm::type_alias_descriptor()],
        &[],
        &[],
    )
}

#[cfg(test)]
mod tests {
    use super::CryptModule;

    #[test]
    fn luau_module_hashes_and_base64_encodes() -> harmony_luau::runtime::Result<()> {
        let vm = harmony_luau::Vm::new()?;
        let spec = CryptModule::module_spec();
        let table =
            harmony_core::luau::install_module(&vm, &harmony_core::ChunkOrigin::default(), &spec)?;
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
        let spec = CryptModule::module_spec();
        let table =
            harmony_core::luau::install_module(&vm, &harmony_core::ChunkOrigin::default(), &spec)?;
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
