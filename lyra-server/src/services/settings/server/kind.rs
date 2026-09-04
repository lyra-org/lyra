// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    net::IpAddr,
    path::PathBuf,
};

use anyhow::{
    Result,
    anyhow,
    bail,
};
use serde_json::{
    Value,
    json,
};

use crate::config::BootConfig;

/// Value type of a server setting. Each kind owns its default and its
/// normalization, so a setting is declared as data.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Kind {
    Bool {
        default: bool,
    },
    U32 {
        default: u32,
    },
    U64 {
        default: u64,
    },
    NullableU64,
    NullableOrigin,
    /// Relative values resolve under the boot data directory.
    Path {
        default: fn(&BootConfig) -> PathBuf,
    },
    OriginList {
        default: &'static [&'static str],
    },
    IpList {
        default: &'static [&'static str],
    },
}

impl Kind {
    pub(crate) fn default(self, boot: &BootConfig) -> Result<Value> {
        Ok(match self {
            Self::Bool { default } => json!(default),
            Self::U32 { default } => json!(default),
            Self::U64 { default } => json!(default),
            Self::NullableU64 | Self::NullableOrigin => Value::Null,
            Self::Path { default } => path_value(default(boot))?,
            Self::OriginList { default } | Self::IpList { default } => json!(default),
        })
    }

    /// Validates and canonicalizes a candidate from any source. Null is only
    /// a value for nullable kinds.
    pub(crate) fn normalize(self, value: &Value, boot: &BootConfig) -> Result<Value> {
        match self {
            Self::Bool { .. } => match value {
                Value::Bool(_) => Ok(value.clone()),
                _ => bail!("must be a boolean"),
            },
            Self::U32 { .. } => Ok(json!(integer_of(value, u64::from(u32::MAX))?)),
            Self::U64 { .. } => Ok(json!(integer_of(value, u64::MAX)?)),
            Self::NullableU64 => match value {
                Value::Null => Ok(Value::Null),
                _ => Ok(json!(integer_of(value, u64::MAX)?)),
            },
            Self::NullableOrigin => match value {
                Value::Null => Ok(Value::Null),
                Value::String(raw) => Ok(json!(normalize_origin(raw)?)),
                _ => bail!("must be a string or null"),
            },
            Self::Path { .. } => {
                let Some(raw) = value.as_str() else {
                    bail!("must be a string");
                };
                if raw.trim().is_empty() {
                    bail!("must not be empty");
                }
                path_value(boot.data_dir.join(raw.trim()))
            }
            Self::OriginList { .. } => {
                let origins = strings_of(value)?
                    .into_iter()
                    .map(|raw| {
                        normalize_cors_origin(raw)
                            .map_err(|err| anyhow!("entry '{}': {err}", raw.trim()))
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(json!(origins))
            }
            Self::IpList { .. } => {
                let addresses = strings_of(value)?
                    .into_iter()
                    .map(|raw| {
                        raw.trim()
                            .parse::<IpAddr>()
                            .map(|address| address.to_string())
                            .map_err(|err| anyhow!("entry '{}': {err}", raw.trim()))
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(json!(addresses))
            }
        }
    }
}

/// Paths travel as JSON strings; a non-UTF-8 path cannot be stored or
/// compared, so it is an error rather than a lossy conversion.
fn path_value(path: PathBuf) -> Result<Value> {
    path.to_str()
        .map(|path| json!(path))
        .ok_or_else(|| anyhow!("path '{}' is not valid UTF-8", path.display()))
}

fn integer_of(value: &Value, max: u64) -> Result<u64> {
    let Some(number) = value.as_u64() else {
        bail!("must be a non-negative integer");
    };
    if number > max {
        bail!("must be at most {max}");
    }
    Ok(number)
}

fn strings_of(value: &Value) -> Result<Vec<&str>> {
    let Some(items) = value.as_array() else {
        bail!("must be an array of strings");
    };
    items
        .iter()
        .map(|item| {
            item.as_str()
                .ok_or_else(|| anyhow!("must contain only strings"))
        })
        .collect()
}

/// Normalizes an http(s) origin: trims, lowercases the host, and rejects
/// paths, queries, fragments, and credentials.
pub(crate) fn normalize_origin(raw: &str) -> Result<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("empty origin"));
    }

    let parsed = url::Url::parse(trimmed)?;
    let scheme = parsed.scheme();
    if scheme != "http" && scheme != "https" {
        return Err(anyhow!("scheme must be http or https, got '{scheme}'"));
    }
    if parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.path() != "/"
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err(anyhow!(
            "expected an origin without path, query, fragment, or credentials"
        ));
    }

    Ok(parsed.origin().ascii_serialization())
}

/// CORS entries are origins, plus the `*` wildcard.
pub(crate) fn normalize_cors_origin(raw: &str) -> Result<String> {
    if raw.trim() == "*" {
        return Ok("*".to_string());
    }
    normalize_origin(raw)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    fn boot() -> BootConfig {
        BootConfig {
            data_dir: PathBuf::from("/srv/lyra"),
            ..BootConfig::default()
        }
    }

    fn normalize(kind: Kind, value: Value) -> Result<Value> {
        kind.normalize(&value, &boot())
    }

    #[test]
    fn integers_must_be_whole_and_non_negative() {
        let kind = Kind::U32 { default: 0 };
        assert_eq!(normalize(kind, json!(7)).unwrap(), json!(7));
        assert!(normalize(kind, json!(7.5)).is_err());
        assert!(normalize(kind, json!(-1)).is_err());
        assert!(normalize(kind, json!("7")).is_err());
        assert!(normalize(kind, json!(null)).is_err());
    }

    #[test]
    fn u32_and_u64_enforce_their_ranges() {
        let above_u32 = json!(u64::from(u32::MAX) + 1);
        assert!(normalize(Kind::U32 { default: 0 }, above_u32.clone()).is_err());
        assert_eq!(
            normalize(Kind::U64 { default: 0 }, above_u32.clone()).unwrap(),
            above_u32
        );
    }

    #[test]
    fn nullable_integer_accepts_null_only_as_null() {
        assert_eq!(
            normalize(Kind::NullableU64, json!(null)).unwrap(),
            Value::Null
        );
        assert_eq!(normalize(Kind::NullableU64, json!(5)).unwrap(), json!(5));
        assert!(normalize(Kind::NullableU64, json!(-5)).is_err());
    }

    #[test]
    fn booleans_reject_other_types() {
        let kind = Kind::Bool { default: true };
        assert_eq!(normalize(kind, json!(false)).unwrap(), json!(false));
        assert!(normalize(kind, json!("true")).is_err());
        assert!(normalize(kind, json!(1)).is_err());
        assert!(normalize(kind, json!(null)).is_err());
    }

    #[test]
    fn origin_is_canonicalized() {
        assert_eq!(
            normalize_origin(" http://LOCALHOST:8080/ ").unwrap(),
            "http://localhost:8080"
        );
    }

    #[test]
    fn origin_rejects_paths_queries_and_credentials() {
        for raw in [
            "https://example.com/app",
            "https://example.com?token=secret",
            "https://example.com#top",
            "https://user:pw@example.com",
        ] {
            let error = normalize_origin(raw).expect_err(raw);
            assert!(error.to_string().contains("expected an origin"), "{raw}");
        }
    }

    #[test]
    fn origin_rejects_non_http_schemes_and_empty_input() {
        let error = normalize_origin("file://localhost/tmp").expect_err("scheme");
        assert!(error.to_string().contains("scheme must be http or https"));
        assert!(normalize_origin("   ").is_err());
    }

    #[test]
    fn nullable_origin_accepts_null_and_rejects_wildcard() {
        assert_eq!(
            normalize(Kind::NullableOrigin, json!(null)).unwrap(),
            Value::Null
        );
        assert_eq!(
            normalize(Kind::NullableOrigin, json!("http://LOCALHOST:8080/")).unwrap(),
            json!("http://localhost:8080")
        );
        assert!(normalize(Kind::NullableOrigin, json!("*")).is_err());
        assert!(normalize(Kind::NullableOrigin, json!(1)).is_err());
    }

    #[test]
    fn origin_list_allows_wildcard_entries() {
        let kind = Kind::OriginList { default: &[] };
        assert_eq!(
            normalize(
                kind,
                json!([" http://LOCALHOST:8080 ", "https://example.com", "*"])
            )
            .unwrap(),
            json!(["http://localhost:8080", "https://example.com", "*"])
        );
        let error = normalize(kind, json!(["http://localhost:8080/app"])).expect_err("path");
        assert!(
            error
                .to_string()
                .contains("entry 'http://localhost:8080/app'")
        );
        assert!(normalize(kind, json!(null)).is_err());
        assert!(normalize(kind, json!("*")).is_err());
    }

    #[test]
    fn ip_list_parses_and_canonicalizes_addresses() {
        let kind = Kind::IpList { default: &[] };
        assert_eq!(
            normalize(
                kind,
                json!([" 127.0.0.1 ", "0000:0000:0000:0000:0000:0000:0000:0001"])
            )
            .unwrap(),
            json!(["127.0.0.1", "::1"])
        );
        assert_eq!(normalize(kind, json!([])).unwrap(), json!([]));
        let error = normalize(kind, json!(["10.0.0.0/8"])).expect_err("cidr is not an address");
        assert!(error.to_string().contains("entry '10.0.0.0/8'"));
        assert!(normalize(kind, json!(["localhost"])).is_err());
        assert!(normalize(kind, json!([1])).is_err());
    }

    #[test]
    fn paths_resolve_relative_values_under_the_data_dir() {
        let kind = Kind::Path {
            default: BootConfig::default_covers_path,
        };
        assert_eq!(
            normalize(kind, json!("./covers")).unwrap(),
            json!("/srv/lyra/./covers")
        );
        assert_eq!(
            normalize(kind, json!("/srv/covers")).unwrap(),
            json!("/srv/covers")
        );
        assert_eq!(
            normalize(kind, json!(" covers")).unwrap(),
            json!("/srv/lyra/covers")
        );
        assert_eq!(kind.default(&boot()).unwrap(), json!("/srv/lyra/covers"));
        assert!(normalize(kind, json!("  ")).is_err());
        assert!(normalize(kind, json!(null)).is_err());
        assert_eq!(
            Path::new("/srv/lyra/./covers"),
            Path::new("/srv/lyra/covers")
        );
    }

    #[cfg(unix)]
    #[test]
    fn non_utf8_paths_are_rejected() {
        use std::{
            ffi::OsStr,
            os::unix::ffi::OsStrExt,
        };

        let kind = Kind::Path {
            default: BootConfig::default_covers_path,
        };
        let boot = BootConfig {
            data_dir: PathBuf::from(OsStr::from_bytes(b"/srv/\xff")),
            ..BootConfig::default()
        };
        assert!(kind.default(&boot).is_err());
        assert!(kind.normalize(&json!("covers"), &boot).is_err());
    }
}
