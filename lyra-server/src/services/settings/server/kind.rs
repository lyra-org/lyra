// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::path::{
    Component,
    Path,
    PathBuf,
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

use super::TrustedProxies;

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
    /// Stored as written; relative values resolve under the data directory
    /// when read, through [`resolve_path`].
    Path {
        default: &'static str,
    },
    OriginList {
        default: &'static [&'static str],
    },
    /// IP addresses and CIDR ranges, canonicalized by [`TrustedProxies`].
    IpRangeList {
        default: &'static [&'static str],
    },
}

impl Kind {
    pub(crate) fn default(self) -> Value {
        match self {
            Self::Bool { default } => json!(default),
            Self::U32 { default } => json!(default),
            Self::U64 { default } => json!(default),
            Self::NullableU64 | Self::NullableOrigin => Value::Null,
            Self::Path { default } => json!(default),
            Self::OriginList { default } | Self::IpRangeList { default } => json!(default),
        }
    }

    /// Validates and canonicalizes a candidate from any source. Null is only
    /// a value for nullable kinds.
    pub(crate) fn normalize(self, value: &Value) -> Result<Value> {
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
                let path = without_current_dir(Path::new(raw.trim()));
                if path.as_os_str().is_empty() {
                    bail!("must name a directory");
                }
                Ok(json!(path.to_string_lossy()))
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
            Self::IpRangeList { .. } => {
                Ok(json!(TrustedProxies::parse(strings_of(value)?)?.entries()))
            }
        }
    }
}

/// Where a `Path` setting points: relative values sit under `data_dir`.
pub(crate) fn resolve_path(data_dir: &Path, value: &Path) -> PathBuf {
    without_current_dir(&data_dir.join(value))
}

/// Drops `.` segments, which `Path::components` keeps at the start of a
/// path. `..` stays: collapsing it lexically can change the target through
/// a symlink.
fn without_current_dir(path: &Path) -> PathBuf {
    path.components()
        .filter(|component| *component != Component::CurDir)
        .collect()
}

/// Accepts integral floats too: JSON clients round-trip large integers
/// through doubles.
fn integer_of(value: &Value, max: u64) -> Result<u64> {
    let integral_float = value
        .as_f64()
        .filter(|float| float.fract() == 0.0 && *float >= 0.0 && *float < u64::MAX as f64)
        .map(|float| float as u64);
    let Some(number) = value.as_u64().or(integral_float) else {
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
    use super::*;

    fn normalize(kind: Kind, value: Value) -> Result<Value> {
        kind.normalize(&value)
    }

    #[test]
    fn integers_must_be_whole_and_non_negative() {
        let kind = Kind::U32 { default: 0 };
        assert_eq!(normalize(kind, json!(7)).unwrap(), json!(7));
        assert_eq!(normalize(kind, json!(7.0)).unwrap(), json!(7));
        assert!(normalize(kind, json!(7.5)).is_err());
        assert!(normalize(kind, json!(-1.0)).is_err());
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
        assert_eq!(
            normalize(Kind::U64 { default: 0 }, json!(u64::MAX)).unwrap(),
            json!(u64::MAX)
        );
        assert!(normalize(Kind::U64 { default: 0 }, json!(1.0e20)).is_err());
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
    fn ip_range_list_parses_and_canonicalizes_entries() {
        let kind = Kind::IpRangeList { default: &[] };
        assert_eq!(
            normalize(
                kind,
                json!([" 127.0.0.1 ", "0000:0000:0000:0000:0000:0000:0000:0001"])
            )
            .unwrap(),
            json!(["127.0.0.1", "::1"])
        );
        assert_eq!(
            normalize(kind, json!(["10.0.0.0/8", "::ffff:192.0.2.1", "::/0"])).unwrap(),
            json!(["10.0.0.0/8", "192.0.2.1", "::/0"])
        );
        assert_eq!(normalize(kind, json!([])).unwrap(), json!([]));
        let error = normalize(kind, json!(["10.0.0.0/33"])).expect_err("prefix out of range");
        assert!(error.to_string().contains("entry '10.0.0.0/33'"));
        assert!(normalize(kind, json!(["localhost"])).is_err());
        assert!(normalize(kind, json!([1])).is_err());
    }

    #[test]
    fn paths_keep_the_written_form_without_current_dir_segments() {
        let kind = Kind::Path { default: "covers" };
        assert_eq!(normalize(kind, json!("./covers")).unwrap(), json!("covers"));
        assert_eq!(
            normalize(kind, json!(" covers/ ")).unwrap(),
            json!("covers")
        );
        assert_eq!(
            normalize(kind, json!("art/./covers")).unwrap(),
            json!("art/covers")
        );
        assert_eq!(
            normalize(kind, json!("../covers")).unwrap(),
            json!("../covers")
        );
        assert_eq!(
            normalize(kind, json!("/srv/./covers")).unwrap(),
            json!("/srv/covers")
        );
        assert_eq!(kind.default(), json!("covers"));
        assert!(normalize(kind, json!("  ")).is_err());
        assert!(normalize(kind, json!("./")).is_err());
        assert!(normalize(kind, json!(null)).is_err());
    }

    #[test]
    fn paths_resolve_relative_values_under_the_data_dir() {
        let data_dir = Path::new("/srv/./lyra");
        assert_eq!(
            resolve_path(data_dir, Path::new("covers")),
            PathBuf::from("/srv/lyra/covers")
        );
        assert_eq!(
            resolve_path(data_dir, Path::new("/srv/covers")),
            PathBuf::from("/srv/covers")
        );
    }
}
