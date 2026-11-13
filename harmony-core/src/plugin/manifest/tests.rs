use crate::plugin::testing::*;
use crate::plugin::{
    Dependency,
    LoadedManifest,
    PluginDiscoveryError,
    PluginHost,
    PluginLoadError,
    PluginManager,
    PluginManifest,
    validate_plugin_id,
};

use semver::{
    Version,
    VersionReq,
};
use std::collections::{
    BTreeMap,
    HashSet,
};

use std::sync::Arc;

#[test]
fn accepts_well_formed_ids() {
    for id in ["demo", "Demo_Plugin", "my-plugin", "plugin123", "a"] {
        assert!(validate_plugin_id(id).is_ok(), "rejected {id:?}");
    }
}

#[test]
fn rejects_empty() {
    assert!(validate_plugin_id("").is_err());
}

#[test]
fn rejects_slash() {
    assert!(validate_plugin_id("foo/bar").is_err());
}

#[test]
fn rejects_whitespace_dots_and_other_symbols() {
    for id in [
        "foo bar", "foo.bar", "foo:bar", "foo@bar", "foo/..", "foo\\bar",
    ] {
        assert!(validate_plugin_id(id).is_err(), "accepted {id:?}");
    }
}

fn parse_dependencies(json: &str) -> serde_json::Result<BTreeMap<String, Dependency>> {
    super::deserialize_dependencies(&mut serde_json::Deserializer::from_str(json))
}

#[test]
fn parses_dependency_short_and_long_forms() {
    let json =
        r#"{"a": "0.2", "b": {"version": "^1.4"}, "c": {"version": "=0.0.3", "optional": true}}"#;
    assert_eq!(
        parse_dependencies(json).unwrap(),
        dependencies(&[
            ("a", dependency("0.2")),
            ("b", dependency("^1.4")),
            ("c", optional("=0.0.3")),
        ])
    );
}

#[test]
fn rejects_malformed_dependencies() {
    for (json, expected) in [
        (
            r#"{"a": "nope"}"#,
            "invalid dependency 'a' version requirement \"nope\"",
        ),
        (
            r#"{"a": "1", "b": {"version": "nope"}}"#,
            "invalid dependency 'b' version requirement \"nope\"",
        ),
        (
            r#"{"a": "1", "b": "1", "a": {"version": "2"}}"#,
            "dependency 'a' is listed more than once",
        ),
        (r#"{"a": {"optional": true}}"#, "missing field `version`"),
        (
            r#"{"a": {"version": "1", "required": false}}"#,
            "unknown field `required`",
        ),
        (r#"{"a": ["b"]}"#, "a version requirement or an object"),
        (r#"["a"]"#, "an object mapping plugin ids to dependencies"),
    ] {
        let error = parse_dependencies(json).unwrap_err();
        assert!(error.to_string().contains(expected), "{json}: {error}");
    }
}

#[test]
fn load_manifest_accepts_valid_manifests_without_a_directory() {
    let json = manifest_json(
        "demo",
        r#","dependencies":{"other":"0.2","extra":{"version":"1","optional":true}},"unknown":1"#,
    );
    let loaded = load_manifest(json.as_bytes()).unwrap();

    assert_eq!(loaded.manifest.id, "demo");
    assert_eq!(loaded.manifest.name, "Demo");
    assert_eq!(loaded.manifest.version, Version::new(1, 0, 0));
    assert_eq!(
        loaded.manifest.host_requirement,
        VersionReq::parse("0.0.1").unwrap()
    );
    assert_eq!(loaded.manifest.description, "d");
    assert_eq!(loaded.manifest.scopes, vec!["net"]);
    assert!(loaded.declared_scopes.contains("net"));
    assert_eq!(
        loaded.manifest.dependencies,
        dependencies(&[("other", dependency("0.2")), ("extra", optional("1"))])
    );
}

#[test]
fn load_manifest_rejects_a_narrow_scope_without_its_parent() {
    let host = PluginHost::new(
        "host",
        Version::new(0, 0, 1),
        HashSet::from([Arc::<str>::from("net"), Arc::<str>::from("net.all")]),
    )
    .unwrap()
    .with_scope_parents([("net.all", "net")]);
    let load = |scopes: &str| {
        let json = manifest_json("demo", "").replace(r#"["net"]"#, scopes);
        LoadedManifest::load(std::path::Path::new("plugin.json"), json.as_bytes(), &host)
    };

    assert!(load(r#"["net.all","net"]"#).is_ok());
    let error = load(r#"["net.all"]"#).unwrap_err();
    assert!(matches!(
        &error,
        PluginLoadError::ScopeWithoutParent { scope, parent, .. }
            if scope == "net.all" && parent == "net"
    ));
    assert_eq!(
        error.to_string(),
        "plugin 'demo' declares scope 'net.all' without 'net', the scope it narrows"
    );
}

fn load_scopes(scopes: serde_json::Value) -> Result<LoadedManifest, PluginLoadError> {
    let host = PluginHost::new(
        "host",
        Version::new(0, 0, 1),
        HashSet::from([
            Arc::<str>::from("net"),
            Arc::<str>::from("net.all"),
            Arc::<str>::from("dns"),
        ]),
    )
    .unwrap()
    .with_scope_parents([("net.all", "net")]);
    let json = manifest_json("demo", "").replace(r#"["net"]"#, &scopes.to_string());
    LoadedManifest::load(std::path::Path::new("plugin.json"), json.as_bytes(), &host)
}

fn scope_parse_error(scopes: serde_json::Value) -> String {
    match load_scopes(scopes) {
        Err(PluginLoadError::ConfigParseError { error, .. }) => error,
        other => panic!("expected a parse error: {other:?}"),
    }
}

#[test]
fn scopes_are_plain_ids_or_objects_with_a_reason_in_the_order_written() {
    let plain = load_scopes(serde_json::json!(["net", "dns"]))
        .unwrap()
        .manifest;
    assert_eq!(plain.scopes, ["net", "dns"]);
    assert!(plain.scope_reasons.is_empty());

    let objects = load_scopes(serde_json::json!([
        { "id": "net", "reason": "Fetches things." },
        { "id": "dns", "reason": "Resolves things." },
    ]))
    .unwrap();
    assert_eq!(objects.manifest.scopes, ["net", "dns"]);
    assert_eq!(objects.manifest.scope_reasons.len(), 2);
    assert!(objects.declared_scopes.contains("dns"));

    let mixed = load_scopes(serde_json::json!([
        "net.all",
        { "id": "dns", "reason": "Resolves things." },
        "net",
    ]))
    .unwrap()
    .manifest;
    assert_eq!(mixed.scopes, ["net.all", "dns", "net"]);
    assert_eq!(
        mixed.scope_reasons,
        BTreeMap::from([("dns".to_string(), "Resolves things.".to_string())])
    );
}

#[test]
fn scope_objects_hold_exactly_a_string_id_and_reason() {
    for (scopes, expected) in [
        (
            serde_json::json!([{ "id": "net", "reason": "r", "extra": 1 }]),
            "unknown field `extra`",
        ),
        (
            serde_json::json!([{ "id": "net" }]),
            "missing field `reason`",
        ),
        (serde_json::json!([{ "reason": "r" }]), "missing field `id`"),
        (
            serde_json::json!([{ "id": 1, "reason": "r" }]),
            "invalid type",
        ),
        (
            serde_json::json!([{ "id": "net", "reason": 1 }]),
            "invalid type",
        ),
        (
            serde_json::json!([{ "id": "net", "reason": null }]),
            "invalid type",
        ),
        (serde_json::json!([1]), "a scope id or an object"),
        (
            serde_json::json!(["net", ["dns"]]),
            "a scope id or an object",
        ),
        (serde_json::json!("net"), "invalid type"),
    ] {
        let error = scope_parse_error(scopes.clone());
        assert!(error.contains(expected), "{scopes}: {error}");
    }
    let missing = manifest_json("demo", "").replace(r#""scopes":["net"]"#, r#""z":0"#);
    assert!(matches!(
        load_manifest(missing.as_bytes()),
        Err(PluginLoadError::ConfigParseError { error, .. }) if error.contains("scopes")
    ));
}

#[test]
fn scope_reasons_are_short_single_lines_of_text() {
    let reason = |reason: &str| serde_json::json!([{ "id": "net", "reason": reason }]);
    for bad in [
        "",
        "   ",
        "first\nsecond",
        "tab\there",
        "bell\u{7}",
        "delete\u{7f}",
        "next line\u{85}",
        "line\u{2028}separator",
        "paragraph\u{2029}separator",
        "\u{202e}reversed",
        "\u{2067}isolated",
    ] {
        let error = scope_parse_error(reason(bad));
        assert!(
            error.contains("plugin 'demo' gives scope 'net' an invalid reason"),
            "{bad:?}: {error}"
        );
    }
    assert!(
        scope_parse_error(reason(&"x".repeat(201)))
            .ends_with("an invalid reason: it is longer than 200 characters")
    );

    for accepted in ["x".repeat(200), "é".repeat(200)] {
        let manifest = load_scopes(reason(&accepted)).unwrap().manifest;
        assert_eq!(manifest.scope_reasons["net"], accepted);
    }
}

/// Items are lowercase words, written as an array, in the key `words`.
#[derive(Debug)]
struct Words;

impl crate::scopes::ScopeGrant for Words {
    fn key(&self) -> &str {
        "words"
    }

    fn parse(&self, value: &serde_json::Value) -> Result<Vec<String>, String> {
        let words: Vec<String> = serde_json::from_value(value.clone())
            .map_err(|error| format!("it is not a list of words: {error}"))?;
        let mut items = std::collections::BTreeSet::new();
        for word in words {
            if word.is_empty() || !word.chars().all(|c| c.is_ascii_lowercase()) {
                return Err(format!("'{word}' is not a lowercase word"));
            }
            if !items.insert(word.clone()) {
                return Err(format!("'{word}' is listed more than once"));
            }
        }
        Ok(items.into_iter().collect())
    }

    fn covers(&self, granted: &[String], item: &str) -> bool {
        granted.iter().any(|word| word == item)
    }
}

fn load_granted_scopes(scopes: serde_json::Value) -> Result<LoadedManifest, PluginLoadError> {
    let host = PluginHost::new(
        "host",
        Version::new(0, 0, 1),
        HashSet::from([Arc::<str>::from("net"), Arc::<str>::from("dns")]),
    )
    .unwrap()
    .with_scope_grant("net", Arc::new(Words));
    let json = manifest_json("demo", "").replace(r#"["net"]"#, &scopes.to_string());
    LoadedManifest::load(std::path::Path::new("plugin.json"), json.as_bytes(), &host)
}

fn grant_parse_error(scopes: serde_json::Value) -> String {
    match load_granted_scopes(scopes) {
        Err(PluginLoadError::ConfigParseError { error, .. }) => error,
        other => panic!("expected a parse error: {other:?}"),
    }
}

#[test]
fn a_scope_entry_holds_the_grant_its_scope_takes() {
    let manifest = load_granted_scopes(serde_json::json!([
        "dns",
        { "id": "net", "reason": "Fetches things.", "words": ["b", "a"] },
    ]))
    .unwrap()
    .manifest;

    assert_eq!(manifest.scopes, ["dns", "net"]);
    assert_eq!(
        manifest.scope_grants,
        BTreeMap::from([("net".to_string(), vec!["a".to_string(), "b".to_string()])])
    );

    let empty = load_granted_scopes(
        serde_json::json!([{ "id": "net", "reason": "Encodes things.", "words": [] }]),
    )
    .unwrap()
    .manifest;
    assert_eq!(empty.scope_grants["net"], Vec::<String>::new());
}

#[test]
fn a_scope_entry_takes_no_key_but_its_scopes_grant() {
    for (scopes, expected) in [
        (
            serde_json::json!([{ "id": "net", "reason": "r", "words": [], "extra": 1 }]),
            "plugin 'demo' gives scope 'net' an unknown field `extra`",
        ),
        (
            serde_json::json!([{ "id": "dns", "reason": "r", "words": [] }]),
            "plugin 'demo' gives scope 'dns' an unknown field `words`",
        ),
        (
            serde_json::json!([{ "id": "net", "words": [] }]),
            "missing field `reason`",
        ),
        (
            serde_json::json!([{ "id": "net", "reason": "r", "words": "a" }]),
            "plugin 'demo' gives scope 'net' an invalid `words`: it is not a list of words",
        ),
        (
            serde_json::json!([{ "id": "net", "reason": "r", "words": ["A"] }]),
            "an invalid `words`: 'A' is not a lowercase word",
        ),
        (
            serde_json::json!([{ "id": "net", "reason": "r", "words": ["a", "a"] }]),
            "an invalid `words`: 'a' is listed more than once",
        ),
    ] {
        let error = grant_parse_error(scopes.clone());
        assert!(error.contains(expected), "{scopes}: {error}");
    }

    for (field, twice) in [
        ("id", r#"{"id":"net","id":"net","reason":"r","words":[]}"#),
        (
            "reason",
            r#"{"id":"net","reason":"r","reason":"r","words":[]}"#,
        ),
        (
            "words",
            r#"{"id":"net","reason":"r","words":[],"words":[]}"#,
        ),
    ] {
        let json = manifest_json("demo", "").replace(r#""net""#, twice);
        let error = match load_manifest(json.as_bytes()) {
            Err(PluginLoadError::ConfigParseError { error, .. }) => error,
            other => panic!("expected a parse error: {other:?}"),
        };
        assert!(
            error.contains(&format!("duplicate field `{field}`")),
            "{error}"
        );
    }
}

#[test]
fn a_grant_lists_no_more_items_than_its_cap() {
    let words = |count: usize| -> Vec<String> {
        (0..count)
            .map(|index| {
                let first = char::from(b'a' + (index / 26) as u8);
                let second = char::from(b'a' + (index % 26) as u8);
                format!("{first}{second}")
            })
            .collect()
    };
    let scopes = |count| serde_json::json!([{ "id": "net", "reason": "r", "words": words(count) }]);
    let cap = crate::plugin::PLUGIN_CONFIG_MAX_SCOPE_GRANT_ITEMS;

    let manifest = load_granted_scopes(scopes(cap)).unwrap().manifest;
    assert_eq!(manifest.scope_grants["net"].len(), cap);
    assert!(
        grant_parse_error(scopes(cap + 1))
            .ends_with("an invalid `words`: it lists 33 items, exceeds cap of 32")
    );
}

#[test]
fn a_scope_without_its_required_grant_parses_but_does_not_load() {
    let object = serde_json::json!([{ "id": "net", "reason": "Fetches things." }]);
    for scopes in [serde_json::json!(["net"]), object.clone()] {
        let error = load_granted_scopes(scopes.clone()).unwrap_err();
        assert!(
            matches!(
                &error,
                PluginLoadError::MissingScopeGrant { plugin_id, scope, key }
                    if plugin_id == "demo" && scope == "net" && key == "words"
            ),
            "{scopes}: {error}"
        );
        assert_eq!(
            error.to_string(),
            "plugin 'demo' declares scope 'net' without `words`, which the scope requires"
        );
    }

    let host = PluginHost::new(
        "host",
        Version::new(0, 0, 1),
        HashSet::from([Arc::<str>::from("net")]),
    )
    .unwrap()
    .with_scope_grant("net", Arc::new(Words));
    let json = manifest_json("demo", "").replace(r#"["net"]"#, &object.to_string());
    let parsed = PluginManifest::from_json(&json, &host).expect("the manifest still parses");
    assert_eq!(parsed.scopes, ["net"]);
    assert!(parsed.scope_grants.is_empty());
}

#[test]
fn a_host_mismatch_is_reported_before_a_missing_grant() {
    let host = PluginHost::new(
        "host",
        Version::new(0, 0, 2),
        HashSet::from([Arc::<str>::from("net")]),
    )
    .unwrap()
    .with_scope_grant("net", Arc::new(Words));
    let error = LoadedManifest::load(
        std::path::Path::new("plugin.json"),
        manifest_json("demo", "").as_bytes(),
        &host,
    )
    .unwrap_err();
    assert!(matches!(error, PluginLoadError::IncompatibleHost { .. }));
}

#[test]
fn a_scope_may_be_listed_once_in_either_form() {
    let object = serde_json::json!({ "id": "net", "reason": "Fetches things." });
    for scopes in [
        serde_json::json!(["net", "net"]),
        serde_json::json!(["net", object]),
        serde_json::json!([object, "net"]),
        serde_json::json!([object, object]),
    ] {
        assert_eq!(
            scope_parse_error(scopes),
            "plugin 'demo' lists scope 'net' more than once"
        );
    }
}

#[test]
fn scope_objects_pass_the_same_checks_as_plain_ids() {
    let explained = |id: &str| serde_json::json!([{ "id": id, "reason": "Needs it." }]);
    assert!(matches!(
        load_scopes(explained("fs")),
        Err(PluginLoadError::UnknownScope { scope, .. }) if scope == "fs"
    ));
    assert!(matches!(
        load_scopes(explained("net.all")),
        Err(PluginLoadError::ScopeWithoutParent { scope, parent, .. })
            if scope == "net.all" && parent == "net"
    ));
    assert!(matches!(
        load_scopes(explained(
            &"s".repeat(super::PLUGIN_CONFIG_MAX_SCOPE_LEN + 1)
        )),
        Err(PluginLoadError::ScopeEntryTooLong { .. })
    ));
}

#[test]
fn load_manifest_rejects_manifest_level_errors() {
    let long_scope = format!(
        r#"{{"schema_version":1,"id":"demo","name":"d","version":"1.0.0","host":"*","description":"","scopes":["{}"]}}"#,
        "s".repeat(super::PLUGIN_CONFIG_MAX_SCOPE_LEN + 1)
    );
    let err = |bytes: &[u8]| load_manifest(bytes).unwrap_err();

    assert!(matches!(
        err(&vec![
            b' ';
            crate::plugin::PLUGIN_CONFIG_MAX_BYTES as usize + 1
        ]),
        PluginLoadError::ConfigTooLarge { .. }
    ));
    assert!(matches!(
        err(&[0xff, 0xfe]),
        PluginLoadError::ConfigParseError { error, .. }
            if error == "stream did not contain valid UTF-8"
    ));
    assert!(matches!(
        err(b"{"),
        PluginLoadError::ConfigParseError { .. }
    ));
    assert!(matches!(
        err(manifest_json("demo", "")
            .replace(r#""schema_version":1"#, r#""schema_version":2"#)
            .as_bytes()),
        PluginLoadError::InvalidSchemaVersion { version: 2, .. }
    ));
    assert!(matches!(
        err(manifest_json("not/valid", "").as_bytes()),
        PluginLoadError::InvalidPluginId { .. }
    ));
    assert!(matches!(
        err(long_scope.as_bytes()),
        PluginLoadError::ScopeEntryTooLong { .. }
    ));
    assert!(matches!(
        err(manifest_json("demo", "").replace(r#"["net"]"#, r#"["fs"]"#).as_bytes()),
        PluginLoadError::UnknownScope { scope, .. } if scope == "fs"
    ));
    assert!(matches!(
        err(manifest_json("demo", r#","dependencies":{"other":"1","other":"2"}"#).as_bytes()),
        PluginLoadError::ConfigParseError { error, .. }
            if error.starts_with("dependency 'other' is listed more than once")
    ));
    assert!(matches!(
        err(manifest_json("demo", r#","dependencies":{"demo":"1"}"#).as_bytes()),
        PluginLoadError::SelfDependency { .. }
    ));
    assert!(matches!(
        err(manifest_json("demo", r#","dependencies":{"not/valid":"1"}"#).as_bytes()),
        PluginLoadError::InvalidDependencyId { .. }
    ));
    let too_many = (0..=super::PLUGIN_CONFIG_MAX_DEPENDENCIES)
        .map(|i| format!(r#""dep{i}":"1""#))
        .collect::<Vec<_>>()
        .join(",");
    assert!(matches!(
        err(manifest_json("demo", &format!(r#","dependencies":{{{too_many}}}"#)).as_bytes()),
        PluginLoadError::TooManyDependencies { .. }
    ));
}

#[test]
fn load_manifest_parses_versions_and_checks_the_host() {
    let err = |json: String| load_manifest(json.as_bytes()).unwrap_err().to_string();

    for (from, to, expected) in [
        (
            r#""version":"1.0.0""#,
            r#""version":"1""#,
            r#"invalid version "1""#,
        ),
        (r#""version":"1.0.0","#, "", "missing field `version`"),
        (
            r#""host":"0.0.1""#,
            r#""host":"latest""#,
            r#"invalid host version requirement "latest""#,
        ),
        (r#""host":"0.0.1","#, "", "missing field `host`"),
    ] {
        let message = err(manifest_json("demo", "").replace(from, to));
        assert!(message.contains(expected), "{message}");
    }

    assert_eq!(
        err(manifest_json("demo", "").replace(r#""host":"0.0.1""#, r#""host":"0.0.2""#)),
        "plugin 'demo' requires host ^0.0.2, but this is host 0.0.1"
    );
    for requirement in ["0.0", ">=0.0.1, <0.1.0", "*"] {
        let json = manifest_json("demo", "")
            .replace(r#""host":"0.0.1""#, &format!(r#""host":"{requirement}""#));
        assert!(load_manifest(json.as_bytes()).is_ok(), "{requirement}");
    }
}

#[test]
fn load_manifest_reads_the_requirement_under_the_host_name() {
    let other = PluginHost::new(
        "other",
        Version::new(2, 0, 0),
        HashSet::from([Arc::<str>::from("net")]),
    )
    .unwrap();
    let load = |json: String| {
        LoadedManifest::load(std::path::Path::new("plugin.json"), json.as_bytes(), &other)
    };
    let named = |requirement: &str| manifest_json("demo", &format!(r#","other":"{requirement}""#));

    let loaded = load(named("2")).unwrap();
    assert_eq!(
        loaded.manifest.host_requirement,
        VersionReq::parse("2").unwrap()
    );
    assert_eq!(
        load(named("1")).unwrap_err().to_string(),
        "plugin 'demo' requires other ^1, but this is other 2.0.0"
    );
    let message = load(manifest_json("demo", "")).unwrap_err().to_string();
    assert!(message.contains("missing field `other`"), "{message}");
}

#[test]
fn load_manifest_rejects_a_host_requirement_named_twice() {
    let json =
        manifest_json("demo", "").replace(r#""host":"0.0.1""#, r#""host":"0.0.1","host":"0.0.2""#);

    let message = load_manifest(json.as_bytes()).unwrap_err().to_string();

    assert!(message.contains("duplicate field `host`"), "{message}");
}

#[test]
fn hosts_reserve_plugin_ids_at_manifest_and_directory_load() {
    let reserving = host().with_reserved_ids(["update"]);
    let json = manifest_json("update", "");

    assert!(load_manifest(json.as_bytes()).is_ok());
    let err = LoadedManifest::load(
        std::path::Path::new("plugin.json"),
        json.as_bytes(),
        &reserving,
    )
    .unwrap_err();
    assert_eq!(
        err.to_string(),
        "invalid plugin id 'update': 'update' is reserved by host"
    );

    let plugins_dir = temp_plugin_dir("reserved-ids");
    for id in ["update", "demo"] {
        let dir = plugins_dir.join(id);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("init.luau"), "").unwrap();
        std::fs::write(
            dir.join(crate::plugin::PLUGIN_CONFIG_FILENAME),
            manifest_json(id, ""),
        )
        .unwrap();
    }
    let mut mgr = PluginManager::new(plugins_dir);
    let errors = mgr.discover_plugins(&reserving).unwrap();
    assert!(
        matches!(errors.as_slice(), [PluginDiscoveryError { directories, error: PluginLoadError::InvalidPluginId { plugin_id, .. } }] if plugin_id == "update" && directories == &["update"]),
        "{errors:?}"
    );
    assert_eq!(order(&mgr), vec!["demo"]);
    assert!(matches!(
        mgr.reload_plugin("update", &reserving),
        Err(PluginLoadError::InvalidPluginId { .. })
    ));
}

#[test]
fn host_names_must_not_shadow_fixed_fields() {
    for name in ["", "id", "version", "dependencies", "commit"] {
        assert_eq!(
            PluginHost::new(name, Version::new(0, 0, 1), HashSet::new()).unwrap_err(),
            crate::plugin::InvalidHostName(name.to_string()),
            "{name:?}"
        );
    }
}

#[test]
fn rejects_reserved_self_id() {
    assert!(validate_plugin_id("self").is_err());
}
