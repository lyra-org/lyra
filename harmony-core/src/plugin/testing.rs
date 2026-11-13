// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Helpers shared by the plugin tests.

use super::discovery::execution_order;
use super::{
    Dependency,
    LoadedManifest,
    LoadedPlugin,
    PluginHost,
    PluginLoadError,
    PluginManager,
    PluginManifest,
};
use semver::{
    Version,
    VersionReq,
};
use std::collections::{
    BTreeMap,
    HashSet,
};
use std::path::PathBuf;
use std::sync::Arc;

pub(super) fn manifest(id: &str, scopes: &[&str]) -> PluginManifest {
    PluginManifest {
        schema_version: super::PLUGIN_SCHEMA_VERSION,
        id: id.to_string(),
        name: id.to_string(),
        version: Version::new(1, 0, 0),
        host_requirement: VersionReq::STAR,
        description: String::new(),
        entrypoint: Some("init.luau".to_string()),
        scopes: scopes.iter().map(|scope| scope.to_string()).collect(),
        scope_reasons: BTreeMap::new(),
        scope_grants: BTreeMap::new(),
        dependencies: BTreeMap::new(),
    }
}

pub(super) fn dependency(requirement: &str) -> Dependency {
    Dependency {
        version: VersionReq::parse(requirement).unwrap(),
        optional: false,
    }
}

pub(super) fn optional(requirement: &str) -> Dependency {
    Dependency {
        optional: true,
        ..dependency(requirement)
    }
}

pub(super) fn dependencies(entries: &[(&str, Dependency)]) -> BTreeMap<String, Dependency> {
    entries
        .iter()
        .map(|(id, dependency)| (id.to_string(), dependency.clone()))
        .collect()
}

/// `manifest` as discovery would load it from `plugins/<id>`.
pub(super) fn loaded(manifest: PluginManifest) -> LoadedPlugin {
    LoadedPlugin {
        directory: PathBuf::from(format!("plugins/{}", manifest.id)),
        manifest,
        declared_scopes: HashSet::new(),
    }
}

pub(super) fn loaded_plugin(
    id: &str,
    version: &str,
    dependencies: BTreeMap<String, Dependency>,
) -> LoadedPlugin {
    LoadedPlugin {
        manifest: PluginManifest {
            version: Version::parse(version).unwrap(),
            dependencies,
            ..manifest(id, &[])
        },
        directory: PathBuf::from(format!("plugins/{id}")),
        declared_scopes: HashSet::new(),
    }
}

pub(super) fn order(mgr: &PluginManager) -> Vec<String> {
    execution_order_ids(mgr.list_plugins().cloned().collect())
}

pub(super) fn execution_order_ids(plugins: Vec<LoadedPlugin>) -> Vec<String> {
    execution_order(plugins)
        .expect("plugins have an execution order")
        .into_iter()
        .map(|plugin| plugin.manifest.id)
        .collect()
}

pub(super) fn temp_plugin_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "harmony-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).expect("create temp plugin dir");
    dir
}

pub(super) fn manifest_json(id: &str, extra: &str) -> String {
    format!(
        r#"{{"schema_version":1,"id":"{id}","name":"Demo","version":"1.0.0","host":"0.0.1","description":"d","entrypoint":"init.luau","scopes":["net"]{extra}}}"#
    )
}

pub(super) fn host() -> PluginHost {
    PluginHost::new(
        "host",
        Version::new(0, 0, 1),
        HashSet::from([Arc::<str>::from("net")]),
    )
    .unwrap()
}

pub(super) fn load_manifest(bytes: &[u8]) -> Result<LoadedManifest, PluginLoadError> {
    LoadedManifest::load(std::path::Path::new("plugin.json"), bytes, &host())
}
