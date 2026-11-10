// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! The plugins in the plugins directory as the runtime took them: loaded,
//! or left out and why.

use std::{
    collections::{
        BTreeMap,
        BTreeSet,
    },
    path::Path,
};

use harmony_core::plugin::{
    PLUGIN_CONFIG_FILENAME,
    PLUGIN_CONFIG_MAX_BYTES,
    PluginDiscoveryError,
    PluginManifest,
};
use harmony_repository::SourceRecord;

use super::{
    InstalledStatus,
    PluginRepoError,
    Subscription,
    installed_manifests,
    installed_statuses,
};

/// Why a plugin neither loaded nor failed to: the runtime was built before
/// it was installed.
const NOT_DISCOVERED: &str = "installed after the plugins were last loaded";

/// Whether the runtime runs a plugin.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LoadState {
    Loaded,
    NotLoaded { reason: String },
}

/// Where an installed plugin came from.
#[derive(Debug)]
pub(crate) enum InstallSource {
    /// Placed in the plugins directory by hand.
    Local,
    /// Its source record cannot be read.
    Invalid(std::io::Error),
    Repository(Box<RepositoryInstall>),
}

/// A plugin installed from a repository, and where it stands against it.
#[derive(Debug)]
pub(crate) struct RepositoryInstall {
    pub(crate) record: SourceRecord,
    pub(crate) status: InstalledStatus,
}

/// One plugin of the plugins directory.
#[derive(Debug)]
pub(crate) struct Installation {
    /// The name of its directory, which a loaded plugin's id matches.
    pub(crate) id: String,
    /// As its manifest gives it, or `id` when the manifest cannot be read.
    pub(crate) name: String,
    /// As its manifest gives it, or else its source record.
    pub(crate) version: Option<String>,
    pub(crate) description: String,
    pub(crate) load: LoadState,
    pub(crate) source: InstallSource,
}

/// Every plugin the runtime loaded, with the `manifests` it loaded them
/// at, and every other plugin directory in `plugins_dir`, sorted by id: the
/// ones its discovery left out, for the reason in `errors`, and the ones
/// added since. A plugin that isn't loaded is described by its manifest on
/// disk as far as that parses, and listed from its source record alone
/// otherwise.
pub(crate) fn installations(
    manifests: &[PluginManifest],
    errors: &[PluginDiscoveryError],
    plugins_dir: &Path,
    subscriptions: &[Subscription],
) -> Result<Vec<Installation>, PluginRepoError> {
    let mut load: BTreeMap<String, LoadState> = manifests
        .iter()
        .map(|manifest| (manifest.id.clone(), LoadState::Loaded))
        .collect();
    for directory in plugin_directories(plugins_dir)? {
        let reason = errors
            .iter()
            .find(|error| error.directories.contains(&directory))
            .map_or_else(|| NOT_DISCOVERED.to_string(), ToString::to_string);
        load.entry(directory)
            .or_insert(LoadState::NotLoaded { reason });
    }

    let mut records: BTreeMap<&str, std::io::Result<Option<SourceRecord>>> = load
        .keys()
        .map(|id| (id.as_str(), SourceRecord::load(&plugins_dir.join(id))))
        .collect();
    let managed: Vec<(String, SourceRecord)> = records
        .iter()
        .filter_map(|(id, record)| Some((id.to_string(), record.as_ref().ok()?.clone()?)))
        .collect();
    let mut statuses =
        installed_statuses(&managed, subscriptions, &installed_manifests(plugins_dir));

    let installations = load
        .iter()
        .map(|(id, state)| {
            let record = records.remove(id.as_str()).unwrap_or(Ok(None));
            let manifest = match state {
                LoadState::Loaded => manifests
                    .iter()
                    .find(|manifest| manifest.id == *id)
                    .cloned(),
                LoadState::NotLoaded { .. } => manifest_on_disk(&plugins_dir.join(id)),
            };
            let recorded = record
                .as_ref()
                .ok()
                .and_then(Option::as_ref)
                .map(|record| record.version.to_string());
            let (name, version, description) = match manifest {
                Some(manifest) => (
                    manifest.name,
                    Some(manifest.version.to_string()),
                    manifest.description,
                ),
                None => (id.clone(), recorded, String::new()),
            };
            Installation {
                name,
                version,
                description,
                load: state.clone(),
                source: match record {
                    Ok(Some(record)) => match statuses.remove(id) {
                        Some(status) => InstallSource::Repository(Box::new(RepositoryInstall {
                            record,
                            status,
                        })),
                        None => InstallSource::Local,
                    },
                    Ok(None) => InstallSource::Local,
                    Err(error) => InstallSource::Invalid(error),
                },
                id: id.clone(),
            }
        })
        .collect();
    Ok(installations)
}

/// The directories in `plugins_dir` that discovery takes for plugins.
fn plugin_directories(plugins_dir: &Path) -> Result<BTreeSet<String>, PluginRepoError> {
    let entries = match std::fs::read_dir(plugins_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(BTreeSet::new());
        }
        Err(error) => return Err(PluginRepoError::Internal(error.into())),
    };
    let mut directories = BTreeSet::new();
    for entry in entries {
        let entry = entry.map_err(|error| PluginRepoError::Internal(error.into()))?;
        if let Ok(name) = entry.file_name().into_string()
            && !name.starts_with('.')
            && entry.path().is_dir()
        {
            directories.insert(name);
        }
    }
    Ok(directories)
}

/// The manifest in the plugin directory `dir`, whatever server it asks
/// for, when it parses and is no larger than a manifest may be.
fn manifest_on_disk(dir: &Path) -> Option<PluginManifest> {
    let path = dir.join(PLUGIN_CONFIG_FILENAME);
    if std::fs::metadata(&path).ok()?.len() > PLUGIN_CONFIG_MAX_BYTES {
        return None;
    }
    let config = std::fs::read_to_string(path).ok()?;
    PluginManifest::from_json(&config, &crate::plugins::plugin_host()).ok()
}

#[cfg(test)]
pub(crate) mod tests {
    use harmony_core::plugin::PluginManager;
    use lyra_db::plugin_repositories as repo_db;

    use super::{
        super::{
            UpdateStatus,
            tests::{
                install_records,
                manifest_json,
            },
        },
        *,
    };
    use crate::plugins::testing::HOST_REQUIREMENT;

    pub(crate) const ORIGIN: &str = "https://github.com/o/r";

    /// What a discovery of `plugins_dir` loads and leaves out.
    pub(crate) fn discover(
        plugins_dir: &Path,
    ) -> anyhow::Result<(Vec<PluginManifest>, Vec<PluginDiscoveryError>)> {
        let mut manager = PluginManager::new(plugins_dir.to_path_buf());
        let errors = manager.discover_plugins(&crate::plugins::plugin_host())?;
        let manifests = manager
            .list_plugins()
            .map(|plugin| plugin.manifest.clone())
            .collect();
        Ok((manifests, errors))
    }

    /// A subscription to [`ORIGIN`] that offers each of `ids` at 2.0.0.
    pub(crate) fn offering(ids: &[&str]) -> Subscription {
        Subscription {
            repository: repo_db::PluginRepository {
                db_id: None,
                id: "catalog".into(),
                origin: ORIGIN.into(),
                name: "Catalog".into(),
                description: String::new(),
                git_ref: None,
                resolved_ref: None,
                commit: None,
                refreshed_at_ms: Some(1),
            },
            entries: ids
                .iter()
                .map(|id| repo_db::PluginRepositoryEntry {
                    db_id: None,
                    plugin_id: id.to_string(),
                    subpath: Some(format!("plugins/{id}")),
                    source_origin: None,
                    name: None,
                    description: None,
                    version: Some("2.0.0".into()),
                    commit: Some("b".repeat(40)),
                    scopes: Some(Vec::new()),
                    scope_reasons: None,
                    scope_grants: None,
                    dependencies: None,
                    position: None,
                    requires: None,
                    error: None,
                })
                .collect(),
        }
    }

    /// Replaces the manifest of the plugin installed as `id`.
    pub(crate) fn write_manifest(plugins_dir: &Path, id: &str, manifest: &str) {
        std::fs::write(plugins_dir.join(id).join(PLUGIN_CONFIG_FILENAME), manifest)
            .expect("write manifest");
    }

    fn update_status(installation: &Installation) -> Option<&UpdateStatus> {
        match &installation.source {
            InstallSource::Repository(install) => Some(&install.status.status),
            _ => None,
        }
    }

    fn reason(installation: &Installation) -> &str {
        match &installation.load {
            LoadState::NotLoaded { reason } => reason,
            LoadState::Loaded => panic!("{} loaded", installation.id),
        }
    }

    #[test]
    fn plugins_refused_for_the_server_version_are_listed_with_the_reason_and_their_update()
    -> anyhow::Result<()> {
        let plugins_dir = tempfile::TempDir::new()?;
        install_records(plugins_dir.path(), ORIGIN, &[("refused", "1.0.0")])?;
        let mut manifest: serde_json::Value =
            serde_json::from_str(&manifest_json("refused", "1.0.0", "=0.0.0"))?;
        manifest["name"] = "Refused".into();
        manifest["description"] = "Its description.".into();
        write_manifest(plugins_dir.path(), "refused", &manifest.to_string());
        let (manifests, errors) = discover(plugins_dir.path())?;

        let listed = installations(
            &manifests,
            &errors,
            plugins_dir.path(),
            &[offering(&["refused"])],
        )?;

        let [refused] = &listed[..] else {
            panic!("{listed:?}");
        };
        assert_eq!(
            reason(refused),
            format!(
                "plugin 'refused' requires lyra =0.0.0, but this is lyra {}",
                env!("CARGO_PKG_VERSION")
            )
        );
        assert_eq!(
            (
                refused.name.as_str(),
                refused.version.as_deref(),
                refused.description.as_str()
            ),
            ("Refused", Some("1.0.0"), "Its description.")
        );
        let InstallSource::Repository(install) = &refused.source else {
            panic!("{refused:?}");
        };
        let status = &install.status;
        assert_eq!(status.status, UpdateStatus::UpdateAvailable);
        assert_eq!(
            status.offered.as_ref().map(|release| &release.version),
            Some(&semver::Version::new(2, 0, 0))
        );
        Ok(())
    }

    #[test]
    fn repository_plugins_are_listed_from_their_record_when_their_manifest_cannot_be_read()
    -> anyhow::Result<()> {
        let plugins_dir = tempfile::TempDir::new()?;
        install_records(
            plugins_dir.path(),
            ORIGIN,
            &[("gone", "1.2.0"), ("unreadable", "1.0.0")],
        )?;
        std::fs::remove_file(plugins_dir.path().join("gone").join(PLUGIN_CONFIG_FILENAME))?;
        write_manifest(plugins_dir.path(), "unreadable", r#"{"schema_version": 9"#);
        let (manifests, errors) = discover(plugins_dir.path())?;

        let listed = installations(
            &manifests,
            &errors,
            plugins_dir.path(),
            &[offering(&["gone", "unreadable"])],
        )?;

        let ids: Vec<_> = listed.iter().map(|plugin| plugin.id.as_str()).collect();
        assert_eq!(ids, ["gone", "unreadable"]);
        for (plugin, version, why) in [
            (&listed[0], "1.2.0", "not found"),
            (&listed[1], "1.0.0", "failed to parse"),
        ] {
            assert!(reason(plugin).contains(why), "{}", reason(plugin));
            assert_eq!(plugin.name, plugin.id);
            assert_eq!(plugin.version.as_deref(), Some(version));
            assert_eq!(plugin.description, "");
            assert_eq!(update_status(plugin), Some(&UpdateStatus::UpdateAvailable));
        }
        Ok(())
    }

    #[test]
    fn loaded_local_and_undiscovered_plugins_are_listed_together() -> anyhow::Result<()> {
        let plugins_dir = tempfile::TempDir::new()?;
        install_records(plugins_dir.path(), ORIGIN, &[("loaded", "1.0.0")])?;
        std::fs::write(plugins_dir.path().join("loaded").join("init.luau"), "")?;
        for local in ["handmade", "dependent"] {
            std::fs::create_dir(plugins_dir.path().join(local))?;
        }
        write_manifest(plugins_dir.path(), "handmade", "{");
        let mut dependent: serde_json::Value =
            serde_json::from_str(&manifest_json("dependent", "0.3.0", HOST_REQUIREMENT))?;
        dependent["dependencies"] = serde_json::json!({ "handmade": "1" });
        std::fs::write(plugins_dir.path().join("dependent").join("init.luau"), "")?;
        write_manifest(plugins_dir.path(), "dependent", &dependent.to_string());
        let (manifests, errors) = discover(plugins_dir.path())?;
        install_records(plugins_dir.path(), ORIGIN, &[("later", "1.0.0")])?;
        std::fs::create_dir(plugins_dir.path().join("copied"))?;
        std::fs::create_dir(plugins_dir.path().join(".staging"))?;

        let listed = installations(&manifests, &errors, plugins_dir.path(), &[])?;

        let ids: Vec<_> = listed.iter().map(|plugin| plugin.id.as_str()).collect();
        assert_eq!(ids, ["copied", "dependent", "handmade", "later", "loaded"]);
        let [copied, dependent, handmade, later, loaded] = &listed[..] else {
            unreachable!()
        };
        assert_eq!(reason(copied), NOT_DISCOVERED);
        assert!(matches!(copied.source, InstallSource::Local));
        assert_eq!(
            reason(dependent),
            "plugin 'dependent' requires plugin 'handmade' ^1, but it is not installed or failed \
             to load"
        );
        assert_eq!(dependent.version.as_deref(), Some("0.3.0"));
        assert!(matches!(dependent.source, InstallSource::Local));
        assert!(reason(handmade).contains("failed to parse"));
        assert_eq!(handmade.version, None);
        assert!(matches!(handmade.source, InstallSource::Local));
        assert_eq!(reason(later), NOT_DISCOVERED);
        assert!(update_status(later).is_some());
        assert_eq!(loaded.load, LoadState::Loaded);
        assert!(update_status(loaded).is_some());
        Ok(())
    }

    #[test]
    fn plugins_removed_since_they_were_left_out_are_not_listed() -> anyhow::Result<()> {
        let plugins_dir = tempfile::TempDir::new()?;
        install_records(plugins_dir.path(), ORIGIN, &[("removed", "1.0.0")])?;
        write_manifest(plugins_dir.path(), "removed", "{");
        let (manifests, errors) = discover(plugins_dir.path())?;
        std::fs::remove_dir_all(plugins_dir.path().join("removed"))?;

        let listed = installations(&manifests, &errors, plugins_dir.path(), &[])?;

        assert!(listed.is_empty(), "{listed:?}");
        Ok(())
    }

    #[test]
    fn manifests_larger_than_discovery_reads_describe_nothing() -> anyhow::Result<()> {
        let plugins_dir = tempfile::TempDir::new()?;
        install_records(plugins_dir.path(), ORIGIN, &[("oversized", "1.0.0")])?;
        let mut manifest: serde_json::Value =
            serde_json::from_str(&manifest_json("oversized", "1.0.0", "=0.0.0"))?;
        manifest["description"] = "d".repeat(PLUGIN_CONFIG_MAX_BYTES as usize).into();
        write_manifest(plugins_dir.path(), "oversized", &manifest.to_string());
        let (manifests, errors) = discover(plugins_dir.path())?;

        let listed = installations(&manifests, &errors, plugins_dir.path(), &[])?;

        let [oversized] = &listed[..] else {
            panic!("{listed:?}");
        };
        assert_eq!(oversized.name, "oversized");
        assert_eq!(oversized.description, "");
        assert_eq!(oversized.version.as_deref(), Some("1.0.0"));
        Ok(())
    }
}
