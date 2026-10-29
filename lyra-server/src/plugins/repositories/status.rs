// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Where installed plugins stand against the releases their subscribed
//! repositories offer, and why an update to one waits for an admin.

use std::{
    collections::{
        BTreeMap,
        BTreeSet,
        HashMap,
    },
    path::Path,
};

use harmony_core::plugin::{
    Dependency,
    PLUGIN_CONFIG_FILENAME,
    PluginManifest,
};
use harmony_repository::{
    ListedPlugin,
    SourceRecord,
};
use lyra_db::plugin_repositories as repo_db;
use semver::{
    Version,
    VersionReq,
};

use super::{
    Subscription,
    find_subscription,
};

/// Whether a repository-managed plugin has a newer release to install.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum UpdateStatus {
    /// No release newer than the installed one runs on this server, or
    /// pinned.
    UpToDate,
    /// A newer release runs on this server.
    UpdateAvailable,
    /// No release the repository offers the plugin runs on this server;
    /// `requires` is the server requirement that excludes it.
    Incompatible { requires: String },
    /// The releases on offer are unknown.
    Unknown,
}

impl UpdateStatus {
    pub(super) fn compare(installed: &Version, offered: &Offered) -> Self {
        match offered {
            Ok(offered) if offered.version > *installed => Self::UpdateAvailable,
            Ok(_) => Self::UpToDate,
            Err(Some(requires)) => Self::Incompatible {
                requires: requires.clone(),
            },
            Err(None) => Self::Unknown,
        }
    }
}

/// The release a repository offers one plugin on this server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OfferedRelease {
    /// The plugin's version at the release.
    pub(crate) version: Version,
    pub(crate) commit: String,
    /// The scopes the plugin declares at the release.
    pub(crate) scopes: Vec<String>,
    /// The plugins it depends on at the release.
    pub(crate) dependencies: BTreeMap<String, Dependency>,
}

/// What a repository offers one plugin on this server: the release an
/// install would choose, or, without one, the server requirement that
/// excludes this server when that is the reason.
pub(super) type Offered = Result<OfferedRelease, Option<String>>;

pub(super) fn listed_offer(plugin: &ListedPlugin) -> Offered {
    match &plugin.offer {
        Ok(offer) => Ok(OfferedRelease {
            version: offer.manifest.version.clone(),
            commit: offer.commit.clone(),
            scopes: offer.manifest.scopes.clone(),
            dependencies: offer.manifest.dependencies.clone(),
        }),
        Err(error) => Err(error.required_host().map(ToString::to_string)),
    }
}

pub(super) fn stored_offer(entry: &repo_db::PluginRepositoryEntry) -> Offered {
    match (&entry.version, &entry.commit, &entry.requires) {
        (Some(version), Some(commit), _) => {
            let dependencies = entry
                .dependencies
                .iter()
                .flatten()
                .map(|dependency| {
                    let version = VersionReq::parse(&dependency.version).map_err(|_| None)?;
                    let optional = dependency.optional;
                    Ok((
                        dependency.plugin_id.clone(),
                        Dependency { version, optional },
                    ))
                })
                .collect::<Result<_, Option<String>>>()?;
            Ok(OfferedRelease {
                version: Version::parse(version).map_err(|_| None)?,
                commit: commit.clone(),
                scopes: entry.scopes.clone().unwrap_or_default(),
                dependencies,
            })
        }
        (None, _, requires) => Err(requires.clone()),
        (Some(_), None, _) => Err(None),
    }
}

/// The dependencies a subscription stores for `offered`.
pub(super) fn stored_dependencies(offered: &OfferedRelease) -> Vec<repo_db::PluginDependency> {
    offered
        .dependencies
        .iter()
        .map(|(plugin_id, dependency)| repo_db::PluginDependency {
            plugin_id: plugin_id.clone(),
            version: dependency.version.to_string(),
            optional: dependency.optional,
        })
        .collect()
}

/// Why an update to a newer release waits for an admin.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Hold {
    /// The release declares scopes the installed plugin does not; an
    /// update that names the release grants them.
    AddsScopes { scopes: Vec<String> },
    /// Plugins depend on this one at versions outside the release's; no
    /// update applies until they change.
    BreaksDependents { dependents: Vec<Requirement> },
    /// The release depends on plugins that would be missing or at
    /// versions outside its requirements; no update applies until they
    /// change.
    UnmetDependencies { dependencies: Vec<Requirement> },
    /// The release would leave `plugins`, sorted, depending on each other
    /// in a cycle, which none of them loads in; no update applies until
    /// they change.
    DependencyCycle { plugins: Vec<String> },
}

impl std::fmt::Display for Hold {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let requirements = |requirements: &[Requirement]| {
            requirements
                .iter()
                .map(|requirement| format!("{} {}", requirement.id, requirement.requires))
                .collect::<Vec<_>>()
                .join(", ")
        };
        match self {
            Self::AddsScopes { scopes } => write!(f, "adds scopes {}", scopes.join(", ")),
            Self::BreaksDependents { dependents } => {
                write!(f, "breaks dependents {}", requirements(dependents))
            }
            Self::UnmetDependencies { dependencies } => {
                write!(f, "needs {}", requirements(dependencies))
            }
            Self::DependencyCycle { plugins } => {
                write!(f, "forms a dependency cycle of {}", plugins.join(", "))
            }
        }
    }
}

/// One plugin's version requirement on another: `id` is the dependent in
/// [`Hold::BreaksDependents`] and the dependency in
/// [`Hold::UnmetDependencies`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Requirement {
    pub(crate) id: String,
    pub(crate) requires: String,
}

/// A newer release an update would install, for [`holds`].
pub(super) struct Candidate<'a> {
    pub(super) id: &'a str,
    pub(super) release: &'a OfferedRelease,
    /// Whether the request named the release, granting the scopes it adds.
    pub(super) approved: bool,
}

/// The scopes `release` declares that the installed `plugin_id` does not;
/// all of them when it is not installed.
pub(super) fn added_scopes(
    plugin_id: &str,
    release: &OfferedRelease,
    installed: &[PluginManifest],
) -> Vec<String> {
    let current = installed.iter().find(|manifest| manifest.id == plugin_id);
    release
        .scopes
        .iter()
        .filter(|scope| current.is_none_or(|manifest| !manifest.scopes.contains(scope)))
        .cloned()
        .collect()
}

/// Which of `candidates`, installed together over the `installed`
/// plugins, wait, and why. Every candidate is judged against the plugins
/// as they would be afterwards, cycles among them included: the installed
/// ones with each candidate not held replaced by its release. Holding one
/// can break another, so this repeats until no further candidate is held.
/// A dependency problem takes precedence over added scopes, which approval
/// cannot resolve.
pub(super) fn holds(
    installed: &[PluginManifest],
    candidates: &[Candidate<'_>],
) -> HashMap<String, Hold> {
    let mut held: HashMap<String, Hold> = candidates
        .iter()
        .filter(|candidate| !candidate.approved)
        .filter_map(|candidate| {
            let scopes = added_scopes(candidate.id, candidate.release, installed);
            (!scopes.is_empty()).then(|| (candidate.id.to_string(), Hold::AddsScopes { scopes }))
        })
        .collect();
    loop {
        let mut after: BTreeMap<&str, (&Version, &BTreeMap<String, Dependency>)> = installed
            .iter()
            .map(|manifest| {
                (
                    manifest.id.as_str(),
                    (&manifest.version, &manifest.dependencies),
                )
            })
            .collect();
        for candidate in candidates {
            if !held.contains_key(candidate.id) {
                let release = candidate.release;
                after.insert(candidate.id, (&release.version, &release.dependencies));
            }
        }
        let mut changed = Vec::new();
        for candidate in candidates {
            if matches!(held.get(candidate.id), None | Some(Hold::AddsScopes { .. }))
                && let Some(hold) = dependency_hold(candidate, &after)
            {
                changed.push((candidate.id.to_string(), hold));
            }
        }
        if changed.is_empty() {
            return held;
        }
        held.extend(changed);
    }
}

/// Why installing `candidate` would leave `after` with a plugin that
/// cannot load.
fn dependency_hold(
    candidate: &Candidate<'_>,
    after: &BTreeMap<&str, (&Version, &BTreeMap<String, Dependency>)>,
) -> Option<Hold> {
    let release = candidate.release;
    let dependents: Vec<Requirement> = after
        .iter()
        .filter(|(id, _)| **id != candidate.id)
        .filter_map(|(id, (_, dependencies))| {
            let dependency = dependencies.get(candidate.id)?;
            (!dependency.version.matches(&release.version)).then(|| Requirement {
                id: id.to_string(),
                requires: dependency.version.to_string(),
            })
        })
        .collect();
    if !dependents.is_empty() {
        return Some(Hold::BreaksDependents { dependents });
    }
    let dependencies: Vec<Requirement> = release
        .dependencies
        .iter()
        .filter(|(id, dependency)| match after.get(id.as_str()) {
            Some((version, _)) => !dependency.version.matches(version),
            None => !dependency.optional,
        })
        .map(|(id, dependency)| Requirement {
            id: id.clone(),
            requires: dependency.version.to_string(),
        })
        .collect();
    if !dependencies.is_empty() {
        return Some(Hold::UnmetDependencies { dependencies });
    }
    let cycle = cycle_through(candidate.id, after);
    (!cycle.is_empty()).then_some(Hold::DependencyCycle { plugins: cycle })
}

/// The plugins of `after`, sorted, that `id` depends on and that depend on
/// it, `id` among them, when it is in a cycle; a dependency counts once
/// it is present, required or not, as it does when plugins load.
fn cycle_through(
    id: &str,
    after: &BTreeMap<&str, (&Version, &BTreeMap<String, Dependency>)>,
) -> Vec<String> {
    let reached = |from: &str| {
        let mut reached = BTreeSet::new();
        let mut stack = vec![from];
        while let Some(current) = stack.pop() {
            for dependency in after[current].1.keys() {
                if let Some((dependency, _)) = after.get_key_value(dependency.as_str())
                    && reached.insert(*dependency)
                {
                    stack.push(dependency);
                }
            }
        }
        reached
    };
    let from_id = reached(id);
    if !from_id.contains(id) {
        return Vec::new();
    }
    from_id
        .into_iter()
        .filter(|plugin| *plugin == id || reached(plugin).contains(id))
        .map(ToString::to_string)
        .collect()
}

/// The manifests of the plugins in `plugins_dir`, as installed on disk;
/// unreadable ones are left out.
pub(crate) fn installed_manifests(plugins_dir: &Path) -> Vec<PluginManifest> {
    let Ok(entries) = std::fs::read_dir(plugins_dir) else {
        return Vec::new();
    };
    let host = crate::plugins::plugin_host();
    entries
        .filter_map(Result::ok)
        .filter(|entry| !entry.file_name().to_string_lossy().starts_with('.'))
        .filter_map(|entry| {
            let config = std::fs::read_to_string(entry.path().join(PLUGIN_CONFIG_FILENAME)).ok()?;
            PluginManifest::from_json(&config, &host).ok()
        })
        .collect()
}

/// Where an installed plugin stands against its subscribed repository.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InstalledStatus {
    pub(crate) status: UpdateStatus,
    /// The release the repository offers, when it offers one.
    pub(crate) offered: Option<OfferedRelease>,
    /// Why an available update waits for an admin.
    pub(crate) hold: Option<Hold>,
}

/// Where each installed plugin in `records` stands relative to the
/// subscribed repository it was installed from, using only the releases
/// the subscriptions stored and the `installed` manifests: no forge call is
/// made. Pinned plugins are always up to date. Without a subscription for
/// the same origin and ref that lists a plugin, the answer is `Unknown`.
/// Holds are judged as if every available update were installed together.
pub(crate) fn installed_statuses(
    records: &[(String, SourceRecord)],
    subscriptions: &[Subscription],
    installed: &[PluginManifest],
) -> HashMap<String, InstalledStatus> {
    let mut statuses: HashMap<String, InstalledStatus> = records
        .iter()
        .map(|(id, record)| (id.clone(), offered_status(id, record, subscriptions)))
        .collect();
    let candidates: Vec<Candidate<'_>> = statuses
        .iter()
        .filter(|(_, status)| status.status == UpdateStatus::UpdateAvailable)
        .filter_map(|(id, status)| {
            Some(Candidate {
                id,
                release: status.offered.as_ref()?,
                approved: false,
            })
        })
        .collect();
    let mut held = holds(installed, &candidates);
    for (id, status) in &mut statuses {
        status.hold = held.remove(id);
    }
    statuses
}

fn offered_status(
    plugin_id: &str,
    record: &SourceRecord,
    subscriptions: &[Subscription],
) -> InstalledStatus {
    let unknown = InstalledStatus {
        status: UpdateStatus::Unknown,
        offered: None,
        hold: None,
    };
    if record.pinned {
        return InstalledStatus {
            status: UpdateStatus::UpToDate,
            ..unknown
        };
    }
    let Some(entry) = find_subscription(subscriptions, &record.origin, record.git_ref.as_deref())
        .and_then(|subscription| {
            subscription
                .entries
                .iter()
                .find(|entry| entry.plugin_id == plugin_id && entry.subpath == record.subpath)
        })
    else {
        return unknown;
    };
    let offered = stored_offer(entry);
    InstalledStatus {
        status: UpdateStatus::compare(&record.version, &offered),
        offered: offered.ok(),
        hold: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dependencies(requirements: &[(&str, &str)]) -> BTreeMap<String, Dependency> {
        requirements
            .iter()
            .map(|(id, requires)| {
                let version = VersionReq::parse(requires).expect("requirement");
                (
                    id.to_string(),
                    Dependency {
                        version,
                        optional: false,
                    },
                )
            })
            .collect()
    }

    fn manifest(id: &str, version: &str, requires: &[(&str, &str)]) -> PluginManifest {
        PluginManifest {
            schema_version: 1,
            id: id.into(),
            name: id.into(),
            version: Version::parse(version).expect("version"),
            host_requirement: VersionReq::STAR,
            description: String::new(),
            entrypoint: None,
            scopes: Vec::new(),
            dependencies: dependencies(requires),
        }
    }

    fn release(version: &str, requires: &[(&str, &str)]) -> OfferedRelease {
        OfferedRelease {
            version: Version::parse(version).expect("version"),
            commit: "2".repeat(40),
            scopes: Vec::new(),
            dependencies: dependencies(requires),
        }
    }

    fn requirement(id: &str, requires: &str) -> Requirement {
        Requirement {
            id: id.into(),
            requires: requires.into(),
        }
    }

    /// The holds of `releases`, installed together over `installed`.
    fn held(
        installed: &[PluginManifest],
        releases: &[(&str, &OfferedRelease)],
    ) -> BTreeMap<String, Hold> {
        let candidates: Vec<_> = releases
            .iter()
            .map(|(id, release)| Candidate {
                id,
                release,
                approved: false,
            })
            .collect();
        holds(installed, &candidates).into_iter().collect()
    }

    #[test]
    fn releases_that_need_each_other_install_together() {
        let installed = [
            manifest("a", "1.0.0", &[]),
            manifest("b", "1.0.0", &[("a", "^1")]),
        ];
        let a = release("2.0.0", &[]);
        let b = release("2.0.0", &[("a", "^2")]);

        assert!(held(&installed, &[("a", &a), ("b", &b)]).is_empty());
        assert_eq!(
            held(&installed, &[("a", &a)]),
            [(
                "a".into(),
                Hold::BreaksDependents {
                    dependents: vec![requirement("b", "^1")],
                }
            )]
            .into()
        );
        assert_eq!(
            held(&installed, &[("b", &b)]),
            [(
                "b".into(),
                Hold::UnmetDependencies {
                    dependencies: vec![requirement("a", "^2")],
                }
            )]
            .into()
        );
    }

    #[test]
    fn releases_closing_a_dependency_cycle_are_held() {
        let installed = [
            manifest("a", "1.0.0", &[("b", "^1")]),
            manifest("b", "1.0.0", &[]),
            manifest("c", "1.0.0", &[("a", "^1")]),
        ];
        let b = release("1.1.0", &[("c", "^1")]);
        let mut optional = b.clone();
        optional
            .dependencies
            .values_mut()
            .for_each(|dependency| dependency.optional = true);
        let cycle = Hold::DependencyCycle {
            plugins: vec!["a".into(), "b".into(), "c".into()],
        };

        assert_eq!(
            held(&installed, &[("b", &b)]),
            [("b".into(), cycle.clone())].into()
        );
        assert_eq!(
            held(&installed, &[("b", &optional)]),
            [("b".into(), cycle)].into(),
            "an optional dependency that is present orders loading too"
        );
        let a = release("1.1.0", &[]);
        assert!(
            held(&installed, &[("a", &a), ("b", &b)]).is_empty(),
            "b closes no cycle once a no longer depends on it"
        );
        let breaks = release("2.0.0", &[]);
        assert_eq!(
            held(&installed, &[("a", &breaks), ("b", &b)]),
            [
                (
                    "a".into(),
                    Hold::BreaksDependents {
                        dependents: vec![requirement("c", "^1")],
                    }
                ),
                (
                    "b".into(),
                    Hold::DependencyCycle {
                        plugins: vec!["a".into(), "b".into(), "c".into()],
                    }
                ),
            ]
            .into(),
            "holding a keeps its dependency on b, which b's release closes"
        );
    }

    #[test]
    fn holding_a_release_holds_the_releases_that_need_it() {
        let installed = [
            manifest("a", "1.0.0", &[]),
            manifest("b", "1.0.0", &[]),
            manifest("c", "1.0.0", &[("a", "^1")]),
        ];
        let a = release("2.0.0", &[]);
        let b = release("2.0.0", &[("a", "^2")]);

        assert_eq!(
            held(&installed, &[("a", &a), ("b", &b)]),
            [
                (
                    "a".into(),
                    Hold::BreaksDependents {
                        dependents: vec![requirement("c", "^1")],
                    }
                ),
                (
                    "b".into(),
                    Hold::UnmetDependencies {
                        dependencies: vec![requirement("a", "^2")],
                    }
                ),
            ]
            .into()
        );
    }

    #[test]
    fn releases_needing_a_missing_plugin_are_held_unless_it_is_optional() {
        let installed = [manifest("a", "1.0.0", &[])];
        let needs = release("2.0.0", &[("missing", "^1")]);
        let mut optional = needs.clone();
        optional
            .dependencies
            .values_mut()
            .for_each(|dependency| dependency.optional = true);

        assert_eq!(
            held(&installed, &[("a", &needs)]),
            [(
                "a".into(),
                Hold::UnmetDependencies {
                    dependencies: vec![requirement("missing", "^1")],
                }
            )]
            .into()
        );
        assert!(held(&installed, &[("a", &optional)]).is_empty());
    }

    #[test]
    fn dependency_problems_outrank_added_scopes() {
        let installed = [
            manifest("a", "1.0.0", &[]),
            manifest("b", "1.0.0", &[("a", "^1")]),
        ];
        let mut a = release("2.0.0", &[]);
        a.scopes = vec!["lyra.metadata".into()];

        assert_eq!(
            held(&installed, &[("a", &a)]),
            [(
                "a".into(),
                Hold::BreaksDependents {
                    dependents: vec![requirement("b", "^1")],
                }
            )]
            .into()
        );
        let mut alone = installed.to_vec();
        alone.pop();
        assert_eq!(
            held(&alone, &[("a", &a)]),
            [(
                "a".into(),
                Hold::AddsScopes {
                    scopes: vec!["lyra.metadata".into()],
                }
            )]
            .into()
        );
    }
}
