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

use harmony_core::{
    plugin::{
        Dependency,
        PLUGIN_CONFIG_FILENAME,
        PluginManifest,
    },
    scopes::ScopedPlugin,
};
use harmony_repository::{
    ListedPlugin,
    SourceRecord,
};
use lyra_db::plugin_repositories as repo_db;

use crate::plugins::scopes::{
    ScopeGain,
    scope_gains,
};
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
    /// The releases on offer are unknown, for `reason`.
    Unknown { reason: String },
}

impl UpdateStatus {
    pub(super) fn compare(installed: &Version, offered: &Offered) -> Self {
        match offered {
            Ok(offered) if offered.version > *installed => Self::UpdateAvailable,
            Ok(_) => Self::UpToDate,
            Err(Unoffered::Incompatible { requires }) => Self::Incompatible {
                requires: requires.clone(),
            },
            Err(Unoffered::Unknown { reason }) => Self::Unknown {
                reason: reason.clone(),
            },
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

/// Why a repository offers one plugin no release on this server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Unoffered {
    /// `requires` is the server requirement that excludes this server.
    Incompatible {
        requires: String,
    },
    Unknown {
        reason: String,
    },
}

/// What a repository offers one plugin on this server: the release an
/// install would choose, or why there is none.
pub(super) type Offered = Result<OfferedRelease, Unoffered>;

/// The reason given for an entry stored without one, before entries
/// carried it.
pub(super) const UNSTORED_REASON: &str =
    "the repository offers no release of the plugin; refresh it for the reason";

pub(super) fn listed_offer(plugin: &ListedPlugin) -> Offered {
    match &plugin.offer {
        Ok(offer) => Ok(OfferedRelease {
            version: offer.manifest.version.clone(),
            commit: offer.commit.clone(),
            scopes: offer.manifest.scopes.clone(),
            dependencies: offer.manifest.dependencies.clone(),
        }),
        Err(error) => Err(match error.required_host() {
            Some(requires) => Unoffered::Incompatible {
                requires: requires.to_string(),
            },
            None => Unoffered::Unknown {
                reason: error.to_string(),
            },
        }),
    }
}

pub(super) fn stored_offer(entry: &repo_db::PluginRepositoryEntry) -> Offered {
    let (Some(version), Some(commit)) = (&entry.version, &entry.commit) else {
        return Err(match &entry.requires {
            Some(requires) => Unoffered::Incompatible {
                requires: requires.clone(),
            },
            None => Unoffered::Unknown {
                reason: stored_error(entry),
            },
        });
    };
    let unreadable = |_| Unoffered::Unknown {
        reason: "the stored release cannot be read; refresh the repository".to_string(),
    };
    let dependencies = entry
        .dependencies
        .iter()
        .flatten()
        .map(|dependency| {
            let version = VersionReq::parse(&dependency.version).map_err(unreadable)?;
            let optional = dependency.optional;
            Ok((
                dependency.plugin_id.clone(),
                Dependency { version, optional },
            ))
        })
        .collect::<Result<_, Unoffered>>()?;
    Ok(OfferedRelease {
        version: Version::parse(version).map_err(unreadable)?,
        commit: commit.clone(),
        scopes: entry.scopes.clone().unwrap_or_default(),
        dependencies,
    })
}

/// Why the stored `entry` has no release, as its listing gave it.
pub(super) fn stored_error(entry: &repo_db::PluginRepositoryEntry) -> String {
    entry
        .error
        .clone()
        .unwrap_or_else(|| UNSTORED_REASON.to_string())
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
    /// The release lets plugins reach scopes they could not before: the
    /// updated plugin, or installed plugins depending on it, keyed by id.
    /// An update that names the release grants them.
    AddsScopes { gains: BTreeMap<String, ScopeGain> },
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
            Self::AddsScopes { gains } => {
                let reaches = gains
                    .iter()
                    .map(|(id, ScopeGain { growth, .. })| {
                        let unknown = growth
                            .unresolved
                            .iter()
                            .map(|dependency| format!("unknown dependency {dependency}"));
                        let reached: Vec<_> =
                            growth.scopes.iter().cloned().chain(unknown).collect();
                        format!("{id} ({})", reached.join(", "))
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "lets plugins reach new scopes: {reaches}")
            }
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
    /// Whether the request named the release, granting the scopes it lets
    /// plugins reach.
    pub(super) approved: bool,
}

/// A plugin as [`holds`] and previews weigh what it reaches: installed,
/// or at a release.
#[derive(Clone, Copy)]
pub(super) enum Reach<'a> {
    Installed(&'a PluginManifest),
    Release(&'a str, &'a OfferedRelease),
}

impl ScopedPlugin for Reach<'_> {
    fn id(&self) -> &str {
        match self {
            Self::Installed(manifest) => &manifest.id,
            Self::Release(id, _) => id,
        }
    }

    fn scopes(&self) -> &[String] {
        match self {
            Self::Installed(manifest) => &manifest.scopes,
            Self::Release(_, release) => &release.scopes,
        }
    }

    fn dependency_ids(&self) -> impl Iterator<Item = &str> {
        let dependencies = match self {
            Self::Installed(manifest) => &manifest.dependencies,
            Self::Release(_, release) => &release.dependencies,
        };
        dependencies.keys().map(String::as_str)
    }
}

/// What installing `candidate` over `plugins`, which may hold other
/// updates, lets each plugin newly reach, as a hold when anything.
fn scope_hold(
    installed: &[Reach<'_>],
    plugins: &[Reach<'_>],
    candidate: &Candidate<'_>,
) -> Option<Hold> {
    let gains = scope_gains(
        installed,
        plugins,
        &Reach::Release(candidate.id, candidate.release),
    );
    (!gains.is_empty()).then_some(Hold::AddsScopes { gains })
}

/// Which of `candidates`, installed together over the `installed`
/// plugins, wait, and why. A candidate not approved waits when it lets a
/// plugin reach new scopes, against the installed plugins or against the
/// plugins as they would be afterwards: the installed ones with each other
/// candidate not held replaced by its release. Dependencies are judged
/// against the latter, cycles among them included. Holding one can change
/// what another reaches or break it, so this repeats until no further
/// candidate is held. A dependency problem takes precedence over added
/// scopes, which approval cannot resolve.
pub(super) fn holds(
    installed: &[PluginManifest],
    candidates: &[Candidate<'_>],
) -> HashMap<String, Hold> {
    let current: Vec<Reach<'_>> = installed.iter().map(Reach::Installed).collect();
    let mut held: HashMap<String, Hold> = candidates
        .iter()
        .filter(|candidate| !candidate.approved)
        .filter_map(|candidate| {
            Some((
                candidate.id.to_string(),
                scope_hold(&current, &current, candidate)?,
            ))
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
            } else if !candidate.approved
                && !held.contains_key(candidate.id)
                && let Some(hold) = scope_hold(
                    &current,
                    &others_after(&current, candidates, &held, candidate),
                    candidate,
                )
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

/// The `current` plugins with every candidate but `candidate` that is not
/// `held` replaced by its release.
fn others_after<'a>(
    current: &[Reach<'a>],
    candidates: &[Candidate<'a>],
    held: &HashMap<String, Hold>,
    candidate: &Candidate<'_>,
) -> Vec<Reach<'a>> {
    let mut plugins = current.to_vec();
    for other in candidates {
        if other.id == candidate.id || held.contains_key(other.id) {
            continue;
        }
        plugins.retain(|plugin| plugin.id() != other.id);
        plugins.push(Reach::Release(other.id, other.release));
    }
    plugins
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
    let status = |status| InstalledStatus {
        status,
        offered: None,
        hold: None,
    };
    if record.pinned {
        return status(UpdateStatus::UpToDate);
    }
    let unknown = |reason: &str| {
        status(UpdateStatus::Unknown {
            reason: reason.to_string(),
        })
    };
    let Some(subscription) =
        find_subscription(subscriptions, &record.origin, record.git_ref.as_deref())
    else {
        return unknown("no subscribed repository has its origin and ref");
    };
    let Some(entry) = subscription.entries.iter().find(|entry| {
        entry.plugin_id == plugin_id
            && entry.subpath == record.subpath
            && entry.source_origin.is_none()
    }) else {
        return unknown(match subscription.repository.refreshed_at_ms {
            None => "its repository has not been refreshed yet",
            Some(_) => "its repository did not list it at the last refresh",
        });
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
    use harmony_core::scopes::ScopeGrowth;

    use super::*;
    use crate::plugins::scopes::{
        Effect,
        Risk,
        RiskAssessment,
    };

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
        held_approving(installed, releases, &[])
    }

    /// [`held`], with the `approved` releases named by the request. Gains
    /// are reduced to what is reached, as [`gain`] builds them.
    fn held_approving(
        installed: &[PluginManifest],
        releases: &[(&str, &OfferedRelease)],
        approved: &[&str],
    ) -> BTreeMap<String, Hold> {
        let candidates: Vec<_> = releases
            .iter()
            .map(|(id, release)| Candidate {
                id,
                release,
                approved: approved.contains(id),
            })
            .collect();
        holds(installed, &candidates)
            .into_iter()
            .map(|(id, hold)| match hold {
                Hold::AddsScopes { gains } => {
                    let gains = gains
                        .into_iter()
                        .map(|(id, gained)| (id, gain(gained.growth)))
                        .collect();
                    (id, Hold::AddsScopes { gains })
                }
                hold => (id, hold),
            })
            .collect()
    }

    fn gain(growth: ScopeGrowth) -> ScopeGain {
        ScopeGain {
            growth,
            effects: Vec::new(),
            risk: RiskAssessment::of(&[]),
            previous_risk: Risk::Standard,
        }
    }

    /// An [`Hold::AddsScopes`] letting each plugin reach its scopes.
    fn adds(gains: &[(&str, &[&str])]) -> Hold {
        Hold::AddsScopes {
            gains: gains
                .iter()
                .map(|(id, scopes)| {
                    let scopes = scopes.iter().map(ToString::to_string).collect();
                    let growth = ScopeGrowth {
                        scopes,
                        ..ScopeGrowth::default()
                    };
                    (id.to_string(), gain(growth))
                })
                .collect(),
        }
    }

    fn with_scopes(mut manifest: PluginManifest, scopes: &[&str]) -> PluginManifest {
        manifest.scopes = scopes.iter().map(ToString::to_string).collect();
        manifest
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
    fn releases_needing_a_missing_plugin_wait_for_it_unless_it_is_optional() {
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
        assert!(
            held_approving(&installed, &[("a", &optional)], &["a"]).is_empty(),
            "a missing optional dependency is no dependency problem"
        );
        assert_eq!(
            held(&installed, &[("a", &optional)]),
            [(
                "a".into(),
                Hold::AddsScopes {
                    gains: [(
                        "a".into(),
                        gain(ScopeGrowth {
                            unresolved: ["missing".into()].into(),
                            ..ScopeGrowth::default()
                        })
                    )]
                    .into(),
                }
            )]
            .into(),
            "what it reaches once installed is unknown"
        );
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
            [("a".into(), adds(&[("a", &["lyra.metadata"])]))].into()
        );
    }

    #[test]
    fn releases_are_held_for_scopes_their_dependents_would_reach() {
        let installed = [
            with_scopes(manifest("a", "1.0.0", &[]), &["lyra.metadata"]),
            with_scopes(manifest("b", "1.0.0", &[("a", "^1")]), &["lyra.favorites"]),
        ];
        let mut a = release("1.1.0", &[]);
        a.scopes = vec!["lyra.metadata".into(), "harmony.http".into()];

        assert_eq!(
            held(&installed, &[("a", &a)]),
            [(
                "a".into(),
                adds(&[("a", &["harmony.http"]), ("b", &["harmony.http"])])
            )]
            .into()
        );
        assert!(held_approving(&installed, &[("a", &a)], &["a"]).is_empty());
    }

    #[test]
    fn releases_are_held_for_scopes_they_would_reach_through_new_dependencies() {
        let installed = [
            manifest("a", "1.0.0", &[]),
            with_scopes(manifest("http", "1.0.0", &[]), &["harmony.http"]),
        ];
        let a = release("1.1.0", &[("http", "^1")]);

        assert_eq!(
            held(&installed, &[("a", &a)]),
            [("a".into(), adds(&[("a", &["harmony.http"])]))].into()
        );
    }

    #[test]
    fn approving_one_release_does_not_approve_what_another_reaches_through_it() {
        let installed = [manifest("a", "1.0.0", &[]), manifest("b", "1.0.0", &[])];
        let a = release("1.1.0", &[("b", "^1")]);
        let mut b = release("1.1.0", &[]);
        b.scopes = vec!["harmony.http".into()];

        assert!(
            held(&installed, &[("a", &a)]).is_empty(),
            "a reaches nothing new through the installed b"
        );
        assert_eq!(
            held_approving(&installed, &[("a", &a), ("b", &b)], &["b"]),
            [("a".into(), adds(&[("a", &["harmony.http"])]))].into()
        );
    }

    #[test]
    fn held_releases_describe_what_each_plugin_newly_reaches_and_its_risk() {
        let installed = [
            manifest("a", "1.0.0", &[]),
            with_scopes(manifest("b", "1.0.0", &[("a", "^1")]), &["lyra.favorites"]),
        ];
        let mut a = release("1.1.0", &[]);
        a.scopes = vec!["harmony.http".into()];
        let candidates = [Candidate {
            id: "a",
            release: &a,
            approved: false,
        }];

        let Some(Hold::AddsScopes { gains }) = holds(&installed, &candidates).remove("a") else {
            panic!("a is held for its scopes");
        };
        let effects = |id: &str| {
            gains[id]
                .effects
                .iter()
                .map(|effect| (effect.effect, effect.plugins()))
                .collect::<Vec<_>>()
        };
        let egress = vec![(Effect::Egress, ["a"].into())];
        assert_eq!(
            (effects("a"), gains["a"].previous_risk, gains["a"].risk.risk),
            (egress.clone(), Risk::Standard, Risk::Standard)
        );
        assert_eq!(
            (effects("b"), gains["b"].previous_risk, gains["b"].risk.risk),
            (egress, Risk::Standard, Risk::Elevated),
            "b sends data off the server through a, and touches its favorites"
        );
    }

    #[test]
    fn previous_risk_is_the_installed_plugins() {
        let installed = [
            with_scopes(manifest("a", "1.0.0", &[("d", "^1")]), &["lyra.favorites"]),
            with_scopes(manifest("c", "1.0.0", &[]), &["harmony.http"]),
            with_scopes(manifest("d", "1.0.0", &[]), &["harmony.http"]),
        ];
        let mut a = release("1.1.0", &[("c", "^1"), ("d", "^1")]);
        a.scopes = vec!["lyra.favorites".into()];
        let d = release("1.1.0", &[]);
        let candidates = [
            Candidate {
                id: "a",
                release: &a,
                approved: false,
            },
            Candidate {
                id: "d",
                release: &d,
                approved: false,
            },
        ];

        let Some(Hold::AddsScopes { gains }) = holds(&installed, &candidates).remove("a") else {
            panic!("a is held for reaching http through c once d drops it");
        };
        assert_eq!(
            (gains["a"].previous_risk, gains["a"].risk.risk),
            (Risk::Elevated, Risk::Elevated),
            "installed a already sends favorites off the server through d"
        );
    }
}
