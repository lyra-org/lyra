// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! What plugins can reach through their scopes.
//!
//! A scope is checked once, when a plugin `require`s the module behind it or
//! the module holding the functions it gates, and a plugin's own modules run
//! as that plugin. A plugin that `require`s a
//! dependency's modules therefore reaches whatever the dependency hands it,
//! so its reach is its own scopes and those of every plugin it depends on,
//! directly or not. Optional dependencies count whether or not they are
//! installed: a dependency installed later widens its dependents unasked.

use std::collections::{
    BTreeMap,
    BTreeSet,
};

use crate::plugin::PluginManifest;

/// What a plugin's reach is computed from: its own scopes and the plugins
/// it depends on. Implemented by manifests, and by hosts for releases they
/// know only from an index.
pub trait ScopedPlugin {
    fn id(&self) -> &str;
    fn scopes(&self) -> &[String];
    fn dependency_ids(&self) -> impl Iterator<Item = &str>;
}

impl ScopedPlugin for PluginManifest {
    fn id(&self) -> &str {
        &self.id
    }

    fn scopes(&self) -> &[String] {
        &self.scopes
    }

    fn dependency_ids(&self) -> impl Iterator<Item = &str> {
        self.dependencies.keys().map(String::as_str)
    }
}

/// The scopes one plugin can reach.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EffectiveScopes {
    /// Each reachable scope, with the plugins that declare it: the plugin
    /// itself, its dependencies, or both.
    pub scopes: BTreeMap<String, BTreeSet<String>>,
    /// Dependencies, direct or not, without a known manifest, whose scopes
    /// are therefore unknown.
    pub unresolved: BTreeSet<String>,
}

impl EffectiveScopes {
    /// Walks `plugin` and its dependencies, finding each dependency with
    /// `lookup`.
    pub fn of<'a, P: ScopedPlugin>(plugin: &'a P, lookup: impl Fn(&str) -> Option<&'a P>) -> Self {
        Self::of_any(plugin, |id| lookup(id).into_iter().collect())
    }

    /// Like [`Self::of`], when a dependency may end up as any of several
    /// plugins, such as an installed version and an offered one: every one
    /// of them is followed, so the result covers whichever is present.
    pub fn of_any<'a, P: ScopedPlugin>(plugin: &'a P, lookup: impl Fn(&str) -> Vec<&'a P>) -> Self {
        let mut effective = Self::default();
        let mut visited = BTreeSet::from([plugin.id()]);
        let mut pending = vec![plugin];
        while let Some(current) = pending.pop() {
            for scope in current.scopes() {
                effective
                    .scopes
                    .entry(scope.clone())
                    .or_default()
                    .insert(current.id().to_string());
            }
            for dependency in current.dependency_ids() {
                if !visited.insert(dependency) {
                    continue;
                }
                let candidates = lookup(dependency);
                if candidates.is_empty() {
                    effective.unresolved.insert(dependency.to_string());
                }
                pending.extend(candidates);
            }
        }
        effective
    }

    /// What `self` reaches that `previous` did not.
    pub fn added_since(&self, previous: &Self) -> ScopeGrowth {
        ScopeGrowth {
            scopes: self
                .scopes
                .keys()
                .filter(|scope| !previous.scopes.contains_key(*scope))
                .cloned()
                .collect(),
            unresolved: self
                .unresolved
                .difference(&previous.unresolved)
                .cloned()
                .collect(),
        }
    }
}

/// Scopes a plugin newly reaches, and dependencies it newly reaches whose
/// scopes are unknown.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScopeGrowth {
    pub scopes: BTreeSet<String>,
    pub unresolved: BTreeSet<String>,
}

impl ScopeGrowth {
    pub fn is_empty(&self) -> bool {
        self.scopes.is_empty() && self.unresolved.is_empty()
    }
}

/// What installing `candidate` among the `installed` plugins, replacing the
/// one with its id if any, lets each plugin newly reach: the candidate
/// itself, and every installed plugin that depends on it, directly or not.
/// Plugins that reach nothing new are left out.
pub fn scope_growth<P: ScopedPlugin>(
    installed: &[P],
    candidate: &P,
) -> BTreeMap<String, ScopeGrowth> {
    let before = |id: &str| installed.iter().find(|plugin| plugin.id() == id);
    let after = |id: &str| {
        if id == candidate.id() {
            Some(candidate)
        } else {
            before(id)
        }
    };

    let mut growth = BTreeMap::new();
    let previous = before(candidate.id())
        .map(|current| EffectiveScopes::of(current, before))
        .unwrap_or_default();
    let added = EffectiveScopes::of(candidate, after).added_since(&previous);
    if !added.is_empty() {
        growth.insert(candidate.id().to_string(), added);
    }
    for dependent in installed {
        if dependent.id() == candidate.id() {
            continue;
        }
        let added = EffectiveScopes::of(dependent, after)
            .added_since(&EffectiveScopes::of(dependent, before));
        if !added.is_empty() {
            growth.insert(dependent.id().to_string(), added);
        }
    }
    growth
}

#[cfg(test)]
mod tests {
    use std::collections::{
        BTreeMap,
        BTreeSet,
    };

    use semver::{
        Version,
        VersionReq,
    };

    use super::{
        EffectiveScopes,
        ScopeGrowth,
        scope_growth,
    };
    use crate::plugin::{
        Dependency,
        PluginManifest,
    };

    fn manifest(id: &str, scopes: &[&str], dependencies: &[(&str, bool)]) -> PluginManifest {
        PluginManifest {
            schema_version: 1,
            id: id.to_string(),
            name: id.to_string(),
            version: Version::new(1, 0, 0),
            host_requirement: VersionReq::STAR,
            description: String::new(),
            entrypoint: Some("init.luau".to_string()),
            scopes: scopes.iter().map(ToString::to_string).collect(),
            dependencies: dependencies
                .iter()
                .map(|(id, optional)| {
                    (
                        id.to_string(),
                        Dependency {
                            version: VersionReq::STAR,
                            optional: *optional,
                        },
                    )
                })
                .collect(),
        }
    }

    fn set(items: &[&str]) -> BTreeSet<String> {
        items.iter().map(ToString::to_string).collect()
    }

    fn effective(plugin: &PluginManifest, installed: &[PluginManifest]) -> EffectiveScopes {
        EffectiveScopes::of(plugin, |id| {
            installed.iter().find(|manifest| manifest.id == id)
        })
    }

    #[test]
    fn effective_scopes_follow_dependencies_transitively() {
        let net = manifest("net", &["net"], &[]);
        let helper = manifest("helper", &["serde"], &[("net", false)]);
        let plugin = manifest("plugin", &["serde"], &[("helper", false)]);
        let installed = [net, helper];

        let effective = effective(&plugin, &installed);

        assert_eq!(
            effective.scopes,
            BTreeMap::from([
                ("net".to_string(), set(&["net"])),
                ("serde".to_string(), set(&["helper", "plugin"])),
            ])
        );
        assert!(effective.unresolved.is_empty());
    }

    #[test]
    fn effective_scopes_report_dependencies_without_manifests() {
        let plugin = manifest("plugin", &[], &[("missing", true)]);

        let effective = effective(&plugin, &[]);

        assert!(effective.scopes.is_empty());
        assert_eq!(effective.unresolved, set(&["missing"]));
    }

    #[test]
    fn effective_scopes_follow_every_candidate_of_a_dependency() {
        let installed = manifest("helper", &[], &[]);
        let offered = manifest("helper", &["net"], &[("extra", false)]);
        let extra = manifest("extra", &["fs"], &[]);
        let plugin = manifest("plugin", &["listens"], &[("helper", false)]);

        let effective = EffectiveScopes::of_any(&plugin, |id| match id {
            "helper" => vec![&installed, &offered],
            "extra" => vec![&extra],
            _ => Vec::new(),
        });

        assert_eq!(
            effective.scopes.keys().cloned().collect::<BTreeSet<_>>(),
            set(&["fs", "listens", "net"])
        );
    }

    #[test]
    fn effective_scopes_terminate_on_dependency_cycles() {
        let a = manifest("a", &["one"], &[("b", false)]);
        let b = manifest("b", &["two"], &[("a", false)]);
        let installed = [a.clone(), b];

        let effective = effective(&a, &installed);

        assert_eq!(
            effective.scopes.keys().cloned().collect::<BTreeSet<_>>(),
            set(&["one", "two"])
        );
    }

    #[test]
    fn scope_growth_is_empty_when_an_update_keeps_its_scopes() {
        let installed = [manifest("plugin", &["serde"], &[])];
        let mut candidate = manifest("plugin", &["serde"], &[]);
        candidate.version = Version::new(1, 1, 0);

        assert!(scope_growth(&installed, &candidate).is_empty());
    }

    #[test]
    fn scope_growth_reports_scopes_an_update_adds() {
        let installed = [manifest("plugin", &["serde"], &[])];
        let candidate = manifest("plugin", &["serde", "net"], &[]);

        assert_eq!(
            scope_growth(&installed, &candidate),
            BTreeMap::from([(
                "plugin".to_string(),
                ScopeGrowth {
                    scopes: set(&["net"]),
                    unresolved: BTreeSet::new(),
                }
            )])
        );
    }

    #[test]
    fn scope_growth_reports_dependents_widened_by_a_dependency_update() {
        let installed = [
            manifest("helper", &[], &[]),
            manifest("plugin", &["serde"], &[("helper", false)]),
            manifest("indirect", &[], &[("plugin", false)]),
            manifest("unrelated", &[], &[]),
        ];
        let candidate = manifest("helper", &["net"], &[]);

        let growth = scope_growth(&installed, &candidate);

        assert_eq!(
            growth.keys().cloned().collect::<BTreeSet<_>>(),
            set(&["helper", "indirect", "plugin"])
        );
        assert_eq!(growth["indirect"].scopes, set(&["net"]));
    }

    #[test]
    fn scope_growth_reports_dependents_an_install_widens() {
        let installed = [manifest("plugin", &[], &[("extra", true)])];
        let candidate = manifest("extra", &["net"], &[]);

        let growth = scope_growth(&installed, &candidate);

        assert_eq!(growth["extra"].scopes, set(&["net"]));
        assert_eq!(growth["plugin"].scopes, set(&["net"]));
        assert!(growth["plugin"].unresolved.is_empty());
    }

    #[test]
    fn scope_growth_reports_new_dependencies_without_manifests() {
        let installed = [manifest("plugin", &[], &[])];
        let candidate = manifest("plugin", &[], &[("unknown", false)]);

        assert_eq!(
            scope_growth(&installed, &candidate)["plugin"].unresolved,
            set(&["unknown"])
        );
    }
}
