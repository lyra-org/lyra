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
//!
//! A scope can take a grant: a list of items, such as the hosts a plugin may
//! send requests to, that narrows what the scope lets the plugin reach, and
//! that every plugin declaring the scope must give. The module owning the
//! scope gives the items their meaning through [`ScopeGrant`]; here they are
//! only stored and compared through it.

use std::{
    collections::{
        BTreeMap,
        BTreeSet,
    },
    fmt,
};

use crate::plugin::{
    PluginHost,
    PluginManifest,
};

/// The grant one scope takes, as the module owning the scope defines it. A
/// host registers it for the scope's id with
/// [`PluginHost::with_scope_grant`].
pub trait ScopeGrant: fmt::Debug + Send + Sync {
    /// The key of a manifest's scope entry that holds the grant, other than
    /// `id` and `reason`.
    fn key(&self) -> &str;

    /// The items a manifest's `value` grants, each in the one form it is
    /// stored, shown and matched in, sorted and without duplicates, or what
    /// is wrong with the value.
    fn parse(&self, value: &serde_json::Value) -> Result<Vec<String>, String>;

    /// `item` of `plugin_id`'s grant as it stands beside other plugins'
    /// items, which is the form [`Self::covers`] compares and growth reports.
    /// An item that means the same whoever declares it stays as it is; one
    /// whose meaning depends on its plugin names the plugin, so that it is
    /// not taken for another plugin's.
    fn qualify(&self, plugin_id: &str, item: &str) -> String {
        let _ = plugin_id;
        item.to_string()
    }

    /// Whether the `granted` items already allow everything `item` does,
    /// all as [`Self::qualify`] gives them.
    fn covers(&self, granted: &[String], item: &str) -> bool;
}

/// What a plugin's reach is computed from: its own scopes and the plugins
/// it depends on. Implemented by manifests, and by hosts for releases they
/// know only from an index.
pub trait ScopedPlugin {
    fn id(&self) -> &str;
    fn scopes(&self) -> &[String];
    /// The items of the grant it declares `scope` with, when it gives one.
    fn scope_grant(&self, scope: &str) -> Option<&[String]>;
    fn dependency_ids(&self) -> impl Iterator<Item = &str>;
}

impl ScopedPlugin for PluginManifest {
    fn id(&self) -> &str {
        &self.id
    }

    fn scopes(&self) -> &[String] {
        &self.scopes
    }

    fn scope_grant(&self, scope: &str) -> Option<&[String]> {
        self.scope_grants.get(scope).map(Vec::as_slice)
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
    /// The grant items reachable under each scope that takes a grant, from
    /// every plugin that declares the scope with one, each item as
    /// [`ScopeGrant::qualify`] gives it for its plugin.
    pub grants: BTreeMap<String, BTreeSet<String>>,
    /// The scopes taking a grant that some plugin declares without one, as a
    /// manifest from before the scope took a grant does. Such a plugin does
    /// not load, and counts as reaching the scope unrestricted.
    pub ungranted: BTreeSet<String>,
    /// Dependencies, direct or not, without a known manifest, whose scopes
    /// are therefore unknown.
    pub unresolved: BTreeSet<String>,
}

impl EffectiveScopes {
    /// Walks `plugin` and its dependencies, finding each dependency with
    /// `lookup`, and reading grants as `host`'s scopes take them.
    pub fn of<'a, P: ScopedPlugin>(
        host: &PluginHost,
        plugin: &'a P,
        lookup: impl Fn(&str) -> Option<&'a P>,
    ) -> Self {
        Self::of_any(host, plugin, |id| lookup(id).into_iter().collect())
    }

    /// Like [`Self::of`], when a dependency may end up as any of several
    /// plugins, such as an installed version and an offered one: every one
    /// of them is followed, so the result covers whichever is present.
    pub fn of_any<'a, P: ScopedPlugin>(
        host: &PluginHost,
        plugin: &'a P,
        lookup: impl Fn(&str) -> Vec<&'a P>,
    ) -> Self {
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
                let Some(grant) = host.scope_grant(scope) else {
                    continue;
                };
                match current.scope_grant(scope) {
                    Some(items) => effective
                        .grants
                        .entry(scope.clone())
                        .or_default()
                        .extend(items.iter().map(|item| grant.qualify(current.id(), item))),
                    None => {
                        effective.ungranted.insert(scope.clone());
                    }
                }
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

    /// What `self` reaches that `previous` did not, comparing grant items as
    /// `host`'s scopes do. A scope `previous` reached unrestricted gains
    /// nothing from a grant.
    pub fn added_since(&self, previous: &Self, host: &PluginHost) -> ScopeGrowth {
        let mut growth = ScopeGrowth {
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
            ..ScopeGrowth::default()
        };
        for scope in self.scopes.keys() {
            let Some(grant) = host.scope_grant(scope) else {
                continue;
            };
            if previous.ungranted.contains(scope) {
                continue;
            }
            if self.ungranted.contains(scope) {
                growth.ungranted.insert(scope.clone());
                continue;
            }
            let before: Vec<String> = previous
                .grants
                .get(scope)
                .into_iter()
                .flatten()
                .cloned()
                .collect();
            let added: BTreeSet<String> = self
                .grants
                .get(scope)
                .into_iter()
                .flatten()
                .filter(|item| !grant.covers(&before, item))
                .cloned()
                .collect();
            if !added.is_empty() {
                growth.grants.insert(scope.clone(), added);
            }
        }
        growth
    }
}

/// Scopes a plugin newly reaches, what it newly reaches under scopes that
/// take a grant, and dependencies it newly reaches whose scopes are unknown.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScopeGrowth {
    pub scopes: BTreeSet<String>,
    /// By scope, the grant items newly reachable that nothing reachable
    /// before covers: every item of a scope in `scopes`, and the additions to
    /// one reached before.
    pub grants: BTreeMap<String, BTreeSet<String>>,
    /// Scopes taking a grant that are newly reached unrestricted, whether
    /// reached before within grants or not at all: through a plugin that
    /// declares one without a grant, as a newly reached dependency whose
    /// manifest predates the grant does.
    pub ungranted: BTreeSet<String>,
    pub unresolved: BTreeSet<String>,
}

impl ScopeGrowth {
    pub fn is_empty(&self) -> bool {
        self.scopes.is_empty()
            && self.grants.is_empty()
            && self.ungranted.is_empty()
            && self.unresolved.is_empty()
    }
}

/// What installing `candidate` among the `installed` plugins, replacing the
/// one with its id if any, lets each plugin newly reach: the candidate
/// itself, and every installed plugin that depends on it, directly or not.
/// Plugins that reach nothing new are left out. Grant items are compared as
/// `host`'s scopes do.
pub fn scope_growth<P: ScopedPlugin>(
    host: &PluginHost,
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
        .map(|current| EffectiveScopes::of(host, current, before))
        .unwrap_or_default();
    let added = EffectiveScopes::of(host, candidate, after).added_since(&previous, host);
    if !added.is_empty() {
        growth.insert(candidate.id().to_string(), added);
    }
    for dependent in installed {
        if dependent.id() == candidate.id() {
            continue;
        }
        let added = EffectiveScopes::of(host, dependent, after)
            .added_since(&EffectiveScopes::of(host, dependent, before), host);
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

    use std::{
        collections::HashSet,
        sync::Arc,
    };

    use super::{
        EffectiveScopes,
        ScopeGrant,
        ScopeGrowth,
    };
    use crate::plugin::{
        Dependency,
        PluginHost,
        PluginManifest,
    };

    /// Items are names, and `*` allows every name. A name starting with `~`
    /// is the plugin's own, apart from any other plugin's.
    #[derive(Debug)]
    struct Names;

    impl ScopeGrant for Names {
        fn key(&self) -> &str {
            "names"
        }

        fn qualify(&self, plugin_id: &str, item: &str) -> String {
            match item.strip_prefix('~') {
                Some(own) => format!("~{plugin_id}/{own}"),
                None => item.to_string(),
            }
        }

        fn parse(&self, _value: &serde_json::Value) -> Result<Vec<String>, String> {
            Err("unused".into())
        }

        fn covers(&self, granted: &[String], item: &str) -> bool {
            granted.iter().any(|name| name == "*" || name == item)
        }
    }

    fn host() -> PluginHost {
        PluginHost::new("host", Version::new(0, 0, 1), HashSet::new())
            .expect("valid host name")
            .with_scope_grant("net", Arc::new(Names))
    }

    fn scope_growth(
        installed: &[PluginManifest],
        candidate: &PluginManifest,
    ) -> BTreeMap<String, ScopeGrowth> {
        super::scope_growth(&host(), installed, candidate)
    }

    fn granted(mut manifest: PluginManifest, scope: &str, items: &[&str]) -> PluginManifest {
        manifest.scope_grants.insert(
            scope.to_string(),
            items.iter().map(ToString::to_string).collect(),
        );
        manifest
    }

    fn items(scope: &str, items: &[&str]) -> BTreeMap<String, BTreeSet<String>> {
        BTreeMap::from([(scope.to_string(), set(items))])
    }

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
            scope_reasons: BTreeMap::new(),
            scope_grants: BTreeMap::new(),
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
        EffectiveScopes::of(&host(), plugin, |id| {
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

        let effective = EffectiveScopes::of_any(&host(), &plugin, |id| match id {
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
        let candidate = manifest("plugin", &["serde", "fs"], &[]);

        assert_eq!(
            scope_growth(&installed, &candidate),
            BTreeMap::from([(
                "plugin".to_string(),
                ScopeGrowth {
                    scopes: set(&["fs"]),
                    ..ScopeGrowth::default()
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

    #[test]
    fn effective_scopes_union_grant_items_through_dependencies() {
        let helper = granted(manifest("helper", &["net"], &[]), "net", &["b", "c"]);
        let bare = manifest("bare", &["net", "fs"], &[]);
        let plugin = granted(
            manifest("plugin", &["net"], &[("helper", false)]),
            "net",
            &["a", "b"],
        );

        let reach = effective(&plugin, std::slice::from_ref(&helper));
        assert_eq!(reach.grants, items("net", &["a", "b", "c"]));
        assert!(reach.ungranted.is_empty());

        let widened = manifest("widened", &[], &[("plugin", false), ("bare", false)]);
        let reach = effective(&widened, &[plugin, helper, bare]);
        assert_eq!(reach.grants, items("net", &["a", "b", "c"]));
        assert_eq!(
            reach.ungranted,
            set(&["net"]),
            "only a scope that takes a grant can go without one"
        );
    }

    #[test]
    fn an_item_that_depends_on_its_plugin_is_never_another_plugins() {
        let helper = granted(
            manifest("helper", &["net"], &[]),
            "net",
            &["~own", "shared"],
        );
        let caller = granted(
            manifest("caller", &["net"], &[("helper", false)]),
            "net",
            &[],
        );
        let installed = [helper, caller];
        assert_eq!(
            effective(&installed[1], &installed).grants,
            items("net", &["shared", "~helper/own"])
        );

        let update = granted(
            manifest("caller", &["net"], &[("helper", false)]),
            "net",
            &["~own", "shared"],
        );
        assert_eq!(
            scope_growth(&installed, &update),
            BTreeMap::from([(
                "caller".to_string(),
                ScopeGrowth {
                    grants: items("net", &["~caller/own"]),
                    ..ScopeGrowth::default()
                }
            )]),
            "the dependency's own item does not cover the plugin's"
        );

        let installed = [installed[0].clone(), update.clone()];
        let mut next = update;
        next.version = Version::new(1, 1, 0);
        assert!(
            scope_growth(&installed, &next).is_empty(),
            "the plugin's own item covers itself across versions"
        );
    }

    #[test]
    fn scope_growth_reports_grant_items_nothing_reached_before_covers() {
        let installed = [granted(manifest("plugin", &["net"], &[]), "net", &["a"])];

        let same = granted(manifest("plugin", &["net"], &[]), "net", &["a"]);
        assert!(scope_growth(&installed, &same).is_empty());

        let narrower = granted(manifest("plugin", &["net"], &[]), "net", &[]);
        assert!(scope_growth(&installed, &narrower).is_empty());

        let wider = granted(manifest("plugin", &["net"], &[]), "net", &["a", "b"]);
        assert_eq!(
            scope_growth(&installed, &wider),
            BTreeMap::from([(
                "plugin".to_string(),
                ScopeGrowth {
                    grants: items("net", &["b"]),
                    ..ScopeGrowth::default()
                }
            )])
        );

        let any = [granted(manifest("plugin", &["net"], &[]), "net", &["*"])];
        assert!(
            scope_growth(&any, &wider).is_empty(),
            "the scope's grant decides what an item covers"
        );
        let to_any = granted(manifest("plugin", &["net"], &[]), "net", &["*"]);
        assert_eq!(
            scope_growth(&installed, &to_any)["plugin"].grants,
            items("net", &["*"])
        );
    }

    #[test]
    fn a_newly_reached_scope_reports_every_item_of_its_grant() {
        let installed = [manifest("plugin", &[], &[])];
        let candidate = granted(manifest("plugin", &["net"], &[]), "net", &["a", "b"]);

        assert_eq!(
            scope_growth(&installed, &candidate)["plugin"],
            ScopeGrowth {
                scopes: set(&["net"]),
                grants: items("net", &["a", "b"]),
                ..ScopeGrowth::default()
            }
        );
    }

    #[test]
    fn a_plugin_declaring_a_scope_without_its_grant_reaches_it_unrestricted() {
        let installed = [manifest("plugin", &["net"], &[])];
        let narrowed = granted(manifest("plugin", &["net"], &[]), "net", &["a", "b"]);
        assert!(
            scope_growth(&installed, &narrowed).is_empty(),
            "a grant only narrows a scope reached without one"
        );

        let installed = [narrowed];
        let unrestricted = manifest("plugin", &["net"], &[]);
        assert_eq!(
            scope_growth(&installed, &unrestricted)["plugin"],
            ScopeGrowth {
                ungranted: set(&["net"]),
                ..ScopeGrowth::default()
            }
        );
    }

    #[test]
    fn a_scope_first_reached_through_a_plugin_without_its_grant_is_reached_unrestricted() {
        let installed = [
            manifest("plugin", &[], &[]),
            manifest("bare", &["net"], &[]),
        ];
        let candidate = manifest("plugin", &[], &[("bare", false)]);

        assert_eq!(
            scope_growth(&installed, &candidate),
            BTreeMap::from([(
                "plugin".to_string(),
                ScopeGrowth {
                    scopes: set(&["net"]),
                    ungranted: set(&["net"]),
                    ..ScopeGrowth::default()
                }
            )])
        );
    }

    #[test]
    fn scope_growth_reports_grant_items_dependents_reach_through_an_update() {
        let installed = [
            granted(manifest("helper", &["net"], &[]), "net", &["a"]),
            granted(
                manifest("plugin", &["net"], &[("helper", false)]),
                "net",
                &["b"],
            ),
            manifest("indirect", &[], &[("plugin", false)]),
            granted(manifest("unrelated", &["net"], &[]), "net", &["z"]),
        ];
        let candidate = granted(manifest("helper", &["net"], &[]), "net", &["a", "b", "c"]);

        let growth = scope_growth(&installed, &candidate);

        assert_eq!(
            growth.keys().cloned().collect::<BTreeSet<_>>(),
            set(&["helper", "indirect", "plugin"])
        );
        assert_eq!(growth["helper"].grants, items("net", &["b", "c"]));
        assert_eq!(
            growth["plugin"].grants,
            items("net", &["c"]),
            "the dependent already reached its own items"
        );
        assert_eq!(growth["indirect"].grants, items("net", &["c"]));
    }
}
