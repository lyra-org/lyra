// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Which entities external IDs link as duplicates of one another.

use std::collections::{
    BTreeMap,
    BTreeSet,
    HashMap,
    HashSet,
};
use std::fmt;

use agdb::{
    DbAny,
    DbId,
};
use lyra_db::IdSource;
use lyra_db::external_ids::{
    ExternalId,
    ExternalIdAlias,
};

use crate::services::providers::active_providers;

/// One external ID value, as a provider names it.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct IdKey {
    provider_id: String,
    id_type: String,
    value: String,
}

impl fmt::Debug for IdKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}:{}", self.provider_id, self.id_type, self.value)
    }
}

/// Entities linked through any chain of shared external IDs.
#[derive(Default)]
pub(crate) struct LinkedComponent<'a> {
    pub(crate) ids: Vec<DbId>,
    pub(crate) keys: Vec<&'a IdKey>,
}

/// The values entities hold for one ID type that names exactly one entity.
#[derive(Clone, Debug, Default)]
struct IdValues {
    values: BTreeSet<String>,
    /// The values a user set, which outrank every provider.
    manual: BTreeSet<String>,
}

impl IdValues {
    fn extend(&mut self, other: IdValues) {
        self.values.extend(other.values);
        self.manual.extend(other.manual);
    }

    /// Whether these values and `other` name different entities, so the
    /// entities holding them cannot be one. Differing manual values, or a
    /// manual value that is none of the other side's values, always
    /// conflict; resolved values only when `blocks` says their provider may.
    fn conflicts_with(&self, other: &IdValues, blocks: bool) -> bool {
        let overrules = |own: &IdValues, other: &IdValues| {
            !own.manual.is_empty() && own.manual.is_disjoint(&other.values)
        };
        let manual_differs = !self.manual.is_empty()
            && !other.manual.is_empty()
            && self.manual.is_disjoint(&other.manual);
        manual_differs
            || overrules(self, other)
            || overrules(other, self)
            || (blocks && self.values.is_disjoint(&other.values))
    }
}

/// The `(provider_id, id_type)` pairs the loaded providers declare for one
/// entity type.
#[derive(Default)]
pub(crate) struct DeclaredIds {
    /// The pairs whose value names exactly one entity.
    unique: HashSet<(String, String)>,
    /// Every declared pair, unique or not.
    all: HashSet<(String, String)>,
}

impl DeclaredIds {
    pub(crate) fn new(unique: HashSet<(String, String)>, all: HashSet<(String, String)>) -> Self {
        let all = all.union(&unique).cloned().collect();
        Self { unique, all }
    }

    /// Only `pairs`, each declared unique.
    #[cfg(test)]
    pub(crate) fn unique<'a>(pairs: impl IntoIterator<Item = (&'a str, &'a str)>) -> Self {
        let unique: HashSet<(String, String)> = pairs
            .into_iter()
            .map(|(provider_id, id_type)| (provider_id.to_string(), id_type.to_string()))
            .collect();
        Self::new(unique, HashSet::new())
    }

    /// These pairs and `pairs`, declared but not unique.
    #[cfg(test)]
    pub(crate) fn and_non_unique<'a>(
        mut self,
        pairs: impl IntoIterator<Item = (&'a str, &'a str)>,
    ) -> Self {
        self.all.extend(
            pairs
                .into_iter()
                .map(|(provider_id, id_type)| (provider_id.to_string(), id_type.to_string())),
        );
        self
    }

    pub(crate) fn has_unique(&self) -> bool {
        !self.unique.is_empty()
    }
}

/// The external IDs of the entities a dedup run considers.
#[derive(Default)]
pub(crate) struct LinkedIds {
    groups: BTreeMap<IdKey, Vec<DbId>>,
    /// Each entity's values for the ID types that name exactly one entity.
    identities: HashMap<DbId, BTreeMap<(String, String), IdValues>>,
}

impl LinkedIds {
    /// Records an ID that names exactly one entity, by its effective value:
    /// a resolved value beneath a manual one is what the user overrode, so it
    /// neither links nor conflicts. Entities holding the same value link when
    /// `links` is set; different values can keep them apart. Empty values say
    /// nothing.
    pub(crate) fn add(
        &mut self,
        entity: DbId,
        provider_id: &str,
        id_type: &str,
        value: &str,
        source: IdSource,
        links: bool,
    ) {
        let value = value.trim();
        if value.is_empty() {
            return;
        }
        let values = self
            .identities
            .entry(entity)
            .or_default()
            .entry((provider_id.to_string(), id_type.to_string()))
            .or_default();
        values.values.insert(value.to_string());
        if source == IdSource::Manual {
            values.manual.insert(value.to_string());
        }
        if links {
            self.groups
                .entry(IdKey {
                    provider_id: provider_id.to_string(),
                    id_type: id_type.to_string(),
                    value: value.to_string(),
                })
                .or_default()
                .push(entity);
        }
    }

    /// Records `external_id` when it tells which entity holds it: a value of a
    /// unique pair, which links when `links` is set, or a manual value of a
    /// pair no loaded provider declares. That provider may only be missing,
    /// so its manual value keeps entities apart but links none.
    pub(crate) fn add_external_id(
        &mut self,
        entity: DbId,
        external_id: &ExternalId,
        declared: &DeclaredIds,
        links: bool,
    ) {
        let pair = (external_id.provider_id.clone(), external_id.id_type.clone());
        let unique = declared.unique.contains(&pair);
        if !unique && (external_id.source != IdSource::Manual || declared.all.contains(&pair)) {
            return;
        }
        self.add(
            entity,
            &external_id.provider_id,
            &external_id.id_type,
            &external_id.id_value,
            external_id.source,
            unique && links,
        );
    }

    /// Records a kept value as a resolved one of the pair it was kept for, when that pair names
    /// exactly one entity, so its holder links with a new entity holding the value.
    pub(crate) fn add_alias(
        &mut self,
        entity: DbId,
        alias: &ExternalIdAlias,
        declared: &DeclaredIds,
        links: bool,
    ) {
        let pair = (alias.provider_id.clone(), alias.id_type.clone());
        if declared.unique.contains(&pair) {
            self.add(
                entity,
                &alias.provider_id,
                &alias.id_type,
                &alias.id_value,
                IdSource::Resolved,
                links,
            );
        }
    }

    /// Joins the entities that share an ID into components, so each component
    /// picks one winner however its IDs are ordered. A join is skipped when
    /// the two sides share no value from a provider ranked at or above
    /// the linking one, or when a manual value on one side is none of the
    /// other's values of its type: a merge cannot be undone, while a missed
    /// one leaves a visible duplicate.
    /// A lower-ranked provider's value gives way, as a merge's loser's IDs do.
    /// Single-member components are left out.
    pub(crate) fn components(&self, ranks: &ProviderRanks) -> Vec<LinkedComponent<'_>> {
        fn root(parents: &HashMap<DbId, DbId>, mut id: DbId) -> DbId {
            while let Some(&parent) = parents.get(&id) {
                id = parent;
            }
            id
        }

        let mut parents: HashMap<DbId, DbId> = HashMap::new();
        let mut root_identities = self.identities.clone();
        for (key, ids) in &self.groups {
            let link_rank = ranks.get(&key.provider_id);
            // An unranked provider never blocks; an unranked link ranks lowest.
            let blocks = |provider_id: &str| match (ranks.get(provider_id), link_rank) {
                (None, _) => false,
                (Some(_), None) => true,
                (Some(rank), Some(link_rank)) => rank <= link_rank,
            };
            // Every pair, so members that conflict with one another can each
            // still join the rest.
            let pairs = ids
                .iter()
                .enumerate()
                .flat_map(|(index, &first)| ids[index + 1..].iter().map(move |&id| (first, id)));
            for (first, id) in pairs {
                let (first_root, id_root) = (root(&parents, first), root(&parents, id));
                if first_root == id_root {
                    continue;
                }
                let conflict = root_identities.get(&first_root).and_then(|first_ids| {
                    root_identities
                        .get(&id_root)?
                        .iter()
                        .find_map(|(id_type, values)| {
                            let first_values = first_ids.get(id_type)?;
                            first_values
                                .conflicts_with(values, blocks(&id_type.0))
                                .then_some((id_type, first_values, values))
                        })
                });
                if let Some(((provider_id, id_type), first_values, values)) = conflict {
                    tracing::warn!(
                        external_id = ?key,
                        first = first.0,
                        other = id.0,
                        provider_id = %provider_id,
                        id_type = %id_type,
                        first_values = ?first_values,
                        other_values = ?values,
                        "not merging entities whose external ids conflict"
                    );
                    continue;
                }
                let (kept, joined) = (first_root.min(id_root), first_root.max(id_root));
                if let Some(joined_ids) = root_identities.remove(&joined) {
                    let kept_ids = root_identities.entry(kept).or_default();
                    for (id_type, values) in joined_ids {
                        kept_ids.entry(id_type).or_default().extend(values);
                    }
                }
                parents.insert(joined, kept);
            }
        }

        let mut components: BTreeMap<DbId, LinkedComponent<'_>> = BTreeMap::new();
        for (key, ids) in &self.groups {
            for &id in ids {
                let component = components.entry(root(&parents, id)).or_default();
                if component.keys.last() != Some(&key) {
                    component.keys.push(key);
                }
                component.ids.push(id);
            }
        }
        components
            .into_values()
            .filter_map(|mut component| {
                component.ids.sort();
                component.ids.dedup();
                (component.ids.len() > 1).then_some(component)
            })
            .collect()
    }
}

/// Active providers' positions, the admin-set trust order; 0 ranks highest.
pub(crate) type ProviderRanks = HashMap<String, u32>;

pub(crate) fn provider_ranks(db: &DbAny) -> anyhow::Result<ProviderRanks> {
    Ok(active_providers(db)?
        .into_iter()
        .map(|provider| (provider.provider_id, provider.position))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn joined(links: &LinkedIds, ranks: &ProviderRanks) -> bool {
        !links.components(ranks).is_empty()
    }

    #[test]
    fn unranked_providers_link_lowest_and_never_block() {
        let ranks = ProviderRanks::from([("ranked".to_string(), 0)]);
        let pair = |link: &str, conflict: &str| {
            let mut links = LinkedIds::default();
            for (entity, value) in [(DbId(1), "one"), (DbId(2), "two")] {
                links.add(entity, link, "id", "shared", IdSource::Resolved, true);
                links.add(entity, conflict, "id", value, IdSource::Resolved, false);
            }
            links
        };

        assert!(
            !joined(&pair("unranked", "ranked"), &ranks),
            "an unranked link gives way to a ranked conflict"
        );
        assert!(
            joined(&pair("ranked", "unranked"), &ranks),
            "an unranked conflict never blocks"
        );
        assert!(
            joined(&pair("unranked", "other-unranked"), &ranks),
            "nor does one under an unranked link"
        );
    }

    #[test]
    fn a_value_a_component_gathered_still_links_through_its_own_provider() {
        let ranks = ProviderRanks::from([
            ("musicbrainz".to_string(), 0),
            ("provider-a".to_string(), 1),
        ]);
        let mut links = LinkedIds::default();
        for (entity, provider_id, value) in [
            (DbId(1), "musicbrainz", "m"),
            (DbId(1), "provider-a", "x"),
            (DbId(2), "musicbrainz", "m"),
            (DbId(2), "provider-a", "y"),
            (DbId(3), "provider-a", "x"),
        ] {
            links.add(entity, provider_id, "id", value, IdSource::Resolved, true);
        }

        let components = links.components(&ranks);
        assert_eq!(components.len(), 1);
        assert_eq!(components[0].ids, [DbId(1), DbId(2), DbId(3)]);
    }

    #[test]
    fn members_that_conflict_with_the_first_still_join_each_other() {
        let ranks =
            ProviderRanks::from([("musicbrainz".to_string(), 0), ("provider".to_string(), 1)]);
        let mut links = LinkedIds::default();
        for (entity, value) in [(DbId(1), "one"), (DbId(2), "two"), (DbId(3), "two")] {
            links.add(entity, "provider", "id", "shared", IdSource::Resolved, true);
            links.add(
                entity,
                "musicbrainz",
                "id",
                value,
                IdSource::Resolved,
                false,
            );
        }

        let components = links.components(&ranks);
        assert_eq!(components.len(), 1);
        assert_eq!(components[0].ids, [DbId(2), DbId(3)]);
    }

    #[test]
    fn differing_manual_values_block_whatever_the_ranks() {
        let ranks = ProviderRanks::from([("link".to_string(), 0), ("pinned".to_string(), 1)]);
        let pair = |first: IdSource, second: IdSource| {
            let mut links = LinkedIds::default();
            for (entity, value, source) in [(DbId(1), "one", first), (DbId(2), "two", second)] {
                links.add(entity, "link", "id", "shared", IdSource::Resolved, true);
                links.add(entity, "pinned", "id", value, source, false);
            }
            links
        };

        assert!(
            !joined(&pair(IdSource::Manual, IdSource::Manual), &ranks),
            "differing manual values outrank the linking provider"
        );
        assert!(
            !joined(&pair(IdSource::Manual, IdSource::Resolved), &ranks),
            "as does a manual value against a resolved one"
        );
        assert!(
            joined(&pair(IdSource::Resolved, IdSource::Resolved), &ranks),
            "two resolved values keep the rank rules"
        );
    }

    #[test]
    fn differing_manual_values_block_components_that_gathered_both() {
        let ranks = ProviderRanks::from([("top".to_string(), 0)]);
        let mut links = LinkedIds::default();
        for (entities, link, pinned) in [
            ([DbId(1), DbId(2), DbId(3)], "first", "x"),
            ([DbId(4), DbId(5), DbId(6)], "second", "y"),
        ] {
            for (entity, value, source) in [
                (entities[0], "x", IdSource::Resolved),
                (entities[1], "y", IdSource::Resolved),
                (entities[2], pinned, IdSource::Manual),
            ] {
                links.add(entity, "low", "id", link, IdSource::Resolved, true);
                links.add(entity, "pinned", "id", value, source, false);
            }
            links.add(entities[0], "top", "id", "shared", IdSource::Resolved, true);
        }

        let components = links.components(&ranks);
        let ids: Vec<_> = components.iter().map(|c| c.ids.clone()).collect();
        assert_eq!(
            ids,
            [
                vec![DbId(1), DbId(2), DbId(3)],
                vec![DbId(4), DbId(5), DbId(6)]
            ]
        );
    }

    #[test]
    fn only_unique_ids_and_undeclared_manual_ones_are_recorded() {
        let declared = DeclaredIds::unique([("link", "id")]).and_non_unique([("declared", "id")]);
        let external_id = |provider_id: &str, value: &str, source| ExternalId {
            db_id: None,
            provider_id: provider_id.to_string(),
            id_type: "id".to_string(),
            id_value: value.to_string(),
            source,
            resolved_value: None,
        };
        let pair = |provider_id: &str, source| {
            let mut links = LinkedIds::default();
            for (entity, value) in [(DbId(1), "one"), (DbId(2), "two")] {
                for id in [
                    external_id("link", "shared", IdSource::Resolved),
                    external_id(provider_id, value, source),
                ] {
                    links.add_external_id(entity, &id, &declared, true);
                }
            }
            links
        };
        let ranks = ProviderRanks::new();

        assert!(
            !joined(&pair("undeclared", IdSource::Manual), &ranks),
            "an undeclared provider's manual values keep entities apart"
        );
        assert!(
            joined(&pair("undeclared", IdSource::Resolved), &ranks),
            "its resolved ones say nothing"
        );
        assert!(
            joined(&pair("declared", IdSource::Manual), &ranks),
            "nor do a non-unique type's manual ones"
        );
        let mut undeclared_shared = LinkedIds::default();
        for entity in [DbId(1), DbId(2)] {
            undeclared_shared.add_external_id(
                entity,
                &external_id("undeclared", "shared", IdSource::Manual),
                &declared,
                true,
            );
        }
        assert!(
            !joined(&undeclared_shared, &ranks),
            "an undeclared value never links"
        );
    }

    #[test]
    fn a_shared_manual_value_links_like_a_resolved_one() {
        let mut links = LinkedIds::default();
        links.add(DbId(1), "provider", "id", "shared", IdSource::Manual, true);
        links.add(
            DbId(2),
            "provider",
            "id",
            "shared",
            IdSource::Resolved,
            true,
        );

        assert!(joined(&links, &ProviderRanks::new()));
    }
}
