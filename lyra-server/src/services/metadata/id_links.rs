// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Which entities external IDs link as duplicates of one another.

use std::collections::{
    BTreeMap,
    BTreeSet,
    HashMap,
};
use std::fmt;

use agdb::{
    DbAny,
    DbId,
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

/// The external IDs of the entities a dedup run considers.
#[derive(Default)]
pub(crate) struct LinkedIds {
    groups: BTreeMap<IdKey, Vec<DbId>>,
    /// Each entity's values for the ID types that name exactly one entity.
    identities: HashMap<DbId, BTreeMap<(String, String), BTreeSet<String>>>,
}

impl LinkedIds {
    /// Records an ID that names exactly one entity. Entities holding the same
    /// value link when `links` is set; different values can keep them apart.
    /// Empty values say nothing.
    pub(crate) fn add(
        &mut self,
        entity: DbId,
        provider_id: &str,
        id_type: &str,
        value: &str,
        links: bool,
    ) {
        let value = value.trim();
        if value.is_empty() {
            return;
        }
        self.identities
            .entry(entity)
            .or_default()
            .entry((provider_id.to_string(), id_type.to_string()))
            .or_default()
            .insert(value.to_string());
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

    /// Joins the entities that share an ID into components, so each component
    /// picks one winner however its IDs are ordered. A join is skipped when
    /// the two sides share no value from a provider ranked at or above
    /// the linking one: a merge cannot be undone, while a missed one leaves a
    /// visible duplicate. A lower-ranked provider's value gives way, as a
    /// merge's loser's IDs do. Single-member components are left out.
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
                let conflict =
                    root_identities.get(&first_root).and_then(|first_ids| {
                        root_identities
                            .get(&id_root)?
                            .iter()
                            .find_map(|(id_type, values)| {
                                let first_values = first_ids.get(id_type)?;
                                (first_values.is_disjoint(values) && blocks(&id_type.0))
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
                links.add(entity, link, "id", "shared", true);
                links.add(entity, conflict, "id", value, false);
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
            links.add(entity, provider_id, "id", value, true);
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
            links.add(entity, "provider", "id", "shared", true);
            links.add(entity, "musicbrainz", "id", value, false);
        }

        let components = links.components(&ranks);
        assert_eq!(components.len(), 1);
        assert_eq!(components[0].ids, [DbId(2), DbId(3)]);
    }
}
