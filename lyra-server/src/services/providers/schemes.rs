// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::{
    BTreeMap,
    BTreeSet,
    HashMap,
    btree_map::Entry,
};

use lyra_db::{
    IdSource,
    external_ids::ExternalId,
};

/// Checks that `scheme` is `namespace:kind` or a bare standard name, where
/// each part is lowercase ASCII letters, digits, `-` or `_`, starting with a
/// letter.
pub(crate) fn validate_id_scheme(scheme: &str) -> Result<(), String> {
    let mut parts = scheme.split(':');
    let valid = parts.next().is_some_and(is_valid_part)
        && parts.next().is_none_or(is_valid_part)
        && parts.next().is_none();
    if valid {
        Ok(())
    } else {
        Err(format!(
            "scheme '{scheme}' must be lowercase 'namespace:kind' or a bare name (e.g. 'example:thing', 'isrc')"
        ))
    }
}

fn is_valid_part(part: &str) -> bool {
    let mut chars = part.chars();
    chars.next().is_some_and(|c| c.is_ascii_lowercase())
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_')
}

/// Every provider's resolved, non-empty ids on an entity, keyed by scheme.
pub(crate) type ExternalIdsByScheme = BTreeMap<String, String>;

/// The id fields of an entity in a refresh context.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub(crate) struct RefreshIds {
    /// Every provider's ids, keyed by scheme, as [`IdSchemes::resolve`] gives them.
    pub(crate) external_ids: ExternalIdsByScheme,
    /// The schemes whose provider searched and found no match, in order.
    pub(crate) unmatched: Vec<String>,
    /// The schemes whose value the user set, in order.
    pub(crate) user_set: Vec<String>,
}

/// Snapshot of registered schemes keyed by provider id, then id type.
#[derive(Clone, Debug, Default)]
pub(crate) struct IdSchemes(HashMap<String, HashMap<String, String>>);

impl IdSchemes {
    pub(crate) fn insert(&mut self, provider_id: &str, id_type: &str, scheme: &str) {
        self.0
            .entry(provider_id.to_string())
            .or_default()
            .insert(id_type.to_string(), scheme.to_string());
    }

    pub(crate) fn scheme(&self, provider_id: &str, id_type: &str) -> Option<&str> {
        self.0.get(provider_id)?.get(id_type).map(String::as_str)
    }

    /// The refresh id fields of an entity with `ids`. An empty value is a provider's mark that
    /// it found no match; it counts as unmatched only while no value stands for its scheme.
    pub(crate) fn refresh_ids(&self, ids: &[ExternalId]) -> RefreshIds {
        let external_ids = self.resolve(ids);
        let mut unmatched = BTreeSet::new();
        let mut user_set = BTreeSet::new();
        for id in ids {
            let Some(scheme) = self.scheme(&id.provider_id, &id.id_type) else {
                continue;
            };
            if id.id_value.trim().is_empty() {
                if !external_ids.contains_key(scheme) {
                    unmatched.insert(scheme.to_string());
                }
            } else if id.source == IdSource::Manual {
                user_set.insert(scheme.to_string());
            }
        }
        RefreshIds {
            external_ids,
            unmatched: unmatched.into_iter().collect(),
            user_set: user_set.into_iter().collect(),
        }
    }

    /// Rows without a registered scheme are omitted. When several rows share
    /// a scheme, user-set IDs beat plugin-set ones, then the lowest
    /// `(provider_id, id_type)` wins.
    pub(crate) fn resolve(&self, ids: &[ExternalId]) -> ExternalIdsByScheme {
        let mut best: BTreeMap<&str, ((u8, &str, &str), &str)> = BTreeMap::new();
        for id in ids {
            let Some(scheme) = self.scheme(&id.provider_id, &id.id_type) else {
                continue;
            };
            let value = id.id_value.trim();
            if value.is_empty() {
                continue;
            }
            let rank = match id.source {
                IdSource::Manual => 0,
                IdSource::Resolved => 1,
            };
            let key = (rank, id.provider_id.as_str(), id.id_type.as_str());
            match best.entry(scheme) {
                Entry::Vacant(entry) => {
                    entry.insert((key, value));
                }
                Entry::Occupied(mut entry) if key < entry.get().0 => {
                    entry.insert((key, value));
                }
                Entry::Occupied(_) => {}
            }
        }
        best.into_iter()
            .map(|(scheme, (_, value))| (scheme.to_string(), value.to_string()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(provider_id: &str, id_type: &str, value: &str, source: IdSource) -> ExternalId {
        ExternalId {
            db_id: None,
            provider_id: provider_id.to_string(),
            id_type: id_type.to_string(),
            id_value: value.to_string(),
            source,
            resolved_value: None,
        }
    }

    #[test]
    fn refresh_ids_list_unmatched_and_user_set_schemes() {
        let mut schemes = IdSchemes::default();
        schemes.insert("alpha", "release_id", "alpha:release");
        schemes.insert("beta", "album_id", "beta:album");
        schemes.insert("gamma", "item_id", "gamma:item");

        let ids = schemes.refresh_ids(&[
            row("alpha", "release_id", "user-value", IdSource::Manual),
            row("beta", "album_id", "", IdSource::Resolved),
            row("gamma", "item_id", "found", IdSource::Resolved),
            row("unregistered", "thing", "", IdSource::Resolved),
        ]);

        assert_eq!(
            ids.external_ids,
            BTreeMap::from([
                ("alpha:release".to_string(), "user-value".to_string()),
                ("gamma:item".to_string(), "found".to_string()),
            ])
        );
        assert_eq!(ids.unmatched, ["beta:album"]);
        assert_eq!(ids.user_set, ["alpha:release"]);
    }

    #[test]
    fn accepts_namespaced_and_bare_schemes() {
        for scheme in ["example:thing", "example:thing-group", "isrc", "a1:b_2"] {
            assert!(validate_id_scheme(scheme).is_ok(), "{scheme}");
        }
    }

    #[test]
    fn rejects_malformed_schemes() {
        for scheme in [
            "",
            "Example:thing",
            "example:",
            ":thing",
            "example:thing:extra",
            "example.thing",
            "example thing",
            "1example",
            "example:-thing",
        ] {
            assert!(validate_id_scheme(scheme).is_err(), "{scheme}");
        }
    }

    fn id(provider_id: &str, id_type: &str, id_value: &str, source: IdSource) -> ExternalId {
        ExternalId {
            db_id: None,
            provider_id: provider_id.to_string(),
            id_type: id_type.to_string(),
            id_value: id_value.to_string(),
            source,
            resolved_value: None,
        }
    }

    fn schemes() -> IdSchemes {
        let mut schemes = IdSchemes::default();
        schemes.insert("alpha", "thing_id", "example:thing");
        schemes.insert("beta", "thing_ref", "example:thing");
        schemes.insert("beta", "code", "code");
        schemes
    }

    #[test]
    fn resolve_maps_rows_to_schemes_and_omits_unregistered() {
        let resolved = schemes().resolve(&[
            id("alpha", "thing_id", "a-1", IdSource::Resolved),
            id("beta", "code", "c-1", IdSource::Resolved),
            id("beta", "other", "o-1", IdSource::Resolved),
            id("gamma", "thing_id", "g-1", IdSource::Manual),
        ]);

        assert_eq!(
            resolved,
            BTreeMap::from([
                ("code".to_string(), "c-1".to_string()),
                ("example:thing".to_string(), "a-1".to_string()),
            ])
        );
    }

    #[test]
    fn resolve_prefers_user_ids_then_lowest_provider() {
        let schemes = schemes();
        let user_wins = schemes.resolve(&[
            id("alpha", "thing_id", "from-plugin", IdSource::Resolved),
            id("beta", "thing_ref", "from-user", IdSource::Manual),
        ]);
        assert_eq!(user_wins["example:thing"], "from-user");

        let tie = [
            id("beta", "thing_ref", "from-beta", IdSource::Resolved),
            id("alpha", "thing_id", "from-alpha", IdSource::Resolved),
        ];
        assert_eq!(schemes.resolve(&tie)["example:thing"], "from-alpha");
        let reversed = [tie[1].clone(), tie[0].clone()];
        assert_eq!(schemes.resolve(&reversed)["example:thing"], "from-alpha");
    }
}
