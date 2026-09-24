// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::{
    BTreeMap,
    HashMap,
    btree_map::Entry,
};

use crate::db::{
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

#[cfg(feature = "docgen")]
pub(crate) const IDENTIFIERS_DOC: &str = "Non-empty identifier values keyed by scheme (e.g. `isrc`), whichever provider stored them. Read other plugins' IDs here; read your own from `external_ids`.";

/// Resolved, non-empty identifier values keyed by scheme.
pub(crate) type IdentifiersByScheme = BTreeMap<String, String>;

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

    fn scheme(&self, provider_id: &str, id_type: &str) -> Option<&str> {
        self.0.get(provider_id)?.get(id_type).map(String::as_str)
    }

    /// Rows without a registered scheme are omitted. When several rows share
    /// a scheme, user-set IDs beat plugin-set ones, then the lowest
    /// `(provider_id, id_type)` wins.
    pub(crate) fn resolve(&self, ids: &[ExternalId]) -> IdentifiersByScheme {
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
                IdSource::User => 0,
                IdSource::Plugin => 1,
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
            id("alpha", "thing_id", "a-1", IdSource::Plugin),
            id("beta", "code", "c-1", IdSource::Plugin),
            id("beta", "other", "o-1", IdSource::Plugin),
            id("gamma", "thing_id", "g-1", IdSource::User),
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
            id("alpha", "thing_id", "from-plugin", IdSource::Plugin),
            id("beta", "thing_ref", "from-user", IdSource::User),
        ]);
        assert_eq!(user_wins["example:thing"], "from-user");

        let tie = [
            id("beta", "thing_ref", "from-beta", IdSource::Plugin),
            id("alpha", "thing_id", "from-alpha", IdSource::Plugin),
        ];
        assert_eq!(schemes.resolve(&tie)["example:thing"], "from-alpha");
        let reversed = [tie[1].clone(), tie[0].clone()];
        assert_eq!(schemes.resolve(&reversed)["example:thing"], "from-alpha");
    }
}
