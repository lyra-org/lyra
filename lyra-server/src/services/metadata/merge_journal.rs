// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Where merged artists and releases went, for provider writes that began before the merge.
//! Merges enter it once committed, under the database write guard, so no writer sees a loser
//! missing without its entry here.

use std::{
    collections::HashMap,
    sync::Mutex,
};

use agdb::DbId;
use anyhow::Context;
use lyra_db::DbAccess;

/// A merge's public ids, read before the loser is removed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Merge {
    loser: String,
    winner: String,
}

impl Merge {
    pub(crate) fn of(db: &impl DbAccess, winner: DbId, loser: DbId) -> anyhow::Result<Self> {
        let public_id = |id: DbId| {
            lyra_db::lookup::find_id_by_db_id(db, id)?
                .with_context(|| format!("merged entity {} has no public id", id.0))
        };
        Ok(Self {
            loser: public_id(loser)?,
            winner: public_id(winner)?,
        })
    }
}

#[derive(Default)]
pub(crate) struct MergeJournal {
    winners: Mutex<HashMap<String, String>>,
}

impl MergeJournal {
    /// Records committed merges. The caller must still hold the database write guard.
    pub(crate) fn record(&self, merges: impl IntoIterator<Item = Merge>) {
        let mut winners = self.winners.lock().expect("merge journal poisoned");
        winners.extend(merges.into_iter().map(|merge| (merge.loser, merge.winner)));
    }

    /// The id `id` ended up as, following its winner through any later merges; `None` for an id
    /// no merge took. The id returned may name nothing, if its entity was later removed.
    pub(crate) fn resolve(&self, id: &str) -> Option<String> {
        let winners = self.winners.lock().expect("merge journal poisoned");
        let mut current = winners.get(id)?;
        for _ in 0..winners.len() {
            match winners.get(current) {
                Some(next) => current = next,
                None => break,
            }
        }
        Some(current.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn merge(loser: &str, winner: &str) -> Merge {
        Merge {
            loser: loser.to_string(),
            winner: winner.to_string(),
        }
    }

    #[test]
    fn an_id_no_merge_took_resolves_to_nothing() {
        assert_eq!(MergeJournal::default().resolve("live"), None);
    }

    #[test]
    fn a_chain_of_merges_resolves_to_its_last_winner() {
        let journal = MergeJournal::default();
        journal.record([merge("a", "b"), merge("b", "c")]);
        assert_eq!(journal.resolve("a").as_deref(), Some("c"));
        assert_eq!(journal.resolve("b").as_deref(), Some("c"));
        assert_eq!(journal.resolve("c"), None);
    }
}
