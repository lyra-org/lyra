// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::cmp::Ordering;
use std::collections::{
    HashMap,
    HashSet,
};
use std::sync::{
    Arc,
    Mutex,
    Weak,
};

use agdb::{
    DbAny,
    DbAnyTransactionMut,
    DbId,
};

use super::id_links::{
    DeclaredIds,
    LinkedIds,
    provider_ranks,
};
use super::layers::MANUAL_SOURCE_ID;
use super::merge_journal::{
    Merge,
    MergeJournal,
};
use lyra_db::graph::RetargetLookup;
use lyra_db::roots::{
    self,
    Root,
};
use lyra_db::{
    Artist,
    DbAsync,
    graph::edge_count_map,
};

/// How many artist candidates one write checks, and at most how many artists it removes;
/// collecting many holds the write lock a batch at a time. A batch still reads every artist
/// behind each candidate it checks.
const ARTIST_BATCH: usize = 256;

/// The flows that reference artists across writes of their own, against the collection of
/// unreferenced artists.
///
/// A provider's refresh ensures an artist in one write and credits it in a later one, or drops a
/// credit and names the artist again further on; between the two the artist is unreferenced. The
/// refresh holds this shared until its handler has settled. A collection takes it for each of its
/// writes without waiting, and stops where a flow holds it: its candidates are stored, and the
/// collection starts again when a flow lets go. Each generation has its own.
#[derive(Clone, Default)]
pub(crate) struct ArtistReferences {
    inner: Arc<ArtistReferencesInner>,
}

#[derive(Default)]
struct ArtistReferencesInner {
    flows: Arc<tokio::sync::RwLock<()>>,
    /// The collection a flow held back, to start again on the runtime it ran on.
    held_back: Mutex<Option<(DbAsync, tokio::runtime::Handle)>>,
}

/// A flow's hold of [`ArtistReferences`], for as long as it lives.
pub(crate) struct ArtistReferenceFlow {
    flow: Option<tokio::sync::OwnedRwLockReadGuard<()>>,
    /// Weak, so a flow that outlives its generation starts nothing on a database no longer in
    /// use.
    references: Weak<ArtistReferencesInner>,
}

impl Drop for ArtistReferenceFlow {
    fn drop(&mut self) {
        // Let go first, so the collection this starts finds the flows free unless another holds.
        drop(self.flow.take());
        let Some(inner) = self.references.upgrade() else {
            return;
        };
        let held_back = inner
            .held_back
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        let Some((db, runtime)) = held_back else {
            return;
        };
        let references = ArtistReferences { inner };
        runtime.spawn(async move {
            if let Err(err) = references.collect_unreferenced(&db).await {
                tracing::warn!(
                    error = %format!("{err:#}"),
                    "collecting the unreferenced artists a refresh held back failed"
                );
            }
        });
    }
}

/// What a collection of unreferenced artists did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct CollectedArtists {
    pub(crate) removed: usize,
    /// Whether a flow held the collection back, leaving candidates for the collection that
    /// starts when it lets go.
    pub(crate) held_back: bool,
}

impl ArtistReferences {
    /// Holds collections back until the returned flow is dropped. Waits only for a collection's
    /// write in progress.
    pub(crate) async fn flow(&self) -> ArtistReferenceFlow {
        ArtistReferenceFlow {
            flow: Some(Arc::clone(&self.inner.flows).read_owned().await),
            references: Arc::downgrade(&self.inner),
        }
    }

    /// Removes the artists nothing references any more, among the candidates the writes since
    /// the last collection left. Takes no write lock when there are none.
    pub(crate) async fn collect_unreferenced(
        &self,
        db: &DbAsync,
    ) -> anyhow::Result<CollectedArtists> {
        let mut collected = CollectedArtists::default();
        if !lyra_db::orphans::has_artist_candidates(&*db.read().await)? {
            return Ok(collected);
        }
        loop {
            let pass = {
                let mut db_write = db.write().await;
                // Taken under the write lock, so a flow that starts now waits for this write
                // rather than for the lock.
                let Some(_no_flow) = self.without_flows(db) else {
                    collected.held_back = true;
                    return Ok(collected);
                };
                let pass =
                    lyra_db::orphans::collect_artist_candidates(&mut db_write, ARTIST_BATCH)?;
                crate::services::covers::discard_listed_cover_files(&*db_write);
                pass
            };
            collected.removed += pass.removed;
            if !pass.remaining {
                return Ok(collected);
            }
        }
    }

    /// The flows, taken while none holds them. Otherwise the collection of `db` is left for the
    /// next flow to let go to start again.
    fn without_flows(&self, db: &DbAsync) -> Option<tokio::sync::RwLockWriteGuard<'_, ()>> {
        if let Ok(no_flow) = self.inner.flows.try_write() {
            return Some(no_flow);
        }
        let mut held_back = self
            .inner
            .held_back
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *held_back = Some((db.clone(), tokio::runtime::Handle::current()));
        // A flow that let go before it could see that left nobody to start the collection.
        let no_flow = self.inner.flows.try_write().ok()?;
        *held_back = None;
        Some(no_flow)
    }
}

/// [`ArtistReferences::collect_unreferenced`] among the current generation's flows.
pub(crate) async fn collect_unreferenced_artists(db: &DbAsync) -> anyhow::Result<CollectedArtists> {
    crate::services::STATE
        .generation()
        .artist_references
        .collect_unreferenced(db)
        .await
}

/// Merge artists that share the same external ID (e.g. MusicBrainz MBID) but were
/// created as separate nodes due to different file tag spellings. Only the
/// `(provider_id, id_type)` pairs declared unique link or keep artists apart.
pub(crate) fn deduplicate_artists_by_external_id(
    db: &mut DbAny,
    declared: &DeclaredIds,
    journal: &MergeJournal,
) -> anyhow::Result<u32> {
    if !declared.has_unique() {
        return Ok(0);
    }

    let all_artists: Vec<Artist> = roots::members::<Artist>(db, Root::Artists)?;

    let mut linked = LinkedIds::default();
    let mut artist_lookup: HashMap<DbId, Artist> = HashMap::new();
    for artist in &all_artists {
        let Some(artist_db_id) = artist.db_id.clone().map(DbId::from) else {
            continue;
        };
        artist_lookup.insert(artist_db_id, artist.clone());
        for ext_id in lyra_db::external_ids::get_for_entity(db, artist_db_id)? {
            linked.add_external_id(artist_db_id, &ext_id, declared, true);
        }
        for alias in lyra_db::external_ids::get_aliases_for_entity(db, artist_db_id)? {
            linked.add_alias(artist_db_id, &alias, declared, true);
        }
    }

    let ranks = provider_ranks(db)?;
    let mut merged_count: u32 = 0;
    for component in linked.components(&ranks) {
        let winner = select_merge_winner(db, &artist_lookup, &component.ids)?;
        for &loser in component.ids.iter().filter(|&&id| id != winner) {
            tracing::info!(
                winner = winner.0,
                loser = loser.0,
                external_ids = ?component.keys,
                "merging duplicate artist"
            );
            let merge = db.transaction_mut(|t| {
                migrate_metadata(t, winner, loser)?;
                lyra_db::external_ids::absorb_inside_tx(t, winner, loser)?;
                merge_artist_into(t, winner, loser)
            })?;
            journal.record([merge]);
            merged_count += 1;
        }
    }

    if merged_count > 0 {
        tracing::info!(merged_count, "deduplicated artists by external id");
    }

    Ok(merged_count)
}

fn select_merge_winner(
    db: &DbAny,
    artist_lookup: &HashMap<DbId, Artist>,
    artist_ids: &[DbId],
) -> anyhow::Result<DbId> {
    // Layer edges are not effective references, so they never pick the winner; nor is waiting
    // for a collection.
    let mut edge_counts = edge_count_map(db, artist_ids)?;
    for (&artist_id, count) in &mut edge_counts {
        *count = count
            .saturating_sub(lyra_db::metadata::layers::edges::reference_count(
                db, artist_id,
            )?)
            .saturating_sub(u64::from(lyra_db::orphans::is_artist_candidate(
                db, artist_id,
            )?));
    }
    let mut ordered_ids = artist_ids.to_vec();

    ordered_ids.sort_by(|a, b| {
        let a_artist = artist_lookup.get(a);
        let b_artist = artist_lookup.get(b);

        let a_edges = edge_counts.get(a).copied().unwrap_or(0);
        let b_edges = edge_counts.get(b).copied().unwrap_or(0);
        match b_edges.cmp(&a_edges) {
            Ordering::Equal => {}
            ord => return ord,
        }

        let a_created_at = a_artist
            .and_then(|artist| artist.created_at_ms)
            .unwrap_or(u64::MAX);
        let b_created_at = b_artist
            .and_then(|artist| artist.created_at_ms)
            .unwrap_or(u64::MAX);
        match a_created_at.cmp(&b_created_at) {
            Ordering::Equal => {}
            ord => return ord,
        }

        a.0.cmp(&b.0)
    });

    let winner = ordered_ids[0];
    let winner_artist = artist_lookup.get(&winner);
    tracing::debug!(
        winner = winner.0,
        winner_created_at = winner_artist.and_then(|artist| artist.created_at_ms),
        winner_edges = edge_counts.get(&winner).copied().unwrap_or(0),
        "selected artist merge winner"
    );

    Ok(winner)
}

pub(super) fn merge_artist_into(
    db: &mut DbAnyTransactionMut<'_>,
    winner: DbId,
    loser: DbId,
) -> anyhow::Result<Merge> {
    let merge = Merge::of(db, winner, loser)?;
    let outgoing_relations = lyra_db::artists::relations::get_relations_from(db, loser, None)?;

    // Layers own the incoming relations; re-applying the winner below rebuilds
    // them. The outgoing ones follow the related artists' retargeted layers.
    for (relation, target_id) in outgoing_relations {
        if target_id != winner
            && !lyra_db::artists::relations::exists(db, winner, target_id, relation.relation_type)?
        {
            lyra_db::artists::relations::link(
                db,
                winner,
                target_id,
                relation.relation_type,
                relation.attributes,
            )?;
        }
    }

    // A locked winner keeps its stored relations through the apply below, so
    // the loser's move onto it until it is unlocked.
    if lyra_db::artists::get_by_id(db, winner)?.is_some_and(|artist| artist.locked.unwrap_or(false))
    {
        for (relation, source_id) in lyra_db::artists::relations::get_relations_to(db, loser, None)?
        {
            if source_id != winner
                && !lyra_db::artists::relations::exists(
                    db,
                    source_id,
                    winner,
                    relation.relation_type,
                )?
            {
                lyra_db::artists::relations::link(
                    db,
                    source_id,
                    winner,
                    relation.relation_type,
                    relation.attributes,
                )?;
            }
        }
    }

    lyra_db::metadata::layers::edges::retarget_artist(db, winner, loser)?;

    // Layers own the credits; re-applying their owners below rebuilds them.
    // A locked owner keeps what is stored, so its credits move.
    let winner_credits =
        lyra_db::credits::CreditSet::new(&lyra_db::credits::links_to_artist(db, winner)?);
    let credit_links = lyra_db::credits::links_to_artist(db, loser)?;
    let mut reapplied_owners = Vec::new();
    for link in &credit_links {
        if !credits_locked(db, link.owner_id)? {
            if !reapplied_owners.contains(&link.owner_id) {
                reapplied_owners.push(link.owner_id);
            }
            continue;
        }
        if !winner_credits.contains(link.owner_id, winner, link.credit.role()) {
            lyra_db::credits::link(db, link.owner_id, winner, &link.credit)?;
        }
    }
    lyra_db::artists::absorb_scan_names(db, winner, loser)?;
    lyra_db::favorites::retarget(db, loser, winner, RetargetLookup::Owners)?;
    lyra_db::ratings::retarget(db, loser, winner, RetargetLookup::Owners)?;
    lyra_db::tags::retarget(db, loser, winner, RetargetLookup::Owners)?;
    lyra_db::covers::adopt_missing(db, winner, loser)?;
    // `merge_artist_into` runs inside `deduplicate_artists_by_external_id`'s outer
    // `transaction_mut`; use the in-txn variant — agdb does not support reentrant transactions.
    lyra_db::metadata::cascade_remove_entities_in_txn(db, &[loser])?;
    crate::services::covers::discard_listed_cover_files(db);
    for owner_id in reapplied_owners {
        crate::services::metadata::merging::apply_merged_metadata_to_entity_inside_tx(
            db, owner_id,
        )?;
    }
    crate::services::metadata::merging::apply_merged_metadata_to_entity_inside_tx(db, winner)?;
    Ok(merge)
}

/// Whether a lock keeps a release's or track's stored credits.
fn credits_locked(db: &impl lyra_db::DbAccess, owner_id: DbId) -> anyhow::Result<bool> {
    Ok(match lyra_db::releases::get_by_id(db, owner_id)? {
        Some(release) => release.locked.unwrap_or(false),
        None => lyra_db::tracks::get_by_id(db, owner_id)?
            .is_some_and(|track| track.locked.unwrap_or(false)),
    })
}

pub(super) fn migrate_metadata(
    db: &mut DbAnyTransactionMut<'_>,
    winner: DbId,
    loser: DbId,
) -> anyhow::Result<()> {
    let loser_layers = lyra_db::metadata::layers::get_for_entity(db, loser)?;
    let winner_layers = lyra_db::metadata::layers::get_for_entity(db, winner)?;
    let winner_source_ids: HashSet<&str> =
        winner_layers.iter().map(|l| l.source_id.as_str()).collect();

    for layer in &loser_layers {
        if layer.source_id == MANUAL_SOURCE_ID {
            // The winner's manual edits win; the loser's fill what they leave out.
            lyra_db::metadata::layers::fill_inside_tx(db, loser, winner, MANUAL_SOURCE_ID)?;
            continue;
        }
        if !winner_source_ids.contains(layer.source_id.as_str()) {
            lyra_db::metadata::layers::copy_inside_tx(db, loser, winner, &layer.source_id)?;
            continue;
        }
        // Both duplicates heard from this source; neither one's relations
        // are wrong, so the winner's layer keeps both.
        let Some(winner_layer) = winner_layers
            .iter()
            .find(|winner_layer| winner_layer.source_id == layer.source_id)
        else {
            continue;
        };
        let mut loser_relations = layer.relations.clone();
        for inputs in loser_relations.values_mut() {
            inputs.retain(|input| input.artist_id != winner);
        }
        if loser_relations.is_empty() {
            continue;
        }
        let relations =
            lyra_db::metadata::layers::union_relations(&winner_layer.relations, loser_relations);
        let mut merged = winner_layer.clone().with_relations(relations);
        merged.updated_at_ms = merged.updated_at_ms.max(layer.updated_at_ms);
        lyra_db::metadata::layers::upsert_inside_tx(db, winner, &merged)?;
    }

    lyra_db::metadata::custom_fields::copy_between_entities(db, loser, winner)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use agdb::QueryBuilder;
    use lyra_db::{
        IdSource,
        test_db,
    };

    fn deduplicate_artists_by_external_id(
        db: &mut DbAny,
        declared: &DeclaredIds,
    ) -> anyhow::Result<u32> {
        let merged =
            super::deduplicate_artists_by_external_id(db, declared, &MergeJournal::default())?;
        lyra_db::orphans::testing::assert_artists_tracked(db)?;
        Ok(merged)
    }

    #[tokio::test]
    async fn collecting_artists_takes_no_write_lock_without_candidates() -> anyhow::Result<()> {
        let db = DbAsync::new(test_db::new_test_db()?);
        let (credited, unreferenced) = {
            let mut db = db.write().await;
            let track = lyra_db::fixtures::insert_track(&mut db, "Track")?;
            let credited = lyra_db::artists::create(&mut *db, "Credited", None)?;
            test_db::connect_artist(&mut db, track, credited)?;
            (
                credited,
                lyra_db::artists::create(&mut *db, "Unreferenced", None)?,
            )
        };

        let flows = ArtistReferences::default();
        assert_eq!(flows.collect_unreferenced(&db).await?.removed, 1);
        {
            let db = db.read().await;
            assert!(lyra_db::artists::get_by_id(&*db, credited)?.is_some());
            assert!(lyra_db::artists::get_by_id(&*db, unreferenced)?.is_none());
        }

        // A writer would wait for this reader for ever.
        let reading = db.read().await;
        let collected = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            flows.collect_unreferenced(&db),
        )
        .await;
        drop(reading);
        assert_eq!(
            collected.expect("no write lock is taken")?,
            CollectedArtists::default()
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_collection_a_flow_holds_back_runs_once_the_flows_let_go() -> anyhow::Result<()> {
        use lyra_db::credits::{
            CreditLinkInput,
            replace_for_owner,
        };

        let db = DbAsync::new(test_db::new_test_db()?);
        let flows = ArtistReferences::default();
        let (track, kept, abandoned) = {
            let mut db = db.write().await;
            let track = lyra_db::fixtures::insert_track(&mut db, "Track")?;
            let kept = lyra_db::artists::create(&mut *db, "Kept", None)?;
            test_db::connect_artist(&mut db, track, kept)?;
            (
                track,
                kept,
                lyra_db::artists::create(&mut *db, "Abandoned", None)?,
            )
        };
        let reader = db.clone();
        let exists = async |artist: DbId| -> anyhow::Result<bool> {
            Ok(lyra_db::artists::get_by_id(&*reader.read().await, artist)?.is_some())
        };
        let credit = |artist_ids: &[DbId]| {
            artist_ids
                .iter()
                .map(|&artist_id| CreditLinkInput {
                    artist_id,
                    credit_type: lyra_db::CreditType::Artist,
                    detail: None,
                })
                .collect::<Vec<_>>()
        };

        // The flow drops the artist's only reference in one write.
        let flow = flows.flow().await;
        db.write()
            .await
            .transaction_mut(|t| replace_for_owner(t, track, &credit(&[])))?;

        let collected = flows.collect_unreferenced(&db).await?;
        assert_eq!(
            collected,
            CollectedArtists {
                removed: 0,
                held_back: true
            }
        );
        assert!(exists(kept).await?);
        assert!(exists(abandoned).await?);

        // And names it again in a later one.
        db.write()
            .await
            .transaction_mut(|t| replace_for_owner(t, track, &credit(&[kept])))?;
        // A second flow still holds: letting go of the first starts the collection again, only
        // for it to be held back once more.
        let second = flows.flow().await;
        drop(flow);
        for _ in 0..64 {
            tokio::task::yield_now().await;
        }
        assert!(exists(abandoned).await?);

        // The collection held back starts by itself once the last flow lets go.
        drop(second);
        tokio::time::timeout(std::time::Duration::from_secs(60), async {
            while exists(abandoned).await? {
                tokio::task::yield_now().await;
            }
            anyhow::Ok(())
        })
        .await
        .expect("the held-back collection runs")?;
        assert!(exists(kept).await?);

        // Nothing was held back since, so a flow that ends starts no collection.
        let unreferenced = lyra_db::artists::create(&mut *db.write().await, "Later", None)?;
        drop(flows.flow().await);
        for _ in 0..64 {
            tokio::task::yield_now().await;
        }
        assert!(exists(unreferenced).await?);
        let collected = flows.collect_unreferenced(&db).await?;
        assert_eq!(
            collected,
            CollectedArtists {
                removed: 1,
                held_back: false
            }
        );
        lyra_db::orphans::testing::assert_artists_tracked(&*db.read().await)?;
        assert!(!lyra_db::orphans::has_artist_candidates(&*db.read().await)?);
        Ok(())
    }

    #[tokio::test]
    async fn a_flow_that_outlives_its_generation_starts_no_collection() -> anyhow::Result<()> {
        let db = DbAsync::new(test_db::new_test_db()?);
        let flows = ArtistReferences::default();
        let unreferenced = lyra_db::artists::create(&mut *db.write().await, "Left", None)?;
        let flow = flows.flow().await;
        assert!(flows.collect_unreferenced(&db).await?.held_back);

        // The generation goes, as a reset drops it, while the flow still runs.
        drop(flows);
        drop(flow);
        for _ in 0..64 {
            tokio::task::yield_now().await;
        }

        assert!(lyra_db::artists::get_by_id(&*db.read().await, unreferenced)?.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn collecting_goes_on_past_one_batch_of_candidates() -> anyhow::Result<()> {
        let db = DbAsync::new(test_db::new_test_db()?);
        {
            let mut db = db.write().await;
            for index in 0..ARTIST_BATCH + 3 {
                lyra_db::artists::create(&mut *db, &format!("Artist {index}"), None)?;
            }
        }

        let collected = ArtistReferences::default()
            .collect_unreferenced(&db)
            .await?;
        assert_eq!(collected.removed, ARTIST_BATCH + 3);
        assert!(!collected.held_back);

        lyra_db::orphans::testing::assert_drained(&*db.read().await)
    }

    #[test]
    fn merges_committed_before_a_later_failure_stay_recorded() -> anyhow::Result<()> {
        let mut db = test_db::new_test_db()?;
        let winner = lyra_db::fixtures::insert_artist(&mut db, "Radiohead")?;
        for _ in 0..3 {
            let release = lyra_db::fixtures::insert_release(&mut db, "Release")?;
            test_db::connect_artist(&mut db, release, winner)?;
        }
        // Merged in id order, so the broken one fails after the first has committed.
        let merged = lyra_db::fixtures::insert_artist(&mut db, "Radiohead")?;
        let broken = lyra_db::fixtures::insert_artist(&mut db, "Radiohead")?;
        let mut public_ids = Vec::new();
        for artist in [winner, merged, broken] {
            lyra_db::external_ids::upsert(
                &mut db,
                artist,
                "provider-a",
                "artist_id",
                "shared",
                IdSource::Resolved,
            )?;
            public_ids.push(lyra_db::lookup::find_id_by_db_id(&db, artist)?.expect("public id"));
        }
        let malformed_layer = db
            .exec_mut(
                QueryBuilder::insert()
                    .nodes()
                    .values([[("db_element_id", "ArtistLayer").into()]])
                    .query(),
            )?
            .ids()[0];
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(broken)
                .to(malformed_layer)
                .query(),
        )?;
        let journal = MergeJournal::default();

        let result =
            super::deduplicate_artists_by_external_id(&mut db, &unique_artist_ids(), &journal);

        assert!(result.is_err(), "the broken artist's merge fails");
        assert_eq!(
            journal.resolve(&public_ids[1]).as_deref(),
            Some(public_ids[0].as_str()),
            "the merge before it stays recorded"
        );
        assert_eq!(journal.resolve(&public_ids[2]), None);
        assert!(lyra_db::artists::get_by_id(&db, broken)?.is_some());
        Ok(())
    }

    #[test]
    fn a_committed_artist_deduplication_records_each_loser() -> anyhow::Result<()> {
        let mut db = test_db::new_test_db()?;
        let public_id = |artist: DbId, db: &mut DbAny| -> anyhow::Result<String> {
            lyra_db::external_ids::upsert(
                db,
                artist,
                "provider-a",
                "artist_id",
                "shared",
                IdSource::Resolved,
            )?;
            Ok(lyra_db::lookup::find_id_by_db_id(&*db, artist)?.expect("public id"))
        };
        let winner = lyra_db::fixtures::insert_artist(&mut db, "Radiohead")?;
        let release = lyra_db::fixtures::insert_release(&mut db, "Release")?;
        test_db::connect_artist(&mut db, release, winner)?;
        let loser = lyra_db::fixtures::insert_artist(&mut db, "Radiohead")?;
        let (winner_id, loser_id) = (public_id(winner, &mut db)?, public_id(loser, &mut db)?);
        let journal = MergeJournal::default();

        super::deduplicate_artists_by_external_id(&mut db, &unique_artist_ids(), &journal)?;

        assert_eq!(journal.resolve(&loser_id), Some(winner_id));
        assert_eq!(lyra_db::lookup::find_node_id_by_id(&db, &loser_id)?, None);
        Ok(())
    }

    #[test]
    fn a_merged_artists_favorites_ratings_and_tags_move_to_the_winner() -> anyhow::Result<()> {
        use lyra_db::{
            favorites::FavoriteKind,
            ratings::RatingValue,
        };
        let mut db = test_db::new_test_db()?;
        let alice = test_db::insert_user(&mut db, "alice")?;
        let bob = test_db::insert_user(&mut db, "bob")?;
        let artist = |db: &mut DbAny, credits: usize| -> anyhow::Result<DbId> {
            let artist_id = lyra_db::fixtures::insert_artist(db, "Radiohead")?;
            for _ in 0..credits {
                let release = lyra_db::fixtures::insert_release(db, "Release")?;
                test_db::connect_artist(db, release, artist_id)?;
            }
            lyra_db::external_ids::upsert(
                db,
                artist_id,
                "musicbrainz",
                "artist_id",
                "mbid",
                IdSource::Resolved,
            )?;
            Ok(artist_id)
        };
        let winner = artist(&mut db, 10)?;
        let loser = artist(&mut db, 0)?;
        let rating = |value| RatingValue::new(value).expect("rating");

        lyra_db::favorites::add(&mut db, alice, winner, FavoriteKind::Artist, 10)?;
        lyra_db::favorites::add(&mut db, alice, loser, FavoriteKind::Artist, 5)?;
        lyra_db::favorites::add(&mut db, alice, winner, FavoriteKind::Artist, 30)?;
        lyra_db::favorites::add(&mut db, bob, loser, FavoriteKind::Artist, 7)?;
        lyra_db::ratings::upsert(&mut db, alice, winner, rating(2), 10)?;
        lyra_db::ratings::upsert(&mut db, alice, loser, rating(5), 20)?;
        lyra_db::ratings::upsert(&mut db, bob, loser, rating(3), 8)?;
        for (user, target, name) in [
            (alice, winner, "kept"),
            (alice, loser, "kept"),
            (alice, loser, "moved"),
            (bob, loser, "bob"),
        ] {
            lyra_db::tags::create(&mut db, user, target, name, "blue", 1)?;
        }

        assert_eq!(
            deduplicate_artists_by_external_id(&mut db, &unique_artist_ids())?,
            1
        );
        assert!(lyra_db::artists::get_by_id(&db, loser)?.is_none());

        let favorited = |db: &DbAny, user| -> anyhow::Result<Option<i64>> {
            Ok(
                lyra_db::favorites::first_favorited_at(db, user, FavoriteKind::Artist)?
                    .get(&winner)
                    .copied(),
            )
        };
        assert_eq!(favorited(&db, alice)?, Some(5), "the earlier favorite wins");
        assert_eq!(favorited(&db, bob)?, Some(7));

        let alice_rating = lyra_db::ratings::get(&db, alice, winner)?.expect("alice's rating");
        assert_eq!(
            (alice_rating.value.get(), alice_rating.updated_at_ms),
            (5, 20),
            "the rating changed last wins"
        );
        let bob_rating = lyra_db::ratings::get(&db, bob, winner)?.expect("bob's rating");
        assert_eq!(bob_rating.value.get(), 3);

        let tags = |db: &DbAny, user| -> anyhow::Result<Vec<String>> {
            let mut names: Vec<String> = lyra_db::tags::get_for_target(db, user, winner)?
                .into_iter()
                .map(|tag| tag.tag)
                .collect();
            names.sort();
            Ok(names)
        };
        assert_eq!(tags(&db, alice)?, ["kept", "moved"]);
        assert_eq!(tags(&db, bob)?, ["bob"]);
        Ok(())
    }

    #[test]
    fn a_merged_artists_covers_fill_the_winners_missing_sources() -> anyhow::Result<()> {
        use lyra_db::covers::CoverSource;

        let mut db = test_db::new_test_db()?;
        let artist = |db: &mut DbAny, credits: usize, cover: &str| -> anyhow::Result<DbId> {
            let artist_id = lyra_db::fixtures::insert_artist(db, "Radiohead")?;
            for _ in 0..credits {
                let release = lyra_db::fixtures::insert_release(db, "Release")?;
                test_db::connect_artist(db, release, artist_id)?;
            }
            lyra_db::external_ids::upsert(
                db,
                artist_id,
                "musicbrainz",
                "artist_id",
                "mbid",
                IdSource::Resolved,
            )?;
            let source = if cover == "winner" {
                CoverSource::Provider
            } else {
                CoverSource::Manual
            };
            db.transaction_mut(|t| {
                lyra_db::covers::upsert(
                    t,
                    artist_id,
                    source,
                    lyra_db::Cover {
                        db_id: None,
                        id: cover.to_string(),
                        path: format!("/covers/{cover}.jpg"),
                        mime_type: "image/jpeg".to_string(),
                        hash: cover.to_string(),
                        blurhash: None,
                        provider_id: None,
                    },
                )
            })?;
            Ok(artist_id)
        };
        let winner = artist(&mut db, 10, "winner")?;
        let loser = artist(&mut db, 0, "loser")?;

        assert_eq!(
            deduplicate_artists_by_external_id(&mut db, &unique_artist_ids())?,
            1
        );
        assert!(lyra_db::artists::get_by_id(&db, loser)?.is_none());
        let kept = lyra_db::covers::sources(&db, winner)?
            .into_iter()
            .map(|sourced| (sourced.source, sourced.cover.id))
            .collect::<Vec<_>>();
        assert_eq!(
            kept,
            [
                (CoverSource::Manual, "loser".to_string()),
                (CoverSource::Provider, "winner".to_string()),
            ]
        );
        Ok(())
    }

    /// Each test provider's `artist_id`, declared unique.
    fn unique_artist_ids() -> DeclaredIds {
        DeclaredIds::unique(
            ["musicbrainz", "provider-a", "provider-b"]
                .map(|provider_id| (provider_id, "artist_id")),
        )
    }

    /// Two artists of one name that share `provider_id`'s `id_type`, deduplicated
    /// with only `strict:artist` declared unique.
    fn dedup_artists_sharing(provider_id: &str, id_type: &str) -> anyhow::Result<u32> {
        let mut db = test_db::new_test_db()?;
        for _ in 0..2 {
            let artist = lyra_db::fixtures::insert_artist(&mut db, "Radiohead")?;
            lyra_db::external_ids::upsert(
                &mut db,
                artist,
                provider_id,
                id_type,
                "shared",
                IdSource::Resolved,
            )?;
        }
        deduplicate_artists_by_external_id(&mut db, &DeclaredIds::unique([("strict", "artist")]))
    }

    type HeldId = (&'static str, &'static str, IdSource);

    /// MusicBrainz's and `provider-a`'s `artist_id`, both unique.
    fn musicbrainz_unique() -> DeclaredIds {
        DeclaredIds::unique([("musicbrainz", "artist_id"), ("provider-a", "artist_id")])
    }

    /// Only `provider-a`'s `artist_id`, unique, as when MusicBrainz's plugin is
    /// not loaded.
    fn musicbrainz_unloaded() -> DeclaredIds {
        DeclaredIds::unique([("provider-a", "artist_id")])
    }

    /// A credited winner and a loser that hold `artist_id`s, written in order,
    /// deduplicated with no provider ranked. Returns the winner and the number
    /// merged.
    fn dedup_artists_holding(
        declared: &DeclaredIds,
        winner_ids: &[HeldId],
        loser_ids: &[HeldId],
    ) -> anyhow::Result<(DbAny, DbId, u32)> {
        let mut db = test_db::new_test_db()?;
        let winner = lyra_db::fixtures::insert_artist(&mut db, "Winner")?;
        let release = lyra_db::fixtures::insert_release(&mut db, "Release")?;
        test_db::connect_artist(&mut db, release, winner)?;
        let loser = lyra_db::fixtures::insert_artist(&mut db, "Loser")?;
        for (artist, ids) in [(winner, winner_ids), (loser, loser_ids)] {
            for &(provider_id, value, source) in ids {
                lyra_db::external_ids::upsert(
                    &mut db,
                    artist,
                    provider_id,
                    "artist_id",
                    value,
                    source,
                )?;
            }
        }
        let merged = deduplicate_artists_by_external_id(&mut db, declared)?;
        Ok((db, winner, merged))
    }

    /// The artist's MusicBrainz ID: its effective value, source and resolved value.
    fn musicbrainz_id(
        db: &DbAny,
        artist: DbId,
    ) -> anyhow::Result<(String, IdSource, Option<String>)> {
        let id = lyra_db::external_ids::get(db, artist, "musicbrainz", "artist_id")?
            .expect("musicbrainz id");
        let resolved = id.resolved_value().map(str::to_string);
        Ok((id.id_value, id.source, resolved))
    }

    #[test]
    fn a_losers_manual_id_stays_manual_on_the_winner() -> anyhow::Result<()> {
        let (db, winner, merged) = dedup_artists_holding(
            &musicbrainz_unique(),
            &[("musicbrainz", "mbid-1", IdSource::Resolved)],
            &[
                ("musicbrainz", "mbid-1", IdSource::Resolved),
                ("musicbrainz", "mbid-1", IdSource::Manual),
            ],
        )?;
        assert_eq!(merged, 1);
        assert_eq!(
            musicbrainz_id(&db, winner)?,
            ("mbid-1".into(), IdSource::Manual, Some("mbid-1".into()))
        );
        Ok(())
    }

    #[test]
    fn a_manual_id_keeps_artists_apart_from_a_differing_one() -> anyhow::Result<()> {
        let mixed = (
            [
                ("musicbrainz", "mbid-1", IdSource::Resolved),
                ("provider-a", "shared", IdSource::Resolved),
            ],
            [
                ("musicbrainz", "mbid-2", IdSource::Manual),
                ("provider-a", "shared", IdSource::Resolved),
            ],
        );
        let manual = (
            [
                ("musicbrainz", "mbid-1", IdSource::Manual),
                ("provider-a", "shared", IdSource::Resolved),
            ],
            mixed.1,
        );
        for (declared, (winner_ids, loser_ids)) in [
            (musicbrainz_unique(), mixed),
            (musicbrainz_unique(), manual),
            (musicbrainz_unloaded(), manual),
        ] {
            let (_, _, merged) = dedup_artists_holding(&declared, &winner_ids, &loser_ids)?;
            assert_eq!(merged, 0, "{winner_ids:?} / {loser_ids:?}");
        }
        Ok(())
    }

    #[test]
    fn a_losers_manual_id_of_a_non_unique_type_overrides_the_winners() -> anyhow::Result<()> {
        let declared = musicbrainz_unloaded().and_non_unique([("musicbrainz", "artist_id")]);
        for (winner_source, expected) in [
            (
                IdSource::Resolved,
                ("mbid-2".into(), IdSource::Manual, Some("mbid-1".into())),
            ),
            (IdSource::Manual, ("mbid-1".into(), IdSource::Manual, None)),
        ] {
            let (db, winner, merged) = dedup_artists_holding(
                &declared,
                &[
                    ("musicbrainz", "mbid-1", winner_source),
                    ("provider-a", "shared", IdSource::Resolved),
                ],
                &[
                    ("musicbrainz", "mbid-2", IdSource::Manual),
                    ("provider-a", "shared", IdSource::Resolved),
                ],
            )?;
            assert_eq!(merged, 1);
            assert_eq!(
                musicbrainz_id(&db, winner)?,
                expected,
                "only a resolved value gives way"
            );
        }
        Ok(())
    }

    #[test]
    fn a_losers_resolved_id_beneath_its_manual_one_carries_over() -> anyhow::Result<()> {
        let (db, winner, merged) = dedup_artists_holding(
            &musicbrainz_unique(),
            &[("provider-a", "shared", IdSource::Resolved)],
            &[
                ("musicbrainz", "mbid-1", IdSource::Resolved),
                ("musicbrainz", "mbid-2", IdSource::Manual),
                ("provider-a", "shared", IdSource::Resolved),
            ],
        )?;
        assert_eq!(merged, 1);
        assert_eq!(
            musicbrainz_id(&db, winner)?,
            ("mbid-2".into(), IdSource::Manual, Some("mbid-1".into()))
        );
        Ok(())
    }

    #[test]
    fn artists_sharing_a_non_unique_artist_id_stay_apart() -> anyhow::Result<()> {
        assert_eq!(dedup_artists_sharing("loose", "artist_id")?, 0);
        Ok(())
    }

    #[test]
    fn artists_sharing_a_unique_id_of_any_name_merge() -> anyhow::Result<()> {
        assert_eq!(dedup_artists_sharing("strict", "artist")?, 1);
        Ok(())
    }

    #[test]
    fn dedup_drops_loser_metadata_layers_instead_of_migrating() -> anyhow::Result<()> {
        let mut db = test_db::new_test_db()?;

        lyra_db::providers::upsert(
            &mut db,
            &lyra_db::ProviderConfig {
                db_id: None,
                provider_id: "musicbrainz".to_string(),
                position: 0,
                enabled: true,
            },
        )?;

        let artist_a = lyra_db::fixtures::insert_artist(&mut db, "Radiohead")?;
        let artist_b = lyra_db::fixtures::insert_artist(&mut db, "Radiohead")?;

        // Give artist A more edges so it wins the merge
        let release = lyra_db::fixtures::insert_release(&mut db, "OK Computer")?;
        lyra_db::fixtures::connect(&mut db, release, artist_a)?;
        let track = lyra_db::fixtures::insert_track(&mut db, "Paranoid Android")?;
        lyra_db::fixtures::connect(&mut db, track, artist_a)?;

        lyra_db::external_ids::upsert(
            &mut db,
            artist_a,
            "musicbrainz",
            "artist_id",
            "mbid-123",
            IdSource::Resolved,
        )?;
        lyra_db::external_ids::upsert(
            &mut db,
            artist_b,
            "musicbrainz",
            "artist_id",
            "mbid-123",
            IdSource::Resolved,
        )?;

        // Artist B (the loser) has a layer from a different provider
        lyra_db::metadata::layers::upsert(
            &mut db,
            artist_b,
            &lyra_db::MetadataLayer::new(
                "local-scanner",
                crate::services::metadata::layers::json::fields_from_json(serde_json::json!({
                    "artist_name": "Radiohead",
                    "description": "English rock band",
                })),
                1000,
            ),
        )?;

        let merged_count = deduplicate_artists_by_external_id(&mut db, &unique_artist_ids())?;
        assert_eq!(merged_count, 1);

        let a_exists = lyra_db::artists::get_by_id(&db, artist_a)?.is_some();
        assert!(a_exists, "artist A should win (more edges)");

        let winner_layers = lyra_db::metadata::layers::get_for_entity(&db, artist_a)?;
        let has_local_scanner_layer = winner_layers.iter().any(|l| l.source_id == "local-scanner");

        assert!(
            has_local_scanner_layer,
            "winner should have inherited loser's local-scanner layer"
        );

        Ok(())
    }

    #[test]
    fn artist_merge_retargets_layer_edges_before_removing_the_loser() -> anyhow::Result<()> {
        use lyra_db::metadata::layers::{
            self,
            edges,
        };

        let mut db = test_db::new_test_db()?;
        let credited = lyra_db::fixtures::insert_artist(&mut db, "Radiohead")?;
        let layer_only = lyra_db::fixtures::insert_artist(&mut db, "Radiohead")?;
        let member = lyra_db::fixtures::insert_artist(&mut db, "Thom Yorke")?;
        let track = lyra_db::fixtures::insert_track(&mut db, "Creep")?;
        lyra_db::fixtures::connect(&mut db, track, credited)?;
        for artist in [credited, layer_only] {
            lyra_db::external_ids::upsert(
                &mut db,
                artist,
                "musicbrainz",
                "artist_id",
                "mbid-123",
                IdSource::Resolved,
            )?;
        }
        for node_id in [track, member] {
            layers::upsert(
                &mut db,
                node_id,
                &lyra_db::MetadataLayer::new("musicbrainz", Default::default(), 1),
            )?;
        }
        let track_layer = layers::layer_id(&db, track, "musicbrainz")?.expect("track layer");
        let member_layer = layers::layer_id(&db, member, "musicbrainz")?.expect("member layer");
        db.transaction_mut(|t| {
            edges::replace_credits(
                t,
                track_layer,
                lyra_db::CreditType::Vocalist,
                &[edges::LayerCreditInput {
                    artist_id: layer_only,
                    detail: Some("lead".to_string()),
                }],
            )?;
            edges::replace_relations(
                t,
                member_layer,
                lyra_db::ArtistRelationType::MemberOf,
                &[edges::LayerRelationInput {
                    artist_id: layer_only,
                    attributes: Some("vocals".to_string()),
                }],
            )
        })?;

        assert_eq!(
            deduplicate_artists_by_external_id(&mut db, &unique_artist_ids())?,
            1
        );

        let (winner, loser) = (credited, layer_only);
        assert!(lyra_db::artists::get_by_id(&db, loser)?.is_none());
        let credits = edges::credits(&db, track_layer)?;
        assert_eq!(credits.len(), 1);
        assert_eq!(credits[0].target_id, winner);
        assert_eq!(credits[0].edge.credit_type, lyra_db::CreditType::Vocalist);
        assert_eq!(credits[0].edge.detail.as_deref(), Some("lead"));
        let relations = edges::relations(&db, member_layer)?;
        assert_eq!(relations.len(), 1);
        assert_eq!(relations[0].target_id, winner);
        assert_eq!(relations[0].edge.attributes.as_deref(), Some("vocals"));
        Ok(())
    }

    #[test]
    fn merging_duplicate_characters_keeps_both_voice_actors() -> anyhow::Result<()> {
        use crate::services::metadata::layers::LOCAL_SOURCE_ID;
        use lyra_db::metadata::layers::{
            self,
            edges::LayerRelationInput,
        };

        let mut db = test_db::new_test_db()?;
        let first = lyra_db::fixtures::insert_artist(&mut db, "Character")?;
        let second = lyra_db::fixtures::insert_artist(&mut db, "Character")?;
        let first_actor = lyra_db::fixtures::insert_artist(&mut db, "First Actor")?;
        let second_actor = lyra_db::fixtures::insert_artist(&mut db, "Second Actor")?;
        for (character, actor, updated_at_ms) in
            [(first, first_actor, 1), (second, second_actor, 2)]
        {
            lyra_db::external_ids::upsert(
                &mut db,
                character,
                "musicbrainz",
                "artist_id",
                "character-mbid",
                IdSource::Resolved,
            )?;
            layers::upsert(
                &mut db,
                character,
                &lyra_db::MetadataLayer::new(
                    LOCAL_SOURCE_ID,
                    crate::services::metadata::layers::json::fields_from_json(
                        serde_json::json!({"artist_name": "Character"}),
                    ),
                    updated_at_ms,
                )
                .with_relations(std::collections::BTreeMap::from([(
                    lyra_db::ArtistRelationType::VoiceActor,
                    vec![LayerRelationInput {
                        artist_id: actor,
                        attributes: None,
                    }],
                )])),
            )?;
            crate::services::metadata::merging::apply_merged_metadata_to_entity(
                &mut db, character,
            )?;
        }

        assert_eq!(
            deduplicate_artists_by_external_id(&mut db, &unique_artist_ids())?,
            1
        );
        let winner = if lyra_db::artists::get_by_id(&db, first)?.is_some() {
            first
        } else {
            second
        };
        let voice_actors = |db: &DbAny| -> anyhow::Result<Vec<DbId>> {
            let mut actors: Vec<DbId> = lyra_db::artists::relations::get_relations_to(
                db,
                winner,
                Some(lyra_db::ArtistRelationType::VoiceActor),
            )?
            .into_iter()
            .map(|(_, actor)| actor)
            .collect();
            actors.sort();
            Ok(actors)
        };
        let mut expected = vec![first_actor, second_actor];
        expected.sort();
        assert_eq!(voice_actors(&db)?, expected);
        let local = layers::get_for_entity(&db, winner)?
            .into_iter()
            .find(|layer| layer.source_id == LOCAL_SOURCE_ID)
            .expect("winner keeps its layer");
        assert_eq!(
            local.updated_at_ms, 2,
            "the combined layer is as recent as its newer half"
        );

        crate::services::metadata::merging::apply_merged_metadata_to_entity(&mut db, winner)?;
        assert_eq!(voice_actors(&db)?, expected, "a later apply keeps both");
        Ok(())
    }

    #[test]
    fn a_locked_winner_keeps_the_losers_voice_actors_until_unlocked() -> anyhow::Result<()> {
        use crate::services::metadata::layers::LOCAL_SOURCE_ID;
        use lyra_db::metadata::layers::{
            self,
            edges::LayerRelationInput,
        };

        let mut db = test_db::new_test_db()?;
        let winner = lyra_db::fixtures::insert_artist(&mut db, "Character")?;
        let loser = lyra_db::fixtures::insert_artist(&mut db, "Character")?;
        let actor = lyra_db::fixtures::insert_artist(&mut db, "Actor")?;
        layers::upsert(
            &mut db,
            loser,
            &lyra_db::MetadataLayer::new(
                LOCAL_SOURCE_ID,
                crate::services::metadata::layers::json::fields_from_json(
                    serde_json::json!({"artist_name": "Character"}),
                ),
                1,
            )
            .with_relations(std::collections::BTreeMap::from([(
                lyra_db::ArtistRelationType::VoiceActor,
                vec![LayerRelationInput {
                    artist_id: actor,
                    attributes: None,
                }],
            )])),
        )?;
        crate::services::metadata::merging::apply_merged_metadata_to_entity(&mut db, loser)?;
        let mut locked = lyra_db::artists::get_by_id(&db, winner)?.expect("winner exists");
        locked.locked = Some(true);
        lyra_db::artists::update(&mut db, &locked)?;

        db.transaction_mut(|t| {
            migrate_metadata(t, winner, loser)?;
            merge_artist_into(t, winner, loser)
        })?;
        let voice_actors = |db: &DbAny| -> anyhow::Result<Vec<DbId>> {
            Ok(lyra_db::artists::relations::get_relations_to(
                db,
                winner,
                Some(lyra_db::ArtistRelationType::VoiceActor),
            )?
            .into_iter()
            .map(|(_, actor)| actor)
            .collect())
        };
        assert_eq!(voice_actors(&db)?, vec![actor]);

        let mut unlocked = lyra_db::artists::get_by_id(&db, winner)?.expect("winner exists");
        unlocked.locked = None;
        lyra_db::artists::update(&mut db, &unlocked)?;
        crate::services::metadata::merging::apply_merged_metadata_to_entity(&mut db, winner)?;
        assert_eq!(
            voice_actors(&db)?,
            vec![actor],
            "the layers resolve the same once unlocked"
        );
        Ok(())
    }

    #[test]
    fn dedup_drops_loser_external_ids_from_other_providers() -> anyhow::Result<()> {
        let mut db = test_db::new_test_db()?;

        lyra_db::providers::upsert(
            &mut db,
            &lyra_db::ProviderConfig {
                db_id: None,
                provider_id: "provider-a".to_string(),
                position: 0,
                enabled: true,
            },
        )?;
        lyra_db::providers::upsert(
            &mut db,
            &lyra_db::ProviderConfig {
                db_id: None,
                provider_id: "provider-b".to_string(),
                position: 1,
                enabled: true,
            },
        )?;

        let artist_a = lyra_db::fixtures::insert_artist(&mut db, "Radiohead")?;
        let artist_b = lyra_db::fixtures::insert_artist(&mut db, "Radiohead")?;

        // Give artist A more edges so it wins the merge
        let release = lyra_db::fixtures::insert_release(&mut db, "OK Computer")?;
        lyra_db::fixtures::connect(&mut db, release, artist_a)?;
        let track = lyra_db::fixtures::insert_track(&mut db, "Paranoid Android")?;
        lyra_db::fixtures::connect(&mut db, track, artist_a)?;

        // Both artists share the same ID from provider A (triggers dedup)
        lyra_db::external_ids::upsert(
            &mut db,
            artist_a,
            "provider-a",
            "artist_id",
            "shared-id",
            IdSource::Resolved,
        )?;
        lyra_db::external_ids::upsert(
            &mut db,
            artist_b,
            "provider-a",
            "artist_id",
            "shared-id",
            IdSource::Resolved,
        )?;

        // Artist B (the loser) also has an ID from provider B
        lyra_db::external_ids::upsert(
            &mut db,
            artist_b,
            "provider-b",
            "artist_id",
            "provider-b-id",
            IdSource::Resolved,
        )?;

        let merged_count = deduplicate_artists_by_external_id(&mut db, &unique_artist_ids())?;
        assert_eq!(merged_count, 1);

        let a_exists = lyra_db::artists::get_by_id(&db, artist_a)?.is_some();
        assert!(a_exists, "artist A should win (more edges)");

        let winner_ext_ids = lyra_db::external_ids::get_for_entity(&db, artist_a)?;
        let has_provider_b_id = winner_ext_ids
            .iter()
            .any(|id| id.provider_id == "provider-b");

        assert!(
            has_provider_b_id,
            "winner should have inherited loser's provider-b external ID"
        );

        Ok(())
    }

    /// Three artists of one name, each credited on as many releases as
    /// `releases` gives it and holding the `artist_id`s `ids` lists by index.
    /// MusicBrainz ranks above `provider-a`, which ranks above `provider-b`.
    fn insert_linked_artists(
        db: &mut DbAny,
        releases: [usize; 3],
        ids: &[(usize, &str, &str)],
    ) -> anyhow::Result<[DbId; 3]> {
        for (position, provider_id) in ["musicbrainz", "provider-a", "provider-b"]
            .into_iter()
            .enumerate()
        {
            lyra_db::providers::upsert(
                db,
                &lyra_db::ProviderConfig {
                    db_id: None,
                    provider_id: provider_id.to_string(),
                    position: position as u32,
                    enabled: true,
                },
            )?;
        }
        let mut artists = [DbId(0); 3];
        for (artist, count) in artists.iter_mut().zip(releases) {
            *artist = lyra_db::fixtures::insert_artist(db, "Radiohead")?;
            for _ in 0..count {
                let release = lyra_db::fixtures::insert_release(db, "Release")?;
                lyra_db::fixtures::connect(db, release, *artist)?;
            }
        }
        for &(index, provider_id, id_value) in ids {
            lyra_db::external_ids::upsert(
                db,
                artists[index],
                provider_id,
                "artist_id",
                id_value,
                IdSource::Resolved,
            )?;
        }
        Ok(artists)
    }

    fn remaining_artists(db: &DbAny, artists: [DbId; 3]) -> anyhow::Result<Vec<DbId>> {
        let mut remaining = Vec::new();
        for artist in artists {
            if lyra_db::artists::get_by_id(db, artist)?.is_some() {
                remaining.push(artist);
            }
        }
        Ok(remaining)
    }

    /// Both orders in which the two linking providers' groups can run.
    const LINK_ORDERS: [(&str, &str); 2] =
        [("provider-a", "provider-b"), ("provider-b", "provider-a")];

    #[test]
    fn an_artist_sharing_ids_with_two_others_merges_all_three() -> anyhow::Result<()> {
        // The bridging artist loses whichever of its two groups merges first.
        for (first, second) in LINK_ORDERS {
            let mut db = test_db::new_test_db()?;
            let artists = insert_linked_artists(
                &mut db,
                [2, 0, 2],
                &[
                    (0, first, "left"),
                    (1, first, "left"),
                    (1, second, "right"),
                    (2, second, "right"),
                ],
            )?;

            assert_eq!(
                deduplicate_artists_by_external_id(&mut db, &unique_artist_ids())?,
                2
            );
            let remaining = remaining_artists(&db, artists)?;
            assert_eq!(remaining.len(), 1, "{first} first leaves {remaining:?}");
        }
        Ok(())
    }

    #[test]
    fn linked_artists_merge_into_the_one_ranked_first_across_all_of_them() -> anyhow::Result<()> {
        // Merging the first pair alone would lift its winner above the third
        // artist, which has the most releases of all three.
        for (first, second) in LINK_ORDERS {
            let mut db = test_db::new_test_db()?;
            let artists = insert_linked_artists(
                &mut db,
                [3, 1, 4],
                &[
                    (0, first, "left"),
                    (1, first, "left"),
                    (1, second, "right"),
                    (2, second, "right"),
                ],
            )?;

            assert_eq!(
                deduplicate_artists_by_external_id(&mut db, &unique_artist_ids())?,
                2
            );
            assert_eq!(
                remaining_artists(&db, artists)?,
                [artists[2]],
                "{first} first"
            );
        }
        Ok(())
    }

    #[test]
    fn artists_sharing_an_id_but_not_their_musicbrainz_ids_stay_apart() -> anyhow::Result<()> {
        let mut db = test_db::new_test_db()?;
        let artists = insert_linked_artists(
            &mut db,
            [1, 1, 0],
            &[
                (0, "provider-a", "shared"),
                (1, "provider-a", "shared"),
                (0, "musicbrainz", "one"),
                (1, "musicbrainz", "two"),
            ],
        )?;

        assert_eq!(
            deduplicate_artists_by_external_id(&mut db, &unique_artist_ids())?,
            0
        );
        assert_eq!(remaining_artists(&db, artists)?, artists);
        Ok(())
    }

    #[test]
    fn artists_sharing_a_musicbrainz_id_merge_past_a_lower_ranked_conflict() -> anyhow::Result<()> {
        // provider-a matched each spelling to its own artist by name.
        let mut db = test_db::new_test_db()?;
        let artists = insert_linked_artists(
            &mut db,
            [1, 0, 0],
            &[
                (0, "musicbrainz", "shared"),
                (1, "musicbrainz", "shared"),
                (0, "provider-a", "one"),
                (1, "provider-a", "two"),
            ],
        )?;

        assert_eq!(
            deduplicate_artists_by_external_id(&mut db, &unique_artist_ids())?,
            1
        );
        assert_eq!(remaining_artists(&db, artists)?, [artists[0], artists[2]]);
        let provider_a_ids = lyra_db::external_ids::get_for_entity(&db, artists[0])?
            .into_iter()
            .filter(|id| id.provider_id == "provider-a")
            .count();
        assert_eq!(provider_a_ids, 1);
        Ok(())
    }

    #[test]
    fn artists_that_conflict_only_with_a_third_still_merge() -> anyhow::Result<()> {
        // All three share a provider-b ID. The odd one conflicts with each of
        // the others through a higher-ranked provider, but they agree; it
        // takes every place, so it is first in its group in one of them.
        for odd in 0..3 {
            let (second, third) = ((odd + 1) % 3, (odd + 2) % 3);
            let mut db = test_db::new_test_db()?;
            let artists = insert_linked_artists(
                &mut db,
                [0, 0, 0],
                &[
                    (0, "provider-b", "shared"),
                    (1, "provider-b", "shared"),
                    (2, "provider-b", "shared"),
                    (odd, "musicbrainz", "one"),
                    (second, "musicbrainz", "two"),
                    (odd, "provider-a", "x"),
                    (third, "provider-a", "y"),
                ],
            )?;

            assert_eq!(
                deduplicate_artists_by_external_id(&mut db, &unique_artist_ids())?,
                1,
                "{odd}"
            );
            let remaining = remaining_artists(&db, artists)?;
            assert_eq!(remaining.len(), 2, "{odd}");
            assert!(remaining.contains(&artists[odd]), "{odd}");
        }
        Ok(())
    }

    #[test]
    fn a_chain_of_shared_ids_does_not_merge_across_a_conflict() -> anyhow::Result<()> {
        // The bridge may join either end, but never both ends together.
        for (first, second) in LINK_ORDERS {
            let mut db = test_db::new_test_db()?;
            let [left, bridge, right] = insert_linked_artists(
                &mut db,
                [1, 0, 1],
                &[
                    (0, "musicbrainz", "one"),
                    (0, first, "left"),
                    (1, first, "left"),
                    (1, second, "right"),
                    (2, second, "right"),
                    (2, "musicbrainz", "two"),
                ],
            )?;

            assert_eq!(
                deduplicate_artists_by_external_id(&mut db, &unique_artist_ids())?,
                1,
                "{first} first"
            );
            assert_eq!(
                remaining_artists(&db, [left, bridge, right])?,
                [left, right],
                "{first} first"
            );
        }
        Ok(())
    }

    #[test]
    fn artist_merge_combines_manual_layers_and_keeps_outgoing_relations() -> anyhow::Result<()> {
        use lyra_db::metadata::layers::{
            self,
            edges::LayerRelationInput,
        };

        let mut db = test_db::new_test_db()?;
        let source = lyra_db::fixtures::insert_artist(&mut db, "Source")?;
        let actor = lyra_db::fixtures::insert_artist(&mut db, "Actor")?;
        let winner = lyra_db::fixtures::insert_artist(&mut db, "Winner")?;
        let loser = lyra_db::fixtures::insert_artist(&mut db, "Loser")?;
        let target = lyra_db::fixtures::insert_artist(&mut db, "Target")?;
        let relation = |artist_id, attributes: &str| LayerRelationInput {
            artist_id,
            attributes: Some(attributes.to_string()),
        };
        layers::upsert(
            &mut db,
            winner,
            &lyra_db::MetadataLayer::new(
                MANUAL_SOURCE_ID,
                crate::services::metadata::layers::json::fields_from_json(
                    serde_json::json!({"artist_name": "Winner"}),
                ),
                1,
            )
            .with_relations(std::collections::BTreeMap::from([(
                lyra_db::ArtistRelationType::MemberOf,
                vec![relation(source, "winner's")],
            )])),
        )?;
        layers::upsert(
            &mut db,
            loser,
            &lyra_db::MetadataLayer::new(
                MANUAL_SOURCE_ID,
                crate::services::metadata::layers::json::fields_from_json(serde_json::json!({
                    "artist_name": "Loser",
                    "description": "Loser's description",
                })),
                1,
            )
            .with_relations(std::collections::BTreeMap::from([
                (
                    lyra_db::ArtistRelationType::MemberOf,
                    vec![relation(source, "loser's")],
                ),
                (
                    lyra_db::ArtistRelationType::VoiceActor,
                    vec![relation(actor, "loser's"), relation(winner, "self")],
                ),
            ])),
        )?;
        lyra_db::artists::relations::link(
            &mut db,
            winner,
            target,
            lyra_db::ArtistRelationType::VoiceActor,
            Some("existing outgoing".to_string()),
        )?;
        lyra_db::artists::relations::link(
            &mut db,
            loser,
            target,
            lyra_db::ArtistRelationType::VoiceActor,
            Some("outgoing".to_string()),
        )?;

        db.transaction_mut(|transaction| {
            migrate_metadata(transaction, winner, loser)?;
            merge_artist_into(transaction, winner, loser)
        })?;

        let artist = lyra_db::artists::get_by_id(&db, winner)?.expect("winner exists");
        assert_eq!(artist.artist_name, "Winner");
        assert_eq!(artist.description.as_deref(), Some("Loser's description"));
        let incoming = |relation_type| -> anyhow::Result<Vec<(DbId, Option<String>)>> {
            Ok(
                lyra_db::artists::relations::get_relations_to(&db, winner, Some(relation_type))?
                    .into_iter()
                    .map(|(relation, source)| (source, relation.attributes))
                    .collect(),
            )
        };
        assert_eq!(
            incoming(lyra_db::ArtistRelationType::MemberOf)?,
            vec![(source, Some("winner's".to_string()))],
            "the winner's manual relations win"
        );
        assert_eq!(
            incoming(lyra_db::ArtistRelationType::VoiceActor)?,
            vec![(actor, Some("loser's".to_string()))],
            "the loser's fill what the winner leaves out"
        );
        let outgoing = lyra_db::artists::relations::get_relations_from(&db, winner, None)?;
        assert_eq!(outgoing.len(), 1);
        assert_eq!(outgoing[0].1, target);
        assert_eq!(
            outgoing[0].0.attributes.as_deref(),
            Some("existing outgoing")
        );
        Ok(())
    }

    #[test]
    fn artist_merge_does_not_turn_relations_into_ownership_edges() -> anyhow::Result<()> {
        let mut db = test_db::new_test_db()?;
        let winner = lyra_db::fixtures::insert_artist(&mut db, "Winner")?;
        let loser = lyra_db::fixtures::insert_artist(&mut db, "Loser")?;
        lyra_db::artists::relations::link(
            &mut db,
            winner,
            loser,
            lyra_db::ArtistRelationType::MemberOf,
            None,
        )?;

        db.transaction_mut(|transaction| merge_artist_into(transaction, winner, loser))?;

        assert!(lyra_db::graph::direct_edge_ids(&db, winner, winner)?.is_empty());
        Ok(())
    }

    #[test]
    fn artist_merge_moves_credits_with_their_values() -> anyhow::Result<()> {
        let mut db = test_db::new_test_db()?;
        let track = lyra_db::fixtures::insert_track(&mut db, "Track")?;
        let winner = lyra_db::fixtures::insert_artist(&mut db, "Winner")?;
        let loser = lyra_db::fixtures::insert_artist(&mut db, "Loser")?;
        let credit =
            |artist_id, detail: Option<&str>| lyra_db::metadata::layers::edges::LayerCreditInput {
                artist_id,
                detail: detail.map(str::to_string),
            };
        lyra_db::metadata::layers::upsert(
            &mut db,
            track,
            &lyra_db::MetadataLayer::new("local", Default::default(), 1).with_credits(
                std::collections::BTreeMap::from([
                    (
                        lyra_db::CreditType::Artist,
                        vec![credit(winner, None), credit(loser, None)],
                    ),
                    (
                        lyra_db::CreditType::Producer,
                        vec![credit(loser, Some("co-producer"))],
                    ),
                ]),
            ),
        )?;
        crate::services::metadata::merging::apply_merged_metadata_to_entity(&mut db, track)?;
        assert_eq!(lyra_db::credits::links_for_owner(&db, track)?.len(), 3);

        db.transaction_mut(|transaction| merge_artist_into(transaction, winner, loser))?;

        let mut credits: Vec<lyra_db::Credit> = lyra_db::credits::links_for_owner(&db, track)?
            .into_iter()
            .inspect(|link| assert_eq!(link.artist_id, winner))
            .map(|link| link.credit)
            .collect();
        credits.sort_by_key(|credit| credit.artist_order);
        assert_eq!(
            credits,
            [
                lyra_db::Credit {
                    credit_type: lyra_db::CreditType::Artist,
                    detail: None,
                    artist_order: 0,
                },
                lyra_db::Credit {
                    credit_type: lyra_db::CreditType::Producer,
                    detail: Some("co-producer".to_string()),
                    artist_order: 1,
                },
            ]
        );
        let before = lyra_db::credits::links_for_owner(&db, track)?;
        crate::services::metadata::merging::apply_merged_metadata_to_entity(&mut db, track)?;
        assert_eq!(
            lyra_db::credits::links_for_owner(&db, track)?,
            before,
            "a later apply keeps the merged credits"
        );
        Ok(())
    }

    #[test]
    fn an_artist_merge_credits_the_winner() -> anyhow::Result<()> {
        let mut db = test_db::new_test_db()?;
        let layer_track = lyra_db::fixtures::insert_track(&mut db, "Layer Track")?;
        let manual_track = lyra_db::fixtures::insert_track(&mut db, "Manual Track")?;
        let winner = lyra_db::fixtures::insert_artist(&mut db, "Winner")?;
        let loser = lyra_db::fixtures::insert_artist(&mut db, "Loser")?;
        lyra_db::metadata::layers::upsert(
            &mut db,
            layer_track,
            &lyra_db::MetadataLayer::new("local", Default::default(), 1).with_credits(
                std::collections::BTreeMap::from([(
                    lyra_db::CreditType::Artist,
                    vec![lyra_db::metadata::layers::edges::LayerCreditInput {
                        artist_id: loser,
                        detail: None,
                    }],
                )]),
            ),
        )?;
        crate::services::metadata::merging::apply_merged_metadata_to_entity(&mut db, layer_track)?;
        let credited = |db: &DbAny, artist_id| -> anyhow::Result<bool> {
            Ok(lyra_db::artists::get_by_id(db, artist_id)?.is_some_and(|artist| artist.credited))
        };
        assert!(!credited(&db, winner)?);

        db.transaction_mut(|transaction| merge_artist_into(transaction, winner, loser))?;
        assert!(
            credited(&db, winner)?,
            "the reapplied layer credits the winner"
        );

        let other = lyra_db::fixtures::insert_artist(&mut db, "Other")?;
        let manual_loser = lyra_db::fixtures::insert_artist(&mut db, "Manual Loser")?;
        lyra_db::metadata::layers::upsert(
            &mut db,
            manual_track,
            &lyra_db::MetadataLayer::new(MANUAL_SOURCE_ID, Default::default(), 1).with_credits(
                std::collections::BTreeMap::from([(
                    lyra_db::CreditType::Artist,
                    vec![lyra_db::metadata::layers::edges::LayerCreditInput {
                        artist_id: manual_loser,
                        detail: None,
                    }],
                )]),
            ),
        )?;
        crate::services::metadata::merging::apply_merged_metadata_to_entity(&mut db, manual_track)?;
        assert!(credited(&db, manual_loser)?);
        db.transaction_mut(|transaction| merge_artist_into(transaction, other, manual_loser))?;
        assert!(credited(&db, other)?, "a manual credit moves to the winner");
        Ok(())
    }

    #[test]
    fn chained_merges_carry_scan_aliases() -> anyhow::Result<()> {
        let mut db = test_db::new_test_db()?;
        let tag_artist = |db: &mut DbAny, name: &str, credited: bool| -> anyhow::Result<DbId> {
            let artist_id = lyra_db::fixtures::insert_artist(db, name)?;
            if credited {
                let release = lyra_db::fixtures::insert_release(db, "Release")?;
                test_db::connect_artist(db, release, artist_id)?;
            }
            lyra_db::external_ids::upsert(
                db,
                artist_id,
                "musicbrainz",
                "artist_id",
                "mbid-123",
                IdSource::Resolved,
            )?;
            Ok(artist_id)
        };
        let first = tag_artist(&mut db, "Mr.Children", false)?;
        tag_artist(&mut db, "Mr Children", false)?;
        assert_eq!(
            deduplicate_artists_by_external_id(&mut db, &unique_artist_ids())?,
            1
        );
        let last = tag_artist(&mut db, "Mr.Children (JP)", true)?;
        assert_eq!(
            deduplicate_artists_by_external_id(&mut db, &unique_artist_ids())?,
            1
        );

        assert!(lyra_db::artists::get_by_id(&db, first)?.is_none());
        let winner = lyra_db::artists::get_by_id(&db, last)?.expect("winner");
        assert_eq!(winner.scan_aliases, ["mr.children", "mr children"]);
        for name in ["mr children", "mr.children"] {
            assert_eq!(
                lyra_db::artists::find_id_by_scan_name(&db, name)?,
                Some(last)
            );
        }
        Ok(())
    }

    #[test]
    fn a_manual_relation_only_to_the_winner_does_not_clear_its_type() -> anyhow::Result<()> {
        use lyra_db::metadata::layers::edges::LayerRelationInput;

        let mut db = test_db::new_test_db()?;
        let winner = lyra_db::fixtures::insert_artist(&mut db, "Winner")?;
        let loser = lyra_db::fixtures::insert_artist(&mut db, "Loser")?;
        let actor = lyra_db::fixtures::insert_artist(&mut db, "Actor")?;
        let member = lyra_db::fixtures::insert_artist(&mut db, "Member")?;
        lyra_db::providers::upsert(
            &mut db,
            &lyra_db::ProviderConfig {
                db_id: None,
                provider_id: "provider".to_string(),
                position: 0,
                enabled: true,
            },
        )?;
        let relations =
            |pairs: &[(lyra_db::ArtistRelationType, &[DbId])]| -> lyra_db::metadata::layers::LayerRelations {
                pairs
                    .iter()
                    .map(|(relation_type, artist_ids)| {
                        (
                            *relation_type,
                            artist_ids
                                .iter()
                                .map(|&artist_id| LayerRelationInput {
                                    artist_id,
                                    attributes: None,
                                })
                                .collect(),
                        )
                    })
                    .collect()
            };
        lyra_db::metadata::layers::upsert(
            &mut db,
            winner,
            &lyra_db::MetadataLayer::new("provider", Default::default(), 1).with_relations(
                relations(&[
                    (lyra_db::ArtistRelationType::VoiceActor, &[actor]),
                    (lyra_db::ArtistRelationType::MemberOf, &[member]),
                ]),
            ),
        )?;
        lyra_db::metadata::layers::upsert(
            &mut db,
            winner,
            &lyra_db::MetadataLayer::new(
                MANUAL_SOURCE_ID,
                crate::services::metadata::layers::json::fields_from_json(
                    serde_json::json!({"artist_name": "Winner"}),
                ),
                1,
            ),
        )?;
        lyra_db::metadata::layers::upsert(
            &mut db,
            loser,
            &lyra_db::MetadataLayer::new(MANUAL_SOURCE_ID, Default::default(), 1).with_relations(
                relations(&[
                    (lyra_db::ArtistRelationType::VoiceActor, &[winner]),
                    (lyra_db::ArtistRelationType::MemberOf, &[]),
                ]),
            ),
        )?;

        db.transaction_mut(|transaction| {
            migrate_metadata(transaction, winner, loser)?;
            merge_artist_into(transaction, winner, loser)
        })?;

        let incoming = |relation_type| -> anyhow::Result<Vec<DbId>> {
            Ok(
                lyra_db::artists::relations::get_relations_to(&db, winner, Some(relation_type))?
                    .into_iter()
                    .map(|(_, source)| source)
                    .collect(),
            )
        };
        assert_eq!(
            incoming(lyra_db::ArtistRelationType::VoiceActor)?,
            vec![actor],
            "a type that only named the winner falls through"
        );
        assert!(
            incoming(lyra_db::ArtistRelationType::MemberOf)?.is_empty(),
            "a type the loser cleared stays cleared"
        );
        Ok(())
    }

    #[test]
    fn a_relation_type_left_only_naming_the_winner_still_clears() -> anyhow::Result<()> {
        use lyra_db::metadata::layers::edges::LayerRelationInput;

        let mut db = test_db::new_test_db()?;
        let winner = lyra_db::fixtures::insert_artist(&mut db, "Winner")?;
        let loser = lyra_db::fixtures::insert_artist(&mut db, "Loser")?;
        let member = lyra_db::fixtures::insert_artist(&mut db, "Member")?;
        for (provider_id, position) in [("high", 0), ("low", 1)] {
            lyra_db::providers::upsert(
                &mut db,
                &lyra_db::ProviderConfig {
                    db_id: None,
                    provider_id: provider_id.to_string(),
                    position,
                    enabled: true,
                },
            )?;
        }
        for (provider_id, artist_id) in [("high", loser), ("low", member)] {
            lyra_db::metadata::layers::upsert(
                &mut db,
                winner,
                &lyra_db::MetadataLayer::new(provider_id, Default::default(), 1).with_relations(
                    lyra_db::metadata::layers::LayerRelations::from([(
                        lyra_db::ArtistRelationType::MemberOf,
                        vec![LayerRelationInput {
                            artist_id,
                            attributes: None,
                        }],
                    )]),
                ),
            )?;
        }

        db.transaction_mut(|transaction| {
            migrate_metadata(transaction, winner, loser)?;
            merge_artist_into(transaction, winner, loser)
        })?;

        let high = lyra_db::metadata::layers::find(&db, winner, "high")?.expect("high layer");
        assert_eq!(
            high.relations[&lyra_db::ArtistRelationType::MemberOf],
            Vec::new(),
            "the self relation goes, but the type stays supplied"
        );
        assert!(
            lyra_db::artists::relations::get_relations_to(
                &db,
                winner,
                Some(lyra_db::ArtistRelationType::MemberOf)
            )?
            .is_empty(),
            "the emptied type still clears the lower layer's members"
        );
        Ok(())
    }
}
