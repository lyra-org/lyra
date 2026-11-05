// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    cmp::Ordering,
    collections::{
        HashMap,
        HashSet,
    },
};

use agdb::{
    DbAny,
    DbAnyTransactionMut,
    DbId,
};

use crate::services::metadata::id_links::{
    DeclaredIds,
    LinkedIds,
    provider_ranks,
};
use crate::services::metadata::layers::MANUAL_SOURCE_ID;
use lyra_db::graph::RetargetLookup;

pub(super) fn select_release_merge_winner(
    db: &DbAny,
    release_ids: &[DbId],
) -> anyhow::Result<DbId> {
    let mut winner = release_ids
        .first()
        .copied()
        .ok_or_else(|| anyhow::anyhow!("select_release_merge_winner called with empty list"))?;

    let mut winner_release = lyra_db::releases::get_by_id(db, winner)?
        .ok_or_else(|| anyhow::anyhow!("winner release {} missing", winner.0))?;
    let mut winner_tracks = lyra_db::tracks::get(db, winner)?;
    let mut winner_min_disc = winner_tracks.iter().filter_map(|track| track.disc).min();
    let mut winner_track_count = winner_tracks.len();
    let mut winner_created_at = winner_release.created_at_ms.unwrap_or(u64::MAX);

    for candidate in release_ids.iter().copied().skip(1) {
        let Some(candidate_release) = lyra_db::releases::get_by_id(db, candidate)? else {
            continue;
        };
        let candidate_tracks = lyra_db::tracks::get(db, candidate)?;
        let candidate_min_disc = candidate_tracks.iter().filter_map(|track| track.disc).min();
        let candidate_track_count = candidate_tracks.len();
        let candidate_created_at = candidate_release.created_at_ms.unwrap_or(u64::MAX);

        let ordering = lyra_db::compare_option(&candidate_min_disc, &winner_min_disc)
            .then_with(|| winner_track_count.cmp(&candidate_track_count))
            .then_with(|| candidate_created_at.cmp(&winner_created_at))
            .then_with(|| candidate.0.cmp(&winner.0));

        if ordering == Ordering::Less {
            winner = candidate;
            winner_release = candidate_release;
            winner_tracks = candidate_tracks;
            winner_min_disc = candidate_min_disc;
            winner_track_count = winner_tracks.len();
            winner_created_at = winner_release.created_at_ms.unwrap_or(u64::MAX);
        }
    }

    Ok(winner)
}

fn merge_release_into(db: &mut DbAny, winner: DbId, loser: DbId) -> anyhow::Result<()> {
    db.transaction_mut(|transaction| merge_release_into_inside_tx(transaction, winner, loser))
}

fn merge_release_into_inside_tx(
    db: &mut DbAnyTransactionMut<'_>,
    winner: DbId,
    loser: DbId,
) -> anyhow::Result<()> {
    if winner == loser {
        return Ok(());
    }

    for library in lyra_db::libraries::get_by_release(db, loser)? {
        let Some(library_db_id) = library.db_id else {
            continue;
        };
        lyra_db::graph::ensure_owned_edge(db, library_db_id, winner)?;
    }

    // Every loser track moves to the winner, so its chains stay whole there.
    lyra_db::chains::move_release_chains(db, loser, winner)?;
    for track in lyra_db::tracks::get_direct(db, loser)? {
        let Some(track_db_id) = track.db_id.map(Into::into) else {
            continue;
        };
        lyra_db::releases::link_track(db, winner, track_db_id)?;
        lyra_db::releases::unlink_track(db, loser, track_db_id)?;
    }

    for external_id in lyra_db::external_ids::get_for_entity_inside_tx(db, loser)? {
        lyra_db::external_ids::merge_inside_tx(db, winner, &external_id)?;
    }

    let mut winner_layers_by_source = HashMap::new();
    for layer in lyra_db::metadata::layers::get_for_entity(db, winner)? {
        winner_layers_by_source.insert(layer.source_id.clone(), layer);
    }
    for layer in lyra_db::metadata::layers::get_for_entity(db, loser)? {
        if layer.source_id == MANUAL_SOURCE_ID {
            // The winner's manual edits win; the loser's fill what they leave out.
            lyra_db::metadata::layers::fill_inside_tx(db, loser, winner, MANUAL_SOURCE_ID)?;
            continue;
        }
        let should_upsert = winner_layers_by_source
            .get(&layer.source_id)
            .is_none_or(|existing| layer.updated_at_ms > existing.updated_at_ms);
        if !should_upsert {
            continue;
        }

        lyra_db::metadata::layers::copy_inside_tx(db, loser, winner, &layer.source_id)?;
        winner_layers_by_source.insert(layer.source_id.clone(), layer);
    }
    lyra_db::labels::migrate_release_labels_inside_tx(db, winner, loser)?;
    crate::services::metadata::merging::apply_merged_metadata_to_entity_inside_tx(db, winner)?;

    lyra_db::metadata::custom_fields::copy_between_entities(db, loser, winner)?;
    lyra_db::favorites::retarget(db, loser, winner, RetargetLookup::Target)?;
    lyra_db::ratings::retarget(db, loser, winner, RetargetLookup::Target)?;
    lyra_db::tags::retarget(db, loser, winner, RetargetLookup::Target)?;

    // The winner keeps its own covers and takes the loser's for the sources it lacks.
    lyra_db::covers::adopt_missing(db, winner, loser)?;
    crate::services::covers::discard_unreferenced_cover_files(
        lyra_db::metadata::cascade_remove_entities_in_txn(db, &[loser])?,
    );

    Ok(())
}

fn collect_track_unique_ids(
    db: &DbAny,
    release_db_id: DbId,
    unique_track_id_pairs: &HashSet<(String, String)>,
) -> anyhow::Result<HashSet<String>> {
    let mut ids = HashSet::new();
    if unique_track_id_pairs.is_empty() {
        return Ok(ids);
    }
    let ext_ids = lyra_db::external_ids::get_for_album_tracks(db, release_db_id)?;
    for ext_id in ext_ids {
        if !unique_track_id_pairs.contains(&(ext_id.provider_id.clone(), ext_id.id_type.clone())) {
            continue;
        }
        let val = ext_id.id_value.trim();
        if val.is_empty() {
            continue;
        }
        ids.insert(format!("{}:{}:{}", ext_id.provider_id, ext_id.id_type, val));
    }
    Ok(ids)
}

pub(super) fn deduplicate_releases_by_external_id(
    db: &mut DbAny,
    library_db_id: DbId,
    declared: &DeclaredIds,
    unique_track_id_pairs: &HashSet<(String, String)>,
    provider_scope: Option<&HashSet<String>>,
) -> anyhow::Result<u32> {
    if !declared.has_unique() {
        return Ok(0);
    }

    let releases = lyra_db::releases::get(db, library_db_id)?;
    let mut linked = LinkedIds::default();
    for release in releases {
        let Some(release_db_id) = release.db_id.map(Into::into) else {
            continue;
        };
        for external_id in lyra_db::external_ids::get_for_entity(db, release_db_id)? {
            // Every provider's IDs keep conflicting releases apart, in scope or not.
            let in_scope =
                provider_scope.is_none_or(|scope| scope.contains(&external_id.provider_id));
            linked.add_external_id(release_db_id, &external_id, declared, in_scope);
        }
    }

    let ranks = provider_ranks(db)?;
    let components = linked.components(&ranks);
    let mut track_ids_cache: HashMap<DbId, HashSet<String>> = HashMap::new();
    if !unique_track_id_pairs.is_empty() {
        for &release_db_id in components.iter().flat_map(|component| &component.ids) {
            let ids = collect_track_unique_ids(db, release_db_id, unique_track_id_pairs)?;
            track_ids_cache.insert(release_db_id, ids);
        }
    }

    let mut merged_count = 0;
    for component in components {
        let winner = select_release_merge_winner(db, &component.ids)?;
        for &loser in component.ids.iter().filter(|&&id| id != winner) {
            // The winner's tracks include those it absorbed from earlier losers.
            if let Some(w_ids) = track_ids_cache.get(&winner)
                && !w_ids.is_empty()
                && let Some(l_ids) = track_ids_cache.get(&loser)
                && !l_ids.is_empty()
                && !w_ids.is_disjoint(l_ids)
            {
                tracing::debug!(
                    library_db_id = library_db_id.0,
                    winner = winner.0,
                    loser = loser.0,
                    unique_ids = ?component.keys,
                    "skipping release merge due to overlapping track unique ids"
                );
                continue;
            }

            tracing::info!(
                library_db_id = library_db_id.0,
                winner = winner.0,
                loser = loser.0,
                unique_ids = ?component.keys,
                "merging releases by provider unique id"
            );
            merge_release_into(db, winner, loser)?;
            if let Some(l_ids) = track_ids_cache.remove(&loser) {
                track_ids_cache.entry(winner).or_default().extend(l_ids);
            }
            merged_count += 1;
        }
    }

    Ok(merged_count)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use serde_json::json;

    use super::*;
    use crate::services::{
        STATE,
        libraries::sync::sync_library,
    };
    use lyra_db::fixtures::{
        insert_artist,
        insert_release,
    };
    use lyra_db::test_db::new_test_db;
    use lyra_db::{
        Entry,
        Library,
        entries::EntryKind,
    };

    #[test]
    fn release_merge_combines_manual_layers() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let winner = insert_release(&mut db, "Winner")?;
        let loser = insert_release(&mut db, "Loser")?;
        for (release, fields) in [
            (winner, json!({"labels": [{"name": "Manual"}]})),
            (
                loser,
                json!({"labels": [{"name": "Loser Manual"}], "genres": ["Loser Genre"]}),
            ),
        ] {
            let fields =
                crate::services::metadata::layers::json::resolved_fields_from_json(&mut db, fields);
            lyra_db::metadata::layers::upsert(
                &mut db,
                release,
                &lyra_db::MetadataLayer::new(MANUAL_SOURCE_ID, fields, 1),
            )?;
        }
        let layer = lyra_db::MetadataLayer::new(
            "local",
            crate::services::metadata::layers::json::resolved_fields_from_json(
                &mut db,
                json!({"labels": [{"name": "Provider"}]}),
            ),
            1,
        );
        lyra_db::metadata::layers::upsert(&mut db, loser, &layer)?;
        for release in [winner, loser] {
            crate::services::metadata::merging::apply_merged_metadata_to_entity(&mut db, release)?;
        }

        merge_release_into(&mut db, winner, loser)?;

        let labels = lyra_db::labels::get_for_release(&db, winner)?;
        assert_eq!(labels.len(), 1);
        assert_eq!(labels[0].label.name, "Manual");
        let genres = lyra_db::genres::get_for_release(&db, winner)?;
        assert_eq!(genres.len(), 1);
        assert_eq!(genres[0].name, "Loser Genre");
        Ok(())
    }

    #[test]
    fn release_merge_carries_the_newer_layers_relationship_edges() -> anyhow::Result<()> {
        use lyra_db::metadata::layers::{
            self,
            edges,
        };

        let mut db = new_test_db()?;
        let winner = insert_release(&mut db, "Winner")?;
        let loser = insert_release(&mut db, "Loser")?;
        let stale = insert_artist(&mut db, "Stale")?;
        let artist = insert_artist(&mut db, "Artist")?;
        let label_id = lyra_db::labels::resolve(
            &mut db,
            &lyra_db::labels::ResolveLabel {
                name: "Blue Note",
                external_id: None,
            },
        )?;
        let credit = |artist_id| edges::LayerCreditInput {
            artist_id,
            detail: None,
        };
        lyra_db::providers::upsert(
            &mut db,
            &lyra_db::ProviderConfig {
                db_id: None,
                provider_id: "provider".to_string(),
                position: 0,
                enabled: true,
            },
        )?;
        layers::upsert(
            &mut db,
            winner,
            &lyra_db::MetadataLayer::new("provider", Default::default(), 1).with_credits(
                std::collections::BTreeMap::from([(
                    lyra_db::CreditType::Artist,
                    vec![credit(stale)],
                )]),
            ),
        )?;
        let layer = lyra_db::MetadataLayer::new(
            "provider",
            crate::services::metadata::layers::json::resolved_fields_from_json(
                &mut db,
                serde_json::json!({
                    "genres": ["Jazz"],
                    "labels": [{"name": "Blue Note", "catalog_number": "BN-1"}],
                }),
            ),
            2,
        )
        .with_credits(std::collections::BTreeMap::from([(
            lyra_db::CreditType::Artist,
            vec![credit(artist)],
        )]));
        layers::upsert(&mut db, loser, &layer)?;

        merge_release_into(&mut db, winner, loser)?;

        let winner_layer = layers::layer_id(&db, winner, "provider")?.expect("winner layer");
        let credited: Vec<DbId> = edges::credits(&db, winner_layer)?
            .into_iter()
            .map(|link| link.target_id)
            .collect();
        assert_eq!(credited, vec![artist]);
        let labels = edges::labels(&db, winner_layer)?;
        assert_eq!(labels.len(), 1);
        assert_eq!(labels[0].target_id, label_id);
        assert_eq!(labels[0].edge.catalog_number.as_deref(), Some("BN-1"));
        assert!(lyra_db::labels::get_by_id(&db, label_id)?.is_some());
        assert!(!edges::is_referenced(&db, stale)?);
        let effective = lyra_db::labels::get_for_release(&db, winner)?;
        assert_eq!(effective.len(), 1);
        assert_eq!(effective[0].catalog_number.as_deref(), Some("BN-1"));
        let genres = lyra_db::genres::get_for_release(&db, winner)?;
        assert_eq!(genres.len(), 1);
        assert_eq!(genres[0].name, "Jazz");
        assert_eq!(lyra_db::credits::links_for_owner(&db, winner)?.len(), 1);
        assert_eq!(
            lyra_db::credits::links_for_owner(&db, winner)?[0].artist_id,
            artist,
            "the effective credits follow the carried layer"
        );
        Ok(())
    }

    #[test]
    fn release_merge_keeps_loser_credits_in_other_roles() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let winner = insert_release(&mut db, "Winner")?;
        let loser = insert_release(&mut db, "Loser")?;
        let artist = insert_artist(&mut db, "Artist")?;
        let credits = |roles: &[lyra_db::CreditType]| -> lyra_db::metadata::layers::LayerCredits {
            roles
                .iter()
                .map(|&role| {
                    (
                        role,
                        vec![lyra_db::metadata::layers::edges::LayerCreditInput {
                            artist_id: artist,
                            detail: None,
                        }],
                    )
                })
                .collect()
        };
        for (release, roles, updated_at_ms) in [
            (winner, &[lyra_db::CreditType::Artist][..], 1),
            (
                loser,
                &[lyra_db::CreditType::Artist, lyra_db::CreditType::Producer][..],
                2,
            ),
        ] {
            lyra_db::metadata::layers::upsert(
                &mut db,
                release,
                &lyra_db::MetadataLayer::new("local", Default::default(), updated_at_ms)
                    .with_credits(credits(roles)),
            )?;
        }

        merge_release_into(&mut db, winner, loser)?;

        let mut roles: Vec<lyra_db::CreditType> = lyra_db::credits::links_for_owner(&db, winner)?
            .into_iter()
            .map(|link| link.credit.credit_type)
            .collect();
        roles.sort_by_key(ToString::to_string);
        assert_eq!(
            roles,
            [lyra_db::CreditType::Artist, lyra_db::CreditType::Producer]
        );
        Ok(())
    }

    #[test]
    fn tied_releases_merge_into_the_older_then_the_lower_id() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let mut insert = |created_at_ms: Option<u64>| -> anyhow::Result<DbId> {
            let release_id = insert_release(&mut db, "Release")?;
            let mut release = lyra_db::releases::get_by_id(&db, release_id)?.expect("release");
            release.created_at_ms = created_at_ms;
            lyra_db::releases::update(&mut db, &release)?;
            Ok(release_id)
        };
        let undated = insert(None)?;
        let newer = insert(Some(200))?;
        let older = insert(Some(100))?;
        let also_older = insert(Some(100))?;

        assert_eq!(select_release_merge_winner(&db, &[newer, older])?, older);
        assert_eq!(select_release_merge_winner(&db, &[undated, newer])?, newer);
        assert_eq!(
            select_release_merge_winner(&db, &[also_older, older])?,
            older
        );
        Ok(())
    }

    fn tagged_file(path: &Path, album: &str, track: u32) -> anyhow::Result<()> {
        use lofty::{
            config::WriteOptions,
            prelude::*,
        };

        std::fs::create_dir_all(path.parent().expect("parent"))?;
        let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/assets/metadata/integration_track.flac");
        std::fs::copy(fixture, path)?;
        let mut tagged = lofty::read_from_path(path)?;
        let tag = tagged
            .primary_tag_mut()
            .ok_or_else(|| anyhow::anyhow!("fixture has no primary tag"))?;
        tag.set_album(album.to_string());
        tag.set_track(track);
        tag.save_to_path(path, WriteOptions::default())?;
        Ok(())
    }

    async fn synced_library(name: &str, path: &Path) -> anyhow::Result<Library> {
        let library = STATE.db.write().await.transaction_mut(|t| {
            lyra_db::libraries::create_system(
                t,
                lyra_db::libraries::LibraryInsert {
                    id: nanoid::nanoid!(),
                    name: name.to_string(),
                    path: path.to_path_buf(),
                    path_key: lyra_db::libraries::path_key_for(path),
                    language: None,
                    country: None,
                },
            )
        })?;
        sync(&library).await?;
        Ok(library)
    }

    async fn sync(library: &Library) -> anyhow::Result<()> {
        sync_library(&STATE.db.get(), library).await
    }

    /// The library's stored file at `path`.
    async fn file_of(library: &Library, path: &Path) -> anyhow::Result<Entry> {
        lyra_db::entries::load_existing(&*STATE.db.read().await, library.db_id.expect("library"))?
            .into_iter()
            .find(|entry| entry.full_path == path)
            .ok_or_else(|| anyhow::anyhow!("{} has no entry", path.display()))
    }

    async fn track_of(entry: &Entry) -> anyhow::Result<DbId> {
        let db = STATE.db.read().await;
        let tracks = lyra_db::tracks::get_by_entry(&db, entry.db_id.expect("entry"))?;
        anyhow::ensure!(tracks.len() == 1, "{} tracks", tracks.len());
        Ok(tracks[0].db_id.clone().expect("track").into())
    }

    /// Merges the release of `loser`'s track into that of `winner`'s, as a provider's id does.
    async fn merge_releases_of(winner: &Entry, loser: &Entry) -> anyhow::Result<DbId> {
        let (winner, loser) = (track_of(winner).await?, track_of(loser).await?);
        let mut db = STATE.db.write().await;
        let release_of = |db: &DbAny, track| -> anyhow::Result<DbId> {
            Ok(lyra_db::releases::get_by_track(db, track)?[0]
                .db_id
                .clone()
                .expect("release")
                .into())
        };
        let (winner, loser) = (release_of(&db, winner)?, release_of(&db, loser)?);
        merge_release_into(&mut db, winner, loser)?;
        Ok(winner)
    }

    /// Two libraries with roots apart, each with a file of a release merged between them.
    async fn libraries_sharing_a_release(
        root: &Path,
    ) -> anyhow::Result<(Library, Entry, Library, Entry)> {
        let (a, b) = (root.join("a"), root.join("b"));
        tagged_file(&a.join("Album/01.flac"), "Album", 1)?;
        tagged_file(&b.join("Album/02.flac"), "Album", 2)?;
        tagged_file(&b.join("Kept/03.flac"), "Kept", 3)?;
        let library_a = synced_library("A", &a).await?;
        let library_b = synced_library("B", &b).await?;
        let file_a = file_of(&library_a, &a.join("Album/01.flac")).await?;
        let file_b = file_of(&library_b, &b.join("Album/02.flac")).await?;
        merge_releases_of(&file_a, &file_b).await?;
        Ok((library_a, file_a, library_b, file_b))
    }

    async fn is_missing(library: &Library, file: &Entry) -> anyhow::Result<bool> {
        Ok(file_of(library, &file.full_path).await?.is_missing())
    }

    async fn age_missing_entries() -> anyhow::Result<()> {
        let three_days_ago = lyra_db::users::now_ms().unsigned_abs() - 3 * 86_400_000;
        let mut db = STATE.db.write().await;
        let missing = lyra_db::entries::missing_ids(&*db)?;
        db.exec_mut(
            agdb::QueryBuilder::insert()
                .values_uniform([("missing_at_ms", three_days_ago).into()])
                .ids(&missing)
                .query(),
        )?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_merged_release_leaves_each_library_its_own_files() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let root = tempfile::tempdir()?;
        let (a, file_a, b, file_b) = libraries_sharing_a_release(root.path()).await?;

        for library in [&a, &b, &a] {
            sync(library).await?;
            assert!(!is_missing(&a, &file_a).await?, "A's file went missing");
            assert!(!is_missing(&b, &file_b).await?, "B's file went missing");
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_librarys_purge_leaves_the_present_files_of_a_shared_release() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let root = tempfile::tempdir()?;
        let (mut a, _, b, file_b) = libraries_sharing_a_release(root.path()).await?;
        let track_b = track_of(&file_b).await?;
        a.purge_missing_after_days = Some(1);

        sync(&a).await?;
        age_missing_entries().await?;
        sync(&a).await?;

        assert!(!is_missing(&b, &file_b).await?);
        assert_eq!(track_of(&file_b).await?, track_b);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn only_their_own_library_purges_files_gone_from_a_shared_release() -> anyhow::Result<()>
    {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let root = tempfile::tempdir()?;
        let (mut a, _, mut b, file_b) = libraries_sharing_a_release(root.path()).await?;
        std::fs::remove_file(&file_b.full_path)?;
        sync(&b).await?;
        assert!(is_missing(&b, &file_b).await?);
        age_missing_entries().await?;
        a.purge_missing_after_days = Some(1);

        sync(&a).await?;
        assert!(
            is_missing(&b, &file_b).await?,
            "A's sync purged B's missing file"
        );

        b.purge_missing_after_days = Some(1);
        sync(&b).await?;
        assert!(file_of(&b, &file_b.full_path).await.is_err());
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_chosen_purge_refuses_the_tracks_of_another_library() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let root = tempfile::tempdir()?;
        let (a, _, b, file_b) = libraries_sharing_a_release(root.path()).await?;
        let track_b = track_of(&file_b).await?;
        std::fs::remove_file(&file_b.full_path)?;
        sync(&b).await?;

        let scope = lyra_db::entries::PurgeScope::Tracks(vec![track_b]);
        let db = STATE.db.read().await;
        assert!(matches!(
            lyra_db::entries::missing_purge_plan(&db, &a, &scope),
            Err(lyra_db::entries::PurgePlanError::NotMissing(_))
        ));
        let plan = lyra_db::entries::missing_purge_plan(&db, &b, &scope)?;
        assert_eq!(plan.entries, vec![file_b.db_id.expect("entry")]);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_chosen_purge_refuses_a_track_another_library_also_holds() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let root = tempfile::tempdir()?;
        let (a, file_a, _, file_b) = libraries_sharing_a_release(root.path()).await?;
        let track_a = track_of(&file_a).await?;
        let (entry_a, entry_b) = (file_a.db_id.expect("entry"), file_b.db_id.expect("entry"));

        let mut db = STATE.db.write().await;
        db.transaction_mut(|t| {
            lyra_db::track_sources::upsert(
                t,
                track_a,
                entry_b,
                lyra_db::track_sources::TrackSourceUpsert {
                    source_kind: "embedded_tags".into(),
                    source_key: "held-by-b".into(),
                    is_primary: false,
                    start_ms: None,
                    end_ms: None,
                    format: Default::default(),
                },
                None,
            )
        })?;
        lyra_db::entries::set_missing(&mut db, &[entry_a, entry_b], 1)?;

        assert!(matches!(
            lyra_db::entries::missing_purge_plan(
                &db,
                &a,
                &lyra_db::entries::PurgeScope::Tracks(vec![track_a]),
            ),
            Err(lyra_db::entries::PurgePlanError::NotMissing(_))
        ));
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn nested_libraries_keep_one_release_with_a_track_of_each_file_each() -> anyhow::Result<()>
    {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let root = tempfile::tempdir()?;
        let first = root.path().join("Artist/Album/01.flac");
        let second = root.path().join("Artist/Album/02.flac");
        tagged_file(&first, "Album", 1)?;
        let outer = synced_library("Outer", root.path()).await?;
        let inner = synced_library("Inner", &root.path().join("Artist")).await?;
        let release = merge_releases_of(
            &file_of(&outer, &first).await?,
            &file_of(&inner, &first).await?,
        )
        .await?;

        tagged_file(&second, "Album", 2)?;
        let state = || async {
            let db = STATE.db.read().await;
            let mut tracks = Vec::new();
            for library in [&outer, &inner] {
                let library = library.db_id.expect("library");
                let releases = lyra_db::releases::get(&db, library)?;
                assert_eq!(releases.len(), 1, "the merge came undone");
                assert_eq!(DbId::from(releases[0].db_id.clone().expect("id")), release);
                let entries = lyra_db::entries::load_existing(&*db, library)?;
                for entry in entries.iter().filter(|entry| entry.kind == EntryKind::File) {
                    assert!(!entry.is_missing());
                    let held = lyra_db::tracks::get_by_entry(&db, entry.db_id.expect("entry"))?;
                    assert_eq!(held.len(), 1, "{} tracks", held.len());
                    let track = held[0].db_id.clone().expect("track").into();
                    assert!(lyra_db::track_sources::track_available(&*db, track)?);
                    tracks.push(track);
                }
            }
            tracks.sort_unstable();
            anyhow::Ok(tracks)
        };

        sync(&inner).await?;
        sync(&outer).await?;
        let settled = state().await?;
        assert_eq!(
            settled.len(),
            4,
            "a track for each library's copy of each file"
        );
        assert_eq!(
            lyra_db::tracks::get_direct(&*STATE.db.read().await, release)?.len(),
            4
        );

        sync(&inner).await?;
        sync(&outer).await?;
        assert_eq!(state().await?, settled, "a second sync changed nothing");
        Ok(())
    }
}
