// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::BTreeMap;
use std::path::Path;

use agdb::DbAny;

use lyra_db::{
    CreditType,
    IdSource,
    MetadataLayer,
    fixtures::{
        insert_artist,
        insert_release,
        insert_track,
    },
    genres::{
        ResolveExternalId,
        ResolveGenre,
    },
    labels::ResolveLabel,
    metadata::layers::edges::LayerCreditInput,
    roots::{
        self,
        Root,
    },
    test_db::{
        insert_library,
        insert_user,
    },
};
use nanoid::nanoid;
use serde_json::{
    Value,
    json,
};

use super::*;
use crate::services::{
    STATE,
    auth::Principal,
    libraries::sync::sync_library,
    metadata::{
        changes::{
            ChangeSetError,
            model::{
                ChangePreview,
                ChangeResult,
                MetadataChange,
            },
        },
        cleanup::deduplicate_artists_by_external_id,
        id_links::DeclaredIds,
        layers::{
            ProviderArtist,
            ensure_provider_artist,
            json::resolved_fields_from_json,
        },
        merging::apply_merged_metadata_to_entity,
    },
    testing::{
        init_default_test_state,
        run_with_recycle_at,
        runtime_test_lock,
        user_principal,
    },
};

async fn admin() -> anyhow::Result<Principal> {
    let user = {
        let mut db = STATE.db.write().await;
        lyra_db::roles::ensure_builtin_roles(&mut db)?;
        let user = insert_user(&mut db, &format!("merge-admin-{}", nanoid!()))?;
        lyra_db::roles::ensure_user_has_role(&mut db, user, lyra_db::roles::BUILTIN_ADMIN_ROLE)?;
        user
    };
    user_principal(user).await
}

fn public_id(db: &DbAny, id: DbId) -> String {
    lyra_db::lookup::find_id_by_db_id(db, id)
        .expect("lookup")
        .expect("public id")
}

fn public_ids(db: &DbAny, ids: &[DbId]) -> Vec<String> {
    ids.iter().map(|&id| public_id(db, id)).collect()
}

fn operation_error(error: ChangeSetError) -> ChangeError {
    match error {
        ChangeSetError::Failed { error, .. } => error,
        other => panic!("not an operation's failure: {other:?}"),
    }
}

fn merge_change(
    winner_id: &str,
    loser_ids: &[String],
    expected: Option<Vec<ManualFill>>,
) -> MetadataChange {
    MetadataChange::Merge {
        winner_id: winner_id.to_string(),
        loser_ids: loser_ids.to_vec(),
        expected,
    }
}

async fn preview(
    principal: &Principal,
    winner_id: &str,
    loser_ids: &[String],
) -> Result<MergePreview, ChangeError> {
    let change = merge_change(winner_id, loser_ids, None);
    let mut results = super::super::preview(principal, &[change])
        .await
        .map_err(operation_error)?;
    match results.remove(0) {
        ChangePreview::Merge(preview) => Ok(preview),
        other => panic!("not a merge's preview: {other:?}"),
    }
}

async fn apply_merge(
    principal: &Principal,
    winner_id: &str,
    loser_ids: &[String],
    expected: Vec<ManualFill>,
) -> Result<MergeOutcome, ChangeError> {
    let change = merge_change(winner_id, loser_ids, Some(expected));
    let mut results = super::super::apply(principal, &[change])
        .await
        .map_err(operation_error)?;
    match results.remove(0) {
        ChangeResult::Merge(outcome) => Ok(outcome),
        other => panic!("not a merge's result: {other:?}"),
    }
}

/// Merges as a client does: with what a preview reported.
async fn merge(
    principal: &Principal,
    winner_id: &str,
    loser_ids: &[String],
) -> Result<MergeOutcome, ChangeError> {
    let expected = preview(principal, winner_id, loser_ids).await?.manual_fills;
    apply_merge(principal, winner_id, loser_ids, expected).await
}

async fn rename_genre(principal: &Principal, id: &str, name: &str) -> Result<String, ChangeError> {
    let change = |expected| MetadataChange::RenameGenre {
        genre_id: id.to_string(),
        name: name.to_string(),
        expected,
    };
    let previewed = super::super::preview(principal, &[change(None)])
        .await
        .map_err(operation_error)?;
    let [ChangePreview::RenameGenre { current_name, .. }] = previewed.as_slice() else {
        panic!("not a rename's preview: {previewed:?}");
    };
    let applied = super::super::apply(principal, &[change(Some(current_name.clone()))])
        .await
        .map_err(operation_error)?;
    let [ChangeResult::RenameGenre { name, .. }] = applied.as_slice() else {
        panic!("not a rename's result: {applied:?}");
    };
    Ok(name.clone())
}

async fn merge_ids(
    principal: &Principal,
    winner: DbId,
    losers: &[DbId],
) -> Result<MergeOutcome, ChangeError> {
    let (winner_id, loser_ids) = {
        let db = STATE.db.read().await;
        (public_id(&db, winner), public_ids(&db, losers))
    };
    merge(principal, &winner_id, &loser_ids).await
}

fn save_layer(db: &mut DbAny, entity: DbId, source_id: &str, fields: Value) -> anyhow::Result<()> {
    let fields = resolved_fields_from_json(db, fields);
    lyra_db::metadata::layers::upsert(db, entity, &MetadataLayer::new(source_id, fields, 1))?;
    apply_merged_metadata_to_entity(db, entity)
}

fn insert_provider(db: &mut DbAny, provider_id: &str, position: u32) -> anyhow::Result<()> {
    lyra_db::providers::upsert(
        db,
        &lyra_db::ProviderConfig {
            db_id: None,
            provider_id: provider_id.to_string(),
            position,
            enabled: true,
        },
    )?;
    Ok(())
}

fn genre_names(db: &DbAny, release: DbId) -> anyhow::Result<Vec<String>> {
    let mut names: Vec<String> = lyra_db::genres::get_for_release(db, release)?
        .into_iter()
        .map(|genre| genre.name)
        .collect();
    names.sort();
    Ok(names)
}

fn genre_count(db: &DbAny) -> anyhow::Result<usize> {
    Ok(roots::member_ids(db, Root::Genres)?.len())
}

fn label_count(db: &DbAny) -> anyhow::Result<usize> {
    Ok(roots::member_ids(db, Root::Labels)?.len())
}

fn nodes_of_kind(db: &DbAny, kind: &str) -> anyhow::Result<usize> {
    Ok(db
        .exec(
            agdb::QueryBuilder::search()
                .elements()
                .where_()
                .key("db_element_id")
                .value(kind)
                .query(),
        )?
        .elements
        .len())
}

/// Genres by the given names, each on a release of its own.
fn genres_on_releases(db: &mut DbAny, names: &[&str]) -> anyhow::Result<Vec<DbId>> {
    let mut genres = Vec::new();
    for name in names {
        let release = insert_release(db, name)?;
        save_layer(db, release, "local", json!({ "genres": [name] }))?;
        genres.push(
            lyra_db::genres::find_by_name(db, name)?
                .ok_or_else(|| anyhow::anyhow!("genre {name} missing"))?,
        );
    }
    Ok(genres)
}

/// Both the scan's and a provider's way in find the winner under `name`, and create nothing.
fn assert_name_lands(db: &mut DbAny, name: &str, winner: DbId) -> anyhow::Result<()> {
    let before = genre_count(db)?;
    assert_eq!(
        lyra_db::genres::resolve_by_name(db, name)?,
        winner,
        "{name}"
    );
    assert_eq!(
        lyra_db::genres::resolve(
            db,
            &ResolveGenre {
                name,
                aliases: &[],
                external_id: None,
            },
        )?,
        winner,
        "{name}"
    );
    assert_eq!(genre_count(db)?, before, "{name} created a genre");
    Ok(())
}

#[tokio::test]
async fn japanese_pop_spellings_merge_into_one_genre() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let admin = admin().await?;
    let names = [
        "J-pop",
        "Jpop",
        "Japanese Pop",
        "ジェイポップ",
        "ジェーポップ",
    ];
    let genres = {
        let mut db = STATE.db.write().await;
        genres_on_releases(&mut db, &names)?
    };

    let outcome = merge_ids(&admin, genres[0], &genres[1..]).await?;

    assert_eq!(outcome.kind, MergeKind::Genre);
    assert_eq!(outcome.counts.losers, 4);
    assert_eq!(outcome.counts.releases, 4);
    let mut db = STATE.db.write().await;
    assert_eq!(genre_count(&db)?, 1);
    for name in names {
        assert_name_lands(&mut db, name, genres[0])?;
    }
    assert_eq!(lyra_db::genres::get_releases(&*db, genres[0])?.len(), 5);
    Ok(())
}

#[tokio::test]
async fn soundtrack_absorbs_its_variants() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let admin = admin().await?;
    let names = ["Soundtrack", "Soundtracks", "OST"];
    let genres = {
        let mut db = STATE.db.write().await;
        genres_on_releases(&mut db, &names)?
    };

    merge_ids(&admin, genres[0], &genres[1..]).await?;

    let mut db = STATE.db.write().await;
    for name in names {
        assert_name_lands(&mut db, name, genres[0])?;
    }
    Ok(())
}

#[tokio::test]
async fn slash_named_genres_merge_as_any_others() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let admin = admin().await?;
    let names = ["R&B", "R", "B/Soul"];
    let genres = {
        let mut db = STATE.db.write().await;
        genres_on_releases(&mut db, &names)?
    };

    merge_ids(&admin, genres[0], &genres[1..]).await?;

    let mut db = STATE.db.write().await;
    for name in names {
        assert_name_lands(&mut db, name, genres[0])?;
    }
    Ok(())
}

#[tokio::test]
async fn hip_hop_variants_merge_and_take_a_new_name() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let admin = admin().await?;
    let names = ["Hip-Hop/Rap", "Rap/Hip Hop", "hip hop"];
    let genres = {
        let mut db = STATE.db.write().await;
        genres_on_releases(&mut db, &names)?
    };
    merge_ids(&admin, genres[0], &genres[1..]).await?;
    let id = public_id(&*STATE.db.read().await, genres[0]);

    let name = rename_genre(&admin, &id, " Hip Hop ").await?;

    assert_eq!(name, "Hip Hop");
    let mut db = STATE.db.write().await;
    assert_eq!(
        lyra_db::genres::get_by_id(&*db, genres[0])?
            .expect("genre")
            .name,
        "Hip Hop"
    );
    for name in names {
        assert_name_lands(&mut db, name, genres[0])?;
    }
    Ok(())
}

fn resolve_with_id(db: &mut DbAny, name: &str, id_value: &str) -> anyhow::Result<DbId> {
    lyra_db::genres::resolve(
        db,
        &ResolveGenre {
            name,
            aliases: &[],
            external_id: Some(ResolveExternalId {
                provider_id: "provider",
                id_type: "genre_id",
                id_value,
            }),
        },
    )
}

#[tokio::test]
async fn provider_sourced_genres_keep_both_ids() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let admin = admin().await?;
    let (winner, loser) = {
        let mut db = STATE.db.write().await;
        (
            resolve_with_id(&mut db, "pop punk", "a")?,
            resolve_with_id(&mut db, "pop-punk", "b")?,
        )
    };

    merge_ids(&admin, winner, &[loser]).await?;

    let mut db = STATE.db.write().await;
    assert_name_lands(&mut db, "pop-punk", winner)?;
    for (name, value) in [
        ("pop-punk", "b"),
        ("pop punk", "a"),
        ("other", "b"),
        ("x", "a"),
    ] {
        assert_eq!(resolve_with_id(&mut db, name, value)?, winner, "{value}");
        assert_eq!(
            lyra_db::external_ids::get(&db, winner, "provider", "genre_id")?
                .expect("row")
                .id_value,
            "a"
        );
    }
    assert_eq!(genre_count(&db)?, 1);
    assert_eq!(nodes_of_kind(&db, "ExternalIdAlias")?, 1);
    Ok(())
}

#[tokio::test]
async fn a_genre_merges_into_its_own_parent_and_child() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let admin = admin().await?;
    let [top, middle, bottom] = {
        let mut db = STATE.db.write().await;
        let [top, middle, bottom] = ["Top", "Middle", "Bottom"]
            .map(|name| lyra_db::genres::resolve_by_name(&mut *db, name).expect("genre"));
        lyra_db::genres::link_to_parent(&mut *db, middle, top)?;
        lyra_db::genres::link_to_parent(&mut *db, bottom, middle)?;
        [top, middle, bottom]
    };
    let parents = |db: &DbAny, genre| -> anyhow::Result<Vec<String>> {
        Ok(lyra_db::genres::get_parents(db, genre)?
            .into_iter()
            .map(|genre| genre.name)
            .collect())
    };

    merge_ids(&admin, middle, &[top]).await?;
    {
        let db = STATE.db.read().await;
        assert!(parents(&db, middle)?.is_empty(), "its parent became itself");
        assert_eq!(parents(&db, bottom)?, ["Middle"]);
    }

    merge_ids(&admin, bottom, &[middle]).await?;
    let db = STATE.db.read().await;
    assert!(parents(&db, bottom)?.is_empty());
    assert!(lyra_db::genres::get_children(&*db, bottom)?.is_empty());
    Ok(())
}

#[tokio::test]
async fn a_merge_closes_no_cycle_of_three() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let admin = admin().await?;
    let [a, b, c, loser] = {
        let mut db = STATE.db.write().await;
        let genres = ["A", "B", "C", "L"]
            .map(|name| lyra_db::genres::resolve_by_name(&mut *db, name).expect("genre"));
        let [a, b, c, loser] = genres;
        // A → B → C, and C is a child of the loser: moved, it would close C → A.
        lyra_db::genres::link_to_parent(&mut *db, a, b)?;
        lyra_db::genres::link_to_parent(&mut *db, b, c)?;
        lyra_db::genres::link_to_parent(&mut *db, c, loser)?;
        genres
    };

    merge_ids(&admin, a, &[loser]).await?;

    let db = STATE.db.read().await;
    assert!(lyra_db::genres::get_parents(&*db, c)?.is_empty());
    assert_eq!(lyra_db::genres::get_parents(&*db, a)?.len(), 1);
    assert_eq!(lyra_db::genres::get_parents(&*db, b)?.len(), 1);
    Ok(())
}

#[tokio::test]
async fn every_layer_moves_and_one_holding_both_keeps_the_winner_once() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let admin = admin().await?;
    let (release, winner, loser, other) = {
        let mut db = STATE.db.write().await;
        insert_provider(&mut db, "provider", 0)?;
        let release = insert_release(&mut db, "Release")?;
        save_layer(
            &mut db,
            release,
            "local",
            json!({ "genres": ["Jpop", "Other", "J-pop"] }),
        )?;
        // This layer wins, which leaves the loser in the layer below alone.
        save_layer(&mut db, release, "provider", json!({ "genres": ["Other"] }))?;
        let winner = lyra_db::genres::find_by_name(&*db, "J-pop")?.expect("winner");
        let loser = lyra_db::genres::find_by_name(&*db, "Jpop")?.expect("loser");
        let other = lyra_db::genres::find_by_name(&*db, "Other")?.expect("other");
        (release, winner, loser, other)
    };

    merge_ids(&admin, winner, &[loser]).await?;

    let db = STATE.db.read().await;
    let layer = lyra_db::metadata::layers::layer_id(&*db, release, "local")?.expect("layer");
    let ids: Vec<DbId> = lyra_db::metadata::layers::edges::genres(&*db, layer)?
        .into_iter()
        .map(|link| link.target_id)
        .collect();
    assert_eq!(ids, [other, winner]);
    assert_eq!(genre_names(&db, release)?, ["Other"]);
    Ok(())
}

#[tokio::test]
async fn a_locked_release_keeps_its_genre_through_the_winner() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let admin = admin().await?;
    let (release, genres) = {
        let mut db = STATE.db.write().await;
        let genres = genres_on_releases(&mut db, &["J-pop", "Jpop"])?;
        let release = lyra_db::genres::get_releases(&*db, genres[1])?[0];
        lyra_db::entities::set_locked(&mut db, release, true)?;
        (release, genres)
    };

    merge_ids(&admin, genres[0], &genres[1..]).await?;

    let db = STATE.db.read().await;
    assert_eq!(genre_names(&db, release)?, ["J-pop"]);
    Ok(())
}

#[tokio::test]
async fn a_blank_winner_is_refused_and_a_blank_loser_leaves_no_alias() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let admin = admin().await?;
    let (named, blank) = {
        let mut db = STATE.db.write().await;
        (
            lyra_db::genres::resolve_by_name(&mut *db, "Soundtrack")?,
            lyra_db::genres::resolve_by_name(&mut *db, "  ")?,
        )
    };

    assert!(matches!(
        merge_ids(&admin, blank, &[named]).await,
        Err(ChangeError::Invalid(_))
    ));

    merge_ids(&admin, named, &[blank]).await?;
    let db = STATE.db.read().await;
    assert_eq!(nodes_of_kind(&db, "GenreAlias")?, 0);
    assert_eq!(genre_count(&db)?, 1);
    Ok(())
}

#[tokio::test]
async fn renames_follow_the_name_rules() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let admin = admin().await?;
    let (genre, other, id) = {
        let mut db = STATE.db.write().await;
        let genres = genres_on_releases(&mut db, &["Rock", "Pop"])?;
        lyra_db::genres::resolve(
            &mut db,
            &ResolveGenre {
                name: "Pop",
                aliases: &[("Popular", None)],
                external_id: None,
            },
        )?;
        let id = public_id(&db, genres[0]);
        (genres[0], genres[1], id)
    };

    for blank in ["", "   "] {
        assert!(matches!(
            rename_genre(&admin, &id, blank).await,
            Err(ChangeError::Invalid(_))
        ));
    }
    for taken in ["pop", "POPULAR"] {
        assert!(matches!(
            rename_genre(&admin, &id, taken).await,
            Err(ChangeError::Conflict(_))
        ));
    }
    assert_eq!(rename_genre(&admin, &id, "ROCK").await?, "ROCK");
    assert_eq!(nodes_of_kind(&*STATE.db.read().await, "GenreAlias")?, 1);

    assert_eq!(rename_genre(&admin, &id, "Rock Music").await?, "Rock Music");
    let mut db = STATE.db.write().await;
    assert_eq!(lyra_db::genres::resolve_by_name(&mut *db, "rock")?, genre);
    assert_eq!(
        lyra_db::genres::resolve_by_name(&mut *db, "popular")?,
        other
    );
    Ok(())
}

fn credited_release(db: &mut DbAny, title: &str, artist: DbId) -> anyhow::Result<DbId> {
    let release = insert_release(db, title)?;
    lyra_db::metadata::layers::upsert(
        db,
        release,
        &MetadataLayer::new("local", Default::default(), 1).with_credits(BTreeMap::from([(
            CreditType::Artist,
            vec![LayerCreditInput {
                artist_id: artist,
                detail: None,
            }],
        )])),
    )?;
    apply_merged_metadata_to_entity(db, release)?;
    Ok(release)
}

fn provider_artist(id_value: &str, name: &str) -> ProviderArtist {
    ProviderArtist {
        id_type: "artist_id".to_string(),
        id_value: id_value.to_string(),
        artist_name: Some(name.to_string()),
        sort_name: None,
        artist_type: None,
        description: None,
    }
}

#[tokio::test]
async fn artists_merge_with_their_credits_and_both_ids() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let admin = admin().await?;
    let (winner, loser, release) = {
        let mut db = STATE.db.write().await;
        insert_provider(&mut db, "provider", 0)?;
        let winner = insert_artist(&mut db, "Winner")?;
        let loser = insert_artist(&mut db, "Loser")?;
        credited_release(&mut db, "Kept", winner)?;
        let release = credited_release(&mut db, "Moved", loser)?;
        for (artist, value) in [(winner, "a"), (loser, "b")] {
            lyra_db::external_ids::upsert(
                &mut db,
                artist,
                "provider",
                "artist_id",
                value,
                IdSource::Resolved,
            )?;
        }
        (winner, loser, release)
    };

    let outcome = merge_ids(&admin, winner, &[loser]).await?;

    assert_eq!(outcome.counts.releases, 1);
    let mut db = STATE.db.write().await;
    assert!(lyra_db::artists::get_by_id(&*db, loser)?.is_none());
    let credited: Vec<DbId> = lyra_db::artists::get(&*db, release)?
        .into_iter()
        .filter_map(|artist| artist.db_id.map(DbId::from))
        .collect();
    assert_eq!(credited, [winner]);
    // A provider naming either ID finds the winner, whose own ID stays.
    for value in ["b", "a", "b"] {
        assert_eq!(
            ensure_provider_artist(&mut db, "provider", provider_artist(value, "Loser"))
                .expect("artist"),
            winner
        );
        assert_eq!(
            lyra_db::external_ids::get(&db, winner, "provider", "artist_id")?
                .expect("row")
                .id_value,
            "a"
        );
    }
    assert_eq!(roots::member_ids(&*db, Root::Artists)?.len(), 1);
    Ok(())
}

#[tokio::test]
async fn chained_artist_merges_leave_one_of_each_row() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let admin = admin().await?;
    let [first, second, third] = {
        let mut db = STATE.db.write().await;
        let artists =
            ["First", "Second", "Third"].map(|name| insert_artist(&mut db, name).expect("artist"));
        for (artist, value) in artists.iter().zip(["a", "b", "c"]) {
            lyra_db::external_ids::upsert(
                &mut db,
                *artist,
                "provider",
                "artist_id",
                value,
                IdSource::Resolved,
            )?;
        }
        artists
    };

    merge_ids(&admin, second, &[third]).await?;
    merge_ids(&admin, first, &[second]).await?;

    let db = STATE.db.read().await;
    assert_eq!(nodes_of_kind(&db, "ExternalId")?, 1);
    assert_eq!(nodes_of_kind(&db, "ExternalIdAlias")?, 2);
    for value in ["a", "b", "c"] {
        assert_eq!(
            lyra_db::external_ids::get_owner(&*db, "provider", "artist_id", value, Some("Artist"))?,
            Some(first)
        );
    }
    Ok(())
}

#[tokio::test]
async fn chained_genre_merges_leave_one_of_each_row() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let admin = admin().await?;
    let [first, second, third] = {
        let mut db = STATE.db.write().await;
        let first = resolve_with_id(&mut db, "First", "a")?;
        let second = resolve_with_id(&mut db, "Second", "b")?;
        let third = resolve_with_id(&mut db, "Third", "c")?;
        [first, second, third]
    };

    merge_ids(&admin, second, &[third]).await?;
    merge_ids(&admin, first, &[second]).await?;

    let mut db = STATE.db.write().await;
    assert_eq!(genre_count(&db)?, 1);
    assert_eq!(nodes_of_kind(&db, "GenreAlias")?, 2);
    assert_eq!(nodes_of_kind(&db, "ExternalId")?, 1);
    assert_eq!(nodes_of_kind(&db, "ExternalIdAlias")?, 2);
    for (name, value) in [("Second", "b"), ("Third", "c")] {
        assert_name_lands(&mut db, name, first)?;
        assert_eq!(resolve_with_id(&mut db, "x", value)?, first);
    }
    Ok(())
}

#[tokio::test]
async fn chained_label_merges_leave_one_of_each_row() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let admin = admin().await?;
    let [first, second, third] = {
        let mut db = STATE.db.write().await;
        let mut labels = Vec::new();
        for (name, value) in [("First", "a"), ("Second", "b"), ("Third", "c")] {
            let release = insert_release(&mut db, name)?;
            save_layer(
                &mut db,
                release,
                "local",
                json!({"labels": [{
                    "name": name,
                    "external_id": {"provider_id": "provider", "id_type": "label_id", "id_value": value},
                }]}),
            )?;
            labels.push(lyra_db::labels::find_by_name(&*db, name)?.expect("label"));
        }
        <[DbId; 3]>::try_from(labels).expect("three labels")
    };

    merge_ids(&admin, second, &[third]).await?;
    merge_ids(&admin, first, &[second]).await?;

    let db = STATE.db.read().await;
    assert_eq!(label_count(&db)?, 1);
    assert_eq!(nodes_of_kind(&db, "LabelAlias")?, 2);
    assert_eq!(nodes_of_kind(&db, "ExternalId")?, 1);
    assert_eq!(nodes_of_kind(&db, "ExternalIdAlias")?, 2);
    assert_eq!(lyra_db::labels::get_releases(&*db, first)?.len(), 3);
    Ok(())
}

#[tokio::test]
async fn chained_release_merges_leave_one_of_each_row() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let admin = admin().await?;
    let [first, second, third] = {
        let mut db = STATE.db.write().await;
        let library = insert_library(&mut db, "Music", "/music")?;
        let releases = ["First", "Second", "Third"]
            .map(|title| insert_release(&mut db, title).expect("release"));
        for (release, value) in releases.iter().zip(["a", "b", "c"]) {
            lyra_db::graph::ensure_owned_edge(&mut *db, library, *release)?;
            let track = insert_track(&mut db, value)?;
            lyra_db::releases::link_track(&mut *db, *release, track)?;
            lyra_db::external_ids::upsert(
                &mut db,
                *release,
                "provider",
                "release_id",
                value,
                IdSource::Resolved,
            )?;
        }
        releases
    };

    merge_ids(&admin, second, &[third]).await?;
    merge_ids(&admin, first, &[second]).await?;

    let db = STATE.db.read().await;
    assert_eq!(roots::member_ids(&*db, Root::Releases)?, [first]);
    assert_eq!(lyra_db::tracks::get_direct(&*db, first)?.len(), 3);
    assert_eq!(nodes_of_kind(&db, "ExternalId")?, 1);
    assert_eq!(nodes_of_kind(&db, "ExternalIdAlias")?, 2);
    Ok(())
}

#[tokio::test]
async fn a_locked_winner_takes_loser_manual_values_for_its_empty_keys() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let admin = admin().await?;
    let (winner, loser, library) = {
        let mut db = STATE.db.write().await;
        let library = insert_library(&mut db, "Music", "/music")?;
        let winner = insert_release(&mut db, "Winner")?;
        let loser = insert_release(&mut db, "Loser")?;
        for release in [winner, loser] {
            lyra_db::graph::ensure_owned_edge(&mut *db, library, release)?;
        }
        save_layer(
            &mut db,
            winner,
            MANUAL_SOURCE_ID,
            json!({"release_title": "Edited"}),
        )?;
        save_layer(
            &mut db,
            loser,
            MANUAL_SOURCE_ID,
            json!({"release_title": "Other", "description": "From the loser"}),
        )?;
        save_layer(&mut db, loser, "local", json!({"release_type": "single"}))?;
        lyra_db::entities::set_locked(&mut db, winner, true)?;
        (winner, loser, library)
    };
    let (winner_id, loser_id) = {
        let db = STATE.db.read().await;
        (public_id(&db, winner), public_id(&db, loser))
    };

    let preview = preview(&admin, &winner_id, std::slice::from_ref(&loser_id)).await?;
    assert_eq!(preview.locked_ids, std::slice::from_ref(&winner_id));
    assert_eq!(
        preview.manual_fills,
        [ManualFill {
            key: "description".to_string(),
            value: json!("From the loser"),
        }]
    );

    merge(&admin, &winner_id, &[loser_id]).await?;

    let db = STATE.db.read().await;
    let release = lyra_db::releases::get_by_id(&*db, winner)?.expect("release");
    assert_eq!(release.release_title, "Edited");
    assert_eq!(release.description.as_deref(), Some("From the loser"));
    assert!(release.release_type.is_none(), "provider data stays out");
    assert_eq!(lyra_db::releases::get(&db, library)?.len(), 1);
    Ok(())
}

#[tokio::test]
async fn a_merge_overrides_a_losers_lock() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let admin = admin().await?;
    let (winner, loser) = {
        let mut db = STATE.db.write().await;
        let winner = insert_artist(&mut db, "Winner")?;
        let loser = insert_artist(&mut db, "Loser")?;
        lyra_db::entities::set_locked(&mut db, loser, true)?;
        (winner, loser)
    };

    merge_ids(&admin, winner, &[loser]).await?;

    assert!(lyra_db::artists::get_by_id(&*STATE.db.read().await, loser)?.is_none());
    Ok(())
}

#[tokio::test]
async fn releases_merge_in_one_library_and_preview_reports_shared_numbers() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let admin = admin().await?;
    let (winner, loser, library) = {
        let mut db = STATE.db.write().await;
        let library = insert_library(&mut db, "Music", "/music")?;
        let winner = insert_release(&mut db, "Winner")?;
        let loser = insert_release(&mut db, "Loser")?;
        for (release, number) in [(winner, 1), (loser, 1), (loser, 2)] {
            lyra_db::graph::ensure_owned_edge(&mut *db, library, release)?;
            let track = insert_track(&mut db, "Track")?;
            save_layer(&mut db, track, "local", json!({"track": number, "disc": 1}))?;
            lyra_db::releases::link_track(&mut *db, release, track)?;
        }
        for (release, value) in [(winner, "a"), (loser, "b")] {
            lyra_db::external_ids::upsert(
                &mut db,
                release,
                "provider",
                "release_id",
                value,
                IdSource::Resolved,
            )?;
        }
        (winner, loser, library)
    };
    let (winner_id, loser_id) = {
        let db = STATE.db.read().await;
        (public_id(&db, winner), public_id(&db, loser))
    };

    let preview = preview(&admin, &winner_id, std::slice::from_ref(&loser_id)).await?;
    assert_eq!(
        preview.track_collisions,
        [TrackCollision { disc: 1, track: 1 }]
    );
    assert_eq!(preview.outcome.counts.tracks, 2);
    assert_eq!(preview.kept_external_ids.len(), 1);
    assert_eq!(preview.kept_external_ids[0].id_value, "b");
    assert!(
        lyra_db::releases::get_by_id(&*STATE.db.read().await, loser)?.is_some(),
        "a preview changes nothing"
    );

    let outcome = merge(&admin, &winner_id, &[loser_id]).await?;

    assert_eq!(outcome.counts, preview.outcome.counts);
    let mut db = STATE.db.write().await;
    assert_eq!(lyra_db::tracks::get_direct(&*db, winner)?.len(), 3);
    assert_eq!(lyra_db::releases::get(&db, library)?.len(), 1);
    // A provider writing the loser's ID leaves the winner's alone.
    lyra_db::external_ids::upsert(
        &mut db,
        winner,
        "provider",
        "release_id",
        "b",
        IdSource::Resolved,
    )?;
    assert_eq!(
        lyra_db::external_ids::get(&db, winner, "provider", "release_id")?
            .expect("row")
            .id_value,
        "a"
    );
    assert_eq!(
        lyra_db::external_ids::get_owner(&*db, "provider", "release_id", "b", Some("Release"))?,
        Some(winner)
    );
    Ok(())
}

#[tokio::test]
async fn releases_in_different_libraries_or_with_the_same_track_stay_apart() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let admin = admin().await?;
    let (winner, elsewhere, both, same_track) = {
        let mut db = STATE.db.write().await;
        let a = insert_library(&mut db, "A", "/a")?;
        let b = insert_library(&mut db, "B", "/b")?;
        let release = |db: &mut DbAny, libraries: &[DbId]| -> anyhow::Result<DbId> {
            let release = insert_release(db, "Release")?;
            for &library in libraries {
                lyra_db::graph::ensure_owned_edge(db, library, release)?;
            }
            Ok(release)
        };
        let winner = release(&mut db, &[a])?;
        let elsewhere = release(&mut db, &[b])?;
        let both = release(&mut db, &[a, b])?;
        let same_track = release(&mut db, &[a])?;
        for owner in [winner, same_track] {
            let track = insert_track(&mut db, "Track")?;
            lyra_db::releases::link_track(&mut *db, owner, track)?;
            lyra_db::external_ids::upsert(
                &mut db,
                track,
                "provider",
                "recording_id",
                "shared",
                IdSource::Resolved,
            )?;
        }
        (winner, elsewhere, both, same_track)
    };

    for loser in [elsewhere, both] {
        assert!(matches!(
            merge_ids(&admin, winner, &[loser]).await,
            Err(ChangeError::Conflict(_))
        ));
    }

    let db = STATE.db.read().await;
    let pairs = HashSet::from([("provider".to_string(), "recording_id".to_string())]);
    assert!(matches!(
        check_releases(&db, winner, &[same_track], &pairs),
        Err(ChangeError::Conflict(_))
    ));
    check_releases(&db, winner, &[same_track], &HashSet::new())?;
    Ok(())
}

#[tokio::test]
async fn labels_merge_with_catalog_numbers_ids_and_names() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let admin = admin().await?;
    let label = |name: &str, catalog: &str, id: &str| {
        json!({
            "name": name,
            "catalog_number": catalog,
            "external_id": {"provider_id": "provider", "id_type": "label_id", "id_value": id},
        })
    };
    let (winner, loser, both, moved) = {
        let mut db = STATE.db.write().await;
        let both = insert_release(&mut db, "Both")?;
        save_layer(
            &mut db,
            both,
            "local",
            json!({"labels": [label("Blue Note", "W-1", "a"), label("Blue Note Records", "L-1", "b")]}),
        )?;
        let moved = insert_release(&mut db, "Moved")?;
        save_layer(
            &mut db,
            moved,
            "local",
            json!({"labels": [label("Blue Note Records", "L-2", "b")]}),
        )?;
        (
            lyra_db::labels::find_by_name(&*db, "Blue Note")?.expect("winner"),
            lyra_db::labels::find_by_name(&*db, "Blue Note Records")?.expect("loser"),
            both,
            moved,
        )
    };

    let outcome = merge_ids(&admin, winner, &[loser]).await?;

    assert_eq!(outcome.kind, MergeKind::Label);
    assert_eq!(outcome.counts.releases, 2);
    let mut db = STATE.db.write().await;
    assert_eq!(label_count(&db)?, 1);
    let catalogs = |db: &DbAny, release| -> anyhow::Result<Vec<(String, Option<String>)>> {
        Ok(lyra_db::labels::get_for_release(db, release)?
            .into_iter()
            .map(|entry| (entry.label.name, entry.catalog_number))
            .collect())
    };
    assert_eq!(
        catalogs(&db, both)?,
        [("Blue Note".to_string(), Some("W-1".to_string()))]
    );
    assert_eq!(
        catalogs(&db, moved)?,
        [("Blue Note".to_string(), Some("L-2".to_string()))]
    );

    let resolve = |db: &mut DbAny, name, id_value: Option<&'static str>| {
        lyra_db::labels::resolve(
            db,
            &ResolveLabel {
                name,
                external_id: id_value.map(|id_value| lyra_db::labels::ResolveExternalId {
                    provider_id: "provider",
                    id_type: "label_id",
                    id_value,
                }),
            },
        )
    };
    assert_eq!(resolve(&mut db, "Blue Note Records", None)?, winner);
    for value in ["b", "a", "b"] {
        assert_eq!(resolve(&mut db, "Anything", Some(value))?, winner);
        assert_eq!(
            lyra_db::external_ids::get(&db, winner, "provider", "label_id")?
                .expect("row")
                .id_value,
            "a"
        );
    }
    assert_eq!(label_count(&db)?, 1);
    Ok(())
}

#[tokio::test]
async fn a_locked_release_keeps_its_label_through_the_winner() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let admin = admin().await?;
    let (winner, loser, locked) = {
        let mut db = STATE.db.write().await;
        let kept = insert_release(&mut db, "Kept")?;
        save_layer(
            &mut db,
            kept,
            "local",
            json!({"labels": [{"name": "Winner"}]}),
        )?;
        let locked = insert_release(&mut db, "Locked")?;
        save_layer(
            &mut db,
            locked,
            "local",
            json!({"labels": [{"name": "Loser", "catalog_number": "L-1"}]}),
        )?;
        lyra_db::entities::set_locked(&mut db, locked, true)?;
        (
            lyra_db::labels::find_by_name(&*db, "Winner")?.expect("winner"),
            lyra_db::labels::find_by_name(&*db, "Loser")?.expect("loser"),
            locked,
        )
    };

    merge_ids(&admin, winner, &[loser]).await?;

    let db = STATE.db.read().await;
    let labels = lyra_db::labels::get_for_release(&*db, locked)?;
    assert_eq!(labels.len(), 1);
    assert_eq!(labels[0].label.name, "Winner");
    assert_eq!(labels[0].catalog_number.as_deref(), Some("L-1"));
    assert!(lyra_db::labels::get_by_id(&*db, loser)?.is_none());
    Ok(())
}

#[tokio::test]
async fn a_failing_loser_rolls_back_every_loser() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let (winner, first, second) = {
        let mut db = STATE.db.write().await;
        let genres = genres_on_releases(&mut db, &["W", "L1", "L2"])?;
        (genres[0], genres[1], genres[2])
    };

    let mut db = STATE.db.write().await;
    let failed: anyhow::Result<()> = db.transaction_mut(|t| {
        merge_into(t, MergeKind::Genre, winner, first)?;
        merge_into(t, MergeKind::Genre, winner, second)?;
        anyhow::bail!("stopped")
    });

    assert!(failed.is_err());
    assert_eq!(genre_count(&db)?, 3);
    assert_eq!(nodes_of_kind(&db, "GenreAlias")?, 0);
    Ok(())
}

#[tokio::test]
async fn the_request_is_checked_before_anything_else() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let admin = admin().await?;
    let (genre, artist, track) = {
        let mut db = STATE.db.write().await;
        let genre = lyra_db::genres::resolve_by_name(&mut *db, "Genre")?;
        let artist = insert_artist(&mut db, "Artist")?;
        let track = insert_track(&mut db, "Track")?;
        (
            public_id(&db, genre),
            public_id(&db, artist),
            public_id(&db, track),
        )
    };
    let invalid = |result: Result<MergeOutcome, ChangeError>| {
        assert!(matches!(result, Err(ChangeError::Invalid(_))), "{result:?}");
    };

    invalid(merge(&admin, &genre, &[]).await);
    invalid(merge(&admin, &genre, std::slice::from_ref(&genre)).await);
    invalid(merge(&admin, &genre, std::slice::from_ref(&artist)).await);
    invalid(merge(&admin, &track, std::slice::from_ref(&genre)).await);
    assert!(matches!(
        merge(&admin, &genre, &["missing".to_string()]).await,
        Err(ChangeError::NotFound(_))
    ));
    Ok(())
}

#[tokio::test]
async fn repeated_losers_merge_once() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let admin = admin().await?;
    let genres = {
        let mut db = STATE.db.write().await;
        genres_on_releases(&mut db, &["W", "L"])?
    };

    let outcome = merge_ids(&admin, genres[0], &[genres[1], genres[1]]).await?;

    assert_eq!(outcome.counts.losers, 1);
    Ok(())
}

#[tokio::test]
async fn a_library_the_entities_reach_after_the_run_check_is_a_conflict() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let admin = admin().await?;
    let (winner, loser, other_release) = {
        let mut db = STATE.db.write().await;
        let library = insert_library(&mut db, "A", "/a")?;
        let other = insert_library(&mut db, "B", "/b")?;
        let genres = genres_on_releases(&mut db, &["W", "L"])?;
        let release = lyra_db::genres::get_releases(&*db, genres[1])?[0];
        lyra_db::graph::ensure_owned_edge(&mut *db, library, release)?;
        let other_release = insert_release(&mut db, "Other")?;
        lyra_db::graph::ensure_owned_edge(&mut *db, other, other_release)?;
        (
            public_id(&db, genres[0]),
            public_id(&db, genres[1]),
            other_release,
        )
    };
    let losers = [loser.clone()];

    let merge = apply_merge(&admin, &winner, &losers, Vec::new());
    let (reached, result) = run_with_recycle_at(Some(0), merge, |db| {
        let genre = lyra_db::lookup::find_node_id_by_id(&*db, &loser)
            .expect("lookup")
            .expect("loser");
        lyra_db::genres::link_to_release(db, genre, other_release).expect("link");
    })
    .await;

    assert!(reached);
    assert!(
        matches!(result, Err(ChangeError::Conflict(_))),
        "{result:?}"
    );
    let db = STATE.db.read().await;
    assert_eq!(genre_count(&db)?, 2, "nothing merged");
    Ok(())
}

fn artist_ids_declared() -> DeclaredIds {
    DeclaredIds::unique([("provider", "artist_id")])
}

#[tokio::test]
async fn a_new_artist_holding_a_kept_id_links_to_its_holder() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let admin = admin().await?;
    let (winner, loser) = {
        let mut db = STATE.db.write().await;
        insert_provider(&mut db, "provider", 0)?;
        let winner = insert_artist(&mut db, "Winner")?;
        let loser = insert_artist(&mut db, "Loser")?;
        for (artist, value) in [(winner, "a"), (loser, "b")] {
            lyra_db::external_ids::upsert(
                &mut db,
                artist,
                "provider",
                "artist_id",
                value,
                IdSource::Resolved,
            )?;
        }
        (winner, loser)
    };
    merge_ids(&admin, winner, &[loser]).await?;

    let mut db = STATE.db.write().await;
    let newcomer = insert_artist(&mut db, "Newcomer")?;
    lyra_db::external_ids::upsert(
        &mut db,
        newcomer,
        "provider",
        "artist_id",
        "b",
        IdSource::Resolved,
    )?;

    assert_eq!(
        deduplicate_artists_by_external_id(&mut db, &artist_ids_declared())?,
        1
    );

    assert_eq!(roots::member_ids(&*db, Root::Artists)?.len(), 1);
    Ok(())
}

#[tokio::test]
async fn a_manual_value_still_keeps_a_new_artist_from_a_kept_id() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let admin = admin().await?;
    let (winner, loser) = {
        let mut db = STATE.db.write().await;
        insert_provider(&mut db, "provider", 0)?;
        let winner = insert_artist(&mut db, "Winner")?;
        let loser = insert_artist(&mut db, "Loser")?;
        lyra_db::external_ids::upsert(
            &mut db,
            winner,
            "provider",
            "artist_id",
            "a",
            IdSource::Manual,
        )?;
        lyra_db::external_ids::upsert(
            &mut db,
            loser,
            "provider",
            "artist_id",
            "b",
            IdSource::Resolved,
        )?;
        (winner, loser)
    };
    merge_ids(&admin, winner, &[loser]).await?;

    let mut db = STATE.db.write().await;
    let newcomer = insert_artist(&mut db, "Newcomer")?;
    lyra_db::external_ids::upsert(
        &mut db,
        newcomer,
        "provider",
        "artist_id",
        "c",
        IdSource::Manual,
    )?;
    lyra_db::external_ids::upsert(
        &mut db,
        newcomer,
        "provider",
        "artist_id",
        "b",
        IdSource::Resolved,
    )?;

    assert_eq!(
        deduplicate_artists_by_external_id(&mut db, &artist_ids_declared())?,
        0
    );
    Ok(())
}

struct Tags<'a> {
    album: &'a str,
    artist: &'a str,
    genre: &'a str,
    label: &'a str,
    track: u32,
}

fn write_tagged(path: &Path, tags: &Tags<'_>) -> anyhow::Result<()> {
    use lofty::{
        config::WriteOptions,
        prelude::*,
        tag::ItemKey,
    };

    std::fs::create_dir_all(path.parent().expect("parent"))?;
    let fixture =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/assets/metadata/integration_track.flac");
    std::fs::copy(fixture, path)?;
    let mut tagged = lofty::read_from_path(path)?;
    let tag = tagged
        .primary_tag_mut()
        .ok_or_else(|| anyhow::anyhow!("fixture has no primary tag"))?;
    tag.set_album(tags.album.to_string());
    tag.set_artist(tags.artist.to_string());
    tag.insert_text(ItemKey::AlbumArtist, tags.artist.to_string());
    tag.set_genre(tags.genre.to_string());
    tag.insert_text(ItemKey::Label, tags.label.to_string());
    tag.set_track(tags.track);
    tag.save_to_path(path, WriteOptions::default())?;
    Ok(())
}

async fn scanned_library(root: &Path) -> anyhow::Result<lyra_db::libraries::Library> {
    let library = STATE.db.write().await.transaction_mut(|t| {
        lyra_db::libraries::create_system(
            t,
            lyra_db::libraries::LibraryInsert {
                id: nanoid!(),
                name: "Music".to_string(),
                path: root.to_path_buf(),
                path_key: lyra_db::libraries::path_key_for(root),
                language: None,
                country: None,
                follow_links: false,
            },
        )
    })?;
    sync_library(&STATE.db.get(), &library).await?;
    Ok(library)
}

const WINNER_TAGS: Tags<'static> = Tags {
    album: "Winner Album",
    artist: "Winner Artist",
    genre: "J-pop",
    label: "Blue Note",
    track: 1,
};
const LOSER_TAGS: Tags<'static> = Tags {
    album: "Loser Album",
    artist: "Loser Artist",
    genre: "Jpop",
    label: "Blue Note Records",
    track: 2,
};

/// A scanned library of an album tagged [`WINNER_TAGS`] and one tagged [`LOSER_TAGS`].
async fn library_of_duplicates(root: &Path) -> anyhow::Result<lyra_db::libraries::Library> {
    write_tagged(&root.join("Winner/01.flac"), &WINNER_TAGS)?;
    write_tagged(&root.join("Loser/02.flac"), &LOSER_TAGS)?;
    scanned_library(root).await
}

async fn rescan_with_loser_names(
    root: &Path,
    library: &lyra_db::libraries::Library,
) -> anyhow::Result<()> {
    write_tagged(
        &root.join("Third/03.flac"),
        &Tags {
            album: "Third Album",
            track: 3,
            ..LOSER_TAGS
        },
    )?;
    sync_library(&STATE.db.get(), library).await
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rescan_under_a_losers_genre_name_lands_on_the_winner() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let admin = admin().await?;
    let root = tempfile::tempdir()?;
    let library = library_of_duplicates(root.path()).await?;
    let (winner, loser) = {
        let db = STATE.db.read().await;
        (
            lyra_db::genres::find_by_name(&*db, "J-pop")?.expect("winner"),
            lyra_db::genres::find_by_name(&*db, "Jpop")?.expect("loser"),
        )
    };

    merge_ids(&admin, winner, &[loser]).await?;
    rescan_with_loser_names(root.path(), &library).await?;

    let db = STATE.db.read().await;
    assert_eq!(genre_count(&db)?, 1);
    assert_eq!(lyra_db::genres::get_releases(&*db, winner)?.len(), 3);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rescan_under_a_losers_label_name_lands_on_the_winner() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let admin = admin().await?;
    let root = tempfile::tempdir()?;
    let library = library_of_duplicates(root.path()).await?;
    let (winner, loser) = {
        let db = STATE.db.read().await;
        (
            lyra_db::labels::find_by_name(&*db, "Blue Note")?.expect("winner"),
            lyra_db::labels::find_by_name(&*db, "Blue Note Records")?.expect("loser"),
        )
    };

    merge_ids(&admin, winner, &[loser]).await?;
    rescan_with_loser_names(root.path(), &library).await?;

    let db = STATE.db.read().await;
    assert_eq!(label_count(&db)?, 1);
    assert_eq!(lyra_db::labels::get_releases(&*db, winner)?.len(), 3);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rescan_under_a_losers_artist_name_lands_on_the_winner() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let admin = admin().await?;
    let root = tempfile::tempdir()?;
    let library = library_of_duplicates(root.path()).await?;
    let artist = |db: &DbAny, name: &str| -> anyhow::Result<DbId> {
        lyra_db::artists::with_scan_name(db, name)?
            .into_iter()
            .next()
            .and_then(|artist| artist.db_id.map(DbId::from))
            .ok_or_else(|| anyhow::anyhow!("no artist {name}"))
    };
    let (winner, loser) = {
        let db = STATE.db.read().await;
        (artist(&db, "Winner Artist")?, artist(&db, "Loser Artist")?)
    };

    merge_ids(&admin, winner, &[loser]).await?;
    rescan_with_loser_names(root.path(), &library).await?;

    let db = STATE.db.read().await;
    assert_eq!(roots::member_ids(&*db, Root::Artists)?.len(), 1);
    assert_eq!(lyra_db::releases::get_by_artist(&*db, winner)?.len(), 3);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rescan_of_a_retagged_file_keeps_a_merged_release_whole() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let admin = admin().await?;
    let root = tempfile::tempdir()?;
    let library = library_of_duplicates(root.path()).await?;
    let library_db_id = library.db_id.expect("library");
    let (winner, loser) = {
        let db = STATE.db.read().await;
        let release = |title: &str| -> anyhow::Result<DbId> {
            lyra_db::releases::get(&db, library_db_id)?
                .into_iter()
                .find(|release| release.release_title == title)
                .and_then(|release| release.db_id.map(DbId::from))
                .ok_or_else(|| anyhow::anyhow!("no release {title}"))
        };
        (release("Winner Album")?, release("Loser Album")?)
    };

    merge_ids(&admin, winner, &[loser]).await?;
    sync_library(&STATE.db.get(), &library).await?;
    write_tagged(
        &root.path().join("Loser/02.flac"),
        &Tags {
            track: 4,
            ..LOSER_TAGS
        },
    )?;
    sync_library(&STATE.db.get(), &library).await?;

    let db = STATE.db.read().await;
    let releases = lyra_db::releases::get(&db, library_db_id)?;
    assert_eq!(releases.len(), 1, "the merge came undone");
    assert_eq!(DbId::from(releases[0].db_id.clone().expect("id")), winner);
    assert_eq!(lyra_db::tracks::get_direct(&*db, winner)?.len(), 2);
    Ok(())
}
