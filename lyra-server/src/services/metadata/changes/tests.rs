// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::DbAny;
use lyra_db::{
    IdSource,
    MetadataField,
    MetadataLayer,
    fixtures::{
        insert_release,
        insert_track,
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

use super::{
    model::{
        ExternalIdState,
        ManualFill,
    },
    *,
};
use crate::services::{
    EntityType,
    metadata::{
        editing::model::{
            MetadataChangeRequest,
            MetadataEditOperation,
        },
        layers::{
            MANUAL_SOURCE_ID,
            json::resolved_fields_from_json,
        },
        merging::apply_merged_metadata_to_entity,
    },
    plugin_scope::PluginId,
    providers::ProviderIdSpec,
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
        let user = insert_user(&mut db, &format!("changes-admin-{}", nanoid!()))?;
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

fn title(entity_id: &str, value: &str, expected: Option<Value>) -> MetadataChange {
    MetadataChange::EditMetadata {
        entity_id: entity_id.to_string(),
        edits: vec![MetadataChangeRequest {
            field: MetadataField::ReleaseTitle,
            edit: MetadataEditOperation::Set {
                value: json!(value),
            },
        }],
        expected: expected.map(|expected| serde_json::from_value(expected).expect("diff")),
    }
}

fn title_diff(before: &str, source: &str, after: &str) -> Value {
    json!([{
        "field": "release_title",
        "before": {"value": before, "source": source},
        "after": {"value": after, "source": "manual"},
    }])
}

fn rename(genre_id: &str, name: &str, expected: &str) -> MetadataChange {
    MetadataChange::RenameGenre {
        genre_id: genre_id.to_string(),
        name: name.to_string(),
        expected: Some(expected.to_string()),
    }
}

fn merge(winner_id: &str, loser_ids: &[&str], expected: Vec<ManualFill>) -> MetadataChange {
    MetadataChange::Merge {
        winner_id: winner_id.to_string(),
        loser_ids: loser_ids.iter().map(ToString::to_string).collect(),
        expected: Some(expected),
    }
}

fn set_id(entity_id: &str, id_value: &str, expected: Option<ExternalIdState>) -> MetadataChange {
    MetadataChange::SetExternalId {
        entity_id: entity_id.to_string(),
        provider_id: "provider".to_string(),
        id_type: "recording_id".to_string(),
        id_value: id_value.to_string(),
        expected: Some(expected),
    }
}

fn inherit_id(entity_id: &str, expected: ExternalIdState) -> MetadataChange {
    MetadataChange::InheritExternalId {
        entity_id: entity_id.to_string(),
        provider_id: "provider".to_string(),
        id_type: "recording_id".to_string(),
        expected: Some(expected),
    }
}

fn state(id_value: &str, source: IdSource) -> ExternalIdState {
    ExternalIdState {
        id_value: id_value.to_string(),
        source,
        resolved_value: None,
    }
}

fn failure(result: Result<Vec<ChangeResult>, ChangeSetError>) -> (usize, ChangeError) {
    match result {
        Err(ChangeSetError::Failed { index, error }) => (index, error),
        other => panic!("not an operation's failure: {other:?}"),
    }
}

fn assert_stale(result: Result<Vec<ChangeResult>, ChangeSetError>, current: Value) {
    match failure(result) {
        (
            0,
            ChangeError::Stale {
                current: reported, ..
            },
        ) => assert_eq!(reported, current),
        other => panic!("not stale: {other:?}"),
    }
}

async fn release_title(release: DbId) -> anyhow::Result<String> {
    let db = STATE.db.read().await;
    Ok(lyra_db::releases::get_by_id(&*db, release)?
        .ok_or_else(|| anyhow::anyhow!("release missing"))?
        .release_title)
}

fn save_manual(db: &mut DbAny, entity: DbId, fields: Value) -> anyhow::Result<()> {
    let fields = resolved_fields_from_json(db, fields);
    lyra_db::metadata::layers::upsert(
        db,
        entity,
        &MetadataLayer::new(MANUAL_SOURCE_ID, fields, 1),
    )?;
    apply_merged_metadata_to_entity(db, entity)
}

/// Two genres on a release each, of one library, with public IDs.
async fn genres(names: [&str; 2]) -> anyhow::Result<[String; 2]> {
    let mut db = STATE.db.write().await;
    let library = insert_library(&mut db, "Music", "/changes-genres")?;
    let mut ids = Vec::new();
    for name in names {
        let genre = lyra_db::genres::resolve_by_name(&mut *db, name)?;
        let release = insert_release(&mut db, "Release")?;
        lyra_db::graph::ensure_owned_edge(&mut *db, library, release)?;
        lyra_db::genres::link_to_release(&mut *db, genre, release)?;
        ids.push(public_id(&db, genre));
    }
    Ok([ids[0].clone(), ids[1].clone()])
}

#[tokio::test]
async fn a_later_failure_rolls_back_the_whole_set() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let admin = admin().await?;
    let (release, track, id, track_id) = {
        let mut db = STATE.db.write().await;
        let release = insert_release(&mut db, "Before")?;
        let track = insert_track(&mut db, "Track")?;
        (
            release,
            track,
            public_id(&db, release),
            public_id(&db, track),
        )
    };
    let [first, second] = genres(["First", "Second"]).await?;

    let (index, error) = failure(
        apply(
            &admin,
            &[
                title(
                    &id,
                    "After",
                    Some(title_diff("Before", "resolved", "After")),
                ),
                set_id(&track_id, "value", None),
                merge(&first, &[&second], Vec::new()),
                rename("missing", "Name", "Old"),
            ],
        )
        .await,
    );

    assert_eq!(index, 3);
    assert!(matches!(error, ChangeError::NotFound(_)), "{error:?}");
    assert_eq!(release_title(release).await?, "Before");
    let db = STATE.db.read().await;
    assert!(lyra_db::external_ids::get(&db, track, "provider", "recording_id")?.is_none());
    assert!(lyra_db::lookup::find_node_id_by_id(&*db, &second)?.is_some());
    assert!(lyra_db::genres::find_by_name(&*db, "Second")?.is_some());
    Ok(())
}

#[tokio::test]
async fn a_set_applies_in_order_and_later_operations_see_earlier_ones() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let admin = admin().await?;
    let (release, id) = {
        let mut db = STATE.db.write().await;
        let release = insert_release(&mut db, "Before")?;
        (release, public_id(&db, release))
    };

    let results = apply(
        &admin,
        &[
            title(&id, "One", Some(title_diff("Before", "resolved", "One"))),
            title(&id, "Two", Some(title_diff("One", "manual", "Two"))),
        ],
    )
    .await?;

    assert_eq!(results.len(), 2);
    assert_eq!(release_title(release).await?, "Two");
    Ok(())
}

#[tokio::test]
async fn two_renames_to_one_unused_name_are_rejected() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let admin = admin().await?;
    let [first, second] = genres(["First", "Second"]).await?;

    let (index, error) = failure(
        apply(
            &admin,
            &[
                rename(&first, "Same", "First"),
                rename(&second, "same", "Second"),
            ],
        )
        .await,
    );

    assert_eq!(index, 1);
    assert!(matches!(error, ChangeError::Conflict(_)), "{error:?}");
    let db = STATE.db.read().await;
    assert!(lyra_db::genres::find_by_name(&*db, "First")?.is_some());
    assert!(lyra_db::genres::find_by_name(&*db, "Same")?.is_none());
    Ok(())
}

#[tokio::test]
async fn releases_given_one_unique_id_in_the_set_do_not_merge() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let admin = admin().await?;
    let (first_release, second_release, first_track, second_track) = {
        let mut db = STATE.db.write().await;
        let library = insert_library(&mut db, "Music", "/changes-unique")?;
        let mut ids = Vec::new();
        for _ in 0..2 {
            let release = insert_release(&mut db, "Release")?;
            lyra_db::graph::ensure_owned_edge(&mut *db, library, release)?;
            let track = insert_track(&mut db, "Track")?;
            lyra_db::releases::link_track(&mut *db, release, track)?;
            ids.push(public_id(&db, release));
            ids.push(public_id(&db, track));
        }
        (
            ids[0].clone(),
            ids[2].clone(),
            ids[1].clone(),
            ids[3].clone(),
        )
    };
    {
        let mut registry = provider_registry().write_owned().await;
        registry.register(
            PluginId::new("demo").expect("plugin id"),
            "provider".to_string(),
            None,
        )?;
        registry
            .set_id_registration(
                "provider",
                ProviderIdSpec {
                    id_type: "recording_id".to_string(),
                    entity: EntityType::Track,
                    unique: true,
                    scheme: "example:recording".to_string(),
                    label: None,
                },
                || None,
            )
            .map_err(anyhow::Error::msg)?;
    }
    let set = [
        set_id(&first_track, "shared", None),
        set_id(&second_track, "shared", None),
        merge(&first_release, &[&second_release], Vec::new()),
    ];

    let (index, error) = failure(apply(&admin, &set).await);

    assert_eq!(index, 2);
    assert!(matches!(error, ChangeError::Conflict(_)), "{error:?}");
    let without_the_ids = apply(
        &admin,
        &[merge(&first_release, &[&second_release], Vec::new())],
    )
    .await;
    assert!(without_the_ids.is_ok(), "the IDs rolled back");
    Ok(())
}

#[tokio::test]
async fn an_entity_merged_away_earlier_in_the_set_reads_as_missing() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let admin = admin().await?;
    let [winner, loser] = genres(["Winner", "Loser"]).await?;
    let third = {
        let mut db = STATE.db.write().await;
        let third = lyra_db::genres::resolve_by_name(&mut *db, "Third")?;
        public_id(&db, third)
    };
    let merged = merge(&winner, &[&loser], Vec::new());

    for naming_it in [
        rename(&loser, "Name", "Loser"),
        merge(&third, &[&loser], Vec::new()),
        merge(&loser, &[&third], Vec::new()),
        set_id(&loser, "value", None),
        title(
            &loser,
            "Title",
            Some(title_diff("Loser", "resolved", "Title")),
        ),
        MetadataChange::RemoveExternalIdAlias {
            entity_id: loser.clone(),
            provider_id: "provider".to_string(),
            id_type: "genre_id".to_string(),
            id_value: "value".to_string(),
        },
    ] {
        let (index, error) = failure(apply(&admin, &[merged.clone(), naming_it]).await);
        assert_eq!(index, 1);
        assert!(matches!(error, ChangeError::NotFound(_)), "{error:?}");
    }

    let db = STATE.db.read().await;
    assert!(
        lyra_db::lookup::find_node_id_by_id(&*db, &loser)?.is_some(),
        "every failed set rolled the merge back"
    );
    Ok(())
}

#[tokio::test]
async fn a_changed_field_stops_an_edit() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let admin = admin().await?;
    let (release, id) = {
        let mut db = STATE.db.write().await;
        let release = insert_release(&mut db, "Moved")?;
        (release, public_id(&db, release))
    };

    assert_stale(
        apply(
            &admin,
            &[title(
                &id,
                "After",
                Some(title_diff("Before", "resolved", "After")),
            )],
        )
        .await,
        title_diff("Moved", "resolved", "After"),
    );
    assert_eq!(release_title(release).await?, "Moved");
    Ok(())
}

#[tokio::test]
async fn a_changed_record_stops_an_external_id_edit() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let admin = admin().await?;
    let (track, id) = {
        let mut db = STATE.db.write().await;
        let track = insert_track(&mut db, "Track")?;
        lyra_db::external_ids::upsert(
            &mut db,
            track,
            "provider",
            "recording_id",
            "found",
            IdSource::Resolved,
        )?;
        (track, public_id(&db, track))
    };

    let found = json!({"id_value": "found", "source": "resolved"});
    assert_stale(
        apply(&admin, &[set_id(&id, "typed", None)]).await,
        found.clone(),
    );
    assert_stale(
        apply(
            &admin,
            &[set_id(
                &id,
                "typed",
                Some(state("older", IdSource::Resolved)),
            )],
        )
        .await,
        found,
    );

    apply(
        &admin,
        &[set_id(
            &id,
            " typed ",
            Some(state("found", IdSource::Resolved)),
        )],
    )
    .await?;
    let typed = ExternalIdState {
        id_value: "typed".to_string(),
        source: IdSource::Manual,
        resolved_value: Some("found".to_string()),
    };
    assert_stale(
        apply(&admin, &[inherit_id(&id, state("typed", IdSource::Manual))]).await,
        serde_json::to_value(&typed)?,
    );

    let (index, error) = failure(apply(&admin, &[set_id(&id, "typed", Some(typed.clone()))]).await);
    assert_eq!(index, 0);
    assert!(matches!(error, ChangeError::Invalid(_)), "no effect fails");

    apply(&admin, &[inherit_id(&id, typed)]).await?;
    let db = STATE.db.read().await;
    assert_eq!(
        lyra_db::external_ids::get(&db, track, "provider", "recording_id")?
            .ok_or_else(|| anyhow::anyhow!("id missing"))?
            .id_value,
        "found"
    );
    Ok(())
}

#[tokio::test]
async fn a_changed_loser_value_stops_a_merge() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let admin = admin().await?;
    let (winner, loser, loser_db_id) = {
        let mut db = STATE.db.write().await;
        let library = insert_library(&mut db, "Music", "/changes-fills")?;
        let winner = insert_release(&mut db, "Winner")?;
        let loser = insert_release(&mut db, "Loser")?;
        for release in [winner, loser] {
            lyra_db::graph::ensure_owned_edge(&mut *db, library, release)?;
        }
        save_manual(&mut db, loser, json!({"description": "Reviewed"}))?;
        (public_id(&db, winner), public_id(&db, loser), loser)
    };
    let previewed = preview(
        &admin,
        &[MetadataChange::Merge {
            winner_id: winner.clone(),
            loser_ids: vec![loser.clone()],
            expected: None,
        }],
    )
    .await?;
    let [ChangePreview::Merge(report)] = previewed.as_slice() else {
        panic!("not a merge's preview: {previewed:?}");
    };
    let reviewed = report.manual_fills.clone();
    assert_eq!(
        reviewed,
        [ManualFill {
            key: "description".to_string(),
            value: json!("Reviewed"),
        }]
    );

    save_manual(
        &mut *STATE.db.write().await,
        loser_db_id,
        json!({"description": "Changed"}),
    )?;

    assert_stale(
        apply(&admin, &[merge(&winner, &[&loser], reviewed)]).await,
        json!([{"key": "description", "value": "Changed"}]),
    );
    assert!(
        lyra_db::lookup::find_node_id_by_id(&*STATE.db.read().await, &loser)?.is_some(),
        "the loser survived"
    );
    Ok(())
}

#[tokio::test]
async fn a_changed_name_stops_a_rename() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let admin = admin().await?;
    let [genre, _] = genres(["Current", "Other"]).await?;

    assert_stale(
        apply(&admin, &[rename(&genre, "New", "Former")]).await,
        json!("Current"),
    );
    Ok(())
}

#[tokio::test]
async fn an_operation_without_effect_or_expected_is_invalid() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let admin = admin().await?;
    let [genre, loser] = genres(["Same", "Loser"]).await?;
    let id = {
        let mut db = STATE.db.write().await;
        let release = insert_release(&mut db, "Before")?;
        save_manual(&mut db, release, json!({"release_title": "Same"}))?;
        public_id(&db, release)
    };

    let no_effect = [rename(&genre, " Same ", "Same"), title(&id, "Same", None)];
    for change in no_effect {
        let (index, error) = match preview(&admin, std::slice::from_ref(&change)).await {
            Err(ChangeSetError::Failed { index, error }) => (index, error),
            other => panic!("not an operation's failure: {other:?}"),
        };
        assert_eq!(index, 0);
        assert!(matches!(error, ChangeError::Invalid(_)), "{error:?}");
    }

    for change in [
        title(&id, "After", None),
        MetadataChange::SetExternalId {
            entity_id: id.clone(),
            provider_id: "provider".to_string(),
            id_type: "release_id".to_string(),
            id_value: "x".to_string(),
            expected: None,
        },
        MetadataChange::InheritExternalId {
            entity_id: id.clone(),
            provider_id: "provider".to_string(),
            id_type: "release_id".to_string(),
            expected: None,
        },
        MetadataChange::Merge {
            winner_id: genre.clone(),
            loser_ids: vec![loser.clone()],
            expected: None,
        },
        MetadataChange::RenameGenre {
            genre_id: genre.clone(),
            name: "New".to_string(),
            expected: None,
        },
    ] {
        let (_, error) = failure(apply(&admin, &[change]).await);
        assert!(matches!(error, ChangeError::Invalid(_)), "{error:?}");
    }
    Ok(())
}

#[test]
fn set_external_id_tells_a_null_expected_from_a_missing_one() {
    let change = |extra: Value| -> MetadataChange {
        let mut change = json!({
            "action": "set_external_id",
            "entity_id": "e",
            "provider_id": "p",
            "id_type": "t",
            "id_value": "v",
        });
        if let (Some(change), Value::Object(extra)) = (change.as_object_mut(), extra) {
            change.extend(extra);
        }
        serde_json::from_value(change).expect("change")
    };

    let MetadataChange::SetExternalId { expected, .. } = change(json!({})) else {
        panic!("not a set");
    };
    assert_eq!(expected, None);
    let MetadataChange::SetExternalId { expected, .. } = change(json!({"expected": null})) else {
        panic!("not a set");
    };
    assert_eq!(expected, Some(None));
}

#[test]
fn operations_deny_unknown_fields() {
    let change = json!({"action": "rename_genre", "genre_id": "g", "name": "n", "stray": 1});
    assert!(serde_json::from_value::<MetadataChange>(change).is_err());
}

#[tokio::test]
async fn a_preview_writes_nothing_and_leaves_the_change_count() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let admin = admin().await?;
    let [winner, loser] = genres(["Winner", "Loser"]).await?;
    let (id, track_id) = {
        let mut db = STATE.db.write().await;
        let release = insert_release(&mut db, "Before")?;
        let track = insert_track(&mut db, "Track")?;
        (public_id(&db, release), public_id(&db, track))
    };
    let set = [
        title(&id, "After", None),
        set_id(&track_id, "value", None),
        rename(&winner, "New", "Winner"),
        merge(&winner, &[&loser], Vec::new()),
    ];
    let shared = STATE.db.get();
    let stamp = shared.stamp();

    let results = preview(&admin, &set).await?;

    assert_eq!(results.len(), 4);
    assert_eq!(shared.stamp(), stamp, "a preview is no write");
    let db = STATE.db.read().await;
    assert!(lyra_db::lookup::find_node_id_by_id(&*db, &loser)?.is_some());
    assert!(lyra_db::genres::find_by_name(&*db, "Winner")?.is_some());
    Ok(())
}

#[tokio::test]
async fn a_preview_does_not_model_earlier_operations() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let admin = admin().await?;
    let [winner, loser] = genres(["Winner", "Loser"]).await?;

    let set = [
        merge(&winner, &[&loser], Vec::new()),
        rename(&loser, "New", "Loser"),
    ];

    assert_eq!(preview(&admin, &set).await?.len(), 2);
    let (index, error) = failure(apply(&admin, &set).await);
    assert_eq!(index, 1);
    assert!(matches!(error, ChangeError::NotFound(_)), "{error:?}");
    Ok(())
}

#[tokio::test]
async fn a_library_reached_after_the_run_check_stops_the_operation_there() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let admin = admin().await?;
    let [first, second] = genres(["First", "Second"]).await?;
    let (genre, other_release) = {
        let mut db = STATE.db.write().await;
        let other = insert_library(&mut db, "Other", "/changes-other")?;
        let release = insert_release(&mut db, "Other")?;
        lyra_db::graph::ensure_owned_edge(&mut *db, other, release)?;
        let genre = lyra_db::lookup::find_node_id_by_id(&*db, &second)?
            .ok_or_else(|| anyhow::anyhow!("genre missing"))?;
        (genre, release)
    };
    let set = [
        rename(&first, "Renamed", "First"),
        rename(&second, "Moved", "Second"),
    ];

    let (reached, result) = run_with_recycle_at(Some(0), apply(&admin, &set), |db| {
        lyra_db::genres::link_to_release(db, genre, other_release).expect("link");
    })
    .await;

    assert!(reached);
    let (index, error) = failure(result);
    assert_eq!(index, 1);
    assert!(matches!(error, ChangeError::Conflict(_)), "{error:?}");
    assert!(
        lyra_db::genres::find_by_name(&*STATE.db.read().await, "First")?.is_some(),
        "the first rename rolled back"
    );
    Ok(())
}

fn insert_running_run(db: &mut DbAny, library_id: &str) -> anyhow::Result<()> {
    lyra_db::sync_runs::create(
        db,
        &lyra_db::sync_runs::SyncRunRecord {
            db_id: None,
            id: nanoid!(),
            library_id: library_id.to_string(),
            kind: "library_sync".to_string(),
            status: "running".to_string(),
            started_at_ms: 1,
            updated_at_ms: 1,
            finished_at_ms: None,
            error: None,
            cancellation_requested: false,
            sequence: 1,
            progress_mode: "indeterminate".to_string(),
            total_state: "discovering".to_string(),
            completed_units: 0,
            failed_units: 0,
            skipped_units: 0,
            total_units: 0,
            current_stage: None,
            current_subject: None,
            active_units: 0,
            failure_count: 0,
        },
    )?;
    Ok(())
}

/// A release of a library with a run on it, and a genre of another library with none.
async fn running_scene() -> anyhow::Result<(String, String)> {
    let [genre, _] = genres(["Idle", "Other"]).await?;
    let mut db = STATE.db.write().await;
    let library = insert_library(&mut db, "Busy", "/changes-busy")?;
    let library_id = public_id(&db, library);
    let release = insert_release(&mut db, "Before")?;
    lyra_db::graph::ensure_owned_edge(&mut *db, library, release)?;
    insert_running_run(&mut db, &library_id)?;
    Ok((public_id(&db, release), genre))
}

#[tokio::test]
async fn edits_alone_apply_during_a_run_and_a_set_with_a_rename_does_not() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let admin = admin().await?;
    let (release, genre) = running_scene().await?;
    let edit = title(
        &release,
        "After",
        Some(title_diff("Before", "resolved", "After")),
    );

    let (index, error) =
        failure(apply(&admin, &[rename(&genre, "New", "Idle"), edit.clone()]).await);
    assert_eq!(index, 1);
    assert!(matches!(error, ChangeError::Conflict(_)), "{error:?}");
    assert!(
        lyra_db::genres::find_by_name(&*STATE.db.read().await, "Idle")?.is_some(),
        "the rename rolled back"
    );

    assert_eq!(apply(&admin, &[edit]).await?.len(), 1);
    Ok(())
}

#[tokio::test]
async fn an_earlier_edit_brings_a_running_library_under_a_rename() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let admin = admin().await?;
    let (release, genre) = running_scene().await?;
    let edit = MetadataChange::EditMetadata {
        entity_id: release,
        edits: vec![MetadataChangeRequest {
            field: MetadataField::Genres,
            edit: MetadataEditOperation::Set {
                value: json!(["Idle"]),
            },
        }],
        expected: Some(serde_json::from_value(json!([{
            "field": "genres",
            "before": {"value": [], "source": "resolved"},
            "after": {"value": ["Idle"], "source": "manual"},
        }]))?),
    };

    let (index, error) = failure(apply(&admin, &[edit, rename(&genre, "New", "Idle")]).await);

    assert_eq!(index, 0, "the set's run check covers the edit too");
    assert!(matches!(error, ChangeError::Conflict(_)), "{error:?}");
    Ok(())
}

#[tokio::test]
async fn a_set_is_refused_whole_past_its_cap() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let admin = admin().await?;
    let change = rename("missing", "Name", "Old");

    for result in [
        apply(&admin, &[]).await,
        apply(&admin, &vec![change; MAX_CHANGES + 1]).await,
    ] {
        assert!(
            matches!(result, Err(ChangeSetError::Invalid(_))),
            "{result:?}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn access_lost_after_the_run_check_is_read_under_the_write_guard() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let [genre, _] = genres(["Current", "Other"]).await?;
    let (user, library) = {
        let mut db = STATE.db.write().await;
        lyra_db::roles::ensure_builtin_roles(&mut db)?;
        let user = insert_user(&mut db, &format!("changes-manager-{}", nanoid!()))?;
        let role = format!("changes-role-{}", nanoid!());
        lyra_db::roles::create(
            &mut db,
            &lyra_db::roles::Role {
                db_id: None,
                id: nanoid!(),
                name: role.clone(),
                permissions: vec![Permission::ManageMetadata],
            },
        )?;
        lyra_db::roles::ensure_user_has_role(&mut db, user, &role)?;
        let genre_id = lyra_db::lookup::find_node_id_by_id(&*db, &genre)?
            .ok_or_else(|| anyhow::anyhow!("genre missing"))?;
        let library = access::affected_libraries(&*db, genre_id)?
            .remove(0)
            .db_id
            .ok_or_else(|| anyhow::anyhow!("library missing"))?;
        lyra_db::libraries::grant_access(
            &mut db,
            user,
            library,
            lyra_db::libraries::AccessKind::ReadWrite,
        )?;
        (user, library)
    };
    let manager = user_principal(user).await?;
    let set = [rename(&genre, "New", "Current")];

    let (reached, result) = run_with_recycle_at(Some(0), apply(&manager, &set), |db| {
        lyra_db::libraries::revoke_access(db, user, library).expect("revoke");
    })
    .await;

    assert!(reached);
    let (index, error) = failure(result);
    assert_eq!(index, 0);
    assert!(matches!(error, ChangeError::NotFound(_)), "{error:?}");
    Ok(())
}

#[tokio::test]
async fn someone_elses_finished_change_reads_as_stale() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let admin = admin().await?;
    let [genre, _] = genres(["Done", "Other"]).await?;
    let (track, id) = {
        let mut db = STATE.db.write().await;
        let track = insert_track(&mut db, "Track")?;
        lyra_db::external_ids::upsert(
            &mut db,
            track,
            "provider",
            "recording_id",
            "b",
            IdSource::Manual,
        )?;
        (track, public_id(&db, track))
    };
    let manual_b = json!({"id_value": "b", "source": "manual"});

    // Previewed a set from `a` to `b`, which someone else then set.
    assert_stale(
        apply(
            &admin,
            &[set_id(&id, "b", Some(state("a", IdSource::Resolved)))],
        )
        .await,
        manual_b,
    );

    // Previewed a rename, which someone else then finished.
    assert_stale(
        apply(&admin, &[rename(&genre, "Done", "Before")]).await,
        json!("Done"),
    );

    // Previewed an inherit of the manual value, which someone else then inherited.
    let stored = {
        let mut db = STATE.db.write().await;
        db.transaction_mut(|t| {
            lyra_db::external_ids::remove_manual_inside_tx(t, track, "provider", "recording_id")
        })?
    };
    assert!(stored);
    assert_stale(
        apply(&admin, &[inherit_id(&id, state("b", IdSource::Manual))]).await,
        Value::Null,
    );
    Ok(())
}

#[tokio::test]
async fn an_expected_that_still_holds_leaves_the_operations_own_checks() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let admin = admin().await?;
    let [genre, other] = genres(["Mine", "Taken"]).await?;
    let id = {
        let mut db = STATE.db.write().await;
        let track = insert_track(&mut db, "Track")?;
        lyra_db::external_ids::upsert(
            &mut db,
            track,
            "provider",
            "recording_id",
            "found",
            IdSource::Resolved,
        )?;
        public_id(&db, track)
    };

    let (_, error) = failure(
        apply(
            &admin,
            &[inherit_id(&id, state("found", IdSource::Resolved))],
        )
        .await,
    );
    assert!(matches!(error, ChangeError::NotFound(_)), "{error:?}");
    let (_, error) = failure(apply(&admin, &[rename(&genre, "Mine", "Mine")]).await);
    assert!(matches!(error, ChangeError::Invalid(_)), "{error:?}");
    let (_, error) = failure(apply(&admin, &[rename(&genre, "Taken", "Mine")]).await);
    assert!(matches!(error, ChangeError::Conflict(_)), "{error:?}");
    let (_, error) = failure(apply(&admin, &[rename(&other, "Mine", "Former")]).await);
    assert!(matches!(error, ChangeError::Stale { .. }), "{error:?}");
    Ok(())
}

#[tokio::test]
async fn a_changed_fill_reads_as_stale_ahead_of_what_the_merge_would_refuse() -> anyhow::Result<()>
{
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let admin = admin().await?;
    let (winner, loser) = {
        let mut db = STATE.db.write().await;
        let library = insert_library(&mut db, "One", "/changes-stale-one")?;
        let other = insert_library(&mut db, "Two", "/changes-stale-two")?;
        let winner = insert_release(&mut db, "Winner")?;
        let loser = insert_release(&mut db, "Loser")?;
        lyra_db::graph::ensure_owned_edge(&mut *db, library, winner)?;
        lyra_db::graph::ensure_owned_edge(&mut *db, other, loser)?;
        save_manual(&mut db, loser, json!({"description": "Now"}))?;
        (public_id(&db, winner), public_id(&db, loser))
    };

    assert_stale(
        apply(&admin, &[merge(&winner, &[&loser], Vec::new())]).await,
        json!([{"key": "description", "value": "Now"}]),
    );
    Ok(())
}

#[tokio::test]
async fn a_set_over_the_loser_cap_is_refused_whole() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let admin = admin().await?;
    let losers = |count: usize| -> Vec<String> { vec!["missing".to_string(); count] };
    let merging = |count: usize| MetadataChange::Merge {
        winner_id: "winner".to_string(),
        loser_ids: losers(count),
        expected: Some(Vec::new()),
    };
    let over = [merging(MAX_LOSERS - 1), merging(2)];

    assert!(matches!(
        apply(&admin, &over).await,
        Err(ChangeSetError::Invalid(_))
    ));
    assert!(matches!(
        preview(&admin, &over).await,
        Err(ChangeSetError::Invalid(_))
    ));
    let at_the_cap = [merging(MAX_LOSERS - 1), merging(1)];
    let (index, error) = failure(apply(&admin, &at_the_cap).await);
    assert_eq!(index, 0);
    assert!(matches!(error, ChangeError::NotFound(_)), "{error:?}");
    Ok(())
}

#[tokio::test]
async fn only_a_set_checking_runs_waits_for_the_library_mutation_guard() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let admin = admin().await?;
    let [genre, _] = genres(["Genre", "Other"]).await?;
    let (edited, identified) = {
        let mut db = STATE.db.write().await;
        let release = insert_release(&mut db, "Before")?;
        let track = insert_track(&mut db, "Track")?;
        (public_id(&db, release), public_id(&db, track))
    };
    let wait = std::time::Duration::from_millis(200);
    let held = library_mutation_guard().await;

    let edits = [
        title(
            &edited,
            "After",
            Some(title_diff("Before", "resolved", "After")),
        ),
        set_id(&identified, "value", None),
    ];
    let result = tokio::time::timeout(wait, apply(&admin, &edits)).await?;
    assert_eq!(result?.len(), 2);

    let rename = [rename(&genre, "New", "Genre")];
    assert!(
        tokio::time::timeout(wait, apply(&admin, &rename))
            .await
            .is_err(),
        "a rename waits for the guard"
    );
    drop(held);
    Ok(())
}

#[tokio::test]
async fn a_set_without_a_merge_leaves_the_provider_registry_alone() -> anyhow::Result<()> {
    let _guard = runtime_test_lock().await;
    init_default_test_state()?;
    let admin = admin().await?;
    let id = {
        let mut db = STATE.db.write().await;
        let release = insert_release(&mut db, "Before")?;
        public_id(&db, release)
    };
    let held = provider_registry().write_owned().await;

    let edit = [title(
        &id,
        "After",
        Some(title_diff("Before", "resolved", "After")),
    )];
    let wait = std::time::Duration::from_millis(200);
    let result = tokio::time::timeout(wait, apply(&admin, &edit)).await?;

    assert_eq!(result?.len(), 1);
    drop(held);
    Ok(())
}
