// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::{
    BTreeMap,
    HashMap,
};

use agdb::{
    DbAny,
    DbId,
    QueryBuilder,
};
use serde_json::Value;
use serde_json::json;

use super::*;
use super::{
    model::{
        FieldState,
        MetadataChangeRequest,
        MetadataEditOperation,
        MetadataFieldDiff,
        MetadataValueSource,
    },
    state::credit_values,
};
use crate::services::metadata::layers::MANUAL_SOURCE_ID;
use lyra_db::{
    MetadataField,
    fixtures::{
        insert_artist,
        insert_release,
        insert_track,
    },
    test_db::new_test_db,
};

fn manual_layer(db: &DbAny, entity_id: DbId) -> anyhow::Result<Option<lyra_db::MetadataLayer>> {
    lyra_db::metadata::layers::find(db, entity_id, MANUAL_SOURCE_ID)
}

fn set(field: MetadataField, value: Value) -> MetadataChangeRequest {
    MetadataChangeRequest {
        field,
        edit: MetadataEditOperation::Set { value },
    }
}

fn inherit(field: MetadataField) -> MetadataChangeRequest {
    MetadataChangeRequest {
        field,
        edit: MetadataEditOperation::Inherit,
    }
}

fn preview_edit(
    db: &DbAny,
    entity_id: DbId,
    changes: Vec<MetadataChangeRequest>,
) -> Result<MetadataApplyRequest, MetadataEditingError> {
    let expected = preview(
        db,
        entity_id,
        &MetadataPreviewRequest {
            changes: changes.clone(),
        },
    )?;
    Ok(MetadataApplyRequest { changes, expected })
}

#[test]
fn requests_deny_unknown_fields() {
    let change = json!({"field": "track_title", "operation": "set", "value": "Title"});
    let field_state = json!({"value": "Title", "source": "manual"});
    let diff = json!({"field": "track_title", "before": field_state, "after": field_state});
    assert!(serde_json::from_value::<MetadataPreviewRequest>(json!({"changes": [change]})).is_ok());
    assert!(
        serde_json::from_value::<MetadataApplyRequest>(json!({
            "changes": [change],
            "expected": [diff]
        }))
        .is_ok()
    );

    let with_unexpected = |value: &Value| {
        let mut value = value.clone();
        value["unexpected"] = json!(true);
        value
    };
    assert!(
        serde_json::from_value::<MetadataPreviewRequest>(with_unexpected(
            &json!({"changes": [change]})
        ))
        .is_err()
    );
    assert!(
        serde_json::from_value::<MetadataApplyRequest>(with_unexpected(&json!({
            "changes": [change],
            "expected": [diff]
        })))
        .is_err()
    );
    assert!(serde_json::from_value::<MetadataChangeRequest>(with_unexpected(&change)).is_err());
    assert!(serde_json::from_value::<MetadataFieldDiff>(with_unexpected(&diff)).is_err());
    assert!(serde_json::from_value::<FieldState>(with_unexpected(&field_state)).is_err());
}

#[test]
fn preview_is_read_only_and_apply_persists_normalized_edit() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let track_id = insert_track(&mut db, "Old title")?;

    let edit = preview_edit(
        &db,
        track_id,
        vec![set(MetadataField::TrackTitle, json!("  New title  "))],
    )?;
    assert_eq!(edit.expected[0].after.value, json!("New title"));
    let preview_json = serde_json::to_value(&edit.expected)?;
    let entry = &preview_json.as_array().expect("diff array")[0];
    assert_eq!(entry.as_object().expect("diff entry").len(), 3);
    assert_eq!(entry["field"], json!("track_title"));
    assert_eq!(
        entry["before"],
        json!({"value": "Old title", "source": "resolved"})
    );
    assert_eq!(
        entry["after"],
        json!({"value": "New title", "source": "manual"})
    );
    assert_eq!(
        lyra_db::tracks::get_by_id(&db, track_id)?
            .expect("track exists")
            .track_title,
        "Old title"
    );

    let result = apply(&mut db, track_id, &edit)?;
    let result_json = serde_json::to_value(&result)?;
    assert!(result_json.get("entity_id").is_some());
    assert!(result_json.get("entity").is_none());
    assert!(result_json.get("diff").is_none());
    assert_eq!(result.fields["track_title"].value, json!("New title"));
    assert_eq!(
        result.fields["track_title"].source,
        MetadataValueSource::Manual
    );
    assert_eq!(
        manual_layer(&db, track_id)?
            .expect("manual layer exists")
            .fields[&MetadataField::TrackTitle],
        Some("New title".into())
    );
    Ok(())
}

#[test]
fn apply_rejects_a_field_changed_after_preview() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let track_id = insert_track(&mut db, "First")?;
    let edit = preview_edit(
        &db,
        track_id,
        vec![set(MetadataField::TrackTitle, json!("Second"))],
    )?;

    let mut concurrent = lyra_db::tracks::get_by_id(&db, track_id)?.expect("track exists");
    concurrent.set_track_title("Concurrent".to_string());
    lyra_db::tracks::update(&mut db, &concurrent)?;

    let error = apply(&mut db, track_id, &edit).expect_err("stale preview must conflict");
    let MetadataEditingError::Conflict(current) = error else {
        panic!("expected a metadata conflict");
    };
    assert_eq!(current.len(), 1);
    assert_eq!(current[0].field, MetadataField::TrackTitle);
    assert_eq!(current[0].before.value, json!("Concurrent"));
    assert_eq!(current[0].after.value, json!("Second"));
    assert!(manual_layer(&db, track_id)?.is_none());
    Ok(())
}

#[test]
fn apply_conflicts_with_an_empty_diff_when_the_edit_was_already_applied() -> anyhow::Result<()> {
    let mut db = new_test_db()?;

    let track_id = insert_track(&mut db, "First")?;
    let changes = vec![set(MetadataField::TrackTitle, json!("Second"))];
    let edit = preview_edit(&db, track_id, changes.clone())?;
    let other = preview_edit(&db, track_id, changes)?;
    apply(&mut db, track_id, &other)?;

    let error = apply(&mut db, track_id, &edit).expect_err("an already applied edit must conflict");
    let MetadataEditingError::Conflict(current) = error else {
        panic!("expected a metadata conflict");
    };
    assert!(current.is_empty());
    Ok(())
}

#[test]
fn apply_requires_expected_to_cover_only_requested_changes() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let track_id = insert_track(&mut db, "First")?;
    let changes = vec![set(MetadataField::TrackTitle, json!("Second"))];
    let diff = preview(
        &db,
        track_id,
        &MetadataPreviewRequest {
            changes: changes.clone(),
        },
    )?;

    let error = apply(
        &mut db,
        track_id,
        &MetadataApplyRequest {
            changes: changes.clone(),
            expected: Vec::new(),
        },
    )
    .expect_err("an empty expected diff must be rejected");
    assert!(matches!(error, MetadataEditingError::BadRequest(_)));

    let error = apply(
        &mut db,
        track_id,
        &MetadataApplyRequest {
            changes: changes.clone(),
            expected: vec![diff[0].clone(), diff[0].clone()],
        },
    )
    .expect_err("a duplicate expected field must be rejected");
    assert!(matches!(error, MetadataEditingError::BadRequest(_)));

    let error = apply(
        &mut db,
        track_id,
        &MetadataApplyRequest {
            changes: vec![set(MetadataField::SortTitle, json!("Second, The"))],
            expected: diff,
        },
    )
    .expect_err("an expected field outside changes must be rejected");
    let MetadataEditingError::BadRequest(message) = error else {
        panic!("expected a bad request, got {error:?}");
    };
    assert!(message.contains("title"));
    assert_eq!(
        lyra_db::tracks::get_by_id(&db, track_id)?
            .expect("track exists")
            .track_title,
        "First"
    );
    Ok(())
}

#[test]
fn apply_accepts_the_preview_diff_as_expected() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let track_id = insert_track(&mut db, "First")?;
    let changes = vec![
        set(MetadataField::TrackTitle, json!("Second")),
        set(MetadataField::Year, json!(2001)),
    ];
    let diff = preview(
        &db,
        track_id,
        &MetadataPreviewRequest {
            changes: changes.clone(),
        },
    )?;
    let expected: Vec<MetadataFieldDiff> = serde_json::from_value(serde_json::to_value(&diff)?)?;

    let result = apply(
        &mut db,
        track_id,
        &MetadataApplyRequest { changes, expected },
    )?;
    assert_eq!(result.fields["track_title"].value, json!("Second"));
    assert_eq!(result.fields["year"].value, json!(2001));
    assert_eq!(result.fields["year"].source, MetadataValueSource::Manual);
    Ok(())
}

#[test]
fn apply_revalidates_track_number_invariants_against_current_state() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let track_id = insert_track(&mut db, "Track")?;
    let mut track = lyra_db::tracks::get_by_id(&db, track_id)?.expect("track exists");
    track.set_track(1);
    track.set_track_total(3);
    lyra_db::tracks::update(&mut db, &track)?;

    let edit = preview_edit(&db, track_id, vec![set(MetadataField::Track, json!(2))])?;

    let mut concurrent = lyra_db::tracks::get_by_id(&db, track_id)?.expect("track exists");
    concurrent.set_track_total(1);
    lyra_db::tracks::update(&mut db, &concurrent)?;

    let error =
        apply(&mut db, track_id, &edit).expect_err("the current track_total must be revalidated");
    assert!(matches!(error, MetadataEditingError::BadRequest(_)));
    let stored = lyra_db::tracks::get_by_id(&db, track_id)?.expect("track exists");
    assert_eq!(stored.track, Some(1));
    assert_eq!(stored.track_total, Some(1));
    assert!(manual_layer(&db, track_id)?.is_none());
    Ok(())
}

#[test]
fn a_manual_clear_outranks_later_provider_values() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let track_id = insert_track(&mut db, "Track")?;
    let mut track = lyra_db::tracks::get_by_id(&db, track_id)?.expect("track exists");
    track.sort_title = Some("Track, The".to_string());
    track.set_year(1999);
    lyra_db::tracks::update(&mut db, &track)?;

    let edit = preview_edit(
        &db,
        track_id,
        vec![
            set(MetadataField::SortTitle, Value::Null),
            set(MetadataField::Year, Value::Null),
        ],
    )?;
    apply(&mut db, track_id, &edit)?;

    lyra_db::providers::upsert(
        &mut db,
        &lyra_db::ProviderConfig {
            db_id: None,
            provider_id: "test-provider".to_string(),
            position: 0,
            enabled: true,
        },
    )?;
    lyra_db::metadata::layers::upsert(
        &mut db,
        track_id,
        &lyra_db::MetadataLayer {
            source_id: "test-provider".to_string(),
            relations: Default::default(),
            credits: Default::default(),
            fields: crate::services::metadata::layers::json::fields_from_json(
                json!({"sort_title": "Provider sort", "year": 2024}),
            ),
            updated_at_ms: 10,
        },
    )?;
    crate::services::metadata::merging::apply_merged_metadata_to_entity(&mut db, track_id)?;

    let stored = lyra_db::tracks::get_by_id(&db, track_id)?.expect("track exists");
    assert_eq!(stored.sort_title, None);
    assert_eq!(stored.year, None);
    Ok(())
}

#[test]
fn set_accepts_null_and_empty_lists_only_for_clearable_fields() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let release_id = insert_release(&mut db, "Release")?;

    let error = preview_edit(
        &db,
        release_id,
        vec![set(MetadataField::ReleaseTitle, Value::Null)],
    )
    .expect_err("title cannot be cleared");
    let MetadataEditingError::BadRequest(message) = error else {
        panic!("expected bad request, got {error:?}");
    };
    assert!(message.contains("cannot be cleared"));

    let error = preview_edit(
        &db,
        release_id,
        vec![set(MetadataField::Genres, Value::Null)],
    )
    .expect_err("list fields are cleared with []");
    assert!(matches!(error, MetadataEditingError::BadRequest(_)));
    let error = preview_edit(
        &db,
        release_id,
        vec![set(MetadataField::Labels, Value::Null)],
    )
    .expect_err("labels are cleared with []");
    let MetadataEditingError::BadRequest(message) = error else {
        panic!("expected bad request, got {error:?}");
    };
    assert!(message.contains("use [] to clear"));

    let edit = preview_edit(
        &db,
        release_id,
        vec![
            set(MetadataField::ReleaseType, Value::Null),
            set(MetadataField::Genres, json!([])),
            set(MetadataField::Labels, json!([])),
            set(MetadataField::Credits, json!([])),
        ],
    )?;
    let after: HashMap<_, _> = edit
        .expected
        .iter()
        .map(|entry| (entry.field, entry.after.value.clone()))
        .collect();
    assert_eq!(after[&MetadataField::ReleaseType], Value::Null);
    assert_eq!(after[&MetadataField::Genres], json!([]));
    assert_eq!(after[&MetadataField::Labels], json!([]));
    assert_eq!(after[&MetadataField::Credits], json!([]));
    Ok(())
}

#[test]
fn inherit_drops_the_manual_value_and_restores_the_resolved_one() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let track_id = insert_track(&mut db, "Original")?;

    let manual = preview_edit(
        &db,
        track_id,
        vec![set(MetadataField::TrackTitle, json!("Manual"))],
    )?;
    apply(&mut db, track_id, &manual)?;

    lyra_db::providers::upsert(
        &mut db,
        &lyra_db::ProviderConfig {
            db_id: None,
            provider_id: "provider".to_string(),
            position: 0,
            enabled: true,
        },
    )?;
    lyra_db::metadata::layers::upsert(
        &mut db,
        track_id,
        &lyra_db::MetadataLayer {
            source_id: "provider".to_string(),
            relations: Default::default(),
            credits: Default::default(),
            fields: crate::services::metadata::layers::json::fields_from_json(
                json!({"track_title": "Resolved"}),
            ),
            updated_at_ms: 10,
        },
    )?;
    crate::services::metadata::merging::apply_merged_metadata_to_entity(&mut db, track_id)?;
    assert_eq!(
        lyra_db::tracks::get_by_id(&db, track_id)?
            .expect("track exists")
            .track_title,
        "Manual"
    );

    let inherited = preview_edit(&db, track_id, vec![inherit(MetadataField::TrackTitle)])?;
    assert_eq!(inherited.expected[0].after.value, json!("Resolved"));
    assert_eq!(
        inherited.expected[0].after.source,
        MetadataValueSource::Resolved
    );
    let result = apply(&mut db, track_id, &inherited)?;
    assert_eq!(result.fields["track_title"].value, json!("Resolved"));
    assert!(
        result
            .fields
            .values()
            .all(|field| field.source == MetadataValueSource::Resolved)
    );
    assert!(manual_layer(&db, track_id)?.is_none());
    Ok(())
}

#[test]
fn inherit_graph_field_restores_provider_resolved_value() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let release_id = insert_release(&mut db, "Release")?;
    lyra_db::providers::upsert(
        &mut db,
        &lyra_db::ProviderConfig {
            db_id: None,
            provider_id: "provider".to_string(),
            position: 0,
            enabled: true,
        },
    )?;
    let layer = lyra_db::MetadataLayer {
        source_id: "provider".to_string(),
        relations: Default::default(),
        credits: Default::default(),
        fields: crate::services::metadata::layers::json::resolved_fields_from_json(
            &mut db,
            json!({"genres": ["Provider"]}),
        ),
        updated_at_ms: 10,
    };
    lyra_db::metadata::layers::upsert(&mut db, release_id, &layer)?;
    crate::services::metadata::merging::apply_merged_metadata_to_entity(&mut db, release_id)?;

    let manual = preview_edit(
        &db,
        release_id,
        vec![set(MetadataField::Genres, json!(["Manual"]))],
    )?;
    apply(&mut db, release_id, &manual)?;

    let inherited = preview_edit(&db, release_id, vec![inherit(MetadataField::Genres)])?;
    assert_eq!(inherited.expected[0].after.value, json!(["Provider"]));
    let result = apply(&mut db, release_id, &inherited)?;
    assert_eq!(result.fields["genres"].value, json!(["Provider"]));
    assert!(
        result
            .fields
            .values()
            .all(|field| field.source == MetadataValueSource::Resolved)
    );
    Ok(())
}

#[test]
fn inherit_conflicts_when_provider_resolution_changes_after_preview() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let release_id = insert_release(&mut db, "Release")?;
    lyra_db::providers::upsert(
        &mut db,
        &lyra_db::ProviderConfig {
            db_id: None,
            provider_id: "provider".to_string(),
            position: 0,
            enabled: true,
        },
    )?;
    let provider_layer = |db: &mut DbAny, genres: &[&str], updated_at_ms| lyra_db::MetadataLayer {
        source_id: "provider".to_string(),
        relations: Default::default(),
        credits: Default::default(),
        fields: crate::services::metadata::layers::json::resolved_fields_from_json(
            db,
            json!({"genres": genres}),
        ),
        updated_at_ms,
    };
    let first = provider_layer(&mut db, &["First"], 10);
    lyra_db::metadata::layers::upsert(&mut db, release_id, &first)?;
    crate::services::metadata::merging::apply_merged_metadata_to_entity(&mut db, release_id)?;
    let manual = preview_edit(
        &db,
        release_id,
        vec![set(MetadataField::Genres, json!(["Manual"]))],
    )?;
    apply(&mut db, release_id, &manual)?;

    let inherited = preview_edit(&db, release_id, vec![inherit(MetadataField::Genres)])?;
    assert_eq!(inherited.expected[0].after.value, json!(["First"]));
    let second = provider_layer(&mut db, &["Second"], 20);
    lyra_db::metadata::layers::upsert(&mut db, release_id, &second)?;

    let error = apply(&mut db, release_id, &inherited)
        .expect_err("provider resolution changed after preview");
    let MetadataEditingError::Conflict(current) = error else {
        panic!("expected a metadata conflict");
    };
    assert_eq!(current[0].field, MetadataField::Genres);
    assert_eq!(current[0].after.value, json!(["Second"]));
    assert_eq!(current[0].after.source, MetadataValueSource::Resolved);
    assert_eq!(
        lyra_db::genres::get_for_release(&db, release_id)?[0].name,
        "Manual"
    );
    assert!(
        manual_layer(&db, release_id)?
            .is_some_and(|layer| layer.fields.contains_key(&MetadataField::Genres))
    );
    Ok(())
}

#[test]
fn label_edits_use_stable_label_identity() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let release_id = insert_release(&mut db, "Release")?;
    let keeper_id = insert_release(&mut db, "Keeper")?;
    let first_label_id = lyra_db::labels::resolve(
        &mut db,
        &lyra_db::labels::ResolveLabel {
            name: "Shared Name",
            external_id: Some(lyra_db::labels::ResolveExternalId {
                provider_id: "provider",
                id_type: "label",
                id_value: "first",
            }),
        },
    )?;
    let second_label_id = lyra_db::labels::resolve(
        &mut db,
        &lyra_db::labels::ResolveLabel {
            name: "Shared Name",
            external_id: Some(lyra_db::labels::ResolveExternalId {
                provider_id: "provider",
                id_type: "label",
                id_value: "second",
            }),
        },
    )?;
    assert_ne!(first_label_id, second_label_id);
    db.transaction_mut(|transaction| {
        lyra_db::labels::sync_release_label_links_inside_tx(
            transaction,
            release_id,
            &[lyra_db::labels::LabelLinkInput {
                label_id: first_label_id,
                catalog_number: None,
            }],
        )
    })?;
    db.transaction_mut(|transaction| {
        lyra_db::labels::sync_release_label_links_inside_tx(
            transaction,
            keeper_id,
            &[lyra_db::labels::LabelLinkInput {
                label_id: second_label_id,
                catalog_number: None,
            }],
        )
    })?;
    let second = lyra_db::labels::get_by_id(&db, second_label_id)?.expect("label exists");

    let edit = preview_edit(
        &db,
        release_id,
        vec![set(
            MetadataField::Labels,
            json!([{
                "id": second.id,
                "catalog_number": "CAT-2"
            }]),
        )],
    )?;
    assert_eq!(
        edit.expected[0].after.value[0]["name"],
        json!("Shared Name")
    );
    apply(&mut db, release_id, &edit)?;

    let labels = lyra_db::labels::get_for_release(&db, release_id)?;
    assert_eq!(labels.len(), 1);
    assert_eq!(labels[0].label.id, second.id);
    assert_eq!(labels[0].catalog_number.as_deref(), Some("CAT-2"));
    Ok(())
}

#[test]
fn inherit_labels_restores_resolvable_provider_labels() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let release_id = insert_release(&mut db, "Release")?;
    let keeper_id = insert_release(&mut db, "Keeper")?;
    let provider_label_id = lyra_db::labels::resolve(
        &mut db,
        &lyra_db::labels::ResolveLabel {
            name: "Provider Label",
            external_id: Some(lyra_db::labels::ResolveExternalId {
                provider_id: "provider",
                id_type: "label",
                id_value: "provider-label",
            }),
        },
    )?;
    let manual_label_id = lyra_db::labels::resolve(
        &mut db,
        &lyra_db::labels::ResolveLabel {
            name: "Manual Label",
            external_id: None,
        },
    )?;
    let provider_label =
        lyra_db::labels::get_by_id(&db, provider_label_id)?.expect("provider label exists");
    let manual_label =
        lyra_db::labels::get_by_id(&db, manual_label_id)?.expect("manual label exists");
    for owner in [release_id, keeper_id] {
        db.transaction_mut(|transaction| {
            lyra_db::labels::sync_release_label_links_inside_tx(
                transaction,
                owner,
                &[lyra_db::labels::LabelLinkInput {
                    label_id: provider_label_id,
                    catalog_number: Some("PROV-1".to_string()),
                }],
            )
        })?;
    }
    db.transaction_mut(|transaction| {
        lyra_db::labels::sync_release_label_links_inside_tx(
            transaction,
            keeper_id,
            &[
                lyra_db::labels::LabelLinkInput {
                    label_id: provider_label_id,
                    catalog_number: Some("PROV-1".to_string()),
                },
                lyra_db::labels::LabelLinkInput {
                    label_id: manual_label_id,
                    catalog_number: None,
                },
            ],
        )
    })?;
    lyra_db::providers::upsert(
        &mut db,
        &lyra_db::ProviderConfig {
            db_id: None,
            provider_id: "provider".to_string(),
            position: 0,
            enabled: true,
        },
    )?;
    let layer = lyra_db::MetadataLayer {
        source_id: "provider".to_string(),
        relations: Default::default(),
        credits: Default::default(),
        fields: crate::services::metadata::layers::json::resolved_fields_from_json(
            &mut db,
            json!({
                "labels": [{
                    "name": "Provider Label",
                    "catalog_number": "PROV-1",
                    "external_id": {
                        "provider_id": "provider",
                        "id_type": "label",
                        "id_value": "provider-label"
                    }
                }]
            }),
        ),
        updated_at_ms: 10,
    };
    lyra_db::metadata::layers::upsert(&mut db, release_id, &layer)?;

    let manual = preview_edit(
        &db,
        release_id,
        vec![set(
            MetadataField::Labels,
            json!([{"id": manual_label.id, "catalog_number": null}]),
        )],
    )?;
    apply(&mut db, release_id, &manual)?;

    let inherited = preview_edit(&db, release_id, vec![inherit(MetadataField::Labels)])?;
    assert_eq!(
        inherited.expected[0].after.value[0]["id"],
        json!(provider_label.id)
    );
    let result = apply(&mut db, release_id, &inherited)?;
    assert_eq!(
        result.fields["labels"].value[0]["id"],
        json!(provider_label.id)
    );
    assert!(
        result
            .fields
            .values()
            .all(|field| field.source == MetadataValueSource::Resolved)
    );
    Ok(())
}

#[test]
fn inherit_labels_takes_the_layers_own_label_not_one_with_the_same_name() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let release_id = insert_release(&mut db, "Release")?;
    let local_label_id = lyra_db::labels::resolve(
        &mut db,
        &lyra_db::labels::ResolveLabel {
            name: "Shared Name",
            external_id: Some(lyra_db::labels::ResolveExternalId {
                provider_id: "other",
                id_type: "label",
                id_value: "local-identity",
            }),
        },
    )?;
    let local_label = lyra_db::labels::get_by_id(&db, local_label_id)?.expect("label exists");
    db.transaction_mut(|transaction| {
        lyra_db::labels::sync_release_label_links_inside_tx(
            transaction,
            release_id,
            &[lyra_db::labels::LabelLinkInput {
                label_id: local_label_id,
                catalog_number: None,
            }],
        )
    })?;
    lyra_db::providers::upsert(
        &mut db,
        &lyra_db::ProviderConfig {
            db_id: None,
            provider_id: "provider".to_string(),
            position: 0,
            enabled: true,
        },
    )?;
    let layer = lyra_db::MetadataLayer {
        source_id: "provider".to_string(),
        relations: Default::default(),
        credits: Default::default(),
        fields: crate::services::metadata::layers::json::resolved_fields_from_json(
            &mut db,
            json!({
                "labels": [{
                    "name": "Shared Name",
                    "catalog_number": null,
                    "external_id": {
                        "provider_id": "provider",
                        "id_type": "label",
                        "id_value": "different-identity"
                    }
                }]
            }),
        ),
        updated_at_ms: 10,
    };
    lyra_db::metadata::layers::upsert(&mut db, release_id, &layer)?;
    let manual = preview_edit(
        &db,
        release_id,
        vec![set(
            MetadataField::Labels,
            json!([{"id": local_label.id, "catalog_number": null}]),
        )],
    )?;
    apply(&mut db, release_id, &manual)?;

    let provider_label_id =
        lyra_db::labels::find_by_external_id(&db, "provider", "label", "different-identity")?
            .expect("the layer's label was resolved when the layer saved");
    assert_ne!(provider_label_id, local_label_id);
    let provider_label = lyra_db::labels::get_by_id(&db, provider_label_id)?.expect("label exists");

    let inherited = preview_edit(&db, release_id, vec![inherit(MetadataField::Labels)])?;
    assert_eq!(
        inherited.expected[0].after.value[0]["id"],
        json!(provider_label.id)
    );
    Ok(())
}

#[test]
fn a_manually_masked_provider_label_survives_while_its_layer_names_it() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let release_id = insert_release(&mut db, "Release")?;
    let keeper_id = insert_release(&mut db, "Keeper")?;
    lyra_db::providers::upsert(
        &mut db,
        &lyra_db::ProviderConfig {
            db_id: None,
            provider_id: "provider".to_string(),
            position: 0,
            enabled: true,
        },
    )?;
    let fields = crate::services::metadata::layers::json::inputs_from_json(json!({
        "labels": [{
            "name": "Provider Label",
            "catalog_number": "PROV-1",
            "external_id": {
                "provider_id": "provider",
                "id_type": "label",
                "id_value": "provider-label"
            }
        }],
    }));
    crate::services::metadata::layers::save_provider_layer(
        &mut db,
        release_id,
        "provider",
        &crate::services::metadata::layers::ProviderValues {
            fields: fields.clone(),
            ..Default::default()
        },
        &HashMap::new(),
        &Default::default(),
    )?;
    let provider_label_id =
        lyra_db::labels::find_by_external_id(&db, "provider", "label", "provider-label")?
            .expect("provider label was materialized and linked");
    let manual_label_id = lyra_db::labels::resolve(
        &mut db,
        &lyra_db::labels::ResolveLabel {
            name: "Manual Label",
            external_id: None,
        },
    )?;
    db.transaction_mut(|transaction| {
        lyra_db::labels::sync_release_label_links_inside_tx(
            transaction,
            keeper_id,
            &[lyra_db::labels::LabelLinkInput {
                label_id: manual_label_id,
                catalog_number: None,
            }],
        )
    })?;
    let manual_label =
        lyra_db::labels::get_by_id(&db, manual_label_id)?.expect("manual label exists");
    let manual = preview_edit(
        &db,
        release_id,
        vec![set(
            MetadataField::Labels,
            json!([{"id": manual_label.id, "catalog_number": null}]),
        )],
    )?;
    apply(&mut db, release_id, &manual)?;
    assert!(lyra_db::labels::get_by_id(&db, provider_label_id)?.is_some());

    crate::services::metadata::layers::save_provider_layer(
        &mut db,
        release_id,
        "provider",
        &crate::services::metadata::layers::ProviderValues {
            fields: fields.clone(),
            ..Default::default()
        },
        &HashMap::new(),
        &Default::default(),
    )?;
    assert_eq!(
        lyra_db::labels::find_by_external_id(&db, "provider", "label", "provider-label")?,
        Some(provider_label_id)
    );
    assert_eq!(lyra_db::labels::get_for_release(&db, release_id)?.len(), 1);
    assert_eq!(
        lyra_db::labels::get_for_release(&db, release_id)?[0]
            .label
            .id,
        manual_label.id,
    );

    let inherited = preview_edit(&db, release_id, vec![inherit(MetadataField::Labels)])?;
    apply(&mut db, release_id, &inherited)?;
    let labels = lyra_db::labels::get_for_release(&db, release_id)?;
    assert_eq!(labels.len(), 1);
    assert_eq!(
        labels[0].label.db_id.clone().map(DbId::from),
        Some(provider_label_id),
    );
    Ok(())
}

#[test]
fn label_disappearance_after_preview_is_rejected() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let release_id = insert_release(&mut db, "Release")?;
    let keeper_id = insert_release(&mut db, "Keeper")?;
    let label_id = lyra_db::labels::resolve(
        &mut db,
        &lyra_db::labels::ResolveLabel {
            name: "Temporary",
            external_id: None,
        },
    )?;
    let label = lyra_db::labels::get_by_id(&db, label_id)?.expect("label exists");
    db.transaction_mut(|transaction| {
        lyra_db::labels::sync_release_label_links_inside_tx(
            transaction,
            keeper_id,
            &[lyra_db::labels::LabelLinkInput {
                label_id,
                catalog_number: None,
            }],
        )
    })?;
    let edit = preview_edit(
        &db,
        release_id,
        vec![set(
            MetadataField::Labels,
            json!([{"id": label.id, "catalog_number": null}]),
        )],
    )?;
    db.exec_mut(QueryBuilder::remove().ids(label_id).query())?;

    let error =
        apply(&mut db, release_id, &edit).expect_err("a disappeared label must be rejected");
    assert!(matches!(error, MetadataEditingError::BadRequest(_)));
    assert!(manual_layer(&db, release_id)?.is_none());
    Ok(())
}

#[test]
fn inherited_label_disappearance_after_preview_is_rejected() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let release_id = insert_release(&mut db, "Release")?;
    let keeper_id = insert_release(&mut db, "Keeper")?;
    let provider_label_id = lyra_db::labels::resolve(
        &mut db,
        &lyra_db::labels::ResolveLabel {
            name: "Provider Label",
            external_id: Some(lyra_db::labels::ResolveExternalId {
                provider_id: "provider",
                id_type: "label",
                id_value: "provider-label",
            }),
        },
    )?;
    let manual_label_id = lyra_db::labels::resolve(
        &mut db,
        &lyra_db::labels::ResolveLabel {
            name: "Manual Label",
            external_id: None,
        },
    )?;
    for owner in [release_id, keeper_id] {
        db.transaction_mut(|transaction| {
            lyra_db::labels::sync_release_label_links_inside_tx(
                transaction,
                owner,
                &[lyra_db::labels::LabelLinkInput {
                    label_id: provider_label_id,
                    catalog_number: None,
                }],
            )
        })?;
    }
    db.transaction_mut(|transaction| {
        lyra_db::labels::sync_release_label_links_inside_tx(
            transaction,
            keeper_id,
            &[
                lyra_db::labels::LabelLinkInput {
                    label_id: provider_label_id,
                    catalog_number: None,
                },
                lyra_db::labels::LabelLinkInput {
                    label_id: manual_label_id,
                    catalog_number: None,
                },
            ],
        )
    })?;
    lyra_db::providers::upsert(
        &mut db,
        &lyra_db::ProviderConfig {
            db_id: None,
            provider_id: "provider".to_string(),
            position: 0,
            enabled: true,
        },
    )?;
    let layer = lyra_db::MetadataLayer {
        source_id: "provider".to_string(),
        relations: Default::default(),
        credits: Default::default(),
        fields: crate::services::metadata::layers::json::resolved_fields_from_json(
            &mut db,
            json!({
                "labels": [{
                    "name": "Provider Label",
                    "catalog_number": null,
                    "external_id": {
                        "provider_id": "provider",
                        "id_type": "label",
                        "id_value": "provider-label"
                    }
                }]
            }),
        ),
        updated_at_ms: 10,
    };
    lyra_db::metadata::layers::upsert(&mut db, release_id, &layer)?;
    let manual_label =
        lyra_db::labels::get_by_id(&db, manual_label_id)?.expect("manual label exists");
    let manual = preview_edit(
        &db,
        release_id,
        vec![set(
            MetadataField::Labels,
            json!([{"id": manual_label.id, "catalog_number": null}]),
        )],
    )?;
    apply(&mut db, release_id, &manual)?;
    let inherited = preview_edit(&db, release_id, vec![inherit(MetadataField::Labels)])?;
    db.exec_mut(QueryBuilder::remove().ids(provider_label_id).query())?;

    let error = apply(&mut db, release_id, &inherited)
        .expect_err("an inherited label disappearing after preview must be rejected");
    assert!(matches!(error, MetadataEditingError::Conflict(_)));
    assert!(
        manual_layer(&db, release_id)?
            .is_some_and(|layer| layer.fields.contains_key(&MetadataField::Labels))
    );
    Ok(())
}

#[test]
fn credit_reference_disappearance_after_preview_is_rejected() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let release_id = insert_release(&mut db, "Release")?;
    let artist_id = insert_artist(&mut db, "Temporary Artist")?;
    let artist = lyra_db::artists::get_by_id(&db, artist_id)?.expect("artist exists");
    let edit = preview_edit(
        &db,
        release_id,
        vec![set(
            MetadataField::Credits,
            json!([{"artist_id": artist.id, "type": "artist", "detail": null}]),
        )],
    )?;
    db.exec_mut(QueryBuilder::remove().ids(artist_id).query())?;

    let error = apply(&mut db, release_id, &edit)
        .expect_err("an artist disappearing after preview must be rejected");
    assert!(matches!(error, MetadataEditingError::BadRequest(_)));
    assert!(manual_layer(&db, release_id)?.is_none());
    Ok(())
}

#[test]
fn a_credit_edit_supplies_every_role() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let release_id = insert_release(&mut db, "Release")?;
    let artist_id = insert_artist(&mut db, "Artist")?;
    let artist = lyra_db::artists::get_by_id(&db, artist_id)?.expect("artist exists");

    let edit = preview_edit(
        &db,
        release_id,
        vec![set(
            MetadataField::Credits,
            json!([{"artist_id": artist.id, "type": "artist", "detail": null}]),
        )],
    )?;
    apply(&mut db, release_id, &edit)?;

    assert_eq!(credit_values(&db, release_id)?.len(), 1);
    let layer = manual_layer(&db, release_id)?.expect("manual layer exists");
    assert_eq!(layer.credits.len(), lyra_db::CreditType::ALL.len());
    for (credit_type, inputs) in layer.credits {
        let expected: &[DbId] = if credit_type == lyra_db::CreditType::Artist {
            &[artist_id]
        } else {
            &[]
        };
        assert!(
            inputs
                .iter()
                .map(|input| input.artist_id)
                .eq(expected.iter().copied()),
            "{credit_type} holds {inputs:?}"
        );
    }
    Ok(())
}

#[test]
fn inherit_graph_field_without_a_provider_value_clears_manual_edges() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let release_id = insert_release(&mut db, "Release")?;
    let artist_id = insert_artist(&mut db, "Artist")?;
    let artist = lyra_db::artists::get_by_id(&db, artist_id)?.expect("artist exists");
    let manual = preview_edit(
        &db,
        release_id,
        vec![set(
            MetadataField::Credits,
            json!([{"artist_id": artist.id, "type": "artist", "detail": null}]),
        )],
    )?;
    apply(&mut db, release_id, &manual)?;

    let inherited = preview_edit(&db, release_id, vec![inherit(MetadataField::Credits)])?;
    assert_eq!(inherited.expected[0].after.value, json!([]));
    assert_eq!(
        inherited.expected[0].after.source,
        MetadataValueSource::Resolved
    );
    let result = apply(&mut db, release_id, &inherited)?;

    assert!(credit_values(&db, release_id)?.is_empty());
    assert!(
        result
            .fields
            .values()
            .all(|field| field.source == MetadataValueSource::Resolved)
    );
    Ok(())
}

#[test]
fn inheriting_credits_restores_what_the_layers_supply() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let track_id = insert_track(&mut db, "Track")?;
    let layer_artist = insert_artist(&mut db, "Layer Artist")?;
    let manual_artist = insert_artist(&mut db, "Manual Artist")?;
    lyra_db::metadata::layers::upsert(
        &mut db,
        track_id,
        &lyra_db::MetadataLayer::new("local", Default::default(), 1).with_credits(BTreeMap::from(
            [(
                lyra_db::CreditType::Artist,
                vec![lyra_db::metadata::layers::edges::LayerCreditInput {
                    artist_id: layer_artist,
                    detail: None,
                }],
            )],
        )),
    )?;
    crate::services::metadata::merging::apply_merged_metadata_to_entity(&mut db, track_id)?;
    let manual = lyra_db::artists::get_by_id(&db, manual_artist)?.expect("artist exists");
    let edit = preview_edit(
        &db,
        track_id,
        vec![set(
            MetadataField::Credits,
            json!([{"artist_id": manual.id, "type": "artist"}]),
        )],
    )?;
    apply(&mut db, track_id, &edit)?;
    assert_eq!(
        lyra_db::credits::links_for_owner(&db, track_id)?[0].artist_id,
        manual_artist
    );

    let inherited = preview_edit(&db, track_id, vec![inherit(MetadataField::Credits)])?;
    apply(&mut db, track_id, &inherited)?;

    let links = lyra_db::credits::links_for_owner(&db, track_id)?;
    assert_eq!(links.len(), 1);
    assert_eq!(links[0].artist_id, layer_artist);
    Ok(())
}

#[test]
fn a_manual_credit_edit_moves_the_credited_flag() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let track_id = insert_track(&mut db, "Track")?;
    let layer_artist = insert_artist(&mut db, "Layer Artist")?;
    let manual_artist = insert_artist(&mut db, "Manual Artist")?;
    lyra_db::metadata::layers::upsert(
        &mut db,
        track_id,
        &lyra_db::MetadataLayer::new("local", Default::default(), 1).with_credits(BTreeMap::from(
            [(
                lyra_db::CreditType::Artist,
                vec![lyra_db::metadata::layers::edges::LayerCreditInput {
                    artist_id: layer_artist,
                    detail: None,
                }],
            )],
        )),
    )?;
    crate::services::metadata::merging::apply_merged_metadata_to_entity(&mut db, track_id)?;
    let credited = |db: &DbAny, artist_id| -> anyhow::Result<bool> {
        Ok(lyra_db::artists::get_by_id(db, artist_id)?.is_some_and(|artist| artist.credited))
    };
    assert!(credited(&db, layer_artist)? && !credited(&db, manual_artist)?);

    let manual = lyra_db::artists::get_by_id(&db, manual_artist)?.expect("artist exists");
    let edit = preview_edit(
        &db,
        track_id,
        vec![set(
            MetadataField::Credits,
            json!([{"artist_id": manual.id, "type": "artist"}]),
        )],
    )?;
    apply(&mut db, track_id, &edit)?;

    assert!(
        !credited(&db, layer_artist)?,
        "the layer still names it, uncredited"
    );
    assert!(credited(&db, manual_artist)?);
    Ok(())
}

#[test]
fn relation_edits_replace_the_incoming_relations_and_inherit_restores_them() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let actor_id = insert_artist(&mut db, "Actor")?;
    let character_id = insert_artist(&mut db, "Character")?;
    let member_id = insert_artist(&mut db, "Member")?;
    lyra_db::providers::upsert(
        &mut db,
        &lyra_db::ProviderConfig {
            db_id: None,
            provider_id: "provider".to_string(),
            position: 0,
            enabled: true,
        },
    )?;
    crate::services::metadata::layers::save_provider_layer(
        &mut db,
        character_id,
        "provider",
        &crate::services::metadata::layers::ProviderValues {
            relations: BTreeMap::from([(
                lyra_db::ArtistRelationType::VoiceActor,
                vec![lyra_db::metadata::layers::edges::LayerRelationInput {
                    artist_id: actor_id,
                    attributes: Some("lead".to_string()),
                }],
            )]),
            ..Default::default()
        },
        &HashMap::new(),
        &Default::default(),
    )?;
    let actor = lyra_db::artists::get_by_id(&db, actor_id)?.expect("artist exists");
    let member = lyra_db::artists::get_by_id(&db, member_id)?.expect("artist exists");
    let snapshot = get_snapshot(&db, character_id)?;
    assert_eq!(
        snapshot.fields["relations"].value,
        json!([{"artist_id": actor.id, "type": "voice_actor", "attributes": "lead"}])
    );
    assert_eq!(
        get_snapshot(&db, actor_id)?.fields["relations"].value,
        json!([]),
        "an artist edits the relations into it, not the ones it holds"
    );

    let manual = preview_edit(
        &db,
        character_id,
        vec![set(
            MetadataField::Relations,
            json!([{"artist_id": member.id, "type": "member_of"}]),
        )],
    )?;
    apply(&mut db, character_id, &manual)?;
    let incoming = |db: &DbAny| -> anyhow::Result<Vec<(DbId, lyra_db::ArtistRelationType)>> {
        Ok(
            lyra_db::artists::relations::get_relations_to(db, character_id, None)?
                .into_iter()
                .map(|(relation, source)| (source, relation.relation_type))
                .collect(),
        )
    };
    assert_eq!(
        incoming(&db)?,
        vec![(member_id, lyra_db::ArtistRelationType::MemberOf)],
        "a type the edit leaves out clears"
    );
    let layer = manual_layer(&db, character_id)?.expect("manual layer exists");
    assert_eq!(
        layer.relations.len(),
        lyra_db::ArtistRelationType::ALL.len()
    );

    let inherited = preview_edit(&db, character_id, vec![inherit(MetadataField::Relations)])?;
    apply(&mut db, character_id, &inherited)?;

    let relations = lyra_db::artists::relations::get_relations_to(&db, character_id, None)?;
    assert_eq!(relations.len(), 1);
    assert_eq!(relations[0].1, actor_id);
    assert_eq!(
        relations[0].0.relation_type,
        lyra_db::ArtistRelationType::VoiceActor
    );
    assert_eq!(relations[0].0.attributes.as_deref(), Some("lead"));
    assert!(manual_layer(&db, character_id)?.is_none());
    Ok(())
}

#[test]
fn a_relation_edit_applies_through_a_lock() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let band_id = insert_artist(&mut db, "Band")?;
    let member_id = insert_artist(&mut db, "Member")?;
    let mut band = lyra_db::artists::get_by_id(&db, band_id)?.expect("artist exists");
    band.locked = Some(true);
    lyra_db::artists::update(&mut db, &band)?;
    let member = lyra_db::artists::get_by_id(&db, member_id)?.expect("artist exists");

    let edit = preview_edit(
        &db,
        band_id,
        vec![set(
            MetadataField::Relations,
            json!([{"artist_id": member.id, "type": "member_of"}]),
        )],
    )?;
    apply(&mut db, band_id, &edit)?;
    assert_eq!(
        lyra_db::artists::relations::get_relations_to(&db, band_id, None)?.len(),
        1
    );

    let inherited = preview_edit(&db, band_id, vec![inherit(MetadataField::Relations)])?;
    apply(&mut db, band_id, &inherited)?;
    assert!(lyra_db::artists::relations::get_relations_to(&db, band_id, None)?.is_empty());
    Ok(())
}

#[test]
fn inherit_clears_a_scalar_that_no_layer_supplies() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let track_id = insert_track(&mut db, "Track")?;

    let manual = preview_edit(
        &db,
        track_id,
        vec![set(MetadataField::SortTitle, json!("Manual Sort"))],
    )?;
    apply(&mut db, track_id, &manual)?;

    let inherited = preview_edit(&db, track_id, vec![inherit(MetadataField::SortTitle)])?;
    assert_eq!(inherited.expected[0].after.value, Value::Null);

    apply(&mut db, track_id, &inherited)?;

    assert_eq!(
        lyra_db::tracks::get_by_id(&db, track_id)?
            .expect("track exists")
            .sort_title,
        None
    );
    assert!(manual_layer(&db, track_id)?.is_none());
    Ok(())
}

#[test]
fn credits_apply_in_role_order() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let track_id = insert_track(&mut db, "Track")?;
    let [first, composer, second] = [
        insert_artist(&mut db, "First")?,
        insert_artist(&mut db, "Composer")?,
        insert_artist(&mut db, "Second")?,
    ];
    let public_id = |artist_id| -> anyhow::Result<String> {
        Ok(lyra_db::artists::get_by_id(&db, artist_id)?
            .expect("artist exists")
            .id)
    };
    let [first, composer, second] = [public_id(first)?, public_id(composer)?, public_id(second)?];
    let credit = |artist_id: &str, credit_type: &str| json!({"artist_id": artist_id, "type": credit_type, "detail": null});

    let edit = preview_edit(
        &db,
        track_id,
        vec![set(
            MetadataField::Credits,
            json!([
                credit(&first, "artist"),
                credit(&composer, "composer"),
                credit(&second, "artist"),
            ]),
        )],
    )?;
    let expected = json!([
        credit(&first, "artist"),
        credit(&second, "artist"),
        credit(&composer, "composer"),
    ]);
    assert_eq!(edit.expected[0].after.value, expected);

    let snapshot = apply(&mut db, track_id, &edit)?;
    assert_eq!(snapshot.fields["credits"].value, expected);
    Ok(())
}

#[test]
fn edits_apply_through_a_lock_that_keeps_provider_changes_out() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let track_id = insert_track(&mut db, "Stored")?;
    lyra_db::providers::upsert(
        &mut db,
        &lyra_db::ProviderConfig {
            db_id: None,
            provider_id: "provider".to_string(),
            position: 0,
            enabled: true,
        },
    )?;
    let provider_title = |db: &mut DbAny, title: &str| -> anyhow::Result<()> {
        crate::services::metadata::layers::save_provider_layer(
            db,
            track_id,
            "provider",
            &crate::services::metadata::layers::ProviderValues {
                fields: crate::services::metadata::layers::json::inputs_from_json(
                    json!({"track_title": title}),
                ),
                ..Default::default()
            },
            &HashMap::new(),
            &Default::default(),
        )
    };
    provider_title(&mut db, "Provider")?;
    let mut track = lyra_db::tracks::get_by_id(&db, track_id)?.expect("track exists");
    track.locked = Some(true);
    lyra_db::tracks::update(&mut db, &track)?;
    let title = |db: &DbAny| -> anyhow::Result<String> {
        Ok(lyra_db::tracks::get_by_id(db, track_id)?
            .expect("track exists")
            .track_title)
    };

    let manual = preview_edit(
        &db,
        track_id,
        vec![set(MetadataField::TrackTitle, json!("Manual"))],
    )?;
    apply(&mut db, track_id, &manual)?;
    assert_eq!(title(&db)?, "Manual");

    provider_title(&mut db, "Renamed")?;
    let inherited = preview_edit(&db, track_id, vec![inherit(MetadataField::TrackTitle)])?;
    assert_eq!(inherited.expected[0].after.value, json!("Renamed"));
    apply(&mut db, track_id, &inherited)?;
    assert_eq!(
        title(&db)?,
        "Renamed",
        "an inherit applies through the lock"
    );
    assert!(manual_layer(&db, track_id)?.is_none());

    provider_title(&mut db, "Later")?;
    assert_eq!(
        title(&db)?,
        "Renamed",
        "the lock keeps provider changes out"
    );
    Ok(())
}

#[test]
fn an_invalid_manual_layer_fails_the_edit_instead_of_being_replaced() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let track_id = insert_track(&mut db, "Track")?;
    let edit = preview_edit(&db, track_id, vec![set(MetadataField::Year, json!(1999))])?;
    apply(&mut db, track_id, &edit)?;
    let layer_id = lyra_db::metadata::layers::layer_id(&db, track_id, MANUAL_SOURCE_ID)?
        .expect("manual layer exists");
    db.exec_mut(
        QueryBuilder::insert()
            .values_uniform([("supplied", vec!["year".to_string(), "work_id".to_string()]).into()])
            .ids(layer_id)
            .query(),
    )?;

    let changes = vec![set(MetadataField::TrackTitle, json!("Renamed"))];
    for error in [
        preview_edit(&db, track_id, changes.clone()).expect_err("preview fails"),
        apply(
            &mut db,
            track_id,
            &MetadataApplyRequest {
                changes,
                expected: edit.expected.clone(),
            },
        )
        .expect_err("apply fails"),
    ] {
        let MetadataEditingError::Internal(error) = error else {
            panic!("expected an internal error, got {error:?}");
        };
        assert!(
            format!("{error:#}").contains("'manual' metadata layer is invalid"),
            "{error:#}"
        );
    }
    assert_eq!(
        lyra_db::tracks::get_by_id(&db, track_id)?
            .expect("track exists")
            .year,
        Some(1999)
    );
    assert_eq!(
        lyra_db::metadata::layers::ids_for_entity(&db, track_id)?,
        vec![layer_id],
        "the stored layer is left for recovery"
    );
    Ok(())
}
