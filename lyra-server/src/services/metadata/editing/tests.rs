// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::{
    HashMap,
    HashSet,
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
        MetadataChangeRequest,
        MetadataEditOperation,
        MetadataValueSource,
    },
    state::credit_values,
};
use crate::{
    db::{
        self,
        Permission,
        metadata::manual_overrides::ManualMetadataField,
        test_db::{
            connect_artist,
            insert_artist,
            insert_library,
            insert_release,
            insert_track,
            new_test_db,
        },
    },
    services::auth::Principal,
};

fn principal(user_id: &str) -> Principal {
    Principal {
        user_db_id: DbId(1),
        user_public_id: user_id.to_string(),
        username: user_id.to_string(),
        permissions: vec![Permission::Admin],
        role_name: None,
        accessible_library_ids: HashSet::new(),
    }
}

fn set(field: MetadataField, value: Value) -> MetadataChangeRequest {
    MetadataChangeRequest {
        field,
        edit: MetadataEditOperation::Set { value },
    }
}

fn clear(field: MetadataField) -> MetadataChangeRequest {
    MetadataChangeRequest {
        field,
        edit: MetadataEditOperation::Clear,
    }
}

fn inherit(field: MetadataField) -> MetadataChangeRequest {
    MetadataChangeRequest {
        field,
        edit: MetadataEditOperation::Inherit,
    }
}

fn apply_preview(
    db: &mut DbAny,
    principal: &Principal,
    entity_id: DbId,
    preview: MetadataPreviewResponse,
) -> Result<MetadataSnapshot, MetadataEditingError> {
    apply(
        db,
        principal,
        entity_id,
        &MetadataApplyRequest {
            preview_id: preview.preview_id,
        },
    )
}

#[test]
fn requests_reject_removed_contract_fields() {
    assert!(
        serde_json::from_value::<MetadataPreviewRequest>(json!({
            "changes": [{
                "field": "title",
                "operation": "set",
                "value": "Title"
            }]
        }))
        .is_ok()
    );
    assert!(
        serde_json::from_value::<MetadataPreviewRequest>(json!({
            "entity_type": "track",
            "changes": []
        }))
        .is_err()
    );
    assert!(
        serde_json::from_value::<MetadataApplyRequest>(json!({
            "preview_id": "preview",
            "changes": [],
            "expected": []
        }))
        .is_err()
    );
    assert!(
        serde_json::from_value::<MetadataPreviewRequest>(json!({
            "changes": [{
                "field": "title",
                "operation": "set",
                "value": "Title",
                "unexpected": true
            }]
        }))
        .is_err()
    );
}

#[test]
fn preview_is_read_only_and_apply_persists_normalized_edit() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let principal = principal("user-1");
    let track_id = insert_track(&mut db, "Old title")?;

    let preview = preview(
        &db,
        &principal,
        track_id,
        &MetadataPreviewRequest {
            changes: vec![set(MetadataField::Title, json!("  New title  "))],
        },
    )?;
    assert_eq!(preview.diff[0].after, json!("New title"));
    let preview_json = serde_json::to_value(&preview)?;
    assert_eq!(preview_json.as_object().expect("preview object").len(), 2);
    assert!(preview_json.get("preview_id").is_some());
    assert!(preview_json.get("diff").is_some());
    assert_eq!(
        db::tracks::get_by_id(&db, track_id)?
            .expect("track exists")
            .track_title,
        "Old title"
    );

    let result = apply_preview(&mut db, &principal, track_id, preview)?;
    let result_json = serde_json::to_value(&result)?;
    assert!(result_json.get("entity_id").is_some());
    assert!(result_json.get("entity").is_none());
    assert!(result_json.get("diff").is_none());
    assert_eq!(result.fields["title"], json!("New title"));
    assert_eq!(result.manual_fields, vec![MetadataField::Title]);
    assert_eq!(
        db::metadata::manual_overrides::get(&db, track_id)?
            .expect("manual override exists")
            .parsed_fields()?[&ManualMetadataField::TrackTitle],
        json!("New title")
    );
    Ok(())
}

#[test]
fn preview_is_bound_to_user_and_is_single_use() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let owner = principal("owner");
    let other = principal("other");
    let track_id = insert_track(&mut db, "First")?;

    let wrong_user_preview = preview(
        &db,
        &owner,
        track_id,
        &MetadataPreviewRequest {
            changes: vec![set(MetadataField::Title, json!("Second"))],
        },
    )?;
    apply_preview(&mut db, &other, track_id, wrong_user_preview)
        .expect_err("another user must not apply the preview");

    let preview = preview(
        &db,
        &owner,
        track_id,
        &MetadataPreviewRequest {
            changes: vec![set(MetadataField::Title, json!("Second"))],
        },
    )?;
    let preview_id = preview.preview_id.clone();
    apply_preview(&mut db, &owner, track_id, preview)?;
    apply(
        &mut db,
        &owner,
        track_id,
        &MetadataApplyRequest { preview_id },
    )
    .expect_err("a consumed preview must not be reusable");
    Ok(())
}

#[test]
fn apply_rejects_a_field_changed_after_preview() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let principal = principal("user-1");
    let track_id = insert_track(&mut db, "First")?;
    let preview = preview(
        &db,
        &principal,
        track_id,
        &MetadataPreviewRequest {
            changes: vec![set(MetadataField::Title, json!("Second"))],
        },
    )?;

    let mut concurrent = db::tracks::get_by_id(&db, track_id)?.expect("track exists");
    concurrent.set_track_title("Concurrent".to_string());
    db::tracks::update(&mut db, &concurrent)?;

    let error = apply_preview(&mut db, &principal, track_id, preview)
        .expect_err("stale preview must conflict");
    let MetadataEditingError::Conflict(conflicts) = error else {
        panic!("expected a metadata conflict");
    };
    assert_eq!(conflicts.len(), 1);
    assert_eq!(conflicts[0].field, MetadataField::Title);
    assert_eq!(conflicts[0].current, json!("Concurrent"));
    assert!(db::metadata::manual_overrides::get(&db, track_id)?.is_none());
    Ok(())
}

#[test]
fn apply_revalidates_track_number_invariants_against_current_state() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let principal = principal("user-1");
    let track_id = insert_track(&mut db, "Track")?;
    let mut track = db::tracks::get_by_id(&db, track_id)?.expect("track exists");
    track.set_track(1);
    track.set_track_total(3);
    db::tracks::update(&mut db, &track)?;

    let preview = preview(
        &db,
        &principal,
        track_id,
        &MetadataPreviewRequest {
            changes: vec![set(MetadataField::Track, json!(2))],
        },
    )?;

    let mut concurrent = db::tracks::get_by_id(&db, track_id)?.expect("track exists");
    concurrent.set_track_total(1);
    db::tracks::update(&mut db, &concurrent)?;

    let error = apply_preview(&mut db, &principal, track_id, preview)
        .expect_err("the current track_total must be revalidated");
    assert!(matches!(error, MetadataEditingError::BadRequest(_)));
    let stored = db::tracks::get_by_id(&db, track_id)?.expect("track exists");
    assert_eq!(stored.track, Some(1));
    assert_eq!(stored.track_total, Some(1));
    assert!(db::metadata::manual_overrides::get(&db, track_id)?.is_none());
    Ok(())
}

#[test]
fn manual_clear_overrides_later_provider_values() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let principal = principal("user-1");
    let track_id = insert_track(&mut db, "Track")?;
    let mut track = db::tracks::get_by_id(&db, track_id)?.expect("track exists");
    track.set_sort_title("Track, The".to_string());
    track.set_year(1999);
    db::tracks::update(&mut db, &track)?;

    let preview = preview(
        &db,
        &principal,
        track_id,
        &MetadataPreviewRequest {
            changes: vec![clear(MetadataField::SortTitle), clear(MetadataField::Year)],
        },
    )?;
    apply_preview(&mut db, &principal, track_id, preview)?;

    db::providers::upsert(
        &mut db,
        &db::ProviderConfig {
            db_id: None,
            provider_id: "test-provider".to_string(),
            display_name: "Test provider".to_string(),
            priority: 100,
            enabled: true,
        },
    )?;
    db::metadata::layers::upsert(
        &mut db,
        track_id,
        &db::MetadataLayer {
            db_id: None,
            provider_id: "test-provider".to_string(),
            fields: json!({"sort_title": "Provider sort", "year": 2024}).to_string(),
            updated_at: 10,
        },
    )?;
    crate::services::metadata::merging::apply_merged_metadata_to_entity(&mut db, track_id)?;

    let stored = db::tracks::get_by_id(&db, track_id)?.expect("track exists");
    assert_eq!(stored.sort_title, None);
    assert_eq!(stored.year, None);
    Ok(())
}

#[test]
fn inherit_relinquishes_ownership_and_restores_resolved_value() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let principal = principal("user-1");
    let track_id = insert_track(&mut db, "Original")?;

    let manual = preview(
        &db,
        &principal,
        track_id,
        &MetadataPreviewRequest {
            changes: vec![set(MetadataField::Title, json!("Manual"))],
        },
    )?;
    apply_preview(&mut db, &principal, track_id, manual)?;

    db::providers::upsert(
        &mut db,
        &db::ProviderConfig {
            db_id: None,
            provider_id: "provider".to_string(),
            display_name: "Provider".to_string(),
            priority: 100,
            enabled: true,
        },
    )?;
    db::metadata::layers::upsert(
        &mut db,
        track_id,
        &db::MetadataLayer {
            db_id: None,
            provider_id: "provider".to_string(),
            fields: json!({"track_title": "Resolved"}).to_string(),
            updated_at: 10,
        },
    )?;
    crate::services::metadata::merging::apply_merged_metadata_to_entity(&mut db, track_id)?;
    assert_eq!(
        db::tracks::get_by_id(&db, track_id)?
            .expect("track exists")
            .track_title,
        "Manual"
    );

    let inherited = preview(
        &db,
        &principal,
        track_id,
        &MetadataPreviewRequest {
            changes: vec![inherit(MetadataField::Title)],
        },
    )?;
    assert_eq!(inherited.diff[0].after, json!("Resolved"));
    assert_eq!(
        inherited.diff[0].source_after,
        MetadataValueSource::Resolved
    );
    let result = apply_preview(&mut db, &principal, track_id, inherited)?;
    assert_eq!(result.fields["title"], json!("Resolved"));
    assert!(result.manual_fields.is_empty());
    assert!(db::metadata::manual_overrides::get(&db, track_id)?.is_none());
    Ok(())
}

#[test]
fn inherit_graph_field_restores_provider_resolved_value() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let principal = principal("user-1");
    let release_id = insert_release(&mut db, "Release")?;
    db::providers::upsert(
        &mut db,
        &db::ProviderConfig {
            db_id: None,
            provider_id: "provider".to_string(),
            display_name: "Provider".to_string(),
            priority: 100,
            enabled: true,
        },
    )?;
    db::metadata::layers::upsert(
        &mut db,
        release_id,
        &db::MetadataLayer {
            db_id: None,
            provider_id: "provider".to_string(),
            fields: json!({"genres": ["Provider"]}).to_string(),
            updated_at: 10,
        },
    )?;
    crate::services::metadata::merging::apply_merged_metadata_to_entity(&mut db, release_id)?;

    let manual = preview(
        &db,
        &principal,
        release_id,
        &MetadataPreviewRequest {
            changes: vec![set(MetadataField::Genres, json!(["Manual"]))],
        },
    )?;
    apply_preview(&mut db, &principal, release_id, manual)?;

    let inherited = preview(
        &db,
        &principal,
        release_id,
        &MetadataPreviewRequest {
            changes: vec![inherit(MetadataField::Genres)],
        },
    )?;
    assert_eq!(inherited.diff[0].after, json!(["Provider"]));
    let result = apply_preview(&mut db, &principal, release_id, inherited)?;
    assert_eq!(result.fields["genres"], json!(["Provider"]));
    assert!(result.manual_fields.is_empty());
    Ok(())
}

#[test]
fn inherit_conflicts_when_provider_resolution_changes_after_preview() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let principal = principal("user-1");
    let release_id = insert_release(&mut db, "Release")?;
    db::providers::upsert(
        &mut db,
        &db::ProviderConfig {
            db_id: None,
            provider_id: "provider".to_string(),
            display_name: "Provider".to_string(),
            priority: 100,
            enabled: true,
        },
    )?;
    let provider_layer = |genres: &[&str], updated_at| db::MetadataLayer {
        db_id: None,
        provider_id: "provider".to_string(),
        fields: json!({"genres": genres}).to_string(),
        updated_at,
    };
    db::metadata::layers::upsert(&mut db, release_id, &provider_layer(&["First"], 10))?;
    crate::services::metadata::merging::apply_merged_metadata_to_entity(&mut db, release_id)?;
    let manual = preview(
        &db,
        &principal,
        release_id,
        &MetadataPreviewRequest {
            changes: vec![set(MetadataField::Genres, json!(["Manual"]))],
        },
    )?;
    apply_preview(&mut db, &principal, release_id, manual)?;

    let inherited = preview(
        &db,
        &principal,
        release_id,
        &MetadataPreviewRequest {
            changes: vec![inherit(MetadataField::Genres)],
        },
    )?;
    db::metadata::layers::upsert(&mut db, release_id, &provider_layer(&["Second"], 20))?;

    let error = apply_preview(&mut db, &principal, release_id, inherited)
        .expect_err("provider resolution changed after preview");
    let MetadataEditingError::Conflict(conflicts) = error else {
        panic!("expected a metadata conflict");
    };
    assert_eq!(conflicts[0].field, MetadataField::Genres);
    assert_eq!(conflicts[0].expected, json!(["First"]));
    assert_eq!(conflicts[0].current, json!(["Second"]));
    assert_eq!(
        db::genres::get_for_release(&db, release_id)?[0].name,
        "Manual"
    );
    assert!(db::metadata::manual_overrides::owns_field(
        &db,
        release_id,
        ManualMetadataField::Genres,
    )?);
    Ok(())
}

#[test]
fn label_edits_use_stable_label_identity() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let principal = principal("user-1");
    let release_id = insert_release(&mut db, "Release")?;
    let keeper_id = insert_release(&mut db, "Keeper")?;
    let first_label_id = db::labels::resolve(
        &mut db,
        &db::labels::ResolveLabel {
            name: "Shared Name",
            external_id: Some(db::labels::ResolveExternalId {
                provider_id: "provider",
                id_type: "label",
                id_value: "first",
            }),
        },
    )?;
    let second_label_id = db::labels::resolve(
        &mut db,
        &db::labels::ResolveLabel {
            name: "Shared Name",
            external_id: Some(db::labels::ResolveExternalId {
                provider_id: "provider",
                id_type: "label",
                id_value: "second",
            }),
        },
    )?;
    assert_ne!(first_label_id, second_label_id);
    db.transaction_mut(|transaction| {
        db::labels::sync_release_label_links_inside_tx(
            transaction,
            release_id,
            &[db::labels::LabelLinkInput {
                label_id: first_label_id,
                catalog_number: None,
            }],
        )
    })?;
    db.transaction_mut(|transaction| {
        db::labels::sync_release_label_links_inside_tx(
            transaction,
            keeper_id,
            &[db::labels::LabelLinkInput {
                label_id: second_label_id,
                catalog_number: None,
            }],
        )
    })?;
    let second = db::labels::get_by_id(&db, second_label_id)?.expect("label exists");

    let preview = preview(
        &db,
        &principal,
        release_id,
        &MetadataPreviewRequest {
            changes: vec![set(
                MetadataField::Labels,
                json!([{
                    "id": second.id,
                    "catalog_number": "CAT-2"
                }]),
            )],
        },
    )?;
    assert_eq!(preview.diff[0].after[0]["name"], json!("Shared Name"));
    apply_preview(&mut db, &principal, release_id, preview)?;

    let labels = db::labels::get_for_release(&db, release_id)?;
    assert_eq!(labels.len(), 1);
    assert_eq!(labels[0].label.id, second.id);
    assert_eq!(labels[0].catalog_number.as_deref(), Some("CAT-2"));
    Ok(())
}

#[test]
fn inherit_labels_restores_resolvable_provider_labels() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let principal = principal("user-1");
    let release_id = insert_release(&mut db, "Release")?;
    let keeper_id = insert_release(&mut db, "Keeper")?;
    let provider_label_id = db::labels::resolve(
        &mut db,
        &db::labels::ResolveLabel {
            name: "Provider Label",
            external_id: Some(db::labels::ResolveExternalId {
                provider_id: "provider",
                id_type: "label",
                id_value: "provider-label",
            }),
        },
    )?;
    let manual_label_id = db::labels::resolve(
        &mut db,
        &db::labels::ResolveLabel {
            name: "Manual Label",
            external_id: None,
        },
    )?;
    let provider_label =
        db::labels::get_by_id(&db, provider_label_id)?.expect("provider label exists");
    let manual_label = db::labels::get_by_id(&db, manual_label_id)?.expect("manual label exists");
    for owner in [release_id, keeper_id] {
        db.transaction_mut(|transaction| {
            db::labels::sync_release_label_links_inside_tx(
                transaction,
                owner,
                &[db::labels::LabelLinkInput {
                    label_id: provider_label_id,
                    catalog_number: Some("PROV-1".to_string()),
                }],
            )
        })?;
    }
    db.transaction_mut(|transaction| {
        db::labels::sync_release_label_links_inside_tx(
            transaction,
            keeper_id,
            &[
                db::labels::LabelLinkInput {
                    label_id: provider_label_id,
                    catalog_number: Some("PROV-1".to_string()),
                },
                db::labels::LabelLinkInput {
                    label_id: manual_label_id,
                    catalog_number: None,
                },
            ],
        )
    })?;
    db::providers::upsert(
        &mut db,
        &db::ProviderConfig {
            db_id: None,
            provider_id: "provider".to_string(),
            display_name: "Provider".to_string(),
            priority: 100,
            enabled: true,
        },
    )?;
    db::metadata::layers::upsert(
        &mut db,
        release_id,
        &db::MetadataLayer {
            db_id: None,
            provider_id: "provider".to_string(),
            fields: json!({
                "labels": [{
                    "name": "Provider Label",
                    "catalog_number": "PROV-1",
                    "external_id": {
                        "provider_id": "provider",
                        "id_type": "label",
                        "id_value": "provider-label"
                    }
                }]
            })
            .to_string(),
            updated_at: 10,
        },
    )?;

    let manual = preview(
        &db,
        &principal,
        release_id,
        &MetadataPreviewRequest {
            changes: vec![set(
                MetadataField::Labels,
                json!([{"id": manual_label.id, "catalog_number": null}]),
            )],
        },
    )?;
    apply_preview(&mut db, &principal, release_id, manual)?;

    let inherited = preview(
        &db,
        &principal,
        release_id,
        &MetadataPreviewRequest {
            changes: vec![inherit(MetadataField::Labels)],
        },
    )?;
    assert_eq!(inherited.diff[0].after[0]["id"], json!(provider_label.id));
    let result = apply_preview(&mut db, &principal, release_id, inherited)?;
    assert_eq!(result.fields["labels"][0]["id"], json!(provider_label.id));
    assert!(result.manual_fields.is_empty());
    Ok(())
}

#[test]
fn inherit_labels_does_not_fall_back_by_name_for_an_external_identity() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let principal = principal("user-1");
    let release_id = insert_release(&mut db, "Release")?;
    let local_label_id = db::labels::resolve(
        &mut db,
        &db::labels::ResolveLabel {
            name: "Shared Name",
            external_id: None,
        },
    )?;
    let local_label = db::labels::get_by_id(&db, local_label_id)?.expect("label exists");
    db.transaction_mut(|transaction| {
        db::labels::sync_release_label_links_inside_tx(
            transaction,
            release_id,
            &[db::labels::LabelLinkInput {
                label_id: local_label_id,
                catalog_number: None,
            }],
        )
    })?;
    db::providers::upsert(
        &mut db,
        &db::ProviderConfig {
            db_id: None,
            provider_id: "provider".to_string(),
            display_name: "Provider".to_string(),
            priority: 100,
            enabled: true,
        },
    )?;
    db::metadata::layers::upsert(
        &mut db,
        release_id,
        &db::MetadataLayer {
            db_id: None,
            provider_id: "provider".to_string(),
            fields: json!({
                "labels": [{
                    "name": "Shared Name",
                    "catalog_number": null,
                    "external_id": {
                        "provider_id": "provider",
                        "id_type": "label",
                        "id_value": "different-identity"
                    }
                }]
            })
            .to_string(),
            updated_at: 10,
        },
    )?;
    let manual = preview(
        &db,
        &principal,
        release_id,
        &MetadataPreviewRequest {
            changes: vec![set(
                MetadataField::Labels,
                json!([{"id": local_label.id, "catalog_number": null}]),
            )],
        },
    )?;
    apply_preview(&mut db, &principal, release_id, manual)?;

    let error = preview(
        &db,
        &principal,
        release_id,
        &MetadataPreviewRequest {
            changes: vec![inherit(MetadataField::Labels)],
        },
    )
    .expect_err("an external identity must not resolve by name");
    let MetadataEditingError::BadRequest(message) = error else {
        panic!("expected a bad request");
    };
    assert!(message.contains("is not present locally"));
    assert_eq!(db::labels::get_for_release(&db, release_id)?.len(), 1);
    assert!(db::metadata::manual_overrides::owns_field(
        &db,
        release_id,
        ManualMetadataField::Labels,
    )?);
    Ok(())
}

#[test]
fn unchanged_provider_refresh_rematerializes_a_manually_masked_label() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let principal = principal("user-1");
    let release_id = insert_release(&mut db, "Release")?;
    let keeper_id = insert_release(&mut db, "Keeper")?;
    db::providers::upsert(
        &mut db,
        &db::ProviderConfig {
            db_id: None,
            provider_id: "provider".to_string(),
            display_name: "Provider".to_string(),
            priority: 100,
            enabled: true,
        },
    )?;
    let fields = HashMap::from([(
        "labels".to_string(),
        json!([{
            "name": "Provider Label",
            "catalog_number": "PROV-1",
            "external_id": {
                "provider_id": "provider",
                "id_type": "label",
                "id_value": "provider-label"
            }
        }]),
    )]);
    crate::services::metadata::layers::save_provider_layer(
        &mut db,
        release_id,
        "provider",
        &fields,
        &HashMap::new(),
        &HashMap::new(),
        &HashSet::new(),
    )?;
    let provider_label_id =
        db::labels::find_by_external_id(&db, "provider", "label", "provider-label")?
            .expect("provider label was materialized and linked");
    let manual_label_id = db::labels::resolve(
        &mut db,
        &db::labels::ResolveLabel {
            name: "Manual Label",
            external_id: None,
        },
    )?;
    db.transaction_mut(|transaction| {
        db::labels::sync_release_label_links_inside_tx(
            transaction,
            keeper_id,
            &[db::labels::LabelLinkInput {
                label_id: manual_label_id,
                catalog_number: None,
            }],
        )
    })?;
    let manual_label = db::labels::get_by_id(&db, manual_label_id)?.expect("manual label exists");
    let manual = preview(
        &db,
        &principal,
        release_id,
        &MetadataPreviewRequest {
            changes: vec![set(
                MetadataField::Labels,
                json!([{"id": manual_label.id, "catalog_number": null}]),
            )],
        },
    )?;
    apply_preview(&mut db, &principal, release_id, manual)?;
    assert!(db::labels::get_by_id(&db, provider_label_id)?.is_none());

    crate::services::metadata::layers::save_provider_layer(
        &mut db,
        release_id,
        "provider",
        &fields,
        &HashMap::new(),
        &HashMap::new(),
        &HashSet::new(),
    )?;
    let rematerialized_id =
        db::labels::find_by_external_id(&db, "provider", "label", "provider-label")?
            .expect("unchanged refresh rematerializes the provider label");
    assert_ne!(rematerialized_id, provider_label_id);
    assert_eq!(db::labels::get_for_release(&db, release_id)?.len(), 1);
    assert_eq!(
        db::labels::get_for_release(&db, release_id)?[0].label.id,
        manual_label.id,
    );

    let inherited = preview(
        &db,
        &principal,
        release_id,
        &MetadataPreviewRequest {
            changes: vec![inherit(MetadataField::Labels)],
        },
    )?;
    apply_preview(&mut db, &principal, release_id, inherited)?;
    let labels = db::labels::get_for_release(&db, release_id)?;
    assert_eq!(labels.len(), 1);
    assert_eq!(
        labels[0].label.db_id.clone().map(DbId::from),
        Some(rematerialized_id),
    );
    Ok(())
}

#[test]
fn label_reference_change_after_preview_conflicts() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let principal = principal("user-1");
    let release_id = insert_release(&mut db, "Release")?;
    let keeper_id = insert_release(&mut db, "Keeper")?;
    let label_id = db::labels::resolve(
        &mut db,
        &db::labels::ResolveLabel {
            name: "Temporary",
            external_id: None,
        },
    )?;
    let label = db::labels::get_by_id(&db, label_id)?.expect("label exists");
    db.transaction_mut(|transaction| {
        db::labels::sync_release_label_links_inside_tx(
            transaction,
            keeper_id,
            &[db::labels::LabelLinkInput {
                label_id,
                catalog_number: None,
            }],
        )
    })?;
    let preview = preview(
        &db,
        &principal,
        release_id,
        &MetadataPreviewRequest {
            changes: vec![set(
                MetadataField::Labels,
                json!([{"id": label.id, "catalog_number": null}]),
            )],
        },
    )?;
    db.exec_mut(QueryBuilder::remove().ids(label_id).query())?;

    let error = apply_preview(&mut db, &principal, release_id, preview)
        .expect_err("a disappeared label must conflict");
    assert!(matches!(error, MetadataEditingError::Conflict(_)));
    assert!(db::metadata::manual_overrides::get(&db, release_id)?.is_none());
    Ok(())
}

#[test]
fn inherited_label_disappearance_after_preview_conflicts() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let principal = principal("user-1");
    let release_id = insert_release(&mut db, "Release")?;
    let keeper_id = insert_release(&mut db, "Keeper")?;
    let provider_label_id = db::labels::resolve(
        &mut db,
        &db::labels::ResolveLabel {
            name: "Provider Label",
            external_id: Some(db::labels::ResolveExternalId {
                provider_id: "provider",
                id_type: "label",
                id_value: "provider-label",
            }),
        },
    )?;
    let manual_label_id = db::labels::resolve(
        &mut db,
        &db::labels::ResolveLabel {
            name: "Manual Label",
            external_id: None,
        },
    )?;
    for owner in [release_id, keeper_id] {
        db.transaction_mut(|transaction| {
            db::labels::sync_release_label_links_inside_tx(
                transaction,
                owner,
                &[db::labels::LabelLinkInput {
                    label_id: provider_label_id,
                    catalog_number: None,
                }],
            )
        })?;
    }
    db.transaction_mut(|transaction| {
        db::labels::sync_release_label_links_inside_tx(
            transaction,
            keeper_id,
            &[
                db::labels::LabelLinkInput {
                    label_id: provider_label_id,
                    catalog_number: None,
                },
                db::labels::LabelLinkInput {
                    label_id: manual_label_id,
                    catalog_number: None,
                },
            ],
        )
    })?;
    db::providers::upsert(
        &mut db,
        &db::ProviderConfig {
            db_id: None,
            provider_id: "provider".to_string(),
            display_name: "Provider".to_string(),
            priority: 100,
            enabled: true,
        },
    )?;
    db::metadata::layers::upsert(
        &mut db,
        release_id,
        &db::MetadataLayer {
            db_id: None,
            provider_id: "provider".to_string(),
            fields: json!({
                "labels": [{
                    "name": "Provider Label",
                    "catalog_number": null,
                    "external_id": {
                        "provider_id": "provider",
                        "id_type": "label",
                        "id_value": "provider-label"
                    }
                }]
            })
            .to_string(),
            updated_at: 10,
        },
    )?;
    let manual_label = db::labels::get_by_id(&db, manual_label_id)?.expect("manual label exists");
    let manual = preview(
        &db,
        &principal,
        release_id,
        &MetadataPreviewRequest {
            changes: vec![set(
                MetadataField::Labels,
                json!([{"id": manual_label.id, "catalog_number": null}]),
            )],
        },
    )?;
    apply_preview(&mut db, &principal, release_id, manual)?;
    let inherited = preview(
        &db,
        &principal,
        release_id,
        &MetadataPreviewRequest {
            changes: vec![inherit(MetadataField::Labels)],
        },
    )?;
    db.exec_mut(QueryBuilder::remove().ids(provider_label_id).query())?;

    let error = apply_preview(&mut db, &principal, release_id, inherited)
        .expect_err("an inherited label disappearing after preview must conflict");
    let MetadataEditingError::Conflict(conflicts) = error else {
        panic!("expected a metadata conflict");
    };
    assert_eq!(conflicts[0].field, MetadataField::Labels);
    assert_eq!(conflicts[0].current, Value::Null);
    assert_eq!(conflicts[0].expected_source, MetadataValueSource::Resolved);
    assert_eq!(conflicts[0].current_source, MetadataValueSource::Resolved);
    assert!(db::metadata::manual_overrides::owns_field(
        &db,
        release_id,
        ManualMetadataField::Labels,
    )?);
    Ok(())
}

#[test]
fn credit_reference_disappearance_after_preview_conflicts() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let principal = principal("user-1");
    let release_id = insert_release(&mut db, "Release")?;
    let artist_id = insert_artist(&mut db, "Temporary Artist")?;
    let artist = db::artists::get_by_id(&db, artist_id)?.expect("artist exists");
    let preview = preview(
        &db,
        &principal,
        release_id,
        &MetadataPreviewRequest {
            changes: vec![set(
                MetadataField::Credits,
                json!([{"artist_id": artist.id, "type": "artist", "detail": null}]),
            )],
        },
    )?;
    db.exec_mut(QueryBuilder::remove().ids(artist_id).query())?;

    let error = apply_preview(&mut db, &principal, release_id, preview)
        .expect_err("an artist disappearing after preview must conflict");
    let MetadataEditingError::Conflict(conflicts) = error else {
        panic!("expected a metadata conflict");
    };
    assert_eq!(conflicts[0].field, MetadataField::Credits);
    assert_eq!(conflicts[0].current, Value::Null);
    assert_eq!(conflicts[0].expected_source, MetadataValueSource::Manual);
    assert_eq!(conflicts[0].current_source, MetadataValueSource::Manual);
    assert!(db::metadata::manual_overrides::get(&db, release_id)?.is_none());
    Ok(())
}

#[test]
fn graph_edits_store_only_ownership_markers() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let principal = principal("user-1");
    let release_id = insert_release(&mut db, "Release")?;
    let artist_id = insert_artist(&mut db, "Artist")?;
    let artist = db::artists::get_by_id(&db, artist_id)?.expect("artist exists");

    let preview = preview(
        &db,
        &principal,
        release_id,
        &MetadataPreviewRequest {
            changes: vec![set(
                MetadataField::Credits,
                json!([{"artist_id": artist.id, "type": "artist", "detail": null}]),
            )],
        },
    )?;
    apply_preview(&mut db, &principal, release_id, preview)?;

    assert_eq!(credit_values(&db, release_id)?.len(), 1);
    let fields = db::metadata::manual_overrides::get(&db, release_id)?
        .expect("manual override exists")
        .parsed_fields()?;
    assert_eq!(fields[&ManualMetadataField::Credits], Value::Bool(true));
    Ok(())
}

#[test]
fn inherit_graph_field_without_a_provider_value_clears_manual_edges() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let principal = principal("user-1");
    let release_id = insert_release(&mut db, "Release")?;
    let artist_id = insert_artist(&mut db, "Artist")?;
    let artist = db::artists::get_by_id(&db, artist_id)?.expect("artist exists");
    let manual = preview(
        &db,
        &principal,
        release_id,
        &MetadataPreviewRequest {
            changes: vec![set(
                MetadataField::Credits,
                json!([{"artist_id": artist.id, "type": "artist", "detail": null}]),
            )],
        },
    )?;
    apply_preview(&mut db, &principal, release_id, manual)?;

    let inherited = preview(
        &db,
        &principal,
        release_id,
        &MetadataPreviewRequest {
            changes: vec![inherit(MetadataField::Credits)],
        },
    )?;
    assert_eq!(inherited.diff[0].after, json!([]));
    assert_eq!(
        inherited.diff[0].source_after,
        MetadataValueSource::Resolved
    );
    let result = apply_preview(&mut db, &principal, release_id, inherited)?;

    assert!(credit_values(&db, release_id)?.is_empty());
    assert!(result.manual_fields.is_empty());
    Ok(())
}

#[test]
fn referenced_artists_must_be_accessible_to_the_principal() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let visible_library_id = insert_library(&mut db, "Visible", "/tmp/visible")?;
    let hidden_library_id = insert_library(&mut db, "Hidden", "/tmp/hidden")?;
    let release_id = insert_release(&mut db, "Release")?;
    let hidden_release_id = insert_release(&mut db, "Hidden Release")?;
    let hidden_artist_id = insert_artist(&mut db, "Hidden Artist")?;
    db::graph::ensure_owned_edge(&mut db, visible_library_id, release_id)?;
    db::graph::ensure_owned_edge(&mut db, hidden_library_id, hidden_release_id)?;
    connect_artist(&mut db, hidden_release_id, hidden_artist_id)?;

    let visible_library =
        db::libraries::get_by_id(&db, visible_library_id)?.expect("visible library exists");
    let hidden_library =
        db::libraries::get_by_id(&db, hidden_library_id)?.expect("hidden library exists");
    let mut scoped = principal("scoped");
    scoped.permissions.clear();
    scoped.accessible_library_ids = HashSet::from([visible_library.id]);
    let hidden_artist =
        db::artists::get_by_id(&db, hidden_artist_id)?.expect("hidden artist exists");

    let error = preview(
        &db,
        &scoped,
        release_id,
        &MetadataPreviewRequest {
            changes: vec![set(
                MetadataField::Credits,
                json!([{
                    "artist_id": hidden_artist.id,
                    "type": "artist",
                    "detail": null
                }]),
            )],
        },
    )
    .expect_err("hidden artist reference must be rejected");
    assert!(matches!(error, MetadataEditingError::BadRequest(_)));

    scoped
        .accessible_library_ids
        .insert(hidden_library.id.clone());
    let preview = preview(
        &db,
        &scoped,
        release_id,
        &MetadataPreviewRequest {
            changes: vec![set(
                MetadataField::Credits,
                json!([{
                    "artist_id": hidden_artist.id,
                    "type": "artist",
                    "detail": null
                }]),
            )],
        },
    )?;
    scoped.accessible_library_ids.remove(&hidden_library.id);
    apply_preview(&mut db, &scoped, release_id, preview)
        .expect_err("artist access must be revalidated when the preview is applied");
    Ok(())
}

#[test]
fn hidden_relation_targets_are_neither_exposed_nor_replaceable() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let visible_library_id = insert_library(&mut db, "Visible", "/tmp/relation-visible")?;
    let hidden_library_id = insert_library(&mut db, "Hidden", "/tmp/relation-hidden")?;
    let source_id = insert_artist(&mut db, "Visible Source")?;
    let target_id = insert_artist(&mut db, "Hidden Target")?;
    db::graph::ensure_owned_edge(&mut db, visible_library_id, source_id)?;
    db::graph::ensure_owned_edge(&mut db, hidden_library_id, target_id)?;
    db::artists::relations::link(
        &mut db,
        source_id,
        target_id,
        db::ArtistRelationType::MemberOf,
        Some("hidden detail".to_string()),
    )?;
    let visible_library =
        db::libraries::get_by_id(&db, visible_library_id)?.expect("visible library exists");
    let mut scoped = principal("scoped");
    scoped.permissions = vec![Permission::ManageMetadata];
    scoped.accessible_library_ids = HashSet::from([visible_library.id]);

    let snapshot = get_snapshot(&db, &scoped, source_id)?;
    assert!(!snapshot.fields.contains_key("relations"));
    let error = preview(
        &db,
        &scoped,
        source_id,
        &MetadataPreviewRequest {
            changes: vec![set(MetadataField::Relations, json!([]))],
        },
    )
    .expect_err("relations with hidden targets must not be replaceable");
    assert!(matches!(error, MetadataEditingError::BadRequest(_)));
    assert_eq!(
        db::artists::relations::get_relations_from(&db, source_id, None)?.len(),
        1,
    );
    Ok(())
}

#[test]
fn hidden_relation_added_after_preview_conflicts_without_replacement() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let visible_library_id = insert_library(&mut db, "Visible", "/tmp/relation-race-visible")?;
    let hidden_library_id = insert_library(&mut db, "Hidden", "/tmp/relation-race-hidden")?;
    let visible_release_id = insert_release(&mut db, "Visible Release")?;
    let hidden_release_id = insert_release(&mut db, "Hidden Release")?;
    let source_id = insert_artist(&mut db, "Visible Source")?;
    let visible_target_id = insert_artist(&mut db, "Visible Target")?;
    let hidden_target_id = insert_artist(&mut db, "Hidden Target")?;
    db::graph::ensure_owned_edge(&mut db, visible_library_id, visible_release_id)?;
    db::graph::ensure_owned_edge(&mut db, hidden_library_id, hidden_release_id)?;
    connect_artist(&mut db, visible_release_id, source_id)?;
    connect_artist(&mut db, visible_release_id, visible_target_id)?;
    connect_artist(&mut db, hidden_release_id, hidden_target_id)?;
    let visible_library =
        db::libraries::get_by_id(&db, visible_library_id)?.expect("visible library exists");
    let visible_target =
        db::artists::get_by_id(&db, visible_target_id)?.expect("visible target exists");
    let mut scoped = principal("scoped");
    scoped.permissions = vec![Permission::ManageMetadata];
    scoped.accessible_library_ids = HashSet::from([visible_library.id]);
    let preview = preview(
        &db,
        &scoped,
        source_id,
        &MetadataPreviewRequest {
            changes: vec![set(
                MetadataField::Relations,
                json!([{
                    "target_artist_id": visible_target.id,
                    "type": "member_of",
                    "attributes": null
                }]),
            )],
        },
    )?;
    db::artists::relations::link(
        &mut db,
        source_id,
        hidden_target_id,
        db::ArtistRelationType::MemberOf,
        Some("hidden detail".to_string()),
    )?;

    let error = apply_preview(&mut db, &scoped, source_id, preview)
        .expect_err("a newly hidden relation must conflict");
    let MetadataEditingError::Conflict(conflicts) = error else {
        panic!("expected a metadata conflict");
    };
    assert_eq!(conflicts[0].field, MetadataField::Relations);
    assert_eq!(conflicts[0].current, Value::Null);
    let relations = db::artists::relations::get_relations_from(&db, source_id, None)?;
    assert_eq!(relations.len(), 1);
    assert_eq!(relations[0].1, hidden_target_id);
    Ok(())
}

#[test]
fn hidden_label_ids_are_rejected_without_disclosing_their_names() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let visible_library_id = insert_library(&mut db, "Visible", "/tmp/label-visible")?;
    let hidden_library_id = insert_library(&mut db, "Hidden", "/tmp/label-hidden")?;
    let visible_release_id = insert_release(&mut db, "Visible Release")?;
    let hidden_release_id = insert_release(&mut db, "Hidden Release")?;
    db::graph::ensure_owned_edge(&mut db, visible_library_id, visible_release_id)?;
    db::graph::ensure_owned_edge(&mut db, hidden_library_id, hidden_release_id)?;
    let label_id = db::labels::resolve(
        &mut db,
        &db::labels::ResolveLabel {
            name: "Secret Label Name",
            external_id: None,
        },
    )?;
    db.transaction_mut(|transaction| {
        db::labels::sync_release_label_links_inside_tx(
            transaction,
            hidden_release_id,
            &[db::labels::LabelLinkInput {
                label_id,
                catalog_number: None,
            }],
        )
    })?;
    let label = db::labels::get_by_id(&db, label_id)?.expect("label exists");
    let visible_library =
        db::libraries::get_by_id(&db, visible_library_id)?.expect("visible library exists");
    let mut scoped = principal("scoped");
    scoped.permissions = vec![Permission::ManageMetadata];
    scoped.accessible_library_ids = HashSet::from([visible_library.id]);

    let error = preview(
        &db,
        &scoped,
        visible_release_id,
        &MetadataPreviewRequest {
            changes: vec![set(
                MetadataField::Labels,
                json!([{"id": label.id, "catalog_number": null}]),
            )],
        },
    )
    .expect_err("hidden label must not be accepted");
    let MetadataEditingError::BadRequest(message) = error else {
        panic!("expected bad request");
    };
    assert!(!message.contains("Secret Label Name"));
    Ok(())
}

#[test]
fn label_access_loss_after_preview_conflicts() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let target_library_id = insert_library(&mut db, "Target", "/tmp/label-race-target")?;
    let source_library_id = insert_library(&mut db, "Source", "/tmp/label-race-source")?;
    let target_release_id = insert_release(&mut db, "Target Release")?;
    let source_release_id = insert_release(&mut db, "Source Release")?;
    db::graph::ensure_owned_edge(&mut db, target_library_id, target_release_id)?;
    db::graph::ensure_owned_edge(&mut db, source_library_id, source_release_id)?;
    let label_id = db::labels::resolve(
        &mut db,
        &db::labels::ResolveLabel {
            name: "Scoped Label",
            external_id: None,
        },
    )?;
    db.transaction_mut(|transaction| {
        db::labels::sync_release_label_links_inside_tx(
            transaction,
            source_release_id,
            &[db::labels::LabelLinkInput {
                label_id,
                catalog_number: None,
            }],
        )
    })?;
    let label = db::labels::get_by_id(&db, label_id)?.expect("label exists");
    let target_library =
        db::libraries::get_by_id(&db, target_library_id)?.expect("target library exists");
    let source_library =
        db::libraries::get_by_id(&db, source_library_id)?.expect("source library exists");
    let mut scoped = principal("scoped");
    scoped.permissions = vec![Permission::ManageMetadata];
    scoped.accessible_library_ids =
        HashSet::from([target_library.id.clone(), source_library.id.clone()]);
    let preview = preview(
        &db,
        &scoped,
        target_release_id,
        &MetadataPreviewRequest {
            changes: vec![set(
                MetadataField::Labels,
                json!([{"id": label.id, "catalog_number": null}]),
            )],
        },
    )?;
    scoped.accessible_library_ids.remove(&source_library.id);

    let error = apply_preview(&mut db, &scoped, target_release_id, preview)
        .expect_err("label access must be revalidated at apply");
    assert!(matches!(error, MetadataEditingError::Conflict(_)));
    assert!(db::labels::get_for_release(&db, target_release_id)?.is_empty());
    Ok(())
}
