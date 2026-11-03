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
use serde_json::json;

use super::json::inputs_from_json;
use super::*;
use lyra_db::fixtures::{
    insert_artist,
    insert_release,
};
use lyra_db::test_db::new_test_db;

fn enable(db: &mut DbAny, providers: &[&str]) -> anyhow::Result<()> {
    for (position, provider_id) in providers.iter().enumerate() {
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
    Ok(())
}

fn fields(value: serde_json::Value) -> ProviderValues {
    ProviderValues {
        fields: inputs_from_json(value),
        ..Default::default()
    }
}

fn credits(roles: &[(CreditType, &[DbId])]) -> ProviderValues {
    ProviderValues {
        credits: roles
            .iter()
            .map(|(role, artist_ids)| {
                (
                    *role,
                    artist_ids
                        .iter()
                        .map(|&artist_id| LayerCreditInput {
                            artist_id,
                            detail: None,
                        })
                        .collect(),
                )
            })
            .collect(),
        ..Default::default()
    }
}

fn relations(types: &[(ArtistRelationType, &[DbId])]) -> ProviderValues {
    ProviderValues {
        relations: types
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
            .collect(),
        ..Default::default()
    }
}

fn save(db: &mut DbAny, node_id: DbId, provider_id: &str, values: &ProviderValues) {
    save_provider_layer(
        db,
        node_id,
        provider_id,
        values,
        &HashMap::new(),
        &Default::default(),
    )
    .expect("layer saves");
}

fn replace(db: &mut DbAny, node_id: DbId, provider_id: &str, values: &ProviderValues) {
    replace_provider_layer(
        db,
        node_id,
        provider_id,
        values,
        &HashMap::new(),
        &Default::default(),
    )
    .expect("layer replaces");
}

fn release(db: &DbAny, release_id: DbId) -> lyra_db::Release {
    lyra_db::releases::get_by_id(db, release_id)
        .expect("release reads")
        .expect("release exists")
}

fn credited(db: &DbAny, owner: DbId, role: CreditType) -> anyhow::Result<Vec<DbId>> {
    let mut links = lyra_db::credits::links_for_owner(db, owner)?;
    links.sort_by_key(|link| link.credit.artist_order);
    Ok(links
        .into_iter()
        .filter(|link| link.credit.credit_type == role)
        .map(|link| link.artist_id)
        .collect())
}

fn related_into(
    db: &DbAny,
    artist_id: DbId,
    relation_type: ArtistRelationType,
) -> anyhow::Result<Vec<DbId>> {
    Ok(
        lyra_db::artists::relations::get_relations_to(db, artist_id, Some(relation_type))?
            .into_iter()
            .map(|(_, source_id)| source_id)
            .collect(),
    )
}

fn source_ids(db: &DbAny, node_id: DbId) -> anyhow::Result<Vec<String>> {
    let mut ids: Vec<String> = lyra_db::metadata::layers::get_for_entity(db, node_id)?
        .into_iter()
        .map(|layer| layer.source_id)
        .collect();
    ids.sort();
    Ok(ids)
}

fn exists(db: &DbAny, id: DbId) -> bool {
    db.exec(QueryBuilder::select().ids(id).query()).is_ok()
}

#[test]
fn a_replacing_save_leaves_a_manual_value_in_place() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let release_id = insert_release(&mut db, "Release")?;
    enable(&mut db, &["provider"])?;
    replace(
        &mut db,
        release_id,
        "provider",
        &fields(json!({"description": "Provider", "release_date": "2001"})),
    );
    lyra_db::metadata::layers::upsert(
        &mut db,
        release_id,
        &MetadataLayer::new(
            MANUAL_SOURCE_ID,
            json::fields_from_json(json!({"description": "Manual"})),
            1,
        ),
    )?;
    crate::services::metadata::merging::apply_merged_metadata_to_entity(&mut db, release_id)?;
    assert_eq!(
        release(&db, release_id).description.as_deref(),
        Some("Manual")
    );

    replace(
        &mut db,
        release_id,
        "provider",
        &fields(json!({"description": "Newer"})),
    );
    assert_eq!(
        release(&db, release_id).description.as_deref(),
        Some("Manual")
    );

    replace(&mut db, release_id, "provider", &fields(json!({})));
    let stored = release(&db, release_id);
    assert_eq!(stored.description.as_deref(), Some("Manual"));
    assert_eq!(
        stored.release_date, None,
        "the unowned field still falls away"
    );
    assert_eq!(source_ids(&db, release_id)?, vec![MANUAL_SOURCE_ID]);
    Ok(())
}

#[test]
fn a_replacing_save_leaves_a_locked_release_alone_until_unlocked() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let release_id = insert_release(&mut db, "Release")?;
    enable(&mut db, &["provider"])?;
    replace(
        &mut db,
        release_id,
        "provider",
        &fields(json!({"description": "Provider", "release_date": "2001"})),
    );
    let mut locked = release(&db, release_id);
    locked.locked = Some(true);
    lyra_db::releases::update(&mut db, &locked)?;

    replace(
        &mut db,
        release_id,
        "provider",
        &fields(json!({"release_date": "1999"})),
    );
    replace(&mut db, release_id, "provider", &fields(json!({})));
    let stored = release(&db, release_id);
    assert_eq!(stored.description.as_deref(), Some("Provider"));
    assert_eq!(stored.release_date.as_deref(), Some("2001"));
    assert!(
        source_ids(&db, release_id)?.is_empty(),
        "the layer goes even while the lock holds its values"
    );

    let mut unlocked = release(&db, release_id);
    unlocked.locked = None;
    lyra_db::releases::update(&mut db, &unlocked)?;
    crate::services::metadata::merging::apply_merged_metadata_to_entity(&mut db, release_id)?;
    let stored = release(&db, release_id);
    assert_eq!(stored.description, None);
    assert_eq!(stored.release_date, None);
    Ok(())
}

#[test]
fn credit_roles_a_replacing_save_leaves_out_fall_through() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let release_id = insert_release(&mut db, "Release")?;
    let [low_artist, low_composer, high_artist, high_composer] = [
        insert_artist(&mut db, "Low Artist")?,
        insert_artist(&mut db, "Low Composer")?,
        insert_artist(&mut db, "High Artist")?,
        insert_artist(&mut db, "High Composer")?,
    ];
    enable(&mut db, &["high", "low"])?;
    save(
        &mut db,
        release_id,
        "low",
        &credits(&[
            (CreditType::Artist, &[low_artist]),
            (CreditType::Composer, &[low_composer]),
        ]),
    );
    replace(
        &mut db,
        release_id,
        "high",
        &credits(&[
            (CreditType::Artist, &[high_artist]),
            (CreditType::Composer, &[high_composer]),
        ]),
    );
    assert_eq!(
        credited(&db, release_id, CreditType::Composer)?,
        vec![high_composer]
    );

    replace(
        &mut db,
        release_id,
        "high",
        &credits(&[(CreditType::Artist, &[high_artist])]),
    );
    assert_eq!(
        credited(&db, release_id, CreditType::Artist)?,
        vec![high_artist]
    );
    assert_eq!(
        credited(&db, release_id, CreditType::Composer)?,
        vec![low_composer]
    );

    replace(&mut db, release_id, "high", &ProviderValues::default());
    assert_eq!(
        credited(&db, release_id, CreditType::Artist)?,
        vec![low_artist]
    );
    for artist_id in [low_artist, low_composer, high_artist, high_composer] {
        assert!(
            lyra_db::artists::get_by_id(&db, artist_id)?.is_some(),
            "an artist outlives the credits it lost"
        );
    }
    Ok(())
}

#[test]
fn relation_types_a_replacing_save_leaves_out_fall_through() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let character = insert_artist(&mut db, "Character")?;
    let [low_actor, low_member, high_actor] = [
        insert_artist(&mut db, "Low Actor")?,
        insert_artist(&mut db, "Low Member")?,
        insert_artist(&mut db, "High Actor")?,
    ];
    enable(&mut db, &["high", "low"])?;
    save(
        &mut db,
        character,
        "low",
        &relations(&[
            (ArtistRelationType::VoiceActor, &[low_actor]),
            (ArtistRelationType::MemberOf, &[low_member]),
        ]),
    );
    replace(
        &mut db,
        character,
        "high",
        &relations(&[
            (ArtistRelationType::VoiceActor, &[high_actor]),
            (ArtistRelationType::MemberOf, &[]),
        ]),
    );
    assert_eq!(
        related_into(&db, character, ArtistRelationType::VoiceActor)?,
        vec![high_actor]
    );
    assert!(related_into(&db, character, ArtistRelationType::MemberOf)?.is_empty());

    replace(
        &mut db,
        character,
        "high",
        &relations(&[(ArtistRelationType::VoiceActor, &[high_actor])]),
    );
    assert_eq!(
        related_into(&db, character, ArtistRelationType::MemberOf)?,
        vec![low_member]
    );

    replace(&mut db, character, "high", &ProviderValues::default());
    assert_eq!(
        related_into(&db, character, ArtistRelationType::VoiceActor)?,
        vec![low_actor]
    );
    for artist_id in [character, low_actor, low_member, high_actor] {
        assert!(lyra_db::artists::get_by_id(&db, artist_id)?.is_some());
    }
    Ok(())
}

#[test]
fn removing_the_only_provider_layer_restores_the_scanned_values() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    let release_id = insert_release(&mut db, "Tagged")?;
    let mut local = LocalLayer::default();
    local.supply(MetadataField::ReleaseTitle, Some("Tagged"));
    local.supply(MetadataField::ReleaseDate, Some("1990"));
    db.transaction_mut(|t| local.save(t, release_id))?;
    enable(&mut db, &["provider"])?;

    replace(
        &mut db,
        release_id,
        "provider",
        &fields(json!({
            "release_title": "Provider",
            "release_date": "2001",
            "description": "Provider",
            "genres": ["Provider Genre"],
        })),
    );
    let stored = release(&db, release_id);
    assert_eq!(stored.release_title, "Provider");
    let before = lyra_db::metadata::layers::ids_for_entity(&db, release_id)?;
    assert_eq!(before.len(), 2);

    replace(&mut db, release_id, "provider", &ProviderValues::default());
    let stored = release(&db, release_id);
    assert_eq!(stored.release_title, "Tagged");
    assert_eq!(stored.release_date.as_deref(), Some("1990"));
    assert_eq!(stored.description, None);
    assert!(lyra_db::genres::get_for_release(&db, release_id)?.is_empty());
    assert_eq!(source_ids(&db, release_id)?, vec![LOCAL_SOURCE_ID]);
    let after = lyra_db::metadata::layers::ids_for_entity(&db, release_id)?;
    for removed in before.iter().filter(|id| !after.contains(id)) {
        assert!(!exists(&db, *removed), "the removed layer leaves no node");
    }
    assert_eq!(after.len(), 1);
    Ok(())
}

#[test]
fn a_replacing_save_keeps_ids_and_custom_fields_as_an_ordinary_save_does() -> anyhow::Result<()> {
    let mut db = new_test_db()?;
    lyra_db::providers::register(&mut db, "provider")?;
    enable(&mut db, &["provider"])?;
    let ids = HashMap::from([("album_id".to_string(), "a-1".to_string())]);
    let custom = CustomFieldChanges {
        set: HashMap::from([(
            1,
            HashMap::from([("description_language".to_string(), json!("fra"))]),
        )]),
        ..Default::default()
    };

    let mut outcomes = Vec::new();
    for replacing in [false, true] {
        let release_id = insert_release(&mut db, "Release")?;
        let write = if replacing {
            replace_provider_layer
        } else {
            save_provider_layer
        };
        write(
            &mut db,
            release_id,
            "provider",
            &fields(json!({"description": "Text"})),
            &ids,
            &custom,
        )?;
        write(
            &mut db,
            release_id,
            "provider",
            &ProviderValues::default(),
            &HashMap::new(),
            &Default::default(),
        )?;
        let id = lyra_db::external_ids::get(&db, release_id, "provider", "album_id")?
            .and_then(|id| id.resolved_value().map(str::to_string));
        let custom_fields: BTreeMap<u64, String> =
            lyra_db::metadata::custom_fields::get_for_entity(&db, release_id)?
                .into_iter()
                .map(|row| (row.version, row.fields))
                .collect();
        outcomes.push((id, custom_fields));
    }
    assert_eq!(outcomes[0], outcomes[1]);
    assert_eq!(outcomes[1].0.as_deref(), Some("a-1"));
    assert_eq!(outcomes[1].1.len(), 1);
    Ok(())
}
