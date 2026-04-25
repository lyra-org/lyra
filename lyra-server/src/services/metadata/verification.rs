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
};

use crate::db;

fn linked_entity_count(db: &DbAny, artist_db_id: DbId) -> anyhow::Result<usize> {
    let release_count = db::releases::get_by_artist(db, artist_db_id)?.len();
    let track_count = db::tracks::get_by_artist(db, artist_db_id)?.len();
    Ok(release_count + track_count)
}

pub(crate) fn recompute_artist_verified(
    db: &mut DbAny,
    artist_db_id: DbId,
) -> anyhow::Result<bool> {
    let Some(mut artist) = db::artists::get_by_id(db, artist_db_id)? else {
        return Ok(false);
    };

    let external_ids = db::external_ids::get_for_entity(db, artist_db_id)?;
    let mut provider_to_values: HashMap<String, HashSet<String>> = HashMap::new();
    let mut has_user_artist_id = false;

    for external_id in external_ids {
        if external_id.id_type != "artist_db_id" {
            continue;
        }

        let value = external_id.id_value.trim();
        if value.is_empty() {
            continue;
        }

        provider_to_values
            .entry(external_id.provider_id.clone())
            .or_default()
            .insert(value.to_string());

        if external_id.source == db::IdSource::User {
            has_user_artist_id = true;
        }
    }

    let has_any_artist_id = !provider_to_values.is_empty();
    let has_provider_conflict = provider_to_values.values().any(|values| values.len() > 1);

    let verified = if !has_any_artist_id {
        tracing::debug!(
            artist_db_id = artist_db_id.0,
            "artist verification: no artist_db_id values"
        );
        false
    } else if has_provider_conflict {
        tracing::warn!(
            artist_db_id = artist_db_id.0,
            "artist verification: conflicting artist_db_id values per provider"
        );
        false
    } else if has_user_artist_id {
        tracing::debug!(
            artist_db_id = artist_db_id.0,
            "artist verification: user-sourced artist_db_id"
        );
        true
    } else {
        let link_count = linked_entity_count(db, artist_db_id)?;
        let meets_threshold = link_count >= 2;
        tracing::debug!(
            artist_db_id = artist_db_id.0,
            link_count,
            verified = meets_threshold,
            "artist verification: plugin-sourced artist_db_id threshold"
        );
        meets_threshold
    };

    if artist.verified != verified {
        artist.verified = verified;
        db::artists::update(db, &artist)?;
    }

    Ok(verified)
}

#[cfg(test)]
mod tests {
    use agdb::QueryBuilder;

    use super::*;
    use crate::db::test_db::{
        connect,
        connect_artist,
        insert_artist,
        insert_release,
        insert_track,
        new_test_db,
    };
    use crate::db::{
        self,
        IdSource,
    };
    use nanoid::nanoid;

    fn artist_verified(db: &DbAny, artist_db_id: DbId) -> anyhow::Result<bool> {
        let artist = db::artists::get_by_id(db, artist_db_id)?
            .ok_or_else(|| anyhow::anyhow!("artist missing after verification"))?;
        Ok(artist.verified)
    }

    #[test]
    fn no_artist_id_keeps_artist_unverified() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let artist_db_id = insert_artist(&mut db, "No ID Artist")?;

        let verified = recompute_artist_verified(&mut db, artist_db_id)?;
        assert!(!verified);
        assert!(!artist_verified(&db, artist_db_id)?);

        Ok(())
    }

    #[test]
    fn blank_artist_id_is_ignored() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let artist_db_id = insert_artist(&mut db, "Blank ID Artist")?;

        db::external_ids::upsert(
            &mut db,
            artist_db_id,
            "musicbrainz",
            "artist_db_id",
            "   ",
            IdSource::Plugin,
        )?;

        let verified = recompute_artist_verified(&mut db, artist_db_id)?;
        assert!(!verified);
        assert!(!artist_verified(&db, artist_db_id)?);

        Ok(())
    }

    #[test]
    fn user_artist_id_verifies_immediately() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let artist_db_id = insert_artist(&mut db, "User Verified Artist")?;

        db::external_ids::upsert(
            &mut db,
            artist_db_id,
            "musicbrainz",
            "artist_db_id",
            "user-artist-id",
            IdSource::User,
        )?;

        let verified = recompute_artist_verified(&mut db, artist_db_id)?;
        assert!(verified);
        assert!(artist_verified(&db, artist_db_id)?);

        Ok(())
    }

    #[test]
    fn plugin_artist_id_requires_link_threshold() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let artist_db_id = insert_artist(&mut db, "Plugin Threshold Artist")?;

        db::external_ids::upsert(
            &mut db,
            artist_db_id,
            "musicbrainz",
            "artist_db_id",
            "plugin-artist-id",
            IdSource::Plugin,
        )?;

        let verified_before_links = recompute_artist_verified(&mut db, artist_db_id)?;
        assert!(!verified_before_links);

        let release_db_id = insert_release(&mut db, "Linked Album")?;
        connect_artist(&mut db, release_db_id, artist_db_id)?;
        let track_db_id = insert_track(&mut db, "Linked Track")?;
        connect_artist(&mut db, track_db_id, artist_db_id)?;

        let verified_after_links = recompute_artist_verified(&mut db, artist_db_id)?;
        assert!(verified_after_links);
        assert!(artist_verified(&db, artist_db_id)?);

        Ok(())
    }

    #[test]
    fn conflicting_artist_ids_for_same_provider_do_not_verify() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let artist_db_id = insert_artist(&mut db, "Conflict Artist")?;

        let first = db::external_ids::ExternalId {
            db_id: None,
            id: nanoid!(),
            provider_id: "musicbrainz".to_string(),
            id_type: "artist_db_id".to_string(),
            id_value: "artist-a".to_string(),
            source: IdSource::Plugin,
        };
        let first_qr = db.exec_mut(QueryBuilder::insert().element(&first).query())?;
        let first_id = first_qr
            .elements
            .first()
            .map(|element| element.id)
            .ok_or_else(|| anyhow::anyhow!("first conflicting id insert missing id"))?;
        connect(&mut db, artist_db_id, first_id)?;

        let second = db::external_ids::ExternalId {
            db_id: None,
            id: nanoid!(),
            provider_id: "musicbrainz".to_string(),
            id_type: "artist_db_id".to_string(),
            id_value: "artist-b".to_string(),
            source: IdSource::Plugin,
        };
        let second_qr = db.exec_mut(QueryBuilder::insert().element(&second).query())?;
        let second_id = second_qr
            .elements
            .first()
            .map(|element| element.id)
            .ok_or_else(|| anyhow::anyhow!("second conflicting id insert missing id"))?;
        connect(&mut db, artist_db_id, second_id)?;

        let verified = recompute_artist_verified(&mut db, artist_db_id)?;
        assert!(!verified);
        assert!(!artist_verified(&db, artist_db_id)?);

        Ok(())
    }
}
