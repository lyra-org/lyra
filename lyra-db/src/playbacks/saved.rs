// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! The playback a plugin's client saves its queue to, found by an alias of the user, the plugin
//! and the client.

use agdb::{
    DbId,
    QueryBuilder,
};

use super::{
    DbAccess,
    Playback,
    get_by_id,
    get_owner_id,
};

fn saved_alias(user_db_id: DbId, plugin_id: &str, client: &str) -> String {
    format!(
        "{}:{client}:{}:{plugin_id}:saved-playback",
        client.len(),
        user_db_id.0
    )
}

/// The queue-backed playback the user's `client` of the plugin saved its queue to.
pub fn get_saved(
    db: &impl DbAccess,
    user_db_id: DbId,
    plugin_id: &str,
    client: &str,
) -> anyhow::Result<Option<Playback>> {
    let alias = saved_alias(user_db_id, plugin_id, client);
    let result = match db.exec(QueryBuilder::select().ids(alias).query()) {
        Ok(result) => result,
        Err(error) if error.ty == agdb::DbErrorType::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let Some(element) = result.elements.first() else {
        return Ok(None);
    };
    let Some(playback) = get_by_id(db, element.id)? else {
        anyhow::bail!("invalid saved playback binding")
    };
    anyhow::ensure!(
        get_owner_id(db, element.id)? == Some(user_db_id) && playback.queue().is_some(),
        "saved playback binding identity mismatch"
    );
    Ok(Some(playback))
}

/// Makes `playback_db_id` the playback the user's `client` of the plugin saves to, while none is.
pub fn claim(
    db: &mut impl DbAccess,
    user_db_id: DbId,
    plugin_id: &str,
    client: &str,
    playback_db_id: DbId,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        get_saved(db, user_db_id, plugin_id, client)?.is_none(),
        "saved playback binding already exists"
    );
    db.exec_mut(
        QueryBuilder::insert()
            .aliases(saved_alias(user_db_id, plugin_id, client))
            .ids(playback_db_id)
            .query(),
    )?;
    Ok(())
}

/// Releases the playback the user's `client` of the plugin saves to, leaving the playback.
pub fn unclaim(
    db: &mut impl DbAccess,
    user_db_id: DbId,
    plugin_id: &str,
    client: &str,
) -> anyhow::Result<()> {
    db.exec_mut(
        QueryBuilder::remove()
            .aliases(saved_alias(user_db_id, plugin_id, client))
            .query(),
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::nanoid;

    use super::*;
    use crate::{
        PlaybackSession,
        PlaybackState,
        fixtures::{
            insert_track,
            test_user,
        },
        playbacks::{
            PlaybackSource,
            Queue,
            RepeatMode,
            create,
            delete,
            insert_reported,
        },
        test_db::new_test_db,
    };

    fn session() -> PlaybackSession {
        PlaybackSession {
            db_id: None,
            id: nanoid(),
            play_id: nanoid(),
            client_name: None,
            plugin_id: None,
            position_ms: 0,
            duration_ms: None,
            played_ms: 0,
            last_position_ms: 0,
            last_position_at_ms: 1,
            latency_allowance_used_ms: 0,
            rate_permille: 1000,
            counts_as_listen: false,
            state: PlaybackState::Stopped,
            updated_at_ms: 1,
            live_until_ms: 1,
            created_at_ms: 1,
        }
    }

    fn queue_playback() -> Playback {
        Playback {
            db_id: None,
            id: nanoid(),
            source: PlaybackSource::Queue(Queue {
                revision: 1,
                track_ids: vec!["track".to_string()],
                current_index: 0,
                repeat_mode: RepeatMode::None,
                shuffle_enabled: false,
            }),
            created_at_ms: 1,
            updated_at_ms: 1,
        }
    }

    #[test]
    fn a_claim_is_found_by_user_plugin_and_client_until_released() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = crate::users::create(&mut db, &test_user("saver")?)?;
        let other = crate::users::create(&mut db, &test_user("other")?)?;
        let track = insert_track(&mut db, "Track")?;
        let session = crate::playback_sessions::create(&mut db, &session(), track, user)?;
        let playback = create(&mut db, &queue_playback(), user, session)?;

        db.transaction_mut(|t| claim(t, user, "subsonic", "Phone", playback))?;
        let found = |db: &agdb::DbAny, user, plugin, client| {
            get_saved(db, user, plugin, client).map(|saved| saved.and_then(|p| p.db_id))
        };
        assert_eq!(found(&db, user, "subsonic", "Phone")?, Some(playback));
        assert_eq!(found(&db, user, "subsonic", "")?, None);
        assert_eq!(found(&db, user, "subsonic", "Phone2")?, None);
        assert_eq!(found(&db, user, "other", "Phone")?, None);
        assert_eq!(found(&db, other, "subsonic", "Phone")?, None);
        assert!(
            db.transaction_mut(|t| claim(t, user, "subsonic", "Phone", playback))
                .is_err(),
            "a claimed alias is not free"
        );

        db.transaction_mut(|t| unclaim(t, user, "subsonic", "Phone"))?;
        assert_eq!(found(&db, user, "subsonic", "Phone")?, None);
        assert!(get_by_id(&db, playback)?.is_some(), "the playback stays");

        db.transaction_mut(|t| claim(t, user, "subsonic", "", playback))?;
        delete(&mut db, playback)?;
        assert_eq!(found(&db, user, "subsonic", "")?, None);
        Ok(())
    }

    #[test]
    fn a_reported_playback_is_no_saved_one() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = crate::users::create(&mut db, &test_user("saver")?)?;
        let track = insert_track(&mut db, "Track")?;
        let session = crate::playback_sessions::create(&mut db, &session(), track, user)?;
        let reported =
            db.transaction_mut(|t| insert_reported(t, user, "subsonic", "device", session, 1))?;
        db.exec_mut(
            QueryBuilder::insert()
                .aliases(saved_alias(user, "subsonic", "Phone"))
                .ids(reported.db_id.unwrap())
                .query(),
        )?;
        assert!(get_saved(&db, user, "subsonic", "Phone").is_err());
        Ok(())
    }
}
