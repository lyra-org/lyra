// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Users, connections and playbacks for remote control tests.

use agdb::DbId;
use tokio::sync::watch;

use super::registry;
use crate::services::STATE;
use crate::services::auth::Principal;
use crate::services::playback_sessions::{
    self,
    PlaybackMutation,
};
use crate::services::playbacks::{
    self,
    QueueSnapshot,
    bindings,
};
use crate::services::testing;
use lyra_db::PlaybackState;

pub(crate) struct TestUser {
    pub(crate) db_id: DbId,
    pub(crate) principal: Principal,
}

/// A fresh library state with no connections registered.
pub(crate) async fn init() -> anyhow::Result<()> {
    testing::init_library_test_state(std::path::PathBuf::from(".")).await?;
    for connection in registry::list_connections().await {
        registry::unregister(connection.connection_id).await;
    }
    Ok(())
}

/// An admin, so every track is visible to it.
pub(crate) async fn user(name: &str) -> anyhow::Result<TestUser> {
    let db_id = {
        let mut db = STATE.db.write().await;
        let user = lyra_db::fixtures::test_user(&format!("{name}-{}", lyra_db::nanoid()))?;
        let db_id = lyra_db::users::create(&mut db, &user)?;
        lyra_db::roles::ensure_builtin_roles(&mut db)?;
        lyra_db::roles::ensure_user_has_role(&mut db, db_id, lyra_db::roles::BUILTIN_ADMIN_ROLE)?;
        db_id
    };
    Ok(TestUser {
        db_id,
        principal: testing::user_principal(db_id).await?,
    })
}

/// A user with no libraries, so it sees no track an admin's fixture inserted.
pub(crate) async fn listener(name: &str) -> anyhow::Result<TestUser> {
    let db_id = {
        let mut db = STATE.db.write().await;
        let user = lyra_db::fixtures::test_user(&format!("{name}-{}", lyra_db::nanoid()))?;
        lyra_db::users::create(&mut db, &user)?
    };
    Ok(TestUser {
        db_id,
        principal: testing::user_principal(db_id).await?,
    })
}

/// A track in no library, seen only by admins, and its public id.
pub(crate) async fn track(title: &str) -> anyhow::Result<String> {
    let mut db = STATE.db.write().await;
    let track_db_id = lyra_db::fixtures::insert_track(&mut db, title)?;
    Ok(lyra_db::lookup::find_id_by_db_id(&*db, track_db_id)?.expect("track has a public id"))
}

pub(crate) async fn connect(
    user: &TestUser,
    session_key: &str,
) -> anyhow::Result<registry::RegisterResult> {
    registry::register(
        user.principal.user_public_id.clone(),
        Some(format!("{session_key} client")),
        session_key.to_string(),
        watch::channel(None).0,
    )
    .await
    .map_err(|error| anyhow::anyhow!(error.to_string()))
}

pub(crate) fn connection_key<'a>(
    user: &'a TestUser,
    session_key: &'a str,
) -> bindings::ConnectionKey<'a> {
    bindings::ConnectionKey::new(&user.principal.user_public_id, session_key)
}

fn reporting(user: &TestUser, session_key: &str) -> Option<playbacks::ReportingConnection> {
    Some(playbacks::ReportingConnection {
        user_public_id: user.principal.user_public_id.clone(),
        session_key: session_key.to_string(),
    })
}

fn play_id() -> String {
    format!("connection-play-{}", &lyra_db::nanoid()[..8])
}

/// Creates a playback for the user, reported by the connection with the session key, and returns
/// its id and current track session.
pub(crate) async fn drive_new_playback(
    user: &TestUser,
    session_key: &str,
) -> anyhow::Result<(String, DbId)> {
    let now_ms = playback_sessions::now_ms()?;
    let mut db = STATE.db.write().await;
    let track_db_id = lyra_db::fixtures::insert_track(&mut db, "Track")?;
    let track_id =
        lyra_db::lookup::find_id_by_db_id(&*db, track_db_id)?.expect("track has a public id");
    let queue = playbacks::validate_queue(&*db, &user.principal, QueueSnapshot::single(track_id))?;
    let update = playbacks::create_playback(
        &mut db,
        playbacks::CreatePlaybackRequest {
            id: lyra_db::nanoid(),
            play_id: play_id(),
            user_db_id: user.db_id,
            client_name: None,
            queue,
            mutation: PlaybackMutation {
                position_ms: Some(0),
                duration_ms: Some(100_000),
                state: Some(PlaybackState::Playing),
                ..PlaybackMutation::default()
            },
            now_ms,
            reporting_connection: reporting(user, session_key),
        },
    )?;
    Ok((update.playback.id, update.session.playback_session_id))
}

/// Reports a new play of the playback's current track from the connection with the session key,
/// which moves it to a new track session of the same playback. Returns that session.
pub(crate) async fn play_again(
    user: &TestUser,
    session_key: &str,
    playback_id: &str,
) -> anyhow::Result<DbId> {
    report(
        user,
        session_key,
        playback_id,
        play_id(),
        PlaybackState::Playing,
    )
    .await
}

/// Reports the playback's current track session stopped from the connection with the session key.
pub(crate) async fn stop(
    user: &TestUser,
    session_key: &str,
    playback_id: &str,
) -> anyhow::Result<()> {
    let current_play_id = {
        let db = STATE.db.read().await;
        let playback_db_id =
            lyra_db::lookup::find_node_id_by_id(&*db, playback_id)?.expect("playback exists");
        let session_id = lyra_db::playbacks::get_current_session_id(&*db, playback_db_id)?
            .expect("playback has a current session");
        lyra_db::playback_sessions::get_by_id(&*db, session_id)?
            .expect("current session exists")
            .play_id
    };
    report(
        user,
        session_key,
        playback_id,
        current_play_id,
        PlaybackState::Stopped,
    )
    .await?;
    Ok(())
}

async fn report(
    user: &TestUser,
    session_key: &str,
    playback_id: &str,
    play_id: String,
    state: PlaybackState,
) -> anyhow::Result<DbId> {
    let now_ms = playback_sessions::now_ms()?;
    let mut db = STATE.db.write().await;
    let playback_db_id =
        lyra_db::lookup::find_node_id_by_id(&*db, playback_id)?.expect("playback exists");
    let playback = lyra_db::playbacks::get_by_id(&*db, playback_db_id)?.expect("playback exists");
    let queue = playbacks::queue_from_playback(&playback)?;
    let current_track_db_id = lyra_db::lookup::find_node_id_by_id(&*db, queue.current_track_id())?
        .expect("current track exists");
    let update = playbacks::report_progress(
        &mut db,
        playbacks::ReportProgressRequest {
            playback_db_id,
            play_id,
            user_db_id: user.db_id,
            client_name: None,
            queue_revision: playback.queue().expect("queue").revision,
            current_track_db_id,
            mutation: PlaybackMutation {
                position_ms: Some(0),
                duration_ms: Some(100_000),
                state: Some(state),
                ..PlaybackMutation::default()
            },
            now_ms,
            require_full_queue_access: false,
            reporting_connection: reporting(user, session_key),
        },
    )?;
    Ok(update.session.playback_session_id)
}
