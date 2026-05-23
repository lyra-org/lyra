use super::*;

#[test]
fn plugin_executor_exposes_db_backed_lyra_ids_module() -> Result<()> {
    let mut db = crate::plugins::db::test_db::new_test_db()?;
    let track_db_id = crate::plugins::db::test_db::insert_track(&mut db, "Raw ID Track")?;
    let track_public_id = crate::plugins::db::lookup::find_id_by_db_id(&db, track_db_id)?
        .context("inserted track has public id")?;
    let db = std::sync::Arc::new(tokio::sync::RwLock::new(db));

    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("demo", &["lyra.ids"])]),
        default_server_info(),
        db,
    )?;
    runtime.run_plugin_source(
        "demo",
        "init.luau",
        format!(
            r#"
                local ids = require("@lyra/ids")
                local track_db_id = {track_db_id}
                local track_public_id = "{track_public_id}"

                executor_public_id = ids.get_id(track_db_id)
                executor_public_ids = ids.get_ids({{ track_db_id, -1, track_db_id, 999999 }})
                executor_db_id = ids.get_db_id(track_public_id)
                executor_db_ids = ids.get_db_ids({{ track_public_id, " ", track_public_id, "missing-public-id" }})
            "#,
            track_db_id = track_db_id.0,
        )
        .into_bytes(),
    )?;

    let values = runtime.eval_plugin_source(
        "demo",
        "check.luau",
        format!(
            r#"
                local track_db_id = {track_db_id}
                local track_public_id = "{track_public_id}"

                return executor_public_id,
                    executor_public_ids[track_db_id],
                    executor_public_ids[999999] == nil,
                    executor_db_id,
                    executor_db_ids[track_public_id],
                    executor_db_ids["missing-public-id"] == nil
            "#,
            track_db_id = track_db_id.0,
        )
        .into_bytes(),
    )?;
    assert_eq!(
        values,
        vec![
            luau::Value::String(track_public_id.as_bytes().to_vec()),
            luau::Value::String(track_public_id.as_bytes().to_vec()),
            luau::Value::Boolean(true),
            luau::Value::Integer(track_db_id.0),
            luau::Value::Integer(track_db_id.0),
            luau::Value::Boolean(true),
        ]
    );
    Ok(())
}

#[test]
fn plugin_executor_exposes_db_backed_lyra_entities_module() -> Result<()> {
    let mut db = crate::plugins::db::test_db::new_test_db()?;
    let track_db_id = crate::plugins::db::test_db::insert_track(&mut db, "Raw Entity Track")?;
    let track_public_id = crate::plugins::db::lookup::find_id_by_db_id(&db, track_db_id)?
        .context("inserted track has public id")?;
    let db = std::sync::Arc::new(tokio::sync::RwLock::new(db));

    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("demo", &["lyra.entities"])]),
        default_server_info(),
        db,
    )?;
    let values = runtime.eval_plugin_source(
        "demo",
        "init.luau",
        format!(
            r#"
                local entities = require("@lyra/entities")
                local track_db_id = {track_db_id}
                local track_public_id = "{track_public_id}"

                local by_db = entities.query_track({{ id = track_db_id }})
                local by_public = entities.query_track({{ id = track_public_id }})
                local many = entities.query_many({{ ids = {{ track_public_id, track_db_id }} }})

                return by_db.entity.track_title,
                    by_public.entity.id,
                    many[track_public_id].entity.track_title,
                    many[tostring(track_db_id)].entity.track_title,
                    entities.get_type(track_public_id),
                    entities.CreditType.Artist
            "#,
            track_db_id = track_db_id.0,
        )
        .into_bytes(),
    )?;

    assert_eq!(
        values,
        vec![
            luau::Value::String(b"Raw Entity Track".to_vec()),
            luau::Value::String(track_public_id.as_bytes().to_vec()),
            luau::Value::String(b"Raw Entity Track".to_vec()),
            luau::Value::String(b"Raw Entity Track".to_vec()),
            luau::Value::String(b"Track".to_vec()),
            luau::Value::String(b"artist".to_vec()),
        ]
    );
    Ok(())
}

#[test]
fn plugin_executor_exposes_db_backed_lyra_entries_module() -> Result<()> {
    let mut db = crate::plugins::db::test_db::new_test_db()?;
    let track_db_id = crate::plugins::db::test_db::insert_track(&mut db, "Raw Entry Track")?;
    let entry = crate::plugins::db::entries::Entry {
        db_id: None,
        id: nanoid::nanoid!(),
        full_path: std::path::PathBuf::from("/music/raw-entry.flac"),
        kind: crate::plugins::db::entries::EntryKind::File,
        file_kind: Some("audio".to_string()),
        name: "raw-entry.flac".to_string(),
        hash: Some("raw-entry-hash".to_string()),
        size: 123,
        mtime: 456,
        ctime: 789,
    };
    let entry_public_id = entry.id.clone();
    let entry_db_id = db
        .exec_mut(agdb::QueryBuilder::insert().element(&entry).query())?
        .ids()[0];
    crate::plugins::db::track_sources::upsert(
        &mut db,
        track_db_id,
        entry_db_id,
        crate::plugins::db::track_sources::TrackSourceUpsert {
            source_kind: "embedded_tags".to_string(),
            source_key: format!("entry:{}:embedded", entry_db_id.0),
            is_primary: true,
            start_ms: None,
            end_ms: None,
        },
        None,
    )?;
    let db = std::sync::Arc::new(tokio::sync::RwLock::new(db));

    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("demo", &["lyra.entries"])]),
        default_server_info(),
        db,
    )?;
    let values = runtime.eval_plugin_source(
        "demo",
        "init.luau",
        format!(
            r#"
                local entries = require("@lyra/entries")
                local rows = entries.get({track_db_id})
                return rows[1].db_id,
                    rows[1].id,
                    rows[1].kind,
                    rows[1].name,
                    rows[1].hash,
                    rows[1].size,
                    rows[1].mtime,
                    rows[1].full_path == nil
            "#,
            track_db_id = track_db_id.0,
        )
        .into_bytes(),
    )?;

    assert_eq!(
        values,
        vec![
            luau::Value::Integer(entry_db_id.0),
            luau::Value::String(entry_public_id.into_bytes()),
            luau::Value::String(b"file".to_vec()),
            luau::Value::String(b"raw-entry.flac".to_vec()),
            luau::Value::String(b"raw-entry-hash".to_vec()),
            luau::Value::Integer(123),
            luau::Value::Integer(456),
            luau::Value::Boolean(true),
        ]
    );
    Ok(())
}

#[test]
fn plugin_executor_exposes_db_backed_lyra_chromaprint_module() -> Result<()> {
    let mut db = crate::plugins::db::test_db::new_test_db()?;
    let entry = crate::plugins::db::entries::Entry {
        db_id: None,
        id: nanoid::nanoid!(),
        full_path: std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/assets/metadata/integration_track.flac"),
        kind: crate::plugins::db::entries::EntryKind::File,
        file_kind: Some("audio".to_string()),
        name: "integration_track.flac".to_string(),
        hash: None,
        size: 1,
        mtime: 1,
        ctime: 1,
    };
    let entry_db_id = db
        .exec_mut(agdb::QueryBuilder::insert().element(&entry).query())?
        .ids()[0];
    let db = std::sync::Arc::new(tokio::sync::RwLock::new(db));

    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("demo", &["lyra.chromaprint"])]),
        default_server_info(),
        db,
    )?;
    runtime.run_plugin_source(
        "demo",
        "init.luau",
        format!(
            r#"
                local chromaprint = require("@lyra/chromaprint")
                local result = chromaprint.compute({entry_db_id})
                executor_chromaprint_result =
                    type(result.fingerprint) == "string"
                    and type(result.duration) == "number"
                    and result.duration > 0
            "#,
            entry_db_id = entry_db_id.0,
        )
        .into_bytes(),
    )?;

    let values = runtime.eval_plugin_source(
        "demo",
        "check.luau",
        &b"return executor_chromaprint_result"[..],
    )?;
    assert_eq!(values, vec![luau::Value::Boolean(true)]);
    Ok(())
}

#[test]
fn plugin_executor_exposes_db_backed_lyra_users_module() -> Result<()> {
    let mut db = crate::plugins::db::test_db::new_test_db()?;
    crate::plugins::db::roles::ensure_builtin_roles(&mut db)?;
    crate::plugins::db::users::create(&mut db, &crate::plugins::db::test_db::test_user("bob")?)?;
    let alice_id = crate::plugins::db::users::create(
        &mut db,
        &crate::plugins::db::test_db::test_user("alice")?,
    )?;
    crate::plugins::db::roles::ensure_user_has_role(
        &mut db,
        alice_id,
        crate::plugins::db::roles::BUILTIN_ADMIN_ROLE,
    )?;
    let db = std::sync::Arc::new(tokio::sync::RwLock::new(db));

    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("demo", &["lyra.users"])]),
        default_server_info(),
        db,
    )?;
    runtime.run_plugin_source(
        "demo",
        "init.luau",
        r#"
            local users = require("@lyra/users")
            local listed = users.list()
            executor_users_count = #listed
            executor_users_first = listed[1].username
            executor_users_first_role = listed[1].role
            executor_users_second = listed[2].username
            executor_users_second_role = listed[2].role
        "#
        .as_bytes()
        .to_vec(),
    )?;

    let values = runtime.eval_plugin_source(
        "demo",
        "check.luau",
        &b"return executor_users_count, executor_users_first, executor_users_first_role, executor_users_second, executor_users_second_role"[..],
    )?;
    assert_eq!(
        values,
        vec![
            luau::Value::Number(2.0),
            luau::Value::String(b"alice".to_vec()),
            luau::Value::String(b"admin".to_vec()),
            luau::Value::String(b"bob".to_vec()),
            luau::Value::Nil,
        ]
    );
    Ok(())
}

#[test]
fn plugin_executor_exposes_db_backed_lyra_listens_module() -> Result<()> {
    let mut db = crate::plugins::db::test_db::new_test_db()?;
    let user = crate::plugins::db::test_db::test_user("listener")?;
    let user_public_id = user.id.clone();
    let user_db_id = crate::plugins::db::users::create(&mut db, &user)?;
    let track_db_id = crate::plugins::db::test_db::insert_track(&mut db, "Raw Listen Track")?;
    let track_public_id = crate::plugins::db::lookup::find_id_by_db_id(&db, track_db_id)?
        .context("inserted track has public id")?;
    for listened_at_ms in [1000_u64, 2500] {
        let listen = crate::plugins::db::listens::Listen {
            db_id: None,
            id: nanoid::nanoid!(),
            track_public_id: track_public_id.clone(),
            position_ms: 0,
            duration_ms: Some(180_000),
            activity_ms: 180_000,
            state: crate::plugins::db::PlaybackState::Completed,
            listened_at_ms,
            created_at_ms: listened_at_ms,
        };
        let session = crate::services::playback_sessions::PlaybackSession {
            db_id: None,
            id: nanoid::nanoid!(),
            position_ms: 0,
            duration_ms: Some(180_000),
            activity_ms: Some(180_000),
            last_position_ms: None,
            state: crate::plugins::db::PlaybackState::Completed,
            listen_recorded: Some(true),
            updated_at_ms: listened_at_ms,
            created_at_ms: listened_at_ms,
        };
        crate::plugins::db::listens::create_and_mark_recorded(
            &mut db,
            &listen,
            track_db_id,
            user_db_id,
            &session,
        )?;
    }
    let db = std::sync::Arc::new(tokio::sync::RwLock::new(db));

    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("demo", &["lyra.listens"])]),
        default_server_info(),
        db,
    )?;
    let mut context = CallContext {
        origin: plugin_origin("demo", "init.luau"),
        ..CallContext::default()
    };
    context.caller.insert(crate::services::auth::Principal {
        user_db_id,
        user_public_id,
        username: "listener".to_string(),
        permissions: vec![crate::plugins::db::Permission::Admin],
        role_name: Some("admin".to_string()),
        accessible_library_ids: std::collections::HashSet::new(),
    });
    runtime.run_plugin_source_with_call_context(
        format!(
            r#"
                local listens = require("@lyra/listens")
                local track_db_id = {track_db_id}
                executor_listen_count = listens.get_count(track_db_id, {user_db_id})
                executor_listen_counts = listens.get_counts({{ track_db_id, -1, track_db_id }}, {user_db_id})
                executor_listen_stats = listens.get_stats({{ track_db_id }}, {user_db_id})
            "#,
            track_db_id = track_db_id.0,
            user_db_id = user_db_id.0,
        )
        .into_bytes(),
        context,
    )?;

    let values = runtime.eval_plugin_source(
        "demo",
        "check.luau",
        format!(
            r#"
                local track_db_id = {track_db_id}
                return executor_listen_count,
                    executor_listen_counts[track_db_id],
                    executor_listen_stats.counts[track_db_id],
                    executor_listen_stats.last_played[track_db_id]
            "#,
            track_db_id = track_db_id.0,
        )
        .into_bytes(),
    )?;
    assert_eq!(
        values,
        vec![
            luau::Value::Integer(2),
            luau::Value::Integer(2),
            luau::Value::Integer(2),
            luau::Value::Integer(2500),
        ]
    );
    Ok(())
}

#[test]
fn plugin_executor_exposes_db_backed_lyra_favorites_module() -> Result<()> {
    let mut db = crate::plugins::db::test_db::new_test_db()?;
    let user = crate::plugins::db::test_db::test_user("favorite-user")?;
    let user_public_id = user.id.clone();
    let user_db_id = crate::plugins::db::users::create(&mut db, &user)?;
    let library_db_id =
        crate::plugins::db::test_db::insert_library(&mut db, "Raw Favorites", "/tmp/raw-fav")?;
    let library_public_id = crate::plugins::db::lookup::find_id_by_db_id(&db, library_db_id)?
        .context("inserted library has public id")?;
    let track_db_id = crate::plugins::db::test_db::insert_track(&mut db, "Raw Favorite Track")?;
    crate::plugins::db::test_db::connect(&mut db, library_db_id, track_db_id)?;
    let db = std::sync::Arc::new(tokio::sync::RwLock::new(db));

    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("demo", &["lyra.favorites"])]),
        default_server_info(),
        db,
    )?;
    let mut context = CallContext {
        origin: plugin_origin("demo", "init.luau"),
        ..CallContext::default()
    };
    context.caller.insert(crate::services::auth::Principal {
        user_db_id,
        user_public_id,
        username: "favorite-user".to_string(),
        permissions: Vec::new(),
        role_name: None,
        accessible_library_ids: std::collections::HashSet::from([library_public_id]),
    });
    runtime.run_plugin_source_with_call_context(
        format!(
            r#"
                local favorites = require("@lyra/favorites")
                local user_db_id = {user_db_id}
                local track_db_id = {track_db_id}

                executor_favorite_add = favorites.add(user_db_id, track_db_id)
                executor_favorite_has = favorites.has(user_db_id, track_db_id)
                executor_favorite_many = favorites.has_many(user_db_id, {{ track_db_id, -1, track_db_id, 999999 }})
                executor_favorite_ids = favorites.list_ids(user_db_id, "track")
                executor_favorite_remove = favorites.remove(user_db_id, track_db_id)
                executor_favorite_has_after_remove = favorites.has(user_db_id, track_db_id)
            "#,
            user_db_id = user_db_id.0,
            track_db_id = track_db_id.0,
        )
        .into_bytes(),
        context,
    )?;

    let values = runtime.eval_plugin_source(
        "demo",
        "check.luau",
        format!(
            r#"
                local track_db_id = {track_db_id}
                return executor_favorite_add,
                    executor_favorite_has,
                    executor_favorite_many[track_db_id],
                    executor_favorite_many[999999],
                    executor_favorite_ids[1],
                    executor_favorite_remove,
                    executor_favorite_has_after_remove
            "#,
            track_db_id = track_db_id.0,
        )
        .into_bytes(),
    )?;
    assert_eq!(
        values,
        vec![
            luau::Value::Boolean(true),
            luau::Value::Boolean(true),
            luau::Value::Boolean(true),
            luau::Value::Boolean(false),
            luau::Value::Integer(track_db_id.0),
            luau::Value::Boolean(true),
            luau::Value::Boolean(false),
        ]
    );
    Ok(())
}

#[test]
fn plugin_executor_exposes_db_backed_lyra_artists_module() -> Result<()> {
    let mut db = crate::plugins::db::test_db::new_test_db()?;
    let library_db_id = crate::plugins::db::test_db::insert_library(
        &mut db,
        "Raw Artists Library",
        "/tmp/raw-artists",
    )?;
    let release_db_id =
        crate::plugins::db::test_db::insert_release(&mut db, "Raw Artists Release")?;
    crate::plugins::db::test_db::connect(&mut db, library_db_id, release_db_id)?;
    let artist_db_id = crate::plugins::db::test_db::insert_artist(&mut db, "Raw Artist Module")?;
    let mut artist = crate::plugins::db::artists::get_by_id(&db, artist_db_id)?
        .context("inserted artist exists")?;
    artist.set_artist_type(crate::plugins::db::ArtistType::Person);
    crate::plugins::db::artists::update(&mut db, &artist)?;
    crate::plugins::db::test_db::connect_artist(&mut db, release_db_id, artist_db_id)?;
    let actor_db_id = crate::plugins::db::test_db::insert_artist(&mut db, "Raw Voice Actor")?;
    let character_db_id = crate::plugins::db::test_db::insert_artist(&mut db, "Raw Character")?;
    crate::plugins::db::artists::relations::link(
        &mut db,
        actor_db_id,
        character_db_id,
        crate::plugins::db::ArtistRelationType::VoiceActor,
        Some("Lead".to_string()),
    )?;
    let db = std::sync::Arc::new(tokio::sync::RwLock::new(db));

    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("demo", &["lyra.artists"])]),
        default_server_info(),
        db,
    )?;
    let values = runtime.eval_plugin_source(
        "demo",
        "init.luau",
        format!(
            r#"
                local artists = require("@lyra/artists")
                local library_db_id = {library_db_id}
                local release_db_id = {release_db_id}
                local actor_db_id = {actor_db_id}

                local listed = artists.list()
                local by_library = artists.list_by_library(library_db_id)
                local many = artists.list_many({{ release_db_id }})
                local relations = artists.list_relations_many({{ actor_db_id }})
                local queried = artists.query({{
                    search_term = "Module",
                    artist_type = artists.ArtistType.Person,
                    sort_by = {{ "name" }},
                    sort_order = "ascending",
                }})
                local credited = artists.query_credited({{
                    scope = release_db_id,
                    artist_type = artists.ArtistType.Person,
                    credit_types = {{ artists.CreditType.Artist }},
                }})

                return artists.ArtistType.Person,
                    artists.ArtistRelationType.VoiceActor,
                    artists.CreditType.Artist,
                    listed[1] ~= nil,
                    by_library[1].artist_name,
                    many[release_db_id][1].artist_name,
                    relations[actor_db_id][1].relation_type,
                    relations[actor_db_id][1].direction,
                    relations[actor_db_id][1].artist.artist_name,
                    queried.entities[1].artist_name,
                    credited.entities[1].artist_name,
                    queried.total_count,
                    credited.total_count
            "#,
            library_db_id = library_db_id.0,
            release_db_id = release_db_id.0,
            actor_db_id = actor_db_id.0,
        )
        .into_bytes(),
    )?;

    let mut values = values;
    let artist_type = crate::plugins::db::ArtistType::_harmony_userdata_class().read_value(
        &runtime.vm,
        "artist_type",
        values.remove(0),
    )?;
    let relation_type = crate::plugins::db::ArtistRelationType::_harmony_userdata_class()
        .read_value(&runtime.vm, "relation_type", values.remove(0))?;
    let credit_type = crate::plugins::db::CreditType::_harmony_userdata_class().read_value(
        &runtime.vm,
        "credit_type",
        values.remove(0),
    )?;
    assert_eq!(artist_type, crate::plugins::db::ArtistType::Person);
    assert_eq!(
        relation_type,
        crate::plugins::db::ArtistRelationType::VoiceActor
    );
    assert_eq!(credit_type, crate::plugins::db::CreditType::Artist);
    assert_eq!(
        values,
        vec![
            luau::Value::Boolean(true),
            luau::Value::String(b"Raw Artist Module".to_vec()),
            luau::Value::String(b"Raw Artist Module".to_vec()),
            luau::Value::String(b"voice_actor".to_vec()),
            luau::Value::String(b"outgoing".to_vec()),
            luau::Value::String(b"Raw Character".to_vec()),
            luau::Value::String(b"Raw Artist Module".to_vec()),
            luau::Value::String(b"Raw Artist Module".to_vec()),
            luau::Value::Integer(1),
            luau::Value::Integer(1),
        ]
    );
    Ok(())
}

#[test]
fn plugin_executor_exposes_db_backed_lyra_tracks_module() -> Result<()> {
    let mut db = crate::plugins::db::test_db::new_test_db()?;
    let release_db_id = crate::plugins::db::test_db::insert_release(&mut db, "Raw Track Release")?;
    let track_db_id = crate::plugins::db::test_db::insert_track(&mut db, "Raw Track Module Song")?;
    db.exec_mut(
        agdb::QueryBuilder::insert()
            .edges()
            .from(release_db_id)
            .to(track_db_id)
            .query(),
    )?;
    let track_public_id = crate::plugins::db::lookup::find_id_by_db_id(&db, track_db_id)?
        .context("inserted track has public id")?;
    let db = std::sync::Arc::new(tokio::sync::RwLock::new(db));

    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("demo", &["lyra.tracks"])]),
        default_server_info(),
        db,
    )?;
    let values = runtime.eval_plugin_source(
        "demo",
        "init.luau",
        format!(
            r#"
                local tracks = require("@lyra/tracks")
                local release_db_id = {release_db_id}
                local track_db_id = {track_db_id}

                local listed = tracks.list(release_db_id)
                local all = tracks.list()
                local fetched = tracks.get_by_ids({{ track_db_id, -1, track_db_id }})
                local related = tracks.list_many({{ release_db_id }})
                local queried = tracks.query({{
                    scope = release_db_id,
                    search_term = "Module",
                    sort_by = {{ "name" }},
                    sort_order = "ascending",
                    limit = 5,
                }})

                return listed[1].track_title,
                    all[1] ~= nil,
                    fetched[track_db_id].id,
                    related[release_db_id][1].track_title,
                    queried.entities[1].track_title,
                    queried.total_count,
                    queried.offset
            "#,
            release_db_id = release_db_id.0,
            track_db_id = track_db_id.0,
        )
        .into_bytes(),
    )?;

    assert_eq!(
        values,
        vec![
            luau::Value::String(b"Raw Track Module Song".to_vec()),
            luau::Value::Boolean(true),
            luau::Value::String(track_public_id.as_bytes().to_vec()),
            luau::Value::String(b"Raw Track Module Song".to_vec()),
            luau::Value::String(b"Raw Track Module Song".to_vec()),
            luau::Value::Integer(1),
            luau::Value::Integer(0),
        ]
    );
    Ok(())
}

#[test]
fn plugin_executor_exposes_db_backed_lyra_track_sources_module() -> Result<()> {
    let mut db = crate::plugins::db::test_db::new_test_db()?;
    let track_db_id = crate::plugins::db::test_db::insert_track(&mut db, "Raw Source Track")?;
    let entry = crate::plugins::db::entries::Entry {
        db_id: None,
        id: nanoid::nanoid!(),
        full_path: std::path::PathBuf::from("/music/raw-source.FLAC"),
        kind: crate::plugins::db::entries::EntryKind::File,
        file_kind: Some("audio".to_string()),
        name: "raw-source.FLAC".to_string(),
        hash: None,
        size: 1,
        mtime: 1,
        ctime: 1,
    };
    let entry_db_id = db
        .exec_mut(agdb::QueryBuilder::insert().element(&entry).query())?
        .ids()[0];
    let source_key = format!("entry:{}:embedded", entry_db_id.0);
    crate::plugins::db::track_sources::upsert(
        &mut db,
        track_db_id,
        entry_db_id,
        crate::plugins::db::track_sources::TrackSourceUpsert {
            source_kind: "embedded_tags".to_string(),
            source_key: source_key.clone(),
            is_primary: true,
            start_ms: None,
            end_ms: None,
        },
        None,
    )?;
    let db = std::sync::Arc::new(tokio::sync::RwLock::new(db));

    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("demo", &["lyra.track_sources"])]),
        default_server_info(),
        db,
    )?;
    let values = runtime.eval_plugin_source(
        "demo",
        "init.luau",
        format!(
            r#"
                local track_sources = require("@lyra/track_sources")
                local track_db_id = {track_db_id}

                local many = track_sources.get_primary_containers({{ track_db_id, -1, track_db_id }})
                return track_sources.get_primary_source_key(track_db_id),
                    track_sources.get_primary_container(track_db_id),
                    many[track_db_id],
                    track_sources.get_primary_source_key(-1) == nil
            "#,
            track_db_id = track_db_id.0,
        )
        .into_bytes(),
    )?;

    assert_eq!(
        values,
        vec![
            luau::Value::String(source_key.into_bytes()),
            luau::Value::String(b"flac".to_vec()),
            luau::Value::String(b"flac".to_vec()),
            luau::Value::Boolean(true),
        ]
    );
    Ok(())
}

#[test]
fn plugin_executor_exposes_db_backed_lyra_playback_sources_module() -> Result<()> {
    let mut db = crate::plugins::db::test_db::new_test_db()?;
    let user = crate::plugins::db::test_db::test_user("playback-source-user")?;
    let user_public_id = user.id.clone();
    let user_db_id = crate::plugins::db::users::create(&mut db, &user)?;
    let track_db_id =
        crate::plugins::db::test_db::insert_track(&mut db, "Raw Playback Source Track")?;
    let entry_path = std::env::temp_dir().join(format!(
        "lyra-raw-playback-source-{}.flac",
        nanoid::nanoid!()
    ));
    std::fs::write(&entry_path, b"plugin playback source")?;
    let entry = crate::plugins::db::entries::Entry {
        db_id: None,
        id: nanoid::nanoid!(),
        full_path: entry_path.clone(),
        kind: crate::plugins::db::entries::EntryKind::File,
        file_kind: Some("audio".to_string()),
        name: "raw-playback-source.flac".to_string(),
        hash: Some("raw-playback-hash".to_string()),
        size: 19,
        mtime: 321,
        ctime: 654,
    };
    let entry_db_id = db
        .exec_mut(agdb::QueryBuilder::insert().element(&entry).query())?
        .ids()[0];
    let source_key = format!("entry:{}:embedded", entry_db_id.0);
    let source_id = crate::plugins::db::track_sources::upsert(
        &mut db,
        track_db_id,
        entry_db_id,
        crate::plugins::db::track_sources::TrackSourceUpsert {
            source_kind: "embedded_tags".to_string(),
            source_key: source_key.clone(),
            is_primary: true,
            start_ms: Some(100),
            end_ms: Some(200),
        },
        None,
    )?;
    let db = std::sync::Arc::new(tokio::sync::RwLock::new(db));

    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("demo", &["lyra.playback_sources"])]),
        default_server_info(),
        db,
    )?;
    let mut context = CallContext {
        origin: plugin_origin("demo", "init.luau"),
        ..CallContext::default()
    };
    context.caller.insert(crate::services::auth::Principal {
        user_db_id,
        user_public_id,
        username: "playback-source-user".to_string(),
        permissions: vec![
            crate::plugins::db::Permission::Admin,
            crate::plugins::db::Permission::ManageLibraries,
        ],
        role_name: Some("admin".to_string()),
        accessible_library_ids: std::collections::HashSet::new(),
    });
    runtime.run_plugin_source_with_call_context(
        format!(
            r#"
                local playback_sources = require("@lyra/playback_sources")
                local track_db_id = {track_db_id}

                executor_playback_source_rows = playback_sources.get(track_db_id, true)
                executor_playback_source_many = playback_sources.get_many({{ track_db_id, -1, track_db_id, 999999 }}, true)
            "#,
            track_db_id = track_db_id.0,
        )
        .into_bytes(),
        context,
    )?;

    let values = runtime.eval_plugin_source(
        "demo",
        "check.luau",
        format!(
            r#"
                local track_db_id = {track_db_id}
                local first = executor_playback_source_rows[1]
                local many = executor_playback_source_many[track_db_id]

                return first.track_id,
                    first.source_id,
                    first.source_kind,
                    first.source_key,
                    first.is_primary,
                    first.start_ms,
                    first.end_ms,
                    first.is_virtual,
                    first.entry.name,
                    first.entry.hash,
                    first.entry.full_path,
                    many.entry.name,
                    executor_playback_source_many[999999] == nil
            "#,
            track_db_id = track_db_id.0,
        )
        .into_bytes(),
    )?;
    let _ = std::fs::remove_file(&entry_path);

    assert_eq!(
        values,
        vec![
            luau::Value::Integer(track_db_id.0),
            luau::Value::Integer(source_id.0),
            luau::Value::String(b"embedded_tags".to_vec()),
            luau::Value::String(source_key.into_bytes()),
            luau::Value::Boolean(true),
            luau::Value::Integer(100),
            luau::Value::Integer(200),
            luau::Value::Boolean(true),
            luau::Value::String(b"raw-playback-source.flac".to_vec()),
            luau::Value::String(b"raw-playback-hash".to_vec()),
            luau::Value::String(entry_path.to_string_lossy().into_owned().into_bytes()),
            luau::Value::String(b"raw-playback-source.flac".to_vec()),
            luau::Value::Boolean(true),
        ]
    );
    Ok(())
}

#[test]
fn plugin_executor_exposes_db_backed_lyra_playlists_module() -> Result<()> {
    let mut db = crate::plugins::db::test_db::new_test_db()?;
    let user = crate::plugins::db::test_db::test_user("playlist-user")?;
    let user_public_id = user.id.clone();
    let user_db_id = crate::plugins::db::users::create(&mut db, &user)?;
    let track_db_id = crate::plugins::db::test_db::insert_track(&mut db, "Raw Playlist Track")?;
    let db = std::sync::Arc::new(tokio::sync::RwLock::new(db));

    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("demo", &["lyra.playlists"])]),
        default_server_info(),
        db,
    )?;
    let mut context = CallContext {
        origin: plugin_origin("demo", "init.luau"),
        ..CallContext::default()
    };
    context.caller.insert(crate::services::auth::Principal {
        user_db_id,
        user_public_id,
        username: "playlist-user".to_string(),
        permissions: vec![crate::plugins::db::Permission::Admin],
        role_name: Some("admin".to_string()),
        accessible_library_ids: std::collections::HashSet::new(),
    });
    runtime.run_plugin_source_with_call_context(
        format!(
            r#"
                local playlists = require("@lyra/playlists")
                local user_db_id = {user_db_id}
                local track_db_id = {track_db_id}

                executor_playlist_id = playlists.create({{
                    user_id = user_db_id,
                    name = "Raw Playlist",
                    description = "seed",
                    is_public = false,
                    created_at = 10,
                    updated_at = 20,
                }})
                executor_playlist = playlists.get_by_id(executor_playlist_id)
                executor_playlist_owner = playlists.get_owner(executor_playlist_id)
                executor_user_playlists = playlists.get_by_user(user_db_id)
                executor_playlist_entry_id = playlists.add_track(executor_playlist_id, track_db_id)
                executor_playlist_tracks = playlists.get_tracks(executor_playlist_id)
                executor_playlist_tracks_many = playlists.get_tracks_many({{ executor_playlist_id, -1, executor_playlist_id }})
                executor_updated_playlist = playlists.update({{
                    playlist_id = executor_playlist_id,
                    name = "Raw Updated Playlist",
                    description = "changed",
                    is_public = true,
                    updated_at = 30,
                }})
                playlists.remove_track(executor_playlist_entry_id)
                executor_playlist_tracks_after_remove = playlists.get_tracks(executor_playlist_id)
            "#,
            user_db_id = user_db_id.0,
            track_db_id = track_db_id.0,
        )
        .into_bytes(),
        context,
    )?;

    let values = runtime.eval_plugin_source(
        "demo",
        "check.luau",
        format!(
            r#"
                return executor_playlist_id ~= nil,
                    executor_playlist.name,
                    executor_playlist.db_id ~= nil,
                    executor_playlist_owner,
                    executor_user_playlists[1].name,
                    executor_playlist_entry_id ~= nil,
                    executor_playlist_tracks[1].track_id,
                    executor_playlist_tracks[1].entry_id ~= nil,
                    executor_playlist_tracks_many[executor_playlist_id][1].track_id,
                    executor_updated_playlist.name,
                    executor_updated_playlist.is_public,
                    executor_playlist_tracks_after_remove[1] == nil
            "#,
        )
        .into_bytes(),
    )?;

    assert_eq!(
        values,
        vec![
            luau::Value::Boolean(true),
            luau::Value::String(b"Raw Playlist".to_vec()),
            luau::Value::Boolean(true),
            luau::Value::Integer(user_db_id.0),
            luau::Value::String(b"Raw Playlist".to_vec()),
            luau::Value::Boolean(true),
            luau::Value::Integer(track_db_id.0),
            luau::Value::Boolean(true),
            luau::Value::Integer(track_db_id.0),
            luau::Value::String(b"Raw Updated Playlist".to_vec()),
            luau::Value::Boolean(true),
            luau::Value::Boolean(true),
        ]
    );
    Ok(())
}

#[test]
fn plugin_executor_exposes_db_backed_lyra_covers_module() -> Result<()> {
    let mut db = crate::plugins::db::test_db::new_test_db()?;
    let user = crate::plugins::db::test_db::test_user("cover-user")?;
    let user_public_id = user.id.clone();
    let user_db_id = crate::plugins::db::users::create(&mut db, &user)?;
    let release_db_id =
        crate::plugins::db::test_db::insert_release(&mut db, "Raw Covered Release")?;
    let track_db_id = crate::plugins::db::test_db::insert_track(&mut db, "Raw Covered Track")?;
    crate::plugins::db::test_db::connect(&mut db, release_db_id, track_db_id)?;
    let cover_path = std::env::temp_dir().join(format!("lyra-raw-cover-{}.jpg", nanoid::nanoid!()));
    std::fs::write(&cover_path, b"raw cover")?;
    let cover_path_string = cover_path.to_string_lossy().into_owned();
    crate::plugins::db::covers::upsert(
        &mut db,
        release_db_id,
        crate::plugins::db::Cover {
            db_id: None,
            id: nanoid::nanoid!(),
            path: cover_path_string.clone(),
            mime_type: "image/jpeg".to_string(),
            hash: "raw-cover-hash".to_string(),
            blurhash: Some("raw-blurhash".to_string()),
        },
    )?;
    let db = std::sync::Arc::new(tokio::sync::RwLock::new(db));

    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("demo", &["lyra.covers"])]),
        default_server_info(),
        db,
    )?;
    let mut context = CallContext {
        origin: plugin_origin("demo", "init.luau"),
        ..CallContext::default()
    };
    context.caller.insert(crate::services::auth::Principal {
        user_db_id,
        user_public_id,
        username: "cover-user".to_string(),
        permissions: vec![crate::plugins::db::Permission::Admin],
        role_name: Some("admin".to_string()),
        accessible_library_ids: std::collections::HashSet::new(),
    });
    runtime.run_plugin_source_with_call_context(
        format!(
            r#"
                local covers = require("@lyra/covers")
                local release_db_id = {release_db_id}
                local track_db_id = {track_db_id}

                executor_cover_release = covers.get(release_db_id)
                executor_cover_track = covers.get(track_db_id)
                executor_covers_many = covers.get_many({{ release_db_id, track_db_id, -1, release_db_id, 999999 }})
            "#,
            release_db_id = release_db_id.0,
            track_db_id = track_db_id.0,
        )
        .into_bytes(),
        context,
    )?;

    let values = runtime.eval_plugin_source(
        "demo",
        "check.luau",
        format!(
            r#"
                local release_db_id = {release_db_id}
                local track_db_id = {track_db_id}

                return executor_cover_release.path,
                    executor_cover_release.mime_type,
                    executor_cover_release.hash,
                    executor_cover_release.blurhash,
                    executor_cover_release.release_id,
                    executor_cover_track.release_id,
                    executor_covers_many[release_db_id].hash,
                    executor_covers_many[track_db_id].release_id,
                    executor_covers_many[999999] == nil
            "#,
            release_db_id = release_db_id.0,
            track_db_id = track_db_id.0,
        )
        .into_bytes(),
    )?;
    let _ = std::fs::remove_file(&cover_path);

    assert_eq!(
        values,
        vec![
            luau::Value::String(cover_path_string.into_bytes()),
            luau::Value::String(b"image/jpeg".to_vec()),
            luau::Value::String(b"raw-cover-hash".to_vec()),
            luau::Value::String(b"raw-blurhash".to_vec()),
            luau::Value::Integer(release_db_id.0),
            luau::Value::Integer(release_db_id.0),
            luau::Value::String(b"raw-cover-hash".to_vec()),
            luau::Value::Integer(release_db_id.0),
            luau::Value::Boolean(true),
        ]
    );
    Ok(())
}

#[test]
fn plugin_executor_exposes_db_backed_lyra_releases_module() -> Result<()> {
    let mut db = crate::plugins::db::test_db::new_test_db()?;
    let release_db_id = crate::plugins::db::test_db::insert_release(&mut db, "Raw Release Module")?;
    crate::plugins::db::test_db::insert_release(&mut db, "Other Release")?;
    let track_db_id = crate::plugins::db::test_db::insert_track(&mut db, "Raw Release Track")?;
    crate::plugins::db::test_db::connect(&mut db, release_db_id, track_db_id)?;
    let album_artist_id = crate::plugins::db::test_db::insert_artist(&mut db, "Raw Album Artist")?;
    crate::plugins::db::test_db::connect_artist(&mut db, release_db_id, album_artist_id)?;
    let guest_artist_id = crate::plugins::db::test_db::insert_artist(&mut db, "Raw Guest Artist")?;
    crate::plugins::db::test_db::connect_artist(&mut db, track_db_id, guest_artist_id)?;
    let db = std::sync::Arc::new(tokio::sync::RwLock::new(db));

    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("demo", &["lyra.releases"])]),
        default_server_info(),
        db,
    )?;
    let values = runtime.eval_plugin_source(
        "demo",
        "init.luau",
        format!(
            r#"
                local releases = require("@lyra/releases")
                local release_db_id = {release_db_id}
                local track_db_id = {track_db_id}
                local album_artist_id = {album_artist_id}
                local guest_artist_id = {guest_artist_id}

                local listed = releases.list(track_db_id)
                local all = releases.list()
                local by_artist = releases.get_by_artist(album_artist_id)
                local appearances = releases.get_appearances(guest_artist_id)
                local many = releases.list_many({{ track_db_id, -1, track_db_id }})
                local queried = releases.query({{
                    scope = "releases",
                    search_term = "Module",
                    sort_by = {{ "name" }},
                    sort_order = "ascending",
                    limit = 5,
                }})

                return listed[1].release_title,
                    all[1] ~= nil,
                    by_artist[1].release_title,
                    appearances[1].release_title,
                    many[track_db_id][1].release_title,
                    queried.entities[1].release_title,
                    queried.total_count,
                    queried.offset
            "#,
            release_db_id = release_db_id.0,
            track_db_id = track_db_id.0,
            album_artist_id = album_artist_id.0,
            guest_artist_id = guest_artist_id.0,
        )
        .into_bytes(),
    )?;

    assert_eq!(
        values,
        vec![
            luau::Value::String(b"Raw Release Module".to_vec()),
            luau::Value::Boolean(true),
            luau::Value::String(b"Raw Release Module".to_vec()),
            luau::Value::String(b"Raw Release Module".to_vec()),
            luau::Value::String(b"Raw Release Module".to_vec()),
            luau::Value::String(b"Raw Release Module".to_vec()),
            luau::Value::Integer(1),
            luau::Value::Integer(0),
        ]
    );
    Ok(())
}

#[test]
fn plugin_executor_exposes_db_backed_lyra_libraries_module() -> Result<()> {
    let mut db = crate::plugins::db::test_db::new_test_db()?;
    let library_db_id =
        crate::plugins::db::test_db::insert_library(&mut db, "Raw Library", "/tmp/raw-lib")?;
    let track_db_id = crate::plugins::db::test_db::insert_track(&mut db, "Raw Library Track")?;
    crate::plugins::db::test_db::connect(&mut db, library_db_id, track_db_id)?;
    let library_public_id = crate::plugins::db::lookup::find_id_by_db_id(&db, library_db_id)?
        .context("inserted library has public id")?;
    let db = std::sync::Arc::new(tokio::sync::RwLock::new(db));

    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("demo", &["lyra.libraries"])]),
        default_server_info(),
        db,
    )?;
    let values = runtime.eval_plugin_source(
        "demo",
        "init.luau",
        format!(
            r#"
                local libraries = require("@lyra/libraries")
                local library_db_id = {library_db_id}
                local library_public_id = "{library_public_id}"
                local track_db_id = {track_db_id}

                local all = libraries.list()
                local by_id = libraries.list(library_db_id)
                local by_public = libraries.list(library_public_id)
                local for_entity = libraries.get_for_entity(track_db_id)
                local many = libraries.get_for_entities({{ track_db_id, library_db_id }})

                return all[1].name,
                    by_id[1].path,
                    by_public[1].db_id,
                    for_entity[1].name,
                    many[track_db_id].name,
                    many[library_db_id].name
            "#,
            library_db_id = library_db_id.0,
            track_db_id = track_db_id.0,
        )
        .into_bytes(),
    )?;

    assert_eq!(
        values,
        vec![
            luau::Value::String(b"Raw Library".to_vec()),
            luau::Value::String(b"/tmp/raw-lib".to_vec()),
            luau::Value::Integer(library_db_id.0),
            luau::Value::String(b"Raw Library".to_vec()),
            luau::Value::String(b"Raw Library".to_vec()),
            luau::Value::String(b"Raw Library".to_vec()),
        ]
    );
    Ok(())
}

#[test]
fn plugin_executor_exposes_db_backed_lyra_genres_module() -> Result<()> {
    let mut db = crate::plugins::db::test_db::new_test_db()?;
    let release_db_id = crate::plugins::db::test_db::insert_release(&mut db, "Raw Genre Release")?;
    let db = std::sync::Arc::new(tokio::sync::RwLock::new(db));

    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("demo", &["lyra.genres"])]),
        default_server_info(),
        db,
    )?;
    let values = runtime.eval_plugin_source(
        "demo",
        "init.luau",
        format!(
            r#"
                local genres = require("@lyra/genres")
                local release_db_id = {release_db_id}

                local parent_id = genres.resolve({{
                    name = "Electronic",
                    aliases = {{ {{ name = "Electronica", locale = "en" }} }},
                }})
                local child_id = genres.add(release_db_id, {{
                    name = "Synthpop",
                    aliases = {{ {{ name = "Synth Pop" }} }},
                    external_id = {{
                        provider_id = "wikidata",
                        id_type = "qid",
                        id = "Q12345",
                    }},
                }})
                genres.add_parent(child_id, parent_id)

                local by_id = genres.get_by_id(child_id)
                local by_name = genres.find_by_name(" synthpop ")
                local parents = genres.get_parents(child_id)
                local children = genres.get_children(parent_id)
                local releases = genres.get_releases(child_id)
                local releases_many = genres.get_releases_many({{ child_id, parent_id, child_id }})
                local for_release = genres.get_for_release(release_db_id)
                local for_many = genres.get_for_releases_many({{ release_db_id }})

                return by_id.name,
                    by_name.name,
                    parents[1].name,
                    children[1].name,
                    releases[1],
                    releases_many[child_id][1],
                    for_release[1].name,
                    for_many[release_db_id][1].name,
                    #releases_many[parent_id]
            "#,
            release_db_id = release_db_id.0,
        )
        .into_bytes(),
    )?;

    assert_eq!(
        values,
        vec![
            luau::Value::String(b"Synthpop".to_vec()),
            luau::Value::String(b"Synthpop".to_vec()),
            luau::Value::String(b"Electronic".to_vec()),
            luau::Value::String(b"Synthpop".to_vec()),
            luau::Value::Integer(release_db_id.0),
            luau::Value::Integer(release_db_id.0),
            luau::Value::String(b"Synthpop".to_vec()),
            luau::Value::String(b"Synthpop".to_vec()),
            luau::Value::Number(0.0),
        ]
    );
    Ok(())
}

#[test]
fn plugin_executor_exposes_db_backed_lyra_tags_module() -> Result<()> {
    let mut db = crate::plugins::db::test_db::new_test_db()?;
    let user_db_id = crate::plugins::db::users::create(
        &mut db,
        &crate::plugins::db::test_db::test_user("raw-tags")?,
    )?;
    let track_db_id = crate::plugins::db::test_db::insert_track(&mut db, "Raw Tag Track")?;
    let db = std::sync::Arc::new(tokio::sync::RwLock::new(db));

    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("demo", &["lyra.tags"])]),
        default_server_info(),
        db,
    )?;
    runtime.run_plugin_source(
        "demo",
        "init.luau",
        format!(
            r##"
                local tags = require("@lyra/tags")
                local user_id = {user_db_id}
                local track_id = {track_db_id}

                executor_tag_name = tags.add(user_id, track_id, " Workout ", "#335577")
                executor_has = tags.has(user_id, track_id, "Workout")
                executor_has_many = tags.has_many(user_id, {{ track_id, -1, track_id, 999999 }}, "Workout")
                executor_for_target = tags.get_for_target(user_id, track_id)
                executor_for_targets = tags.get_for_targets_many(user_id, {{ track_id, 999999 }})
                executor_tagged = tags.get_tagged(user_id, "Workout")
                tags.remove(user_id, track_id, "Workout")
                executor_has_after_remove = tags.has(user_id, track_id, "Workout")
            "##,
            user_db_id = user_db_id.0,
            track_db_id = track_db_id.0,
        )
        .into_bytes(),
    )?;

    let values = runtime.eval_plugin_source(
        "demo",
        "check.luau",
        format!(
            r#"
                local track_id = {track_db_id}

                return executor_tag_name,
                    executor_has,
                    executor_has_many[track_id],
                    executor_has_many[999999] == false,
                    executor_for_target[1].tag,
                    executor_for_target[1].color,
                    executor_for_targets[track_id][1].tag,
                    executor_for_targets[999999] ~= nil and #executor_for_targets[999999] == 0,
                    executor_tagged[1],
                    executor_has_after_remove
            "#,
            track_db_id = track_db_id.0,
        )
        .into_bytes(),
    )?;
    assert_eq!(
        values,
        vec![
            luau::Value::String(b"Workout".to_vec()),
            luau::Value::Boolean(true),
            luau::Value::Boolean(true),
            luau::Value::Boolean(true),
            luau::Value::String(b"Workout".to_vec()),
            luau::Value::String(b"#335577".to_vec()),
            luau::Value::String(b"Workout".to_vec()),
            luau::Value::Boolean(true),
            luau::Value::Integer(track_db_id.0),
            luau::Value::Boolean(false),
        ]
    );
    Ok(())
}

#[test]
fn plugin_executor_exposes_db_backed_lyra_datastore_module() -> Result<()> {
    let db = crate::plugins::db::test_db::new_test_db()?;
    let db = std::sync::Arc::new(tokio::sync::RwLock::new(db));

    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("demo", &["lyra.datastore"])]),
        default_server_info(),
        db,
    )?;
    runtime.run_plugin_source(
        "demo",
        "init.luau",
        &br#"
            local datastore = require("@lyra/datastore")
            local store = datastore.get_or_create("raw-store")

            store:set("answer", { value = 42, tags = { "a", "b" } })
            executor_answer = store:get("answer")
            store:set_many({ alpha = true, beta = "two" })
            executor_many = store:get_many({ "alpha", "missing", "beta" })
            executor_removed = store:remove("beta")
            executor_cleared = store:clear()
        "#[..],
    )?;

    let values = runtime.eval_plugin_source(
        "demo",
        "check.luau",
        &br#"
            return executor_answer.value,
                executor_answer.tags[2],
                executor_many[1],
                executor_many[2] == nil,
                executor_many[3],
                executor_removed,
                executor_cleared
        "#[..],
    )?;
    assert_eq!(
        values,
        vec![
            luau::Value::Number(42.0),
            luau::Value::String(b"b".to_vec()),
            luau::Value::Boolean(true),
            luau::Value::Boolean(true),
            luau::Value::String(b"two".to_vec()),
            luau::Value::Boolean(true),
            luau::Value::Integer(2),
        ]
    );
    Ok(())
}
