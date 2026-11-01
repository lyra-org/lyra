use super::*;

#[test]
fn id_list_arguments_reject_entries_that_are_not_strings() -> Result<()> {
    let db = lyra_db::test_db::new_test_db()?;
    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("demo", &["lyra.tracks"])]),
        default_server_info(),
        lyra_db::DbAsync::new(db),
    )?;
    let values = runtime.eval_plugin_source(
        "demo",
        "init.luau",
        r#"
            local tracks = require("@lyra/tracks")
            local function rejected(list)
                local ok, err = pcall(tracks.get, list)
                return not ok and string.find(tostring(err), "must be a string", 1, true) ~= nil
            end
            return rejected({ "a", 1 }), rejected({ 0 }), rejected({ true }), rejected({ {} })
        "#
        .as_bytes()
        .to_vec(),
    )?;

    assert_eq!(values, vec![luau::Value::Boolean(true); 4]);
    Ok(())
}

#[test]
fn plugin_executor_exposes_db_backed_lyra_entities_module() -> Result<()> {
    let _guard = futures::executor::block_on(crate::services::testing::runtime_test_lock());
    crate::plugins::testing::init_default_test_state()?;
    let mut db = lyra_db::test_db::new_test_db()?;
    let track_db_id = lyra_db::fixtures::insert_track(&mut db, "Raw Entity Track")?;
    let track_public_id = lyra_db::lookup::find_id_by_db_id(&db, track_db_id)?
        .context("inserted track has public id")?;
    let db = lyra_db::DbAsync::new(db);

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
                local track_id = "{track_public_id}"

                local fetched = entities.query_track({{ id = track_id }})
                local many = entities.query_many({{ ids = {{ track_id, "missing" }} }})

                return fetched.entity.track_title,
                    fetched.entity.id,
                    many[track_id].entity.track_title,
                    many["missing"] == nil,
                    entities.get_type(track_id),
                    entities.get_type("missing") == nil,
                    entities.CreditType.Artist
            "#,
        )
        .into_bytes(),
    )?;

    assert_eq!(
        values,
        vec![
            luau::Value::String(b"Raw Entity Track".to_vec()),
            luau::Value::String(track_public_id.as_bytes().to_vec()),
            luau::Value::String(b"Raw Entity Track".to_vec()),
            luau::Value::Boolean(true),
            luau::Value::String(b"Track".to_vec()),
            luau::Value::Boolean(true),
            luau::Value::String(b"artist".to_vec()),
        ]
    );
    Ok(())
}

#[test]
fn plugin_executor_exposes_db_backed_lyra_chromaprint_module() -> Result<()> {
    let mut db = lyra_db::test_db::new_test_db()?;
    let entry = lyra_db::entries::Entry {
        db_id: None,
        id: nanoid::nanoid!(),
        full_path: std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/assets/metadata/integration_track.flac"),
        kind: lyra_db::entries::EntryKind::File,
        file_kind: Some("audio".to_string()),
        name: "integration_track.flac".to_string(),
        hash: None,
        size: 1,
        file_modified_at_ms: 1,
        file_created_at_ms: 1,
        synced_at_ms: None,
    };
    db.exec_mut(agdb::QueryBuilder::insert().element(&entry).query())?;
    let db = lyra_db::DbAsync::new(db);

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
                -- The one-second asset decodes but is too short to fingerprint.
                local ok, err = pcall(chromaprint.compute, "{entry_id}")
                executor_chromaprint_result =
                    not ok and string.find(tostring(err), "too short to fingerprint", 1, true) ~= nil
            "#,
            entry_id = entry.id,
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
    let mut db = lyra_db::test_db::new_test_db()?;
    lyra_db::roles::ensure_builtin_roles(&mut db)?;
    lyra_db::users::create(&mut db, &lyra_db::fixtures::test_user("bob")?)?;
    let alice = lyra_db::fixtures::test_user("alice")?;
    let alice_public_id = alice.id.clone();
    let alice_id = lyra_db::users::create(&mut db, &alice)?;
    lyra_db::roles::ensure_user_has_role(&mut db, alice_id, lyra_db::roles::BUILTIN_ADMIN_ROLE)?;
    let db = lyra_db::DbAsync::new(db);

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
            executor_users_first_id = listed[1].user_id
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
        &b"return executor_users_count, executor_users_first, executor_users_first_id, executor_users_first_role, executor_users_second, executor_users_second_role"[..],
    )?;
    assert_eq!(
        values,
        vec![
            luau::Value::Number(2.0),
            luau::Value::String(b"alice".to_vec()),
            luau::Value::String(alice_public_id.into_bytes()),
            luau::Value::String(b"admin".to_vec()),
            luau::Value::String(b"bob".to_vec()),
            luau::Value::Nil,
        ]
    );
    Ok(())
}

#[test]
fn plugin_executor_exposes_db_backed_lyra_listens_module() -> Result<()> {
    let mut db = lyra_db::test_db::new_test_db()?;
    let user = lyra_db::fixtures::test_user("listener")?;
    let user_public_id = user.id.clone();
    let user_db_id = lyra_db::users::create(&mut db, &user)?;
    let other_user_db_id = lyra_db::test_db::insert_user(&mut db, "other-listener")?;
    let track_db_id = lyra_db::fixtures::insert_track(&mut db, "Raw Listen Track")?;
    for (listener_db_id, started_at_ms) in [
        (user_db_id, 1000_u64),
        (user_db_id, 2500),
        (other_user_db_id, 3000),
    ] {
        lyra_db::fixtures::insert_listen(&mut db, listener_db_id, track_db_id, started_at_ms)?;
    }
    let track_id = public_id(&db, track_db_id)?;
    let db = lyra_db::DbAsync::new(db);

    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("demo", &["lyra.listens"])]),
        default_server_info(),
        db,
    )?;
    let mut context = CallContext {
        origin: plugin_origin("demo", "init.luau"),
        ..CallContext::default()
    };
    seed_caller_principal(
        &mut context,
        crate::services::auth::Principal::from_parts(
            user_db_id,
            user_public_id,
            "listener".to_string(),
            vec![lyra_db::Permission::Admin],
            Some("admin".to_string()),
            std::collections::HashSet::new(),
        ),
    );
    runtime.run_plugin_source_with_call_context(
        format!(
            r#"
                local listens = require("@lyra/listens")
                executor_listen_stats = listens.get({{ "{track_id}", "missing" }})
            "#,
        )
        .into_bytes(),
        context,
    )?;

    let values = runtime.eval_plugin_source(
        "demo",
        "check.luau",
        format!(
            r#"
                local track_id = "{track_id}"
                return executor_listen_stats[track_id].count,
                    executor_listen_stats[track_id].last_played_at_ms,
                    executor_listen_stats["missing"] == nil
            "#,
        )
        .into_bytes(),
    )?;
    assert_eq!(
        values,
        vec![
            luau::Value::Number(2.0),
            luau::Value::Number(2500.0),
            luau::Value::Boolean(true),
        ]
    );
    Ok(())
}

#[test]
fn removals_answer_hidden_targets_as_missing_and_still_clear_them() -> Result<()> {
    let mut db = lyra_db::test_db::new_test_db()?;
    let user = lyra_db::fixtures::test_user("hidden-removal-user")?;
    let user_public_id = user.id.clone();
    let user_db_id = lyra_db::users::create(&mut db, &user)?;
    let library_db_id = lyra_db::test_db::insert_library(&mut db, "Seen", "/tmp/hidden-removal")?;
    let library_public_id = public_id(&db, library_db_id)?;
    let hidden_db_id = lyra_db::fixtures::insert_track(&mut db, "Hidden Favorite")?;
    let untouched_db_id = lyra_db::fixtures::insert_track(&mut db, "Hidden Untouched")?;
    lyra_db::favorites::add(
        &mut db,
        user_db_id,
        hidden_db_id,
        lyra_db::favorites::FavoriteKind::Track,
        1,
    )?;
    let value = lyra_db::ratings::RatingValue::new(3).context("rating value")?;
    lyra_db::ratings::upsert(&mut db, user_db_id, hidden_db_id, value, 1)?;
    let hidden = public_id(&db, hidden_db_id)?;
    let untouched = public_id(&db, untouched_db_id)?;
    let db = lyra_db::DbAsync::new(db);

    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("demo", &["lyra.favorites", "lyra.ratings"])]),
        default_server_info(),
        db.clone(),
    )?;
    let mut context = CallContext {
        origin: plugin_origin("demo", "init.luau"),
        ..CallContext::default()
    };
    seed_caller_principal(
        &mut context,
        crate::services::auth::Principal::from_parts(
            user_db_id,
            user_public_id,
            "hidden-removal-user".to_string(),
            Vec::new(),
            None,
            std::collections::HashSet::from([library_public_id]),
        ),
    );
    runtime.run_plugin_source_with_call_context(
        format!(
            r#"
                local favorites = require("@lyra/favorites")
                local ratings = require("@lyra/ratings")
                executor_removals = {{
                    favorites.remove("{hidden}"),
                    ratings.remove("{hidden}"),
                    favorites.remove("{untouched}"),
                    ratings.remove("{untouched}"),
                    favorites.remove("missing-target"),
                    ratings.remove("missing-target"),
                }}
            "#,
        )
        .into_bytes(),
        context,
    )?;

    let values = runtime.eval_plugin_source(
        "demo",
        "check.luau",
        &b"return table.unpack(executor_removals)"[..],
    )?;
    assert_eq!(
        values,
        vec![luau::Value::Boolean(false); 6],
        "a hidden target answers as a missing one, whether or not the caller marked it"
    );
    let db = futures::executor::block_on(db.read());
    assert!(
        !lyra_db::favorites::has(&*db, user_db_id, hidden_db_id)?,
        "the caller's favorite of a hidden target is still removed"
    );
    assert!(
        lyra_db::ratings::get(&*db, user_db_id, hidden_db_id)?.is_none(),
        "the caller's rating of a hidden target is still cleared"
    );
    Ok(())
}

#[test]
fn plugin_executor_exposes_db_backed_lyra_favorites_module() -> Result<()> {
    let mut db = lyra_db::test_db::new_test_db()?;
    let user = lyra_db::fixtures::test_user("favorite-user")?;
    let user_public_id = user.id.clone();
    let user_db_id = lyra_db::users::create(&mut db, &user)?;
    let library_db_id = lyra_db::test_db::insert_library(&mut db, "Raw Favorites", "/tmp/raw-fav")?;
    let library_public_id = lyra_db::lookup::find_id_by_db_id(&db, library_db_id)?
        .context("inserted library has public id")?;
    let track_db_id = lyra_db::fixtures::insert_track(&mut db, "Raw Favorite Track")?;
    lyra_db::fixtures::connect(&mut db, library_db_id, track_db_id)?;
    let track_id = public_id(&db, track_db_id)?;
    let db = lyra_db::DbAsync::new(db);

    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("demo", &["lyra.favorites"])]),
        default_server_info(),
        db,
    )?;
    let mut context = CallContext {
        origin: plugin_origin("demo", "init.luau"),
        ..CallContext::default()
    };
    seed_caller_principal(
        &mut context,
        crate::services::auth::Principal::from_parts(
            user_db_id,
            user_public_id,
            "favorite-user".to_string(),
            Vec::new(),
            None,
            std::collections::HashSet::from([library_public_id]),
        ),
    );
    runtime.run_plugin_source_with_call_context(
        format!(
            r#"
                local favorites = require("@lyra/favorites")
                local track_id = "{track_id}"

                executor_favorite_add = favorites.add(track_id)
                executor_favorite_times = favorites.get({{ track_id, track_id, "missing" }})
                executor_favorite_ids = favorites.list_ids("track")
                executor_favorite_remove = favorites.remove(track_id)
                executor_favorite_after_remove = favorites.get({{ track_id }})
            "#,
        )
        .into_bytes(),
        context,
    )?;

    let values = runtime.eval_plugin_source(
        "demo",
        "check.luau",
        format!(
            r#"
                local track_id = "{track_id}"
                return executor_favorite_add,
                    type(executor_favorite_times[track_id]) == "number",
                    executor_favorite_times["missing"] == nil,
                    executor_favorite_ids[1],
                    executor_favorite_remove,
                    executor_favorite_after_remove[track_id] == nil
            "#,
        )
        .into_bytes(),
    )?;
    assert_eq!(
        values,
        vec![
            luau::Value::Boolean(true),
            luau::Value::Boolean(true),
            luau::Value::Boolean(true),
            luau::Value::String(track_id.into_bytes()),
            luau::Value::Boolean(true),
            luau::Value::Boolean(true),
        ]
    );
    Ok(())
}

#[test]
fn plugin_executor_exposes_db_backed_lyra_artists_module() -> Result<()> {
    let mut db = lyra_db::test_db::new_test_db()?;
    let library_db_id =
        lyra_db::test_db::insert_library(&mut db, "Raw Artists Library", "/tmp/raw-artists")?;
    let release_db_id = lyra_db::fixtures::insert_release(&mut db, "Raw Artists Release")?;
    lyra_db::fixtures::connect(&mut db, library_db_id, release_db_id)?;
    let artist_db_id = lyra_db::fixtures::insert_artist(&mut db, "Raw Artist Module")?;
    let mut artist =
        lyra_db::artists::get_by_id(&db, artist_db_id)?.context("inserted artist exists")?;
    artist.set_artist_type(lyra_db::ArtistType::Person);
    lyra_db::artists::update(&mut db, &artist)?;
    lyra_db::test_db::connect_artist(&mut db, release_db_id, artist_db_id)?;
    let actor_db_id = lyra_db::fixtures::insert_artist(&mut db, "Raw Voice Actor")?;
    let character_db_id = lyra_db::fixtures::insert_artist(&mut db, "Raw Character")?;
    lyra_db::artists::relations::link(
        &mut db,
        actor_db_id,
        character_db_id,
        lyra_db::ArtistRelationType::VoiceActor,
        Some("Lead".to_string()),
    )?;
    let library_id = public_id(&db, library_db_id)?;
    let release_id = public_id(&db, release_db_id)?;
    let actor_id = public_id(&db, actor_db_id)?;
    let artist_id = public_id(&db, artist_db_id)?;
    let db = lyra_db::DbAsync::new(db);

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
                local library_id = "{library_id}"
                local release_id = "{release_id}"
                local actor_id = "{actor_id}"
                local artist_id = "{artist_id}"

                local fetched = artists.get({{ artist_id }})
                local by_library = artists.query({{ library_id = library_id }})
                local many = artists.by_release({{ release_id }})
                local relations = artists.relations({{ actor_id }})
                local queried = artists.query({{
                    search = "Module",
                    artist_type = artists.ArtistType.Person,
                    sort = {{ {{ key = "name" }} }},
                }})
                local credited = artists.query({{
                    release_ids = {{ release_id }},
                    artist_type = artists.ArtistType.Person,
                    credit_types = {{ artists.CreditType.Artist }},
                }})

                return artists.ArtistType.Person,
                    artists.ArtistRelationType.VoiceActor,
                    artists.CreditType.Artist,
                    fetched[artist_id].artist_name,
                    by_library.items[1].artist_name,
                    many[release_id][1].artist_name,
                    relations[actor_id][1].relation_type,
                    relations[actor_id][1].direction,
                    relations[actor_id][1].artist.artist_name,
                    queried.items[1].artist_name,
                    credited.items[1].artist_name,
                    queried.total,
                    credited.total
            "#,
        )
        .into_bytes(),
    )?;

    let mut values = values;
    let artist_type = crate::plugins::types::LuauArtistType::class().read_value(
        &runtime.vm,
        "artist_type",
        values.remove(0),
    )?;
    let relation_type = crate::plugins::types::LuauArtistRelationType::class().read_value(
        &runtime.vm,
        "relation_type",
        values.remove(0),
    )?;
    let credit_type = crate::plugins::types::LuauCreditType::class().read_value(
        &runtime.vm,
        "credit_type",
        values.remove(0),
    )?;
    assert_eq!(
        lyra_db::ArtistType::from(artist_type),
        lyra_db::ArtistType::Person
    );
    assert_eq!(
        lyra_db::ArtistRelationType::from(relation_type),
        lyra_db::ArtistRelationType::VoiceActor
    );
    assert_eq!(
        lyra_db::CreditType::from(credit_type),
        lyra_db::CreditType::Artist
    );
    assert_eq!(
        values,
        vec![
            luau::Value::String(b"Raw Artist Module".to_vec()),
            luau::Value::String(b"Raw Artist Module".to_vec()),
            luau::Value::String(b"Raw Artist Module".to_vec()),
            luau::Value::String(b"voice_actor".to_vec()),
            luau::Value::String(b"outgoing".to_vec()),
            luau::Value::String(b"Raw Character".to_vec()),
            luau::Value::String(b"Raw Artist Module".to_vec()),
            luau::Value::String(b"Raw Artist Module".to_vec()),
            luau::Value::Number(1.0),
            luau::Value::Number(1.0),
        ]
    );
    Ok(())
}

#[test]
fn plugin_executor_exposes_db_backed_lyra_tracks_module() -> Result<()> {
    let mut db = lyra_db::test_db::new_test_db()?;
    let release_db_id = lyra_db::fixtures::insert_release(&mut db, "Raw Track Release")?;
    let track_db_id = lyra_db::fixtures::insert_track(&mut db, "Raw Track Module Song")?;
    db.exec_mut(
        agdb::QueryBuilder::insert()
            .edges()
            .from(release_db_id)
            .to(track_db_id)
            .query(),
    )?;
    let track_public_id = public_id(&db, track_db_id)?;
    let release_id = public_id(&db, release_db_id)?;
    let db = lyra_db::DbAsync::new(db);

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
                local release_id = "{release_id}"
                local track_id = "{track_public_id}"

                local all = tracks.query({{}})
                local fetched = tracks.get({{ track_id }})
                local related = tracks.by_release({{ release_id }})
                local queried = tracks.query({{
                    release_ids = {{ release_id }},
                    search = "Module",
                    sort = {{ {{ key = "name" }} }},
                    limit = 5,
                }})

                return all.items[1] ~= nil,
                    fetched[track_id].id,
                    related[release_id][1].track_title,
                    queried.items[1].track_title,
                    queried.total,
                    queried.offset
            "#,
        )
        .into_bytes(),
    )?;

    assert_eq!(
        values,
        vec![
            luau::Value::Boolean(true),
            luau::Value::String(track_public_id.as_bytes().to_vec()),
            luau::Value::String(b"Raw Track Module Song".to_vec()),
            luau::Value::String(b"Raw Track Module Song".to_vec()),
            luau::Value::Number(1.0),
            luau::Value::Number(0.0),
        ]
    );
    Ok(())
}

#[test]
fn plugin_executor_exposes_db_backed_lyra_track_sources_module() -> Result<()> {
    let mut db = lyra_db::test_db::new_test_db()?;
    let track_db_id = lyra_db::fixtures::insert_track(&mut db, "Raw Source Track")?;
    let entry = lyra_db::entries::Entry {
        db_id: None,
        id: nanoid::nanoid!(),
        full_path: std::path::PathBuf::from("/music/raw-source.FLAC"),
        kind: lyra_db::entries::EntryKind::File,
        file_kind: Some("audio".to_string()),
        name: "raw-source.FLAC".to_string(),
        hash: None,
        size: 1,
        file_modified_at_ms: 1,
        file_created_at_ms: 1,
        synced_at_ms: None,
    };
    let entry_db_id = db
        .exec_mut(agdb::QueryBuilder::insert().element(&entry).query())?
        .ids()[0];
    let source_key = format!("entry:{}:embedded", entry_db_id.0);
    db.transaction_mut(|t| {
        lyra_db::track_sources::upsert(
            t,
            track_db_id,
            entry_db_id,
            lyra_db::track_sources::TrackSourceUpsert {
                source_kind: "embedded_tags".to_string(),
                source_key: source_key.clone(),
                is_primary: true,
                start_ms: None,
                end_ms: None,
                format: Default::default(),
            },
            None,
        )
    })?;
    let track_id = public_id(&db, track_db_id)?;
    let db = lyra_db::DbAsync::new(db);

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

                return track_sources.get_primary_source_key("{track_id}"),
                    track_sources.get_primary_source_key("missing") == nil
            "#,
        )
        .into_bytes(),
    )?;

    assert_eq!(
        values,
        vec![
            luau::Value::String(source_key.into_bytes()),
            luau::Value::Boolean(true),
        ]
    );
    Ok(())
}

#[test]
fn plugin_executor_exposes_db_backed_lyra_playback_sources_module() -> Result<()> {
    let mut db = lyra_db::test_db::new_test_db()?;
    let user = lyra_db::fixtures::test_user("playback-source-user")?;
    let user_public_id = user.id.clone();
    let user_db_id = lyra_db::users::create(&mut db, &user)?;
    let track_db_id = lyra_db::fixtures::insert_track(&mut db, "Raw Playback Source Track")?;
    let entry_path = std::env::temp_dir().join(format!(
        "lyra-raw-playback-source-{}.flac",
        nanoid::nanoid!()
    ));
    std::fs::write(&entry_path, b"plugin playback source")?;
    let entry = lyra_db::entries::Entry {
        db_id: None,
        id: nanoid::nanoid!(),
        full_path: entry_path.clone(),
        kind: lyra_db::entries::EntryKind::File,
        file_kind: Some("audio".to_string()),
        name: "raw-playback-source.flac".to_string(),
        hash: Some("raw-playback-hash".to_string()),
        size: 19,
        file_modified_at_ms: 321,
        file_created_at_ms: 654,
        synced_at_ms: None,
    };
    let entry_db_id = db
        .exec_mut(agdb::QueryBuilder::insert().element(&entry).query())?
        .ids()[0];
    let library_db_id = lyra_db::test_db::insert_library(&mut db, "Music", "/music")?;
    lyra_db::graph::ensure_owned_edge(&mut db, library_db_id, entry_db_id)?;
    let source_key = format!("entry:{}:embedded", entry_db_id.0);
    let source_id = db.transaction_mut(|t| {
        lyra_db::track_sources::upsert(
            t,
            track_db_id,
            entry_db_id,
            lyra_db::track_sources::TrackSourceUpsert {
                source_kind: "embedded_tags".to_string(),
                source_key: source_key.clone(),
                is_primary: true,
                start_ms: Some(100),
                end_ms: Some(200),
                format: lyra_db::track_sources::SourceFormat {
                    container: Some(lyra_db::track_sources::SourceContainer::M4a),
                    codec: Some(lyra_db::track_sources::SourceCodec::Alac),
                    ..Default::default()
                },
            },
            None,
        )
    })?;
    let track_id = public_id(&db, track_db_id)?;
    let source_id = public_id(&db, source_id)?;
    let db = lyra_db::DbAsync::new(db);

    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("demo", &["lyra.playback_sources"])]),
        default_server_info(),
        db,
    )?;
    let mut context = CallContext {
        origin: plugin_origin("demo", "init.luau"),
        ..CallContext::default()
    };
    seed_caller_principal(
        &mut context,
        crate::services::auth::Principal::from_parts(
            user_db_id,
            user_public_id,
            "playback-source-user".to_string(),
            vec![
                lyra_db::Permission::Admin,
                lyra_db::Permission::ManageLibraries,
            ],
            Some("admin".to_string()),
            std::collections::HashSet::new(),
        ),
    );
    runtime.run_plugin_source_with_call_context(
        format!(
            r#"
                local playback_sources = require("@lyra/playback_sources")
                local track_id = "{track_id}"

                executor_playback_source_rows = playback_sources.get(track_id, true)
                executor_playback_source_many = playback_sources.get_many({{ track_id, track_id, "missing" }}, true)
            "#,
        )
        .into_bytes(),
        context,
    )?;

    let values = runtime.eval_plugin_source(
        "demo",
        "check.luau",
        format!(
            r#"
                local first = executor_playback_source_rows[1]
                local many = executor_playback_source_many["{track_id}"]

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
                    first.container,
                    first.codec,
                    many.entry.name,
                    executor_playback_source_many["missing"] == nil
            "#,
        )
        .into_bytes(),
    )?;
    let _ = std::fs::remove_file(&entry_path);

    assert_eq!(
        values,
        vec![
            luau::Value::String(track_id.into_bytes()),
            luau::Value::String(source_id.into_bytes()),
            luau::Value::String(b"embedded_tags".to_vec()),
            luau::Value::String(source_key.into_bytes()),
            luau::Value::Boolean(true),
            luau::Value::Number(100.0),
            luau::Value::Number(200.0),
            luau::Value::Boolean(true),
            luau::Value::String(b"raw-playback-source.flac".to_vec()),
            luau::Value::String(b"raw-playback-hash".to_vec()),
            luau::Value::String(entry_path.to_string_lossy().into_owned().into_bytes()),
            luau::Value::String(b"m4a".to_vec()),
            luau::Value::String(b"alac".to_vec()),
            luau::Value::String(b"raw-playback-source.flac".to_vec()),
            luau::Value::Boolean(true),
        ]
    );
    Ok(())
}

#[test]
fn plugin_executor_exposes_db_backed_lyra_playlists_module() -> Result<()> {
    let mut db = lyra_db::test_db::new_test_db()?;
    let user = lyra_db::fixtures::test_user("playlist-user")?;
    let user_public_id = user.id.clone();
    let expected_owner = user_public_id.clone();
    let user_db_id = lyra_db::users::create(&mut db, &user)?;
    let track_db_id = lyra_db::fixtures::insert_track(&mut db, "Raw Playlist Track")?;
    let track_id = public_id(&db, track_db_id)?;
    let db = lyra_db::DbAsync::new(db);

    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("demo", &["lyra.playlists"])]),
        default_server_info(),
        db,
    )?;
    let mut context = CallContext {
        origin: plugin_origin("demo", "init.luau"),
        ..CallContext::default()
    };
    seed_caller_principal(
        &mut context,
        crate::services::auth::Principal::from_parts(
            user_db_id,
            user_public_id,
            "playlist-user".to_string(),
            vec![lyra_db::Permission::Admin],
            Some("admin".to_string()),
            std::collections::HashSet::new(),
        ),
    );
    runtime.run_plugin_source_with_call_context(
        format!(
            r#"
                local playlists = require("@lyra/playlists")
                local track_id = "{track_id}"

                executor_playlist_id = playlists.create({{
                    name = "Raw Playlist",
                    description = "seed",
                    is_public = false,
                    created_at_ms = 10,
                    updated_at_ms = 20,
                }})
                executor_playlist = playlists.get_by_id(executor_playlist_id)
                executor_playlist_owner = playlists.get_owner(executor_playlist_id)
                executor_user_playlists = playlists.list_owned()
                executor_playlist_entry_id = playlists.add_track(executor_playlist_id, track_id)
                executor_playlist_tracks = playlists.get_tracks(executor_playlist_id)
                executor_playlist_tracks_many = playlists.get_tracks_many({{ executor_playlist_id, executor_playlist_id }})
                executor_updated_playlist = playlists.update({{
                    playlist_id = executor_playlist_id,
                    name = "Raw Updated Playlist",
                    description = "changed",
                    is_public = true,
                    updated_at_ms = 30,
                }})
                playlists.remove_track(executor_playlist_id, executor_playlist_entry_id)
                executor_playlist_tracks_after_remove = playlists.get_tracks(executor_playlist_id)
            "#,
        )
        .into_bytes(),
        context,
    )?;

    let values = runtime.eval_plugin_source(
        "demo",
        "check.luau",
        r#"
                return executor_playlist_id ~= nil,
                    executor_playlist.name,
                    executor_playlist.id == executor_playlist_id,
                    executor_playlist_owner,
                    executor_user_playlists[1].name,
                    executor_playlist_entry_id ~= nil,
                    executor_playlist_tracks[1].track_id,
                    executor_playlist_tracks[1].entry_id ~= nil,
                    executor_playlist_tracks_many[executor_playlist_id][1].track_id,
                    executor_updated_playlist.name,
                    executor_updated_playlist.is_public,
                    executor_playlist_tracks_after_remove[1] == nil
            "#
        .to_string()
        .into_bytes(),
    )?;

    assert_eq!(
        values,
        vec![
            luau::Value::Boolean(true),
            luau::Value::String(b"Raw Playlist".to_vec()),
            luau::Value::Boolean(true),
            luau::Value::String(expected_owner.into_bytes()),
            luau::Value::String(b"Raw Playlist".to_vec()),
            luau::Value::Boolean(true),
            luau::Value::String(track_id.clone().into_bytes()),
            luau::Value::Boolean(true),
            luau::Value::String(track_id.into_bytes()),
            luau::Value::String(b"Raw Updated Playlist".to_vec()),
            luau::Value::Boolean(true),
            luau::Value::Boolean(true),
        ]
    );
    Ok(())
}

#[test]
fn plugin_executor_exposes_db_backed_lyra_releases_module() -> Result<()> {
    let mut db = lyra_db::test_db::new_test_db()?;
    let release_db_id = lyra_db::fixtures::insert_release(&mut db, "Raw Release Module")?;
    lyra_db::fixtures::insert_release(&mut db, "Other Release")?;
    let track_db_id = lyra_db::fixtures::insert_track(&mut db, "Raw Release Track")?;
    lyra_db::fixtures::connect(&mut db, release_db_id, track_db_id)?;
    let album_artist_id = lyra_db::fixtures::insert_artist(&mut db, "Raw Album Artist")?;
    lyra_db::test_db::connect_artist(&mut db, release_db_id, album_artist_id)?;
    let guest_artist_id = lyra_db::fixtures::insert_artist(&mut db, "Raw Guest Artist")?;
    lyra_db::test_db::connect_artist(&mut db, track_db_id, guest_artist_id)?;
    let release_id = public_id(&db, release_db_id)?;
    let track_id = public_id(&db, track_db_id)?;
    let album_artist_id = public_id(&db, album_artist_id)?;
    let guest_artist_id = public_id(&db, guest_artist_id)?;
    let db = lyra_db::DbAsync::new(db);

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
                local release_id = "{release_id}"
                local track_id = "{track_id}"
                local album_artist_id = "{album_artist_id}"
                local guest_artist_id = "{guest_artist_id}"

                local fetched = releases.get({{ release_id }})
                local all = releases.query({{}})
                local by_artist = releases.query({{
                    artist_ids = {{ album_artist_id }},
                    credit_role = "release",
                }})
                local appearances = releases.query({{
                    artist_ids = {{ guest_artist_id }},
                    credit_role = "track",
                    exclude_credit_role = "release",
                }})
                local many = releases.by_track({{ track_id }})
                local queried = releases.query({{
                    search = "Module",
                    sort = {{ {{ key = "name" }} }},
                    limit = 5,
                }})

                return fetched[release_id].release_title,
                    all.total,
                    by_artist.items[1].release_title,
                    appearances.items[1].release_title,
                    many[track_id][1].release_title,
                    queried.items[1].release_title,
                    queried.total,
                    queried.offset
            "#,
        )
        .into_bytes(),
    )?;

    assert_eq!(
        values,
        vec![
            luau::Value::String(b"Raw Release Module".to_vec()),
            luau::Value::Number(2.0),
            luau::Value::String(b"Raw Release Module".to_vec()),
            luau::Value::String(b"Raw Release Module".to_vec()),
            luau::Value::String(b"Raw Release Module".to_vec()),
            luau::Value::String(b"Raw Release Module".to_vec()),
            luau::Value::Number(1.0),
            luau::Value::Number(0.0),
        ]
    );
    Ok(())
}

#[test]
fn plugin_executor_exposes_db_backed_lyra_libraries_module() -> Result<()> {
    let mut db = lyra_db::test_db::new_test_db()?;
    let library_db_id = lyra_db::test_db::insert_library(&mut db, "Raw Library", "/tmp/raw-lib")?;
    let track_db_id = lyra_db::fixtures::insert_track(&mut db, "Raw Library Track")?;
    lyra_db::fixtures::connect(&mut db, library_db_id, track_db_id)?;
    let library_id = public_id(&db, library_db_id)?;
    let track_id = public_id(&db, track_db_id)?;
    let db = lyra_db::DbAsync::new(db);

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
                local library_id = "{library_id}"
                local track_id = "{track_id}"

                local all = libraries.list()
                local by_id = libraries.list(library_id)
                local for_entity = libraries.get_for_entity(track_id)
                local many = libraries.get_for_entities({{ track_id, library_id, "missing" }})

                return all[1].name,
                    by_id[1].path,
                    by_id[1].id,
                    #libraries.list("missing"),
                    for_entity[1].name,
                    many[track_id].name,
                    many[library_id].name,
                    many["missing"] == nil
            "#,
        )
        .into_bytes(),
    )?;

    assert_eq!(
        values,
        vec![
            luau::Value::String(b"Raw Library".to_vec()),
            luau::Value::String(b"/tmp/raw-lib".to_vec()),
            luau::Value::String(library_id.into_bytes()),
            luau::Value::Number(0.0),
            luau::Value::String(b"Raw Library".to_vec()),
            luau::Value::String(b"Raw Library".to_vec()),
            luau::Value::String(b"Raw Library".to_vec()),
            luau::Value::Boolean(true),
        ]
    );
    Ok(())
}

#[test]
fn libraries_expose_only_accessible_libraries_to_a_dispatch_principal() -> Result<()> {
    let mut db = lyra_db::test_db::new_test_db()?;
    let shared_db_id = lyra_db::test_db::insert_library(&mut db, "Shared Library", "/tmp/shared")?;
    let hidden_db_id = lyra_db::test_db::insert_library(&mut db, "Hidden Library", "/tmp/hidden")?;
    let shared_track_db_id = lyra_db::fixtures::insert_track(&mut db, "Shared Track")?;
    let hidden_track_db_id = lyra_db::fixtures::insert_track(&mut db, "Hidden Track")?;
    lyra_db::fixtures::connect(&mut db, shared_db_id, shared_track_db_id)?;
    lyra_db::fixtures::connect(&mut db, hidden_db_id, hidden_track_db_id)?;
    let shared_public_id = public_id(&db, shared_db_id)?;
    let hidden_public_id = public_id(&db, hidden_db_id)?;
    let shared_track_id = public_id(&db, shared_track_db_id)?;
    let hidden_track_id = public_id(&db, hidden_track_db_id)?;
    let db = lyra_db::DbAsync::new(db);

    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("demo", &["lyra.libraries"])]),
        default_server_info(),
        db,
    )?;
    let mut context = CallContext {
        origin: plugin_origin("demo", "init.luau".to_string()),
        ..CallContext::default()
    };
    seed_caller_principal(
        &mut context,
        crate::services::auth::Principal::from_parts(
            agdb::DbId(7),
            "user-public-id".to_string(),
            "viewer".to_string(),
            Vec::new(),
            None,
            std::collections::HashSet::from([shared_public_id.clone()]),
        ),
    );
    let values = runtime.eval_plugin_source_with_call_context(
        format!(
            r#"
                local libraries = require("@lyra/libraries")
                local all = libraries.list()
                local for_entity = libraries.get_for_entity("{shared_track_id}")
                local many = libraries.get_for_entities({{ "{shared_track_id}", "{hidden_track_id}" }})
                return #all,
                    all[1].id,
                    all[1].path,
                    for_entity[1].id,
                    many["{shared_track_id}"].id,
                    many["{hidden_track_id}"],
                    #libraries.list("{hidden_public_id}"),
                    #libraries.get_for_entity("{hidden_track_id}")
            "#,
        )
        .into_bytes(),
        context,
    )?;

    let shared_id = luau::Value::String(shared_public_id.into_bytes());
    assert_eq!(
        values,
        vec![
            luau::Value::Number(1.0),
            shared_id.clone(),
            luau::Value::Nil,
            shared_id.clone(),
            shared_id,
            luau::Value::Nil,
            luau::Value::Number(0.0),
            luau::Value::Number(0.0),
        ]
    );
    Ok(())
}

#[test]
fn libraries_refuse_a_client_dispatch_without_a_principal() -> Result<()> {
    let mut db = lyra_db::test_db::new_test_db()?;
    let library_db_id =
        lyra_db::test_db::insert_library(&mut db, "Private Library", "/tmp/private")?;
    let track_db_id = lyra_db::fixtures::insert_track(&mut db, "Private Track")?;
    lyra_db::fixtures::connect(&mut db, library_db_id, track_db_id)?;
    let track_id = public_id(&db, track_db_id)?;
    let db = lyra_db::DbAsync::new(db);

    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("demo", &["lyra.libraries"])]),
        default_server_info(),
        db,
    )?;
    let mut context = CallContext {
        origin: plugin_origin("demo", "init.luau".to_string()),
        ..CallContext::default()
    };
    context
        .caller
        .insert(crate::plugins::auth::DispatchAuth::default());
    let values = runtime.eval_plugin_source_with_call_context(
        format!(
            r#"
                local libraries = require("@lyra/libraries")
                local function refused(f, ...)
                    local ok, err = pcall(f, ...)
                    return not ok and string.find(tostring(err), "no authenticated caller", 1, true) ~= nil
                end
                return refused(libraries.list),
                    refused(libraries.get_for_entity, "{track_id}"),
                    refused(libraries.get_for_entities, {{ "{track_id}" }})
            "#,
        )
        .into_bytes(),
        context,
    )?;

    assert_eq!(values, vec![luau::Value::Boolean(true); 3]);
    Ok(())
}

#[test]
fn plugin_executor_exposes_db_backed_lyra_genres_module() -> Result<()> {
    let mut db = lyra_db::test_db::new_test_db()?;
    let release_db_id = lyra_db::fixtures::insert_release(&mut db, "Raw Genre Release")?;
    lyra_db::genres::sync_release_genres(&mut db, release_db_id, &["Synthpop".to_string()])?;
    let release_id = public_id(&db, release_db_id)?;
    let db = lyra_db::DbAsync::new(db);

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
                local release_id = "{release_id}"

                local parent_id = genres.resolve({{
                    name = "Electronic",
                    aliases = {{ {{ name = "Electronica", locale = "en" }} }},
                }})
                local child_id = genres.resolve({{
                    name = "Synthpop",
                    aliases = {{ {{ name = "Synth Pop" }} }},
                    external_id = {{
                        provider_id = "wikidata",
                        id_type = "qid",
                        id_value = "Q12345",
                    }},
                }})
                genres.add_parent(child_id, parent_id)

                local by_id = genres.get({{ child_id, parent_id }})
                local by_name = genres.find_by_name(" synthpop ")
                local queried = genres.query({{ search = "synth" }})
                local for_release = genres.by_release({{ release_id }})

                return by_id[child_id].name,
                    by_id[parent_id] == nil,
                    by_name.name,
                    queried.items[1].name,
                    queried.total,
                    for_release[release_id][1].name
            "#,
        )
        .into_bytes(),
    )?;

    assert_eq!(
        values,
        vec![
            luau::Value::String(b"Synthpop".to_vec()),
            luau::Value::Boolean(true),
            luau::Value::String(b"Synthpop".to_vec()),
            luau::Value::String(b"Synthpop".to_vec()),
            luau::Value::Number(1.0),
            luau::Value::String(b"Synthpop".to_vec()),
        ]
    );
    Ok(())
}

#[test]
fn plugin_executor_exposes_db_backed_lyra_tags_module() -> Result<()> {
    let mut db = lyra_db::test_db::new_test_db()?;
    let user = lyra_db::fixtures::test_user("raw-tags")?;
    let user_public_id = user.id.clone();
    let user_db_id = lyra_db::users::create(&mut db, &user)?;
    let track_db_id = lyra_db::fixtures::insert_track(&mut db, "Raw Tag Track")?;
    let suffix = nanoid::nanoid!();
    let library_path = std::path::PathBuf::from(format!("/tmp/lyra-plugin-tags-{suffix}"));
    let library = lyra_db::libraries::create_with_creator(
        &mut db,
        lyra_db::libraries::LibraryInsert {
            id: nanoid::nanoid!(),
            name: format!("Plugin Tags {suffix}"),
            path_key: lyra_db::libraries::path_key_for(&library_path),
            path: library_path,
            language: None,
            country: None,
        },
        user_db_id,
    )?;
    db.exec_mut(
        agdb::QueryBuilder::insert()
            .edges()
            .from(library.db_id.context("library db id")?)
            .to(track_db_id)
            .query(),
    )?;
    let track_id = public_id(&db, track_db_id)?;
    let db = lyra_db::DbAsync::new(db);

    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("demo", &["lyra.tags"])]),
        default_server_info(),
        db,
    )?;
    let mut context = CallContext {
        origin: plugin_origin("demo", "init.luau"),
        ..CallContext::default()
    };
    seed_caller_principal(
        &mut context,
        crate::services::auth::Principal::from_parts(
            user_db_id,
            user_public_id,
            "raw-tags".to_string(),
            Vec::new(),
            None,
            std::collections::HashSet::from([library.id.clone()]),
        ),
    );
    runtime.run_plugin_source_with_call_context(
        format!(
            r##"
                local tags = require("@lyra/tags")
                local track_id = "{track_id}"

                executor_tag_name = tags.add(track_id, " Workout ", "#335577")
                executor_has = tags.has(track_id, "Workout")
                executor_has_many = tags.has_many({{ track_id, track_id, "missing" }}, "Workout")
                executor_for_target = tags.get_for_target(track_id)
                executor_for_targets = tags.get_for_targets_many({{ track_id, "missing" }})
                executor_tagged = tags.get_tagged("Workout")
                tags.remove(track_id, "Workout")
                executor_has_after_remove = tags.has(track_id, "Workout")
            "##,
        )
        .into_bytes(),
        context,
    )?;

    let values = runtime.eval_plugin_source(
        "demo",
        "check.luau",
        format!(
            r#"
                local track_id = "{track_id}"

                return executor_tag_name,
                    executor_has,
                    executor_has_many[track_id],
                    executor_has_many["missing"] == nil,
                    executor_for_target[1].tag,
                    executor_for_target[1].color,
                    executor_for_targets[track_id][1].tag,
                    executor_for_targets["missing"] == nil,
                    executor_tagged[1],
                    executor_has_after_remove
            "#,
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
            luau::Value::String(track_id.into_bytes()),
            luau::Value::Boolean(false),
        ]
    );
    Ok(())
}

#[test]
fn plugin_executor_exposes_db_backed_lyra_datastore_module() -> Result<()> {
    let db = lyra_db::test_db::new_test_db()?;
    let db = lyra_db::DbAsync::new(db);

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
            luau::Value::Number(2.0),
        ]
    );
    Ok(())
}

#[test]
fn plugin_executor_reports_playback_acceptance_rejection_and_internal_failure() -> Result<()> {
    let _guard = futures::executor::block_on(crate::services::testing::runtime_test_lock());
    crate::plugins::testing::init_default_test_state()?;
    let mut db = lyra_db::test_db::new_test_db()?;
    let user = lyra_db::fixtures::test_user("report-user")?;
    let user_db_id = lyra_db::users::create(&mut db, &user)?;
    let track_db_id = lyra_db::fixtures::insert_track(&mut db, "Reported Track")?;
    let track_id = public_id(&db, track_db_id)?;
    let db = lyra_db::DbAsync::new(db);
    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("reporter", &["lyra.playback_sessions"])]),
        default_server_info(),
        db.clone(),
    )?;
    let mut context = CallContext {
        origin: plugin_origin("reporter", "init.luau"),
        ..CallContext::default()
    };
    seed_caller_principal(
        &mut context,
        crate::services::auth::Principal::from_parts(
            user_db_id,
            user.id,
            user.username,
            vec![lyra_db::Permission::Admin],
            Some("admin".into()),
            Default::default(),
        ),
    );
    runtime.run_plugin_source_with_call_context(
        format!(
            r#"
        local sessions = require("@lyra/playback_sessions")
        function report(key, state)
            return sessions.report_session({{
                track_id = "{track_id}", session_key = key, state = state,
                client_name = "  Kitchen  ",
            }})
        end
        accepted_id, accepted_error = report("player", "playing")
        ignored_id, ignored_error = report("unknown", "stopped")
    "#,
        )
        .into_bytes(),
        context.clone(),
    )?;
    let (root, quota_message) = futures::executor::block_on(async {
        let mut db = db.write().await;
        let now = crate::services::playback_sessions::now_ms()?;
        let root = lyra_db::playbacks::get_reported(&*db, user_db_id, "reporter", "player", now)?
            .context("accepted report creates durable root")?;
        let session_id = lyra_db::playbacks::get_current_session_id(&*db, root.db_id.unwrap())?
            .context("accepted root has accounting session")?;
        let session = lyra_db::playback_sessions::get_by_id(&*db, session_id)?
            .context("accounting session is stored")?;
        assert_eq!(session.client_name.as_deref(), Some("Kitchen"));
        assert_eq!(session.plugin_id.as_deref(), Some("reporter"));
        for index in 0..lyra_db::playbacks::MAX_PLAYBACKS_PER_USER {
            let result = db.transaction_mut(|db| {
                lyra_db::playbacks::insert_reported(
                    db,
                    user_db_id,
                    "reporter",
                    &format!("seed-{index}"),
                    session_id,
                    now,
                )
            });
            if let Err(error) = result {
                assert!(error.is::<lyra_db::playbacks::ReportedPlaybackLimitReached>());
                return Ok::<_, anyhow::Error>((root, error.to_string()));
            }
        }
        anyhow::bail!("reported context admission did not reach its bound")
    })?;
    runtime.run_plugin_source_with_call_context(
        b"rejected_id, rejected_error = report('extra', 'playing'); continued = true".to_vec(),
        context.clone(),
    )?;
    let values = runtime.eval_plugin_source("reporter", "check.luau", b"return accepted_id, accepted_error, ignored_id, ignored_error, rejected_id, rejected_error, continued".to_vec())?;
    assert_eq!(
        values,
        vec![
            luau::Value::String(root.id.into_bytes()),
            luau::Value::Nil,
            luau::Value::Nil,
            luau::Value::Nil,
            luau::Value::Nil,
            luau::Value::String(quota_message.into_bytes()),
            luau::Value::Boolean(true),
        ]
    );
    futures::executor::block_on(async {
        db.write().await.exec_mut(
            agdb::QueryBuilder::remove()
                .values([agdb::DbValue::from("updated_at_ms")])
                .ids(root.db_id.unwrap())
                .query(),
        )
    })?;
    let error = runtime
        .run_plugin_source_with_call_context(b"report('player', 'playing')".to_vec(), context)
        .expect_err("malformed storage must raise, not become an admission rejection");
    assert!(format!("{error:#}").contains("updated_at_ms"));
    Ok(())
}

/// Deletes a user and creates another that reuses its `DbId`, returning the deleted user's
/// principal alongside the new user's id.
fn recycled_user_principal(
    db: &mut agdb::DbAny,
    accessible_library_ids: std::collections::HashSet<String>,
) -> Result<(crate::services::auth::Principal, agdb::DbId)> {
    let stale = lyra_db::fixtures::test_user("deleted-user")?;
    let stale_public_id = stale.id.clone();
    let stale_db_id = lyra_db::users::create(db, &stale)?;
    lyra_db::users::delete_user(db, stale_db_id)?;
    let current_db_id = lyra_db::test_db::insert_user(db, "current-user")?;
    assert_eq!(current_db_id, stale_db_id, "agdb reuses freed ids");
    Ok((
        crate::services::auth::Principal::from_parts(
            stale_db_id,
            stale_public_id,
            "deleted-user".to_string(),
            Vec::new(),
            None,
            accessible_library_ids,
        ),
        current_db_id,
    ))
}

#[test]
fn stale_principal_cannot_use_favorites_of_a_user_with_its_recycled_db_id() -> Result<()> {
    let mut db = lyra_db::test_db::new_test_db()?;
    let library_db_id =
        lyra_db::test_db::insert_library(&mut db, "Stale Favorites", "/tmp/stale-fav")?;
    let library_public_id = lyra_db::lookup::find_id_by_db_id(&db, library_db_id)?
        .context("inserted library has public id")?;
    let track_db_id = lyra_db::fixtures::insert_track(&mut db, "Favored Track")?;
    let other_track_db_id = lyra_db::fixtures::insert_track(&mut db, "Other Track")?;
    lyra_db::fixtures::connect(&mut db, library_db_id, track_db_id)?;
    lyra_db::fixtures::connect(&mut db, library_db_id, other_track_db_id)?;
    let (stale, current_db_id) = recycled_user_principal(
        &mut db,
        std::collections::HashSet::from([library_public_id]),
    )?;
    lyra_db::favorites::add(
        &mut db,
        current_db_id,
        track_db_id,
        lyra_db::favorites::FavoriteKind::Track,
        1,
    )?;
    let track = public_id(&db, track_db_id)?;
    let other = public_id(&db, other_track_db_id)?;
    let db = lyra_db::DbAsync::new(db);

    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("demo", &["lyra.favorites"])]),
        default_server_info(),
        db.clone(),
    )?;
    let mut context = CallContext {
        origin: plugin_origin("demo", "stale.luau"),
        ..CallContext::default()
    };
    seed_caller_principal(&mut context, stale);
    let values = runtime.eval_plugin_source_with_call_context(
        format!(
            r#"
                local favorites = require("@lyra/favorites")
                local function rejects(f, ...)
                    local ok, err = pcall(f, ...)
                    return not ok
                        and string.find(tostring(err), "invalid bearer credential", 1, true) ~= nil
                end
                return rejects(favorites.get, {{ "{track}" }}),
                    rejects(favorites.list_ids, "track"),
                    rejects(favorites.add, "{other}"),
                    rejects(favorites.remove, "{track}")
            "#,
        )
        .into_bytes(),
        context,
    )?;
    assert_eq!(values, vec![luau::Value::Boolean(true); 4]);

    let db = futures::executor::block_on(db.read());
    assert!(lyra_db::favorites::has(&*db, current_db_id, track_db_id)?);
    assert!(!lyra_db::favorites::has(
        &*db,
        current_db_id,
        other_track_db_id
    )?);
    Ok(())
}

#[test]
fn stale_principal_cannot_use_playlists_of_a_user_with_its_recycled_db_id() -> Result<()> {
    let mut db = lyra_db::test_db::new_test_db()?;
    let (stale, current_db_id) =
        recycled_user_principal(&mut db, std::collections::HashSet::new())?;
    let playlist_db_id = crate::services::playlists::create(
        &mut db,
        &crate::services::playlists::CreatePlaylistRequest {
            user_db_id: current_db_id,
            name: "Private".to_string(),
            description: None,
            is_public: Some(false),
            created_at_ms: Some(1),
            updated_at_ms: Some(1),
            track_db_ids: Vec::new(),
        },
    )?;
    let playlist_id = public_id(&db, playlist_db_id)?;
    let db = lyra_db::DbAsync::new(db);

    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("demo", &["lyra.playlists"])]),
        default_server_info(),
        db.clone(),
    )?;
    let mut context = CallContext {
        origin: plugin_origin("demo", "stale.luau"),
        ..CallContext::default()
    };
    seed_caller_principal(&mut context, stale);
    let values = runtime.eval_plugin_source_with_call_context(
        format!(
            r#"
                local playlists = require("@lyra/playlists")
                local playlist_id = "{playlist_id}"
                local function rejects(f, ...)
                    return not pcall(f, ...)
                end
                return rejects(playlists.get_by_id, playlist_id),
                    rejects(playlists.list_owned),
                    rejects(playlists.list),
                    rejects(playlists.update, {{ playlist_id = playlist_id, name = "Taken" }}),
                    rejects(playlists.delete, playlist_id),
                    rejects(playlists.create, {{ name = "Stale" }})
            "#,
        )
        .into_bytes(),
        context,
    )?;
    assert_eq!(values, vec![luau::Value::Boolean(true); 6]);

    let db = futures::executor::block_on(db.read());
    let playlists = crate::services::playlists::get_by_user(&db, current_db_id)?;
    assert_eq!(playlists.len(), 1);
    assert_eq!(playlists[0].name, "Private");
    Ok(())
}

#[test]
fn stale_principal_cannot_report_playback_for_a_user_with_its_recycled_db_id() -> Result<()> {
    let _guard = futures::executor::block_on(crate::services::testing::runtime_test_lock());
    crate::plugins::testing::init_default_test_state()?;
    let mut db = lyra_db::test_db::new_test_db()?;
    let track_db_id = lyra_db::fixtures::insert_track(&mut db, "Reported Track")?;
    let track_id = public_id(&db, track_db_id)?;
    let (mut stale, current_db_id) = recycled_user_principal(&mut db, Default::default())?;
    stale.permissions = vec![lyra_db::Permission::Admin];
    let db = lyra_db::DbAsync::new(db);
    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("reporter", &["lyra.playback_sessions"])]),
        default_server_info(),
        db.clone(),
    )?;
    let mut context = CallContext {
        origin: plugin_origin("reporter", "init.luau"),
        ..CallContext::default()
    };
    seed_caller_principal(&mut context, stale);
    let values = runtime.eval_plugin_source_with_call_context(
        format!(
            r#"
                local sessions = require("@lyra/playback_sessions")
                local ok, err = pcall(sessions.report_session, {{
                    track_id = "{track_id}", session_key = "player", state = "playing",
                }})
                return ok, string.find(tostring(err), "invalid bearer credential", 1, true) ~= nil
            "#,
        )
        .into_bytes(),
        context,
    )?;
    assert_eq!(
        values,
        vec![luau::Value::Boolean(false), luau::Value::Boolean(true)]
    );
    let reported = futures::executor::block_on(async {
        let db = db.read().await;
        let now = crate::services::playback_sessions::now_ms()?;
        lyra_db::playbacks::get_reported(&*db, current_db_id, "reporter", "player", now)
    })?;
    assert!(reported.is_none());
    Ok(())
}

#[test]
fn entities_links_include_resolves_templates_and_functions() -> Result<()> {
    let _guard = futures::executor::block_on(crate::services::testing::runtime_test_lock());
    crate::plugins::testing::init_default_test_state()?;
    let release = futures::executor::block_on(async {
        use lyra_db::external_ids::{
            self,
            IdSource,
        };
        use lyra_db::test_db;
        let mut db = crate::services::STATE.db.write().await;
        let library = test_db::insert_library(&mut db, "Link Library", "/tmp/lyra-link-library")?;
        db.exec_mut(
            agdb::QueryBuilder::insert()
                .values_uniform([("language", "jpn").into(), ("country", "JP").into()])
                .ids(library)
                .query(),
        )?;
        let release = lyra_db::fixtures::insert_release(&mut db, "Link Release")?;
        let track = lyra_db::fixtures::insert_track(&mut db, "Link Track")?;
        lyra_db::fixtures::connect(&mut db, library, release)?;
        lyra_db::fixtures::connect(&mut db, release, track)?;
        for (node, id_type, value) in [
            (release, "thing_id", "r1"),
            (release, "localized_id", "l1"),
            (release, "nil_id", "n1"),
            (release, "broken_id", "b1"),
            (track, "thing_id", "t1"),
        ] {
            external_ids::upsert(
                &mut db,
                node,
                "link-provider",
                id_type,
                value,
                IdSource::Resolved,
            )?;
        }
        public_id(&*db, release)
    })?;
    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest("demo", &["lyra.metadata", "lyra.entities"])]),
        default_server_info(),
        crate::services::STATE.db.get(),
    )?;
    let source = format!(
        r#"
            local metadata = require("@lyra/metadata")
            local entities = require("@lyra/entities")
            local ET = metadata.EntityType
            local provider = metadata.Provider.new("link-provider", {{ display_name = "Link Site" }})
            provider:id({{ id_type = "thing_id", entity = ET.Release, scheme = "example:thing", label = "Thing Site" }},
                "https://example.test/release/{{id}}")
            provider:id({{ id_type = "thing_id", entity = ET.Track, scheme = "example:thing" }},
                "https://example.test/track/{{id}}")
            provider:id({{ id_type = "localized_id", entity = ET.Release }}, function(id, ctx)
                seen_entity = ctx.entity
                return `https://example.test/{{ctx.library.language}}-{{ctx.library.country}}/{{ctx.id_type}}/{{id}}?thing={{ctx.external_ids.thing_id}}`
            end)
            provider:id({{ id_type = "nil_id", entity = ET.Release }}, function()
                return nil
            end)
            provider:id({{ id_type = "broken_id", entity = ET.Release }}, function()
                error("boom")
            end)

            local without = entities.query_release({{ id = "{release}" }})
            local with = entities.query_release({{ id = "{release}", include = {{ "links", "tracks" }} }})
            local urls = {{}}
            for _, link in with.includes.links do
                table.insert(urls, link.url)
            end
            local first = with.includes.links[1]
            return without.includes.links == nil,
                table.concat(urls, " "),
                first.label,
                first.id_type,
                first.id,
                with.includes.tracks[1].links[1].url,
                with.includes.links[2].scheme,
                with.includes.links[2].label
        "#,
    );

    let values = runtime.eval_plugin_source("demo", "init.luau", source.as_bytes())?;

    let text = |value: &str| luau::Value::String(value.as_bytes().to_vec());
    assert_eq!(
        values,
        vec![
            luau::Value::Boolean(true),
            text(
                "https://example.test/jpn-JP/localized_id/l1?thing=r1 https://example.test/release/r1"
            ),
            text("Link Site"),
            text("localized_id"),
            text("l1"),
            text("https://example.test/track/t1"),
            text("example:thing"),
            text("Thing Site"),
        ]
    );
    let seen = runtime.eval_plugin_source("demo", "seen.luau", &b"return seen_entity"[..])?;
    let entity = crate::plugins::types::LuauEntityType::class().read_value(
        &runtime.vm,
        "entity",
        seen[0].clone(),
    )?;
    assert_eq!(
        crate::services::EntityType::from(entity),
        crate::services::EntityType::Release
    );
    Ok(())
}

fn insert_release_with_ids(provider_id: &str, ids: &[(&str, &str)]) -> Result<String> {
    futures::executor::block_on(async {
        use lyra_db::external_ids::{
            self,
            IdSource,
        };

        let mut db = crate::services::STATE.db.write().await;
        let release = lyra_db::fixtures::insert_release(&mut db, "Link Release")?;
        for (id_type, value) in ids {
            external_ids::upsert(
                &mut db,
                release,
                provider_id,
                id_type,
                value,
                IdSource::Resolved,
            )?;
        }
        public_id(&*db, release)
    })
}

fn link_runtime() -> Result<PluginExecutor> {
    PluginExecutor::with_database(
        Arc::from(vec![manifest(
            "demo",
            &["harmony.task", "lyra.metadata", "lyra.entities"],
        )]),
        default_server_info(),
        crate::services::STATE.db.get(),
    )
}

fn release_link_urls(runtime: &PluginExecutor, release: &str) -> Result<String> {
    let values = runtime.eval_plugin_source(
        "demo",
        "links.luau",
        format!(
            r#"
                local entities = require("@lyra/entities")
                local release = entities.query_release({{ id = "{release}", include = "links" }})
                local urls = {{}}
                for _, link in release.includes.links do
                    table.insert(urls, link.url)
                end
                return table.concat(urls, " ")
            "#,
        )
        .as_bytes(),
    )?;
    match values.first() {
        Some(luau::Value::String(urls)) => Ok(String::from_utf8(urls.clone())?),
        other => anyhow::bail!("expected link urls, got {other:?}"),
    }
}

#[test]
fn entities_links_isolate_failing_slow_and_unsafe_generators() -> Result<()> {
    let _guard = futures::executor::block_on(crate::services::testing::runtime_test_lock());
    crate::plugins::testing::init_default_test_state()?;
    let release = insert_release_with_ids(
        "isolated-links",
        &[
            ("busy_id", "b"),
            ("z_slow_id", "s"),
            ("unsafe_id", "u"),
            ("ok_id", "o"),
            ("template_id", "a/b c"),
        ],
    )?;
    let runtime = link_runtime()?;
    runtime.run_plugin_source(
        "demo",
        "init.luau",
        &br#"
            local task = require("@harmony/task")
            local entities = require("@lyra/entities")
            local metadata = require("@lyra/metadata")
            local ET = metadata.EntityType
            local provider = metadata.Provider.new("isolated-links")
            busy_runs = 0
            slow_runs = 0
            provider:id({ id_type = "busy_id", entity = ET.Release }, function()
                busy_runs += 1
                while true do
                end
            end)
            provider:id({ id_type = "z_slow_id", entity = ET.Release }, function(id)
                slow_runs += 1
                task.spawn(function()
                    local ok, err = pcall(entities.query, { id = "any", include = "links" })
                    spawned_links_error = if ok then "allowed" else tostring(err)
                end)
                task.wait(60)
                return `https://example.test/slow/{id}`
            end)
            provider:id({ id_type = "unsafe_id", entity = ET.Release }, function(id)
                return `javascript:alert("{id}")`
            end)
            provider:id({ id_type = "ok_id", entity = ET.Release }, function(id)
                return `https://example.test/ok/{id}`
            end)
            provider:id({ id_type = "template_id", entity = ET.Release }, "https://example.test/t/{id}")
        "#[..],
    )?;

    let started = std::time::Instant::now();
    let urls = release_link_urls(&runtime, &release)?;
    assert!(started.elapsed() >= crate::services::providers::ID_LINK_BATCH_TIMEOUT);
    assert!(started.elapsed() < std::time::Duration::from_secs(10));
    assert_eq!(
        urls,
        "https://example.test/ok/o https://example.test/t/a%2Fb%20c"
    );
    let spawned =
        runtime.eval_plugin_source("demo", "spawned.luau", &b"return spawned_links_error"[..])?;
    assert!(
        matches!(&spawned[0], luau::Value::String(error)
            if String::from_utf8_lossy(error).contains("cannot be requested from an id link generator")),
        "{spawned:?}"
    );

    assert_eq!(release_link_urls(&runtime, &release)?, urls);
    let runs =
        runtime.eval_plugin_source("demo", "runs.luau", &b"return busy_runs, slow_runs"[..])?;
    assert_eq!(
        runs,
        vec![luau::Value::Number(1.0), luau::Value::Number(2.0)],
        "the busy generator is suspended; the one cut off by the batch deadline is not"
    );
    Ok(())
}

#[test]
fn entities_links_skip_generators_registered_on_another_vm() -> Result<()> {
    let _guard = futures::executor::block_on(crate::services::testing::runtime_test_lock());
    crate::plugins::testing::init_default_test_state()?;
    let release = insert_release_with_ids("vm-links", &[("thing_id", "x")])?;
    let register = |runtime: &PluginExecutor, site: &str| {
        runtime.run_plugin_source(
            "demo",
            "init.luau",
            format!(
                r#"
                    local metadata = require("@lyra/metadata")
                    local provider = metadata.Provider.new("vm-links")
                    provider:id({{ id_type = "thing_id", entity = metadata.EntityType.Release }}, function(id)
                        return `https://{site}.example.test/{{id}}`
                    end)
                "#
            )
            .into_bytes(),
        )
    };
    let old = link_runtime()?;
    register(&old, "old")?;
    assert_eq!(
        release_link_urls(&old, &release)?,
        "https://old.example.test/x"
    );

    let plugin_id = crate::services::plugin_scope::PluginId::new("demo")?;
    futures::executor::block_on(crate::services::providers::teardown_plugin_providers(
        &plugin_id,
    ));
    let new = link_runtime()?;
    register(&new, "new")?;

    assert_eq!(
        release_link_urls(&old, &release)?,
        "",
        "the old VM must not run the new VM's handler ids"
    );
    assert_eq!(
        release_link_urls(&new, &release)?,
        "https://new.example.test/x"
    );
    Ok(())
}

#[test]
fn entities_links_time_out_and_suspend_a_busy_generator_within_the_batch_deadline() -> Result<()> {
    let _guard = futures::executor::block_on(crate::services::testing::runtime_test_lock());
    crate::plugins::testing::init_default_test_state()?;
    let releases = (0..40)
        .map(|index| {
            let id = index.to_string();
            insert_release_with_ids("busy-links", &[("busy_id", &id), ("ok_id", &id)])
        })
        .collect::<Result<Vec<_>>>()?;
    let runtime = link_runtime()?;
    runtime.run_plugin_source(
        "demo",
        "init.luau",
        &br#"
            local metadata = require("@lyra/metadata")
            local ET = metadata.EntityType
            local provider = metadata.Provider.new("busy-links")
            busy_runs = 0
            provider:id({ id_type = "busy_id", entity = ET.Release }, function()
                busy_runs += 1
                while true do
                end
            end)
            provider:id({ id_type = "ok_id", entity = ET.Release }, function(id)
                return `https://example.test/ok/{id}`
            end)
        "#[..],
    )?;
    let busy_runs = || -> Result<Vec<luau::Value>> {
        runtime.eval_plugin_source("demo", "busy_runs.luau", &b"return busy_runs"[..])
    };
    let ids = releases
        .iter()
        .map(|release| format!(r#""{release}""#))
        .collect::<Vec<_>>()
        .join(", ");

    let link_count = || {
        runtime.eval_plugin_source(
            "demo",
            "links.luau",
            format!(
                r#"
                    local entities = require("@lyra/entities")
                    local projections = entities.query_many({{ ids = {{ {ids} }}, include = "links" }})
                    local count = 0
                    for _, projection in projections do
                        count += #projection.includes.links
                    end
                    return count
                "#
            )
            .as_bytes(),
        )
    };
    let all_ok_links = vec![luau::Value::Number(releases.len() as f64)];

    let started = std::time::Instant::now();
    assert_eq!(link_count()?, all_ok_links);
    assert!(
        started.elapsed() < crate::services::providers::ID_LINK_BATCH_TIMEOUT,
        "took {:?}",
        started.elapsed()
    );
    assert_eq!(
        busy_runs()?,
        vec![luau::Value::Number(1.0)],
        "an interrupted generator must count as timed out, so its other calls are skipped"
    );

    assert_eq!(link_count()?, all_ok_links);
    assert_eq!(
        busy_runs()?,
        vec![luau::Value::Number(1.0)],
        "a timed-out generator is suspended"
    );
    Ok(())
}
