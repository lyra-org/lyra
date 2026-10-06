use super::*;
use crate::plugins::db::{
    self,
    test_db,
};

fn tracks_runtime(db: agdb::DbAny) -> Result<PluginExecutor> {
    PluginExecutor::with_database(
        Arc::from(vec![manifest(
            "demo",
            &[
                "lyra.artists",
                "lyra.genres",
                "lyra.releases",
                "lyra.tracks",
            ],
        )]),
        default_server_info(),
        std::sync::Arc::new(tokio::sync::RwLock::new(db)),
    )
}

fn track_titles(values: Vec<luau::Value>) -> Vec<String> {
    values
        .into_iter()
        .map(|value| match value {
            luau::Value::String(bytes) => String::from_utf8(bytes).expect("utf-8 title"),
            other => panic!("expected a title, got {other:?}"),
        })
        .collect()
}

#[test]
fn plugin_track_query_sorts_a_release_in_album_order() -> Result<()> {
    let mut db = test_db::new_test_db()?;
    let release_db_id = test_db::insert_release(&mut db, "Album Order")?;
    for (title, disc, track) in [("Third", 2, 1), ("Second", 1, 2), ("First", 1, 1)] {
        let track_db_id = test_db::insert_track(&mut db, title)?;
        let mut stored =
            db::tracks::get_by_id(&db, track_db_id)?.context("inserted track exists")?;
        stored.disc = Some(disc);
        stored.track = Some(track);
        db::tracks::update(&mut db, &stored)?;
        test_db::connect(&mut db, release_db_id, track_db_id)?;
    }

    let runtime = tracks_runtime(db)?;
    let values = runtime.eval_plugin_source(
        "demo",
        "init.luau",
        format!(
            r#"
                local tracks = require("@lyra/tracks")
                local result = tracks.query({{ release_ids = {{ {release_db_id} }} }})
                local titles = {{}}
                for _, track in result.items do
                    table.insert(titles, track.track_title)
                end
                return table.unpack(titles)
            "#,
            release_db_id = release_db_id.0,
        )
        .into_bytes(),
    )?;

    assert_eq!(track_titles(values), vec!["First", "Second", "Third"]);
    Ok(())
}

#[test]
fn plugin_track_query_ranks_search_matches_by_relevance() -> Result<()> {
    let mut db = test_db::new_test_db()?;
    for title in ["Blue", "A b l u e"] {
        test_db::insert_track(&mut db, title)?;
    }

    let runtime = tracks_runtime(db)?;
    let values = runtime.eval_plugin_source(
        "demo",
        "init.luau",
        r#"
            local tracks = require("@lyra/tracks")
            local result = tracks.query({ search = "blue" })
            local titles = {}
            for _, track in result.items do
                table.insert(titles, track.track_title)
            end
            return table.unpack(titles)
        "#
        .as_bytes()
        .to_vec(),
    )?;

    assert_eq!(track_titles(values), vec!["Blue", "A b l u e"]);
    Ok(())
}

#[test]
fn plugin_track_query_follows_the_dispatch_principal() -> Result<()> {
    let mut db = test_db::new_test_db()?;
    let visible_library = test_db::insert_library(&mut db, "Visible", "/tmp/lyra-query-visible")?;
    let hidden_library = test_db::insert_library(&mut db, "Hidden", "/tmp/lyra-query-hidden")?;
    for (library, title) in [(visible_library, "Visible"), (hidden_library, "Hidden")] {
        let release = test_db::insert_release(&mut db, title)?;
        let track = test_db::insert_track(&mut db, title)?;
        test_db::connect(&mut db, library, release)?;
        test_db::connect(&mut db, release, track)?;
    }
    let user = test_db::insert_user(&mut db, "query-viewer")?;
    let visible_library_id = db::libraries::get_by_id(&db, visible_library)?
        .context("library exists")?
        .id;
    let principal = crate::services::auth::Principal::for_user(
        &db,
        user,
        Vec::new(),
        std::collections::HashSet::from([visible_library_id]),
    );
    let runtime = tracks_runtime(db)?;
    let source = r#"
        local tracks = require("@lyra/tracks")
        local result = tracks.query({})
        local titles = {}
        for _, track in result.items do
            table.insert(titles, track.track_title)
        end
        return table.unpack(titles)
    "#;

    let mut context = CallContext {
        origin: plugin_origin("demo", "init.luau".to_string()),
        ..CallContext::default()
    };
    seed_caller_principal(&mut context, principal);
    let values =
        runtime.eval_plugin_source_with_call_context(source.as_bytes().to_vec(), context)?;
    assert_eq!(track_titles(values), vec!["Visible"]);

    let system = runtime.eval_plugin_source("demo", "init.luau", source.as_bytes().to_vec())?;
    assert_eq!(track_titles(system), vec!["Hidden", "Visible"]);

    let values = runtime.eval_plugin_source(
        "demo",
        "init.luau",
        r#"
            local tracks = require("@lyra/tracks")
            local ok, err = pcall(tracks.query, { favorite = true })
            return not ok and string.find(tostring(err), "needs a user", 1, true) ~= nil
        "#
        .as_bytes()
        .to_vec(),
    )?;
    assert_eq!(values, vec![luau::Value::Boolean(true)]);

    let mut unauthenticated = CallContext {
        origin: plugin_origin("demo", "init.luau".to_string()),
        ..CallContext::default()
    };
    unauthenticated
        .caller
        .insert(crate::plugins::auth::DispatchAuth::default());
    let values = runtime.eval_plugin_source_with_call_context(
        r#"
            local tracks = require("@lyra/tracks")
            return pcall(tracks.query, {})
        "#
        .as_bytes()
        .to_vec(),
        unauthenticated,
    )?;
    assert_eq!(values[0], luau::Value::Boolean(false));
    Ok(())
}

/// A small catalog queried by an admin who favorited Alpha and listened to Bravo.
///
/// "Album" is credited to Amy and holds Alpha (1999, credited to Amy), Bravo (2001) and Charlie
/// (2001). "Other" holds Delta (credited to Amy) and Echo (credited to Zed).
struct Catalog {
    runtime: PluginExecutor,
    admin: crate::services::auth::Principal,
    album: agdb::DbId,
    amy: agdb::DbId,
    zed: agdb::DbId,
}

fn catalog() -> Result<Catalog> {
    let mut db = test_db::new_test_db()?;
    let admin_db_id = test_db::insert_user(&mut db, "catalog-admin")?;
    let amy = test_db::insert_artist(&mut db, "Amy")?;
    let zed = test_db::insert_artist(&mut db, "Zed")?;
    let album = test_db::insert_release(&mut db, "Album")?;
    let other = test_db::insert_release(&mut db, "Other")?;
    test_db::connect_artist(&mut db, album, amy)?;
    let mut tracks = std::collections::HashMap::new();
    for (release, title, year, artist) in [
        (album, "Alpha", 1999, Some(amy)),
        (album, "Bravo", 2001, None),
        (album, "Charlie", 2001, None),
        (other, "Delta", 2005, Some(amy)),
        (other, "Echo", 2005, Some(zed)),
    ] {
        let track = test_db::insert_track(&mut db, title)?;
        let mut stored = db::tracks::get_by_id(&db, track)?.context("track exists")?;
        stored.year = Some(year);
        db::tracks::update(&mut db, &stored)?;
        test_db::connect(&mut db, release, track)?;
        if let Some(artist) = artist {
            test_db::connect_artist(&mut db, track, artist)?;
        }
        tracks.insert(title, track);
    }
    db::genres::sync_release_genres(&mut db, album, &["Rock".to_string()])?;
    db::genres::sync_release_genres(
        &mut db,
        other,
        &["Rockabilly".to_string(), "Jazz".to_string()],
    )?;
    db::favorites::add(
        &mut db,
        admin_db_id,
        tracks["Alpha"],
        db::favorites::FavoriteKind::Track,
        1,
    )?;
    let bravo_public_id = db::tracks::get_by_id(&db, tracks["Bravo"])?
        .context("track exists")?
        .id;
    crate::plugins::db::listens::create_and_mark_recorded(
        &mut db,
        &crate::plugins::db::listens::Listen {
            db_id: None,
            id: nanoid::nanoid!(),
            track_public_id: bravo_public_id,
            position_ms: 0,
            duration_ms: Some(180_000),
            activity_ms: 180_000,
            state: crate::plugins::db::PlaybackState::Completed,
            listened_at_ms: 1_000,
            created_at_ms: 1_000,
        },
        tracks["Bravo"],
        admin_db_id,
        &crate::services::playback_sessions::PlaybackSession {
            db_id: None,
            id: nanoid::nanoid!(),
            client_name: None,
            position_ms: 0,
            duration_ms: Some(180_000),
            activity_ms: Some(180_000),
            last_position_ms: None,
            state: crate::plugins::db::PlaybackState::Completed,
            listen_recorded: Some(true),
            updated_at_ms: 1_000,
            created_at_ms: 1_000,
        },
    )?;
    let admin = crate::services::auth::Principal::for_user(
        &db,
        admin_db_id,
        vec![db::Permission::Admin],
        Default::default(),
    );
    Ok(Catalog {
        runtime: tracks_runtime(db)?,
        admin,
        album,
        amy,
        zed,
    })
}

impl Catalog {
    /// The titles `tracks.query(<query>)` returns for the admin.
    fn titles(&self, query: &str) -> Result<Vec<String>> {
        self.names("tracks", "track_title", query)
    }

    /// The names `artists.query(<query>)` returns for the admin.
    fn artist_names(&self, query: &str) -> Result<Vec<String>> {
        self.names("artists", "artist_name", query)
    }

    /// The names `genres.query(<query>)` returns for the admin.
    fn genre_names(&self, query: &str) -> Result<Vec<String>> {
        self.names("genres", "name", query)
    }

    /// The titles `releases.query(<query>)` returns for the admin.
    fn release_titles(&self, query: &str) -> Result<Vec<String>> {
        self.names("releases", "release_title", query)
    }

    fn names(&self, module: &str, name: &str, query: &str) -> Result<Vec<String>> {
        names_as(&self.runtime, &self.admin, module, name, query)
    }
}

/// The `name` field of each item `<module>.query(<query>)` returns for `principal`.
fn names_as(
    runtime: &PluginExecutor,
    principal: &crate::services::auth::Principal,
    module: &str,
    name: &str,
    query: &str,
) -> Result<Vec<String>> {
    let mut context = CallContext {
        origin: plugin_origin("demo", "init.luau"),
        ..CallContext::default()
    };
    seed_caller_principal(&mut context, principal.clone());
    let values = runtime.eval_plugin_source_with_call_context(
        format!(
            r#"
                local catalog = require("@lyra/{module}")
                local page = catalog.query({query})
                local names = {{}}
                for _, item in page.items do
                    table.insert(names, item.{name})
                end
                return table.unpack(names)
            "#
        )
        .into_bytes(),
        context,
    )?;
    Ok(track_titles(values))
}

#[test]
fn plugin_catalog_query_scopes_a_library_to_what_the_principal_sees() -> Result<()> {
    let mut db = test_db::new_test_db()?;
    let visible = test_db::insert_library(&mut db, "Visible", "/tmp/lyra-scope-visible")?;
    let hidden = test_db::insert_library(&mut db, "Hidden", "/tmp/lyra-scope-hidden")?;
    for (libraries, title, artist) in [
        (vec![visible], "Seen", "Ann"),
        (vec![hidden], "Unseen", "Hal"),
        (vec![visible, hidden], "Shared", "Sam"),
    ] {
        let release = test_db::insert_release(&mut db, title)?;
        let track = test_db::insert_track(&mut db, title)?;
        let artist = test_db::insert_artist(&mut db, artist)?;
        for library in libraries {
            test_db::connect(&mut db, library, release)?;
        }
        test_db::connect(&mut db, release, track)?;
        test_db::connect_artist(&mut db, release, artist)?;
    }
    let user = test_db::insert_user(&mut db, "scope-viewer")?;
    let admin_user = test_db::insert_user(&mut db, "scope-admin")?;
    let visible_id = db::libraries::get_by_id(&db, visible)?
        .context("library exists")?
        .id;
    let viewer = crate::services::auth::Principal::for_user(
        &db,
        user,
        Vec::new(),
        std::collections::HashSet::from([visible_id]),
    );
    let admin = crate::services::auth::Principal::for_user(
        &db,
        admin_user,
        vec![db::Permission::Admin],
        Default::default(),
    );
    let runtime = tracks_runtime(db)?;
    let releases =
        |principal, query: &str| names_as(&runtime, principal, "releases", "release_title", query);
    let (visible, hidden) = (visible.0, hidden.0);

    assert_eq!(releases(&viewer, "{}")?, ["Seen", "Shared"]);
    assert_eq!(
        releases(&viewer, &format!("{{ library_id = {visible} }}"))?,
        ["Seen", "Shared"]
    );
    // A release in a library the viewer can't see stays visible through another library.
    assert_eq!(
        releases(&viewer, &format!("{{ library_id = {hidden} }}"))?,
        ["Shared"]
    );
    assert!(releases(&viewer, "{ library_id = 999999 }")?.is_empty());
    assert_eq!(
        releases(&admin, &format!("{{ library_id = {hidden} }}"))?,
        ["Shared", "Unseen"]
    );
    assert_eq!(
        names_as(
            &runtime,
            &viewer,
            "tracks",
            "track_title",
            &format!("{{ library_id = {hidden} }}")
        )?,
        ["Shared"]
    );
    let artists = |query: &str| names_as(&runtime, &viewer, "artists", "artist_name", query);
    assert_eq!(artists("{}")?, ["Ann", "Sam"]);
    assert_eq!(artists(&format!("{{ library_id = {hidden} }}"))?, ["Sam"]);
    Ok(())
}

#[test]
fn track_query_filters_by_credit_role() -> Result<()> {
    let catalog = catalog()?;
    let amy = format!("artist_ids = {{ {} }}", catalog.amy.0);
    for (role, expected) in [
        ("", vec!["Alpha", "Bravo", "Charlie", "Delta"]),
        (r#", credit_role = "track""#, vec!["Alpha", "Delta"]),
        (
            r#", credit_role = "release""#,
            vec!["Alpha", "Bravo", "Charlie"],
        ),
        (
            r#", credit_role = "track", exclude_credit_role = "release""#,
            vec!["Delta"],
        ),
    ] {
        assert_eq!(
            catalog.titles(&format!("{{ {amy}{role} }}"))?,
            expected,
            "{role}"
        );
    }
    Ok(())
}

#[test]
fn track_query_bounds_the_sort_name() -> Result<()> {
    let catalog = catalog()?;
    assert_eq!(
        catalog.titles(r#"{ sort_name_prefix = "B" }"#)?,
        vec!["Bravo"]
    );
    assert_eq!(
        catalog.titles(r#"{ sort_name_at_least = "charlie" }"#)?,
        vec!["Charlie", "Delta", "Echo"]
    );
    assert_eq!(
        catalog.titles(r#"{ sort_name_below = "C" }"#)?,
        vec!["Alpha", "Bravo"]
    );
    Ok(())
}

#[test]
fn track_query_filters_by_user_state_and_year() -> Result<()> {
    let catalog = catalog()?;
    assert_eq!(catalog.titles("{ favorite = true }")?, vec!["Alpha"]);
    assert_eq!(
        catalog.titles("{ favorite = false }")?,
        vec!["Bravo", "Charlie", "Delta", "Echo"]
    );
    assert_eq!(catalog.titles("{ listened = true }")?, vec!["Bravo"]);
    assert_eq!(
        catalog.titles("{ listened = false }")?,
        vec!["Alpha", "Charlie", "Delta", "Echo"]
    );
    assert_eq!(
        catalog.titles("{ years = { 1999, 2005 } }")?,
        vec!["Alpha", "Delta", "Echo"]
    );
    Ok(())
}

#[test]
fn track_query_sorts_each_key_in_its_own_direction() -> Result<()> {
    let catalog = catalog()?;
    assert_eq!(
        catalog
            .titles(r#"{ sort = { { key = "year", order = "descending" }, { key = "name" } } }"#)?,
        vec!["Delta", "Echo", "Bravo", "Charlie", "Alpha"]
    );
    assert_eq!(
        catalog.titles(r#"{ sort = { { key = "artist_name" } } }"#)?,
        vec!["Alpha", "Delta", "Echo", "Bravo", "Charlie"],
        "tracks without a credited artist sort last"
    );
    assert_eq!(
        catalog.titles(r#"{ sort = { { key = "release_artist_name" } } }"#)?,
        vec!["Alpha", "Bravo", "Charlie", "Delta", "Echo"],
        "tracks sort by their release's artist, and a release without one sorts last"
    );
    assert_eq!(
        catalog.titles(&format!(
            r#"{{ release_ids = {{ {} }}, sort = {{ {{ key = "release_title" }} }} }}"#,
            catalog.album.0
        ))?,
        vec!["Alpha", "Bravo", "Charlie"]
    );
    Ok(())
}

#[test]
fn track_query_shuffles_stably_per_seed() -> Result<()> {
    let catalog = catalog()?;
    let shuffled = |seed: u64| {
        catalog.titles(&format!(
            r#"{{ sort = {{ {{ key = "random" }} }}, seed = {seed} }}"#
        ))
    };
    assert_eq!(shuffled(7)?, shuffled(7)?);
    assert!(
        (8..40).any(|seed| shuffled(seed).ok() != shuffled(7).ok()),
        "every seed produced the same order"
    );
    Ok(())
}

#[test]
fn track_query_pages_after_sorting() -> Result<()> {
    let catalog = catalog()?;
    assert_eq!(
        catalog.titles("{ offset = 1, limit = 2 }")?,
        vec!["Bravo", "Charlie"]
    );
    assert_eq!(catalog.titles("{ offset = 4, limit = 2 }")?, vec!["Echo"]);
    assert!(catalog.titles("{ offset = 9 }")?.is_empty());
    Ok(())
}

#[test]
fn track_query_rejects_relevance_without_a_search() -> Result<()> {
    let catalog = catalog()?;
    let error = catalog
        .titles(r#"{ sort = { { key = "relevance" } } }"#)
        .expect_err("relevance needs a search term");
    assert!(format!("{error:#}").contains("needs a search term"));
    Ok(())
}

#[test]
fn track_query_ignores_ids_that_do_not_exist() -> Result<()> {
    let catalog = catalog()?;
    let stale = 999_999;
    assert!(
        catalog
            .titles(&format!("{{ ids = {{ {stale} }} }}"))?
            .is_empty()
    );
    assert!(
        catalog
            .titles(&format!("{{ release_ids = {{ {stale} }} }}"))?
            .is_empty()
    );
    let system = catalog.runtime.eval_plugin_source(
        "demo",
        "init.luau",
        format!(
            r#"
                local tracks = require("@lyra/tracks")
                return tracks.query({{ ids = {{ {stale} }}, release_ids = {{ {stale} }} }}).total
            "#
        )
        .into_bytes(),
    )?;
    assert_eq!(system, vec![luau::Value::Number(0.0)]);
    Ok(())
}

#[test]
fn track_query_treats_missing_and_hidden_ids_alike() -> Result<()> {
    let mut db = test_db::new_test_db()?;
    let visible_library = test_db::insert_library(&mut db, "Visible", "/tmp/lyra-oracle-visible")?;
    let hidden_library = test_db::insert_library(&mut db, "Hidden", "/tmp/lyra-oracle-hidden")?;
    let hidden_artist = test_db::insert_artist(&mut db, "Hidden Artist")?;
    let mut hidden_release = None;
    for (library, title) in [(visible_library, "Visible"), (hidden_library, "Hidden")] {
        let release = test_db::insert_release(&mut db, title)?;
        let track = test_db::insert_track(&mut db, title)?;
        test_db::connect(&mut db, library, release)?;
        test_db::connect(&mut db, release, track)?;
        if library == hidden_library {
            test_db::connect_artist(&mut db, track, hidden_artist)?;
            db::genres::sync_release_genres(&mut db, release, &["Hidden Genre".to_string()])?;
            hidden_release = Some(release);
        }
    }
    let hidden_genre = db::genres::get_for_release(&db, hidden_release.context("hidden release")?)?
        .into_iter()
        .find_map(|genre| genre.db_id.map(agdb::DbId::from))
        .context("hidden genre")?;
    let user = test_db::insert_user(&mut db, "oracle-viewer")?;
    let visible_library_id = db::libraries::get_by_id(&db, visible_library)?
        .context("library exists")?
        .id;
    let principal = crate::services::auth::Principal::for_user(
        &db,
        user,
        Vec::new(),
        std::collections::HashSet::from([visible_library_id]),
    );
    let runtime = tracks_runtime(db)?;
    let total = |query: String| -> Result<Vec<luau::Value>> {
        let mut context = CallContext {
            origin: plugin_origin("demo", "init.luau"),
            ..CallContext::default()
        };
        seed_caller_principal(&mut context, principal.clone());
        runtime.eval_plugin_source_with_call_context(
            format!(
                r#"
                    local tracks = require("@lyra/tracks")
                    local ok, page = pcall(tracks.query, {query})
                    return ok, if ok then page.total else tostring(page)
                "#
            )
            .into_bytes(),
            context,
        )
    };
    let empty = vec![luau::Value::Boolean(true), luau::Value::Number(0.0)];
    let missing = 999_999;
    for (field, hidden) in [
        ("library_id", format!("{}", hidden_library.0)),
        ("genre_ids", format!("{{ {} }}", hidden_genre.0)),
        ("artist_ids", format!("{{ {} }}", hidden_artist.0)),
        (
            "release_ids",
            format!("{{ {} }}", hidden_release.context("release")?.0),
        ),
    ] {
        let missing = if field == "library_id" {
            format!("{missing}")
        } else {
            format!("{{ {missing} }}")
        };
        assert_eq!(
            total(format!("{{ {field} = {hidden} }}"))?,
            empty,
            "hidden {field}"
        );
        assert_eq!(
            total(format!("{{ {field} = {missing} }}"))?,
            empty,
            "missing {field}"
        );
    }
    Ok(())
}

#[test]
fn release_query_filters_and_sorts_by_its_tracks() -> Result<()> {
    let catalog = catalog()?;
    assert_eq!(
        catalog.release_titles("{ listened = true }")?,
        Vec::<String>::new(),
        "a release is listened only once every track is"
    );
    assert_eq!(
        catalog.release_titles("{ listened = false }")?,
        vec!["Album", "Other"]
    );
    let amy = catalog.amy.0;
    assert_eq!(
        catalog.release_titles(&format!(
            r#"{{ artist_ids = {{ {amy} }}, credit_role = "release" }}"#
        ))?,
        vec!["Album"]
    );
    assert_eq!(
        catalog.release_titles(&format!(
            r#"{{ artist_ids = {{ {amy} }}, credit_role = "track", exclude_credit_role = "release" }}"#
        ))?,
        vec!["Other"]
    );
    assert_eq!(
        catalog
            .release_titles(r#"{ sort = { { key = "track_count", order = "descending" } } }"#)?,
        vec!["Album", "Other"]
    );
    assert_eq!(
        catalog
            .release_titles(r#"{ sort = { { key = "listen_count", order = "descending" } } }"#)?,
        vec!["Album", "Other"]
    );
    Ok(())
}

#[test]
fn artist_query_filters_by_credit_role() -> Result<()> {
    let catalog = catalog()?;
    assert_eq!(catalog.artist_names("{}")?, vec!["Amy", "Zed"]);
    assert_eq!(
        catalog.artist_names(r#"{ credit_role = "release" }"#)?,
        vec!["Amy"]
    );
    assert_eq!(
        catalog.artist_names(r#"{ credit_role = "track", exclude_credit_role = "release" }"#)?,
        vec!["Zed"]
    );
    assert_eq!(
        catalog.artist_names(r#"{ sort = { { key = "track_count", order = "descending" } } }"#)?,
        vec!["Amy", "Zed"]
    );
    Ok(())
}

#[test]
fn genre_query_follows_releases_and_ranks_searches() -> Result<()> {
    let catalog = catalog()?;
    assert_eq!(
        catalog.genre_names("{}")?,
        vec!["Jazz", "Rock", "Rockabilly"]
    );
    assert_eq!(
        catalog.genre_names(&format!("{{ release_ids = {{ {} }} }}", catalog.album.0))?,
        vec!["Rock"]
    );
    assert_eq!(
        catalog.genre_names(r#"{ search = "rock" }"#)?,
        vec!["Rock", "Rockabilly"]
    );
    assert_eq!(
        catalog.genre_names(r#"{ sort = { { key = "track_count", order = "descending" } } }"#)?,
        vec!["Rock", "Jazz", "Rockabilly"]
    );
    Ok(())
}

#[test]
fn track_and_release_queries_leave_out_excluded_artists() -> Result<()> {
    let catalog = catalog()?;
    let (amy, zed) = (catalog.amy.0, catalog.zed.0);
    assert_eq!(
        catalog.titles(&format!("{{ exclude_artist_ids = {{ {zed} }} }}"))?,
        vec!["Alpha", "Bravo", "Charlie", "Delta"]
    );
    assert_eq!(
        catalog.release_titles(&format!("{{ exclude_artist_ids = {{ {zed} }} }}"))?,
        vec!["Album"]
    );
    assert!(
        catalog
            .release_titles(&format!("{{ exclude_artist_ids = {{ {amy} }} }}"))?
            .is_empty()
    );
    Ok(())
}

#[test]
fn artist_and_genre_pages_carry_their_counts() -> Result<()> {
    let catalog = catalog()?;
    assert_eq!(
        catalog.names(
            "artists",
            r#"artist_name .. ":" .. item.release_count .. ":" .. item.track_count .. ":" .. item.total_duration"#,
            "{}",
        )?,
        vec!["Amy:1:4:0", "Zed:0:1:0"]
    );
    assert_eq!(
        catalog.names(
            "genres",
            r#"name .. ":" .. item.release_count .. ":" .. item.track_count"#,
            "{}",
        )?,
        vec!["Jazz:1:2", "Rock:1:3", "Rockabilly:1:2"]
    );
    Ok(())
}

#[test]
fn artwork_lookups_resolve_without_a_caller() -> Result<()> {
    let catalog = catalog()?;
    let mut anonymous = CallContext {
        origin: plugin_origin("demo", "init.luau"),
        ..CallContext::default()
    };
    anonymous
        .caller
        .insert(crate::plugins::auth::DispatchAuth::default());
    let values = catalog.runtime.eval_plugin_source_with_call_context(
        format!(
            r#"
                local artists = require("@lyra/artists")
                local genres = require("@lyra/genres")
                local rock = assert(genres.find_by_name("ROCK")).db_id
                local releases = genres.artwork_release_ids(rock)
                return artists.artwork_id_by_name("AMY"),
                    artists.artwork_id_by_name("Nobody") == nil,
                    #releases,
                    releases[1],
                    #genres.artwork_release_ids({amy}),
                    pcall(artists.query, {{}})
            "#,
            amy = catalog.amy.0,
        )
        .into_bytes(),
        anonymous,
    )?;
    assert_eq!(
        values[..6],
        [
            luau::Value::Number(catalog.amy.0 as f64),
            luau::Value::Boolean(true),
            luau::Value::Number(1.0),
            luau::Value::Number(catalog.album.0 as f64),
            luau::Value::Number(0.0),
            luau::Value::Boolean(false),
        ]
    );
    Ok(())
}
