use super::*;
use crate::plugins::db::{
    self,
    test_db,
};

/// The checked-in Jellyfin plugin over a small catalog.
///
/// "Solo" (2001, Rock, added last) is credited to Amy and holds S1 and S2, added first. "Various"
/// (2005, added first) is credited to Various Artists and holds V1, added last and credited to
/// Aaron. The admin listened
/// to S1 and S2, favorited S2, and owns the playlists "Mix" (V1, S2, S1, S2), "Played" (S1, S2),
/// "Liked" (empty, favorited) and the public "Shared" (S1). Another admin owns nothing.
struct Jellyfin {
    runtime: PluginExecutor,
    admin: User,
    other: User,
    solo: agdb::DbId,
    amy: agdb::DbId,
    rock: agdb::DbId,
    mix: agdb::DbId,
    shared: agdb::DbId,
    liked: agdb::DbId,
    s1: agdb::DbId,
    s2: agdb::DbId,
}

#[derive(Clone)]
struct User {
    principal: crate::services::auth::Principal,
    id: String,
    db_id: agdb::DbId,
}

fn user(db: &mut agdb::DbAny, username: &str) -> Result<User> {
    let db_id = test_db::insert_user(db, username)?;
    let id = db::users::get_by_id(db, db_id)?.context("user exists")?.id;
    let principal = crate::services::auth::Principal::for_user(
        db,
        db_id,
        vec![db::Permission::Admin],
        Default::default(),
    );
    Ok(User {
        principal,
        id,
        db_id,
    })
}

fn listen(db: &mut agdb::DbAny, track: agdb::DbId, user: agdb::DbId) -> Result<()> {
    let track_public_id = db::tracks::get_by_id(db, track)?
        .context("track exists")?
        .id;
    crate::plugins::db::listens::create_and_mark_recorded(
        db,
        &crate::plugins::db::listens::Listen {
            db_id: None,
            id: nanoid::nanoid!(),
            track_public_id,
            position_ms: 0,
            duration_ms: Some(180_000),
            activity_ms: 180_000,
            state: crate::plugins::db::PlaybackState::Completed,
            listened_at_ms: 1_000,
            created_at_ms: 1_000,
        },
        track,
        user,
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
    Ok(())
}

fn jellyfin() -> Result<Jellyfin> {
    crate::testing::init_default_test_state()?;
    let mut db = test_db::new_test_db()?;
    let admin = user(&mut db, "jellyfin-admin")?;
    let other = user(&mut db, "jellyfin-other")?;
    let admin_db_id = admin.db_id;
    let amy = test_db::insert_artist(&mut db, "Amy")?;
    let aaron = test_db::insert_artist(&mut db, "Aaron")?;
    let various = test_db::insert_artist(&mut db, "Various Artists")?;
    let solo = test_db::insert_release(&mut db, "Solo")?;
    let compilation = test_db::insert_release(&mut db, "Various")?;
    for (release, added) in [(solo, 400), (compilation, 200)] {
        let mut stored = db::releases::get_by_id(&db, release)?.context("release exists")?;
        stored.ctime = Some(added);
        db::releases::update(&mut db, &stored)?;
    }
    test_db::connect_artist(&mut db, solo, amy)?;
    test_db::connect_artist(&mut db, compilation, various)?;
    let mut tracks = std::collections::HashMap::new();
    for (release, title, number, year, artist, added) in [
        (solo, "S1", 1, 2001, amy, 50),
        (solo, "S2", 2, 2001, amy, 50),
        (compilation, "V1", 1, 2005, aaron, 300),
    ] {
        let track = test_db::insert_track(&mut db, title)?;
        let mut stored = db::tracks::get_by_id(&db, track)?.context("track exists")?;
        stored.disc = Some(1);
        stored.track = Some(number);
        stored.year = Some(year);
        stored.ctime = Some(added);
        db::tracks::update(&mut db, &stored)?;
        test_db::connect(&mut db, release, track)?;
        test_db::connect_artist(&mut db, track, artist)?;
        tracks.insert(title, track);
    }
    db::genres::sync_release_genres(&mut db, solo, &["Rock".to_string()])?;
    let rock = db::genres::find_by_name(&db, "Rock")?.context("genre exists")?;
    listen(&mut db, tracks["S1"], admin_db_id)?;
    listen(&mut db, tracks["S2"], admin_db_id)?;
    db::favorites::add(
        &mut db,
        admin_db_id,
        tracks["S2"],
        db::favorites::FavoriteKind::Track,
        1,
    )?;

    let plugins_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .context("lyra-server manifest directory has parent")?
        .join("plugins");
    let (runtime, errors) = PluginExecutor::discover_from_plugins_dir_with_db(
        plugins_dir,
        default_server_info(),
        std::sync::Arc::new(tokio::sync::RwLock::new(db)),
    )?;
    assert!(errors.is_empty(), "plugin discovery errors: {errors:?}");

    let mut jellyfin = Jellyfin {
        runtime,
        admin,
        other,
        solo,
        amy,
        rock,
        mix: agdb::DbId(0),
        shared: agdb::DbId(0),
        liked: agdb::DbId(0),
        s1: tracks["S1"],
        s2: tracks["S2"],
    };
    // Playlists belong to the dispatch principal, so they are made through the plugin API.
    let ids = jellyfin.eval(
        &jellyfin.admin,
        &format!(
            r#"
                local playlists = require("@lyra/playlists")
                local favorites = require("@lyra/favorites")
                local mix = playlists.create({{ name = "Mix" }})
                playlists.add_tracks(mix, {{ {v1}, {s2}, {s1}, {s2} }})
                local played = playlists.create({{ name = "Played" }})
                playlists.add_tracks(played, {{ {s1}, {s2} }})
                local liked = playlists.create({{ name = "Liked" }})
                favorites.add(liked)
                local shared = playlists.create({{ name = "Shared", is_public = true }})
                playlists.add_tracks(shared, {{ {s1} }})
                return `{{mix}},{{shared}},{{liked}}`
            "#,
            v1 = tracks["V1"].0,
            s2 = tracks["S2"].0,
            s1 = tracks["S1"].0,
        ),
    )?;
    let ids = ids
        .split(',')
        .map(|id| id.parse().map(agdb::DbId))
        .collect::<Result<Vec<_>, _>>()?;
    let [mix, shared, liked] = ids[..] else {
        anyhow::bail!("expected three playlist ids, got {ids:?}");
    };
    (jellyfin.mix, jellyfin.shared, jellyfin.liked) = (mix, shared, liked);
    Ok(jellyfin)
}

impl Jellyfin {
    fn eval(&self, user: &User, source: &str) -> Result<String> {
        let mut context = CallContext {
            origin: plugin_origin("jellyfin", "init.luau"),
            ..CallContext::default()
        };
        seed_caller_principal(&mut context, user.principal.clone());
        let values = self
            .runtime
            .eval_plugin_source_with_call_context(source.as_bytes().to_vec(), context)?;
        match values.as_slice() {
            [luau::Value::String(bytes)] => Ok(String::from_utf8(bytes.clone())?),
            other => anyhow::bail!("expected one string, got {other:?}"),
        }
    }

    /// `name:type` for each item the admin's `/Items` listing returns, then its total.
    fn items(&self, params: &[(&str, &str)], parent: Option<agdb::DbId>) -> Result<String> {
        self.list(&self.admin, "list_items", params, parent)
    }

    /// The Jellyfin item id of `id`.
    fn hex(&self, id: agdb::DbId) -> Result<String> {
        self.eval(
            &self.admin,
            &format!(
                r#"return require("./protocol/items/identifiers").encode_jellyfin_item_id({})"#,
                id.0
            ),
        )
    }

    /// `name:type` for each item an `/Items/Latest` listing returns.
    fn latest(&self, params: &[(&str, &str)]) -> Result<String> {
        self.list(&self.admin, "list_latest_items", params, None)
    }

    fn list(
        &self,
        user: &User,
        list: &str,
        params: &[(&str, &str)],
        parent: Option<agdb::DbId>,
    ) -> Result<String> {
        let params = params
            .iter()
            .map(|(key, value)| format!("{key} = {{ {value:?} }}"))
            .collect::<Vec<_>>()
            .join(", ");
        self.eval(
            user,
            &format!(
                r#"
                    local item_ids = require("./protocol/items/identifiers")
                    local listing = require("./protocol/items/listing")
                    local options = require("./protocol/items/options")
                    local raw: {{ [string]: {{ string }} }} = {{ {params} }}
                    local parent = {parent}
                    if parent then
                        raw.ParentId = {{ item_ids.encode_jellyfin_item_id(parent) }}
                    end
                    local parsed = options.for_user(options.parse(raw :: any), {user_id:?})
                    local items, total = listing.{list}(parsed)
                    local names = {{}}
                    for _, item in items do
                        table.insert(names, `{{item.Name}}:{{item.Type}}`)
                    end
                    local listed = table.concat(names, ",")
                    return if total then `{{listed}} ({{total}})` else listed
                "#,
                parent = parent.map_or("nil".to_string(), |id| id.0.to_string()),
                user_id = user.id,
            ),
        )
    }
}

#[test]
fn jellyfin_lists_a_playlists_tracks_under_its_parent_id() -> Result<()> {
    let _guard = futures::executor::block_on(crate::testing::runtime_test_lock());
    let jellyfin = jellyfin()?;
    let mix = Some(jellyfin.mix);
    // Each track once, where it first appears.
    let in_order = "V1:Audio,S2:Audio,S1:Audio (3)";
    assert_eq!(
        jellyfin.items(
            &[
                ("IncludeItemTypes", "Audio"),
                ("Recursive", "false"),
                ("Limit", "400"),
            ],
            mix
        )?,
        in_order
    );
    assert_eq!(jellyfin.items(&[], mix)?, in_order);
    assert_eq!(
        jellyfin.items(&[("StartIndex", "1"), ("Limit", "1")], mix)?,
        "S2:Audio (3)"
    );
    assert_eq!(
        jellyfin.items(&[("IncludeItemTypes", "MusicAlbum")], mix)?,
        " (0)"
    );
    assert_eq!(
        jellyfin.items(
            &[("SortBy", "SortName"), ("StartIndex", "1"), ("Limit", "1")],
            mix
        )?,
        "V1:Audio (3)",
        "SortBy orders a playlist's tracks before they page"
    );
    for untranslatable in ["Default", "Album", "CommunityRating,IsFavoriteOrLiked"] {
        assert_eq!(
            jellyfin.items(&[("SortBy", untranslatable)], mix)?,
            in_order,
            "keys without a Lyra meaning keep playlist order: {untranslatable}"
        );
    }
    assert_eq!(
        jellyfin.items(&[("Years", "2005")], mix)?,
        "V1:Audio (1)",
        "the listing's filters apply to a playlist's tracks"
    );
    assert_eq!(
        jellyfin.items(&[("Filters", "IsFavorite")], mix)?,
        "S2:Audio (1)"
    );
    Ok(())
}

#[test]
fn jellyfin_sorts_named_ids_unless_no_sort_is_named() -> Result<()> {
    let _guard = futures::executor::block_on(crate::testing::runtime_test_lock());
    let jellyfin = jellyfin()?;
    let (s1, s2) = (jellyfin.hex(jellyfin.s1)?, jellyfin.hex(jellyfin.s2)?);
    let tracks = format!("{s2},{s1}");
    assert_eq!(
        jellyfin.items(&[("Ids", tracks.as_str())], None)?,
        "S2:Audio,S1:Audio (2)",
        "request order without SortBy"
    );
    assert_eq!(
        jellyfin.items(&[("Ids", tracks.as_str()), ("SortBy", "Default")], None)?,
        "S1:Audio,S2:Audio (2)",
        "a SortBy without a Lyra meaning falls back to SortName"
    );
    let solo = jellyfin.hex(jellyfin.solo)?;
    assert_eq!(
        jellyfin.items(
            &[
                ("Ids", tracks.as_str()),
                ("AlbumIds", solo.as_str()),
                ("SortBy", "Album"),
            ],
            None
        )?,
        "S1:Audio,S2:Audio (2)"
    );
    let playlists = format!(
        "{},{}",
        jellyfin.hex(jellyfin.mix)?,
        jellyfin.hex(jellyfin.liked)?
    );
    assert_eq!(
        jellyfin.items(
            &[
                ("Ids", playlists.as_str()),
                ("IncludeItemTypes", "Playlist"),
                ("SortBy", "SortName"),
            ],
            None
        )?,
        "Liked:Playlist,Mix:Playlist (2)"
    );
    Ok(())
}

#[test]
fn jellyfin_lists_another_users_playlist_only_when_public() -> Result<()> {
    let _guard = futures::executor::block_on(crate::testing::runtime_test_lock());
    let jellyfin = jellyfin()?;
    let as_other = |playlist| jellyfin.list(&jellyfin.other, "list_items", &[], Some(playlist));
    assert_eq!(as_other(jellyfin.mix)?, " (0)");
    assert_eq!(as_other(jellyfin.shared)?, "S1:Audio (1)");
    Ok(())
}

#[test]
fn jellyfin_lists_a_parents_own_children_when_no_type_is_named() -> Result<()> {
    let _guard = futures::executor::block_on(crate::testing::runtime_test_lock());
    let jellyfin = jellyfin()?;
    assert_eq!(
        jellyfin.items(&[], Some(jellyfin.solo))?,
        "S1:Audio,S2:Audio (2)"
    );
    assert_eq!(
        jellyfin.items(&[], Some(jellyfin.amy))?,
        " (0)",
        "an artist is only a name, with no children"
    );
    assert_eq!(
        jellyfin.items(&[], Some(jellyfin.rock))?,
        " (0)",
        "a genre is only a name, with no children"
    );
    assert_eq!(
        jellyfin.items(&[("IncludeItemTypes", "MusicArtist")], Some(jellyfin.solo))?,
        "Amy:MusicArtist (1)",
        "a named type still lists"
    );
    for parent in [jellyfin.solo, jellyfin.mix] {
        assert_eq!(
            jellyfin.items(&[("ExcludeItemTypes", "Audio")], Some(parent))?,
            " (0)",
            "excluding a parent's only child type leaves nothing"
        );
    }
    Ok(())
}

#[test]
fn jellyfin_filters_playlists_by_favorite() -> Result<()> {
    let _guard = futures::executor::block_on(crate::testing::runtime_test_lock());
    let jellyfin = jellyfin()?;
    let playlists = |filter: &[(&str, &str)]| {
        let mut params = vec![("IncludeItemTypes", "Playlist"), ("Recursive", "true")];
        params.extend_from_slice(filter);
        jellyfin.items(&params, None)
    };
    assert_eq!(
        playlists(&[])?,
        "Liked:Playlist,Mix:Playlist,Played:Playlist,Shared:Playlist (4)"
    );
    assert_eq!(
        playlists(&[("Filters", "IsFavorite")])?,
        "Liked:Playlist (1)"
    );
    assert_eq!(
        playlists(&[("IsFavorite", "false")])?,
        "Mix:Playlist,Played:Playlist,Shared:Playlist (3)"
    );
    assert_eq!(
        playlists(&[("Filters", "IsFavorite"), ("IsFavorite", "false")])?,
        "Liked:Playlist (1)",
        "the IsFavorite filter overrides the IsFavorite parameter"
    );
    assert_eq!(
        playlists(&[("Filters", "IsFavoriteOrLikes")])?,
        "Liked:Playlist (1)"
    );
    assert_eq!(
        playlists(&[("Filters", "IsFavoriteOrLikes"), ("IsFavorite", "false")])?,
        " (0)",
        "a favourite or liked playlist can't also be no favourite"
    );
    Ok(())
}

#[test]
fn jellyfin_filters_by_played_state() -> Result<()> {
    let _guard = futures::executor::block_on(crate::testing::runtime_test_lock());
    let jellyfin = jellyfin()?;
    let playlists = |filter: &[(&str, &str)]| {
        let mut params = vec![("IncludeItemTypes", "Playlist"), ("Recursive", "true")];
        params.extend_from_slice(filter);
        jellyfin.items(&params, None)
    };
    // A playlist is played once none of its tracks is unplayed, so an empty one is played.
    let played = "Liked:Playlist,Played:Playlist,Shared:Playlist (3)";
    assert_eq!(playlists(&[("Filters", "IsPlayed")])?, played);
    assert_eq!(playlists(&[("IsPlayed", "true")])?, played);
    assert_eq!(playlists(&[("Filters", "IsUnplayed")])?, "Mix:Playlist (1)");
    assert_eq!(
        playlists(&[("Filters", "IsPlayed"), ("IsPlayed", "false")])?,
        played,
        "the IsPlayed filter overrides the IsPlayed parameter"
    );
    assert_eq!(
        playlists(&[("Filters", "IsPlayed,IsUnplayed")])?,
        " (0)",
        "played and unplayed together match nothing"
    );

    let shared_unplayed = |user: &User| {
        let shared = jellyfin.hex(jellyfin.shared)?;
        jellyfin.list(
            user,
            "list_items",
            &[
                ("Ids", shared.as_str()),
                ("IncludeItemTypes", "Playlist"),
                ("Filters", "IsUnplayed"),
            ],
            None,
        )
    };
    assert_eq!(
        shared_unplayed(&jellyfin.other)?,
        "Shared:Playlist (1)",
        "played state is the caller's own"
    );
    assert_eq!(shared_unplayed(&jellyfin.admin)?, " (0)");

    let albums = |filter: &str| {
        jellyfin.items(
            &[
                ("IncludeItemTypes", "MusicAlbum"),
                ("Recursive", "true"),
                ("Filters", filter),
            ],
            None,
        )
    };
    assert_eq!(
        albums("IsPlayed")?,
        "Solo:MusicAlbum (1)",
        "an album is played once every track is"
    );
    assert_eq!(albums("IsUnplayed")?, "Various:MusicAlbum (1)");

    let mix = Some(jellyfin.mix);
    assert_eq!(
        jellyfin.items(&[("Filters", "IsPlayed"), ("IsPlayed", "false")], mix)?,
        "S2:Audio,S1:Audio (2)"
    );
    assert_eq!(
        jellyfin.items(&[("Filters", "IsUnplayed"), ("IsPlayed", "true")], mix)?,
        "V1:Audio (1)"
    );
    Ok(())
}

#[test]
fn jellyfin_sorts_tracks_by_their_album_artist() -> Result<()> {
    let _guard = futures::executor::block_on(crate::testing::runtime_test_lock());
    let jellyfin = jellyfin()?;
    let sorted = |sort_by: &str| {
        jellyfin.items(
            &[
                ("IncludeItemTypes", "Audio"),
                ("Recursive", "true"),
                ("SortBy", sort_by),
            ],
            None,
        )
    };
    assert_eq!(
        sorted("AlbumArtist,Album,SortName")?,
        "S1:Audio,S2:Audio,V1:Audio (3)",
        "the compilation track sorts under Various Artists"
    );
    assert_eq!(
        sorted("Artist,Album,SortName")?,
        "V1:Audio,S1:Audio,S2:Audio (3)"
    );
    Ok(())
}

#[test]
fn jellyfin_groups_the_latest_audio_into_albums() -> Result<()> {
    let _guard = futures::executor::block_on(crate::testing::runtime_test_lock());
    let jellyfin = jellyfin()?;
    assert_eq!(
        jellyfin.latest(&[("IncludeItemTypes", "Audio")])?,
        "Solo:MusicAlbum,Various:MusicAlbum",
        "albums order by the date they were added, not their newest track"
    );
    assert_eq!(
        jellyfin.latest(&[("IncludeItemTypes", "Audio"), ("Limit", "1")])?,
        "Solo:MusicAlbum",
        "the limit counts albums"
    );
    assert_eq!(
        jellyfin.latest(&[("IncludeItemTypes", "Audio"), ("GroupItems", "false")])?,
        "V1:Audio,S2:Audio,S1:Audio"
    );
    Ok(())
}
