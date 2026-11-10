// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! The artist cover pass run against a plugin's own handlers.

use std::{
    path::PathBuf,
    time::{
        SystemTime,
        UNIX_EPOCH,
    },
};

use anyhow::Result;
use tokio::io::{
    AsyncReadExt,
    AsyncWriteExt,
};

use crate::{
    plugins::executor::PluginExecutorHandle,
    services::{
        STATE,
        covers::CoverPaths,
        providers::{
            ListedArtist,
            sync_artist_covers,
        },
    },
};

const PLUGIN_ID: &str = "coverpass";

struct TempPluginDir {
    root: PathBuf,
}

impl TempPluginDir {
    /// A plugin whose artist refresh corrects the stored id `wrong` to `right`, and whose cover
    /// handler points at `base_url` followed by the stored id.
    fn new(base_url: &str) -> Result<Self> {
        let root = std::env::temp_dir().join(format!(
            "lyra-cover-pass-test-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ));
        let plugin = root.join(PLUGIN_ID);
        std::fs::create_dir_all(&plugin)?;
        std::fs::write(
            plugin.join("plugin.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "schema_version": 1,
                "id": PLUGIN_ID,
                "name": "Cover Pass Test",
                "version": "1.0.0",
                "lyra": crate::plugins::testing::HOST_REQUIREMENT,
                "description": "Artist cover pass test",
                "entrypoint": "init.luau",
                "scopes": [
                    { "id": "harmony.http", "reason": "Fetches covers.", "hosts": ["127.0.0.1"] },
                    "lyra.metadata",
                ],
            }))?,
        )?;
        std::fs::write(
            plugin.join("init.luau"),
            format!(
                r#"
                    local http = require("@harmony/http")
                    local metadata = require("@lyra/metadata")
                    local ET = metadata.EntityType
                    http.set_rate_limit({{ domain = "127.0.0.1", requests_per_second = 100 }})
                    local provider = metadata.Provider.new("{PLUGIN_ID}")
                    provider:id({{ id_type = "artist_id", entity = ET.Artist, scheme = "coverpass:artist" }},
                        "https://example.test/artist/{{id}}")
                    provider:refresh(ET.Artist, function(ctx)
                        if ctx.external_ids["coverpass:artist"] == "wrong" then
                            local layer = provider:layer(ctx.id)
                            layer:set_id("artist_id", "right")
                            layer:save()
                        end
                    end)
                    provider:cover(ET.Artist, {{}}, function(ctx)
                        local id = ctx.external_ids["coverpass:artist"]
                        if id == nil then
                            return nil
                        end
                        return {{ url = "{base_url}/" .. id .. ".png" }}
                    end)
                "#
            ),
        )?;
        Ok(Self { root })
    }
}

impl Drop for TempPluginDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn png(red: u8) -> Vec<u8> {
    let image = image::RgbImage::from_pixel(2, 2, image::Rgb([red, 0, 0]));
    let mut bytes = std::io::Cursor::new(Vec::new());
    image
        .write_to(&mut bytes, image::ImageFormat::Png)
        .expect("encode png");
    bytes.into_inner()
}

/// Serves `/right.png` and `/wrong.png` as two different images, until dropped.
async fn image_server() -> Result<(String, tokio::task::JoinHandle<()>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let base_url = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let mut request = vec![0; 4096];
            let read = socket.read(&mut request).await.unwrap_or(0);
            let request = String::from_utf8_lossy(&request[..read]);
            let body = if request.starts_with("GET /right.png") {
                png(200)
            } else {
                png(10)
            };
            let head = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: image/png\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = socket.write_all(head.as_bytes()).await;
            let _ = socket.write_all(&body).await;
        }
    });
    Ok((base_url, server))
}

async fn publish_test_runtime(plugin: &TempPluginDir) -> Result<PluginExecutorHandle> {
    let server_info = crate::plugins::server::load_server_info().await?;
    let auth_capabilities =
        crate::plugins::auth::AuthCapabilities::from_config(&STATE.config().auth);
    let (runtime, errors) = PluginExecutorHandle::discover_from_plugins_dir_with_db_and_modules(
        plugin.root.clone(),
        server_info,
        auth_capabilities,
        STATE.db.get(),
        Vec::new(),
        Some(STATE.settings.clone()),
    )?;
    assert!(errors.is_empty(), "{errors:?}");
    for manifest in runtime.plugin_manifests().await? {
        crate::plugins::lifecycle::set_http_hosts(&manifest).await;
    }
    runtime.exec_plugin(PLUGIN_ID).await?;
    crate::plugins::STATE.publish_runtime(&crate::services::STATE.generation(), runtime.clone());
    Ok(runtime)
}

#[tokio::test(flavor = "multi_thread")]
async fn the_pass_refreshes_an_artist_before_fetching_its_cover() -> Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let library = tempfile::tempdir()?;
    crate::plugins::testing::init_library_test_state(library.path().to_path_buf()).await?;
    let (base_url, server) = image_server().await?;
    let plugin = TempPluginDir::new(&base_url)?;
    let runtime = publish_test_runtime(&plugin).await?;
    let (artist_db_id, listed) = {
        let mut db = STATE.db.write().await;
        let artist = lyra_db::fixtures::insert_artist(&mut db, "Artist")?;
        lyra_db::external_ids::upsert(
            &mut db,
            artist,
            PLUGIN_ID,
            "artist_id",
            "wrong",
            lyra_db::IdSource::Resolved,
        )?;
        let record = lyra_db::artists::get_by_id(&db, artist)?.expect("artist");
        (artist, ListedArtist::list(vec![record]))
    };
    let covers_root = tempfile::tempdir()?;

    let synced = sync_artist_covers(
        &listed,
        CoverPaths {
            library_root: None,
            covers_root: covers_root.path(),
        },
        crate::services::covers::CoverSyncOptions::default(),
        None,
        &crate::services::libraries::SyncRunProgress::unreported_for_test(),
    )
    .await?;

    let db = STATE.db.read().await;
    let stored = lyra_db::artists::get_by_id(&db, artist_db_id)?.expect("artist");
    let id =
        lyra_db::external_ids::get(&db, artist_db_id, PLUGIN_ID, "artist_id")?.expect("stored id");
    let cover =
        lyra_db::covers::get_source(&db, artist_db_id, lyra_db::covers::CoverSource::Provider)?
            .expect("provider cover");
    drop(db);
    runtime.shutdown().await?;
    server.abort();

    assert_eq!(synced, 1);
    assert_eq!(stored.artist_name, "Artist");
    assert_eq!(id.id_value, "right", "the refresh replaced the wrong id");
    assert_eq!(
        std::fs::read(&cover.path)?,
        png(200),
        "the cover is the right artist's"
    );
    Ok(())
}
