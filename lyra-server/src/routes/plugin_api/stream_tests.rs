// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Streamed plugin responses as a client sees them on the wire.

use std::{
    collections::HashMap,
    time::Duration,
};

use anyhow::{
    Context,
    Result,
};
use axum::http::{
    HeaderMap,
    Method,
    Uri,
};
use axum::response::Response;
use tokio::io::{
    AsyncReadExt,
    AsyncWriteExt,
};

use crate::plugins::executor::{
    ApiHandlerRequest,
    PluginExecutor,
    PluginExecutorHandle,
};

const PLUGIN_ID: &str = "streams";

const INIT: &str = r#"
local api = require("@lyra/api")

local started = 0

api.get("/count", function()
	return api.response.json(200, { started = started })
end, { auth = "public" })

api.get("/ping", function()
	return api.response.text(200, "pong")
end, { auth = "public" })

api.get("/stream/ok", function()
	return api.response.stream(200, function(writer)
		started += 1
		for index = 1, 3 do
			writer:write(`chunk {index}\n`)
		end
	end, { ["content-type"] = "text/plain" })
end, { auth = "public" })

api.get("/stream/fail", function()
	return api.response.stream(200, function(writer)
		started += 1
		writer:write("partial")
		error("the producer failed")
	end)
end, { auth = "public" })

api.get("/stream/endless", function()
	return api.response.stream(200, function(writer)
		started += 1
		local chunk = string.rep("x", 1024)
		while true do
			writer:write(chunk)
		end
	end)
end, { auth = "public" })

api.get("/stream/spin", function()
	return api.response.stream(200, function(writer)
		started += 1
		writer:write("x")
		while true do
		end
	end)
end, { auth = "public" })
"#;

struct Server {
    address: std::net::SocketAddr,
    handle: PluginExecutorHandle,
    _dir: tempfile::TempDir,
}

fn server_info() -> crate::plugins::server::ServerInfo {
    crate::plugins::server::ServerInfo {
        id: "stream-tests".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        commit_hash: "test".to_string(),
        hostname: "localhost".to_string(),
        port: 0,
        published_url: None,
        setup: crate::services::setup::Status {
            account_required: true,
            plugin_selection_required: true,
        },
    }
}

async fn serve() -> Result<Server> {
    crate::plugins::testing::init_default_test_state()?;
    crate::plugins::api::initialize_registry(std::collections::HashSet::new(), None).await;
    let dir = tempfile::tempdir()?;
    let plugins_dir = dir.path().join("plugins");
    let plugin_dir = plugins_dir.join(PLUGIN_ID);
    std::fs::create_dir_all(&plugin_dir)?;
    std::fs::write(
        plugin_dir.join("plugin.json"),
        serde_json::json!({
            "schema_version": 1,
            "id": PLUGIN_ID,
            "name": PLUGIN_ID,
            "version": "1.0.0",
            "lyra": crate::plugins::testing::HOST_REQUIREMENT,
            "description": "",
            "entrypoint": "init.luau",
            "scopes": ["lyra.api"],
        })
        .to_string(),
    )?;
    std::fs::write(plugin_dir.join("init.luau"), INIT)?;
    let (handle, errors) = PluginExecutorHandle::spawn(move || {
        let (runtime, errors) =
            PluginExecutor::discover_from_plugins_dir(plugins_dir, server_info())?;
        let failed = runtime.exec_all();
        anyhow::ensure!(failed.is_empty(), "plugins failed to start: {failed:?}");
        Ok((runtime, errors))
    })?;
    anyhow::ensure!(errors.is_empty(), "plugin load errors: {errors:?}");

    for path in [
        "/count",
        "/ping",
        "/stream/ok",
        "/stream/fail",
        "/stream/endless",
        "/stream/spin",
    ] {
        crate::plugins::api::tests::registered_handler("GET", path)
            .await
            .with_context(|| format!("{path} is registered"))?;
    }
    let dispatch = handle.clone();
    let app = axum::Router::new().fallback(move |method: Method, uri: Uri, headers: HeaderMap| {
        let dispatch = dispatch.clone();
        async move { respond(&dispatch, method, uri, headers).await }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    tokio::spawn(async move { axum::serve(listener, app).await });
    Ok(Server {
        address,
        handle,
        _dir: dir,
    })
}

async fn respond(
    dispatch: &PluginExecutorHandle,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
) -> Response {
    use axum::response::IntoResponse;
    // Looked up each time, as a restart registers the routes anew.
    let Some(handler) = crate::plugins::api::tests::registered_handler("GET", uri.path()).await
    else {
        return axum::http::StatusCode::NOT_FOUND.into_response();
    };
    let request = ApiHandlerRequest {
        handler,
        plugin_id: PLUGIN_ID.to_string(),
        method: method.as_str().to_string(),
        path: uri.path().to_string(),
        headers: Vec::new(),
        query: HashMap::new(),
        params: HashMap::new(),
        body: Vec::new(),
        auth: None,
        client: Default::default(),
    };
    match dispatch.dispatch_api_handler(request).await {
        Ok(response) => super::plugin_api_response_to_axum(response, &method, &headers)
            .await
            .unwrap_or_else(|error| {
                (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    error.to_string(),
                )
                    .into_response()
            }),
        Err(error) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            error.to_string(),
        )
            .into_response(),
    }
}

impl Server {
    async fn connect(&self, request: &str) -> Result<tokio::net::TcpStream> {
        let mut stream = tokio::net::TcpStream::connect(self.address).await?;
        stream.write_all(request.as_bytes()).await?;
        Ok(stream)
    }

    /// Everything the server sends for `request` until it closes the connection.
    async fn exchange(&self, request: &str) -> Result<String> {
        let mut stream = self.connect(request).await?;
        let mut raw = Vec::new();
        tokio::time::timeout(Duration::from_secs(10), stream.read_to_end(&mut raw))
            .await
            .context("the server never closed the connection")??;
        Ok(String::from_utf8_lossy(&raw).into_owned())
    }

    async fn get(&self, path: &str) -> Result<String> {
        self.exchange(&format!(
            "GET {path} HTTP/1.1\r\nhost: test\r\nconnection: close\r\n\r\n"
        ))
        .await
    }

    async fn started(&self) -> Result<u64> {
        let raw = self.get("/count").await?;
        let body = raw.split("\r\n\r\n").nth(1).context("a body")?;
        let json: serde_json::Value = serde_json::from_str(body)?;
        json["started"].as_u64().context("a count")
    }
}

/// Reads from `stream` until `needle` has arrived.
async fn read_until(stream: &mut tokio::net::TcpStream, needle: &str) -> Result<String> {
    let mut raw = Vec::new();
    let mut buffer = [0u8; 4096];
    tokio::time::timeout(Duration::from_secs(10), async {
        while !String::from_utf8_lossy(&raw).contains(needle) {
            let read = stream.read(&mut buffer).await?;
            anyhow::ensure!(read > 0, "the connection closed before {needle:?}");
            raw.extend_from_slice(&buffer[..read]);
        }
        Ok(())
    })
    .await
    .with_context(|| format!("{needle:?} never arrived"))??;
    Ok(String::from_utf8_lossy(&raw).into_owned())
}

const TERMINATING_CHUNK: &str = "\r\n0\r\n\r\n";

#[tokio::test]
async fn streamed_bodies_on_the_wire() -> Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let server = serve().await?;

    // A finished producer ends the chunked body with its terminating chunk.
    let ok = server.get("/stream/ok").await?;
    assert!(ok.starts_with("HTTP/1.1 200"), "{ok}");
    let lowered = ok.to_ascii_lowercase();
    assert!(lowered.contains("transfer-encoding: chunked"), "{ok}");
    assert!(lowered.contains("content-type: text/plain"), "{ok}");
    assert!(!lowered.contains("content-length"), "{ok}");
    assert!(ok.ends_with(TERMINATING_CHUNK), "{ok}");
    for index in 1..=3 {
        assert!(ok.contains(&format!("chunk {index}\n")), "{ok}");
    }

    // A failure after the first chunk aborts the body: no terminating chunk ever comes.
    let failed = server.get("/stream/fail").await?;
    assert!(failed.starts_with("HTTP/1.1 200"), "{failed}");
    assert!(failed.contains("partial"), "{failed}");
    assert!(!failed.ends_with(TERMINATING_CHUNK), "{failed}");
    assert!(!failed.contains("\r\n0\r\n"), "{failed}");

    // HTTP/1.0 has no chunks: the body runs until the connection closes.
    let old = server
        .exchange("GET /stream/ok HTTP/1.0\r\nhost: test\r\n\r\n")
        .await?;
    assert!(old.starts_with("HTTP/1.0 200"), "{old}");
    assert!(
        !old.to_ascii_lowercase().contains("transfer-encoding"),
        "{old}"
    );
    assert!(
        old.ends_with("\r\n\r\nchunk 1\nchunk 2\nchunk 3\n"),
        "{old}"
    );

    // HEAD never starts the producer, so it takes no place among the streams.
    let before = server.started().await?;
    for _ in 0..20 {
        let head = server
            .exchange("HEAD /stream/endless HTTP/1.1\r\nhost: test\r\nconnection: close\r\n\r\n")
            .await?;
        assert!(head.starts_with("HTTP/1.1 200"), "{head}");
        assert!(
            head.ends_with("\r\n\r\n"),
            "a HEAD answer has no body: {head}"
        );
        assert!(
            !head.to_ascii_lowercase().contains("content-length"),
            "a HEAD claims no length the GET body lacks: {head}"
        );
    }
    assert_eq!(server.started().await?, before, "a HEAD started a producer");
    assert!(server.get("/stream/ok").await?.ends_with(TERMINATING_CHUNK));
    Ok(())
}

#[tokio::test]
async fn streams_wait_for_a_place_and_free_it_when_the_client_leaves() -> Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let server = serve().await?;
    let before = server.started().await?;

    // Clients that read nothing leave chunks queued behind them.
    let mut endless = Vec::new();
    for _ in 0..16 {
        let mut stream = server
            .connect("GET /stream/endless HTTP/1.1\r\nhost: test\r\n\r\n")
            .await?;
        read_until(&mut stream, "xxxx").await?;
        endless.push(stream);
    }
    assert_eq!(server.started().await?, before + 16);

    // A seventeenth waits for a place, while ordinary handlers go on.
    let mut waiting = server
        .connect("GET /stream/ok HTTP/1.1\r\nhost: test\r\nconnection: close\r\n\r\n")
        .await?;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        server.started().await?,
        before + 16,
        "past the cap, a stream waits"
    );
    assert!(server.get("/ping").await?.ends_with("pong"));

    // A client going away cancels its producer, queued chunks and all, and frees its place.
    drop(endless.pop());
    let rest = read_until(&mut waiting, TERMINATING_CHUNK).await?;
    assert!(rest.contains("chunk 3\n"), "{rest}");
    drop(endless);
    for _ in 0..50 {
        if server.get("/stream/ok").await?.ends_with(TERMINATING_CHUNK) {
            break;
        }
    }
    Ok(())
}

#[tokio::test]
async fn a_producer_keeping_up_with_its_client_leaves_room_for_other_work() -> Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let server = serve().await?;
    let mut stream = server
        .connect("GET /stream/endless HTTP/1.1\r\nhost: test\r\n\r\n")
        .await?;
    let reader = tokio::spawn(async move {
        let mut buffer = vec![0u8; 64 * 1024];
        let mut total = 0usize;
        loop {
            match stream.read(&mut buffer).await {
                Ok(0) | Err(_) => return total,
                Ok(read) => total += read,
            }
        }
    });
    for _ in 0..10 {
        let pong = tokio::time::timeout(Duration::from_secs(2), server.get("/ping"))
            .await
            .context("an ordinary handler waited on a stream")??;
        assert!(pong.ends_with("pong"), "{pong}");
    }
    reader.abort();
    Ok(())
}

#[tokio::test]
async fn a_client_leaving_interrupts_a_computing_producer() -> Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let server = serve().await?;
    let mut stream = server
        .connect("GET /stream/spin HTTP/1.1\r\nhost: test\r\n\r\n")
        .await?;
    read_until(&mut stream, "\r\n\r\n").await?;
    drop(stream);
    let pong = tokio::time::timeout(Duration::from_secs(10), server.get("/ping"))
        .await
        .context("the computing producer kept the executor")??;
    assert!(pong.ends_with("pong"), "{pong}");
    Ok(())
}

#[tokio::test]
async fn stopping_the_plugin_interrupts_a_computing_producer() -> Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let server = serve().await?;
    // The client stays, so only the stop can end the producer's loop.
    let mut stream = server
        .connect("GET /stream/spin HTTP/1.1\r\nhost: test\r\n\r\n")
        .await?;
    read_until(&mut stream, "\r\n\r\n").await?;
    tokio::time::timeout(
        Duration::from_secs(10),
        server.handle.stop_plugin(PLUGIN_ID),
    )
    .await
    .context("the stop waited on the computing producer")??;
    let mut rest = Vec::new();
    tokio::time::timeout(Duration::from_secs(10), stream.read_to_end(&mut rest))
        .await
        .context("the stream outlived its plugin's run")??;
    let rest = String::from_utf8_lossy(&rest);
    assert!(!rest.ends_with(TERMINATING_CHUNK), "{rest}");
    Ok(())
}

#[tokio::test]
async fn a_restarted_plugin_streams_again() -> Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let server = serve().await?;
    let mut stream = server
        .connect("GET /stream/endless HTTP/1.1\r\nhost: test\r\n\r\n")
        .await?;
    read_until(&mut stream, "xxxx").await?;
    // As the lifecycle restarts a plugin: stop, tear its routes down, start its next run.
    server.handle.stop_plugin(PLUGIN_ID).await?;
    let plugin_id = crate::services::plugin_scope::PluginId::new(PLUGIN_ID)?;
    crate::plugins::api::teardown_plugin_routes(&plugin_id).await;
    server.handle.restart_plugin(PLUGIN_ID).await?;
    let mut rest = Vec::new();
    tokio::time::timeout(Duration::from_secs(10), stream.read_to_end(&mut rest))
        .await
        .context("the stream outlived its plugin's run")??;
    assert!(!String::from_utf8_lossy(&rest).ends_with(TERMINATING_CHUNK));
    // The new run's streams start unstopped.
    let ok = server.get("/stream/ok").await?;
    assert!(ok.ends_with(TERMINATING_CHUNK), "{ok}");
    Ok(())
}

#[tokio::test]
async fn stopping_the_plugin_aborts_its_streams() -> Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    let server = serve().await?;
    let mut stream = server
        .connect("GET /stream/endless HTTP/1.1\r\nhost: test\r\n\r\n")
        .await?;
    read_until(&mut stream, "xxxx").await?;
    server.handle.stop_plugin(PLUGIN_ID).await?;
    let mut rest = Vec::new();
    tokio::time::timeout(Duration::from_secs(10), stream.read_to_end(&mut rest))
        .await
        .context("the stream outlived its plugin's run")??;
    let rest = String::from_utf8_lossy(&rest);
    assert!(!rest.ends_with(TERMINATING_CHUNK), "{rest}");
    Ok(())
}
