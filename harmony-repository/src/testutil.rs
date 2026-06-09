// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use flate2::Compression;
use flate2::write::GzEncoder;
use tar::{
    EntryType,
    Header,
};
use tokio::io::{
    AsyncReadExt,
    AsyncWriteExt,
};
use tokio::net::TcpListener;

pub(crate) fn client() -> reqwest::Client {
    let _ = rustls::crypto::ring::default_provider().install_default();
    reqwest::Client::new()
}

pub(crate) struct CannedResponse {
    status: u16,
    content_type: &'static str,
    body: Vec<u8>,
    content_length_override: Option<u64>,
}

impl CannedResponse {
    pub(crate) fn json(body: &str) -> Self {
        Self {
            status: 200,
            content_type: "application/json",
            body: body.as_bytes().to_vec(),
            content_length_override: None,
        }
    }

    pub(crate) fn targz(body: Vec<u8>) -> Self {
        Self {
            status: 200,
            content_type: "application/gzip",
            body,
            content_length_override: None,
        }
    }

    pub(crate) fn with_content_length(mut self, length: u64) -> Self {
        self.content_length_override = Some(length);
        self
    }

    fn render(&self) -> Vec<u8> {
        let reason = match self.status {
            200 => "OK",
            404 => "Not Found",
            _ => "Status",
        };
        let length = self
            .content_length_override
            .unwrap_or(self.body.len() as u64);
        let mut response = format!(
            "HTTP/1.1 {} {reason}\r\nContent-Type: {}\r\nContent-Length: {length}\r\nConnection: close\r\n\r\n",
            self.status, self.content_type
        )
        .into_bytes();
        response.extend_from_slice(&self.body);
        response
    }
}

/// Minimal HTTP/1.1 server resolving exact request paths to canned
/// responses; unknown paths return 404.
pub(crate) struct TestServer {
    addr: SocketAddr,
    handle: tokio::task::JoinHandle<()>,
}

impl TestServer {
    pub(crate) async fn start(routes: HashMap<String, CannedResponse>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test server");
        let addr = listener.local_addr().expect("test server addr");
        let routes = Arc::new(routes);

        let handle = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let routes = routes.clone();
                tokio::spawn(async move {
                    let mut buffer = vec![0u8; 16 * 1024];
                    let mut read = 0;
                    loop {
                        match stream.read(&mut buffer[read..]).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => read += n,
                        }
                        if buffer[..read].windows(4).any(|w| w == b"\r\n\r\n")
                            || read == buffer.len()
                        {
                            break;
                        }
                    }

                    let request = String::from_utf8_lossy(&buffer[..read]);
                    let path = request.split_whitespace().nth(1).unwrap_or("/");
                    let response = match routes.get(path) {
                        Some(canned) => canned.render(),
                        None => CannedResponse {
                            status: 404,
                            content_type: "text/plain",
                            body: b"not found".to_vec(),
                            content_length_override: None,
                        }
                        .render(),
                    };
                    let _ = stream.write_all(&response).await;
                    let _ = stream.shutdown().await;
                });
            }
        });

        Self { addr, handle }
    }

    pub(crate) fn url(&self) -> String {
        format!("http://{}", self.addr)
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

pub(crate) struct TarGzBuilder {
    builder: tar::Builder<GzEncoder<Vec<u8>>>,
}

impl TarGzBuilder {
    pub(crate) fn file(&mut self, path: &str, content: &str) {
        let mut header = Header::new_gnu();
        header.set_entry_type(EntryType::Regular);
        header.set_mode(0o644);
        header.set_size(content.len() as u64);
        self.builder
            .append_data(&mut header, path, content.as_bytes())
            .expect("append tar entry");
    }

    /// Writes the path straight into the header, bypassing the builder's
    /// traversal validation, to fabricate hostile archives.
    pub(crate) fn file_with_raw_path(&mut self, path: &str, content: &str) {
        let mut header = Header::new_gnu();
        header.set_entry_type(EntryType::Regular);
        header.set_mode(0o644);
        header.set_size(content.len() as u64);
        let name = &mut header.as_gnu_mut().expect("gnu header").name;
        name[..path.len()].copy_from_slice(path.as_bytes());
        header.set_cksum();
        self.builder
            .append(&header, content.as_bytes())
            .expect("append raw tar entry");
    }

    pub(crate) fn symlink(&mut self, path: &str, target: &str) {
        let mut header = Header::new_gnu();
        header.set_entry_type(EntryType::Symlink);
        header.set_mode(0o777);
        header.set_size(0);
        self.builder
            .append_link(&mut header, path, target)
            .expect("append tar symlink");
    }

    pub(crate) fn finish(self) -> Vec<u8> {
        self.builder
            .into_inner()
            .expect("finish tar")
            .finish()
            .expect("finish gzip")
    }
}

pub(crate) fn targz_entries() -> TarGzBuilder {
    TarGzBuilder {
        builder: tar::Builder::new(GzEncoder::new(Vec::new(), Compression::default())),
    }
}

pub(crate) fn targz(files: &[(&str, &str)]) -> Vec<u8> {
    let mut builder = targz_entries();
    for (path, content) in files {
        builder.file(path, content);
    }
    builder.finish()
}
