// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::{
    HashMap,
    HashSet,
};
use std::net::SocketAddr;
use std::sync::{
    Arc,
    Mutex,
};

use flate2::Compression;
use flate2::write::GzEncoder;
use harmony_core::plugin::PluginHost;
use tar::{
    EntryType,
    Header,
};
use tokio::io::{
    AsyncReadExt,
    AsyncWriteExt,
};
use tokio::net::TcpListener;

pub(crate) fn host() -> PluginHost {
    PluginHost::new("host", semver::Version::new(0, 0, 1), HashSet::new()).unwrap()
}

pub(crate) fn client() -> reqwest::Client {
    let _ = rustls::crypto::ring::default_provider().install_default();
    reqwest::Client::new()
}

pub(crate) struct CannedResponse {
    status: u16,
    content_type: &'static str,
    body: Vec<u8>,
    content_length_override: Option<u64>,
    headers: Vec<(&'static str, String)>,
}

impl CannedResponse {
    pub(crate) fn json(body: &str) -> Self {
        Self {
            status: 200,
            content_type: "application/json",
            body: body.as_bytes().to_vec(),
            content_length_override: None,
            headers: Vec::new(),
        }
    }

    pub(crate) fn targz(body: Vec<u8>) -> Self {
        Self {
            status: 200,
            content_type: "application/gzip",
            body,
            content_length_override: None,
            headers: Vec::new(),
        }
    }

    pub(crate) fn bytes(body: Vec<u8>) -> Self {
        Self {
            status: 200,
            content_type: "application/octet-stream",
            body,
            content_length_override: None,
            headers: Vec::new(),
        }
    }

    pub(crate) fn upload_pack(body: &str) -> Self {
        Self {
            status: 200,
            content_type: "application/x-git-upload-pack-result",
            body: body.as_bytes().to_vec(),
            content_length_override: None,
            headers: Vec::new(),
        }
    }

    pub(crate) fn status(status: u16) -> Self {
        Self {
            status,
            content_type: "text/plain",
            body: Vec::new(),
            content_length_override: None,
            headers: Vec::new(),
        }
    }

    pub(crate) fn with_status(mut self, status: u16) -> Self {
        self.status = status;
        self
    }

    pub(crate) fn with_content_length(mut self, length: u64) -> Self {
        self.content_length_override = Some(length);
        self
    }

    pub(crate) fn with_header(mut self, name: &'static str, value: &str) -> Self {
        self.headers.push((name, value.to_string()));
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
        let headers: String = self
            .headers
            .iter()
            .map(|(name, value)| format!("{name}: {value}\r\n"))
            .collect();
        let mut response = format!(
            "HTTP/1.1 {} {reason}\r\nContent-Type: {}\r\nContent-Length: {length}\r\n{headers}Connection: close\r\n\r\n",
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
    requests: Arc<Mutex<Vec<String>>>,
}

impl TestServer {
    pub(crate) async fn start(routes: HashMap<String, CannedResponse>) -> Self {
        Self::start_with(|_| routes).await
    }

    /// Like [`Self::start`], but the route table may reference the
    /// server's own base URL.
    pub(crate) async fn start_with(
        routes: impl FnOnce(&str) -> HashMap<String, CannedResponse>,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test server");
        let addr = listener.local_addr().expect("test server addr");
        let routes = Arc::new(routes(&format!("http://{addr}")));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let log = requests.clone();

        let handle = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let routes = routes.clone();
                let log = log.clone();
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
                    let head = request.split("\r\n\r\n").next().unwrap_or_default();
                    log.lock().unwrap().push(head.to_string());
                    let response = match routes.get(path) {
                        Some(canned) => canned.render(),
                        None => CannedResponse {
                            status: 404,
                            content_type: "text/plain",
                            body: b"not found".to_vec(),
                            content_length_override: None,
                            headers: Vec::new(),
                        }
                        .render(),
                    };
                    let _ = stream.write_all(&response).await;
                    let _ = stream.shutdown().await;
                });
            }
        });

        Self {
            addr,
            handle,
            requests,
        }
    }

    /// Request paths received so far, in arrival order.
    pub(crate) fn requests(&self) -> Vec<String> {
        self.request_heads()
            .iter()
            .map(|head| head.split_whitespace().nth(1).unwrap_or("/").to_string())
            .collect()
    }

    /// Request lines and headers received so far, in arrival order.
    pub(crate) fn request_heads(&self) -> Vec<String> {
        self.requests.lock().unwrap().clone()
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

pub(crate) fn plugin_json(id: &str) -> String {
    format!(
        r#"{{
            "schema_version": 1,
            "id": "{id}",
            "name": "{id}",
            "version": "0.0.1",
            "host": "0.0.1",
            "description": "test plugin",
            "entrypoint": "init.luau",
            "scopes": []
        }}"#
    )
}

/// A `versions.json` listing each `(commit, requirement)` release, newest
/// first.
pub(crate) fn versions_json(releases: &[(&str, &str)]) -> String {
    let entries: Vec<String> = releases
        .iter()
        .map(|(commit, requirement)| {
            format!(r#"{{ "commit": "{commit}", "host": "{requirement}" }}"#)
        })
        .collect();
    format!("[{}]", entries.join(", "))
}

/// The commit test repositories release at.
pub(crate) fn release_commit() -> String {
    "c".repeat(40)
}

/// A `versions.json` releasing [`release_commit`] to host 0.0.1.
pub(crate) fn released() -> String {
    versions_json(&[(&release_commit(), "0.0.1")])
}

/// Routes for a repository holding the single valid plugin `id` at its
/// root: its `main` archive, which the forge API cannot resolve, and its
/// release's raw `plugin.json` and archive.
pub(crate) fn single_plugin_routes(owner_repo: &str, id: &str) -> Vec<(String, CannedResponse)> {
    vec![
        raw_file(
            owner_repo,
            &release_commit(),
            "plugin.json",
            &plugin_json(id),
        ),
        (
            format!("/{owner_repo}/archive/main.tar.gz"),
            CannedResponse::targz(targz(&[
                ("repo/plugin.json", &plugin_json(id)),
                ("repo/versions.json", &released()),
            ])),
        ),
        (
            format!("/{owner_repo}/archive/{}.tar.gz", release_commit()),
            CannedResponse::targz(targz(&[
                ("repo/plugin.json", &plugin_json(id)),
                ("repo/init.luau", "return {}"),
            ])),
        ),
    ]
}

/// Routes for a Gitea-style repository whose default branch `main` is at
/// `sha`, serving `files` as raw files and no archives.
pub(crate) fn raw_repo_routes(
    owner_repo: &str,
    sha: &str,
    files: &[(&str, &str)],
) -> Vec<(String, CannedResponse)> {
    let mut routes = vec![
        (
            format!("/api/v1/repos/{owner_repo}"),
            CannedResponse::json(r#"{"default_branch":"main"}"#),
        ),
        (
            format!("/api/v1/repos/{owner_repo}/git/commits/main"),
            CannedResponse::json(&format!(r#"{{"sha":"{sha}"}}"#)),
        ),
    ];
    routes.extend(
        files
            .iter()
            .map(|(path, content)| raw_file(owner_repo, sha, path, content)),
    );
    routes
}

/// Route for a Gitea-style raw file of `owner_repo` at `commit`.
pub(crate) fn raw_file(
    owner_repo: &str,
    commit: &str,
    path: &str,
    content: &str,
) -> (String, CannedResponse) {
    (
        format!("/{owner_repo}/raw/commit/{commit}/{path}"),
        CannedResponse::json(content),
    )
}

/// Routes for a GitLab-style project `owner/repo` whose default branch
/// `main` is at `sha`, serving `files` as raw files and no archives.
pub(crate) fn gitlab_repo_routes(
    sha: &str,
    files: &[(&str, &str)],
) -> Vec<(String, CannedResponse)> {
    let project = "/api/v4/projects/owner%2Frepo";
    let mut routes = vec![
        (
            project.to_string(),
            CannedResponse::json(r#"{"default_branch":"main"}"#),
        ),
        (
            format!("{project}/repository/commits/main"),
            CannedResponse::json(&format!(r#"{{"id":"{sha}"}}"#)),
        ),
    ];
    routes.extend(
        files
            .iter()
            .map(|(path, content)| gitlab_raw_file(sha, path, content)),
    );
    routes
}

/// Route for a raw file of the GitLab-style project `owner/repo` at
/// `commit`.
pub(crate) fn gitlab_raw_file(commit: &str, path: &str, content: &str) -> (String, CannedResponse) {
    (
        format!(
            "/api/v4/projects/owner%2Frepo/repository/files/{}/raw?ref={commit}",
            path.replace('/', "%2F")
        ),
        CannedResponse::json(content),
    )
}

/// Route for GitLab's archive of `dir` alone at `sha`, its entries kept
/// under their full path inside one top-level directory.
pub(crate) fn gitlab_directory_archive(
    sha: &str,
    dir: &str,
    archive: Vec<u8>,
) -> (String, CannedResponse) {
    (
        format!(
            "/api/v4/projects/owner%2Frepo/repository/archive.tar.gz?sha={sha}&path={}",
            dir.replace('/', "%2F")
        ),
        CannedResponse::targz(archive),
    )
}

/// `lines` as pkt-lines ended by a flush packet, the shape of an `ls-refs`
/// response.
pub(crate) fn pkt_lines(lines: &[&str]) -> String {
    let mut body: String = lines
        .iter()
        .map(|line| format!("{:04x}{line}", line.len() + 4))
        .collect();
    body.push_str("0000");
    body
}
