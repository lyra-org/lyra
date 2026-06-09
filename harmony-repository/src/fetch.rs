// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::io::Cursor;
use std::path::{
    Path,
    PathBuf,
};

use flate2::read::GzDecoder;
use percent_encoding::{
    AsciiSet,
    NON_ALPHANUMERIC,
    utf8_percent_encode,
};
use reqwest::{
    Client,
    StatusCode,
    header,
};
use serde::{
    Deserialize,
    Serialize,
};
use tar::{
    Archive,
    EntryType,
};
use tempfile::TempDir;
use url::Url;

const FALLBACK_REFS: &[&str] = &["main", "master", "trunk"];
const USER_AGENT: &str = concat!("harmony-repository/", env!("CARGO_PKG_VERSION"));

pub const MAX_ARCHIVE_BYTES: u64 = 50 * 1024 * 1024;
pub const MAX_UNPACKED_BYTES: u64 = 256 * 1024 * 1024;
pub const MAX_ARCHIVE_ENTRIES: usize = 16 * 1024;

/// Percent-encode set for refs embedded in API URL path segments. Keeps the
/// unreserved characters and encodes everything else, including `/`, so refs
/// like `release/v2` stay a single segment.
const REF_SEGMENT: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~');

#[derive(Debug, thiserror::Error)]
pub enum FetchError {
    #[error("invalid repository URL '{url}': {reason}")]
    InvalidUrl { url: String, reason: String },
    #[error("failed to fetch {url}: {source}")]
    Network {
        url: String,
        #[source]
        source: reqwest::Error,
    },
    #[error("failed to fetch {url}: HTTP {status}")]
    Status { url: String, status: StatusCode },
    #[error("archive at {url} exceeds the {limit} byte limit")]
    ArchiveTooLarge { url: String, limit: u64 },
    #[error("invalid archive: {reason}")]
    InvalidArchive { reason: String },
    #[error("archive entry '{path}' is not allowed: {reason}")]
    UnsafeArchiveEntry { path: String, reason: String },
    #[error("no downloadable ref found for {repo}: {detail}")]
    RefNotFound { repo: String, detail: String },
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Forge {
    GitHub,
    GitLab,
    Gitea,
}

impl Forge {
    pub fn as_str(self) -> &'static str {
        match self {
            Forge::GitHub => "github",
            Forge::GitLab => "gitlab",
            Forge::Gitea => "gitea",
        }
    }

    fn from_override(value: &str) -> Option<Self> {
        match value.to_ascii_lowercase().as_str() {
            "github" => Some(Forge::GitHub),
            "gitlab" => Some(Forge::GitLab),
            "gitea" | "forgejo" => Some(Forge::Gitea),
            _ => None,
        }
    }
}

/// A Git repository on a supported forge, parsed from a user-supplied URL.
///
/// Accepts plain repository URLs as well as browser URLs carrying a ref
/// (`/tree/<ref>`, `/blob/<ref>`, GitLab `/-/tree/<ref>`, Gitea
/// `/src/branch/<ref>`) and the query parameters `?ref=` and `?forge=`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoSpec {
    pub scheme: String,
    /// Hostname, including `:port` when one is present in the URL.
    pub host: String,
    pub namespace: String,
    pub repo_name: String,
    pub forge: Forge,
    pub explicit_ref: Option<String>,
}

impl RepoSpec {
    pub fn parse(input: &str, ref_override: Option<&str>) -> Result<Self, FetchError> {
        let invalid = |reason: String| FetchError::InvalidUrl {
            url: input.to_string(),
            reason,
        };

        let url = Url::parse(input).map_err(|e| invalid(e.to_string()))?;

        let scheme = url.scheme().to_string();
        if scheme != "http" && scheme != "https" {
            return Err(invalid(format!(
                "unsupported scheme '{scheme}'; only http and https are supported"
            )));
        }

        let hostname = url
            .host_str()
            .ok_or_else(|| invalid("missing host".into()))?
            .to_ascii_lowercase();
        let host = match url.port() {
            Some(port) => format!("{hostname}:{port}"),
            None => hostname.clone(),
        };

        let mut segments: Vec<String> = url
            .path_segments()
            .map(|segments| {
                segments
                    .filter(|s| !s.is_empty())
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();

        let mut ref_from_url = extract_ref_from_segments(&mut segments);
        let mut forge_override = None;

        for (key, value) in url.query_pairs() {
            match key.as_ref() {
                "ref" if ref_from_url.is_none() => ref_from_url = Some(value.into_owned()),
                "forge" => {
                    forge_override = Some(Forge::from_override(&value).ok_or_else(|| {
                        invalid(format!(
                            "unknown forge '{value}'; expected github, gitlab, gitea, or forgejo"
                        ))
                    })?);
                }
                _ => {}
            }
        }

        if let Some(last) = segments.last_mut()
            && let Some(stripped) = last.strip_suffix(".git")
        {
            *last = stripped.to_string();
        }

        if segments.len() < 2 {
            return Err(invalid(
                "expected at least an owner and a repository name in the path".into(),
            ));
        }

        let forge = forge_override.unwrap_or_else(|| detect_forge(&hostname));

        let (namespace, repo_name) = match forge {
            Forge::GitLab => {
                let repo = segments.last().cloned().expect("checked above");
                let namespace = segments[..segments.len() - 1].join("/");
                (namespace, repo)
            }
            Forge::GitHub | Forge::Gitea => {
                if segments.len() != 2 {
                    return Err(invalid(format!(
                        "{} URLs must be of the form {scheme}://{host}/<owner>/<repo>",
                        forge.as_str()
                    )));
                }
                (segments[0].clone(), segments[1].clone())
            }
        };

        if repo_name.is_empty() {
            return Err(invalid("missing repository name".into()));
        }

        let explicit_ref = ref_override.map(str::to_string).or(ref_from_url);
        if let Some(git_ref) = &explicit_ref
            && git_ref.is_empty()
        {
            return Err(invalid("ref must not be empty".into()));
        }

        Ok(Self {
            scheme,
            host,
            namespace,
            repo_name,
            forge,
            explicit_ref,
        })
    }

    pub fn base_url(&self) -> String {
        format!("{}://{}", self.scheme, self.host)
    }

    /// Canonical form used for persistence and self-reference comparison.
    pub fn canonical_url(&self) -> String {
        format!("{}/{}/{}", self.base_url(), self.namespace, self.repo_name)
    }

    pub fn human_id(&self) -> String {
        format!("{}/{}/{}", self.host, self.namespace, self.repo_name)
    }

    /// Whether both specs name the same repository, ignoring refs. Namespace
    /// and name compare case-insensitively since the major forges treat them
    /// that way for routing.
    pub fn is_same_repo(&self, other: &RepoSpec) -> bool {
        self.host == other.host
            && self.namespace.eq_ignore_ascii_case(&other.namespace)
            && self.repo_name.eq_ignore_ascii_case(&other.repo_name)
    }

    pub fn tarball_url(&self, git_ref: &str) -> String {
        let base = self.base_url();
        match self.forge {
            Forge::GitHub | Forge::Gitea => format!(
                "{base}/{}/{}/archive/{git_ref}.tar.gz",
                self.namespace, self.repo_name
            ),
            Forge::GitLab => format!(
                "{base}/{}/{}/-/archive/{git_ref}/{}-{git_ref}.tar.gz",
                self.namespace, self.repo_name, self.repo_name
            ),
        }
    }

    fn repo_api_url(&self) -> String {
        let base = self.base_url();
        match self.forge {
            Forge::GitHub if self.host == "github.com" => format!(
                "https://api.github.com/repos/{}/{}",
                self.namespace, self.repo_name
            ),
            Forge::GitHub => format!("{base}/api/v3/repos/{}/{}", self.namespace, self.repo_name),
            Forge::GitLab => {
                let project = format!("{}/{}", self.namespace, self.repo_name);
                format!(
                    "{base}/api/v4/projects/{}",
                    utf8_percent_encode(&project, REF_SEGMENT)
                )
            }
            Forge::Gitea => format!("{base}/api/v1/repos/{}/{}", self.namespace, self.repo_name),
        }
    }

    fn commit_api_url(&self, git_ref: &str) -> String {
        let repo = self.repo_api_url();
        let git_ref = utf8_percent_encode(git_ref, REF_SEGMENT);
        match self.forge {
            Forge::GitHub => format!("{repo}/commits/{git_ref}"),
            Forge::GitLab => format!("{repo}/repository/commits/{git_ref}"),
            Forge::Gitea => format!("{repo}/git/commits/{git_ref}"),
        }
    }
}

fn extract_ref_from_segments(segments: &mut Vec<String>) -> Option<String> {
    if let Some(idx) = segments.iter().position(|segment| segment == "-") {
        let git_ref = segments
            .get(idx + 1)
            .filter(|verb| *verb == "tree" || *verb == "blob")
            .and_then(|_| segments.get(idx + 2))
            .cloned();
        segments.truncate(idx);
        return git_ref;
    }

    for verb in ["tree", "blob", "src"] {
        let Some(idx) = segments
            .iter()
            .position(|segment| segment == verb)
            .filter(|idx| *idx >= 2 && segments.len() > *idx + 1)
        else {
            continue;
        };

        let next = &segments[idx + 1];
        let git_ref = if verb == "src" && next == "branch" {
            segments.get(idx + 2).unwrap_or(next).clone()
        } else {
            next.clone()
        };
        segments.truncate(idx);
        return Some(git_ref);
    }

    None
}

fn detect_forge(hostname: &str) -> Forge {
    if hostname == "github.com" || hostname.starts_with("ghe.") || hostname.contains(".github.") {
        return Forge::GitHub;
    }
    if hostname.contains("gitlab") {
        return Forge::GitLab;
    }
    Forge::Gitea
}

fn is_commit_sha(value: &str) -> bool {
    matches!(value.len(), 40 | 64) && value.chars().all(|c| c.is_ascii_hexdigit())
}

/// A repository tree downloaded into a temporary directory. The directory is
/// removed when this value is dropped; callers copy what they keep.
#[derive(Debug)]
pub struct FetchedRepo {
    _tempdir: TempDir,
    root: PathBuf,
    pub git_ref: String,
    pub commit: Option<String>,
}

impl FetchedRepo {
    pub fn root(&self) -> &Path {
        &self.root
    }
}

struct RefPlan {
    candidates: Vec<String>,
    commit: Option<String>,
    /// True when the candidates came from an explicit ref or the forge API.
    /// Guessed candidates may 404 without it being an error.
    authoritative: bool,
}

/// Downloads the repository as a tarball and extracts it.
///
/// The ref is resolved to a commit through the forge API when possible so
/// the download is pinned and the commit can be recorded for update checks.
/// Unreachable forge APIs (rate limits, exotic self-hosted setups) degrade
/// to probing common branch names with `commit: None`.
pub async fn fetch_repo(client: &Client, spec: &RepoSpec) -> Result<FetchedRepo, FetchError> {
    let plan = plan_refs(client, spec).await;
    let mut last_status: Option<FetchError> = None;

    for candidate in &plan.candidates {
        let download_ref = plan.commit.as_deref().unwrap_or(candidate);
        let url = spec.tarball_url(download_ref);

        let bytes = match download(client, &url).await {
            Ok(bytes) => bytes,
            Err(FetchError::Status { url, status })
                if status == StatusCode::NOT_FOUND && !plan.authoritative =>
            {
                last_status = Some(FetchError::Status { url, status });
                continue;
            }
            Err(err) => return Err(err),
        };

        let tempdir = TempDir::new()?;
        let dest = tempdir.path().to_path_buf();
        tokio::task::spawn_blocking(move || extract_archive(&bytes, &dest))
            .await
            .map_err(|e| FetchError::InvalidArchive {
                reason: format!("extraction task failed: {e}"),
            })??;

        return Ok(FetchedRepo {
            root: locate_root(tempdir.path())?,
            _tempdir: tempdir,
            git_ref: candidate.clone(),
            commit: plan.commit.clone(),
        });
    }

    Err(FetchError::RefNotFound {
        repo: spec.human_id(),
        detail: last_status
            .map(|e| e.to_string())
            .unwrap_or_else(|| "no candidate refs".into()),
    })
}

async fn plan_refs(client: &Client, spec: &RepoSpec) -> RefPlan {
    if let Some(explicit) = &spec.explicit_ref {
        let commit = if is_commit_sha(explicit) {
            Some(explicit.to_ascii_lowercase())
        } else {
            resolve_commit(client, spec, explicit).await
        };
        return RefPlan {
            candidates: vec![explicit.clone()],
            commit,
            authoritative: true,
        };
    }

    if let Some(branch) = default_branch(client, spec).await {
        let commit = resolve_commit(client, spec, &branch).await;
        return RefPlan {
            candidates: vec![branch],
            commit,
            authoritative: true,
        };
    }

    RefPlan {
        candidates: FALLBACK_REFS.iter().map(|r| r.to_string()).collect(),
        commit: None,
        authoritative: false,
    }
}

/// Resolves the repository's default branch through the forge API. All three
/// supported forges expose it as `default_branch`.
pub async fn default_branch(client: &Client, spec: &RepoSpec) -> Option<String> {
    let value = api_get(client, &spec.repo_api_url()).await?;
    let branch = value.get("default_branch")?.as_str()?;
    (!branch.is_empty()).then(|| branch.to_string())
}

/// Resolves a ref to a commit SHA through the forge API.
pub async fn resolve_commit(client: &Client, spec: &RepoSpec, git_ref: &str) -> Option<String> {
    let value = api_get(client, &spec.commit_api_url(git_ref)).await?;
    let key = match spec.forge {
        Forge::GitLab => "id",
        Forge::GitHub | Forge::Gitea => "sha",
    };
    value.get(key)?.as_str().map(str::to_string)
}

async fn api_get(client: &Client, url: &str) -> Option<serde_json::Value> {
    let response = client
        .get(url)
        .header(header::USER_AGENT, USER_AGENT)
        .header(header::ACCEPT, "application/json")
        .send()
        .await;

    let response = match response {
        Ok(response) => response,
        Err(err) => {
            tracing::debug!("forge api request {url} failed: {err}");
            return None;
        }
    };

    if !response.status().is_success() {
        tracing::debug!(
            "forge api request {url} returned HTTP {}",
            response.status()
        );
        return None;
    }

    match response.json().await {
        Ok(value) => Some(value),
        Err(err) => {
            tracing::debug!("forge api response from {url} is not JSON: {err}");
            None
        }
    }
}

async fn download(client: &Client, url: &str) -> Result<Vec<u8>, FetchError> {
    let mut response = client
        .get(url)
        .header(header::USER_AGENT, USER_AGENT)
        .send()
        .await
        .map_err(|source| FetchError::Network {
            url: url.to_string(),
            source,
        })?;

    let status = response.status();
    if !status.is_success() {
        return Err(FetchError::Status {
            url: url.to_string(),
            status,
        });
    }

    if response
        .content_length()
        .is_some_and(|len| len > MAX_ARCHIVE_BYTES)
    {
        return Err(FetchError::ArchiveTooLarge {
            url: url.to_string(),
            limit: MAX_ARCHIVE_BYTES,
        });
    }

    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|source| FetchError::Network {
            url: url.to_string(),
            source,
        })?
    {
        if (bytes.len() + chunk.len()) as u64 > MAX_ARCHIVE_BYTES {
            return Err(FetchError::ArchiveTooLarge {
                url: url.to_string(),
                limit: MAX_ARCHIVE_BYTES,
            });
        }
        bytes.extend_from_slice(&chunk);
    }

    Ok(bytes)
}

fn extract_archive(bytes: &[u8], dest: &Path) -> Result<(), FetchError> {
    let invalid = |reason: String| FetchError::InvalidArchive { reason };

    let mut archive = Archive::new(GzDecoder::new(Cursor::new(bytes)));
    let mut entry_count = 0usize;
    let mut unpacked_bytes = 0u64;

    for entry in archive.entries().map_err(|e| invalid(e.to_string()))? {
        let mut entry = entry.map_err(|e| invalid(e.to_string()))?;
        let path = entry
            .path()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|_| "<undecodable path>".to_string());

        match entry.header().entry_type() {
            EntryType::Regular | EntryType::Directory => {}
            EntryType::XGlobalHeader
            | EntryType::XHeader
            | EntryType::GNULongName
            | EntryType::GNULongLink => continue,
            other => {
                return Err(FetchError::UnsafeArchiveEntry {
                    path,
                    reason: format!("entry type {other:?} is not allowed"),
                });
            }
        }

        entry_count += 1;
        if entry_count > MAX_ARCHIVE_ENTRIES {
            return Err(invalid(format!("more than {MAX_ARCHIVE_ENTRIES} entries")));
        }

        unpacked_bytes = unpacked_bytes.saturating_add(entry.size());
        if unpacked_bytes > MAX_UNPACKED_BYTES {
            return Err(invalid(format!(
                "unpacked size exceeds {MAX_UNPACKED_BYTES} bytes"
            )));
        }

        if !entry.unpack_in(dest).map_err(|e| invalid(e.to_string()))? {
            return Err(FetchError::UnsafeArchiveEntry {
                path,
                reason: "path escapes the extraction directory".into(),
            });
        }
    }

    Ok(())
}

/// Forge tarballs wrap the tree in a single `<repo>-<ref>` directory; unwrap
/// it so callers see the repository root.
fn locate_root(extracted: &Path) -> Result<PathBuf, FetchError> {
    let entries = std::fs::read_dir(extracted)?.collect::<Result<Vec<_>, _>>()?;

    if entries.len() == 1 && entries[0].file_type()?.is_dir() {
        return Ok(entries[0].path());
    }

    Ok(extracted.to_path_buf())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::testutil::{
        CannedResponse,
        TestServer,
        client,
        targz,
        targz_entries,
    };

    #[test]
    fn parses_github_url() {
        let spec = RepoSpec::parse("https://github.com/owner/repo.git", None).unwrap();
        assert_eq!(spec.forge, Forge::GitHub);
        assert_eq!(spec.namespace, "owner");
        assert_eq!(spec.repo_name, "repo");
        assert_eq!(spec.explicit_ref, None);
    }

    #[test]
    fn parses_gitlab_nested_namespace() {
        let spec =
            RepoSpec::parse("https://gitlab.example.org/platform/plugins/example", None).unwrap();
        assert_eq!(spec.forge, Forge::GitLab);
        assert_eq!(spec.host, "gitlab.example.org");
        assert_eq!(spec.namespace, "platform/plugins");
        assert_eq!(spec.repo_name, "example");
    }

    #[test]
    fn parses_codeberg_as_gitea() {
        let spec = RepoSpec::parse("https://codeberg.org/owner/repo", None).unwrap();
        assert_eq!(spec.forge, Forge::Gitea);
    }

    #[test]
    fn extracts_ref_from_github_tree_url() {
        let spec = RepoSpec::parse("https://github.com/owner/repo/tree/develop/dir", None).unwrap();
        assert_eq!(spec.explicit_ref.as_deref(), Some("develop"));
        assert_eq!(spec.namespace, "owner");
        assert_eq!(spec.repo_name, "repo");
    }

    #[test]
    fn extracts_ref_from_gitlab_tree_url() {
        let spec =
            RepoSpec::parse("https://gitlab.com/group/proj/-/tree/develop/sub/dir", None).unwrap();
        assert_eq!(spec.explicit_ref.as_deref(), Some("develop"));
        assert_eq!(spec.namespace, "group");
        assert_eq!(spec.repo_name, "proj");
    }

    #[test]
    fn extracts_ref_from_gitea_branch_url() {
        let spec =
            RepoSpec::parse("https://codeberg.org/owner/repo/src/branch/develop", None).unwrap();
        assert_eq!(spec.explicit_ref.as_deref(), Some("develop"));
    }

    #[test]
    fn query_ref_and_forge_overrides_apply() {
        let spec = RepoSpec::parse(
            "https://git.example.org/owner/repo?ref=v1&forge=gitlab",
            None,
        )
        .unwrap();
        assert_eq!(spec.explicit_ref.as_deref(), Some("v1"));
        assert_eq!(spec.forge, Forge::GitLab);
    }

    #[test]
    fn ref_override_wins_over_url_ref() {
        let spec =
            RepoSpec::parse("https://github.com/owner/repo/tree/develop", Some("v2")).unwrap();
        assert_eq!(spec.explicit_ref.as_deref(), Some("v2"));
    }

    #[test]
    fn rejects_unknown_forge_override() {
        let err = RepoSpec::parse("https://git.example.org/owner/repo?forge=svn", None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("unknown forge"), "{err}");
    }

    #[test]
    fn rejects_non_http_schemes() {
        let err = RepoSpec::parse("ssh://git@github.com/owner/repo", None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("unsupported scheme"), "{err}");
    }

    #[test]
    fn rejects_urls_without_owner_and_repo() {
        assert!(RepoSpec::parse("https://github.com/owner", None).is_err());
        assert!(RepoSpec::parse("https://github.com/", None).is_err());
    }

    #[test]
    fn preserves_port_in_host() {
        let spec = RepoSpec::parse("http://127.0.0.1:3000/owner/repo", None).unwrap();
        assert_eq!(spec.host, "127.0.0.1:3000");
        assert_eq!(spec.forge, Forge::Gitea);
        assert_eq!(spec.base_url(), "http://127.0.0.1:3000");
    }

    #[test]
    fn same_repo_ignores_case_and_refs() {
        let a = RepoSpec::parse("https://github.com/Owner/Repo", None).unwrap();
        let b = RepoSpec::parse("https://github.com/owner/repo/tree/dev", None).unwrap();
        assert!(a.is_same_repo(&b));

        let c = RepoSpec::parse("https://github.com/owner/other", None).unwrap();
        assert!(!a.is_same_repo(&c));
    }

    #[test]
    fn tarball_urls_match_forge_conventions() {
        let github = RepoSpec::parse("https://github.com/o/r", None).unwrap();
        assert_eq!(
            github.tarball_url("main"),
            "https://github.com/o/r/archive/main.tar.gz"
        );

        let gitlab = RepoSpec::parse("https://gitlab.example.org/group/sub/proj", None).unwrap();
        assert_eq!(
            gitlab.tarball_url("main"),
            "https://gitlab.example.org/group/sub/proj/-/archive/main/proj-main.tar.gz"
        );

        let gitea = RepoSpec::parse("https://codeberg.org/o/r", None).unwrap();
        assert_eq!(
            gitea.tarball_url("main"),
            "https://codeberg.org/o/r/archive/main.tar.gz"
        );
    }

    #[test]
    fn api_urls_match_forge_conventions() {
        let github = RepoSpec::parse("https://github.com/o/r", None).unwrap();
        assert_eq!(github.repo_api_url(), "https://api.github.com/repos/o/r");
        assert_eq!(
            github.commit_api_url("release/v2"),
            "https://api.github.com/repos/o/r/commits/release%2Fv2"
        );

        let ghe = RepoSpec::parse("https://ghe.example.org/o/r", None).unwrap();
        assert_eq!(
            ghe.repo_api_url(),
            "https://ghe.example.org/api/v3/repos/o/r"
        );

        let gitlab = RepoSpec::parse("https://gitlab.com/group/sub/proj", None).unwrap();
        assert_eq!(
            gitlab.repo_api_url(),
            "https://gitlab.com/api/v4/projects/group%2Fsub%2Fproj"
        );
        assert_eq!(
            gitlab.commit_api_url("main"),
            "https://gitlab.com/api/v4/projects/group%2Fsub%2Fproj/repository/commits/main"
        );

        let gitea = RepoSpec::parse("https://codeberg.org/o/r", None).unwrap();
        assert_eq!(
            gitea.repo_api_url(),
            "https://codeberg.org/api/v1/repos/o/r"
        );
        assert_eq!(
            gitea.commit_api_url("main"),
            "https://codeberg.org/api/v1/repos/o/r/git/commits/main"
        );
    }

    #[test]
    fn detects_commit_shas() {
        assert!(is_commit_sha(&"a".repeat(40)));
        assert!(is_commit_sha(&"F".repeat(64)));
        assert!(!is_commit_sha("main"));
        assert!(!is_commit_sha(&"a".repeat(39)));
        assert!(!is_commit_sha(&"g".repeat(40)));
    }

    #[test]
    fn extracts_archives_and_locates_root() {
        let bytes = targz(&[("repo-main/plugin.json", "{}")]);
        let dir = TempDir::new().unwrap();
        extract_archive(&bytes, dir.path()).unwrap();

        let root = locate_root(dir.path()).unwrap();
        assert_eq!(root.file_name().unwrap(), "repo-main");
        assert!(root.join("plugin.json").is_file());
    }

    #[test]
    fn locate_root_keeps_multi_entry_roots() {
        let bytes = targz(&[("a.txt", "a"), ("b.txt", "b")]);
        let dir = TempDir::new().unwrap();
        extract_archive(&bytes, dir.path()).unwrap();

        let root = locate_root(dir.path()).unwrap();
        assert_eq!(root, dir.path());
    }

    #[test]
    fn rejects_symlink_entries() {
        let mut entries = targz_entries();
        entries.symlink("repo/link", "/etc/passwd");
        let err = extract_archive(&entries.finish(), TempDir::new().unwrap().path()).unwrap_err();
        assert!(
            matches!(err, FetchError::UnsafeArchiveEntry { .. }),
            "{err}"
        );
    }

    #[test]
    fn rejects_path_traversal_entries() {
        let mut entries = targz_entries();
        entries.file_with_raw_path("../evil.txt", "x");
        let err = extract_archive(&entries.finish(), TempDir::new().unwrap().path()).unwrap_err();
        assert!(
            matches!(err, FetchError::UnsafeArchiveEntry { .. }),
            "{err}"
        );
    }

    #[tokio::test]
    async fn fetches_repo_pinned_by_api_resolved_commit() {
        let sha = "a".repeat(40);
        let server = TestServer::start(HashMap::from([
            (
                "/api/v1/repos/owner/repo".to_string(),
                CannedResponse::json(r#"{"default_branch":"develop"}"#),
            ),
            (
                "/api/v1/repos/owner/repo/git/commits/develop".to_string(),
                CannedResponse::json(&format!(r#"{{"sha":"{sha}"}}"#)),
            ),
            (
                format!("/owner/repo/archive/{sha}.tar.gz"),
                CannedResponse::targz(targz(&[("repo/plugin.json", "{}")])),
            ),
        ]))
        .await;

        let spec = RepoSpec::parse(&format!("{}/owner/repo", server.url()), None).unwrap();
        let fetched = fetch_repo(&client(), &spec).await.unwrap();

        assert_eq!(fetched.git_ref, "develop");
        assert_eq!(fetched.commit.as_deref(), Some(sha.as_str()));
        assert!(fetched.root().join("plugin.json").is_file());
    }

    #[tokio::test]
    async fn probes_fallback_refs_without_forge_api() {
        let server = TestServer::start(HashMap::from([(
            "/owner/repo/archive/master.tar.gz".to_string(),
            CannedResponse::targz(targz(&[("repo/plugin.json", "{}")])),
        )]))
        .await;

        let spec = RepoSpec::parse(&format!("{}/owner/repo", server.url()), None).unwrap();
        let fetched = fetch_repo(&client(), &spec).await.unwrap();

        assert_eq!(fetched.git_ref, "master");
        assert_eq!(fetched.commit, None);
    }

    #[tokio::test]
    async fn sha_refs_download_without_api_lookups() {
        let sha = "b".repeat(40);
        let server = TestServer::start(HashMap::from([(
            format!("/owner/repo/archive/{sha}.tar.gz"),
            CannedResponse::targz(targz(&[("repo/plugin.json", "{}")])),
        )]))
        .await;

        let url = format!("{}/owner/repo?ref={sha}", server.url());
        let spec = RepoSpec::parse(&url, None).unwrap();
        let fetched = fetch_repo(&client(), &spec).await.unwrap();

        assert_eq!(fetched.commit.as_deref(), Some(sha.as_str()));
    }

    #[tokio::test]
    async fn missing_explicit_ref_is_an_error() {
        let server = TestServer::start(HashMap::new()).await;

        let url = format!("{}/owner/repo?ref=v9", server.url());
        let spec = RepoSpec::parse(&url, None).unwrap();
        let err = fetch_repo(&client(), &spec).await.unwrap_err();

        assert!(
            matches!(
                &err,
                FetchError::Status { status, .. } if *status == StatusCode::NOT_FOUND
            ),
            "{err}"
        );
    }

    #[tokio::test]
    async fn missing_repo_reports_attempted_refs() {
        let server = TestServer::start(HashMap::new()).await;

        let spec = RepoSpec::parse(&format!("{}/owner/repo", server.url()), None).unwrap();
        let err = fetch_repo(&client(), &spec).await.unwrap_err();

        assert!(matches!(err, FetchError::RefNotFound { .. }), "{err}");
    }

    #[tokio::test]
    async fn rejects_archives_with_oversized_content_length() {
        let server = TestServer::start(HashMap::from([(
            "/owner/repo/archive/main.tar.gz".to_string(),
            CannedResponse::targz(targz(&[("repo/plugin.json", "{}")]))
                .with_content_length(MAX_ARCHIVE_BYTES + 1),
        )]))
        .await;

        let url = format!("{}/owner/repo?ref=main", server.url());
        let spec = RepoSpec::parse(&url, None).unwrap();
        let err = fetch_repo(&client(), &spec).await.unwrap_err();

        assert!(matches!(err, FetchError::ArchiveTooLarge { .. }), "{err}");
    }
}
