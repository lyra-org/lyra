// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Where a repository lives and how its forge is reached: URL parsing, the
//! endpoints each forge serves, and the forge API calls built on them. API
//! answers are parsed here; byte downloads (archives, raw files) are URLs
//! that `fetch` downloads under its size limits.

use std::time::{
    Duration,
    SystemTime,
    UNIX_EPOCH,
};

use percent_encoding::{
    AsciiSet,
    NON_ALPHANUMERIC,
    utf8_percent_encode,
};
use reqwest::header::HeaderMap;
use reqwest::{
    Client,
    Response,
    StatusCode,
    header,
};
use serde::{
    Deserialize,
    Serialize,
};
use url::Url;

mod ls_refs;

pub(crate) use ls_refs::{
    AdvertisedRef,
    ls_refs,
};

/// Percent-encode set for values embedded in a single URL path segment or
/// query value. Keeps the unreserved characters and encodes everything else,
/// including `/`, so refs like `release/v2` stay a single segment.
const SEGMENT: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~');

#[derive(Debug, thiserror::Error)]
#[error("invalid repository URL '{url}': {reason}")]
pub struct RepoUrlError {
    pub url: String,
    pub reason: String,
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
    pub fn parse(input: &str, ref_override: Option<&str>) -> Result<Self, RepoUrlError> {
        let invalid = |reason: String| RepoUrlError {
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

    /// Whether the network between here and the repository cannot alter what
    /// is fetched from it: it uses TLS, or never leaves this machine.
    pub fn is_protected_in_transit(&self) -> bool {
        self.scheme == "https"
            || Url::parse(&self.base_url()).is_ok_and(|url| match url.host() {
                Some(url::Host::Domain(domain)) => domain == "localhost",
                Some(url::Host::Ipv4(address)) => address.is_loopback(),
                Some(url::Host::Ipv6(address)) => address.is_loopback(),
                None => false,
            })
    }

    /// Canonical form used for persistence and self-reference comparison.
    pub fn canonical_url(&self) -> String {
        let origin = format!("{}/{}/{}", self.base_url(), self.namespace, self.repo_name);
        let hostname = self.host.split(':').next().unwrap_or(&self.host);
        if self.forge != detect_forge(hostname) {
            format!("{origin}?forge={}", self.forge.as_str())
        } else {
            origin
        }
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

    /// Whether both specs fetch the same repository the same way, ignoring
    /// refs: a listing from one can stand for the other.
    pub fn is_same_source(&self, other: &RepoSpec) -> bool {
        self.is_same_repo(other) && self.scheme == other.scheme && self.forge == other.forge
    }

    /// The whole repository as a tarball at `git_ref`, which may be a branch,
    /// tag or commit.
    pub fn archive_url(&self, git_ref: &str) -> String {
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
                    utf8_percent_encode(&project, SEGMENT)
                )
            }
            Forge::Gitea => format!("{base}/api/v1/repos/{}/{}", self.namespace, self.repo_name),
        }
    }

    fn commit_api_url(&self, git_ref: &str) -> String {
        let repo = self.repo_api_url();
        let git_ref = utf8_percent_encode(git_ref, SEGMENT);
        match self.forge {
            Forge::GitHub => format!("{repo}/commits/{git_ref}"),
            Forge::GitLab => format!("{repo}/repository/commits/{git_ref}"),
            Forge::Gitea => format!("{repo}/git/commits/{git_ref}"),
        }
    }

    fn branch_api_url(&self, git_ref: &str) -> String {
        let repo = self.repo_api_url();
        let git_ref = utf8_percent_encode(git_ref, SEGMENT);
        match self.forge {
            Forge::GitHub | Forge::Gitea => format!("{repo}/branches/{git_ref}"),
            Forge::GitLab => format!("{repo}/repository/branches/{git_ref}"),
        }
    }

    /// The Git smart HTTP endpoint that answers protocol v2 commands.
    pub fn upload_pack_url(&self) -> String {
        format!(
            "{}/{}/{}.git/git-upload-pack",
            self.base_url(),
            self.namespace,
            self.repo_name
        )
    }

    /// Whether the forge serves tarballs of a single directory. GitLab alone
    /// does.
    pub fn has_directory_archives(&self) -> bool {
        self.forge == Forge::GitLab
    }

    /// A tarball of only `dir` at `commit`, with the directory's full path
    /// kept under the archive's top-level directory. `None` unless
    /// [`Self::has_directory_archives`]; callers then download
    /// [`Self::archive_url`].
    pub fn directory_archive_url(&self, commit: &str, dir: &str) -> Option<String> {
        self.has_directory_archives().then(|| {
            format!(
                "{}/repository/archive.tar.gz?sha={}&path={}",
                self.repo_api_url(),
                utf8_percent_encode(commit, SEGMENT),
                utf8_percent_encode(dir, SEGMENT)
            )
        })
    }

    /// The raw contents of the file at `path` in `commit`. GitHub Enterprise
    /// goes through the web `/raw/` route, which redirects to wherever the
    /// instance serves raw files (`raw.<host>` under subdomain isolation).
    pub fn raw_file_url(&self, commit: &str, path: &str) -> String {
        let base = self.base_url();
        let (namespace, repo) = (&self.namespace, &self.repo_name);
        let commit = utf8_percent_encode(commit, SEGMENT);
        match self.forge {
            Forge::GitHub if self.host == "github.com" => format!(
                "https://raw.githubusercontent.com/{namespace}/{repo}/{commit}/{}",
                encode_path(path)
            ),
            Forge::GitHub => format!(
                "{base}/{namespace}/{repo}/raw/{commit}/{}",
                encode_path(path)
            ),
            Forge::GitLab => format!(
                "{}/repository/files/{}/raw?ref={commit}",
                self.repo_api_url(),
                utf8_percent_encode(path, SEGMENT)
            ),
            Forge::Gitea => format!(
                "{base}/{namespace}/{repo}/raw/commit/{commit}/{}",
                encode_path(path)
            ),
        }
    }
}

/// Encodes each segment of a repository path, keeping its `/` separators.
fn encode_path(path: &str) -> String {
    path.split('/')
        .map(|segment| utf8_percent_encode(segment, SEGMENT).to_string())
        .collect::<Vec<_>>()
        .join("/")
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

/// Whether `value` is a full SHA-1 or SHA-256 object id.
pub(crate) fn is_object_id(value: &str) -> bool {
    matches!(value.len(), 40 | 64) && value.chars().all(|c| c.is_ascii_hexdigit())
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

/// Longest wait a forge's rate limit is taken to ask for.
const MAX_RATE_LIMIT_WAIT: Duration = Duration::from_secs(3600);

/// Most bytes of a GitHub 403 read to tell a secondary rate limit apart.
const MAX_FORBIDDEN_BODY_BYTES: usize = 4096;

/// A forge refused a request for its rate limit; `retry_after` is the wait
/// it asked for, when it said. A rate limit stops the operation instead of
/// degrading into fallback requests.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "failed to fetch {url}: rate limited by the forge{}",
    retry_after.map(|wait| format!(", retry after {}s", wait.as_secs())).unwrap_or_default()
)]
pub struct RateLimited {
    /// The host of the repository the request was for, as [`RepoSpec::host`]
    /// names it, which `url` may not share.
    pub host: String,
    pub url: String,
    pub retry_after: Option<Duration>,
}

impl RateLimited {
    /// The rate limit `response` reports: HTTP 429, GitHub's 403 with no
    /// requests remaining, or a 403 carrying `Retry-After`, as GitHub's
    /// secondary limits send. The wait is `Retry-After` in delta-seconds,
    /// else the time until GitHub's `x-ratelimit-reset`; either is at most an
    /// hour.
    pub(crate) fn from_response(spec: &RepoSpec, url: &str, response: &Response) -> Option<Self> {
        let headers = response.headers();
        let exhausted = headers
            .get("x-ratelimit-remaining")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.trim() == "0");
        let limited = match response.status() {
            StatusCode::TOO_MANY_REQUESTS => true,
            StatusCode::FORBIDDEN => exhausted || headers.contains_key(header::RETRY_AFTER),
            _ => false,
        };
        limited.then(|| Self::new(spec, url, retry_after(headers)))
    }

    /// A rate limit on `url`, a request for `spec`. Waits are at least a
    /// second: a zero wait still means the request was refused.
    fn new(spec: &RepoSpec, url: &str, retry_after: Option<Duration>) -> Self {
        Self {
            host: spec.host.clone(),
            url: url.to_string(),
            retry_after: retry_after.map(|wait| wait.max(Duration::from_secs(1))),
        }
    }
}

fn retry_after(headers: &HeaderMap) -> Option<Duration> {
    let seconds = |name| headers.get(name)?.to_str().ok()?.trim().parse::<u64>().ok();
    let wait = match seconds(header::RETRY_AFTER.as_str()) {
        Some(delay) => delay,
        None => {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or(Duration::ZERO);
            seconds("x-ratelimit-reset")?.saturating_sub(now.as_secs())
        }
    };
    Some(Duration::from_secs(wait).min(MAX_RATE_LIMIT_WAIT))
}

/// Whether a GitHub 403 is a secondary rate limit that only its message
/// names, carrying neither `Retry-After` nor an exhausted quota.
async fn mentions_rate_limit(mut response: Response) -> bool {
    let mut body = Vec::new();
    while body.len() < MAX_FORBIDDEN_BODY_BYTES {
        match response.chunk().await {
            Ok(Some(chunk)) => body.extend_from_slice(&chunk),
            Ok(None) | Err(_) => break,
        }
    }
    body.truncate(MAX_FORBIDDEN_BODY_BYTES);
    String::from_utf8_lossy(&body)
        .to_ascii_lowercase()
        .contains("rate limit")
}

/// What the forge API answered: `Ok(None)` when it gave no usable answer
/// (unsupported, missing, malformed, unreachable), so callers degrade.
type ApiResult<T> = Result<Option<T>, RateLimited>;

/// Resolves the repository's default branch through the forge API. All three
/// supported forges expose it as `default_branch`.
pub async fn default_branch(client: &Client, spec: &RepoSpec) -> ApiResult<String> {
    let Some(value) = api_get(client, spec, &spec.repo_api_url()).await? else {
        return Ok(None);
    };
    Ok(value
        .get("default_branch")
        .and_then(|branch| branch.as_str())
        .filter(|branch| !branch.is_empty())
        .map(str::to_string))
}

/// Resolves a ref to a commit SHA through the forge API.
pub async fn resolve_commit(client: &Client, spec: &RepoSpec, git_ref: &str) -> ApiResult<String> {
    let Some(value) = api_get(client, spec, &spec.commit_api_url(git_ref)).await? else {
        return Ok(None);
    };
    let key = match spec.forge {
        Forge::GitLab => "id",
        Forge::GitHub | Forge::Gitea => "sha",
    };
    Ok(value
        .get(key)
        .and_then(|sha| sha.as_str())
        .map(str::to_string))
}

/// Asks the forge whether `git_ref` names a branch.
pub async fn branch_exists(client: &Client, spec: &RepoSpec, git_ref: &str) -> ApiResult<bool> {
    let Some(response) = api_request(client, spec, &spec.branch_api_url(git_ref)).await? else {
        return Ok(None);
    };
    Ok(match response.status() {
        status if status.is_success() => Some(true),
        StatusCode::NOT_FOUND => Some(false),
        _ => None,
    })
}

/// A forge API request. GitHub's secondary rate limits may answer 403 with
/// only a message to say so, so its body is read; any other GitHub 403 is no
/// usable answer either way.
async fn api_request(client: &Client, spec: &RepoSpec, url: &str) -> ApiResult<Response> {
    let response = client
        .get(url)
        .header(header::ACCEPT, "application/json")
        .send()
        .await;

    match response {
        Ok(response) => {
            if let Some(limited) = RateLimited::from_response(spec, url, &response) {
                return Err(limited);
            }
            if spec.forge == Forge::GitHub && response.status() == StatusCode::FORBIDDEN {
                if mentions_rate_limit(response).await {
                    return Err(RateLimited::new(spec, url, None));
                }
                tracing::debug!("forge api request {url} returned HTTP 403");
                return Ok(None);
            }
            if !response.status().is_success() {
                tracing::debug!(
                    "forge api request {url} returned HTTP {}",
                    response.status()
                );
            }
            Ok(Some(response))
        }
        Err(err) => {
            tracing::debug!("forge api request {url} failed: {err}");
            Ok(None)
        }
    }
}

async fn api_get(client: &Client, spec: &RepoSpec, url: &str) -> ApiResult<serde_json::Value> {
    let Some(response) = api_request(client, spec, url).await? else {
        return Ok(None);
    };
    if !response.status().is_success() {
        return Ok(None);
    }
    Ok(api_json(url, response).await)
}

async fn api_json(url: &str, response: Response) -> Option<serde_json::Value> {
    match response.json().await {
        Ok(value) => Some(value),
        Err(err) => {
            tracing::debug!("forge api response from {url} is not JSON: {err}");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::testutil::{
        CannedResponse,
        TestServer,
        client,
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
    fn canonical_url_retains_non_default_forge_for_persisted_repositories() {
        for url in [
            "https://git.lyra.pub/lyra/lyra?forge=gitlab",
            "http://127.0.0.1:3000/group/repo?forge=gitlab",
            "https://gitlab.example.org/owner/repo?forge=gitea",
        ] {
            let original = RepoSpec::parse(url, Some("v1")).unwrap();
            let restored =
                RepoSpec::parse(&original.canonical_url(), original.explicit_ref.as_deref())
                    .unwrap();
            assert_eq!(restored.forge, original.forge);
            assert_eq!(restored.canonical_url(), original.canonical_url());
            assert_eq!(restored.explicit_ref, original.explicit_ref);
        }
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
    fn only_tls_and_loopback_are_protected_in_transit() {
        for url in [
            "https://github.com/owner/repo",
            "https://192.168.1.5/owner/repo",
            "http://localhost:3000/owner/repo",
            "http://LOCALHOST/owner/repo",
            "http://127.0.0.1:3000/owner/repo",
            "http://127.8.9.10/owner/repo",
            "http://[::1]:3000/owner/repo",
        ] {
            let spec = RepoSpec::parse(url, None).unwrap();
            assert!(spec.is_protected_in_transit(), "{url}");
        }
        for url in [
            "http://github.com/owner/repo",
            "http://192.168.1.5:3000/owner/repo",
            "http://[fd00::1]:3000/owner/repo",
            "http://nas.local/owner/repo",
            "http://localhost.example.org/owner/repo",
            "http://127.0.0.1.example.org/owner/repo",
        ] {
            let spec = RepoSpec::parse(url, None).unwrap();
            assert!(!spec.is_protected_in_transit(), "{url}");
        }
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
    fn same_source_also_needs_the_scheme_and_forge() {
        let a = RepoSpec::parse("https://github.com/Owner/Repo", None).unwrap();
        let b = RepoSpec::parse("https://github.com/owner/repo/tree/dev", None).unwrap();
        assert!(a.is_same_source(&b));

        for other in [
            "http://github.com/owner/repo",
            "https://github.com/owner/repo?forge=gitea",
        ] {
            let other = RepoSpec::parse(other, None).unwrap();
            assert!(a.is_same_repo(&other));
            assert!(!a.is_same_source(&other));
        }
    }

    #[test]
    fn archive_urls_match_forge_conventions() {
        let github = RepoSpec::parse("https://github.com/o/r", None).unwrap();
        assert_eq!(
            github.archive_url("main"),
            "https://github.com/o/r/archive/main.tar.gz"
        );

        let gitlab = RepoSpec::parse("https://gitlab.example.org/group/sub/proj", None).unwrap();
        assert_eq!(
            gitlab.archive_url("main"),
            "https://gitlab.example.org/group/sub/proj/-/archive/main/proj-main.tar.gz"
        );

        let gitea = RepoSpec::parse("https://codeberg.org/o/r", None).unwrap();
        assert_eq!(
            gitea.archive_url("main"),
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
    fn branch_api_urls_match_forge_conventions() {
        let github = RepoSpec::parse("https://github.com/o/r", None).unwrap();
        assert_eq!(
            github.branch_api_url("release/v2"),
            "https://api.github.com/repos/o/r/branches/release%2Fv2"
        );

        let gitlab = RepoSpec::parse("https://gitlab.com/group/proj", None).unwrap();
        assert_eq!(
            gitlab.branch_api_url("main"),
            "https://gitlab.com/api/v4/projects/group%2Fproj/repository/branches/main"
        );

        let gitea = RepoSpec::parse("https://codeberg.org/o/r", None).unwrap();
        assert_eq!(
            gitea.branch_api_url("main"),
            "https://codeberg.org/api/v1/repos/o/r/branches/main"
        );
    }

    #[test]
    fn raw_file_urls_match_forge_conventions() {
        let path = "plugins/audio muse/plugin.json";

        let github = RepoSpec::parse("https://github.com/o/r", None).unwrap();
        assert_eq!(
            github.raw_file_url("abc", path),
            "https://raw.githubusercontent.com/o/r/abc/plugins/audio%20muse/plugin.json"
        );

        let ghe = RepoSpec::parse("https://ghe.example.org/o/r", None).unwrap();
        assert_eq!(
            ghe.raw_file_url("abc", path),
            "https://ghe.example.org/o/r/raw/abc/plugins/audio%20muse/plugin.json"
        );

        let gitlab = RepoSpec::parse("https://gitlab.com/group/sub/proj", None).unwrap();
        assert_eq!(
            gitlab.raw_file_url("abc", path),
            "https://gitlab.com/api/v4/projects/group%2Fsub%2Fproj/repository/files/plugins%2Faudio%20muse%2Fplugin.json/raw?ref=abc"
        );

        let gitea = RepoSpec::parse("https://codeberg.org/o/r", None).unwrap();
        assert_eq!(
            gitea.raw_file_url("abc", path),
            "https://codeberg.org/o/r/raw/commit/abc/plugins/audio%20muse/plugin.json"
        );
    }

    #[test]
    fn directory_archives_are_gitlab_only() {
        let gitlab = RepoSpec::parse("https://gitlab.com/group/proj", None).unwrap();
        assert_eq!(
            gitlab
                .directory_archive_url("abc", "plugins/audiomuse")
                .as_deref(),
            Some(
                "https://gitlab.com/api/v4/projects/group%2Fproj/repository/archive.tar.gz?sha=abc&path=plugins%2Faudiomuse"
            )
        );

        for url in ["https://github.com/o/r", "https://codeberg.org/o/r"] {
            let spec = RepoSpec::parse(url, None).unwrap();
            assert_eq!(spec.directory_archive_url("abc", "plugins/audiomuse"), None);
        }
    }

    #[tokio::test]
    async fn forge_responses_are_classified_as_rate_limited() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let commits = "/api/v1/repos/owner/repo/git/commits";
        let server = TestServer::start(HashMap::from([
            (
                format!("{commits}/exhausted"),
                CannedResponse::status(403)
                    .with_header("x-ratelimit-remaining", "0")
                    .with_header("x-ratelimit-reset", &(now + 120).to_string()),
            ),
            (
                format!("{commits}/reset-passed"),
                CannedResponse::status(403)
                    .with_header("x-ratelimit-remaining", "0")
                    .with_header("x-ratelimit-reset", &(now - 5).to_string()),
            ),
            (
                format!("{commits}/secondary"),
                CannedResponse::status(403)
                    .with_header("retry-after", "60")
                    .with_header("x-ratelimit-reset", &(now + 999).to_string()),
            ),
            (format!("{commits}/too-many"), CannedResponse::status(429)),
            (
                format!("{commits}/forbidden"),
                CannedResponse::status(403).with_header("x-ratelimit-remaining", "12"),
            ),
            (format!("{commits}/broken"), CannedResponse::status(500)),
            (
                format!("{commits}/far-reset"),
                CannedResponse::status(403)
                    .with_header("x-ratelimit-remaining", "0")
                    .with_header("x-ratelimit-reset", &(now + 86_400).to_string()),
            ),
            (
                format!("{commits}/max-reset"),
                CannedResponse::status(429).with_header("x-ratelimit-reset", &u64::MAX.to_string()),
            ),
            (
                format!("{commits}/far-retry"),
                CannedResponse::status(429).with_header("retry-after", &u64::MAX.to_string()),
            ),
            (
                format!("{commits}/zero"),
                CannedResponse::status(429).with_header("retry-after", "0"),
            ),
        ]))
        .await;
        let spec = RepoSpec::parse(&format!("{}/owner/repo", server.url()), None).unwrap();
        let wait = async |git_ref| {
            resolve_commit(&client(), &spec, git_ref)
                .await
                .map(|_| ())
                .map_err(|limited| limited.retry_after)
        };

        let Err(Some(exhausted)) = wait("exhausted").await else {
            panic!("an exhausted quota is a rate limit");
        };
        assert!((115..=120).contains(&exhausted.as_secs()), "{exhausted:?}");
        assert_eq!(
            wait("reset-passed").await,
            Err(Some(Duration::from_secs(1)))
        );
        assert_eq!(wait("secondary").await, Err(Some(Duration::from_secs(60))));
        assert_eq!(wait("too-many").await, Err(None));
        assert_eq!(wait("forbidden").await, Ok(()));
        assert_eq!(wait("broken").await, Ok(()));
        assert_eq!(wait("far-reset").await, Err(Some(MAX_RATE_LIMIT_WAIT)));
        assert_eq!(wait("max-reset").await, Err(Some(MAX_RATE_LIMIT_WAIT)));
        assert_eq!(wait("far-retry").await, Err(Some(MAX_RATE_LIMIT_WAIT)));
        assert_eq!(wait("zero").await, Err(Some(Duration::from_secs(1))));
    }

    #[tokio::test]
    async fn github_secondary_limits_named_only_in_the_body_are_rate_limits() {
        let commits = "/api/v3/repos/owner/repo/commits";
        let message = |text: &str| {
            CannedResponse::json(&format!(r#"{{"message":"{text}"}}"#)).with_status(403)
        };
        let server = TestServer::start(HashMap::from([
            (
                format!("{commits}/secondary"),
                message("You have exceeded a secondary Rate Limit. Please wait."),
            ),
            (
                format!("{commits}/forbidden"),
                message("Resource not accessible by integration"),
            ),
            (
                format!("{commits}/long"),
                message(&format!(
                    "{} rate limit",
                    "x".repeat(MAX_FORBIDDEN_BODY_BYTES)
                )),
            ),
            (
                "/api/v1/repos/owner/repo/git/commits/secondary".to_string(),
                message("rate limit"),
            ),
        ]))
        .await;
        let github = format!("{}/owner/repo?forge=github", server.url());
        let github = RepoSpec::parse(&github, None).unwrap();
        let gitea = RepoSpec::parse(&format!("{}/owner/repo", server.url()), None).unwrap();

        let limited = resolve_commit(&client(), &github, "secondary")
            .await
            .unwrap_err();
        assert_eq!(limited.retry_after, None);
        assert_eq!(limited.host, github.host);
        assert_eq!(
            resolve_commit(&client(), &github, "forbidden").await,
            Ok(None)
        );
        assert_eq!(
            resolve_commit(&client(), &github, "long").await,
            Ok(None),
            "text past the body cap is not read"
        );
        assert_eq!(
            resolve_commit(&client(), &gitea, "secondary").await,
            Ok(None),
            "only GitHub's 403 bodies are read"
        );
    }
}
