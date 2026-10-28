// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Where a repository lives and how its forge is reached: URL parsing, the
//! endpoints each forge serves, and the forge API calls built on them. API
//! answers are parsed here; byte downloads (archives, raw files) are URLs
//! that `fetch` downloads under its size limits.

use std::collections::{
    BTreeSet,
    HashMap,
};

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
use url::Url;

pub(crate) const USER_AGENT: &str = concat!("harmony-repository/", env!("CARGO_PKG_VERSION"));

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

    /// A tarball of only `dir` at `commit`. GitLab alone serves one; GitHub
    /// and Gitea answer `None` and callers download [`Self::archive_url`].
    pub fn directory_archive_url(&self, commit: &str, dir: &str) -> Option<String> {
        match self.forge {
            Forge::GitLab => Some(format!(
                "{}/repository/archive.tar.gz?sha={}&path={}",
                self.repo_api_url(),
                utf8_percent_encode(commit, SEGMENT),
                utf8_percent_encode(dir, SEGMENT)
            )),
            Forge::GitHub | Forge::Gitea => None,
        }
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

    /// Lists the entries directly under `dir` at `commit`; an empty `dir`
    /// lists the repository root.
    fn tree_api_url(&self, commit: &str, dir: &str) -> String {
        let repo = self.repo_api_url();
        let commit = utf8_percent_encode(commit, SEGMENT);
        match self.forge {
            Forge::GitHub | Forge::Gitea if dir.is_empty() => format!("{repo}/git/trees/{commit}"),
            Forge::GitHub | Forge::Gitea => {
                format!("{repo}/git/trees/{commit}:{}", encode_path(dir))
            }
            Forge::GitLab if dir.is_empty() => {
                format!("{repo}/repository/tree?ref={commit}&per_page=100")
            }
            Forge::GitLab => format!(
                "{repo}/repository/tree?path={}&ref={commit}&per_page=100",
                utf8_percent_encode(dir, SEGMENT)
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

fn detect_forge(hostname: &str) -> Forge {
    if hostname == "github.com" || hostname.starts_with("ghe.") || hostname.contains(".github.") {
        return Forge::GitHub;
    }
    if hostname.contains("gitlab") {
        return Forge::GitLab;
    }
    Forge::Gitea
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

/// Asks the forge whether `git_ref` names a branch. `None` when the API
/// gave no usable answer.
pub async fn branch_exists(client: &Client, spec: &RepoSpec, git_ref: &str) -> Option<bool> {
    let response = api_request(client, &spec.branch_api_url(git_ref)).await?;
    match response.status() {
        status if status.is_success() => Some(true),
        StatusCode::NOT_FOUND => Some(false),
        _ => None,
    }
}

/// The Git tree hash of the directory at `path` in `commit`, where `path` is
/// a normalized repository path as `repository.json` entries are and empty
/// names the root. GitLab's API does not expose the root tree, so it answers
/// `None` there and callers identify root-level plugins by the commit.
pub async fn tree_hash(
    client: &Client,
    spec: &RepoSpec,
    commit: &str,
    path: &str,
) -> Option<String> {
    if path.is_empty() {
        return match spec.forge {
            Forge::GitLab => None,
            Forge::GitHub | Forge::Gitea => {
                let value = api_get(client, &spec.tree_api_url(commit, "")).await?;
                value.get("sha")?.as_str().map(str::to_string)
            }
        };
    }
    let (parent, name) = path.rsplit_once('/').unwrap_or(("", path));
    child_tree_hashes(client, spec, commit, parent)
        .await?
        .remove(name)
}

/// The Git tree hash at `commit` of each directory in `dirs` and of every
/// sibling directory, keyed by path, asking once per parent directory. The
/// result covers whole parents, so it answers later lookups of other
/// directories under them. Directories the forge reports no hash for are
/// left out.
pub async fn tree_hashes(
    client: &Client,
    spec: &RepoSpec,
    commit: &str,
    dirs: &[&str],
) -> HashMap<String, String> {
    let mut hashes = HashMap::new();
    if dirs.contains(&"")
        && let Some(hash) = tree_hash(client, spec, commit, "").await
    {
        hashes.insert(String::new(), hash);
    }
    let parents: BTreeSet<&str> = dirs
        .iter()
        .filter(|dir| !dir.is_empty())
        .map(|dir| dir.rsplit_once('/').map_or("", |(parent, _)| parent))
        .collect();
    for parent in parents {
        let Some(children) = child_tree_hashes(client, spec, commit, parent).await else {
            continue;
        };
        hashes.extend(children.into_iter().map(|(name, hash)| {
            let dir = match parent {
                "" => name,
                parent => format!("{parent}/{name}"),
            };
            (dir, hash)
        }));
    }
    hashes
}

/// The Git tree hash of each directory directly under `dir` at `commit`,
/// keyed by directory name. One request covers every sibling, so callers
/// checking several plugin directories ask once per parent. `None` when the
/// API gave no usable answer, including a listing too long for one page.
pub async fn child_tree_hashes(
    client: &Client,
    spec: &RepoSpec,
    commit: &str,
    dir: &str,
) -> Option<HashMap<String, String>> {
    let url = spec.tree_api_url(commit, dir);
    let response = api_request(client, &url).await?;
    if !response.status().is_success() {
        return None;
    }
    let more_pages = response
        .headers()
        .get("x-next-page")
        .is_some_and(|page| !page.is_empty());
    let value = api_json(&url, response).await?;

    let (entries, name_key, hash_key) = match spec.forge {
        Forge::GitLab if more_pages => return None,
        Forge::GitLab => (value.as_array()?, "name", "id"),
        Forge::GitHub | Forge::Gitea => {
            if value.get("truncated").and_then(|t| t.as_bool()) == Some(true) {
                return None;
            }
            (value.get("tree")?.as_array()?, "path", "sha")
        }
    };

    entries
        .iter()
        .filter(|entry| entry.get("type").and_then(|t| t.as_str()) == Some("tree"))
        .map(|entry| {
            let name = entry.get(name_key)?.as_str()?;
            let hash = entry.get(hash_key)?.as_str()?;
            Some((name.to_string(), hash.to_string()))
        })
        .collect()
}

async fn api_request(client: &Client, url: &str) -> Option<reqwest::Response> {
    let response = client
        .get(url)
        .header(header::USER_AGENT, USER_AGENT)
        .header(header::ACCEPT, "application/json")
        .send()
        .await;

    match response {
        Ok(response) => {
            if !response.status().is_success() {
                tracing::debug!(
                    "forge api request {url} returned HTTP {}",
                    response.status()
                );
            }
            Some(response)
        }
        Err(err) => {
            tracing::debug!("forge api request {url} failed: {err}");
            None
        }
    }
}

async fn api_get(client: &Client, url: &str) -> Option<serde_json::Value> {
    let response = api_request(client, url).await?;
    if !response.status().is_success() {
        return None;
    }
    api_json(url, response).await
}

async fn api_json(url: &str, response: reqwest::Response) -> Option<serde_json::Value> {
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
    fn same_repo_ignores_case_and_refs() {
        let a = RepoSpec::parse("https://github.com/Owner/Repo", None).unwrap();
        let b = RepoSpec::parse("https://github.com/owner/repo/tree/dev", None).unwrap();
        assert!(a.is_same_repo(&b));

        let c = RepoSpec::parse("https://github.com/owner/other", None).unwrap();
        assert!(!a.is_same_repo(&c));
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
    fn tree_api_urls_match_forge_conventions() {
        let github = RepoSpec::parse("https://github.com/o/r", None).unwrap();
        assert_eq!(
            github.tree_api_url("abc", "plugins"),
            "https://api.github.com/repos/o/r/git/trees/abc:plugins"
        );
        assert_eq!(
            github.tree_api_url("abc", ""),
            "https://api.github.com/repos/o/r/git/trees/abc"
        );

        let ghe = RepoSpec::parse("https://ghe.example.org/o/r", None).unwrap();
        assert_eq!(
            ghe.tree_api_url("abc", "a/b"),
            "https://ghe.example.org/api/v3/repos/o/r/git/trees/abc:a/b"
        );

        let gitlab = RepoSpec::parse("https://gitlab.com/group/proj", None).unwrap();
        assert_eq!(
            gitlab.tree_api_url("abc", "a/b"),
            "https://gitlab.com/api/v4/projects/group%2Fproj/repository/tree?path=a%2Fb&ref=abc&per_page=100"
        );
        assert_eq!(
            gitlab.tree_api_url("abc", ""),
            "https://gitlab.com/api/v4/projects/group%2Fproj/repository/tree?ref=abc&per_page=100"
        );

        let gitea = RepoSpec::parse("https://codeberg.org/o/r", None).unwrap();
        assert_eq!(
            gitea.tree_api_url("abc", "plugins"),
            "https://codeberg.org/api/v1/repos/o/r/git/trees/abc:plugins"
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
    async fn child_tree_hashes_list_directories_from_trees_api() {
        let server = TestServer::start(HashMap::from([
            (
                "/api/v1/repos/owner/repo/git/trees/abc:plugins".to_string(),
                CannedResponse::json(
                    r#"{"sha":"p","truncated":false,"tree":[
                        {"path":"audiomuse","type":"tree","sha":"t1"},
                        {"path":"README.md","type":"blob","sha":"b1"},
                        {"path":"lrclib","type":"tree","sha":"t2"}
                    ]}"#,
                ),
            ),
            (
                "/api/v1/repos/owner/repo/git/trees/abc:big".to_string(),
                CannedResponse::json(r#"{"sha":"p","truncated":true,"tree":[]}"#),
            ),
        ]))
        .await;

        let spec = RepoSpec::parse(&format!("{}/owner/repo", server.url()), None).unwrap();
        let hashes = child_tree_hashes(&client(), &spec, "abc", "plugins")
            .await
            .unwrap();
        assert_eq!(
            hashes,
            HashMap::from([
                ("audiomuse".to_string(), "t1".to_string()),
                ("lrclib".to_string(), "t2".to_string()),
            ])
        );

        assert_eq!(
            child_tree_hashes(&client(), &spec, "abc", "big").await,
            None
        );
        assert_eq!(
            child_tree_hashes(&client(), &spec, "abc", "gone").await,
            None
        );
    }

    #[tokio::test]
    async fn child_tree_hashes_read_gitlab_tree_ids() {
        let server = TestServer::start(HashMap::from([
            (
                "/api/v4/projects/owner%2Frepo/repository/tree?path=plugins&ref=abc&per_page=100"
                    .to_string(),
                CannedResponse::json(
                    r#"[
                        {"id":"t1","name":"audiomuse","type":"tree","path":"plugins/audiomuse"},
                        {"id":"b1","name":"README.md","type":"blob","path":"plugins/README.md"}
                    ]"#,
                ),
            ),
            (
                "/api/v4/projects/owner%2Frepo/repository/tree?path=big&ref=abc&per_page=100"
                    .to_string(),
                CannedResponse::json("[]").with_header("x-next-page", "2"),
            ),
        ]))
        .await;

        let url = format!("{}/owner/repo?forge=gitlab", server.url());
        let spec = RepoSpec::parse(&url, None).unwrap();
        let hashes = child_tree_hashes(&client(), &spec, "abc", "plugins")
            .await
            .unwrap();
        assert_eq!(
            hashes,
            HashMap::from([("audiomuse".to_string(), "t1".to_string())])
        );

        assert_eq!(
            child_tree_hashes(&client(), &spec, "abc", "big").await,
            None
        );
    }

    #[tokio::test]
    async fn tree_hash_looks_paths_up_in_their_parent_listing() {
        let server = TestServer::start(HashMap::from([
            (
                "/api/v1/repos/owner/repo/git/trees/abc".to_string(),
                CannedResponse::json(
                    r#"{"sha":"root","truncated":false,"tree":[
                        {"path":"plugin.json","type":"blob","sha":"b1"}
                    ]}"#,
                ),
            ),
            (
                "/api/v1/repos/owner/repo/git/trees/abc:a/b".to_string(),
                CannedResponse::json(
                    r#"{"sha":"ab","truncated":false,"tree":[
                        {"path":"c","type":"tree","sha":"abc-hash"},
                        {"path":"d","type":"blob","sha":"d-hash"}
                    ]}"#,
                ),
            ),
        ]))
        .await;

        let spec = RepoSpec::parse(&format!("{}/owner/repo", server.url()), None).unwrap();
        let client = client();
        assert_eq!(
            tree_hash(&client, &spec, "abc", "").await.as_deref(),
            Some("root")
        );
        assert_eq!(
            tree_hash(&client, &spec, "abc", "a/b/c").await.as_deref(),
            Some("abc-hash")
        );
        assert_eq!(tree_hash(&client, &spec, "abc", "a/b/d").await, None);
        assert_eq!(tree_hash(&client, &spec, "abc", "a/b/e").await, None);
    }

    #[tokio::test]
    async fn tree_hash_reads_gitlab_listings_but_not_the_root() {
        let server = TestServer::start(HashMap::from([
            (
                "/api/v4/projects/owner%2Frepo/repository/tree?ref=abc&per_page=100".to_string(),
                CannedResponse::json(r#"[{"id":"t1","name":"a","type":"tree","path":"a"}]"#),
            ),
            (
                "/api/v4/projects/owner%2Frepo/repository/tree?path=a%2Fb&ref=abc&per_page=100"
                    .to_string(),
                CannedResponse::json(
                    r#"[{"id":"abc-hash","name":"c","type":"tree","path":"a/b/c"}]"#,
                ),
            ),
        ]))
        .await;

        let url = format!("{}/owner/repo?forge=gitlab", server.url());
        let spec = RepoSpec::parse(&url, None).unwrap();
        assert_eq!(tree_hash(&client(), &spec, "abc", "").await, None);
        assert_eq!(
            tree_hash(&client(), &spec, "abc", "a").await.as_deref(),
            Some("t1")
        );
        assert_eq!(
            tree_hash(&client(), &spec, "abc", "a/b/c").await.as_deref(),
            Some("abc-hash")
        );
    }

    #[tokio::test]
    async fn tree_hashes_ask_once_per_parent_and_cover_its_siblings() {
        let server = TestServer::start(HashMap::from([
            (
                "/api/v1/repos/owner/repo/git/trees/abc".to_string(),
                CannedResponse::json(r#"{"sha":"root","truncated":false,"tree":[]}"#),
            ),
            (
                "/api/v1/repos/owner/repo/git/trees/abc:plugins".to_string(),
                CannedResponse::json(
                    r#"{"sha":"p","truncated":false,"tree":[
                        {"path":"a","type":"tree","sha":"a-hash"},
                        {"path":"b","type":"tree","sha":"b-hash"}
                    ]}"#,
                ),
            ),
        ]))
        .await;

        let spec = RepoSpec::parse(&format!("{}/owner/repo", server.url()), None).unwrap();
        let hashes = tree_hashes(
            &client(),
            &spec,
            "abc",
            &["plugins/a", "plugins/gone", "", ""],
        )
        .await;

        assert_eq!(
            hashes,
            HashMap::from([
                ("plugins/a".to_string(), "a-hash".to_string()),
                ("plugins/b".to_string(), "b-hash".to_string()),
                (String::new(), "root".to_string()),
            ])
        );
        let mut requests = server.requests();
        requests.sort();
        assert_eq!(
            requests,
            [
                "/api/v1/repos/owner/repo/git/trees/abc",
                "/api/v1/repos/owner/repo/git/trees/abc:plugins",
            ]
        );
    }
}
