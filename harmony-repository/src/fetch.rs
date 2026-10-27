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
use reqwest::{
    Client,
    StatusCode,
    header,
};
use tar::{
    Archive,
    EntryType,
};
use tempfile::TempDir;

use crate::forge::{
    RepoSpec,
    USER_AGENT,
    branch_exists,
    default_branch,
    resolve_commit,
};

const FALLBACK_REFS: &[&str] = &["main", "master", "trunk"];

pub const MAX_ARCHIVE_BYTES: u64 = 50 * 1024 * 1024;
pub const MAX_UNPACKED_BYTES: u64 = 256 * 1024 * 1024;
pub const MAX_ARCHIVE_ENTRIES: usize = 16 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum FetchError {
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

fn is_commit_sha(value: &str) -> bool {
    matches!(value.len(), 40 | 64) && value.chars().all(|c| c.is_ascii_hexdigit())
}

/// Whether `git_ref` is an abbreviated SHA that the forge expanded to
/// `commit`. Branches and tags can be named like hex strings, so the
/// resolved commit has to confirm the interpretation.
fn is_abbreviated_sha_of(git_ref: &str, commit: Option<&str>) -> bool {
    (7..40).contains(&git_ref.len())
        && git_ref.chars().all(|c| c.is_ascii_hexdigit())
        && commit.is_some_and(|commit| {
            commit
                .to_ascii_lowercase()
                .starts_with(&git_ref.to_ascii_lowercase())
        })
}

/// Whether an explicit ref pins the install. Commit SHAs and tags are
/// pinned; branches and `HEAD` track new commits. `commit` is what the
/// forge resolved the ref to and `branch` its answer on whether the ref
/// names a branch. Without a resolved commit the API is unreachable, so
/// the branch answer is not trusted and the ref is assumed to track.
fn ref_is_pinned(git_ref: &str, commit: Option<&str>, branch: Option<bool>) -> bool {
    if git_ref == "HEAD" {
        return false;
    }
    if is_commit_sha(git_ref) || is_abbreviated_sha_of(git_ref, commit) {
        return true;
    }
    commit.is_some() && branch.is_some_and(|branch| !branch)
}

/// A repository tree downloaded into a temporary directory. The directory is
/// removed when this value is dropped; callers copy what they keep.
#[derive(Debug)]
pub struct FetchedRepo {
    _tempdir: TempDir,
    root: PathBuf,
    pub git_ref: String,
    pub commit: Option<String>,
    /// Whether the ref is a tag or commit and so never moves. Branches and
    /// refs the forge API could not classify track new commits.
    pub pinned: bool,
}

impl FetchedRepo {
    pub fn root(&self) -> &Path {
        &self.root
    }
}

/// What a repository's ref resolves to, worked out from the forge API
/// alone. [`fetch_planned`] downloads it.
#[derive(Debug)]
pub struct RefPlan {
    candidates: Vec<String>,
    commit: Option<String>,
    pinned: bool,
    /// True when the candidates came from an explicit ref or the forge API.
    /// Guessed candidates may 404 without it being an error.
    authoritative: bool,
}

impl RefPlan {
    /// The commit the ref resolves to; `None` when the forge API could not
    /// say, so only a download reveals the content.
    pub fn commit(&self) -> Option<&str> {
        self.commit.as_deref()
    }
}

/// Downloads the repository as a tarball and extracts it.
///
/// The ref is resolved to a commit through the forge API when possible so
/// the download is pinned and the commit can be recorded for update checks.
/// Unreachable forge APIs (rate limits, exotic self-hosted setups) degrade
/// to probing common branch names with `commit: None`.
pub async fn fetch_repo(client: &Client, spec: &RepoSpec) -> Result<FetchedRepo, FetchError> {
    fetch_planned(client, spec, &plan_refs(client, spec).await).await
}

/// Downloads and extracts the tarball `plan` resolved.
pub async fn fetch_planned(
    client: &Client,
    spec: &RepoSpec,
    plan: &RefPlan,
) -> Result<FetchedRepo, FetchError> {
    let mut last_status: Option<FetchError> = None;

    for candidate in &plan.candidates {
        let download_ref = plan.commit.as_deref().unwrap_or(candidate);
        let url = spec.archive_url(download_ref);

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
            pinned: plan.pinned,
        });
    }

    Err(FetchError::RefNotFound {
        repo: spec.human_id(),
        detail: last_status
            .map(|e| e.to_string())
            .unwrap_or_else(|| "no candidate refs".into()),
    })
}

/// Resolves the ref `spec` names, or the default branch, without
/// downloading anything.
pub async fn plan_refs(client: &Client, spec: &RepoSpec) -> RefPlan {
    if let Some(explicit) = &spec.explicit_ref {
        if is_commit_sha(explicit) {
            return RefPlan {
                candidates: vec![explicit.clone()],
                commit: Some(explicit.to_ascii_lowercase()),
                pinned: true,
                authoritative: true,
            };
        }
        let commit = resolve_commit(client, spec, explicit).await;
        let branch = if commit.is_none() || is_abbreviated_sha_of(explicit, commit.as_deref()) {
            None
        } else {
            branch_exists(client, spec, explicit).await
        };
        return RefPlan {
            pinned: ref_is_pinned(explicit, commit.as_deref(), branch),
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
            pinned: false,
            authoritative: true,
        };
    }

    RefPlan {
        candidates: FALLBACK_REFS.iter().map(|r| r.to_string()).collect(),
        commit: None,
        pinned: false,
        authoritative: false,
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
            EntryType::Regular | EntryType::Directory | EntryType::Symlink => {}
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

        if entry.header().entry_type() == EntryType::Symlink {
            continue;
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
    fn classifies_refs_as_pinned_or_tracking() {
        let sha = "a".repeat(40);
        assert!(ref_is_pinned(&sha, None, Some(true)));
        assert!(ref_is_pinned("AAAAAAA", Some(&sha), Some(true)));
        assert!(!ref_is_pinned("aaaaaaa", Some(&"b".repeat(40)), Some(true)));
        assert!(!ref_is_pinned("aaaaaaa", None, None));
        assert!(!ref_is_pinned("abcdef", Some("abcdef0123"), None));
        assert!(!ref_is_pinned("main", Some(&sha), Some(true)));
        assert!(ref_is_pinned("v1.2.0", Some(&sha), Some(false)));
        assert!(!ref_is_pinned("v1.2.0", None, Some(false)));
        assert!(!ref_is_pinned("v1.2.0", Some(&sha), None));
        assert!(!ref_is_pinned("HEAD", Some(&sha), Some(false)));
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
    fn skipped_symlinks_cannot_redirect_extraction() {
        let outside = TempDir::new().unwrap();
        let sentinel = outside.path().join("sentinel");
        std::fs::write(&sentinel, "original").unwrap();
        let mut entries = targz_entries();
        entries.symlink("repo/link", outside.path().to_str().unwrap());
        entries.file("repo/link/sentinel", "replacement");
        entries.symlink("repo/file", sentinel.to_str().unwrap());
        entries.file("repo/file", "replacement");
        let dest = TempDir::new().unwrap();

        extract_archive(&entries.finish(), dest.path()).unwrap();

        assert_eq!(std::fs::read_to_string(&sentinel).unwrap(), "original");
        assert_eq!(
            std::fs::read_to_string(dest.path().join("repo/link/sentinel")).unwrap(),
            "replacement"
        );
        assert_eq!(
            std::fs::read_to_string(dest.path().join("repo/file")).unwrap(),
            "replacement"
        );
        assert!(
            !dest
                .path()
                .join("repo/link")
                .symlink_metadata()
                .unwrap()
                .is_symlink()
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
        assert!(!fetched.pinned);
        assert!(fetched.root().join("plugin.json").is_file());
    }

    #[tokio::test]
    async fn plans_refs_from_the_api_without_downloading() {
        let sha = "b".repeat(40);
        let server = TestServer::start(HashMap::from([
            (
                "/api/v1/repos/owner/repo".to_string(),
                CannedResponse::json(r#"{"default_branch":"develop"}"#),
            ),
            (
                "/api/v1/repos/owner/repo/git/commits/develop".to_string(),
                CannedResponse::json(&format!(r#"{{"sha":"{sha}"}}"#)),
            ),
        ]))
        .await;

        let spec = RepoSpec::parse(&format!("{}/owner/repo", server.url()), None).unwrap();
        let plan = plan_refs(&client(), &spec).await;
        assert_eq!(plan.commit(), Some(sha.as_str()));

        let err = fetch_planned(&client(), &spec, &plan).await.unwrap_err();
        assert!(
            matches!(&err, FetchError::Status { url, .. } if url.ends_with(&format!("{sha}.tar.gz"))),
            "{err}"
        );
    }

    #[tokio::test]
    async fn explicit_refs_are_pinned_unless_the_forge_knows_them_as_branches() {
        let sha = "c".repeat(40);
        let server = TestServer::start(HashMap::from([
            (
                "/api/v1/repos/owner/repo/git/commits/v1.2.0".to_string(),
                CannedResponse::json(&format!(r#"{{"sha":"{sha}"}}"#)),
            ),
            (
                "/api/v1/repos/owner/repo/git/commits/develop".to_string(),
                CannedResponse::json(&format!(r#"{{"sha":"{sha}"}}"#)),
            ),
            (
                "/api/v1/repos/owner/repo/branches/develop".to_string(),
                CannedResponse::json(r#"{"name":"develop"}"#),
            ),
            (
                format!("/owner/repo/archive/{sha}.tar.gz"),
                CannedResponse::targz(targz(&[("repo/plugin.json", "{}")])),
            ),
        ]))
        .await;

        for (git_ref, pinned) in [("v1.2.0", true), ("develop", false)] {
            let url = format!("{}/owner/repo?ref={git_ref}", server.url());
            let spec = RepoSpec::parse(&url, None).unwrap();
            let fetched = fetch_repo(&client(), &spec).await.unwrap();

            assert_eq!(fetched.git_ref, git_ref);
            assert_eq!(fetched.commit.as_deref(), Some(sha.as_str()));
            assert_eq!(fetched.pinned, pinned, "{git_ref}");
        }
    }

    #[tokio::test]
    async fn abbreviated_shas_pin_without_branch_lookups() {
        let sha = "d".repeat(40);
        let server = TestServer::start(HashMap::from([
            (
                "/api/v1/repos/owner/repo/git/commits/ddddddd".to_string(),
                CannedResponse::json(&format!(r#"{{"sha":"{sha}"}}"#)),
            ),
            (
                format!("/owner/repo/archive/{sha}.tar.gz"),
                CannedResponse::targz(targz(&[("repo/plugin.json", "{}")])),
            ),
        ]))
        .await;

        let url = format!("{}/owner/repo?ref=ddddddd", server.url());
        let spec = RepoSpec::parse(&url, None).unwrap();
        let fetched = fetch_repo(&client(), &spec).await.unwrap();

        assert_eq!(fetched.commit.as_deref(), Some(sha.as_str()));
        assert!(fetched.pinned);
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
        assert!(!fetched.pinned);
    }

    #[tokio::test]
    async fn explicit_refs_track_when_the_forge_api_is_unreachable() {
        let server = TestServer::start(HashMap::from([(
            "/owner/repo/archive/v1.tar.gz".to_string(),
            CannedResponse::targz(targz(&[("repo/plugin.json", "{}")])),
        )]))
        .await;

        let url = format!("{}/owner/repo?ref=v1", server.url());
        let spec = RepoSpec::parse(&url, None).unwrap();
        let fetched = fetch_repo(&client(), &spec).await.unwrap();

        assert_eq!(fetched.commit, None);
        assert!(!fetched.pinned);
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
        assert!(fetched.pinned);
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
