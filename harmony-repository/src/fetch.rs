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
use harmony_core::plugin::PLUGIN_CONFIG_MAX_BYTES;
use reqwest::{
    Client,
    StatusCode,
};
use tar::{
    Archive,
    EntryType,
};
use tempfile::TempDir;

use crate::forge::{
    RepoSpec,
    branch_exists,
    default_branch,
    is_object_id,
    ls_refs,
    resolve_commit,
};
use crate::manifest::REPOSITORY_MANIFEST_MAX_BYTES;

const FALLBACK_REFS: &[&str] = &["main", "master", "trunk"];

pub const MAX_ARCHIVE_BYTES: u64 = 50 * 1024 * 1024;
pub const MAX_UNPACKED_BYTES: u64 = 256 * 1024 * 1024;
pub const MAX_ARCHIVE_ENTRIES: usize = 16 * 1024;
/// Cap on a single manifest file read through [`fetch_file`] or from an
/// extracted archive: the `repository.json` cap, which `plugin.json` shares.
pub const MAX_FILE_BYTES: u64 = REPOSITORY_MANIFEST_MAX_BYTES;
const _: () = assert!(PLUGIN_CONFIG_MAX_BYTES == REPOSITORY_MANIFEST_MAX_BYTES);

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
    #[error("file at {location} exceeds the {limit} byte limit")]
    FileTooLarge { location: String, limit: u64 },
    #[error("invalid archive: {reason}")]
    InvalidArchive { reason: String },
    #[error("archive entry '{path}' is not allowed: {reason}")]
    UnsafeArchiveEntry { path: String, reason: String },
    #[error("no downloadable ref found for {repo}: {detail}")]
    RefNotFound { repo: String, detail: String },
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// Whether `git_ref` names a ref outright: a branch or tag name, or a full
/// `refs/...` name, rather than a revision expression such as `main~1`.
/// Only such names can be looked up by ref prefix.
fn is_plain_ref_name(git_ref: &str) -> bool {
    !git_ref.is_empty()
        && !git_ref.contains("@{")
        && !git_ref.chars().any(|c| {
            c.is_whitespace()
                || c.is_control()
                || matches!(c, '~' | '^' | ':' | '?' | '*' | '[' | '\\')
        })
}

/// Whether `git_ref` is shaped like an abbreviated commit SHA.
fn could_be_abbreviated_sha(git_ref: &str) -> bool {
    (7..40).contains(&git_ref.len()) && git_ref.chars().all(|c| c.is_ascii_hexdigit())
}

/// Whether `git_ref` is an abbreviated SHA that the forge expanded to
/// `commit`. Branches and tags can be named like hex strings, so the
/// resolved commit has to confirm the interpretation.
fn is_abbreviated_sha_of(git_ref: &str, commit: Option<&str>) -> bool {
    could_be_abbreviated_sha(git_ref)
        && commit.is_some_and(|commit| {
            commit
                .to_ascii_lowercase()
                .starts_with(&git_ref.to_ascii_lowercase())
        })
}

/// Whether an explicit ref pins the install. Commit SHAs and tags are
/// pinned; branches, full `refs/heads/` names and `HEAD` track new commits.
/// `commit` is what the forge resolved the ref to and `branch` its answer on
/// whether the ref names a branch by its short name. Without a resolved
/// commit the API is unreachable, so the branch answer is not trusted and
/// the ref is assumed to track.
fn ref_is_pinned(git_ref: &str, commit: Option<&str>, branch: Option<bool>) -> bool {
    if git_ref == "HEAD" || git_ref.starts_with("refs/heads/") {
        return false;
    }
    if is_object_id(git_ref) || is_abbreviated_sha_of(git_ref, commit) {
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

/// One directory of a repository downloaded into a temporary directory,
/// removed when this value is dropped.
#[derive(Debug)]
pub struct FetchedDirectory {
    _tempdir: TempDir,
    path: PathBuf,
}

impl FetchedDirectory {
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// What a repository's ref resolves to, worked out from the forge API
/// alone. [`fetch_planned`] downloads it.
#[derive(Debug, Clone)]
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

    /// The ref and the commit it resolves to, when the forge API said.
    pub(crate) fn resolved(&self) -> Option<(&str, &str)> {
        Some((self.candidates.first()?, self.commit.as_deref()?))
    }

    pub(crate) fn pinned(&self) -> bool {
        self.pinned
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

        let bytes = match download(client, &url, MAX_ARCHIVE_BYTES, |url, limit| {
            FetchError::ArchiveTooLarge { url, limit }
        })
        .await
        {
            Ok(bytes) => bytes,
            Err(FetchError::Status { url, status })
                if status == StatusCode::NOT_FOUND && !plan.authoritative =>
            {
                last_status = Some(FetchError::Status { url, status });
                continue;
            }
            Err(err) => return Err(err),
        };

        let (tempdir, root) = unpack(bytes).await?;
        return Ok(FetchedRepo {
            root,
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

/// Downloads the directory `dir` at `commit` and extracts it: as a
/// single-directory archive where the forge serves one, otherwise, and for
/// the root, as the whole repository.
pub async fn fetch_directory(
    client: &Client,
    spec: &RepoSpec,
    commit: &str,
    dir: &str,
) -> Result<FetchedDirectory, FetchError> {
    let url = Some(dir)
        .filter(|dir| !dir.is_empty())
        .and_then(|dir| spec.directory_archive_url(commit, dir))
        .unwrap_or_else(|| spec.archive_url(commit));
    let bytes = download(client, &url, MAX_ARCHIVE_BYTES, |url, limit| {
        FetchError::ArchiveTooLarge { url, limit }
    })
    .await?;

    let (tempdir, root) = unpack(bytes).await?;
    let path = match dir {
        "" => root,
        dir => root.join(dir),
    };
    if !path.is_dir() {
        return Err(FetchError::InvalidArchive {
            reason: format!("archive from {url} has no directory '{dir}'"),
        });
    }
    Ok(FetchedDirectory {
        _tempdir: tempdir,
        path,
    })
}

/// Extracts a downloaded tarball into a new temporary directory and
/// returns it with the archive's root.
async fn unpack(bytes: Vec<u8>) -> Result<(TempDir, PathBuf), FetchError> {
    let tempdir = TempDir::new()?;
    let dest = tempdir.path().to_path_buf();
    tokio::task::spawn_blocking(move || extract_archive(&bytes, &dest))
        .await
        .map_err(|e| FetchError::InvalidArchive {
            reason: format!("extraction task failed: {e}"),
        })??;
    let root = locate_root(tempdir.path())?;
    Ok((tempdir, root))
}

/// Resolves the ref `spec` names, or the default branch, without
/// downloading anything: with one Git protocol v2 `ls-refs` request, or
/// through the forge API when the forge does not answer in v2.
pub async fn plan_refs(client: &Client, spec: &RepoSpec) -> RefPlan {
    if let Some(explicit) = &spec.explicit_ref
        && is_object_id(explicit)
    {
        return RefPlan {
            candidates: vec![explicit.clone()],
            commit: Some(explicit.to_ascii_lowercase()),
            pinned: true,
            authoritative: true,
        };
    }
    match plan_from_ls_refs(client, spec).await {
        Some(plan) => plan,
        None => plan_from_api(client, spec).await,
    }
}

/// Plans from the refs the forge advertises. `None` when it gave no v2
/// answer or one without `HEAD`, when `HEAD` names no branch, for revision
/// expressions, and for an explicit ref that matches nothing but could be an
/// abbreviated commit: only the API resolves those. Like `git rev-parse`, a
/// tag takes precedence over a branch of the same name for the commit, while
/// the branch still makes the ref track. A full `refs/heads/` name tracks;
/// other full names are pinned.
async fn plan_from_ls_refs(client: &Client, spec: &RepoSpec) -> Option<RefPlan> {
    let explicit = spec.explicit_ref.as_deref();
    if explicit.is_none_or(|git_ref| git_ref == "HEAD") {
        let refs = ls_refs(client, spec, &["HEAD"]).await?;
        let head = refs.iter().find(|advertised| advertised.name == "HEAD")?;
        let git_ref = match explicit {
            Some(explicit) => explicit.to_string(),
            None => head
                .symref_target
                .as_deref()?
                .strip_prefix("refs/heads/")?
                .to_string(),
        };
        return Some(RefPlan {
            candidates: vec![git_ref],
            commit: Some(head.commit().to_string()),
            pinned: false,
            authoritative: true,
        });
    }

    let git_ref = explicit.filter(|git_ref| is_plain_ref_name(git_ref))?;
    let (branch_name, tag_name) = (
        format!("refs/heads/{git_ref}"),
        format!("refs/tags/{git_ref}"),
    );
    let full = git_ref.starts_with("refs/");
    let prefixes: &[&str] = if full {
        &["HEAD", git_ref]
    } else {
        &["HEAD", &branch_name, &tag_name]
    };
    let refs = ls_refs(client, spec, prefixes).await?;
    let named = |name: &str| refs.iter().find(|advertised| advertised.name == name);
    // Every repository with commits has a HEAD; an answer without one did
    // not list what was asked for.
    named("HEAD")?;
    let (found, branch) = if full {
        (named(git_ref), None)
    } else {
        let branch = named(&branch_name);
        (named(&tag_name).or(branch), branch)
    };
    let commit = found.map(|advertised| advertised.commit().to_string());
    if commit.is_none() && could_be_abbreviated_sha(git_ref) {
        return None;
    }
    Some(RefPlan {
        pinned: ref_is_pinned(git_ref, commit.as_deref(), Some(branch.is_some())),
        candidates: vec![git_ref.to_string()],
        commit,
        authoritative: true,
    })
}

/// Plans through the forge API, which also expands abbreviated commits.
async fn plan_from_api(client: &Client, spec: &RepoSpec) -> RefPlan {
    if let Some(explicit) = &spec.explicit_ref {
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

/// Downloads the file at `path` in `commit`; `None` when the forge has no
/// such file.
pub async fn fetch_file(
    client: &Client,
    spec: &RepoSpec,
    commit: &str,
    path: &str,
) -> Result<Option<Vec<u8>>, FetchError> {
    let url = spec.raw_file_url(commit, path);
    let downloaded = download(client, &url, MAX_FILE_BYTES, |url, limit| {
        FetchError::FileTooLarge {
            location: url,
            limit,
        }
    })
    .await;
    match downloaded {
        Ok(bytes) => Ok(Some(bytes)),
        Err(FetchError::Status { status, .. }) if status == StatusCode::NOT_FOUND => Ok(None),
        Err(err) => Err(err),
    }
}

async fn download(
    client: &Client,
    url: &str,
    limit: u64,
    too_large: fn(String, u64) -> FetchError,
) -> Result<Vec<u8>, FetchError> {
    let mut response = client
        .get(url)
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

    if response.content_length().is_some_and(|len| len > limit) {
        return Err(too_large(url.to_string(), limit));
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
        if (bytes.len() + chunk.len()) as u64 > limit {
            return Err(too_large(url.to_string(), limit));
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
        pkt_lines,
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
        assert!(!ref_is_pinned("refs/heads/main", Some(&sha), Some(false)));
        assert!(ref_is_pinned("refs/tags/v1", Some(&sha), Some(false)));
    }

    #[test]
    fn detects_commit_shas() {
        assert!(is_object_id(&"a".repeat(40)));
        assert!(is_object_id(&"F".repeat(64)));
        assert!(!is_object_id("main"));
        assert!(!is_object_id(&"a".repeat(39)));
        assert!(!is_object_id(&"g".repeat(40)));
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
    async fn full_branch_names_track_through_the_api() {
        let sha = "c".repeat(40);
        let server = TestServer::start(HashMap::from([(
            "/api/v1/repos/owner/repo/git/commits/refs%2Fheads%2Fdevelop".to_string(),
            CannedResponse::json(&format!(r#"{{"sha":"{sha}"}}"#)),
        )]))
        .await;

        let url = format!("{}/owner/repo", server.url());
        let spec = RepoSpec::parse(&url, Some("refs/heads/develop")).unwrap();
        let plan = plan_refs(&client(), &spec).await;

        assert_eq!(plan.commit(), Some(sha.as_str()));
        assert!(!plan.pinned);
        assert!(
            server.requests().contains(
                &"/api/v1/repos/owner/repo/git/commits/refs%2Fheads%2Fdevelop".to_string()
            )
        );
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

    fn upload_pack_route(lines: &[&str]) -> (String, CannedResponse) {
        (
            "/owner/repo.git/git-upload-pack".to_string(),
            CannedResponse::upload_pack(&pkt_lines(lines)),
        )
    }

    async fn plan_at(
        routes: Vec<(String, CannedResponse)>,
        git_ref: Option<&str>,
    ) -> (RefPlan, Vec<String>) {
        let (plan, server) = plan_on(routes, git_ref).await;
        (plan, server.requests())
    }

    async fn plan_on(
        routes: Vec<(String, CannedResponse)>,
        git_ref: Option<&str>,
    ) -> (RefPlan, TestServer) {
        let server = TestServer::start(routes.into_iter().collect()).await;
        let url = format!("{}/owner/repo", server.url());
        let spec = RepoSpec::parse(&url, git_ref).unwrap();
        let plan = plan_refs(&client(), &spec).await;
        (plan, server)
    }

    fn head_line() -> String {
        format!("{} HEAD symref-target:refs/heads/main\n", "a".repeat(40))
    }

    #[tokio::test]
    async fn plans_refs_from_one_ls_refs_request() {
        let (head, branch, tag, peeled, light, other) = (
            "a".repeat(40),
            "b".repeat(40),
            "c".repeat(40),
            "d".repeat(40),
            "f".repeat(40),
            "e".repeat(40),
        );
        let refs = [
            head_line(),
            format!("{branch} refs/heads/master\n"),
            format!("{other} refs/heads/master-foo\n"),
            format!("{tag} refs/tags/v1 peeled:{peeled}\n"),
            format!("{other} refs/tags/v1.1\n"),
            format!("{light} refs/tags/light\n"),
            format!("{tag} refs/tags/both peeled:{peeled}\n"),
            format!("{branch} refs/heads/both\n"),
            format!("{other} refs/pull/1/head\n"),
        ];
        let lines: Vec<&str> = refs.iter().map(String::as_str).collect();

        for (git_ref, candidate, commit, pinned) in [
            (None, "main", &head, false),
            (Some("HEAD"), "HEAD", &head, false),
            (Some("master"), "master", &branch, false),
            (Some("v1"), "v1", &peeled, true),
            (Some("light"), "light", &light, true),
            (Some("both"), "both", &peeled, false),
            (
                Some("refs/heads/master"),
                "refs/heads/master",
                &branch,
                false,
            ),
            (Some("refs/tags/v1"), "refs/tags/v1", &peeled, true),
            (Some("refs/pull/1/head"), "refs/pull/1/head", &other, true),
        ] {
            let (plan, server) = plan_on(vec![upload_pack_route(&lines)], git_ref).await;
            assert_eq!(plan.candidates, [candidate], "{git_ref:?}");
            assert_eq!(plan.commit(), Some(commit.as_str()), "{git_ref:?}");
            assert_eq!(plan.pinned, pinned, "{git_ref:?}");
            assert_eq!(
                server.requests(),
                ["/owner/repo.git/git-upload-pack"],
                "{git_ref:?}"
            );
            let head = server.request_heads()[0].to_ascii_lowercase();
            assert!(head.starts_with("post "), "{head}");
            assert!(head.contains("\r\ngit-protocol: version=2\r\n"), "{head}");
            assert!(
                head.contains("\r\nuser-agent: git/harmony-repository-"),
                "{head}"
            );
            assert!(
                head.contains("\r\ncontent-type: application/x-git-upload-pack-request\r\n"),
                "{head}"
            );
        }
    }

    #[tokio::test]
    async fn refs_ls_refs_does_not_list_are_not_found_without_api_calls() {
        let master_foo = format!("{} refs/heads/master-foo\n", "e".repeat(40));
        let head = head_line();

        for lines in [
            vec![head.as_str()],
            vec![head.as_str(), master_foo.as_str()],
        ] {
            let (plan, requests) = plan_at(vec![upload_pack_route(&lines)], Some("master")).await;
            assert_eq!(plan.commit(), None);
            assert!(!plan.pinned);
            assert!(plan.authoritative);
            assert_eq!(requests, ["/owner/repo.git/git-upload-pack"]);
        }
    }

    #[tokio::test]
    async fn revisions_ls_refs_cannot_answer_are_resolved_by_the_api() {
        let sha = "d".repeat(40);
        let head = head_line();
        for (git_ref, api_ref, upload_pack) in [
            ("ddddddd", "ddddddd", true),
            ("main~1", "main~1", false),
            ("v1^{}", "v1%5E%7B%7D", false),
            ("main\n0000", "main%0A0000", false),
        ] {
            let (plan, requests) = plan_at(
                vec![
                    upload_pack_route(&[&head]),
                    (
                        format!("/api/v1/repos/owner/repo/git/commits/{api_ref}"),
                        CannedResponse::json(&format!(r#"{{"sha":"{sha}"}}"#)),
                    ),
                ],
                Some(git_ref),
            )
            .await;
            assert_eq!(plan.commit(), Some(sha.as_str()), "{git_ref:?}");
            assert_eq!(
                requests.contains(&"/owner/repo.git/git-upload-pack".to_string()),
                upload_pack,
                "{git_ref:?}: {requests:?}"
            );
        }
    }

    #[tokio::test]
    async fn forges_without_a_v2_answer_are_planned_through_the_api() {
        let sha = "b".repeat(40);
        let api = || {
            vec![
                (
                    "/api/v1/repos/owner/repo".to_string(),
                    CannedResponse::json(r#"{"default_branch":"develop"}"#),
                ),
                (
                    "/api/v1/repos/owner/repo/git/commits/develop".to_string(),
                    CannedResponse::json(&format!(r#"{{"sha":"{sha}"}}"#)),
                ),
            ]
        };
        let malformed = || {
            (
                "/owner/repo.git/git-upload-pack".to_string(),
                CannedResponse::upload_pack("0010garbage"),
            )
        };
        let headless = || upload_pack_route(&[&format!("{sha} refs/heads/develop\n")]);
        let detached = || upload_pack_route(&[&format!("{sha} HEAD\n")]);

        for extra in [None, Some(malformed()), Some(headless()), Some(detached())] {
            let mut routes = api();
            routes.extend(extra);
            let (plan, requests) = plan_at(routes, None).await;
            assert_eq!(plan.candidates, ["develop"]);
            assert_eq!(plan.commit(), Some(sha.as_str()));
            assert_eq!(
                requests,
                [
                    "/owner/repo.git/git-upload-pack",
                    "/api/v1/repos/owner/repo",
                    "/api/v1/repos/owner/repo/git/commits/develop",
                ]
            );
        }

        let mut routes = api();
        routes.push(upload_pack_route(&[]));
        routes.push((
            "/api/v1/repos/owner/repo/git/commits/v1".to_string(),
            CannedResponse::json(&format!(r#"{{"sha":"{sha}"}}"#)),
        ));
        let (plan, requests) = plan_at(routes, Some("v1")).await;
        assert_eq!(
            plan.commit(),
            Some(sha.as_str()),
            "an answer without HEAD is no authoritative not-found"
        );
        assert!(requests.contains(&"/api/v1/repos/owner/repo/git/commits/v1".to_string()));
    }
}
