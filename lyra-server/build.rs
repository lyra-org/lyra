// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Embeds the build's identity: `LYRA_GIT_HASH` and `LYRA_RELEASE_TAG`, taken
//! from the environment when set, else resolved from the checkout's git state.
//! Reruns only when the overrides or the checked-out commit change.

use std::{
    env,
    path::{
        Path,
        PathBuf,
    },
    process::Command,
};

fn main() {
    println!("cargo:rerun-if-env-changed=LYRA_GIT_HASH");
    println!("cargo:rerun-if-env-changed=LYRA_RELEASE_TAG");

    let release_tag = env::var("LYRA_RELEASE_TAG").unwrap_or_default();
    let release_tag = release_tag.trim();
    assert!(
        !release_tag.contains(['\n', '\r']),
        "invalid LYRA_RELEASE_TAG"
    );
    println!("cargo:rustc-env=LYRA_RELEASE_TAG={release_tag}");

    let hash = match env::var("LYRA_GIT_HASH")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
    {
        Some(hash) => hash,
        None => {
            let repo = Path::new(&env::var("CARGO_MANIFEST_DIR").expect("set by cargo"))
                .parent()
                .expect("lyra-server lives in the workspace root")
                .to_path_buf();
            // A hash without its rerun triggers would go stale silently.
            watch_head(&repo)
                .then(|| git(&repo, &["rev-parse", "--short=7", "HEAD"]))
                .flatten()
                .unwrap_or_else(|| "unknown".into())
        }
    };
    println!("cargo:rustc-env=LYRA_GIT_HASH={hash}");
}

/// Reruns the build script when the checked-out commit changes: on a commit,
/// checkout, reset, or ref packing, in a plain checkout or a linked worktree,
/// with files or reftable ref storage. Only paths that exist are watched,
/// since cargo reruns on every build for a missing one. Returns whether the
/// triggers are in place.
fn watch_head(repo: &Path) -> bool {
    let Some((git_dir, common_dir)) = git_dirs(repo) else {
        return false;
    };
    // A worktree's HEAD lives in its own git dir; refs are shared.
    let head = git_dir.join("HEAD");
    if !head.is_file() {
        return false;
    }
    watch(&head);
    watch(&common_dir.join("packed-refs"));
    watch(&common_dir.join("reftable"));
    if let Some(reference) = std::fs::read_to_string(&head).ok().and_then(|head| {
        head.strip_prefix("ref: ")
            .map(|name| name.trim().to_string())
    }) {
        // A packed ref has no loose file, and packing removes emptied
        // directories, so watch the nearest ancestor that exists; cargo scans
        // a watched directory recursively.
        let loose = common_dir.join(&reference);
        if let Some(existing) = loose
            .ancestors()
            .take_while(|path| path.starts_with(&common_dir))
            .find(|path| path.exists())
        {
            watch(existing);
        }
    }
    true
}

/// The worktree's git dir and the repository's common dir, made absolute
/// against `repo`, where git ran.
fn git_dirs(repo: &Path) -> Option<(PathBuf, PathBuf)> {
    let output = git(repo, &["rev-parse", "--git-dir", "--git-common-dir"])?;
    let mut lines = output.lines().map(|line| repo.join(line.trim()));
    Some((lines.next()?, lines.next()?))
}

fn watch(path: &Path) {
    if path.exists() {
        println!("cargo:rerun-if-changed={}", path.display());
    }
}

fn git(repo: &Path, args: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .args(args)
        .current_dir(repo)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let value = String::from_utf8_lossy(&output.stdout).trim().to_string();
    (!value.is_empty()).then_some(value)
}
