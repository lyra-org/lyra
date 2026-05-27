// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::env;
use std::path::PathBuf;
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=LYRA_GIT_HASH");
    println!("cargo:rerun-if-changed=../.git/HEAD");

    let hash = env::var("LYRA_GIT_HASH")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .or_else(resolve_git_hash)
        .unwrap_or_else(|| "unknown".to_string());

    println!("cargo:rustc-env=LYRA_GIT_HASH={hash}");
}

fn resolve_git_hash() -> Option<String> {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").ok()?;
    let crate_dir = PathBuf::from(manifest_dir);
    let repo_dir = crate_dir.parent().unwrap_or(crate_dir.as_path());

    let output = Command::new("git")
        .arg("rev-parse")
        .arg("--short=7")
        .arg("HEAD")
        .current_dir(repo_dir)
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let hash = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if hash.is_empty() { None } else { Some(hash) }
}
