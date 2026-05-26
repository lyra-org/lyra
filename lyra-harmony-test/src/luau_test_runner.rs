// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::path::{
    Path,
    PathBuf,
};

pub struct LuauRunSummary {
    pub failed: usize,
}

pub async fn run(root: &Path, filter: Option<&str>) -> anyhow::Result<LuauRunSummary> {
    let root = root
        .canonicalize()
        .map_err(|error| anyhow::anyhow!("canonicalize {}: {error}", root.display()))?;
    let tests = discover_luau_tests(&root, filter)?;
    if tests.is_empty() {
        anyhow::bail!("no Luau test files found under {}", root.display());
    }

    let mut passed = 0usize;
    let mut failed = 0usize;
    for test_path in tests {
        let name = test_name(&root, &test_path);
        match lyra_server::testing::run_luau_plugin_test_file(&root, &test_path).await {
            Ok(()) => {
                println!("PASS {name}");
                passed += 1;
            }
            Err(error) => {
                println!("FAIL {name}");
                println!("  {error:#}");
                failed += 1;
            }
        }
    }

    println!(
        "{} Luau tests: {passed} passed, {failed} failed",
        passed + failed
    );
    Ok(LuauRunSummary { failed })
}

fn discover_luau_tests(root: &Path, filter: Option<&str>) -> anyhow::Result<Vec<PathBuf>> {
    let mut tests = Vec::new();
    discover_luau_tests_recursive(root, root, filter, &mut tests)?;
    tests.sort();
    Ok(tests)
}

fn discover_luau_tests_recursive(
    root: &Path,
    dir: &Path,
    filter: Option<&str>,
    tests: &mut Vec<PathBuf>,
) -> anyhow::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let file_name = entry.file_name();
        let file_name = file_name.to_string_lossy();
        if file_name == "fixtures" {
            continue;
        }
        if path.is_dir() {
            discover_luau_tests_recursive(root, &path, filter, tests)?;
            continue;
        }
        if path.extension().is_none_or(|extension| extension != "luau") {
            continue;
        }
        if file_name.starts_with('_') {
            continue;
        }
        let name = test_name(root, &path);
        if filter.is_some_and(|filter| !name.contains(filter)) {
            continue;
        }
        tests.push(path);
    }
    Ok(())
}

fn test_name(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .with_extension("")
        .to_string_lossy()
        .replace('\\', "/")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn runs_checked_in_luau_tests() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests");
        let summary = run(&root, None).await.expect("run checked-in Luau tests");

        assert_eq!(summary.failed, 0);
    }
}
