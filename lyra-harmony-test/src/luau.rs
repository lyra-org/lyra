// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::path::{
    Path,
    PathBuf,
};

pub struct Summary {
    pub passed: usize,
    pub failed: usize,
}

/// Runs `tests`, which `discover` found under `root`, as the plugin owning `root`.
pub async fn run(root: &Path, tests: Vec<PathBuf>) -> anyhow::Result<Summary> {
    let plugin = lyra_server::testing::PluginUnderTest::locate(root)?;
    let mut passed = 0usize;
    let mut failed = 0usize;
    for test_path in tests {
        let name = test_name(root, &test_path);
        let outcome = match load_fixture(&test_path) {
            Ok(fixture) => {
                lyra_server::testing::run_luau_plugin_test_file(
                    &plugin,
                    &test_path,
                    fixture.as_ref(),
                )
                .await
            }
            Err(error) => Err(error),
        };
        match outcome {
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
    Ok(Summary { passed, failed })
}

/// The fixture a Luau test declares in a `<test>.fixture.toml` beside it, if any.
fn load_fixture(test_path: &Path) -> anyhow::Result<Option<lyra_server::testing::Fixture>> {
    let path = test_path.with_extension("fixture.toml");
    if !path.is_file() {
        return Ok(None);
    }
    let content = std::fs::read_to_string(&path)
        .map_err(|error| anyhow::anyhow!("read {}: {error}", path.display()))?;
    let fixture = toml::from_str(&content)
        .map_err(|error| anyhow::anyhow!("parse {}: {error}", path.display()))?;
    Ok(Some(fixture))
}

/// Whether `dir` lies within a test directory's `luau/` tree.
pub fn within_tree(dir: &Path) -> bool {
    dir.ancestors().any(|ancestor| {
        ancestor.file_name().is_some_and(|name| name == "luau")
            && ancestor
                .parent()
                .and_then(Path::file_name)
                .is_some_and(|name| name == "tests")
    })
}

/// The `.luau` tests under `dir`'s `luau/` tree, or under `dir` itself when it lies within one,
/// skipping `_`-prefixed helpers.
pub fn discover(dir: &Path, filter: Option<&str>) -> anyhow::Result<Vec<PathBuf>> {
    let mut tests = Vec::new();
    let root = if within_tree(dir) {
        dir.to_path_buf()
    } else {
        dir.join("luau")
    };
    if root.is_dir() {
        discover_recursive(&root, &root, filter, &mut tests)?;
    }
    tests.sort();
    Ok(tests)
}

fn discover_recursive(
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
        if path.is_dir() {
            discover_recursive(root, &path, filter, tests)?;
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
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .canonicalize()
            .expect("canonicalize checked-in Luau tests");
        let tests = discover(&root, None).expect("discover checked-in Luau tests");
        let summary = run(&root, tests).await.expect("run checked-in Luau tests");

        assert_eq!(summary.failed, 0);
    }
}
