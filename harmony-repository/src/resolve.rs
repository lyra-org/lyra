// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashSet;
use std::path::{
    Path,
    PathBuf,
};
use std::sync::Arc;

use harmony_core::plugin::{
    LoadedPlugin,
    PLUGIN_CONFIG_FILENAME,
    PluginLoadError,
    PluginManager,
};
use reqwest::Client;

use crate::fetch::{
    FetchError,
    FetchedRepo,
    RepoSpec,
    fetch_repo,
};
use crate::manifest::{
    ManifestError,
    REPOSITORY_MANIFEST_FILENAME,
    RepositoryEntry,
    RepositoryManifest,
    SOURCE_RECORD_SCHEMA_VERSION,
    SourceRecord,
};

#[derive(Debug, thiserror::Error)]
pub enum ResolveError {
    #[error(transparent)]
    Fetch(#[from] FetchError),
    #[error(transparent)]
    Manifest(#[from] ManifestError),
    #[error(
        "repository {repo} has both {PLUGIN_CONFIG_FILENAME} and \
         {REPOSITORY_MANIFEST_FILENAME} at its root"
    )]
    AmbiguousRoot { repo: String },
    #[error(
        "repository {repo} has neither {PLUGIN_CONFIG_FILENAME} nor \
         {REPOSITORY_MANIFEST_FILENAME} at its root"
    )]
    NotAPluginRepository { repo: String },
    #[error("path entry '{path}' in {repo} does not contain a {PLUGIN_CONFIG_FILENAME}")]
    PathEntryMissing { repo: String, path: String },
    #[error(
        "'{target}' is a plugin repository index; repositories may not reference other \
         repositories"
    )]
    NestedRepository { target: String },
    #[error("plugin at {location} failed validation: {source}")]
    Plugin {
        location: String,
        #[source]
        source: PluginLoadError,
    },
    #[error("entries '{first}' and '{second}' resolve to the same plugin id")]
    DuplicatePluginId { first: String, second: String },
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// An installable plugin discovered by [`resolve_plugins`]. The source
/// tree lives in the owning [`ResolvedRepository`]'s temporary storage.
#[derive(Debug)]
pub struct PluginCandidate {
    pub plugin: LoadedPlugin,
    /// Provenance to record at install time; `installed_at` is unset.
    pub source: SourceRecord,
    source_dir: PathBuf,
}

impl PluginCandidate {
    pub fn id(&self) -> &str {
        &self.plugin.manifest.id
    }

    pub fn source_dir(&self) -> &Path {
        &self.source_dir
    }
}

/// Every plugin reachable from one repository URL, with the fetched trees
/// kept alive until this value drops.
#[derive(Debug)]
pub struct ResolvedRepository {
    pub spec: RepoSpec,
    pub git_ref: String,
    pub commit: Option<String>,
    /// Present when the root carried a `repository.json`.
    pub index: Option<RepositoryManifest>,
    pub candidates: Vec<PluginCandidate>,
    _sources: Vec<FetchedRepo>,
}

/// Fetches a repository URL and resolves it to installable plugins.
///
/// A root `plugin.json` resolves to that single plugin. A root
/// `repository.json` resolves each entry: path entries from the same tree,
/// remote entries by fetching the referenced repository, which must be a
/// single plugin — never another index.
pub async fn resolve_plugins(
    client: &Client,
    spec: &RepoSpec,
    valid_scope_ids: &HashSet<Arc<str>>,
) -> Result<ResolvedRepository, ResolveError> {
    let fetched = fetch_repo(client, spec).await?;
    let root = fetched.root().to_path_buf();

    let has_plugin = root.join(PLUGIN_CONFIG_FILENAME).is_file();
    let has_index = root.join(REPOSITORY_MANIFEST_FILENAME).is_file();

    let mut candidates = Vec::new();
    let mut sources = Vec::new();
    let mut index = None;

    match (has_plugin, has_index) {
        (true, true) => {
            return Err(ResolveError::AmbiguousRoot {
                repo: spec.human_id(),
            });
        }
        (false, false) => {
            return Err(ResolveError::NotAPluginRepository {
                repo: spec.human_id(),
            });
        }
        (true, false) => {
            let record = SourceRecord {
                schema_version: SOURCE_RECORD_SCHEMA_VERSION,
                origin: spec.canonical_url(),
                forge: spec.forge,
                git_ref: spec.explicit_ref.clone(),
                commit: fetched.commit.clone(),
                subpath: None,
                via_repository: None,
                installed_at: None,
            };
            candidates.push(load_candidate(
                &root,
                spec.human_id(),
                record,
                valid_scope_ids,
            )?);
        }
        (false, true) => {
            let json = std::fs::read_to_string(root.join(REPOSITORY_MANIFEST_FILENAME))?;
            let manifest = RepositoryManifest::parse(&json, Some(spec))?;

            for entry in &manifest.entries {
                match entry {
                    RepositoryEntry::Path { path } => {
                        let dir = root.join(path);
                        let location = format!("{}/{path}", spec.human_id());

                        if dir.join(REPOSITORY_MANIFEST_FILENAME).is_file() {
                            return Err(ResolveError::NestedRepository { target: location });
                        }
                        if !dir.join(PLUGIN_CONFIG_FILENAME).is_file() {
                            return Err(ResolveError::PathEntryMissing {
                                repo: spec.human_id(),
                                path: path.clone(),
                            });
                        }

                        let record = SourceRecord {
                            schema_version: SOURCE_RECORD_SCHEMA_VERSION,
                            origin: spec.canonical_url(),
                            forge: spec.forge,
                            git_ref: spec.explicit_ref.clone(),
                            commit: fetched.commit.clone(),
                            subpath: Some(path.clone()),
                            via_repository: None,
                            installed_at: None,
                        };
                        candidates.push(load_candidate(&dir, location, record, valid_scope_ids)?);
                    }
                    RepositoryEntry::Remote { spec: remote } => {
                        let sub = fetch_repo(client, remote).await?;
                        let subroot = sub.root();

                        if subroot.join(REPOSITORY_MANIFEST_FILENAME).is_file() {
                            return Err(ResolveError::NestedRepository {
                                target: remote.human_id(),
                            });
                        }
                        if !subroot.join(PLUGIN_CONFIG_FILENAME).is_file() {
                            return Err(ResolveError::NotAPluginRepository {
                                repo: remote.human_id(),
                            });
                        }

                        let record = SourceRecord {
                            schema_version: SOURCE_RECORD_SCHEMA_VERSION,
                            origin: remote.canonical_url(),
                            forge: remote.forge,
                            git_ref: remote.explicit_ref.clone(),
                            commit: sub.commit.clone(),
                            subpath: None,
                            via_repository: Some(spec.canonical_url()),
                            installed_at: None,
                        };
                        candidates.push(load_candidate(
                            subroot,
                            remote.human_id(),
                            record,
                            valid_scope_ids,
                        )?);
                        sources.push(sub);
                    }
                }
            }

            index = Some(manifest);
        }
    }

    // Ids that differ only by case collide on case-insensitive
    // filesystems, so they count as duplicates too.
    for (i, first) in candidates.iter().enumerate() {
        for second in candidates.iter().skip(i + 1) {
            if first.id().eq_ignore_ascii_case(second.id()) {
                return Err(ResolveError::DuplicatePluginId {
                    first: first.source.location(),
                    second: second.source.location(),
                });
            }
        }
    }

    sources.push(fetched);

    Ok(ResolvedRepository {
        spec: spec.clone(),
        git_ref: sources
            .last()
            .expect("origin fetch present")
            .git_ref
            .clone(),
        commit: sources.last().expect("origin fetch present").commit.clone(),
        index,
        candidates,
        _sources: sources,
    })
}

fn load_candidate(
    dir: &Path,
    location: String,
    source: SourceRecord,
    valid_scope_ids: &HashSet<Arc<str>>,
) -> Result<PluginCandidate, ResolveError> {
    let plugin =
        PluginManager::load_plugin_dir(dir, valid_scope_ids).map_err(|e| ResolveError::Plugin {
            location,
            source: e,
        })?;

    Ok(PluginCandidate {
        plugin,
        source,
        source_dir: dir.to_path_buf(),
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::testutil::{
        CannedResponse,
        TestServer,
        client,
        plugin_json,
        single_plugin_routes,
        targz,
    };

    async fn resolve(server: &TestServer, path: &str) -> Result<ResolvedRepository, ResolveError> {
        let spec = RepoSpec::parse(&format!("{}{path}", server.url()), None).unwrap();
        resolve_plugins(&client(), &spec, &HashSet::new()).await
    }

    #[tokio::test]
    async fn resolves_single_plugin_repositories() {
        let server =
            TestServer::start(HashMap::from([single_plugin_routes("owner/repo", "foo")])).await;

        let resolved = resolve(&server, "/owner/repo").await.unwrap();

        assert!(resolved.index.is_none());
        assert_eq!(resolved.candidates.len(), 1);
        let candidate = &resolved.candidates[0];
        assert_eq!(candidate.id(), "foo");
        assert_eq!(
            candidate.source.origin,
            format!("{}/owner/repo", server.url())
        );
        assert_eq!(candidate.source.subpath, None);
        assert_eq!(candidate.source.via_repository, None);
        assert!(candidate.source_dir().join("init.luau").is_file());
    }

    #[tokio::test]
    async fn resolves_repository_indexes_with_path_and_remote_entries() {
        let server = TestServer::start_with(|base| {
            let index = format!(
                r#"{{
                    "schema_version": 1,
                    "name": "Test Plugins",
                    "plugins": [
                        {{ "path": "plugins/foo" }},
                        {{ "url": "{base}/other/single" }}
                    ]
                }}"#
            );
            HashMap::from([
                (
                    "/owner/repo/archive/main.tar.gz".to_string(),
                    CannedResponse::targz(targz(&[
                        ("repo/repository.json", &index),
                        ("repo/plugins/foo/plugin.json", &plugin_json("foo")),
                        ("repo/plugins/foo/init.luau", "return {}"),
                    ])),
                ),
                single_plugin_routes("other/single", "bar"),
            ])
        })
        .await;

        let resolved = resolve(&server, "/owner/repo").await.unwrap();

        let index = resolved.index.as_ref().unwrap();
        assert_eq!(index.name, "Test Plugins");
        assert_eq!(resolved.candidates.len(), 2);

        let foo = &resolved.candidates[0];
        assert_eq!(foo.id(), "foo");
        assert_eq!(foo.source.subpath.as_deref(), Some("plugins/foo"));
        assert_eq!(foo.source.via_repository, None);

        let bar = &resolved.candidates[1];
        assert_eq!(bar.id(), "bar");
        assert_eq!(bar.source.subpath, None);
        assert_eq!(
            bar.source.via_repository.as_deref(),
            Some(format!("{}/owner/repo", server.url()).as_str())
        );
    }

    #[tokio::test]
    async fn rejects_roots_with_both_manifests() {
        let server = TestServer::start(HashMap::from([(
            "/owner/repo/archive/main.tar.gz".to_string(),
            CannedResponse::targz(targz(&[
                ("repo/plugin.json", &plugin_json("foo")),
                (
                    "repo/repository.json",
                    r#"{ "schema_version": 1, "name": "X", "plugins": [] }"#,
                ),
            ])),
        )]))
        .await;

        let err = resolve(&server, "/owner/repo").await.unwrap_err();
        assert!(matches!(err, ResolveError::AmbiguousRoot { .. }), "{err}");
    }

    #[tokio::test]
    async fn rejects_roots_without_manifests() {
        let server = TestServer::start(HashMap::from([(
            "/owner/repo/archive/main.tar.gz".to_string(),
            CannedResponse::targz(targz(&[("repo/README.md", "hi")])),
        )]))
        .await;

        let err = resolve(&server, "/owner/repo").await.unwrap_err();
        assert!(
            matches!(err, ResolveError::NotAPluginRepository { .. }),
            "{err}"
        );
    }

    #[tokio::test]
    async fn rejects_path_entries_without_plugins() {
        let server = TestServer::start(HashMap::from([(
            "/owner/repo/archive/main.tar.gz".to_string(),
            CannedResponse::targz(targz(&[(
                "repo/repository.json",
                r#"{ "schema_version": 1, "name": "X", "plugins": [{ "path": "missing" }] }"#,
            )])),
        )]))
        .await;

        let err = resolve(&server, "/owner/repo").await.unwrap_err();
        assert!(
            matches!(err, ResolveError::PathEntryMissing { .. }),
            "{err}"
        );
    }

    #[tokio::test]
    async fn rejects_nested_indexes_in_path_entries() {
        let server = TestServer::start(HashMap::from([(
            "/owner/repo/archive/main.tar.gz".to_string(),
            CannedResponse::targz(targz(&[
                (
                    "repo/repository.json",
                    r#"{ "schema_version": 1, "name": "X", "plugins": [{ "path": "sub" }] }"#,
                ),
                ("repo/sub/plugin.json", &plugin_json("foo")),
                (
                    "repo/sub/repository.json",
                    r#"{ "schema_version": 1, "name": "Y", "plugins": [] }"#,
                ),
            ])),
        )]))
        .await;

        let err = resolve(&server, "/owner/repo").await.unwrap_err();
        assert!(
            matches!(err, ResolveError::NestedRepository { .. }),
            "{err}"
        );
    }

    #[tokio::test]
    async fn rejects_remote_entries_that_are_indexes() {
        let server = TestServer::start_with(|base| {
            let index = format!(
                r#"{{ "schema_version": 1, "name": "X", "plugins": [{{ "url": "{base}/other/nested" }}] }}"#
            );
            HashMap::from([
                (
                    "/owner/repo/archive/main.tar.gz".to_string(),
                    CannedResponse::targz(targz(&[("repo/repository.json", &index)])),
                ),
                (
                    "/other/nested/archive/main.tar.gz".to_string(),
                    CannedResponse::targz(targz(&[(
                        "repo/repository.json",
                        r#"{ "schema_version": 1, "name": "Y", "plugins": [] }"#,
                    )])),
                ),
            ])
        })
        .await;

        let err = resolve(&server, "/owner/repo").await.unwrap_err();
        assert!(
            matches!(err, ResolveError::NestedRepository { .. }),
            "{err}"
        );
    }

    #[tokio::test]
    async fn rejects_duplicate_plugin_ids_across_entries() {
        let server = TestServer::start_with(|base| {
            let index = format!(
                r#"{{
                    "schema_version": 1,
                    "name": "X",
                    "plugins": [{{ "path": "foo" }}, {{ "url": "{base}/other/single" }}]
                }}"#
            );
            HashMap::from([
                (
                    "/owner/repo/archive/main.tar.gz".to_string(),
                    CannedResponse::targz(targz(&[
                        ("repo/repository.json", &index),
                        ("repo/foo/plugin.json", &plugin_json("Foo")),
                        ("repo/foo/init.luau", "return {}"),
                    ])),
                ),
                single_plugin_routes("other/single", "foo"),
            ])
        })
        .await;

        let err = resolve(&server, "/owner/repo").await.unwrap_err();
        assert!(
            matches!(err, ResolveError::DuplicatePluginId { .. }),
            "{err}"
        );
    }

    #[tokio::test]
    async fn rejects_plugins_with_unknown_scopes() {
        let manifest = r#"{
            "schema_version": 1,
            "id": "foo",
            "name": "Foo",
            "version": "0.0.1",
            "description": "d",
            "entrypoint": "init.luau",
            "scopes": ["no.such.scope"]
        }"#;
        let server = TestServer::start(HashMap::from([(
            "/owner/repo/archive/main.tar.gz".to_string(),
            CannedResponse::targz(targz(&[
                ("repo/plugin.json", manifest),
                ("repo/init.luau", "return {}"),
            ])),
        )]))
        .await;

        let err = resolve(&server, "/owner/repo").await.unwrap_err();
        assert!(matches!(err, ResolveError::Plugin { .. }), "{err}");
    }
}
