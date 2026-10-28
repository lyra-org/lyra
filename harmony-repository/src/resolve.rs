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

use futures_util::stream::{
    self,
    StreamExt,
};
use harmony_core::plugin::{
    LoadedManifest,
    LoadedPlugin,
    PLUGIN_CONFIG_FILENAME,
    PluginLoadError,
    PluginManager,
};
use reqwest::Client;

use crate::fetch::{
    FetchError,
    FetchedRepo,
    MAX_FILE_BYTES,
    RefPlan,
    fetch_file,
    fetch_planned,
    plan_refs,
};
use crate::forge::RepoSpec;
use crate::manifest::{
    ManifestError,
    REPOSITORY_MANIFEST_FILENAME,
    RepositoryEntry,
    RepositoryManifest,
    SOURCE_RECORD_SCHEMA_VERSION,
    SourceRecord,
};

/// Path entries whose manifests are read at once, to stay polite to forges.
const MAX_CONCURRENT_ENTRIES: usize = 4;

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

/// A plugin offered by a repository, known from its `plugin.json` alone.
#[derive(Debug)]
pub struct ListedPlugin {
    pub loaded: LoadedManifest,
    /// Provenance an install from the same commit would record.
    pub source: SourceRecord,
}

impl ListedPlugin {
    pub fn id(&self) -> &str {
        &self.loaded.manifest.id
    }
}

/// Every plugin reachable from one repository URL, as [`list_plugins`]
/// read it from the manifests.
#[derive(Debug)]
pub struct RepositoryListing {
    pub spec: RepoSpec,
    pub git_ref: String,
    pub commit: Option<String>,
    /// Present when the root carried a `repository.json`.
    pub index: Option<RepositoryManifest>,
    pub plugins: Vec<ListedPlugin>,
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
    let plan = plan_refs(client, spec).await;
    resolve_planned(client, spec, &plan, valid_scope_ids).await
}

/// Like [`resolve_plugins`], starting from a ref `plan` already resolved.
pub async fn resolve_planned(
    client: &Client,
    spec: &RepoSpec,
    plan: &RefPlan,
    valid_scope_ids: &HashSet<Arc<str>>,
) -> Result<ResolvedRepository, ResolveError> {
    let walk = walk(client, spec, plan.clone(), |spec, plan| {
        open_archive(client.clone(), spec, plan)
    })
    .await?;

    let mut candidates = Vec::with_capacity(walk.entries.len());
    for entry in walk.entries {
        let root = walk.trees[entry.tree].root();
        let source_dir = match entry.dir.as_str() {
            "" => root.to_path_buf(),
            dir => root.join(dir),
        };
        let plugin =
            PluginManager::load_plugin_dir(&source_dir, valid_scope_ids).map_err(|source| {
                ResolveError::Plugin {
                    location: entry.location,
                    source,
                }
            })?;
        candidates.push(PluginCandidate {
            plugin,
            source: entry.source,
            source_dir,
        });
    }
    reject_duplicate_ids(candidates.iter().map(|c| (c.id(), &c.source)))?;

    Ok(ResolvedRepository {
        spec: spec.clone(),
        git_ref: walk.git_ref,
        commit: walk.commit,
        index: walk.index,
        candidates,
        _sources: walk.trees,
    })
}

/// Lists the plugins [`resolve_plugins`] would offer, under the same rules,
/// by reading only `repository.json` and `plugin.json` files at the
/// resolved commit. Manifests are validated on their own, so a missing
/// entrypoint only fails at install. When the forge API cannot resolve a
/// commit, raw file URLs would have to guess at branch names, so that
/// repository is downloaded as an archive instead. Archive extraction
/// skips symlinks while raw endpoints may serve or follow them, so a
/// symlinked manifest can list here and still be missing at install.
pub async fn list_plugins(
    client: &Client,
    spec: &RepoSpec,
    valid_scope_ids: &HashSet<Arc<str>>,
) -> Result<RepositoryListing, ResolveError> {
    let plan = plan_refs(client, spec).await;
    let walk = walk(client, spec, plan, |spec, plan| {
        open_listed(client.clone(), spec, plan)
    })
    .await?;

    let mut plugins = Vec::with_capacity(walk.entries.len());
    for entry in walk.entries {
        let label = PathBuf::from(format!("{}/{PLUGIN_CONFIG_FILENAME}", entry.location));
        let loaded = PluginManager::load_manifest(&label, &entry.config, valid_scope_ids).map_err(
            |source| ResolveError::Plugin {
                location: entry.location,
                source,
            },
        )?;
        plugins.push(ListedPlugin {
            loaded,
            source: entry.source,
        });
    }
    reject_duplicate_ids(plugins.iter().map(|p| (p.id(), &p.source)))?;

    Ok(RepositoryListing {
        spec: spec.clone(),
        git_ref: walk.git_ref,
        commit: walk.commit,
        index: walk.index,
        plugins,
    })
}

/// Opens a repository as the extracted archive installs copy from.
async fn open_archive(
    client: Client,
    spec: RepoSpec,
    plan: RefPlan,
) -> Result<Opened<FetchedRepo>, ResolveError> {
    let fetched = fetch_planned(&client, &spec, &plan).await?;
    Ok(Opened {
        files: Files::Archive {
            root: fetched.root().to_path_buf(),
            repo: spec.human_id(),
        },
        git_ref: fetched.git_ref.clone(),
        commit: fetched.commit.clone(),
        pinned: fetched.pinned,
        tree: fetched,
    })
}

/// Opens a repository for listing: raw files when the forge API resolved
/// a commit, otherwise the archive.
async fn open_listed(
    client: Client,
    spec: RepoSpec,
    plan: RefPlan,
) -> Result<Opened<Option<FetchedRepo>>, ResolveError> {
    let Some((git_ref, commit)) = plan.resolved() else {
        return Ok(open_archive(client, spec, plan).await?.map_tree(Some));
    };
    Ok(Opened {
        git_ref: git_ref.to_string(),
        commit: Some(commit.to_string()),
        pinned: plan.pinned(),
        files: Files::Raw {
            commit: commit.to_string(),
            client,
            spec,
        },
        tree: None,
    })
}

/// Where a walk reads one repository's files from.
enum Files {
    /// An extracted archive; `repo` names it in errors.
    Archive { root: PathBuf, repo: String },
    /// Raw files at a commit the forge API resolved.
    Raw {
        client: Client,
        spec: RepoSpec,
        commit: String,
    },
}

impl Files {
    /// The file at `path`, relative to the repository root, if present.
    async fn read(&self, path: &str) -> Result<Option<Vec<u8>>, ResolveError> {
        match self {
            Files::Archive { root, repo } => {
                let file = root.join(path);
                if !file.is_file() {
                    return Ok(None);
                }
                if std::fs::metadata(&file)?.len() > MAX_FILE_BYTES {
                    return Err(FetchError::FileTooLarge {
                        location: format!("{repo}/{path}"),
                        limit: MAX_FILE_BYTES,
                    }
                    .into());
                }
                Ok(Some(std::fs::read(file)?))
            }
            Files::Raw {
                client,
                spec,
                commit,
            } => Ok(fetch_file(client, spec, commit, path).await?),
        }
    }

    /// Reads a directory's `repository.json` and `plugin.json` together.
    async fn read_manifests(&self, dir: &str) -> (FileRead, FileRead) {
        let (index, config) = match dir {
            "" => (
                REPOSITORY_MANIFEST_FILENAME.to_string(),
                PLUGIN_CONFIG_FILENAME.to_string(),
            ),
            dir => (
                format!("{dir}/{REPOSITORY_MANIFEST_FILENAME}"),
                format!("{dir}/{PLUGIN_CONFIG_FILENAME}"),
            ),
        };
        futures_util::join!(self.read(&index), self.read(&config))
    }
}

type FileRead = Result<Option<Vec<u8>>, ResolveError>;

/// A repository opened for a walk: `tree` is whatever the caller keeps
/// alive, `files` where its manifests are read.
struct Opened<T> {
    tree: T,
    files: Files,
    git_ref: String,
    commit: Option<String>,
    pinned: bool,
}

impl<T> Opened<T> {
    fn map_tree<U>(self, f: impl FnOnce(T) -> U) -> Opened<U> {
        Opened {
            tree: f(self.tree),
            files: self.files,
            git_ref: self.git_ref,
            commit: self.commit,
            pinned: self.pinned,
        }
    }
}

/// A plugin a walk found, before it is validated.
struct FoundEntry {
    /// Index into [`Walk::trees`].
    tree: usize,
    /// Directory within the tree; empty for the root.
    dir: String,
    config: Vec<u8>,
    location: String,
    source: SourceRecord,
}

struct Walk<T> {
    git_ref: String,
    commit: Option<String>,
    index: Option<RepositoryManifest>,
    /// Every tree opened, the origin first.
    trees: Vec<T>,
    entries: Vec<FoundEntry>,
}

/// An index entry, with a path entry's manifests read.
enum Probe<'a> {
    Path {
        path: &'a str,
        nested: FileRead,
        config: FileRead,
    },
    Remote(&'a RepoSpec),
}

/// Reads a path entry's manifests; remote entries are opened by the walk
/// itself, one at a time.
async fn probe<'a>(files: &Files, entry: &'a RepositoryEntry) -> Probe<'a> {
    match entry {
        RepositoryEntry::Path { path } => {
            let (nested, config) = files.read_manifests(path).await;
            Probe::Path {
                path,
                nested,
                config,
            }
        }
        RepositoryEntry::Remote { spec } => Probe::Remote(spec),
    }
}

/// The repository rules shared by installs and listings: a root holds
/// exactly one of `plugin.json` and `repository.json`, and index entries
/// hold a plugin, never another index. `open` opens the origin and each
/// remote entry's repository.
async fn walk<T, Fut>(
    client: &Client,
    spec: &RepoSpec,
    plan: RefPlan,
    open: impl Fn(RepoSpec, RefPlan) -> Fut,
) -> Result<Walk<T>, ResolveError>
where
    Fut: Future<Output = Result<Opened<T>, ResolveError>>,
{
    let Opened {
        tree,
        files,
        git_ref,
        commit,
        pinned,
    } = open(spec.clone(), plan).await?;
    let (index_file, config) = files.read_manifests("").await;
    let (config, index_file) = (config?, index_file?);

    let origin_source = |subpath| source_record(spec, &commit, pinned, subpath, None);
    let mut entries = Vec::new();
    let mut trees = vec![tree];
    let mut index = None;

    match (config, index_file) {
        (Some(_), Some(_)) => {
            return Err(ResolveError::AmbiguousRoot {
                repo: spec.human_id(),
            });
        }
        (None, None) => {
            return Err(ResolveError::NotAPluginRepository {
                repo: spec.human_id(),
            });
        }
        (Some(config), None) => {
            entries.push(FoundEntry {
                tree: 0,
                dir: String::new(),
                config,
                location: spec.human_id(),
                source: origin_source(None),
            });
        }
        (None, Some(index_file)) => {
            let json = String::from_utf8(index_file).map_err(|e| ManifestError::Parse {
                reason: e.to_string(),
            })?;
            let manifest = RepositoryManifest::parse(&json, Some(spec))?;

            // Path entries read ahead in order; dropping the stream on an
            // early return cancels reads still in flight.
            // Indices rather than `&RepositoryEntry` items keep the closure
            // free of higher-ranked lifetimes, which axum's `Send` bound
            // on handler futures cannot see through.
            let entries_in_order = &manifest.entries;
            let mut probes = stream::iter(0..entries_in_order.len())
                .map(|i| probe(&files, &entries_in_order[i]))
                .buffered(MAX_CONCURRENT_ENTRIES);

            while let Some(probe) = probes.next().await {
                match probe {
                    Probe::Path {
                        path,
                        nested,
                        config,
                    } => {
                        let location = format!("{}/{path}", spec.human_id());

                        if nested?.is_some() {
                            return Err(ResolveError::NestedRepository { target: location });
                        }
                        let Some(config) = config? else {
                            return Err(ResolveError::PathEntryMissing {
                                repo: spec.human_id(),
                                path: path.to_string(),
                            });
                        };

                        entries.push(FoundEntry {
                            tree: 0,
                            dir: path.to_string(),
                            config,
                            location,
                            source: origin_source(Some(path.to_string())),
                        });
                    }
                    Probe::Remote(remote) => {
                        let sub = open(remote.clone(), plan_refs(client, remote).await).await?;
                        let (nested, config) = sub.files.read_manifests("").await;

                        if nested?.is_some() {
                            return Err(ResolveError::NestedRepository {
                                target: remote.human_id(),
                            });
                        }
                        let Some(config) = config? else {
                            return Err(ResolveError::NotAPluginRepository {
                                repo: remote.human_id(),
                            });
                        };

                        let source = source_record(
                            remote,
                            &sub.commit,
                            sub.pinned,
                            None,
                            Some(spec.canonical_url()),
                        );
                        trees.push(sub.tree);
                        entries.push(FoundEntry {
                            tree: trees.len() - 1,
                            dir: String::new(),
                            config,
                            location: remote.human_id(),
                            source,
                        });
                    }
                }
            }
            drop(probes);

            index = Some(manifest);
        }
    }

    Ok(Walk {
        git_ref,
        commit,
        index,
        trees,
        entries,
    })
}

/// Ids that differ only by case collide on case-insensitive filesystems,
/// so they count as duplicates too.
fn reject_duplicate_ids<'a>(
    plugins: impl Iterator<Item = (&'a str, &'a SourceRecord)>,
) -> Result<(), ResolveError> {
    let plugins: Vec<_> = plugins.collect();
    for (i, (first_id, first)) in plugins.iter().enumerate() {
        for (second_id, second) in plugins.iter().skip(i + 1) {
            if first_id.eq_ignore_ascii_case(second_id) {
                return Err(ResolveError::DuplicatePluginId {
                    first: first.location(),
                    second: second.location(),
                });
            }
        }
    }
    Ok(())
}

fn source_record(
    spec: &RepoSpec,
    commit: &Option<String>,
    pinned: bool,
    subpath: Option<String>,
    via_repository: Option<String>,
) -> SourceRecord {
    SourceRecord {
        schema_version: SOURCE_RECORD_SCHEMA_VERSION,
        origin: spec.canonical_url(),
        forge: spec.forge,
        git_ref: spec.explicit_ref.clone(),
        commit: commit.clone(),
        pinned,
        subpath,
        via_repository,
        installed_at: None,
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
        plugin_json,
        raw_repo_routes,
        single_plugin_routes,
        targz,
        targz_entries,
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
        assert!(!candidate.source.pinned);
        assert!(candidate.source_dir().join("init.luau").is_file());
    }

    #[tokio::test]
    async fn resolves_catalog_with_documentation_symlink_but_requires_real_entrypoint() {
        for linked_entrypoint in [false, true] {
            let mut archive = targz_entries();
            archive.file("repo/AGENTS.md", "Repository documentation");
            archive.symlink("repo/CLAUDE.md", "AGENTS.md");
            archive.file(
                "repo/repository.json",
                r#"{
                "schema_version": 1,
                "name": "Plugins",
                "plugins": [{ "path": "plugins/foo" }]
            }"#,
            );
            archive.file("repo/plugins/foo/plugin.json", &plugin_json("foo"));
            if linked_entrypoint {
                archive.file("repo/shared.luau", "return {}");
                archive.symlink("repo/plugins/foo/init.luau", "../../shared.luau");
            } else {
                archive.file("repo/plugins/foo/init.luau", "return {}");
            }
            let server = TestServer::start(HashMap::from([(
                "/owner/repo/archive/main.tar.gz".to_string(),
                CannedResponse::targz(archive.finish()),
            )]))
            .await;

            let result = resolve(&server, "/owner/repo").await;
            if linked_entrypoint {
                assert!(
                    matches!(
                        result,
                        Err(ResolveError::Plugin {
                            source: PluginLoadError::EntrypointNotFound { .. },
                            ..
                        })
                    ),
                    "{result:?}"
                );
            } else {
                let resolved = result.unwrap();
                assert_eq!(resolved.index.as_ref().unwrap().name, "Plugins");
                assert_eq!(resolved.candidates.len(), 1);
                assert_eq!(resolved.candidates[0].id(), "foo");
            }
        }
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

    async fn list(server: &TestServer, path: &str) -> Result<RepositoryListing, ResolveError> {
        let spec = RepoSpec::parse(&format!("{}{path}", server.url()), None).unwrap();
        list_plugins(&client(), &spec, &HashSet::new()).await
    }

    async fn list_routes(
        routes: impl FnOnce(&str) -> Vec<(String, CannedResponse)>,
    ) -> Result<RepositoryListing, ResolveError> {
        let server = TestServer::start_with(|base| routes(base).into_iter().collect()).await;
        list(&server, "/owner/repo").await
    }

    fn index_json(entries: &str) -> String {
        format!(r#"{{ "schema_version": 1, "name": "Test Plugins", "plugins": [{entries}] }}"#)
    }

    #[tokio::test]
    async fn lists_single_plugins_from_raw_manifests_alone() {
        let sha = "a".repeat(40);
        let server = TestServer::start(
            raw_repo_routes("owner/repo", &sha, &[("plugin.json", &plugin_json("foo"))])
                .into_iter()
                .collect(),
        )
        .await;

        let listing = list(&server, "/owner/repo").await.unwrap();

        assert!(listing.index.is_none());
        assert_eq!(listing.git_ref, "main");
        assert_eq!(listing.commit.as_deref(), Some(sha.as_str()));
        assert_eq!(listing.plugins.len(), 1);
        let plugin = &listing.plugins[0];
        assert_eq!(plugin.id(), "foo");
        assert_eq!(plugin.loaded.manifest.version, "0.0.1");
        assert_eq!(plugin.source.origin, format!("{}/owner/repo", server.url()));
        assert_eq!(plugin.source.commit.as_deref(), Some(sha.as_str()));
        assert_eq!(plugin.source.subpath, None);
        assert_eq!(plugin.source.via_repository, None);
        assert!(!plugin.source.pinned);
    }

    #[tokio::test]
    async fn lists_index_path_and_remote_entries_from_raw_manifests() {
        let (sha, other_sha) = ("a".repeat(40), "b".repeat(40));
        let listing = list_routes(|base| {
            let index = index_json(&format!(
                r#"{{ "path": "plugins/foo" }}, {{ "url": "{base}/other/single" }}"#
            ));
            let mut routes = raw_repo_routes(
                "owner/repo",
                &sha,
                &[
                    ("repository.json", &index),
                    ("plugins/foo/plugin.json", &plugin_json("foo")),
                ],
            );
            routes.extend(raw_repo_routes(
                "other/single",
                &other_sha,
                &[("plugin.json", &plugin_json("bar"))],
            ));
            routes
        })
        .await
        .unwrap();

        assert_eq!(listing.index.as_ref().unwrap().name, "Test Plugins");
        assert_eq!(listing.commit.as_deref(), Some(sha.as_str()));
        let [foo, bar] = &listing.plugins[..] else {
            panic!("expected two plugins: {:?}", listing.plugins);
        };
        assert_eq!(foo.id(), "foo");
        assert_eq!(foo.source.subpath.as_deref(), Some("plugins/foo"));
        assert_eq!(foo.source.commit.as_deref(), Some(sha.as_str()));
        assert_eq!(bar.id(), "bar");
        assert_eq!(bar.source.commit.as_deref(), Some(other_sha.as_str()));
        assert_eq!(bar.source.subpath, None);
        assert_eq!(
            bar.source.via_repository.as_deref(),
            Some(listing.spec.canonical_url().as_str())
        );
    }

    #[tokio::test]
    async fn listings_record_the_sources_an_install_would() {
        let (sha, other_sha) = ("a".repeat(40), "b".repeat(40));
        let server = TestServer::start_with(|base| {
            let index = index_json(&format!(
                r#"{{ "path": "plugins/foo" }}, {{ "url": "{base}/other/single?ref=v1" }}"#
            ));
            let mut routes = raw_repo_routes(
                "owner/repo",
                &sha,
                &[
                    ("repository.json", &index),
                    ("plugins/foo/plugin.json", &plugin_json("foo")),
                ],
            );
            routes.push((
                format!("/owner/repo/archive/{sha}.tar.gz"),
                CannedResponse::targz(targz(&[
                    ("repo/repository.json", &index),
                    ("repo/plugins/foo/plugin.json", &plugin_json("foo")),
                    ("repo/plugins/foo/init.luau", "return {}"),
                ])),
            ));
            routes.extend([
                (
                    "/api/v1/repos/other/single/git/commits/v1".to_string(),
                    CannedResponse::json(&format!(r#"{{"sha":"{other_sha}"}}"#)),
                ),
                (
                    format!("/other/single/raw/commit/{other_sha}/plugin.json"),
                    CannedResponse::json(&plugin_json("bar")),
                ),
                (
                    format!("/other/single/archive/{other_sha}.tar.gz"),
                    CannedResponse::targz(targz(&[
                        ("repo/plugin.json", &plugin_json("bar")),
                        ("repo/init.luau", "return {}"),
                    ])),
                ),
            ]);
            routes.into_iter().collect()
        })
        .await;

        let listing = list(&server, "/owner/repo").await.unwrap();
        let resolved = resolve(&server, "/owner/repo").await.unwrap();

        assert_eq!(listing.git_ref, resolved.git_ref);
        assert_eq!(listing.commit, resolved.commit);
        let listed: Vec<_> = listing
            .plugins
            .iter()
            .map(|p| (p.id(), &p.source))
            .collect();
        let installed: Vec<_> = resolved
            .candidates
            .iter()
            .map(|c| (c.id(), &c.source))
            .collect();
        assert_eq!(listed, installed);
        assert!(listing.plugins[1].source.pinned);
    }

    #[tokio::test]
    async fn listings_enforce_the_repository_rules() {
        let sha = "a".repeat(40);
        let other = "b".repeat(40);

        let err = list_routes(|_| {
            raw_repo_routes(
                "owner/repo",
                &sha,
                &[
                    ("plugin.json", &plugin_json("foo")),
                    ("repository.json", &index_json("")),
                ],
            )
        })
        .await
        .unwrap_err();
        assert!(matches!(err, ResolveError::AmbiguousRoot { .. }), "{err}");

        let err = list_routes(|_| raw_repo_routes("owner/repo", &sha, &[]))
            .await
            .unwrap_err();
        assert!(
            matches!(err, ResolveError::NotAPluginRepository { .. }),
            "{err}"
        );

        let err = list_routes(|_| {
            raw_repo_routes(
                "owner/repo",
                &sha,
                &[("repository.json", &index_json(r#"{ "path": "missing" }"#))],
            )
        })
        .await
        .unwrap_err();
        assert!(
            matches!(&err, ResolveError::PathEntryMissing { path, .. } if path == "missing"),
            "{err}"
        );

        let err = list_routes(|_| {
            raw_repo_routes(
                "owner/repo",
                &sha,
                &[
                    ("repository.json", &index_json(r#"{ "path": "sub" }"#)),
                    ("sub/plugin.json", &plugin_json("foo")),
                    ("sub/repository.json", &index_json("")),
                ],
            )
        })
        .await
        .unwrap_err();
        assert!(
            matches!(err, ResolveError::NestedRepository { .. }),
            "{err}"
        );

        let err = list_routes(|base| {
            let index = index_json(&format!(r#"{{ "url": "{base}/other/nested" }}"#));
            let mut routes = raw_repo_routes("owner/repo", &sha, &[("repository.json", &index)]);
            routes.extend(raw_repo_routes(
                "other/nested",
                &other,
                &[("repository.json", &index_json(""))],
            ));
            routes
        })
        .await
        .unwrap_err();
        assert!(
            matches!(&err, ResolveError::NestedRepository { target } if target.ends_with("other/nested")),
            "{err}"
        );

        let err = list_routes(|base| {
            let index = index_json(&format!(r#"{{ "url": "{base}/other/empty" }}"#));
            let mut routes = raw_repo_routes("owner/repo", &sha, &[("repository.json", &index)]);
            routes.extend(raw_repo_routes("other/empty", &other, &[]));
            routes
        })
        .await
        .unwrap_err();
        assert!(
            matches!(&err, ResolveError::NotAPluginRepository { repo } if repo.ends_with("other/empty")),
            "{err}"
        );

        let err = list_routes(|base| {
            let index = index_json(&format!(
                r#"{{ "path": "foo" }}, {{ "url": "{base}/other/single" }}"#
            ));
            let mut routes = raw_repo_routes(
                "owner/repo",
                &sha,
                &[
                    ("repository.json", &index),
                    ("foo/plugin.json", &plugin_json("Foo")),
                ],
            );
            routes.extend(raw_repo_routes(
                "other/single",
                &other,
                &[("plugin.json", &plugin_json("foo"))],
            ));
            routes
        })
        .await
        .unwrap_err();
        assert!(
            matches!(err, ResolveError::DuplicatePluginId { .. }),
            "{err}"
        );
    }

    #[tokio::test]
    async fn listings_validate_manifests_and_fail_on_raw_read_errors() {
        let sha = "a".repeat(40);
        let unknown_scope = plugin_json("foo").replace(r#""scopes": []"#, r#""scopes": ["nope"]"#);

        let err = list_routes(|_| {
            raw_repo_routes("owner/repo", &sha, &[("plugin.json", &unknown_scope)])
        })
        .await
        .unwrap_err();
        assert!(
            matches!(
                err,
                ResolveError::Plugin {
                    source: PluginLoadError::UnknownScope { .. },
                    ..
                }
            ),
            "{err}"
        );

        let err = list_routes(|_| {
            let mut routes = raw_repo_routes("owner/repo", &sha, &[]);
            routes.push((
                format!("/owner/repo/raw/commit/{sha}/plugin.json"),
                CannedResponse::status(500),
            ));
            routes
        })
        .await
        .unwrap_err();
        assert!(
            matches!(
                &err,
                ResolveError::Fetch(FetchError::Status { status, .. })
                    if *status == reqwest::StatusCode::INTERNAL_SERVER_ERROR
            ),
            "{err}"
        );

        let oversized = " ".repeat(crate::fetch::MAX_FILE_BYTES as usize + 1);
        let err =
            list_routes(|_| raw_repo_routes("owner/repo", &sha, &[("plugin.json", &oversized)]))
                .await
                .unwrap_err();
        assert!(
            matches!(err, ResolveError::Fetch(FetchError::FileTooLarge { .. })),
            "{err}"
        );
    }

    #[tokio::test]
    async fn listings_download_archives_when_the_forge_api_has_no_commit() {
        let listing = list_routes(|_| {
            vec![(
                "/owner/repo/archive/main.tar.gz".to_string(),
                CannedResponse::targz(targz(&[("repo/plugin.json", &plugin_json("foo"))])),
            )]
        })
        .await
        .unwrap();

        assert_eq!(listing.git_ref, "main");
        assert_eq!(listing.commit, None);
        assert_eq!(listing.plugins[0].id(), "foo");
        assert_eq!(listing.plugins[0].source.commit, None);
        assert!(!listing.plugins[0].source.pinned);
    }

    #[tokio::test]
    async fn resolving_caps_manifests_read_from_archives() {
        let oversized = " ".repeat(crate::fetch::MAX_FILE_BYTES as usize + 1);
        let server = TestServer::start(HashMap::from([(
            "/owner/repo/archive/main.tar.gz".to_string(),
            CannedResponse::targz(targz(&[("repo/plugin.json", &oversized)])),
        )]))
        .await;

        let err = resolve(&server, "/owner/repo").await.unwrap_err();
        assert!(
            matches!(
                &err,
                ResolveError::Fetch(FetchError::FileTooLarge { location, .. })
                    if location.ends_with("/owner/repo/plugin.json")
            ),
            "{err}"
        );
    }

    #[tokio::test]
    async fn listings_reject_indexes_that_are_not_utf8() {
        let sha = "a".repeat(40);
        let err = list_routes(|_| {
            let mut routes = raw_repo_routes("owner/repo", &sha, &[]);
            routes.push((
                format!("/owner/repo/raw/commit/{sha}/repository.json"),
                CannedResponse::bytes(vec![0xff, 0xfe]),
            ));
            routes
        })
        .await
        .unwrap_err();
        assert!(
            matches!(err, ResolveError::Manifest(ManifestError::Parse { .. })),
            "{err}"
        );
    }
}
