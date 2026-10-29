// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::path::{
    Path,
    PathBuf,
};

use futures_util::stream::{
    self,
    StreamExt,
};
use harmony_core::plugin::{
    LoadedPlugin,
    PLUGIN_CONFIG_FILENAME,
    PluginHost,
    PluginLoadError,
    PluginManager,
    PluginManifest,
    validate_plugin_id,
};
use reqwest::{
    Client,
    StatusCode,
};
use semver::{
    Version,
    VersionReq,
};
use serde::Deserialize;

use crate::fetch::{
    FetchError,
    FetchedDirectory,
    FetchedRepo,
    MAX_FILE_BYTES,
    RefPlan,
    UnpackBudget,
    fetch_directory,
    fetch_file,
    fetch_planned,
    plan_refs,
};
use crate::forge::{
    Forge,
    RepoSpec,
};
use crate::manifest::{
    ManifestError,
    REPOSITORY_MANIFEST_FILENAME,
    RepositoryEntry,
    RepositoryManifest,
    SOURCE_RECORD_SCHEMA_VERSION,
    SourceRecord,
};
use crate::releases::{
    RELEASE_INDEX_FILENAME,
    ReleaseError,
    ReleaseIndex,
};

/// Path entries whose manifests are read at once, to stay polite to forges.
const MAX_CONCURRENT_ENTRIES: usize = 4;

/// Most plugins an install downloads from one commit as single-directory
/// archives; past it the commit's whole archive is downloaded once instead.
/// GitLab rate-limits archive requests per project (anonymous clients share
/// one allowance, believed to be a few a minute), and every archive costs a
/// request and an archive build on the forge.
const MAX_DIRECTORY_ARCHIVES: usize = 3;

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
        source: Box<PluginLoadError>,
    },
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// Why one plugin cannot be installed. It fails on its own; the
/// repository's other plugins still resolve.
#[derive(Debug, thiserror::Error)]
pub enum PluginError {
    #[error(transparent)]
    Load(#[from] PluginLoadError),
    #[error(transparent)]
    Release(#[from] ReleaseError),
    #[error("the plugin at {other} has the same id")]
    DuplicateId { other: String },
}

impl PluginError {
    /// The server requirement that excludes this host, when that is why
    /// the plugin has no offer: the newest release's `lyra` when none
    /// matches, or the release `plugin.json`'s own.
    pub fn required_host(&self) -> Option<&VersionReq> {
        match self {
            PluginError::Release(ReleaseError::Incompatible { requirement, .. })
            | PluginError::Load(PluginLoadError::IncompatibleHost { requirement, .. }) => {
                Some(requirement)
            }
            _ => None,
        }
    }
}

/// Where a listed plugin comes from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PluginSource {
    /// Canonical URL of the repository holding the plugin.
    pub origin: String,
    pub forge: Forge,
    /// The ref its `versions.json` was read at; `None` for the default
    /// branch.
    pub git_ref: Option<String>,
    /// Whether `git_ref` is a tag or commit and so never moves.
    pub pinned: bool,
    /// Directory within `origin` for a repository index's path entry.
    pub subpath: Option<String>,
    /// Canonical URL of the repository whose index referenced the plugin,
    /// when it differs from `origin`.
    pub via_repository: Option<String>,
    /// Index into [`RepositoryListing::trees`].
    tree: usize,
}

impl PluginSource {
    /// The plugin's directory within its origin; empty for the root.
    fn dir(&self) -> &str {
        self.subpath.as_deref().unwrap_or("")
    }

    fn record(&self, version: &Version, commit: &str) -> SourceRecord {
        SourceRecord {
            schema_version: SOURCE_RECORD_SCHEMA_VERSION,
            origin: self.origin.clone(),
            forge: self.forge,
            git_ref: self.git_ref.clone(),
            version: version.clone(),
            commit: commit.to_string(),
            pinned: self.pinned,
            subpath: self.subpath.clone(),
            via_repository: self.via_repository.clone(),
            installed_at: None,
            installed_automatically: false,
        }
    }
}

/// The release of a listed plugin this host would install.
#[derive(Debug, Clone)]
pub struct Offer {
    /// The `plugin.json` at `commit`, checked against the host. Its
    /// version is the release's.
    pub manifest: PluginManifest,
    pub commit: String,
}

/// A plugin offered by a repository, known from the repository's
/// `versions.json` at the listed ref and its `plugin.json` at the release
/// commit that chose.
#[derive(Debug)]
pub struct ListedPlugin {
    pub id: String,
    /// Where the plugin is, as errors name it.
    pub location: String,
    pub source: PluginSource,
    /// The release to install, or why there is none.
    pub offer: Result<Offer, PluginError>,
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
    /// Every repository read, the origin first.
    trees: Vec<RepoSpec>,
}

/// An installable plugin discovered by [`resolve_listed`]. The source tree
/// lives in the owning [`ResolvedRepository`]'s temporary storage.
#[derive(Debug)]
pub struct PluginCandidate {
    pub plugin: LoadedPlugin,
    /// Provenance to record at install time; `installed_at` and
    /// `installed_automatically`
    /// are unset.
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

/// The selected plugins of one listing, downloaded at their releases and
/// kept on disk until this value drops.
#[derive(Debug)]
pub struct ResolvedRepository {
    /// In listing order.
    pub candidates: Vec<PluginCandidate>,
    /// Selected plugins without an installable release.
    pub rejected: Vec<RejectedPlugin>,
    _downloads: Downloads,
}

/// A selected plugin without an installable release, such as one whose
/// repository's releases all require another server version.
#[derive(Debug)]
pub struct RejectedPlugin {
    pub id: String,
    pub location: String,
    pub error: PluginError,
    pub source: PluginSource,
}

/// Lists a repository URL and downloads its plugins' releases; see
/// [`list_plugins`] and [`resolve_listed`].
pub async fn resolve_plugins(
    client: &Client,
    spec: &RepoSpec,
    selected: Option<&[String]>,
    host: &PluginHost,
) -> Result<ResolvedRepository, ResolveError> {
    let listing = list_plugins(client, spec, host).await?;
    resolve_listed(client, listing, selected, host, &UnpackBudget::new()).await
}

/// Lists every plugin a repository URL offers, with the release this host
/// would install, by reading only manifest files: the root `versions.json`
/// at the resolved commit, then `repository.json` and `plugin.json` files
/// at the release commit it selects.
///
/// A root `plugin.json` is a single plugin. A root `repository.json` is an
/// index: path entries list plugins in the same tree, remote entries in the
/// referenced repository, which must be a single plugin — never another
/// index — with a `versions.json` of its own.
///
/// A plugin's `plugin.json` at the release commit must load on this host;
/// a plugin failing that, or whose repository has no release for it, is
/// listed with the error instead of failing the repository. When the forge
/// API cannot resolve a commit, raw file URLs would have to guess at branch
/// names, so that repository's `versions.json` is read from its archive;
/// releases are always read at their commit.
pub async fn list_plugins(
    client: &Client,
    spec: &RepoSpec,
    host: &PluginHost,
) -> Result<RepositoryListing, ResolveError> {
    let plan = plan_refs(client, spec).await?;
    let mut walk = walk(client, spec, plan, host, &UnpackBudget::new()).await?;
    reject_duplicate_ids(&mut walk.plugins);

    Ok(RepositoryListing {
        spec: spec.clone(),
        git_ref: walk.git_ref,
        commit: walk.commit,
        index: walk.index,
        plugins: walk.plugins,
        trees: walk.trees,
    })
}

/// Downloads the plugins `selected` names (all of them when `None`) at their
/// repositories' release commits, within `budget`, and validates them as
/// plugin directories. A plugin failing that, or without a release, is
/// returned as rejected instead of failing the repository. A release whose
/// commit or directory the forge does not have is rejected the same way.
pub async fn resolve_listed(
    client: &Client,
    listing: RepositoryListing,
    selected: Option<&[String]>,
    host: &PluginHost,
    budget: &UnpackBudget,
) -> Result<ResolvedRepository, ResolveError> {
    let mut rejected = Vec::new();
    let mut offered = Vec::new();
    for plugin in listing.plugins {
        if selected.is_some_and(|ids| !ids.contains(&plugin.id)) {
            continue;
        }
        let ListedPlugin {
            id,
            location,
            source,
            offer,
        } = plugin;
        match offer {
            Ok(offer) => offered.push((id, location, source, offer)),
            Err(error) => rejected.push(RejectedPlugin {
                id,
                location,
                error,
                source,
            }),
        }
    }

    let mut downloads = Downloads::new(offered.iter().map(|(_, _, source, _)| source.tree));
    let mut candidates = Vec::new();
    for (id, location, source, Offer { manifest, commit }) in offered {
        let version = manifest.version;
        let reject = |error: PluginError| RejectedPlugin {
            id: id.clone(),
            location: location.clone(),
            error,
            source: source.clone(),
        };
        let Some(source_dir) = downloads
            .source_dir(client, &listing.trees, &source, &commit, budget)
            .await?
        else {
            rejected.push(reject(ReleaseError::Unavailable { commit }.into()));
            continue;
        };
        let loaded = match PluginManager::load_plugin_dir(&source_dir, host) {
            Ok(loaded) => loaded,
            Err(error) => {
                rejected.push(reject(error.into()));
                continue;
            }
        };
        candidates.push(PluginCandidate {
            plugin: loaded,
            source: source.record(&version, &commit),
            source_dir,
        });
    }

    Ok(ResolvedRepository {
        candidates,
        rejected,
        _downloads: downloads,
    })
}

/// What an install downloads, each archive at most once: a plugin's own
/// directory at its repository's release commit while at most
/// [`MAX_DIRECTORY_ARCHIVES`] plugins are selected from that repository,
/// otherwise the commit's whole archive, which they share. Forges without
/// single-directory archives always download the whole archive.
#[derive(Debug)]
struct Downloads {
    /// Selected plugins per tree.
    selected: HashMap<usize, usize>,
    /// Keyed by tree and the directory downloaded, empty for the whole
    /// tree; `None` when the forge has no such commit or directory.
    fetched: HashMap<(usize, String), Option<FetchedDirectory>>,
}

impl Downloads {
    fn new(selected: impl Iterator<Item = usize>) -> Self {
        let mut counts = HashMap::new();
        for tree in selected {
            *counts.entry(tree).or_default() += 1;
        }
        Self {
            selected: counts,
            fetched: HashMap::new(),
        }
    }

    /// Where the plugin directory of `source` at `commit` is on disk,
    /// downloading it first; `None` when the forge does not have it.
    async fn source_dir(
        &mut self,
        client: &Client,
        trees: &[RepoSpec],
        source: &PluginSource,
        commit: &str,
        budget: &UnpackBudget,
    ) -> Result<Option<PathBuf>, FetchError> {
        let spec = &trees[source.tree];
        let whole =
            !spec.has_directory_archives() || self.selected[&source.tree] > MAX_DIRECTORY_ARCHIVES;
        let dir = if whole { "" } else { source.dir() };
        let fetched = match self.fetched.entry((source.tree, dir.to_string())) {
            Entry::Occupied(slot) => slot.into_mut(),
            Entry::Vacant(slot) => {
                let fetched = match fetch_directory(client, spec, commit, dir, budget).await {
                    Ok(fetched) => Some(fetched),
                    Err(FetchError::Status { status, .. }) if status == StatusCode::NOT_FOUND => {
                        None
                    }
                    Err(FetchError::DirectoryMissing { .. }) => None,
                    Err(error) => return Err(error),
                };
                slot.insert(fetched)
            }
        };
        Ok(fetched.as_ref().map(|fetched| {
            if whole {
                tree_dir(fetched.path(), source.dir())
            } else {
                fetched.path().to_path_buf()
            }
        }))
    }
}

/// The directory `dir` of a tree extracted at `root`; empty names the root.
fn tree_dir(root: &Path, dir: &str) -> PathBuf {
    match dir {
        "" => root.to_path_buf(),
        dir => root.join(dir),
    }
}

/// A plugin's id with its offer, or why it has none.
type Described = (String, Result<Offer, PluginError>);

/// Names the plugin a `plugin.json` describes and makes its offer at the
/// repository's `release` commit, checked against `host`. Without a
/// release, the `plugin.json` was read at the listed ref and only names
/// the plugin. A plugin without a readable, valid id cannot be addressed,
/// and the whole repository fails.
fn describe(
    config: &[u8],
    release: &Result<String, ReleaseError>,
    location: &str,
    host: &PluginHost,
) -> Result<Described, ResolveError> {
    let label = PathBuf::from(format!("{location}/{PLUGIN_CONFIG_FILENAME}"));
    let loaded = PluginManager::load_manifest(&label, config, host);
    let Some(id) = plugin_id(config) else {
        let Err(source) = loaded else {
            unreachable!("a plugin.json that loads has a valid id");
        };
        return Err(ResolveError::Plugin {
            location: location.to_string(),
            source: Box::new(source),
        });
    };
    let offer = match release {
        Err(error) => Err(error.clone().into()),
        Ok(commit) => loaded
            .map(|loaded| Offer {
                manifest: loaded.manifest,
                commit: commit.clone(),
            })
            .map_err(PluginError::from),
    };
    Ok((id, offer))
}

/// The id a `plugin.json` names, when it reads as a valid one, even if the
/// rest does not parse.
fn plugin_id(config: &[u8]) -> Option<String> {
    #[derive(Deserialize)]
    struct Identity {
        id: String,
    }

    let Identity { id } = serde_json::from_slice(config).ok()?;
    validate_plugin_id(&id).is_ok().then_some(id)
}

/// Opens a repository at its listed ref, reads its `versions.json` there,
/// and reads its root manifests at the release that selects for `host`.
/// Without a release, or when the release commit holds no plugin root, the
/// root manifests are read at the listed ref instead, to name the plugins
/// that fail with the reason.
async fn open(
    client: &Client,
    spec: &RepoSpec,
    plan: RefPlan,
    host: &PluginHost,
    budget: &UnpackBudget,
) -> Result<Opened, ResolveError> {
    let Tip {
        files: tip,
        git_ref,
        commit,
        pinned,
    } = open_tip(client, spec, plan, budget).await?;
    let release = match tip.read(RELEASE_INDEX_FILENAME).await? {
        None => Err(ReleaseError::Missing),
        Some(releases) => ReleaseIndex::parse(&releases)
            .and_then(|index| Ok(index.select(&host.version)?.commit.clone())),
    };
    if let Ok(release_commit) = &release {
        let files = Files::Raw {
            client: client.clone(),
            spec: spec.clone(),
            commit: release_commit.clone(),
        };
        let (index, config) = files.read_manifests("").await;
        let (index, config) = (index?, config?);
        if index.is_some() || config.is_some() {
            return Ok(Opened {
                files,
                release,
                index,
                config,
                git_ref,
                commit,
                pinned,
            });
        }
    }
    let release = release.and_then(|commit| Err(ReleaseError::Unavailable { commit }));
    let (index, config) = tip.read_manifests("").await;
    Ok(Opened {
        files: tip,
        release,
        index: index?,
        config: config?,
        git_ref,
        commit,
        pinned,
    })
}

/// A repository at its listed ref.
struct Tip {
    files: Files,
    git_ref: String,
    commit: Option<String>,
    pinned: bool,
}

/// Opens a repository at its listed ref: raw files when the forge API
/// resolved a commit, otherwise the archive.
async fn open_tip(
    client: &Client,
    spec: &RepoSpec,
    plan: RefPlan,
    budget: &UnpackBudget,
) -> Result<Tip, ResolveError> {
    if let Some((git_ref, commit)) = plan.resolved() {
        return Ok(Tip {
            files: Files::Raw {
                client: client.clone(),
                spec: spec.clone(),
                commit: commit.to_string(),
            },
            git_ref: git_ref.to_string(),
            commit: Some(commit.to_string()),
            pinned: plan.pinned(),
        });
    }
    let fetched = fetch_planned(client, spec, &plan, budget).await?;
    Ok(Tip {
        git_ref: fetched.git_ref.clone(),
        commit: fetched.commit.clone(),
        pinned: fetched.pinned,
        files: Files::Archive {
            repo: spec.human_id(),
            tree: fetched,
        },
    })
}

/// Where a walk reads one repository's files from.
enum Files {
    /// An extracted archive; `repo` names it in errors.
    Archive { tree: FetchedRepo, repo: String },
    /// Raw files at a commit.
    Raw {
        client: Client,
        spec: RepoSpec,
        commit: String,
    },
}

impl Files {
    /// The file at `path`, relative to the repository root, if present.
    async fn read(&self, path: &str) -> FileRead {
        match self {
            Files::Archive { tree, repo } => {
                let file = tree.root().join(path);
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
        let (index, config) = (
            dir_file(dir, REPOSITORY_MANIFEST_FILENAME),
            dir_file(dir, PLUGIN_CONFIG_FILENAME),
        );
        futures_util::join!(self.read(&index), self.read(&config))
    }
}

/// The path of `file` in the directory `dir`; empty names the root.
fn dir_file(dir: &str, file: &str) -> String {
    match dir {
        "" => file.to_string(),
        dir => format!("{dir}/{file}"),
    }
}

type FileRead = Result<Option<Vec<u8>>, ResolveError>;

/// A repository opened for a walk, with its root manifests read.
struct Opened {
    /// At the release commit, or at the listed ref without a release.
    files: Files,
    /// The release commit, or why there is none.
    release: Result<String, ReleaseError>,
    index: Option<Vec<u8>>,
    config: Option<Vec<u8>>,
    /// The listed ref and the commit it resolved to.
    git_ref: String,
    commit: Option<String>,
    pinned: bool,
}

struct Walk {
    git_ref: String,
    commit: Option<String>,
    index: Option<RepositoryManifest>,
    /// Every repository opened, the origin first.
    trees: Vec<RepoSpec>,
    plugins: Vec<ListedPlugin>,
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

/// The repository rules: a root holds exactly one of `plugin.json` and
/// `repository.json`, and index entries hold a plugin, never another index.
/// Each repository's release commit is chosen by its own `versions.json`.
/// The origin and each remote entry's repository are opened within
/// `budget`.
async fn walk(
    client: &Client,
    spec: &RepoSpec,
    plan: RefPlan,
    host: &PluginHost,
    budget: &UnpackBudget,
) -> Result<Walk, ResolveError> {
    let Opened {
        files,
        release,
        index: index_file,
        config,
        git_ref,
        commit,
        pinned,
    } = open(client, spec, plan, host, budget).await?;

    let source = |spec: &RepoSpec, pinned, subpath, via_repository, tree| PluginSource {
        origin: spec.canonical_url(),
        forge: spec.forge,
        git_ref: spec.explicit_ref.clone(),
        pinned,
        subpath,
        via_repository,
        tree,
    };
    let mut plugins = Vec::new();
    let mut trees = vec![spec.clone()];
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
            let location = spec.human_id();
            let (id, offer) = describe(&config, &release, &location, host)?;
            plugins.push(ListedPlugin {
                id,
                location,
                source: source(spec, pinned, None, None, 0),
                offer,
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
                        let (id, offer) = describe(&config, &release, &location, host)?;
                        plugins.push(ListedPlugin {
                            id,
                            location,
                            source: source(spec, pinned, Some(path.to_string()), None, 0),
                            offer,
                        });
                    }
                    Probe::Remote(remote) => {
                        let plan = plan_refs(client, remote).await?;
                        // Without a release of the index, its remote entries
                        // are not offered either: their tip only names them.
                        let (nested, config, pinned, remote_release) = match &release {
                            Ok(_) => {
                                let sub = open(client, remote, plan, host, budget).await?;
                                (sub.index, sub.config, sub.pinned, sub.release)
                            }
                            Err(error) => {
                                let tip = open_tip(client, remote, plan, budget).await?;
                                let (nested, config) = tip.files.read_manifests("").await;
                                (nested?, config?, tip.pinned, Err(error.clone()))
                            }
                        };
                        if nested.is_some() {
                            return Err(ResolveError::NestedRepository {
                                target: remote.human_id(),
                            });
                        }
                        let Some(config) = config else {
                            return Err(ResolveError::NotAPluginRepository {
                                repo: remote.human_id(),
                            });
                        };
                        let location = remote.human_id();
                        let (id, offer) = describe(&config, &remote_release, &location, host)?;
                        trees.push(remote.clone());
                        plugins.push(ListedPlugin {
                            id,
                            location,
                            source: source(
                                remote,
                                pinned,
                                None,
                                Some(spec.canonical_url()),
                                trees.len() - 1,
                            ),
                            offer,
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
        plugins,
    })
}

/// Fails every plugin whose id another one shares, each on its own. Ids
/// that differ only by case collide on case-insensitive filesystems, so
/// they count as duplicates too.
fn reject_duplicate_ids(plugins: &mut [ListedPlugin]) {
    let others: Vec<Option<String>> = plugins
        .iter()
        .enumerate()
        .map(|(i, plugin)| {
            plugins
                .iter()
                .enumerate()
                .find(|(j, other)| *j != i && other.id.eq_ignore_ascii_case(&plugin.id))
                .map(|(_, other)| other.location.clone())
        })
        .collect();
    for (plugin, other) in plugins.iter_mut().zip(others) {
        if let Some(other) = other {
            plugin.offer = Err(PluginError::DuplicateId { other });
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
        gitlab_directory_archive,
        gitlab_raw_file,
        gitlab_repo_routes,
        host,
        plugin_json,
        raw_file,
        raw_repo_routes,
        release_commit,
        released,
        single_plugin_routes,
        targz,
        targz_entries,
        versions_json,
    };

    async fn resolve(server: &TestServer, path: &str) -> Result<ResolvedRepository, ResolveError> {
        let spec = RepoSpec::parse(&format!("{}{path}", server.url()), None).unwrap();
        resolve_plugins(&client(), &spec, None, &host()).await
    }

    async fn list(server: &TestServer, path: &str) -> Result<RepositoryListing, ResolveError> {
        let spec = RepoSpec::parse(&format!("{}{path}", server.url()), None).unwrap();
        list_plugins(&client(), &spec, &host()).await
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

    /// The Gitea-style routes of `owner_repo` at `commit`: its `files` as
    /// raw files, and as an archive under one top-level directory.
    fn archive_at(
        owner_repo: &str,
        commit: &str,
        files: &[(&str, &str)],
    ) -> Vec<(String, CannedResponse)> {
        let mut routes: Vec<_> = files
            .iter()
            .map(|(path, content)| raw_file(owner_repo, commit, path, content))
            .collect();
        let files: Vec<(String, &str)> = files
            .iter()
            .map(|(path, content)| (format!("repo/{path}"), *content))
            .collect();
        let files: Vec<(&str, &str)> = files.iter().map(|(p, c)| (p.as_str(), *c)).collect();
        routes.push((
            format!("/{owner_repo}/archive/{commit}.tar.gz"),
            CannedResponse::targz(targz(&files)),
        ));
        routes
    }

    /// Routes for the Gitea-style `owner/repo` whose `main` at `tip` holds
    /// `versions` and `tip_files`, released at [`release_commit`] with
    /// `release_files`.
    fn released_repo(
        tip: &str,
        versions: Option<&str>,
        tip_files: &[(&str, &str)],
        release_files: &[(&str, &str)],
    ) -> Vec<(String, CannedResponse)> {
        let mut files = tip_files.to_vec();
        files.extend(versions.map(|versions| ("versions.json", versions)));
        let mut routes = raw_repo_routes("owner/repo", tip, &files);
        routes.extend(archive_at("owner/repo", &release_commit(), release_files));
        routes
    }

    /// `plugin.json` and `init.luau` of plugin `id` in `dir`.
    fn plugin_files(dir: &str, manifest: &str) -> Vec<(String, String)> {
        vec![
            (format!("{dir}/plugin.json"), manifest.to_string()),
            (format!("{dir}/init.luau"), "return {}".to_string()),
        ]
    }

    fn borrowed(files: &[(String, String)]) -> Vec<(&str, &str)> {
        files
            .iter()
            .map(|(path, content)| (path.as_str(), content.as_str()))
            .collect()
    }

    fn offered(plugin: &ListedPlugin) -> (&str, String, &str) {
        let offer = plugin.offer.as_ref().expect("an offered release");
        (
            plugin.id.as_str(),
            offer.manifest.version.to_string(),
            offer.commit.as_str(),
        )
    }

    fn errors(listing: &RepositoryListing) -> Vec<(&str, String)> {
        listing
            .plugins
            .iter()
            .filter_map(|plugin| {
                let error = plugin.offer.as_ref().err()?;
                Some((plugin.id.as_str(), error.to_string()))
            })
            .collect()
    }

    fn ids(resolved: &ResolvedRepository) -> (Vec<&str>, Vec<&str>) {
        (
            resolved.candidates.iter().map(|c| c.id()).collect(),
            resolved.rejected.iter().map(|r| r.id.as_str()).collect(),
        )
    }

    fn sha() -> String {
        "a".repeat(40)
    }

    #[tokio::test]
    async fn resolves_single_plugins_at_their_release() {
        let server = TestServer::start(
            single_plugin_routes("owner/repo", "foo")
                .into_iter()
                .collect(),
        )
        .await;

        let resolved = resolve(&server, "/owner/repo").await.unwrap();

        assert!(resolved.rejected.is_empty(), "{:?}", resolved.rejected);
        let [candidate] = &resolved.candidates[..] else {
            panic!("expected one candidate");
        };
        assert_eq!(candidate.id(), "foo");
        assert_eq!(
            candidate.source.origin,
            format!("{}/owner/repo", server.url())
        );
        assert_eq!(candidate.source.version, Version::new(0, 0, 1));
        assert_eq!(candidate.source.commit, release_commit());
        assert_eq!(candidate.source.subpath, None);
        assert_eq!(candidate.source.via_repository, None);
        assert!(!candidate.source.pinned);
        assert!(candidate.source_dir().join("init.luau").is_file());
    }

    #[tokio::test]
    async fn the_first_compatible_release_commit_defines_the_repository() {
        let (newer, older) = ("e".repeat(40), release_commit());
        let manifest = |id: &str, version: &str, scopes: &str, description: &str| {
            plugin_json(id)
                .replace(
                    r#""version": "0.0.1""#,
                    &format!(r#""version": "{version}""#),
                )
                .replace(r#""scopes": []"#, &format!(r#""scopes": [{scopes}]"#))
                .replace("test plugin", description)
        };
        let tip_index = index_json(r#"{ "path": "tip-only" }"#);
        let tip_plugin = manifest("tip-only", "9.0.0", r#""harmony.http""#, "the tip");
        let index = index_json(r#"{ "path": "foo" }"#);
        let released_foo = manifest("foo", "0.1.0", "", "the release");
        let routes = released_repo(
            &sha(),
            Some(&versions_json(&[(&newer, "0.1"), (&older, "0.0.1")])),
            &[
                ("repository.json", &tip_index),
                ("tip-only/plugin.json", &tip_plugin),
            ],
            &borrowed(
                &[
                    vec![("repository.json".to_string(), index)],
                    plugin_files("foo", &released_foo),
                ]
                .concat(),
            ),
        );
        let server = TestServer::start(routes.into_iter().collect()).await;

        let listing = list(&server, "/owner/repo").await.unwrap();
        assert_eq!(
            offered(&listing.plugins[0]),
            ("foo", "0.1.0".into(), older.as_str())
        );
        let manifest = &listing.plugins[0].offer.as_ref().unwrap().manifest;
        assert_eq!(
            (manifest.scopes.as_slice(), manifest.description.as_str()),
            (&[][..], "the release"),
        );

        let resolved = resolve(&server, "/owner/repo").await.unwrap();
        let [candidate] = &resolved.candidates[..] else {
            panic!("expected one candidate: {:?}", resolved.rejected);
        };
        assert_eq!(candidate.source.commit, older);
        let tip_reads: Vec<String> = server
            .requests()
            .into_iter()
            .filter(|path| path.contains(&sha()) || path.contains("archive/main"))
            .collect();
        assert_eq!(
            tip_reads,
            vec![format!("/owner/repo/raw/commit/{}/versions.json", sha()); 2],
            "only versions.json is read at the tip"
        );
    }

    #[tokio::test]
    async fn repositories_without_a_release_fail_each_plugin_named_at_the_tip() {
        let index = index_json(r#"{ "path": "foo" }, { "path": "bar" }"#);
        let tip = [
            ("repository.json", index.as_str()),
            ("foo/plugin.json", &plugin_json("foo")),
            ("bar/plugin.json", &plugin_json("bar")),
        ];
        let tip: Vec<(&str, String)> = tip.iter().map(|(p, c)| (*p, c.to_string())).collect();
        let tip: Vec<(&str, &str)> = tip.iter().map(|(p, c)| (*p, c.as_str())).collect();
        let broken = r#"[{ "commit": "abc", "lyra": "*" }]"#;
        let future = versions_json(&[(&"e".repeat(40), ">=0.2"), (&"d".repeat(40), "0.1")]);
        for (versions, expected) in [
            (
                None,
                "no releases: the repository has no versions.json".to_string(),
            ),
            (
                Some("[]"),
                "no releases: versions.json lists none".to_string(),
            ),
            (
                Some(broken),
                "failed to parse versions.json: release #0 has commit \"abc\", which is not a \
                 full 40-character commit id"
                    .to_string(),
            ),
            (
                Some(future.as_str()),
                "no release runs on lyra 0.0.1; the newest needs lyra >=0.2".to_string(),
            ),
        ] {
            let server = TestServer::start(
                released_repo(&sha(), versions, &tip, &[])
                    .into_iter()
                    .collect(),
            )
            .await;

            let listing = list(&server, "/owner/repo").await.unwrap();
            assert_eq!(
                errors(&listing),
                [("foo", expected.clone()), ("bar", expected.clone())],
                "{versions:?}"
            );
            let resolved = resolve(&server, "/owner/repo").await.unwrap();
            assert_eq!(ids(&resolved), (vec![], vec!["foo", "bar"]));
            assert!(
                !server
                    .requests()
                    .iter()
                    .any(|path| path.contains("archive")),
                "{:?}",
                server.requests()
            );
        }
    }

    #[tokio::test]
    async fn release_manifests_must_load_on_the_host() {
        let index = index_json(
            r#"{ "path": "widened" }, { "path": "scoped" }, { "path": "unparsable" },
               { "path": "ok" }"#,
        );
        let widened = plugin_json("widened").replace(r#""lyra": "0.0.1""#, r#""lyra": "0.1""#);
        let scoped =
            plugin_json("scoped").replace(r#""scopes": []"#, r#""scopes": ["no.such.scope"]"#);
        let unparsable =
            plugin_json("unparsable").replace(r#""version": "0.0.1""#, r#""version": "one""#);
        let release = [
            vec![("repository.json".to_string(), index)],
            plugin_files("widened", &widened),
            plugin_files("scoped", &scoped),
            plugin_files("unparsable", &unparsable),
            plugin_files("ok", &plugin_json("ok")),
        ]
        .concat();
        let routes = released_repo(&sha(), Some(&released()), &[], &borrowed(&release));
        let server = TestServer::start(routes.into_iter().collect()).await;

        let listing = list(&server, "/owner/repo").await.unwrap();
        let listed: Vec<_> = listing
            .plugins
            .iter()
            .map(|plugin| (plugin.id.as_str(), plugin.offer.as_ref().err()))
            .collect();
        assert!(
            matches!(
                &listed[..],
                [
                    (
                        "widened",
                        Some(PluginError::Load(PluginLoadError::IncompatibleHost { .. }))
                    ),
                    (
                        "scoped",
                        Some(PluginError::Load(PluginLoadError::UnknownScope { .. }))
                    ),
                    (
                        "unparsable",
                        Some(PluginError::Load(PluginLoadError::ConfigParseError { .. }))
                    ),
                    ("ok", None),
                ]
            ),
            "a release's plugin.json that fails is never offered: {listed:?}"
        );

        let resolved = resolve(&server, "/owner/repo").await.unwrap();
        assert_eq!(
            ids(&resolved),
            (vec!["ok"], vec!["widened", "scoped", "unparsable"])
        );
    }

    #[tokio::test]
    async fn release_plugins_without_a_readable_id_fail_the_repository() {
        let index = index_json(r#"{ "path": "foo" }"#);
        for manifest in ["{", r#"{"id": "not/valid"}"#] {
            let routes = released_repo(
                &sha(),
                Some(&released()),
                &[],
                &[("repository.json", &index), ("foo/plugin.json", manifest)],
            );
            let server = TestServer::start(routes.into_iter().collect()).await;
            let err = list(&server, "/owner/repo").await.unwrap_err();
            assert!(
                matches!(&err, ResolveError::Plugin { location, .. } if location.ends_with("/foo")),
                "{err}"
            );
        }
    }

    #[tokio::test]
    async fn releases_the_forge_does_not_have_fail_each_plugin() {
        let missing = "d".repeat(40);
        let index = index_json(r#"{ "path": "foo" }"#);
        let routes = raw_repo_routes(
            "owner/repo",
            &sha(),
            &[
                ("versions.json", &versions_json(&[(&missing, "*")])),
                ("repository.json", &index),
                ("foo/plugin.json", &plugin_json("foo")),
            ],
        );
        let server = TestServer::start(routes.into_iter().collect()).await;

        let listing = list(&server, "/owner/repo").await.unwrap();
        assert!(
            matches!(
                &listing.plugins[..],
                [ListedPlugin {
                    id,
                    offer: Err(PluginError::Release(ReleaseError::Unavailable { commit })),
                    ..
                }] if id == "foo" && *commit == missing
            ),
            "the plugins are named at the tip: {:?}",
            listing.plugins
        );

        let release = [
            vec![("repository.json".to_string(), index.clone())],
            plugin_files("gone", &plugin_json("gone")),
        ]
        .concat();
        let mut routes = raw_repo_routes("owner/repo", &sha(), &[("versions.json", &released())]);
        routes.extend(
            borrowed(&release)
                .iter()
                .map(|(path, content)| raw_file("owner/repo", &release_commit(), path, content)),
        );
        let index = index_json(r#"{ "path": "gone" }"#);
        routes.push(raw_file(
            "owner/repo",
            &release_commit(),
            "repository.json",
            &index,
        ));
        let server = TestServer::start(routes.into_iter().collect()).await;
        let resolved = resolve(&server, "/owner/repo").await.unwrap();
        assert!(
            matches!(
                &resolved.rejected[..],
                [RejectedPlugin {
                    id,
                    error: PluginError::Release(ReleaseError::Unavailable { .. }),
                    ..
                }] if id == "gone"
            ),
            "a release whose archive the forge lacks fails alone: {:?}",
            resolved.rejected
        );
    }

    #[tokio::test]
    async fn rate_limited_release_downloads_fail_the_install() {
        let mut routes = raw_repo_routes("owner/repo", &sha(), &[("versions.json", &released())]);
        routes.push(raw_file(
            "owner/repo",
            &release_commit(),
            "plugin.json",
            &plugin_json("foo"),
        ));
        routes.push((
            format!("/owner/repo/archive/{}.tar.gz", release_commit()),
            CannedResponse::status(429).with_header("Retry-After", "9"),
        ));
        let server = TestServer::start(routes.into_iter().collect()).await;

        let err = resolve(&server, "/owner/repo").await.unwrap_err();
        assert!(
            matches!(
                &err,
                ResolveError::Fetch(FetchError::RateLimited(limited))
                    if limited.retry_after == Some(std::time::Duration::from_secs(9))
            ),
            "{err}"
        );
    }

    #[tokio::test]
    async fn resolves_catalog_with_documentation_symlink_but_requires_real_entrypoint() {
        let index = index_json(r#"{ "path": "plugins/foo" }"#);
        for linked_entrypoint in [false, true] {
            let mut release = targz_entries();
            release.file("repo/AGENTS.md", "Repository documentation");
            release.symlink("repo/CLAUDE.md", "AGENTS.md");
            release.file("repo/repository.json", &index);
            release.file("repo/plugins/foo/plugin.json", &plugin_json("foo"));
            if linked_entrypoint {
                release.file("repo/shared.luau", "return {}");
                release.symlink("repo/plugins/foo/init.luau", "../../shared.luau");
            } else {
                release.file("repo/plugins/foo/init.luau", "return {}");
            }
            let commit = release_commit();
            let server = TestServer::start(HashMap::from([
                (
                    "/owner/repo/archive/main.tar.gz".to_string(),
                    CannedResponse::targz(targz(&[("repo/versions.json", &released())])),
                ),
                (
                    format!("/owner/repo/archive/{commit}.tar.gz"),
                    CannedResponse::targz(release.finish()),
                ),
                raw_file("owner/repo", &commit, "repository.json", &index),
                raw_file(
                    "owner/repo",
                    &commit,
                    "plugins/foo/plugin.json",
                    &plugin_json("foo"),
                ),
            ]))
            .await;

            let resolved = resolve(&server, "/owner/repo").await.unwrap();
            if linked_entrypoint {
                assert!(resolved.candidates.is_empty());
                assert!(
                    matches!(
                        &resolved.rejected[..],
                        [RejectedPlugin {
                            id,
                            error: PluginError::Load(PluginLoadError::EntrypointNotFound { .. }),
                            ..
                        }] if id == "foo"
                    ),
                    "{:?}",
                    resolved.rejected
                );
            } else {
                assert!(resolved.rejected.is_empty(), "{:?}", resolved.rejected);
                assert_eq!(ids(&resolved), (vec!["foo"], vec![]));
            }
        }
    }

    #[tokio::test]
    async fn resolves_repository_indexes_with_path_and_remote_entries() {
        let server = TestServer::start_with(|base| {
            let index = index_json(&format!(
                r#"{{ "path": "plugins/foo" }}, {{ "url": "{base}/other/single" }}"#
            ));
            let release = [
                vec![("repository.json".to_string(), index)],
                plugin_files("plugins/foo", &plugin_json("foo")),
            ]
            .concat();
            let mut routes = released_repo(&sha(), Some(&released()), &[], &borrowed(&release));
            routes.extend(single_plugin_routes("other/single", "bar"));
            routes.into_iter().collect()
        })
        .await;

        let resolved = resolve(&server, "/owner/repo").await.unwrap();

        let [foo, bar] = &resolved.candidates[..] else {
            panic!("expected two candidates: {:?}", resolved.rejected);
        };
        assert_eq!(foo.id(), "foo");
        assert_eq!(foo.source.subpath.as_deref(), Some("plugins/foo"));
        assert_eq!(foo.source.via_repository, None);
        assert!(foo.source_dir().join("init.luau").is_file());
        assert_eq!(bar.id(), "bar");
        assert_eq!(bar.source.origin, format!("{}/other/single", server.url()));
        assert_eq!(bar.source.subpath, None);
        assert_eq!(
            bar.source.via_repository.as_deref(),
            Some(format!("{}/owner/repo", server.url()).as_str())
        );
    }

    /// Routes for an `owner/repo` index of the url entry `other/single` at
    /// `v1`, released by `index_versions`, whose own `versions.json`
    /// releases plugin `bar` at its own commit.
    fn index_of_remote(base: &str, index_versions: Option<&str>) -> Vec<(String, CannedResponse)> {
        let (other_tip, other_release) = ("b".repeat(40), "d".repeat(40));
        let index = index_json(&format!(r#"{{ "url": "{base}/other/single?ref=v1" }}"#));
        let mut routes = released_repo(
            &sha(),
            index_versions,
            &[("repository.json", &index)],
            &[("repository.json", &index)],
        );
        routes.extend(raw_repo_routes(
            "other/single",
            &other_tip,
            &[
                ("versions.json", &versions_json(&[(&other_release, "*")])),
                ("plugin.json", &plugin_json("bar")),
            ],
        ));
        routes.push((
            "/api/v1/repos/other/single/git/commits/v1".to_string(),
            CannedResponse::json(&format!(r#"{{"sha":"{other_tip}"}}"#)),
        ));
        routes.push(raw_file(
            "other/single",
            &other_release,
            "plugin.json",
            &plugin_json("bar"),
        ));
        routes
    }

    #[tokio::test]
    async fn remote_entries_of_a_released_index_are_released_by_their_own_versions_json() {
        let listing = list_routes(|base| index_of_remote(base, Some(&released())))
            .await
            .unwrap();

        let [bar] = &listing.plugins[..] else {
            panic!("expected one plugin: {:?}", listing.plugins);
        };
        assert_eq!(
            offered(bar),
            ("bar", "0.0.1".into(), "d".repeat(40).as_str())
        );
        assert_eq!(bar.source.git_ref.as_deref(), Some("v1"));
        assert!(bar.source.pinned);
        assert_eq!(
            bar.source.via_repository.as_deref(),
            Some(listing.spec.canonical_url().as_str())
        );
    }

    #[tokio::test]
    async fn remote_entries_of_an_unreleased_index_carry_its_error() {
        let server =
            TestServer::start_with(|base| index_of_remote(base, None).into_iter().collect()).await;

        let listing = list(&server, "/owner/repo").await.unwrap();

        assert_eq!(
            errors(&listing),
            [(
                "bar",
                "no releases: the repository has no versions.json".to_string()
            )]
        );
        let remote_reads: Vec<String> = server
            .requests()
            .into_iter()
            .filter(|path| path.starts_with("/other/single/raw/"))
            .collect();
        let mut remote_reads = remote_reads;
        remote_reads.sort();
        let at_tip = |file: &str| format!("/other/single/raw/commit/{}/{file}", "b".repeat(40));
        assert_eq!(
            remote_reads,
            [at_tip("plugin.json"), at_tip("repository.json")],
            "the remote's tip only names it; its releases are not read"
        );
    }

    #[tokio::test]
    async fn resolving_enforces_the_repository_rules() {
        let archive = |files: &[(&str, &str)]| {
            TestServer::start(HashMap::from([(
                "/owner/repo/archive/main.tar.gz".to_string(),
                CannedResponse::targz(targz(files)),
            )]))
        };

        let server = archive(&[
            ("repo/plugin.json", &plugin_json("foo")),
            ("repo/repository.json", &index_json("")),
        ])
        .await;
        let err = resolve(&server, "/owner/repo").await.unwrap_err();
        assert!(matches!(err, ResolveError::AmbiguousRoot { .. }), "{err}");

        let server = archive(&[("repo/README.md", "hi")]).await;
        let err = resolve(&server, "/owner/repo").await.unwrap_err();
        assert!(
            matches!(err, ResolveError::NotAPluginRepository { .. }),
            "{err}"
        );

        let server = archive(&[(
            "repo/repository.json",
            &index_json(r#"{ "path": "missing" }"#),
        )])
        .await;
        let err = resolve(&server, "/owner/repo").await.unwrap_err();
        assert!(
            matches!(err, ResolveError::PathEntryMissing { .. }),
            "{err}"
        );

        let server = archive(&[
            ("repo/repository.json", &index_json(r#"{ "path": "sub" }"#)),
            ("repo/sub/plugin.json", &plugin_json("foo")),
            ("repo/sub/repository.json", &index_json("")),
        ])
        .await;
        let err = resolve(&server, "/owner/repo").await.unwrap_err();
        assert!(
            matches!(err, ResolveError::NestedRepository { .. }),
            "{err}"
        );

        let oversized = " ".repeat(crate::fetch::MAX_FILE_BYTES as usize + 1);
        for file in ["repo/plugin.json", "repo/versions.json"] {
            let manifest = plugin_json("foo");
            let mut files = vec![("repo/plugin.json", manifest.as_str())];
            files.retain(|(path, _)| *path != file);
            files.push((file, &oversized));
            let server = archive(&files).await;
            let err = resolve(&server, "/owner/repo").await.unwrap_err();
            assert!(
                matches!(
                    &err,
                    ResolveError::Fetch(FetchError::FileTooLarge { location, .. })
                        if location.ends_with(&format!("/owner/{file}"))
                ),
                "{err}"
            );
        }
    }

    #[tokio::test]
    async fn lists_single_plugins_from_raw_manifests_alone() {
        let routes = released_repo(
            &sha(),
            Some(&released()),
            &[],
            &[("plugin.json", &plugin_json("foo"))],
        );
        let server = TestServer::start(routes.into_iter().collect()).await;

        let listing = list(&server, "/owner/repo").await.unwrap();

        assert!(listing.index.is_none());
        assert_eq!(listing.git_ref, "main");
        assert_eq!(listing.commit.as_deref(), Some(sha().as_str()));
        let [plugin] = &listing.plugins[..] else {
            panic!("expected one plugin: {:?}", listing.plugins);
        };
        assert_eq!(
            offered(plugin),
            ("foo", "0.0.1".into(), release_commit().as_str())
        );
        assert_eq!(plugin.source.origin, format!("{}/owner/repo", server.url()));
        assert!(!plugin.source.pinned);
        assert!(
            server
                .requests()
                .iter()
                .all(|path| !path.contains("archive")),
            "{:?}",
            server.requests()
        );
    }

    #[tokio::test]
    async fn listings_read_versions_json_then_the_release_structure() {
        let index = index_json(r#"{ "path": "foo" }, { "path": "bar" }"#);
        let release = [
            vec![("repository.json".to_string(), index)],
            plugin_files("foo", &plugin_json("foo")),
            plugin_files("bar", &plugin_json("bar")),
        ]
        .concat();
        let routes = released_repo(&sha(), Some(&released()), &[], &borrowed(&release));
        let server = TestServer::start(routes.into_iter().collect()).await;

        let listing = list(&server, "/owner/repo").await.unwrap();
        assert!(listing.plugins.iter().all(|plugin| plugin.offer.is_ok()));

        let mut raw: Vec<String> = server
            .requests()
            .into_iter()
            .filter(|path| path.contains("/raw/"))
            .collect();
        raw.sort();
        let at = |commit: &str, path: &str| format!("/owner/repo/raw/commit/{commit}/{path}");
        let commit = release_commit();
        let mut expected = vec![
            at(&sha(), "versions.json"),
            at(&commit, "repository.json"),
            at(&commit, "plugin.json"),
            at(&commit, "foo/repository.json"),
            at(&commit, "foo/plugin.json"),
            at(&commit, "bar/repository.json"),
            at(&commit, "bar/plugin.json"),
        ];
        expected.sort();
        assert_eq!(raw, expected);
    }

    #[tokio::test]
    async fn listings_enforce_the_repository_rules() {
        let other = "b".repeat(40);

        let err = list_routes(|_| {
            raw_repo_routes(
                "owner/repo",
                &sha(),
                &[
                    ("plugin.json", &plugin_json("foo")),
                    ("repository.json", &index_json("")),
                ],
            )
        })
        .await
        .unwrap_err();
        assert!(matches!(err, ResolveError::AmbiguousRoot { .. }), "{err}");

        let err = list_routes(|_| raw_repo_routes("owner/repo", &sha(), &[]))
            .await
            .unwrap_err();
        assert!(
            matches!(err, ResolveError::NotAPluginRepository { .. }),
            "{err}"
        );

        let err = list_routes(|_| {
            released_repo(
                &sha(),
                Some(&released()),
                &[],
                &[("repository.json", &index_json(r#"{ "path": "missing" }"#))],
            )
        })
        .await
        .unwrap_err();
        assert!(
            matches!(&err, ResolveError::PathEntryMissing { path, .. } if path == "missing"),
            "a path entry needs a plugin.json at the release commit: {err}"
        );

        let err = list_routes(|_| {
            raw_repo_routes(
                "owner/repo",
                &sha(),
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
            let mut routes = raw_repo_routes("owner/repo", &sha(), &[("repository.json", &index)]);
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
            let mut routes = raw_repo_routes("owner/repo", &sha(), &[("repository.json", &index)]);
            routes.extend(raw_repo_routes("other/empty", &other, &[]));
            routes
        })
        .await
        .unwrap_err();
        assert!(
            matches!(&err, ResolveError::NotAPluginRepository { repo } if repo.ends_with("other/empty")),
            "{err}"
        );
    }

    #[tokio::test]
    async fn duplicate_ids_fail_only_the_colliding_plugins() {
        let server = TestServer::start_with(|base| {
            let index = index_json(&format!(
                r#"{{ "path": "foo" }}, {{ "path": "bar" }}, {{ "url": "{base}/other/single" }},
                   {{ "path": "ok" }}"#
            ));
            let release = [
                vec![("repository.json".to_string(), index)],
                plugin_files("foo", &plugin_json("foo")),
                plugin_files("bar", &plugin_json("Foo")),
                plugin_files("ok", &plugin_json("ok")),
            ]
            .concat();
            let mut routes = released_repo(&sha(), Some(&released()), &[], &borrowed(&release));
            routes.extend(raw_repo_routes(
                "other/single",
                &"b".repeat(40),
                &[("plugin.json", &plugin_json("ok"))],
            ));
            routes.into_iter().collect()
        })
        .await;

        let listing = list(&server, "/owner/repo").await.unwrap();
        let listed: Vec<(&str, Option<String>)> = listing
            .plugins
            .iter()
            .map(|plugin| {
                let other = match &plugin.offer {
                    Err(PluginError::DuplicateId { other }) => Some(other.clone()),
                    _ => None,
                };
                (plugin.id.as_str(), other)
            })
            .collect();
        let repo = listing.spec.human_id();
        assert_eq!(
            listed,
            [
                ("foo", Some(format!("{repo}/bar"))),
                ("Foo", Some(format!("{repo}/foo"))),
                ("ok", Some(format!("{repo}/ok"))),
                (
                    "ok",
                    Some(format!(
                        "{}/other/single",
                        &repo[..repo.len() - "/owner/repo".len()]
                    ))
                ),
            ]
        );

        let resolved = resolve(&server, "/owner/repo").await.unwrap();
        assert_eq!(ids(&resolved), (vec![], vec!["foo", "Foo", "ok", "ok"]));
    }

    #[tokio::test]
    async fn listings_fail_on_raw_read_errors() {
        let commit = release_commit();
        for (at, file, limited) in [
            (sha(), "versions.json", false),
            (commit.clone(), "plugin.json", false),
            (sha(), "versions.json", true),
            (commit.clone(), "repository.json", true),
        ] {
            let response = if limited {
                CannedResponse::status(429).with_header("Retry-After", "7")
            } else {
                CannedResponse::status(500)
            };
            let err = list_routes(|_| {
                let mut routes = released_repo(
                    &sha(),
                    Some(&released()),
                    &[],
                    &[("plugin.json", &plugin_json("foo"))],
                );
                routes.push((format!("/owner/repo/raw/commit/{at}/{file}"), response));
                routes
            })
            .await
            .unwrap_err();
            let url_matches = |url: &str| url.ends_with(&format!("{at}/{file}"));
            assert!(
                match &err {
                    ResolveError::Fetch(FetchError::RateLimited(rate_limited)) =>
                        limited && url_matches(&rate_limited.url),
                    ResolveError::Fetch(FetchError::Status { status, url }) =>
                        !limited
                            && *status == reqwest::StatusCode::INTERNAL_SERVER_ERROR
                            && url_matches(url),
                    _ => false,
                },
                "{at}/{file}: {err}"
            );
        }

        let oversized = " ".repeat(crate::fetch::MAX_FILE_BYTES as usize + 1);
        let err = list_routes(|_| {
            raw_repo_routes("owner/repo", &sha(), &[("versions.json", &oversized)])
        })
        .await
        .unwrap_err();
        assert!(
            matches!(err, ResolveError::Fetch(FetchError::FileTooLarge { .. })),
            "{err}"
        );
    }

    #[tokio::test]
    async fn listings_read_versions_json_from_archives_when_the_forge_api_has_no_commit() {
        let server = TestServer::start(
            single_plugin_routes("owner/repo", "foo")
                .into_iter()
                .collect(),
        )
        .await;

        let listing = list(&server, "/owner/repo").await.unwrap();

        assert_eq!(listing.git_ref, "main");
        assert_eq!(listing.commit, None);
        assert_eq!(
            offered(&listing.plugins[0]),
            ("foo", "0.0.1".into(), release_commit().as_str())
        );
        assert!(!listing.plugins[0].source.pinned);
    }

    #[tokio::test]
    async fn listings_reject_indexes_that_are_not_utf8() {
        let err = list_routes(|_| {
            let mut routes = raw_repo_routes("owner/repo", &sha(), &[]);
            routes.push((
                format!("/owner/repo/raw/commit/{}/repository.json", sha()),
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

    #[tokio::test]
    async fn rate_limited_remote_entry_plans_fail_the_listing() {
        let server = TestServer::start_with(|base| {
            let index = index_json(&format!(r#"{{ "url": "{base}/other/single" }}"#));
            let mut routes = raw_repo_routes("owner/repo", &sha(), &[("repository.json", &index)]);
            routes.push((
                "/api/v1/repos/other/single".to_string(),
                CannedResponse::status(429).with_header("Retry-After", "30"),
            ));
            routes.into_iter().collect()
        })
        .await;

        let err = list(&server, "/owner/repo").await.unwrap_err();
        assert!(
            matches!(
                &err,
                ResolveError::Fetch(FetchError::RateLimited(limited))
                    if limited.url.ends_with("/api/v1/repos/other/single")
                        && limited.retry_after == Some(std::time::Duration::from_secs(30))
            ),
            "{err}"
        );
        assert!(
            !server
                .requests()
                .iter()
                .any(|path| path.starts_with("/other/single/")),
            "no guessed raw files or archives: {:?}",
            server.requests()
        );
    }

    /// A GitLab index of `plugins/<id>` for each of `ids`, released at
    /// [`release_commit`], downloadable both as directory archives and as
    /// the release's whole archive.
    async fn gitlab_index(ids: &[&str]) -> TestServer {
        let commit = release_commit();
        let entries: Vec<String> = ids
            .iter()
            .map(|id| format!(r#"{{ "path": "plugins/{id}" }}"#))
            .collect();
        let index = index_json(&entries.join(", "));
        let mut routes = gitlab_repo_routes(&sha(), &[("versions.json", &released())]);
        routes.push(gitlab_raw_file(&commit, "repository.json", &index));

        let prefix = format!("repo-{commit}");
        let mut whole = vec![(format!("{prefix}/repository.json"), index.clone())];
        for id in ids {
            routes.push(gitlab_raw_file(
                &commit,
                &format!("plugins/{id}/plugin.json"),
                &plugin_json(id),
            ));
            let top = format!("{prefix}-plugins-{id}/plugins/{id}");
            routes.push(gitlab_directory_archive(
                &commit,
                &format!("plugins/{id}"),
                targz(&[
                    (&format!("{top}/plugin.json"), &plugin_json(id)),
                    (&format!("{top}/init.luau"), "return {}"),
                ]),
            ));
            whole.extend(plugin_files(
                &format!("{prefix}/plugins/{id}"),
                &plugin_json(id),
            ));
        }
        routes.push((
            format!("/owner/repo/-/archive/{commit}/repo-{commit}.tar.gz"),
            CannedResponse::targz(targz(&borrowed(&whole))),
        ));
        TestServer::start(routes.into_iter().collect()).await
    }

    fn archive_requests(server: &TestServer) -> Vec<String> {
        server
            .requests()
            .into_iter()
            .filter(|path| path.contains("archive"))
            .collect()
    }

    fn gitlab_spec(server: &TestServer) -> RepoSpec {
        RepoSpec::parse(&format!("{}/owner/repo?forge=gitlab", server.url()), None).unwrap()
    }

    fn directory_archive(id: &str) -> String {
        format!(
            "/api/v4/projects/owner%2Frepo/repository/archive.tar.gz?sha={}&path=plugins%2F{id}",
            release_commit()
        )
    }

    #[tokio::test]
    async fn gitlab_installs_download_only_the_selected_plugin_directories() {
        let server = gitlab_index(&["foo", "bar"]).await;

        let selected = ["foo".to_string()];
        let resolved = resolve_plugins(&client(), &gitlab_spec(&server), Some(&selected), &host())
            .await
            .unwrap();

        let [foo] = &resolved.candidates[..] else {
            panic!("expected only the selected plugin: {:?}", resolved.rejected);
        };
        assert_eq!(foo.id(), "foo");
        assert!(foo.source_dir().join("init.luau").is_file());
        assert_eq!(foo.source.subpath.as_deref(), Some("plugins/foo"));
        assert_eq!(foo.source.commit, release_commit());
        assert_eq!(archive_requests(&server), [directory_archive("foo")]);
    }

    #[tokio::test]
    async fn gitlab_installs_past_the_directory_archive_limit_download_the_whole_archive_once() {
        let commit = release_commit();
        let ids = ["p1", "p2", "p3", "p4"];
        let server = gitlab_index(&ids).await;
        let spec = gitlab_spec(&server);

        let within: Vec<String> = ids[..MAX_DIRECTORY_ARCHIVES]
            .iter()
            .map(|id| id.to_string())
            .collect();
        let resolved = resolve_plugins(&client(), &spec, Some(&within), &host())
            .await
            .unwrap();
        assert_eq!(resolved.candidates.len(), MAX_DIRECTORY_ARCHIVES);
        let expected: Vec<String> = within.iter().map(|id| directory_archive(id)).collect();
        assert_eq!(archive_requests(&server), expected);

        let seen = archive_requests(&server).len();
        let resolved = resolve_plugins(&client(), &spec, None, &host())
            .await
            .unwrap();
        assert_eq!(
            archive_requests(&server)[seen..],
            [format!(
                "/owner/repo/-/archive/{commit}/repo-{commit}.tar.gz"
            )]
        );
        let installed: Vec<_> = resolved
            .candidates
            .iter()
            .map(|candidate| {
                assert!(candidate.source_dir().join("init.luau").is_file());
                (candidate.id(), candidate.source.subpath.as_deref())
            })
            .collect();
        assert_eq!(
            installed,
            [
                ("p1", Some("plugins/p1")),
                ("p2", Some("plugins/p2")),
                ("p3", Some("plugins/p3")),
                ("p4", Some("plugins/p4")),
            ]
        );
    }

    #[tokio::test]
    async fn gitlab_directory_archives_reject_unsafe_entries_and_fail_missing_directories_alone() {
        let commit = release_commit();
        let index = index_json(r#"{ "path": "plugins/foo" }"#);
        let routes = |archive: Vec<u8>| {
            let mut routes = gitlab_repo_routes(&sha(), &[("versions.json", &released())]);
            routes.push(gitlab_raw_file(&commit, "repository.json", &index));
            routes.push(gitlab_raw_file(
                &commit,
                "plugins/foo/plugin.json",
                &plugin_json("foo"),
            ));
            routes.push(gitlab_directory_archive(&commit, "plugins/foo", archive));
            routes.into_iter().collect()
        };

        let mut hostile = targz_entries();
        hostile.file("top/plugins/foo/plugin.json", &plugin_json("foo"));
        hostile.file_with_raw_path("../evil.txt", "x");
        let server = TestServer::start(routes(hostile.finish())).await;
        let err = resolve_plugins(&client(), &gitlab_spec(&server), None, &host())
            .await
            .unwrap_err();
        assert!(
            matches!(
                err,
                ResolveError::Fetch(FetchError::UnsafeArchiveEntry { .. })
            ),
            "{err}"
        );

        let server = TestServer::start(routes(targz(&[(
            "top/plugins/other/plugin.json",
            &plugin_json("foo"),
        )])))
        .await;
        let resolved = resolve_plugins(&client(), &gitlab_spec(&server), None, &host())
            .await
            .unwrap();
        assert!(
            matches!(
                &resolved.rejected[..],
                [RejectedPlugin {
                    error: PluginError::Release(ReleaseError::Unavailable { .. }),
                    ..
                }]
            ),
            "{:?}",
            resolved.rejected
        );
    }

    async fn resolve_within(
        server: &TestServer,
        path: &str,
        budget: &UnpackBudget,
    ) -> Result<ResolvedRepository, ResolveError> {
        let spec = RepoSpec::parse(&format!("{}{path}", server.url()), None).unwrap();
        let listing = list_plugins(&client(), &spec, &host()).await?;
        resolve_listed(&client(), listing, None, &host(), budget).await
    }

    #[tokio::test]
    async fn releases_share_one_unpack_budget() {
        let server = TestServer::start_with(|base| {
            let index = index_json(&format!(
                r#"{{ "path": "plugins/foo" }}, {{ "url": "{base}/other/single" }}"#
            ));
            let release = [
                vec![("repository.json".to_string(), index)],
                plugin_files("plugins/foo", &plugin_json("foo")),
            ]
            .concat();
            let mut routes = released_repo(&sha(), Some(&released()), &[], &borrowed(&release));
            routes.extend(single_plugin_routes("other/single", "bar"));
            routes.into_iter().collect()
        })
        .await;
        let index_len = index_json(&format!(
            r#"{{ "path": "plugins/foo" }}, {{ "url": "{}/other/single" }}"#,
            server.url()
        ))
        .len();
        let plugin_len = plugin_json("foo").len() + "return {}".len();
        let total = (index_len + 2 * plugin_len) as u64;

        let err = resolve_within(
            &server,
            "/owner/repo",
            &UnpackBudget::with_limits(total - 1, 100),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(
                err,
                ResolveError::Fetch(FetchError::UnpackedBytesExceeded { .. })
            ),
            "{err}"
        );
        let resolved = resolve_within(
            &server,
            "/owner/repo",
            &UnpackBudget::with_limits(total, 100),
        )
        .await
        .unwrap();
        assert_eq!(resolved.candidates.len(), 2);
    }

    #[tokio::test]
    async fn dropped_resolves_release_their_share_of_a_shared_budget() {
        let server = TestServer::start(
            single_plugin_routes("good/repo", "bar")
                .into_iter()
                .collect(),
        )
        .await;
        let good_len = (plugin_json("bar").len() + "return {}".len()) as u64;
        let budget = UnpackBudget::with_limits(good_len, 100);

        let resolved = resolve_within(&server, "/good/repo", &budget)
            .await
            .unwrap();
        assert_eq!(resolved.candidates[0].id(), "bar");

        let err = resolve_within(&server, "/good/repo", &budget)
            .await
            .unwrap_err();
        assert!(
            matches!(
                err,
                ResolveError::Fetch(FetchError::UnpackedBytesExceeded { .. })
            ),
            "a kept tree holds its share: {err}"
        );
        drop(resolved);
        resolve_within(&server, "/good/repo", &budget)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn selected_plugins_resolve_past_siblings_that_fail() {
        let index = index_json(r#"{ "path": "plugins/foo" }, { "path": "plugins/bar" }"#);
        let incompatible = plugin_json("bar").replace(r#""lyra": "0.0.1""#, r#""lyra": "0.1""#);
        let release = [
            vec![("repository.json".to_string(), index)],
            plugin_files("plugins/foo", &plugin_json("foo")),
            plugin_files("plugins/bar", &incompatible),
        ]
        .concat();
        let routes = released_repo(&sha(), Some(&released()), &[], &borrowed(&release));
        let server = TestServer::start(routes.into_iter().collect()).await;
        let spec = RepoSpec::parse(&format!("{}/owner/repo", server.url()), None).unwrap();
        let resolve_selected = async |selected: &[&str]| {
            let selected: Vec<String> = selected.iter().map(|id| id.to_string()).collect();
            resolve_plugins(&client(), &spec, Some(&selected), &host())
                .await
                .unwrap()
        };

        let resolved = resolve_selected(&["foo"]).await;
        assert_eq!(ids(&resolved), (vec!["foo"], vec![]));

        let resolved = resolve_selected(&["foo", "bar"]).await;
        assert_eq!(ids(&resolved), (vec!["foo"], vec!["bar"]));
        assert!(resolved.rejected[0].location.ends_with("plugins/bar"));
        assert_eq!(
            resolved.rejected[0].source.subpath.as_deref(),
            Some("plugins/bar")
        );
    }
}
