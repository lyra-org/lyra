// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::fs::File;
use std::path::{
    Path,
    PathBuf,
};

use crate::manifest::{
    SOURCE_RECORD_FILENAME,
    SourceRecord,
};
use crate::resolve::PluginCandidate;

/// Hidden staging area inside `plugins_dir`; plugin discovery skips
/// dot-directories. Staying on the same filesystem keeps the final
/// promotion a rename.
const STAGING_DIRNAME: &str = ".staging";

#[derive(Debug, thiserror::Error)]
pub enum InstallError {
    #[error("invalid plugin id '{id}'")]
    InvalidId { id: String },
    #[error("plugin '{id}' is not installed")]
    NotInstalled { id: String },
    #[error("plugin '{id}' is local and not managed by a repository")]
    NotManaged { id: String },
    #[error(
        "plugin '{id}' is already installed from {existing}; refusing to replace it from {requested}"
    )]
    OriginMismatch {
        id: String,
        existing: String,
        requested: String,
    },
    #[error(
        "plugin id '{id}' collides with installed directory '{existing}' on case-insensitive filesystems"
    )]
    CaseCollision { id: String, existing: String },
    #[error(
        "plugin '{id}' is not installed, but its previous installation remains at `{previous}`"
    )]
    PreviousOnly { id: String, previous: PathBuf },
    #[error("{context}: {source}")]
    Io {
        context: String,
        #[source]
        source: std::io::Error,
    },
}

/// Who an install was made for, recorded on its source record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstalledBy {
    /// A person: an API request or the command line.
    Request,
    /// The host's automatic update check.
    AutomaticUpdate,
}

/// Copies a resolved candidate into `plugins_dir/<id>`, replacing a
/// previous install from the same origin. The candidate is staged next to
/// the target and promoted by rename, so a failed install never leaves a
/// half-written plugin directory.
///
/// The caller must have exclusive access to `plugins_dir`, as
/// [`stage_candidate`] describes.
///
/// Returns the source record written into the installed directory.
pub fn install_candidate(
    candidate: &PluginCandidate,
    plugins_dir: &Path,
    installed_at: Option<String>,
    installed_by: InstalledBy,
) -> Result<SourceRecord, InstallError> {
    let staged = stage_candidate(candidate, plugins_dir, installed_at, installed_by)?;
    let mut records = promote_together(vec![staged])?;
    Ok(records.remove(0))
}

/// A candidate copied into the staging area of `plugins_dir`, checked to
/// be able to replace what is installed under its id, and waiting for
/// [`promote_together`]. Dropping it removes the staged copy, so the
/// exclusive access [`stage_candidate`] requires lasts until it is dropped,
/// whether promoted or abandoned.
pub struct StagedInstall {
    id: String,
    plugins_dir: PathBuf,
    staged: PathBuf,
    record: SourceRecord,
}

impl StagedInstall {
    pub fn id(&self) -> &str {
        &self.id
    }

    fn staging_root(&self) -> PathBuf {
        self.plugins_dir.join(STAGING_DIRNAME)
    }
}

impl Drop for StagedInstall {
    fn drop(&mut self) {
        if self.staged.exists()
            && let Err(error) = std::fs::remove_dir_all(&self.staged)
        {
            tracing::warn!(path = %self.staged.display(), %error, "could not remove staged plugin");
        }
        if let Err(error) = std::fs::remove_dir(self.staging_root()) {
            tracing::debug!(path = %self.staging_root().display(), %error, "could not remove staging directory");
        }
    }
}

/// Stages `candidate` for installing into `plugins_dir`, as
/// [`install_candidate`] does before it promotes it.
///
/// Staging and promotion use fixed paths inside `plugins_dir`, so only one
/// caller at a time may stage, promote, or uninstall there. Locking each
/// call is not enough: a caller's exclusive access must last from its first
/// `stage_candidate` through promotion or rollback and the drop of every
/// [`StagedInstall`] it staged, abandoned ones included. A caller holds at
/// most one live [`StagedInstall`] per plugin id: staging an id again
/// replaces the earlier staged copy, which that handle's drop then deletes.
pub fn stage_candidate(
    candidate: &PluginCandidate,
    plugins_dir: &Path,
    installed_at: Option<String>,
    installed_by: InstalledBy,
) -> Result<StagedInstall, InstallError> {
    let id = candidate.id();
    ensure_valid_id(id)?;
    std::fs::create_dir_all(plugins_dir).map_err(|source| InstallError::Io {
        context: format!(
            "Could not create plugin directory `{}`",
            plugins_dir.display()
        ),
        source,
    })?;

    let entries = std::fs::read_dir(plugins_dir).map_err(|source| InstallError::Io {
        context: format!(
            "Could not read plugin directory `{}`",
            plugins_dir.display()
        ),
        source,
    })?;
    for entry in entries {
        let name = entry
            .map_err(|source| InstallError::Io {
                context: format!("Could not read an entry in `{}`", plugins_dir.display()),
                source,
            })?
            .file_name();
        let name = name.to_string_lossy();
        if name.eq_ignore_ascii_case(id) && name != id {
            return Err(InstallError::CaseCollision {
                id: id.to_string(),
                existing: name.into_owned(),
            });
        }
    }

    let target = plugins_dir.join(id);
    let displaced = displaced_path(plugins_dir, id);
    let installed = path_exists(&target)?;
    if installed {
        match managed_state(&target)? {
            Managed::Local => {
                return Err(InstallError::NotManaged { id: id.to_string() });
            }
            Managed::Record(existing) if existing.origin != candidate.source.origin => {
                return Err(InstallError::OriginMismatch {
                    id: id.to_string(),
                    existing: existing.origin,
                    requested: candidate.source.origin.clone(),
                });
            }
            Managed::Record(_) | Managed::Broken => {}
        }
    }
    if path_exists(&displaced)? && (!installed || resolves_to(&target, &displaced)?) {
        return Err(InstallError::PreviousOnly {
            id: id.to_string(),
            previous: displaced,
        });
    }

    let mut record = candidate.source.clone();
    record.installed_at = installed_at;
    record.installed_automatically = installed_by == InstalledBy::AutomaticUpdate;

    let staging_root = plugins_dir.join(STAGING_DIRNAME);
    let staged = staging_root.join(id);
    if path_exists(&staged)? {
        std::fs::remove_dir_all(&staged).map_err(|source| InstallError::Io {
            context: format!("Could not remove directory `{}`", staged.display()),
            source,
        })?;
    }
    if path_exists(&displaced)? {
        std::fs::remove_dir_all(&displaced).map_err(|source| InstallError::Io {
            context: format!("Could not remove directory `{}`", displaced.display()),
            source,
        })?;
    }

    copy_dir(candidate.source_dir(), &staged)?;
    record.store(&staged).map_err(|source| InstallError::Io {
        context: format!(
            "Could not write source record `{}`",
            staged.join(SOURCE_RECORD_FILENAME).display()
        ),
        source,
    })?;

    Ok(StagedInstall {
        id: id.to_string(),
        plugins_dir: plugins_dir.to_path_buf(),
        staged,
        record,
    })
}

/// Where the install a promotion replaces waits until the promotion
/// succeeds.
fn displaced_path(plugins_dir: &Path, id: &str) -> PathBuf {
    plugins_dir
        .join(STAGING_DIRNAME)
        .join(format!("{id}.previous"))
}

/// Moves every `staged` install into its plugins directory, replacing what
/// is installed under its id, all or none: when one cannot be promoted,
/// those promoted before it are put back as they were.
///
/// The caller must still hold the exclusive access to their `plugins_dir`
/// that [`stage_candidate`] requires.
///
/// Returns the source records written into the installed directories, in
/// the order of `staged`.
pub fn promote_together(staged: Vec<StagedInstall>) -> Result<Vec<SourceRecord>, InstallError> {
    let mut promoted = Vec::new();
    for install in &staged {
        match promote(install) {
            Ok(done) => promoted.push(done),
            Err(error) => {
                for done in promoted.into_iter().rev() {
                    done.restore();
                }
                return Err(error);
            }
        }
    }
    for done in promoted {
        done.finish();
    }
    Ok(staged
        .iter()
        .map(|install| install.record.clone())
        .collect())
}

/// A staged install moved into place, with the install it replaced.
struct Promoted {
    target: PathBuf,
    displaced: Option<PathBuf>,
}

impl Promoted {
    /// Puts back what was installed before the promotion.
    fn restore(self) {
        if let Err(error) = std::fs::remove_dir_all(&self.target) {
            tracing::warn!(path = %self.target.display(), %error, "could not remove promoted plugin");
        }
        if let Some(displaced) = &self.displaced
            && let Err(error) = std::fs::rename(displaced, &self.target)
        {
            tracing::warn!(from = %displaced.display(), to = %self.target.display(), %error, "could not restore previous plugin installation");
        }
    }

    /// Removes the replaced install once every promotion succeeded.
    fn finish(self) {
        if let Some(displaced) = &self.displaced
            && let Err(error) = std::fs::remove_dir_all(displaced)
        {
            tracing::warn!(path = %displaced.display(), %error, "could not remove previous plugin installation");
        }
    }
}

fn promote(install: &StagedInstall) -> Result<Promoted, InstallError> {
    let target = install.plugins_dir.join(&install.id);
    let staged = &install.staged;
    let displaced = displaced_path(&install.plugins_dir, &install.id);
    let had_previous = path_exists(&target)?;
    if had_previous {
        std::fs::rename(&target, &displaced).map_err(|source| InstallError::Io {
            context: format!(
                "Could not rename `{}` to `{}`",
                target.display(),
                displaced.display()
            ),
            source,
        })?;
    }
    if let Err(promote) = std::fs::rename(staged, &target) {
        if had_previous {
            // Best-effort restore; the promotion error is the one worth
            // surfacing.
            if let Err(error) = std::fs::rename(&displaced, &target) {
                tracing::warn!(from = %displaced.display(), to = %target.display(), %error, "could not restore previous plugin installation");
            }
        }
        return Err(InstallError::Io {
            context: format!(
                "Could not rename `{}` to `{}`",
                staged.display(),
                target.display()
            ),
            source: promote,
        });
    }
    Ok(Promoted {
        target,
        displaced: had_previous.then_some(displaced),
    })
}

/// Removes a repository-managed plugin. Local plugins (no source record)
/// are refused; deleting those is a deliberate filesystem operation.
/// Returns the removed record, or `None` when it was present but
/// unreadable.
///
/// The caller must have exclusive access to `plugins_dir`, as
/// [`stage_candidate`] describes, and stage nothing it has not yet
/// promoted or dropped.
pub fn uninstall_plugin(
    plugins_dir: &Path,
    id: &str,
) -> Result<Option<SourceRecord>, InstallError> {
    ensure_valid_id(id)?;

    let target = plugins_dir.join(id);
    match std::fs::metadata(&target) {
        Ok(metadata) if metadata.is_dir() => {}
        Ok(_) => return Err(InstallError::NotInstalled { id: id.to_string() }),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
            return Err(InstallError::NotInstalled { id: id.to_string() });
        }
        Err(source) => {
            return Err(InstallError::Io {
                context: format!("Could not inspect plugin directory `{}`", target.display()),
                source,
            });
        }
    }

    let record = match managed_state(&target)? {
        Managed::Local => return Err(InstallError::NotManaged { id: id.to_string() }),
        Managed::Record(record) => Some(record),
        Managed::Broken => None,
    };

    std::fs::remove_dir_all(&target).map_err(|source| InstallError::Io {
        context: format!("Could not remove directory `{}`", target.display()),
        source,
    })?;
    Ok(record)
}

enum Managed {
    /// No source record: bundled or hand-copied.
    Local,
    Record(SourceRecord),
    /// A source record is present but cannot be parsed, for example one
    /// written by an older schema. The directory is still managed, and
    /// replacing or removing it is how such records are recovered.
    Broken,
}

fn managed_state(target: &Path) -> Result<Managed, InstallError> {
    match SourceRecord::load(target) {
        Ok(None) => Ok(Managed::Local),
        Ok(Some(record)) => Ok(Managed::Record(record)),
        Err(source) if source.kind() == std::io::ErrorKind::InvalidData => {
            tracing::warn!(path = %target.join(SOURCE_RECORD_FILENAME).display(), %source, "unreadable source record; treating plugin as managed");
            Ok(Managed::Broken)
        }
        Err(source) => Err(InstallError::Io {
            context: format!(
                "Could not read source record `{}`",
                target.join(SOURCE_RECORD_FILENAME).display()
            ),
            source,
        }),
    }
}

/// Ids come from validated manifests, but install and uninstall also take
/// them straight from host APIs — never let one name a path outside
/// `plugins_dir`.
fn ensure_valid_id(id: &str) -> Result<(), InstallError> {
    let valid = !id.is_empty()
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-');
    if !valid {
        return Err(InstallError::InvalidId { id: id.to_string() });
    }
    Ok(())
}

fn path_exists(path: &Path) -> Result<bool, InstallError> {
    path.try_exists().map_err(|source| InstallError::Io {
        context: format!("Could not inspect `{}`", path.display()),
        source,
    })
}

/// Whether `path` resolves to `other`, as a symlink to it does.
fn resolves_to(path: &Path, other: &Path) -> Result<bool, InstallError> {
    let canonical = |path: &Path| {
        std::fs::canonicalize(path).map_err(|source| InstallError::Io {
            context: format!("Could not resolve `{}`", path.display()),
            source,
        })
    };
    Ok(canonical(path)? == canonical(other)?)
}

fn copy_dir(from: &Path, to: &Path) -> Result<(), InstallError> {
    std::fs::create_dir_all(to).map_err(|source| InstallError::Io {
        context: format!("Could not create directory `{}`", to.display()),
        source,
    })?;
    let entries = std::fs::read_dir(from).map_err(|source| InstallError::Io {
        context: format!("Could not read directory `{}`", from.display()),
        source,
    })?;
    for entry in entries {
        let entry = entry.map_err(|source| InstallError::Io {
            context: format!("Could not read an entry in `{}`", from.display()),
            source,
        })?;
        let path = entry.path();
        let file_type = entry.file_type().map_err(|source| InstallError::Io {
            context: format!("Could not inspect `{}`", path.display()),
            source,
        })?;
        let dest = to.join(entry.file_name());
        if file_type.is_dir() {
            copy_dir(&path, &dest)?;
        } else if file_type.is_file() {
            let mut input = File::open(&path).map_err(|source| InstallError::Io {
                context: format!("Could not open `{}` for reading", path.display()),
                source,
            })?;
            let mut output = File::create(&dest).map_err(|source| InstallError::Io {
                context: format!("Could not create file `{}`", dest.display()),
                source,
            })?;
            std::io::copy(&mut input, &mut output).map_err(|source| InstallError::Io {
                context: format!(
                    "Could not copy `{}` to `{}`",
                    path.display(),
                    dest.display()
                ),
                source,
            })?;
        } else {
            return Err(InstallError::Io {
                context: format!(
                    "Could not copy `{}` to `{}`",
                    path.display(),
                    dest.display()
                ),
                source: std::io::Error::other("refusing to copy non-regular file"),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::forge::RepoSpec;
    use crate::manifest::SOURCE_RECORD_FILENAME;
    use crate::resolve::{
        ResolvedRepository,
        resolve_plugins,
    };
    use crate::testutil::{
        TestServer,
        client,
        host,
        plugin_json,
        single_plugin_routes,
    };

    async fn resolved_foo() -> (TestServer, ResolvedRepository) {
        let server = TestServer::start(
            single_plugin_routes("owner/repo", "foo")
                .into_iter()
                .collect(),
        )
        .await;
        let spec = RepoSpec::parse(&format!("{}/owner/repo", server.url()), None).unwrap();
        let resolved = resolve_plugins(&client(), &spec, None, &host())
            .await
            .unwrap();
        (server, resolved)
    }

    #[tokio::test]
    async fn installs_and_records_source() {
        let (_server, resolved) = resolved_foo().await;
        let plugins_dir = TempDir::new().unwrap();

        let record = install_candidate(
            &resolved.candidates[0],
            plugins_dir.path(),
            Some("2026-06-09T12:00:00Z".into()),
            InstalledBy::Request,
        )
        .unwrap();

        let installed = plugins_dir.path().join("foo");
        assert!(installed.join("plugin.json").is_file());
        assert!(installed.join("init.luau").is_file());
        assert!(installed.join(SOURCE_RECORD_FILENAME).is_file());
        assert_eq!(record.installed_at.as_deref(), Some("2026-06-09T12:00:00Z"));
        assert_eq!(SourceRecord::load(&installed).unwrap(), Some(record));
        assert!(!plugins_dir.path().join(STAGING_DIRNAME).exists());
    }

    #[tokio::test]
    async fn reinstalls_from_the_same_origin() {
        let (_server, resolved) = resolved_foo().await;
        let plugins_dir = TempDir::new().unwrap();

        install_candidate(
            &resolved.candidates[0],
            plugins_dir.path(),
            None,
            InstalledBy::Request,
        )
        .unwrap();
        std::fs::write(plugins_dir.path().join("foo/extra.txt"), "stale").unwrap();

        install_candidate(
            &resolved.candidates[0],
            plugins_dir.path(),
            None,
            InstalledBy::Request,
        )
        .unwrap();

        // The replacement is the candidate tree, not a merge.
        assert!(!plugins_dir.path().join("foo/extra.txt").exists());
        assert!(plugins_dir.path().join("foo/init.luau").is_file());
    }

    #[tokio::test]
    async fn promotions_together_put_back_every_install_when_one_fails() {
        let (_foo_server, foo) = resolved_foo().await;
        let bar_server = TestServer::start(
            single_plugin_routes("owner/repo", "bar")
                .into_iter()
                .collect(),
        )
        .await;
        let spec = RepoSpec::parse(&format!("{}/owner/repo", bar_server.url()), None).unwrap();
        let bar = resolve_plugins(&client(), &spec, None, &host())
            .await
            .unwrap();
        let plugins_dir = TempDir::new().unwrap();
        let candidates = [&foo.candidates[0], &bar.candidates[0]];
        for candidate in candidates {
            install_candidate(candidate, plugins_dir.path(), None, InstalledBy::Request).unwrap();
        }
        std::fs::write(plugins_dir.path().join("foo/previous.txt"), "kept").unwrap();

        let staged: Vec<_> = candidates
            .into_iter()
            .map(|candidate| {
                stage_candidate(candidate, plugins_dir.path(), None, InstalledBy::Request).unwrap()
            })
            .collect();
        let blocker = displaced_path(plugins_dir.path(), "bar").join("blocker");
        std::fs::create_dir_all(&blocker).unwrap();

        promote_together(staged).unwrap_err();
        assert!(
            plugins_dir.path().join("foo/previous.txt").is_file(),
            "foo, promoted before bar failed, is put back"
        );
        assert!(plugins_dir.path().join("bar/init.luau").is_file());
        assert!(
            !plugins_dir
                .path()
                .join(STAGING_DIRNAME)
                .join("foo")
                .exists()
        );
        assert!(
            !plugins_dir
                .path()
                .join(STAGING_DIRNAME)
                .join("bar")
                .exists()
        );
    }

    #[tokio::test]
    async fn refuses_to_stage_over_the_only_previous_installation() {
        let (_server, resolved) = resolved_foo().await;
        let plugins_dir = TempDir::new().unwrap();
        install_candidate(
            &resolved.candidates[0],
            plugins_dir.path(),
            None,
            InstalledBy::Request,
        )
        .unwrap();
        let previous = displaced_path(plugins_dir.path(), "foo");
        std::fs::create_dir_all(plugins_dir.path().join(STAGING_DIRNAME)).unwrap();
        std::fs::rename(plugins_dir.path().join("foo"), &previous).unwrap();

        let Err(err) = stage_candidate(
            &resolved.candidates[0],
            plugins_dir.path(),
            None,
            InstalledBy::Request,
        ) else {
            panic!("staging over the only previous installation must fail");
        };
        assert!(matches!(err, InstallError::PreviousOnly { .. }), "{err}");
        assert!(previous.join("init.luau").is_file());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn refuses_to_stage_when_the_target_links_to_the_previous_installation() {
        let (_server, resolved) = resolved_foo().await;
        let plugins_dir = TempDir::new().unwrap();
        install_candidate(
            &resolved.candidates[0],
            plugins_dir.path(),
            None,
            InstalledBy::Request,
        )
        .unwrap();
        let target = plugins_dir.path().join("foo");
        let previous = displaced_path(plugins_dir.path(), "foo");
        std::fs::create_dir_all(plugins_dir.path().join(STAGING_DIRNAME)).unwrap();
        std::fs::rename(&target, &previous).unwrap();
        std::os::unix::fs::symlink(&previous, &target).unwrap();

        let Err(err) = stage_candidate(
            &resolved.candidates[0],
            plugins_dir.path(),
            None,
            InstalledBy::Request,
        ) else {
            panic!("staging over a link to the only previous installation must fail");
        };
        assert!(matches!(err, InstallError::PreviousOnly { .. }), "{err}");
        assert!(previous.join("init.luau").is_file());
    }

    #[tokio::test]
    async fn stages_over_a_stale_previous_installation() {
        let (_server, resolved) = resolved_foo().await;
        let plugins_dir = TempDir::new().unwrap();
        install_candidate(
            &resolved.candidates[0],
            plugins_dir.path(),
            None,
            InstalledBy::Request,
        )
        .unwrap();
        let previous = displaced_path(plugins_dir.path(), "foo");
        std::fs::create_dir_all(&previous).unwrap();
        std::fs::write(previous.join("stale.txt"), "stale").unwrap();

        let staged = stage_candidate(
            &resolved.candidates[0],
            plugins_dir.path(),
            None,
            InstalledBy::Request,
        )
        .unwrap();
        assert!(!previous.exists());
        drop(staged);
        assert!(plugins_dir.path().join("foo/init.luau").is_file());
    }

    #[tokio::test]
    async fn refuses_to_replace_local_plugins() {
        let (_server, resolved) = resolved_foo().await;
        let plugins_dir = TempDir::new().unwrap();
        let local = plugins_dir.path().join("foo");
        std::fs::create_dir_all(&local).unwrap();
        std::fs::write(local.join("plugin.json"), plugin_json("foo")).unwrap();

        let err = install_candidate(
            &resolved.candidates[0],
            plugins_dir.path(),
            None,
            InstalledBy::Request,
        )
        .unwrap_err();
        assert!(matches!(err, InstallError::NotManaged { .. }), "{err}");
    }

    #[tokio::test]
    async fn refuses_origin_mismatches() {
        let (_server, resolved) = resolved_foo().await;
        let plugins_dir = TempDir::new().unwrap();

        let mut record = install_candidate(
            &resolved.candidates[0],
            plugins_dir.path(),
            None,
            InstalledBy::Request,
        )
        .unwrap();
        record.origin = "https://github.com/somewhere/else".into();
        record.store(&plugins_dir.path().join("foo")).unwrap();

        let err = install_candidate(
            &resolved.candidates[0],
            plugins_dir.path(),
            None,
            InstalledBy::Request,
        )
        .unwrap_err();
        assert!(matches!(err, InstallError::OriginMismatch { .. }), "{err}");
    }

    #[tokio::test]
    async fn refuses_case_colliding_ids() {
        let (_server, resolved) = resolved_foo().await;
        let plugins_dir = TempDir::new().unwrap();
        std::fs::create_dir_all(plugins_dir.path().join("Foo")).unwrap();

        let err = install_candidate(
            &resolved.candidates[0],
            plugins_dir.path(),
            None,
            InstalledBy::Request,
        )
        .unwrap_err();
        assert!(matches!(err, InstallError::CaseCollision { .. }), "{err}");
    }

    #[tokio::test]
    async fn uninstalls_managed_plugins() {
        let (_server, resolved) = resolved_foo().await;
        let plugins_dir = TempDir::new().unwrap();
        install_candidate(
            &resolved.candidates[0],
            plugins_dir.path(),
            None,
            InstalledBy::Request,
        )
        .unwrap();

        let record = uninstall_plugin(plugins_dir.path(), "foo")
            .unwrap()
            .expect("record was readable");
        assert_eq!(record.origin, resolved.candidates[0].source.origin);
        assert!(!plugins_dir.path().join("foo").exists());
    }

    #[tokio::test]
    async fn unreadable_records_can_be_replaced_and_removed() {
        let (_server, resolved) = resolved_foo().await;
        let plugins_dir = TempDir::new().unwrap();
        let installed = plugins_dir.path().join("foo");
        let broken = |json: &str| {
            std::fs::create_dir_all(&installed).unwrap();
            std::fs::write(installed.join(SOURCE_RECORD_FILENAME), json).unwrap();
        };

        broken(r#"{"schema_version": 1}"#);
        install_candidate(
            &resolved.candidates[0],
            plugins_dir.path(),
            None,
            InstalledBy::Request,
        )
        .unwrap();
        assert!(SourceRecord::load(&installed).unwrap().is_some());

        broken("{");
        assert_eq!(uninstall_plugin(plugins_dir.path(), "foo").unwrap(), None);
        assert!(!installed.exists());
    }

    #[test]
    fn uninstall_refuses_local_plugins() {
        let plugins_dir = TempDir::new().unwrap();
        std::fs::create_dir_all(plugins_dir.path().join("foo")).unwrap();

        let err = uninstall_plugin(plugins_dir.path(), "foo").unwrap_err();
        assert!(matches!(err, InstallError::NotManaged { .. }), "{err}");
    }

    #[test]
    fn uninstall_reports_missing_plugins() {
        let plugins_dir = TempDir::new().unwrap();
        let err = uninstall_plugin(plugins_dir.path(), "foo").unwrap_err();
        assert!(matches!(err, InstallError::NotInstalled { .. }), "{err}");
    }

    #[test]
    fn uninstall_rejects_path_like_ids() {
        let plugins_dir = TempDir::new().unwrap();
        for id in ["../foo", "a/b", "", "."] {
            let err = uninstall_plugin(plugins_dir.path(), id).unwrap_err();
            assert!(
                matches!(err, InstallError::InvalidId { .. }),
                "{id:?}: {err}"
            );
        }
    }
}
