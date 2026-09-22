// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::fs::File;
use std::path::Path;

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
    #[error("{context}: {source}")]
    Io {
        context: String,
        #[source]
        source: std::io::Error,
    },
}

/// Copies a resolved candidate into `plugins_dir/<id>`, replacing a
/// previous install from the same origin. The candidate is staged next to
/// the target and promoted by rename, so a failed install never leaves a
/// half-written plugin directory.
///
/// Returns the source record written into the installed directory.
pub fn install_candidate(
    candidate: &PluginCandidate,
    plugins_dir: &Path,
    installed_at: Option<String>,
) -> Result<SourceRecord, InstallError> {
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
    if path_exists(&target)? {
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

    let mut record = candidate.source.clone();
    record.installed_at = installed_at;

    let staging_root = plugins_dir.join(STAGING_DIRNAME);
    let staged = staging_root.join(id);
    let displaced = staging_root.join(format!("{id}.previous"));
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
    if let Err(promote) = std::fs::rename(&staged, &target) {
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
    if had_previous {
        std::fs::remove_dir_all(&displaced).map_err(|source| InstallError::Io {
            context: format!("Could not remove directory `{}`", displaced.display()),
            source,
        })?;
    }
    if let Err(error) = std::fs::remove_dir(&staging_root) {
        tracing::debug!(path = %staging_root.display(), %error, "could not remove staging directory");
    }

    Ok(record)
}

/// Removes a repository-managed plugin. Local plugins (no source record)
/// are refused; deleting those is a deliberate filesystem operation.
/// Returns the removed record, or `None` when it was present but
/// unreadable.
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
    use std::collections::HashMap;
    use std::collections::HashSet;

    use tempfile::TempDir;

    use super::*;
    use crate::fetch::RepoSpec;
    use crate::manifest::SOURCE_RECORD_FILENAME;
    use crate::resolve::{
        ResolvedRepository,
        resolve_plugins,
    };
    use crate::testutil::{
        TestServer,
        client,
        plugin_json,
        single_plugin_routes,
    };

    async fn resolved_foo() -> (TestServer, ResolvedRepository) {
        let server =
            TestServer::start(HashMap::from([single_plugin_routes("owner/repo", "foo")])).await;
        let spec = RepoSpec::parse(&format!("{}/owner/repo", server.url()), None).unwrap();
        let resolved = resolve_plugins(&client(), &spec, &HashSet::new())
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

        install_candidate(&resolved.candidates[0], plugins_dir.path(), None).unwrap();
        std::fs::write(plugins_dir.path().join("foo/extra.txt"), "stale").unwrap();

        install_candidate(&resolved.candidates[0], plugins_dir.path(), None).unwrap();

        // The replacement is the candidate tree, not a merge.
        assert!(!plugins_dir.path().join("foo/extra.txt").exists());
        assert!(plugins_dir.path().join("foo/init.luau").is_file());
    }

    #[tokio::test]
    async fn refuses_to_replace_local_plugins() {
        let (_server, resolved) = resolved_foo().await;
        let plugins_dir = TempDir::new().unwrap();
        let local = plugins_dir.path().join("foo");
        std::fs::create_dir_all(&local).unwrap();
        std::fs::write(local.join("plugin.json"), plugin_json("foo")).unwrap();

        let err = install_candidate(&resolved.candidates[0], plugins_dir.path(), None).unwrap_err();
        assert!(matches!(err, InstallError::NotManaged { .. }), "{err}");
    }

    #[tokio::test]
    async fn refuses_origin_mismatches() {
        let (_server, resolved) = resolved_foo().await;
        let plugins_dir = TempDir::new().unwrap();

        let mut record =
            install_candidate(&resolved.candidates[0], plugins_dir.path(), None).unwrap();
        record.origin = "https://github.com/somewhere/else".into();
        record.store(&plugins_dir.path().join("foo")).unwrap();

        let err = install_candidate(&resolved.candidates[0], plugins_dir.path(), None).unwrap_err();
        assert!(matches!(err, InstallError::OriginMismatch { .. }), "{err}");
    }

    #[tokio::test]
    async fn refuses_case_colliding_ids() {
        let (_server, resolved) = resolved_foo().await;
        let plugins_dir = TempDir::new().unwrap();
        std::fs::create_dir_all(plugins_dir.path().join("Foo")).unwrap();

        let err = install_candidate(&resolved.candidates[0], plugins_dir.path(), None).unwrap_err();
        assert!(matches!(err, InstallError::CaseCollision { .. }), "{err}");
    }

    #[tokio::test]
    async fn uninstalls_managed_plugins() {
        let (_server, resolved) = resolved_foo().await;
        let plugins_dir = TempDir::new().unwrap();
        install_candidate(&resolved.candidates[0], plugins_dir.path(), None).unwrap();

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
        install_candidate(&resolved.candidates[0], plugins_dir.path(), None).unwrap();
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
