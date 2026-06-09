// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::path::Path;

use crate::manifest::SourceRecord;
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
    #[error(transparent)]
    Io(#[from] std::io::Error),
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
    std::fs::create_dir_all(plugins_dir)?;

    for entry in std::fs::read_dir(plugins_dir)? {
        let name = entry?.file_name();
        let name = name.to_string_lossy();
        if name.eq_ignore_ascii_case(id) && name != id {
            return Err(InstallError::CaseCollision {
                id: id.to_string(),
                existing: name.into_owned(),
            });
        }
    }

    let target = plugins_dir.join(id);
    if target.exists() {
        match SourceRecord::load(&target)? {
            None => {
                return Err(InstallError::NotManaged { id: id.to_string() });
            }
            Some(existing) if existing.origin != candidate.source.origin => {
                return Err(InstallError::OriginMismatch {
                    id: id.to_string(),
                    existing: existing.origin,
                    requested: candidate.source.origin.clone(),
                });
            }
            Some(_) => {}
        }
    }

    let mut record = candidate.source.clone();
    record.installed_at = installed_at;

    let staging_root = plugins_dir.join(STAGING_DIRNAME);
    let staged = staging_root.join(id);
    let displaced = staging_root.join(format!("{id}.previous"));
    if staged.exists() {
        std::fs::remove_dir_all(&staged)?;
    }
    if displaced.exists() {
        std::fs::remove_dir_all(&displaced)?;
    }

    copy_dir(candidate.source_dir(), &staged)?;
    record.store(&staged)?;

    let had_previous = target.exists();
    if had_previous {
        std::fs::rename(&target, &displaced)?;
    }
    if let Err(promote) = std::fs::rename(&staged, &target) {
        if had_previous {
            // Best-effort restore; the promotion error is the one worth
            // surfacing.
            let _ = std::fs::rename(&displaced, &target);
        }
        return Err(promote.into());
    }
    if had_previous {
        std::fs::remove_dir_all(&displaced)?;
    }
    let _ = std::fs::remove_dir(&staging_root);

    Ok(record)
}

/// Removes a repository-managed plugin. Local plugins (no source record)
/// are refused; deleting those is a deliberate filesystem operation.
pub fn uninstall_plugin(plugins_dir: &Path, id: &str) -> Result<SourceRecord, InstallError> {
    ensure_valid_id(id)?;

    let target = plugins_dir.join(id);
    if !target.is_dir() {
        return Err(InstallError::NotInstalled { id: id.to_string() });
    }

    let record = SourceRecord::load(&target)?
        .ok_or_else(|| InstallError::NotManaged { id: id.to_string() })?;

    std::fs::remove_dir_all(&target)?;
    Ok(record)
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

fn copy_dir(from: &Path, to: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(to)?;
    for entry in std::fs::read_dir(from)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let dest = to.join(entry.file_name());
        if file_type.is_dir() {
            copy_dir(&entry.path(), &dest)?;
        } else if file_type.is_file() {
            std::fs::copy(entry.path(), &dest)?;
        } else {
            return Err(std::io::Error::other(format!(
                "refusing to copy non-regular file {}",
                entry.path().display()
            )));
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

        let record = uninstall_plugin(plugins_dir.path(), "foo").unwrap();
        assert_eq!(record.origin, resolved.candidates[0].source.origin);
        assert!(!plugins_dir.path().join("foo").exists());
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
