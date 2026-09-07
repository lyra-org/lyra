// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    io::ErrorKind,
    path::Path,
};

use anyhow::Context;
use serde::Serialize;

use crate::{
    STATE,
    db,
};

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize)]
pub(crate) struct Status {
    pub(crate) account_required: bool,
    pub(crate) plugin_selection_required: bool,
}

pub(crate) async fn status() -> anyhow::Result<Status> {
    let (account_required, skipped) = {
        let db = STATE.db.read().await;
        (
            !db::roles::has_non_default_admin(&db)?,
            db::server::plugin_selection_skipped(&*db)?,
        )
    };
    Ok(Status {
        account_required,
        plugin_selection_required: !skipped
            && !has_installed_plugins(&crate::plugins::bootstrap::plugins_dir()).await?,
    })
}

async fn has_installed_plugins(path: &Path) -> anyhow::Result<bool> {
    let mut entries = match tokio::fs::read_dir(path).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(false),
        Err(error) => {
            return Err(error).with_context(|| format!("read plugin directory {}", path.display()));
        }
    };
    while let Some(entry) = entries.next_entry().await? {
        if entry.file_name().to_string_lossy().starts_with('.') {
            continue;
        }
        // Discovery treats every visible directory as a plugin candidate,
        // including plugins whose manifests or entrypoints fail to load.
        match tokio::fs::metadata(entry.path()).await {
            Ok(metadata) if metadata.is_dir() => return Ok(true),
            Ok(_) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("inspect plugin candidate {}", entry.path().display())
                });
            }
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn installed_plugins_include_failed_candidates_but_not_staging() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let plugins = root.path().join("plugins");
        assert!(!has_installed_plugins(&plugins).await?);
        std::fs::create_dir(&plugins)?;
        assert!(!has_installed_plugins(&plugins).await?);
        std::fs::create_dir(plugins.join(".staging"))?;
        std::fs::write(plugins.join("README.md"), "plugins")?;
        assert!(!has_installed_plugins(&plugins).await?);
        std::fs::create_dir(plugins.join("broken-plugin"))?;
        std::fs::write(plugins.join("broken-plugin/plugin.json"), "invalid json")?;
        assert!(has_installed_plugins(&plugins).await?);
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn dangling_links_are_ignored_and_directory_links_count() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let plugins = root.path().join("plugins");
        std::fs::create_dir(&plugins)?;
        std::os::unix::fs::symlink(root.path().join("missing"), plugins.join("dangling"))?;
        assert!(!has_installed_plugins(&plugins).await?);
        let external = root.path().join("external");
        std::fs::create_dir(&external)?;
        std::os::unix::fs::symlink(external, plugins.join("linked"))?;
        assert!(has_installed_plugins(&plugins).await?);
        Ok(())
    }

    #[tokio::test]
    async fn invalid_plugin_directory_is_an_error() -> anyhow::Result<()> {
        let file = tempfile::NamedTempFile::new()?;
        assert!(has_installed_plugins(file.path()).await.is_err());
        Ok(())
    }
}
