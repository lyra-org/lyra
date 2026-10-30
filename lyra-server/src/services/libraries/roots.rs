// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::path::PathBuf;

use anyhow::{
    Context,
    bail,
};

use crate::{
    services::STATE,
    services::{
        auth::Principal,
        catalog::Viewer,
    },
};

/// Root directories of the libraries `principal` can see, or of every library without one.
async fn library_roots(principal: Option<&Principal>) -> anyhow::Result<Vec<PathBuf>> {
    let db = STATE.db.read().await;
    let visible = Viewer::of(&*db, principal.cloned())?.visible_libraries(&*db)?;
    Ok(lyra_db::libraries::get(&*db)?
        .into_iter()
        .filter(|library| {
            visible
                .as_ref()
                .is_none_or(|ids| library.db_id.is_some_and(|id| ids.contains(&id)))
        })
        .map(|library| library.path)
        .collect())
}

/// Resolves `path`, following symlinks, and refuses anything outside the roots
/// of the libraries `principal` can see and the configured cover storage root.
/// Plugins may name any path, so every file they have the server read goes
/// through here.
pub(crate) async fn confine_to_media_roots(
    path: &str,
    principal: Option<&Principal>,
) -> anyhow::Result<PathBuf> {
    let canonical = tokio::fs::canonicalize(path)
        .await
        .with_context(|| format!("failed to resolve path '{path}'"))?;
    let mut roots = library_roots(principal).await?;
    roots.push(crate::services::covers::configured_covers_root());
    for root in roots {
        let Ok(root) = tokio::fs::canonicalize(&root).await else {
            continue;
        };
        if canonical.starts_with(&root) {
            return Ok(canonical);
        }
    }
    bail!("path '{path}' is outside library and cover roots");
}
