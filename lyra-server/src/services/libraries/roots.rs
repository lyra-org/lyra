// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::path::PathBuf;

use crate::{
    STATE,
    db,
    services::{
        auth::Principal,
        catalog::Viewer,
    },
};

/// Root directories of the libraries `principal` can see, or of every library without one.
pub(crate) async fn library_roots(principal: Option<&Principal>) -> anyhow::Result<Vec<PathBuf>> {
    let db = STATE.db.read().await;
    let visible = Viewer::of(&*db, principal.cloned())?.visible_libraries(&*db)?;
    Ok(db::libraries::get(&*db)?
        .into_iter()
        .filter(|library| {
            visible
                .as_ref()
                .is_none_or(|ids| library.db_id.is_some_and(|id| ids.contains(&id)))
        })
        .map(|library| library.path)
        .collect())
}
