// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::path::PathBuf;

use crate::{
    STATE,
    db,
};

/// Current library root directories, read live from the database.
pub(crate) async fn library_roots() -> anyhow::Result<Vec<PathBuf>> {
    let db = STATE.db.read().await;
    Ok(db::libraries::get(&*db)?
        .into_iter()
        .map(|library| library.path)
        .collect())
}
