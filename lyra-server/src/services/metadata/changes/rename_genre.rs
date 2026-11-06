// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Renaming a genre by hand, under a genre merge's access rule.

use agdb::{
    DbAnyTransactionMut,
    DbId,
};
use lyra_db::DbAccess;

use super::{
    ChangeContext,
    ChangeError,
};

/// A genre the caller may rename, and the name it goes by now.
struct RenameTarget {
    genre: DbId,
    current_name: String,
}

fn rename_target(
    db: &impl DbAccess,
    context: &ChangeContext,
    id: &str,
    name: &str,
) -> Result<RenameTarget, ChangeError> {
    if name.is_empty() {
        return Err(ChangeError::Invalid(
            "a genre's name can't be blank".to_string(),
        ));
    }
    let (genre, libraries) = context.reached(db, id)?;
    if lyra_db::graph::element_kind(db, genre)?.as_deref() != Some("Genre") {
        return Err(ChangeError::entity_not_found(id));
    }
    context.require_every_library(&libraries)?;
    context.check_runs(&libraries)?;
    let current_name = lyra_db::genres::get_by_id(db, genre)?
        .ok_or_else(|| anyhow::anyhow!("genre {} missing", genre.0))?
        .name;
    Ok(RenameTarget {
        genre,
        current_name,
    })
}

/// What the data itself allows of the rename: a name of its own, and a new one.
fn check_rename(db: &impl DbAccess, target: &RenameTarget, name: &str) -> Result<(), ChangeError> {
    if lyra_db::genres::name_taken(db, name, target.genre)? {
        return Err(ChangeError::Conflict(format!(
            "another genre goes by {name}"
        )));
    }
    if target.current_name == name {
        return Err(ChangeError::Invalid("rename has no effect".to_string()));
    }
    Ok(())
}

/// The name the genre `id` goes by, which a rename to `name` would replace.
pub(super) fn preview(
    db: &impl DbAccess,
    context: &ChangeContext,
    id: &str,
    name: &str,
) -> Result<String, ChangeError> {
    let name = name.trim();
    let target = rename_target(db, context, id, name)?;
    check_rename(db, &target, name)?;
    Ok(target.current_name)
}

/// Renames the genre `id`. Returns the name it now goes by.
pub(super) fn apply(
    t: &mut DbAnyTransactionMut<'_>,
    context: &ChangeContext,
    id: &str,
    name: &str,
    expected: &str,
) -> Result<String, ChangeError> {
    let name = name.trim();
    let target = rename_target(t, context, id, name)?;
    if target.current_name != expected {
        return Err(ChangeError::stale(
            "the genre's name changed after preview",
            &target.current_name,
        )?);
    }
    check_rename(t, &target, name)?;
    lyra_db::genres::rename(t, target.genre, name)?;
    Ok(name.to_string())
}
