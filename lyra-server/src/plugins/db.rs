// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

pub(crate) use crate::db::MetadataField;
pub(crate) use crate::db::metadata::layers::{
    LayerFields,
    normalize_value,
};
pub(crate) use crate::db::plugin::*;

pub(crate) fn manual_metadata_owns_field(
    db: &impl crate::db::DbAccess,
    entity_id: agdb::DbId,
    field: MetadataField,
) -> anyhow::Result<bool> {
    crate::db::metadata::manual_overrides::owns_field(db, entity_id, field)
}

#[cfg(test)]
pub(crate) use crate::db::{
    playbacks,
    playlists,
    test_db,
};

#[derive(Clone, Default)]
pub(crate) struct PluginDb {
    db: Option<DbAsync>,
}

impl PluginDb {
    #[cfg(test)]
    pub(crate) fn empty() -> Self {
        Self { db: None }
    }

    pub(crate) fn with_db(db: DbAsync) -> Self {
        Self { db: Some(db) }
    }
}

/// The executor's raw database handle. Library reads go through [`scoped`] or [`system`].
pub(crate) fn get(vm: &harmony_luau::Vm) -> harmony_luau::runtime::Result<DbAsync> {
    get_optional(vm)?.ok_or_else(|| {
        crate::plugins::runtime_error("this module requires a database-backed plugin executor")
    })
}

pub(crate) fn get_optional(
    vm: &harmony_luau::Vm,
) -> harmony_luau::runtime::Result<Option<DbAsync>> {
    Ok(vm.data().get::<PluginDb>()?.db.clone())
}

/// The executor's database with the dispatch principal, so reads see only what the caller can.
pub(crate) struct ViewerDb {
    db: DbAsync,
    principal: Option<crate::services::auth::Principal>,
}

pub(crate) fn scoped(
    frame: &harmony_luau::AsyncCallFrame<'_>,
) -> harmony_luau::runtime::Result<ViewerDb> {
    Ok(ViewerDb {
        db: get(frame.vm)?,
        principal: crate::plugins::auth::dispatch_principal(&frame.context)?,
    })
}

/// The executor's database read as the system, which sees every library and playlist, for host
/// calls documented as unscoped.
pub(crate) fn system(
    frame: &harmony_luau::AsyncCallFrame<'_>,
) -> harmony_luau::runtime::Result<ViewerDb> {
    Ok(ViewerDb {
        db: get(frame.vm)?,
        principal: None,
    })
}

impl ViewerDb {
    /// A read guard and the caller's viewer, verified under it.
    pub(crate) async fn read(
        &self,
    ) -> harmony_luau::runtime::Result<(
        tokio::sync::OwnedRwLockReadGuard<agdb::DbAny>,
        crate::services::catalog::Viewer,
    )> {
        let db = self.db.clone().read_owned().await;
        let viewer = self.viewer(&db)?;
        Ok((db, viewer))
    }

    /// The caller's viewer, verified under the guard `db` was read through.
    pub(crate) fn viewer(
        &self,
        db: &agdb::DbAny,
    ) -> harmony_luau::runtime::Result<crate::services::catalog::Viewer> {
        crate::services::catalog::Viewer::of(db, self.principal.clone())
            .map_err(crate::plugins::runtime_error)
    }

    /// The same database, for writes that follow a scoped read.
    pub(crate) fn handle(&self) -> DbAsync {
        self.db.clone()
    }
}
