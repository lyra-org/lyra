// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use lyra_db::DbAsync;

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

/// The executor's raw database handle. Library reads go through [`ViewerDb`] or [`SystemDb`].
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

/// [`get`], as a module function takes it from its call.
pub(crate) struct Db(DbAsync);

impl std::ops::Deref for Db {
    type Target = DbAsync;

    fn deref(&self) -> &DbAsync {
        &self.0
    }
}

impl harmony_luau::FromCallFrame for Db {
    fn from_call_frame(
        vm: &harmony_luau::Vm,
        _context: &harmony_luau::CallContext,
    ) -> harmony_luau::runtime::Result<Self> {
        get(vm).map(Self)
    }
}

/// [`get_optional`], as a module function takes it from its call.
pub(crate) struct OptionalDb(pub(crate) Option<DbAsync>);

impl harmony_luau::FromCallFrame for OptionalDb {
    fn from_call_frame(
        vm: &harmony_luau::Vm,
        _context: &harmony_luau::CallContext,
    ) -> harmony_luau::runtime::Result<Self> {
        get_optional(vm).map(Self)
    }
}

/// The executor's database with the dispatch principal, so reads see only what the caller can.
pub(crate) struct ViewerDb {
    db: DbAsync,
    principal: Option<crate::services::auth::Principal>,
}

impl harmony_luau::FromCallFrame for ViewerDb {
    fn from_call_frame(
        vm: &harmony_luau::Vm,
        context: &harmony_luau::CallContext,
    ) -> harmony_luau::runtime::Result<Self> {
        Ok(Self {
            db: get(vm)?,
            principal: crate::plugins::auth::dispatch_principal(context)?,
        })
    }
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

/// The executor's database read as the system, which sees every library and playlist, for host
/// calls documented as unscoped.
pub(crate) struct SystemDb(pub(crate) ViewerDb);

impl std::ops::Deref for SystemDb {
    type Target = ViewerDb;

    fn deref(&self) -> &ViewerDb {
        &self.0
    }
}

impl harmony_luau::FromCallFrame for SystemDb {
    fn from_call_frame(
        vm: &harmony_luau::Vm,
        _context: &harmony_luau::CallContext,
    ) -> harmony_luau::runtime::Result<Self> {
        Ok(Self(ViewerDb {
            db: get(vm)?,
            principal: None,
        }))
    }
}
