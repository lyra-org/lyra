// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use anyhow::Result;
use harmony_luau as luau;

pub(super) struct PluginModuleStores {
    pub(super) server_settings: Option<crate::services::state::SettingsHandle>,
    db: crate::plugins::db::PluginDb,
    scope: crate::plugins::state::VmScope,
}

impl PluginModuleStores {
    #[cfg(test)]
    pub(super) fn empty() -> Self {
        Self {
            server_settings: None,
            db: crate::plugins::db::PluginDb::empty(),
            scope: published_or_new(),
        }
    }

    pub(super) fn with_db(db: lyra_db::DbAsync) -> Self {
        Self {
            server_settings: None,
            db: crate::plugins::db::PluginDb::with_db(db),
            scope: Default::default(),
        }
    }

    /// For an executor a test drives directly, with no handle to publish: it registers into the
    /// scope the test reads.
    #[cfg(test)]
    pub(super) fn sharing_published_scope(mut self) -> Self {
        self.scope = published_or_new();
        self
    }

    pub(super) fn scope(&self) -> crate::plugins::state::VmScope {
        self.scope.clone()
    }

    pub(super) fn install_into(self, vm: &luau::Vm) -> Result<()> {
        vm.data().insert(self.db)?;
        vm.data().insert(self.scope)?;
        Ok(())
    }
}

#[cfg(test)]
fn published_or_new() -> crate::plugins::state::VmScope {
    crate::services::STATE
        .get()
        .and_then(|_| crate::plugins::STATE.vm_scope())
        .unwrap_or_default()
}
