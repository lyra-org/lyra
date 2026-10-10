// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use anyhow::Result;
use harmony_luau as luau;

pub(super) struct PluginModuleStores {
    pub(super) server_settings: Option<crate::SettingsHandle>,
    db: crate::plugins::db::PluginDb,
}

impl PluginModuleStores {
    #[cfg(test)]
    pub(super) fn empty() -> Self {
        Self {
            server_settings: None,
            db: crate::plugins::db::PluginDb::empty(),
        }
    }

    pub(super) fn with_db(db: crate::plugins::db::DbAsync) -> Self {
        Self {
            server_settings: None,
            db: crate::plugins::db::PluginDb::with_db(db),
        }
    }

    pub(super) fn install_into(self, vm: &luau::Vm) -> Result<()> {
        vm.data().insert(self.db)?;
        Ok(())
    }
}
