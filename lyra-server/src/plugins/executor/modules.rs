// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use anyhow::{
    Result,
    bail,
};
use harmony_core::{
    GatedModule,
    ModuleSpec,
};

/// Every gated module, with each override replacing the module of the same
/// id while keeping its scope.
pub(super) fn generic_modules(module_overrides: Vec<ModuleSpec>) -> Result<Vec<GatedModule>> {
    let mut modules = crate::plugins::scopes::gated_modules();
    for override_spec in module_overrides {
        let Some(existing) = modules
            .iter_mut()
            .find(|module| module.spec.id == override_spec.id)
        else {
            bail!("module override {} replaces no module", override_spec.id.0);
        };
        existing.spec = override_spec;
    }
    Ok(modules)
}
