// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::HashSet,
    sync::Arc,
};

use anyhow::Result;
use harmony_core::{
    LuauRequireRuntime,
    ModuleSpec,
};

pub(super) fn register_generic_modules(
    require: &LuauRequireRuntime,
    module_overrides: Vec<ModuleSpec>,
) -> Result<()> {
    let mut specs = crate::plugins::module_specs();
    for override_spec in module_overrides {
        if let Some(existing) = specs.iter_mut().find(|spec| spec.id == override_spec.id) {
            *existing = override_spec;
        } else {
            specs.push(override_spec);
        }
    }

    for spec in specs {
        require.register(spec)?;
    }
    Ok(())
}

pub(super) fn plugin_scope_ids() -> HashSet<Arc<str>> {
    crate::plugins::module_scope_ids()
}

pub(crate) fn plugin_scope_ids_for_test() -> Vec<String> {
    let mut scopes = plugin_scope_ids()
        .into_iter()
        .map(|scope| scope.to_string())
        .collect::<Vec<_>>();
    scopes.sort();
    scopes
}
