// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::sync::Arc;

use harmony_core::{
    CallerResolver,
    Global,
};
use mlua::Lua;

use super::lifecycle::resolve_caller_plugin_id;

pub(crate) fn plugin_globals() -> Vec<Global> {
    harmony_globals::plugin_log_globals()
}

pub(crate) fn caller_resolver() -> Arc<dyn CallerResolver> {
    Arc::new(LyraCallerResolver)
}

struct LyraCallerResolver;

impl CallerResolver for LyraCallerResolver {
    fn resolve(&self, lua: &Lua) -> Option<Arc<str>> {
        let plugin_id = resolve_caller_plugin_id(lua)?;
        Some(Arc::<str>::from(plugin_id.as_str()))
    }
}
