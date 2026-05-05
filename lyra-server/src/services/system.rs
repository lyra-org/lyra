// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SystemContext {
    _private: (),
}

impl SystemContext {
    pub(in crate::services::system) const fn new() -> Self {
        Self { _private: () }
    }
}

#[allow(dead_code)]
pub(crate) const fn ensure_default_user_context() -> SystemContext {
    SystemContext::new()
}

#[allow(dead_code)]
pub(crate) const fn library_sync_context() -> SystemContext {
    SystemContext::new()
}

#[allow(dead_code)]
pub(crate) const fn cleanup_evicted_playbacks_context() -> SystemContext {
    SystemContext::new()
}
