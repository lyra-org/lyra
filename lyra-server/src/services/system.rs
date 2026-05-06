// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SystemContext {
    _private: (),
}

impl SystemContext {
    pub(in crate::services::system) const fn new() -> Self {
        Self { _private: () }
    }
}
pub(crate) const fn library_sync_context() -> SystemContext {
    SystemContext::new()
}
