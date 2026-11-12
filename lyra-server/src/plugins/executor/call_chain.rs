// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    sync::Arc,
    time::Instant,
};

use harmony_core::{
    ResumeLimit,
    luau::ThreadDriveOptions,
};
use harmony_luau::CallContext;

use crate::services::plugin_dispatch::INTERACTIVE_TRANSITION_WAIT;
use crate::services::plugin_scope::PluginId;

/// The handler call a nested call belongs to: how long it counts, how long what it calls may
/// wait, and the plugins already running in it. Set where a handler starts and inherited through
/// `CallContext.caller` by what it calls. Its times are by the executor's clock, [`super::now`].
#[derive(Clone)]
pub(crate) struct CallChain {
    /// When the handler the chain began with must have ended. `None` for a task no handler
    /// answers for.
    deadline: Option<Instant>,
    valid_until: Instant,
    /// `None` when nothing in the chain may wait.
    wait_until: Option<Instant>,
    admitted: Arc<[PluginId]>,
}

impl CallChain {
    /// A handler starting on the executor, which has until `deadline`.
    pub(crate) fn root(deadline: Instant, plugin: Option<Arc<str>>) -> Self {
        Self {
            deadline: Some(deadline),
            valid_until: deadline,
            wait_until: Some(deadline),
            admitted: plugin.map(PluginId::validated).into_iter().collect(),
        }
    }

    /// An update callback, driven to completion on the executor thread under `options` from
    /// `now`: nothing it calls may wait, or it would hold up what it waits for.
    pub(crate) fn synchronous(
        options: &ThreadDriveOptions,
        plugin: Option<Arc<str>>,
        now: Instant,
    ) -> Self {
        Self {
            deadline: Some(now + options.timeout),
            valid_until: now + options.timeout,
            wait_until: None,
            admitted: plugin.map(PluginId::validated).into_iter().collect(),
        }
    }

    /// What `context` runs in at `now`. A chain counts only until `valid_until`, since a detached
    /// task keeps the chain it was spawned from after that chain's handlers end. A Luau task with
    /// no chain in force gets the interactive budget, and is admitted to its own plugin only.
    pub(crate) fn of(context: &CallContext, now: Instant) -> Self {
        let chain = context
            .caller
            .get::<Self>()
            .ok()
            .filter(|chain| now < chain.valid_until)
            .map_or_else(
                || Self {
                    deadline: None,
                    ..Self::root(now + INTERACTIVE_TRANSITION_WAIT, None)
                },
                |chain| (*chain).clone(),
            );
        chain.admitting(context.origin.plugin.as_ref())
    }

    /// This chain as a handler of `plugin` runs in it.
    pub(crate) fn admitting(&self, plugin: Option<&Arc<str>>) -> Self {
        match plugin {
            Some(plugin) if !self.admits(plugin) => Self {
                deadline: self.deadline,
                valid_until: self.valid_until,
                wait_until: self.wait_until,
                admitted: self
                    .admitted
                    .iter()
                    .cloned()
                    .chain([PluginId::validated(plugin.clone())])
                    .collect(),
            },
            _ => self.clone(),
        }
    }

    /// Until when a nested call made at `now` may wait for a transition.
    pub(crate) fn wait_deadline(&self, now: Instant) -> Instant {
        self.wait_until.unwrap_or(now)
    }

    /// When the handler the chain began with must have ended, which is also as long as anything
    /// it calls may run.
    pub(crate) fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    /// `limit` for a handler called in this chain: none of its resumes runs past the chain's
    /// deadline either.
    pub(crate) fn limit_resumes(&self, limit: ResumeLimit) -> ResumeLimit {
        match self.deadline {
            Some(deadline) => limit.not_past(deadline),
            None => limit,
        }
    }

    pub(crate) fn admits(&self, plugin: &str) -> bool {
        self.admitted
            .iter()
            .any(|admitted| admitted.as_str() == plugin)
    }

    pub(crate) fn admitted(&self) -> &[PluginId] {
        &self.admitted
    }
}
