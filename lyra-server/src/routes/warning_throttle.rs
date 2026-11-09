// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Holds a warning a client can provoke to one a minute.

use std::time::{
    Duration,
    Instant,
};

const WARNING_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Default)]
pub(super) struct WarningThrottle {
    last: Option<Instant>,
}

impl WarningThrottle {
    pub(super) const fn new() -> Self {
        Self { last: None }
    }

    pub(super) fn allows(&mut self, now: Instant) -> bool {
        if self
            .last
            .is_some_and(|last| now.duration_since(last) < WARNING_INTERVAL)
        {
            return false;
        }
        self.last = Some(now);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn warnings_are_held_to_one_per_interval() {
        let mut throttle = WarningThrottle::default();
        let start = Instant::now();
        assert!(throttle.allows(start));
        assert!(!throttle.allows(start + WARNING_INTERVAL - Duration::from_secs(1)));
        assert!(throttle.allows(start + WARNING_INTERVAL));
        assert!(!throttle.allows(start + WARNING_INTERVAL));
    }
}
