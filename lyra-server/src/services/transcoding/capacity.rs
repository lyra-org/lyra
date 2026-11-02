// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! How many encodes may run at once. Every running FFmpeg encode, HLS or progressive, holds a
//! permit against `transcoding.max_concurrent` until it stops. Progressive encodes also count
//! against `transcoding.max_concurrent_per_user` for the [`Caller`] they are charged to, and are
//! refused rather than queued when a limit is reached. Anonymous callers together get at most
//! half the global pool, rounded down, so authenticated users always keep at least the other
//! half, and at least one encode. HLS jobs, shared across sessions and users, wait a short while
//! for a permit.

use std::{
    collections::HashMap,
    sync::{
        Arc,
        LazyLock,
        Mutex,
    },
    time::Duration,
};

use tokio::sync::Notify;

use crate::services::auth::caller::{
    Account,
    Caller,
};

/// How long a refused request is told to wait before retrying.
pub(crate) const SATURATED_RETRY_AFTER: Duration = Duration::from_secs(5);
/// How long an HLS job waits for a permit before its request is refused.
const SHARED_WAIT: Duration = Duration::from_secs(10);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Limits {
    /// `0` means unlimited.
    pub(crate) global: u32,
    /// `0` means unlimited.
    pub(crate) per_user: u32,
}

impl Limits {
    fn current() -> Self {
        let config = crate::services::STATE.config();
        Self {
            global: config.transcoding.max_concurrent,
            per_user: config.transcoding.max_concurrent_per_user,
        }
    }

    /// Whether `account`, running `active` encodes, may start another.
    fn allow(self, account: &Account, active: usize) -> bool {
        match account {
            Account::Caller(_) => below(self.per_user, active),
            // A pool of one leaves anonymous callers none.
            Account::Anonymous => self.global == 0 || active < (self.global / 2) as usize,
        }
    }
}

fn below(limit: u32, active: usize) -> bool {
    limit == 0 || active < limit as usize
}

#[derive(Default)]
struct Active {
    total: usize,
    by_account: HashMap<Account, usize>,
}

#[derive(Default)]
pub(crate) struct TranscodeCapacity {
    active: Mutex<Active>,
    changed: Notify,
}

/// A running encode's claim on capacity, released when dropped.
pub(crate) struct TranscodePermit {
    capacity: Arc<TranscodeCapacity>,
    accounts: Vec<Account>,
}

impl TranscodePermit {
    pub(crate) fn is_charged_to(&self, caller: &Caller) -> bool {
        self.accounts
            .iter()
            .any(|account| matches!(account, Account::Caller(charged) if charged == caller))
    }
}

impl Drop for TranscodePermit {
    fn drop(&mut self) {
        {
            let mut active = self.capacity.lock();
            active.total -= 1;
            for account in &self.accounts {
                if let Some(count) = active.by_account.get_mut(account) {
                    *count -= 1;
                    if *count == 0 {
                        active.by_account.remove(account);
                    }
                }
            }
        }
        self.capacity.changed.notify_waiters();
    }
}

/// No permit was free: the server, the caller, or anonymous callers together are at their limit.
#[derive(Debug, Default, thiserror::Error)]
#[error("too many transcodes are running; retry shortly")]
pub(crate) struct Saturated {
    /// The caller, when it is a user and its own limit refused it. Anonymous callers can share an
    /// address, so their encodes are never anyone's own.
    pub(crate) at_own_limit: Option<Caller>,
}

static CAPACITY: LazyLock<Arc<TranscodeCapacity>> =
    LazyLock::new(|| Arc::new(TranscodeCapacity::default()));

impl TranscodeCapacity {
    fn lock(&self) -> std::sync::MutexGuard<'_, Active> {
        self.active.lock().expect("transcode capacity poisoned")
    }

    /// Waits for a permit under the global limit, rereading it whenever capacity changes.
    pub(crate) async fn acquire(self: &Arc<Self>, limits: impl Fn() -> Limits) -> TranscodePermit {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let mut active = self.lock();
                if below(limits().global, active.total) {
                    active.total += 1;
                    return TranscodePermit {
                        capacity: Arc::clone(self),
                        accounts: Vec::new(),
                    };
                }
            }
            changed.await;
        }
    }

    /// A permit for an encode charged to `caller`, or [`Saturated`] when the global limit, the
    /// caller's, or the anonymous callers' together is reached.
    pub(crate) fn try_acquire(
        self: &Arc<Self>,
        limits: Limits,
        caller: &Caller,
    ) -> Result<TranscodePermit, Saturated> {
        let accounts = caller.accounts().collect::<Vec<_>>();
        let mut active = self.lock();
        for account in &accounts {
            let count = active.by_account.get(account).copied().unwrap_or(0);
            if !limits.allow(account, count) {
                return Err(Saturated {
                    at_own_limit: matches!(account, Account::Caller(Caller::User(_)))
                        .then(|| caller.clone()),
                });
            }
        }
        if !below(limits.global, active.total) {
            return Err(Saturated::default());
        }
        for account in &accounts {
            *active.by_account.entry(account.clone()).or_default() += 1;
        }
        active.total += 1;
        Ok(TranscodePermit {
            capacity: Arc::clone(self),
            accounts,
        })
    }
}

/// Wakes encodes waiting for a permit, after the limits change.
pub(crate) fn notify_changed() {
    CAPACITY.changed.notify_waiters();
}

/// Waits a short while for a permit for an HLS job, refusing it when none frees up.
pub(crate) async fn acquire_shared() -> Result<TranscodePermit, Saturated> {
    tokio::time::timeout(SHARED_WAIT, CAPACITY.acquire(Limits::current))
        .await
        .map_err(|_| Saturated::default())
}

/// A permit for a progressive encode charged to `caller`, without waiting.
pub(crate) fn try_acquire_for(caller: &Caller) -> Result<TranscodePermit, Saturated> {
    CAPACITY.try_acquire(Limits::current(), caller)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::auth::caller::ClientAddress;
    use std::sync::atomic::{
        AtomicU32,
        Ordering,
    };

    fn limits(global: u32, per_user: u32) -> Limits {
        Limits { global, per_user }
    }

    fn user(name: &str) -> Caller {
        Caller::User(name.to_string())
    }

    fn client(raw: &str) -> Caller {
        Caller::Client(ClientAddress::new(raw.parse().expect("valid ip")))
    }

    fn unknown() -> Caller {
        Caller::Client(ClientAddress::default())
    }

    #[tokio::test]
    async fn waiting_permits_track_jobs_across_limit_changes() {
        let capacity = Arc::new(TranscodeCapacity::default());
        let limit = AtomicU32::new(0);
        let current = || limits(limit.load(Ordering::Relaxed), 0);
        let first = capacity.acquire(current).await;
        let second = capacity.acquire(current).await;
        limit.store(1, Ordering::Relaxed);
        let queued = capacity.acquire(current);
        tokio::pin!(queued);
        assert!(futures::poll!(queued.as_mut()).is_pending());
        drop(first);
        assert!(futures::poll!(queued.as_mut()).is_pending());
        drop(second);
        let third = queued.await;

        {
            let cancelled = capacity.acquire(current);
            tokio::pin!(cancelled);
            assert!(futures::poll!(cancelled.as_mut()).is_pending());
        }
        assert_eq!(capacity.lock().total, 1);
        let queued = capacity.acquire(current);
        tokio::pin!(queued);
        assert!(futures::poll!(queued.as_mut()).is_pending());
        limit.store(2, Ordering::Relaxed);
        capacity.changed.notify_waiters();
        let fourth = queued.await;
        let queued = capacity.acquire(current);
        tokio::pin!(queued);
        assert!(futures::poll!(queued.as_mut()).is_pending());
        limit.store(0, Ordering::Relaxed);
        capacity.changed.notify_waiters();
        let fifth = queued.await;
        assert_eq!(capacity.lock().total, 3);
        drop((third, fourth, fifth));
        assert_eq!(capacity.lock().total, 0);
    }

    #[test]
    fn refusals_apply_the_global_and_per_user_limits() {
        let capacity = Arc::new(TranscodeCapacity::default());
        let limits = limits(3, 2);

        let first = capacity.try_acquire(limits, &user("amy")).expect("free");
        let second = capacity.try_acquire(limits, &user("amy")).expect("free");
        assert!(
            capacity
                .try_acquire(limits, &user("amy"))
                .is_err_and(|refused| refused.at_own_limit == Some(user("amy")))
        );
        let other = capacity.try_acquire(limits, &user("bob")).expect("free");
        assert!(
            capacity
                .try_acquire(limits, &unknown())
                .is_err_and(|refused| refused.at_own_limit.is_none()),
            "the global limit binds anonymous encodes"
        );

        drop(first);
        let third = capacity.try_acquire(limits, &user("amy")).expect("freed");
        drop((second, third, other));
        let active = capacity.lock();
        assert_eq!(active.total, 0);
        assert!(active.by_account.is_empty());
    }

    #[test]
    fn anonymous_encodes_share_one_allowance() {
        let capacity = Arc::new(TranscodeCapacity::default());
        let limits = limits(10, 2);

        let held = [
            capacity.try_acquire(limits, &unknown()).expect("free"),
            capacity.try_acquire(limits, &unknown()).expect("free"),
        ];
        assert!(
            capacity
                .try_acquire(limits, &unknown())
                .is_err_and(|refused| refused.at_own_limit.is_none()),
            "anonymous encodes are limited like one user's"
        );
        let user = capacity
            .try_acquire(limits, &user("amy"))
            .expect("users are unaffected");
        drop((held, user));
        assert!(capacity.lock().by_account.is_empty());
    }

    #[tokio::test]
    async fn waiting_permits_share_the_global_limit_with_refusals() {
        let capacity = Arc::new(TranscodeCapacity::default());
        let limits = limits(1, 0);

        let hls = capacity.acquire(|| limits).await;
        assert!(capacity.try_acquire(limits, &user("amy")).is_err());
        drop(hls);
        let progressive = capacity.try_acquire(limits, &user("amy")).expect("freed");
        let queued = capacity.acquire(|| limits);
        tokio::pin!(queued);
        assert!(futures::poll!(queued.as_mut()).is_pending());
        drop(progressive);
        drop(queued.await);
    }

    #[test]
    fn anonymous_clients_together_get_half_the_pool() {
        let capacity = Arc::new(TranscodeCapacity::default());
        let limits = limits(4, 3);

        let rotating = [
            capacity
                .try_acquire(limits, &client("2001:db8::1"))
                .expect("free"),
            capacity
                .try_acquire(limits, &client("203.0.113.1"))
                .expect("free"),
        ];
        assert!(
            capacity
                .try_acquire(limits, &client("203.0.113.2"))
                .is_err_and(|refused| refused.at_own_limit.is_none()),
            "anonymous clients never take more than half of the pool"
        );
        let user = capacity
            .try_acquire(limits, &user("amy"))
            .expect("users keep the rest");
        drop((rotating, user));
        let active = capacity.lock();
        assert_eq!(active.total, 0);
        assert!(active.by_account.is_empty());
    }

    #[test]
    fn users_keep_an_encode_however_small_the_pool() {
        let capacity = Arc::new(TranscodeCapacity::default());

        assert!(
            capacity.try_acquire(limits(1, 0), &unknown()).is_err(),
            "a pool of one is the users'"
        );
        let anonymous = capacity
            .try_acquire(limits(3, 0), &unknown())
            .expect("a pool of three spares one");
        assert!(capacity.try_acquire(limits(3, 0), &unknown()).is_err());
        let users = [
            capacity
                .try_acquire(limits(3, 0), &user("amy"))
                .expect("free"),
            capacity
                .try_acquire(limits(3, 0), &user("bob"))
                .expect("free"),
        ];
        let unlimited = capacity
            .try_acquire(limits(0, 0), &unknown())
            .expect("an unlimited pool limits no one");
        drop((anonymous, users, unlimited));
    }

    #[test]
    fn ipv6_clients_share_an_allowance_across_their_64() {
        let capacity = Arc::new(TranscodeCapacity::default());
        let limits = limits(10, 1);

        let held = capacity
            .try_acquire(limits, &client("2001:db8:1:2::1"))
            .expect("free");
        assert!(
            capacity
                .try_acquire(limits, &client("2001:db8:1:2:ffff::2"))
                .is_err(),
            "another address in the same /64 is the same client"
        );
        let elsewhere = capacity
            .try_acquire(limits, &client("2001:db8:1:3::1"))
            .expect("another /64 is another client");
        drop((held, elsewhere));
    }
}
