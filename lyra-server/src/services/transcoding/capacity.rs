// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! How many encodes may run at once. Every running FFmpeg encode, HLS or progressive, holds a
//! permit against `transcoding.max_concurrent` until it stops. Progressive encodes also count
//! against `transcoding.max_concurrent_per_user` for the [`Account`] they are charged to, and are
//! refused rather than queued when a limit is reached. Anonymous accounts, clients identified
//! only by their address, together get at most half the global pool, rounded down, so
//! authenticated users always keep at least the other half, and at least one encode, however
//! many addresses a client can rotate through. HLS jobs, shared across sessions and users, wait
//! a short while for a permit.

use std::{
    collections::HashMap,
    net::IpAddr,
    sync::{
        Arc,
        LazyLock,
        Mutex,
    },
    time::Duration,
};

use tokio::sync::Notify;

/// How long a refused request is told to wait before retrying.
pub(crate) const SATURATED_RETRY_AFTER: Duration = Duration::from_secs(5);
/// How long an HLS job waits for a permit before its request is refused.
const SHARED_WAIT: Duration = Duration::from_secs(10);

/// Whom a progressive encode is charged to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Account {
    User(String),
    /// An anonymous client, by its address key.
    Client(String),
    /// Anonymous, without even an address.
    Unknown,
}

impl Account {
    /// An anonymous client's account. IPv6 clients are grouped by their /64, the block one
    /// subscriber usually holds, so rotating addresses within it buys no extra allowance.
    pub(crate) fn client(client_key: &str) -> Self {
        let key = match client_key.parse::<IpAddr>() {
            Ok(IpAddr::V6(address)) => {
                let segments = address.segments();
                let prefix = std::net::Ipv6Addr::new(
                    segments[0],
                    segments[1],
                    segments[2],
                    segments[3],
                    0,
                    0,
                    0,
                    0,
                );
                format!("{prefix}/64")
            }
            _ => client_key.to_string(),
        };
        Self::Client(key)
    }

    fn is_anonymous(&self) -> bool {
        !matches!(self, Self::User(_))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Limits {
    /// `0` means unlimited.
    pub(crate) global: u32,
    /// `0` means unlimited.
    pub(crate) per_user: u32,
}

impl Limits {
    fn current() -> Self {
        let config = crate::STATE.config();
        Self {
            global: config.transcoding.max_concurrent,
            per_user: config.transcoding.max_concurrent_per_user,
        }
    }

    /// Whether anonymous accounts, running `active` encodes together, may start another. A pool
    /// of one leaves them none.
    fn anonymous_allows(self, active: usize) -> bool {
        self.global == 0 || active < (self.global / 2) as usize
    }
}

fn below(limit: u32, active: usize) -> bool {
    limit == 0 || active < limit as usize
}

#[derive(Default)]
struct Active {
    total: usize,
    anonymous: usize,
    by_account: HashMap<AccountKey, usize>,
}

/// An account as counted: a user, or an anonymous client (with `None` for no address).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum AccountKey {
    User(String),
    Client(Option<String>),
}

impl From<&Account> for AccountKey {
    fn from(account: &Account) -> Self {
        match account {
            Account::User(user) => Self::User(user.clone()),
            Account::Client(client) => Self::Client(Some(client.clone())),
            Account::Unknown => Self::Client(None),
        }
    }
}

#[derive(Default)]
pub(crate) struct TranscodeCapacity {
    active: Mutex<Active>,
    changed: Notify,
}

/// A running encode's claim on capacity, released when dropped.
pub(crate) struct TranscodePermit {
    capacity: Arc<TranscodeCapacity>,
    account: Option<AccountKey>,
}

impl Drop for TranscodePermit {
    fn drop(&mut self) {
        {
            let mut active = self.capacity.lock();
            active.total -= 1;
            if let Some(account) = &self.account {
                if matches!(account, AccountKey::Client(_)) {
                    active.anonymous -= 1;
                }
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

/// No permit was free: the server, the account, or anonymous callers together are at their limit.
#[derive(Debug, thiserror::Error)]
#[error("too many transcodes are running; retry shortly")]
pub(crate) struct Saturated;

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
                        account: None,
                    };
                }
            }
            changed.await;
        }
    }

    /// A permit for an encode charged to `account`, or [`Saturated`] when the global limit, the
    /// account's, or the anonymous accounts' together is reached.
    pub(crate) fn try_acquire(
        self: &Arc<Self>,
        limits: Limits,
        account: &Account,
    ) -> Result<TranscodePermit, Saturated> {
        let key = AccountKey::from(account);
        let mut active = self.lock();
        if !below(limits.global, active.total) {
            return Err(Saturated);
        }
        if account.is_anonymous() && !limits.anonymous_allows(active.anonymous) {
            return Err(Saturated);
        }
        let count = active.by_account.get(&key).copied().unwrap_or(0);
        if !below(limits.per_user, count) {
            return Err(Saturated);
        }
        *active.by_account.entry(key.clone()).or_default() += 1;
        active.total += 1;
        if account.is_anonymous() {
            active.anonymous += 1;
        }
        Ok(TranscodePermit {
            capacity: Arc::clone(self),
            account: Some(key),
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
        .map_err(|_| Saturated)
}

/// A permit for a progressive encode charged to `account`, without waiting.
pub(crate) fn try_acquire_for(account: &Account) -> Result<TranscodePermit, Saturated> {
    CAPACITY.try_acquire(Limits::current(), account)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{
        AtomicU32,
        Ordering,
    };

    fn limits(global: u32, per_user: u32) -> Limits {
        Limits { global, per_user }
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

        let first = capacity
            .try_acquire(limits, &Account::User("amy".into()))
            .expect("free");
        let second = capacity
            .try_acquire(limits, &Account::User("amy".into()))
            .expect("free");
        assert!(
            capacity
                .try_acquire(limits, &Account::User("amy".into()))
                .is_err()
        );
        let other = capacity
            .try_acquire(limits, &Account::User("bob".into()))
            .expect("free");
        assert!(
            capacity.try_acquire(limits, &Account::Unknown).is_err(),
            "the global limit binds anonymous encodes"
        );

        drop(first);
        let third = capacity
            .try_acquire(limits, &Account::User("amy".into()))
            .expect("freed");
        drop((second, third, other));
        let active = capacity.lock();
        assert_eq!(active.total, 0);
        assert!(active.by_account.is_empty());
    }

    #[test]
    fn users_keep_an_encode_however_small_the_pool() {
        let capacity = Arc::new(TranscodeCapacity::default());

        assert!(
            capacity
                .try_acquire(limits(1, 0), &Account::Unknown)
                .is_err(),
            "a pool of one is the users'"
        );
        let anonymous = capacity
            .try_acquire(limits(3, 0), &Account::Unknown)
            .expect("a pool of three spares one");
        assert!(
            capacity
                .try_acquire(limits(3, 0), &Account::Unknown)
                .is_err()
        );
        let users = [
            capacity
                .try_acquire(limits(3, 0), &Account::User("amy".into()))
                .expect("free"),
            capacity
                .try_acquire(limits(3, 0), &Account::User("bob".into()))
                .expect("free"),
        ];
        let unlimited = capacity
            .try_acquire(limits(0, 0), &Account::Unknown)
            .expect("an unlimited pool limits no one");
        drop((anonymous, users, unlimited));
    }

    #[test]
    fn anonymous_encodes_share_one_allowance() {
        let capacity = Arc::new(TranscodeCapacity::default());
        let limits = limits(10, 2);

        let held = [
            capacity
                .try_acquire(limits, &Account::Unknown)
                .expect("free"),
            capacity
                .try_acquire(limits, &Account::Unknown)
                .expect("free"),
        ];
        assert!(
            capacity.try_acquire(limits, &Account::Unknown).is_err(),
            "anonymous encodes are limited like one user's"
        );
        let user = capacity
            .try_acquire(limits, &Account::User("amy".into()))
            .expect("users are unaffected");
        drop((held, user));
        assert!(capacity.lock().by_account.is_empty());
    }

    #[tokio::test]
    async fn waiting_permits_share_the_global_limit_with_refusals() {
        let capacity = Arc::new(TranscodeCapacity::default());
        let limits = limits(1, 0);

        let hls = capacity.acquire(|| limits).await;
        assert!(
            capacity
                .try_acquire(limits, &Account::User("amy".into()))
                .is_err()
        );
        drop(hls);
        let progressive = capacity
            .try_acquire(limits, &Account::User("amy".into()))
            .expect("freed");
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
                .try_acquire(limits, &Account::client("2001:db8::1"))
                .expect("free"),
            capacity
                .try_acquire(limits, &Account::client("203.0.113.1"))
                .expect("free"),
        ];
        assert!(
            capacity
                .try_acquire(limits, &Account::client("203.0.113.2"))
                .is_err(),
            "anonymous clients never take more than half of the pool"
        );
        let user = capacity
            .try_acquire(limits, &Account::User("amy".into()))
            .expect("users keep the rest");
        drop((rotating, user));
        let active = capacity.lock();
        assert_eq!((active.total, active.anonymous), (0, 0));
    }

    #[test]
    fn ipv6_clients_are_grouped_by_their_64() {
        assert_eq!(
            Account::client("2001:db8:1:2:aaaa::1"),
            Account::client("2001:db8:1:2:bbbb::2")
        );
        assert_ne!(
            Account::client("2001:db8:1:2::1"),
            Account::client("2001:db8:1:3::1")
        );
        assert_eq!(
            Account::client("203.0.113.7"),
            Account::Client("203.0.113.7".to_string())
        );
    }
}
