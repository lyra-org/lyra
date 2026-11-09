// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Bounds the wrong passwords one account name can take, however many addresses they come from.
//! A check reserves a token before it hashes and gets it back only when the password was right.
//! An address a user logged in from lately checks that user's password without reserving, so
//! guesses from elsewhere cannot lock the user out of where they already are.

use std::{
    collections::HashMap,
    hash::{
        BuildHasher,
        RandomState,
    },
    sync::{
        LazyLock,
        Mutex,
    },
    time::{
        Duration,
        Instant,
    },
};

use super::{
    Bucket,
    BucketKey,
    LimiterState,
    Policy,
};
use crate::services::auth::caller::ClientAddress;

const PER_MINUTE: u32 = 1;
const BURST: u32 = 10;
/// The idle cleanup forgets no failures: a bucket it drops has refilled by then.
const _: () = assert!(BURST as u64 * 60 <= PER_MINUTE as u64 * super::IDLE_BUCKET_TTL.as_secs());
const MAX_NAME_BUCKETS: usize = 8192;
const KNOWN_ADDRESS_TTL: Duration = Duration::from_secs(30 * 24 * 60 * 60);
const MAX_KNOWN_ADDRESSES: usize = 1024;

static LIMITER: LazyLock<Mutex<PasswordCheckLimiter>> =
    LazyLock::new(|| Mutex::new(PasswordCheckLimiter::new()));

fn enabled() -> bool {
    crate::services::STATE
        .startup_config
        .get()
        .rate_limit
        .enabled
}

fn limiter() -> std::sync::MutexGuard<'static, PasswordCheckLimiter> {
    LIMITER
        .lock()
        .expect("password check limiter state poisoned")
}

/// Admits one check of a password submitted for `username`, before any hashing. `user_id` is the
/// user the name resolved to, if any; an unknown name is admitted and charged like a real one.
pub(crate) fn admit(
    username: &str,
    user_id: Option<&str>,
    client: ClientAddress,
) -> Result<Admission, Duration> {
    if !enabled() {
        return Ok(Admission(None));
    }
    limiter()
        .admit(username, user_id, client, Instant::now())
        .map(Admission)
}

/// An admitted password check. Dropping it leaves the attempt charged.
#[must_use]
pub(crate) struct Admission(Option<NameKey>);

impl Admission {
    /// Gives the attempt back, for a password that was right.
    pub(crate) fn refund(self) {
        if let Some(name) = self.0 {
            limiter().refund(name);
        }
    }
}

/// Records that the user proved their password from `client`, so their next checks from there are
/// not charged.
pub(crate) fn remember_login(user_id: &str, client: ClientAddress) {
    if enabled() {
        limiter().known.remember(user_id, client, Instant::now());
    }
}

/// Forgets where the user logged in from, as a password change must.
pub(crate) fn forget_logins(user_id: &str) {
    limiter().known.forget_user(user_id);
}

#[cfg(test)]
pub(crate) fn reset() {
    *limiter() = PasswordCheckLimiter::new();
}

/// Charges `attempts` wrong passwords to `username`, each of which must be admitted.
#[cfg(test)]
pub(crate) fn spend(username: &str, attempts: u32) {
    for _ in 0..attempts {
        let _charged = admit(username, None, ClientAddress::default()).expect("attempt admitted");
    }
}

/// An address no other test logs in from, so the per-address limit stays out of the way.
#[cfg(test)]
pub(crate) fn fresh_address() -> ClientAddress {
    static NEXT: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0x0a00_0001);
    let next = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    ClientAddress::new(std::net::IpAddr::V4(next.into()))
}

type NameKey = u64;

struct PasswordCheckLimiter {
    policy: Policy,
    names: LimiterState,
    /// Keyed per process, so nobody can pick names that share a bucket with another's.
    name_hasher: RandomState,
    known: KnownAddresses,
}

impl PasswordCheckLimiter {
    fn new() -> Self {
        Self {
            policy: Policy::new(PER_MINUTE, BURST).expect("the name policy is enabled"),
            names: LimiterState::new(),
            name_hasher: RandomState::new(),
            known: KnownAddresses::default(),
        }
    }

    /// The key of names `lyra_db::users::get_by_username` reads as one.
    fn name_key(&self, username: &str) -> NameKey {
        self.name_hasher.hash_one(username.trim().to_lowercase())
    }

    /// `None` is an exempt check, which reserved nothing.
    fn admit(
        &mut self,
        username: &str,
        user_id: Option<&str>,
        client: ClientAddress,
        now: Instant,
    ) -> Result<Option<NameKey>, Duration> {
        if user_id.is_some_and(|user_id| self.known.contains(user_id, client, now)) {
            return Ok(None);
        }

        let name = self.name_key(username);
        self.reserve(name, now)?;
        Ok(Some(name))
    }

    fn reserve(&mut self, name: NameKey, now: Instant) -> Result<(), Duration> {
        let key = BucketKey::LoginName(name);
        self.names.cleanup(now);
        if !self.names.buckets.contains_key(&key) && self.names.buckets.len() >= MAX_NAME_BUCKETS {
            self.evict_oldest();
        }

        let policy = self.policy;
        let bucket = self
            .names
            .buckets
            .entry(key)
            .or_insert_with(|| Bucket::new(policy.capacity(), now));
        bucket.refill(policy, now);
        if let Some(retry_after) = policy.retry_after(bucket.tokens) {
            return Err(retry_after);
        }

        bucket.tokens -= 1.0;
        Ok(())
    }

    fn evict_oldest(&mut self) {
        let oldest = self
            .names
            .buckets
            .iter()
            .min_by_key(|(_, bucket)| bucket.last_seen)
            .map(|(key, _)| key.clone());
        if let Some(oldest) = oldest {
            self.names.buckets.remove(&oldest);
        }
    }

    fn refund(&mut self, name: NameKey) {
        if let Some(bucket) = self.names.buckets.get_mut(&BucketKey::LoginName(name)) {
            bucket.tokens = (bucket.tokens + 1.0).min(self.policy.capacity());
        }
    }
}

/// When each user last proved their password from each address. The unknown address stands
/// for every client the server could not tell apart, so it is never known.
#[derive(Default)]
struct KnownAddresses {
    logins: HashMap<(String, ClientAddress), Instant>,
}

impl KnownAddresses {
    fn contains(&self, user_id: &str, client: ClientAddress, now: Instant) -> bool {
        client != ClientAddress::default()
            && self
                .logins
                .get(&(user_id.to_owned(), client))
                .is_some_and(|&at| !expired(at, now))
    }

    /// A full map keeps the logins it has: losing a new one only costs that address its exemption.
    fn remember(&mut self, user_id: &str, client: ClientAddress, now: Instant) {
        if client == ClientAddress::default() {
            return;
        }

        let key = (user_id.to_owned(), client);
        if !self.logins.contains_key(&key) && self.logins.len() >= MAX_KNOWN_ADDRESSES {
            self.logins.retain(|_, at| !expired(*at, now));
            if self.logins.len() >= MAX_KNOWN_ADDRESSES {
                return;
            }
        }
        self.logins.insert(key, now);
    }

    fn forget_user(&mut self, user_id: &str) {
        self.logins.retain(|(known, _), _| known != user_id);
    }
}

fn expired(at: Instant, now: Instant) -> bool {
    now.saturating_duration_since(at) >= KNOWN_ADDRESS_TTL
}

#[cfg(test)]
mod tests {
    use std::{
        net::{
            IpAddr,
            Ipv4Addr,
        },
        sync::Arc,
    };

    use super::*;

    const MINUTE: Duration = Duration::from_secs(60);

    fn address(raw: &str) -> ClientAddress {
        ClientAddress::new(raw.parse().expect("valid ip"))
    }

    fn numbered_address(number: u32) -> ClientAddress {
        ClientAddress::new(IpAddr::V4(Ipv4Addr::from(number)))
    }

    fn reserve(
        limiter: &mut PasswordCheckLimiter,
        username: &str,
        now: Instant,
    ) -> Result<NameKey, Duration> {
        limiter
            .admit(username, None, address("203.0.113.5"), now)
            .map(|name| name.expect("an unknown name is never exempt"))
    }

    fn drain(limiter: &mut PasswordCheckLimiter, username: &str, now: Instant) {
        for _ in 0..BURST {
            reserve(limiter, username, now).expect("the burst admits");
        }
    }

    fn tokens(limiter: &PasswordCheckLimiter, username: &str) -> f64 {
        limiter.names.buckets[&BucketKey::LoginName(limiter.name_key(username))].tokens
    }

    #[test]
    fn a_name_takes_its_burst_then_one_attempt_a_minute() {
        let mut limiter = PasswordCheckLimiter::new();
        let now = Instant::now();

        drain(&mut limiter, "amy", now);
        assert_eq!(reserve(&mut limiter, "amy", now), Err(MINUTE));
        assert!(reserve(&mut limiter, "bob", now).is_ok());

        let later = now + MINUTE;
        assert!(reserve(&mut limiter, "amy", later).is_ok());
        assert!(reserve(&mut limiter, "amy", later).is_err());
    }

    #[test]
    fn a_refund_returns_only_its_own_token() {
        let mut limiter = PasswordCheckLimiter::new();
        let now = Instant::now();
        drain(&mut limiter, "amy", now);

        let later = now + MINUTE;
        let name = reserve(&mut limiter, "amy", later).expect("one token refilled");
        limiter.refund(name);

        assert!(reserve(&mut limiter, "amy", later).is_ok());
        assert!(
            reserve(&mut limiter, "amy", later).is_err(),
            "a right password does not clear the wrong ones before it"
        );
    }

    #[test]
    fn a_refund_never_exceeds_the_burst() {
        let mut limiter = PasswordCheckLimiter::new();
        let now = Instant::now();
        let name = reserve(&mut limiter, "amy", now).expect("the burst admits");

        limiter.refund(name);
        limiter.refund(name);

        assert_eq!(tokens(&limiter, "amy"), f64::from(BURST));
    }

    #[test]
    fn a_refund_for_an_evicted_name_creates_no_bucket() {
        let mut limiter = PasswordCheckLimiter::new();
        let name = limiter.name_key("amy");

        limiter.refund(name);

        assert!(limiter.names.buckets.is_empty());
    }

    #[test]
    fn concurrent_reservations_cannot_exceed_the_remaining_tokens() {
        let limiter = Arc::new(Mutex::new(PasswordCheckLimiter::new()));
        let now = Instant::now();

        let attempts: Vec<_> = (0..BURST * 4)
            .map(|_| {
                let limiter = Arc::clone(&limiter);
                std::thread::spawn(move || reserve(&mut limiter.lock().unwrap(), "amy", now))
            })
            .collect();
        let admitted = attempts
            .into_iter()
            .map(|attempt| attempt.join().expect("attempt should not panic"))
            .filter(Result::is_ok)
            .count();

        assert_eq!(admitted, BURST as usize);
    }

    #[test]
    fn whitespace_and_case_variants_share_a_bucket() {
        let mut limiter = PasswordCheckLimiter::new();
        let now = Instant::now();

        for username in ["amy", " Amy ", "AMY\t", "aMy"] {
            reserve(&mut limiter, username, now).expect("the burst admits");
        }

        assert_eq!(limiter.names.buckets.len(), 1);
        assert_eq!(tokens(&limiter, "amy"), f64::from(BURST - 4));
    }

    #[test]
    fn a_full_limiter_evicts_the_name_seen_longest_ago() {
        let mut limiter = PasswordCheckLimiter::new();
        let start = Instant::now();
        for index in 0..MAX_NAME_BUCKETS {
            let seen = start + Duration::from_millis(index as u64);
            reserve(&mut limiter, &format!("name-{index}"), seen).expect("the burst admits");
        }
        let now = start + Duration::from_millis(MAX_NAME_BUCKETS as u64);
        reserve(&mut limiter, "name-0", now).expect("the burst admits");

        reserve(&mut limiter, "newcomer", now).expect("the burst admits");

        assert_eq!(limiter.names.buckets.len(), MAX_NAME_BUCKETS);
        let holds = |username: &str| {
            let key = BucketKey::LoginName(limiter.name_key(username));
            limiter.names.buckets.contains_key(&key)
        };
        assert!(holds("newcomer"));
        assert!(holds("name-0"), "a name seen again is no longer the oldest");
        assert!(!holds("name-1"));
        assert!(holds("name-2"));
    }

    #[test]
    fn a_known_address_checks_its_user_without_reserving() {
        let mut limiter = PasswordCheckLimiter::new();
        let now = Instant::now();
        let home = address("198.51.100.7");
        limiter.known.remember("user-amy", home, now);
        drain(&mut limiter, "amy", now);

        assert_eq!(limiter.admit("amy", Some("user-amy"), home, now), Ok(None));
        assert!(
            limiter
                .admit("amy", Some("user-amy"), address("198.51.100.8"), now)
                .is_err(),
            "another address is not exempt"
        );
        assert!(
            limiter.admit("amy", Some("user-bob"), home, now).is_err(),
            "another user is not exempt from the same address"
        );
        assert!(
            limiter.admit("amy", None, home, now).is_err(),
            "a name that resolved to no user is never exempt"
        );
    }

    #[test]
    fn a_known_address_expires_thirty_days_after_its_last_login() {
        let mut known = KnownAddresses::default();
        let start = Instant::now();
        let home = address("198.51.100.7");
        known.remember("user-amy", home, start);
        let renewed = start + KNOWN_ADDRESS_TTL - MINUTE;
        assert!(known.contains("user-amy", home, renewed));
        known.remember("user-amy", home, renewed);

        assert!(known.contains("user-amy", home, start + KNOWN_ADDRESS_TTL));
        assert!(!known.contains("user-amy", home, renewed + KNOWN_ADDRESS_TTL));
    }

    #[test]
    fn a_full_known_address_map_drops_expired_logins_but_never_live_ones() {
        let mut known = KnownAddresses::default();
        let start = Instant::now();
        let stale = numbered_address(1);
        known.remember("user-amy", stale, start);
        let now = start + KNOWN_ADDRESS_TTL;
        for number in 2..=MAX_KNOWN_ADDRESSES as u32 {
            known.remember("user-amy", numbered_address(number), now);
        }
        assert_eq!(known.logins.len(), MAX_KNOWN_ADDRESSES);

        let replacing = address("198.51.100.7");
        known.remember("user-amy", replacing, now);
        assert!(known.contains("user-amy", replacing, now));
        assert!(!known.logins.contains_key(&("user-amy".to_owned(), stale)));

        let refused = address("198.51.100.8");
        known.remember("user-amy", refused, now);
        assert!(!known.contains("user-amy", refused, now));
        assert_eq!(known.logins.len(), MAX_KNOWN_ADDRESSES);

        let later = now + MINUTE;
        known.remember("user-amy", replacing, later);
        assert_eq!(
            known.logins[&("user-amy".to_owned(), replacing)],
            later,
            "a full map still renews a login it holds"
        );
    }

    #[test]
    fn the_unknown_address_is_never_recorded_or_exempt() {
        let mut limiter = PasswordCheckLimiter::new();
        let now = Instant::now();
        let unknown = ClientAddress::default();

        limiter.known.remember("user-amy", unknown, now);
        assert!(limiter.known.logins.is_empty());

        limiter
            .known
            .logins
            .insert(("user-amy".to_owned(), unknown), now);
        assert!(
            limiter
                .admit("amy", Some("user-amy"), unknown, now)
                .is_ok_and(|name| name.is_some())
        );
    }

    #[test]
    fn forgetting_a_user_drops_every_address_of_theirs_alone() {
        let mut known = KnownAddresses::default();
        let now = Instant::now();
        let home = address("198.51.100.7");
        known.remember("user-amy", home, now);
        known.remember("user-amy", address("2001:db8:1:2::1"), now);
        known.remember("user-bob", home, now);

        known.forget_user("user-amy");

        assert!(!known.contains("user-amy", home, now));
        assert!(!known.contains("user-amy", address("2001:db8:1:2::1"), now));
        assert!(known.contains("user-bob", home, now));
    }
}
