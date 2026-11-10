// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Bounds the wrong login codes one signed-in user can try. Codes are short, so a look-up is
//! admitted first and charged only when it finds nothing. This guards something a guess can
//! redeem, so unlike the request limits it stays on when rate limiting is turned off.

use std::time::{
    Duration,
    Instant,
};

use super::{
    Bucket,
    BucketKey,
    LimiterState,
    Policy,
};

const PER_MINUTE: u32 = 10;
const BURST: u32 = 10;
/// The idle cleanup forgets no failures: a bucket it drops has refilled by then.
const _: () = assert!(BURST as u64 * 60 <= PER_MINUTE as u64 * super::IDLE_BUCKET_TTL.as_secs());

pub(crate) struct CodeGuessLimiter {
    policy: Policy,
    users: LimiterState,
}

impl Default for CodeGuessLimiter {
    fn default() -> Self {
        Self {
            policy: Policy::new(PER_MINUTE, BURST).expect("the guess policy is enabled"),
            users: LimiterState::new(),
        }
    }
}

impl CodeGuessLimiter {
    fn bucket(&mut self, user_id: &str, now: Instant) -> &mut Bucket {
        self.users.cleanup(now);
        let policy = self.policy;
        let bucket = self
            .users
            .buckets
            .entry(BucketKey::User(user_id.to_string()))
            .or_insert_with(|| Bucket::new(policy.capacity(), now));
        bucket.refill(policy, now);
        bucket
    }

    /// Refuses a look-up from a user who has run out of guesses, charging nothing.
    pub(crate) fn admit(&mut self, user_id: &str, now: Instant) -> Result<(), Duration> {
        let policy = self.policy;
        match policy.retry_after(self.bucket(user_id, now).tokens) {
            Some(retry_after) => Err(retry_after),
            None => Ok(()),
        }
    }

    /// Charges a look-up that found nothing.
    pub(crate) fn charge(&mut self, user_id: &str, now: Instant) {
        let bucket = self.bucket(user_id, now);
        bucket.tokens = (bucket.tokens - 1.0).max(0.0);
    }
}
