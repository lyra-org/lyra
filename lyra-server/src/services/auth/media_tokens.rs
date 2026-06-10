// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::HashMap,
    sync::Mutex,
    time::{
        Duration,
        Instant,
    },
};

use agdb::DbId;

use crate::{
    STATE,
    db,
};

use super::random_hex_secret;

pub(crate) const MEDIA_TOKEN_IDLE_TTL_SECONDS: u64 = 15 * 60;
pub(crate) const MEDIA_TOKEN_MAX_TTL_SECONDS: u64 = 8 * 60 * 60;
const MEDIA_TOKEN_BYTES: usize = 24;
const MAX_MEDIA_TOKENS: usize = 4096;

/// Opaque grant store on the generation state so a reset drops all
/// outstanding media tokens; grant internals stay private to this module.
#[derive(Default)]
pub(crate) struct MediaTokenStore(Mutex<HashMap<String, MediaTokenGrant>>);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MediaTokenPurpose {
    Stream,
    HlsPlaylist,
    Download,
}

#[derive(Clone, Debug)]
struct MediaTokenGrant {
    track_db_id: DbId,
    purpose: MediaTokenPurpose,
    issued_at: Instant,
    last_used_at: Instant,
    expires_at: i64,
}

#[derive(Clone, Debug)]
pub(crate) struct IssuedMediaToken {
    pub(crate) token: String,
    pub(crate) expires_at: i64,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum MediaTokenError {
    #[error("invalid media token")]
    Invalid,
    #[error("media token expired")]
    Expired,
}

fn tokens(
    generation: &crate::GenerationState,
) -> std::sync::MutexGuard<'_, HashMap<String, MediaTokenGrant>> {
    generation
        .auth_caches
        .media_tokens
        .0
        .lock()
        .expect("media token cache poisoned")
}

fn now_secs() -> i64 {
    db::users::now_secs()
}

fn token_is_expired(grant: &MediaTokenGrant, now: Instant, now_secs: i64) -> bool {
    now_secs >= grant.expires_at
        || now.duration_since(grant.last_used_at)
            >= Duration::from_secs(MEDIA_TOKEN_IDLE_TTL_SECONDS)
        || now.duration_since(grant.issued_at) >= Duration::from_secs(MEDIA_TOKEN_MAX_TTL_SECONDS)
}

fn cleanup_expired_tokens(
    cache: &mut HashMap<String, MediaTokenGrant>,
    now: Instant,
    now_secs: i64,
) {
    cache.retain(|_, grant| !token_is_expired(grant, now, now_secs));
}

fn enforce_media_token_cap(cache: &mut HashMap<String, MediaTokenGrant>) {
    if cache.len() <= MAX_MEDIA_TOKENS {
        return;
    }

    let overflow = cache.len() - MAX_MEDIA_TOKENS;
    let mut by_age = cache
        .iter()
        .map(|(token, grant)| (token.clone(), grant.last_used_at))
        .collect::<Vec<_>>();
    by_age.sort_by_key(|(_, last_used_at)| *last_used_at);
    for (token, _) in by_age.into_iter().take(overflow) {
        cache.remove(&token);
    }
}

pub(crate) fn issue_media_token(track_db_id: DbId, purpose: MediaTokenPurpose) -> IssuedMediaToken {
    let now = Instant::now();
    let expires_at = now_secs().saturating_add(MEDIA_TOKEN_MAX_TTL_SECONDS as i64);
    let token = random_hex_secret::<MEDIA_TOKEN_BYTES>();
    let grant = MediaTokenGrant {
        track_db_id,
        purpose,
        issued_at: now,
        last_used_at: now,
        expires_at,
    };

    let generation = STATE.generation();
    let mut cache = tokens(&generation);
    cleanup_expired_tokens(&mut cache, now, now_secs());
    cache.insert(token.clone(), grant);
    enforce_media_token_cap(&mut cache);

    IssuedMediaToken { token, expires_at }
}

pub(crate) fn validate_media_token(
    token: &str,
    purpose: MediaTokenPurpose,
    track_db_id: DbId,
) -> Result<(), MediaTokenError> {
    let token = token.trim();
    if token.is_empty() {
        return Err(MediaTokenError::Invalid);
    }

    let now = Instant::now();
    let now_secs = now_secs();
    let generation = STATE.generation();
    let mut cache = tokens(&generation);

    let Some(grant) = cache.get_mut(token) else {
        cleanup_expired_tokens(&mut cache, now, now_secs);
        return Err(MediaTokenError::Invalid);
    };

    if token_is_expired(grant, now, now_secs) {
        cache.remove(token);
        return Err(MediaTokenError::Expired);
    }

    if grant.purpose != purpose || grant.track_db_id != track_db_id {
        return Err(MediaTokenError::Invalid);
    }

    grant.last_used_at = now;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn media_token_validates_for_matching_track_and_purpose() {
        let _guard = futures::executor::block_on(crate::testing::runtime_test_lock());
        crate::testing::init_default_test_state().expect("init test state");
        let issued = issue_media_token(DbId(42), MediaTokenPurpose::Stream);

        validate_media_token(&issued.token, MediaTokenPurpose::Stream, DbId(42))
            .expect("matching media token should validate");
    }

    #[test]
    fn media_token_rejects_wrong_track_or_purpose() {
        let _guard = futures::executor::block_on(crate::testing::runtime_test_lock());
        crate::testing::init_default_test_state().expect("init test state");
        let issued = issue_media_token(DbId(42), MediaTokenPurpose::Stream);

        assert!(matches!(
            validate_media_token(&issued.token, MediaTokenPurpose::Stream, DbId(43)),
            Err(MediaTokenError::Invalid)
        ));
        assert!(matches!(
            validate_media_token(&issued.token, MediaTokenPurpose::HlsPlaylist, DbId(42)),
            Err(MediaTokenError::Invalid)
        ));
    }
}
