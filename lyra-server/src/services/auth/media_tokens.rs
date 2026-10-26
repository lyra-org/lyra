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

use crate::{
    STATE,
    db,
    services::playback_sources::SourceVersion,
};

use super::{
    AuthCredential,
    Principal,
    ResolvedAuth,
    random_hex_secret,
};

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

/// Who a media token was issued to: the user and the credential they presented. The token only
/// works while that credential still authenticates that user.
#[derive(Clone, Debug)]
pub(crate) struct MediaTokenHolder {
    user_public_id: String,
    credential: AuthCredential,
}

impl MediaTokenHolder {
    pub(crate) fn new(auth: &ResolvedAuth) -> Self {
        Self {
            user_public_id: auth.principal.user_public_id.clone(),
            credential: auth.credential.clone(),
        }
    }

    pub(crate) fn user_public_id(&self) -> &str {
        &self.user_public_id
    }

    /// The holder's principal as it stands now, read under the caller's guard, or `None` when
    /// the user is gone or the credential that obtained the token no longer authenticates them.
    pub(crate) fn principal(&self, db: &impl db::DbAccess) -> anyhow::Result<Option<Principal>> {
        let Some(user_db_id) = db::users::find_db_id_by_public_id(db, &self.user_public_id)? else {
            return Ok(None);
        };
        if !self.credential.is_live(db, user_db_id)? {
            return Ok(None);
        }
        Principal::load(db, user_db_id)
    }
}

/// What a validated media token grants: its purpose, its holder, and the exact source file it was
/// issued against.
#[derive(Clone, Debug)]
pub(crate) struct MediaTokenAccess {
    pub(crate) purpose: MediaTokenPurpose,
    pub(crate) holder: MediaTokenHolder,
    pub(crate) source: SourceVersion,
}

#[derive(Clone, Debug)]
struct MediaTokenGrant {
    track_id: String,
    access: MediaTokenAccess,
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

pub(crate) fn issue_media_token(
    track_id: &str,
    purpose: MediaTokenPurpose,
    holder: MediaTokenHolder,
    source: SourceVersion,
) -> IssuedMediaToken {
    let now = Instant::now();
    let expires_at = now_secs().saturating_add(MEDIA_TOKEN_MAX_TTL_SECONDS as i64);
    let token = random_hex_secret::<MEDIA_TOKEN_BYTES>();
    let grant = MediaTokenGrant {
        track_id: track_id.to_string(),
        access: MediaTokenAccess {
            purpose,
            holder,
            source,
        },
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

/// Checks the token against its purpose and track. The holder and source it is bound to are
/// checked where the track's source is resolved, under the database guard.
pub(crate) fn validate_media_token(
    token: &str,
    purpose: MediaTokenPurpose,
    track_id: &str,
) -> Result<MediaTokenAccess, MediaTokenError> {
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

    if grant.access.purpose != purpose || grant.track_id != track_id {
        return Err(MediaTokenError::Invalid);
    }

    grant.last_used_at = now;
    Ok(grant.access.clone())
}

/// A token for `track_id` held by a placeholder user over a placeholder source, for tests that
/// only exercise purpose and track matching.
#[cfg(test)]
pub(crate) fn issue_unbound_media_token(
    track_id: &str,
    purpose: MediaTokenPurpose,
) -> IssuedMediaToken {
    issue_media_token(
        track_id,
        purpose,
        MediaTokenHolder {
            user_public_id: "placeholder-user".to_string(),
            credential: AuthCredential::Default,
        },
        SourceVersion::fixed("placeholder-source"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn issue(track_id: &str, purpose: MediaTokenPurpose) -> IssuedMediaToken {
        issue_unbound_media_token(track_id, purpose)
    }

    #[test]
    fn media_token_validates_for_matching_track_and_purpose() {
        let _guard = futures::executor::block_on(crate::testing::runtime_test_lock());
        crate::testing::init_default_test_state().expect("init test state");
        let issued = issue("track-42", MediaTokenPurpose::Stream);

        validate_media_token(&issued.token, MediaTokenPurpose::Stream, "track-42")
            .expect("matching media token should validate");
    }

    #[test]
    fn media_token_rejects_wrong_track_or_purpose() {
        let _guard = futures::executor::block_on(crate::testing::runtime_test_lock());
        crate::testing::init_default_test_state().expect("init test state");
        let issued = issue("track-42", MediaTokenPurpose::Stream);

        assert!(matches!(
            validate_media_token(&issued.token, MediaTokenPurpose::Stream, "track-43"),
            Err(MediaTokenError::Invalid)
        ));
        assert!(matches!(
            validate_media_token(&issued.token, MediaTokenPurpose::HlsPlaylist, "track-42"),
            Err(MediaTokenError::Invalid)
        ));
    }
}
