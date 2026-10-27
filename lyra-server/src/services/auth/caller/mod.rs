// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Whom a request's use is charged to: the user when the request authenticates, otherwise its
//! [`ClientAddress`]. Where a limiter guards a pool the whole server shares, anonymous callers are
//! also charged together to one [`Account::Anonymous`], so authenticated users keep their share
//! however many addresses anonymous clients rotate through.

mod address;

use std::{
    collections::HashMap,
    sync::{
        LazyLock,
        Mutex,
    },
    time::{
        Duration,
        Instant,
    },
};

use axum::http::HeaderMap;

use super::AuthCredential;
use crate::{
    db,
    services::STATE,
};

pub(crate) use address::ClientAddress;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Caller {
    /// An authenticated user, by public id.
    User(String),
    Client(ClientAddress),
}

/// What a limiter counts a caller's use against.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Account {
    Caller(Caller),
    /// Every anonymous caller together.
    Anonymous,
}

impl Caller {
    pub(crate) fn is_anonymous(&self) -> bool {
        matches!(self, Self::Client(_))
    }

    /// The accounts one use by this caller is charged to: its own, and for an anonymous caller
    /// also the one all anonymous callers share.
    pub(crate) fn accounts(&self) -> impl Iterator<Item = Account> {
        let shared = self.is_anonymous().then_some(Account::Anonymous);
        std::iter::once(Account::Caller(self.clone())).chain(shared)
    }
}

/// A user as one of their credentials identifies them.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct Identity {
    pub(crate) user: String,
    pub(crate) credential: AuthCredential,
}

/// How long a credential's identity is remembered, found or not. A revoked or expired credential
/// may go on being counted against its user for this long; it only ever affects accounting,
/// never authorization.
const IDENTITY_TTL: Duration = Duration::from_secs(10);
const MAX_CACHED_IDENTITIES: usize = 4_096;

type IdentityCache = HashMap<String, (Option<Identity>, Instant)>;

static IDENTITIES: LazyLock<Mutex<IdentityCache>> = LazyLock::new(Default::default);

/// The hashed bearer credential a request presents, the key its identity is looked up by.
pub(crate) fn bearer_hash(headers: &HeaderMap) -> Option<String> {
    super::extract_bearer_credential(headers).map(|bearer| super::hash_secret(&bearer))
}

/// The identity a bearer credential was recently found to have: `Some(None)` when it identified
/// no one, `None` when it has not been looked up lately.
pub(crate) fn remembered_identity(bearer_hash: &str) -> Option<Option<Identity>> {
    let identities = IDENTITIES.lock().expect("identity cache poisoned");
    let (identity, found_at) = identities.get(bearer_hash)?;
    (found_at.elapsed() < IDENTITY_TTL).then(|| identity.clone())
}

/// Looks up whom a bearer credential identifies, as cheaply as the database allows: it neither
/// authorizes the request nor records the credential's use. A lookup that fails identifies no
/// one, so the request is counted against its address.
pub(crate) async fn look_up_identity(bearer_hash: &str) -> Option<Identity> {
    let identity = match bearer_identity(bearer_hash).await {
        Ok(identity) => identity,
        Err(error) => {
            tracing::warn!(error = %error, "failed to identify a bearer credential's user");
            return None;
        }
    };
    let mut identities = IDENTITIES.lock().expect("identity cache poisoned");
    if identities.len() >= MAX_CACHED_IDENTITIES {
        identities.retain(|_, (_, found_at)| found_at.elapsed() < IDENTITY_TTL);
        if identities.len() >= MAX_CACHED_IDENTITIES {
            identities.clear();
        }
    }
    identities.insert(bearer_hash.to_string(), (identity.clone(), Instant::now()));
    identity
}

async fn bearer_identity(hash: &str) -> anyhow::Result<Option<Identity>> {
    let db = STATE.db.read().await;
    if let Some((user, session, _)) = db::users::find_by_session_token_hash(&*db, hash)? {
        let expired = super::sessions::is_expired(&session);
        return Ok((!expired).then_some(Identity {
            user: user.id,
            credential: AuthCredential::Session {
                public_id: session.id,
            },
        }));
    }
    let Some(key) = db::api_keys::find_by_key_hash(&*db, hash)? else {
        return Ok(None);
    };
    let Some(key_id) = key.db_id else {
        return Ok(None);
    };
    let Some(owner) = db::api_keys::get_owner_id(&*db, key_id.into())? else {
        return Ok(None);
    };
    Ok(db::users::get_by_id(&*db, owner)?.map(|user| Identity {
        user: user.id,
        credential: AuthCredential::ApiKey {
            public_id: key.id,
            name: key.name,
        },
    }))
}
