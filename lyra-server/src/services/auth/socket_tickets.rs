// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Single-use tickets that open a WebSocket, so the long-lived credential behind them never
//! travels in the upgrade URL, where proxies log it.

use std::{
    collections::HashMap,
    sync::Mutex,
    time::{
        Duration,
        Instant,
    },
};

use crate::services::STATE;

use super::{
    AuthCredential,
    AuthResult,
    ResolvedAuth,
    random_hex_secret,
};

pub(crate) const SOCKET_TICKET_TTL: Duration = Duration::from_secs(30);
const SOCKET_TICKET_BYTES: usize = 24;
/// What one user may have outstanding; a client opens one socket at a time.
const MAX_SOCKET_TICKETS_PER_USER: usize = 16;
/// A backstop on memory across every user.
const MAX_SOCKET_TICKETS: usize = 4_096;

/// Opaque ticket store on the generation state so a reset drops all outstanding tickets.
#[derive(Default)]
pub(crate) struct SocketTicketStore(Mutex<HashMap<String, SocketTicketGrant>>);

#[derive(Clone, Debug)]
struct SocketTicketGrant {
    user_public_id: String,
    credential: AuthCredential,
    client_name: Option<String>,
    issued_at: Instant,
}

#[derive(Clone, Debug)]
pub(crate) struct IssuedSocketTicket {
    pub(crate) ticket: String,
    /// Unix seconds.
    pub(crate) expires_at: i64,
}

fn tickets(
    generation: &crate::services::state::GenerationState,
) -> std::sync::MutexGuard<'_, HashMap<String, SocketTicketGrant>> {
    generation
        .auth_caches
        .socket_tickets
        .0
        .lock()
        .expect("socket ticket cache poisoned")
}

fn is_expired(grant: &SocketTicketGrant, now: Instant) -> bool {
    now.duration_since(grant.issued_at) >= SOCKET_TICKET_TTL
}

/// Removes the oldest tickets `matching`, beyond the newest `cap` of them.
fn evict_oldest(
    cache: &mut HashMap<String, SocketTicketGrant>,
    cap: usize,
    matching: impl Fn(&SocketTicketGrant) -> bool,
) {
    let mut by_age = cache
        .iter()
        .filter(|(_, grant)| matching(grant))
        .map(|(ticket, grant)| (ticket.clone(), grant.issued_at))
        .collect::<Vec<_>>();
    if by_age.len() <= cap {
        return;
    }
    let overflow = by_age.len() - cap;
    by_age.sort_by_key(|(_, issued_at)| *issued_at);
    for (ticket, _) in by_age.into_iter().take(overflow) {
        cache.remove(&ticket);
    }
}

/// Issues a ticket that opens one WebSocket as `auth`, while its credential still authenticates.
pub(crate) fn issue(auth: &ResolvedAuth) -> IssuedSocketTicket {
    let now = Instant::now();
    let ticket = random_hex_secret::<SOCKET_TICKET_BYTES>();
    let user_public_id = auth.principal.user_public_id.clone();

    let generation = STATE.generation();
    let mut cache = tickets(&generation);
    cache.retain(|_, grant| !is_expired(grant, now));
    cache.insert(
        ticket.clone(),
        SocketTicketGrant {
            user_public_id: user_public_id.clone(),
            credential: auth.credential.clone(),
            client_name: auth.client_name.clone(),
            issued_at: now,
        },
    );
    evict_oldest(&mut cache, MAX_SOCKET_TICKETS_PER_USER, |grant| {
        grant.user_public_id == user_public_id
    });
    evict_oldest(&mut cache, MAX_SOCKET_TICKETS, |_| true);

    let expires_at = lyra_db::users::now_ms() / 1000 + SOCKET_TICKET_TTL.as_secs().cast_signed();
    IssuedSocketTicket { ticket, expires_at }
}

/// Spends `ticket`, returning whom it authenticates now, or `None` when it is unknown, used,
/// expired, or its credential no longer authenticates its user.
pub(crate) async fn redeem(ticket: &str) -> AuthResult<Option<ResolvedAuth>> {
    let grant = {
        let generation = STATE.generation();
        let mut cache = tickets(&generation);
        cache.remove(ticket.trim())
    };
    let Some(grant) = grant.filter(|grant| !is_expired(grant, Instant::now())) else {
        return Ok(None);
    };

    let db = STATE.db.read().await;
    let Some(principal) = super::live_principal(&*db, &grant.user_public_id, &grant.credential)?
    else {
        return Ok(None);
    };
    Ok(Some(ResolvedAuth {
        principal,
        credential: grant.credential,
        client_name: grant.client_name,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn session_auth(name: &str) -> anyhow::Result<ResolvedAuth> {
        let user_db_id = {
            let mut db = STATE.db.write().await;
            let user = lyra_db::fixtures::test_user(name)?;
            lyra_db::users::create(&mut db, &user)?
        };
        let session =
            crate::services::testing::create_session(user_db_id, Default::default()).await?;
        Ok(super::super::require_auth(Some(&session.token)).await?)
    }

    #[tokio::test]
    async fn a_ticket_opens_one_socket() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let auth = session_auth("ticket-once").await?;

        let issued = issue(&auth);
        let redeemed = redeem(&issued.ticket)
            .await?
            .expect("a fresh ticket redeems");
        assert_eq!(
            redeemed.principal.user_public_id,
            auth.principal.user_public_id
        );
        assert_eq!(redeemed.credential, auth.credential);
        assert!(redeem(&issued.ticket).await?.is_none(), "a ticket is spent");
        Ok(())
    }

    #[tokio::test]
    async fn a_ticket_dies_with_its_session() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let auth = session_auth("ticket-revoked").await?;
        let AuthCredential::Session { public_id } = &auth.credential else {
            panic!("a session credential");
        };

        let issued = issue(&auth);
        {
            let mut db = STATE.db.write().await;
            let (session_id, _) = lyra_db::users::find_session_by_public_id(&*db, public_id)?
                .expect("the session exists");
            lyra_db::users::revoke_session_by_id(&mut db, session_id)?;
        }
        assert!(redeem(&issued.ticket).await?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn a_user_only_evicts_their_own_tickets() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let bystander = session_auth("ticket-bystander").await?;
        let flooder = session_auth("ticket-flooder").await?;

        let kept = issue(&bystander);
        let oldest = issue(&flooder);
        for _ in 0..MAX_SOCKET_TICKETS_PER_USER {
            issue(&flooder);
        }
        assert!(redeem(&oldest.ticket).await?.is_none());
        assert!(redeem(&kept.ticket).await?.is_some());
        Ok(())
    }
}
