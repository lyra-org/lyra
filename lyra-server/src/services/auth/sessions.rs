// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::ops::DerefMut;

use agdb::DbId;
use nanoid::nanoid;

use crate::{
    STATE,
    db::{
        self,
        Session,
    },
    services::auth::{
        hash_secret,
        random_hex_secret,
    },
};

#[derive(Clone, Debug)]
pub(crate) struct CreatedSession {
    pub(crate) token: String,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum SessionServiceError {
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

type SessionServiceResult<T> = Result<T, SessionServiceError>;

const SESSION_TOKEN_BYTES: usize = 16;

pub(crate) async fn create_session_for_user(
    user_db_id: DbId,
) -> SessionServiceResult<CreatedSession> {
    let token = random_hex_secret::<SESSION_TOKEN_BYTES>();
    let token_hash = hash_secret(&token);
    let ttl = STATE.config.get().auth.session_ttl_seconds;
    let expires_at = if ttl > 0 {
        db::users::now_secs().saturating_add(ttl as i64)
    } else {
        0
    };
    let session = Session {
        db_id: None,
        id: nanoid!(),
        token_hash,
        expires_at,
    };
    db::users::login(STATE.db.write().await.deref_mut(), user_db_id, &session)?;

    Ok(CreatedSession { token })
}

pub(crate) async fn revoke_session_by_token(token: &str) -> SessionServiceResult<bool> {
    let mut db = STATE.db.write().await;
    let token_hash = hash_secret(token.trim());
    Ok(db::users::revoke_session_by_token_hash(
        &mut db,
        &token_hash,
    )?)
}
