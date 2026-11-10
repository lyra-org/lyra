// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Sign-in where a device shows a short login code and an already signed-in device approves it.
//! Requests live in memory only: a request moves from pending to approved to claimed, and the
//! claim, not the approval, creates the session.

use std::{
    collections::HashMap,
    sync::{
        Mutex,
        MutexGuard,
    },
    time::{
        Duration,
        Instant,
    },
};

use crate::services::{
    STATE,
    rate_limit::code_guesses::CodeGuessLimiter,
    state::GenerationState,
};
use agdb::DbId;
use argon2::password_hash::rand_core::{
    OsRng,
    RngCore,
};

use super::{
    AuthCredential,
    AuthError,
    AuthResult,
    LoginResult,
    Principal,
    caller::ClientAddress,
    hash_secret,
    owners,
    random_hex_secret,
    sessions::{
        self,
        SessionMetadata,
    },
};

/// How long a request lives, and how long it stays after its approval and after its claim.
const DEVICE_LOGIN_TTL: Duration = Duration::from_secs(10 * 60);
const SECRET_BYTES: usize = 32;
const CODE_LENGTH: usize = 6;
/// A-Z without I, L and O, which read as digits or as each other.
const CODE_LETTERS: &[u8; 23] = b"ABCDEFGHJKMNPQRSTUVWXYZ";
/// What a caller's own code must be: long enough to be worth guessing at, short enough to type.
const GIVEN_CODE_LENGTHS: std::ops::RangeInclusive<usize> = CODE_LENGTH..=32;
/// Gives up instead of spinning, though live requests are a sliver of either code space.
const MAX_CODE_ATTEMPTS: usize = 32;
/// A backstop on memory across every address.
const MAX_PENDING_DEVICE_LOGINS: usize = 4_096;
/// What one address may have waiting; a device shows one code at a time.
const MAX_PENDING_DEVICE_LOGINS_PER_ADDRESS: usize = 8;
/// Small, since anyone may begin a request and each one holds its data in memory.
pub(crate) const MAX_DEVICE_LOGIN_DATA_BYTES: usize = 1024;

#[derive(Debug, thiserror::Error)]
pub(crate) enum DeviceLoginError {
    #[error("device login is disabled because authentication is disabled")]
    Disabled,
    #[error("device login not found")]
    NotFound,
    #[error("device login is already approved")]
    AlreadyApproved,
    #[error("user not found")]
    UserNotFound,
    #[error("device login data exceeds {MAX_DEVICE_LOGIN_DATA_BYTES} bytes")]
    DataTooLarge,
    #[error("device login code is taken")]
    CodeTaken,
    #[error("a device login code is 6 to 32 ASCII letters and digits")]
    InvalidCode,
    #[error(transparent)]
    Auth(#[from] AuthError),
}

type DeviceLoginResult<T> = Result<T, DeviceLoginError>;

/// Generation-bound, so a reset drops every outstanding request.
#[derive(Default)]
pub(crate) struct DeviceLoginStore {
    requests: Mutex<Requests>,
    /// Held across session creation, so concurrent claims mint one session between them.
    claims: tokio::sync::Mutex<()>,
}

#[derive(Default)]
struct Requests {
    by_code: HashMap<String, DeviceLogin>,
    code_by_secret: HashMap<String, String>,
    code_guesses: CodeGuessLimiter,
}

struct DeviceLogin {
    secret: String,
    metadata: SessionMetadata,
    /// What whoever began the request keeps with it, returned to whoever holds the secret.
    data: Option<serde_json::Value>,
    client: ClientAddress,
    created_at: Instant,
    created_at_ms: i64,
    expires_at: Instant,
    expires_at_ms: i64,
    state: RequestState,
    /// A claim is creating this request's session: it can't expire, so nothing can remove it or
    /// reuse its code until the [`ClaimReservation`] lets go.
    claiming: bool,
}

enum RequestState {
    Pending,
    Approved(Approval),
    Claimed { login: LoginResult },
}

/// Whom an approval signs in, and what must still hold when the session is inserted.
#[derive(Clone)]
struct Approval {
    user_public_id: String,
    /// Of the user's stored password hash, which every password change replaces.
    password_fingerprint: String,
    approver_public_id: String,
    approver_credential: AuthCredential,
}

#[derive(Clone, Debug)]
pub(crate) struct BegunDeviceLogin {
    pub(crate) code: String,
    pub(crate) secret: String,
    pub(crate) created_at_ms: i64,
    pub(crate) expires_at_ms: i64,
}

#[derive(Clone, Debug)]
pub(crate) struct DeviceLoginStatus {
    pub(crate) code: String,
    pub(crate) client_name: Option<String>,
    pub(crate) data: Option<serde_json::Value>,
    pub(crate) created_at_ms: i64,
    pub(crate) expires_at_ms: i64,
    pub(crate) approved: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct DeviceLoginPreview {
    pub(crate) client_name: Option<String>,
    pub(crate) client: ClientAddress,
    pub(crate) created_at_ms: i64,
    pub(crate) expires_at_ms: i64,
}

#[derive(Clone)]
pub(crate) enum DeviceLoginClaim {
    Pending,
    Complete {
        login: LoginResult,
        data: Option<serde_json::Value>,
    },
}

fn require_enabled() -> DeviceLoginResult<()> {
    // With authentication off every caller is the default admin, and a session minted here for a
    // real user would stay valid once authentication is back on.
    if STATE.config().auth.enabled {
        Ok(())
    } else {
        Err(DeviceLoginError::Disabled)
    }
}

fn requests(generation: &GenerationState) -> MutexGuard<'_, Requests> {
    generation
        .auth_caches
        .device_logins
        .requests
        .lock()
        .expect("device login store poisoned")
}

impl DeviceLogin {
    fn is_expired(&self, now: Instant) -> bool {
        !self.claiming && now >= self.expires_at
    }

    fn is_pending(&self) -> bool {
        matches!(self.state, RequestState::Pending)
    }

    fn extend(&mut self, now: Instant) {
        self.expires_at = now + DEVICE_LOGIN_TTL;
        self.expires_at_ms = expires_at_ms();
    }
}

impl Requests {
    fn remove(&mut self, code: &str) {
        if let Some(login) = self.by_code.remove(code) {
            self.code_by_secret.remove(&login.secret);
        }
    }

    fn sweep(&mut self, now: Instant) {
        self.by_code.retain(|_, login| !login.is_expired(now));
        let by_code = &self.by_code;
        self.code_by_secret
            .retain(|_, code| by_code.contains_key(code));
    }

    /// Removes the oldest pending requests `matching`, beyond the newest `cap` of them. Approved
    /// and claimed requests stay: a flood must not undo an approval.
    fn evict_oldest_pending(&mut self, cap: usize, matching: impl Fn(&DeviceLogin) -> bool) {
        let mut by_age = self
            .by_code
            .iter()
            .filter(|(_, login)| login.is_pending() && matching(login))
            .map(|(code, login)| (code.clone(), login.created_at))
            .collect::<Vec<_>>();
        if by_age.len() <= cap {
            return;
        }
        let overflow = by_age.len() - cap;
        by_age.sort_by_key(|(_, created_at)| *created_at);
        for (code, _) in by_age.into_iter().take(overflow) {
            self.remove(&code);
        }
    }

    fn live(&self, code: &str, now: Instant) -> Option<&DeviceLogin> {
        self.by_code
            .get(code)
            .filter(|login| !login.is_expired(now))
    }

    fn live_by_secret(&self, secret: &str, now: Instant) -> Option<(&String, &DeviceLogin)> {
        let code = self.code_by_secret.get(secret)?;
        Some((code, self.live(code, now)?))
    }
}

fn expires_at_ms() -> i64 {
    lyra_db::users::now_ms().saturating_add(DEVICE_LOGIN_TTL.as_millis() as i64)
}

/// A value in `0..bound` from `next`. Values at or past the last whole multiple of `bound` are
/// drawn again, since keeping them would favour the small results.
fn sample_below(bound: u32, mut next: impl FnMut() -> u32) -> u32 {
    let limit = u32::MAX - u32::MAX % bound;
    loop {
        let value = next();
        if value < limit {
            return value % bound;
        }
    }
}

fn random_below(bound: u32) -> u32 {
    sample_below(bound, || OsRng.next_u32())
}

fn generate_code() -> String {
    (0..CODE_LENGTH)
        .map(|_| char::from(CODE_LETTERS[random_below(CODE_LETTERS.len() as u32) as usize]))
        .collect()
}

fn normalize_code(code: &str) -> String {
    code.chars()
        .filter(|c| !c.is_whitespace())
        .map(|c| c.to_ascii_uppercase())
        .collect()
}

/// Starts a request for a device that wants to sign in, returning the code it shows and the
/// secret it claims with. `data` stays with the request and comes back from [`status`] and
/// [`claim`]; null is none. `code` is the caller's own, for one whose codes have a shape of their
/// own, and is [`DeviceLoginError::CodeTaken`] while another request holds it; without one the
/// code is six letters.
pub(crate) fn begin(
    metadata: SessionMetadata,
    client: ClientAddress,
    data: Option<serde_json::Value>,
    code: Option<&str>,
) -> DeviceLoginResult<BegunDeviceLogin> {
    let Some(code) = code else {
        return begin_with(metadata, client, data, MAX_CODE_ATTEMPTS, generate_code).map_err(
            |error| match error {
                DeviceLoginError::CodeTaken => {
                    AuthError::from(anyhow::anyhow!("no free device login code")).into()
                }
                error => error,
            },
        );
    };
    let code = normalize_code(code);
    if !GIVEN_CODE_LENGTHS.contains(&code.len())
        || !code.bytes().all(|byte| byte.is_ascii_alphanumeric())
    {
        return Err(DeviceLoginError::InvalidCode);
    }
    begin_with(metadata, client, data, 1, || code.clone())
}

/// [`DeviceLoginError::CodeTaken`] when none of `attempts` codes from `next_code` is free.
fn begin_with(
    metadata: SessionMetadata,
    client: ClientAddress,
    data: Option<serde_json::Value>,
    attempts: usize,
    mut next_code: impl FnMut() -> String,
) -> DeviceLoginResult<BegunDeviceLogin> {
    require_enabled()?;
    let data = data.filter(|data| !data.is_null());
    if let Some(data) = &data
        && serde_json::to_vec(data)
            .map_err(|error| AuthError::from(anyhow::Error::from(error)))?
            .len()
            > MAX_DEVICE_LOGIN_DATA_BYTES
    {
        return Err(DeviceLoginError::DataTooLarge);
    }
    crate::services::rate_limit::check_login_rate(client).map_err(AuthError::RateLimited)?;
    let metadata = metadata.normalized().map_err(AuthError::from)?;

    let now = Instant::now();
    let created_at_ms = lyra_db::users::now_ms();
    let expires_at_ms = expires_at_ms();
    let secret = random_hex_secret::<SECRET_BYTES>();

    let generation = STATE.generation();
    let mut requests = requests(&generation);
    requests.sweep(now);

    let code = (0..attempts)
        .map(|_| next_code())
        .find(|code| !requests.by_code.contains_key(code))
        .ok_or(DeviceLoginError::CodeTaken)?;
    requests.by_code.insert(
        code.clone(),
        DeviceLogin {
            secret: secret.clone(),
            metadata,
            data,
            client,
            created_at: now,
            created_at_ms,
            expires_at: now + DEVICE_LOGIN_TTL,
            expires_at_ms,
            state: RequestState::Pending,
            claiming: false,
        },
    );
    requests.code_by_secret.insert(secret.clone(), code.clone());
    requests.evict_oldest_pending(MAX_PENDING_DEVICE_LOGINS_PER_ADDRESS, |login| {
        login.client == client
    });
    requests.evict_oldest_pending(MAX_PENDING_DEVICE_LOGINS, |_| true);

    Ok(BegunDeviceLogin {
        code,
        secret,
        created_at_ms,
        expires_at_ms,
    })
}

/// The request behind `secret`. Never creates a session.
pub(crate) fn status(secret: &str) -> DeviceLoginResult<DeviceLoginStatus> {
    require_enabled()?;
    let generation = STATE.generation();
    let requests = requests(&generation);
    let (code, login) = requests
        .live_by_secret(secret.trim(), Instant::now())
        .ok_or(DeviceLoginError::NotFound)?;
    Ok(DeviceLoginStatus {
        code: code.clone(),
        client_name: login.metadata.client_name.clone(),
        data: login.data.clone(),
        created_at_ms: login.created_at_ms,
        expires_at_ms: login.expires_at_ms,
        approved: !login.is_pending(),
    })
}

/// What an approver sees of the request behind `code`. A code it doesn't find counts against the
/// principal as a failed approval would, or previews would be an unthrottled way to guess codes.
pub(crate) fn lookup(principal: &Principal, code: &str) -> DeviceLoginResult<DeviceLoginPreview> {
    require_enabled()?;
    let code = normalize_code(code);
    let now = Instant::now();
    let generation = STATE.generation();
    let mut requests = requests(&generation);
    requests
        .code_guesses
        .admit(&principal.user_public_id, now)
        .map_err(AuthError::RateLimited)?;
    let Some(login) = requests.live(&code, now) else {
        requests.code_guesses.charge(&principal.user_public_id, now);
        return Err(DeviceLoginError::NotFound);
    };
    Ok(DeviceLoginPreview {
        client_name: login.metadata.client_name.clone(),
        client: login.client,
        created_at_ms: login.created_at_ms,
        expires_at_ms: login.expires_at_ms,
    })
}

/// What approving binds: the principal's account, or `target`'s when the principal manages that
/// user, read under one guard as the database has them now.
async fn authorize_approval(
    principal: &Principal,
    credential: &AuthCredential,
    target: Option<&str>,
) -> DeviceLoginResult<Approval> {
    let db = STATE.db.read().await;
    require_enabled()?;
    let current = principal.current(&db)?;
    // The default user only ever stands in while authentication is off. A request that resolved
    // to it then and resumes now no longer authenticates anyone.
    if current.username == lyra_db::users::DEFAULT_USERNAME {
        return Err(AuthError::InvalidBearerCredential.into());
    }
    let principal_db_id = current.require(&*db)?;
    if !credential
        .is_live(&*db, principal_db_id)
        .map_err(AuthError::from)?
    {
        return Err(AuthError::InvalidBearerCredential.into());
    }
    let user = match target.filter(|target| *target != current.user_public_id) {
        None => lyra_db::users::get_by_id(&*db, principal_db_id)
            .map_err(AuthError::from)?
            .ok_or(AuthError::InvalidBearerCredential)?,
        Some(target) => owners::managed_account(
            &*db,
            &current,
            principal_db_id,
            target,
            "cannot approve a device login for a user with permissions you do not hold",
        )?
        .ok_or(DeviceLoginError::UserNotFound)?,
    };
    Ok(Approval {
        user_public_id: user.id,
        password_fingerprint: hash_secret(&user.password),
        approver_public_id: current.user_public_id,
        approver_credential: credential.clone(),
    })
}

/// Approves the request behind `code` for the principal, or for `target`. The first approval wins.
/// `credential` is what the principal authenticated with; the approval dies with it.
pub(crate) async fn approve(
    principal: &Principal,
    credential: &AuthCredential,
    code: &str,
    target: Option<&str>,
) -> DeviceLoginResult<()> {
    require_enabled()?;
    let code = normalize_code(code);
    let approval = authorize_approval(principal, credential, target).await?;

    let now = Instant::now();
    let generation = STATE.generation();
    let mut requests = requests(&generation);
    requests
        .code_guesses
        .admit(&principal.user_public_id, now)
        .map_err(AuthError::RateLimited)?;
    let Some(login) = requests
        .by_code
        .get_mut(&code)
        .filter(|login| !login.is_expired(now))
    else {
        requests.code_guesses.charge(&principal.user_public_id, now);
        return Err(DeviceLoginError::NotFound);
    };
    if !login.is_pending() {
        return Err(DeviceLoginError::AlreadyApproved);
    }
    if approval.user_public_id != approval.approver_public_id {
        tracing::info!(
            approver = %approval.approver_public_id,
            user = %approval.user_public_id,
            "device login approved on behalf of another user"
        );
    }
    login.state = RequestState::Approved(approval);
    login.extend(now);
    Ok(())
}

/// What a claim found before doing any work of its own.
enum ClaimStep {
    Settled(DeviceLoginClaim),
    CreateSession {
        code: String,
        approval: Approval,
        metadata: SessionMetadata,
    },
}

fn claim_step(requests: &Requests, secret: &str) -> DeviceLoginResult<ClaimStep> {
    let (code, login) = requests
        .live_by_secret(secret, Instant::now())
        .ok_or(DeviceLoginError::NotFound)?;
    Ok(match &login.state {
        RequestState::Pending => ClaimStep::Settled(DeviceLoginClaim::Pending),
        RequestState::Claimed { login: session } => {
            ClaimStep::Settled(DeviceLoginClaim::Complete {
                login: session.clone(),
                data: login.data.clone(),
            })
        }
        RequestState::Approved(approval) => ClaimStep::CreateSession {
            code: code.clone(),
            approval: approval.clone(),
            metadata: login.metadata.clone(),
        },
    })
}

/// Holds one request's `claiming` flag while its session is created. Dropping it lets go, also
/// when a client's disconnect cancels the claim.
struct ClaimReservation<'a> {
    generation: &'a GenerationState,
    code: String,
    secret: &'a str,
}

impl Drop for ClaimReservation<'_> {
    fn drop(&mut self) {
        let store = &self.generation.auth_caches.device_logins;
        if let Ok(mut requests) = store.requests.lock()
            && let Some(login) = requests
                .by_code
                .get_mut(&self.code)
                .filter(|login| login.secret == self.secret)
        {
            login.claiming = false;
        }
    }
}

/// Whether `approval` may still sign in `user_db_id`, read under the write lock that inserts the
/// session so a change to any of it either comes first and is seen, or comes after the session
/// exists and revokes it. A password change, any path to it, replaces the stored hash; revoking
/// the session or key that approved, or its expiry, ends the credential. The approver must also
/// still manage the account, since a role change touches neither of those.
fn approval_holds(db: &agdb::DbAny, user_db_id: DbId, approval: &Approval) -> anyhow::Result<bool> {
    if !STATE.config().auth.enabled {
        return Ok(false);
    }
    let Some(user) = lyra_db::users::get_by_id(db, user_db_id)? else {
        return Ok(false);
    };
    if hash_secret(&user.password) != approval.password_fingerprint {
        return Ok(false);
    }
    let Some(approver_db_id) =
        lyra_db::users::find_db_id_by_public_id(db, &approval.approver_public_id)?
    else {
        return Ok(false);
    };
    if !approval.approver_credential.is_live(db, approver_db_id)? {
        return Ok(false);
    }
    let Some(approver) = Principal::load(db, approver_db_id)? else {
        return Ok(false);
    };
    owners::manages(db, &approver, approver_db_id, user_db_id)
}

/// A session for the user `approval` names now, or `None` when the approval no longer holds.
async fn create_session(
    approval: &Approval,
    metadata: SessionMetadata,
) -> AuthResult<Option<LoginResult>> {
    let principal = {
        let db = STATE.db.read().await;
        let Some(user_db_id) =
            lyra_db::users::find_db_id_by_public_id(&*db, &approval.user_public_id)?
        else {
            return Ok(None);
        };
        Principal::load(&*db, user_db_id)?
    };
    let Some(principal) = principal else {
        return Ok(None);
    };
    let created = sessions::create_session_for_user_when(&principal, metadata, |db, user_db_id| {
        approval_holds(db, user_db_id, approval)
    })
    .await;
    match created {
        Ok(session) => Ok(Some(LoginResult {
            token: session.token,
        })),
        Err(AuthError::InvalidBearerCredential) => Ok(None),
        Err(error) => Err(error),
    }
}

/// Signs in the device holding `secret` once its request is approved. The first call after the
/// approval creates the session and repeats return the same one, so a client can retry. No
/// password was proven here, so this never marks the address as one the user logs in from.
pub(crate) async fn claim(secret: &str) -> DeviceLoginResult<DeviceLoginClaim> {
    require_enabled()?;
    let secret = secret.trim();
    let generation = STATE.generation();
    if let ClaimStep::Settled(claim) = claim_step(&requests(&generation), secret)? {
        return Ok(claim);
    }

    let store = &generation.auth_caches.device_logins;
    let _claiming = store.claims.lock().await;
    let (reservation, approval, metadata) = {
        let mut requests = requests(&generation);
        match claim_step(&requests, secret)? {
            ClaimStep::Settled(claim) => return Ok(claim),
            ClaimStep::CreateSession {
                code,
                approval,
                metadata,
            } => {
                requests
                    .by_code
                    .get_mut(&code)
                    .expect("claim_step found this request under the same lock")
                    .claiming = true;
                let reservation = ClaimReservation {
                    generation: &generation,
                    code,
                    secret,
                };
                (reservation, approval, metadata)
            }
        }
    };

    let Some(login) = create_session(&approval, metadata).await? else {
        requests(&generation).remove(&reservation.code);
        return Err(DeviceLoginError::NotFound);
    };
    let mut data = None;
    if let Some(request) = requests(&generation)
        .by_code
        .get_mut(&reservation.code)
        .filter(|request| request.secret == secret)
    {
        request.state = RequestState::Claimed {
            login: login.clone(),
        };
        request.extend(Instant::now());
        data = request.data.clone();
    }
    Ok(DeviceLoginClaim::Complete { login, data })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::{
        auth::{
            accounts,
            api_keys,
            hash_secret,
            login_with_password,
            passwords,
            sessions,
        },
        rate_limit::password_checks,
        testing::{
            create_session as create_test_session,
            for_each_db_gap,
            init_library_test_state,
            publish_config,
            run_with_recycle_at,
            runtime_test_lock,
            user_principal,
        },
    };
    use lyra_db::Permission;

    async fn init(auth_enabled: bool) -> anyhow::Result<()> {
        init_library_test_state(std::path::PathBuf::from(".")).await?;
        let mut config = STATE.config().as_ref().clone();
        config.auth.enabled = auth_enabled;
        publish_config(config);
        Ok(())
    }

    async fn insert_user(
        username: &str,
        permissions: &[Permission],
    ) -> anyhow::Result<(DbId, Principal)> {
        let user_db_id = {
            let mut db = STATE.db.write().await;
            lyra_db::roles::ensure_builtin_roles(&mut db)?;
            let user_db_id =
                lyra_db::users::create(&mut db, &lyra_db::fixtures::test_user(username)?)?;
            if !permissions.is_empty() {
                let role = lyra_db::roles::Role {
                    db_id: None,
                    id: lyra_db::nanoid(),
                    name: format!("{username}-role"),
                    permissions: permissions.to_vec(),
                };
                let role_db_id = lyra_db::roles::create(&mut db, &role)?;
                lyra_db::roles::assign_role_to_user(&mut db, user_db_id, role_db_id)?;
            }
            user_db_id
        };
        Ok((user_db_id, user_principal(user_db_id).await?))
    }

    /// Approves with an API key of the principal's, which adds no session to count.
    async fn approve_as(
        principal: &Principal,
        code: &str,
        target: Option<&str>,
    ) -> DeviceLoginResult<()> {
        let key = api_keys::create_api_key(principal, &lyra_db::nanoid())
            .await
            .expect("the key is created");
        let credential = AuthCredential::ApiKey {
            public_id: key.id,
            name: key.name,
        };
        approve(principal, &credential, code, target).await
    }

    fn without_rate_limits() {
        let mut startup = STATE.startup_config.get().as_ref().clone();
        startup.rate_limit.enabled = false;
        STATE.startup_config.replace(std::sync::Arc::new(startup));
    }

    fn begin_letters() -> DeviceLoginResult<BegunDeviceLogin> {
        begin(
            SessionMetadata {
                user_agent: Some("agent".to_string()),
                client_name: Some("Phone".to_string()),
            },
            password_checks::fresh_address(),
            None,
            None,
        )
    }

    fn expire(code: &str) {
        let generation = STATE.generation();
        requests(&generation)
            .by_code
            .get_mut(code)
            .expect("the request exists")
            .expires_at = Instant::now();
    }

    fn token_of(claim: DeviceLoginClaim) -> String {
        match claim {
            DeviceLoginClaim::Complete { login, .. } => login.token,
            DeviceLoginClaim::Pending => panic!("the claim should be complete"),
        }
    }

    #[test]
    fn sampling_rejects_values_past_the_last_whole_multiple() {
        let mut values = [u32::MAX, u32::MAX - 1, 7].into_iter();
        assert_eq!(
            sample_below(10, || values.next().expect("a value is accepted")),
            7
        );
    }

    #[test]
    fn generated_codes_are_six_of_the_code_letters() {
        for _ in 0..500 {
            let code = generate_code();
            assert_eq!(code.len(), CODE_LENGTH);
            assert!(code.bytes().all(|byte| CODE_LETTERS.contains(&byte)));
        }
    }

    #[test]
    fn code_letters_leave_out_the_ambiguous_ones() {
        for letter in [b'I', b'L', b'O'] {
            assert!(!CODE_LETTERS.contains(&letter));
        }
    }

    #[test]
    fn codes_normalize_whitespace_and_case() {
        assert_eq!(normalize_code(" ab c\tde\nf "), "ABCDEF");
        assert_eq!(normalize_code("123 456"), "123456");
    }

    #[tokio::test]
    async fn every_operation_is_off_while_authentication_is() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init(true).await?;
        let (_, principal) = insert_user("off-owner", &[Permission::Admin]).await?;
        let begun = begin_letters()?;
        approve_as(&principal, &begun.code, None).await?;

        let mut config = STATE.config().as_ref().clone();
        config.auth.enabled = false;
        publish_config(config);

        assert!(matches!(begin_letters(), Err(DeviceLoginError::Disabled)));
        assert!(matches!(
            status(&begun.secret),
            Err(DeviceLoginError::Disabled)
        ));
        assert!(matches!(
            lookup(&principal, &begun.code),
            Err(DeviceLoginError::Disabled)
        ));
        assert!(matches!(
            approve_as(&principal, &begun.code, Some(&principal.user_public_id)).await,
            Err(DeviceLoginError::Disabled)
        ));
        assert!(matches!(
            claim(&begun.secret).await,
            Err(DeviceLoginError::Disabled)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn a_request_runs_from_begin_to_a_retryable_claim() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init(true).await?;
        let (user_db_id, principal) = insert_user("flow-owner", &[]).await?;

        let begun = begin_letters()?;
        assert!(matches!(
            claim(&begun.secret).await?,
            DeviceLoginClaim::Pending
        ));
        let before = status(&begun.secret)?;
        assert!(!before.approved);
        assert_eq!(before.code, begun.code);
        assert_eq!(before.client_name.as_deref(), Some("Phone"));

        let preview = lookup(&principal, &begun.code)?;
        assert_eq!(preview.client_name.as_deref(), Some("Phone"));
        assert_eq!(preview.created_at_ms, begun.created_at_ms);

        approve_as(&principal, &begun.code.to_lowercase(), None).await?;
        assert!(status(&begun.secret)?.approved);
        {
            let db = STATE.db.read().await;
            assert!(
                lyra_db::users::find_sessions_for_user(&db, user_db_id)?.is_empty(),
                "status and approval create no session"
            );
        }

        let token = token_of(claim(&begun.secret).await?);
        assert_eq!(token_of(claim(&begun.secret).await?), token);
        assert!(status(&begun.secret)?.approved);

        let db = STATE.db.read().await;
        assert_eq!(
            lyra_db::users::find_sessions_for_user(&db, user_db_id)?.len(),
            1
        );
        let (user, session, _) =
            lyra_db::users::find_by_session_token_hash(&db, &hash_secret(&token))?
                .expect("the session exists");
        assert_eq!(user.id, principal.user_public_id);
        assert_eq!(session.client_name.as_deref(), Some("Phone"));
        assert_eq!(session.user_agent.as_deref(), Some("agent"));
        Ok(())
    }

    #[tokio::test]
    async fn a_request_keeps_its_data_for_whoever_holds_the_secret() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init(true).await?;
        let (_, principal) = insert_user("data-owner", &[]).await?;
        let begin_with_data = |data| {
            begin(
                SessionMetadata::default(),
                password_checks::fresh_address(),
                Some(data),
                None,
            )
        };

        let data = serde_json::json!({ "device": "Pixel" });
        let begun = begin_with_data(data.clone())?;
        assert_eq!(status(&begun.secret)?.data, Some(data.clone()));
        approve_as(&principal, &begun.code, None).await?;
        for _ in 0..2 {
            let DeviceLoginClaim::Complete { data: claimed, .. } = claim(&begun.secret).await?
            else {
                anyhow::bail!("an approved login completes");
            };
            assert_eq!(claimed, Some(data.clone()));
        }

        let without = begin_with_data(serde_json::Value::Null)?;
        assert_eq!(status(&without.secret)?.data, None);
        assert!(matches!(
            begin_with_data(serde_json::json!("x".repeat(MAX_DEVICE_LOGIN_DATA_BYTES))),
            Err(DeviceLoginError::DataTooLarge)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn a_request_takes_the_code_its_caller_gives() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init(true).await?;
        let (_, principal) = insert_user("given-owner", &[]).await?;
        let begin_given = |code| {
            begin(
                SessionMetadata::default(),
                password_checks::fresh_address(),
                None,
                Some(code),
            )
        };

        let begun = begin_given(" 123 456 ")?;
        assert_eq!(begun.code, "123456");
        assert!(matches!(
            begin_given("123456"),
            Err(DeviceLoginError::CodeTaken)
        ));
        assert_eq!(begin_given("ab12cd")?.code, "AB12CD");
        for unusable in ["12345", "123-456", "１２３４５６", &"1".repeat(33)] {
            assert!(
                matches!(begin_given(unusable), Err(DeviceLoginError::InvalidCode)),
                "{unusable}"
            );
        }

        approve_as(&principal, "123 456", None).await?;
        assert!(status(&begun.secret)?.approved);
        Ok(())
    }

    #[tokio::test]
    async fn a_second_approval_is_told_apart_from_an_unknown_code() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init(true).await?;
        let (first_db_id, first) = insert_user("first-approver", &[]).await?;
        let (second_db_id, second) = insert_user("second-approver", &[]).await?;
        let begun = begin_letters()?;

        approve_as(&first, &begun.code, None).await?;
        assert!(matches!(
            approve_as(&second, &begun.code, None).await,
            Err(DeviceLoginError::AlreadyApproved)
        ));
        assert!(matches!(
            approve_as(&second, "NOSUCH", None).await,
            Err(DeviceLoginError::NotFound)
        ));

        token_of(claim(&begun.secret).await?);
        let db = STATE.db.read().await;
        assert_eq!(
            lyra_db::users::find_sessions_for_user(&db, first_db_id)?.len(),
            1
        );
        assert!(lyra_db::users::find_sessions_for_user(&db, second_db_id)?.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn an_expired_request_is_not_found_everywhere() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init(true).await?;
        let (_, principal) = insert_user("expiry-owner", &[]).await?;
        let begun = begin_letters()?;
        expire(&begun.code);

        assert!(matches!(
            status(&begun.secret),
            Err(DeviceLoginError::NotFound)
        ));
        assert!(matches!(
            lookup(&principal, &begun.code),
            Err(DeviceLoginError::NotFound)
        ));
        assert!(matches!(
            approve_as(&principal, &begun.code, None).await,
            Err(DeviceLoginError::NotFound)
        ));
        assert!(matches!(
            claim(&begun.secret).await,
            Err(DeviceLoginError::NotFound)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn approval_and_claim_each_restart_the_retention() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init(true).await?;
        let (_, principal) = insert_user("retained-owner", &[]).await?;
        let begun = begin_letters()?;

        approve_as(&principal, &begun.code, None).await?;
        let generation = STATE.generation();
        let approved_expiry = requests(&generation).by_code[&begun.code].expires_at;
        assert!(approved_expiry > Instant::now() + DEVICE_LOGIN_TTL - Duration::from_secs(5));

        tokio::time::sleep(Duration::from_millis(5)).await;
        token_of(claim(&begun.secret).await?);
        assert!(requests(&generation).by_code[&begun.code].expires_at > approved_expiry);
        Ok(())
    }

    #[tokio::test]
    async fn a_collision_draws_another_code() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init(true).await?;
        let taken = begin_letters()?;

        let mut codes = [taken.code.clone(), "ZZZZZZ".to_string()].into_iter();
        let begun = begin_with(
            SessionMetadata::default(),
            password_checks::fresh_address(),
            None,
            MAX_CODE_ATTEMPTS,
            || codes.next().expect("two draws are enough"),
        )?;
        assert_eq!(begun.code, "ZZZZZZ");
        assert_eq!(status(&taken.secret)?.code, taken.code);
        assert_eq!(status(&begun.secret)?.code, "ZZZZZZ");
        Ok(())
    }

    #[tokio::test]
    async fn an_address_only_evicts_its_own_oldest_pending_request() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init(true).await?;
        without_rate_limits();
        let flooder = password_checks::fresh_address();
        let (_, principal) = insert_user("cap-owner", &[]).await?;
        let begin_flooded = || begin(SessionMetadata::default(), flooder, None, None);

        let bystander = begin_letters()?;
        let approved = begin_flooded()?;
        approve_as(&principal, &approved.code, None).await?;
        let oldest = begin_flooded()?;
        for _ in 0..MAX_PENDING_DEVICE_LOGINS_PER_ADDRESS {
            begin_flooded()?;
        }

        assert!(matches!(
            status(&oldest.secret),
            Err(DeviceLoginError::NotFound)
        ));
        assert!(status(&bystander.secret).is_ok());
        assert!(
            status(&approved.secret).is_ok(),
            "an approved request stays"
        );
        Ok(())
    }

    #[tokio::test]
    async fn the_global_cap_evicts_the_oldest_pending_request() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init(true).await?;
        let oldest = begin_letters()?;
        for _ in 1..MAX_PENDING_DEVICE_LOGINS {
            begin_letters()?;
        }
        assert!(status(&oldest.secret).is_ok());

        begin_letters()?;
        assert!(matches!(
            status(&oldest.secret),
            Err(DeviceLoginError::NotFound)
        ));
        let generation = STATE.generation();
        let requests = requests(&generation);
        assert_eq!(requests.by_code.len(), MAX_PENDING_DEVICE_LOGINS);
        assert_eq!(requests.code_by_secret.len(), MAX_PENDING_DEVICE_LOGINS);
        Ok(())
    }

    #[tokio::test]
    async fn begin_is_charged_as_a_login() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init(true).await?;
        let mut startup = STATE.startup_config.get().as_ref().clone();
        startup.rate_limit.enabled = true;
        startup.rate_limit.login_per_minute = 60;
        startup.rate_limit.login_burst = 1;
        STATE.startup_config.replace(std::sync::Arc::new(startup));
        let client = password_checks::fresh_address();

        begin(SessionMetadata::default(), client, None, None)?;
        assert!(matches!(
            begin(SessionMetadata::default(), client, None, None),
            Err(DeviceLoginError::Auth(AuthError::RateLimited(_)))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn an_unusable_client_name_is_refused_at_begin() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init(true).await?;
        let refused = begin(
            SessionMetadata {
                user_agent: None,
                client_name: Some("bad\u{0007}name".to_string()),
            },
            password_checks::fresh_address(),
            None,
            None,
        );
        assert!(matches!(
            refused,
            Err(DeviceLoginError::Auth(AuthError::InvalidClientName(_)))
        ));
        Ok(())
    }

    /// How many wrong codes a user gets, as the limiter's burst has them.
    const GUESSES: u32 = 10;

    #[tokio::test]
    async fn wrong_codes_throttle_the_user_who_guesses() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init(true).await?;
        let (_, guesser) = insert_user("guesser", &[]).await?;
        let (_, bystander) = insert_user("bystander", &[]).await?;
        let begun = begin_letters()?;

        // A preview that finds nothing counts the same as an approval that finds nothing.
        for _ in 0..GUESSES / 2 {
            assert!(matches!(
                lookup(&guesser, "NOSUCH"),
                Err(DeviceLoginError::NotFound)
            ));
        }
        for _ in GUESSES / 2..GUESSES {
            assert!(matches!(
                approve_as(&guesser, "NOSUCH", None).await,
                Err(DeviceLoginError::NotFound)
            ));
        }
        for attempt in [
            approve_as(&guesser, &begun.code, None).await.map(|_| ()),
            lookup(&guesser, &begun.code).map(|_| ()),
        ] {
            assert!(matches!(
                attempt,
                Err(DeviceLoginError::Auth(AuthError::RateLimited(_)))
            ));
        }

        approve_as(&bystander, &begun.code, None).await?;
        Ok(())
    }

    #[tokio::test]
    async fn wrong_codes_are_throttled_with_rate_limits_off() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init(true).await?;
        without_rate_limits();
        let (_, guesser) = insert_user("unlimited-guesser", &[]).await?;

        for _ in 0..GUESSES {
            assert!(matches!(
                lookup(&guesser, "NOSUCH"),
                Err(DeviceLoginError::NotFound)
            ));
        }
        assert!(matches!(
            lookup(&guesser, "NOSUCH"),
            Err(DeviceLoginError::Auth(AuthError::RateLimited(_)))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_claims_create_one_session() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init(true).await?;
        let (user_db_id, principal) = insert_user("concurrent-owner", &[]).await?;
        let begun = begin_letters()?;
        approve_as(&principal, &begun.code, None).await?;

        let claims = (0..16).map(|_| claim(&begun.secret));
        let tokens = futures::future::join_all(claims)
            .await
            .into_iter()
            .map(|claim| claim.map(token_of))
            .collect::<Result<Vec<_>, _>>()?;
        assert!(tokens.iter().all(|token| token == &tokens[0]));

        let db = STATE.db.read().await;
        assert_eq!(
            lyra_db::users::find_sessions_for_user(&db, user_db_id)?.len(),
            1
        );
        Ok(())
    }

    #[tokio::test]
    async fn approving_for_another_user_needs_manage_users_over_them() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init(true).await?;
        let (_, listener) = insert_user("listener", &[]).await?;
        let (target_db_id, target) = insert_user("target", &[]).await?;
        let (_, manager) = insert_user("manager", &[Permission::ManageUsers]).await?;
        let (_, admin) = insert_user("the-admin", &[Permission::Admin]).await?;

        let refused = begin_letters()?;
        assert!(matches!(
            approve_as(&listener, &refused.code, Some(&target.user_public_id)).await,
            Err(DeviceLoginError::Auth(AuthError::Forbidden(_)))
        ));
        assert!(matches!(
            approve_as(&manager, &refused.code, Some(&admin.user_public_id)).await,
            Err(DeviceLoginError::Auth(AuthError::Forbidden(_)))
        ));
        assert!(matches!(
            approve_as(&manager, &refused.code, Some("no-such-user")).await,
            Err(DeviceLoginError::UserNotFound)
        ));
        assert!(!status(&refused.secret)?.approved);

        let allowed = begin_letters()?;
        approve_as(&manager, &allowed.code, Some(&target.user_public_id)).await?;
        let token = token_of(claim(&allowed.secret).await?);
        let db = STATE.db.read().await;
        let (user, ..) = lyra_db::users::find_by_session_token_hash(&db, &hash_secret(&token))?
            .expect("the session exists");
        assert_eq!(user.id, target.user_public_id);
        assert_eq!(
            lyra_db::users::find_sessions_for_user(&db, target_db_id)?.len(),
            1
        );
        Ok(())
    }

    #[tokio::test]
    async fn the_default_user_cannot_be_named_while_authentication_is_on() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init(true).await?;
        let (_, admin) = insert_user("default-admin", &[Permission::Admin]).await?;
        crate::services::auth::ensure_default_user(&STATE.config()).await?;
        let default_public_id = {
            let db = STATE.db.read().await;
            lyra_db::users::get_by_username(&db, lyra_db::users::DEFAULT_USERNAME)?
                .expect("the default user exists")
                .id
        };
        let begun = begin_letters()?;
        assert!(matches!(
            approve_as(&admin, &begun.code, Some(&default_public_id)).await,
            Err(DeviceLoginError::UserNotFound)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn a_user_deleted_before_the_claim_gets_no_session() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init(true).await?;
        let (user_db_id, principal) = insert_user("leaving", &[]).await?;
        let begun = begin_letters()?;
        approve_as(&principal, &begun.code, None).await?;
        {
            let mut db = STATE.db.write().await;
            lyra_db::users::delete_user(&mut db, user_db_id)?;
        }

        assert!(matches!(
            claim(&begun.secret).await,
            Err(DeviceLoginError::NotFound)
        ));
        assert!(matches!(
            status(&begun.secret),
            Err(DeviceLoginError::NotFound)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn a_claim_takes_a_recycled_id_for_no_one() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init(true).await?;
        let (user_db_id, principal) = insert_user("recycled-claim", &[]).await?;
        let begun = begin_letters()?;
        approve_as(&principal, &begun.code, None).await?;
        {
            let mut db = STATE.db.write().await;
            lyra_db::test_db::recycle_user(&mut db, user_db_id, "replacement")?;
        }

        assert!(matches!(
            claim(&begun.secret).await,
            Err(DeviceLoginError::NotFound)
        ));
        let db = STATE.db.read().await;
        assert!(lyra_db::users::find_sessions_for_user(&db, user_db_id)?.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn a_claim_does_not_vouch_for_its_address() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init(true).await?;
        const NAME_BURST: u32 = 10;
        let (user_db_id, principal) = insert_user("code-only", &[]).await?;
        let password = "correct-login-password";
        let hash = passwords::hash(password).await?;
        {
            let mut db = STATE.db.write().await;
            db.transaction_mut(|t| lyra_db::users::update_user_password(t, user_db_id, &hash))?;
        }
        let client = password_checks::fresh_address();

        let begun = begin(SessionMetadata::default(), client, None, None)?;
        approve_as(&principal, &begun.code, None).await?;
        token_of(claim(&begun.secret).await?);

        password_checks::spend("code-only", NAME_BURST);
        assert!(matches!(
            login_with_password("code-only", password, Default::default(), client).await,
            Err(AuthError::RateLimited(_))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn the_session_a_claim_returns_authenticates() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init(true).await?;
        let (user_db_id, principal) = insert_user("signed-in", &[]).await?;
        // The approver's own session is independent of the one the claim creates.
        let approver_session = create_test_session(user_db_id, Default::default()).await?;
        let begun = begin_letters()?;
        approve_as(&principal, &begun.code, None).await?;
        let token = token_of(claim(&begun.secret).await?);
        assert_ne!(token, approver_session.token);
        sessions::revoke_session_by_token(&approver_session.token).await?;

        let auth = crate::services::auth::require_auth(Some(&token)).await?;
        assert_eq!(auth.principal.user_public_id, principal.user_public_id);
        Ok(())
    }

    /// A password-hashed user with an API key approving their own device login, returning the
    /// request and the key.
    async fn approved_by_key(
        username: &str,
    ) -> anyhow::Result<(DbId, Principal, BegunDeviceLogin, String)> {
        let (user_db_id, principal) = insert_user(username, &[]).await?;
        let key = api_keys::create_api_key(&principal, "approver").await?;
        let credential = AuthCredential::ApiKey {
            public_id: key.id.clone(),
            name: key.name,
        };
        let begun = begin_letters()?;
        approve(&principal, &credential, &begun.code, None).await?;
        Ok((user_db_id, principal, begun, key.id))
    }

    async fn assert_claim_refused(user_db_id: DbId, secret: &str) -> anyhow::Result<()> {
        assert!(matches!(
            claim(secret).await,
            Err(DeviceLoginError::NotFound)
        ));
        let db = STATE.db.read().await;
        assert!(lyra_db::users::find_sessions_for_user(&db, user_db_id)?.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn an_admin_password_reset_kills_an_approval() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init(true).await?;
        let (_, admin) = insert_user("reset-admin", &[Permission::Admin]).await?;
        let (user_db_id, principal, begun, _) = approved_by_key("reset-target").await?;

        accounts::set_password(&admin, &principal.user_public_id, "replacement-password").await?;
        assert_claim_refused(user_db_id, &begun.secret).await
    }

    #[tokio::test]
    async fn changing_ones_own_password_kills_an_approval() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init(true).await?;
        let password = "correct-login-password";
        let (user_db_id, principal) = insert_user("rotator", &[]).await?;
        let hash = passwords::hash(password).await?;
        STATE
            .db
            .write()
            .await
            .transaction_mut(|t| lyra_db::users::update_user_password(t, user_db_id, &hash))?;
        // The session that changes the password survives the change, and approved the login.
        let session = create_test_session(user_db_id, Default::default()).await?;
        let auth = crate::services::auth::require_auth(Some(&session.token)).await?;
        let AuthCredential::Session { public_id } = auth.credential.clone() else {
            panic!("a session credential");
        };
        let begun = begin_letters()?;
        approve(&principal, &auth.credential, &begun.code, None).await?;

        accounts::change_own_password(
            &principal,
            &public_id,
            password,
            "replacement-password",
            password_checks::fresh_address(),
        )
        .await?;

        assert!(matches!(
            claim(&begun.secret).await,
            Err(DeviceLoginError::NotFound)
        ));
        let db = STATE.db.read().await;
        assert_eq!(
            lyra_db::users::find_sessions_for_user(&db, user_db_id)?.len(),
            1,
            "only the session that changed the password is left"
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_password_reset_of_the_named_user_kills_an_on_behalf_approval() -> anyhow::Result<()>
    {
        let _guard = runtime_test_lock().await;
        init(true).await?;
        let (_, admin) = insert_user("behalf-admin", &[Permission::Admin]).await?;
        let (target_db_id, target) = insert_user("behalf-target", &[]).await?;
        let begun = begin_letters()?;
        approve_as(&admin, &begun.code, Some(&target.user_public_id)).await?;

        accounts::set_password(&admin, &target.user_public_id, "replacement-password").await?;
        assert_claim_refused(target_db_id, &begun.secret).await
    }

    #[tokio::test]
    async fn losing_manage_users_kills_an_on_behalf_approval() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init(true).await?;
        let (manager_db_id, manager) =
            insert_user("demoted-manager", &[Permission::ManageUsers]).await?;
        let (target_db_id, target) = insert_user("demoted-target", &[]).await?;
        let begun = begin_letters()?;
        approve_as(&manager, &begun.code, Some(&target.user_public_id)).await?;

        {
            let mut db = STATE.db.write().await;
            let mut role = lyra_db::roles::get_role_for_user(&*db, manager_db_id)?
                .expect("manager has a role");
            role.permissions.clear();
            lyra_db::roles::update(&mut *db, &role)?;
        }
        assert_claim_refused(target_db_id, &begun.secret).await
    }

    #[tokio::test]
    async fn a_target_promoted_past_the_approver_kills_an_on_behalf_approval() -> anyhow::Result<()>
    {
        let _guard = runtime_test_lock().await;
        init(true).await?;
        let (_, manager) = insert_user("outranked-manager", &[Permission::ManageUsers]).await?;
        let (target_db_id, target) = insert_user("promoted-target", &[]).await?;
        let begun = begin_letters()?;
        approve_as(&manager, &begun.code, Some(&target.user_public_id)).await?;

        {
            let mut db = STATE.db.write().await;
            let role = lyra_db::roles::Role {
                db_id: None,
                id: lyra_db::nanoid(),
                name: "promoted-target-role".to_string(),
                permissions: vec![Permission::Admin],
            };
            let role_db_id = lyra_db::roles::create(&mut db, &role)?;
            lyra_db::roles::assign_role_to_user(&mut db, target_db_id, role_db_id)?;
        }
        assert_claim_refused(target_db_id, &begun.secret).await
    }

    #[tokio::test]
    async fn revoking_the_approving_session_kills_its_approval() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init(true).await?;
        let (user_db_id, principal) = insert_user("revoked-approver", &[]).await?;
        let session = create_test_session(user_db_id, Default::default()).await?;
        let auth = crate::services::auth::require_auth(Some(&session.token)).await?;
        let begun = begin_letters()?;
        approve(&principal, &auth.credential, &begun.code, None).await?;

        sessions::revoke_session_by_token(&session.token).await?;
        assert_claim_refused(user_db_id, &begun.secret).await
    }

    #[tokio::test]
    async fn deleting_the_approving_key_kills_its_approval() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init(true).await?;
        let (user_db_id, principal, begun, key_id) = approved_by_key("key-approver").await?;

        assert!(api_keys::revoke_api_key_for_user(&principal, &key_id).await?);
        assert_claim_refused(user_db_id, &begun.secret).await
    }

    #[tokio::test]
    async fn a_dead_credential_cannot_approve() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init(true).await?;
        let (user_db_id, principal) = insert_user("dead-approver", &[]).await?;
        let session = create_test_session(user_db_id, Default::default()).await?;
        let auth = crate::services::auth::require_auth(Some(&session.token)).await?;
        sessions::revoke_session_by_token(&session.token).await?;
        let begun = begin_letters()?;

        assert!(matches!(
            approve(&principal, &auth.credential, &begun.code, None).await,
            Err(DeviceLoginError::Auth(AuthError::InvalidBearerCredential))
        ));
        assert!(!status(&begun.secret)?.approved);
        Ok(())
    }

    /// Claims while, at gap `gap`, `change` applies to the database or the config.
    async fn claim_with_change_at(
        gap: Option<usize>,
        username: &str,
        change: impl FnOnce(&mut agdb::DbAny, DbId),
    ) -> anyhow::Result<(bool, DbId, DeviceLoginResult<DeviceLoginClaim>)> {
        init(true).await?;
        let (user_db_id, _, begun, _) = approved_by_key(username).await?;
        let (reached, outcome) =
            run_with_recycle_at(gap, claim(&begun.secret), |db| change(db, user_db_id)).await;
        Ok((reached, user_db_id, outcome))
    }

    #[tokio::test]
    async fn a_password_change_racing_a_claim_stops_it() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        for_each_db_gap(
            async |gap| {
                let (reached, user_db_id, outcome) =
                    claim_with_change_at(gap, "racing-password", |db, user_db_id| {
                        lyra_db::users::update_user_password(db, user_db_id, "replaced-hash")
                            .expect("change the password");
                    })
                    .await?;
                Ok((reached, (user_db_id, outcome)))
            },
            async |gap, (user_db_id, outcome): (DbId, DeviceLoginResult<DeviceLoginClaim>)| {
                assert!(
                    matches!(outcome, Err(DeviceLoginError::NotFound)),
                    "a claim survived a password change at gap {gap}"
                );
                let db = STATE.db.read().await;
                assert!(lyra_db::users::find_sessions_for_user(&db, user_db_id)?.is_empty());
                Ok(())
            },
        )
        .await
    }

    #[tokio::test]
    async fn turning_authentication_off_during_a_claim_stops_it() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        for_each_db_gap(
            async |gap| {
                let (reached, user_db_id, outcome) =
                    claim_with_change_at(gap, "racing-config", |_, _| {
                        let mut config = STATE.config().as_ref().clone();
                        config.auth.enabled = false;
                        publish_config(config);
                    })
                    .await?;
                Ok((reached, (user_db_id, outcome)))
            },
            async |gap, (user_db_id, outcome): (DbId, DeviceLoginResult<DeviceLoginClaim>)| {
                assert!(
                    outcome.is_err(),
                    "a claim made a session with authentication off at gap {gap}"
                );
                let db = STATE.db.read().await;
                assert!(lyra_db::users::find_sessions_for_user(&db, user_db_id)?.is_empty());
                Ok(())
            },
        )
        .await
    }

    #[tokio::test]
    async fn the_default_user_cannot_approve_once_authentication_is_on() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init(true).await?;
        let (_, admin) = insert_user("real-admin", &[Permission::Admin]).await?;
        crate::services::auth::ensure_default_user(&STATE.config()).await?;
        let default_db_id = {
            let db = STATE.db.read().await;
            lyra_db::users::get_by_username(&db, lyra_db::users::DEFAULT_USERNAME)?
                .and_then(|user| user.db_id)
                .expect("the default user exists")
        };
        let stale_default = user_principal(default_db_id).await?;
        let begun = begin_letters()?;

        assert!(matches!(
            approve(
                &stale_default,
                &AuthCredential::Default,
                &begun.code,
                Some(&admin.user_public_id),
            )
            .await,
            Err(DeviceLoginError::Auth(AuthError::InvalidBearerCredential))
        ));
        assert!(!status(&begun.secret)?.approved);
        Ok(())
    }

    #[tokio::test]
    async fn an_approval_rechecks_authentication_once_it_holds_the_database() -> anyhow::Result<()>
    {
        let _guard = runtime_test_lock().await;
        init(true).await?;
        let (user_db_id, principal) = insert_user("late-approver", &[]).await?;
        let session = create_test_session(user_db_id, Default::default()).await?;
        let auth = crate::services::auth::require_auth(Some(&session.token)).await?;
        let begun = begin_letters()?;
        let code = begun.code.clone();

        let db_write = STATE.db.write().await;
        let approval =
            tokio::spawn(async move { approve(&principal, &auth.credential, &code, None).await });
        tokio::task::yield_now().await;
        let mut config = STATE.config().as_ref().clone();
        config.auth.enabled = false;
        publish_config(config);
        drop(db_write);

        assert!(matches!(approval.await?, Err(DeviceLoginError::Disabled)));
        Ok(())
    }

    fn is_claiming(code: &str) -> bool {
        let generation = STATE.generation();
        requests(&generation).by_code[code].claiming
    }

    async fn until_claiming(code: &str) {
        while !is_claiming(code) {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test]
    async fn a_claim_keeps_its_request_and_code_while_it_creates_the_session() -> anyhow::Result<()>
    {
        let _guard = runtime_test_lock().await;
        init(true).await?;
        let (user_db_id, principal) = insert_user("reserved", &[]).await?;
        let begun = begin_letters()?;
        approve_as(&principal, &begun.code, None).await?;

        let db_write = STATE.db.write().await;
        let secret = begun.secret.clone();
        let claiming = tokio::spawn(async move { claim(&secret).await });
        until_claiming(&begun.code).await;

        // The request's time runs out while it is reserved, and a new request draws its code.
        expire(&begun.code);
        let mut codes = [begun.code.clone(), "ZZZZZZ".to_string()].into_iter();
        let newcomer = begin_with(
            SessionMetadata::default(),
            password_checks::fresh_address(),
            None,
            MAX_CODE_ATTEMPTS,
            || codes.next().expect("two draws are enough"),
        )?;
        assert_eq!(newcomer.code, "ZZZZZZ", "a reserved code is not reused");
        assert!(status(&begun.secret).is_ok(), "a reserved request stays");

        drop(db_write);
        let token = token_of(claiming.await??);
        assert_eq!(token_of(claim(&begun.secret).await?), token);
        assert!(!is_claiming(&begun.code), "the claim lets go");
        assert!(matches!(
            claim(&newcomer.secret).await?,
            DeviceLoginClaim::Pending
        ));
        assert!(!status(&newcomer.secret)?.approved);

        let db = STATE.db.read().await;
        assert_eq!(
            lyra_db::users::find_sessions_for_user(&db, user_db_id)?.len(),
            1
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_cancelled_claim_lets_go_of_its_request() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init(true).await?;
        let (_, principal) = insert_user("cancelled", &[]).await?;
        let begun = begin_letters()?;
        approve_as(&principal, &begun.code, None).await?;

        let db_write = STATE.db.write().await;
        let secret = begun.secret.clone();
        let claiming = tokio::spawn(async move { claim(&secret).await });
        until_claiming(&begun.code).await;
        claiming.abort();
        assert!(claiming.await.is_err_and(|error| error.is_cancelled()));
        drop(db_write);

        assert!(!is_claiming(&begun.code));
        token_of(claim(&begun.secret).await?);
        Ok(())
    }
}
