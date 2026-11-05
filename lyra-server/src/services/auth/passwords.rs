// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! The only home of password hashing. Argon2 work is deliberately expensive, so every hash and
//! check runs off the async workers and only a few run at once across the server, whoever asks.

#![allow(clippy::disallowed_methods)]

use std::num::NonZeroUsize;
use std::sync::LazyLock;

use argon2::{
    Argon2,
    PasswordHash,
    PasswordHasher,
    PasswordVerifier,
    password_hash::{
        SaltString,
        rand_core::{
            OsRng,
            RngCore,
        },
    },
};
use tokio::sync::{
    OnceCell,
    Semaphore,
};

const MIN_LENGTH: usize = 8;

static PASSWORD_WORK: LazyLock<Semaphore> = LazyLock::new(|| Semaphore::new(slot_count()));

fn slot_count() -> usize {
    let cores = std::thread::available_parallelism().map_or(1, NonZeroUsize::get);
    (cores / 2).max(1)
}

/// Checked when no user matches, so a miss costs as much as a wrong password.
static NO_MATCH_HASH: OnceCell<String> = OnceCell::const_new();

#[derive(Debug, thiserror::Error)]
pub(crate) enum InvalidPassword {
    #[error("password cannot be empty")]
    Empty,
    #[error("password must be at least {MIN_LENGTH} characters long")]
    TooShort,
    #[error("password must be ASCII")]
    NotAscii,
}

pub(crate) fn validate(password: &str) -> Result<(), InvalidPassword> {
    if password.is_empty() {
        return Err(InvalidPassword::Empty);
    }
    if password.len() < MIN_LENGTH {
        return Err(InvalidPassword::TooShort);
    }
    if !password.is_ascii() {
        return Err(InvalidPassword::NotAscii);
    }
    Ok(())
}

pub(crate) async fn hash(password: &str) -> anyhow::Result<String> {
    let password = password.to_owned();
    run(move || hash_blocking(password.as_bytes())).await
}

pub(crate) async fn matches(password: &str, stored: String) -> anyhow::Result<bool> {
    let password = password.to_owned();
    run(move || {
        let parsed = PasswordHash::new(&stored)?;
        Ok(Argon2::default()
            .verify_password(password.as_bytes(), &parsed)
            .is_ok())
    })
    .await
}

/// Computes the hash [`match_nothing`] checks against, so no login pays for it. Call before
/// serving requests.
pub(crate) async fn prepare() -> anyhow::Result<()> {
    NO_MATCH_HASH.get_or_try_init(unusable).await?;
    Ok(())
}

/// Spends the cost of a check for a login that matched no user.
pub(crate) async fn match_nothing(password: &str) -> anyhow::Result<()> {
    let stored = NO_MATCH_HASH.get_or_try_init(unusable).await?.clone();
    matches(password, stored).await?;
    Ok(())
}

/// A hash of a random secret nobody knows, for accounts that cannot log in with a password.
pub(crate) async fn unusable() -> anyhow::Result<String> {
    run(|| {
        let mut secret = [0_u8; 32];
        OsRng.fill_bytes(&mut secret);
        hash_blocking(&secret)
    })
    .await
}

fn hash_blocking(password: &[u8]) -> anyhow::Result<String> {
    let salt = SaltString::generate(&mut OsRng);
    Ok(Argon2::default()
        .hash_password(password, &salt)
        .map_err(|error| anyhow::anyhow!("password hashing failed: {error}"))?
        .to_string())
}

async fn run<T: Send + 'static>(
    work: impl FnOnce() -> anyhow::Result<T> + Send + 'static,
) -> anyhow::Result<T> {
    run_on(&PASSWORD_WORK, work).await
}

/// The permit moves into the blocking task, so a caller that stops waiting does not free its slot
/// while the work still runs.
async fn run_on<T: Send + 'static>(
    slots: &'static Semaphore,
    work: impl FnOnce() -> anyhow::Result<T> + Send + 'static,
) -> anyhow::Result<T> {
    let permit = slots
        .acquire()
        .await
        .expect("the password work semaphore is never closed");
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        work()
    })
    .await?
}

/// Takes every slot, so any hash or check waits until the permit drops.
#[cfg(test)]
pub(crate) async fn hold_all_slots() -> tokio::sync::SemaphorePermit<'static> {
    let slots = u32::try_from(slot_count()).expect("slot count fits u32");
    PASSWORD_WORK
        .acquire_many(slots)
        .await
        .expect("the password work semaphore is never closed")
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn a_caller_that_stops_waiting_keeps_its_slot_until_the_work_ends() -> anyhow::Result<()>
    {
        let slots: &'static Semaphore = Box::leak(Box::new(Semaphore::new(1)));
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (finish_tx, finish_rx) = std::sync::mpsc::channel::<()>();
        let caller = tokio::spawn(run_on(slots, move || {
            let _ = started_tx.send(());
            let _ = finish_rx.recv();
            Ok(())
        }));
        started_rx.await?;

        caller.abort();
        assert!(caller.await.is_err_and(|error| error.is_cancelled()));
        assert_eq!(slots.available_permits(), 0);

        finish_tx.send(())?;
        let _slot = tokio::time::timeout(Duration::from_secs(5), slots.acquire()).await??;
        Ok(())
    }

    #[tokio::test]
    async fn hashed_password_matches_only_itself() -> anyhow::Result<()> {
        let stored = hash("correct-password").await?;
        assert!(matches("correct-password", stored.clone()).await?);
        assert!(!matches("wrong-password", stored).await?);
        Ok(())
    }

    #[tokio::test]
    async fn unusable_hash_is_a_valid_hash_that_matches_nothing() -> anyhow::Result<()> {
        let stored = unusable().await?;
        PasswordHash::new(&stored).map_err(|error| anyhow::anyhow!("{error}"))?;
        assert!(!matches("", stored).await?);
        match_nothing("anything").await?;
        Ok(())
    }

    #[test]
    fn validate_rejects_short_empty_and_non_ascii_passwords() {
        assert!(matches!(validate(""), Err(InvalidPassword::Empty)));
        assert!(matches!(validate("short"), Err(InvalidPassword::TooShort)));
        assert!(matches!(
            validate("päßwörd-long"),
            Err(InvalidPassword::NotAscii)
        ));
        assert!(validate("long-enough").is_ok());
    }
}
