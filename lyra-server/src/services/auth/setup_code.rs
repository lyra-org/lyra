// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! The code that proves whoever creates the first admin can read the server's log. It is drawn
//! at startup and kept in memory only, so a server that loses its admins while running stays
//! closed until it restarts.

use std::sync::Mutex;

use super::{
    AuthError,
    codes,
};
use crate::services::STATE;

const CODE_LENGTH: usize = 12;
const GROUP_LENGTH: usize = 4;

/// The code first-admin creation takes, while there is one.
#[derive(Default)]
pub(crate) struct SetupCode(Mutex<Option<String>>);

impl SetupCode {
    fn lock(&self) -> std::sync::MutexGuard<'_, Option<String>> {
        self.0.lock().expect("setup code poisoned")
    }
}

/// Whether creating the first admin takes the code.
pub(crate) fn required() -> bool {
    STATE.boot.get().require_setup_code
}

/// Draws the code for a server with no admin and logs it, once per start.
pub(crate) async fn issue() -> anyhow::Result<()> {
    if lyra_db::roles::has_non_default_admin(&*STATE.db.read().await)? {
        return Ok(());
    }
    if !required() {
        tracing::warn!(
            "no admin account exists and LYRA_REQUIRE_SETUP_CODE is false: anyone who can reach \
             this server can create the first admin account"
        );
        return Ok(());
    }
    let code = codes::generate(CODE_LENGTH);
    let grouped = grouped(&code);
    *STATE.generation().auth_caches.setup_code.lock() = Some(code);
    tracing::info!(
        "no admin account exists: creating the first one takes the setup code {grouped}"
    );
    Ok(())
}

/// Checks the code a caller gave for creating the first admin.
pub(super) fn check(given: Option<&str>) -> Result<(), AuthError> {
    if !required() {
        return Ok(());
    }
    let refused = |message: &str| Err(AuthError::Forbidden(message.to_string()));
    let generation = STATE.generation();
    let expected = generation.auth_caches.setup_code.lock();
    let Some(expected) = expected.as_deref() else {
        return refused("setup is closed: this server has no setup code");
    };
    let Some(given) = given else {
        return refused("setup code required");
    };
    if !matches(expected, given) {
        return refused("incorrect setup code");
    }
    Ok(())
}

/// Ends the code once an admin exists.
pub(super) fn retire() {
    STATE.generation().auth_caches.setup_code.lock().take();
}

/// Compares in constant time; `blake3::Hash` equality is.
fn matches(expected: &str, given: &str) -> bool {
    let given = codes::normalize(given).replace('-', "");
    blake3::hash(expected.as_bytes()) == blake3::hash(given.as_bytes())
}

fn grouped(code: &str) -> String {
    code.as_bytes()
        .chunks(GROUP_LENGTH)
        .map(|group| String::from_utf8_lossy(group))
        .collect::<Vec<_>>()
        .join("-")
}

#[cfg(test)]
pub(crate) fn current() -> Option<String> {
    STATE.generation().auth_caches.setup_code.lock().clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_are_shown_in_groups_of_four() {
        assert_eq!(grouped("ABCDEFGHJKMN"), "ABCD-EFGH-JKMN");
    }

    #[test]
    fn typed_codes_match_in_any_case_with_or_without_dashes() {
        for given in [
            "ABCDEFGHJKMN",
            " abcd-efgh-jkmn\n",
            "ABCD EFGH JKMN",
            "abcdefgh-jkmn",
        ] {
            assert!(matches("ABCDEFGHJKMN", given), "{given}");
        }
        for given in ["", "ABCDEFGHJKM", "ABCDEFGHJKMNP", "ABCD-EFGH-JKMP"] {
            assert!(!matches("ABCDEFGHJKMN", given), "{given}");
        }
    }
}
