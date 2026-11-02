// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Which player sessions a plugin reports playback for each open plugin socket claims to stand
//! for. Once the last socket claiming a session closes, the session's playback pauses, as a closed
//! connection of Lyra's own clients pauses its playback. Each session counts the reports accepted
//! for it while claimed, so a close leaves alone what a report accepted after it changed.

use std::{
    collections::{
        HashMap,
        HashSet,
    },
    sync::{
        Arc,
        Mutex,
        atomic::{
            AtomicU64,
            Ordering,
        },
    },
};

/// Tells plugin sockets apart for as long as the server runs.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct SocketId(u64);

impl SocketId {
    pub(crate) fn next() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        Self(NEXT.fetch_add(1, Ordering::Relaxed))
    }
}

/// A player session a plugin reports playback for, as reports name it.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct ClaimedSession {
    pub(crate) plugin_id: String,
    pub(crate) user_public_id: String,
    pub(crate) session_key: String,
}

#[derive(Debug, Default)]
struct Claimants {
    /// Counts the reports accepted for the session while it is claimed or a close of it is pending.
    version: u64,
    sockets: HashSet<SocketId>,
    /// Closes of the session that have yet to settle.
    releasing: usize,
}

#[derive(Debug, Default)]
struct ClaimMap {
    sessions: HashMap<ClaimedSession, Claimants>,
    sockets: HashMap<SocketId, HashSet<ClaimedSession>>,
}

impl ClaimMap {
    /// Counts one close of the session as settled, forgetting the session once nothing claims it.
    fn release(&mut self, session: &ClaimedSession) {
        if let Some(claimants) = self.sessions.get_mut(session) {
            claimants.releasing -= 1;
            if claimants.sockets.is_empty() && claimants.releasing == 0 {
                self.sessions.remove(session);
            }
        }
    }
}

/// Generation-owned socket claims.
#[derive(Default)]
pub(crate) struct SocketClaims(Arc<Mutex<ClaimMap>>);

/// A session a closed socket claimed, and how many reports had been accepted for it when it closed.
/// Dropped without [`ReleasedClaim::settle`], the close pauses nothing.
#[derive(Debug)]
pub(crate) struct ReleasedClaim {
    session: ClaimedSession,
    version: u64,
    claims: Arc<Mutex<ClaimMap>>,
    settled: bool,
}

impl ReleasedClaim {
    /// Settles the close, returning the session when the close leaves it unclaimed and unreported
    /// since: then its playback is to pause. Called under the database write lock reports take,
    /// so no report lands between this and the pause.
    pub(crate) fn settle(mut self) -> Option<ClaimedSession> {
        self.settled = true;
        let mut claims = self.claims.lock().expect("socket claims mutex poisoned");
        let claimants = claims.sessions.get(&self.session)?;
        let pause = claimants.sockets.is_empty() && claimants.version == self.version;
        claims.release(&self.session);
        pause.then(|| self.session.clone())
    }
}

impl Drop for ReleasedClaim {
    fn drop(&mut self) {
        if !self.settled {
            self.claims
                .lock()
                .expect("socket claims mutex poisoned")
                .release(&self.session);
        }
    }
}

fn claims() -> Arc<Mutex<ClaimMap>> {
    crate::services::STATE.generation().socket_claims.0.clone()
}

/// Claims the session for the socket while `open` says the socket is open. A socket marks itself
/// closed as it releases its claims, under the claims' lock that `open` runs under too, so a
/// socket that closed claims nothing. Returns whether the socket claims the session.
pub(crate) fn claim(
    socket: SocketId,
    session: ClaimedSession,
    open: impl FnOnce() -> bool,
) -> bool {
    let handle = claims();
    let mut claims = handle.lock().expect("socket claims mutex poisoned");
    if !open() {
        return false;
    }
    claims
        .sessions
        .entry(session.clone())
        .or_default()
        .sockets
        .insert(socket);
    claims.sockets.entry(socket).or_default().insert(session);
    true
}

/// Counts a report accepted for the session of the user `user_public_id` names, resolving it only
/// while some session is claimed. Called under the database write lock reports take; the check
/// and the count share the claims' lock, so a claim taken meanwhile still sees the report.
pub(crate) fn count_report(
    plugin_id: &str,
    session_key: &str,
    user_public_id: impl FnOnce() -> anyhow::Result<Option<String>>,
) -> anyhow::Result<()> {
    let handle = claims();
    let mut claims = handle.lock().expect("socket claims mutex poisoned");
    if claims.sessions.is_empty() {
        return Ok(());
    }
    let Some(user_public_id) = user_public_id()? else {
        return Ok(());
    };
    let session = ClaimedSession {
        plugin_id: plugin_id.to_string(),
        user_public_id,
        session_key: session_key.to_string(),
    };
    if let Some(claimants) = claims.sessions.get_mut(&session) {
        claimants.version = claimants.version.wrapping_add(1);
    }
    Ok(())
}

/// Closes the socket with `close` and lets go of what it claimed, both under the claims' lock, so a
/// report counted after the close is one the releases didn't see.
pub(crate) fn release_socket(socket: SocketId, close: impl FnOnce()) -> Vec<ReleasedClaim> {
    let handle = claims();
    let mut claims = handle.lock().expect("socket claims mutex poisoned");
    close();
    let Some(sessions) = claims.sockets.remove(&socket) else {
        return Vec::new();
    };
    sessions
        .into_iter()
        .filter_map(|session| {
            let claimants = claims.sessions.get_mut(&session)?;
            claimants.sockets.remove(&socket);
            claimants.releasing += 1;
            Some(ReleasedClaim {
                version: claimants.version,
                session,
                claims: handle.clone(),
                settled: false,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session(key: &str) -> ClaimedSession {
        ClaimedSession {
            plugin_id: "demo".to_string(),
            user_public_id: "user".to_string(),
            session_key: key.to_string(),
        }
    }

    fn report_session(key: &str) {
        count_report("demo", key, || Ok(Some("user".to_string()))).expect("count report");
    }

    fn claimed_count() -> usize {
        claims()
            .lock()
            .expect("socket claims mutex poisoned")
            .sessions
            .len()
    }

    #[test]
    fn the_last_socket_to_close_settles_a_pause() {
        let _guard = futures::executor::block_on(crate::services::testing::runtime_test_lock());
        crate::services::testing::init_default_test_state().expect("init test state");
        let (first, second) = (SocketId::next(), SocketId::next());
        assert!(claim(first, session("device"), || true));
        assert!(claim(second, session("device"), || true));
        report_session("device");

        let released = release_socket(first, || {});
        assert_eq!(released.len(), 1);
        let mut released = released.into_iter();
        assert!(
            released.next().unwrap().settle().is_none(),
            "another socket still claims the session"
        );

        let released = release_socket(second, || {}).pop().unwrap();
        assert_eq!(released.settle(), Some(session("device")));
        assert_eq!(claimed_count(), 0);
        assert!(release_socket(second, || {}).is_empty());
    }

    #[test]
    fn a_report_after_the_close_keeps_the_session_playing() {
        let _guard = futures::executor::block_on(crate::services::testing::runtime_test_lock());
        crate::services::testing::init_default_test_state().expect("init test state");
        let socket = SocketId::next();
        claim(socket, session("device"), || true);
        report_session("device");

        let released = release_socket(socket, || {}).pop().unwrap();
        report_session("device");
        report_session("other");

        assert!(released.settle().is_none());
        assert_eq!(claimed_count(), 0);
    }

    #[test]
    fn a_closed_socket_claims_nothing() {
        let _guard = futures::executor::block_on(crate::services::testing::runtime_test_lock());
        crate::services::testing::init_default_test_state().expect("init test state");
        let socket = SocketId::next();
        assert!(!claim(socket, session("device"), || false));
        assert_eq!(claimed_count(), 0);
        assert!(release_socket(socket, || {}).is_empty());
    }

    #[test]
    fn an_unsettled_close_pauses_nothing_and_lets_go() {
        let _guard = futures::executor::block_on(crate::services::testing::runtime_test_lock());
        crate::services::testing::init_default_test_state().expect("init test state");
        let socket = SocketId::next();
        claim(socket, session("device"), || true);

        drop(release_socket(socket, || {}));

        assert_eq!(claimed_count(), 0);
    }

    #[test]
    fn a_report_waits_out_a_close_in_progress() {
        let _guard = futures::executor::block_on(crate::services::testing::runtime_test_lock());
        crate::services::testing::init_default_test_state().expect("init test state");
        let socket = SocketId::next();
        claim(socket, session("device"), || true);

        let (reported, report_done) = std::sync::mpsc::channel();
        let released = release_socket(socket, || {
            let reported = reported.clone();
            std::thread::spawn(move || {
                report_session("device");
                reported.send(()).expect("report signal");
            });
            assert!(
                report_done
                    .recv_timeout(std::time::Duration::from_millis(100))
                    .is_err(),
                "a report lands only once the close has seen what it claims"
            );
        });
        report_done
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("the report lands after the close");

        assert!(
            released.into_iter().next().unwrap().settle().is_none(),
            "the report came after the close"
        );
    }
}
