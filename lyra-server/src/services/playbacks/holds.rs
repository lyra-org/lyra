// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Which player sessions a plugin reports playback for each open plugin socket stands for. Once
//! the last socket holding a session closes, the session's playback pauses, as a closed connection
//! of Lyra's own clients pauses its playback. Each session counts the reports accepted for it while
//! held, so a close leaves alone what a report accepted after it changed.

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
pub(crate) struct HeldSession {
    pub(crate) plugin_id: String,
    pub(crate) user_public_id: String,
    pub(crate) session_key: String,
}

#[derive(Debug, Default)]
struct Holders {
    /// Counts the reports accepted for the session while it is held or a close of it is pending.
    version: u64,
    sockets: HashSet<SocketId>,
    /// Closes of the session that have yet to settle.
    releasing: usize,
}

#[derive(Debug, Default)]
struct HoldMap {
    sessions: HashMap<HeldSession, Holders>,
    sockets: HashMap<SocketId, HashSet<HeldSession>>,
}

impl HoldMap {
    /// Counts one close of the session as settled, forgetting the session once nothing holds it.
    fn release(&mut self, session: &HeldSession) {
        if let Some(holders) = self.sessions.get_mut(session) {
            holders.releasing -= 1;
            if holders.sockets.is_empty() && holders.releasing == 0 {
                self.sessions.remove(session);
            }
        }
    }
}

/// Generation-owned session holds.
#[derive(Default)]
pub(crate) struct SessionHolds(Arc<Mutex<HoldMap>>);

/// A session a closed socket held, and how many reports had been accepted for it when it closed.
/// Dropped without [`Release::settle`], the close pauses nothing.
#[derive(Debug)]
pub(crate) struct Release {
    session: HeldSession,
    version: u64,
    holds: Arc<Mutex<HoldMap>>,
    settled: bool,
}

impl Release {
    /// Settles the close, returning the session when the close leaves it unheld and unreported
    /// since: then its playback is to pause. Called under the database write lock reports take,
    /// so no report lands between this and the pause.
    pub(crate) fn settle(mut self) -> Option<HeldSession> {
        self.settled = true;
        let mut holds = self.holds.lock().expect("session holds mutex poisoned");
        let holders = holds.sessions.get(&self.session)?;
        let pause = holders.sockets.is_empty() && holders.version == self.version;
        holds.release(&self.session);
        pause.then(|| self.session.clone())
    }
}

impl Drop for Release {
    fn drop(&mut self) {
        if !self.settled {
            self.holds
                .lock()
                .expect("session holds mutex poisoned")
                .release(&self.session);
        }
    }
}

fn holds() -> Arc<Mutex<HoldMap>> {
    crate::services::STATE.generation().session_holds.0.clone()
}

/// Holds the session for the socket while `open` says the socket is open. A socket marks itself
/// closed as it releases its holds, under the holds' lock that `open` runs under too, so a socket
/// that closed holds nothing. Returns whether the socket holds the session.
pub(crate) fn hold(socket: SocketId, session: HeldSession, open: impl FnOnce() -> bool) -> bool {
    let handle = holds();
    let mut holds = handle.lock().expect("session holds mutex poisoned");
    if !open() {
        return false;
    }
    holds
        .sessions
        .entry(session.clone())
        .or_default()
        .sockets
        .insert(socket);
    holds.sockets.entry(socket).or_default().insert(session);
    true
}

/// Counts a report accepted for the session of the user `user_public_id` names, resolving it only
/// while some session is held. Called under the database write lock reports take; the check and
/// the count share the holds' lock, so a hold taken meanwhile still sees the report.
pub(crate) fn advance(
    plugin_id: &str,
    session_key: &str,
    user_public_id: impl FnOnce() -> anyhow::Result<Option<String>>,
) -> anyhow::Result<()> {
    let handle = holds();
    let mut holds = handle.lock().expect("session holds mutex poisoned");
    if holds.sessions.is_empty() {
        return Ok(());
    }
    let Some(user_public_id) = user_public_id()? else {
        return Ok(());
    };
    let session = HeldSession {
        plugin_id: plugin_id.to_string(),
        user_public_id,
        session_key: session_key.to_string(),
    };
    if let Some(holders) = holds.sessions.get_mut(&session) {
        holders.version = holders.version.wrapping_add(1);
    }
    Ok(())
}

/// Closes the socket with `close` and lets go of what it held, both under the holds' lock, so a
/// report counted after the close is one the releases didn't see.
pub(crate) fn release_socket(socket: SocketId, close: impl FnOnce()) -> Vec<Release> {
    let handle = holds();
    let mut holds = handle.lock().expect("session holds mutex poisoned");
    close();
    let Some(sessions) = holds.sockets.remove(&socket) else {
        return Vec::new();
    };
    sessions
        .into_iter()
        .filter_map(|session| {
            let holders = holds.sessions.get_mut(&session)?;
            holders.sockets.remove(&socket);
            holders.releasing += 1;
            Some(Release {
                version: holders.version,
                session,
                holds: handle.clone(),
                settled: false,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session(key: &str) -> HeldSession {
        HeldSession {
            plugin_id: "demo".to_string(),
            user_public_id: "user".to_string(),
            session_key: key.to_string(),
        }
    }

    fn advance_session(key: &str) {
        advance("demo", key, || Ok(Some("user".to_string()))).expect("advance");
    }

    fn held_count() -> usize {
        holds()
            .lock()
            .expect("session holds mutex poisoned")
            .sessions
            .len()
    }

    #[test]
    fn the_last_socket_to_close_settles_a_pause() {
        let _guard = futures::executor::block_on(crate::services::testing::runtime_test_lock());
        crate::services::testing::init_default_test_state().expect("init test state");
        let (first, second) = (SocketId::next(), SocketId::next());
        assert!(hold(first, session("device"), || true));
        assert!(hold(second, session("device"), || true));
        advance_session("device");

        let released = release_socket(first, || {});
        assert_eq!(released.len(), 1);
        let mut released = released.into_iter();
        assert!(
            released.next().unwrap().settle().is_none(),
            "another socket still holds the session"
        );

        let released = release_socket(second, || {}).pop().unwrap();
        assert_eq!(released.settle(), Some(session("device")));
        assert_eq!(held_count(), 0);
        assert!(release_socket(second, || {}).is_empty());
    }

    #[test]
    fn a_report_after_the_close_keeps_the_session_playing() {
        let _guard = futures::executor::block_on(crate::services::testing::runtime_test_lock());
        crate::services::testing::init_default_test_state().expect("init test state");
        let socket = SocketId::next();
        hold(socket, session("device"), || true);
        advance_session("device");

        let released = release_socket(socket, || {}).pop().unwrap();
        advance_session("device");
        advance_session("other");

        assert!(released.settle().is_none());
        assert_eq!(held_count(), 0);
    }

    #[test]
    fn a_closed_socket_holds_nothing() {
        let _guard = futures::executor::block_on(crate::services::testing::runtime_test_lock());
        crate::services::testing::init_default_test_state().expect("init test state");
        let socket = SocketId::next();
        assert!(!hold(socket, session("device"), || false));
        assert_eq!(held_count(), 0);
        assert!(release_socket(socket, || {}).is_empty());
    }

    #[test]
    fn an_unsettled_close_pauses_nothing_and_lets_go() {
        let _guard = futures::executor::block_on(crate::services::testing::runtime_test_lock());
        crate::services::testing::init_default_test_state().expect("init test state");
        let socket = SocketId::next();
        hold(socket, session("device"), || true);

        drop(release_socket(socket, || {}));

        assert_eq!(held_count(), 0);
    }

    #[test]
    fn a_report_waits_out_a_close_in_progress() {
        let _guard = futures::executor::block_on(crate::services::testing::runtime_test_lock());
        crate::services::testing::init_default_test_state().expect("init test state");
        let socket = SocketId::next();
        hold(socket, session("device"), || true);

        let (reported, report_done) = std::sync::mpsc::channel();
        let released = release_socket(socket, || {
            let reported = reported.clone();
            std::thread::spawn(move || {
                advance_session("device");
                reported.send(()).expect("report signal");
            });
            assert!(
                report_done
                    .recv_timeout(std::time::Duration::from_millis(100))
                    .is_err(),
                "a report lands only once the close has seen what it holds"
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
