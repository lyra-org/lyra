// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Who may hold a connection.

use std::{
    collections::{
        BTreeMap,
        HashMap,
    },
    net::IpAddr,
    sync::{
        Arc,
        Mutex,
        MutexGuard,
        PoisonError,
        atomic::{
            AtomicBool,
            Ordering,
        },
    },
    time::Instant,
};

use tokio_util::sync::CancellationToken;

use crate::routes::warning_throttle::WarningThrottle;
use crate::services::{
    auth::caller::ClientAddress,
    settings::server::TrustedProxies,
};

/// Counts connections against a cap per peer and a cap on them all. Trusted peers, whose
/// connections carry many clients, count only toward the latter.
///
/// When the global cap is full, a newcomer takes the place of the oldest connection that has yet
/// to deliver a request head, so idle sockets can't lock everyone else out.
pub(super) struct ConnectionGate {
    per_peer: usize,
    global: usize,
    trusted_peers: TrustedProxies,
    held: Mutex<Held>,
}

#[derive(Default)]
struct Held {
    total: usize,
    by_peer: HashMap<ClientAddress, usize>,
    /// Connections yet to deliver a request head, oldest first. Bounded by the global cap.
    unproven: BTreeMap<u64, Unproven>,
    next_id: u64,
    global_warnings: WarningThrottle,
    per_peer_warnings: WarningThrottle,
}

struct Unproven {
    peer: Option<ClientAddress>,
    evicted: CancellationToken,
}

/// A connection's place against the caps, given back when the connection's socket closes.
pub(super) struct ConnectionPermit {
    standing: Arc<ConnectionStanding>,
}

/// What a connection's task and its service know of its place at the gate.
pub(super) struct ConnectionStanding {
    gate: Arc<ConnectionGate>,
    id: u64,
    peer: Option<ClientAddress>,
    evicted: CancellationToken,
    protected: AtomicBool,
}

impl ConnectionGate {
    pub(super) fn new(per_peer: usize, global: usize, trusted_peers: TrustedProxies) -> Arc<Self> {
        Arc::new(Self {
            per_peer,
            global,
            trusted_peers,
            held: Mutex::default(),
        })
    }

    /// A permit for a connection from `peer`, or `None` when it is over a cap and no connection
    /// can make way for it.
    pub(super) fn admit(self: &Arc<Self>, peer: IpAddr) -> Option<ConnectionPermit> {
        let client = (!self.trusted_peers.contains(peer)).then(|| ClientAddress::new(peer));
        let mut held = self.held();
        if let Some(client) = client
            && held.by_peer.get(&client).copied().unwrap_or_default() >= self.per_peer
        {
            let warn = held.per_peer_warnings.allows(Instant::now());
            drop(held);
            if warn {
                tracing::warn!(
                    %client,
                    cap = self.per_peer,
                    "a peer hit the connection cap and was refused; a reverse proxy in front of \
                     the server must be listed in rate_limit.trusted_proxies"
                );
            }
            return None;
        }

        let full = held.total >= self.global;
        if full && !held.evict_oldest_unproven() {
            self.warn_global_cap(held, "refusing a connection");
            return None;
        }
        held.total += 1;
        if let Some(client) = client {
            *held.by_peer.entry(client).or_default() += 1;
        }
        let id = held.next_id;
        held.next_id += 1;
        let evicted = CancellationToken::new();
        held.unproven.insert(
            id,
            Unproven {
                peer: client,
                evicted: evicted.clone(),
            },
        );
        if full {
            self.warn_global_cap(
                held,
                "closing the oldest connection that has sent no request",
            );
        }
        Some(ConnectionPermit {
            standing: Arc::new(ConnectionStanding {
                gate: Arc::clone(self),
                id,
                peer: client,
                evicted,
                protected: AtomicBool::new(false),
            }),
        })
    }

    fn warn_global_cap(&self, mut held: MutexGuard<'_, Held>, outcome: &str) {
        let warn = held.global_warnings.allows(Instant::now());
        drop(held);
        if warn {
            tracing::warn!(
                cap = self.global,
                "the global connection cap is full; {outcome}"
            );
        }
    }

    fn held(&self) -> MutexGuard<'_, Held> {
        self.held.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Held {
    fn evict_oldest_unproven(&mut self) -> bool {
        let Some((_, oldest)) = self.unproven.pop_first() else {
            return false;
        };
        // Its socket closes when its task next runs, so descriptors can briefly pass the cap,
        // within the headroom the cap leaves.
        oldest.evicted.cancel();
        self.release(oldest.peer);
        true
    }

    fn release(&mut self, peer: Option<ClientAddress>) {
        self.total -= 1;
        if let Some(peer) = peer
            && let Some(count) = self.by_peer.get_mut(&peer)
        {
            *count -= 1;
            if *count == 0 {
                self.by_peer.remove(&peer);
            }
        }
    }
}

impl ConnectionPermit {
    pub(super) fn standing(&self) -> Arc<ConnectionStanding> {
        Arc::clone(&self.standing)
    }
}

impl ConnectionStanding {
    /// Records that the connection delivered a request head, which keeps it from being evicted,
    /// or returns `false` when eviction got there first and the request must not be served.
    pub(super) fn protect(&self) -> bool {
        if self.protected.load(Ordering::Relaxed) {
            return true;
        }
        if self.gate.held().unproven.remove(&self.id).is_none() {
            return false;
        }
        self.protected.store(true, Ordering::Relaxed);
        true
    }

    /// Resolves once the connection has been evicted and its task should end.
    pub(super) async fn evicted(&self) {
        self.evicted.cancelled().await;
    }
}

impl Drop for ConnectionPermit {
    fn drop(&mut self) {
        let standing = &self.standing;
        let mut held = standing.gate.held();
        // Eviction already released this connection's place.
        if standing.evicted.is_cancelled() {
            return;
        }
        held.unproven.remove(&standing.id);
        held.release(standing.peer);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(raw: &str) -> IpAddr {
        raw.parse().expect("valid ip")
    }

    fn trusting(entries: &[&str]) -> TrustedProxies {
        TrustedProxies::parse(entries.iter().copied()).expect("valid entries")
    }

    fn admitted(gate: &Arc<ConnectionGate>, peer: &str) -> ConnectionPermit {
        gate.admit(ip(peer)).expect("admitted")
    }

    fn proven(gate: &Arc<ConnectionGate>, peer: &str) -> ConnectionPermit {
        let permit = admitted(gate, peer);
        assert!(permit.standing().protect());
        permit
    }

    fn is_evicted(permit: &ConnectionPermit) -> bool {
        permit.standing.evicted.is_cancelled()
    }

    fn assert_empty(gate: &Arc<ConnectionGate>) {
        let held = gate.held();
        assert_eq!(held.total, 0);
        assert!(held.by_peer.is_empty());
        assert!(held.unproven.is_empty());
    }

    #[test]
    fn a_peer_gets_its_place_back_when_a_permit_drops() {
        let gate = ConnectionGate::new(2, 8, TrustedProxies::none());
        let first = admitted(&gate, "203.0.113.1");
        let _second = admitted(&gate, "203.0.113.1");
        assert!(gate.admit(ip("203.0.113.1")).is_none());
        assert!(gate.admit(ip("203.0.113.2")).is_some());

        drop(first);
        assert!(gate.admit(ip("203.0.113.1")).is_some());
    }

    #[test]
    fn counted_peers_leave_nothing_behind() {
        let gate = ConnectionGate::new(2, 8, TrustedProxies::none());
        drop(gate.admit(ip("203.0.113.1")));
        drop(proven(&gate, "203.0.113.1"));
        assert_empty(&gate);
    }

    #[test]
    fn peers_are_grouped_as_the_limiters_count_them() {
        let gate = ConnectionGate::new(1, 8, TrustedProxies::none());
        let _v6 = admitted(&gate, "2001:db8:1:2::1");
        assert!(gate.admit(ip("2001:db8:1:2:ffff::2")).is_none());
        assert!(gate.admit(ip("2001:db8:1:3::1")).is_some());

        let _v4 = admitted(&gate, "203.0.113.1");
        assert!(gate.admit(ip("::ffff:203.0.113.1")).is_none());
    }

    #[test]
    fn trusted_peers_skip_the_per_peer_cap_but_not_the_global_one() {
        let gate = ConnectionGate::new(1, 3, trusting(&["127.0.0.1"]));
        let _permits = [
            proven(&gate, "127.0.0.1"),
            proven(&gate, "::ffff:127.0.0.1"),
            proven(&gate, "127.0.0.1"),
        ];
        assert!(gate.admit(ip("127.0.0.1")).is_none());
    }

    #[test]
    fn a_trusted_peer_may_be_configured_in_its_mapped_form() {
        let gate = ConnectionGate::new(1, 8, trusting(&["::ffff:127.0.0.1"]));
        let _first = admitted(&gate, "127.0.0.1");
        assert!(gate.admit(ip("127.0.0.1")).is_some());
    }

    #[test]
    fn a_range_of_trusted_peers_skips_the_per_peer_cap() {
        let gate = ConnectionGate::new(1, 8, trusting(&["172.30.5.0/24"]));
        let _first = admitted(&gate, "172.30.5.2");
        assert!(gate.admit(ip("172.30.5.2")).is_some());
        assert!(gate.admit(ip("::ffff:172.30.5.2")).is_some());

        let _outside = admitted(&gate, "172.30.6.2");
        assert!(gate.admit(ip("172.30.6.2")).is_none());
    }

    #[test]
    fn the_gate_and_client_address_agree_on_mapped_peers() {
        use std::net::SocketAddr;

        use axum::http::{
            HeaderMap,
            HeaderValue,
        };

        use crate::{
            routes::request::client_address,
            services::settings::server::ClientIpHeader,
        };

        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-for", HeaderValue::from_static("203.0.113.10"));
        let check = |configured: &str, peer: &str, trusted: bool| {
            let proxies = trusting(&[configured]);
            let gate = ConnectionGate::new(1, 8, proxies.clone());
            let _first = admitted(&gate, peer);
            assert_eq!(
                gate.admit(ip(peer)).is_some(),
                trusted,
                "{configured} {peer}"
            );
            let expected = if trusted {
                ip("203.0.113.10")
            } else {
                ip(peer)
            };
            assert_eq!(
                client_address(
                    Some(SocketAddr::new(ip(peer), 4746)),
                    &headers,
                    &proxies,
                    ClientIpHeader::XForwardedFor,
                ),
                ClientAddress::new(expected),
                "{configured} {peer}"
            );
        };

        check("127.0.0.1", "127.0.0.1", true);
        check("127.0.0.1", "::ffff:127.0.0.1", true);
        check("::ffff:127.0.0.1", "127.0.0.1", true);
        check("::ffff:127.0.0.0/104", "::ffff:127.0.0.1", true);
        check("127.0.0.1", "::ffff:127.0.0.2", false);
    }

    #[test]
    fn a_full_cap_evicts_the_oldest_unproven_connection() {
        let gate = ConnectionGate::new(8, 3, TrustedProxies::none());
        let oldest = admitted(&gate, "203.0.113.1");
        let protected = proven(&gate, "203.0.113.2");
        let next = admitted(&gate, "203.0.113.3");

        let newcomer = admitted(&gate, "203.0.113.4");

        assert!(is_evicted(&oldest));
        assert!(!is_evicted(&protected));
        assert!(!is_evicted(&next));
        assert_eq!(gate.held().total, 3);
        assert!(
            !gate
                .held()
                .by_peer
                .contains_key(&ClientAddress::new(ip("203.0.113.1")))
        );

        drop(oldest);
        assert_eq!(gate.held().total, 3, "its place went to the newcomer");
        drop((protected, next, newcomer));
        assert_empty(&gate);
    }

    #[test]
    fn protected_connections_are_never_evicted() {
        let gate = ConnectionGate::new(8, 2, TrustedProxies::none());
        let first = proven(&gate, "203.0.113.1");
        let second = proven(&gate, "203.0.113.2");

        assert!(gate.admit(ip("203.0.113.3")).is_none());

        assert!(!is_evicted(&first));
        assert!(!is_evicted(&second));
        assert_eq!(gate.held().total, 2);
    }

    #[test]
    fn a_peer_at_its_cap_evicts_nothing() {
        let gate = ConnectionGate::new(1, 2, TrustedProxies::none());
        let same = admitted(&gate, "203.0.113.1");
        let other = admitted(&gate, "203.0.113.2");

        assert!(gate.admit(ip("203.0.113.1")).is_none());

        assert!(!is_evicted(&same));
        assert!(!is_evicted(&other));
        assert_eq!(gate.held().total, 2);
    }

    #[test]
    fn a_request_arriving_after_eviction_changes_nothing() {
        let gate = ConnectionGate::new(8, 1, TrustedProxies::none());
        let evicted = admitted(&gate, "203.0.113.1");
        let newcomer = admitted(&gate, "203.0.113.2");

        assert!(!evicted.standing().protect());
        assert!(is_evicted(&evicted));
        assert_eq!(
            gate.held().unproven.len(),
            1,
            "the newcomer stays evictable"
        );

        drop(evicted);
        assert_eq!(gate.held().total, 1);
        drop(newcomer);
        assert_empty(&gate);
    }

    #[test]
    fn protection_stays_settled_for_later_requests() {
        let gate = ConnectionGate::new(8, 1, TrustedProxies::none());
        let permit = admitted(&gate, "203.0.113.1");
        assert!(permit.standing().protect());
        assert!(permit.standing().protect());
        assert!(gate.admit(ip("203.0.113.2")).is_none());
    }

    #[test]
    fn the_total_never_exceeds_the_cap_under_contention() {
        const CAP: usize = 4;
        let gate = ConnectionGate::new(16, CAP, TrustedProxies::none());
        std::thread::scope(|scope| {
            for thread in 0..8 {
                let gate = &gate;
                scope.spawn(move || {
                    let peer = format!("203.0.113.{thread}");
                    let mut permits = std::collections::VecDeque::new();
                    for round in 0..2000 {
                        if let Some(permit) = gate.admit(ip(&peer)) {
                            if round % 5 == 0 {
                                let _ = permit.standing().protect();
                            }
                            permits.push_back(permit);
                        }
                        if permits.len() > 2 {
                            permits.pop_front();
                        }
                        assert!(gate.held().total <= CAP);
                    }
                });
            }
        });
        assert_empty(&gate);
    }
}
