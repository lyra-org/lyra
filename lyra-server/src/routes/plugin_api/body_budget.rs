// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! A public route authenticates in its own handler, after its body has been buffered, so what
//! anonymous callers can make the server hold in memory is bounded here.

use std::{
    collections::HashSet,
    sync::{
        Arc,
        LazyLock,
        Mutex,
        MutexGuard,
        PoisonError,
    },
};

use crate::plugins::api::MAX_BODY_BYTES;
use crate::services::auth::caller::ClientAddress;

const PUBLIC_BODY_BUDGET_BYTES: usize = 4 * MAX_BODY_BYTES;

static SERVER_BUDGET: LazyLock<PublicBodyBudget> =
    LazyLock::new(|| PublicBodyBudget::new(PUBLIC_BODY_BUDGET_BYTES));

/// The bytes that public routes raising their body limit may reserve between them, at most one
/// reservation per client address.
#[derive(Clone)]
pub(super) struct PublicBodyBudget(Arc<Mutex<Reservations>>);

struct Reservations {
    free_bytes: usize,
    clients: HashSet<ClientAddress>,
}

/// What a request holds of the budget, until it has been handled.
pub(super) struct BodyReservation {
    budget: PublicBodyBudget,
    client: ClientAddress,
    bytes: usize,
}

impl PublicBodyBudget {
    fn new(bytes: usize) -> Self {
        Self(Arc::new(Mutex::new(Reservations {
            free_bytes: bytes,
            clients: HashSet::new(),
        })))
    }

    /// The server's budget, shared across router rebuilds.
    pub(super) fn server() -> Self {
        SERVER_BUDGET.clone()
    }

    /// Reserves `bytes` for `client`, or `None` when the budget can't cover them or `client`
    /// already holds a reservation.
    pub(super) fn reserve(&self, client: ClientAddress, bytes: usize) -> Option<BodyReservation> {
        let mut reservations = self.reservations();
        if reservations.free_bytes < bytes || !reservations.clients.insert(client) {
            return None;
        }
        reservations.free_bytes -= bytes;
        Some(BodyReservation {
            budget: self.clone(),
            client,
            bytes,
        })
    }

    fn reservations(&self) -> MutexGuard<'_, Reservations> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    #[cfg(test)]
    pub(super) fn for_tests(bytes: usize) -> Self {
        Self::new(bytes)
    }

    #[cfg(test)]
    pub(super) fn available(&self) -> usize {
        self.reservations().free_bytes
    }
}

impl Drop for BodyReservation {
    fn drop(&mut self) {
        let mut reservations = self.budget.reservations();
        reservations.free_bytes += self.bytes;
        reservations.clients.remove(&self.client);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client(raw: &str) -> ClientAddress {
        ClientAddress::new(raw.parse().expect("valid ip"))
    }

    #[test]
    fn a_client_holds_one_reservation_at_a_time() {
        let budget = PublicBodyBudget::new(100);
        let first = budget.reserve(client("203.0.113.1"), 10).expect("room");

        assert!(budget.reserve(client("203.0.113.1"), 10).is_none());
        assert_eq!(budget.available(), 90, "the refusal took nothing");
        assert!(budget.reserve(client("203.0.113.2"), 10).is_some());
        drop(first);
    }

    #[test]
    fn a_reservation_needs_the_bytes_to_be_free() {
        let budget = PublicBodyBudget::new(100);
        let _held = budget.reserve(client("203.0.113.1"), 60).expect("room");

        assert!(budget.reserve(client("203.0.113.2"), 41).is_none());
        assert!(
            budget.reserve(client("203.0.113.2"), 40).is_some(),
            "being refused left the client free to reserve"
        );
    }

    #[test]
    fn dropping_a_reservation_releases_its_bytes_and_its_client() {
        let budget = PublicBodyBudget::new(100);
        drop(budget.reserve(client("203.0.113.1"), 100).expect("room"));

        assert_eq!(budget.available(), 100);
        assert!(budget.reserve(client("203.0.113.1"), 100).is_some());
    }
}
