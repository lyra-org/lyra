// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! How long and how many connections the server holds.

use std::time::Duration;

/// How long a request's head may take to arrive in full, and an idle keep-alive connection may
/// wait for its next one.
const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(30);

/// How long a TLS handshake may take, start to finish.
const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// How often the TLS certificate files are read for a replacement.
const CERTIFICATE_POLL_INTERVAL: Duration = Duration::from_secs(30);

/// How long a request body may go without a frame while the handler is reading it.
const BODY_STALL_TIMEOUT: Duration = Duration::from_secs(30);

/// How long a socket may refuse every write. Long enough that a client pausing its reads, as a
/// player does with a full buffer, usually survives it.
const WRITE_STALL_TIMEOUT: Duration = Duration::from_secs(5 * 60);

const SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

/// Generous enough for a household or NAT behind one address, with each browser holding a few.
const MAX_CONNECTIONS_PER_PEER: usize = 64;

const MAX_CONNECTIONS: usize = 4096;

/// The limits one server applies to its connections.
#[derive(Clone, Copy, Debug)]
pub(in crate::routes) struct ConnectionLimits {
    pub(super) tls_handshake: Duration,
    pub(super) certificate_poll: Duration,
    pub(super) header_read: Duration,
    pub(super) body_stall: Duration,
    pub(super) write_stall: Duration,
    pub(super) shutdown_drain: Duration,
    pub(super) per_peer: usize,
    pub(super) global: usize,
}

impl ConnectionLimits {
    /// The limits to serve with, with room left in this process's descriptors.
    pub(in crate::routes) fn for_process() -> Self {
        let global = global_cap(soft_descriptor_limit());
        tracing::debug!(global, "connection limit");
        Self {
            tls_handshake: TLS_HANDSHAKE_TIMEOUT,
            certificate_poll: CERTIFICATE_POLL_INTERVAL,
            header_read: HEADER_READ_TIMEOUT,
            body_stall: BODY_STALL_TIMEOUT,
            write_stall: WRITE_STALL_TIMEOUT,
            shutdown_drain: SHUTDOWN_DRAIN_TIMEOUT,
            per_peer: MAX_CONNECTIONS_PER_PEER,
            global,
        }
    }
}

/// At most half the soft descriptor limit, leaving the rest to media files, the database and
/// transcoders, which would otherwise fail once connections took them all.
fn global_cap(soft_descriptors: Option<u64>) -> usize {
    let Some(soft_descriptors) = soft_descriptors else {
        return MAX_CONNECTIONS;
    };
    usize::try_from(soft_descriptors / 2)
        .map_or(MAX_CONNECTIONS, |half| half.min(MAX_CONNECTIONS))
        .max(1)
}

#[cfg(unix)]
fn soft_descriptor_limit() -> Option<u64> {
    rustix::process::getrlimit(rustix::process::Resource::Nofile).current
}

#[cfg(not(unix))]
fn soft_descriptor_limit() -> Option<u64> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_global_cap_leaves_half_the_descriptors_free() {
        assert_eq!(global_cap(None), MAX_CONNECTIONS);
        assert_eq!(global_cap(Some(1024)), 512);
        assert_eq!(global_cap(Some(1_048_576)), MAX_CONNECTIONS);
        assert_eq!(global_cap(Some(1)), 1);
    }
}
