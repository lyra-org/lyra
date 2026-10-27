// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! The address a request comes from, read from the peer or, behind a trusted proxy, from the
//! forwarded client headers.

use std::{
    collections::HashSet,
    fmt,
    net::{
        IpAddr,
        Ipv6Addr,
        SocketAddr,
    },
    sync::atomic::{
        AtomicBool,
        Ordering,
    },
};

use axum::http::HeaderMap;

static MISSING_PEER_WARNED: AtomicBool = AtomicBool::new(false);
static UNTRUSTED_FORWARD_HEADERS_WARNED: AtomicBool = AtomicBool::new(false);

/// A client as the limiters count it: its IPv4 address, or its IPv6 /64, the block one subscriber
/// usually holds, so rotating addresses within it buys nothing. Clients whose address is unknown
/// all share one.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub(crate) struct ClientAddress(Option<IpAddr>);

impl ClientAddress {
    pub(crate) fn new(ip: IpAddr) -> Self {
        let grouped = match ip {
            IpAddr::V4(_) => ip,
            IpAddr::V6(address) => match address.to_ipv4_mapped() {
                Some(v4) => IpAddr::V4(v4),
                None => {
                    let [a, b, c, d, ..] = address.segments();
                    IpAddr::V6(Ipv6Addr::new(a, b, c, d, 0, 0, 0, 0))
                }
            },
        };
        Self(Some(grouped))
    }

    /// The client of a request from `peer`. Forwarded client headers are read only when the peer
    /// is one of `trusted_proxies`.
    pub(crate) fn resolve(
        peer: Option<SocketAddr>,
        headers: &HeaderMap,
        trusted_proxies: &HashSet<IpAddr>,
    ) -> Self {
        let Some(peer) = peer else {
            if !MISSING_PEER_WARNED.swap(true, Ordering::Relaxed) {
                tracing::warn!("cannot identify a client's address; counting it with all unknown");
            }
            return Self::default();
        };

        let peer_ip = peer.ip();
        if trusted_proxies.contains(&peer_ip) {
            return Self::new(forwarded_client_ip(headers, trusted_proxies).unwrap_or(peer_ip));
        }

        if has_forwarded_client_headers(headers)
            && !UNTRUSTED_FORWARD_HEADERS_WARNED.swap(true, Ordering::Relaxed)
        {
            tracing::warn!(
                peer_ip = %peer_ip,
                "ignored forwarded client headers from an untrusted peer"
            );
        }
        Self::new(peer_ip)
    }
}

impl fmt::Display for ClientAddress {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            Some(ip @ IpAddr::V4(_)) => write!(formatter, "{ip}"),
            Some(ip @ IpAddr::V6(_)) => write!(formatter, "{ip}/64"),
            None => formatter.write_str("unknown"),
        }
    }
}

fn forwarded_client_ip(headers: &HeaderMap, trusted_proxies: &HashSet<IpAddr>) -> Option<IpAddr> {
    x_forwarded_for_client_ip(headers, trusted_proxies)
        .or_else(|| forwarded_header_client_ip(headers, trusted_proxies))
        .or_else(|| x_real_ip(headers))
}

fn x_forwarded_for_client_ip(
    headers: &HeaderMap,
    trusted_proxies: &HashSet<IpAddr>,
) -> Option<IpAddr> {
    let raw = headers.get("x-forwarded-for")?.to_str().ok()?;
    let ips = raw
        .split(',')
        .filter_map(parse_forwarded_ip)
        .collect::<Vec<_>>();
    select_forwarded_client_ip(&ips, trusted_proxies)
}

fn forwarded_header_client_ip(
    headers: &HeaderMap,
    trusted_proxies: &HashSet<IpAddr>,
) -> Option<IpAddr> {
    let raw = headers.get("forwarded")?.to_str().ok()?;
    let ips = raw
        .split(',')
        .filter_map(|entry| {
            entry.split(';').find_map(|part| {
                let (key, value) = part.split_once('=')?;
                key.trim()
                    .eq_ignore_ascii_case("for")
                    .then(|| parse_forwarded_ip(value))
                    .flatten()
            })
        })
        .collect::<Vec<_>>();
    select_forwarded_client_ip(&ips, trusted_proxies)
}

fn x_real_ip(headers: &HeaderMap) -> Option<IpAddr> {
    headers
        .get("x-real-ip")
        .and_then(|value| value.to_str().ok())
        .and_then(parse_forwarded_ip)
}

fn select_forwarded_client_ip(ips: &[IpAddr], trusted_proxies: &HashSet<IpAddr>) -> Option<IpAddr> {
    ips.iter()
        .rev()
        .copied()
        .find(|ip| !trusted_proxies.contains(ip))
        .or_else(|| ips.first().copied())
}

fn parse_forwarded_ip(raw: &str) -> Option<IpAddr> {
    let value = raw.trim().trim_matches('"');
    if value.is_empty() || value.eq_ignore_ascii_case("unknown") {
        return None;
    }

    if let Some(rest) = value.strip_prefix('[') {
        let (host, _) = rest.split_once(']')?;
        return host.parse().ok();
    }

    if let Ok(ip) = value.parse() {
        return Some(ip);
    }

    if let Some((host, _port)) = value.rsplit_once(':')
        && !host.contains(':')
    {
        return host.parse().ok();
    }

    None
}

fn has_forwarded_client_headers(headers: &HeaderMap) -> bool {
    headers.contains_key("x-forwarded-for")
        || headers.contains_key("forwarded")
        || headers.contains_key("x-real-ip")
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use axum::http::HeaderValue;

    use super::*;

    fn address(raw: &str) -> ClientAddress {
        ClientAddress::new(raw.parse().expect("valid ip"))
    }

    #[test]
    fn ipv6_clients_are_grouped_by_their_64() {
        assert_eq!(
            address("2001:db8:1:2:aaaa::1"),
            address("2001:db8:1:2:bbbb::2")
        );
        assert_ne!(address("2001:db8:1:2::1"), address("2001:db8:1:3::1"));
        assert_ne!(address("203.0.113.7"), address("203.0.113.8"));
        assert_eq!(address("::ffff:203.0.113.7"), address("203.0.113.7"));
        assert_eq!(address("2001:db8:1:2::9").to_string(), "2001:db8:1:2::/64");
        assert_eq!(address("203.0.113.7").to_string(), "203.0.113.7");
    }

    #[test]
    fn forwarded_headers_count_only_from_trusted_proxies() {
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-for", HeaderValue::from_static("203.0.113.10"));
        let trusted = HashSet::from([IpAddr::V4(Ipv4Addr::LOCALHOST)]);
        let proxy = Some(SocketAddr::from(([127, 0, 0, 1], 4746)));
        let stranger = Some(SocketAddr::from(([198, 51, 100, 1], 4746)));

        assert_eq!(
            ClientAddress::resolve(proxy, &headers, &trusted),
            address("203.0.113.10")
        );
        assert_eq!(
            ClientAddress::resolve(stranger, &headers, &trusted),
            address("198.51.100.1")
        );
        assert_eq!(
            ClientAddress::resolve(None, &headers, &trusted),
            ClientAddress::default()
        );
    }

    #[test]
    fn forwarded_ipv6_clients_are_grouped_by_their_64() {
        let trusted = HashSet::from([IpAddr::V4(Ipv4Addr::LOCALHOST)]);
        let proxy = Some(SocketAddr::from(([127, 0, 0, 1], 4746)));
        let forwarded = |raw: &'static str| {
            let mut headers = HeaderMap::new();
            headers.insert("x-forwarded-for", HeaderValue::from_static(raw));
            ClientAddress::resolve(proxy, &headers, &trusted)
        };

        assert_eq!(forwarded("2001:db8::1"), forwarded("2001:db8::ffff:2"));
    }

    #[test]
    fn forwarded_header_reads_for_value() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "forwarded",
            HeaderValue::from_static("for=203.0.113.10;proto=https"),
        );

        assert_eq!(
            forwarded_header_client_ip(&headers, &HashSet::new()),
            Some(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 10)))
        );
    }

    #[test]
    fn forwarded_chain_uses_first_untrusted_from_the_right() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-forwarded-for",
            HeaderValue::from_static("198.51.100.99, 203.0.113.10, 127.0.0.1"),
        );
        let trusted = HashSet::from([IpAddr::V4(Ipv4Addr::LOCALHOST)]);

        assert_eq!(
            x_forwarded_for_client_ip(&headers, &trusted),
            Some(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 10)))
        );
    }

    #[test]
    fn x_real_ip_reads_single_client_ip() {
        let mut headers = HeaderMap::new();
        headers.insert("x-real-ip", HeaderValue::from_static("203.0.113.10"));

        assert_eq!(
            x_real_ip(&headers),
            Some(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 10)))
        );
    }
}
