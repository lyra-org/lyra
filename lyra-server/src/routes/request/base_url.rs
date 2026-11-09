// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! The address a caller used to reach the server.

use std::net::{
    IpAddr,
    SocketAddr,
};

use axum::http::{
    HeaderMap,
    Uri,
    header::HOST,
};

use crate::services::settings::server::normalize_origin;

/// What a connection to the server's own port speaks, set on each of its requests.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum ConnectionScheme {
    #[default]
    Http,
    /// The server terminated TLS itself.
    Https,
}

impl ConnectionScheme {
    fn as_str(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Https => "https",
        }
    }
}

/// The `scheme://authority` a request from `peer` was addressed to, over a connection speaking
/// `connection` unless a forwarded header names another scheme. Forwarded headers count only
/// from a peer on a local network, a trust separate from the configured proxies that decide a
/// client's address, since this value only ever describes the address back to its caller.
pub(crate) fn base_url(
    peer: Option<SocketAddr>,
    connection: ConnectionScheme,
    headers: &HeaderMap,
    uri: &Uri,
) -> Option<String> {
    let forwarded = peer.is_some_and(|peer| is_local_network(peer.ip()));
    let scheme = forwarded
        .then(|| forwarded_value(headers, "x-forwarded-proto"))
        .flatten()
        .filter(|proto| matches!(*proto, "http" | "https"))
        .unwrap_or(connection.as_str());
    let forwarded_host = forwarded
        .then(|| forwarded_value(headers, "x-forwarded-host"))
        .flatten();
    let authority = match headers.get(HOST) {
        Some(host) => host.to_str().ok(),
        None => uri.authority().map(|authority| authority.as_str()),
    };

    forwarded_host
        .and_then(|host| origin(scheme, host))
        .or_else(|| origin(scheme, authority?))
}

fn origin(scheme: &str, authority: &str) -> Option<String> {
    // The URL parser ends a host at a slash, drops tabs, and discards empty userinfo, so each
    // would pass as another host.
    if authority
        .chars()
        .any(|c| matches!(c, '/' | '\\' | '@') || c.is_ascii_whitespace())
    {
        return None;
    }
    normalize_origin(&format!("{scheme}://{authority}")).ok()
}

fn forwarded_value<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    let value = headers.get(name)?.to_str().ok()?;
    value.split(',').next().map(str::trim)
}

fn is_local_network(ip: IpAddr) -> bool {
    match ip.to_canonical() {
        IpAddr::V4(ip) => ip.is_loopback() || ip.is_private() || ip.is_link_local(),
        IpAddr::V6(ip) => ip.is_loopback() || ip.is_unique_local() || ip.is_unicast_link_local(),
    }
}

#[cfg(test)]
mod tests {
    use axum::http::{
        HeaderName,
        HeaderValue,
    };

    use super::*;

    const PROXY: &str = "172.18.0.2";
    const PUBLIC: &str = "203.0.113.10";

    fn peer(ip: &str) -> Option<SocketAddr> {
        Some(SocketAddr::new(ip.parse().expect("valid ip"), 4746))
    }

    fn headers(pairs: &[(&str, &[u8])]) -> HeaderMap {
        pairs
            .iter()
            .map(|(name, value)| {
                (
                    HeaderName::from_bytes(name.as_bytes()).expect("valid name"),
                    HeaderValue::from_bytes(value).expect("valid value"),
                )
            })
            .collect()
    }

    fn resolve(peer: Option<SocketAddr>, pairs: &[(&str, &[u8])]) -> Option<String> {
        resolve_over(ConnectionScheme::Http, peer, pairs)
    }

    fn resolve_over(
        connection: ConnectionScheme,
        peer: Option<SocketAddr>,
        pairs: &[(&str, &[u8])],
    ) -> Option<String> {
        base_url(
            peer,
            connection,
            &headers(pairs),
            &Uri::from_static("/jellyfin"),
        )
    }

    fn some(url: &str) -> Option<String> {
        Some(url.to_string())
    }

    #[test]
    fn host_header_gives_the_authority() {
        assert_eq!(
            resolve(peer(PUBLIC), &[("host", b"lyra.home.local")]),
            some("http://lyra.home.local")
        );
    }

    #[test]
    fn uri_authority_stands_in_for_a_missing_host() {
        assert_eq!(
            base_url(
                peer(PUBLIC),
                ConnectionScheme::Http,
                &HeaderMap::new(),
                &Uri::from_static("https://lyra.example:8443/jellyfin"),
            ),
            some("http://lyra.example:8443")
        );
        assert_eq!(resolve(peer(PUBLIC), &[]), None);
    }

    #[test]
    fn host_header_wins_over_the_uri_authority() {
        assert_eq!(
            base_url(
                peer(PUBLIC),
                ConnectionScheme::Http,
                &headers(&[("host", b"lyra.home.local")]),
                &Uri::from_static("http://other.example/jellyfin"),
            ),
            some("http://lyra.home.local")
        );
    }

    #[test]
    fn non_default_ports_stay_and_default_ports_go() {
        assert_eq!(
            resolve(peer(PUBLIC), &[("host", b"lyra.local:4746")]),
            some("http://lyra.local:4746")
        );
        assert_eq!(
            resolve(peer(PUBLIC), &[("host", b"lyra.local:80")]),
            some("http://lyra.local")
        );
        assert_eq!(
            resolve(
                peer(PROXY),
                &[
                    ("host", b"lyra:4746"),
                    ("x-forwarded-proto", b"https"),
                    ("x-forwarded-host", b"lyra.example:443"),
                ],
            ),
            some("https://lyra.example")
        );
    }

    #[test]
    fn a_connection_over_the_servers_own_tls_is_https() {
        assert_eq!(
            resolve_over(
                ConnectionScheme::Https,
                peer(PUBLIC),
                &[("host", b"lyra.local:4746")]
            ),
            some("https://lyra.local:4746")
        );
        assert_eq!(
            resolve_over(
                ConnectionScheme::Https,
                peer(PUBLIC),
                &[("host", b"lyra.local:443")]
            ),
            some("https://lyra.local")
        );
        assert_eq!(
            resolve_over(
                ConnectionScheme::Https,
                None,
                &[("host", b"lyra.local"), ("x-forwarded-proto", b"http")]
            ),
            some("https://lyra.local")
        );
    }

    #[test]
    fn a_trusted_forwarded_scheme_wins_over_the_connections() {
        assert_eq!(
            resolve_over(
                ConnectionScheme::Https,
                peer(PROXY),
                &[
                    ("host", b"lyra:4746"),
                    ("x-forwarded-proto", b"http"),
                    ("x-forwarded-host", b"lyra.example"),
                ],
            ),
            some("http://lyra.example")
        );
        assert_eq!(
            resolve_over(
                ConnectionScheme::Https,
                peer(PROXY),
                &[
                    ("host", b"lyra:4746"),
                    ("x-forwarded-host", b"lyra.example")
                ],
            ),
            some("https://lyra.example")
        );
        assert_eq!(
            resolve_over(
                ConnectionScheme::Https,
                peer(PUBLIC),
                &[("host", b"lyra.local"), ("x-forwarded-proto", b"http")],
            ),
            some("https://lyra.local")
        );
    }

    #[test]
    fn host_is_lowercased() {
        assert_eq!(
            resolve(peer(PUBLIC), &[("host", b"LYRA.Local")]),
            some("http://lyra.local")
        );
    }

    #[test]
    fn forwarded_headers_count_from_each_local_range() {
        for ip in [
            "127.0.0.1",
            "127.255.0.9",
            "10.0.0.1",
            "172.16.0.1",
            "172.31.255.1",
            "192.168.1.1",
            "169.254.0.1",
            "::1",
            "fc00::1",
            "fd12:3456::1",
            "fe80::1",
            "febf::1",
        ] {
            assert_eq!(
                resolve(
                    peer(ip),
                    &[
                        ("host", b"d42dcb3ce5d3:4746"),
                        ("x-forwarded-proto", b"https"),
                        ("x-forwarded-host", b"lyra.example"),
                    ],
                ),
                some("https://lyra.example"),
                "{ip}"
            );
        }
    }

    #[test]
    fn forwarded_headers_are_ignored_from_a_public_peer() {
        for ip in [
            PUBLIC,
            "172.32.0.1",
            "172.15.0.1",
            "192.169.0.1",
            "11.0.0.1",
            "2001:db8::1",
            "fec0::1",
        ] {
            assert_eq!(
                resolve(
                    peer(ip),
                    &[
                        ("host", b"lyra.local:4746"),
                        ("x-forwarded-proto", b"https"),
                        ("x-forwarded-host", b"evil.example"),
                    ],
                ),
                some("http://lyra.local:4746"),
                "{ip}"
            );
        }
    }

    #[test]
    fn forwarded_headers_are_ignored_from_an_unknown_peer() {
        assert_eq!(
            resolve(
                None,
                &[
                    ("host", b"lyra.local:4746"),
                    ("x-forwarded-proto", b"https"),
                    ("x-forwarded-host", b"evil.example"),
                ],
            ),
            some("http://lyra.local:4746")
        );
    }

    #[test]
    fn forwarded_port_is_not_read() {
        assert_eq!(
            resolve(
                peer(PROXY),
                &[
                    ("host", b"lyra:4746"),
                    ("x-forwarded-host", b"lyra.example"),
                    ("x-forwarded-port", b"8443"),
                ],
            ),
            some("http://lyra.example")
        );
    }

    #[test]
    fn comma_separated_forwarded_values_use_the_first() {
        assert_eq!(
            resolve(
                peer(PROXY),
                &[
                    ("host", b"lyra:4746"),
                    ("x-forwarded-proto", b" https , http"),
                    ("x-forwarded-host", b" lyra.example:8443 , proxy.internal"),
                ],
            ),
            some("https://lyra.example:8443")
        );
    }

    #[test]
    fn invalid_proto_is_ignored_alone() {
        for proto in [&b"ftp"[..], b"", b"https://", b"\xff"] {
            assert_eq!(
                resolve(
                    peer(PROXY),
                    &[
                        ("host", b"lyra:4746"),
                        ("x-forwarded-proto", proto),
                        ("x-forwarded-host", b"lyra.example"),
                    ],
                ),
                some("http://lyra.example"),
                "{proto:?}"
            );
        }
    }

    #[test]
    fn invalid_forwarded_host_is_ignored_alone() {
        for host in [
            &b"not a host"[..],
            b"",
            b"lyra.example/path",
            b"lyra.example/",
            b"lyra.example\\",
            b"user@lyra.example",
            b"@lyra.example",
            b"lyra.example:notaport",
            b"\xff",
        ] {
            assert_eq!(
                resolve(
                    peer(PROXY),
                    &[
                        ("host", b"lyra:4746"),
                        ("x-forwarded-proto", b"https"),
                        ("x-forwarded-host", host),
                    ],
                ),
                some("https://lyra:4746"),
                "{host:?}"
            );
        }
    }

    #[test]
    fn malformed_host_gives_nothing() {
        for host in [
            &b"not a host"[..],
            b"",
            b"lyra.local/path",
            b"lyra.local/",
            b"lyra.local\\",
            b"lyr\ta.local",
            b"user@lyra.local",
            b"@lyra.local",
            b"lyra.local?x=1",
            b"lyra.local#top",
            b"lyra.local:notaport",
            b"\xff",
        ] {
            assert_eq!(resolve(peer(PUBLIC), &[("host", host)]), None, "{host:?}");
        }
    }

    #[test]
    fn malformed_host_is_not_replaced_by_the_uri_authority() {
        assert_eq!(
            base_url(
                peer(PUBLIC),
                ConnectionScheme::Http,
                &headers(&[("host", b"not a host")]),
                &Uri::from_static("http://lyra.example/jellyfin"),
            ),
            None
        );
    }

    #[test]
    fn a_usable_forwarded_host_replaces_a_malformed_host() {
        assert_eq!(
            resolve(
                peer(PROXY),
                &[
                    ("host", b"not a host"),
                    ("x-forwarded-host", b"lyra.example")
                ],
            ),
            some("http://lyra.example")
        );
    }

    #[test]
    fn ipv4_mapped_ipv6_peers_read_as_ipv4() {
        let pairs: &[(&str, &[u8])] = &[
            ("host", b"lyra.local:4746"),
            ("x-forwarded-host", b"lyra.example"),
        ];
        assert_eq!(
            resolve(peer("::ffff:192.168.1.5"), pairs),
            some("http://lyra.example")
        );
        assert_eq!(
            resolve(peer("::ffff:203.0.113.10"), pairs),
            some("http://lyra.local:4746")
        );
    }

    #[test]
    fn ipv6_literal_hosts_keep_their_brackets() {
        assert_eq!(
            resolve(peer(PUBLIC), &[("host", b"[2001:db8::1]:4746")]),
            some("http://[2001:db8::1]:4746")
        );
        assert_eq!(
            resolve(peer(PUBLIC), &[("host", b"[::1]:80")]),
            some("http://[::1]")
        );
        assert_eq!(
            resolve(
                peer(PROXY),
                &[
                    ("host", b"lyra:4746"),
                    ("x-forwarded-proto", b"https"),
                    ("x-forwarded-host", b"[2001:db8::1]:443"),
                ],
            ),
            some("https://[2001:db8::1]")
        );
    }
}
