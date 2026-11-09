// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! The address a request comes from, read from the peer or, behind a trusted proxy, from the
//! configured client header.

use std::{
    net::{
        IpAddr,
        SocketAddr,
    },
    sync::{
        Mutex,
        PoisonError,
        atomic::{
            AtomicBool,
            Ordering,
        },
    },
    time::Instant,
};

use axum::http::HeaderMap;

use crate::routes::warning_throttle::WarningThrottle;
use crate::services::{
    auth::caller::ClientAddress,
    settings::server::{
        ClientIpHeader,
        TrustedProxies,
    },
};

static MISSING_PEER_WARNED: AtomicBool = AtomicBool::new(false);
static UNTRUSTED_PEER_WARNED: AtomicBool = AtomicBool::new(false);
static OTHER_HEADER_WARNINGS: Mutex<WarningThrottle> = Mutex::new(WarningThrottle::new());

/// The client of a request from `peer`. Only `header` is read, and only when the peer is one of
/// `trusted_proxies`; without a usable value there, the client is the peer.
pub(crate) fn client_address(
    peer: Option<SocketAddr>,
    headers: &HeaderMap,
    trusted_proxies: &TrustedProxies,
    header: ClientIpHeader,
) -> ClientAddress {
    let Some(peer) = peer else {
        if !MISSING_PEER_WARNED.swap(true, Ordering::Relaxed) {
            tracing::warn!("cannot identify a client's address; counting it with all unknown");
        }
        return ClientAddress::default();
    };

    let (client, finding) = resolve(peer.ip(), headers, trusted_proxies, header);
    if let Some(finding) = finding {
        warn(&finding, peer.ip(), header);
    }
    ClientAddress::new(client)
}

/// Why a request's forwarding headers were not used, when that points at a misconfiguration.
#[derive(Debug, PartialEq, Eq)]
enum Finding {
    UntrustedPeer,
    OtherHeader(&'static str),
}

fn resolve(
    peer: IpAddr,
    headers: &HeaderMap,
    trusted_proxies: &TrustedProxies,
    header: ClientIpHeader,
) -> (IpAddr, Option<Finding>) {
    let sent_header = |name: &str| headers.contains_key(name);
    if !trusted_proxies.contains(peer) {
        let finding = ClientIpHeader::OPTIONS
            .iter()
            .any(|name| sent_header(name))
            .then_some(Finding::UntrustedPeer);
        return (peer, finding);
    }

    if let Some(client) = reported_client(headers, trusted_proxies, header) {
        return (client, None);
    }
    let finding = ClientIpHeader::OPTIONS
        .iter()
        .copied()
        .filter(|name| *name != header.name())
        .find(|name| sent_header(name))
        .map(Finding::OtherHeader);
    (peer, finding)
}

fn warn(finding: &Finding, peer: IpAddr, header: ClientIpHeader) {
    match finding {
        // Any client can send these headers, so this is said once.
        Finding::UntrustedPeer if !UNTRUSTED_PEER_WARNED.swap(true, Ordering::Relaxed) => {
            tracing::warn!(
                %peer,
                "ignored forwarded client headers from a peer that is not a trusted proxy; a reverse \
                 proxy in front of the server must be listed in rate_limit.trusted_proxies"
            )
        }
        Finding::OtherHeader(sent) if allowed(&OTHER_HEADER_WARNINGS) => tracing::warn!(
            %peer,
            sent,
            configured = header.name(),
            "a trusted proxy sent no usable client address in the configured header, so every \
             client behind it counts as the proxy's address; set rate_limit.client_ip_header to \
             the header the proxy sets"
        ),
        Finding::UntrustedPeer | Finding::OtherHeader(_) => {}
    }
}

fn allowed(throttle: &Mutex<WarningThrottle>) -> bool {
    throttle
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .allows(Instant::now())
}

fn reported_client(
    headers: &HeaderMap,
    trusted_proxies: &TrustedProxies,
    header: ClientIpHeader,
) -> Option<IpAddr> {
    match header {
        ClientIpHeader::XForwardedFor => chain_client(
            headers,
            header.name(),
            trusted_proxies,
            x_forwarded_for_hops,
        ),
        ClientIpHeader::Forwarded => {
            chain_client(headers, header.name(), trusted_proxies, forwarded_hops)
        }
        ClientIpHeader::XRealIp => single_client(headers, header.name()),
    }
}

/// What a list header's hops, read left to right, say about the client.
enum Chain {
    NoHops,
    Client(IpAddr),
    Unreadable,
}

impl Chain {
    /// Each proxy appends to what it received, so only what lies right of the last untrusted hop
    /// was not written by the client: that hop is the client. A hop that can't be read voids all
    /// left of it, which can't be vouched for, and when every hop is trusted the farthest counts.
    fn with_hop(self, hop: Option<IpAddr>, trusted_proxies: &TrustedProxies) -> Self {
        match hop {
            None => Self::Unreadable,
            Some(ip) if !trusted_proxies.contains(ip) => Self::Client(ip),
            Some(ip) => match self {
                Self::NoHops => Self::Client(ip),
                settled => settled,
            },
        }
    }
}

fn chain_client<'h, I>(
    headers: &'h HeaderMap,
    name: &str,
    trusted_proxies: &TrustedProxies,
    hops: fn(&'h str) -> I,
) -> Option<IpAddr>
where
    I: Iterator<Item = Option<IpAddr>>,
{
    let mut chain = Chain::NoHops;
    for line in headers.get_all(name) {
        let Ok(line) = line.to_str() else {
            chain = Chain::Unreadable;
            continue;
        };
        for hop in hops(line) {
            chain = chain.with_hop(hop, trusted_proxies);
        }
    }
    match chain {
        Chain::Client(ip) => Some(ip),
        Chain::NoHops | Chain::Unreadable => None,
    }
}

/// The value of a header that holds one address. A repeated header is ambiguous, so unusable.
fn single_client(headers: &HeaderMap, name: &str) -> Option<IpAddr> {
    let mut lines = headers.get_all(name).iter();
    let line = lines.next()?;
    if lines.next().is_some() {
        return None;
    }
    parse_forwarded_ip(line.to_str().ok()?)
}

fn x_forwarded_for_hops(line: &str) -> impl Iterator<Item = Option<IpAddr>> + '_ {
    line.split(',')
        .filter(|entry| !entry.trim().is_empty())
        .map(parse_forwarded_ip)
}

const OPTIONAL_WHITESPACE: [char; 2] = [' ', '\t'];

/// The `for` address of each element of a `Forwarded` line. A proxy can copy client-controlled
/// text into a quoted parameter, so separators count only outside quotes; a malformed element
/// ends the line with an unreadable hop, leaving a client's stray quote unable to absorb what a
/// proxy appended after it.
fn forwarded_hops(line: &str) -> impl Iterator<Item = Option<IpAddr>> + '_ {
    let mut rest = Some(line);
    std::iter::from_fn(move || {
        let input = rest?.trim_start_matches([' ', '\t', ',']);
        if input.is_empty() {
            rest = None;
            return None;
        }
        match forwarded_element(input) {
            Some((client, after)) => {
                rest = Some(after);
                Some(client)
            }
            None => {
                rest = None;
                Some(None)
            }
        }
    })
}

/// Reads the element at the start of `input`, returning its `for` address and the text after it,
/// or `None` when the element is malformed. A repeated `for` is malformed.
fn forwarded_element(mut input: &str) -> Option<(Option<IpAddr>, &str)> {
    let mut read_for = false;
    let mut client = None;
    loop {
        let (key, value) = input.split_once('=')?;
        if key.is_empty() || key.contains([',', ';', '"', ' ', '\t']) {
            return None;
        }
        let (value, after) = forwarded_value(value)?;
        if key.eq_ignore_ascii_case("for") {
            if read_for {
                return None;
            }
            read_for = true;
            client = parse_forwarded_ip(value);
        }

        let after = after.trim_start_matches(OPTIONAL_WHITESPACE);
        if after.is_empty() {
            return Some((client, after));
        }
        if let Some(next_element) = after.strip_prefix(',') {
            return Some((client, next_element));
        }
        input = after
            .strip_prefix(';')?
            .trim_start_matches(OPTIONAL_WHITESPACE);
    }
}

/// Splits a parameter value, quoted or not, from the text after it.
fn forwarded_value(input: &str) -> Option<(&str, &str)> {
    let Some(quoted) = input.strip_prefix('"') else {
        let end = input
            .find([';', ',', '"', ' ', '\t'])
            .unwrap_or(input.len());
        return (end > 0).then(|| input.split_at(end));
    };
    let mut escaped = false;
    for (index, byte) in quoted.bytes().enumerate() {
        match byte {
            _ if escaped => escaped = false,
            b'\\' => escaped = true,
            b'"' => return Some((&quoted[..index], &quoted[index + 1..])),
            _ => {}
        }
    }
    None
}

/// One address, optionally with a port, and nothing else.
fn parse_forwarded_ip(raw: &str) -> Option<IpAddr> {
    let value = raw.trim();
    if let Some(rest) = value.strip_prefix('[') {
        let (host, suffix) = rest.split_once(']')?;
        return (suffix.is_empty() || suffix.strip_prefix(':').is_some_and(is_port))
            .then(|| host.parse().ok())
            .flatten();
    }
    if let Ok(ip) = value.parse() {
        return Some(ip);
    }
    let (host, port) = value.rsplit_once(':')?;
    if host.contains(':') || !is_port(port) {
        return None;
    }
    host.parse().ok()
}

fn is_port(raw: &str) -> bool {
    raw.bytes().all(|byte| byte.is_ascii_digit()) && raw.parse::<u16>().is_ok()
}

#[cfg(test)]
mod tests {
    use axum::http::{
        HeaderName,
        HeaderValue,
    };

    use super::*;

    const XFF: ClientIpHeader = ClientIpHeader::XForwardedFor;
    const REAL_IP: ClientIpHeader = ClientIpHeader::XRealIp;
    const FORWARDED: ClientIpHeader = ClientIpHeader::Forwarded;

    fn address(raw: &str) -> ClientAddress {
        ClientAddress::new(ip(raw))
    }

    fn ip(raw: &str) -> IpAddr {
        raw.parse().expect("valid ip")
    }

    fn peer(raw: &str) -> Option<SocketAddr> {
        Some(SocketAddr::new(ip(raw), 4746))
    }

    fn proxies(entries: &[&str]) -> TrustedProxies {
        TrustedProxies::parse(entries.iter().copied()).expect("valid entries")
    }

    fn loopback() -> TrustedProxies {
        proxies(&["127.0.0.1"])
    }

    /// Each pair is its own header line, in order.
    fn headers(lines: &[(&'static str, &'static str)]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for (name, value) in lines {
            headers.append(
                HeaderName::from_static(name),
                HeaderValue::from_static(value),
            );
        }
        headers
    }

    fn from_loopback(lines: &[(&'static str, &'static str)], header: ClientIpHeader) -> String {
        client_address(peer("127.0.0.1"), &headers(lines), &loopback(), header).to_string()
    }

    const LOOPBACK_ADDRESS: &str = "127.0.0.1";

    #[test]
    fn headers_count_only_from_trusted_proxies() {
        let lines = headers(&[("x-forwarded-for", "203.0.113.10")]);

        assert_eq!(
            client_address(peer("127.0.0.1"), &lines, &loopback(), XFF),
            address("203.0.113.10")
        );
        assert_eq!(
            client_address(peer("198.51.100.1"), &lines, &loopback(), XFF),
            address("198.51.100.1")
        );
        assert_eq!(
            client_address(None, &lines, &loopback(), XFF),
            ClientAddress::default()
        );
    }

    #[test]
    fn an_untrusted_peer_is_its_own_client_whatever_the_header() {
        let lines = headers(&[
            ("x-forwarded-for", "203.0.113.10"),
            ("x-real-ip", "203.0.113.11"),
            ("forwarded", "for=203.0.113.12"),
        ]);
        for header in [XFF, REAL_IP, FORWARDED] {
            assert_eq!(
                client_address(peer("198.51.100.1"), &lines, &loopback(), header),
                address("198.51.100.1")
            );
        }
    }

    #[test]
    fn a_range_of_proxies_is_trusted() {
        let trusted = proxies(&["172.30.5.0/24"]);
        let lines = headers(&[("x-forwarded-for", "203.0.113.10, 172.30.5.9")]);

        assert_eq!(
            client_address(peer("172.30.5.2"), &lines, &trusted, XFF),
            address("203.0.113.10")
        );
        assert_eq!(
            client_address(peer("172.30.6.2"), &lines, &trusted, XFF),
            address("172.30.6.2")
        );
    }

    #[test]
    fn forwarded_ipv6_clients_are_grouped_by_their_64() {
        for value in ["2001:db8::1", "2001:db8::ffff:2"] {
            let lines = [("x-forwarded-for", value)];
            assert_eq!(from_loopback(&lines, XFF), "2001:db8::/64", "{value}");
        }
    }

    #[test]
    fn x_forwarded_for_uses_the_first_untrusted_hop_from_the_right() {
        assert_eq!(
            from_loopback(
                &[("x-forwarded-for", "198.51.100.99, 203.0.113.10, 127.0.0.1")],
                XFF
            ),
            "203.0.113.10"
        );
    }

    #[test]
    fn x_forwarded_for_of_only_trusted_hops_is_its_farthest() {
        let trusted = proxies(&["10.0.0.0/8", "127.0.0.1"]);
        let lines = headers(&[("x-forwarded-for", "10.0.0.7, 10.0.0.8")]);
        assert_eq!(
            client_address(peer("127.0.0.1"), &lines, &trusted, XFF),
            address("10.0.0.7")
        );
    }

    #[test]
    fn repeated_lines_read_as_one_chain_with_the_last_line_nearest() {
        assert_eq!(
            from_loopback(
                &[
                    ("x-forwarded-for", "198.51.100.99"),
                    ("x-forwarded-for", "203.0.113.10"),
                ],
                XFF
            ),
            "203.0.113.10"
        );
        assert_eq!(
            from_loopback(
                &[
                    ("x-forwarded-for", "198.51.100.99"),
                    ("x-forwarded-for", "203.0.113.10, 127.0.0.1"),
                    ("x-forwarded-for", "127.0.0.1"),
                ],
                XFF
            ),
            "203.0.113.10"
        );
        assert_eq!(
            from_loopback(
                &[
                    ("forwarded", "for=198.51.100.99"),
                    ("forwarded", "for=203.0.113.10"),
                ],
                FORWARDED
            ),
            "203.0.113.10"
        );
    }

    #[test]
    fn an_unreadable_hop_ends_the_walk_at_the_peer() {
        for value in [
            "198.51.100.99, garbage, 127.0.0.1",
            "198.51.100.99, unknown",
        ] {
            let lines = [("x-forwarded-for", value)];
            assert_eq!(from_loopback(&lines, XFF), LOOPBACK_ADDRESS, "{value}");
        }
        assert_eq!(
            from_loopback(
                &[("forwarded", "for=198.51.100.99, for=_hidden, for=127.0.0.1")],
                FORWARDED
            ),
            LOOPBACK_ADDRESS
        );
        assert_eq!(
            from_loopback(
                &[("forwarded", "for=198.51.100.99, by=127.0.0.1")],
                FORWARDED
            ),
            LOOPBACK_ADDRESS
        );
    }

    #[test]
    fn empty_list_elements_are_not_hops() {
        assert_eq!(
            from_loopback(&[("x-forwarded-for", "203.0.113.10,, 127.0.0.1, ")], XFF),
            "203.0.113.10"
        );
        assert_eq!(
            from_loopback(&[("x-forwarded-for", " , ")], XFF),
            LOOPBACK_ADDRESS
        );
    }

    #[test]
    fn forwarded_reads_each_elements_for_value() {
        for (value, client) in [
            ("for=203.0.113.10;proto=https", "203.0.113.10"),
            ("proto=https;FOR=203.0.113.10", "203.0.113.10"),
            ("for=\"203.0.113.10:4711\"", "203.0.113.10"),
            ("for=\"[2001:db8::1]:4711\"", "2001:db8::/64"),
            (
                "for=198.51.100.99, for=203.0.113.10;by=127.0.0.1, for=127.0.0.1",
                "203.0.113.10",
            ),
        ] {
            assert_eq!(
                from_loopback(&[("forwarded", value)], FORWARDED),
                client,
                "{value}"
            );
        }
    }

    #[test]
    fn separators_inside_quoted_forwarded_values_are_not_structure() {
        for value in [
            "proto=https;host=\"lyra.example;for=198.51.100.99\";for=203.0.113.10",
            "host=\"a, for=198.51.100.99\";for=203.0.113.10",
            "host=\"a\\\";for=198.51.100.99\";for=203.0.113.10",
            "for=203.0.113.10; proto=https ; host=\"a\"",
        ] {
            assert_eq!(
                from_loopback(&[("forwarded", value)], FORWARDED),
                "203.0.113.10",
                "{value}"
            );
        }
    }

    #[test]
    fn malformed_forwarded_lines_leave_the_peer() {
        for value in [
            "for=\"198.51.100.99, for=203.0.113.10",
            "for=\"198.51.100.99\"x, for=203.0.113.10",
            "for=198.51.100.99;for=203.0.113.10",
            "for=garbage;for=203.0.113.10",
            "for=",
            "for=203.0.113.10;",
            "=203.0.113.10",
        ] {
            assert_eq!(
                from_loopback(&[("forwarded", value)], FORWARDED),
                LOOPBACK_ADDRESS,
                "{value}"
            );
        }
    }

    #[test]
    fn a_malformed_forwarded_line_does_not_void_a_later_line() {
        assert_eq!(
            from_loopback(
                &[
                    ("forwarded", "for=\"198.51.100.99"),
                    ("forwarded", "for=203.0.113.10"),
                ],
                FORWARDED
            ),
            "203.0.113.10"
        );
    }

    #[test]
    fn x_real_ip_reads_a_single_value() {
        for (value, client) in [
            ("203.0.113.10", "203.0.113.10"),
            ("203.0.113.10:4711", "203.0.113.10"),
            ("[2001:db8::1]:4711", "2001:db8::/64"),
            ("[2001:db8::1]", "2001:db8::/64"),
        ] {
            assert_eq!(
                from_loopback(&[("x-real-ip", value)], REAL_IP),
                client,
                "{value}"
            );
        }
    }

    #[test]
    fn an_unusable_x_real_ip_leaves_the_peer() {
        for lines in [
            &[("x-real-ip", "203.0.113.10"), ("x-real-ip", "203.0.113.11")][..],
            &[("x-real-ip", "203.0.113.10, 203.0.113.11")][..],
            &[("x-real-ip", "198.51.100.99:1234, 203.0.113.10")][..],
            &[("x-real-ip", "[198.51.100.99], 203.0.113.10")][..],
            &[("x-real-ip", "[198.51.100.99]x")][..],
            &[("x-real-ip", "203.0.113.10:99999")][..],
            &[("x-real-ip", "203.0.113.10:+80")][..],
            &[("x-real-ip", "\"203.0.113.10\"")][..],
            &[("x-real-ip", "garbage")][..],
            &[][..],
        ] {
            assert_eq!(from_loopback(lines, REAL_IP), LOOPBACK_ADDRESS, "{lines:?}");
        }
    }

    #[test]
    fn only_the_selected_header_is_honoured() {
        let forged = [
            ("x-forwarded-for", "198.51.100.1"),
            ("x-real-ip", "198.51.100.2"),
            ("forwarded", "for=198.51.100.3"),
        ];
        assert_eq!(from_loopback(&forged, XFF), "198.51.100.1");
        assert_eq!(from_loopback(&forged, REAL_IP), "198.51.100.2");
        assert_eq!(from_loopback(&forged, FORWARDED), "198.51.100.3");

        assert_eq!(
            from_loopback(&forged[1..], XFF),
            LOOPBACK_ADDRESS,
            "other headers are no fallback"
        );
        assert_eq!(
            from_loopback(&forged[..1], FORWARDED),
            LOOPBACK_ADDRESS,
            "other headers are no fallback"
        );
    }

    #[test]
    fn mapped_peers_and_hops_are_treated_as_ipv4() {
        let trusted = loopback();
        let lines = headers(&[("x-forwarded-for", "203.0.113.10, ::ffff:127.0.0.1")]);
        assert_eq!(
            client_address(peer("::ffff:127.0.0.1"), &lines, &trusted, XFF),
            address("203.0.113.10"),
            "a mapped peer is the listed proxy and a mapped hop is skipped"
        );

        let mapped_client = headers(&[("x-forwarded-for", "::ffff:203.0.113.10")]);
        assert_eq!(
            client_address(peer("127.0.0.1"), &mapped_client, &trusted, XFF),
            address("203.0.113.10")
        );
        assert_eq!(
            client_address(peer("::ffff:198.51.100.1"), &lines, &trusted, XFF),
            address("198.51.100.1")
        );
    }

    fn finding(
        peer: &str,
        lines: &[(&'static str, &'static str)],
        header: ClientIpHeader,
    ) -> Option<Finding> {
        resolve(ip(peer), &headers(lines), &loopback(), header).1
    }

    #[test]
    fn an_untrusted_peer_sending_forwarding_headers_is_reported() {
        assert_eq!(
            finding("198.51.100.1", &[("x-real-ip", "203.0.113.10")], XFF),
            Some(Finding::UntrustedPeer)
        );
        assert_eq!(finding("198.51.100.1", &[], XFF), None);
    }

    #[test]
    fn a_trusted_peer_using_another_header_is_reported() {
        assert_eq!(
            finding("127.0.0.1", &[("x-real-ip", "203.0.113.10")], XFF),
            Some(Finding::OtherHeader("x-real-ip"))
        );
        assert_eq!(
            finding(
                "127.0.0.1",
                &[
                    ("x-forwarded-for", "garbage"),
                    ("forwarded", "for=203.0.113.10")
                ],
                XFF
            ),
            Some(Finding::OtherHeader("forwarded")),
            "an unusable configured header counts as absent"
        );
    }

    #[test]
    fn a_trusted_peer_is_not_reported_otherwise() {
        assert_eq!(finding("127.0.0.1", &[], XFF), None, "a health check");
        assert_eq!(
            finding("127.0.0.1", &[("x-forwarded-for", "203.0.113.10")], XFF),
            None
        );
        assert_eq!(
            finding(
                "127.0.0.1",
                &[
                    ("x-forwarded-for", "203.0.113.10"),
                    ("x-real-ip", "198.51.100.1")
                ],
                XFF
            ),
            None,
            "the configured header was usable"
        );
        assert_eq!(
            finding("127.0.0.1", &[("x-forwarded-for", "garbage")], XFF),
            None,
            "no other header to point at"
        );
    }
}
