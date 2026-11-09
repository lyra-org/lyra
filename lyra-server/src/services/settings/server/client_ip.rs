// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! How a client is identified behind a reverse proxy: which peers may report its address, and the
//! header they report it in.

use std::{
    fmt,
    net::IpAddr,
};

use anyhow::{
    Result,
    anyhow,
    bail,
};
use ipnet::{
    IpNet,
    Ipv4Net,
};
use serde::{
    Deserialize,
    Deserializer,
    de,
};

/// The peers trusted to report a client's address, as ranges. An IPv4-mapped address
/// or range is held and matched as IPv4, so a range written in IPv6 covers only IPv6 peers unless
/// it lies within the mapped block: `::/0` does not trust IPv4 peers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TrustedProxies(Vec<IpNet>);

impl TrustedProxies {
    #[cfg(test)]
    pub(crate) const fn none() -> Self {
        Self(Vec::new())
    }

    /// Reads addresses and CIDR ranges; a bare address is a single host. A range with address bits
    /// past its prefix is refused, as a mistyped prefix would otherwise widen what is trusted.
    pub(crate) fn parse<'a>(entries: impl IntoIterator<Item = &'a str>) -> Result<Self> {
        entries
            .into_iter()
            .map(str::trim)
            .map(|entry| parse_range(entry).map_err(|err| anyhow!("entry '{entry}': {err}")))
            .collect::<Result<Vec<_>>>()
            .map(Self)
    }

    pub(crate) fn contains(&self, ip: IpAddr) -> bool {
        let ip = ip.to_canonical();
        self.0.iter().any(|range| range.contains(&ip))
    }

    /// The canonical form of each entry, with a single host written as a bare address.
    pub(crate) fn entries(&self) -> Vec<String> {
        self.0.iter().map(entry).collect()
    }
}

impl fmt::Display for TrustedProxies {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.entries().join(", "))
    }
}

impl<'de> Deserialize<'de> for TrustedProxies {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let entries = Vec::<String>::deserialize(deserializer)?;
        Self::parse(entries.iter().map(String::as_str)).map_err(de::Error::custom)
    }
}

fn parse_range(raw: &str) -> Result<IpNet> {
    let range = if raw.contains('/') {
        let range = raw.parse::<IpNet>()?;
        if range.addr() != range.network() {
            bail!(
                "address bits are set past the /{} prefix",
                range.prefix_len()
            );
        }
        range
    } else {
        IpNet::from(raw.parse::<IpAddr>()?)
    };
    Ok(as_ipv4_when_mapped(range))
}

fn as_ipv4_when_mapped(range: IpNet) -> IpNet {
    if let IpNet::V6(net) = range
        && let Some(prefix_len) = net.prefix_len().checked_sub(96)
        && let Some(address) = net.addr().to_ipv4_mapped()
        && let Ok(mapped) = Ipv4Net::new(address, prefix_len)
    {
        return mapped.into();
    }
    range
}

fn entry(range: &IpNet) -> String {
    if range.prefix_len() == range.max_prefix_len() {
        range.addr().to_string()
    } else {
        range.to_string()
    }
}

/// The header a trusted proxy reports a client's address in.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum ClientIpHeader {
    XForwardedFor,
    XRealIp,
    Forwarded,
}

impl ClientIpHeader {
    pub(crate) const OPTIONS: &'static [&'static str] = &[
        Self::XForwardedFor.name(),
        Self::XRealIp.name(),
        Self::Forwarded.name(),
    ];

    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::XForwardedFor => "x-forwarded-for",
            Self::XRealIp => "x-real-ip",
            Self::Forwarded => "forwarded",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn proxies(entries: &[&str]) -> TrustedProxies {
        TrustedProxies::parse(entries.iter().copied()).expect("valid entries")
    }

    fn ip(raw: &str) -> IpAddr {
        raw.parse().expect("valid ip")
    }

    #[test]
    fn a_bare_address_is_a_single_host() {
        let trusted = proxies(&["192.0.2.1", "2001:db8::1"]);
        assert!(trusted.contains(ip("192.0.2.1")));
        assert!(!trusted.contains(ip("192.0.2.2")));
        assert!(trusted.contains(ip("2001:db8::1")));
        assert!(!trusted.contains(ip("2001:db8::2")));
    }

    #[test]
    fn ipv4_ranges_match_only_inside_their_prefix() {
        let trusted = proxies(&["10.1.0.0/16"]);
        assert!(trusted.contains(ip("10.1.255.254")));
        assert!(!trusted.contains(ip("10.2.0.1")));
        assert!(!trusted.contains(ip("2001:db8::1")));
    }

    #[test]
    fn ipv6_ranges_match_only_inside_their_prefix() {
        let trusted = proxies(&["2001:db8:1::/48"]);
        assert!(trusted.contains(ip("2001:db8:1:ffff::1")));
        assert!(!trusted.contains(ip("2001:db8:2::1")));
        assert!(!trusted.contains(ip("10.0.0.1")));
    }

    #[test]
    fn entries_are_canonicalized() {
        assert_eq!(
            proxies(&[
                " 127.0.0.1 ",
                "0000:0000:0000:0000:0000:0000:0000:0001",
                "10.0.0.0/8",
                "2001:0db8:0000::/32",
                "192.0.2.1/32",
                "2001:db8::1/128",
            ])
            .entries(),
            [
                "127.0.0.1",
                "::1",
                "10.0.0.0/8",
                "2001:db8::/32",
                "192.0.2.1",
                "2001:db8::1",
            ]
        );
    }

    #[test]
    fn mapped_entries_become_ipv4() {
        assert_eq!(
            proxies(&[
                "::ffff:127.0.0.1",
                "::ffff:10.0.0.0/104",
                "::ffff:192.0.2.1/128",
                "::ffff:0:0/96",
            ])
            .entries(),
            ["127.0.0.1", "10.0.0.0/8", "192.0.2.1", "0.0.0.0/0"]
        );
    }

    #[test]
    fn mapped_and_plain_forms_match_each_other() {
        let plain = proxies(&["10.0.0.0/8"]);
        assert!(plain.contains(ip("::ffff:10.9.8.7")));
        assert!(!plain.contains(ip("::ffff:11.0.0.1")));

        let mapped = proxies(&["::ffff:10.0.0.0/104"]);
        assert!(mapped.contains(ip("10.9.8.7")));
        assert!(mapped.contains(ip("::ffff:10.9.8.7")));
    }

    #[test]
    fn zero_prefixes_cover_their_own_family() {
        let v4 = proxies(&["0.0.0.0/0"]);
        assert!(v4.contains(ip("203.0.113.1")));
        assert!(v4.contains(ip("::ffff:203.0.113.1")));
        assert!(!v4.contains(ip("2001:db8::1")));

        let v6 = proxies(&["::/0"]);
        assert!(v6.contains(ip("2001:db8::1")));
        assert!(!v6.contains(ip("203.0.113.1")));
        assert!(!v6.contains(ip("::ffff:203.0.113.1")));
    }

    #[test]
    fn invalid_entries_are_named() {
        for raw in [
            "localhost",
            "10.0.0.0/33",
            "2001:db8::/129",
            "10.0.0.0/",
            "10.1.2.3/8",
            "2001:db8::1/32",
            "::ffff:10.1.2.3/104",
            "/8",
            "",
        ] {
            let error = TrustedProxies::parse([raw]).expect_err(raw);
            assert!(
                error.to_string().contains(&format!("entry '{raw}'")),
                "{raw}"
            );
        }
    }

    #[test]
    fn deserializes_from_a_list_of_entries() {
        let trusted: TrustedProxies =
            serde_json::from_value(serde_json::json!(["127.0.0.1", "10.0.0.0/8"])).unwrap();
        assert_eq!(trusted, proxies(&["127.0.0.1", "10.0.0.0/8"]));
        assert!(serde_json::from_value::<TrustedProxies>(serde_json::json!(["nope"])).is_err());
    }

    #[test]
    fn display_lists_the_canonical_entries() {
        assert_eq!(
            proxies(&["127.0.0.1/32", "10.0.0.0/8"]).to_string(),
            "127.0.0.1, 10.0.0.0/8"
        );
    }

    #[test]
    fn every_header_option_names_its_variant() {
        for option in ClientIpHeader::OPTIONS {
            let header: ClientIpHeader = serde_json::from_value(serde_json::json!(option)).unwrap();
            assert_eq!(header.name(), *option);
        }
        assert!(
            serde_json::from_value::<ClientIpHeader>(serde_json::json!("cf-connecting-ip"))
                .is_err()
        );
    }
}
