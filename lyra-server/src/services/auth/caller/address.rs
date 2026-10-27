// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! The address a request comes from, as the limiters count it.

use std::{
    fmt,
    net::{
        IpAddr,
        Ipv6Addr,
    },
};

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

#[cfg(test)]
mod tests {
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
}
