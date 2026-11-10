// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! The hosts a plugin may send requests to.
//!
//! A plugin's manifest lists them as the grant of the scope gating this
//! module, and the host application sets each running plugin's list with
//! [`set_plugin_hosts`]. A list names destinations: it says nothing of the
//! addresses a name resolves to, nor of ports.
//!
//! A host is written as a URL writes it once parsed: lowercase, an
//! internationalized name in its `xn--` form, without a trailing dot, an
//! IPv4 address in dotted decimal, and an IPv6 address in brackets in its
//! shortest form, such as `[2001:db8::1]`. An address is allowed only by an
//! entry naming it; `*.` entries cover names alone.

use std::{
    collections::{
        BTreeSet,
        HashMap,
    },
    fmt,
    str::FromStr,
    sync::{
        Arc,
        LazyLock,
        Mutex as StdMutex,
        PoisonError,
    },
};

use harmony_core::scopes::ScopeGrant;

const ANY: &str = "*";
const SUBDOMAINS_PREFIX: &str = "*.";
/// Starts a named entry's item, which no host can start with.
const NAMED_PREFIX: char = '@';
/// Parts a named entry's plugin from its name, which neither can hold.
const NAMED_BY_SEPARATOR: char = '/';
const MAX_NAME_LEN: usize = 64;

/// One entry of a plugin's host list.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum HostEntry {
    /// Every host, written `*`.
    Any,
    /// This host alone.
    Host(String),
    /// Every name under this one at any depth, but not the name itself,
    /// written `*.example.org`.
    Subdomains(String),
    /// A host the host application looks up by this name when the plugin
    /// starts, such as the one a setting holds.
    Named(String),
    /// The named entry `name` of the plugin `plugin`, as it stands beside
    /// other plugins' entries, written `@plugin/name`. The name is looked up
    /// for each plugin apart, so one plugin's is never another's.
    NamedBy { plugin: String, name: String },
}

impl HostEntry {
    /// The entry allowing the host of `address`, a URL or a bare host with
    /// an optional port; `None` when it holds no host.
    pub fn of_address(address: &str) -> Option<Self> {
        let address = address.trim();
        let url = if address.contains("://") {
            url::Url::parse(address).ok()?
        } else {
            url::Url::parse(&format!("http://{address}")).ok()?
        };
        request_host(&url).map(|host| Self::Host(host.to_string()))
    }

    fn allows(&self, host: &str, is_name: bool) -> bool {
        match self {
            Self::Any => true,
            Self::Host(allowed) => allowed == host,
            Self::Subdomains(parent) => is_name && is_under(host, parent),
            Self::Named(_) | Self::NamedBy { .. } => false,
        }
    }

    /// Whether everything `other` allows, this allows too. A named entry
    /// stands for a host unknown here, so only `*` and the same plugin's
    /// entry of that name cover it.
    fn covers(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Any, _) => true,
            (Self::Subdomains(parent), Self::Host(host)) => {
                is_under(host, parent) && !is_address(host)
            }
            (Self::Subdomains(parent), Self::Subdomains(child)) => {
                parent == child || is_under(child, parent)
            }
            _ => self == other,
        }
    }
}

fn is_under(host: &str, parent: &str) -> bool {
    host.strip_suffix(parent)
        .is_some_and(|rest| rest.ends_with('.'))
}

fn is_address(host: &str) -> bool {
    host.starts_with('[') || host.parse::<std::net::Ipv4Addr>().is_ok()
}

/// `url`'s host as entries name it.
fn request_host(url: &url::Url) -> Option<&str> {
    let host = url.host_str()?;
    Some(host.strip_suffix('.').unwrap_or(host)).filter(|host| !host.is_empty())
}

/// `host` when it is written as a parsed URL writes it.
fn canonical_host(host: &str) -> Result<&str, String> {
    let written = url::Url::parse(&format!("http://{host}/")).ok();
    let parsed = written.as_ref().and_then(|url| url.host_str());
    match parsed {
        Some(parsed) if parsed == host && !host.ends_with('.') && !host.contains('*') => Ok(host),
        Some(parsed) if !parsed.contains('*') && is_plain_host(host) => Err(format!(
            "'{host}' is not written as requests match it; write '{}'",
            parsed.strip_suffix('.').unwrap_or(parsed)
        )),
        _ => Err(format!(
            "'{host}' is not a host name or address, which takes no scheme, port or path"
        )),
    }
}

/// Whether `host` could be a host in another spelling, rather than a host
/// with something around it.
fn is_plain_host(host: &str) -> bool {
    !host.contains(['/', '@', '?', '#', '\\']) && (host.starts_with('[') || !host.contains(':'))
}

impl fmt::Display for HostEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Any => f.write_str(ANY),
            Self::Host(host) => f.write_str(host),
            Self::Subdomains(parent) => write!(f, "{SUBDOMAINS_PREFIX}{parent}"),
            Self::Named(name) => write!(f, "{NAMED_PREFIX}{name}"),
            Self::NamedBy { plugin, name } => {
                write!(f, "{NAMED_PREFIX}{plugin}{NAMED_BY_SEPARATOR}{name}")
            }
        }
    }
}

/// Reads an entry as [`HostEntry`] displays it: `*`, a host, `*.` and a
/// name, or `@` and the name of a named entry, after its plugin and `/`
/// when it carries one.
impl FromStr for HostEntry {
    type Err = String;

    fn from_str(item: &str) -> Result<Self, String> {
        if item == ANY {
            return Ok(Self::Any);
        }
        if let Some(named) = item.strip_prefix(NAMED_PREFIX) {
            let (plugin, name) = match named.split_once(NAMED_BY_SEPARATOR) {
                Some((plugin, name)) => (Some(plugin), name),
                None => (None, named),
            };
            let valid = !name.is_empty()
                && name.len() <= MAX_NAME_LEN
                && name
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'));
            if !valid {
                return Err(format!(
                    "'{name}' is not a name of up to {MAX_NAME_LEN} letters, digits, '_', '-' and \
                     '.'"
                ));
            }
            return match plugin {
                None => Ok(Self::Named(name.to_string())),
                Some(plugin) => match harmony_core::plugin::validate_plugin_id(plugin) {
                    Ok(()) => Ok(Self::NamedBy {
                        plugin: plugin.to_string(),
                        name: name.to_string(),
                    }),
                    Err(reason) => Err(format!("'{plugin}' is not a plugin id: it {reason}")),
                },
            };
        }
        if let Some(parent) = item.strip_prefix(SUBDOMAINS_PREFIX) {
            let parent = canonical_host(parent)?;
            if is_address(parent) {
                return Err(format!("'{item}' puts names under an address"));
            }
            return Ok(Self::Subdomains(parent.to_string()));
        }
        canonical_host(item).map(|host| Self::Host(host.to_string()))
    }
}

/// The grant of the scope gating this module: the `hosts` a manifest lists,
/// each a string [`HostEntry`] reads, or an object holding only the key the
/// host application chose, with the name of a named entry.
#[derive(Clone, Debug)]
pub struct HostsGrant {
    named_key: String,
}

impl HostsGrant {
    /// `named_key` is the key of a named entry's object, such as `setting`
    /// where the host application looks names up among a plugin's settings.
    pub fn new(named_key: impl Into<String>) -> Self {
        Self {
            named_key: named_key.into(),
        }
    }

    fn entry(&self, value: &serde_json::Value) -> Result<HostEntry, String> {
        match value {
            serde_json::Value::String(item) if item.starts_with(NAMED_PREFIX) => Err(format!(
                "'{item}' is not a host; a named entry is written {{ \"{}\": \"<name>\" }}",
                self.named_key
            )),
            serde_json::Value::String(item) => item.parse(),
            serde_json::Value::Object(object) => {
                let name = match object.get(&self.named_key) {
                    Some(serde_json::Value::String(name)) if object.len() == 1 => name,
                    _ => {
                        return Err(format!(
                            "an object entry holds only `{}` with a name",
                            self.named_key
                        ));
                    }
                };
                // A manifest names only its own plugin's entries.
                match format!("{NAMED_PREFIX}{name}").parse()? {
                    HostEntry::NamedBy { .. } => Err(format!(
                        "'{name}' is not a name, which holds no '{NAMED_BY_SEPARATOR}'"
                    )),
                    entry => Ok(entry),
                }
            }
            _ => Err(format!(
                "an entry is a host or an object with `{}`",
                self.named_key
            )),
        }
    }
}

impl ScopeGrant for HostsGrant {
    fn key(&self) -> &str {
        "hosts"
    }

    fn parse(&self, value: &serde_json::Value) -> Result<Vec<String>, String> {
        let serde_json::Value::Array(values) = value else {
            return Err("it is not a list".to_string());
        };
        let mut items = BTreeSet::new();
        for value in values {
            let item = self.entry(value)?.to_string();
            if !items.insert(item.clone()) {
                return Err(format!("'{item}' is listed more than once"));
            }
        }
        Ok(items.into_iter().collect())
    }

    fn qualify(&self, plugin_id: &str, item: &str) -> String {
        match item.parse() {
            Ok(HostEntry::Named(name)) => HostEntry::NamedBy {
                plugin: plugin_id.to_string(),
                name,
            }
            .to_string(),
            _ => item.to_string(),
        }
    }

    fn covers(&self, granted: &[String], item: &str) -> bool {
        let Ok(item) = item.parse::<HostEntry>() else {
            return false;
        };
        granted
            .iter()
            .filter_map(|granted| granted.parse::<HostEntry>().ok())
            .any(|granted| granted.covers(&item))
    }
}

/// The hosts one plugin's requests may reach.
#[derive(Debug, Default)]
pub(crate) struct AllowedHosts(Vec<HostEntry>);

impl AllowedHosts {
    /// Why a request to `url` is refused, when it is.
    pub(crate) fn refusal(&self, url: &url::Url) -> Option<String> {
        let Some(host) = request_host(url) else {
            return Some("the URL has no host".to_string());
        };
        let is_name = matches!(url.host(), Some(url::Host::Domain(_)));
        if self.0.iter().any(|entry| entry.allows(host, is_name)) {
            return None;
        }
        Some(host_not_allowed_message(host))
    }
}

/// What a request refused for its `host` says, as its response's
/// `status_message`.
pub fn host_not_allowed_message(host: &str) -> String {
    format!("host '{host}' is not among the plugin's hosts")
}

static PLUGIN_HOSTS: LazyLock<StdMutex<HashMap<String, Arc<AllowedHosts>>>> =
    LazyLock::new(|| StdMutex::new(HashMap::new()));

fn plugin_hosts() -> std::sync::MutexGuard<'static, HashMap<String, Arc<AllowedHosts>>> {
    PLUGIN_HOSTS.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Sets the hosts `plugin_id`'s requests may reach from now on, replacing
/// the ones set before. A named entry allows nothing: the host application
/// passes the host it stands for in its place, as [`HostEntry::of_address`]
/// gives it. See [`crate::HttpModule`] for when to call it.
pub fn set_plugin_hosts(plugin_id: &str, hosts: impl IntoIterator<Item = HostEntry>) {
    let hosts = Arc::new(AllowedHosts(hosts.into_iter().collect()));
    plugin_hosts().insert(plugin_id.to_string(), hosts);
}

/// Forgets `plugin_id`'s hosts, leaving its requests none to reach.
pub fn remove_plugin_hosts(plugin_id: &str) {
    plugin_hosts().remove(plugin_id);
}

/// The hosts `plugin_id` may reach as they stand now, for one request and
/// the redirects it follows. A plugin without any set reaches none.
pub(crate) fn snapshot(plugin_id: &str) -> Arc<AllowedHosts> {
    plugin_hosts().get(plugin_id).cloned().unwrap_or_default()
}

/// Whether `plugin_id`'s hosts, as they stand, allow a request to `url`.
#[doc(hidden)]
pub fn test_plugin_reaches(plugin_id: &str, url: &str) -> bool {
    url::Url::parse(url).is_ok_and(|url| snapshot(plugin_id).refusal(&url).is_none())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(item: &str) -> HostEntry {
        item.parse()
            .unwrap_or_else(|error| panic!("{item}: {error}"))
    }

    fn grant() -> HostsGrant {
        HostsGrant::new("setting")
    }

    fn allowed(items: &[&str]) -> AllowedHosts {
        AllowedHosts(items.iter().map(|item| entry(item)).collect())
    }

    fn allows(items: &[&str], url: &str) -> bool {
        let url = url::Url::parse(url).expect("url");
        allowed(items).refusal(&url).is_none()
    }

    #[test]
    fn entries_read_back_as_written() {
        for (item, expected) in [
            ("*", HostEntry::Any),
            ("example.org", HostEntry::Host("example.org".into())),
            ("localhost", HostEntry::Host("localhost".into())),
            (
                "xn--bcher-kva.example",
                HostEntry::Host("xn--bcher-kva.example".into()),
            ),
            ("192.0.2.7", HostEntry::Host("192.0.2.7".into())),
            ("[2001:db8::1]", HostEntry::Host("[2001:db8::1]".into())),
            ("*.example.org", HostEntry::Subdomains("example.org".into())),
            ("@base_url", HostEntry::Named("base_url".into())),
            (
                "@my-plugin/base_url",
                HostEntry::NamedBy {
                    plugin: "my-plugin".into(),
                    name: "base_url".into(),
                },
            ),
        ] {
            assert_eq!(entry(item), expected);
            assert_eq!(expected.to_string(), item);
        }
    }

    #[test]
    fn entries_are_hosts_as_a_parsed_url_writes_them() {
        for (item, expected) in [
            ("Example.org", "write 'example.org'"),
            ("example.org.", "write 'example.org'"),
            ("bücher.example", "write 'xn--bcher-kva.example'"),
            ("127.1", "write '127.0.0.1'"),
            ("0x7f.0.0.1", "write '127.0.0.1'"),
            ("[2001:DB8::1]", "write '[2001:db8::1]'"),
            ("[2001:db8:0:0:0:0:0:1]", "write '[2001:db8::1]'"),
            ("2001:db8::1", "not a host name or address"),
            ("https://example.org", "not a host name or address"),
            ("example.org:443", "not a host name or address"),
            ("example.org/path", "not a host name or address"),
            ("user@example.org", "not a host name or address"),
            ("example.org?q", "not a host name or address"),
            ("exa mple.org", "not a host name or address"),
            ("", "not a host name or address"),
            ("**", "not a host name or address"),
            ("a*.example.org", "not a host name or address"),
            ("*.*.example.org", "not a host name or address"),
            ("example.*", "not a host name or address"),
            ("*.", "not a host name or address"),
            ("*.Example.org", "write 'example.org'"),
            ("*.192.0.2.7", "puts names under an address"),
            ("*.[2001:db8::1]", "puts names under an address"),
            ("@", "is not a name"),
            ("@base url", "is not a name"),
            ("@plugin/", "is not a name"),
            ("@plugin/a/b", "is not a name"),
            ("@/base_url", "is not a plugin id"),
            ("@my.plugin/base_url", "is not a plugin id"),
        ] {
            let error = item.parse::<HostEntry>().expect_err(item);
            assert!(error.contains(expected), "{item}: {error}");
        }
    }

    #[test]
    fn a_grant_is_a_sorted_list_of_distinct_entries() {
        let items = grant()
            .parse(&serde_json::json!([
                "musicbrainz.org",
                { "setting": "base_url" },
                "*.archive.org",
                "192.0.2.7",
            ]))
            .expect("valid hosts");
        assert_eq!(
            items,
            ["*.archive.org", "192.0.2.7", "@base_url", "musicbrainz.org"]
        );
        assert_eq!(
            grant().parse(&serde_json::json!([])),
            Ok(Vec::new()),
            "a plugin may reach no host"
        );
        assert_eq!(
            HostsGrant::new("option").parse(&serde_json::json!([{ "option": "server" }])),
            Ok(vec!["@server".to_string()]),
            "the host application names the key"
        );

        for (value, expected) in [
            (serde_json::json!("example.org"), "it is not a list"),
            (
                serde_json::json!({ "setting": "base_url" }),
                "it is not a list",
            ),
            (serde_json::json!(["Example.org"]), "write 'example.org'"),
            (
                serde_json::json!(["a.example", "a.example"]),
                "'a.example' is listed more than once",
            ),
            (
                serde_json::json!([{ "setting": "url" }, { "setting": "url" }]),
                "'@url' is listed more than once",
            ),
            (
                serde_json::json!(["@url"]),
                "a named entry is written { \"setting\": \"<name>\" }",
            ),
            (
                serde_json::json!([1]),
                "an entry is a host or an object with `setting`",
            ),
            (
                serde_json::json!([{ "option": "url" }]),
                "holds only `setting` with a name",
            ),
            (
                serde_json::json!([{ "setting": 1 }]),
                "holds only `setting` with a name",
            ),
            (
                serde_json::json!([{ "setting": "url", "port": 1 }]),
                "holds only `setting` with a name",
            ),
            (serde_json::json!([{ "setting": "" }]), "is not a name"),
            (
                serde_json::json!([{ "setting": "other/base_url" }]),
                "is not a name, which holds no '/'",
            ),
        ] {
            let error = grant().parse(&value).expect_err("invalid hosts");
            assert!(error.contains(expected), "{value}: {error}");
        }
    }

    #[test]
    fn covering_is_inclusion_of_what_entries_allow() {
        const ITEMS: [&str; 9] = [
            "*",
            "*.example.org",
            "*.sub.example.org",
            "example.org",
            "a.example.org",
            "a.sub.example.org",
            "notexample.org",
            "192.0.2.7",
            "@base_url",
        ];
        // Row by row: what the entry covers, in the order of `ITEMS`.
        let expected: [(&str, [u8; 9]); 9] = [
            ("*", [1, 1, 1, 1, 1, 1, 1, 1, 1]),
            ("*.example.org", [0, 1, 1, 0, 1, 1, 0, 0, 0]),
            ("*.sub.example.org", [0, 0, 1, 0, 0, 1, 0, 0, 0]),
            ("example.org", [0, 0, 0, 1, 0, 0, 0, 0, 0]),
            ("a.example.org", [0, 0, 0, 0, 1, 0, 0, 0, 0]),
            ("a.sub.example.org", [0, 0, 0, 0, 0, 1, 0, 0, 0]),
            ("notexample.org", [0, 0, 0, 0, 0, 0, 1, 0, 0]),
            ("192.0.2.7", [0, 0, 0, 0, 0, 0, 0, 1, 0]),
            ("@base_url", [0, 0, 0, 0, 0, 0, 0, 0, 1]),
        ];
        for (granted, row) in expected {
            for (item, covered) in ITEMS.iter().zip(row) {
                assert_eq!(
                    grant().covers(&[granted.to_string()], item),
                    covered == 1,
                    "{granted} covering {item}"
                );
            }
        }

        let own = |plugin: &str| grant().qualify(plugin, "@base_url");
        assert_eq!(own("caller"), "@caller/base_url");
        assert_eq!(grant().qualify("caller", "*.example.org"), "*.example.org");
        assert!(grant().covers(&[own("caller")], &own("caller")));
        assert!(
            !grant().covers(&[own("helper")], &own("caller")),
            "each plugin's name is looked up among its own, so one never covers another's"
        );
        assert!(!grant().covers(&["@base_url".to_string()], &own("caller")));
        assert!(grant().covers(&["*".to_string()], &own("caller")));
        assert!(!grant().covers(&[own("caller")], "example.org"));

        let granted = ["a.example.org".to_string(), "*.sub.example.org".to_string()];
        assert!(grant().covers(&granted, "x.y.sub.example.org"));
        assert!(!grant().covers(&granted, "*.example.org"));
        assert!(!grant().covers(&[], "example.org"));
        assert!(!grant().covers(&["*".to_string()], "Not A Host"));
        assert!(
            !HostEntry::Subdomains("2.7".into()).covers(&HostEntry::Host("192.0.2.7".into())),
            "names under a name never cover an address"
        );
    }

    #[test]
    fn requests_match_hosts_names_under_them_and_nothing_else() {
        let hosts = ["example.org", "*.archive.org", "192.0.2.7", "[2001:db8::1]"];
        for url in [
            "https://example.org/a?b",
            "http://EXAMPLE.org:8080/",
            "https://example.org./",
            "https://user:secret@example.org/",
            "https://ia800.us.archive.org/1/items/x",
            "https://a.archive.org./",
            "http://192.0.2.7/",
            "http://0xc0.0.2.7/",
            "http://[2001:DB8:0:0::1]:9000/",
        ] {
            assert!(allows(&hosts, url), "{url}");
        }
        for url in [
            "https://archive.org/",
            "https://notarchive.org/",
            "https://example.org.evil.example/",
            "https://sub.example.org/",
            "https://evil.example/?example.org",
            "https://example.org@evil.example/",
            "http://192.0.2.8/",
            "http://[2001:db8::2]/",
        ] {
            assert!(!allows(&hosts, url), "{url}");
        }

        assert!(allows(&["*"], "http://203.0.113.9:1/"));
        assert!(allows(&["*"], "https://anything.example/"));
        assert!(!allows(&[], "https://example.org/"));
        let under_digits = AllowedHosts(vec![HostEntry::Subdomains("2.7".into())]);
        let address = url::Url::parse("http://192.0.2.7/").expect("url");
        assert!(
            under_digits.refusal(&address).is_some(),
            "names under a name never allow an address"
        );
        assert!(!allows(&["@plugin/base_url"], "https://base_url/"));
        assert!(
            !allows(&["@base_url"], "https://base_url/"),
            "a named entry allows nothing until the host application resolves it"
        );
    }

    #[test]
    fn an_address_gives_the_entry_of_its_host() {
        for (address, host) in [
            ("https://Music.Example:8096/api/", "music.example"),
            ("http://192.0.2.7:8000", "192.0.2.7"),
            ("music.example", "music.example"),
            ("  music.example:8096/path ", "music.example"),
            ("localhost:8000", "localhost"),
            ("[2001:DB8::1]:8000", "[2001:db8::1]"),
            ("https://music.example./", "music.example"),
        ] {
            assert_eq!(
                HostEntry::of_address(address),
                Some(HostEntry::Host(host.to_string())),
                "{address}"
            );
        }
        for address in ["", "   ", "http://", "not a host", "file:///etc/passwd"] {
            assert_eq!(HostEntry::of_address(address), None, "{address:?}");
        }
    }

    #[test]
    fn a_plugin_without_hosts_set_reaches_none() {
        let url = url::Url::parse("https://example.org/").expect("url");
        assert!(snapshot("hosts-never-set").refusal(&url).is_some());

        set_plugin_hosts("hosts-set", [entry("example.org")]);
        assert!(snapshot("hosts-set").refusal(&url).is_none());
        set_plugin_hosts("hosts-set", [entry("other.example")]);
        assert!(
            snapshot("hosts-set").refusal(&url).is_some(),
            "a set replaces"
        );
        remove_plugin_hosts("hosts-set");
        assert!(snapshot("hosts-set").refusal(&url).is_some());
    }
}
