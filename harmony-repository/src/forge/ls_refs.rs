// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Ref listing over Git's smart HTTP protocol v2 (`ls-refs`): one request
//! answers what the forge API needs two for, and the Git endpoint is outside
//! the forge's API rate limits.

use reqwest::{
    Client,
    header,
};

use super::{
    RepoSpec,
    is_object_id,
};

/// Git endpoints expect a Git client; GitHub's rejects agents without a
/// `git/` token. Only this request carries it: GitHub's API rejects agents
/// that start with one, and other traffic keeps the host client's agent.
const GIT_USER_AGENT: &str = concat!("git/harmony-repository-", env!("CARGO_PKG_VERSION"));

/// Requests only ever name a few exact prefixes, so a longer answer is not
/// one this module asked for.
const MAX_RESPONSE_BYTES: usize = 64 * 1024;

/// The largest pkt-line Git allows, length prefix included.
const MAX_PKT_LEN: usize = 65520;

/// A ref an `ls-refs` response advertised.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AdvertisedRef {
    pub(crate) name: String,
    /// The object the ref names; an annotated tag's own object.
    pub(crate) oid: String,
    /// The commit an annotated tag points at.
    pub(crate) peeled: Option<String>,
    /// The ref a symbolic ref such as `HEAD` points at.
    pub(crate) symref_target: Option<String>,
}

impl AdvertisedRef {
    /// The commit the ref resolves to, through an annotated tag.
    pub(crate) fn commit(&self) -> &str {
        self.peeled.as_deref().unwrap_or(&self.oid)
    }
}

/// Lists the refs whose names start with one of `prefixes`, with annotated
/// tags peeled and symbolic refs' targets. `None` when the forge gave no
/// well-formed protocol v2 answer, so callers ask the forge API instead,
/// and for prefixes that cannot be sent as-is; an empty list is a v2 answer
/// in which no ref matched.
pub(crate) async fn ls_refs(
    client: &Client,
    spec: &RepoSpec,
    prefixes: &[&str],
) -> Option<Vec<AdvertisedRef>> {
    let url = spec.upload_pack_url();
    let body = request_body(prefixes)?;

    let response = client
        .post(&url)
        .header(header::USER_AGENT, GIT_USER_AGENT)
        .header(
            header::CONTENT_TYPE,
            "application/x-git-upload-pack-request",
        )
        .header("Git-Protocol", "version=2")
        .body(body)
        .send()
        .await;
    let mut response = match response {
        Ok(response) => response,
        Err(err) => {
            tracing::debug!("ls-refs request {url} failed: {err}");
            return None;
        }
    };
    let is_result = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|media_type| {
            media_type
                .trim()
                .eq_ignore_ascii_case("application/x-git-upload-pack-result")
        });
    if !response.status().is_success() || !is_result {
        tracing::debug!(
            "ls-refs request {url} returned HTTP {} without a v2 result",
            response.status()
        );
        return None;
    }

    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.ok()? {
        if bytes.len() + chunk.len() > MAX_RESPONSE_BYTES {
            tracing::debug!("ls-refs response from {url} exceeds {MAX_RESPONSE_BYTES} bytes");
            return None;
        }
        bytes.extend_from_slice(&chunk);
    }
    let refs = parse_refs(&bytes);
    if refs.is_none() {
        tracing::debug!("ls-refs response from {url} is not a protocol v2 ref list");
    }
    refs
}

/// An `ls-refs` request for `prefixes`. `None` without prefixes, which would
/// list every ref, and for a prefix that is empty or could break the
/// pkt-line framing.
fn request_body(prefixes: &[&str]) -> Option<Vec<u8>> {
    let sendable = |prefix: &&str| {
        !prefix.is_empty() && !prefix.chars().any(|c| c.is_whitespace() || c.is_control())
    };
    if prefixes.is_empty() || !prefixes.iter().all(sendable) {
        return None;
    }
    let mut body = Vec::new();
    write_pkt(&mut body, "command=ls-refs\n");
    body.extend_from_slice(b"0001");
    write_pkt(&mut body, "peel\n");
    write_pkt(&mut body, "symrefs\n");
    for prefix in prefixes {
        write_pkt(&mut body, &format!("ref-prefix {prefix}\n"));
    }
    body.extend_from_slice(b"0000");
    Some(body)
}

fn write_pkt(body: &mut Vec<u8>, line: &str) {
    body.extend_from_slice(format!("{:04x}", line.len() + 4).as_bytes());
    body.extend_from_slice(line.as_bytes());
}

/// Parses an `ls-refs` response: one pkt-line per ref, ended by a flush
/// packet and nothing after it.
fn parse_refs(mut body: &[u8]) -> Option<Vec<AdvertisedRef>> {
    let mut refs = Vec::new();
    loop {
        let (len, rest) = body.split_at_checked(4)?;
        if !len.iter().all(u8::is_ascii_hexdigit) {
            return None;
        }
        let len = usize::from_str_radix(std::str::from_utf8(len).ok()?, 16).ok()?;
        match len {
            0 => return rest.is_empty().then_some(refs),
            1..=4 => return None,
            len if len > MAX_PKT_LEN => return None,
            _ => {}
        }
        let (payload, rest) = rest.split_at_checked(len - 4)?;
        refs.push(parse_ref(payload)?);
        body = rest;
    }
}

/// Parses `<oid> <name>[ <attribute>]*`. Attributes this module did not ask
/// for are ignored.
fn parse_ref(payload: &[u8]) -> Option<AdvertisedRef> {
    let line = std::str::from_utf8(payload).ok()?;
    let line = line.strip_suffix('\n').unwrap_or(line);
    let mut fields = line.split(' ');
    let oid = fields.next().filter(|oid| is_object_id(oid))?;
    let name = fields.next().filter(|name| !name.is_empty())?;

    let mut advertised = AdvertisedRef {
        name: name.to_string(),
        oid: oid.to_string(),
        peeled: None,
        symref_target: None,
    };
    for attribute in fields {
        if let Some(target) = attribute.strip_prefix("symref-target:") {
            advertised.symref_target = Some(target.to_string());
        } else if let Some(peeled) = attribute.strip_prefix("peeled:") {
            advertised.peeled = Some(Some(peeled).filter(|oid| is_object_id(oid))?.to_string());
        }
    }
    Some(advertised)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::testutil::{
        CannedResponse,
        TestServer,
        client,
        pkt_lines,
    };

    #[test]
    fn writes_ls_refs_requests_as_pkt_lines() {
        assert_eq!(
            request_body(&["HEAD"]).unwrap(),
            b"0014command=ls-refs\n00010009peel\n000csymrefs\n0014ref-prefix HEAD\n0000"
        );
        assert_eq!(
            request_body(&["HEAD", "refs/heads/main", "refs/tags/main"]).unwrap(),
            b"0014command=ls-refs\n00010009peel\n000csymrefs\n0014ref-prefix HEAD\n\
              001fref-prefix refs/heads/main\n001eref-prefix refs/tags/main\n0000"
        );
    }

    #[test]
    fn never_sends_unprefixed_or_unframeable_requests() {
        for prefixes in [
            &[][..],
            &[""],
            &["HEAD", ""],
            &["refs/heads/main\n0000"],
            &["refs/heads/a b"],
            &["refs/heads/\u{1}"],
        ] {
            assert_eq!(request_body(prefixes), None, "{prefixes:?}");
        }
    }

    #[test]
    fn parses_refs_with_their_attributes() {
        let (head, tag, peeled) = ("a".repeat(40), "b".repeat(40), "c".repeat(40));
        let body = pkt_lines(&[
            &format!("{head} HEAD symref-target:refs/heads/main\n"),
            &format!("{tag} refs/tags/v1 peeled:{peeled} unknown:attribute\n"),
        ]);

        let refs = parse_refs(body.as_bytes()).unwrap();
        assert_eq!(refs[0].name, "HEAD");
        assert_eq!(refs[0].commit(), head);
        assert_eq!(refs[0].symref_target.as_deref(), Some("refs/heads/main"));
        assert_eq!(refs[1].commit(), peeled);
        assert_eq!(parse_refs(b"0000"), Some(Vec::new()));
    }

    #[test]
    fn rejects_malformed_responses() {
        let oid = "a".repeat(40);
        let valid = format!("{oid} refs/heads/main\n");
        for body in [
            String::new(),
            format!("{:04x}{valid}", valid.len() + 4),
            format!("{:04x}{valid}0000trailing", valid.len() + 4),
            format!("{:04x}{valid}0000", valid.len() + 10),
            "0001".to_string(),
            "+01a".to_string(),
            "zzzz0000".to_string(),
            pkt_lines(&["ERR upload-pack: not our ref\n"]),
            pkt_lines(&["not-an-oid refs/heads/main\n"]),
            pkt_lines(&[&format!("{oid}\n")]),
            pkt_lines(&[&format!("{oid} refs/tags/v1 peeled:short\n")]),
        ] {
            assert_eq!(parse_refs(body.as_bytes()), None, "{body:?}");
        }
    }

    #[tokio::test]
    async fn non_v2_answers_are_none() {
        let server = TestServer::start(HashMap::from([(
            "/owner/html.git/git-upload-pack".to_string(),
            CannedResponse::json("<html></html>"),
        )]))
        .await;
        for repo in ["missing", "html"] {
            let url = format!("{}/owner/{repo}", server.url());
            let spec = RepoSpec::parse(&url, None).unwrap();
            assert_eq!(ls_refs(&client(), &spec, &["HEAD"]).await, None, "{repo}");
        }
    }
}
