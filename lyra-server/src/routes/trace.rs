// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::fmt;

use axum::{
    Router,
    body::Body,
    http::Request,
};
use tower_http::trace::TraceLayer;
use tracing::{
    Span,
    Subscriber,
};
use tracing_subscriber::{
    Layer,
    filter::filter_fn,
};

/// Target of the event that logs each request's raw query string. Queries
/// carry credentials (API keys, passwords, media tokens, WebSocket tickets),
/// so the request span names the parameters only, and the values reach the
/// log only when a filter directive targets this one by name.
const RAW_QUERY_TARGET: &str = "lyra::http::query";

/// Drops [`RAW_QUERY_TARGET`] unless one of `directives` targets it by name,
/// whatever the others enable, span directives included.
pub(crate) fn raw_query_guard<S: Subscriber>(directives: &str) -> impl Layer<S> {
    let named = directives.split(',').any(|directive| {
        directive
            .split(['[', '='])
            .next()
            .is_some_and(|target| target.trim() == RAW_QUERY_TARGET)
    });
    filter_fn(move |metadata| named || metadata.target() != RAW_QUERY_TARGET)
}

pub(crate) fn apply(router: Router) -> Router {
    router.layer(TraceLayer::new_for_http().make_span_with(request_span))
}

fn request_span(request: &Request<Body>) -> Span {
    let uri = request.uri();
    tracing::trace!(target: RAW_QUERY_TARGET, query = uri.query());
    tracing::debug_span!(
        target: "tower_http::trace",
        "request",
        method = %request.method(),
        path = %uri.path(),
        query = %QueryNames(uri.query().unwrap_or_default()),
    )
}

struct QueryNames<'a>(&'a str);

impl fmt::Display for QueryNames<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[")?;
        let names = self
            .0
            .split('&')
            .filter(|pair| !pair.is_empty())
            .map(|pair| pair.split_once('=').map_or(pair, |(name, _)| name));
        for (index, name) in names.enumerate() {
            if index > 0 {
                f.write_str(", ")?;
            }
            f.write_str(name)?;
        }
        f.write_str("]")
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io,
        sync::{
            Arc,
            Mutex,
        },
    };

    use tracing_subscriber::{
        EnvFilter,
        fmt::MakeWriter,
        layer::SubscriberExt,
    };

    use super::*;

    #[derive(Clone, Default)]
    struct Captured(Arc<Mutex<Vec<u8>>>);

    impl io::Write for Captured {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for Captured {
        type Writer = Self;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    fn logged_request(filter: &str, uri: &str) -> String {
        let captured = Captured::default();
        let subscriber = tracing_subscriber::registry()
            .with(EnvFilter::try_new(filter).unwrap())
            .with(raw_query_guard(filter))
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(captured.clone())
                    .with_ansi(false)
                    .without_time(),
            );
        tracing::subscriber::with_default(subscriber, || {
            let _connection = tracing::info_span!("connection").entered();
            let request = Request::get(uri).body(Body::empty()).unwrap();
            let _entered = request_span(&request).entered();
            tracing::debug!(target: "tower_http::trace::on_response", "finished processing request");
        });
        String::from_utf8(captured.0.lock().unwrap().clone()).unwrap()
    }

    #[test]
    fn query_names_skip_values_and_empty_pairs() {
        assert_eq!(QueryNames("").to_string(), "[]");
        assert_eq!(
            QueryNames("u=alice&p=enc:7365637265&&f&id=1&id=2").to_string(),
            "[u, p, f, id, id]"
        );
    }

    #[test]
    fn request_log_names_query_parameters_without_values() {
        let log = logged_request(
            "tower_http=debug",
            "/rest/stream.view?u=alice&p=hunter2&apiKey=secret&id=42",
        );

        assert!(log.contains("path=/rest/stream.view"), "{log}");
        assert!(log.contains("query=[u, p, apiKey, id]"), "{log}");
        for value in ["alice", "hunter2", "secret", "42"] {
            assert!(!log.contains(value), "{value} in {log}");
        }
    }

    #[test]
    fn raw_query_logs_only_when_its_target_is_named() {
        let uri = "/api/ws?ticket=secret";
        for filter in [
            "trace",
            "lyra=trace",
            "lyra::http=trace",
            "[connection]=trace",
            &format!("trace,[{RAW_QUERY_TARGET}]=off"),
        ] {
            let log = logged_request(filter, uri);
            assert!(!log.contains("secret"), "{filter}: {log}");
        }
        for filter in [
            format!("info,{RAW_QUERY_TARGET}=trace"),
            format!("info,{RAW_QUERY_TARGET}"),
        ] {
            let log = logged_request(&filter, uri);
            assert!(log.contains("query=\"ticket=secret\""), "{filter}: {log}");
        }
    }
}
