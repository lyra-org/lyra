// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::env::VarError;

use anyhow::{
    Result,
    anyhow,
};
use tracing_subscriber::EnvFilter;

/// Log filter directives when `RUST_LOG` holds none. Targets match by prefix, so `lyra` and
/// `harmony` cover every crate of each.
const DEFAULT_LOG_DIRECTIVES: &str = "warn,lyra=info,harmony=info,tower_http=debug";

/// A log filter with the directives it was built from.
pub(crate) struct LogFilter {
    pub(crate) directives: String,
    pub(crate) filter: EnvFilter,
}

impl Default for LogFilter {
    fn default() -> Self {
        Self {
            directives: DEFAULT_LOG_DIRECTIVES.to_owned(),
            filter: EnvFilter::try_new(DEFAULT_LOG_DIRECTIVES)
                .expect("default log directives parse"),
        }
    }
}

/// The log filter for `RUST_LOG`'s value, which replaces the default when it holds any
/// directives. An error is a value that can't be used, for the caller to report before falling
/// back to the default.
pub(crate) fn log_filter(rust_log: Result<String, VarError>) -> Result<LogFilter> {
    let value = match rust_log {
        Ok(value) => value,
        Err(VarError::NotPresent) => return Ok(LogFilter::default()),
        Err(error) => return Err(anyhow!("{}: {error}", EnvFilter::DEFAULT_ENV)),
    };
    let directives = value.trim();
    if directives
        .split(',')
        .all(|directive| directive.trim().is_empty())
    {
        return Ok(LogFilter::default());
    }
    let filter = EnvFilter::try_new(directives)
        .map_err(|error| anyhow!("{} {directives:?}: {error}", EnvFilter::DEFAULT_ENV))?;
    Ok(LogFilter {
        directives: directives.to_owned(),
        filter,
    })
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    use super::*;

    fn directives(rust_log: Result<String, VarError>) -> Result<String> {
        Ok(log_filter(rust_log)?.directives)
    }

    #[test]
    fn unset_or_blank_rust_log_uses_the_default() -> Result<()> {
        assert_eq!(
            directives(Err(VarError::NotPresent))?,
            DEFAULT_LOG_DIRECTIVES
        );
        for blank in ["", " \t\n", ",", " , ,"] {
            assert_eq!(directives(Ok(blank.to_owned()))?, DEFAULT_LOG_DIRECTIVES);
        }
        Ok(())
    }

    #[test]
    fn rust_log_replaces_the_default() -> Result<()> {
        assert_eq!(
            directives(Ok(" lyra_db=debug ".to_owned()))?,
            "lyra_db=debug"
        );
        Ok(())
    }

    #[test]
    fn unusable_rust_log_is_an_error() {
        let invalid = directives(Ok("lyra=loud".to_owned())).unwrap_err();
        assert!(
            invalid.to_string().starts_with("RUST_LOG \"lyra=loud\": "),
            "{invalid}"
        );
        assert!(directives(Err(VarError::NotUnicode(OsString::from("x")))).is_err());
    }
}
