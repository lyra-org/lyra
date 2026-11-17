// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use harmony_luau as luau;
use harmony_luau::StringEnum;
#[cfg(feature = "docgen")]
use harmony_luau::{
    LuauType,
    LuauTypeInfo,
};

use crate::services::auth::AuthError;

/// What a host error means, for plugins that branch on it. A kind means the same for any module.
#[harmony_macros::string_enum(
    name = "ErrorKind",
    description = "What a host error means, whichever module raised it."
)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ErrorKind {
    NotFound,
    Forbidden,
    Unauthenticated,
    Conflict,
    Invalid,
}

pub(crate) fn kinded(kind: ErrorKind, error: impl std::fmt::Display) -> luau::Error {
    luau::Error::Kinded {
        kind: kind.as_str(),
        message: error.to_string(),
    }
}

/// What `error` means to a plugin: a credential that no longer verifies is `unauthenticated`, a
/// refusal `forbidden` and a missing target `not_found`.
pub(crate) fn auth_error_kind(error: &AuthError) -> Option<ErrorKind> {
    match error {
        AuthError::MissingBearerCredential
        | AuthError::InvalidBearerCredential
        | AuthError::SessionExpired => Some(ErrorKind::Unauthenticated),
        AuthError::Forbidden(_) => Some(ErrorKind::Forbidden),
        AuthError::NotFound(_) => Some(ErrorKind::NotFound),
        AuthError::InvalidClientName(_) | AuthError::RateLimited(_) => None,
        AuthError::Internal(error) => error.downcast_ref::<AuthError>().and_then(auth_error_kind),
    }
}

/// `error` raised for a plugin, with the kind [`auth_error_kind`] gives it.
pub(crate) fn auth_error(error: AuthError) -> luau::Error {
    match error {
        AuthError::Internal(error) => match error.downcast::<AuthError>() {
            Ok(error) => auth_error(error),
            Err(error) => crate::plugins::runtime_error(error),
        },
        error => match auth_error_kind(&error) {
            Some(kind) => kinded(kind, error),
            None => crate::plugins::runtime_error(error),
        },
    }
}

#[cfg(feature = "docgen")]
struct Unknown;

#[cfg(feature = "docgen")]
impl LuauTypeInfo for Unknown {
    fn luau_type() -> LuauType {
        LuauType::literal("unknown")
    }
}

pub(crate) struct ErrorsModule;

#[harmony_macros::module(
    id = "lyra/errors",
    description = "The kinds of host errors, for plugins that branch on what an error means rather than its message.",
    docs_feature = "docgen"
)]
impl ErrorsModule {
    #[harmony(
        description = "The kind of an error a host module raised. Returns nil for an error without one, including every error Luau code raises itself.",
        args(err: Unknown),
        returns(Option<ErrorKind>)
    )]
    fn kind(err: Option<luau::Value>) -> Option<ErrorKind> {
        err.and_then(|err| err.error_kind())
            .and_then(<ErrorKind as luau::StringEnum>::from_str)
    }
}

#[cfg(feature = "docgen")]
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    use harmony_luau::{
        DescribeModule,
        DescribeTypeAlias,
    };

    harmony_luau::render_definition_file_with_support(
        &ErrorsModule::module_descriptor(),
        &[ErrorKind::type_alias_descriptor()],
        &[],
        &[],
    )
}
