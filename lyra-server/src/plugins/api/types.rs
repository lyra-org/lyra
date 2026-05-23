// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use harmony_luau::{
    LuauType,
    LuauTypeInfo,
};

pub(super) struct ApiMethod;
pub(super) struct ApiRouteAuthMode;
#[cfg(feature = "docgen")]
pub(super) struct ApiHeaders;
pub(super) struct ApiQueryParams;
#[cfg(feature = "docgen")]
pub(super) struct ApiPathParams;
#[cfg(feature = "docgen")]
pub(super) struct ApiResponse;
pub(super) struct ApiHandler;
pub(super) struct ApiWebSocketHandler;
#[cfg(feature = "docgen")]
pub(super) struct ApiWebSocketReader;
#[cfg(feature = "docgen")]
pub(super) struct ApiWebSocketSender;
#[cfg(feature = "docgen")]
pub(super) struct ApiAuth;
#[cfg(feature = "docgen")]
pub(super) struct ApiRequest;
#[cfg(feature = "docgen")]
pub(super) struct ApiContext;
#[cfg(feature = "docgen")]
pub(super) struct ImageTransformOptions;
#[cfg(feature = "docgen")]
pub(super) struct TrackServeOptions;
#[cfg(feature = "docgen")]
pub(super) struct HlsServeOptions;

impl LuauTypeInfo for ApiMethod {
    fn luau_type() -> LuauType {
        LuauType::literal("ApiMethod")
    }
}

impl LuauTypeInfo for ApiRouteAuthMode {
    fn luau_type() -> LuauType {
        LuauType::literal("ApiRouteAuthMode")
    }
}

#[cfg(feature = "docgen")]
impl LuauTypeInfo for ApiHeaders {
    fn luau_type() -> LuauType {
        LuauType::literal("ApiHeaders")
    }
}

impl LuauTypeInfo for ApiQueryParams {
    fn luau_type() -> LuauType {
        LuauType::literal("ApiQueryParams")
    }
}

#[cfg(feature = "docgen")]
impl LuauTypeInfo for ApiPathParams {
    fn luau_type() -> LuauType {
        LuauType::literal("ApiPathParams")
    }
}

#[cfg(feature = "docgen")]
impl LuauTypeInfo for ApiResponse {
    fn luau_type() -> LuauType {
        LuauType::literal("ApiResponse")
    }
}

impl LuauTypeInfo for ApiHandler {
    fn luau_type() -> LuauType {
        LuauType::literal("ApiHandler")
    }
}

impl LuauTypeInfo for ApiWebSocketHandler {
    fn luau_type() -> LuauType {
        LuauType::literal("ApiWebSocketHandler")
    }
}

#[cfg(feature = "docgen")]
impl LuauTypeInfo for ApiWebSocketReader {
    fn luau_type() -> LuauType {
        LuauType::literal("ApiWebSocketReader")
    }
}

#[cfg(feature = "docgen")]
impl LuauTypeInfo for ApiWebSocketSender {
    fn luau_type() -> LuauType {
        LuauType::literal("ApiWebSocketSender")
    }
}

#[cfg(feature = "docgen")]
impl LuauTypeInfo for ApiAuth {
    fn luau_type() -> LuauType {
        LuauType::literal("ApiAuth")
    }
}

#[cfg(feature = "docgen")]
impl LuauTypeInfo for ApiRequest {
    fn luau_type() -> LuauType {
        LuauType::literal("ApiRequest")
    }
}

#[cfg(feature = "docgen")]
impl LuauTypeInfo for ApiContext {
    fn luau_type() -> LuauType {
        LuauType::literal("ApiContext")
    }
}

#[cfg(feature = "docgen")]
impl LuauTypeInfo for ImageTransformOptions {
    fn luau_type() -> LuauType {
        LuauType::literal("ImageTransformOptions")
    }
}

#[cfg(feature = "docgen")]
impl LuauTypeInfo for TrackServeOptions {
    fn luau_type() -> LuauType {
        LuauType::literal("TrackServeOptions")
    }
}

#[cfg(feature = "docgen")]
impl LuauTypeInfo for HlsServeOptions {
    fn luau_type() -> LuauType {
        LuauType::literal("HlsServeOptions")
    }
}
