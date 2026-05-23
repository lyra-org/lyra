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
pub(super) struct ApiHeaders;
pub(super) struct ApiQueryParams;
#[cfg(any(feature = "docgen", test))]
pub(super) struct ApiPathParams;
pub(super) struct ApiResponse;
pub(super) struct ApiHandler;
pub(super) struct ApiWebSocketHandler;
#[cfg(any(feature = "docgen", test))]
pub(super) struct ApiWebSocketReader;
#[cfg(any(feature = "docgen", test))]
pub(super) struct ApiWebSocketSender;
#[cfg(any(feature = "docgen", test))]
pub(super) struct ApiAuth;
#[cfg(any(feature = "docgen", test))]
pub(super) struct ApiRequest;
#[cfg(any(feature = "docgen", test))]
pub(super) struct ApiContext;
pub(super) struct ImageTransformOptions;
pub(super) struct TrackServeOptions;
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

impl LuauTypeInfo for ApiPathParams {
    fn luau_type() -> LuauType {
        LuauType::literal("ApiPathParams")
    }
}

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

impl LuauTypeInfo for ApiWebSocketReader {
    fn luau_type() -> LuauType {
        LuauType::literal("ApiWebSocketReader")
    }
}

impl LuauTypeInfo for ApiWebSocketSender {
    fn luau_type() -> LuauType {
        LuauType::literal("ApiWebSocketSender")
    }
}

impl LuauTypeInfo for ApiAuth {
    fn luau_type() -> LuauType {
        LuauType::literal("ApiAuth")
    }
}

impl LuauTypeInfo for ApiRequest {
    fn luau_type() -> LuauType {
        LuauType::literal("ApiRequest")
    }
}

impl LuauTypeInfo for ApiContext {
    fn luau_type() -> LuauType {
        LuauType::literal("ApiContext")
    }
}

impl LuauTypeInfo for ImageTransformOptions {
    fn luau_type() -> LuauType {
        LuauType::literal("ImageTransformOptions")
    }
}

impl LuauTypeInfo for TrackServeOptions {
    fn luau_type() -> LuauType {
        LuauType::literal("TrackServeOptions")
    }
}

impl LuauTypeInfo for HlsServeOptions {
    fn luau_type() -> LuauType {
        LuauType::literal("HlsServeOptions")
    }
}
