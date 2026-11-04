// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use super::{
    DeliveryError,
    TrackAccess,
};
use crate::services::auth::media_tokens::DeliveryOptions;

#[derive(Clone, Debug, Default)]
pub(crate) struct ServeTrackOptions {
    pub(crate) format: Option<String>,
    pub(crate) codec: Option<String>,
    pub(crate) copy_profiles: Option<String>,
    pub(crate) bitrate_bps: Option<u32>,
    pub(crate) sample_rate_hz: Option<u32>,
    pub(crate) channels: Option<u32>,
    pub(crate) bit_depth: Option<u32>,
    pub(crate) prefer_vbr: Option<bool>,
    pub(crate) start_offset_ms: Option<u64>,
}

impl ServeTrackOptions {
    pub(crate) fn delivery(&self) -> DeliveryOptions {
        DeliveryOptions {
            format: self.format.clone(),
            codec: self.codec.clone(),
            copy_profiles: self.copy_profiles.clone(),
            bitrate_bps: self.bitrate_bps,
            sample_rate_hz: self.sample_rate_hz,
            channels: self.channels,
            bit_depth: self.bit_depth,
            prefer_vbr: self.prefer_vbr,
        }
    }

    /// The options a request through `access` is served with. A media token carries the delivery
    /// it was issued for, so a request with one may only choose its start offset.
    pub(crate) fn bound_to(self, access: &TrackAccess) -> Result<Self, DeliveryError> {
        let TrackAccess::MediaToken(token) = access else {
            return Ok(self);
        };
        reject_unbound_options(&self.delivery())?;
        let DeliveryOptions {
            format,
            codec,
            copy_profiles,
            bitrate_bps,
            sample_rate_hz,
            channels,
            bit_depth,
            prefer_vbr,
        } = token.options.clone();
        Ok(Self {
            format,
            codec,
            copy_profiles,
            bitrate_bps,
            sample_rate_hz,
            channels,
            bit_depth,
            prefer_vbr,
            start_offset_ms: self.start_offset_ms,
        })
    }
}

pub(crate) fn reject_unbound_options(requested: &DeliveryOptions) -> Result<(), DeliveryError> {
    if *requested == DeliveryOptions::default() {
        return Ok(());
    }
    Err(DeliveryError::bad_request(
        "this media token already fixes the delivery; request a new playback URL to change it",
    ))
}
