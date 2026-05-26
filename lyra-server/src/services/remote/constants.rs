// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::time::Duration;

pub(crate) const PING_INTERVAL: Duration = Duration::from_secs(30);
pub(crate) const PONG_TIMEOUT: Duration = Duration::from_secs(10);
pub(crate) const WRITE_TIMEOUT: Duration = Duration::from_secs(5);
pub(crate) const AUTH_CHECK_INTERVAL: Duration = Duration::from_secs(60);
pub(crate) const MAX_MESSAGE_SIZE: usize = 64 * 1024;
pub(crate) const MAX_CONNECTIONS_PER_USER: usize = 8;
pub(crate) const CLOSE_CODE_DUPLICATE_SESSION: u16 = 4000;
pub(crate) const CLOSE_CODE_REGISTRATION_REJECTED: u16 = 4001;
pub(crate) const CLOSE_REASON_DUPLICATE_SESSION: &str = "duplicate_session";
pub(crate) const CLOSE_REASON_REGISTRATION_REJECTED: &str = "registration_rejected";

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RemoteAction {
    Play,
    Pause,
    Unpause,
    Stop,
    Seek,
    NextTrack,
    PreviousTrack,
    SetVolume,
}
