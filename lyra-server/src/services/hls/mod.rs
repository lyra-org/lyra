// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

pub(crate) mod cleanup;
pub(crate) mod codec;
pub(crate) mod init;
pub(crate) mod signing;
pub(crate) mod state;

#[derive(Debug, thiserror::Error)]
pub(crate) enum HlsError {
    #[error("Unsupported HLS codec. Supported values: copy, aac, alac, flac.")]
    UnsupportedCodec,
    #[error("transcode capacity unavailable")]
    TranscodeCapacityUnavailable,
    #[error("HLS transcode job not found")]
    JobNotFound,
    #[error("HLS session does not belong to current user")]
    SessionForbidden,
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}
