// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use super::*;

pub(super) fn infer_content_type(path: &str) -> &'static str {
    let extension = FsPath::new(path)
        .extension()
        .and_then(|value| value.to_str())
        .map(|value| value.to_ascii_lowercase());
    match extension.as_deref() {
        Some("aac") => "audio/aac",
        Some("aiff") | Some("aif") => "audio/aiff",
        Some("flac") => "audio/flac",
        Some("m4a") | Some("m4b") | Some("mp4") => "audio/mp4",
        Some("mp3") => "audio/mpeg",
        Some("ogg") => "audio/ogg",
        Some("opus") => "audio/ogg",
        Some("webm") => "audio/webm",
        Some("wav") => "audio/wav",
        Some("m3u8") => "application/x-mpegurl",
        _ => "application/octet-stream",
    }
}
