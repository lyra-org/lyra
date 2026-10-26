local fixture = ...

local device_profiles = require("../../protocol/items/device_profile")
local media = require("../../protocol/items/media")
local playback = require("../../protocol/items/playback")

-- The public stream reads omit the file's path, as reads for anyone who can't manage libraries do.
local track = playback.resolve_track_for_public_stream(fixture.ids.song)
assert(track ~= nil, "song resolves")
local source = playback.build_audio_media_source(track.id, track, true)
assert(source ~= nil, "song has a media source")
assert(source.Path == nil, `path {source.Path}`)

-- Format comes from the scanned contents, not the path.
assert(source.Container == "ogg", `container {source.Container}`)
assert(source.MediaStreams[1].Codec == "opus", `codec {source.MediaStreams[1].Codec}`)
assert(source.SupportsDirectPlay, "direct play without a path")
-- Upstream sets no DirectStreamUrl for library audio.
assert(source.DirectStreamUrl == nil, `direct stream url {source.DirectStreamUrl}`)

-- Jellyfin names containers after FFmpeg's demuxers.
assert(media.jellyfin_container({ container = "wavpack" } :: any) == "wv")
assert(media.jellyfin_container({ container = "mpc", codec = "musepack8" } :: any) == "mpc8")
assert(media.jellyfin_container({ container = "mpc", codec = "musepack7" } :: any) == "mpc")
assert(media.jellyfin_container({ container = "m4a", codec = "alac" } :: any) == "m4a")
assert(media.jellyfin_container(nil) == nil)

-- Playback sources keep the whole list FFmpeg's demuxer goes by.
assert(media.probed_container({ container = "m4a", codec = "aac" } :: any) == "mov,mp4,m4a,3gp,3g2,mj2")
assert(media.probed_container({ container = "wavpack" } :: any) == "wv")

-- Profile lists match any of the source's names; a leading `-` negates them.
assert(device_profiles.contains_container("m4a", "mov,mp4,m4a,3gp,3g2,mj2"))
assert(device_profiles.contains_container("MP4,flac", "mov,mp4,m4a"))
assert(not device_profiles.contains_container("aac", "mov,mp4,m4a"))
assert(device_profiles.contains_container("-mkv", "ogg"))
assert(not device_profiles.contains_container("-mkv,ogg", "ogg"))
assert(device_profiles.contains_container(nil, "ogg") and device_profiles.contains_container("", "ogg"))
assert(not device_profiles.contains_container(nil, nil), "an unknown value matches only a negative list")
assert(device_profiles.contains_container("-mp3", nil))
