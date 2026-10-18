local fixture = ...

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
assert(source.DirectStreamUrl ~= nil, "direct stream url without a path")

-- Jellyfin names containers after FFmpeg's demuxers.
assert(media.jellyfin_container({ container = "wavpack" } :: any) == "wv")
assert(media.jellyfin_container({ container = "mpc", codec = "musepack8" } :: any) == "mpc8")
assert(media.jellyfin_container({ container = "mpc", codec = "musepack7" } :: any) == "mpc")
assert(media.jellyfin_container({ container = "m4a", codec = "alac" } :: any) == "m4a")
assert(media.jellyfin_container(nil) == nil)
