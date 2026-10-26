local fixture = ...

local playback_sources = require("@lyra/playback_sources")

-- A source carries its file's format and audio properties, read from the file when it was scanned.
local source = playback_sources.streamable(fixture.ids.song)
assert(source ~= nil, "song has a playback source")
assert(source.container == "ogg", `container {source.container}`)
assert(source.codec == "opus", `codec {source.codec}`)
assert(source.sample_rate_hz == 48000, `sample rate {source.sample_rate_hz}`)
assert(source.channel_count == 1, `channels {source.channel_count}`)
assert(source.bit_depth == nil, `a lossy codec has no bit depth, got {source.bit_depth}`)
assert(source.bitrate_bps ~= nil and source.bitrate_bps > 0, `bitrate {source.bitrate_bps}`)
