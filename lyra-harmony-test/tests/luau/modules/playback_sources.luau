local fixture = ...

local playback_sources = require("@lyra/playback_sources")

local ids = fixture.ids

-- A source carries its file's format and audio properties, read from the file when it was scanned.
local sources = playback_sources.get({ ids.song, ids.part, ids.gone, "unknown" }, { include_entry = true })
local source = sources[ids.song]
assert(source ~= nil, "song has a playback source")
assert(source.container == "ogg", `container {source.container}`)
assert(source.codec == "opus", `codec {source.codec}`)
assert(source.sample_rate_hz == 48000, `sample rate {source.sample_rate_hz}`)
assert(source.channels == 1, `channels {source.channels}`)
assert(source.bit_depth == nil, `a lossy codec has no bit depth, got {source.bit_depth}`)
assert(source.bitrate_bps ~= nil and source.bitrate_bps > 0, `bitrate {source.bitrate_bps}`)
assert(source.start_ms == nil and source.end_ms == nil, "a whole file has no span")

-- The entry comes with `include_entry`, without its full path for a caller who can't manage
-- libraries.
assert(source.entry ~= nil and source.entry.name == "song.opus", "the source's entry")
assert(source.entry.full_path == nil, "no full path for a listener")
assert(playback_sources.get({ ids.song })[ids.song].entry == nil, "no entry unasked")

-- A track cut from a longer file plays a span of it.
local part = sources[ids.part]
assert(part.start_ms == 1000 and part.end_ms == 2000, `span {part.start_ms}-{part.end_ms}`)

-- A track whose file a sync no longer found has no source, nor has an unknown id.
assert(sources[ids.gone] == nil, "a missing file's track has no source")
assert(sources["unknown"] == nil, "an unknown id has none")

-- The system read gives the same source, without the entry's full path.
local streamable = playback_sources.streamable({ ids.song, "unknown" }, { include_entry = true })
assert(streamable[ids.song].container == "ogg", "the system reads the same source")
assert(streamable[ids.song].entry.full_path == nil, "no full path from the system read")
assert(streamable["unknown"] == nil, "an unknown id has none")
