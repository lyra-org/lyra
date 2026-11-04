local fixture = ...

local playback_sources = require("@lyra/playback_sources")

-- Nothing asked of the source: it is served as it is.
local original = playback_sources.decide(fixture.ids.song)
assert(original.transcode == false, "the source streams as it is")
assert(#original.reasons == 0, `reasons {#original.reasons}`)
assert(original.container == "opus", `container {original.container}`)
assert(original.codec == "opus", `codec {original.codec}`)
assert(original.content_type == "audio/ogg", `content type {original.content_type}`)
assert(original.source.container == "ogg", `source container {original.source.container}`)
assert(original.sample_rate_hz == 48000, `sample rate {original.sample_rate_hz}`)
assert(original.bit_depth == nil, `lossy audio has no bit depth, got {original.bit_depth}`)

-- Another container re-encodes, and says so.
local mp3 = playback_sources.decide(fixture.ids.song, { format = "mp3" })
assert(mp3.transcode == true, "mp3 is a transcode")
assert(#mp3.reasons == 1 and mp3.reasons[1] == "container", `reasons {#mp3.reasons}`)
assert(mp3.container == "mp3", `container {mp3.container}`)
assert(mp3.codec == "mp3", `codec {mp3.codec}`)
assert(mp3.content_type == "audio/mpeg", `content type {mp3.content_type}`)
assert(mp3.bitrate_bps == 192000, `bitrate {mp3.bitrate_bps}`)
assert(mp3.source.codec == "opus", `source codec {mp3.source.codec}`)

-- Lossless output of a lossy source is 16-bit, so a 16-bit cap asks for no more than that.
local flac = playback_sources.decide(fixture.ids.song, { format = "flac", bit_depth = 16 })
assert(flac.codec == "flac", `codec {flac.codec}`)
assert(flac.bit_depth == 16, `bit depth {flac.bit_depth}`)
assert(#flac.reasons == 1 and flac.reasons[1] == "container", `reasons {#flac.reasons}`)

local bad_depth_ok, bad_depth_err = pcall(playback_sources.decide, fixture.ids.song, { bit_depth = 20 })
assert(not bad_depth_ok, "20 bits is not a deliverable depth")
assert(string.find(tostring(bad_depth_err), "bit_depth must be 16 or 24", 1, true), `error {bad_depth_err}`)

-- A request the stream would refuse raises its error instead.
local ok, err = pcall(playback_sources.decide, fixture.ids.song, { format = "caf" })
assert(not ok, "caf cannot be streamed")
assert(string.find(tostring(err), "400", 1, true), `error {err}`)
