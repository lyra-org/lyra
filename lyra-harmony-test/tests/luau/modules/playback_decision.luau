local fixture = ...

local playback_sources = require("@lyra/playback_sources")

-- Nothing asked of the source: it is served as it is.
local original = playback_sources.decide(fixture.ids.song)
assert(original.transcode == false, "the source streams as it is")
assert(#original.reasons == 0, `reasons {#original.reasons}`)
assert(original.container == "opus", `container {original.container}`)
assert(original.codec == "opus", `codec {original.codec}`)
assert(original.content_type == "audio/opus", `content type {original.content_type}`)
assert(original.source.container == "ogg", `source container {original.source.container}`)
assert(original.sample_rate_hz == 48000, `sample rate {original.sample_rate_hz}`)

-- Another container re-encodes, and says so.
local mp3 = playback_sources.decide(fixture.ids.song, { format = "mp3" })
assert(mp3.transcode == true, "mp3 is a transcode")
assert(#mp3.reasons == 1 and mp3.reasons[1] == "container", `reasons {#mp3.reasons}`)
assert(mp3.container == "mp3", `container {mp3.container}`)
assert(mp3.codec == "mp3", `codec {mp3.codec}`)
assert(mp3.content_type == "audio/mpeg", `content type {mp3.content_type}`)
assert(mp3.bitrate_bps == 192000, `bitrate {mp3.bitrate_bps}`)
assert(mp3.source.codec == "opus", `source codec {mp3.source.codec}`)

-- A request the stream would refuse raises its error instead.
local ok, err = pcall(playback_sources.decide, fixture.ids.song, { format = "caf" })
assert(not ok, "caf cannot be streamed")
assert(string.find(tostring(err), "400", 1, true), `error {err}`)
