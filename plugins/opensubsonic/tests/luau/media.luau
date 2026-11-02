local fixture = ...
local call = require("./_call")
local lyrics = require("@lyra/lyrics")

local ids = fixture.ids
local key = fixture.api_keys.listener_key

local function stream(params: { [string]: string }): any
	local query: { [string]: string } = { apiKey = key, f = "json" }
	for name, value in params do
		query[name] = value
	end
	return call.raw("stream", query)
end

-- `stream` serves the file as it is, checking the caller's access.
local raw = stream({ id = ids.synced })
assert(raw.kind == "stream_track" and raw.track_id == ids.synced, `stream kind {raw.kind}`)
assert(raw.options == nil, "no options serves the file")
assert(stream({ id = ids.synced, format = "raw", maxBitRate = "0" }).options == nil, "raw and no limit")
assert(stream({ id = ids.synced, format = "raw", maxBitRate = "128", timeOffset = "5" }).options == nil, "raw wins")

local transcoded = stream({ id = ids.synced, format = "mp3", maxBitRate = "128", timeOffset = "1" })
assert(transcoded.kind == "stream_track", `transcode kind {transcoded.kind}`)
assert(transcoded.options.format == "mp3" and transcoded.options.bitrate_bps == 128000)
assert(transcoded.options.start_offset_ms == 1000 and transcoded.options.public == nil)
assert(stream({ id = ids.synced, format = "MP4" }).options.format == "m4a", "mp4 delivers m4a")
local capped = stream({ id = ids.synced, maxBitRate = "96" })
assert(capped.options.bitrate_bps == 96000 and capped.options.format == nil, "a bitrate cap alone")

-- `estimateContentLength` always reaches the server, which waits only for a transcode: even the
-- file as it is gets transcoded when it is a cue sheet's segment.
local estimated = stream({ id = ids.synced, estimateContentLength = "true" })
assert(estimated.options.wait_for_transcode == true and estimated.options.format == nil, "alone")
local raw_estimated = stream({ id = ids.synced, format = "raw", maxBitRate = "128", estimateContentLength = "true" })
assert(raw_estimated.options.wait_for_transcode == true and raw_estimated.options.bitrate_bps == nil, "raw")
local segment = stream({ id = ids.segment, estimateContentLength = "true" })
assert(segment.kind == "stream_track" and segment.options.wait_for_transcode == true, `segment {segment.kind}`)
local complete = stream({ id = ids.synced, format = "mp3", estimateContentLength = "true" })
assert(complete.options.format == "mp3" and complete.options.wait_for_transcode == true, "waits")
assert(transcoded.options.wait_for_transcode == nil, "streams as it encodes by default")
assert(stream({ id = ids.synced, estimateContentLength = "false" }).options == nil, "false asks for nothing")
assert(call.error_code("stream", key, { id = ids.synced, estimateContentLength = "maybe" }) == 0, "a bad boolean")

local refused = call.call("stream", key, { id = ids.synced, format = "bogus" })
assert(refused.error.code == 0, "an unknown format")
assert(refused.error.message == "The song can't be streamed with these options", "the host's error stays in the log")
assert(call.error_code("stream", key, {}) == 10, "stream needs an id")
assert(call.error_code("stream", key, { id = ids.hidden }) == 70, "a hidden song isn't streamed")
assert(call.error_code("stream", key, { id = ids.album }) == 70, "an album isn't streamed")
assert(call.error_code("stream", "not-a-key", { id = ids.synced }) == 44, "streaming needs a key")

-- Lyrics come as one structured entry; a song without any has none.
local none = call.ok("getLyricsBySongId", key, { id = ids.synced }).lyricsList
assert(type(none) == "table" and none.structuredLyrics == nil, "no lyrics")
local none_xml = call.raw("getLyricsBySongId", { apiKey = key, id = ids.synced }).body
assert(string.find(none_xml, "<lyricsList/>", 1, true), none_xml)

lyrics.set_personal(ids.synced, {
	content_type = "application/lrc",
	body = "[00:01.00]First line\n[00:02.50]Second line\n",
	language = "eng",
})
lyrics.set_personal(ids.plain, { content_type = "text/plain", body = "One\nTwo" })

local synced = call.ok("getLyricsBySongId", key, { id = ids.synced }).lyricsList.structuredLyrics
assert(#synced == 1)
local entry = synced[1]
assert(entry.synced == true and entry.lang == "eng", `synced {entry.synced} lang {entry.lang}`)
assert(entry.displayTitle == "Synced" and entry.displayArtist == "Amy")
assert(#entry.line == 2 and entry.line[1].value == "First line" and entry.line[1].start == 1000)
assert(entry.line[2].start == 2500)

local synced_xml = call.raw("getLyricsBySongId", { apiKey = key, id = ids.synced }).body
assert(string.find(synced_xml, '<line start="1000">First line</line>', 1, true), synced_xml)
local empty_xml = call.raw("getLyricsBySongId", { apiKey = key, id = ids.hidden }).body
assert(string.find(empty_xml, "<error ", 1, true), "a hidden song's lyrics fail in XML too")

local plain = call.ok("getLyricsBySongId", key, { id = ids.plain }).lyricsList.structuredLyrics[1]
assert(plain.synced == false, "plain lyrics aren't synced")
assert(plain.lang ~= nil and plain.lang ~= "", "plain lyrics carry a language")
assert(#plain.line == 2 and plain.line[1].value == "One" and plain.line[1].start == nil)

assert(call.error_code("getLyricsBySongId", key, {}) == 10, "lyrics need an id")
assert(call.error_code("getLyricsBySongId", key, { id = ids.hidden }) == 70, "a hidden song's lyrics")

-- Clients learn about lyrics, form posts and stream offsets without signing in.
local extensions = call.raw("getOpenSubsonicExtensions", { f = "json" }) :: any
local serde = require("@harmony/serde")
local names = {}
for _, extension in (serde.json.decode(extensions.body) :: any)["subsonic-response"].openSubsonicExtensions do
	names[extension.name] = extension.versions[1]
end
assert(names.songLyrics == 1 and names.formPost == 1 and names.transcodeOffset == 1 and names.apiKeyAuthentication == 1)
