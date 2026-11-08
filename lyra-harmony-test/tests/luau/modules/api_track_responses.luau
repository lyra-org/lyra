local api = require("@lyra/api")
local playback_sources = require("@lyra/playback_sources")

local function refuses(label: string, expected: string, build: () -> any)
	local ok, err = pcall(build)
	assert(not ok, `{label} built`)
	assert(string.find(tostring(err), expected, 1, true), `{label}: {tostring(err)}`)
end

local stream = api.response.stream_track("track", {
	format = "m4a",
	preferred_codecs = { "alac", "aac" },
	bitrate_bps = 256000,
	public = true,
})
assert(stream.kind == "stream_track" and stream.track_id == "track")
assert(stream.options.format == "m4a" and stream.options.public == true)
assert(api.response.stream_track("track", {}).kind == "stream_track", "empty options")
local waiting = api.response.stream_track("track", { format = "mp3", wait_for_transcode = true })
assert(waiting.options.wait_for_transcode == true, "a stream waits for its transcode")

local download = api.response.download_track("track", { format = "flac" })
assert(download.kind == "download_track" and download.options.format == "flac")

local hls = api.response.hls_playlist("track", { preferred_codecs = { "opus" }, channels = 2 })
assert(hls.kind == "hls_playlist" and hls.options.preferred_codecs[1] == "opus")

refuses(
	"an unknown option",
	"stream_track options: unknown field 'container'; expected format, preferred_codecs, bitrate_bps, sample_rate_hz, channels, bit_depth, prefer_vbr, start_offset_ms, wait_for_transcode, public",
	function()
		return api.response.stream_track("track", { container = "mp3" } :: any)
	end
)
refuses("a format delivery has no name for", `stream_track options.format must be one of "mp3", "flac"`, function()
	return api.response.stream_track("track", { format = "mp4" } :: any)
end)
refuses(
	"a codec that isn't a string",
	"download_track options.preferred_codecs[2] must be a string, got number",
	function()
		return api.response.download_track("track", { preferred_codecs = { "aac", 3 } } :: any)
	end
)
refuses("codecs that aren't an array", "stream_track options.preferred_codecs must be an array of codecs", function()
	return api.response.stream_track("track", { preferred_codecs = { first = "aac" } } :: any)
end)
refuses("options that are an array", "stream_track options: unknown field", function()
	return api.response.stream_track("track", { "mp3" } :: any)
end)
refuses("options that aren't a table", "stream_track options must be a table, got string", function()
	return api.response.stream_track("track", "mp3" :: any)
end)
refuses("a codec delivery has no name for", `got "ac3"`, function()
	return api.response.stream_track("track", { preferred_codecs = { "ac3" } } :: any)
end)
refuses("a non-boolean flag", "stream_track options.public must be a boolean, got string", function()
	return api.response.stream_track("track", { public = "yes" } :: any)
end)
refuses("a non-boolean wait", "stream_track options.wait_for_transcode must be a boolean, got string", function()
	return api.response.stream_track("track", { wait_for_transcode = "yes" } :: any)
end)
refuses("a download waiting for its transcode", "download_track options: unknown field 'wait_for_transcode'", function()
	return api.response.download_track("track", { wait_for_transcode = true } :: any)
end)
refuses("a public download", "download_track options: unknown field 'public'", function()
	return api.response.download_track("track", { public = true } :: any)
end)
refuses("a format for a playlist", "hls_playlist options: unknown field 'format'", function()
	return api.response.hls_playlist("track", { format = "mp3" } :: any)
end)
refuses("a public playlist", "hls_playlist options: unknown field 'public'", function()
	return api.response.hls_playlist("track", { public = true } :: any)
end)
refuses("decide's options", "decide options: unknown field 'container'", function()
	return playback_sources.decide("track", { container = "mp3" } :: any)
end)

-- A response Lyra serves takes a body-only fallback, sent in place of Lyra's error.
local refused = api.response.text(200, "refused", { ["content-type"] = "text/xml" })
local guarded = api.response.with_fallback(stream, refused)
assert(guarded.kind == "stream_track" and guarded.track_id == "track" and guarded.options.public == true)
assert(guarded == stream and guarded.fallback == refused, "the fallback is set on the response")
for _, response in
	{
		api.response.download_track("track"),
		api.response.hls_playlist("track"),
		api.response.cover("cover"),
	} :: { api.ApiResponse }
do
	assert(api.response.with_fallback(response, api.response.empty(404)).fallback ~= nil, response.kind)
end
for _, fallback in { api.response.json(503, {}), api.response.bytes(500, "x") } do
	assert(api.response.with_fallback(download, fallback).fallback == fallback, fallback.kind)
end

refuses(
	"a fallback for a body the plugin builds",
	"takes a cover, stream_track, download_track or hls_playlist response, got text",
	function()
		return api.response.with_fallback(refused, api.response.empty(404))
	end
)
refuses("a fallback Lyra serves", "fallback must be a json, empty, text or bytes response, got cover", function()
	return api.response.with_fallback(stream, api.response.cover("cover"))
end)
refuses("a redirect fallback", "got redirect", function()
	return api.response.with_fallback(stream, api.response.redirect(302, "/elsewhere"))
end)
refuses("a fallback that isn't a response", "fallback must be a response built by api.response", function()
	return api.response.with_fallback(stream, { kind = "bogus" } :: any)
end)
