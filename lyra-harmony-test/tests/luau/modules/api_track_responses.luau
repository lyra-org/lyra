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

local download = api.response.download_track("track", { format = "flac" })
assert(download.kind == "download_track" and download.options.format == "flac")

local hls = api.response.hls_playlist("track", { preferred_codecs = { "opus" }, channels = 2 })
assert(hls.kind == "hls_playlist" and hls.options.preferred_codecs[1] == "opus")

refuses(
	"an unknown option",
	"stream_track options: unknown field 'container'; expected format, preferred_codecs, bitrate_bps, sample_rate_hz, channels, prefer_vbr, start_offset_ms, public",
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
