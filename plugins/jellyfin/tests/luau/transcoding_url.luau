local fixture = ...

local playback = require("../../protocol/items/playback")

local track = playback.resolve_track_for_playback(fixture.ids.song)
assert(track ~= nil, "owner sees the song")

local hls_only = {
	DeviceProfile = {
		DirectPlayProfiles = {},
		TranscodingProfiles = {
			{ Type = "Audio", Context = "Streaming", Protocol = "hls", AudioCodec = "aac", Container = "ts" },
		},
	},
}

local function transcoding_url(access_token: string?): string
	local info = playback.build_playback_info(track.id, track, "session", hls_only :: any, nil, access_token) :: any
	local url = info.MediaSources[1].TranscodingUrl
	assert(type(url) == "string", `no TranscodingUrl: {info.ErrorCode}`)
	return url
end

-- As upstream, the URL carries the request's token so players can fetch it without headers.
local url = transcoding_url("secret")
-- Relative to the Jellyfin API root, as upstream; clients prepend their own base path.
assert(string.sub(url, 1, #"/audio/") == "/audio/", url)
assert(string.find(url, "/master.m3u8?", 1, true), url)
assert(string.find(url, "&ApiKey=secret", 1, true), url)
assert(not string.find(transcoding_url(nil), "ApiKey", 1, true), "no token, no ApiKey")
