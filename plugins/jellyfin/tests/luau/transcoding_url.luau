local fixture = ...

local audio = require("../../routes/audio")
local playback = require("../../protocol/items/playback")

local track = playback.resolve_track_for_playback(fixture.ids.song)
assert(track ~= nil, "owner sees the song")

local function audio_profile(container: string, codec: string, protocol: string)
	return {
		Type = "Audio",
		Context = "Streaming",
		Protocol = protocol,
		Container = container,
		AudioCodec = codec,
		MaxAudioChannels = "6",
	}
end

-- Jellify's profiles at its medium quality: the lossless direct-play profiles are dropped.
local android = {
	DeviceProfile = {
		MaxStreamingBitrate = 192000,
		MusicStreamingTranscodingBitrate = 192000,
		DirectPlayProfiles = { { Type = "Audio", Container = "mp3" } },
		TranscodingProfiles = { audio_profile("opus", "opus", "http") },
	},
	MaxStreamingBitrate = 192000,
}
local apple = {
	DeviceProfile = {
		MaxStreamingBitrate = 192000,
		MusicStreamingTranscodingBitrate = 192000,
		DirectPlayProfiles = { { Type = "Audio", Container = "aac" }, { Type = "Audio", Container = "mp3" } },
		TranscodingProfiles = { audio_profile("aac", "aac", "http") },
	},
	MaxStreamingBitrate = 192000,
}
local hls = {
	DeviceProfile = {
		DirectPlayProfiles = {},
		TranscodingProfiles = { audio_profile("ts", "aac", "hls") },
	},
}

local function playback_info(request: any, access_token: string?): any
	return playback.build_playback_info(track.id, track, "session", request, nil, access_token)
end

local function media_source(request: any, access_token: string?): any
	local info = playback_info(request, access_token)
	assert(info.ErrorCode == nil, `error {info.ErrorCode}`)
	assert(info.PlaySessionId == "session", `play session {info.PlaySessionId}`)
	local source = info.MediaSources[1]
	assert(source ~= nil, "the song's source is listed")
	return source
end

local function contains(url: string, part: string)
	assert(string.find(url, part, 1, true), `{part} missing from {url}`)
end

-- As upstream, URLs are relative to the API root, which clients prepend, and carry the request's
-- token so players can fetch them without headers.
local opus = media_source(android, "secret")
assert(not opus.SupportsDirectPlay and not opus.SupportsDirectStream, "the client cannot play ogg")
assert(opus.SupportsTranscoding, "the client transcodes")
assert(opus.TranscodingSubProtocol == "http", `protocol {opus.TranscodingSubProtocol}`)
assert(opus.TranscodingContainer == "opus", `container {opus.TranscodingContainer}`)
local opus_url: string = opus.TranscodingUrl
assert(string.sub(opus_url, 1, #"/audio/") == "/audio/", opus_url)
contains(opus_url, `/audio/{opus.Id}/stream.opus?`)
contains(opus_url, "AudioCodec=opus")
contains(opus_url, "AudioBitrate=192000")
contains(opus_url, "PlaySessionId=session")
contains(opus_url, "ApiKey=secret")
contains(opus_url, "TranscodingMaxAudioChannels=6")

assert(not string.find(opus_url, "StreamCopy", 1, true), opus_url)
local no_copy_url: string = media_source({
	DeviceProfile = android.DeviceProfile,
	AllowAudioStreamCopy = false,
}, "secret").TranscodingUrl
assert(string.sub(no_copy_url, -#"&allowAudioStreamCopy=false") == "&allowAudioStreamCopy=false", no_copy_url)

local aac_url: string = media_source(apple, "secret").TranscodingUrl
contains(aac_url, "/stream.aac?")
contains(aac_url, "AudioCodec=aac")
contains(aac_url, "ApiKey=secret")

local hls_source = media_source(hls, "secret")
assert(hls_source.TranscodingSubProtocol == "hls", `protocol {hls_source.TranscodingSubProtocol}`)
local hls_url: string = hls_source.TranscodingUrl
assert(string.sub(hls_url, 1, #"/audio/") == "/audio/", hls_url)
contains(hls_url, "/master.m3u8?")
contains(hls_url, "SegmentContainer=ts")
contains(hls_url, "ApiKey=secret")
assert(not string.find(media_source(hls, nil).TranscodingUrl, "ApiKey", 1, true), "no token, no ApiKey")

-- Upstream plays an audio item's default stream whatever stream index the client asks for.
local indexed = playback_info({ AudioStreamIndex = 3 }, "secret")
assert(indexed.ErrorCode == nil and indexed.MediaSources[1] ~= nil, `error {indexed.ErrorCode}`)

-- As upstream, profiles naming no codec, or one upstream finds no encoder for, are passed over.
local passed_over = media_source({
	DeviceProfile = {
		DirectPlayProfiles = {},
		TranscodingProfiles = {
			{ Type = "Audio", Context = "Streaming", Protocol = "http", Container = "mp3" },
			audio_profile("ogg", "vorbis", "http"),
			audio_profile("wav", "pcm_s16le", "http"),
			audio_profile("mp3", "mp3", "http"),
		},
	},
}, "secret")
contains(passed_over.TranscodingUrl, "/stream.mp3?")
contains(passed_over.TranscodingUrl, "AudioCodec=mp3")

-- A client that can neither play nor transcode the source still gets it, with every flag off.
local stuck = media_source({ DeviceProfile = { DirectPlayProfiles = {}, TranscodingProfiles = {} } }, "secret")
assert(not stuck.SupportsDirectPlay and not stuck.SupportsTranscoding, "nothing applies")
assert(stuck.TranscodingUrl == nil, `url {stuck.TranscodingUrl}`)

local direct = media_source({
	DeviceProfile = {
		DirectPlayProfiles = { { Type = "Audio", Container = "ogg" } },
		TranscodingProfiles = { audio_profile("opus", "opus", "http") },
	},
}, "secret")
assert(direct.SupportsDirectPlay, "the client plays ogg")
assert(direct.TranscodingUrl == nil, `url {direct.TranscodingUrl}`)
assert(direct.SupportsTranscoding, "a client with a streaming profile transcodes even when it plays")
assert(media_source(nil, "secret").SupportsTranscoding, "a source transcodes when no profile says otherwise")
assert(
	not media_source({ EnableTranscoding = false, DeviceProfile = android.DeviceProfile }, "secret").SupportsTranscoding,
	"transcoding off"
)

-- The stream route answers the URL it was handed.
local function stream(url: string, method: string?): any
	local path, raw_query = string.match(url, "^([^?]*)%??(.*)$")
	assert(path and raw_query, url)
	local item_id, file = string.match(path, "^/audio/([^/]+)/([^/]+)$")
	assert(item_id and file, path)
	local query = {}
	for key, value in string.gmatch(raw_query, "([^&=]+)=([^&]*)") do
		query[key] = { value }
	end
	return audio.handle_stream_route({
		plugin_id = "jellyfin",
		request = { method = method or "GET", path = path, headers = {}, query = query, body_raw = "" },
		params = { itemId = item_id, container = string.match(file, "^stream%.(.+)$") },
	} :: any)
end

local function stream_options(url: string, method: string?): any
	local response = stream(url, method)
	assert(response.kind == "stream_track", `{url}: kind {response.kind} status {response.status}`)
	assert(response.track_id == fixture.ids.song, `{url}: track {response.track_id}`)
	local options = response.options
	assert(options and options.public == true, `{url}: not public`)
	return options
end

local function codecs(options: any): string
	return table.concat(options.preferred_codecs or {}, ",")
end

-- As upstream, a transcode encodes even when the source already has the client's codec.
local opus_options = stream_options(opus_url)
assert(opus_options.format == "opus", `format {opus_options.format}`)
assert(codecs(opus_options) == "opus", `codecs {codecs(opus_options)}`)
assert(opus_options.bitrate_bps == 192000, `bitrate {opus_options.bitrate_bps}`)
assert(opus_options.channels == 6, `channels {opus_options.channels}`)
assert(opus_options.prefer_vbr == nil, "upstream's default encoding options use a constant bitrate")

local aac_options = stream_options(aac_url)
assert(aac_options.format == "aac", `format {aac_options.format}`)
assert(codecs(aac_options) == "aac", `codecs {codecs(aac_options)}`)

local head_options = stream_options(aac_url, "HEAD")
assert(head_options.format == "aac" and codecs(head_options) == "aac", "HEAD answers as GET does")

-- `static=true` sends the original file whatever else the request asks.
local static_options = stream_options(`/audio/{opus.Id}/stream.mp3?static=true&audioCodec=mp3`)
assert(static_options.format == nil and static_options.preferred_codecs == nil, "the original file")

-- Without a container or codec, upstream transcodes to AAC; a container alone names its codec.
local bare = stream_options(`/audio/{opus.Id}/stream`)
assert(bare.format == "aac" and codecs(bare) == "aac", `bare {bare.format} {codecs(bare)}`)
local mp3 = stream_options(`/audio/{opus.Id}/stream?container=mp3`)
assert(mp3.format == "mp3" and codecs(mp3) == "mp3", `mp3 {mp3.format} {codecs(mp3)}`)
local copy = stream_options(`/audio/{opus.Id}/stream.ogg?audioCodec=copy`)
assert(copy.format == "ogg" and codecs(copy) == "copy", `copy {codecs(copy)}`)
local start = stream_options(`/audio/{opus.Id}/stream.mp3?startTimeTicks=50000000`)
assert(start.start_offset_ms == 5000, `start {start.start_offset_ms}`)

assert(stream(`/audio/{opus.Id}/stream.mp3?audioBitRate=fast`).status == 400, "a malformed bitrate")
