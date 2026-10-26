local fixture = ...

local playback = require("../../protocol/items/playback")

local track = playback.resolve_track_for_playback(fixture.ids.song)
assert(track ~= nil, "owner sees the song")

-- The song with its source's audio properties as a scan might have stored them, in place of the
-- file's; a property left out is unknown.
local audio_by_song: { [any]: any } = {}
local resolve_source = playback.resolve_playback_source_for_track
playback.resolve_playback_source_for_track = function(song: any, include_entry: boolean?): any
	local source = resolve_source(song, include_entry)
	local audio = audio_by_song[song]
	if source == nil or audio == nil then
		return source
	end
	local described_source = table.clone(source)
	described_source.sample_rate_hz = audio.sample_rate_hz
	described_source.channel_count = audio.channel_count
	described_source.bit_depth = audio.bit_depth
	described_source.bitrate_bps = audio.bitrate_bps
	return described_source
end
local function described(properties: any): any
	local copy = table.clone(track)
	audio_by_song[copy] = properties
	return copy
end
local surround = described({ channel_count = 6, sample_rate_hz = 48000, bitrate_bps = 320000 })

local function media_source(request: any, song: any?): any
	local played = song or track
	local info = playback.build_playback_info(played.id, played, "session", request, nil, "secret")
	assert(info.ErrorCode == nil, `error {info.ErrorCode}`)
	local source = info.MediaSources[1]
	assert(source ~= nil, "the song's source is listed")
	return source
end

local function plays(direct_play_profiles: any): boolean
	return media_source({ DeviceProfile = { DirectPlayProfiles = direct_play_profiles, TranscodingProfiles = {} } }).SupportsDirectPlay
end

local function contains(url: string, part: string)
	assert(string.find(url, part, 1, true), `{part} missing from {url}`)
end

local ogg = { { Type = "Audio", Container = "ogg" } }
local opus = { { Type = "Audio", Context = "Streaming", Protocol = "http", Container = "opus", AudioCodec = "opus" } }

-- Direct-play profiles match containers and codecs as upstream's ContainerHelper does.
assert(plays({ { Type = "Audio", Container = "ogg", AudioCodec = "opus" } }), "ogg/opus")
assert(plays({ { Type = "Audio", Container = "-mp3" } }), "anything but mp3")
assert(not plays({ { Type = "Audio", Container = "-mp3,ogg" } }), "anything but mp3 or ogg")
assert(plays({ { Type = "Audio", Container = "ogg", AudioCodec = "-flac" } }), "any codec but flac")
assert(not plays({ { Type = "Audio", Container = "ogg", AudioCodec = "vorbis,flac" } }), "only vorbis or flac")
assert(plays({ { Container = "ogg" } }), "a profile without a type is an audio one")
assert(not plays({ { Type = "Video", Container = "ogg" } }), "a video profile")

-- Upstream leaves the request's MaxAudioChannels out of direct play.
local capped = media_source({ MaxAudioChannels = 2, DeviceProfile = { DirectPlayProfiles = ogg } }, surround)
assert(capped.SupportsDirectPlay, "six channels direct play under a two-channel request")

-- A source of unknown bitrate counts as 40 Mbps against the maximum streaming bitrate.
local unmeasured = described({})
local unknown = media_source({
	MaxStreamingBitrate = 192000,
	DeviceProfile = { DirectPlayProfiles = ogg, TranscodingProfiles = opus },
}, unmeasured)
assert(not unknown.SupportsDirectPlay and unknown.TranscodingUrl ~= nil, "an unknown bitrate exceeds 192 kbps")
assert(
	media_source({ MaxStreamingBitrate = 40000000, DeviceProfile = { DirectPlayProfiles = ogg } }, unmeasured).SupportsDirectPlay
)
assert(
	media_source({ DeviceProfile = { MaxStreamingBitrate = 320000, DirectPlayProfiles = ogg } }, surround).SupportsDirectPlay,
	"the profile's maximum applies without the request's"
)
assert(
	not media_source({ DeviceProfile = { MaxStreamingBitrate = 319999, DirectPlayProfiles = ogg } }, surround).SupportsDirectPlay
)

-- Turning direct play off transcodes a source the client could play.
local refused =
	media_source({ EnableDirectPlay = false, DeviceProfile = { DirectPlayProfiles = ogg, TranscodingProfiles = opus } })
assert(not refused.SupportsDirectPlay and not refused.SupportsDirectStream, "direct play is off")
contains(refused.TranscodingUrl, "/stream.opus?")

-- Without a device profile, upstream tailors nothing to the request.
local bare = media_source({ EnableDirectPlay = false, MaxStreamingBitrate = 1 })
assert(bare.SupportsDirectPlay and bare.SupportsTranscoding and bare.TranscodingUrl == nil, "an untailored source")

-- Upstream finds no stream when transcoding is off and the client can't play the source, and
-- leaves the source's flags as they were.
local untouched =
	media_source({ EnableTranscoding = false, DeviceProfile = { DirectPlayProfiles = {}, TranscodingProfiles = opus } })
assert(untouched.SupportsDirectPlay and not untouched.SupportsTranscoding, "the source's own flags")
assert(untouched.TranscodingUrl == nil, `url {untouched.TranscodingUrl}`)

-- A client whose streaming profiles name nothing the server encodes still transcodes, to
-- whatever the stream route picks.
local unencodable = media_source({
	DeviceProfile = {
		DirectPlayProfiles = {},
		TranscodingProfiles = { { Type = "Audio", Container = "ogg", AudioCodec = "vorbis" } },
	},
})
assert(unencodable.SupportsTranscoding, "a streaming profile")
local unencodable_url: string = unencodable.TranscodingUrl
contains(unencodable_url, `/audio/{unencodable.Id}/stream?MediaSourceId=`)
assert(not string.find(unencodable_url, "AudioCodec", 1, true), unencodable_url)
assert(not string.find(unencodable_url, "AudioBitrate", 1, true), unencodable_url)
contains(unencodable_url, "EnableAudioVbrEncoding=false")
