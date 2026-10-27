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
-- Upstream's profiles cap direct play at 8 Mbps unless they say otherwise, and count a source of
-- unknown bitrate as 40 Mbps, so most tests play the song at a known bitrate.
local known = described({ bitrate_bps = 96000 })

local function media_source(request: any, song: any?): any
	local played = song or known
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

-- A profile without a maximum streaming bitrate takes upstream's default of 8 Mbps, and one
-- without a music transcoding bitrate transcodes at 128 kbps.
assert(not media_source({ DeviceProfile = { DirectPlayProfiles = ogg } }, unmeasured).SupportsDirectPlay, "40 Mbps")
assert(
	not media_source({ DeviceProfile = { DirectPlayProfiles = ogg } }, described({ bitrate_bps = 8000001 })).SupportsDirectPlay
)
assert(
	media_source({ DeviceProfile = { DirectPlayProfiles = ogg } }, described({ bitrate_bps = 8000000 })).SupportsDirectPlay
)
local original = media_source({ MaxStreamingBitrate = 100000000, DeviceProfile = { TranscodingProfiles = opus } })
contains(original.TranscodingUrl, "AudioBitrate=128000&")
local nulls = media_source({
	DeviceProfile = { MaxStreamingBitrate = nil, MusicStreamingTranscodingBitrate = 320000, TranscodingProfiles = opus },
})
contains(nulls.TranscodingUrl, "AudioBitrate=320000&")
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

-- Upstream names the first audio stream the default, until a device profile's stream, which names
-- none for audio, replaces it.
assert(media_source(nil).DefaultAudioStreamIndex == 0, "the first audio stream")
assert(media_source({ DeviceProfile = { DirectPlayProfiles = ogg } }).DefaultAudioStreamIndex == nil, "a profile's")
assert(
	media_source({ EnableTranscoding = false, DeviceProfile = { TranscodingProfiles = opus } }).DefaultAudioStreamIndex
		== 0,
	"no stream, no change"
)

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
assert(not string.find(unencodable_url, "AudioCodec=", 1, true), unencodable_url)
assert(not string.find(unencodable_url, "AudioBitrate=", 1, true), unencodable_url)
contains(unencodable_url, "EnableAudioVbrEncoding=false")

-- Codec profiles limit what direct plays, and the transcode carries their limits.
local function limited(codec_profiles: any, song: any?): any
	return media_source({
		DeviceProfile = {
			MusicStreamingTranscodingBitrate = 192000,
			DirectPlayProfiles = ogg,
			TranscodingProfiles = {
				{
					Type = "Audio",
					Context = "Streaming",
					Container = "opus",
					AudioCodec = "opus",
					MaxAudioChannels = "6",
				},
			},
			CodecProfiles = codec_profiles,
		},
	}, song or surround)
end
local function audio_profile(conditions: any, extra: any?): any
	local profile = { Type = "Audio", Conditions = conditions }
	for key, value in extra or {} do
		profile[key] = value
	end
	return profile
end
local stereo = { Condition = "LessThanEqual", Property = "AudioChannels", Value = "2", IsRequired = false }

local downmixed = limited({ audio_profile({ stereo }) })
assert(not downmixed.SupportsDirectPlay, "six channels exceed the codec profile")
contains(downmixed.TranscodingUrl, "TranscodingMaxAudioChannels=6")
local downmixed_tail = "&audiochannels=2&TranscodeReasons=AudioChannelsNotSupported"
assert(string.sub(downmixed.TranscodingUrl, -#downmixed_tail) == downmixed_tail, downmixed.TranscodingUrl)
assert(
	limited({ audio_profile({ stereo }) }, described({ channel_count = 2, bitrate_bps = 96000 })).SupportsDirectPlay,
	"stereo plays"
)
assert(limited({ audio_profile({ stereo }, { Codec = "flac" }) }).SupportsDirectPlay, "a profile for another codec")
assert(
	limited({ audio_profile({ stereo }, { Container = "-ogg" }) }).SupportsDirectPlay,
	"a profile for other containers"
)
assert(not limited({ audio_profile({ stereo }, { Container = "ogg", Codec = "opus,flac" }) }).SupportsDirectPlay)
assert(limited({ { Type = "VideoAudio", Conditions = { stereo } } }).SupportsDirectPlay, "a video's audio profile")

-- Apply conditions decide whether a profile applies at all, judged on the source.
local hires = { Condition = "GreaterThanEqual", Property = "AudioSampleRate", Value = "48000" }
local at_most_cd = { Condition = "LessThanEqual", Property = "AudioSampleRate", Value = "44100" }
local resampled = limited({ audio_profile({ at_most_cd }, { ApplyConditions = { hires } }) })
assert(not resampled.SupportsDirectPlay, "48 kHz exceeds 44.1 kHz")
contains(resampled.TranscodingUrl, "&AudioBitrate=192000&AudioSampleRate=44100&")
local cd = described({ channel_count = 6, sample_rate_hz = 44100, bitrate_bps = 320000 })
assert(limited({ audio_profile({ stereo }, { ApplyConditions = { hires } }) }, cd).SupportsDirectPlay, "not hi-res")

-- An unknown value fails only a required condition, which conditions are unless they say not.
assert(limited({ audio_profile({ stereo }) }, known).SupportsDirectPlay, "unknown channels")
local required = { Condition = "LessThanEqual", Property = "AudioChannels", Value = "2" }
assert(not limited({ audio_profile({ required }) }, known).SupportsDirectPlay, "unknown channels, required")
local any_rate = { Condition = "EqualsAny", Property = "AudioSampleRate", Value = "44100|48000" }
assert(limited({ audio_profile({ any_rate }) }).SupportsDirectPlay, "48 kHz is one of them")
assert(
	not limited({ audio_profile({ { Condition = "NotEquals", Property = 22, Value = "48000" } }) }).SupportsDirectPlay
)

-- A transcode takes the lowest bitrate, and an exact channel count over any earlier limit.
local capped_bitrate = limited({
	audio_profile({ { Condition = "LessThanEqual", Property = "AudioBitrate", Value = "128000" } }),
	audio_profile({ { Condition = "Equals", Property = "AudioChannels", Value = "1" } }),
})
contains(capped_bitrate.TranscodingUrl, "AudioBitrate=128000&")
local capped_tail = "&audiochannels=1&TranscodeReasons=AudioChannelsNotSupported,AudioBitrateNotSupported"
assert(string.sub(capped_bitrate.TranscodingUrl, -#capped_tail) == capped_tail, capped_bitrate.TranscodingUrl)

-- Upstream refuses a condition it can't read, and one on anything but audio once it applies.
local function refused_with(codec_profiles: any): string?
	local request = { DeviceProfile = { DirectPlayProfiles = ogg, CodecProfiles = codec_profiles } }
	local info, err = playback.build_playback_info(surround.id, surround, "session", request, nil, "secret")
	assert(err == nil or info == nil, "an error comes alone")
	return err
end
assert(refused_with({ audio_profile({ { Condition = "Below", Property = "AudioChannels", Value = "2" } }) }))
assert(refused_with({ audio_profile({ { Property = "Loudness", Value = "2" } }) }))
assert(refused_with({ { Type = "Sound" } }))
assert(refused_with({ { Type = "Video", Conditions = { { Property = "VideoRotation", Value = "90" } } } }) == nil)
assert(refused_with({ audio_profile({ { Condition = "LessThanEqual", Property = "Width", Value = "2" } }) }))
assert(
	refused_with({ audio_profile({ { Property = "Width", Value = "2" } }, { Codec = "flac" }) }) == nil,
	"never applies"
)

-- A track cut from a longer file can't be handed over as the file, so clients transcode it.
local cued = playback.resolve_track_for_playback(fixture.ids.cued)
assert(cued ~= nil, "owner sees the cued track")
local cued_source = media_source(nil, cued)
assert(not cued_source.SupportsDirectPlay and not cued_source.SupportsDirectStream, "not the file")
assert(cued_source.SupportsTranscoding, "transcodes")
assert(cued_source.Size == nil, `size {cued_source.Size}`)
-- Without a profile to transcode by, the stream route's default encode.
contains(cued_source.TranscodingUrl, `/audio/{cued_source.Id}/stream?`)
assert(cued_source.TranscodingSubProtocol == "http", `protocol {cued_source.TranscodingSubProtocol}`)
local unstreamable = media_source({ DeviceProfile = { DirectPlayProfiles = ogg, TranscodingProfiles = {} } }, cued)
assert(unstreamable.SupportsTranscoding and unstreamable.TranscodingUrl ~= nil, "a profile with no streaming one")
contains(unstreamable.TranscodingUrl, `/audio/{unstreamable.Id}/stream?`)
local refusing = media_source({ EnableTranscoding = false, DeviceProfile = { DirectPlayProfiles = ogg } }, cued)
assert(not refusing.SupportsTranscoding and refusing.TranscodingUrl == nil, "transcoding turned off")
local cued_transcode = media_source({ DeviceProfile = { DirectPlayProfiles = ogg, TranscodingProfiles = opus } }, cued)
assert(not cued_transcode.SupportsDirectPlay, "the client could play the file, not the track")
contains(cued_transcode.TranscodingUrl, `/audio/{cued_transcode.Id}/stream.opus?`)
local universal_source = playback.build_audio_media_source(cued.id, cued, false)
assert(not playback.audio_supports_direct_play_from_container_csv(universal_source, "ogg"), "universal transcodes too")
assert(media_source(nil).Size ~= nil, "an uncut track has its file's size")

-- The TranscodingUrl carries what upstream's `StreamInfo.ToUrl` does, in its order.
local function url_for(request: any, song: any?, device_id: string?): string
	local played = song or known
	local info = playback.build_playback_info(played.id, played, "session", request, nil, "secret", device_id)
	return info.MediaSources[1].TranscodingUrl
end
local full = url_for({
	StartTimeTicks = 50000000,
	DeviceProfile = {
		Id = "6F9619FF-8B86-D011-B42D-00C04FC964FF",
		DirectPlayProfiles = { { Type = "Audio", Container = "mp3" } },
		TranscodingProfiles = {
			{
				Type = "Audio",
				Container = "opus",
				AudioCodec = "opus",
				MaxAudioChannels = "2",
				EstimateContentLength = true,
				CopyTimestamps = true,
				TranscodeSeekInfo = "Bytes",
				EnableAudioVbrEncoding = false,
			},
		},
	},
}, nil, "phone")
local expected = "/audio/{id}/stream.opus?DeviceProfileId=6f9619ff8b86d011b42d00c04fc964ff&DeviceId=phone"
	.. "&MediaSourceId={id}&AudioCodec=opus&AudioBitrate=128000&StartTimeTicks=50000000&PlaySessionId=session"
	.. "&ApiKey=secret&TranscodingMaxAudioChannels=2&EstimateContentLength=True&TranscodeSeekInfo=Bytes"
	.. "&CopyTimestamps=True&RequireAvc=false&EnableAudioVbrEncoding=false"
local id = string.match(full, "^/audio/(%x+)/")
assert(id, full)
expected = string.gsub(expected, "{id}", id)
local tag = string.match(full, "&Tag=([^&]*)")
local without_tag = string.gsub(full, "&Tag=[^&]*", "")
assert(
	without_tag == expected .. "&TranscodeReasons=ContainerNotSupported,VideoCodecNotSupported,AudioCodecNotSupported",
	full
)
assert(tag == nil or tag == media_source(nil).ETag, `tag {tag}`)

local segmented = url_for({
	DeviceProfile = {
		TranscodingProfiles = {
			{
				Type = "Audio",
				Protocol = "hls",
				Container = "ts",
				AudioCodec = "aac",
				SegmentLength = 6,
				MinSegments = 2,
			},
		},
	},
})
contains(segmented, "&SegmentContainer=ts&SegmentLength=6&MinSegments=2&PlaySessionId=")

-- Upstream's transcode reasons for audio: an unsupported codec in a supported container, a bitrate
-- over the limit, and a codec the client takes in another container.
local codec_only = url_for({
	DeviceProfile = {
		DirectPlayProfiles = { { Type = "Audio", Container = "ogg", AudioCodec = "vorbis" } },
		TranscodingProfiles = opus,
	},
})
assert(
	string.sub(codec_only, -#"&TranscodeReasons=AudioCodecNotSupported") == "&TranscodeReasons=AudioCodecNotSupported"
)
local over =
	url_for({ MaxStreamingBitrate = 64000, DeviceProfile = { DirectPlayProfiles = ogg, TranscodingProfiles = opus } })
contains(over, "&TranscodeReasons=ContainerBitrateExceedsLimit")
local remux = url_for({
	DeviceProfile = { DirectPlayProfiles = { { Type = "Audio", Container = "opus" } }, TranscodingProfiles = opus },
})
assert(
	string.sub(remux, -#"&TranscodeReasons=ContainerNotSupported") == "&TranscodeReasons=ContainerNotSupported",
	remux
)
