local encoder = require("../../protocol/items/encoder")

local function settings(
	query: any,
	request: any,
	codecs: { string },
	bitrate: number?,
	input: number?,
	hls: boolean?
): any
	return encoder.settings(query, request, codecs, bitrate, input, hls == true)
end

-- Upstream caps lossy encodes at 128 kbps a channel, or 640 kbps for surround.
assert(settings(nil, {}, { "opus" }, nil, 2).bitrate_bps == 256000, "stereo opus defaults to 256 kbps")
assert(settings(nil, {}, { "aac" }, 320000, 1).bitrate_bps == 128000, "mono aac")
assert(settings(nil, {}, { "mp3" }, 96000, 2).bitrate_bps == 96000, "under the cap")
assert(settings(nil, {}, { "aac" }, nil, 6).bitrate_bps == 640000, "surround")
assert(settings(nil, {}, { "aac" }, nil, nil).bitrate_bps == 384000, "unknown channels")
assert(settings(nil, { max_audio_channels = 2 }, { "aac" }, nil, 6).bitrate_bps == 256000, "downmixed surround")
assert(settings(nil, {}, { "flac" }, 320000, 2).bitrate_bps == nil, "lossless carries no bitrate")
assert(settings(nil, {}, { "vorbis", "opus" }, nil, 2).bitrate_bps == 256000, "the first codec upstream encodes")

-- The output channels: a codec profile's stream option first, then the parameters in upstream's
-- order, never above the source, the encoder, or the transcoding maximum.
assert(settings(nil, {}, { "aac" }, nil, 6).channels == 6, "the source's channels")
assert(settings(nil, {}, { "mp3" }, nil, 6).channels == 2, "lame encodes stereo at most")
assert(settings(nil, { max_audio_channels = 6, audio_channels = 2 }, { "aac" }, nil, 6).channels == 6)
assert(settings(nil, { audio_channels = 2, transcoding_max_audio_channels = 6 }, { "aac" }, nil, 6).channels == 2)
assert(settings(nil, { max_audio_channels = 6, transcoding_max_audio_channels = 1 }, { "aac" }, nil, 6).channels == 1)
assert(settings(nil, { max_audio_channels = 8 }, { "aac" }, nil, 2).channels == 2, "no upmix")
local option = { audiochannels = { "1" }, ["opus-audiochannels"] = { "4" } }
assert(settings(option, { max_audio_channels = 2 }, { "aac" }, nil, 6).channels == 1, "the stream option")
assert(settings(option, { max_audio_channels = 2 }, { "opus" }, nil, 6).channels == 4, "the codec's stream option")
assert(settings({ AudioChannels = { "1" } }, {}, { "aac" }, nil, 6).channels == 6, "options go by their exact name")
assert(settings(nil, { max_audio_channels = 4 }, { "aac" }, nil, 6, true).channels == 2, "HLS avoids four channels")
assert(settings(nil, { max_audio_channels = 5 }, { "aac" }, nil, 6, true).channels == 6, "HLS makes five 5.1")
assert(settings(nil, { max_audio_channels = 5 }, { "aac" }, nil, 6).channels == 5, "progressive keeps five")

-- A source of unknown channels gets a count only when the request names one.
assert(settings(nil, {}, { "aac" }, nil, nil).channels == nil, "nothing to downmix to")
assert(settings(nil, { transcoding_max_audio_channels = 6 }, { "aac" }, nil, nil).channels == 6)
