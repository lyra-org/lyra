--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

-- The channels and bitrate upstream's `EncodingHelper` hands its encoder for a stream request.

local api = require("@lyra/api")

local device_profiles = require("./device_profile")

type ApiQueryParams = api.ApiQueryParams

local module = {}

-- The codecs upstream finds an encoder for that the server can also encode to. Upstream looks
-- `vorbis` and `pcm_*` up by names its encoder list does not carry, so it never picks them.
local ENCODABLE_CODECS: { [string]: boolean } = {
	aac = true,
	alac = true,
	flac = true,
	mp3 = true,
	opus = true,
}

-- Upstream's per-encoder channel limits for the codecs the server encodes; others take up to 8.
local ENCODER_CHANNEL_LIMITS: { [string]: number } = {
	mp3 = 2,
}

local LOSSLESS_CODECS: { [string]: boolean } = {
	alac = true,
	ape = true,
	flac = true,
	mlp = true,
	truehd = true,
	wavpack = true,
}

-- Codecs upstream caps at 128 kbps a channel, or 640 kbps for surround.
local PER_CHANNEL_CODECS: { [string]: boolean } = {
	aac = true,
	mp3 = true,
	opus = true,
	vorbis = true,
	ac3 = true,
	eac3 = true,
}

-- The channel parameters of a stream request.
export type ChannelRequest = {
	max_audio_channels: number?,
	audio_channels: number?,
	transcoding_max_audio_channels: number?,
}

-- Upstream's `MediaEncoder.CanEncodeToAudioCodec`, for the codecs the server also encodes to.
function module.can_encode(codec: string): boolean
	return ENCODABLE_CODECS[string.lower(codec)] == true
end

-- Upstream's output codec among those a request lists: the first it can encode to, else the first.
function module.output_codec(codecs: { string }): string?
	for _, codec in codecs do
		if module.can_encode(codec) then
			return codec
		end
	end
	return codecs[1]
end

-- A stream option, which upstream reads by its exact name.
local function stream_option_int(query: ApiQueryParams?, name: string): number?
	local values = if query then query[name] else nil
	return device_profiles.parse_int(if values then values[1] else nil)
end

-- The channels a request asks for: a codec profile's `audiochannels` stream option, qualified by
-- codec or not, then the channel parameters in upstream's order.
local function requested_channels(query: ApiQueryParams?, request: ChannelRequest, codec: string?): number?
	local option = if codec and codec ~= ""
		then stream_option_int(query, `{codec}-audiochannels`) or stream_option_int(query, "audiochannels")
		else nil
	return option or request.max_audio_channels or request.audio_channels or request.transcoding_max_audio_channels
end

-- Upstream's `EncodingHelper.GetNumAudioChannelsParam`. `segmented` is an HLS encode, which
-- avoids 3 to 5 and 7 channel layouts.
local function output_channels(
	requested: number?,
	request: ChannelRequest,
	codec: string?,
	input_channels: number?,
	segmented: boolean
): number?
	local channels = requested
	if input_channels ~= nil and input_channels > 0 then
		channels = if channels ~= nil and channels < input_channels then channels else input_channels
	end
	if codec == "copy" then
		return channels
	end

	local limit = ENCODER_CHANNEL_LIMITS[codec or ""] or 8
	channels = math.min(limit, channels or limit)
	local transcoding_max = request.transcoding_max_audio_channels
	if transcoding_max ~= nil and transcoding_max < channels then
		channels = transcoding_max
	end
	if segmented and ((channels > 2 and channels < 6) or channels == 7) then
		channels = if channels == 5 then 6 elseif channels == 7 then 8 else 2
	end
	return channels
end

-- Upstream's `EncodingHelper.GetAudioBitrateParam`: the request's bitrate, capped by the codec's
-- per-channel default. A lossless encode carries no bitrate.
local function output_bitrate(
	requested_bps: number?,
	codec: string?,
	input_channels: number?,
	output_channels: number?
): number?
	local name = codec or ""
	if LOSSLESS_CODECS[name] then
		return nil
	end
	if name ~= "" and not PER_CHANNEL_CODECS[name] then
		return 128000 * (output_channels or input_channels or 2)
	end

	local bitrate = requested_bps or 2147483647
	local input = input_channels or 0
	local output = output_channels or 0
	if input >= 6 and (output >= 6 or output == 0) then
		return math.min(640000, bitrate)
	elseif input > 0 and output > 0 then
		return math.min(output * 128000, bitrate)
	elseif input > 0 then
		return math.min(input * 128000, bitrate)
	end
	return math.min(384000, bitrate)
end

export type EncoderSettings = {
	channels: number?,
	bitrate_bps: number?,
}

-- The channels and bitrate to encode a stream request at. The server never upmixes a source of
-- known channels, but takes a count it can't check against the source as asked, so a source of
-- unknown channels gets a count only when the request names one.
function module.settings(
	query: ApiQueryParams?,
	request: ChannelRequest,
	codecs: { string },
	requested_bps: number?,
	input_channels: number?,
	segmented: boolean
): EncoderSettings
	local codec = module.output_codec(codecs)
	local requested = requested_channels(query, request, codec)
	local channels = output_channels(requested, request, codec, input_channels, segmented)
	local known_input = input_channels ~= nil and input_channels > 0
	return {
		channels = if known_input or requested ~= nil then channels else nil,
		bitrate_bps = output_bitrate(requested_bps, codec, input_channels, channels),
	}
end

return module
