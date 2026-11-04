--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")
local playback_sources = require("@lyra/playback_sources")
local tracks = require("@lyra/tracks")

local client_info = require("../protocol/client_info")
local request = require("../protocol/request")
local response = require("../protocol/response")

type ApiQueryParams = api.ApiQueryParams
type ApiResponse = api.ApiResponse
type ClientInfo = client_info.ClientInfo
type CodecProfile = client_info.CodecProfile
type DeliveryDescription = playback_sources.DeliveryDescription
type DirectPlayProfile = client_info.DirectPlayProfile
type Limitation = client_info.Limitation
type PlaybackSourceInfo = playback_sources.PlaybackSourceInfo
type Request = request.Request
type SourceDescription = playback_sources.SourceDescription
type TrackStreamOptions = api.TrackStreamOptions
type TranscodingProfile = client_info.TranscodingProfile

local module = {}

-- Delivery formats for container names `api.delivery_format` doesn't know.
local FORMATS: { [string]: string } = { mpeg = "mp3", adts = "aac", aif = "aiff", asf = "wma", wave = "wav" }

local DELIVERY_CODECS: { [string]: boolean } = {
	mp3 = true,
	aac = true,
	alac = true,
	flac = true,
	opus = true,
	vorbis = true,
	wma = true,
	pcm_s16le = true,
	pcm_s16be = true,
	pcm_s24le = true,
	pcm_s24be = true,
}

-- Codec names that ask for PCM in a WAV or AIFF container at the source's depth.
local PCM_NAMES: { [string]: boolean } = { [""] = true, pcm = true, wav = true, aiff = true }

local LOSSLESS: { [string]: boolean } = { flac = true, alac = true, ape = true, wavpack = true }

local REASONS: { [string]: string } = {
	audioChannels = "AudioChannelsNotSupported",
	audioBitrate = "AudioBitrateNotSupported",
	audioProfile = "AudioProfileNotSupported",
	audioSamplerate = "AudioSampleRateNotSupported",
	audioBitdepth = "AudioBitDepthNotSupported",
}

-- The fields of a delivery or source description that limitations compare.
local FIELDS: { [string]: string } = {
	audioBitrate = "bitrate_bps",
	audioSamplerate = "sample_rate_hz",
	audioChannels = "channels",
	audioBitdepth = "bit_depth",
}

-- Audio as a delivery or source description gives it.
type Audio = {
	read codec: string?,
	read bitrate_bps: number?,
	read sample_rate_hz: number?,
	read channels: number?,
	read bit_depth: number?,
}

local function is_pcm(codec: string): boolean
	return string.sub(codec, 1, 4) == "pcm_"
end

local function is_lossless(codec: string?): boolean
	return codec ~= nil and (LOSSLESS[codec] or is_pcm(codec))
end

local function lesser(a: number?, b: number?): number?
	if a and b then
		return math.min(a, b)
	end
	return a or b
end

-- Whether a client's codec name `wanted` names `codec`, Lyra's name for audio in `container`.
local function names_codec(wanted: string, codec: string?, container: string?): boolean
	if not codec then
		return false
	elseif wanted == codec then
		return true
	elseif is_pcm(codec) then
		return wanted == "pcm" or (wanted == "wav" and container == "wav") or (wanted == "aiff" and container == "aiff")
	end
	return wanted == "mpc" and (codec == "musepack7" or codec == "musepack8")
end

-- The codec profiles that apply to `codec` in `container`, or that name it `alias`, as a client did.
local function profiles_for(info: ClientInfo, codec: string?, container: string?, alias: string?): { CodecProfile }
	local applying: { CodecProfile } = {}
	for _, profile in info.codec_profiles do
		if profile.codec == alias or names_codec(profile.codec, codec, container) then
			table.insert(applying, profile)
		end
	end
	return applying
end

local function compares(limitation: Limitation, value: number): boolean
	local numbers = limitation.numbers :: { number }
	local comparison = limitation.comparison
	if comparison == "lessthanequal" then
		return value <= numbers[1]
	elseif comparison == "greaterthanequal" then
		return value >= numbers[1]
	end
	local listed = table.find(numbers, value) ~= nil
	return if comparison == "equals" then listed else not listed
end

-- Whether `audio`, at `bitrate`, meets `limitation`. Lossy audio has no bit depth, so it meets
-- every bit depth limitation; a limitation Lyra can't check is never met.
local function meets(limitation: Limitation, audio: Audio, bitrate: number?): boolean
	if not limitation.comparison then
		return false
	elseif limitation.name == "audioBitdepth" and not is_lossless(audio.codec) then
		return true
	end
	local value = if limitation.name == "audioBitrate" then bitrate else (audio :: any)[FIELDS[limitation.name]]
	return value ~= nil and compares(limitation, value)
end

-- What audio must meet: a protocol among `protocols` when any are listed, at most `max_channels`
-- and `max_bitrate`, and every required limitation of `profiles`.
type Demands = {
	protocols: { string },
	max_channels: number?,
	max_bitrate: number?,
	profiles: { CodecProfile },
}

-- Why `audio` doesn't meet `demands`, nil when it does. Lyra knows no bitrate for a lossless
-- encode, so it is taken as `lossless_bitrate`, the source's, which such an encode comes close to.
local function refusal(demands: Demands, audio: Audio, lossless_bitrate: number?): string?
	if #demands.protocols > 0 and not table.find(demands.protocols, "http") then
		return "ProtocolNotSupported"
	end
	if demands.max_channels and audio.channels and audio.channels > demands.max_channels then
		return "AudioChannelsNotSupported"
	end
	local bitrate = audio.bitrate_bps or if is_lossless(audio.codec) then lossless_bitrate else nil
	if demands.max_bitrate and bitrate and bitrate > demands.max_bitrate then
		return "ContainerBitrateExceedsLimit"
	end
	for _, profile in demands.profiles do
		for _, limitation in profile.limitations do
			if limitation.required and not meets(limitation, audio, bitrate) then
				return REASONS[limitation.name] or "LimitationNotSupported"
			end
		end
	end
	return nil
end

-- Why `source` can't be played as it is through `profile`; nil when it can.
local function direct_play_refusal(info: ClientInfo, profile: DirectPlayProfile, source: SourceDescription): string?
	if #profile.containers > 0 and not (source.container and table.find(profile.containers, source.container)) then
		return "ContainerNotSupported"
	end
	if #profile.codecs > 0 then
		local found = false
		for _, codec in profile.codecs do
			found = found or names_codec(codec, source.codec, source.container)
		end
		if not found then
			return "AudioCodecNotSupported"
		end
	end
	return refusal({
		protocols = profile.protocols,
		max_channels = profile.max_channels,
		max_bitrate = info.max_bitrate,
		profiles = profiles_for(info, source.codec, source.container, nil),
	}, source, nil)
end

-- The format and codec Lyra delivers for a transcoding profile; nil when it delivers none. A nil
-- codec is the format's own.
local function target_of(profile: TranscodingProfile, source: SourceDescription): (string?, string?)
	local container = if profile.container ~= "" then profile.container else profile.codec
	local format: string? = api.delivery_format(container) or api.delivery_format(FORMATS[container] or "")
	if not format then
		return nil, nil
	elseif (format == "wav" or format == "aiff") and PCM_NAMES[profile.codec] then
		local depth = if (source.bit_depth or 0) > 16 then "s24" else "s16"
		return format, `pcm_{depth}{if format == "wav" then "le" else "be"}`
	elseif profile.codec == "" then
		return format, nil
	elseif not DELIVERY_CODECS[profile.codec] then
		return nil, nil
	end
	return format, profile.codec
end

local function joined(values: { any }, count: number): string
	local fields: { string } = {}
	for index = 1, count do
		local value = values[index]
		fields[index] = if value == nil then "" else tostring(value)
	end
	return table.concat(fields, ".")
end

local function decide(track_id: string, options: TrackStreamOptions?): DeliveryDescription?
	local ok, delivery = pcall(playback_sources.decide, track_id, options)
	return if ok then delivery else nil
end

-- The most `decide` calls a transcoding profile may take to settle one property for its required
-- limitations, and to try its optional limitations, all properties together. Values left untried
-- count as unmet; an optional limitation never spends what a required one needs.
local REQUIRED_DECIDES = 12
local OPTIONAL_DECIDES = 8

-- The bitrate Lyra encodes a lossless source at when nothing lowers it.
local DEFAULT_BITRATE = 192000

-- Bitrates tried after the ones the client names, so one Lyra's default doesn't meet has others.
local BITRATES = { 320000, 256000, 224000, 192000, 160000, 128000, 96000, 64000, 32000 }

local SAMPLE_RATES = { 8000, 11025, 16000, 22050, 24000, 32000, 44100, 48000, 88200, 96000, 176400, 192000 }

-- A property of the audio Lyra can lower, named as limitations, options, and descriptions name
-- it, in the order a transcode settles them: Opus's highest bitrate depends on its channels.
type Property = { limitation: string, field: string }

local CHANNELS: Property = { limitation = "audioChannels", field = "channels" }
local SAMPLE_RATE: Property = { limitation = "audioSamplerate", field = "sample_rate_hz" }
local BIT_DEPTH: Property = { limitation = "audioBitdepth", field = "bit_depth" }
local BITRATE: Property = { limitation = "audioBitrate", field = "bitrate_bps" }

-- A value to ask Lyra for, nil to leave the property as Lyra delivers it, with the value that is
-- expected to give for ordering.
type Candidate = { value: number?, expected: number? }

-- The values to try for `property`, highest first: the source's, every value a limitation names,
-- the client's maximum, and the property's usual values, none above the source or the maximum.
-- Bitrates from `BITRATES` follow the others, so they never replace Lyra's default unasked, and
-- are always asked for explicitly.
local function candidates_for(
	property: Property,
	source: SourceDescription,
	maximum: number?,
	profiles: { CodecProfile }
): { Candidate }
	local current = (source :: any)[property.field] :: number?
	local values: { number } = {}
	if property == BIT_DEPTH then
		values = { 24, 16 }
	else
		for _, profile in profiles do
			for _, limitation in profile.limitations do
				if limitation.name == property.limitation and limitation.numbers then
					for _, value in limitation.numbers do
						table.insert(values, value)
					end
				end
			end
		end
		if maximum then
			table.insert(values, maximum)
		end
		if property == SAMPLE_RATE then
			for _, rate in SAMPLE_RATES do
				table.insert(values, rate)
			end
		elseif property == CHANNELS then
			for count = 1, current or maximum or 2 do
				table.insert(values, count)
			end
		end
	end

	-- A lossy source's bitrate is asked for, so Lyra encodes at it rather than its default.
	local candidates: { Candidate } = {}
	local unset: number? = current
	if property == BITRATE then
		if current and not is_lossless(source.codec) then
			table.insert(values, current)
			unset = nil
		else
			unset = DEFAULT_BITRATE
		end
	end
	local ceiling = lesser(current, maximum)
	if property ~= BITRATE or unset then
		if not (maximum and unset and unset > maximum) then
			table.insert(candidates, { value = nil, expected = unset })
		end
	end
	local seen: { [number]: boolean } = {}
	for _, value in values do
		if value > 0 and not seen[value] and value ~= unset and (ceiling == nil or value <= ceiling) then
			seen[value] = true
			table.insert(candidates, { value = value, expected = value })
		end
	end
	local function by_value(a: Candidate, b: Candidate): boolean
		return (a.expected or math.huge) > (b.expected or math.huge)
	end
	table.sort(candidates, by_value)
	if property == BITRATE then
		local ladder: { Candidate } = {}
		for _, value in BITRATES do
			if not seen[value] and (ceiling == nil or value <= ceiling) then
				table.insert(ladder, { value = value, expected = value })
			end
		end
		table.sort(ladder, by_value)
		table.move(ladder, 1, #ladder, #candidates + 1, candidates)
	end
	return candidates
end

type Transcode = { options: TrackStreamOptions, delivery: DeliveryDescription, container: string, codec: string }

-- How Lyra transcodes the track for `profile`, or nil when it can't deliver what the profile and
-- the client's limits ask for. Each property Lyra can lower is settled in turn, the others held:
-- it takes the highest value whose delivery, as `decide` reports it, meets the client's maximum
-- and the target codec's required limitations, then the highest that also meets each optional
-- limitation that still leaves one, in order.
local function transcode_for(
	track_id: string,
	info: ClientInfo,
	profile: TranscodingProfile,
	source: SourceDescription
): Transcode?
	if profile.protocol ~= "" and profile.protocol ~= "http" then
		return nil
	end
	local format, codec = target_of(profile, source)
	if not format then
		return nil
	end

	local options: TrackStreamOptions =
		{ format = format :: any, preferred_codecs = if codec then { codec :: any } else nil }
	local delivered: { [string]: DeliveryDescription | false } = {}
	-- What Lyra delivers with `options`, and `field` set to `value`; each asked of it once, and
	-- paid for from `budget` when it hasn't been yet.
	local function delivery_with(field: string?, value: number?, budget: { left: number }): DeliveryDescription?
		local asked = table.clone(options) :: { [string]: any }
		if field then
			asked[field] = value
		end
		local key = joined({
			asked.format,
			codec,
			asked.channels,
			asked.sample_rate_hz,
			asked.bit_depth,
			asked.bitrate_bps,
		}, 6)
		local known = delivered[key]
		if known == nil then
			if budget.left <= 0 then
				return nil
			end
			budget.left -= 1
			known = decide(track_id, asked :: TrackStreamOptions) or false
			delivered[key] = known
		end
		return if known then known else nil
	end

	local plain = delivery_with(nil, nil, { left = 1 })
	if not plain or not plain.codec then
		return nil
	end
	local lossless = is_lossless(plain.codec)
	if lossless and source.codec and not is_lossless(source.codec) then
		return nil
	end

	local alias = if profile.codec ~= "" then profile.codec else nil
	local profiles = profiles_for(info, plain.codec, format, alias)
	local max_bitrate = lesser(info.max_bitrate, info.max_transcoding_bitrate)
	local properties: { { property: Property, maximum: number? } } = {
		{ property = CHANNELS, maximum = profile.max_channels },
		{ property = SAMPLE_RATE, maximum = nil },
		if lossless then { property = BIT_DEPTH, maximum = nil } else { property = BITRATE, maximum = max_bitrate },
	}
	local optional_budget = { left = OPTIONAL_DECIDES }
	for _, entry in properties do
		local property, maximum = entry.property, entry.maximum
		local required: { Limitation } = {}
		local optional: { Limitation } = {}
		for _, codec_profile in profiles do
			for _, limitation in codec_profile.limitations do
				if limitation.name == property.limitation then
					table.insert(if limitation.required then required else optional, limitation)
				end
			end
		end
		local candidates = candidates_for(property, source, maximum, profiles)
		-- The first candidate meeting `limitations`, skipping without a `decide` those whose
		-- expected value they rule out; a delivery Lyra adjusts is checked as it is delivered.
		local function first_meeting(limitations: { Limitation }, budget: { left: number }): Candidate?
			for _, candidate in candidates do
				local expected = candidate.expected
				local admitted = true
				for _, limitation in limitations do
					admitted = admitted
						and limitation.comparison ~= nil
						and (expected == nil or compares(limitation, expected))
				end
				local delivery = if admitted then delivery_with(property.field, candidate.value, budget) else nil
				if delivery then
					local value = (delivery :: any)[property.field] :: number?
					local limit = maximum
					local fits = not (limit and value and value > limit)
					for _, limitation in limitations do
						fits = fits and meets(limitation, delivery, value)
					end
					if fits then
						return candidate
					end
				end
			end
			return nil
		end
		local chosen = first_meeting(required, { left = REQUIRED_DECIDES })
		if not chosen then
			return nil
		end
		for _, limitation in optional do
			table.insert(required, limitation)
			local better = first_meeting(required, optional_budget)
			if better then
				chosen = better
			else
				table.remove(required)
			end
		end
		(options :: any)[property.field] = chosen.value
	end

	local delivery = delivery_with(nil, nil, { left = 1 })
	if not delivery or not delivery.codec then
		return nil
	end
	local demands =
		{ protocols = {}, max_channels = profile.max_channels, max_bitrate = max_bitrate, profiles = profiles }
	if refusal(demands, delivery, if is_lossless(source.codec) then source.bitrate_bps else nil) then
		return nil
	end
	return {
		options = options,
		delivery = delivery,
		container = if profile.container ~= "" then profile.container else delivery.container,
		codec = alias or delivery.codec :: string,
	}
end

local function stream_details(container: string?, codec: string?, audio: Audio): { [string]: any }
	return {
		protocol = "http",
		container = container or "",
		codec = codec or "",
		audioChannels = audio.channels,
		audioBitrate = audio.bitrate_bps,
		audioSamplerate = audio.sample_rate_hz,
		audioBitdepth = audio.bit_depth,
	}
end

-- `transcodeParams` hold the source they were decided for and how to deliver it, as dot-separated
-- fields that need no escaping, an unknown or unset one empty:
-- `d.<source>` sends the source as it is, and
-- `t.<source>.<format>.<codec>.<bitrate>.<sample rate>.<channels>.<bit depth>` transcodes it,
-- where `<source>` is `<container>.<codec>.<bitrate>.<sample rate>.<channels>.<bit depth>`. The
-- options are those `stream` takes, so they need no signing; they hold while the source is as
-- it was.

local function source_params(source: SourceDescription): string
	return joined({
		source.container,
		source.codec,
		source.bitrate_bps,
		source.sample_rate_hz,
		source.channels,
		source.bit_depth,
	}, 6)
end

local function direct_params(source: SourceDescription): string
	return `d.{source_params(source)}`
end

local function transcode_params(source: SourceDescription, options: TrackStreamOptions): string
	local codecs = options.preferred_codecs
	local delivery = joined({
		options.format,
		if codecs then codecs[1] else nil,
		options.bitrate_bps,
		options.sample_rate_hz,
		options.channels,
		options.bit_depth,
	}, 6)
	return `t.{source_params(source)}.{delivery}`
end

local MAX_U32 = 4294967295

-- An option's value: nil when the field is empty, false when it isn't a number Lyra takes.
local function option_value(field: string): number? | false
	if field == "" then
		return nil
	end
	local number = if string.match(field, "^%d+$") then tonumber(field) else nil
	return if number and number > 0 and number <= MAX_U32 then number else false
end

-- The options `params` asks to stream with, nil to send the source as it is, or false when they
-- weren't decided for `source` as it is now.
local function parse_params(params: string, source: SourceDescription): TrackStreamOptions? | false
	local fields = string.split(params, ".")
	local kind = fields[1]
	if not ((kind == "d" and #fields == 7) or (kind == "t" and #fields == 13)) then
		return false
	end
	if table.concat(fields, ".", 2, 7) ~= source_params(source) then
		return false
	end
	if kind == "d" then
		return nil
	end
	local values: { number? } = {}
	for index = 10, 13 do
		local value = option_value(fields[index])
		if value == false then
			return false
		end
		values[index - 9] = value
	end
	return {
		format = if fields[8] ~= "" then fields[8] :: any else nil,
		preferred_codecs = if fields[9] ~= "" then { fields[9] :: any } else nil,
		bitrate_bps = values[1],
		sample_rate_hz = values[2],
		channels = values[3],
		bit_depth = values[4],
	}
end

local SONG = "song"
local PODCAST = "podcast"

local NOT_STREAMABLE = "The song can't be streamed"

-- A playback source as `decide` describes one.
local function described(source: PlaybackSourceInfo): SourceDescription
	return {
		container = source.container,
		codec = source.codec,
		bitrate_bps = source.bitrate_bps,
		sample_rate_hz = source.sample_rate_hz,
		channels = source.channel_count,
		bit_depth = source.bit_depth,
	}
end

-- The track's source, and how Lyra sends it as it is; nil without a source, and without a
-- delivery when Lyra can't send the source as it is.
local function source_of(track_id: string): (SourceDescription?, DeliveryDescription?)
	local direct = decide(track_id, nil)
	if direct then
		return direct.source, direct
	end
	local source = playback_sources.get_many({ track_id })[track_id]
	return if source then described(source) else nil, nil
end

local function decision_for(track_id: string, info: ClientInfo): { [string]: any }
	local source, direct = source_of(track_id)
	local decision: { [string]: any } = { canDirectPlay = false, canTranscode = false }
	if source then
		decision.sourceStream = stream_details(source.container, source.codec, source)
	end

	local reasons: { string } = {}
	for _, profile in info.direct_play do
		local reason = if source and direct then direct_play_refusal(info, profile, source) else "ContainerNotSupported"
		if not reason then
			decision.canDirectPlay = true
			decision.transcodeParams = direct_params(source :: SourceDescription)
			return decision
		end
		table.insert(reasons, reason)
	end
	decision.transcodeReason = reasons

	if not source then
		decision.errorReason = NOT_STREAMABLE
		return decision
	end
	for _, profile in info.transcoding do
		local transcode = transcode_for(track_id, info, profile, source)
		if transcode then
			decision.canTranscode = true
			decision.transcodeParams = transcode_params(source, transcode.options)
			decision.transcodeStream = stream_details(transcode.container, transcode.codec, transcode.delivery)
			return decision
		end
	end
	decision.errorReason = "No direct play or transcoding profile fits the song"
	return decision
end

type MediaFailure = { status: number, code: number, message: string }

-- The visible song `mediaId` and `mediaType` name, or why they name none, for each handler to
-- answer in its own way. Lyra has no podcasts.
local function media_of(params: ApiQueryParams): (string?, MediaFailure?)
	for _, name in { "mediaId", "mediaType" } do
		if not request.param(params, name) then
			return nil, { status = 400, code = 10, message = `Required parameter is missing: {name}` }
		end
	end
	local id = request.param(params, "mediaId") :: string
	local media_type = request.param(params, "mediaType") :: string
	if media_type == PODCAST then
		return nil, { status = 404, code = 70, message = "Podcast episode not found" }
	elseif media_type ~= SONG then
		return nil, { status = 400, code = 0, message = `Invalid mediaType: {media_type}` }
	elseif not tracks.get({ id })[id] then
		return nil, { status = 404, code = 70, message = "Song not found" }
	end
	return id, nil
end

-- Methods that answer every error with a bare HTTP status and message.
module.http_errors = {
	getTranscodeStream = true,
} :: { [string]: boolean }

-- The most seconds `offset` takes, so its milliseconds stay exact.
local MAX_OFFSET_S = 2 ^ 53 / 1000

module.methods = {
	getTranscodeDecision = function(req: Request): ApiResponse
		if string.upper(req.ctx.request.method) ~= "POST" then
			return request.fail(0, "getTranscodeDecision takes a POST request with the client's ClientInfo as its body")
		end
		local id, failure = media_of(req.params)
		if failure then
			return request.fail(failure.code, failure.message)
		end
		local info = client_info.parse(req.ctx.request.body_raw)
		return response.ok(req.params, { transcodeDecision = decision_for(id :: string, info) })
	end,

	-- Streams as a `getTranscodeDecision` result's `transcodeParams` say, starting `offset`
	-- seconds in.
	getTranscodeStream = function(req: Request): ApiResponse
		local params = req.params
		local id, failure = media_of(params)
		if failure then
			return response.http_error(failure.status, failure.message)
		end
		local transcode_params = request.param(params, "transcodeParams")
		if not transcode_params then
			return response.http_error(400, "Required parameter is missing: transcodeParams")
		end
		local source = playback_sources.get_many({ id :: string })[id :: string]
		if not source then
			return response.http_error(404, NOT_STREAMABLE)
		end

		local raw_offset = request.param(params, "offset")
		local offset = if raw_offset then tonumber(raw_offset) else 0
		if not offset or not (offset >= 0 and offset <= MAX_OFFSET_S) then
			return response.http_error(400, `Invalid offset: {raw_offset}`)
		end

		local options = parse_params(transcode_params, described(source))
		if options == false then
			return response.http_error(410, "transcodeParams no longer fit the song; ask getTranscodeDecision again")
		end
		local start_offset_ms = math.floor(offset * 1000)
		if start_offset_ms > 0 then
			local with_offset: TrackStreamOptions = if options then options else {}
			with_offset.start_offset_ms = start_offset_ms
			options = with_offset
		end
		-- Lyra checks the format and codec as it builds the response.
		local built, streamed = pcall(api.response.stream_track, id :: string, options :: TrackStreamOptions?)
		if not built then
			return response.http_error(410, "transcodeParams no longer fit the song; ask getTranscodeDecision again")
		end
		return streamed
	end,
}

return module
