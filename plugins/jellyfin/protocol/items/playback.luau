--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")
local lyrics = require("@lyra/lyrics")
local playback_sources = require("@lyra/playback_sources")
local playbacks = require("@lyra/playback_sessions")
local tracks = require("@lyra/tracks")

local constants = require("../../constants")
local device_profiles = require("./device_profile")
local encoder = require("./encoder")

local PLAYBACK_COMPLETION_TOLERANCE_MS = constants.PLAYBACK_COMPLETION_TOLERANCE_MS

local item_ids = require("./identifiers")
local media = require("./media")
local requests = require("./requests")
local responses = require("./responses")

local next_generated_play_session_id = 1
type Track = tracks.Track
type PlaybackSourceInfo = playback_sources.PlaybackSourceInfo
type EntryInfo = playback_sources.EntryInfo
type ApiContext = api.ApiContext
type ApiRequest = api.ApiRequest
type ApiResponse = api.ApiResponse
type ApiJsonValue = api.JsonValue
type AuthContext = api.ApiAuth
type JsonObject = { [string]: ApiJsonValue }

export type PlaybackPayload = { [string]: string | number | boolean? }

-- What a Jellyfin client reported playing on its session, keyed by the Jellyfin session id.
export type ClientPlayback = {
	user_id: string,
	session_info: JsonObject,
	-- Whether the client reports over its socket rather than HTTP.
	via_socket: boolean,
	reported_s: number,
	track_id: string,
	position_ms: number?,
	duration_ms: number?,
	is_paused: boolean,
}

type PlaybackSessionState = ClientPlayback & {
	playback_start_ticks: number,
}

-- Who reported a playback event: the Jellyfin session it lists under, and its transport.
export type PlaybackReporter = {
	session_info: JsonObject,
	via_socket: boolean,
}

type MediaStream = {
	Codec: string?,
	Type: string,
	Index: number,
	Channels: number?,
	SampleRate: number?,
	BitDepth: number?,
	IsDefault: boolean,
	IsForced: boolean,
	IsHearingImpaired: boolean,
	IsOriginal: boolean,
	IsInterlaced: boolean,
	IsExternal: boolean,
	IsTextSubtitleStream: boolean,
	SupportsExternalStream: boolean,
	BitRate: number?,
}

type MediaSource = {
	Protocol: string,
	Id: string?,
	Path: string?,
	Type: string,
	Container: string?,
	Size: number?,
	Name: string,
	IsRemote: boolean,
	RunTimeTicks: number?,
	ReadAtNativeFramerate: boolean,
	IgnoreDts: boolean,
	IgnoreIndex: boolean,
	GenPtsInput: boolean,
	SupportsTranscoding: boolean,
	SupportsDirectStream: boolean,
	SupportsDirectPlay: boolean,
	IsInfiniteStream: boolean,
	RequiresOpening: boolean,
	RequiresClosing: boolean,
	RequiresLooping: boolean,
	SupportsProbing: boolean,
	MediaStreams: { MediaStream },
	Bitrate: number?,
	TranscodingContainer: string?,
	TranscodingUrl: string?,
	TranscodingSubProtocol: string?,
	DefaultAudioStreamIndex: number?,
	HasSegments: boolean,
	ETag: string?,
}

type AudioPlaybackLimits = {
	max_bitrate_bps: number?,
	max_sample_rate_hz: number?,
	max_bit_depth: number?,
	max_channels: number?,
}

local playback_session_state_by_key: { [string]: PlaybackSessionState } = {}
local playback_session_state_count = 0
local MAX_PLAYBACK_SESSION_STATES = 500
-- Jellyfin stops a session's playback once it has gone this long without a report.
local CLIENT_PLAYBACK_IDLE_SECONDS = 300

local function forget_session_state(session_key: string)
	if playback_session_state_by_key[session_key] ~= nil then
		playback_session_state_by_key[session_key] = nil
		playback_session_state_count -= 1
	end
end

-- Makes room for one more session: idle sessions go first, then the least recently reported.
local function evict_session_states()
	if playback_session_state_count < MAX_PLAYBACK_SESSION_STATES then
		return
	end
	local oldest_allowed_s = os.time() - CLIENT_PLAYBACK_IDLE_SECONDS
	local oldest_key: string? = nil
	local oldest_s = math.huge
	for session_key, state in playback_session_state_by_key do
		if state.reported_s < oldest_allowed_s then
			forget_session_state(session_key)
		elseif state.reported_s < oldest_s then
			oldest_key = session_key
			oldest_s = state.reported_s
		end
	end
	if playback_session_state_count >= MAX_PLAYBACK_SESSION_STATES and oldest_key then
		forget_session_state(oldest_key)
	end
end

local module = {}
module.trim = requests.trim
module.decode_jellyfin_item_id = item_ids.decode_jellyfin_item_id
module.bad_request = responses.bad_request
module.not_found = responses.not_found
module.parse_query_string = requests.parse_query_string
module.resolve_playback_source_for_track = media.resolve_playback_source_for_track
module.encode_jellyfin_item_id = item_ids.encode_jellyfin_item_id

local function ticks_to_ms(value: number | string?): number?
	local ticks = tonumber(value)
	if not ticks then
		return nil
	end

	if ticks < 0 then
		ticks = 0
	end

	return math.floor(ticks / 10000)
end

local function as_playback_payload(value: ApiJsonValue): PlaybackPayload?
	if type(value) ~= "table" then
		return nil
	end
	return value :: PlaybackPayload
end

local function as_json_object(value: ApiJsonValue?): JsonObject?
	if type(value) ~= "table" then
		return nil
	end
	return value :: JsonObject
end

local function scalar_field(payload: PlaybackPayload?, key: string): string | number?
	local value = if payload then payload[key] else nil
	if type(value) == "string" or type(value) == "number" then
		return value
	end
	return nil
end

local function is_completed_position(position_ms: number?, duration_ms: number?): boolean
	if position_ms == nil or duration_ms == nil then
		return false
	end
	if duration_ms <= 0 then
		return false
	end

	local completion_threshold_ms = math.max(duration_ms - PLAYBACK_COMPLETION_TOLERANCE_MS, 0)
	return position_ms >= completion_threshold_ms
end

type PlaybackState = "playing" | "paused" | "stopped" | "completed"
type PlaybackEvent = "started" | "progress" | "stopped" | "completed"

local function as_playback_event(value: string): PlaybackEvent
	if value == "started" then
		return "started"
	end
	if value == "stopped" then
		return "stopped"
	end
	if value == "completed" then
		return "completed"
	end
	return "progress"
end

local function playback_state_for_event(
	event_name: string,
	payload: PlaybackPayload?,
	position_ms: number?,
	duration_ms: number?
): PlaybackState
	if event_name == "stopped" then
		if is_completed_position(position_ms, duration_ms) then
			return "completed"
		end
		return "stopped"
	end
	if type(if payload then payload.IsPaused else nil) == "boolean" and payload ~= nil and payload.IsPaused then
		return "paused"
	end

	return "playing"
end

local function should_normalize_progress_to_started(
	event_name: string,
	payload: PlaybackPayload?,
	state: string,
	position_ms: number?
): boolean
	if event_name ~= "progress" then
		return false
	end
	if state ~= "paused" then
		return false
	end
	if position_ms == nil or position_ms ~= 0 then
		return false
	end

	local playback_start_ticks = tonumber(scalar_field(payload, "PlaybackStartTimeTicks"))
	if playback_start_ticks and playback_start_ticks > 0 then
		return false
	end

	return true
end

local function is_terminal_playback_state(state: string): boolean
	return state == "stopped" or state == "completed"
end

local function ensure_playback_session_id(requested_play_session_id: string?): string
	local existing_session_id = module.trim(tostring(requested_play_session_id or ""))
	if existing_session_id then
		return existing_session_id
	end

	local new_id = tostring(next_generated_play_session_id)
	next_generated_play_session_id += 1
	return new_id
end

local function ms_to_ticks(duration_ms: number?): number?
	local value = tonumber(duration_ms)
	if not value then
		return nil
	end

	return math.floor(value * 10000)
end

local function json_string_field(object: JsonObject?, key: string): string?
	local value = if object then object[key] else nil
	if type(value) ~= "string" then
		return nil
	end
	return module.trim(value)
end

local function json_boolean_field(object: JsonObject?, key: string): boolean?
	local value = if object then object[key] else nil
	if type(value) == "boolean" then
		return value
	end
	return nil
end

local function json_number_field(object: JsonObject?, key: string): number?
	local value = if object then object[key] else nil
	if type(value) == "number" then
		return value
	end
	return nil
end

local function json_object_field(object: JsonObject?, key: string): JsonObject?
	return as_json_object(if object then object[key] else nil)
end

local function media_source_audio_codec(media_source: MediaSource): string?
	local media_streams = media_source.MediaStreams
	for _, stream in ipairs(media_streams) do
		if stream.Type == "Audio" then
			return stream.Codec
		end
	end
	return nil
end

local function first_audio_stream(media_source: MediaSource): MediaStream?
	for _, stream in media_source.MediaStreams do
		if stream.Type == "Audio" then
			return stream
		end
	end

	return nil
end

-- Upstream's `IsBitrateLimitExceeded`: a source of unknown bitrate counts as 40 Mbps, and a limit
-- of zero or less is no limit.
local function bitrate_limit_exceeded(media_source: MediaSource, max_bitrate_bps: number?): boolean
	if media_source.IsRemote or max_bitrate_bps == nil or max_bitrate_bps <= 0 then
		return false
	end
	return (media_source.Bitrate or 40000000) > max_bitrate_bps
end

local function media_source_respects_audio_limits(media_source: MediaSource, limits: AudioPlaybackLimits): boolean
	local max_sample_rate_hz = limits.max_sample_rate_hz
	local max_bit_depth = limits.max_bit_depth
	local max_channels = limits.max_channels
	if bitrate_limit_exceeded(media_source, limits.max_bitrate_bps) then
		return false
	end

	local stream = first_audio_stream(media_source)
	if stream == nil then
		return true
	end
	if max_sample_rate_hz ~= nil and stream.SampleRate ~= nil and stream.SampleRate > max_sample_rate_hz then
		return false
	end
	if max_bit_depth ~= nil and stream.BitDepth ~= nil and stream.BitDepth > max_bit_depth then
		return false
	end
	if max_channels ~= nil and stream.Channels ~= nil and stream.Channels > max_channels then
		return false
	end

	return true
end

local function supports_audio_direct_play_from_container_csv(
	media_source: MediaSource,
	direct_play_csv: string?
): boolean
	local trimmed_csv = module.trim(direct_play_csv)
	if trimmed_csv == nil then
		return false
	end

	local source_container = media_source.Container
	local source_codec = media_source_audio_codec(media_source)
	for raw_token in string.gmatch(trimmed_csv, "[^,]+") do
		local token = module.trim(raw_token)
		if token then
			local delimiter_index = string.find(token, "|", 1, true)
			local container = if delimiter_index then string.sub(token, 1, delimiter_index - 1) else token
			local codec_tail = if delimiter_index then string.sub(token, delimiter_index + 1) else nil
			local codec_csv = if codec_tail then (string.gsub(codec_tail, "|", ",")) else nil
			if
				device_profiles.contains_container(container, source_container)
				and device_profiles.contains_container(codec_csv, source_codec)
			then
				return true
			end
		end
	end

	return false
end

local function is_audio_streaming_profile(profile: JsonObject): boolean
	return device_profiles.is_audio_profile(profile) and device_profiles.is_streaming_profile(profile)
end

-- As upstream, the first streaming audio profile whose codec the server can encode to; a profile
-- whose codec is null names it by its container. Upstream reads a missing codec as an empty one and
-- skips the profile, but JSON reaches the plugin with null and missing alike, so both name it.
local function audio_transcoding_profile(device_profile: JsonObject): JsonObject?
	for _, profile in device_profiles.object_list(device_profile, "TranscodingProfiles") do
		local codec = device_profiles.string_field(profile, "AudioCodec")
			or device_profiles.string_field(profile, "Container")
		if is_audio_streaming_profile(profile) and codec and encoder.can_encode(codec) then
			return profile
		end
	end

	return nil
end

-- Upstream says a client transcodes when it has any streaming audio profile, usable or not.
local function has_audio_streaming_profile(device_profile: JsonObject): boolean
	for _, profile in device_profiles.object_list(device_profile, "TranscodingProfiles") do
		if is_audio_streaming_profile(profile) then
			return true
		end
	end
	return false
end

local function audio_direct_play_profile(media_source: MediaSource, device_profile: JsonObject): JsonObject?
	local codec = media_source_audio_codec(media_source)
	for _, profile in device_profiles.object_list(device_profile, "DirectPlayProfiles") do
		if
			device_profiles.is_audio_profile(profile)
			and device_profiles.contains_container(
				device_profiles.string_field(profile, "Container"),
				media_source.Container
			)
			and device_profiles.contains_container(device_profiles.string_field(profile, "AudioCodec"), codec)
		then
			return profile
		end
	end
	return nil
end

-- Upstream's `IsAudioDirectStreamSupported`: a profile the source's container isn't in, naming the
-- source's codec as its codec or its container.
local function has_audio_direct_stream_profile(media_source: MediaSource, device_profile: JsonObject): boolean
	local codec = media_source_audio_codec(media_source)
	if codec == nil then
		return false
	end
	local lowered = string.lower(codec)
	for _, profile in device_profiles.object_list(device_profile, "DirectPlayProfiles") do
		local container = device_profiles.string_field(profile, "Container")
		local profile_codec = device_profiles.string_field(profile, "AudioCodec")
		if
			device_profiles.is_audio_profile(profile)
			and not device_profiles.contains_container(container, media_source.Container)
			and (
				(profile_codec ~= nil and string.lower(profile_codec) == lowered)
				or (container ~= nil and string.lower(container) == lowered)
			)
		then
			return true
		end
	end
	return false
end

type TranscodeReasons = { [string]: boolean }

-- Upstream's `TranscodeReason` flags an audio source can raise, in flag order.
local TRANSCODE_REASON_ORDER = {
	"ContainerNotSupported",
	"VideoCodecNotSupported",
	"AudioCodecNotSupported",
	"AudioChannelsNotSupported",
	"AudioProfileNotSupported",
	"AudioSampleRateNotSupported",
	"AudioBitDepthNotSupported",
	"ContainerBitrateExceedsLimit",
	"AudioBitrateNotSupported",
}

local CONDITION_TRANSCODE_REASONS: { [string]: string } = {
	AudioBitrate = "AudioBitrateNotSupported",
	AudioChannels = "AudioChannelsNotSupported",
	AudioProfile = "AudioProfileNotSupported",
	AudioSampleRate = "AudioSampleRateNotSupported",
	AudioBitDepth = "AudioBitDepthNotSupported",
}

-- Upstream's `GetTranscodeReasonsFromDirectPlayProfile` when no profile plays the source. With no
-- video stream to judge, upstream leaves the video codec unsupported along with the container.
local function add_direct_play_profile_reasons(
	reasons: TranscodeReasons,
	media_source: MediaSource,
	device_profile: JsonObject
)
	for _, profile in device_profiles.object_list(device_profile, "DirectPlayProfiles") do
		if
			device_profiles.is_audio_profile(profile)
			and device_profiles.contains_container(
				device_profiles.string_field(profile, "Container"),
				media_source.Container
			)
		then
			reasons.AudioCodecNotSupported = true
			return
		end
	end
	reasons.ContainerNotSupported = true
	reasons.VideoCodecNotSupported = true
	reasons.AudioCodecNotSupported = true
end

-- How upstream's `StreamBuilder` plays a source: directly, or by the transcode it describes.
type StreamInfo = {
	direct_play: boolean,
	reasons: TranscodeReasons,
	container: string?,
	hls: boolean,
	audio_codec: string?,
	audio_bitrate: number?,
	audio_sample_rate: number?,
	-- The target channel count a codec profile asks for, which upstream passes as a stream option.
	audio_channels: number?,
	transcoding_max_audio_channels: number?,
	enable_audio_vbr_encoding: boolean,
	min_segments: number?,
	segment_length: number?,
	estimate_content_length: boolean,
	enable_mpegts_m2ts_mode: boolean,
	copy_timestamps: boolean,
	enable_subtitles_in_manifest: boolean,
	transcode_seek_info: string?,
}

local function new_stream_info(): StreamInfo
	return {
		direct_play = false,
		reasons = {},
		hls = false,
		enable_audio_vbr_encoding = false,
		estimate_content_length = false,
		enable_mpegts_m2ts_mode = false,
		copy_timestamps = false,
		enable_subtitles_in_manifest = false,
	}
end

-- Upstream's `StreamBuilder.ApplyTranscodingConditions` for the audio properties a URL carries.
-- It can't express a minimum, and ignores conditions on other properties.
local function apply_transcoding_conditions(stream: StreamInfo, conditions: { device_profiles.Condition })
	for _, condition in conditions do
		local value = device_profiles.parse_int(condition.value)
		local kind = condition.condition
		if value ~= nil and (kind == "Equals" or kind == "LessThanEqual") then
			local at_most = kind == "LessThanEqual"
			if condition.property == "AudioBitrate" then
				stream.audio_bitrate = if at_most then math.min(value, stream.audio_bitrate or value) else value
			elseif condition.property == "AudioSampleRate" then
				stream.audio_sample_rate = if at_most then math.min(value, stream.audio_sample_rate or value) else value
			elseif condition.property == "AudioChannels" then
				local target = stream.audio_channels
				if target ~= nil and stream.transcoding_max_audio_channels ~= nil then
					target = math.min(target, stream.transcoding_max_audio_channels)
				end
				target = target or stream.transcoding_max_audio_channels
				stream.audio_channels = if at_most then math.min(value, target or value) else value
			end
		end
	end
end

-- Upstream's `DeviceProfile` defaults, which a profile that leaves the field out, or sends null, keeps.
local DEFAULT_MAX_STREAMING_BITRATE = 8000000
local DEFAULT_MUSIC_TRANSCODING_BITRATE = 128000
module.DEFAULT_MAX_STREAMING_BITRATE = DEFAULT_MAX_STREAMING_BITRATE

-- A transcoding profile's `TranscodeSeekInfo`, when it isn't upstream's default of `Auto`.
local function transcode_seek_info(profile: JsonObject): string?
	local raw = profile.TranscodeSeekInfo
	if raw == 1 or (type(raw) == "string" and (string.lower(raw) == "bytes" or raw == "1")) then
		return "Bytes"
	end
	return nil
end

local function positive_int(profile: JsonObject, key: string): number?
	local value = device_profiles.parse_int(device_profiles.string_field(profile, key))
	return if value and value > 0 then value else nil
end

-- Upstream's `StreamBuilder.GetOptimalAudioStream` for a single source, or the error upstream
-- answers with a 400. Only codec profiles limit the audio itself; the request's
-- `MaxAudioChannels` plays no part. Upstream also direct-streams a source whose codec a profile
-- names as its container or codec, into a `ts` remux; that is left out, and such sources transcode.
local function optimal_audio_stream(
	media_source: MediaSource,
	request_object: JsonObject?,
	device_profile: JsonObject,
	codec_profiles: { device_profiles.CodecProfile }
): (StreamInfo?, string?)
	local max_bitrate = json_number_field(request_object, "MaxStreamingBitrate")
		or json_number_field(device_profile, "MaxStreamingBitrate")
		or DEFAULT_MAX_STREAMING_BITRATE
	local stream = new_stream_info()
	local reasons = stream.reasons
	local audio_stream = first_audio_stream(media_source)
	local audio: device_profiles.AudioProperties = {
		channels = if audio_stream then audio_stream.Channels else nil,
		bitrate = if audio_stream then audio_stream.BitRate else nil,
		sample_rate = if audio_stream then audio_stream.SampleRate else nil,
		bit_depth = if audio_stream then audio_stream.BitDepth else nil,
	}

	-- Upstream's `GetAudioDirectPlayProfile`.
	local exceeded = bitrate_limit_exceeded(media_source, max_bitrate)
	local playable = false
	if audio_direct_play_profile(media_source, device_profile) ~= nil then
		if media_source.SupportsDirectPlay then
			playable = not exceeded
			reasons.ContainerBitrateExceedsLimit = exceeded or nil
		end
		if not playable and media_source.SupportsDirectStream and exceeded then
			reasons.ContainerBitrateExceedsLimit = true
		end
	elseif has_audio_direct_stream_profile(media_source, device_profile) then
		reasons.ContainerNotSupported = true
		if media_source.SupportsDirectStream and exceeded then
			reasons.ContainerBitrateExceedsLimit = true
		end
	else
		add_direct_play_profile_reasons(reasons, media_source, device_profile)
	end

	if playable then
		local failed, err = device_profiles.audio_conditions(
			codec_profiles,
			media_source.Container,
			media_source_audio_codec(media_source),
			audio,
			true
		)
		if failed == nil then
			return nil, err
		elseif #failed == 0 then
			stream.direct_play = true
			return stream, nil
		end
		for _, condition in failed do
			reasons[CONDITION_TRANSCODE_REASONS[condition.property]] = true
		end
	end

	local profile = audio_transcoding_profile(device_profile)
	if profile == nil then
		return stream, nil
	end
	if not media_source.SupportsTranscoding then
		return nil, nil
	end

	local protocol = device_profiles.string_field(profile, "Protocol")
	local raw_container = device_profiles.string_field(profile, "Container")
	stream.hls = protocol ~= nil and string.lower(protocol) == "hls"
	stream.container = if raw_container then string.lower(raw_container) elseif stream.hls then "ts" else nil
	stream.transcoding_max_audio_channels =
		device_profiles.parse_int(device_profiles.string_field(profile, "MaxAudioChannels"))
	stream.enable_audio_vbr_encoding = json_boolean_field(profile, "EnableAudioVbrEncoding") ~= false
	stream.min_segments = positive_int(profile, "MinSegments")
	stream.segment_length = positive_int(profile, "SegmentLength")
	stream.estimate_content_length = json_boolean_field(profile, "EstimateContentLength") == true
	stream.enable_mpegts_m2ts_mode = json_boolean_field(profile, "EnableMpegtsM2TsMode") == true
	stream.copy_timestamps = json_boolean_field(profile, "CopyTimestamps") == true
	stream.enable_subtitles_in_manifest = json_boolean_field(profile, "EnableSubtitlesInManifest") == true
	stream.transcode_seek_info = transcode_seek_info(profile)
	media_source.TranscodingContainer = stream.container
	media_source.TranscodingSubProtocol = if stream.hls then "hls" else "http"

	local codec = device_profiles.string_field(profile, "AudioCodec")
	local conditions, err = device_profiles.audio_conditions(
		codec_profiles,
		device_profiles.string_field(profile, "Container"),
		codec,
		audio,
		false
	)
	if conditions == nil then
		return nil, err
	end
	apply_transcoding_conditions(stream, conditions)

	-- The profile's music transcoding bitrate, capped by the maximum streaming bitrate and the
	-- codec profiles.
	local bitrate = math.min(
		json_number_field(device_profile, "MusicStreamingTranscodingBitrate") or DEFAULT_MUSIC_TRANSCODING_BITRATE,
		max_bitrate
	)
	stream.audio_bitrate = math.floor(math.min(bitrate, stream.audio_bitrate or bitrate))

	if codec ~= nil and string.find(codec, "%S") then
		stream.audio_codec = codec
	end
	return stream, nil
end

local function no_compatible_stream(): ApiJsonValue
	return {
		MediaSources = {},
		PlaySessionId = nil,
		ErrorCode = "NoCompatibleStream",
	}
end

-- What a TranscodingUrl carries beyond the stream: the request's and the source's identifiers.
type UrlContext = {
	play_session_id: string,
	request_object: JsonObject?,
	access_token: string?,
	device_id: string?,
	device_profile_id: string?,
	etag: string?,
}

-- A device profile's id as upstream formats a Guid for URLs: 32 lowercase hex digits.
local function device_profile_id(device_profile: JsonObject?): string?
	local raw = device_profiles.string_field(device_profile, "Id")
	local digits = if raw then string.lower((string.gsub(raw, "[{}%-]", ""))) else nil
	return if digits and string.match(digits, "^%x+$") and #digits == 32 then digits else nil
end

-- Builds the URL as upstream's `StreamInfo.ToUrl` does, in its order and casing: relative to the
-- API root, which clients prepend, and carrying the request's token so players can fetch it
-- without headers. Upstream appends the copy switches after building it, and only when the client
-- turned copying off.
local function transcoding_url(media_source_id: string, stream: StreamInfo, context: UrlContext): string
	local parts: { string } = {}
	local function add(name: string, value: string | number)
		table.insert(parts, `{name}={value}`)
	end
	if context.device_profile_id then
		add("DeviceProfileId", context.device_profile_id)
	end
	if context.device_id then
		add("DeviceId", context.device_id)
	end
	add("MediaSourceId", media_source_id)
	if stream.audio_codec ~= nil then
		add("AudioCodec", stream.audio_codec)
	end
	if stream.audio_bitrate ~= nil then
		add("AudioBitrate", stream.audio_bitrate)
	end
	if stream.audio_sample_rate ~= nil then
		add("AudioSampleRate", stream.audio_sample_rate)
	end
	local request_object = context.request_object
	local start_time_ticks = json_number_field(request_object, "StartTimeTicks")
	if stream.hls then
		if stream.container ~= nil then
			add("SegmentContainer", stream.container)
		end
		if stream.segment_length ~= nil then
			add("SegmentLength", stream.segment_length)
		end
		if stream.min_segments ~= nil then
			add("MinSegments", stream.min_segments)
		end
	elseif start_time_ticks ~= nil and start_time_ticks ~= 0 then
		add("StartTimeTicks", start_time_ticks)
	end
	add("PlaySessionId", context.play_session_id)
	if context.access_token ~= nil then
		add("ApiKey", context.access_token)
	end
	if stream.transcoding_max_audio_channels ~= nil then
		add("TranscodingMaxAudioChannels", stream.transcoding_max_audio_channels)
	end
	if stream.enable_subtitles_in_manifest then
		add("EnableSubtitlesInManifest", "True")
	end
	if stream.enable_mpegts_m2ts_mode then
		add("EnableMpegtsM2TsMode", "True")
	end
	if stream.estimate_content_length then
		add("EstimateContentLength", "True")
	end
	if stream.transcode_seek_info ~= nil then
		add("TranscodeSeekInfo", stream.transcode_seek_info)
	end
	if stream.copy_timestamps then
		add("CopyTimestamps", "True")
	end
	add("RequireAvc", "false")
	add("EnableAudioVbrEncoding", tostring(stream.enable_audio_vbr_encoding))
	if context.etag ~= nil then
		add("Tag", context.etag)
	end
	if stream.audio_channels ~= nil then
		add("audiochannels", stream.audio_channels)
	end
	local reasons: { string } = {}
	for _, reason in TRANSCODE_REASON_ORDER do
		if stream.reasons[reason] then
			table.insert(reasons, reason)
		end
	end
	if #reasons > 0 then
		add("TranscodeReasons", table.concat(reasons, ","))
	end
	if json_boolean_field(request_object, "AllowVideoStreamCopy") == false then
		add("allowVideoStreamCopy", "false")
	end
	if json_boolean_field(request_object, "AllowAudioStreamCopy") == false then
		add("allowAudioStreamCopy", "false")
	end

	local path = if stream.hls
		then `/audio/{media_source_id}/master.m3u8`
		elseif
			stream.container ~= nil and stream.container ~= ""
		then `/audio/{media_source_id}/stream.{stream.container}`
		else `/audio/{media_source_id}/stream`
	return `{path}?{table.concat(parts, "&")}`
end

-- Upstream's `MediaInfoHelper.SetDeviceSpecificData` for an audio source: the stream it built,
-- if any, or the error upstream answers with a 400.
local function apply_device_profile(
	media_source: MediaSource,
	device_profile: JsonObject,
	context: UrlContext
): (StreamInfo?, string?)
	local request_object = context.request_object
	local codec_profiles, codec_profiles_err = device_profiles.codec_profiles(device_profile)
	if codec_profiles == nil then
		return nil, codec_profiles_err
	end
	if json_boolean_field(request_object, "EnableDirectPlay") == false then
		media_source.SupportsDirectPlay = false
	end
	if
		json_boolean_field(request_object, "EnableDirectStream") == false
		or json_boolean_field(request_object, "AllowVideoStreamCopy") == false
	then
		media_source.SupportsDirectStream = false
	end
	if json_boolean_field(request_object, "EnableTranscoding") == false then
		media_source.SupportsTranscoding = false
	end

	-- Without a stream, as when transcoding is off and the client can't play the source,
	-- upstream leaves the source's flags as they are.
	local stream, stream_err = optimal_audio_stream(media_source, request_object, device_profile, codec_profiles)
	local media_source_id = module.trim(media_source.Id)
	if stream == nil or media_source_id == nil then
		return nil, stream_err
	end

	-- Upstream takes the default audio stream from the stream it built, which names none for audio.
	media_source.DefaultAudioStreamIndex = nil
	-- Upstream turns direct streaming off for every request, so it follows direct play.
	media_source.SupportsDirectPlay = stream.direct_play
	media_source.SupportsDirectStream = stream.direct_play
	media_source.SupportsTranscoding = has_audio_streaming_profile(device_profile)
	if stream.direct_play or not media_source.SupportsTranscoding then
		return stream, nil
	end

	media_source.TranscodingSubProtocol = if stream.hls then "hls" else "http"
	media_source.TranscodingUrl = transcoding_url(media_source_id, stream, context)
	return stream, nil
end

local function build_media_info(
	track: Track?,
	source: PlaybackSourceInfo?,
	item_id: string?,
	fallback_name: string?,
	has_lyrics: boolean?
): ({ MediaSource }, { MediaStream }, string?)
	local entry: EntryInfo? = if source and type(source.entry) == "table" then source.entry else nil
	local media_item_id = module.encode_jellyfin_item_id(item_id)
	local container = media.probed_container(source)
	local codec = media.jellyfin_codec(source)
	local trimmed = media.is_trimmed(source)
	local size = if entry and entry.size and not trimmed then tonumber(entry.size) else nil
	local bitrate = if source then source.bitrate_bps else nil
	local direct_playable = source ~= nil and media_item_id ~= nil and not trimmed

	local media_stream: MediaStream = {
		Codec = codec,
		Type = "Audio",
		Index = 0,
		Channels = if source then source.channel_count else nil,
		SampleRate = if source then source.sample_rate_hz else nil,
		BitDepth = if source then source.bit_depth else nil,
		IsDefault = true,
		IsForced = false,
		IsHearingImpaired = false,
		IsOriginal = false,
		IsInterlaced = false,
		IsExternal = false,
		IsTextSubtitleStream = false,
		SupportsExternalStream = false,
	}
	if bitrate then
		media_stream.BitRate = bitrate
	end
	local media_streams = { media_stream }
	if has_lyrics then
		local lyric_stream: MediaStream = {
			Type = "Lyric",
			Index = 1,
			IsDefault = false,
			IsForced = false,
			IsHearingImpaired = false,
			IsOriginal = false,
			IsInterlaced = false,
			IsExternal = false,
			IsTextSubtitleStream = false,
			SupportsExternalStream = false,
		}
		table.insert(media_streams, lyric_stream)
	end

	local media_source: MediaSource = {
		Protocol = "File",
		Id = media_item_id,
		Path = if entry then entry.full_path else nil,
		Type = "Default",
		Container = container,
		Size = size,
		Name = if entry
			then (entry.name or fallback_name or tostring(item_id))
			else (fallback_name or tostring(item_id)),
		IsRemote = false,
		RunTimeTicks = if track then ms_to_ticks(track.duration_ms) else nil,
		ReadAtNativeFramerate = false,
		IgnoreDts = false,
		IgnoreIndex = false,
		GenPtsInput = false,
		SupportsTranscoding = true,
		SupportsDirectStream = direct_playable,
		SupportsDirectPlay = direct_playable,
		IsInfiniteStream = false,
		RequiresOpening = false,
		RequiresClosing = false,
		RequiresLooping = false,
		SupportsProbing = false,
		MediaStreams = media_streams,
		Bitrate = bitrate,
		DefaultAudioStreamIndex = 0,
		HasSegments = false,
	}
	if entry and entry.hash then
		media_source.ETag = tostring(entry.hash)
	end

	return { media_source }, media_streams, container
end

function module.build_audio_media_source(item_id: string?, track: Track?, public: boolean?): MediaSource?
	local source = if public
		then media.resolve_public_playback_source(track, true)
		else module.resolve_playback_source_for_track(track, true)
	local has_lyrics = false
	if track then
		local ok, value = pcall(lyrics.has, track.id)
		has_lyrics = ok and value == true
	end
	local media_sources, _ =
		build_media_info(track, source, item_id, if track then track.track_title else item_id, has_lyrics)
	return media_sources[1]
end

function module.audio_supports_direct_play_from_container_csv(
	media_source: MediaSource?,
	direct_play_csv: string?,
	max_bitrate_bps: number?,
	max_sample_rate_hz: number?,
	max_bit_depth: number?,
	max_channels: number?,
	enable_direct_play: boolean?,
	forbid_stream_copy: boolean?
): boolean
	if
		media_source == nil
		or not media_source.SupportsDirectPlay
		or enable_direct_play == false
		or forbid_stream_copy == true
	then
		return false
	end

	local limits: AudioPlaybackLimits = {
		max_bitrate_bps = max_bitrate_bps,
		max_sample_rate_hz = max_sample_rate_hz,
		max_bit_depth = max_bit_depth,
		max_channels = max_channels,
	}
	return supports_audio_direct_play_from_container_csv(media_source, direct_play_csv)
		and media_source_respects_audio_limits(media_source, limits)
end

function module.parse_playback_report_body(ctx: ApiContext?): (PlaybackPayload?, ApiResponse?)
	local request = if ctx then ctx.request else nil
	local request_json = if request then request.json else nil
	local payload = as_playback_payload(request_json)
	if payload == nil then
		return nil, module.bad_request("invalid playback report body")
	end

	return payload, nil
end

function module.as_playback_payload(value: ApiJsonValue): PlaybackPayload?
	return as_playback_payload(value)
end

function module.ensure_payload_user_matches(auth_context: AuthContext, payload: PlaybackPayload?): boolean
	local raw_user_id = scalar_field(payload, "UserId")
	local payload_user_id = if type(raw_user_id) == "string" then module.trim(raw_user_id) else nil
	if payload_user_id and item_ids.decode_jellyfin_item_id(payload_user_id) ~= auth_context.principal.user_id then
		return false
	end

	return true
end

function module.report_playback_event(
	auth_context: AuthContext,
	payload: PlaybackPayload?,
	event_name: string,
	session_key: string,
	reporter: PlaybackReporter
): (boolean, string?)
	local raw_item_id = scalar_field(payload, "ItemId")
	local item_id = item_ids.decode_jellyfin_item_id(if type(raw_item_id) == "string" then raw_item_id else nil)
	-- As upstream, a report naming no item records nothing and still succeeds. A track the caller
	-- can't see reads as one that doesn't exist.
	local track = module.resolve_track_for_playback(item_id)
	if item_id == nil or track == nil then
		-- A stop ends whatever this session was playing, even when it can't name the item.
		if event_name == "stopped" then
			forget_session_state(session_key)
		end
		return true, nil
	end

	local position_ms = ticks_to_ms(scalar_field(payload, "PositionTicks"))
	local duration_ms = ticks_to_ms(scalar_field(payload, "RunTimeTicks")) or track.duration_ms
	local state = playback_state_for_event(event_name, payload, position_ms, duration_ms)
	local normalized_event_name: string = if should_normalize_progress_to_started(
			event_name,
			payload,
			state,
			position_ms
		)
		then "started"
		else event_name
	local normalized_state: PlaybackState = if normalized_event_name == "started" then "playing" else state
	local playback_start_ticks = tonumber(scalar_field(payload, "PlaybackStartTimeTicks")) or 0
	local prior_session_state = playback_session_state_by_key[session_key]

	-- Ignore late reports from older playbacks after a track switch.
	if
		prior_session_state
		and playback_start_ticks > 0
		and prior_session_state.playback_start_ticks ~= nil
		and prior_session_state.playback_start_ticks > 0
		and playback_start_ticks < prior_session_state.playback_start_ticks
	then
		return true, nil
	end

	if
		prior_session_state
		and prior_session_state.track_id ~= item_id
		and not is_terminal_playback_state(normalized_state)
	then
		-- A track gone since has no playback left to stop.
		local prior_track_id = prior_session_state.track_id
		if module.resolve_track_for_playback(prior_track_id) then
			local _, stop_error = playbacks.report_session({
				session_key = session_key :: string,
				track_id = prior_track_id,
				position_ms = prior_session_state.position_ms,
				duration_ms = prior_session_state.duration_ms,
				state = "stopped",
				event = "progress",
			})
			if stop_error then
				return false, stop_error
			end
		end
	end

	local _, report_error = playbacks.report_session({
		session_key = session_key :: string,
		track_id = item_id,
		position_ms = position_ms,
		duration_ms = duration_ms,
		state = normalized_state,
		event = as_playback_event(normalized_event_name),
	})

	-- A stop ends the session's playback when it names the item playing, even when the server
	-- rejects it.
	if is_terminal_playback_state(normalized_state) then
		local current = playback_session_state_by_key[session_key]
		if current and current.track_id == item_id then
			forget_session_state(session_key)
		end
	end
	if report_error then
		return false, report_error
	end

	if not is_terminal_playback_state(normalized_state) then
		if playback_session_state_by_key[session_key] == nil then
			evict_session_states()
			playback_session_state_count += 1
		end
		playback_session_state_by_key[session_key] = {
			user_id = auth_context.principal.user_id,
			session_info = reporter.session_info,
			via_socket = reporter.via_socket,
			track_id = item_id,
			playback_start_ticks = playback_start_ticks,
			position_ms = position_ms,
			duration_ms = duration_ms,
			is_paused = normalized_state == "paused",
			reported_s = os.time(),
		}
	end

	return true, nil
end

-- Drops what a session reported, e.g. once its socket is gone and nothing will report a stop.
function module.forget_client_playback(session_key: string)
	forget_session_state(session_key)
end

function module.list_client_playbacks(): { [string]: ClientPlayback }
	local oldest_s = os.time() - CLIENT_PLAYBACK_IDLE_SECONDS
	local result: { [string]: ClientPlayback } = {}
	for session_key, state in playback_session_state_by_key do
		if state.reported_s >= oldest_s then
			result[session_key] = state
		end
	end
	return result
end

function module.resolve_playback_info_item(ctx: ApiContext?): (string?, Track?, ApiResponse?)
	local params = if ctx then ctx.params else nil
	local raw_item_id = if params then params.id else nil
	if not raw_item_id then
		return nil, nil, module.bad_request("missing item id")
	end

	local track = module.resolve_track_for_playback(module.decode_jellyfin_item_id(raw_item_id))
	if not track then
		return nil, nil, module.not_found("item not found")
	end

	return track.id, track, nil
end

function module.resolve_requested_play_session_id(request: ApiRequest?, request_json: PlaybackPayload?): string?
	local play_session_id = module.trim(
		if scalar_field(request_json, "PlaySessionId") ~= nil
			then tostring(scalar_field(request_json, "PlaySessionId"))
			else nil
	)
	if play_session_id then
		return play_session_id
	end

	local query = if request then request.query else nil
	return module.parse_query_string(query, { "PlaySessionId", "playSessionId" }, nil)
end

function module.resolve_requested_media_source_id(request: ApiRequest?, request_object: JsonObject?): string?
	local query = if request then request.query else nil
	local media_source_id = module.parse_query_string(query, { "MediaSourceId", "mediaSourceId" }, nil)
	if media_source_id then
		return media_source_id
	end

	return json_string_field(request_object, "MediaSourceId")
end

function module.build_playback_info(
	item_id: string?,
	track: Track?,
	existing_play_session_id: string?,
	request_object: JsonObject?,
	requested_media_source_id: string?,
	access_token: string?,
	device_id: string?
): (ApiJsonValue, string?)
	local source = module.resolve_playback_source_for_track(track, true)
	if source == nil then
		return no_compatible_stream(), nil
	end

	local has_lyrics = false
	if track then
		local ok, value = pcall(lyrics.has, track.id)
		has_lyrics = ok and value == true
	end
	local media_sources, _ =
		build_media_info(track, source, item_id, if track then track.track_title else item_id, has_lyrics)
	local primary_media_source = media_sources[1]
	if requested_media_source_id ~= nil then
		local primary_media_source_id = module.trim(if primary_media_source then primary_media_source.Id else nil)
		if
			primary_media_source_id == nil
			or string.lower(primary_media_source_id) ~= string.lower(requested_media_source_id)
		then
			return no_compatible_stream(), nil
		end
	end

	-- As upstream, a playable item always lists its source, and only a device profile tailors it.
	local play_session_id = ensure_playback_session_id(existing_play_session_id)
	local device_profile = json_object_field(request_object, "DeviceProfile")
	local context: UrlContext = {
		play_session_id = play_session_id,
		request_object = request_object,
		access_token = access_token,
		device_id = device_id,
		device_profile_id = device_profile_id(device_profile),
		etag = if primary_media_source then primary_media_source.ETag else nil,
	}
	local stream: StreamInfo? = nil
	if primary_media_source and device_profile then
		local err
		stream, err = apply_device_profile(primary_media_source, device_profile, context)
		if err then
			return nil, err
		end
	end

	-- A trimmed source plays only by transcoding. Where upstream's answer would offer no way to,
	-- without a device profile or a streaming one, it gets the URL upstream hands a client whose
	-- streaming profiles name no codec it encodes: the stream route's default encode. A client
	-- that turned transcoding off is left without one.
	local primary_media_source_id = module.trim(if primary_media_source then primary_media_source.Id else nil)
	if
		primary_media_source
		and primary_media_source_id
		and media.is_trimmed(source)
		and primary_media_source.TranscodingUrl == nil
		and json_boolean_field(request_object, "EnableTranscoding") ~= false
	then
		primary_media_source.SupportsTranscoding = true
		primary_media_source.TranscodingSubProtocol = "http"
		primary_media_source.TranscodingUrl =
			transcoding_url(primary_media_source_id, stream or new_stream_info(), context)
	end
	return {
		MediaSources = media_sources,
		PlaySessionId = play_session_id,
		ErrorCode = nil,
	}, nil
end

local function lookup_track(lookup: ({ string }) -> { [string]: Track }, item_id: string?): Track?
	if not item_id then
		return nil
	end

	-- Typed-to-Track-node: release/artist ids resolve to nil, not the first related track.
	local ok, result = pcall(lookup, { item_id })
	if not ok or type(result) ~= "table" then
		return nil
	end
	return (result :: { [string]: Track })[item_id]
end

function module.resolve_track_for_playback(item_id: string?): Track?
	return lookup_track(tracks.get, item_id)
end

-- Reads as the system for public streaming routes, which must serve nothing from it but the audio.
function module.resolve_track_for_public_stream(item_id: string?): Track?
	return lookup_track(tracks.streamable, item_id)
end

return module
