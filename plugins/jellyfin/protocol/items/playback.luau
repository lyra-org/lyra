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

local JELLYFIN_PLUGIN_ID = constants.JELLYFIN_PLUGIN_ID
local PLAYBACK_COMPLETION_TOLERANCE_MS = constants.PLAYBACK_COMPLETION_TOLERANCE_MS
local ROUTE_PREFIX = constants.ROUTE_PREFIX

local base = require("./common")

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
type JsonArray = { ApiJsonValue }

export type PlaybackPayload = { [string]: string | number | boolean? }

type PlaybackSessionState = {
	track_id: number,
	playback_start_ticks: number,
	position_ms: number?,
	duration_ms: number?,
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
	DefaultAudioStreamIndex: number,
	HasSegments: boolean,
	ETag: string?,
	DirectStreamUrl: string?,
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

local module = {}
module.trim = base.trim
module.decode_jellyfin_item_id = base.decode_jellyfin_item_id
module.bad_request = base.bad_request
module.not_found = base.not_found
module.parse_query_string = base.parse_query_string
module.resolve_playback_source_for_track = base.resolve_playback_source_for_track
module.encode_jellyfin_item_id = base.encode_jellyfin_item_id
module.extension_from_path = base.extension_from_path

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

local function as_json_array(value: ApiJsonValue?): JsonArray?
	if type(value) ~= "table" then
		return nil
	end
	return value :: JsonArray
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

local function resolve_playback_session_key(
	auth_context: AuthContext,
	payload: PlaybackPayload?,
	request_device_id: string?
): string?
	local principal_session_id = if auth_context then auth_context.credential.session_id else nil
	if principal_session_id ~= nil and principal_session_id > 0 then
		return string.format("auth:%d", principal_session_id)
	end

	local principal_user_id = if auth_context then auth_context.principal.user_id else nil
	if principal_user_id ~= nil and principal_user_id > 0 and request_device_id then
		return string.format("device:%d:%s", principal_user_id, request_device_id)
	end

	local session_id = module.trim(
		if scalar_field(payload, "SessionId") ~= nil then tostring(scalar_field(payload, "SessionId")) else nil
	)
	if session_id then
		return string.format("session:%s", session_id)
	end

	local play_session_id = module.trim(
		if scalar_field(payload, "PlaySessionId") ~= nil then tostring(scalar_field(payload, "PlaySessionId")) else nil
	)
	if play_session_id then
		return string.format("play:%s", play_session_id)
	end

	return nil
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

local function json_array_field(object: JsonObject?, key: string): JsonArray?
	return as_json_array(if object then object[key] else nil)
end

local function csv_contains_token(csv: string?, token: string?): boolean
	local normalized_token = if token then string.lower(token) else nil
	if normalized_token == nil then
		return false
	end

	for raw_part in string.gmatch(csv or "", "[^,]+") do
		local part = module.trim(raw_part)
		if part and string.lower(part) == normalized_token then
			return true
		end
	end

	return false
end

local function codec_for_extension(extension: string?): string?
	if extension == "flac" then
		return "flac"
	end
	if extension == "mp3" then
		return "mp3"
	end
	if extension == "aac" or extension == "m4a" or extension == "mp4" then
		return "aac"
	end
	if extension == "opus" then
		return "opus"
	end
	if extension == "ogg" then
		return "vorbis"
	end
	if extension == "wav" then
		return "pcm_s16le"
	end
	if extension == "alac" then
		return "alac"
	end

	return nil
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

local function supports_audio_direct_play(media_source: MediaSource, device_profile: JsonObject?): boolean
	local direct_play_profiles = json_array_field(device_profile, "DirectPlayProfiles")
	if direct_play_profiles == nil then
		return media_source.SupportsDirectPlay
	end

	local source_container = media_source.Container
	local source_codec = media_source_audio_codec(media_source)
	for _, raw_profile in ipairs(direct_play_profiles) do
		local profile = as_json_object(raw_profile)
		if profile then
			local profile_type = json_string_field(profile, "Type")
			if profile_type and string.lower(profile_type) == "audio" then
				local profile_container = json_string_field(profile, "Container")
				if csv_contains_token(profile_container, source_container) then
					local profile_audio_codec = json_string_field(profile, "AudioCodec")
					if profile_audio_codec == nil or csv_contains_token(profile_audio_codec, source_codec) then
						return true
					end
				end
			end
		end
	end

	return false
end

local function first_audio_stream(media_source: MediaSource): MediaStream?
	for _, stream in media_source.MediaStreams do
		if stream.Type == "Audio" then
			return stream
		end
	end

	return nil
end

local function request_audio_playback_limits(request_object: JsonObject?): AudioPlaybackLimits
	return {
		max_bitrate_bps = json_number_field(request_object, "MaxStreamingBitrate"),
		max_sample_rate_hz = nil,
		max_bit_depth = nil,
		max_channels = json_number_field(request_object, "MaxAudioChannels"),
	}
end

local function media_source_respects_audio_limits(media_source: MediaSource, limits: AudioPlaybackLimits): boolean
	local max_bitrate_bps = limits.max_bitrate_bps
	local max_sample_rate_hz = limits.max_sample_rate_hz
	local max_bit_depth = limits.max_bit_depth
	local max_channels = limits.max_channels
	local bitrate = media_source.Bitrate
	if max_bitrate_bps ~= nil and bitrate ~= nil and bitrate > max_bitrate_bps then
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
				csv_contains_token(container, source_container)
				and (codec_csv == nil or csv_contains_token(codec_csv, source_codec))
			then
				return true
			end
		end
	end

	return false
end

local function preferred_audio_streaming_profile(device_profile: JsonObject?): JsonObject?
	local transcoding_profiles = json_array_field(device_profile, "TranscodingProfiles")
	if transcoding_profiles == nil then
		return nil
	end

	for _, raw_profile in ipairs(transcoding_profiles) do
		local profile = as_json_object(raw_profile)
		if profile then
			local profile_type = json_string_field(profile, "Type")
			local profile_context = json_string_field(profile, "Context")
			if
				profile_type
				and profile_context
				and string.lower(profile_type) == "audio"
				and string.lower(profile_context) == "streaming"
			then
				return profile
			end
		end
	end

	return nil
end

local function request_supports_audio_direct_play(media_source: MediaSource, request_object: JsonObject?): boolean
	if json_boolean_field(request_object, "EnableDirectPlay") == false then
		return false
	end

	local limits = request_audio_playback_limits(request_object)
	local device_profile = json_object_field(request_object, "DeviceProfile")
	if device_profile == nil then
		return media_source.SupportsDirectPlay and media_source_respects_audio_limits(media_source, limits)
	end

	return supports_audio_direct_play(media_source, device_profile)
		and media_source_respects_audio_limits(media_source, limits)
end

local function requested_audio_stream_is_supported(request_object: JsonObject?): boolean
	local requested_audio_stream_index = json_number_field(request_object, "AudioStreamIndex")
	return requested_audio_stream_index == nil or requested_audio_stream_index == 0
end

local function no_compatible_stream(): ApiJsonValue
	return {
		MediaSources = {},
		PlaySessionId = nil,
		ErrorCode = "NoCompatibleStream",
	}
end

local function build_hls_transcoding_url(
	item_id: string,
	play_session_id: string,
	codec: string,
	audio_bitrate: number?,
	max_streaming_bitrate: number?,
	max_audio_channels: number?,
	enable_audio_vbr_encoding: boolean?,
	start_time_ticks: number?
): string
	local parts = {
		`audioCodec={codec}`,
		`PlaySessionId={play_session_id}`,
	}
	if audio_bitrate ~= nil then
		table.insert(parts, `audioBitRate={audio_bitrate}`)
	end
	if max_streaming_bitrate ~= nil then
		table.insert(parts, `MaxStreamingBitrate={max_streaming_bitrate}`)
	end
	if max_audio_channels ~= nil then
		table.insert(parts, `MaxAudioChannels={max_audio_channels}`)
	end
	if enable_audio_vbr_encoding ~= nil then
		table.insert(parts, `EnableAudioVbrEncoding={tostring(enable_audio_vbr_encoding)}`)
	end
	if start_time_ticks ~= nil and start_time_ticks > 0 then
		table.insert(parts, `StartTimeTicks={start_time_ticks}`)
	end
	return `{ROUTE_PREFIX}/Audio/{item_id}/master.m3u8?{table.concat(parts, "&")}`
end

local function apply_preferred_hls_transcoding(
	media_source: MediaSource,
	play_session_id: string,
	request_object: JsonObject?
): boolean
	if request_object == nil or json_boolean_field(request_object, "EnableTranscoding") == false then
		return false
	end

	local device_profile = json_object_field(request_object, "DeviceProfile")
	if device_profile == nil then
		return false
	end

	local preferred_profile = preferred_audio_streaming_profile(device_profile)
	if preferred_profile == nil then
		return false
	end

	local protocol = json_string_field(preferred_profile, "Protocol")
	if protocol == nil or string.lower(protocol) ~= "hls" then
		return false
	end

	local direct_play_supported = request_supports_audio_direct_play(media_source, request_object)
	if direct_play_supported then
		return false
	end

	local media_source_id = module.trim(media_source.Id)
	local codec = string.lower(json_string_field(preferred_profile, "AudioCodec") or "aac")
	local transcoding_container = string.lower(json_string_field(preferred_profile, "Container") or "ts")
	if media_source_id == nil then
		return false
	end

	media_source.SupportsDirectPlay = false
	media_source.SupportsDirectStream = false
	media_source.SupportsTranscoding = true
	media_source.TranscodingContainer = transcoding_container
	media_source.TranscodingSubProtocol = "hls"
	media_source.TranscodingUrl = build_hls_transcoding_url(
		media_source_id,
		play_session_id,
		codec,
		json_number_field(request_object, "AudioBitRate"),
		json_number_field(request_object, "MaxStreamingBitrate"),
		json_number_field(request_object, "MaxAudioChannels"),
		json_boolean_field(preferred_profile, "EnableAudioVbrEncoding"),
		json_number_field(request_object, "StartTimeTicks")
	)
	return true
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
	local container = module.extension_from_path(if entry then entry.full_path else nil)
	local codec = codec_for_extension(container)
	local size = if entry and entry.size then tonumber(entry.size) else nil
	local bitrate = if track then track.bitrate_bps else nil
	local full_path = if entry then module.trim(entry.full_path) else nil
	local direct_stream_url = if full_path and media_item_id then `{ROUTE_PREFIX}/Items/{media_item_id}/File` else nil

	local media_stream: MediaStream = {
		Codec = codec,
		Type = "Audio",
		Index = 0,
		Channels = if track then track.channel_count else nil,
		SampleRate = if track then track.sample_rate_hz else nil,
		BitDepth = if track then track.bit_depth else nil,
		IsDefault = true,
		IsForced = false,
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
		SupportsTranscoding = false,
		SupportsDirectStream = direct_stream_url ~= nil,
		SupportsDirectPlay = direct_stream_url ~= nil,
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
	if direct_stream_url then
		media_source.DirectStreamUrl = direct_stream_url
	end

	return { media_source }, media_streams, container
end

function module.build_audio_media_source(item_id: string?, track: Track?): MediaSource?
	local source = module.resolve_playback_source_for_track(track, true)
	local has_lyrics = false
	local track_db_id = if track then track.db_id else nil
	if track_db_id then
		local ok, value = pcall(lyrics.has, track_db_id)
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
	if media_source == nil or enable_direct_play == false or forbid_stream_copy == true then
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
	if payload_user_id and module.decode_jellyfin_item_id(payload_user_id) ~= auth_context.principal.user_id then
		return false
	end

	return true
end

function module.report_playback_event(
	auth_context: AuthContext,
	payload: PlaybackPayload?,
	event_name: string,
	request_device_id: string?
): (boolean, ApiResponse?)
	local raw_item_id = scalar_field(payload, "ItemId")
	local item_id = module.decode_jellyfin_item_id(if type(raw_item_id) == "string" then raw_item_id else nil)
	if item_id == nil then
		return false, module.bad_request("invalid or missing playback item id")
	end

	local session_key = resolve_playback_session_key(auth_context, payload, request_device_id)
	if not session_key then
		return false, module.bad_request("missing or invalid playback session key")
	end

	local position_ms = ticks_to_ms(scalar_field(payload, "PositionTicks"))
	local duration_ms = ticks_to_ms(scalar_field(payload, "RunTimeTicks"))
	if duration_ms == nil then
		local track = module.resolve_track_for_playback(item_id)
		duration_ms = if track then track.duration_ms else nil
	end
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
		and prior_session_state.track_id ~= nil
		and prior_session_state.track_id ~= item_id
		and not is_terminal_playback_state(normalized_state)
	then
		playbacks.report_session({
			plugin_id = JELLYFIN_PLUGIN_ID,
			user_id = auth_context.principal.user_id,
			session_key = session_key :: string,
			track_id = prior_session_state.track_id,
			position_ms = prior_session_state.position_ms,
			duration_ms = prior_session_state.duration_ms,
			state = "stopped",
			event = "progress",
		})
	end

	playbacks.report_session({
		plugin_id = JELLYFIN_PLUGIN_ID,
		user_id = auth_context.principal.user_id,
		session_key = session_key :: string,
		track_id = item_id,
		position_ms = position_ms,
		duration_ms = duration_ms,
		state = normalized_state,
		event = as_playback_event(normalized_event_name),
	})

	if is_terminal_playback_state(normalized_state) then
		local current_session_state = playback_session_state_by_key[session_key]
		if current_session_state and current_session_state.track_id == item_id then
			playback_session_state_by_key[session_key] = nil
			playback_session_state_count -= 1
		end
	else
		if playback_session_state_by_key[session_key] == nil then
			if playback_session_state_count >= MAX_PLAYBACK_SESSION_STATES then
				playback_session_state_by_key = {}
				playback_session_state_count = 0
			end
			playback_session_state_count += 1
		end
		playback_session_state_by_key[session_key] = {
			track_id = item_id,
			playback_start_ticks = playback_start_ticks,
			position_ms = position_ms,
			duration_ms = duration_ms,
		}
	end

	return true, nil
end

function module.resolve_playback_info_item(ctx: ApiContext?): (string?, Track?, ApiResponse?)
	local params = if ctx then ctx.params else nil
	local raw_item_id = if params then params.id else nil
	if not raw_item_id then
		return nil, nil, module.bad_request("missing item id")
	end

	local db_id = module.decode_jellyfin_item_id(raw_item_id)
	local track = module.resolve_track_for_playback(db_id)
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
	requested_media_source_id: string?
): ApiJsonValue
	local source = module.resolve_playback_source_for_track(track, true)
	if source == nil then
		return no_compatible_stream()
	end

	local has_lyrics = false
	local track_db_id = if track then track.db_id else nil
	if track_db_id then
		local ok, value = pcall(lyrics.has, track_db_id)
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
			return no_compatible_stream()
		end
	end
	if not requested_audio_stream_is_supported(request_object) then
		return no_compatible_stream()
	end

	local play_session_id = ensure_playback_session_id(existing_play_session_id)
	local direct_play_supported = if primary_media_source
		then request_supports_audio_direct_play(primary_media_source, request_object)
		else false
	if primary_media_source then
		if not direct_play_supported then
			primary_media_source.SupportsDirectPlay = false
			primary_media_source.SupportsDirectStream = false
		end
		local transcoding_applied =
			apply_preferred_hls_transcoding(primary_media_source, play_session_id, request_object)
		if request_object ~= nil and not direct_play_supported and not transcoding_applied then
			return no_compatible_stream()
		end
	end
	return {
		MediaSources = media_sources,
		PlaySessionId = play_session_id,
		ErrorCode = nil,
	}
end

function module.resolve_track_for_playback(item_id: number?): Track?
	if not item_id or item_id <= 0 then
		return nil
	end

	-- Typed-to-Track-node: release/artist db_ids resolve to nil, not the first related track.
	local ok, result = pcall(tracks.get_by_ids, { item_id })
	if not ok or type(result) ~= "table" then
		return nil
	end
	return (result :: { [number]: Track })[item_id]
end

return module
