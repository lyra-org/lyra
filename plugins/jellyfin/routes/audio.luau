--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")
local lyrics = require("@lyra/lyrics")
local tracks = require("@lyra/tracks")

local constants = require("../constants")
local encoder = require("../protocol/items/encoder")
local item_ids = require("../protocol/items/identifiers")
local lyrics_protocol = require("../protocol/lyrics")
local media = require("../protocol/items/media")
local playback = require("../protocol/items/playback")
local requests = require("../protocol/items/requests")
local responses = require("../protocol/items/responses")
local session = require("../protocol/session")

local P = constants.ROUTE_PREFIX

type ApiContext = api.ApiContext
type ApiRequest = api.ApiRequest
type ApiQueryParams = api.ApiQueryParams
type ApiResponse = api.ApiResponse
type LyricsInfo = lyrics.LyricsInfo
type Track = tracks.Track

local module = {}

local function extract_integer_param(raw: string?): (number?, string?)
	if type(raw) ~= "string" or #raw == 0 then
		return nil, nil
	end
	local value = tonumber(raw)
	if value == nil or value ~= value then
		return nil, `must be an integer; got "{raw}"`
	end
	if value < 0 then
		return nil, `must not be negative; got {raw}`
	end
	return math.floor(value), nil
end

local function jellyfin_ticks_to_ms(ticks: number?): number?
	if ticks == nil then
		return nil
	end

	return math.floor(ticks / 10000)
end

local function requested_media_source_matches_item(request: ApiRequest?, item_id: string): boolean
	local requested_media_source_id = playback.resolve_requested_media_source_id(request, nil)
	return requested_media_source_id == nil or string.lower(requested_media_source_id) == string.lower(item_id)
end

local function parse_preferred_csv_values(raw: string?): { string }?
	if type(raw) ~= "string" then
		return nil
	end

	local values: { string } = {}
	for token in string.gmatch(raw, "[^,]+") do
		local trimmed = requests.trim(token)
		if trimmed and #trimmed > 0 then
			table.insert(values, string.lower(trimmed))
		end
	end

	return if #values > 0 then values else nil
end

local function parse_first_csv_value(raw: string?): string?
	local values = parse_preferred_csv_values(raw)
	return if values then values[1] else nil
end

-- The HLS codec strings of the codecs the server segments, as upstream's `HlsCodecStringHelpers`.
local AUDIO_HLS_CODEC_STRINGS: { [string]: string } = {
	aac = "mp4a.40.2",
	alac = "alac",
	flac = "fLaC",
	mp3 = "mp4a.40.34",
	opus = "Opus",
}

local LOSSY_AUDIO_HLS_CODECS: { [string]: boolean } = {
	aac = true,
	mp3 = true,
	opus = true,
}

local function is_supported_audio_hls_codec(codec: string): boolean
	return AUDIO_HLS_CODEC_STRINGS[codec] ~= nil
end

local function find_supported_audio_hls_codec(preferred_codecs: { string }?): string?
	if preferred_codecs == nil then
		return nil
	end

	for _, codec in preferred_codecs do
		if is_supported_audio_hls_codec(codec) then
			return codec
		end
	end

	return nil
end

local function is_hls_transcoding_protocol(raw: string?): boolean
	local value = requests.trim(raw)
	return value ~= nil and string.lower(value) == "hls"
end

local function jellyfin_boolean(raw: string?): boolean?
	if type(raw) ~= "string" then
		return nil
	end
	local lower = string.lower(raw)
	if lower == "true" or lower == "1" then
		return true
	end
	if lower == "false" or lower == "0" then
		return false
	end
	return nil
end

-- Upstream names the codec a container carries when a request leaves the codec out.
local function infer_audio_codec(container: string?): string
	if
		container == "ogg"
		or container == "oga"
		or container == "ogv"
		or container == "webm"
		or container == "webma"
	then
		return "opus"
	end
	if
		container == "m4a"
		or container == "m4b"
		or container == "mp4"
		or container == "mov"
		or container == "mkv"
		or container == "mka"
	then
		return "aac"
	end
	if container == "ts" or container == "avi" or container == "flv" or container == "f4v" or container == "swf" then
		return "mp3"
	end
	if
		container == "aac"
		or container == "ac3"
		or container == "alac"
		or container == "dts"
		or container == "eac3"
		or container == "flac"
		or container == "mp2"
		or container == "mp3"
		or container == "opus"
		or container == "truehd"
		or container == "vorbis"
	then
		return container
	end
	return "aac"
end

-- `/universal` takes the client's direct-play containers and falls back to its transcoding settings.
function module.handle_universal_route(ctx: ApiContext): ApiResponse
	local query = if ctx and ctx.request then ctx.request.query else nil
	local request = if ctx then ctx.request else nil
	local raw_static = requests.parse_query_string(query, { "static", "Static" }, nil)
	local raw_container = requests.parse_query_string(query, { "container", "Container" }, nil)
	local static_direct_copy = jellyfin_boolean(raw_static) == true

	local raw_codec = requests.parse_query_string(query, { "audioCodec", "AudioCodec" }, nil)
	local raw_transcoding_container =
		requests.parse_query_string(query, { "transcodingContainer", "TranscodingContainer" }, nil)
	local raw_max_bitrate = requests.parse_query_string(query, { "MaxStreamingBitrate", "maxStreamingBitrate" }, nil)
	local raw_audio_bitrate =
		requests.parse_query_string(query, { "audioBitRate", "AudioBitRate", "AudioBitrate" }, nil)
	local raw_sample_rate = requests.parse_query_string(query, { "audioSampleRate", "AudioSampleRate" }, nil)
	local raw_bit_depth = requests.parse_query_string(query, { "maxAudioBitDepth", "MaxAudioBitDepth" }, nil)
	local raw_channels = requests.parse_query_string(query, { "maxAudioChannels", "MaxAudioChannels" }, nil)
	local raw_auto_copy = requests.parse_query_string(query, { "enableAutoStreamCopy", "EnableAutoStreamCopy" }, nil)
	local raw_enable_direct_play = requests.parse_query_string(query, { "enableDirectPlay", "EnableDirectPlay" }, nil)
	local raw_enable_transcoding = requests.parse_query_string(query, { "enableTranscoding", "EnableTranscoding" }, nil)
	local raw_transcoding_protocol =
		requests.parse_query_string(query, { "transcodingProtocol", "TranscodingProtocol" }, nil)
	local raw_start_ticks = requests.parse_query_string(query, { "startTimeTicks", "StartTimeTicks" }, nil)

	local transcoding_container = parse_first_csv_value(raw_transcoding_container)
	local preferred_codecs = parse_preferred_csv_values(raw_codec)
	local hls_codec = find_supported_audio_hls_codec(preferred_codecs)
	local hls_requested = is_hls_transcoding_protocol(raw_transcoding_protocol)
	local max_bitrate_value, max_bitrate_err = extract_integer_param(raw_max_bitrate)
	if max_bitrate_err then
		return responses.bad_request(`MaxStreamingBitrate {max_bitrate_err}`)
	end
	local audio_bitrate_value, audio_bitrate_err = extract_integer_param(raw_audio_bitrate)
	if audio_bitrate_err then
		return responses.bad_request(`audioBitRate {audio_bitrate_err}`)
	end
	local bitrate_bps = audio_bitrate_value or max_bitrate_value

	local sample_rate_hz, sample_rate_err = extract_integer_param(raw_sample_rate)
	if sample_rate_err then
		return responses.bad_request(`audioSampleRate {sample_rate_err}`)
	end
	local bit_depth, bit_depth_err = extract_integer_param(raw_bit_depth)
	if bit_depth_err then
		return responses.bad_request(`maxAudioBitDepth {bit_depth_err}`)
	end
	local channels, channels_err = extract_integer_param(raw_channels)
	if channels_err then
		return responses.bad_request(`maxAudioChannels {channels_err}`)
	end
	local start_ticks, start_ticks_err = extract_integer_param(raw_start_ticks)
	if start_ticks_err then
		return responses.bad_request(`StartTimeTicks {start_ticks_err}`)
	end
	local start_offset_ms = jellyfin_ticks_to_ms(start_ticks)
	local forbid_stream_copy = jellyfin_boolean(raw_auto_copy) == false
	local direct_play_enabled = jellyfin_boolean(raw_enable_direct_play) ~= false
	local transcoding_enabled = jellyfin_boolean(raw_enable_transcoding) ~= false

	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	local params = if ctx then ctx.params else nil
	local item_id = if params then params.itemId else nil
	if not item_id then
		return responses.bad_request("missing item id")
	end
	if not requested_media_source_matches_item(request, item_id) then
		return responses.not_found("media source not found")
	end

	local track = playback.resolve_track_for_playback(item_ids.decode_jellyfin_item_id(item_id))
	if not track then
		return responses.not_found("item not found")
	end
	local track_id = track.id

	local source = media.resolve_playback_source_for_track(track)
	local source_channels = if source then source.channel_count else nil
	-- As upstream, only the maximum streaming bitrate, or its profile's 8 Mbps default, limits direct
	-- play; the audio bitrate only sets the encode's.
	local media_source = playback.build_audio_media_source(track.id, track, false)
	if
		playback.audio_supports_direct_play_from_container_csv(
			media_source,
			raw_container,
			max_bitrate_value or playback.DEFAULT_MAX_STREAMING_BITRATE,
			sample_rate_hz,
			bit_depth,
			channels,
			direct_play_enabled,
			forbid_stream_copy
		)
	then
		return api.response.stream_track(track_id)
	end

	if not transcoding_enabled then
		if static_direct_copy then
			return api.response.stream_track(track_id)
		end
		return api.response.json(415, {
			Message = "transcoding is not supported; request with static=true or supply transcode parameters",
		})
	end

	if hls_requested then
		if preferred_codecs ~= nil and hls_codec == nil then
			return responses.bad_request("unsupported hls codec; supported values: aac, alac, flac, mp3, opus")
		end
		local settings = encoder.settings(
			query,
			{ max_audio_channels = channels },
			preferred_codecs or { "aac" },
			bitrate_bps,
			source_channels,
			true
		)
		local options: api.HlsServeOptions = {
			preferred_codecs = preferred_codecs,
			bitrate_bps = settings.bitrate_bps,
			sample_rate_hz = sample_rate_hz,
			channels = settings.channels,
			start_offset_ms = start_offset_ms,
		}
		return api.response.hls_playlist(track_id, options)
	end

	local effective_transcoding_container = transcoding_container
	local effective_preferred_codecs = if preferred_codecs
		then preferred_codecs
		elseif transcoding_container == nil then { "copy", "mp3" }
		else nil
	if preferred_codecs == nil and transcoding_container == nil then
		effective_transcoding_container = "mp3"
	end

	local settings = encoder.settings(
		query,
		{ max_audio_channels = channels, audio_channels = channels },
		effective_preferred_codecs or { infer_audio_codec(effective_transcoding_container) },
		bitrate_bps,
		source_channels,
		false
	)
	local options: api.TrackStreamOptions = {
		format = effective_transcoding_container,
		preferred_codecs = effective_preferred_codecs,
		bitrate_bps = settings.bitrate_bps,
		sample_rate_hz = sample_rate_hz,
		channels = settings.channels,
		start_offset_ms = start_offset_ms,
	}
	return api.response.stream_track(track_id, options)
end

-- Upstream's output container for a codec when a request names none; otherwise the server picks.
local CONTAINER_BY_AUDIO_CODEC: { [string]: string } = {
	aac = "aac",
	mp3 = "mp3",
	vorbis = "ogg",
	wma = "wma",
}

local function parse_requested_channels(query: ApiQueryParams?): (encoder.ChannelRequest?, ApiResponse?)
	local values: { [string]: number? } = {}
	for _, name in { "maxAudioChannels", "audioChannels", "transcodingMaxAudioChannels" } do
		local value, err = extract_integer_param(requests.parse_query_string(query, { name }, nil))
		if err then
			return nil, responses.bad_request(`{name} {err}`)
		end
		values[name] = value
	end
	return {
		max_audio_channels = values.maxAudioChannels,
		audio_channels = values.audioChannels,
		transcoding_max_audio_channels = values.transcodingMaxAudioChannels,
	},
		nil
end

-- Upstream's `/stream` and `/stream.{container}`: `static=true` sends the original file, and
-- anything else encodes into `container` as the audio parameters ask, even audio the client could
-- take as it is. Encodes use a constant bitrate: upstream's `enableAudioVbrEncoding` only applies
-- once a server encoding option it leaves off by default is on. Public, as upstream: any
-- caller streams any track, though one who sends a valid token streams as its user.
function module.handle_stream_route(ctx: ApiContext): ApiResponse
	session.adopt_optional_auth(ctx)
	local request = if ctx then ctx.request else nil
	local query = if request then request.query else nil
	local params = if ctx then ctx.params else nil

	local raw_audio_bitrate = requests.parse_query_string(query, { "audioBitRate" }, nil)
	local bitrate_bps, bitrate_err = extract_integer_param(raw_audio_bitrate)
	if bitrate_err then
		return responses.bad_request(`audioBitRate {bitrate_err}`)
	end
	local sample_rate_hz, sample_rate_err =
		extract_integer_param(requests.parse_query_string(query, { "audioSampleRate" }, nil))
	if sample_rate_err then
		return responses.bad_request(`audioSampleRate {sample_rate_err}`)
	end
	local _, bit_depth_err = extract_integer_param(requests.parse_query_string(query, { "maxAudioBitDepth" }, nil))
	if bit_depth_err then
		return responses.bad_request(`maxAudioBitDepth {bit_depth_err}`)
	end
	local channel_request, channels_err = parse_requested_channels(query)
	if channel_request == nil then
		return channels_err or responses.bad_request("invalid channels")
	end
	local start_ticks, start_ticks_err =
		extract_integer_param(requests.parse_query_string(query, { "startTimeTicks" }, nil))
	if start_ticks_err then
		return responses.bad_request(`startTimeTicks {start_ticks_err}`)
	end

	local item_id = if params then params.itemId else nil
	if not item_id then
		return responses.bad_request("missing item id")
	end
	if not requested_media_source_matches_item(request, item_id) then
		return responses.not_found("media source not found")
	end
	local track = playback.resolve_track_for_public_stream(item_ids.decode_jellyfin_item_id(item_id))
	if track == nil then
		return responses.not_found("item not found")
	end
	local track_id = track.id

	if jellyfin_boolean(requests.parse_query_string(query, { "static" }, nil)) == true then
		return api.response.stream_track(track_id, { public = true })
	end

	local path_container: string? = if params then params.container else nil
	local raw_container = path_container or requests.parse_query_string(query, { "container" }, nil)
	local container = if raw_container then string.lower(raw_container) else nil
	local codecs = parse_preferred_csv_values(requests.parse_query_string(query, { "audioCodec" }, nil))
		or { infer_audio_codec(container) }
	container = container or CONTAINER_BY_AUDIO_CODEC[codecs[1]]
	local source = media.resolve_public_playback_source(track)
	local source_channels = if source then source.channel_count else nil
	local settings = encoder.settings(query, channel_request, codecs, bitrate_bps, source_channels, false)

	return api.response.stream_track(track_id, {
		public = true,
		format = container,
		preferred_codecs = codecs,
		bitrate_bps = settings.bitrate_bps,
		sample_rate_hz = sample_rate_hz,
		channels = settings.channels,
		start_offset_ms = jellyfin_ticks_to_ms(start_ticks),
	})
end

type ResolvedAudioHlsPlaylistRequest = {
	track: Track,
	track_id: string,
	query: ApiQueryParams?,
	selected_codec: string,
	options: api.HlsServeOptions,
}

local DEFAULT_AUDIO_HLS_BANDWIDTH_BPS = 192000
local MIN_AUDIO_HLS_LOSSY_BANDWIDTH_BPS = 32000

local function encode_hls_query_component(value: string): string
	local encoded = string.gsub(value, "[^%w%-_%.~]", function(char)
		return string.format("%%%02X", string.byte(char))
	end)
	return encoded
end

local function build_audio_hls_master_query_string(query: ApiQueryParams?, codec: string): string
	local keys: { string } = {}
	if query then
		for key in query do
			local normalized_key = string.lower(key)
			if normalized_key ~= "audiocodec" and normalized_key ~= "codec" then
				table.insert(keys, key)
			end
		end
	end
	table.sort(keys)

	local parts: { string } = {
		`audioCodec={encode_hls_query_component(codec)}`,
	}
	for _, key in keys do
		local values = if query then query[key] else nil
		if values ~= nil then
			for _, value in values do
				table.insert(parts, `{encode_hls_query_component(key)}={encode_hls_query_component(value)}`)
			end
		end
	end

	return table.concat(parts, "&")
end

local function resolve_audio_hls_query_options(
	query: ApiQueryParams?,
	track: Track
): (api.HlsServeOptions?, string?, ApiResponse?)
	local raw_codec = requests.parse_query_string(query, { "audioCodec", "AudioCodec", "codec" }, nil)
	local preferred_codecs = parse_preferred_csv_values(raw_codec)
	local selected_codec = find_supported_audio_hls_codec(preferred_codecs)
	if preferred_codecs ~= nil and selected_codec == nil then
		return nil, nil, responses.bad_request("unsupported hls codec; supported values: aac, alac, flac, mp3, opus")
	end

	local raw_audio_bitrate =
		requests.parse_query_string(query, { "audioBitRate", "AudioBitRate", "AudioBitrate" }, nil)
	local raw_max_bitrate = requests.parse_query_string(query, { "maxStreamingBitrate", "MaxStreamingBitrate" }, nil)
	local raw_sample_rate = requests.parse_query_string(query, { "audioSampleRate", "AudioSampleRate" }, nil)
	local raw_start_ticks = requests.parse_query_string(query, { "startTimeTicks", "StartTimeTicks" }, nil)

	local requested_audio_bitrate, requested_audio_bitrate_err = extract_integer_param(raw_audio_bitrate)
	if requested_audio_bitrate_err then
		return nil, nil, responses.bad_request(`audioBitRate {requested_audio_bitrate_err}`)
	end
	local requested_max_bitrate, requested_max_bitrate_err = extract_integer_param(raw_max_bitrate)
	if requested_max_bitrate_err then
		return nil, nil, responses.bad_request(`MaxStreamingBitrate {requested_max_bitrate_err}`)
	end
	local sample_rate_hz, sample_rate_err = extract_integer_param(raw_sample_rate)
	if sample_rate_err then
		return nil, nil, responses.bad_request(`audioSampleRate {sample_rate_err}`)
	end
	local channel_request, channels_err = parse_requested_channels(query)
	if channel_request == nil then
		return nil, nil, channels_err
	end
	local start_ticks, start_ticks_err = extract_integer_param(raw_start_ticks)
	if start_ticks_err then
		return nil, nil, responses.bad_request(`StartTimeTicks {start_ticks_err}`)
	end
	local source = media.resolve_playback_source_for_track(track)
	local source_channels = if source then source.channel_count else nil
	local settings = encoder.settings(
		query,
		channel_request,
		{ selected_codec or "aac" },
		requested_audio_bitrate or requested_max_bitrate,
		source_channels,
		true
	)

	return {
		preferred_codecs = preferred_codecs,
		bitrate_bps = settings.bitrate_bps,
		sample_rate_hz = sample_rate_hz,
		channels = settings.channels,
		start_offset_ms = jellyfin_ticks_to_ms(start_ticks),
	},
		selected_codec or "aac",
		nil
end

local function resolve_audio_hls_variant_bandwidth(
	track: Track,
	options: api.HlsServeOptions,
	selected_codec: string
): number
	local codec = selected_codec
	local requested_bitrate = options.bitrate_bps
	local source = media.resolve_playback_source_for_track(track)
	local track_bitrate = if source and source.bitrate_bps and source.bitrate_bps > 0 then source.bitrate_bps else nil

	if LOSSY_AUDIO_HLS_CODECS[codec] then
		if requested_bitrate ~= nil then
			if track_bitrate ~= nil and requested_bitrate > track_bitrate then
				return DEFAULT_AUDIO_HLS_BANDWIDTH_BPS
			end
			if requested_bitrate < MIN_AUDIO_HLS_LOSSY_BANDWIDTH_BPS then
				return MIN_AUDIO_HLS_LOSSY_BANDWIDTH_BPS
			end
			return requested_bitrate
		end
		return DEFAULT_AUDIO_HLS_BANDWIDTH_BPS
	end

	return track_bitrate or DEFAULT_AUDIO_HLS_BANDWIDTH_BPS
end

local function build_audio_hls_master_playlist(
	track: Track,
	query: ApiQueryParams?,
	options: api.HlsServeOptions,
	selected_codec: string
): string
	local bandwidth = resolve_audio_hls_variant_bandwidth(track, options, selected_codec)
	local codec_string = AUDIO_HLS_CODEC_STRINGS[selected_codec]
	local stream_inf = `#EXT-X-STREAM-INF:BANDWIDTH={bandwidth},AVERAGE-BANDWIDTH={bandwidth}`
	if codec_string then
		stream_inf ..= `,CODECS="{codec_string}"`
	end

	local variant_uri = `main.m3u8?{build_audio_hls_master_query_string(query, selected_codec)}`
	return table.concat({
		"#EXTM3U",
		stream_inf,
		variant_uri,
		"",
	}, "\n")
end

local function resolve_audio_hls_playlist_request(ctx: ApiContext): (ResolvedAudioHlsPlaylistRequest?, ApiResponse?)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return nil, session.unauthorized()
	end
	if not session.require_matching_user_scope(ctx, principal) then
		return nil, session.unauthorized()
	end

	local params = if ctx then ctx.params else nil
	local item_id = if params then params.itemId else nil
	if not item_id then
		return nil, responses.bad_request("missing item id")
	end
	local request = if ctx then ctx.request else nil
	if not requested_media_source_matches_item(request, item_id) then
		return nil, responses.not_found("media source not found")
	end

	local track = playback.resolve_track_for_playback(item_ids.decode_jellyfin_item_id(item_id))
	if not track then
		return nil, responses.not_found("item not found")
	end
	local track_id = track.id

	local query = if ctx and ctx.request then ctx.request.query else nil
	local options, selected_codec, options_err = resolve_audio_hls_query_options(query, track)
	if options == nil then
		return nil, options_err or responses.bad_request("invalid hls options")
	end

	return {
		track = track,
		track_id = track_id,
		query = query,
		selected_codec = selected_codec or "aac",
		options = options,
	},
		nil
end

function module.handle_hls_master_route(ctx: ApiContext): ApiResponse
	local request, err = resolve_audio_hls_playlist_request(ctx)
	if request == nil then
		return err or responses.bad_request("invalid hls request")
	end

	return api.response.text(
		200,
		build_audio_hls_master_playlist(request.track, request.query, request.options, request.selected_codec),
		{
			["content-type"] = "application/x-mpegurl",
		}
	)
end

function module.handle_hls_main_route(ctx: ApiContext): ApiResponse
	local request, err = resolve_audio_hls_playlist_request(ctx)
	if request == nil then
		return err or responses.bad_request("invalid hls request")
	end

	return api.response.hls_playlist(request.track_id, request.options)
end

function module.register()
	api.get(P .. "/Audio/{itemId}/Lyrics", function(ctx: ApiContext): ApiResponse
		local track_id, item, resolve_err = lyrics_protocol.resolve_audio_item(ctx)
		if resolve_err then
			return resolve_err
		end
		if track_id == nil or item == nil then
			return responses.not_found("item not found")
		end
		local ok, lyric_info = pcall(lyrics.get, track_id, nil, false, nil)
		if not ok then
			return api.response.empty(500)
		end
		if lyric_info == nil then
			return responses.not_found("lyrics not found")
		end

		return api.response.json(200, lyrics_protocol.to_jellyfin_lyric_dto(item, lyric_info :: LyricsInfo))
	end, { auth = "public", case_insensitive = true })
	api.post(P .. "/Audio/{itemId}/Lyrics", function(ctx: ApiContext): ApiResponse
		local track_id, item, resolve_err = lyrics_protocol.resolve_audio_item(ctx)
		if resolve_err then
			return resolve_err
		end
		if track_id == nil or item == nil then
			return responses.not_found("item not found")
		end
		local body = if ctx and ctx.request then ctx.request.body_raw else nil
		if type(body) ~= "string" or #body == 0 then
			return responses.bad_request("No lyrics uploaded")
		end

		local content_type, content_type_err = lyrics_protocol.resolve_upload_content_type(ctx)
		if content_type_err then
			return content_type_err
		end
		if not content_type then
			return responses.bad_request("Extension is required on filename")
		end

		local ok, lyric_info = pcall(lyrics.upsert_personal, track_id, {
			content_type = content_type,
			body = body,
			language = nil,
		})
		if not ok then
			return responses.bad_request(tostring(lyric_info))
		end

		return api.response.json(200, lyrics_protocol.to_jellyfin_lyric_dto(item, lyric_info :: LyricsInfo))
	end, { auth = "public", case_insensitive = true })
	api.delete(P .. "/Audio/{itemId}/Lyrics", function(ctx: ApiContext): ApiResponse
		local principal = session.require_jellyfin_principal(ctx)
		if not principal then
			return session.unauthorized()
		end
		if not session.require_matching_user_scope(ctx, principal) then
			return session.unauthorized()
		end

		local params = if ctx then ctx.params else nil
		local raw_item_id = if params then params.itemId else nil
		local track_id = item_ids.decode_jellyfin_item_id(raw_item_id)
		if track_id == nil then
			return api.response.empty(204)
		end

		local ok = pcall(lyrics.delete_personal_for_track, track_id)
		if not ok then
			return api.response.empty(500)
		end
		return api.response.empty(204)
	end, { auth = "public", case_insensitive = true })
	api.get(P .. "/Audio/{itemId}/RemoteSearch/Lyrics", function(ctx: ApiContext): ApiResponse
		local _track_id, _item, resolve_err = lyrics_protocol.resolve_audio_item(ctx)
		if resolve_err then
			return resolve_err
		end
		return api.response.json(200, {})
	end, { auth = "public", case_insensitive = true })
	api.post(P .. "/Audio/{itemId}/RemoteSearch/Lyrics/{lyricId}", function(ctx: ApiContext): ApiResponse
		local _track_id, _item, resolve_err = lyrics_protocol.resolve_audio_item(ctx)
		if resolve_err then
			return resolve_err
		end
		return responses.not_found("lyrics not found")
	end, { auth = "public", case_insensitive = true })
	api.get(P .. "/Providers/Lyrics/{lyricId}", function(ctx: ApiContext): ApiResponse
		local principal = session.require_jellyfin_principal(ctx)
		if not principal then
			return session.unauthorized()
		end
		return responses.not_found("lyrics not found")
	end, { auth = "public", case_insensitive = true })
	api.get(
		P .. "/Audio/{itemId}/universal",
		module.handle_universal_route,
		{ auth = "public", case_insensitive = true }
	)
	api.head(
		P .. "/Audio/{itemId}/universal",
		module.handle_universal_route,
		{ auth = "public", case_insensitive = true }
	)
	api.get(P .. "/Audio/{itemId}/stream", module.handle_stream_route, { auth = "public", case_insensitive = true })
	api.head(P .. "/Audio/{itemId}/stream", module.handle_stream_route, { auth = "public", case_insensitive = true })
	api.get(
		P .. "/Audio/{itemId}/stream.{container}",
		module.handle_stream_route,
		{ auth = "public", case_insensitive = true }
	)
	api.head(
		P .. "/Audio/{itemId}/stream.{container}",
		module.handle_stream_route,
		{ auth = "public", case_insensitive = true }
	)
	api.get(
		P .. "/Audio/{itemId}/master.m3u8",
		module.handle_hls_master_route,
		{ auth = "public", case_insensitive = true }
	)
	api.head(
		P .. "/Audio/{itemId}/master.m3u8",
		module.handle_hls_master_route,
		{ auth = "public", case_insensitive = true }
	)
	api.get(
		P .. "/Audio/{itemId}/main.m3u8",
		module.handle_hls_main_route,
		{ auth = "public", case_insensitive = true }
	)
	api.head(
		P .. "/Audio/{itemId}/main.m3u8",
		module.handle_hls_main_route,
		{ auth = "public", case_insensitive = true }
	)
end

return module
