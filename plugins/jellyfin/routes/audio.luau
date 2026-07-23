--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")
local lyrics = require("@lyra/lyrics")
local tracks = require("@lyra/tracks")

local constants = require("../constants")
local item_ids = require("../protocol/items/identifiers")
local lyrics_protocol = require("../protocol/lyrics")
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

function module.register()
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

	local function is_supported_audio_hls_codec(codec: string): boolean
		return codec == "aac" or codec == "alac" or codec == "flac"
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
	end, nil, true)
	api.post(P .. "/Audio/{itemId}/Lyrics", function(ctx: ApiContext): ApiResponse
		local track_id, item, resolve_err = lyrics_protocol.resolve_audio_item(ctx)
		if resolve_err then
			return resolve_err
		end
		if track_id == nil or item == nil then
			return responses.not_found("item not found")
		end
		local raw_track = item._raw
		local track_public_id = if raw_track then raw_track.id else nil
		if type(track_public_id) ~= "string" then
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

		local ok, lyric_info = pcall(lyrics.upsert_personal, track_id, track_public_id, {
			content_type = content_type,
			body = body,
			language = nil,
		})
		if not ok then
			return responses.bad_request(tostring(lyric_info))
		end

		return api.response.json(200, lyrics_protocol.to_jellyfin_lyric_dto(item, lyric_info :: LyricsInfo))
	end, nil, true)
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
	end, nil, true)
	api.get(P .. "/Audio/{itemId}/RemoteSearch/Lyrics", function(ctx: ApiContext): ApiResponse
		local _track_id, _item, resolve_err = lyrics_protocol.resolve_audio_item(ctx)
		if resolve_err then
			return resolve_err
		end
		return api.response.json(200, {})
	end, nil, true)
	api.post(P .. "/Audio/{itemId}/RemoteSearch/Lyrics/{lyricId}", function(ctx: ApiContext): ApiResponse
		local _track_id, _item, resolve_err = lyrics_protocol.resolve_audio_item(ctx)
		if resolve_err then
			return resolve_err
		end
		return responses.not_found("lyrics not found")
	end, nil, true)
	api.get(P .. "/Providers/Lyrics/{lyricId}", function(ctx: ApiContext): ApiResponse
		local principal = session.require_jellyfin_principal(ctx)
		if not principal then
			return session.unauthorized()
		end
		return responses.not_found("lyrics not found")
	end, nil, true)

	local function handle_audio_stream_route(ctx: ApiContext): ApiResponse
		local query = if ctx and ctx.request then ctx.request.query else nil
		local request = if ctx then ctx.request else nil
		local request_method = if request then request.method else nil
		local is_head_request = request_method == "HEAD"
		local request_path = if ctx and ctx.request then ctx.request.path else nil
		local normalized_request_path = if type(request_path) == "string" then string.lower(request_path) else nil
		local raw_static = requests.parse_query_string(query, { "static", "Static" }, nil)
		local raw_container = requests.parse_query_string(query, { "container", "Container" }, nil)
		local static_direct_copy = jellyfin_boolean(raw_static) == true

		local raw_codec = requests.parse_query_string(query, { "audioCodec", "AudioCodec" }, nil)
		local raw_transcoding_container =
			requests.parse_query_string(query, { "transcodingContainer", "TranscodingContainer" }, nil)
		local raw_max_bitrate =
			requests.parse_query_string(query, { "MaxStreamingBitrate", "maxStreamingBitrate" }, nil)
		local raw_audio_bitrate =
			requests.parse_query_string(query, { "audioBitRate", "AudioBitRate", "AudioBitrate" }, nil)
		local raw_sample_rate = requests.parse_query_string(query, { "audioSampleRate", "AudioSampleRate" }, nil)
		local raw_bit_depth = requests.parse_query_string(query, { "maxAudioBitDepth", "MaxAudioBitDepth" }, nil)
		local raw_channels = requests.parse_query_string(query, { "maxAudioChannels", "MaxAudioChannels" }, nil)
		local raw_enable_audio_vbr =
			requests.parse_query_string(query, { "enableAudioVbrEncoding", "EnableAudioVbrEncoding" }, nil)
		local raw_auto_copy =
			requests.parse_query_string(query, { "enableAutoStreamCopy", "EnableAutoStreamCopy" }, nil)
		local raw_enable_direct_play =
			requests.parse_query_string(query, { "enableDirectPlay", "EnableDirectPlay" }, nil)
		local raw_enable_transcoding =
			requests.parse_query_string(query, { "enableTranscoding", "EnableTranscoding" }, nil)
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
		local prefer_vbr = jellyfin_boolean(raw_enable_audio_vbr)
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
		local track_id = track.db_id
		if track_id == nil or track_id <= 0 then
			return responses.not_found("item not found")
		end

		local explicit_stream_path = string.lower(`{P}/Audio/{item_id}/stream`)
		if static_direct_copy and normalized_request_path == explicit_stream_path then
			if is_head_request then
				return api.response.empty(200)
			end
			return session.handle_item_file_stream_request(ctx)
		end

		local media_source = playback.build_audio_media_source(item_id, track)
		if
			playback.audio_supports_direct_play_from_container_csv(
				media_source,
				raw_container,
				bitrate_bps,
				sample_rate_hz,
				bit_depth,
				channels,
				direct_play_enabled,
				forbid_stream_copy
			)
		then
			if is_head_request then
				return api.response.empty(200)
			end
			return session.handle_item_file_stream_request(ctx)
		end

		if not transcoding_enabled then
			if static_direct_copy then
				if is_head_request then
					return api.response.empty(200)
				end
				return session.handle_item_file_stream_request(ctx)
			end
			return api.response.json(415, {
				Message = "transcoding is not supported; request with static=true or supply transcode parameters",
			})
		end

		if hls_requested then
			if preferred_codecs ~= nil and hls_codec == nil then
				return responses.bad_request("unsupported hls codec; supported values: aac, alac, flac")
			end
			local options: api.HlsServeOptions = {
				preferred_codecs = preferred_codecs,
				bitrate_bps = bitrate_bps,
				sample_rate_hz = sample_rate_hz,
				channels = channels,
				prefer_vbr = prefer_vbr,
				start_offset_ms = start_offset_ms,
			}
			if is_head_request then
				return api.response.empty(200, {
					["content-type"] = "application/x-mpegurl",
				})
			end
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

		local options: api.TrackServeOptions = {
			format = effective_transcoding_container,
			preferred_codecs = effective_preferred_codecs,
			bitrate_bps = bitrate_bps,
			sample_rate_hz = sample_rate_hz,
			channels = channels,
			prefer_vbr = prefer_vbr,
			start_offset_ms = start_offset_ms,
		}
		if is_head_request then
			return api.response.empty(200)
		end

		return api.response.stream_track(track_id, options)
	end

	api.get(P .. "/Audio/{itemId}/universal", function(ctx: ApiContext)
		return handle_audio_stream_route(ctx)
	end, nil, true)
	api.head(P .. "/Audio/{itemId}/universal", function(ctx: ApiContext)
		return handle_audio_stream_route(ctx)
	end, nil, true)
	api.get(P .. "/Audio/{itemId}/stream", function(ctx: ApiContext)
		return handle_audio_stream_route(ctx)
	end, nil, true)
	api.head(P .. "/Audio/{itemId}/stream", function(ctx: ApiContext)
		return handle_audio_stream_route(ctx)
	end, nil, true)

	type ResolvedAudioHlsPlaylistRequest = {
		track: Track,
		track_id: number,
		query: ApiQueryParams?,
		selected_codec: string,
		options: api.HlsServeOptions,
	}

	local DEFAULT_AUDIO_HLS_BANDWIDTH_BPS = 192000
	local MIN_AUDIO_HLS_AAC_BANDWIDTH_BPS = 32000

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
				if key ~= "audioCodec" and key ~= "AudioCodec" and key ~= "codec" then
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

	local function codec_string_for_audio_hls(codec: string): string?
		if codec == "aac" then
			return "mp4a.40.2"
		end
		if codec == "flac" then
			return "fLaC"
		end
		if codec == "alac" then
			return "alac"
		end
		return nil
	end

	local function resolve_audio_hls_query_options(
		query: ApiQueryParams?
	): (api.HlsServeOptions?, string?, ApiResponse?)
		local raw_codec = requests.parse_query_string(query, { "audioCodec", "AudioCodec", "codec" }, nil)
		local preferred_codecs = parse_preferred_csv_values(raw_codec)
		local selected_codec = find_supported_audio_hls_codec(preferred_codecs)
		if preferred_codecs ~= nil and selected_codec == nil then
			return nil, nil, responses.bad_request("unsupported hls codec; supported values: aac, alac, flac")
		end

		local raw_audio_bitrate =
			requests.parse_query_string(query, { "audioBitRate", "AudioBitRate", "AudioBitrate" }, nil)
		local raw_max_bitrate =
			requests.parse_query_string(query, { "maxStreamingBitrate", "MaxStreamingBitrate" }, nil)
		local raw_sample_rate = requests.parse_query_string(query, { "audioSampleRate", "AudioSampleRate" }, nil)
		local raw_channels = requests.parse_query_string(query, { "maxAudioChannels", "MaxAudioChannels" }, nil)
		local raw_enable_audio_vbr =
			requests.parse_query_string(query, { "enableAudioVbrEncoding", "EnableAudioVbrEncoding" }, nil)
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
		local channels, channels_err = extract_integer_param(raw_channels)
		if channels_err then
			return nil, nil, responses.bad_request(`maxAudioChannels {channels_err}`)
		end
		local start_ticks, start_ticks_err = extract_integer_param(raw_start_ticks)
		if start_ticks_err then
			return nil, nil, responses.bad_request(`StartTimeTicks {start_ticks_err}`)
		end

		return {
			preferred_codecs = preferred_codecs,
			bitrate_bps = requested_audio_bitrate or requested_max_bitrate,
			sample_rate_hz = sample_rate_hz,
			channels = channels,
			prefer_vbr = jellyfin_boolean(raw_enable_audio_vbr),
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
		local track_bitrate = if track.bitrate_bps and track.bitrate_bps > 0 then track.bitrate_bps else nil

		if codec == "aac" then
			if requested_bitrate ~= nil then
				if track_bitrate ~= nil and requested_bitrate > track_bitrate then
					return DEFAULT_AUDIO_HLS_BANDWIDTH_BPS
				end
				if requested_bitrate < MIN_AUDIO_HLS_AAC_BANDWIDTH_BPS then
					return MIN_AUDIO_HLS_AAC_BANDWIDTH_BPS
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
		local codec_string = codec_string_for_audio_hls(selected_codec)
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
		local track_id = track.db_id
		if track_id == nil or track_id <= 0 then
			return nil, responses.not_found("item not found")
		end

		local query = if ctx and ctx.request then ctx.request.query else nil
		local options, selected_codec, options_err = resolve_audio_hls_query_options(query)
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

	local function handle_audio_hls_master_playlist_route(ctx: ApiContext): ApiResponse
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

	local function handle_audio_hls_main_playlist_route(ctx: ApiContext): ApiResponse
		local request, err = resolve_audio_hls_playlist_request(ctx)
		if request == nil then
			return err or responses.bad_request("invalid hls request")
		end

		return api.response.hls_playlist(request.track_id, request.options)
	end

	api.get(P .. "/Audio/{itemId}/master.m3u8", handle_audio_hls_master_playlist_route, nil, true)
	api.head(P .. "/Audio/{itemId}/master.m3u8", handle_audio_hls_master_playlist_route, nil, true)
	api.get(P .. "/Audio/{itemId}/main.m3u8", handle_audio_hls_main_playlist_route, nil, true)
	api.head(P .. "/Audio/{itemId}/main.m3u8", handle_audio_hls_main_playlist_route, nil, true)
end

return module
