--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")
local lyrics = require("@lyra/lyrics")
local playback_sources = require("@lyra/playback_sources")
local tracks = require("@lyra/tracks")

local cover_art = require("../catalog/cover_art")
local credits = require("../catalog/credits")
local request = require("../protocol/request")
local response = require("../protocol/response")

type ApiQueryParams = api.ApiQueryParams
type ApiResponse = api.ApiResponse
type ImageTransformOptions = api.ImageTransformOptions
type LyricsInfo = lyrics.LyricsInfo
type Request = request.Request
type Track = tracks.Track
type TrackStreamOptions = api.TrackStreamOptions

local module = {}

local function find_track(params: ApiQueryParams): Track
	local id = request.required(params, "id")
	local track = tracks.get({ id })[id]
	if not track then
		return request.not_found("Song")
	end
	return track
end

local REFUSED_OPTIONS = "The song can't be streamed with these options"

-- The stream options a request asks for; nil asks for none. `format=raw` asks for no encoding
-- options whatever else the request says.
local function stream_options(params: ApiQueryParams): TrackStreamOptions?
	local format = request.param(params, "format")
	local max_bitrate_kbps = request.int(params, "maxBitRate")
	local time_offset_s = request.number(params, "timeOffset")
	local options: TrackStreamOptions = {}
	local any = false
	if request.bool(params, "estimateContentLength") then
		options.wait_for_transcode = true
		any = true
	end
	if format and string.lower(format) == "raw" then
		return if any then options else nil
	end
	if format then
		local delivered = api.delivery_format(format)
		if delivered == nil then
			return request.fail(0, REFUSED_OPTIONS)
		end
		options.format = delivered
		any = true
	end
	if max_bitrate_kbps and max_bitrate_kbps > 0 then
		options.bitrate_bps = max_bitrate_kbps * 1000
		any = true
	end
	if time_offset_s and time_offset_s > 0 then
		options.start_offset_ms = math.floor(time_offset_s * 1000)
		any = true
	end
	return if any then options else nil
end

-- Timed lines when the host counts the lyrics as synced, else the lines or the text's lines.
local function structured_lyrics(info: LyricsInfo, track: Track, artist: string?): any
	local synced = info.synced
	local lines: { any } = {}
	if synced then
		for _, line in info.lines do
			table.insert(lines, { value = line.text, start = line.ts_ms })
		end
	elseif #info.lines > 0 then
		for _, line in info.lines do
			table.insert(lines, { value = line.text })
		end
	else
		local text = string.gsub(string.gsub(info.plain_text, "\r\n", "\n"), "\r", "\n")
		if text ~= "" then
			for line in string.gmatch(text .. "\n", "(.-)\n") do
				table.insert(lines, { value = line })
			end
		end
	end
	if #lines == 0 then
		return nil
	end

	return {
		lang = if info.language ~= "" then info.language else "und",
		synced = synced,
		line = lines,
		displayArtist = artist,
		displayTitle = track.track_title,
	}
end

-- The largest bound the host takes; no image is that large, and none is scaled up.
local MAX_IMAGE_SIZE = 4294967295

-- Resized images keep their format; anything but PNG and WebP becomes a JPEG.
local function image_format(mime_type: string): string
	if mime_type == "image/png" then
		return "png"
	elseif mime_type == "image/webp" then
		return "webp"
	end
	return "jpg"
end

-- Methods that answer every error in XML, whatever format the request asks for.
module.xml_errors = {
	getCoverArt = true,
	stream = true,
} :: { [string]: boolean }

module.methods = {
	stream = function(req: Request): ApiResponse
		local track = find_track(req.params)
		local options = stream_options(req.params)
		-- The stream answers a request it can't serve with a bare HTTP error; ask first, so the
		-- client gets a Subsonic one.
		-- Its errors can name the file on disk, so they stay in the log.
		local ok, err = pcall(playback_sources.decide, track.id, options)
		if not ok then
			warn(`stream of {track.id} refused: {tostring(err)}`)
			return request.fail(0, if options then REFUSED_OPTIONS else "The song can't be streamed")
		end
		return api.response.stream_track(track.id, options)
	end,

	-- The image as stored, or scaled down to fit `size` on both sides. A size of 0 or less asks
	-- for the stored image.
	getCoverArt = function(req: Request): ApiResponse
		local id = request.required(req.params, "id")
		local size = request.int(req.params, "size")
		local cover = cover_art.find(id)
		if not cover then
			return request.not_found("Cover art")
		end
		if size and size > 0 then
			local bound = math.min(size, MAX_IMAGE_SIZE)
			local transform: ImageTransformOptions =
				{ format = image_format(cover.mime_type), max_width = bound, max_height = bound }
			return api.response.file(200, cover.path, nil, transform)
		end
		return api.response.file(200, cover.path, { ["content-type"] = cover.mime_type })
	end,

	-- Every lyrics entry the caller sees, in rank order.
	getLyricsBySongId = function(req: Request): ApiResponse
		local track = find_track(req.params)
		local ids: { string } = {}
		local summaries: { lyrics.LyricsSummary } = lyrics.by_track({ track.id })[track.id] or {}
		for _, summary in summaries do
			table.insert(ids, summary.id)
		end
		local entries: { any } = {}
		if #ids > 0 then
			local infos = lyrics.get(ids)
			local projection = credits.projections({ track.id }, { "credits" })[track.id]
			local _, artist = credits.artists(projection)
			for _, id in ids do
				local info = infos[id]
				local entry = if info then structured_lyrics(info, track, artist) else nil
				if entry then
					table.insert(entries, entry)
				end
			end
		end
		return response.ok(
			req.params,
			{ lyricsList = if #entries > 0 then { structuredLyrics = entries } else response.EMPTY_OBJECT }
		)
	end,
}

return module
