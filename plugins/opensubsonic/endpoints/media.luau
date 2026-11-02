--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")
local lyrics = require("@lyra/lyrics")
local playback_sources = require("@lyra/playback_sources")
local tracks = require("@lyra/tracks")

local credits = require("../catalog/credits")
local request = require("../protocol/request")
local response = require("../protocol/response")

type ApiQueryParams = api.ApiQueryParams
type ApiResponse = api.ApiResponse
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
	local time_offset_s = request.int(params, "timeOffset")
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
		options.start_offset_ms = time_offset_s * 1000
		any = true
	end
	return if any then options else nil
end

-- Lines in timestamp order when any line has a time, else the text's lines.
local function structured_lyrics(info: LyricsInfo, track: Track, artist: string?): any
	local synced = false
	for _, line in info.lines do
		if line.ts_ms > 0 then
			synced = true
			break
		end
	end

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

module.methods = {
	stream = function(req: Request): ApiResponse
		local track = find_track(req.params)
		local options = stream_options(req.params)
		if options then
			-- The stream answers a request it can't serve with a bare HTTP error; ask first, so the
			-- client gets a Subsonic one.
			-- Its errors can name the file on disk, so they stay in the log.
			local ok, err = pcall(playback_sources.decide, track.id, options)
			if not ok then
				warn(`stream of {track.id} refused: {tostring(err)}`)
				return request.fail(0, REFUSED_OPTIONS)
			end
		end
		return api.response.stream_track(track.id, options)
	end,

	getLyricsBySongId = function(req: Request): ApiResponse
		local track = find_track(req.params)
		local info = lyrics.get(track.id)
		local entry: any = nil
		if info then
			local projection = credits.projections({ track.id }, { "credits" })[track.id]
			local _, artist = credits.artists(projection)
			entry = structured_lyrics(info, track, artist)
		end
		return response.ok(
			req.params,
			{ lyricsList = if entry then { structuredLyrics = { entry } } else response.EMPTY_OBJECT }
		)
	end,
}

return module
