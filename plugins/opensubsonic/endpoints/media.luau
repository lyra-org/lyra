--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")
local artists = require("@lyra/artists")
local entities = require("@lyra/entities")
local lyrics = require("@lyra/lyrics")
local tracks = require("@lyra/tracks")

local cover_art = require("../catalog/cover_art")
local credits = require("../catalog/credits")
local request = require("../protocol/request")
local response = require("../protocol/response")

type ApiQueryParams = api.ApiQueryParams
type ApiResponse = api.ApiResponse
type EntityProjectionInfo = entities.EntityProjectionInfo
type HlsServeOptions = api.HlsServeOptions
type ImageTransformOptions = api.ImageTransformOptions
type LyricsInfo = lyrics.LyricsInfo
type Request = request.Request
type Track = tracks.Track
type TrackQuery = tracks.TrackQuery
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

-- `served` answering a failure to serve it with a Subsonic error of `message`, rather than the
-- host's error, which can name the file on disk.
local function or_failed(served: ApiResponse, message: string): ApiResponse
	return api.response.with_fallback(served, response.failed_xml(response.error(0, message)))
end

-- The bitrate an `hls` request asks for, in kbps: the first `bitRate`, as `1000` or a video's
-- `1000@480x360`; nil when it asks for none.
local function hls_bitrate_kbps(params: ApiQueryParams): number?
	local raw = request.param(params, "bitRate")
	if not raw then
		return nil
	end
	local kbps = tonumber(string.match(raw, "^%s*(%d+)%s*$") or string.match(raw, "^%s*(%d+)@"))
	if not kbps then
		return request.fail(0, `Invalid bitRate: {raw}`)
	end
	return if kbps > 0 then kbps else nil
end

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

-- Same-named artists, and same-titled songs in pages, that a `getLyrics` lookup reads at most.
local LYRICS_ARTISTS = 100
local LYRICS_SONGS = 1000
local LYRICS_PAGE = 100

-- Whether one of the `named` artists is credited as `artist` on the song `projection` is of.
local function performs(projection: EntityProjectionInfo?, named: { [string]: boolean }): boolean
	for _, ref in credits.artists(projection) or {} do
		if named[ref.id] then
			return true
		end
	end
	return false
end

-- The best ranked lyrics of the first visible song titled `title`, in name order, that has any,
-- with the song and its credits projection. Names compare as an exact search match does. When
-- `artist` is given, only songs crediting an artist of that name as a song's `artist` count;
-- credits of other types, and the album's artists, don't.
local function lyrics_named(title: string, artist: string?): (Track?, LyricsInfo?, EntityProjectionInfo?)
	local query: TrackQuery = { name = title, limit = LYRICS_PAGE }
	local named: { [string]: boolean }? = nil
	if artist then
		local ids: { string } = {}
		local found: { [string]: boolean } = {}
		for _, summary in artists.query({ name = artist, limit = LYRICS_ARTISTS }).items do
			table.insert(ids, summary.id)
			found[summary.id] = true
		end
		if #ids == 0 then
			return nil, nil, nil
		end
		query.artist_ids = ids
		query.credit_role = "track"
		named = found
	end
	local offset = 0
	while offset < LYRICS_SONGS do
		query.offset = offset
		local page = tracks.query(query)
		local ids: { string } = {}
		for _, track in page.items do
			table.insert(ids, track.id)
		end
		local projections = if named then credits.projections(ids, { "credits" }) else {}
		local candidates: { Track } = {}
		local candidate_ids: { string } = {}
		for _, track in page.items do
			if named == nil or performs(projections[track.id], named) then
				table.insert(candidates, track)
				table.insert(candidate_ids, track.id)
			end
		end
		local summaries = if #candidate_ids > 0 then lyrics.by_track(candidate_ids) else {}
		for _, track in candidates do
			local ranked: { lyrics.LyricsSummary } = summaries[track.id] or {}
			local best = ranked[1]
			local info = if best then lyrics.get({ best.id })[best.id] else nil
			if info then
				local projection = projections[track.id] or credits.projections({ track.id }, { "credits" })[track.id]
				return track, info, projection
			end
		end
		offset += #page.items
		if #page.items == 0 or offset >= page.total then
			break
		end
	end
	return nil, nil, nil
end

-- The text of a lyrics entry, its timed lines' texts for lyrics that came only as those.
local function lyrics_text(info: LyricsInfo): string
	if info.plain_text ~= "" or #info.lines == 0 then
		return info.plain_text
	end
	local texts: { string } = {}
	for _, line in info.lines do
		table.insert(texts, line.text)
	end
	return table.concat(texts, "\n")
end

local function can_download(req: Request): boolean
	local permissions = req.auth.principal.permissions
	return table.find(permissions, "admin") ~= nil or table.find(permissions, "download") ~= nil
end

local COVER_FAILED = "The cover art can't be served"

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
	download = true,
	getCoverArt = true,
	hls = true,
	stream = true,
} :: { [string]: boolean }

module.methods = {
	stream = function(req: Request): ApiResponse
		local track = find_track(req.params)
		local options = stream_options(req.params)
		return or_failed(
			api.response.stream_track(track.id, options),
			if options then REFUSED_OPTIONS else "The song can't be streamed"
		)
	end,

	-- The file as it is, for callers allowed to download.
	download = function(req: Request): ApiResponse
		if not can_download(req) then
			return request.fail(50, "Only users allowed to download can download songs")
		end
		local track = find_track(req.params)
		return or_failed(api.response.download_track(track.id), "The song can't be downloaded")
	end,

	-- A media playlist of one bitrate, the first `bitRate` asked for; a repeated `bitRate`, which
	-- asks for a playlist of each, gets only the first.
	hls = function(req: Request): ApiResponse
		local track = find_track(req.params)
		local kbps = hls_bitrate_kbps(req.params)
		-- AAC, which every HLS player plays, rather than whatever the host defaults to.
		local options: HlsServeOptions = {
			preferred_codecs = { "aac" },
			bitrate_bps = if kbps then kbps * 1000 else nil,
		}
		local playlist = api.response.hls_playlist(track.id, options)
		playlist.headers = { ["content-type"] = "application/vnd.apple.mpegurl" }
		return or_failed(playlist, "The song can't be streamed")
	end,

	-- The best ranked lyrics of the first song titled `title`, by an artist named `artist` when
	-- given, that has any; an empty element when none does.
	getLyrics = function(req: Request): ApiResponse
		local title = request.param(req.params, "title")
		local artist = request.param(req.params, "artist")
		if not title then
			return response.ok(req.params, { lyrics = response.EMPTY_OBJECT })
		end
		local track, info, projection = lyrics_named(title, artist)
		if not (track and info) then
			return response.ok(req.params, { lyrics = response.EMPTY_OBJECT })
		end
		local _, display_artist = credits.artists(projection)
		return response.ok(req.params, {
			lyrics = { artist = display_artist, title = track.track_title, value = lyrics_text(info) },
		})
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
			return or_failed(api.response.file(200, cover.path, nil, transform), COVER_FAILED)
		end
		return or_failed(api.response.file(200, cover.path, { ["content-type"] = cover.mime_type }), COVER_FAILED)
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
