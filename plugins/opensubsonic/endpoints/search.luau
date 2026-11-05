--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")
local artists = require("@lyra/artists")
local releases = require("@lyra/releases")
local tracks = require("@lyra/tracks")

local albums = require("../catalog/albums")
local artist_items = require("../catalog/artists")
local folders = require("../catalog/folders")
local request = require("../protocol/request")
local response = require("../protocol/response")
local songs = require("../catalog/songs")

type ApiQueryParams = api.ApiQueryParams
type ApiResponse = api.ApiResponse
type ArtistSummary = artists.ArtistSummary
type JsonObject = { [string]: any }
type ReleaseSummary = releases.ReleaseSummary
type Request = request.Request

local DEFAULT_COUNT = 20

local module = {}

-- The words to search for, or nil to match everything. Clients quote the query or end it with
-- `*`, as the Lucene search Subsonic servers ran on took; Lyra already matches word prefixes.
local function search_text(params: ApiQueryParams): string?
	local raw = request.param_or_empty(params, "query")
	if raw == nil then
		return request.fail(10, "Required parameter is missing: query")
	end
	local text = string.match(raw, "^%s*(.-)%s*$") or ""
	text = string.match(text, '^"(.*)"$') or text
	text = string.gsub(text, "%*+$", "")
	text = string.match(text, "^%s*(.-)%s*$") or ""
	return if text ~= "" then text else nil
end

local function page(params: ApiQueryParams, kind: string): (number, number)
	return request.count(params, `{kind}Count`, DEFAULT_COUNT), request.count(params, `{kind}Offset`, 0)
end

-- What the query finds, as `container` with each kind paged by its count and offset and built into
-- list items by `artist_list` and `album_list`; a count of 0 leaves the kind out.
local function search(
	req: Request,
	container: string,
	artist_list: ({ ArtistSummary }) -> { JsonObject },
	album_list: ({ ReleaseSummary }) -> { JsonObject }
): ApiResponse
	local params = req.params
	local text = search_text(params)
	local library_id = folders.library_id(params)
	local function query(offset: number, limit: number): any
		return { search = text, library_id = library_id, available = true, offset = offset, limit = limit }
	end
	local artist_count, artist_offset = page(params, "artist")
	local album_count, album_offset = page(params, "album")
	local song_count, song_offset = page(params, "song")

	-- In name order, as the lists of a whole document encode.
	return response.ok_stream(params, container, {
		response.paged("album", album_count, album_offset, function(offset, limit)
			return releases.query(query(offset, limit)).items
		end, album_list),
		response.paged("artist", artist_count, artist_offset, function(offset, limit)
			return artists.query(query(offset, limit)).items
		end, artist_list),
		response.paged("song", song_count, song_offset, function(offset, limit)
			return tracks.query(query(offset, limit)).items
		end, function(items)
			return songs.list(items)
		end),
	})
end

module.methods = {
	search3 = function(req: Request): ApiResponse
		return search(req, "searchResult3", artist_items.list, albums.list)
	end,

	-- The same search as `search3`, with artists as `Artist` and albums as directory `Child`ren.
	search2 = function(req: Request): ApiResponse
		return search(req, "searchResult2", artist_items.directories, albums.children)
	end,
}

return module
