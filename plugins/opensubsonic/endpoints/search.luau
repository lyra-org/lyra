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
type JsonObject = { [string]: any }
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

module.methods = {
	search3 = function(req: Request): ApiResponse
		local params = req.params
		local search = search_text(params)
		local library_id = folders.library_id(params)
		local result: JsonObject = {}

		local artist_count, artist_offset = page(params, "artist")
		if artist_count > 0 then
			local found = artists.query({
				search = search,
				library_id = library_id,
				offset = artist_offset,
				limit = artist_count,
			}).items
			if #found > 0 then
				result.artist = artist_items.list(found)
			end
		end

		local album_count, album_offset = page(params, "album")
		if album_count > 0 then
			local found = releases.query({
				search = search,
				library_id = library_id,
				offset = album_offset,
				limit = album_count,
			}).items
			if #found > 0 then
				result.album = albums.list(found)
			end
		end

		local song_count, song_offset = page(params, "song")
		if song_count > 0 then
			local found = tracks.query({
				search = search,
				library_id = library_id,
				offset = song_offset,
				limit = song_count,
			}).items
			if #found > 0 then
				result.song = songs.list(found)
			end
		end

		return response.ok(params, { searchResult3 = if next(result) ~= nil then result else response.EMPTY_OBJECT })
	end,
}

return module
