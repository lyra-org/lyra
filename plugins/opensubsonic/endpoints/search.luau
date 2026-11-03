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

-- Items fetched and encoded at a time, so a page of any size streams in bounded memory.
local BATCH_SIZE = 500

-- The list `name` of up to `count` items from `offset` on, which `fetch` finds and `build` turns
-- into list items, a batch at a time.
local function paged<T>(
	name: string,
	count: number,
	offset: number,
	fetch: (offset: number, limit: number) -> { T },
	build: ({ T }) -> { JsonObject }
): response.StreamedList
	return {
		name = name,
		each_batch = function(emit)
			local done = 0
			while done < count do
				local limit = math.min(BATCH_SIZE, count - done)
				local found = fetch(offset + done, limit)
				if #found > 0 then
					emit(build(found))
				end
				if #found < limit then
					break
				end
				done += #found
			end
		end,
	}
end

module.methods = {
	search3 = function(req: Request): ApiResponse
		local params = req.params
		local search = search_text(params)
		local library_id = folders.library_id(params)
		local function query(offset: number, limit: number): any
			return { search = search, library_id = library_id, offset = offset, limit = limit }
		end
		local artist_count, artist_offset = page(params, "artist")
		local album_count, album_offset = page(params, "album")
		local song_count, song_offset = page(params, "song")

		-- In name order, as the lists of a whole document encode.
		return response.ok_stream(params, "searchResult3", {
			paged("album", album_count, album_offset, function(offset, limit)
				return releases.query(query(offset, limit)).items
			end, albums.list),
			paged("artist", artist_count, artist_offset, function(offset, limit)
				return artists.query(query(offset, limit)).items
			end, artist_items.list),
			paged("song", song_count, song_offset, function(offset, limit)
				return tracks.query(query(offset, limit)).items
			end, songs.list),
		})
	end,
}

return module
