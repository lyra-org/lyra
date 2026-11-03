--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")
local releases = require("@lyra/releases")
local tracks = require("@lyra/tracks")

local albums = require("../catalog/albums")
local folders = require("../catalog/folders")
local genres = require("../catalog/genres")
local request = require("../protocol/request")
local response = require("../protocol/response")
local songs = require("../catalog/songs")

type ApiQueryParams = api.ApiQueryParams
type ApiResponse = api.ApiResponse
type JsonObject = { [string]: any }
type ReleaseQuery = releases.ReleaseQuery
type Request = request.Request
type TrackQuery = tracks.TrackQuery

local DEFAULT_SIZE = 10
local MAX_SIZE = 500
-- Year ranges are cut to this many years.
local MAX_YEARS = 10000
local MAX_SEED = 0x7FFFFFFF
-- The host's years are unsigned 32-bit integers.
local MAX_YEAR = 0xFFFFFFFF

math.randomseed(os.time() + math.floor(os.clock() * 1e6))

local module = {}

local function year_range(params: ApiQueryParams): ({ number }, "ascending" | "descending")
	local from_year = request.int(params, "fromYear")
	local to_year = request.int(params, "toYear")
	if from_year == nil then
		return request.fail(10, "Required parameter is missing: fromYear")
	end
	if to_year == nil then
		return request.fail(10, "Required parameter is missing: toYear")
	end
	local order: "ascending" | "descending" = if from_year > to_year then "descending" else "ascending"
	local low, high = math.min(from_year, to_year), math.max(from_year, to_year)
	high = math.min(high, low + MAX_YEARS - 1)
	local years: { number } = {}
	for year = low, high do
		table.insert(years, year)
	end
	return years, order
end

-- The query for a list type, and whether its albums need a listen to be listed.
local function list_query(params: ApiQueryParams, list_type: string): (ReleaseQuery?, boolean)
	if list_type == "random" then
		-- The host shuffles by the seed, so each request gets its own.
		return { sort = { { key = "random" } }, seed = math.random(0, MAX_SEED) }, false
	elseif list_type == "newest" then
		return { sort = { { key = "date_created", order = "descending" } } }, false
	elseif list_type == "highest" then
		return { rated = true, sort = { { key = "rating", order = "descending" }, { key = "sort_name" } } }, false
	elseif list_type == "frequent" then
		return {
			sort = { { key = "listen_count", order = "descending" }, { key = "last_played_at", order = "descending" } },
		},
			true
	elseif list_type == "recent" then
		return { sort = { { key = "last_played_at", order = "descending" } } }, true
	elseif list_type == "alphabeticalByName" then
		return { sort = { { key = "sort_name" } } }, false
	elseif list_type == "alphabeticalByArtist" then
		return { sort = { { key = "artist_name" }, { key = "sort_name" } } }, false
	elseif list_type == "starred" then
		return { favorite = true, sort = { { key = "favorited_at", order = "descending" } } }, false
	elseif list_type == "byYear" then
		local years, order = year_range(params)
		return { years = years, sort = { { key = "year", order = order }, { key = "sort_name" } } }, false
	elseif list_type == "byGenre" then
		local genre_ids = genres.ids_named(request.required(params, "genre"))
		if #genre_ids == 0 then
			return nil, false
		end
		return { genre_ids = genre_ids, sort = { { key = "sort_name" } } }, false
	end
	return request.fail(0, `Unknown list type: {list_type}`)
end

-- A list sorted by listens ends at its first album without one; later pages are empty.
local function keep_listened(items: { JsonObject }): { JsonObject }
	local kept: { JsonObject } = {}
	for _, item in items do
		if (item.playCount :: number) == 0 then
			break
		end
		table.insert(kept, item)
	end
	return kept
end

local function songs_result(items: { JsonObject }): any
	return if #items > 0 then { song = items } else response.EMPTY_OBJECT
end

module.methods = {
	getAlbumList2 = function(req: Request): ApiResponse
		local params = req.params
		local list_type = request.required(params, "type")
		local size = request.count(params, "size", DEFAULT_SIZE, MAX_SIZE)
		local offset = request.count(params, "offset", 0)
		local library_id = folders.library_id(params)
		local query, needs_listen = list_query(params, list_type)

		local items: { JsonObject } = {}
		if query and size > 0 then
			query.library_id = library_id
			query.offset = offset
			query.limit = size
			items = albums.list(releases.query(query).items)
			if needs_listen then
				items = keep_listened(items)
			end
		end
		return response.ok(params, { albumList2 = if #items > 0 then { album = items } else response.EMPTY_OBJECT })
	end,

	getRandomSongs = function(req: Request): ApiResponse
		local params = req.params
		local size = request.count(params, "size", DEFAULT_SIZE, MAX_SIZE)
		local from_year = request.int(params, "fromYear")
		local to_year = request.int(params, "toYear")
		local query: TrackQuery = { library_id = folders.library_id(params) }
		local matches = true
		if
			(from_year and to_year and from_year > to_year)
			or (to_year and to_year < 0)
			or (from_year and from_year > MAX_YEAR)
		then
			matches = false
		else
			-- The bounds match a song's own year, while the `year` it shows falls back to its
			-- album's release date.
			query.min_year = if from_year then math.max(from_year, 0) else nil
			query.max_year = if to_year then math.min(to_year, MAX_YEAR) else nil
		end
		local genre = request.param(params, "genre")
		if matches and genre then
			local genre_ids = genres.ids_named(genre)
			query.genre_ids = genre_ids
			matches = #genre_ids > 0
		end

		local items: { JsonObject } = {}
		if matches and size > 0 then
			-- The host shuffles by the seed, so each request gets its own.
			query.sort = { { key = "random" } }
			query.seed = math.random(0, MAX_SEED)
			query.limit = size
			items = songs.list(tracks.query(query).items)
		end
		return response.ok(params, { randomSongs = songs_result(items) })
	end,

	getSongsByGenre = function(req: Request): ApiResponse
		local params = req.params
		local genre_ids = genres.ids_named(request.required(params, "genre"))
		local count = request.count(params, "count", DEFAULT_SIZE, MAX_SIZE)
		local offset = request.count(params, "offset", 0)
		local library_id = folders.library_id(params)

		local items: { JsonObject } = {}
		if #genre_ids > 0 and count > 0 then
			items = songs.list(tracks.query({
				genre_ids = genre_ids,
				library_id = library_id,
				sort = { { key = "sort_name" } },
				offset = offset,
				limit = count,
			}).items)
		end
		return response.ok(params, { songsByGenre = songs_result(items) })
	end,
}

return module
