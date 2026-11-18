--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

-- Upstream's filter values: the genres, tags and years of the items a listing finds recursively
-- under its parent. Each is read as Lyra's filter of the same name reads it, so picking a value
-- lists the items it came from: a track lends the genres of every release of its the caller sees,
-- and only its own year.

local artists = require("@lyra/artists")
local genres = require("@lyra/genres")
local releases = require("@lyra/releases")
local tracks = require("@lyra/tracks")

local api = require("@lyra/api")

local item_ids = require("./identifiers")
local lookups = require("./lookups")
local options_module = require("./options")
local query = require("./query")
local requests = require("./requests")

type ItemsQueryOptions = options_module.ItemsQueryOptions
type JsonValue = api.JsonValue
type JsonObject = { [string]: JsonValue }
type GenreInfo = genres.GenreInfo

export type FilterValues = {
	genres: { GenreInfo },
	tags: { string },
	years: { number },
}

local module = {}

-- A listing query as a match: what it finds, unordered and unpaged.
local function as_match<Q>(listing: Q & { sort: any, seed: number?, offset: number?, limit: number? }): Q
	listing.sort, listing.seed, listing.offset, listing.limit = nil, nil, nil, nil
	return listing
end

-- Nil when the parent isn't a folder, which upstream answers with no values.
function module.collect(options: ItemsQueryOptions): FilterValues?
	local scope = lookups.scope(options.parent_id)
	if scope.entity_type == "Track" then
		return nil
	end
	local listed = table.clone(options)
	listed.start_index = 0
	listed.limit = nil
	listed.sort_by = {}
	listed.recursive = true

	local found_genres: { GenreInfo } = {}
	local seen_genres: { [string]: boolean } = {}
	local function add_genres(found: { genres.GenreSummary })
		for _, info in found do
			if not seen_genres[info.id] then
				seen_genres[info.id] = true
				table.insert(found_genres, info)
			end
		end
	end
	local tag_names: { string } = {}
	local function add_tags(names: { string })
		table.move(names, 1, #names, #tag_names + 1, tag_names)
	end
	local years: { [number]: boolean } = {}
	local function add_years(found: { number })
		for _, year in found do
			if year > 0 then
				years[year] = true
			end
		end
	end

	local release_query = if options_module.wants_item_type(listed, "musicalbum")
		then query.releases(listed, scope)
		else nil
	if release_query then
		local albums = as_match(release_query)
		add_genres(genres.query({ releases = albums, available = true }).items)
		add_tags(releases.tags(albums))
		add_years(releases.years(albums))
	end

	local track_query = if options_module.wants_item_type(listed, "audio") then query.tracks(listed, scope) else nil
	if track_query then
		local songs = as_match(track_query)
		add_genres(genres.query({ tracks = songs, available = true }).items)
		add_tags(tracks.tags(songs))
		add_years(tracks.years(songs))
	end

	-- Artists carry tags but no genres or year.
	local artist_query = if options_module.wants_item_type(listed, "musicartist")
		then query.artists(listed, scope, nil)
		else nil
	if artist_query then
		add_tags(artists.tags(as_match(artist_query)))
	end

	table.sort(found_genres, function(a: GenreInfo, b: GenreInfo): boolean
		return string.lower(a.name) < string.lower(b.name)
	end)

	-- Upstream groups tags case-insensitively, names each group by its least spelling, and orders
	-- them ordinally.
	local tag_groups: { [string]: string } = {}
	for _, raw_name in tag_names do
		local name = requests.trim(raw_name)
		if name then
			local key = string.lower(name)
			local current: string? = tag_groups[key]
			if current == nil or name < current then
				tag_groups[key] = name
			end
		end
	end
	local found_tags: { string } = {}
	for _, name in tag_groups do
		table.insert(found_tags, name)
	end
	table.sort(found_tags)

	local found_years: { number } = {}
	for year in years do
		table.insert(found_years, year)
	end
	table.sort(found_years)
	return { genres = found_genres, tags = found_tags, years = found_years }
end

-- `/Items/Filters`: names only, ordered ordinally.
function module.legacy_json(values: FilterValues?): JsonObject
	local genre_names: { JsonValue } = {}
	local tag_names: { JsonValue } = {}
	local years: { JsonValue } = {}
	if values then
		local names: { string } = {}
		for _, genre in values.genres do
			table.insert(names, genre.name)
		end
		table.sort(names)
		for _, name in names do
			table.insert(genre_names, name)
		end
		for _, tag in values.tags do
			table.insert(tag_names, tag)
		end
		for _, year in values.years do
			table.insert(years, year)
		end
	end
	return { Genres = genre_names, Tags = tag_names, OfficialRatings = {}, Years = years }
end

-- Upstream lists music genres only for a single music item type, and other genres otherwise,
-- which Lyra has none of.
local MUSIC_TYPES = { musicalbum = true, musicvideo = true, musicartist = true, audio = true }

-- `/Items/Filters2`: genres with their ids. Languages are only for video upstream.
function module.to_json(values: FilterValues?, include_item_types: { string }): JsonObject
	local genre_pairs: { JsonValue } = {}
	local tag_names: { JsonValue } = {}
	if values then
		if #include_item_types == 1 and MUSIC_TYPES[string.lower(include_item_types[1])] then
			for _, genre in values.genres do
				local pair: JsonObject = { Name = genre.name, Id = item_ids.encode(genre.id) }
				table.insert(genre_pairs, pair)
			end
		end
		for _, tag in values.tags do
			table.insert(tag_names, tag)
		end
	end
	return { Genres = genre_pairs, Tags = tag_names, AudioLanguages = {}, SubtitleLanguages = {} }
end

return module
