--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

-- Upstream's filter values: the genres, tags and years carried by the items a listing finds
-- recursively under its parent, as their own responses carry them.

local artists = require("@lyra/artists")
local genres = require("@lyra/genres")
local releases = require("@lyra/releases")
local tags = require("@lyra/tags")
local tracks = require("@lyra/tracks")

local api = require("@lyra/api")

local enrichment = require("./enrichment")
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

-- Ids per batched lookup.
local BATCH_SIZE = 500

local module = {}

local function batched<T>(ids: { string }, fetch: ({ string }) -> { [string]: T }): { [string]: T }
	local merged: { [string]: T } = {}
	for first = 1, #ids, BATCH_SIZE do
		local chunk = table.move(ids, first, math.min(first + BATCH_SIZE - 1, #ids), 1, {})
		for id, value in fetch(chunk) do
			merged[id] = value
		end
	end
	return merged
end

local function keys<K>(set: { [K]: boolean }): { K }
	local list: { K } = {}
	for key in set do
		table.insert(list, key)
	end
	return list
end

local function by_name(a: { name: string }, b: { name: string }): boolean
	return string.lower(a.name) < string.lower(b.name)
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

	local tagged: { string } = {}
	local genre_releases: { [string]: boolean } = {}
	local years: { [number]: boolean } = {}
	local function add_year(year: number?)
		if year and year > 0 then
			years[year] = true
		end
	end

	local release_query = if options_module.wants_item_type(listed, "musicalbum")
		then query.releases(listed, scope)
		else nil
	if release_query then
		release_query.offset, release_query.limit = 0, nil
		for _, release in releases.query(release_query).items do
			table.insert(tagged, release.id)
			genre_releases[release.id] = true
			add_year(enrichment.release_year(release.release_date))
		end
	end

	-- A track carries its album's genres, and its album's year when it has none of its own.
	local track_query = if options_module.wants_item_type(listed, "audio") then query.tracks(listed, scope) else nil
	if track_query then
		track_query.offset, track_query.limit = 0, nil
		local found = tracks.query(track_query).items
		local track_ids: { string } = {}
		for _, track in found do
			table.insert(track_ids, track.id)
		end
		local releases_by_track = batched(track_ids, releases.by_track)
		for _, track in found do
			table.insert(tagged, track.id)
			local release = lookups.first_added_release(releases_by_track[track.id])
			if release then
				genre_releases[release.id] = true
			end
			add_year(track.year or enrichment.release_year(if release then release.release_date else nil))
		end
	end

	-- Artists carry tags but no genres or year.
	local artist_query = if options_module.wants_item_type(listed, "musicartist")
		then query.artists(listed, scope, nil)
		else nil
	if artist_query then
		artist_query.offset, artist_query.limit = 0, nil
		for _, artist in artists.query(artist_query).items do
			table.insert(tagged, artist.id)
		end
	end

	local found_genres: { GenreInfo } = {}
	local seen_genres: { [string]: boolean } = {}
	for _, infos in batched(keys(genre_releases), genres.by_release) do
		for _, info in infos do
			if not seen_genres[info.id] then
				seen_genres[info.id] = true
				table.insert(found_genres, info)
			end
		end
	end
	table.sort(found_genres, by_name)

	-- Upstream groups tags case-insensitively, names each group by its least spelling, and orders
	-- them ordinally.
	local tag_groups: { [string]: string } = {}
	for _, infos in batched(tagged, tags.get_for_targets_many) do
		for _, info in infos do
			local name = requests.trim(info.tag)
			if name then
				local key = string.lower(name)
				local current: string? = tag_groups[key]
				if current == nil or name < current then
					tag_groups[key] = name
				end
			end
		end
	end
	local found_tags: { string } = {}
	for _, name in tag_groups do
		table.insert(found_tags, name)
	end
	table.sort(found_tags)

	local found_years = keys(years)
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
				local pair: JsonObject = { Name = genre.name, Id = item_ids.nanoid_to_hex(genre.id) }
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
