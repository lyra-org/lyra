--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

-- The query parameters of Jellyfin item listings.

local api = require("@lyra/api")
local genres = require("@lyra/genres")
local tags_module = require("@lyra/tags")

local constants = require("../../constants")
local requests = require("./requests")

type ApiQueryParams = api.ApiQueryParams
type TagInfoMap = { [number]: { tags_module.TagInfo } }

local DEFAULT_ITEMS_FIELDS = constants.DEFAULT_ITEMS_FIELDS
local ITEM_ID_HEX_LENGTH = constants.ITEM_ID_HEX_LENGTH
local ITEM_ID_HEX_PATTERN = constants.ITEM_ID_HEX_PATTERN

export type FavoriteLookup = { [number]: boolean }

-- Ids are Jellyfin item ids (hex); translation to Lyra ids happens when a query is built.
export type ItemsQueryOptions = {
	include_item_types: { string },
	exclude_item_types: { string },
	media_types: { string },
	ids: { string },
	exclude_item_ids: { string },
	exclude_artist_ids: { string },
	studio_ids: { string },
	-- `ArtistIds` and `PersonIds`: an artist credited in either role.
	artist_ids: { string },
	-- `AlbumArtistIds`: an artist credited on the release.
	release_artist_ids: { string },
	-- `ContributingArtistIds`: an artist credited on a track but not on its release.
	contributing_artist_ids: { string },
	album_ids: { string },
	genre_ids: { string },
	filters: { string },
	is_favorite: boolean?,
	is_played: boolean?,
	years: { number },
	name_starts_with: string?,
	name_starts_with_or_greater: string?,
	name_less_than: string?,
	-- `MinCommunityRating` or `MinCriticRating`; music has no ratings, so nothing matches.
	requires_rating: boolean,
	appears_in_item_id: string?,
	parent_id: string?,
	start_index: number,
	limit: number?,
	recursive: boolean?,
	sort_by: { string },
	sort_order: string?,
	fields: { string },
	search_term: string?,
	enable_user_data: boolean?,
	enable_images: boolean?,
	include_media_info: boolean,
	include_people: boolean,
	include_overview: boolean,
	include_parent_id: boolean,
	include_child_count: boolean,
	include_provider_ids: boolean,
	include_external_urls: boolean,
	group_items: boolean,
	user_id: string?,
	_favorite_id_lookup: FavoriteLookup?,
	_genre_infos_by_release_id: { [number]: { genres.GenreInfo } }?,
	_tag_infos_by_item_id: TagInfoMap?,
}

local module = {}

local function lower_or_empty(value: string?): string
	if value == nil then
		return ""
	end
	return string.lower(value)
end

local function split_csv(raw: string?): { string }
	local values: { string } = {}
	if type(raw) ~= "string" then
		return values
	end
	for _, part in requests.split_csv_preserving_quotes(raw) do
		local item = requests.trim(requests.unquote(part))
		if item then
			table.insert(values, item)
		end
	end
	return values
end

local function parse_bool(query: ApiQueryParams?, keys: { string }, default_value: boolean?): boolean?
	local value = requests.query_lookup(query, keys)
	if value == nil then
		return default_value
	end
	local normalized = lower_or_empty(requests.trim(tostring(value)))
	if normalized == "1" or normalized == "true" or normalized == "yes" or normalized == "on" then
		return true
	end
	if normalized == "0" or normalized == "false" or normalized == "no" or normalized == "off" then
		return false
	end
	return default_value
end

local function parse_csv(query: ApiQueryParams?, keys: { string }): { string }
	local results: { string } = {}
	if type(query) ~= "table" then
		return results
	end

	local seen_keys: { [string]: boolean } = {}
	local normalized_keys: { [string]: boolean } = {}
	for _, key in keys do
		normalized_keys[string.lower(key)] = true
	end
	local function consume(key: string, values: { string })
		if seen_keys[key] then
			return
		end
		seen_keys[key] = true
		for _, raw in values do
			for _, item in split_csv(raw) do
				table.insert(results, item)
			end
		end
	end

	for _, key in keys do
		local values = query[key]
		if values ~= nil then
			consume(key, values)
		end
	end
	for query_key, values in query do
		local key = tostring(query_key)
		if normalized_keys[string.lower(key)] then
			consume(key, values)
		end
	end
	return results
end

-- Well-formed item ids, lowercased and without dashes, each once.
local function parse_item_ids(query: ApiQueryParams?, keys: { string }): { string }
	local ids: { string } = {}
	local seen: { [string]: boolean } = {}
	for _, raw_id in parse_csv(query, keys) do
		local normalized = string.lower((string.gsub(raw_id, "-", "")))
		if
			#normalized == ITEM_ID_HEX_LENGTH
			and string.match(normalized, ITEM_ID_HEX_PATTERN)
			and not seen[normalized]
		then
			seen[normalized] = true
			table.insert(ids, normalized)
		end
	end
	return ids
end

local function merge(first: { string }, second: { string }): { string }
	local merged: { string } = {}
	local seen: { [string]: boolean } = {}
	for _, list in { first, second } do
		for _, value in list do
			if not seen[value] then
				seen[value] = true
				table.insert(merged, value)
			end
		end
	end
	return merged
end

local function parse_years(query: ApiQueryParams?): { number }
	local years: { number } = {}
	for _, raw_year in parse_csv(query, { "Years" }) do
		local year = tonumber(raw_year)
		if year ~= nil and year == math.floor(year) then
			table.insert(years, year)
		end
	end
	return years
end

local function requests_field(fields: { string }, name: string): boolean
	for _, field in fields do
		if string.lower(field) == name then
			return true
		end
	end
	return false
end

function module.parse(query: ApiQueryParams?): ItemsQueryOptions
	local fields = parse_csv(query, { "Fields" })
	if #fields == 0 then
		fields = DEFAULT_ITEMS_FIELDS
	end
	return {
		include_item_types = parse_csv(query, { "IncludeItemTypes" }),
		exclude_item_types = parse_csv(query, { "ExcludeItemTypes" }),
		media_types = parse_csv(query, { "MediaTypes" }),
		ids = parse_csv(query, { "Ids" }),
		exclude_item_ids = parse_item_ids(query, { "ExcludeItemIds" }),
		exclude_artist_ids = parse_item_ids(query, { "ExcludeArtistIds" }),
		studio_ids = parse_item_ids(query, { "StudioIds" }),
		artist_ids = merge(parse_item_ids(query, { "ArtistIds" }), parse_item_ids(query, { "PersonIds" })),
		release_artist_ids = parse_item_ids(query, { "AlbumArtistIds" }),
		contributing_artist_ids = parse_item_ids(query, { "ContributingArtistIds" }),
		album_ids = parse_item_ids(query, { "AlbumIds" }),
		genre_ids = parse_item_ids(query, { "GenreIds" }),
		filters = parse_csv(query, { "Filters" }),
		is_favorite = parse_bool(query, { "IsFavorite" }, nil),
		is_played = parse_bool(query, { "IsPlayed" }, nil),
		years = parse_years(query),
		name_starts_with = requests.parse_query_string(query, { "NameStartsWith" }, nil),
		name_starts_with_or_greater = requests.parse_query_string(query, { "NameStartsWithOrGreater" }, nil),
		name_less_than = requests.parse_query_string(query, { "NameLessThan" }, nil),
		requires_rating = requests.query_lookup(query, { "MinCommunityRating" }) ~= nil
			or requests.query_lookup(query, { "MinCriticRating" }) ~= nil,
		appears_in_item_id = requests.parse_query_string(query, { "AppearsInItemId" }, nil),
		parent_id = requests.parse_query_string(query, { "ParentId" }, nil),
		start_index = requests.parse_query_int(query, { "StartIndex" }, 0, 0, nil) or 0,
		limit = requests.parse_query_int(query, { "Limit" }, nil, 0, nil),
		recursive = parse_bool(query, { "Recursive" }, false),
		sort_by = parse_csv(query, { "SortBy" }),
		sort_order = requests.parse_query_string(query, { "SortOrder" }, nil),
		fields = fields,
		search_term = requests.parse_query_string(query, { "SearchTerm" }, nil),
		enable_user_data = parse_bool(query, { "EnableUserData" }, true),
		enable_images = parse_bool(query, { "EnableImages" }, true),
		include_media_info = requests_field(fields, "mediasources") or requests_field(fields, "mediastreams"),
		include_people = requests_field(fields, "people"),
		include_overview = requests_field(fields, "overview"),
		include_parent_id = requests_field(fields, "parentid"),
		include_child_count = requests_field(fields, "childcount"),
		include_provider_ids = requests_field(fields, "providerids"),
		include_external_urls = requests_field(fields, "externalurls"),
		group_items = parse_bool(query, { "GroupItems" }, true) ~= false,
		user_id = nil,
		_favorite_id_lookup = nil,
		_genre_infos_by_release_id = nil,
		_tag_infos_by_item_id = nil,
	}
end

function module.for_user(options: ItemsQueryOptions, user_id: string): ItemsQueryOptions
	local scoped = table.clone(options)
	scoped.user_id = user_id
	return scoped
end

-- The `Filters` values a listing applies, matching nothing where Lyra has no such state.
local APPLIED_FILTERS: { [string]: boolean } = {
	isfavorite = true,
	isfavoriteorlikes = true,
	isplayed = true,
	isunplayed = true,
	likes = true,
	isresumable = true,
}

-- Whether the listing narrows its results by a filter it applies. Upstream searches the libraries
-- for any filtered request at the user root; a filter the listing ignores would turn that search
-- into an unfiltered one, so only the applied filters count.
function module.has_filters(options: ItemsQueryOptions): boolean
	for _, list in
		{
			options.include_item_types,
			options.exclude_item_types,
			options.media_types,
			options.exclude_item_ids,
			options.studio_ids,
			options.artist_ids,
			options.release_artist_ids,
			options.contributing_artist_ids,
			options.album_ids,
			options.genre_ids,
			options.years,
		} :: { { any } }
	do
		if #list > 0 then
			return true
		end
	end
	for _, filter in options.filters do
		if APPLIED_FILTERS[string.lower(filter)] then
			return true
		end
	end
	return options.is_favorite ~= nil
		or options.is_played ~= nil
		or options.requires_rating
		or options.name_starts_with ~= nil
		or options.name_starts_with_or_greater ~= nil
		or options.name_less_than ~= nil
		or options.search_term ~= nil
end

local CONFLICTING_FILTERS: { [string]: string } = {
	isfolder = "isnotfolder",
	isplayed = "isunplayed",
	likes = "dislikes",
}

-- Whether `Filters` names both sides of a pair, which upstream rejects.
function module.has_conflicting_filters(options: ItemsQueryOptions): boolean
	local present: { [string]: boolean } = {}
	for _, filter in options.filters do
		present[string.lower(filter)] = true
	end
	for filter, opposite in CONFLICTING_FILTERS do
		if present[filter] and present[opposite] then
			return true
		end
	end
	return false
end

-- Whether a listing asked for this Jellyfin item type; `IncludeItemTypes` wins over
-- `ExcludeItemTypes`, as upstream.
function module.wants_item_type(options: ItemsQueryOptions, item_type: string): boolean
	local target = string.lower(item_type)

	-- Tracks carry MediaType "Audio"; folder-like items carry "Unknown".
	if #options.media_types > 0 then
		local media_type = if target == "audio" then "audio" else "unknown"
		local matched = false
		for _, requested in options.media_types do
			if string.lower(requested) == media_type then
				matched = true
				break
			end
		end
		if not matched then
			return false
		end
	end

	if #options.include_item_types > 0 then
		for _, included in options.include_item_types do
			if string.lower(included) == target then
				return true
			end
		end
		return false
	end

	for _, excluded in options.exclude_item_types do
		if string.lower(excluded) == target then
			return false
		end
	end
	return true
end

return module
