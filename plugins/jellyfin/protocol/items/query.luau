--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

-- Translates Jellyfin item listings into Lyra catalog queries. Jellyfin's sort keys, defaults and
-- filters are all resolved here, so the server only ever sees Lyra terms.

local artists = require("@lyra/artists")
local genres = require("@lyra/genres")
local releases = require("@lyra/releases")
local tracks = require("@lyra/tracks")

local item_ids = require("./identifiers")
local lookups = require("./lookups")
local options_module = require("./options")

type ItemsQueryOptions = options_module.ItemsQueryOptions
type ArtistQuery = artists.ArtistQuery
type CreditRole = tracks.CreditRole
type CreditType = artists.CreditType
type GenreQuery = genres.GenreQuery
type ReleaseQuery = releases.ReleaseQuery
type TrackQuery = tracks.TrackQuery

export type Entity = "track" | "release" | "artist" | "genre"
export type Scope = lookups.Scope
export type SortTerm = { key: string, order: "ascending" | "descending" }

-- What a translated sort asks of the caller: nil sort means "keep the order of `Ids`".
export type Sort = { sort: { SortTerm }?, seed: number? }

local module = {}

-- The Lyra keys each Jellyfin sort key stands for. Keys missing here have no Lyra meaning
-- (ratings, IsPlayed, Budget, ...) and are dropped, as upstream drops unknown keys.
local SORT_KEYS: { [Entity]: { [string]: { string } } } = {
	-- A track's Jellyfin SortName is "disc - track - title", so it sorts in album order.
	track = {
		sortname = { "disc", "track", "sort_name" },
		name = { "name" },
		album = { "release_title" },
		albumartist = { "release_artist_name" },
		artist = { "artist_name" },
		runtime = { "duration" },
		premieredate = { "year" },
		productionyear = { "year" },
		indexnumber = { "track" },
		parentindexnumber = { "disc" },
		datecreated = { "date_created" },
		playcount = { "listen_count" },
		dateplayed = { "last_played_at" },
		random = { "random" },
	},
	release = {
		sortname = { "sort_name" },
		name = { "name" },
		album = { "name" },
		albumartist = { "artist_name" },
		artist = { "artist_name" },
		runtime = { "total_duration" },
		premieredate = { "release_date" },
		productionyear = { "year" },
		datecreated = { "date_created" },
		playcount = { "listen_count" },
		dateplayed = { "last_played_at" },
		random = { "random" },
	},
	artist = {
		sortname = { "sort_name" },
		name = { "name" },
		runtime = { "total_duration" },
		datecreated = { "date_created" },
		playcount = { "listen_count" },
		dateplayed = { "last_played_at" },
		random = { "random" },
	},
	genre = {
		sortname = { "name" },
		name = { "name" },
		runtime = { "total_duration" },
		playcount = { "listen_count" },
		dateplayed = { "last_played_at" },
		random = { "random" },
	},
}

local function direction(value: string?): "ascending" | "descending"
	if value ~= nil and string.lower(value) == "descending" then
		return "descending"
	end
	return "ascending"
end

-- The first `SortOrder`, for listings sorted by a single key.
function module.direction(sort_order: string?): "ascending" | "descending"
	return direction(string.split(sort_order or "", ",")[1])
end

-- Pairs each `SortBy` key with the `SortOrder` at its index; keys past the orders take the first.
local function translate_sort(
	entity: Entity,
	sort_by: { string },
	sort_order: string?,
	single_release: boolean
): { SortTerm }
	local keys = SORT_KEYS[entity]
	local orders = string.split(sort_order or "", ",")
	local terms: { SortTerm } = {}
	local seen: { [string]: boolean } = {}
	for index, token in sort_by do
		local order = direction(if index <= #orders then orders[index] else orders[1])
		for _, key in keys[string.lower(token)] or {} do
			-- Only tracks of one release have a release title to sort by.
			if key == "release_title" and not single_release then
				continue
			end
			if not seen[key] then
				seen[key] = true
				table.insert(terms, { key = key, order = order })
			end
		end
	end
	return terms
end

local function has_artist_filter(options: ItemsQueryOptions): boolean
	return #options.artist_ids > 0 or #options.release_artist_ids > 0 or #options.contributing_artist_ids > 0
end

-- The Lyra sort for a listing, with upstream's defaults: request order with `Ids`, newest year
-- first for an artist's albums, relevance when searching, and SortName otherwise.
function module.sort(entity: Entity, options: ItemsQueryOptions, single_release: boolean?): Sort
	local sort_by = options.sort_by
	if #sort_by == 0 then
		if #options.ids > 0 then
			return { sort = nil, seed = nil }
		end
		if entity == "release" and has_artist_filter(options) then
			return {
				sort = { { key = "year", order = "descending" }, { key = "sort_name", order = "ascending" } },
				seed = nil,
			}
		end
		if options.search_term ~= nil then
			return { sort = { { key = "relevance", order = "ascending" } }, seed = nil }
		end
		sort_by = { "SortName" }
	end

	local terms = translate_sort(entity, sort_by, options.sort_order, single_release == true)
	local seed: number? = nil
	for _, term in terms do
		if term.key == "random" then
			seed = math.random(1, 2147483647)
		end
	end
	return { sort = terms, seed = seed }
end

-- Lyra ids for Jellyfin item ids; nil when there are ids but none is a known item, so the
-- filter matches nothing instead of being dropped.
local function decode(raw_ids: { string }): ({ number }?, boolean)
	if #raw_ids == 0 then
		return nil, true
	end
	local decoded = item_ids.parse_jellyfin_ids_array(raw_ids)
	if #decoded == 0 then
		return nil, false
	end
	return decoded, true
end

-- Ids in both lists; either may be nil for "no constraint".
local function intersect(first: { number }?, second: { number }?): { number }?
	if first == nil then
		return second
	end
	if second == nil then
		return first
	end
	local wanted: { [number]: boolean } = {}
	for _, id in second do
		wanted[id] = true
	end
	local both: { number } = {}
	for _, id in first do
		if wanted[id] then
			table.insert(both, id)
		end
	end
	return both
end

type Common = {
	ids: { number }?,
	exclude_ids: { number }?,
	genre_ids: { number }?,
	favorite: boolean?,
	listened: boolean?,
}

-- Sets a filter flag, failing when it contradicts an earlier one.
local function flag(current: boolean?, value: boolean): (boolean?, boolean)
	if current ~= nil and current ~= value then
		return current, false
	end
	return value, true
end

-- The user-state filters, which apply to playlists as well as catalog items.
local USER_STATE_FILTERS: { [string]: boolean } =
	{ isfavorite = true, isfavoriteorlikes = true, isplayed = true, isunplayed = true }

-- The favourite flag `IsFavorite` and `Filters` ask for together; false when they contradict.
-- As upstream, `Filters=IsFavorite` overrides `IsFavorite`, while `IsFavoriteOrLikes` must also
-- hold; Lyra has no likes, so it asks for a favourite.
function module.favorite(options: ItemsQueryOptions): (boolean?, boolean)
	local favorite = options.is_favorite
	local favorite_or_liked = false
	for _, raw_filter in options.filters do
		local filter = string.lower(raw_filter)
		if filter == "isfavorite" then
			favorite = true
		elseif filter == "isfavoriteorlikes" then
			favorite_or_liked = true
		end
	end
	if favorite_or_liked then
		return flag(favorite, true)
	end
	return favorite, true
end

-- The played flag `IsPlayed` and `Filters` ask for together. As upstream, `Filters=IsPlayed` or
-- `IsUnplayed` overrides `IsPlayed`; false when `Filters` names both, which upstream rejects.
function module.played(options: ItemsQueryOptions): (boolean?, boolean)
	local filtered: boolean? = nil
	for _, raw_filter in options.filters do
		local filter = string.lower(raw_filter)
		local accepted = true
		if filter == "isplayed" then
			filtered, accepted = flag(filtered, true)
		elseif filter == "isunplayed" then
			filtered, accepted = flag(filtered, false)
		end
		if not accepted then
			return filtered, false
		end
	end
	if filtered ~= nil then
		return filtered, true
	end
	return options.is_played, true
end

-- Whether `Filters` asks for anything besides favourite and played state.
function module.filters_beyond_user_state(options: ItemsQueryOptions): boolean
	for _, raw_filter in options.filters do
		if not USER_STATE_FILTERS[string.lower(raw_filter)] then
			return true
		end
	end
	return false
end

-- The filters every entity shares; nil when they can match nothing.
local function common(options: ItemsQueryOptions): Common?
	if options.requires_rating or #options.studio_ids > 0 then
		return nil
	end

	local ids, ids_ok = decode(options.ids)
	local exclude_ids = item_ids.parse_jellyfin_ids_array(options.exclude_item_ids)
	local genre_ids, genres_ok = decode(options.genre_ids)
	if not ids_ok or not genres_ok then
		return nil
	end

	local favorite, favorite_ok = module.favorite(options)
	local listened, played_ok = module.played(options)
	if not favorite_ok or not played_ok then
		return nil
	end
	for _, raw_filter in options.filters do
		local filter = string.lower(raw_filter)
		-- Lyra has no likes and nothing resumes.
		if filter == "likes" or filter == "isresumable" then
			return nil
		end
	end

	return {
		ids = ids,
		exclude_ids = if #exclude_ids > 0 then exclude_ids else nil,
		genre_ids = genre_ids,
		favorite = favorite,
		listened = listened,
	}
end

-- Narrows the genre filter to a parent genre; false when the two can't both hold.
local function scope_genres(genre_ids: { number }?, genre_id: number): ({ number }?, boolean)
	local scoped = intersect(genre_ids, { genre_id })
	return scoped, scoped == nil or #scoped > 0
end

-- The artist credit of a track or release listing. Upstream ANDs its artist filters; a Lyra
-- query takes one credit, so the most specific filter present wins.
local function artist_credit(options: ItemsQueryOptions): ({ number }?, CreditRole?, CreditRole?, boolean)
	if #options.contributing_artist_ids > 0 then
		local ids, ok = decode(options.contributing_artist_ids)
		return ids, "track", "release", ok
	end
	if #options.release_artist_ids > 0 then
		local ids, ok = decode(options.release_artist_ids)
		return ids, "release", nil, ok
	end
	local ids, ok = decode(options.artist_ids)
	return ids, if ids then "any" else nil, nil, ok
end

-- Narrows the artist filter to a parent artist, keeping the filter's role, or `role` when there
-- is no filter; false when the parent isn't among the filter's artists.
local function scope_artist(
	artist_ids: { number }?,
	credit_role: CreditRole?,
	artist_id: number,
	role: CreditRole
): ({ number }?, CreditRole?, boolean)
	if artist_ids == nil then
		return { artist_id }, role, true
	end
	local scoped = intersect(artist_ids, { artist_id }) :: { number }
	return scoped, credit_role, #scoped > 0
end

local function page(options: ItemsQueryOptions): (number, number?)
	return options.start_index, options.limit
end

function module.tracks(options: ItemsQueryOptions, scope: Scope): TrackQuery?
	local filters = common(options)
	if not filters then
		return nil
	end
	local artist_ids, credit_role, exclude_credit_role, artists_ok = artist_credit(options)
	local release_ids, albums_ok = decode(options.album_ids)
	if not artists_ok or not albums_ok then
		return nil
	end

	local query: TrackQuery = {}
	local genre_ids = filters.genre_ids
	local scope_id = scope.db_id
	if scope_id ~= nil then
		local ok = true
		if scope.entity_type == "Library" then
			query.library_id = scope_id
		elseif scope.entity_type == "Release" then
			release_ids = intersect(release_ids, { scope_id })
		elseif scope.entity_type == "Artist" then
			artist_ids, credit_role, ok = scope_artist(artist_ids, credit_role, scope_id, "any")
		elseif scope.entity_type == "Genre" then
			genre_ids, ok = scope_genres(genre_ids, scope_id)
		else
			ok = false
		end
		if not ok then
			return nil
		end
	end
	if release_ids ~= nil and #release_ids == 0 then
		return nil
	end

	query.ids = filters.ids
	query.exclude_ids = filters.exclude_ids
	query.release_ids = release_ids
	query.genre_ids = genre_ids
	query.years = if #options.years > 0 then options.years else nil
	query.favorite = filters.favorite
	query.listened = filters.listened
	query.artist_ids = artist_ids
	query.credit_role = credit_role
	query.exclude_credit_role = exclude_credit_role
	query.sort_name_prefix = options.name_starts_with
	query.sort_name_at_least = options.name_starts_with_or_greater
	query.sort_name_below = options.name_less_than
	query.search = options.search_term
	local sort = module.sort("track", options, release_ids ~= nil and #release_ids == 1)
	query.sort = sort.sort :: any
	query.seed = sort.seed
	query.offset, query.limit = page(options)
	return query
end

function module.releases(options: ItemsQueryOptions, scope: Scope): ReleaseQuery?
	local filters = common(options)
	if not filters then
		return nil
	end
	local artist_ids, credit_role, exclude_credit_role, artists_ok = artist_credit(options)
	local album_ids, albums_ok = decode(options.album_ids)
	if not artists_ok or not albums_ok then
		return nil
	end

	local query: ReleaseQuery = {}
	local genre_ids = filters.genre_ids
	local scope_id = scope.db_id
	if scope_id ~= nil then
		local ok = true
		if scope.entity_type == "Library" then
			query.library_id = scope_id
		elseif scope.entity_type == "Artist" then
			-- An artist's children are the albums it is credited on.
			artist_ids, credit_role, ok = scope_artist(artist_ids, credit_role, scope_id, "release")
		elseif scope.entity_type == "Genre" then
			genre_ids, ok = scope_genres(genre_ids, scope_id)
		else
			ok = false
		end
		if not ok then
			return nil
		end
	end
	local ids = intersect(filters.ids, album_ids)
	if ids ~= nil and #ids == 0 then
		return nil
	end

	query.ids = ids
	query.exclude_ids = filters.exclude_ids
	query.genre_ids = genre_ids
	query.years = if #options.years > 0 then options.years else nil
	query.favorite = filters.favorite
	query.listened = filters.listened
	query.artist_ids = artist_ids
	query.credit_role = credit_role
	query.exclude_credit_role = exclude_credit_role
	query.sort_name_prefix = options.name_starts_with
	query.sort_name_at_least = options.name_starts_with_or_greater
	query.sort_name_below = options.name_less_than
	query.search = options.search_term
	local sort = module.sort("release", options)
	query.sort = sort.sort :: any
	query.seed = sort.seed
	query.offset, query.limit = page(options)
	return query
end

-- `role` limits the listing to artists credited in that role, as `/AlbumArtists` does.
function module.artists(options: ItemsQueryOptions, scope: Scope, role: CreditRole?): ArtistQuery?
	local filters = common(options)
	if not filters then
		return nil
	end
	local release_ids, albums_ok = decode(options.album_ids)
	if not albums_ok then
		return nil
	end
	-- On an artist listing, the artist filters name the artists themselves.
	local named: { string } = {}
	for _, list in { options.artist_ids, options.release_artist_ids, options.contributing_artist_ids } do
		table.move(list, 1, #list, #named + 1, named)
	end
	local named_ids, named_ok = decode(named)
	if not named_ok then
		return nil
	end

	local query: ArtistQuery = {}
	local genre_ids = filters.genre_ids
	local scope_id = scope.db_id
	if scope_id ~= nil then
		local ok = true
		if scope.entity_type == "Library" then
			query.library_id = scope_id
		elseif scope.entity_type == "Release" then
			release_ids = intersect(release_ids, { scope_id })
		elseif scope.entity_type == "Track" then
			query.track_ids = { scope_id }
		elseif scope.entity_type == "Genre" then
			genre_ids, ok = scope_genres(genre_ids, scope_id)
		else
			ok = false
		end
		if not ok then
			return nil
		end
	end
	local ids = intersect(filters.ids, named_ids)
	if (ids ~= nil and #ids == 0) or (release_ids ~= nil and #release_ids == 0) then
		return nil
	end

	query.ids = ids
	query.exclude_ids = filters.exclude_ids
	query.release_ids = release_ids
	query.genre_ids = genre_ids
	query.favorite = filters.favorite
	query.listened = filters.listened
	query.credit_role = role
	query.sort_name_prefix = options.name_starts_with
	query.sort_name_at_least = options.name_starts_with_or_greater
	query.sort_name_below = options.name_less_than
	query.search = options.search_term
	local sort = module.sort("artist", options)
	query.sort = sort.sort :: any
	query.seed = sort.seed
	query.offset, query.limit = page(options)
	return query
end

-- The credit types behind each person type, the reverse of `catalog.person_type_for_credit`.
local CREDIT_TYPES_BY_PERSON_TYPE: { [string]: { CreditType } } = {
	Artist = { artists.CreditType.Artist },
	AlbumArtist = { artists.CreditType.Artist },
	Actor = { artists.CreditType.Vocalist, artists.CreditType.Instrumentalist },
	Composer = { artists.CreditType.Composer },
	Lyricist = { artists.CreditType.Lyricist },
	Arranger = { artists.CreditType.Arranger },
	Writer = { artists.CreditType.Writer },
	Producer = { artists.CreditType.Producer },
	Conductor = { artists.CreditType.Conductor },
	Engineer = { artists.CreditType.Engineer },
	Mixer = { artists.CreditType.Mixer },
	Remixer = { artists.CreditType.Remixer },
}

local ANY_CREDIT_TYPES: { CreditType } = {}
for _, credit_types in CREDIT_TYPES_BY_PERSON_TYPE do
	for _, credit_type in credit_types do
		if not table.find(ANY_CREDIT_TYPES, credit_type) then
			table.insert(ANY_CREDIT_TYPES, credit_type)
		end
	end
end

local function credit_types_for(person_types: { string }): { CreditType }
	local credit_types: { CreditType } = {}
	for _, person_type in person_types do
		for _, credit_type in CREDIT_TYPES_BY_PERSON_TYPE[person_type] or {} do
			if not table.find(credit_types, credit_type) then
				table.insert(credit_types, credit_type)
			end
		end
	end
	return credit_types
end

-- People are artists holding a credit of the requested person types, any credit by default.
-- Artist credits count as both Artist and AlbumArtist, except that AlbumArtist alone asks for
-- release credits. `appears_in` is `AppearsInItemId`: people credited on a release or track, or
-- voicing an artist.
function module.people(options: ItemsQueryOptions, scope: Scope, appears_in: Scope?): ArtistQuery?
	local query = module.artists(options, scope, nil)
	if not query then
		return nil
	end
	query.credit_types = ANY_CREDIT_TYPES
	if #options.person_types > 0 then
		local credit_types = credit_types_for(options.person_types)
		if #credit_types == 0 then
			return nil
		end
		query.credit_types = credit_types
		if
			table.find(options.person_types, "AlbumArtist")
			and not table.find(options.person_types, "Artist")
			and #credit_types == 1
		then
			query.credit_role = "release"
		end
	end
	if #options.exclude_person_types > 0 then
		query.exclude_credit_types = credit_types_for(options.exclude_person_types)
	end

	if appears_in == nil then
		return query
	end
	local item_id = appears_in.db_id
	if item_id == nil then
		return nil
	end
	if appears_in.entity_type == "Release" then
		query.release_ids = intersect(query.release_ids, { item_id })
	elseif appears_in.entity_type == "Track" then
		query.track_ids = intersect(query.track_ids, { item_id })
	elseif appears_in.entity_type == "Artist" then
		local voices: { number } = {}
		for _, relation in artists.relations({ item_id })[item_id] or {} do
			local voice_id = relation.artist.db_id
			if relation.relation_type == "voice_actor" and relation.direction == "incoming" and voice_id then
				table.insert(voices, voice_id)
			end
		end
		query.ids = intersect(query.ids, voices)
		-- A voice actor needn't have a credit of their own.
		query.credit_types = nil
		query.exclude_credit_types = nil
	else
		return nil
	end
	local narrowed = { query.ids, query.release_ids, query.track_ids }
	for _, ids in narrowed do
		if ids ~= nil and #ids == 0 then
			return nil
		end
	end
	return query
end

function module.genres(options: ItemsQueryOptions, scope: Scope): GenreQuery?
	local filters = common(options)
	-- Genres can't be favourites in Lyra.
	if not filters or filters.favorite == true then
		return nil
	end
	local release_ids, albums_ok = decode(options.album_ids)
	if not albums_ok then
		return nil
	end

	local query: GenreQuery = {}
	local scope_id = scope.db_id
	if scope_id ~= nil then
		if scope.entity_type == "Library" then
			query.library_id = scope_id
		elseif scope.entity_type == "Release" then
			release_ids = intersect(release_ids, { scope_id })
		else
			return nil
		end
	end
	local ids = intersect(filters.ids, filters.genre_ids)
	if (ids ~= nil and #ids == 0) or (release_ids ~= nil and #release_ids == 0) then
		return nil
	end

	query.ids = ids
	query.exclude_ids = filters.exclude_ids
	query.release_ids = release_ids
	query.sort_name_prefix = options.name_starts_with
	query.sort_name_at_least = options.name_starts_with_or_greater
	query.sort_name_below = options.name_less_than
	query.search = options.search_term
	local sort = module.sort("genre", options)
	query.sort = sort.sort :: any
	query.seed = sort.seed
	query.offset, query.limit = page(options)
	return query
end

return module
