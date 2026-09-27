--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

-- Jellyfin item listings, answered by Lyra catalog queries. `query.luau` translates the request;
-- this module runs the queries, pages across item types and realizes the page.

local artists = require("@lyra/artists")
local favorites = require("@lyra/favorites")
local genres = require("@lyra/genres")
local playlists = require("@lyra/playlists")
local releases = require("@lyra/releases")
local tracks = require("@lyra/tracks")

local catalog = require("./catalog")
local item_ids = require("./identifiers")
local lookups = require("./lookups")
local options_module = require("./options")
local playlist_access = require("../playlist_access")
local query = require("./query")
local results = require("./results")

type ItemsQueryOptions = options_module.ItemsQueryOptions
type ItemInfo = catalog.ItemInfo
type Scope = query.Scope
type CreditRole = artists.CreditRole
type PlaylistInfo = playlists.PlaylistInfo

-- Upstream's default page for `/Items/Latest`.
local LATEST_DEFAULT_LIMIT = 20

-- One item type of a listing: its stubs for a page, and how many it has in all.
type Source = (offset: number, limit: number?) -> ({ ItemInfo }, number)

local module = {}

local function empty(options: ItemsQueryOptions): ({ ItemInfo }, number, number)
	return {}, 0, options.start_index
end

local function track_source(options: ItemsQueryOptions, scope: Scope): Source?
	local built = query.tracks(options, scope)
	if not built then
		return nil
	end
	return function(offset, limit)
		built.offset, built.limit = offset, limit
		local page = tracks.query(built)
		return catalog.track_stubs(page.items), page.total
	end
end

local function release_source(options: ItemsQueryOptions, scope: Scope): Source?
	local built = query.releases(options, scope)
	if not built then
		return nil
	end
	return function(offset, limit)
		built.offset, built.limit = offset, limit
		local page = releases.query(built)
		return catalog.release_stubs(page.items), page.total
	end
end

local function artist_source(options: ItemsQueryOptions, scope: Scope, role: CreditRole?): Source?
	local built = query.artists(options, scope, role)
	if not built then
		return nil
	end
	return function(offset, limit)
		built.offset, built.limit = offset, limit
		local page = artists.query(built)
		return catalog.artist_stubs(page.items), page.total
	end
end

local function person_source(options: ItemsQueryOptions, scope: Scope, appears_in: Scope?): Source?
	local built = query.people(options, scope, appears_in)
	if not built then
		return nil
	end
	return function(offset, limit)
		built.offset, built.limit = offset, limit
		local page = artists.query(built)
		return catalog.person_stubs(page.items), page.total
	end
end

-- Playlists aren't catalog entities. They list at the root only, and only under the filters
-- that mean something for a playlist; they sort by name.
local function playlist_source(options: ItemsQueryOptions, scope: Scope): Source?
	local catalog_only = #options.album_ids > 0
		or #options.artist_ids > 0
		or #options.release_artist_ids > 0
		or #options.contributing_artist_ids > 0
		or #options.genre_ids > 0
		or #options.years > 0
		or #options.studio_ids > 0
		or query.filters_beyond_user_state(options)
		or options.requires_rating
		or options.name_starts_with ~= nil
		or options.name_starts_with_or_greater ~= nil
		or options.name_less_than ~= nil
	local is_favorite, favorite_ok = query.favorite(options)
	local is_played, played_ok = query.played(options)
	if scope.db_id ~= nil or catalog_only or not favorite_ok or not played_ok or options.user_id == nil then
		return nil
	end

	local wanted: { [number]: boolean }? = nil
	if #options.ids > 0 then
		wanted = {}
		for _, id in item_ids.parse_jellyfin_ids_array(options.ids) do
			(wanted :: { [number]: boolean })[id] = true
		end
	end
	local excluded: { [number]: boolean } = {}
	for _, id in item_ids.parse_jellyfin_ids_array(options.exclude_item_ids) do
		excluded[id] = true
	end
	local favorite: { [number]: boolean }? = nil
	if is_favorite ~= nil then
		favorite = {}
		for _, id in favorites.list_ids("playlist") do
			(favorite :: { [number]: boolean })[id] = true
		end
	end
	local search = if options.search_term then string.lower(options.search_term) else nil

	-- `Ids` may name playlists others share; without it, the caller's own playlists list.
	local candidates: { PlaylistInfo } = {}
	if wanted ~= nil then
		for id in wanted do
			local playlist = playlist_access.find(id)
			if playlist then
				table.insert(candidates, playlist)
			end
		end
	else
		candidates = playlist_access.list_owned()
	end

	-- As upstream, a playlist is played once none of the tracks the caller can see is unplayed.
	local unplayed: { [number]: boolean } = {}
	if is_played ~= nil then
		local track_ids: { number } = {}
		for _, playlist in candidates do
			for _, track_id in playlist_access.track_ids(playlist) do
				table.insert(track_ids, track_id)
			end
		end
		if #track_ids > 0 then
			for _, track in tracks.query({ ids = track_ids, listened = false }).items do
				if track.db_id then
					unplayed[track.db_id] = true
				end
			end
		end
	end

	local stubs: { ItemInfo } = {}
	for _, playlist in candidates do
		local id = playlist.db_id
		if
			id == nil
			or not playlist_access.can_view(playlist, options.user_id)
			or excluded[id]
			or (favorite ~= nil and (favorite[id] == true) ~= is_favorite)
			or (search ~= nil and string.find(string.lower(playlist.name or ""), search, 1, true) == nil)
		then
			continue
		end
		if is_played ~= nil then
			local played = true
			for _, track_id in playlist_access.track_ids(playlist) do
				if unplayed[track_id] then
					played = false
					break
				end
			end
			if played ~= is_played then
				continue
			end
		end
		local stub = catalog.playlist_stub(playlist)
		if stub then
			table.insert(stubs, stub)
		end
	end
	table.sort(stubs, function(a: ItemInfo, b: ItemInfo): boolean
		return string.lower(a.SortName or "") < string.lower(b.SortName or "")
	end)
	if string.lower(options.sort_order or "") == "descending" then
		local count = #stubs
		for index = 1, count // 2 do
			stubs[index], stubs[count - index + 1] = stubs[count - index + 1], stubs[index]
		end
	end

	return function(offset, limit)
		local page = results.paginate_items(stubs, offset, limit)
		return page, #stubs
	end
end

-- Pages across the sources as if they were one list, in source order.
local function page_sources(sources: { Source }, start_index: number, limit: number?): ({ ItemInfo }, number)
	local stubs: { ItemInfo } = {}
	local total = 0
	local skip = start_index
	local remaining = limit
	for _, source in sources do
		local items, count = source(skip, remaining)
		table.move(items, 1, #items, #stubs + 1, stubs)
		total += count
		skip = math.max(0, skip - count)
		if remaining ~= nil then
			remaining = math.max(0, remaining - #items)
		end
	end
	return stubs, total
end

-- `Ids` with no `SortBy` answers in request order, which only the listing knows.
local function in_request_order(
	sources: { Source },
	options: ItemsQueryOptions,
	extras: { ItemInfo }
): ({ ItemInfo }, number)
	local found: { [number]: ItemInfo } = {}
	for _, source in sources do
		for _, stub in (source(0, nil)) do
			local raw = stub._raw
			if raw and raw.db_id then
				found[raw.db_id] = stub
			end
		end
	end
	for _, extra in extras do
		local id = item_ids.decode_jellyfin_item_id(extra.Id)
		if id then
			found[id] = extra
		end
	end

	local ordered: { ItemInfo } = {}
	local seen: { [number]: boolean } = {}
	for _, id in item_ids.parse_jellyfin_ids_array(options.ids) do
		local stub = found[id]
		if stub and not seen[id] then
			seen[id] = true
			table.insert(ordered, stub)
		end
	end
	local page, total = results.paginate_items(ordered, options.start_index, options.limit)
	return page, total
end

local SORTED_ENTITIES: { [string]: query.Entity } = { audio = "track", musicalbum = "release", musicartist = "artist" }

-- Whether `SortBy` names a key with a Lyra meaning for a listed type. Upstream sorts a playlist's
-- children in memory and skips the keys it can't compare, so if every key drops they keep
-- playlist order.
local function sorted(options: ItemsQueryOptions): boolean
	for item_type, entity in SORTED_ENTITIES do
		local terms = if options_module.wants_item_type(options, item_type)
			then query.sort(entity, options).sort
			else nil
		if terms and #terms > 0 then
			return true
		end
	end
	return false
end

-- Genres and libraries named by `Ids`; they don't list otherwise.
local function named_extras(options: ItemsQueryOptions): { ItemInfo }
	local extras: { ItemInfo } = {}
	if #options.ids == 0 then
		return extras
	end
	local ids = item_ids.parse_jellyfin_ids_array(options.ids)
	if options_module.wants_item_type(options, "musicgenre") then
		for _, genre in genres.get(ids) do
			local item = catalog.genre_item(genre.name, genre.id, nil, nil, nil, nil)
			if item then
				table.insert(extras, item)
			end
		end
	end
	if options_module.wants_item_type(options, "collectionfolder") then
		for _, id in ids do
			local item = catalog.library_view_item(id)
			if item then
				table.insert(extras, item)
			end
		end
	end
	return extras
end

-- Upstream's children of a parent when `IncludeItemTypes` names none. An album holds its tracks
-- and a playlist its entries; an artist or genre is only a name upstream, with no children.
local CHILD_TYPES: { [string]: { string } } = {
	Release = { "Audio" },
	Playlist = { "Audio" },
	Artist = {},
	Genre = {},
}

-- A playlist's children are its tracks, each once, in playlist order. As upstream, they list as the
-- tracks those ids name, so the listing's filters and `SortBy` apply; nil when there are none the
-- caller can see.
local function playlist_children(options: ItemsQueryOptions, scope: Scope): ItemsQueryOptions?
	local playlist = playlist_access.find(scope.db_id)
	if not playlist or not playlist_access.can_view(playlist, options.user_id) then
		return nil
	end
	local wanted: { [number]: boolean }? = nil
	if #options.ids > 0 then
		wanted = {}
		for _, id in item_ids.parse_jellyfin_ids_array(options.ids) do
			(wanted :: { [number]: boolean })[id] = true
		end
	end
	local ids: { string } = {}
	for _, track_id in playlist_access.track_ids(playlist) do
		local hex_id = item_ids.encode_jellyfin_item_id(track_id)
		if hex_id and (wanted == nil or wanted[track_id]) then
			table.insert(ids, hex_id)
		end
	end
	if #ids == 0 then
		return nil
	end
	local children = table.clone(options)
	children.parent_id = nil
	children.ids = ids
	return children
end

-- Lists the catalog items and playlists `options` asks for, paged as one list. `Ids` without
-- `SortBy` answer in request order; a playlist's children keep playlist order also when their
-- `SortBy` has no Lyra meaning.
local function list(options: ItemsQueryOptions, playlist_order: boolean): ({ ItemInfo }, number, number)
	local scope = lookups.scope(options.parent_id)
	local sources: { Source } = {}
	local function add(source: Source?)
		if source then
			table.insert(sources, source)
		end
	end
	if options_module.wants_item_type(options, "musicalbum") then
		add(release_source(options, scope))
	end
	if options_module.wants_item_type(options, "musicartist") then
		add(artist_source(options, scope, nil))
	end
	if options_module.wants_item_type(options, "audio") then
		add(track_source(options, scope))
	end
	if options_module.wants_item_type(options, "playlist") then
		add(playlist_source(options, scope))
	end

	local stubs: { ItemInfo }, total: number
	if #options.ids > 0 and (if playlist_order then not sorted(options) else #options.sort_by == 0) then
		stubs, total = in_request_order(sources, options, named_extras(options))
	else
		stubs, total = page_sources(sources, options.start_index, options.limit)
	end
	return catalog.realize(stubs, options), total, options.start_index
end

function module.list_items(options: ItemsQueryOptions): ({ ItemInfo }, number, number)
	local scope = lookups.scope(options.parent_id)
	local child_types = if scope.entity_type then CHILD_TYPES[scope.entity_type] else nil
	if child_types and #options.include_item_types == 0 then
		local wanted: { string } = {}
		for _, child_type in child_types do
			if options_module.wants_item_type(options, child_type) then
				table.insert(wanted, child_type)
			end
		end
		if #wanted == 0 then
			return empty(options)
		end
		options = table.clone(options)
		options.include_item_types = wanted
	end
	if scope.entity_type == "Playlist" then
		local children = playlist_children(options, scope)
		if not children then
			return empty(options)
		end
		return list(children, true)
	end
	return list(options, false)
end

-- The newest albums by the date they were added; `IsPlayed` is ignored, as upstream. Audio
-- groups into its albums unless `GroupItems=false` asks for the newest tracks themselves.
function module.list_latest_items(options: ItemsQueryOptions): { ItemInfo }
	local latest = options_module.clone(options)
	latest.is_played = nil
	latest.start_index = 0
	latest.limit = options.limit or LATEST_DEFAULT_LIMIT
	latest.sort_by = { "DateCreated", "SortName", "ProductionYear" }
	latest.sort_order = "Descending"

	local scope = lookups.scope(options.parent_id)
	local wants_albums = options_module.wants_item_type(latest, "musicalbum")
	local wants_audio = options_module.wants_item_type(latest, "audio")
	local source: Source?
	if wants_audio and not wants_albums and not options.group_items then
		source = track_source(latest, scope)
	elseif wants_albums or wants_audio then
		source = release_source(latest, scope)
	end
	if not source then
		return {}
	end
	local stubs = source(0, latest.limit)
	return catalog.realize(stubs, latest)
end

-- `/Artists` lists artists credited in any role; `/Artists/AlbumArtists` passes the release role.
function module.list_artist_items(options: ItemsQueryOptions, role: CreditRole?): ({ ItemInfo }, number, number)
	local source = artist_source(options, lookups.scope(options.parent_id), role)
	if not source then
		return empty(options)
	end
	local stubs, total = source(options.start_index, options.limit)
	return catalog.realize(stubs, options), total, options.start_index
end

function module.list_person_items(options: ItemsQueryOptions): ({ ItemInfo }, number, number)
	local appears_in = if options.appears_in_item_id then lookups.scope(options.appears_in_item_id) else nil
	local source = person_source(options, lookups.scope(options.parent_id), appears_in)
	if not source then
		return empty(options)
	end
	local stubs, total = source(options.start_index, options.limit)
	return catalog.realize(stubs, options), total, options.start_index
end

-- Albums, songs and artists in a genre, within the listing's library.
local function genre_counts(genre_id: number, library_id: number?): catalog.GenreCounts
	return {
		release_count = releases.query({ genre_ids = { genre_id }, library_id = library_id, limit = 0 }).total,
		song_count = tracks.query({ genre_ids = { genre_id }, library_id = library_id, limit = 0 }).total,
		artist_count = artists.query({ genre_ids = { genre_id }, library_id = library_id, limit = 0 }).total,
	}
end

function module.list_genre_items(options: ItemsQueryOptions): ({ ItemInfo }, number, number)
	local built = query.genres(options, lookups.scope(options.parent_id))
	if not built then
		return empty(options)
	end
	local page = genres.query(built)

	local genre_ids: { number } = {}
	for _, genre in page.items do
		if genre.db_id then
			table.insert(genre_ids, genre.db_id)
		end
	end
	local releases_by_genre = if #genre_ids > 0 then releases.by_genre(genre_ids) else {}
	local library_hex_id = catalog.library_hex_id_from_parent(options.parent_id)

	local items: { ItemInfo } = {}
	for _, genre in page.items do
		local genre_id = genre.db_id
		if genre_id == nil then
			continue
		end
		local release_ids: { number } = {}
		for _, release in releases_by_genre[genre_id] or {} do
			if release.db_id then
				table.insert(release_ids, release.db_id)
			end
		end
		local item = catalog.genre_item(
			genre.name,
			genre.id,
			nil,
			genre_counts(genre_id, built.library_id),
			library_hex_id,
			catalog.genre_image_tag(release_ids, genre.id)
		)
		if item then
			table.insert(items, item)
		end
	end
	return items, page.total, page.offset
end

-- Library views answer searches by name and sort by name.
function module.list_views(options: ItemsQueryOptions): ({ ItemInfo }, number, number)
	local search = if options.search_term then string.lower(options.search_term) else nil
	local views: { ItemInfo } = {}
	for _, view in catalog.to_music_view_items() do
		if search == nil or string.find(string.lower(view.Name or ""), search, 1, true) ~= nil then
			table.insert(views, view)
		end
	end
	local descending = string.lower(options.sort_order or "") == "descending"
	table.sort(views, function(a: ItemInfo, b: ItemInfo): boolean
		local a_name, b_name = string.lower(a.SortName or a.Name or ""), string.lower(b.SortName or b.Name or "")
		if descending then
			return a_name > b_name
		end
		return a_name < b_name
	end)
	return results.paginate_items(views, options.start_index, options.limit)
end

local NAME_LOOKUP_LIMIT = 200

-- The item among a search's first page with exactly this name, case-insensitively.
local function named<T>(items: { T }, name: string, name_of: (T) -> string?): T?
	local lowered = string.lower(name)
	for _, item in items do
		local item_name = name_of(item)
		if item_name and string.lower(item_name) == lowered then
			return item
		end
	end
	return nil
end

local function search_options(name: string, user_id: string?): ItemsQueryOptions
	local options = options_module.parse(nil)
	options.user_id = user_id
	options.search_term = name
	options.limit = NAME_LOOKUP_LIMIT
	return options
end

function module.find_artist_by_name(name: string, user_id: string): ItemInfo?
	local items = module.list_artist_items(search_options(name, user_id), nil)
	return named(items, name, function(item: ItemInfo): string?
		return item.Name
	end)
end

function module.find_person_by_name(name: string, user_id: string): ItemInfo?
	local items = module.list_person_items(search_options(name, user_id))
	return named(items, name, function(item: ItemInfo): string?
		return item.Name
	end)
end

function module.find_genre_by_name(name: string, user_id: string): ItemInfo?
	local items = module.list_genre_items(search_options(name, user_id))
	return named(items, name, function(item: ItemInfo): string?
		return item.Name
	end)
end

-- `with_provider_ids` adds ProviderIds and ExternalUrls, as single-item responses carry.
function module.find_item_by_id(item_id: number?, user_id: string?, with_provider_ids: boolean?): ItemInfo?
	local item_hex_id = if item_id and item_id > 0 then item_ids.encode_jellyfin_item_id(item_id) else nil
	if not item_hex_id then
		return nil
	end

	local options = options_module.parse(nil)
	options.ids = { item_hex_id }
	options.limit = 1
	options.recursive = true
	options.include_media_info = true
	options.include_people = true
	options.include_overview = true
	options.include_parent_id = true
	options.include_provider_ids = with_provider_ids == true
	options.include_external_urls = with_provider_ids == true
	options.group_items = false
	options.user_id = user_id
	return (module.list_items(options))[1]
end

local ANCESTORS_MAX_DEPTH = 8

-- Visited-set prevents cycle duplicates; boolean return distinguishes "missing" from "no ancestors".
function module.ancestors_chain(item_id: number, user_id: string): ({ ItemInfo }, boolean)
	local chain: { ItemInfo } = {}
	if item_id <= 0 then
		return chain, false
	end

	local current = module.find_item_by_id(item_id, user_id)
	if not current then
		return chain, false
	end

	local visited: { [number]: boolean } = { [item_id] = true }
	while current and #chain < ANCESTORS_MAX_DEPTH do
		local parent_db_id = item_ids.decode_jellyfin_item_id(current.ParentId)
		if not parent_db_id or visited[parent_db_id] then
			break
		end
		visited[parent_db_id] = true
		local parent = module.find_item_by_id(parent_db_id, user_id)
		if not parent then
			break
		end
		table.insert(chain, parent)
		current = parent
	end
	return chain, true
end

return module
