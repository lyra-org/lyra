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

local artwork = require("./images")
local catalog = require("./catalog")
local constants = require("../../constants")
local enrichment = require("./enrichment")
local instant_mix = require("./instant_mix")
local item_ids = require("./identifiers")
local lookups = require("./lookups")
local options_module = require("./options")
local playlist_access = require("../playlist_access")
local preferences = require("../preferences")
local query = require("./query")
local results = require("./results")

type ItemsQueryOptions = options_module.ItemsQueryOptions
type ItemInfo = catalog.ItemInfo
type Scope = query.Scope
type CreditRole = artists.CreditRole
type Playlist = playlists.Playlist

-- Upstream's default page for `/Items/Latest`.
local LATEST_DEFAULT_LIMIT = 20

-- One item type of a listing: its stubs for a page, and how many it has in all.
type Source = (offset: number, limit: number?) -> ({ ItemInfo }, number)
-- Takes a listing's stubs, a batch at a time, in order.
type Emit = ({ ItemInfo }) -> ()

local module = {}

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

-- Orders playlist stubs by the terms, then by name and id. Random draws anew on every call.
local function sort_playlists(stubs: { ItemInfo }, terms: { query.SortTerm }): ()
	local playlist_ids: { [ItemInfo]: string } = {}
	local created: { [ItemInfo]: number } = {}
	local names: { [ItemInfo]: string } = {}
	local ids: { string } = {}
	for _, stub in stubs do
		local playlist = stub._raw :: Playlist
		playlist_ids[stub] = playlist.id
		created[stub] = tonumber(playlist.created_at_ms) or 0
		names[stub] = string.lower(stub.SortName or "")
		table.insert(ids, playlist.id)
	end
	local columns: { { [ItemInfo]: any } } = {}
	for index, term in terms do
		local column: { [ItemInfo]: any } = names :: any
		if term.key == "runtime" then
			local ticks = enrichment.playlist_runtime_ticks(ids)
			column = {}
			for _, stub in stubs do
				column[stub] = ticks[playlist_ids[stub]] or 0
			end
		elseif term.key == "date_created" then
			column = created :: any
		elseif term.key == "random" then
			column = {}
			for _, stub in stubs do
				column[stub] = math.random()
			end
		end
		columns[index] = column
	end
	table.sort(stubs, function(a: ItemInfo, b: ItemInfo): boolean
		for index, term in terms do
			local a_value, b_value = columns[index][a], columns[index][b]
			if a_value ~= b_value then
				if term.order == "descending" then
					return a_value > b_value
				end
				return a_value < b_value
			end
		end
		if names[a] ~= names[b] then
			return names[a] < names[b]
		end
		return playlist_ids[a] < playlist_ids[b]
	end)
end

-- Playlists aren't catalog entities. They list only under the filters that mean something for a
-- playlist, and sort as `query.playlist_sort` says. As upstream, under a parent they list only
-- when they are the one type named, and then only those with a track under that parent.
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
		or options.resume
		or options.name_starts_with ~= nil
		or options.name_starts_with_or_greater ~= nil
		or options.name_less_than ~= nil
	local is_favorite, favorite_ok = query.favorite(options)
	local is_played, played_ok = query.played(options)
	if catalog_only or not favorite_ok or not played_ok or options.user_id == nil then
		return nil
	end
	local only_playlists = #options.include_item_types == 1
		and string.lower(options.include_item_types[1]) == "playlist"
	if scope.id ~= nil and not only_playlists then
		return nil
	end

	local wanted: { [string]: boolean }? = nil
	if #options.ids > 0 then
		wanted = {}
		for _, id in item_ids.decode_list(options.ids) do
			(wanted :: { [string]: boolean })[id] = true
		end
	end
	local excluded: { [string]: boolean } = {}
	for _, id in item_ids.decode_list(options.exclude_item_ids) do
		excluded[id] = true
	end
	local favorite: { [string]: boolean }? = nil
	if is_favorite ~= nil then
		favorite = {}
		for _, id in favorites.list_ids("playlist") do
			(favorite :: { [string]: boolean })[id] = true
		end
	end
	local search = if options.search_term then string.lower(options.search_term) else nil

	-- `Ids` may name any playlist the caller can see; without it, every one lists.
	local candidates: { Playlist } = {}
	if wanted ~= nil then
		local wanted_ids: { string } = {}
		for id in wanted do
			table.insert(wanted_ids, id)
		end
		candidates = playlist_access.find_many(wanted_ids)
	else
		candidates = playlist_access.list_visible()
	end

	local shown: { Playlist } = {}
	for _, playlist in candidates do
		local id = playlist.id
		if
			excluded[id]
			or (favorite ~= nil and (favorite[id] == true) ~= is_favorite)
			or (search ~= nil and string.find(string.lower(playlist.name or ""), search, 1, true) == nil)
		then
			continue
		end
		table.insert(shown, playlist)
	end

	local shown_ids: { string } = {}
	for _, playlist in shown do
		table.insert(shown_ids, playlist.id)
	end
	local entries: { [string]: { playlists.PlaylistEntry } } = if #shown_ids > 0
		then playlists.entries(shown_ids)
		else {}
	local under: { [string]: boolean }? = nil
	if scope.id ~= nil then
		local track_ids: { string } = {}
		for _, links in entries do
			for _, link in links do
				table.insert(track_ids, link.track_id)
			end
		end
		local scoped = query.tracks_under(track_ids, scope)
		if not scoped or #track_ids == 0 then
			return nil
		end
		under = {}
		for _, track in tracks.query(scoped).items do
			(under :: { [string]: boolean })[track.id] = true
		end
	end
	-- As upstream, a playlist is played once none of the tracks the caller can see is unplayed.
	local unplayed: { [string]: boolean } = {}
	if is_played ~= nil then
		local track_ids: { string } = {}
		for _, links in entries do
			for _, link in links do
				table.insert(track_ids, link.track_id)
			end
		end
		if #track_ids > 0 then
			for _, track in tracks.query({ ids = track_ids, listened = false }).items do
				unplayed[track.id] = true
			end
		end
	end
	local stubs: { ItemInfo } = {}
	for _, playlist in shown do
		local links = entries[playlist.id]
		if under ~= nil then
			local found = false
			for _, link in links or {} do
				if under[link.track_id] then
					found = true
					break
				end
			end
			if not found then
				continue
			end
		end
		if is_played ~= nil then
			local played = true
			for _, link in links or {} do
				if unplayed[link.track_id] then
					played = false
					break
				end
			end
			if played ~= is_played then
				continue
			end
		end
		local stub = catalog.playlist_stub(playlist, if links then #links else 0)
		if stub then
			table.insert(stubs, stub)
		end
	end
	sort_playlists(stubs, query.playlist_sort(options))

	return function(offset, limit)
		local page = results.paginate_items(stubs, offset, limit)
		return page, #stubs
	end
end

-- Pages across the sources as if they were one list, in source order, handing `emit` the page a
-- batch at a time; returns the total. Unless `count_all`, the sources after the page are left
-- unqueried and the total leaves them out.
local function page_sources(
	sources: { Source },
	start_index: number,
	limit: number?,
	count_all: boolean,
	emit: Emit
): number
	local total = 0
	local skip = start_index
	local remaining = limit
	for _, source in sources do
		if remaining == 0 and not count_all then
			break
		end
		local offset = skip
		local wanted = if remaining ~= nil then math.min(results.BATCH_SIZE, remaining) else results.BATCH_SIZE
		local items, count = source(offset, wanted)
		total += count
		while true do
			if #items > 0 then
				emit(items)
			end
			if remaining ~= nil then
				remaining = math.max(0, remaining - #items)
			end
			offset += #items
			if #items < wanted or remaining == 0 or offset >= count then
				break
			end
			wanted = if remaining ~= nil then math.min(results.BATCH_SIZE, remaining) else results.BATCH_SIZE
			items = (source(offset, wanted))
		end
		skip = math.max(0, skip - count)
	end
	return total
end

-- The items a listing that takes `emit` hands over, collected whole, with its total and start.
local function collected(each: (emit: Emit) -> (number, number)): ({ ItemInfo }, number, number)
	local items: { ItemInfo } = {}
	local total, start_index = each(function(batch)
		table.move(batch, 1, #batch, #items + 1, items)
	end)
	return items, total, start_index
end

-- Realizes the stubs of a source's pages a batch at a time, handing `emit` each batch it keeps.
local function realizing(options: ItemsQueryOptions, emit: Emit): Emit
	return function(stubs)
		local items = catalog.realize(stubs, options)
		stubs = nil :: any
		if #items > 0 then
			emit(items)
		end
	end
end

-- `Ids` with no `SortBy` answers in request order, which only the listing knows.
local function in_request_order(
	sources: { Source },
	options: ItemsQueryOptions,
	extras: { ItemInfo }
): ({ ItemInfo }, number)
	local found: { [string]: ItemInfo } = {}
	for _, source in sources do
		for _, stub in (source(0, nil)) do
			local raw = stub._raw
			if raw then
				found[raw.id] = stub
			end
		end
	end
	-- Views without an entity of their own match by their item id.
	local found_views: { [string]: ItemInfo } = {}
	for _, extra in extras do
		local id = item_ids.decode(extra.Id)
		if id then
			found[id] = extra
		elseif extra.Id then
			found_views[extra.Id] = extra
		end
	end

	local ordered: { ItemInfo } = {}
	local seen: { [ItemInfo]: boolean } = {}
	for _, raw_id in options.ids do
		local id = item_ids.decode(raw_id)
		local view_id = item_ids.normalize_guid(raw_id)
		local stub = if id then found[id] elseif view_id then found_views[view_id] else nil
		if stub and not seen[stub] then
			seen[stub] = true
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

-- Whether `IncludeItemTypes` names `item_type` itself, not just by naming no type.
local function names_item_type(options: ItemsQueryOptions, item_type: string): boolean
	if #options.include_item_types == 0 then
		return false
	end
	return options_module.wants_item_type(options, item_type)
end

-- Playlists the caller sees, by the folder upstream lists them under.
local function playlists_folder(options: ItemsQueryOptions): ItemInfo
	local count = if options.include_child_count then playlist_access.count_visible() else nil
	return catalog.playlists_folder_item(count)
end

-- Genres and libraries named by `Ids`, and the playlists view when `Ids` or `IncludeItemTypes`
-- names it; they don't list otherwise.
local function named_extras(options: ItemsQueryOptions, scope: Scope): { ItemInfo }
	local extras: { ItemInfo } = {}
	if #options.ids == 0 then
		if scope.id == nil and names_item_type(options, "manualplaylistsfolder") then
			table.insert(extras, playlists_folder(options))
		end
		return extras
	end
	if options_module.wants_item_type(options, "musicgenre") then
		local ids = item_ids.decode_list(options.ids)
		local found = genres.get(ids)
		local cover_infos = artwork.get_artwork_infos(ids)
		for _, id in ids do
			local genre = found[id]
			local item = if genre then catalog.genre_item(genre.name, genre.id, nil, nil, cover_infos[id]) else nil
			if item then
				table.insert(extras, item)
			end
		end
	end
	if options_module.wants_item_type(options, "collectionfolder") then
		local wanted: { [string]: boolean } = {}
		for _, id in options.ids do
			local guid = item_ids.normalize_guid(id)
			if guid then
				wanted[guid] = true
			end
		end
		for _, view in catalog.to_music_view_items(options.include_child_count) do
			if view.Id and wanted[view.Id] then
				table.insert(extras, view)
			end
		end
	end
	if options_module.wants_item_type(options, "manualplaylistsfolder") then
		for _, id in options.ids do
			if constants.is_playlists_folder_id(id) then
				table.insert(extras, playlists_folder(options))
				break
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
	local playlist = playlist_access.find(scope.id)
	if not playlist or not playlist_access.can_view(playlist, options.user_id) then
		return nil
	end
	local wanted: { [string]: boolean }? = nil
	if #options.ids > 0 then
		wanted = {}
		for _, id in item_ids.decode_list(options.ids) do
			(wanted :: { [string]: boolean })[id] = true
		end
	end
	local ids: { string } = {}
	for _, track_id in playlist_access.track_ids(playlist) do
		local hex_id = item_ids.encode(track_id)
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

-- Lists the catalog items and playlists `options` asks for, paged as one list, handing `emit` the
-- stubs a batch at a time; returns the total and the start. `Ids` without `SortBy` answer in
-- request order; a playlist's children keep playlist order also when their `SortBy` has no Lyra
-- meaning.
local function list(
	options: ItemsQueryOptions,
	playlist_order: boolean,
	count_all: boolean,
	emit: Emit
): (number, number)
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

	local extras = named_extras(options, scope)
	local total: number
	if #options.ids > 0 and (if playlist_order then not sorted(options) else #options.sort_by == 0) then
		local stubs
		stubs, total = in_request_order(sources, options, extras)
		results.in_batches(stubs, emit)
	else
		-- Named genres and libraries follow the catalog items, in request order.
		if #extras > 0 then
			table.insert(sources, function(offset, limit)
				return results.paginate_items(extras, offset, limit), #extras
			end)
		end
		total = page_sources(sources, options.start_index, options.limit, count_all, emit)
	end
	return total, options.start_index
end

-- What a listing's total counts: every match, only the page's items, or none. As upstream, only
-- `EnableTotalRecordCount=false` narrows it, and not for the listings upstream holds in memory
-- whole: searches and a parent's own children. Its database counts only the page of resume and
-- recursive listings and those by `Ids`, as of those at the root or of a library's items by type,
-- which upstream makes recursive unless the request says otherwise. Playlists alone, which upstream lists from the root under any
-- parent but a playlist and filters in memory, count none.
type Counted = "all" | "page" | "none"

local function counted(options: ItemsQueryOptions, scope: Scope): Counted
	if options.enable_total_record_count then
		return "all"
	elseif options.resume then
		return "page"
	elseif options.search_term ~= nil then
		return "all"
	elseif #options.ids > 0 then
		return "page"
	end
	local only_playlists = #options.include_item_types == 1
		and string.lower(options.include_item_types[1]) == "playlist"
	if only_playlists and scope.entity_type ~= "Playlist" then
		return "none"
	elseif
		options.recursive == true
		or scope.id == nil
		or (scope.entity_type == "Library" and #options.include_item_types > 0 and not options.recursive_given)
	then
		return "page"
	end
	return "all"
end

-- Lists the items `options` asks for as `list_items` does, handing `emit` the realized items a batch
-- at a time, in order; returns the total and the start.
function module.each_item_batch(options: ItemsQueryOptions, emit: ({ ItemInfo }) -> ()): (number, number)
	-- As upstream, the playlists view lists every playlist the caller sees, whatever type is named.
	if constants.is_playlists_folder_id(options.parent_id) then
		options = table.clone(options)
		options.parent_id = nil
		options.include_item_types = { "Playlist" }
		options.recursive = true
	end
	local scope = lookups.scope(options.parent_id)
	local count = counted(options, scope)
	local child_types = if scope.entity_type then CHILD_TYPES[scope.entity_type] else nil
	if child_types and #options.include_item_types == 0 then
		local wanted: { string } = {}
		for _, child_type in child_types do
			if options_module.wants_item_type(options, child_type) then
				table.insert(wanted, child_type)
			end
		end
		if #wanted == 0 then
			return 0, options.start_index
		end
		options = table.clone(options)
		options.include_item_types = wanted
	end
	local playlist_order = false
	if scope.entity_type == "Playlist" then
		local children = playlist_children(options, scope)
		if not children then
			return 0, options.start_index
		end
		options = children
		playlist_order = true
	end
	local realize_with = options
	local listed = 0
	local total, start_index = list(options, playlist_order, count == "all", function(stubs)
		local items = catalog.realize(stubs, realize_with)
		stubs = nil :: any
		listed += #items
		if #items > 0 then
			emit(items)
		end
	end)
	if count == "page" then
		return listed, start_index
	elseif count == "none" then
		return 0, start_index
	end
	return total, start_index
end

function module.list_items(options: ItemsQueryOptions): ({ ItemInfo }, number, number)
	return collected(function(emit)
		return module.each_item_batch(options, emit)
	end)
end

-- The caller's libraries it keeps in and excludes from its latest media (`LatestItemsExcludes`),
-- which upstream's latest and resume listings leave out when they have no parent.
function module.libraries_by_latest_exclusion(user_id: string): ({ ItemInfo }, { ItemInfo })
	local excludes = preferences.user_configuration_ids(user_id, "LatestItemsExcludes")
	local kept: { ItemInfo } = {}
	local excluded: { ItemInfo } = {}
	for _, library in catalog.to_music_view_items(false) do
		table.insert(if excludes[library.Id or ""] then excluded else kept, library)
	end
	return kept, excluded
end

-- The newest albums by the date they were added. As upstream for music, `IsPlayed` and with it
-- `HidePlayedInLatest` are ignored. Audio groups into its albums unless `GroupItems=false` asks
-- for the newest tracks themselves.
function module.list_latest_items(options: ItemsQueryOptions): { ItemInfo }
	local latest = table.clone(options)
	latest.is_played = nil
	latest.start_index = 0
	latest.limit = options.limit or LATEST_DEFAULT_LIMIT
	latest.sort_by = { "DateCreated", "SortName", "ProductionYear" }
	latest.sort_order = "Descending"

	local wants_albums = options_module.wants_item_type(latest, "musicalbum")
	local wants_audio = options_module.wants_item_type(latest, "audio")
	local source_for: ((ItemsQueryOptions, Scope) -> Source?)? = nil
	if wants_audio and not wants_albums and not options.group_items then
		source_for = track_source
	elseif wants_albums or wants_audio then
		source_for = release_source
	end
	if not source_for then
		return {}
	end

	-- As upstream, without a parent the newest items come from the libraries the caller doesn't
	-- exclude from its latest media: the newest of each library, then the newest of those.
	local user_id = latest.user_id
	if options.parent_id == nil and user_id then
		local kept, excluded = module.libraries_by_latest_exclusion(user_id)
		if #excluded > 0 then
			local candidates: { string } = {}
			for _, library in kept do
				local library_source = source_for(latest, lookups.scope(library.Id))
				if library_source then
					for _, stub in (library_source(0, latest.limit)) do
						if stub.Id then
							table.insert(candidates, stub.Id)
						end
					end
				end
			end
			if #candidates == 0 then
				return {}
			end
			latest.ids = candidates
		end
	end

	local source = source_for(latest, lookups.scope(options.parent_id))
	if not source then
		return {}
	end
	local stubs = source(0, latest.limit)
	return catalog.realize(stubs, latest)
end

-- Upstream's total of a listing by name, such as `/Artists` or `/Genres`: zero with
-- `EnableTotalRecordCount=false`, which leaves it uncounted.
local function by_name_total(options: ItemsQueryOptions, total: number): number
	return if options.enable_total_record_count then total else 0
end

-- `/Artists` lists artists credited in any role; `/Artists/AlbumArtists` passes the release role.
-- Hands `emit` the realized artists a batch at a time, in order; returns the total and the start.
function module.each_artist_batch(options: ItemsQueryOptions, role: CreditRole?, emit: Emit): (number, number)
	local source = artist_source(options, lookups.scope(options.parent_id), role)
	if not source then
		return 0, options.start_index
	end
	local total = page_sources({ source }, options.start_index, options.limit, true, realizing(options, emit))
	return by_name_total(options, total), options.start_index
end

function module.list_artist_items(options: ItemsQueryOptions, role: CreditRole?): ({ ItemInfo }, number, number)
	return collected(function(emit)
		return module.each_artist_batch(options, role, emit)
	end)
end

-- Hands `emit` the realized people a batch at a time, in order; returns the total and the start.
function module.each_person_batch(options: ItemsQueryOptions, emit: Emit): (number, number)
	local appears_in = if options.appears_in_item_id then lookups.scope(options.appears_in_item_id) else nil
	local source = person_source(options, lookups.scope(options.parent_id), appears_in)
	if not source then
		return 0, options.start_index
	end
	local total = page_sources({ source }, options.start_index, options.limit, true, realizing(options, emit))
	return total, options.start_index
end

function module.list_person_items(options: ItemsQueryOptions): ({ ItemInfo }, number, number)
	return collected(function(emit)
		return module.each_person_batch(options, emit)
	end)
end

-- Hands `emit` the genres a batch at a time, in order; returns the total and the start.
function module.each_genre_batch(options: ItemsQueryOptions, emit: Emit): (number, number)
	local built = query.genres(options, lookups.scope(options.parent_id))
	if not built then
		return 0, options.start_index
	end
	local page = genres.query(built)

	local genre_ids: { string } = {}
	for _, genre in page.items do
		table.insert(genre_ids, genre.id)
	end
	local cover_infos = artwork.get_artwork_infos(genre_ids)
	local library_hex_id = catalog.library_hex_id_from_parent(options.parent_id)

	local items: { ItemInfo } = {}
	for _, genre in page.items do
		local item = catalog.genre_item(
			genre.name,
			genre.id,
			{ release_count = genre.release_count, song_count = genre.track_count },
			library_hex_id,
			cover_infos[genre.id]
		)
		if item then
			if options.include_can_delete then
				item.CanDelete = false
			end
			table.insert(items, item)
		end
	end
	results.in_batches(items, emit)
	return by_name_total(options, page.total), page.offset
end

function module.list_genre_items(options: ItemsQueryOptions): ({ ItemInfo }, number, number)
	return collected(function(emit)
		return module.each_genre_batch(options, emit)
	end)
end

-- Library views answer searches by name. As upstream, the playlists view shows only while the
-- caller can see a playlist, the views the caller hid from its media show only when asked for,
-- and the views come in the caller's order and then by name, unless `SortBy` asks otherwise.
function module.list_views(options: ItemsQueryOptions, include_hidden: boolean?): ({ ItemInfo }, number, number)
	local search = if options.search_term then string.lower(options.search_term) else nil
	local candidates = catalog.to_music_view_items(options.include_child_count)
	local visible_playlists = playlist_access.count_visible()
	if visible_playlists > 0 then
		table.insert(
			candidates,
			catalog.playlists_folder_item(if options.include_child_count then visible_playlists else nil)
		)
	end
	local user_id = options.user_id
	local hidden = if user_id and not include_hidden
		then preferences.user_configuration_ids(user_id, "MyMediaExcludes")
		else {}
	local views: { ItemInfo } = {}
	for _, view in candidates do
		if
			not hidden[view.Id or ""]
			and (search == nil or string.find(string.lower(view.Name or ""), search, 1, true) ~= nil)
		then
			table.insert(views, view)
		end
	end

	local function name(view: ItemInfo): string
		return string.lower(view.SortName or view.Name or "")
	end
	if #options.sort_by > 0 then
		local descending = query.direction(options.sort_order) == "descending"
		table.sort(views, function(a: ItemInfo, b: ItemInfo): boolean
			if descending then
				return name(a) > name(b)
			end
			return name(a) < name(b)
		end)
	else
		local order = if user_id then preferences.user_configuration_ids(user_id, "OrderedViews") else {}
		table.sort(views, function(a: ItemInfo, b: ItemInfo): boolean
			local a_position, b_position = order[a.Id or ""] or math.huge, order[b.Id or ""] or math.huge
			if a_position ~= b_position then
				return a_position < b_position
			end
			return name(a) < name(b)
		end)
	end
	return results.paginate_items(views, options.start_index, options.limit)
end

-- The tracks upstream plays for an item one client asks another to play: a track itself, and the
-- tracks under an album, artist, genre, playlist or library, each once, in SortName order. None
-- for an item the caller can't see, which the second result reports.
function module.playable_track_ids(item_id: string, user_id: string): ({ string }, boolean)
	local scope = lookups.scope(item_id)
	local kind = if scope.id then lookups.visible_kind(scope.id, user_id) else nil
	if kind == nil then
		return {}, true
	end
	if kind == "Track" then
		return { item_ids.encode(scope.id) :: string }, false
	end

	local options = options_module.for_user(options_module.parse(nil), user_id)
	options.parent_id = item_id
	options.include_item_types = { "Audio" }
	options.recursive = true
	options.sort_by = { "SortName" }
	local built: tracks.TrackQuery?
	if kind == "Playlist" then
		local children = playlist_children(options, scope)
		built = if children then query.tracks(children, lookups.scope(nil)) else nil
	else
		built = query.tracks(options, scope)
	end
	if not built then
		return {}, false
	end

	local result: { string } = {}
	for _, track in tracks.query(built).items do
		local hex_id = item_ids.encode(track.id)
		if hex_id then
			table.insert(result, hex_id)
		end
	end
	return result, false
end

-- Holds the place in a remote play's queue of an item the caller can't see, one track long.
local HIDDEN_ITEM = ""

-- Of the queue's tracks, by public id, those `user_id` can see. The queries that built it see what
-- the dispatch principal does, which for an admin controlling another user's session is more.
local function tracks_visible_to(queue: { string }, user_id: string): { [string]: boolean }
	local track_ids: { string } = {}
	for _, item_id in queue do
		if item_id ~= HIDDEN_ITEM then
			table.insert(track_ids, item_ids.decode(item_id) :: string)
		end
	end
	return tracks.visible_to(user_id, track_ids)
end

-- The queue without the items that can't play, and where `start_index` lands in it: on the same
-- track, or if that one went, the next one left, else the last one left before it. An index past
-- the queue stays, for the delivery to refuse.
local function playable_queue(
	queue: { string },
	visible: { [string]: boolean }?,
	start_index: number?
): ({ string }, number?)
	local playable: { string } = {}
	local selected: number? = nil
	for position, item_id in queue do
		if item_id ~= HIDDEN_ITEM and (visible == nil or visible[item_ids.decode(item_id) :: string]) then
			table.insert(playable, item_id)
			if start_index and selected == nil and position - 1 >= start_index then
				selected = #playable - 1
			end
		end
	end
	if start_index == nil or start_index >= #queue then
		return playable, start_index
	end
	if selected == nil and #playable > 0 then
		selected = #playable - 1
	end
	return playable, selected
end

-- Past this many tracks a remote play expands no further items, so repeating a large one in a
-- request can't multiply the work. The item that crosses it stays whole.
local MAX_REMOTE_QUEUE_TRACKS = 10000

-- What a remote play command from `controller_user_id` plays on a session of `user_id`, as upstream
-- sends it on: each requested item's playable tracks, or its instant mix for `PlayInstantMix`, in
-- request order, as far as that user can see them, and `start_index` moved to match. An instant mix
-- and a shuffle play now, the shuffle in random order.
function module.remote_play_queue(
	requested_ids: { string },
	play_command: string,
	user_id: string,
	controller_user_id: string,
	start_index: number?
): ({ string }, string, number?)
	local queue: { string } = {}
	if play_command == "PlayInstantMix" then
		local options = options_module.for_user(options_module.parse(nil), user_id)
		for _, requested_id in requested_ids do
			if #queue >= MAX_REMOTE_QUEUE_TRACKS then
				break
			end
			local mix_items = instant_mix.list_instant_mix_audio(item_ids.decode(requested_id), options)
			for _, item in mix_items do
				if item.Id then
					table.insert(queue, item.Id)
				end
			end
		end
		play_command = "PlayNow"
	else
		for _, requested_id in requested_ids do
			if #queue >= MAX_REMOTE_QUEUE_TRACKS then
				break
			end
			local track_ids, hidden = module.playable_track_ids(requested_id, user_id)
			if hidden then
				table.insert(queue, HIDDEN_ITEM)
			end
			table.move(track_ids, 1, #track_ids, #queue + 1, queue)
		end
	end

	local visible = if user_id ~= controller_user_id then tracks_visible_to(queue, user_id) else nil
	queue, start_index = playable_queue(queue, visible, start_index)

	if play_command == "PlayShuffle" then
		for index = #queue, 2, -1 do
			local other = math.random(1, index)
			queue[index], queue[other] = queue[other], queue[index]
		end
		play_command = "PlayNow"
	end
	return queue, play_command, start_index
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

-- `response` asks for the item as single-item responses carry it, with every field: ProviderIds,
-- ExternalUrls and CanDelete, which depends on whether the caller is an admin.
function module.find_item_by_id(item_id: string?, user_id: string?, response: { is_admin: boolean }?): ItemInfo?
	local item_hex_id = item_ids.encode(item_id)
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
	options.include_provider_ids = response ~= nil
	options.include_external_urls = response ~= nil
	options.include_can_delete = response ~= nil
	options.is_admin = response ~= nil and response.is_admin
	options.group_items = false
	options.user_id = user_id
	return (module.list_items(options))[1]
end

local ANCESTORS_MAX_DEPTH = 8

-- Visited-set prevents cycle duplicates; boolean return distinguishes "missing" from "no ancestors".
function module.ancestors_chain(item_id: string, user_id: string): ({ ItemInfo }, boolean)
	local chain: { ItemInfo } = {}
	local current = module.find_item_by_id(item_id, user_id)
	if not current then
		return chain, false
	end

	local visited: { [string]: boolean } = { [item_id] = true }
	while current and #chain < ANCESTORS_MAX_DEPTH do
		local parent_id = item_ids.decode(current.ParentId)
		if not parent_id or visited[parent_id] then
			break
		end
		visited[parent_id] = true
		local parent = module.find_item_by_id(parent_id, user_id)
		if not parent then
			break
		end
		table.insert(chain, parent)
		current = parent
	end
	return chain, true
end

-- Playlist order, as upstream, which honours only `StartIndex` and `Limit` here. Hands `emit` the
-- realized entries a batch at a time; returns the total and the start.
function module.each_playlist_track_batch(playlist: Playlist?, options: ItemsQueryOptions, emit: Emit): (number, number)
	if not playlist then
		return 0, options.start_index
	end

	local track_refs = playlist_access.track_refs(playlist)
	local track_ids: { string } = {}
	for _, track_ref in track_refs do
		table.insert(track_ids, track_ref.track_id)
	end
	local tracks_by_id: { [string]: tracks.Track } = if #track_ids > 0 then tracks.get(track_ids) else {}

	local entries: { tracks.Track } = {}
	for _, track_ref in track_refs do
		local track = tracks_by_id[track_ref.track_id]
		if track and track.available then
			table.insert(entries, track)
		end
	end

	-- As upstream, an entry's `PlaylistItemId` is its track's id.
	local page, total_count, start_index = results.paginate_items(entries, options.start_index, options.limit)
	results.in_batches(page, function(batch)
		local items = catalog.realize(catalog.track_stubs(batch), options)
		for _, item in items do
			item.PlaylistItemId = item.Id
		end
		if #items > 0 then
			emit(items)
		end
	end)
	return total_count, start_index
end

function module.list_playlist_track_items(
	playlist: Playlist?,
	options: ItemsQueryOptions
): ({ ItemInfo }, number, number)
	return collected(function(emit)
		return module.each_playlist_track_batch(playlist, options, emit)
	end)
end

return module
