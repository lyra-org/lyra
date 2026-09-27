--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

-- Similar items, as upstream: items of the same type sharing a genre, in random order.

local artists = require("@lyra/artists")
local genres = require("@lyra/genres")
local releases = require("@lyra/releases")
local tracks = require("@lyra/tracks")

local catalog = require("./catalog")
local item_ids = require("./identifiers")
local listing = require("./listing")
local lookups = require("./lookups")
local options_module = require("./options")

type ItemsQueryOptions = options_module.ItemsQueryOptions
type ItemInfo = catalog.ItemInfo
type Release = releases.Release

local module = {}

local function release_ids(items: { Release }): { number }
	local ids: { number } = {}
	for _, release in items do
		if release.db_id then
			table.insert(ids, release.db_id)
		end
	end
	return ids
end

-- The genres of these releases, each once.
local function genres_of(release_db_ids: { number }): { number }
	local ids: { number } = {}
	local seen: { [number]: boolean } = {}
	if #release_db_ids == 0 then
		return ids
	end
	for _, release_genres in genres.by_release(release_db_ids) do
		for _, genre in release_genres do
			local id = genre.db_id
			if id and not seen[id] then
				seen[id] = true
				table.insert(ids, id)
			end
		end
	end
	return ids
end

local function ids_of<T>(items: { T }): { number }
	local ids: { number } = {}
	for _, item in items do
		local id = (item :: any).db_id
		if id then
			table.insert(ids, id)
		end
	end
	return ids
end

-- The item itself, `ExcludeItemIds`, and whatever `ExcludeArtistIds` credits.
local function excluded(item_id: number, kind: string, options: ItemsQueryOptions): { number }
	local ids = item_ids.parse_jellyfin_ids_array(options.exclude_item_ids)
	table.insert(ids, item_id)
	local artist_ids = item_ids.parse_jellyfin_ids_array(options.exclude_artist_ids)
	if #artist_ids == 0 then
		return ids
	end
	local credited: { number }
	if kind == "musicalbum" then
		credited = ids_of(releases.query({ artist_ids = artist_ids }).items)
	elseif kind == "audio" then
		credited = ids_of(tracks.query({ artist_ids = artist_ids }).items)
	else
		credited = artist_ids
	end
	table.move(credited, 1, #credited, #ids + 1, ids)
	return ids
end

local RANDOM = { { key = "random", order = "ascending" } }

function module.list_similar_items(
	item_id: number?,
	options: ItemsQueryOptions
): ({ ItemInfo }, number, number, boolean)
	if not item_id or item_id <= 0 then
		return {}, 0, 0, false
	end
	local target = listing.find_item_by_id(item_id, options.user_id)
	if not target then
		return {}, 0, 0, false
	end

	local kind = string.lower(target.Type or "")
	local seed = math.random(1, 2147483647)
	local stubs: { ItemInfo } = {}
	local total = 0
	if kind == "musicalbum" then
		local genre_ids = genres_of({ item_id })
		if #genre_ids > 0 then
			local page = releases.query({
				genre_ids = genre_ids,
				exclude_ids = excluded(item_id, kind, options),
				sort = RANDOM :: any,
				seed = seed,
				limit = options.limit,
			})
			stubs = catalog.release_stubs(page.items)
			total = page.total
		end
	elseif kind == "audio" then
		local genre_ids = genres_of(release_ids(lookups.releases_of_track(item_id)))
		if #genre_ids > 0 then
			local page = tracks.query({
				genre_ids = genre_ids,
				exclude_ids = excluded(item_id, kind, options),
				sort = RANDOM :: any,
				seed = seed,
				limit = options.limit,
			})
			stubs = catalog.track_stubs(page.items)
			total = page.total
		end
	elseif kind == "musicartist" then
		local genre_ids = genres_of(release_ids(lookups.releases_credited_to(item_id).items))
		if #genre_ids > 0 then
			local page = artists.query({
				genre_ids = genre_ids,
				exclude_ids = excluded(item_id, kind, options),
				sort = RANDOM :: any,
				seed = seed,
				limit = options.limit,
			})
			stubs = catalog.artist_stubs(page.items)
			total = page.total
		end
	end

	return catalog.realize(stubs, options), total, 0, true
end

return module
