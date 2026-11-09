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
local lookups = require("./lookups")
local options_module = require("./options")

type ItemsQueryOptions = options_module.ItemsQueryOptions
type ItemInfo = catalog.ItemInfo

local module = {}

-- The genres of these releases, each once.
local function genres_of(release_ids: { string }): { string }
	local ids: { string } = {}
	local seen: { [string]: boolean } = {}
	if #release_ids == 0 then
		return ids
	end
	for _, release_genres in genres.by_release(release_ids) do
		for _, genre in release_genres do
			if not seen[genre.id] then
				seen[genre.id] = true
				table.insert(ids, genre.id)
			end
		end
	end
	return ids
end

local function ids_of<T>(items: { T }): { string }
	local ids: { string } = {}
	for _, item in items do
		table.insert(ids, (item :: any).id :: string)
	end
	return ids
end

local RANDOM = { { key = "random", order = "ascending" } }

-- Upstream's limit when the request names none.
local DEFAULT_LIMIT = 50

-- Returns `(items, total_count, start_index, found)`; an item the caller can't see isn't found.
function module.list_similar_items(
	item_id: string?,
	options: ItemsQueryOptions
): ({ ItemInfo }, number, number, boolean)
	if not item_id then
		return {}, 0, 0, false
	end

	local kind = lookups.visible_kind(item_id, options.user_id)
	if kind == nil then
		return {}, 0, 0, false
	end
	local exclude_ids = item_ids.decode_list(options.exclude_item_ids)
	table.insert(exclude_ids, item_id)
	local exclude_artist_ids = item_ids.decode_list(options.exclude_artist_ids)
	local seed = math.random(1, 2147483647)
	local limit = options.limit or DEFAULT_LIMIT
	local stubs: { ItemInfo } = {}
	local total = 0
	if kind == "Release" then
		local genre_ids = genres_of({ item_id })
		if #genre_ids > 0 then
			local page = releases.query({
				genre_ids = genre_ids,
				exclude_ids = exclude_ids,
				exclude_artist_ids = exclude_artist_ids,
				available = true,
				sort = RANDOM :: any,
				seed = seed,
				limit = limit,
			})
			stubs = catalog.release_stubs(page.items)
			total = page.total
		end
	elseif kind == "Track" then
		local genre_ids = genres_of(ids_of(lookups.releases_of_track(item_id)))
		if #genre_ids > 0 then
			local page = tracks.query({
				genre_ids = genre_ids,
				exclude_ids = exclude_ids,
				exclude_artist_ids = exclude_artist_ids,
				available = true,
				sort = RANDOM :: any,
				seed = seed,
				limit = limit,
			})
			stubs = catalog.track_stubs(page.items)
			total = page.total
		end
	elseif kind == "Artist" then
		local genre_ids = genres_of(ids_of(releases.query({ artist_ids = { item_id } }).items))
		if #genre_ids > 0 then
			table.move(exclude_artist_ids, 1, #exclude_artist_ids, #exclude_ids + 1, exclude_ids)
			local page = artists.query({
				genre_ids = genre_ids,
				exclude_ids = exclude_ids,
				available = true,
				sort = RANDOM :: any,
				seed = seed,
				limit = limit,
			})
			stubs = catalog.artist_stubs(page.items)
			total = page.total
		end
	end

	return catalog.realize(stubs, options), total, 0, true
end

return module
