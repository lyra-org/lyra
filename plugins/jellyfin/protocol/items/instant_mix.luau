--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

-- Instant mixes, seeded by an item or a genre name.

local genres = require("@lyra/genres")
local mix = require("@lyra/mix")
local tracks = require("@lyra/tracks")

local catalog = require("./catalog")
local lookups = require("./lookups")
local options_module = require("./options")
local requests = require("./requests")

type ItemsQueryOptions = options_module.ItemsQueryOptions
type ItemInfo = catalog.ItemInfo

local module = {}

-- Mirrors `services::mix::MAX_LIMIT` — avoids a 500 on oversized Limit. Safe drift.
local INSTANT_MIX_MAX_LIMIT: number = 2000
local INSTANT_MIX_DEFAULT_LIMIT: number = 200

-- The item kinds an instant mix seeds from.
local MIX_SEED_KINDS: { [string]: boolean } = {
	Track = true,
	Release = true,
	Artist = true,
	Playlist = true,
	Genre = true,
}

-- Slices to `[start_index, start_index + limit)`. `total_count` is pre-slice;
-- `Limit = 0` → empty page (Jellyfin convention).
local function realize_mix_tracks_paged(
	mix_tracks: { mix.MixedTrack },
	options: ItemsQueryOptions,
	requested_limit: number?
): ({ ItemInfo }, number)
	local total_count = #mix_tracks
	if requested_limit == 0 then
		return {} :: { ItemInfo }, total_count
	end

	local start_index = options.start_index or 0
	if start_index < 0 then
		start_index = 0
	end
	local end_index = total_count
	if requested_limit and requested_limit > 0 then
		end_index = math.min(total_count, start_index + requested_limit)
	end

	local page: { tracks.Track } = {}
	for index = start_index + 1, end_index do
		table.insert(page, mix_tracks[index].track)
	end
	local items = catalog.realize(catalog.track_stubs(page), options)
	return items, total_count
end

-- Engine sees `start_index + limit` (capped); `requested_limit` is returned
-- separately so realize can honor `Limit = 0`.
local function engine_limits(options: ItemsQueryOptions): (number, number?)
	local requested_limit: number? = options.limit
	local engine_limit: number
	if requested_limit == nil or requested_limit <= 0 then
		engine_limit = INSTANT_MIX_DEFAULT_LIMIT
	else
		local start_index = options.start_index or 0
		if start_index < 0 then
			start_index = 0
		end
		engine_limit = start_index + requested_limit
	end
	if engine_limit > INSTANT_MIX_MAX_LIMIT then
		engine_limit = INSTANT_MIX_MAX_LIMIT
	end

	return engine_limit, requested_limit
end

-- Returns `(items, total_count, start_index, found)`. Audio seeds pin the
-- seed first; nil → 404.
function module.list_instant_mix_audio(
	item_id: string?,
	options: ItemsQueryOptions
): ({ ItemInfo }, number, number, boolean)
	local empty_items: { ItemInfo } = {}
	local start_index = options.start_index or 0
	if not item_id then
		return empty_items, 0, start_index, false
	end

	local kind = lookups.visible_kind(item_id, options.user_id)
	if not kind then
		return empty_items, 0, start_index, false
	end

	if not MIX_SEED_KINDS[kind] then
		-- Unsupported seed type (folder, library) — match upstream: 200 with empty.
		return empty_items, 0, start_index, true
	end

	local engine_limit, requested_limit = engine_limits(options)
	local mix_tracks = mix.blend({ seeds = { item_id }, pin_seed = kind == "Track", limit = engine_limit })
	if mix_tracks == nil then
		return empty_items, 0, start_index, false
	end

	local items, total_count = realize_mix_tracks_paged(mix_tracks, options, requested_limit)
	return items, total_count, start_index, true
end

-- Blends from the named genre; 404s on unknown name (upstream Jellyfin
-- silently returns random audio).
function module.list_instant_mix_audio_from_genre_name(
	name: string?,
	options: ItemsQueryOptions
): ({ ItemInfo }, number, number, boolean)
	local empty_items: { ItemInfo } = {}
	local start_index = options.start_index or 0
	if not name then
		return empty_items, 0, start_index, false
	end
	local trimmed = requests.trim(name)
	if not trimmed then
		return empty_items, 0, start_index, false
	end

	local genre_info = genres.find_by_name(trimmed)
	if not genre_info then
		return empty_items, 0, start_index, false
	end

	local engine_limit, requested_limit = engine_limits(options)
	local mix_tracks = mix.blend({ seeds = { genre_info.id }, limit = engine_limit })
	if mix_tracks == nil then
		return empty_items, 0, start_index, false
	end

	local items, total_count = realize_mix_tracks_paged(mix_tracks, options, requested_limit)
	return items, total_count, start_index, true
end

return module
