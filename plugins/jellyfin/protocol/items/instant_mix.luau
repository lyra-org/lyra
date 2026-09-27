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
local listing = require("./listing")
local options_module = require("./options")
local requests = require("./requests")

type ItemsQueryOptions = options_module.ItemsQueryOptions
type ItemInfo = catalog.ItemInfo

local module = {}

-- Mirrors `services::mix::MAX_LIMIT` — avoids a 500 on oversized Limit. Safe drift.
local INSTANT_MIX_MAX_LIMIT: number = 2000
local INSTANT_MIX_DEFAULT_LIMIT: number = 200

-- Slices to `[start_index, start_index + limit)`. `total_count` is pre-slice;
-- `Limit = 0` → empty page (Jellyfin convention).
local function realize_mix_tracks_paged(
	mix_tracks: { tracks.Track },
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

	local page = table.move(mix_tracks, start_index + 1, end_index, 1, {})
	local items = catalog.realize(catalog.track_stubs(page), options)
	return items, total_count
end

-- Engine sees `start_index + limit` (capped); `requested_limit` is returned
-- separately so realize can honor `Limit = 0`.
local function build_consumer_opts(options: ItemsQueryOptions): (mix.MixConsumerOptions, number?)
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

	return { limit = engine_limit }, requested_limit
end

-- Returns `(items, total_count, start_index, found)`. Audio seeds use
-- `instant_mix_from_audio` for atomic seed-prepend; nil → 404.
function module.list_instant_mix_audio(
	item_id: number?,
	options: ItemsQueryOptions
): ({ ItemInfo }, number, number, boolean)
	local empty_items: { ItemInfo } = {}
	local start_index = options.start_index or 0
	if not item_id or item_id <= 0 then
		return empty_items, 0, start_index, false
	end

	local target = listing.find_item_by_id(item_id, options.user_id)
	if not target then
		return empty_items, 0, start_index, false
	end

	local consumer_opts, requested_limit = build_consumer_opts(options)
	local item_type = string.lower(target.Type or "")
	local mix_tracks: { tracks.Track }? = nil

	if item_type == "audio" then
		mix_tracks = mix.instant_mix_from_audio(item_id, consumer_opts)
	elseif item_type == "musicalbum" then
		mix_tracks = mix.from_release(item_id, consumer_opts)
	elseif item_type == "musicartist" then
		mix_tracks = mix.from_artist(item_id, consumer_opts)
	elseif item_type == "playlist" then
		mix_tracks = mix.from_playlist(item_id, consumer_opts)
	elseif item_type == "musicgenre" then
		mix_tracks = mix.from_genre(item_id, consumer_opts)
	else
		-- Unsupported seed type (folder, library) — match upstream: 200 with empty.
		return empty_items, 0, start_index, true
	end

	if mix_tracks == nil then
		return empty_items, 0, start_index, false
	end

	local items, total_count = realize_mix_tracks_paged(mix_tracks, options, requested_limit)
	return items, total_count, start_index, true
end

-- Dispatches `mix.from_genre` directly; 404s on unknown name (upstream
-- Jellyfin silently returns random audio).
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
	if not genre_info or not genre_info.db_id then
		return empty_items, 0, start_index, false
	end

	local consumer_opts, requested_limit = build_consumer_opts(options)
	local mix_tracks = mix.from_genre(genre_info.db_id, consumer_opts)
	if mix_tracks == nil then
		return empty_items, 0, start_index, false
	end

	local items, total_count = realize_mix_tracks_paged(mix_tracks, options, requested_limit)
	return items, total_count, start_index, true
end

return module
