--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")
local favorites = require("@lyra/favorites")
local listens = require("@lyra/listens")

local constants = require("../../constants")

local RESOLVE_MERGE_TRACK_COUNT = constants.RESOLVE_MERGE_TRACK_COUNT

type JsonObject = { [string]: api.JsonValue }

export type UserDataOpts = {
	item_id: number?,
	user_id: number?,
	include_favorite: boolean?,
	include_play_count: boolean?,
	favorite_lookup: { [number]: boolean }?,
	play_count_lookup: { [number]: number }?,
	last_played_lookup: { [number]: number }?,
	key: string?,
	item_hex_id: string?,
}

export type UserData = {
	Key: string?,
	ItemId: string?,
	PlaybackPositionTicks: number,
	PlayCount: number,
	IsFavorite: boolean,
	Played: boolean,
	LastPlayedDate: string?,
}

local module = {}

local function has_favorite(user_id: number?, item_id: number?): boolean
	if not user_id or user_id <= 0 or not item_id or item_id <= 0 then
		return false
	end

	local ok, is_favorite = pcall(favorites.has, user_id, item_id)
	return ok and is_favorite == true
end

function module.has_favorite(user_id: number?, item_id: number?): boolean
	return has_favorite(user_id, item_id)
end

function module.add_favorite(user_id: number?, item_id: number?): boolean
	if not user_id or user_id <= 0 or not item_id or item_id <= 0 then
		return false
	end

	local ok, applied = pcall(favorites.add, user_id, item_id)
	return ok and applied == true
end

function module.remove_favorite(user_id: number?, item_id: number?): boolean
	if not user_id or user_id <= 0 or not item_id or item_id <= 0 then
		return false
	end

	local ok, applied = pcall(favorites.remove, user_id, item_id)
	return ok and applied == true
end

-- Howard Hinnant's civil_from_days algorithm
local function ms_to_iso8601(ms: number?): string?
	if not ms or ms <= 0 then
		return nil
	end
	local s = math.floor(ms / 1000)
	local z = math.floor(s / 86400) + 719468
	local era = math.floor((if z >= 0 then z else z - 146096) / 146097)
	local doe = z - era * 146097
	local yoe = math.floor((doe - math.floor(doe / 1460) + math.floor(doe / 36524) - math.floor(doe / 146096)) / 365)
	local y = yoe + era * 400
	local doy = doe - (365 * yoe + math.floor(yoe / 4) - math.floor(yoe / 100))
	local mp = math.floor((5 * doy + 2) / 153)
	local d = doy - math.floor((153 * mp + 2) / 5) + 1
	local m = if mp < 10 then mp + 3 else mp - 9
	if m <= 2 then
		y += 1
	end
	local remainder = s % 86400
	local h = math.floor(remainder / 3600)
	local min = math.floor((remainder % 3600) / 60)
	local sec = remainder % 60
	return string.format("%04d-%02d-%02dT%02d:%02d:%02d.0000000Z", y, m, d, h, min, sec)
end

function module.get_user_data(opts: UserDataOpts?): UserData
	if not opts then
		return {
			PlaybackPositionTicks = 0,
			PlayCount = 0,
			IsFavorite = false,
			Played = false,
		}
	end

	local item_id = opts.item_id
	local user_id = opts.user_id

	local play_count = 0
	local should_include_play_count = opts.include_play_count ~= false
	if should_include_play_count and item_id ~= nil and item_id > 0 then
		local lookup_count: number? = nil
		if type(opts.play_count_lookup) == "table" then
			lookup_count = opts.play_count_lookup[item_id]
		end

		if lookup_count ~= nil then
			play_count = tonumber(lookup_count) or 0
		else
			local ok, count = pcall(listens.get_count, item_id, user_id, RESOLVE_MERGE_TRACK_COUNT)
			if ok then
				play_count = count
			end
		end
	end

	local is_favorite = false
	if opts.include_favorite == true then
		if type(opts.favorite_lookup) == "table" and item_id ~= nil then
			is_favorite = opts.favorite_lookup[item_id] == true
		else
			is_favorite = has_favorite(opts.user_id, item_id)
		end
	end

	local last_played_date: string? = nil
	if item_id ~= nil and item_id > 0 and type(opts.last_played_lookup) == "table" then
		last_played_date = ms_to_iso8601(opts.last_played_lookup[item_id])
	end

	return {
		Key = opts.key,
		ItemId = opts.item_hex_id,
		PlaybackPositionTicks = 0,
		PlayCount = play_count,
		IsFavorite = is_favorite,
		Played = play_count > 0,
		LastPlayedDate = last_played_date,
	}
end

function module.user_data_to_json(user_data: UserData): JsonObject
	local result: JsonObject = {
		PlaybackPositionTicks = user_data.PlaybackPositionTicks,
		PlayCount = user_data.PlayCount,
		IsFavorite = user_data.IsFavorite,
		Played = user_data.Played,
	}
	if user_data.Key ~= nil then
		result.Key = user_data.Key
	end
	if user_data.ItemId ~= nil then
		result.ItemId = user_data.ItemId
	end
	if user_data.LastPlayedDate ~= nil then
		result.LastPlayedDate = user_data.LastPlayedDate
	end
	return result
end

return module
