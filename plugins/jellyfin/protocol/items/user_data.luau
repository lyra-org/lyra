--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")
local bookmarks = require("@lyra/bookmarks")
local favorites = require("@lyra/favorites")
local listens = require("@lyra/listens")
local ratings = require("@lyra/ratings")

local constants = require("../../constants")
local enrichment = require("./enrichment")

local RESOLVE_MERGE_TRACK_COUNT = constants.RESOLVE_MERGE_TRACK_COUNT

type JsonObject = { [string]: api.JsonValue }

export type UserDataOpts = {
	item_id: string?,
	user_id: string?,
	include_favorite: boolean?,
	include_play_count: boolean?,
	favorite_lookup: { [string]: boolean }?,
	play_count_lookup: { [string]: number }?,
	last_played_lookup: { [string]: number }?,
	-- Set for tracks, the only items that hold a position: the caller's bookmark positions, in
	-- milliseconds, by track id, when already read.
	include_position: boolean?,
	position_lookup: { [string]: number }?,
	-- The caller's ratings in stars by item id, when already read.
	include_rating: boolean?,
	rating_lookup: { [string]: number }?,
	run_time_ticks: number?,
	key: string?,
	item_hex_id: string?,
}

export type UserData = {
	Key: string?,
	ItemId: string?,
	PlaybackPositionTicks: number,
	PlayedPercentage: number?,
	PlayCount: number,
	IsFavorite: boolean,
	Played: boolean,
	LastPlayedDate: string?,
	Rating: number?,
	Likes: boolean?,
}

local module = {}

-- Lyra keeps 1-5 stars where upstream keeps 0-10, so a rating reads back as twice its stars: a
-- client that saves 7 reads 8.
local MIN_LIKE_RATING = 6.5 -- upstream's UserItemData.MinLikeValue

-- The stars a Jellyfin rating saves: nil for 0, which clears it (upstream keeps 0 as a rating).
-- Rounds to the nearest star, except that a like never rounds down to a dislike. False when the
-- rating is outside 0-10.
function module.stars_from_rating(rating: number): (boolean, number?)
	if not (rating >= 0 and rating <= 10) then
		return false, nil
	end
	if rating == 0 then
		return true, nil
	end
	local stars = math.clamp(math.floor(rating / 2 + 0.5), 1, 5)
	if rating >= MIN_LIKE_RATING then
		stars = math.max(stars, 4)
	end
	return true, stars
end

-- As upstream, which saves a like as 10 and a dislike as 1.
function module.stars_from_likes(likes: boolean): number
	return if likes then 5 else 1
end

-- Favorites belong to the dispatch principal; `user_id` only says a user is present.
local function has_favorite(user_id: string?, item_id: string?): boolean
	if not user_id or not item_id then
		return false
	end

	local ok, favorited = pcall(favorites.get, { item_id })
	return ok and favorited[item_id] ~= nil
end

function module.has_favorite(user_id: string?, item_id: string?): boolean
	return has_favorite(user_id, item_id)
end

function module.add_favorite(item_id: string?): boolean
	if not item_id then
		return false
	end

	local ok, applied = pcall(favorites.add, item_id)
	return ok and applied == true
end

function module.remove_favorite(item_id: string?): boolean
	if not item_id then
		return false
	end

	local ok, applied = pcall(favorites.remove, item_id)
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

	local play_count = 0
	local should_include_play_count = opts.include_play_count ~= false
	if should_include_play_count and item_id ~= nil then
		local lookup_count: number? = nil
		if type(opts.play_count_lookup) == "table" then
			lookup_count = opts.play_count_lookup[item_id]
		end

		if lookup_count ~= nil then
			play_count = tonumber(lookup_count) or 0
		else
			local ok, stats = pcall(listens.get, { item_id }, { merge_unique_external_ids = RESOLVE_MERGE_TRACK_COUNT })
			local entry = if ok then (stats :: { [string]: listens.ListenStats })[item_id] else nil
			if entry ~= nil then
				play_count = entry.count
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
	if item_id ~= nil and type(opts.last_played_lookup) == "table" then
		last_played_date = ms_to_iso8601(opts.last_played_lookup[item_id])
	end

	local position_ticks = 0
	if opts.include_position == true and item_id ~= nil then
		local position_ms: number?
		if opts.position_lookup then
			position_ms = opts.position_lookup[item_id]
		else
			local bookmark = bookmarks.get({ item_id })[item_id]
			position_ms = if bookmark then bookmark.position_ms else nil
		end
		position_ticks = enrichment.ms_to_ticks(position_ms) or 0
	end

	local rating: number? = nil
	if opts.include_rating == true and item_id ~= nil then
		local stars = if opts.rating_lookup then opts.rating_lookup[item_id] else ratings.get({ item_id })[item_id]
		rating = if stars then stars * 2 else nil
	end

	-- As upstream, the share of the run time the position has reached, absent at the start.
	local run_time_ticks = opts.run_time_ticks
	local played_percentage = if run_time_ticks
			and run_time_ticks > 0
			and position_ticks > 0
		then 100 * position_ticks / run_time_ticks
		else nil

	return {
		Key = opts.key,
		ItemId = opts.item_hex_id,
		PlaybackPositionTicks = position_ticks,
		PlayedPercentage = played_percentage,
		PlayCount = play_count,
		IsFavorite = is_favorite,
		Played = play_count > 0,
		LastPlayedDate = last_played_date,
		Rating = rating,
		Likes = if rating then rating >= MIN_LIKE_RATING else nil,
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
	if user_data.Rating ~= nil then
		result.Rating = user_data.Rating
		result.Likes = user_data.Likes
	end
	if user_data.PlayedPercentage ~= nil then
		result.PlayedPercentage = user_data.PlayedPercentage
	end
	return result
end

return module
