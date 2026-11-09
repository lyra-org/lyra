--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

-- Upstream's per-item user data: `/UserItems/{itemId}/UserData` and the favorite routes. A track's
-- playback position is the caller's Lyra bookmark of it, shared with every other client, and an
-- item's rating is the caller's Lyra rating of it (see `user_data.stars_from_rating`).

local api = require("@lyra/api")
local bookmarks = require("@lyra/bookmarks")
local ratings = require("@lyra/ratings")

local catalog = require("../protocol/items/catalog")
local constants = require("../constants")
local item_ids = require("../protocol/items/identifiers")
local listing = require("../protocol/items/listing")
local requests = require("../protocol/items/requests")
local responses = require("../protocol/items/responses")
local session = require("../protocol/session")
local user_data = require("../protocol/items/user_data")
local user_data_changes = require("../protocol/user_data_changes")

local P = constants.ROUTE_PREFIX
local TICKS_PER_MS = 10000
-- The largest position a bookmark holds, as `@lyra/bookmarks` bounds it.
local MAX_POSITION_MS = 2 ^ 53 - 1

type ApiContext = api.ApiContext
type ApiResponse = api.ApiResponse
type ItemInfo = catalog.ItemInfo
type JsonObject = { [string]: api.JsonValue }
type Principal = session.Principal

local module = {}

-- The item the route's `itemId` names, as the caller sees it.
local function resolve_item(ctx: ApiContext, principal: Principal): (string?, ItemInfo?, ApiResponse?)
	local params = if ctx then ctx.params else nil
	local raw_item_id = if params then params.itemId else nil
	if not raw_item_id then
		return nil, nil, responses.bad_request("missing item id")
	end

	local item_id = item_ids.decode(raw_item_id)
	local item = if item_id then listing.find_item_by_id(item_id, principal.user_id) else nil
	if not item_id or not item then
		return nil, nil, responses.not_found("item not found")
	end
	return item_id, item, nil
end

-- The value of the body's field, its name matched without case as upstream's binder does.
local function body_field(body: JsonObject, name: string): api.JsonValue
	local lowered = string.lower(name)
	for key, value in body do
		if string.lower(key) == lowered then
			return value
		end
	end
	return nil
end

-- Sets the caller's bookmark of the track to the position, keeping any comment another client
-- left on it, or clears it at a position under one millisecond. False when the track is gone.
local function save_position(track_id: string, position_ticks: number): boolean
	local position_ms = position_ticks // TICKS_PER_MS
	if position_ms <= 0 then
		return bookmarks.remove(track_id)
	end
	local existing = bookmarks.get({ track_id })[track_id]
	return bookmarks.set(track_id, position_ms, { comment = if existing then existing.comment else nil })
end

-- Sets the caller's rating of the item, or clears it without stars. False when the item can't be
-- rated: Lyra rates only tracks, albums and artists, where upstream rates anything.
local function save_rating(item_id: string, stars: number?): boolean
	return if stars then ratings.set(item_id, stars) else ratings.remove(item_id)
end

function module.handle_get(ctx: ApiContext): ApiResponse
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	if not session.require_matching_user_scope(ctx, principal) then
		return session.forbidden_user()
	end

	local item_id, item, err = resolve_item(ctx, principal)
	if not item_id or not item then
		return err :: ApiResponse
	end

	local item_user_data = item.UserData
	if item_user_data then
		return api.response.json(200, user_data.user_data_to_json(item_user_data))
	end
	return api.response.json(200, user_data_changes.user_data_json(item_id, item, principal.user_id))
end

-- As upstream, only the fields a body sets change. `PlaybackPositionTicks` moves a track's
-- bookmark, `IsFavorite` its favorite, and `Rating` or `Likes` its rating; Lyra's own listens
-- decide `Played`, `PlayCount` and `LastPlayedDate`, so they are ignored, as are the rest. Items
-- other than tracks hold no position.
function module.handle_post(ctx: ApiContext): ApiResponse
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	if not session.require_matching_user_scope(ctx, principal) then
		return session.forbidden_user()
	end

	local request = if ctx then ctx.request else nil
	local raw_body = if request then request.json else nil
	if type(raw_body) ~= "table" then
		return responses.bad_request("invalid user data request body")
	end
	local body = raw_body :: JsonObject

	local position_ticks = body_field(body, "PlaybackPositionTicks")
	if position_ticks ~= nil then
		if
			type(position_ticks) ~= "number"
			or position_ticks < 0
			or position_ticks ~= position_ticks
			or math.floor(position_ticks) ~= position_ticks
			or position_ticks // TICKS_PER_MS > MAX_POSITION_MS
		then
			return responses.bad_request("invalid PlaybackPositionTicks value")
		end
	end
	local is_favorite = body_field(body, "IsFavorite")
	if is_favorite ~= nil and type(is_favorite) ~= "boolean" then
		return responses.bad_request("invalid IsFavorite value")
	end

	local likes = body_field(body, "Likes")
	if likes ~= nil and type(likes) ~= "boolean" then
		return responses.bad_request("invalid Likes value")
	end
	local rating = body_field(body, "Rating")
	local rating_ok, stars = true, nil
	if type(rating) == "number" then
		rating_ok, stars = user_data.stars_from_rating(rating)
	elseif rating ~= nil then
		rating_ok = false
	end
	if not rating_ok then
		return responses.bad_request("Rating must be from 0 to 10")
	end

	local item_id, item, err = resolve_item(ctx, principal)
	if not item_id or not item then
		return err :: ApiResponse
	end

	-- Upstream applies Likes before Rating, so Rating wins when a body sets both.
	if rating ~= nil or likes ~= nil then
		if rating == nil then
			stars = user_data.stars_from_likes(likes :: boolean)
		end
		if not save_rating(item_id, stars) then
			return responses.bad_request("item cannot be rated")
		end
	end
	if type(is_favorite) == "boolean" then
		local applied = if is_favorite then user_data.add_favorite(item_id) else user_data.remove_favorite(item_id)
		if not applied then
			return responses.bad_request(if is_favorite then "failed to add favorite" else "failed to remove favorite")
		end
	end
	if type(position_ticks) == "number" and requests.trim(item.Type) == "Audio" then
		if not save_position(item_id, position_ticks) then
			return responses.not_found("item not found")
		end
	end

	return api.response.json(200, user_data_changes.user_data_json(item_id, item, principal.user_id))
end

local function handle_favorite(ctx: ApiContext, should_favorite: boolean): ApiResponse
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	if not session.require_matching_user_scope(ctx, principal) then
		return session.forbidden_user()
	end

	local item_id, item, err = resolve_item(ctx, principal)
	if not item_id or not item then
		return err :: ApiResponse
	end

	local update_ok = if should_favorite then user_data.add_favorite(item_id) else user_data.remove_favorite(item_id)
	if not update_ok then
		return responses.bad_request(if should_favorite then "failed to add favorite" else "failed to remove favorite")
	end

	return api.response.json(200, user_data_changes.user_data_json(item_id, item, principal.user_id))
end

-- `likes` saves a like or dislike, and no `likes` clears the rating, as upstream. Like Navidrome,
-- a numeric `rating` (0-10), which older clients send, is also taken when there is no `likes`.
local function handle_rating(ctx: ApiContext, clear: boolean): ApiResponse
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	if not session.require_matching_user_scope(ctx, principal) then
		return session.forbidden_user()
	end

	local stars: number? = nil
	if not clear then
		local query = if ctx and ctx.request then ctx.request.query else nil
		local likes, invalid_likes = requests.parse_query_bool(query, "likes")
		if invalid_likes then
			return responses.bad_request("invalid likes value")
		end
		local raw_rating = requests.parse_query_string(query, { "rating" })
		if likes ~= nil then
			stars = user_data.stars_from_likes(likes)
		elseif raw_rating ~= nil then
			local rating = tonumber(raw_rating)
			local ok, rated = false, nil
			if rating then
				ok, rated = user_data.stars_from_rating(rating)
			end
			if not ok then
				return responses.bad_request("rating must be from 0 to 10")
			end
			stars = rated
		end
	end

	local item_id, item, err = resolve_item(ctx, principal)
	if not item_id or not item then
		return err :: ApiResponse
	end
	if not save_rating(item_id, stars) then
		return responses.bad_request("item cannot be rated")
	end

	return api.response.json(200, user_data_changes.user_data_json(item_id, item, principal.user_id))
end

function module.handle_rate(ctx: ApiContext): ApiResponse
	return handle_rating(ctx, false)
end

function module.handle_unrate(ctx: ApiContext): ApiResponse
	return handle_rating(ctx, true)
end

function module.handle_favorite(ctx: ApiContext): ApiResponse
	return handle_favorite(ctx, true)
end

function module.handle_unfavorite(ctx: ApiContext): ApiResponse
	return handle_favorite(ctx, false)
end

function module.register()
	local options: api.ApiRouteOptions = { auth = "public", case_insensitive = true }
	api.get(P .. "/UserItems/{itemId}/UserData", module.handle_get, options)
	api.get(P .. "/Users/{userId}/Items/{itemId}/UserData", module.handle_get, options)
	api.post(P .. "/UserItems/{itemId}/UserData", module.handle_post, options)
	api.post(P .. "/Users/{userId}/Items/{itemId}/UserData", module.handle_post, options)
	api.post(P .. "/UserFavoriteItems/{itemId}", module.handle_favorite, options)
	api.post(P .. "/Users/{userId}/FavoriteItems/{itemId}", module.handle_favorite, options)
	api.delete(P .. "/UserFavoriteItems/{itemId}", module.handle_unfavorite, options)
	api.delete(P .. "/Users/{userId}/FavoriteItems/{itemId}", module.handle_unfavorite, options)
	api.post(P .. "/UserItems/{itemId}/Rating", module.handle_rate, options)
	api.post(P .. "/Users/{userId}/Items/{itemId}/Rating", module.handle_rate, options)
	api.delete(P .. "/UserItems/{itemId}/Rating", module.handle_unrate, options)
	api.delete(P .. "/Users/{userId}/Items/{itemId}/Rating", module.handle_unrate, options)
end

return module
