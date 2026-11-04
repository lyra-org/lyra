--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

-- Upstream's per-item user data: `/UserItems/{itemId}/UserData` and the favorite routes. A track's
-- playback position is the caller's Lyra bookmark of it, shared with every other client.

local api = require("@lyra/api")
local bookmarks = require("@lyra/bookmarks")
local listens = require("@lyra/listens")

local catalog = require("../protocol/items/catalog")
local constants = require("../constants")
local item_ids = require("../protocol/items/identifiers")
local listing = require("../protocol/items/listing")
local requests = require("../protocol/items/requests")
local responses = require("../protocol/items/responses")
local session = require("../protocol/session")
local user_data = require("../protocol/items/user_data")

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

	local item_id = item_ids.decode_jellyfin_item_id(raw_item_id)
	local item = if item_id then listing.find_item_by_id(item_id, principal.user_id) else nil
	if not item_id or not item then
		return nil, nil, responses.not_found("item not found")
	end
	return item_id, item, nil
end

local function user_data_key(item: ItemInfo, item_hex_id: string?): string?
	local item_name = requests.trim(item.Name)
	local item_type = requests.trim(item.Type)
	if item_type == "MusicArtist" then
		return `Artist-{item_name or ""}`
	elseif item_type == "MusicAlbum" then
		return `{requests.trim(item.AlbumArtist) or ""}-{item_name or ""}`
	elseif item_type == "Audio" then
		return `{item.ParentIndexNumber or 0}-{item.IndexNumber or 0}{item_name or ""}`
	elseif item_type == "Playlist" then
		return `Playlist-{item_name or ""}`
	elseif item_type == "MusicGenre" then
		return `MusicGenre-{item_name or ""}`
	end
	return item_hex_id
end

-- The caller's `UserItemDataDto` of the item, read afresh.
local function user_data_json(item_id: string, item: ItemInfo, principal: Principal): JsonObject
	local item_hex_id = item_ids.encode_jellyfin_item_id(item_id)
	local is_track = requests.trim(item.Type) == "Audio"
	local last_played: { [string]: number } = {}
	if is_track then
		local stats = listens.get({ item_id })[item_id]
		if stats and stats.last_played_at_ms ~= nil then
			last_played[item_id] = stats.last_played_at_ms
		end
	end
	return user_data.user_data_to_json(user_data.get_user_data({
		item_id = item_id,
		user_id = principal.user_id,
		item_hex_id = item_hex_id,
		key = user_data_key(item, item_hex_id),
		include_favorite = true,
		include_play_count = is_track,
		include_position = is_track,
		last_played_lookup = last_played,
		run_time_ticks = item.RunTimeTicks,
	}))
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

function module.handle_get(ctx: ApiContext): ApiResponse
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	if not session.require_matching_user_scope(ctx, principal) then
		return responses.forbidden("user is not allowed to view this item user data")
	end

	local item_id, item, err = resolve_item(ctx, principal)
	if not item_id or not item then
		return err :: ApiResponse
	end

	local item_user_data = item.UserData
	if item_user_data then
		return api.response.json(200, user_data.user_data_to_json(item_user_data))
	end
	return api.response.json(200, user_data_json(item_id, item, principal))
end

-- As upstream, only the fields a body sets change. `PlaybackPositionTicks` moves a track's
-- bookmark, and `IsFavorite` its favorite; Lyra's own listens decide `Played`, `PlayCount` and
-- `LastPlayedDate`, so they are ignored, as are the rest. Items other than tracks hold no position.
function module.handle_post(ctx: ApiContext): ApiResponse
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	if not session.require_matching_user_scope(ctx, principal) then
		return responses.forbidden("user is not allowed to update this item user data")
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

	local item_id, item, err = resolve_item(ctx, principal)
	if not item_id or not item then
		return err :: ApiResponse
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

	return api.response.json(200, user_data_json(item_id, item, principal))
end

local function handle_favorite(ctx: ApiContext, should_favorite: boolean): ApiResponse
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	local item_id, item, err = resolve_item(ctx, principal)
	if not item_id or not item then
		return err :: ApiResponse
	end

	local update_ok = if should_favorite then user_data.add_favorite(item_id) else user_data.remove_favorite(item_id)
	if not update_ok then
		return responses.bad_request(if should_favorite then "failed to add favorite" else "failed to remove favorite")
	end

	return api.response.json(200, user_data_json(item_id, item, principal))
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
end

return module
