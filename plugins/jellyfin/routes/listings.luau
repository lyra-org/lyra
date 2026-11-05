--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

-- Upstream's listings that grow with the library: artists, people, genres, a playlist's entries,
-- the latest and the similar items. Each streams its result a batch at a time, so no listing is
-- bounded by what one response body may hold.

local api = require("@lyra/api")

local catalog = require("../protocol/items/catalog")
local item_ids = require("../protocol/items/identifiers")
local items_options = require("../protocol/items/options")
local listing = require("../protocol/items/listing")
local responses = require("../protocol/items/responses")
local results = require("../protocol/items/results")
local session = require("../protocol/session")
local similar = require("../protocol/items/similar")

type ApiContext = api.ApiContext
type ApiResponse = api.ApiResponse

local module = {}

local function listing_options(ctx: ApiContext, principal: session.Principal): items_options.ItemsQueryOptions
	local query = if ctx and ctx.request then ctx.request.query else nil
	return items_options.for_user(items_options.parse(query), principal.user_id)
end

-- `/Artists` lists artists credited in any role.
function module.handle_artists(ctx: ApiContext): ApiResponse
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local options = listing_options(ctx, principal)
	if items_options.has_conflicting_filters(options) then
		return responses.bad_request("Conflicting filters")
	end
	return catalog.item_query_response(function(emit)
		return listing.each_artist_batch(options, nil, emit)
	end)
end

-- `/Artists/AlbumArtists` lists the artists credited on a release.
function module.handle_album_artists(ctx: ApiContext): ApiResponse
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	local options = listing_options(ctx, principal)
	if items_options.has_conflicting_filters(options) then
		return responses.bad_request("Conflicting filters")
	end
	return catalog.item_query_response(function(emit)
		return listing.each_artist_batch(options, "release", emit)
	end)
end

function module.handle_persons(ctx: ApiContext): ApiResponse
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local options = listing_options(ctx, principal)
	return catalog.item_query_response(function(emit)
		return listing.each_person_batch(options, emit)
	end)
end

-- `/Genres` and `/MusicGenres`.
function module.handle_genres(ctx: ApiContext): ApiResponse
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local options = listing_options(ctx, principal)
	return catalog.item_query_response(function(emit)
		return listing.each_genre_batch(options, emit)
	end)
end

function module.handle_playlist_items(ctx: ApiContext): ApiResponse
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	local playlist, playlist_err = session.resolve_playlist_for_route(ctx)
	if not playlist then
		return playlist_err or responses.bad_request("playlist not found")
	end

	local can_view, view_err = session.ensure_playlist_visible(playlist, principal)
	if not can_view then
		return view_err or responses.bad_request("playlist not accessible")
	end

	local options = listing_options(ctx, principal)
	return catalog.item_query_response(function(emit)
		return listing.each_playlist_track_batch(playlist, options, emit)
	end)
end

-- `/Items/Latest` and `/Users/{userId}/Items/Latest`, which answer a list rather than a query
-- result.
function module.handle_latest(ctx: ApiContext): ApiResponse
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	local options = listing_options(ctx, principal)
	return catalog.items_response(function(emit)
		results.in_batches(listing.list_latest_items(options), emit)
	end)
end

-- `/Items/{itemId}/Similar`, `/Artists/{itemId}/Similar`, and `/Albums/{itemId}/Similar`.
function module.handle_similar(ctx: ApiContext): ApiResponse
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	local params = if ctx then ctx.params else nil
	local raw_item_id = if params then params.itemId else nil
	if not raw_item_id then
		return responses.bad_request("missing item id")
	end

	local item_id = item_ids.decode_jellyfin_item_id(raw_item_id)
	if not item_id then
		return responses.not_found("item not found")
	end

	local options = listing_options(ctx, principal)
	local items, total_count, start_index, found = similar.list_similar_items(item_id, options)
	if not found then
		return responses.not_found("item not found")
	end

	return catalog.item_query_response(function(emit)
		results.in_batches(items, emit)
		return total_count, start_index
	end)
end

return module
