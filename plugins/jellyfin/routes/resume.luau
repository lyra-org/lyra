--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

-- Upstream's resume listings, `/UserItems/Resume`: the items holding a resume position. For Lyra
-- those are the tracks the caller bookmarked, the most recently saved first.

local api = require("@lyra/api")
local playback_sessions = require("@lyra/playback_sessions")
local tracks = require("@lyra/tracks")

local catalog = require("../protocol/items/catalog")
local constants = require("../constants")
local item_ids = require("../protocol/items/identifiers")
local items_options = require("../protocol/items/options")
local listing = require("../protocol/items/listing")
local requests = require("../protocol/items/requests")
local responses = require("../protocol/items/responses")
local session = require("../protocol/session")

type ApiContext = api.ApiContext
type ApiResponse = api.ApiResponse
type ApiQueryParams = api.ApiQueryParams

local P = constants.ROUTE_PREFIX

-- The listing parameters upstream's resume routes take, lowercased; the rest are ignored.
local LISTING_PARAMS: { [string]: boolean } = {
	startindex = true,
	limit = true,
	searchterm = true,
	parentid = true,
	fields = true,
	mediatypes = true,
	enableuserdata = true,
	enableimages = true,
	excludeitemtypes = true,
	includeitemtypes = true,
	enabletotalrecordcount = true,
}

local module = {}

local function listing_query(query: ApiQueryParams?): ApiQueryParams
	local kept: ApiQueryParams = {}
	for key, values in query or {} do
		if LISTING_PARAMS[string.lower(tostring(key))] then
			kept[key] = values
		end
	end
	return kept
end

-- The tracks now playing in the caller's sessions, as Jellyfin item ids.
local function now_playing_ids(): { string }
	local hex_ids: { string } = {}
	for _, playing in playback_sessions.list_playbacks() do
		local hex_id = item_ids.encode_jellyfin_item_id(playing.track_id)
		if hex_id and not table.find(hex_ids, hex_id) then
			table.insert(hex_ids, hex_id)
		end
	end
	return hex_ids
end

-- The caller's bookmarked tracks in the libraries.
local function bookmarked_in(libraries: { catalog.ItemInfo }): { string }
	local track_ids: { string } = {}
	for _, library in libraries do
		local library_id = item_ids.decode_jellyfin_item_id(library.Id)
		if library_id then
			for _, track in tracks.query({ library_id = library_id, bookmarked = true, available = true }).items do
				table.insert(track_ids, track.id)
			end
		end
	end
	return track_ids
end

-- The caller's bookmarked tracks of the libraries it excludes from its latest media and of none it
-- keeps, as Jellyfin item ids.
local function excluded_library_track_ids(user_id: string): { string }
	local kept, excluded = listing.libraries_by_latest_exclusion(user_id)
	local hex_ids: { string } = {}
	if #excluded == 0 then
		return hex_ids
	end
	local in_kept: { [string]: boolean } = {}
	for _, track_id in bookmarked_in(kept) do
		in_kept[track_id] = true
	end
	for _, track_id in bookmarked_in(excluded) do
		if not in_kept[track_id] then
			table.insert(hex_ids, item_ids.encode_jellyfin_item_id(track_id) :: string)
		end
	end
	return hex_ids
end

-- As upstream, only types that may be audio list, every track in the caller's libraries can be a
-- parent's, and `ExcludeActiveSessions` leaves out what the caller is playing now. Without a parent,
-- the libraries the caller excludes from its latest media leave the list too.
function module.handle_resume(ctx: ApiContext): ApiResponse
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	if not session.require_matching_user_scope(ctx, principal) then
		return session.forbidden_user()
	end

	local query = if ctx and ctx.request then ctx.request.query else nil
	local exclude_active, invalid = requests.parse_query_bool(query, "ExcludeActiveSessions")
	if invalid then
		return responses.bad_request(`The value '{invalid}' is not valid for ExcludeActiveSessions.`)
	end

	local options = items_options.for_user(items_options.parse(listing_query(query)), principal.user_id)
	if not items_options.wants_item_type(options, "audio") then
		return api.response.json(200, catalog.build_item_query_result({}, 0, options.start_index))
	end
	options.include_item_types = { "Audio" }
	options.exclude_item_types = {}
	options.recursive = true
	options.resume = true
	local exclude_ids: { string } = if exclude_active then now_playing_ids() else {}
	if options.parent_id == nil then
		local excluded = excluded_library_track_ids(principal.user_id)
		table.move(excluded, 1, #excluded, #exclude_ids + 1, exclude_ids)
	end
	options.exclude_item_ids = exclude_ids

	return api.response.stream(200, function(writer)
		catalog.write_item_query_result(writer, function(emit)
			return listing.each_item_batch(options, emit)
		end)
	end, { ["content-type"] = "application/json" })
end

function module.register()
	local route_options: api.ApiRouteOptions = { auth = "public", case_insensitive = true }
	api.get(P .. "/UserItems/Resume", module.handle_resume, route_options)
	api.get(P .. "/Users/{userId}/Items/Resume", module.handle_resume, route_options)
end

return module
