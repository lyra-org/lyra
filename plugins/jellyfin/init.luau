--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")
local auth = require("@lyra/auth")
local datastore = require("@lyra/datastore")
local libraries = require("@lyra/libraries")
local playlists = require("@lyra/playlists")
local plugins = require("@lyra/plugins")
local server = require("@lyra/server")
local users = require("@lyra/users")

local constants = require("@self/constants")
local item_ids = require("@self/protocol/items/identifiers")
local config = plugins.declare_settings(constants.declare_settings)
local server_info = server.info()
constants.configure_settings(config)
constants.configure_server_id(server_info.id)

local ROOT_ID_STORE_NAME = "identity"
local ROOT_ITEM_ID_KEY = "root_item_id"

local function resolve_root_item_id(): string
	local identity_store = datastore.get_or_create(ROOT_ID_STORE_NAME)
	local stored = identity_store:get(ROOT_ITEM_ID_KEY)
	if type(stored) == "string" then
		local normalized = item_ids.normalize_guid(stored)
		if normalized and normalized ~= item_ids.EMPTY_GUID then
			return normalized
		end
	end

	local generated = item_ids.new_guid()
	identity_store:set(ROOT_ITEM_ID_KEY, generated)
	return generated
end

constants.configure_root_item_id(resolve_root_item_id())

local active_sessions = require("@self/protocol/active_sessions")
local audio_routes = require("@self/routes/audio")
local bitrate_test = require("@self/routes/bitrate_test")
local capability_routes = require("@self/routes/capabilities")
local catalog = require("@self/protocol/items/catalog")
local discovery = require("@self/protocol/discovery")
local filters = require("@self/protocol/items/filters")
local image_routes = require("@self/routes/images")
local instant_mix = require("@self/protocol/items/instant_mix")
local item_refresh = require("@self/routes/item_refresh")
local items_options = require("@self/protocol/items/options")
local listing = require("@self/protocol/items/listing")
local listing_routes = require("@self/routes/listings")
local playback = require("@self/protocol/items/playback")
local playlist_access = require("@self/protocol/playlist_access")
local preferences = require("@self/protocol/preferences")
local remote_control = require("@self/routes/remote_control")
local requests = require("@self/protocol/items/requests")
local responses = require("@self/protocol/items/responses")
local resume_routes = require("@self/routes/resume")
local scheduled_tasks = require("@self/routes/scheduled_tasks")
local session = require("@self/protocol/session")
local socket_routes = require("@self/routes/socket")
local statics = require("@self/protocol/statics")
local syncplay = require("@self/routes/syncplay")
local system_info = require("@self/protocol/system_info")
local user_data = require("@self/protocol/items/user_data")
local user_data_routes = require("@self/routes/user_data")

local SERVER_ID = constants.SERVER_ID
local P = constants.ROUTE_PREFIX

type ApiContext = api.ApiContext
type ApiRequest = api.ApiRequest
type ApiQueryParams = api.ApiQueryParams
type ApiResponse = api.ApiResponse
type JsonValue = api.JsonValue
type JsonObject = { [string]: JsonValue }
type PlaybackPayload = { [string]: string | number | boolean? }
type PublicUser = users.PublicUser

type PublicSystemInfo = system_info.PublicSystemInfo

type SystemInfo = PublicSystemInfo & {
	HasPendingRestart: boolean,
	IsShuttingDown: boolean,
	SupportsLibraryMonitor: boolean,
	WebSocketPortNumber: number,
	CompletedInstallations: {},
	CastReceiverApplications: {},
	CanSelfRestart: boolean,
	CanLaunchWebBrowser: boolean,
}

type VirtualFolderInfo = {
	Name: string,
	Locations: { string },
	CollectionType: string,
	ItemId: string,
	RefreshStatus: string,
}

type SessionEntry = active_sessions.SessionEntry

local sessions = active_sessions.entries

local get_owned_session = active_sessions.get_owned

socket_routes.register()

local AUTH_PROVIDER_ID = "Lyra"

local function find_public_user_by_id(user_id: string): PublicUser?
	local listed_users = users.list()
	if listed_users == nil then
		return nil
	end

	for _, user in listed_users do
		if user.user_id == user_id then
			return user
		end
	end

	return nil
end

local function root_folder_json(user_id: string): JsonObject
	local views = catalog.to_music_view_items(false)
	return {
		Id = constants.ROOT_ITEM_ID,
		Name = "Root",
		Type = "Folder",
		IsFolder = true,
		ServerId = SERVER_ID,
		ChildCount = #views,
		LocationType = "FileSystem",
		MediaType = "Unknown",
		UserData = user_data.user_data_to_json(user_data.get_user_data({
			key = "Root",
			item_hex_id = constants.ROOT_ITEM_ID,
			user_id = user_id,
			include_favorite = false,
			include_play_count = false,
		})),
	}
end

local function empty_query_result(): JsonObject
	return {
		Items = {},
		TotalRecordCount = 0,
		StartIndex = 0,
	}
end

local function item_count_for(principal: session.Principal, include_item_type: string, is_favorite: boolean): number
	local options = items_options.for_user(items_options.parse(nil), principal.user_id)
	options.include_item_types = { include_item_type }
	options.limit = 0
	options.is_favorite = if is_favorite then true else nil

	local ok, _items, total_count = pcall(listing.list_items, options)
	if not ok or type(total_count) ~= "number" then
		return 0
	end
	return total_count
end

api.post(P .. "/Users/AuthenticateByName", function(ctx: ApiContext)
	local credentials, parse_err = session.parse_authenticate_request(ctx)
	if not credentials then
		return parse_err or responses.bad_request("invalid authentication request body")
	end

	local user_agent, client_name = session.login_metadata(ctx)
	local outcome = auth.login(credentials.username, credentials.password, user_agent, client_name)
	if outcome.status == "rate_limited" then
		local headers: { [string]: string } = {}
		if outcome.retry_after_seconds then
			headers["retry-after"] = tostring(outcome.retry_after_seconds)
		end
		return api.response.text(429, "Too Many Requests", headers)
	end

	local login_result = outcome.result
	if not login_result then
		return session.unauthorized()
	end

	local token = requests.trim(login_result.token)
	if not token then
		return session.unauthorized()
	end
	login_result.token = token
	local resolved_auth = auth.resolve_auth(token)
	if resolved_auth then
		session.remember_login_identity(ctx, resolved_auth, token)
	end

	return api.response.json(200, session.to_authentication_result(login_result))
end, { auth = "public", case_insensitive = true })
api.get(P .. "/Users/Public", function(): ApiResponse
	return api.response.json(200, {})
end, { auth = "public", case_insensitive = true })
api.get(P .. "/Users", function(ctx: ApiContext): ApiResponse
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	if not session.is_admin(principal) then
		return api.response.json(200, { session.to_jellyfin_user(principal) })
	end

	local result: { JsonValue } = {}
	for _, user in users.list() do
		table.insert(result, session.to_jellyfin_user(user))
	end

	return api.response.json(200, result)
end, { auth = "public", case_insensitive = true })
api.get(P .. "/Users/{id}", function(ctx: ApiContext): ApiResponse
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local params = if ctx then ctx.params else nil
	local raw_user_id = if params then params.id else nil
	local user_id = item_ids.decode(raw_user_id)
	if not user_id then
		return responses.not_found("user not found")
	end
	if user_id ~= principal.user_id and not session.is_admin(principal) then
		return session.unauthorized()
	end
	if user_id == principal.user_id then
		return api.response.json(200, session.to_jellyfin_user(principal))
	end

	local user = find_public_user_by_id(user_id)
	if not user then
		return responses.not_found("user not found")
	end
	return api.response.json(200, session.to_jellyfin_user(user))
end, { auth = "public", case_insensitive = true })
-- The caller's own configuration, or with `userId` another user's; only an admin may change
-- someone else's.
local function handle_user_configuration_post(ctx: ApiContext, raw_user_id: string?): ApiResponse
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local user_id = principal.user_id
	if raw_user_id then
		local decoded = item_ids.decode(raw_user_id)
		if not decoded then
			return responses.not_found("user not found")
		end
		user_id = decoded
	end
	if user_id ~= principal.user_id then
		if not find_public_user_by_id(user_id) then
			return responses.not_found("user not found")
		end
		if not session.is_admin(principal) then
			return responses.forbidden("User configuration update not allowed")
		end
	end
	return preferences.user_configuration_set(ctx, user_id)
end

api.post(P .. "/Users/Configuration", function(ctx: ApiContext): ApiResponse
	local query = if ctx and ctx.request then ctx.request.query else nil
	return handle_user_configuration_post(ctx, requests.parse_query_string(query, { "userId" }, nil))
end, { auth = "public", case_insensitive = true })
api.post(P .. "/Users/{userId}/Configuration", function(ctx: ApiContext): ApiResponse
	local params = if ctx then ctx.params else nil
	return handle_user_configuration_post(ctx, if params then params.userId else nil)
end, { auth = "public", case_insensitive = true })
api.get(P .. "/QuickConnect/Enabled", function(): ApiResponse
	return api.response.json(200, false)
end, { auth = "public", case_insensitive = true })
api.post(P .. "/Users/ForgotPassword", function(): ApiResponse
	return api.response.json(200, {
		Action = "ContactAdmin",
		PinFile = nil,
		PinExpirationDate = nil,
	})
end, { auth = "public", case_insensitive = true })
api.post(P .. "/Users/ForgotPassword/Pin", function(): ApiResponse
	return api.response.json(200, {
		Success = false,
		UsersReset = {},
	})
end, { auth = "public", case_insensitive = true })
api.post(P .. "/Sessions/Logout", function(ctx: ApiContext)
	local resolved_auth, token = session.require_jellyfin_auth(ctx)
	if not resolved_auth or not token then
		return session.unauthorized()
	end

	auth.logout_session(token)

	return api.response.empty(204)
end, { auth = "public", case_insensitive = true })
api.get(P .. "/Auth/Providers", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local providers: { JsonValue } = {
		{
			Name = "Lyra",
			Id = AUTH_PROVIDER_ID,
		},
	}
	return api.response.json(200, providers)
end, { auth = "public", case_insensitive = true })
api.get(P .. "/Auth/PasswordResetProviders", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local providers: { JsonValue } = {
		{
			Name = "Lyra",
			Id = AUTH_PROVIDER_ID,
		},
	}
	return api.response.json(200, providers)
end, { auth = "public", case_insensitive = true })
api.get(P .. "/System/Info/Public", function(ctx: ApiContext): ApiResponse
	return api.response.json(200, system_info.public_system_info(server.info(), ctx, {}))
end, { auth = "public", case_insensitive = true })
api.get(P .. "/System/Info", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	return api.response.json(
		200,
		system_info.public_system_info(server.info(), ctx, {
			HasPendingRestart = false,
			IsShuttingDown = false,
			SupportsLibraryMonitor = false,
			WebSocketPortNumber = server_info.port,
			CompletedInstallations = {},
			CastReceiverApplications = {},
			CanSelfRestart = true,
			CanLaunchWebBrowser = false,
		})
	)
end, { auth = "public", case_insensitive = true })
api.get(P .. "/System/Ping", function(): ApiResponse
	return api.response.json(200, "Lyra")
end, { auth = "public", case_insensitive = true })
api.post(P .. "/System/Ping", function(): ApiResponse
	return api.response.json(200, "Lyra")
end, { auth = "public", case_insensitive = true })
api.get(P .. "/System/Endpoint", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local in_network = requests.is_local_network_request(ctx)
	return api.response.json(200, {
		IsLocal = in_network,
		IsInNetwork = in_network,
	})
end, { auth = "public", case_insensitive = true })
api.get(P .. "/Library/VirtualFolders", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local ok, libs = pcall(libraries.list)
	if not ok or not libs then
		return api.response.json(200, {})
	end

	local folders: { JsonValue } = {}
	for _, lib in libs do
		local hex_id = item_ids.encode(lib.id)
		if hex_id then
			local locations = if lib.path then { lib.path } else {}
			local folder: JsonValue = {
				Name = lib.name,
				Locations = locations,
				CollectionType = "music",
				ItemId = hex_id,
				RefreshStatus = "",
			}
			table.insert(folders, folder)
		end
	end

	return api.response.json(200, folders)
end, { auth = "public", case_insensitive = true })
api.get(P .. "/Library/MediaFolders", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local views = catalog.to_music_view_items(false)
	return api.response.json(200, catalog.build_item_query_result(views, #views, 0))
end, { auth = "public", case_insensitive = true })
api.get(P .. "/Channels", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	return api.response.json(200, empty_query_result())
end, { auth = "public", case_insensitive = true })
api.get(P .. "/Devices", session.devices_response, { auth = "public", case_insensitive = true })
api.get(P .. "/Users/Me", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	return api.response.json(200, session.to_jellyfin_user(principal))
end, { auth = "public", case_insensitive = true })
local function respond_with_user_views(ctx: ApiContext, principal: session.Principal): ApiResponse
	local query = if ctx and ctx.request then ctx.request.query else nil
	local options = items_options.for_user(items_options.parse(query), principal.user_id)
	-- Upstream's views take no `SortBy`; they come in the caller's order.
	options.sort_by = {}
	local include_hidden = string.lower(
		requests.parse_query_string(query, { "includeHidden", "IncludeHidden" }, nil) or ""
	) == "true"
	local items, total_count, start_index = listing.list_views(options, include_hidden)
	return api.response.json(200, catalog.build_item_query_result(items, total_count, start_index))
end

api.get(P .. "/Users/{id}/Views", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	if not session.require_matching_user_scope_from_id_param(ctx, principal) then
		return session.unauthorized()
	end

	return respond_with_user_views(ctx, principal)
end, { auth = "public", case_insensitive = true })
api.get(P .. "/UserViews", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	return respond_with_user_views(ctx, principal)
end, { auth = "public", case_insensitive = true })
api.get(P .. "/UserViews/GroupingOptions", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	return api.response.json(200, {})
end, { auth = "public", case_insensitive = true })
api.get(P .. "/Users/{userId}/GroupingOptions", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	return api.response.json(200, {})
end, { auth = "public", case_insensitive = true })
local function handle_root_folder_route(ctx: ApiContext): ApiResponse
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	return api.response.json(200, root_folder_json(principal.user_id))
end

api.get(P .. "/Items/Root", handle_root_folder_route, { auth = "public", case_insensitive = true })
api.get(P .. "/Users/{userId}/Items/Root", handle_root_folder_route, { auth = "public", case_insensitive = true })
local function respond_with_items_listing(ctx: ApiContext, principal: session.Principal): ApiResponse
	local query = if ctx and ctx.request then ctx.request.query else nil
	local options = items_options.for_user(items_options.parse(query), principal.user_id)
	if items_options.has_conflicting_filters(options) then
		return responses.bad_request("Conflicting filters")
	end
	-- Upstream ranks searches by relevance whatever `SortBy` asks for.
	if options.search_term ~= nil then
		options.sort_by = {}
	end

	-- Jellyfin returns the user root's direct children (library views) for an unfiltered,
	-- non-recursive listing without ParentId or Ids.
	if
		(options.parent_id == nil or constants.is_root_item_id(options.parent_id))
		and #options.ids == 0
		and options.recursive == false
		and not items_options.has_filters(options)
	then
		local items, total_count, start_index = listing.list_views(options)
		return api.response.json(200, catalog.build_item_query_result(items, total_count, start_index))
	end

	return catalog.item_query_response(function(emit)
		return listing.each_item_batch(options, emit)
	end)
end

api.get(P .. "/Users/{userId}/Items", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	return respond_with_items_listing(ctx, principal)
end, { auth = "public", case_insensitive = true })
api.get(P .. "/Items", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	return respond_with_items_listing(ctx, principal)
end, { auth = "public", case_insensitive = true })
api.get(P .. "/Artists/{itemId}/Similar", listing_routes.handle_similar, { auth = "public", case_insensitive = true })
api.get(P .. "/Items/{itemId}/Similar", listing_routes.handle_similar, { auth = "public", case_insensitive = true })
api.get(P .. "/Albums/{itemId}/Similar", listing_routes.handle_similar, { auth = "public", case_insensitive = true })
local function respond_with_instant_mix(ctx: ApiContext, raw_item_id: string?): ApiResponse
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	if not raw_item_id then
		return responses.bad_request("missing item id")
	end

	local item_id = item_ids.decode(raw_item_id)
	if not item_id then
		return responses.not_found("item not found")
	end

	local query = if ctx and ctx.request then ctx.request.query else nil
	local options = items_options.for_user(items_options.parse(query), principal.user_id)
	local items, total_count, start_index, found = instant_mix.list_instant_mix_audio(item_id, options)
	if not found then
		return responses.not_found("item not found")
	end

	return api.response.json(200, catalog.build_item_query_result(items, total_count, start_index))
end

local function handle_instant_mix_route(ctx: ApiContext): ApiResponse
	local params = if ctx then ctx.params else nil
	return respond_with_instant_mix(ctx, if params then params.itemId else nil)
end

-- Upstream's obsolete routes that take the item id from the query.
local function handle_instant_mix_query_route(ctx: ApiContext): ApiResponse
	local query = if ctx and ctx.request then ctx.request.query else nil
	return respond_with_instant_mix(ctx, requests.parse_query_string(query, { "id", "Id" }, nil))
end

local function handle_instant_mix_genre_name_route(ctx: ApiContext): ApiResponse
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	local params = if ctx then ctx.params else nil
	local name = if params then (params.name or params.genreName) else nil
	if not name or name == "" then
		return responses.bad_request("missing genre name")
	end

	local query = if ctx and ctx.request then ctx.request.query else nil
	local options = items_options.for_user(items_options.parse(query), principal.user_id)
	local items, total_count, start_index, found = instant_mix.list_instant_mix_audio_from_genre_name(name, options)
	if not found then
		return responses.not_found("genre not found")
	end

	return api.response.json(200, catalog.build_item_query_result(items, total_count, start_index))
end

api.get(P .. "/Items/{itemId}/InstantMix", handle_instant_mix_route, { auth = "public", case_insensitive = true })
api.get(P .. "/Songs/{itemId}/InstantMix", handle_instant_mix_route, { auth = "public", case_insensitive = true })
api.get(P .. "/Albums/{itemId}/InstantMix", handle_instant_mix_route, { auth = "public", case_insensitive = true })
api.get(P .. "/Artists/{itemId}/InstantMix", handle_instant_mix_route, { auth = "public", case_insensitive = true })
api.get(P .. "/Playlists/{itemId}/InstantMix", handle_instant_mix_route, { auth = "public", case_insensitive = true })
api.get(
	P .. "/MusicGenres/{name}/InstantMix",
	handle_instant_mix_genre_name_route,
	{ auth = "public", case_insensitive = true }
)
api.get(P .. "/Artists/InstantMix", handle_instant_mix_query_route, { auth = "public", case_insensitive = true })
api.get(P .. "/MusicGenres/InstantMix", handle_instant_mix_query_route, { auth = "public", case_insensitive = true })
api.get(P .. "/Users/{userId}/Items/Latest", listing_routes.handle_latest, { auth = "public", case_insensitive = true })
api.get(P .. "/Users/{userId}/Items/{itemId}", function(ctx: ApiContext)
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
	if constants.is_playlists_folder_id(raw_item_id) then
		return api.response.json(200, catalog.item_to_json(catalog.playlists_folder_item(nil)))
	end

	local item = listing.find_item_by_id(item_ids.decode(raw_item_id), principal.user_id, true)
	if not item then
		return responses.not_found("item not found")
	end

	return api.response.json(200, catalog.item_to_json(item))
end, { auth = "public", case_insensitive = true })
api.get(P .. "/Items/{id}", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	local params = if ctx then ctx.params else nil
	local raw_id = if params then params.id else nil
	if not raw_id then
		return responses.bad_request("missing item id")
	end
	if constants.is_root_item_id(raw_id) then
		return api.response.json(200, root_folder_json(principal.user_id))
	end
	if constants.is_playlists_folder_id(raw_id) then
		return api.response.json(200, catalog.item_to_json(catalog.playlists_folder_item(nil)))
	end

	local item = listing.find_item_by_id(item_ids.decode(raw_id), principal.user_id, true)
	if not item then
		return responses.not_found("item not found")
	end

	return api.response.json(200, catalog.item_to_json(item))
end, { auth = "public", case_insensitive = true })

user_data_routes.register()
resume_routes.register()
api.get(
	P .. "/Playlists/{playlistId}/Items",
	listing_routes.handle_playlist_items,
	{ auth = "public", case_insensitive = true }
)
api.post(P .. "/Playlists", session.handle_create_playlist_route, { auth = "public", case_insensitive = true })
api.delete(P .. "/Items/{id}", session.handle_delete_item_route, { auth = "public", case_insensitive = true })
api.post(
	P .. "/Playlists/{playlistId}/Items",
	session.handle_add_playlist_items_route,
	{ auth = "public", case_insensitive = true }
)
api.delete(P .. "/Playlists/{playlistId}/Items", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local playlist, playlist_err = session.resolve_playlist_for_route(ctx)
	if not playlist then
		return playlist_err or responses.bad_request("playlist not found")
	end

	local can_edit, edit_err = session.ensure_playlist_editable(playlist, principal)
	if not can_edit then
		return edit_err or responses.bad_request("playlist is read-only")
	end

	local query = if ctx and ctx.request then ctx.request.query else nil
	local entry_ids = session.parse_playlist_query_entry_ids(query, playlist)
	if #entry_ids > 0 then
		local ok, removed = pcall(playlists.remove_entries, playlist.id, entry_ids)
		if not ok or not removed then
			return responses.bad_request("failed to remove playlist entry")
		end
	end

	return api.response.empty(204)
end, { auth = "public", case_insensitive = true })
api.post(P .. "/Playlists/{playlistId}/Items/{itemId}/Move/{newIdx}", function(ctx: ApiContext): ApiResponse
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end
	local playlist, playlist_err = session.resolve_playlist_for_route(ctx)
	if not playlist then
		return playlist_err or responses.not_found("playlist not found")
	end
	local can_edit, edit_err = session.ensure_playlist_editable(playlist, principal)
	if not can_edit then
		return edit_err or responses.forbidden("playlist is read-only")
	end
	local raw_position = ctx.params.newIdx
	local position = tonumber(raw_position)
	if not string.match(raw_position, "^%d+$") or not position or position > 9007199254740991 then
		return responses.bad_request("newIdx must be a non-negative integer within the exact numeric range")
	end
	-- Upstream moves the item's first entry, and moves nothing for an item the playlist lacks.
	local entry_id: string? = playlist_access.entries_of_items(playlist, { ctx.params.itemId })[1]
	if entry_id == nil then
		return api.response.empty(204)
	end
	local ok, moved = pcall(playlists.move_entry, playlist.id, entry_id, position)
	if not ok then
		return responses.bad_request("failed to move playlist entry")
	end
	if not moved then
		return responses.not_found("playlist entry not found")
	end
	return api.response.empty(204)
end, { auth = "public", case_insensitive = true })
api.post(
	P .. "/Playlists/{playlistId}",
	session.handle_update_playlist_route,
	{ auth = "public", case_insensitive = true }
)
api.get(P .. "/Playlists/{playlistId}", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
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

	return api.response.json(200, session.playlist_info_to_json(session.to_playlist_info(playlist)))
end, { auth = "public", case_insensitive = true })
api.get(P .. "/Playlists/{playlistId}/Users/{userId}", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
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

	local params = if ctx then ctx.params else nil
	local requested_user_id = if params then session.normalize_optional_user_id(params.userId) else nil
	if not requested_user_id then
		return responses.bad_request("missing user id")
	end

	local target_user_id = item_ids.decode(requested_user_id)
	if not target_user_id then
		return responses.not_found("user not found")
	end
	local requester_can_edit = playlist_access.can_edit(playlist, principal.user_id)
	if target_user_id ~= principal.user_id and not requester_can_edit then
		return session.unauthorized()
	end

	return api.response.json(
		200,
		session.playlist_user_permission_to_json(session.to_playlist_user_permission(playlist, target_user_id))
	)
end, { auth = "public", case_insensitive = true })
api.get(P .. "/Playlists/{playlistId}/Users", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local playlist, playlist_err = session.resolve_playlist_for_route(ctx)
	if not playlist then
		return playlist_err or responses.bad_request("playlist not found")
	end

	local can_edit, edit_err = session.ensure_playlist_editable(playlist, principal)
	if not can_edit then
		return edit_err or responses.bad_request("playlist is read-only")
	end

	local owner_id = playlist_access.owner_id(playlist)
	local users = {}
	if owner_id then
		table.insert(users, session.to_playlist_user_permission(playlist, owner_id))
	end

	return api.response.json(200, session.playlist_users_to_json(users))
end, { auth = "public", case_insensitive = true })
api.get(P .. "/Artists", listing_routes.handle_artists, { auth = "public", case_insensitive = true })
api.get(P .. "/Persons", listing_routes.handle_persons, { auth = "public", case_insensitive = true })
api.get(P .. "/Artists/AlbumArtists", listing_routes.handle_album_artists, { auth = "public", case_insensitive = true })
local function handle_studios_route(ctx: ApiContext): ApiResponse
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local query = if ctx and ctx.request then ctx.request.query else nil
	local options = items_options.for_user(items_options.parse(query), principal.user_id)
	local empty_items = {}
	return api.response.json(200, catalog.build_item_query_result(empty_items, 0, options.start_index))
end

api.get(P .. "/Genres", listing_routes.handle_genres, { auth = "public", case_insensitive = true })
api.get(P .. "/MusicGenres", listing_routes.handle_genres, { auth = "public", case_insensitive = true })
api.get(P .. "/Studios", function(ctx: ApiContext)
	return handle_studios_route(ctx)
end, { auth = "public", case_insensitive = true })
api.get(P .. "/Items/{imageId}/Images/Primary", function(ctx: ApiContext)
	return session.handle_primary_image_request(ctx)
end, { auth = "public", case_insensitive = true })
api.head(P .. "/Items/{imageId}/Images/Primary", function(ctx: ApiContext)
	return session.handle_primary_image_request(ctx)
end, { auth = "public", case_insensitive = true })
image_routes.register()
item_refresh.register()
scheduled_tasks.register()
api.get(P .. "/Persons/{name}/Images/{imageType}", function(ctx: ApiContext)
	return session.handle_person_image_request(ctx)
end, { auth = "public", case_insensitive = true })
api.head(P .. "/Persons/{name}/Images/{imageType}", function(ctx: ApiContext)
	return session.handle_person_image_request(ctx)
end, { auth = "public", case_insensitive = true })
api.get(P .. "/Persons/{name}/Images/{imageType}/{imageIndex}", function(ctx: ApiContext)
	return session.handle_person_image_request(ctx)
end, { auth = "public", case_insensitive = true })
api.head(P .. "/Persons/{name}/Images/{imageType}/{imageIndex}", function(ctx: ApiContext)
	return session.handle_person_image_request(ctx)
end, { auth = "public", case_insensitive = true })
api.get(P .. "/Artists/{name}/Images/{imageType}/{imageIndex}", function(ctx: ApiContext)
	return session.handle_artist_image_request(ctx)
end, { auth = "public", case_insensitive = true })
api.head(P .. "/Artists/{name}/Images/{imageType}/{imageIndex}", function(ctx: ApiContext)
	return session.handle_artist_image_request(ctx)
end, { auth = "public", case_insensitive = true })
api.get(P .. "/Genres/{name}/Images/{imageType}", function(ctx: ApiContext)
	return session.handle_genre_image_request(ctx)
end, { auth = "public", case_insensitive = true })
api.head(P .. "/Genres/{name}/Images/{imageType}", function(ctx: ApiContext)
	return session.handle_genre_image_request(ctx)
end, { auth = "public", case_insensitive = true })
api.get(P .. "/Genres/{name}/Images/{imageType}/{imageIndex}", function(ctx: ApiContext)
	return session.handle_genre_image_request(ctx)
end, { auth = "public", case_insensitive = true })
api.head(P .. "/Genres/{name}/Images/{imageType}/{imageIndex}", function(ctx: ApiContext)
	return session.handle_genre_image_request(ctx)
end, { auth = "public", case_insensitive = true })
api.get(P .. "/MusicGenres/{name}/Images/{imageType}", function(ctx: ApiContext)
	return session.handle_genre_image_request(ctx)
end, { auth = "public", case_insensitive = true })
api.head(P .. "/MusicGenres/{name}/Images/{imageType}", function(ctx: ApiContext)
	return session.handle_genre_image_request(ctx)
end, { auth = "public", case_insensitive = true })
api.get(P .. "/MusicGenres/{name}/Images/{imageType}/{imageIndex}", function(ctx: ApiContext)
	return session.handle_genre_image_request(ctx)
end, { auth = "public", case_insensitive = true })
api.head(P .. "/MusicGenres/{name}/Images/{imageType}/{imageIndex}", function(ctx: ApiContext)
	return session.handle_genre_image_request(ctx)
end, { auth = "public", case_insensitive = true })
api.post(P .. "/Sessions/Capabilities", function(ctx: ApiContext)
	return capability_routes.handle(ctx, false)
end, { auth = "public", case_insensitive = true })
api.post(P .. "/Sessions/Capabilities/Full", function(ctx: ApiContext)
	return capability_routes.handle(ctx, true)
end, { auth = "public", case_insensitive = true })

api.get(P .. "/Sessions", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local request = if ctx then ctx.request else nil
	local query = if request then request.query else nil
	local controllable = requests.parse_query_string(query, { "controllableByUserId", "ControllableByUserId" }, nil)
	local controllable_user_id = if controllable then item_ids.decode(controllable) else nil
	if controllable and not controllable_user_id then
		return api.response.json(200, {})
	end

	return api.response.json(200, active_sessions.list_visible(principal, controllable_user_id))
end, { auth = "public", case_insensitive = true })

syncplay.register(sessions, get_owned_session)
api.post(
	P .. "/Sessions/{sessionId}/Playing/{command}",
	remote_control.handle_playstate,
	{ auth = "public", case_insensitive = true }
)
api.post(
	P .. "/Sessions/{sessionId}/Command/{command}",
	remote_control.handle_named_command,
	{ auth = "public", case_insensitive = true }
)
api.post(
	P .. "/Sessions/{sessionId}/Command",
	remote_control.handle_general_command,
	{ auth = "public", case_insensitive = true }
)
api.post(P .. "/Sessions/{sessionId}/Playing", remote_control.handle_play, { auth = "public", case_insensitive = true })
api.post(P .. "/Sessions/Playing", function(ctx: ApiContext)
	return session.handle_playback_report_route(ctx, "started")
end, { auth = "public", case_insensitive = true })
api.post(P .. "/Sessions/Playing/Progress", function(ctx: ApiContext)
	return session.handle_playback_report_route(ctx, "progress")
end, { auth = "public", case_insensitive = true })
api.post(P .. "/Sessions/Playing/Stopped", function(ctx: ApiContext)
	return session.handle_playback_report_route(ctx, "stopped")
end, { auth = "public", case_insensitive = true })
api.post(
	P .. "/Sessions/Playing/Ping",
	session.handle_playback_ping_route,
	{ auth = "public", case_insensitive = true }
)
api.delete(
	P .. "/Videos/ActiveEncodings",
	session.handle_stop_encoding_route,
	{ auth = "public", case_insensitive = true }
)
api.get(P .. "/Items/{id}/PlaybackInfo", function(ctx: ApiContext)
	local resolved_auth, access_token = session.require_jellyfin_auth(ctx)
	if not resolved_auth then
		return session.unauthorized()
	end
	local principal = resolved_auth.principal

	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	local item_id, track, resolve_err = playback.resolve_playback_info_item(ctx)
	if resolve_err then
		return resolve_err or responses.bad_request("invalid playback item")
	end

	local request = if ctx then ctx.request else nil
	local play_session_id = playback.resolve_requested_play_session_id(request, nil)
	local identity = session.resolve_client_identity(ctx, resolved_auth, access_token)
	local response = playback.build_playback_info(
		item_id,
		track,
		play_session_id,
		nil,
		nil,
		access_token,
		if identity then identity.device_id else nil
	)
	return api.response.json(200, response)
end, { auth = "public", case_insensitive = true })
api.get(P .. "/Items/{itemId}/File", function(ctx: ApiContext)
	return session.handle_item_file_stream_request(ctx)
end, { auth = "public", case_insensitive = true })
api.get(P .. "/Items/{itemId}/Download", function(ctx: ApiContext)
	return session.handle_item_download_request(ctx)
end, { auth = "public", case_insensitive = true })

-- Returns (value, err) so the handler can distinguish "absent" (value=nil, err=nil)
-- from "present but malformed/negative" (value=nil, err=string). Zero is returned
-- as-is so the server-side policy sees the value and can reject it.
audio_routes.register()
bitrate_test.register()
api.post(P .. "/Items/{id}/PlaybackInfo", function(ctx: ApiContext)
	local resolved_auth, access_token = session.require_jellyfin_auth(ctx)
	if not resolved_auth then
		return session.unauthorized()
	end
	local principal = resolved_auth.principal

	local request = if ctx then ctx.request else nil
	local request_json = if request then request.json else nil
	if request_json ~= nil and type(request_json) ~= "table" then
		return responses.bad_request("invalid playback info request body")
	end
	local request_object = if type(request_json) == "table" then request_json :: JsonObject else nil

	local requested_user_id = if request_object then request_object.UserId else nil
	if type(requested_user_id) == "string" and item_ids.decode(requested_user_id) ~= principal.user_id then
		return session.unauthorized()
	end

	local item_id, track, resolve_err = playback.resolve_playback_info_item(ctx)
	if resolve_err then
		return resolve_err or responses.bad_request("invalid playback item")
	end

	local playback_request = if request_object then request_object :: PlaybackPayload else nil
	local play_session_id = playback.resolve_requested_play_session_id(request, playback_request)
	local requested_media_source_id = playback.resolve_requested_media_source_id(request, request_object)
	local identity = session.resolve_client_identity(ctx, resolved_auth, access_token)
	local response, err = playback.build_playback_info(
		item_id,
		track,
		play_session_id,
		request_object,
		requested_media_source_id,
		access_token,
		if identity then identity.device_id else nil
	)
	if err then
		return responses.bad_request(err)
	end
	return api.response.json(200, response)
end, { auth = "public", case_insensitive = true })

api.get(P .. "/Items/Latest", listing_routes.handle_latest, { auth = "public", case_insensitive = true })
api.get(P .. "/Shows/NextUp", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	return api.response.json(200, empty_query_result())
end, { auth = "public", case_insensitive = true })
api.get(P .. "/Items/Suggestions", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	return api.response.json(200, empty_query_result())
end, { auth = "public", case_insensitive = true })
api.get(P .. "/Users/{userId}/Suggestions", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	return api.response.json(200, empty_query_result())
end, { auth = "public", case_insensitive = true })
api.get(P .. "/Items/Counts", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	local query = if ctx and ctx.request then ctx.request.query else nil
	local options = items_options.parse(query)
	local is_favorite = options.is_favorite == true
	return api.response.json(200, {
		MovieCount = 0,
		SeriesCount = 0,
		EpisodeCount = 0,
		AlbumCount = item_count_for(principal, "MusicAlbum", is_favorite),
		SongCount = item_count_for(principal, "Audio", is_favorite),
		MusicVideoCount = 0,
		BoxSetCount = 0,
		BookCount = 0,
	})
end, { auth = "public", case_insensitive = true })
local function respond_with_filters(ctx: ApiContext, legacy: boolean): ApiResponse
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	local query = if ctx and ctx.request then ctx.request.query else nil
	local options = items_options.for_user(items_options.parse(query), principal.user_id)
	local values = filters.collect(options)
	return api.response.json(
		200,
		if legacy then filters.legacy_json(values) else filters.to_json(values, options.include_item_types)
	)
end

api.get(P .. "/Items/Filters", function(ctx: ApiContext)
	return respond_with_filters(ctx, true)
end, { auth = "public", case_insensitive = true })
api.get(P .. "/Items/Filters2", function(ctx: ApiContext)
	return respond_with_filters(ctx, false)
end, { auth = "public", case_insensitive = true })
api.get(P .. "/Items/{id}/Ancestors", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local params = if ctx then ctx.params else nil
	local raw_id = if params then params.id else nil
	if not raw_id then
		return responses.bad_request("missing item id")
	end
	local item_id = item_ids.decode(raw_id)
	if not item_id then
		return responses.not_found("item not found")
	end

	local chain, item_found = listing.ancestors_chain(item_id, principal.user_id)
	if not item_found then
		return responses.not_found("item not found")
	end

	local result: { api.JsonValue } = {}
	for _, parent in chain do
		table.insert(result, catalog.item_to_json(parent))
	end
	return api.response.json(200, result)
end, { auth = "public", case_insensitive = true })
api.get(P .. "/Items/{itemId}/Intros", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	return api.response.json(200, empty_query_result())
end, { auth = "public", case_insensitive = true })
-- Lyra has no collections, so every visible item is in none.
api.get(P .. "/Items/{itemId}/Collections", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	local params = if ctx then ctx.params else nil
	local raw_item_id = if params then params.itemId else nil
	local item_id = if raw_item_id then item_ids.decode(raw_item_id) else nil
	if not item_id or not listing.find_item_by_id(item_id, principal.user_id) then
		return responses.not_found("item not found")
	end

	return api.response.json(200, empty_query_result())
end, { auth = "public", case_insensitive = true })
local function handle_empty_item_array_route(ctx: ApiContext): ApiResponse
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	return api.response.json(200, {})
end

api.get(
	P .. "/Items/{itemId}/LocalTrailers",
	handle_empty_item_array_route,
	{ auth = "public", case_insensitive = true }
)
api.get(
	P .. "/Users/{userId}/Items/{itemId}/LocalTrailers",
	handle_empty_item_array_route,
	{ auth = "public", case_insensitive = true }
)
api.get(
	P .. "/Items/{itemId}/SpecialFeatures",
	handle_empty_item_array_route,
	{ auth = "public", case_insensitive = true }
)
api.get(
	P .. "/Users/{userId}/Items/{itemId}/SpecialFeatures",
	handle_empty_item_array_route,
	{ auth = "public", case_insensitive = true }
)
api.get(P .. "/Artists/{name}", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local params = if ctx then ctx.params else nil
	local name = if params then params.name else nil
	if not name or name == "" then
		return responses.bad_request("missing artist name")
	end

	local found = listing.find_artist_by_name(name, principal.user_id)
	if not found then
		return responses.not_found("artist not found")
	end
	return api.response.json(200, catalog.item_to_json(found))
end, { auth = "public", case_insensitive = true })
api.get(P .. "/Persons/{name}", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local params = if ctx then ctx.params else nil
	local name = if params then params.name else nil
	if not name or name == "" then
		return responses.bad_request("missing person name")
	end

	local found = listing.find_person_by_name(name, principal.user_id)
	if not found then
		return responses.not_found("person not found")
	end
	return api.response.json(200, catalog.item_to_json(found))
end, { auth = "public", case_insensitive = true })
local function handle_genre_by_name_route(ctx: ApiContext): ApiResponse
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local params = if ctx then ctx.params else nil
	local name = if params then (params.name or params.genreName) else nil
	if not name or name == "" then
		return responses.bad_request("missing genre name")
	end

	local found = listing.find_genre_by_name(name, principal.user_id)
	if not found then
		return responses.not_found("genre not found")
	end
	return api.response.json(200, catalog.item_to_json(found))
end

api.get(P .. "/Genres/{genreName}", handle_genre_by_name_route, { auth = "public", case_insensitive = true })
api.get(P .. "/MusicGenres/{genreName}", handle_genre_by_name_route, { auth = "public", case_insensitive = true })
api.get(P .. "/GetUtcTime", function(): ApiResponse
	return statics.utc_time_response()
end, { auth = "public", case_insensitive = true })
api.get(P .. "/Localization/Cultures", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	return statics.cultures_response()
end, { auth = "public", case_insensitive = true })
api.get(P .. "/Localization/Countries", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	return statics.countries_response()
end, { auth = "public", case_insensitive = true })
api.get(P .. "/Localization/ParentalRatings", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	return statics.parental_ratings_response()
end, { auth = "public", case_insensitive = true })
api.get(P .. "/Branding/Configuration", function(): ApiResponse
	return statics.branding_configuration_response()
end, { auth = "public", case_insensitive = true })
api.get(P .. "/Branding/Css", function(): ApiResponse
	return statics.branding_css_response()
end, { auth = "public", case_insensitive = true })
api.get(P .. "/Branding/Css.css", function(): ApiResponse
	return statics.branding_css_response()
end, { auth = "public", case_insensitive = true })
api.delete(P .. "/Devices", session.delete_devices_response, { auth = "public", case_insensitive = true })
api.get(P .. "/Devices/Info", session.device_info_response, { auth = "public", case_insensitive = true })
api.get(P .. "/Devices/Options", function(ctx: ApiContext)
	return session.require_admin(ctx) or preferences.device_options_get(ctx)
end, { auth = "public", case_insensitive = true })
api.post(P .. "/Devices/Options", function(ctx: ApiContext)
	return session.require_admin(ctx) or preferences.device_options_set(ctx)
end, { auth = "public", case_insensitive = true })
api.get(P .. "/DisplayPreferences/{displayPreferencesId}", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local params = if ctx then ctx.params else nil
	local prefs_id = if params then params.displayPreferencesId else nil
	if not prefs_id or prefs_id == "" then
		return responses.bad_request("missing displayPreferencesId")
	end
	return preferences.display_prefs_get(ctx, principal.user_id, prefs_id)
end, { auth = "public", case_insensitive = true })
api.post(P .. "/DisplayPreferences/{displayPreferencesId}", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local params = if ctx then ctx.params else nil
	local prefs_id = if params then params.displayPreferencesId else nil
	if not prefs_id or prefs_id == "" then
		return responses.bad_request("missing displayPreferencesId")
	end
	return preferences.display_prefs_set(ctx, principal.user_id, prefs_id)
end, { auth = "public", case_insensitive = true })
-- Lyra has no artwork of these types, so upstream would answer 404 to anyone: for an unknown
-- item, one the caller can't see, or one without the image.
local function handle_missing_image_route(_ctx: ApiContext): ApiResponse
	return statics.image_not_found_response()
end

local NON_PRIMARY_IMAGE_TYPES = {
	"Backdrop",
	"Logo",
	"Thumb",
	"Art",
	"Banner",
	"Disc",
	"Box",
	"BoxRear",
	"Menu",
	"Chapter",
	"Screenshot",
	"Profile",
}
for _, image_type in NON_PRIMARY_IMAGE_TYPES do
	api.get(
		P .. "/Items/{imageId}/Images/" .. image_type,
		handle_missing_image_route,
		{ auth = "public", case_insensitive = true }
	)
	api.head(
		P .. "/Items/{imageId}/Images/" .. image_type,
		handle_missing_image_route,
		{ auth = "public", case_insensitive = true }
	)
	api.get(
		P .. "/Items/{imageId}/Images/" .. image_type .. "/{imageIndex}",
		handle_missing_image_route,
		{ auth = "public", case_insensitive = true }
	)
	api.head(
		P .. "/Items/{imageId}/Images/" .. image_type .. "/{imageIndex}",
		handle_missing_image_route,
		{ auth = "public", case_insensitive = true }
	)
end

discovery.start(server_info)
