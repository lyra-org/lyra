--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")
local auth = require("@lyra/auth")
local datastore = require("@lyra/datastore")
local ids = require("@lyra/ids")
local libraries = require("@lyra/libraries")
local playback_sessions = require("@lyra/playback_sessions")
local playlists = require("@lyra/playlists")
local plugins = require("@lyra/plugins")
local server = require("@lyra/server")
local tracks = require("@lyra/tracks")
local users = require("@lyra/users")

local crypt = require("@harmony/crypt")
local task = require("@harmony/task")

local constants = require("@self/constants")
local config = plugins.declare_settings(constants.declare_settings)
local server_info = server.info()
constants.configure_settings(config)
constants.configure_server_id(server_info.id)

local ROOT_ID_STORE_NAME = "jellyfin:identity"
local ROOT_ITEM_ID_KEY = "root_item_id"

local function generate_root_item_id(): string
	local ok, random_hex = pcall(crypt.random, 16)
	if ok and type(random_hex) == "string" then
		local normalized = constants.normalize_item_id(random_hex)
		if normalized and not constants.is_root_item_id(normalized) then
			return normalized
		end
	end

	local fallback = crypt.hash("xxh3_128", `{server_info.id}:{os.time()}`)
	local normalized_fallback = constants.normalize_item_id(fallback)
	if normalized_fallback and normalized_fallback ~= constants.EMPTY_ITEM_ID then
		return normalized_fallback
	end
	error("failed to generate Jellyfin root item id")
end

local function resolve_root_item_id(): string
	local identity_store = datastore.get_or_create(ROOT_ID_STORE_NAME)
	local stored = identity_store:get(ROOT_ITEM_ID_KEY)
	if type(stored) == "string" then
		local normalized = constants.normalize_item_id(stored)
		if normalized and normalized ~= constants.EMPTY_ITEM_ID then
			return normalized
		end
	end

	local generated = generate_root_item_id()
	identity_store:set(ROOT_ITEM_ID_KEY, generated)
	return generated
end

constants.configure_root_item_id(resolve_root_item_id())

local audio_routes = require("@self/routes/audio")
local catalog = require("@self/protocol/items/catalog")
local discovery = require("@self/protocol/discovery")
local item_ids = require("@self/protocol/items/identifiers")
local playback = require("@self/protocol/items/playback")
local playlist_access = require("@self/protocol/playlist_access")
local preferences = require("@self/protocol/preferences")
local requests = require("@self/protocol/items/requests")
local responses = require("@self/protocol/items/responses")
local results = require("@self/protocol/items/results")
local session = require("@self/protocol/session")
local statics = require("@self/protocol/statics")
local syncplay = require("@self/routes/syncplay")
local user_data = require("@self/protocol/items/user_data")
local websocket = require("@self/protocol/websocket")

-- Represents the last known supported Jellyfin server version.
local SERVER_VERSION = "10.11.0"
local SERVER_ID = constants.SERVER_ID
local P = constants.ROUTE_PREFIX

type ApiContext = api.ApiContext
type ApiRequest = api.ApiRequest
type ApiQueryParams = api.ApiQueryParams
type ApiResponse = api.ApiResponse
type JsonValue = api.JsonValue
type JsonObject = { [string]: JsonValue }
type ResolvedAuth = auth.ResolvedAuth
type PlaylistCreateRequest = playlists.PlaylistCreateRequest
type PlaylistUpdateRequest = playlists.PlaylistUpdateRequest
type PlaybackPayload = { [string]: string | number | boolean? }
type PublicUser = users.PublicUser

type UserIdentity = {
	user_id: number,
	username: string,
}

type PublicSystemInfo = {
	ServerName: string,
	Version: string,
	ProductName: string,
	Id: string,
	LocalAddress: string,
	PublicAddress: string?,
	StartupWizardCompleted: boolean,
}

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

type SessionEntry = {
	sender: api.ApiWebSocketSender,
	session_id: string,
	user_id: number,
	is_admin: boolean,
	device_id: string,
	-- Matches playback_sessions' own session_key formula so broadcast-time
	-- joins can pair WS entries with playback connections.
	auth_session_id: number?,
	info: JsonObject,
	capabilities: JsonObject?,
	-- Keys set by the most recent /Capabilities/Full. Non-Full merges must
	-- skip these so Full stays authoritative and removal-via-Full works.
	full_capability_keys: { [string]: boolean },
	subscribed_sessions: boolean,
	last_activity: string,
	last_playback_event_s: number?,
}

type NativeConnectionTarget = {
	user_id: number,
	connection: playback_sessions.ConnectionInfo,
}

local sessions: { [string]: SessionEntry } = {}

local JELLYFIN_WS_REAUTH_INTERVAL_SECONDS = 60
local JELLYFIN_WS_REAUTH_MAX_ERRORS = 5
-- Jellyfin's ForceKeepAlive Data is the interval (seconds) after which the
-- server considers a client dead; clients echo KeepAlive at half that rate.
local JELLYFIN_WS_KEEPALIVE_SECONDS = 60
-- Rate-limit inbound PlaybackProgress so a misbehaving client can't flood the
-- DB. Start / Stopped go through unthrottled. Mirrors Jellyfin's own ~10s
-- progress cadence.
local JELLYFIN_WS_PROGRESS_MIN_INTERVAL_SECONDS = 10

-- Mirrors playback_sessions.resolve_playback_session_key so broadcast joins
-- pair WS entries with native playback connections by the same key.
local function connection_join_key(entry: SessionEntry): string
	if entry.auth_session_id then
		return string.format("auth:%d", entry.auth_session_id)
	end
	return string.format("device:%d:%s", entry.user_id, entry.device_id)
end

-- Returns nil on miss, scope mismatch, or closed sender (evicting the
-- closed entry). Mismatch is logged so a hash collision is distinguishable
-- from the normal miss.
local function get_owned_session(session_id: string?, principal: session.Principal): SessionEntry?
	if not session_id then
		return nil
	end
	local entry = sessions[session_id]
	if not entry then
		return nil
	end
	if entry.user_id ~= principal.user_id then
		warn(`session user_id mismatch: principal={principal.user_id}, entry={entry.user_id}, session_id={session_id}`)
		return nil
	end
	if entry.sender:is_closed() then
		sessions[session_id] = nil
		return nil
	end
	return entry
end

api.websocket(P .. "/socket", function(reader, sender, ctx)
	local resolved_auth, token = session.require_jellyfin_auth(ctx)
	if not resolved_auth or not token then
		return
	end

	local identity = session.resolve_client_identity(ctx)
	if not identity then
		warn(`socket rejected: missing Client/DeviceId`)
		return
	end

	local principal = resolved_auth.principal
	local session_id = session.build_session_id(principal, identity)
	local iso_now = session.current_iso_timestamp()
	local info = session.build_session_info(session_id, principal, identity, iso_now)
	local entry: SessionEntry = {
		sender = sender,
		session_id = session_id,
		user_id = principal.user_id,
		is_admin = session.is_admin(principal),
		device_id = identity.device_id,
		auth_session_id = resolved_auth.credential.session_id,
		info = info,
		capabilities = nil,
		full_capability_keys = {},
		subscribed_sessions = false,
		last_activity = iso_now,
		last_playback_event_s = nil,
	}

	local existing = sessions[session_id]
	if existing then
		if existing.sender:is_closed() then
			-- Stale zombie from a handler that hasn't run cleanup yet; drop
			-- regardless of whose it was so a legitimate reconnect isn't
			-- blocked by a dead foreign slot.
			sessions[session_id] = nil
		elseif existing.user_id ~= principal.user_id then
			-- Different-user means a collision on the id function; refuse to
			-- overwrite another user's live socket.
			warn(
				`session id collision; refusing overwrite: existing.user_id={existing.user_id}, new.user_id={principal.user_id}`
			)
			return
		else
			pcall(function()
				existing.sender:close()
			end)
		end
	end
	sessions[session_id] = entry

	pcall(function()
		sender:send(websocket.encode("ForceKeepAlive", JELLYFIN_WS_KEEPALIVE_SECONDS, nil))
	end)

	-- Public WS routes skip the driver's built-in reauth, so the plugin
	-- owns revocation. Outer xpcall ensures a throw can't silently disable
	-- enforcement for the rest of the connection.
	local reauth_thread = task.spawn(function()
		xpcall(function()
			local consecutive_errors = 0
			while not sender:is_closed() do
				task.wait(JELLYFIN_WS_REAUTH_INTERVAL_SECONDS)
				if sender:is_closed() then
					break
				end
				local ok, result = pcall(auth.resolve_auth, token)
				if ok then
					if result == nil then
						sender:close()
						break
					end
					local refreshed = result :: ResolvedAuth
					if refreshed.principal.user_id ~= principal.user_id then
						sender:close()
						break
					end
					entry.is_admin = session.is_admin(refreshed.principal)
					entry.info.UserName = refreshed.principal.username
					consecutive_errors = 0
				else
					consecutive_errors += 1
					if consecutive_errors >= JELLYFIN_WS_REAUTH_MAX_ERRORS then
						warn(`re-auth failed repeatedly, closing (user_id={principal.user_id}): {tostring(result)}`)
						sender:close()
						break
					end
				end
			end
		end, function(err)
			-- Can't safely restart without risking runaway-loop exceptions,
			-- so close the socket and surface the failure.
			warn(`re-auth thread crashed, closing (user_id={principal.user_id}): {tostring(err)}`)
			pcall(function()
				sender:close()
			end)
		end)
	end)

	-- Keep cleanup reachable on handler throw.
	xpcall(function()
		while true do
			local frame = reader:recv()
			if frame == nil then
				break
			end
			local envelope = websocket.decode(frame)
			if not envelope then
				continue
			end
			entry.last_activity = session.current_iso_timestamp()
			local mt = envelope.MessageType
			if mt == "KeepAlive" then
				pcall(function()
					sender:send(websocket.encode("KeepAlive", JELLYFIN_WS_KEEPALIVE_SECONDS, envelope.MessageId))
				end)
			elseif mt == "SessionsStart" then
				entry.subscribed_sessions = true
			elseif mt == "SessionsStop" then
				entry.subscribed_sessions = false
			-- Malformed branches bump the timer too so a garbage flood can't
			-- amplify the log itself — one diagnostic per window.
			elseif mt == "PlaybackStart" then
				if session.handle_ws_playback_event(resolved_auth, envelope.Data, "started", identity.device_id) then
					entry.last_playback_event_s = os.time()
				else
					warn(`discarded malformed PlaybackStart (user_id={principal.user_id})`)
					entry.last_playback_event_s = os.time()
				end
			elseif mt == "PlaybackProgress" then
				local now_s = os.time()
				local last_s = entry.last_playback_event_s
				if last_s and now_s - last_s < JELLYFIN_WS_PROGRESS_MIN_INTERVAL_SECONDS then
					continue
				end
				if session.handle_ws_playback_event(resolved_auth, envelope.Data, "progress", identity.device_id) then
					entry.last_playback_event_s = now_s
				else
					warn(`discarded malformed PlaybackProgress (user_id={principal.user_id})`)
					entry.last_playback_event_s = now_s
				end
			elseif mt == "PlaybackStopped" then
				if session.handle_ws_playback_event(resolved_auth, envelope.Data, "stopped", identity.device_id) then
					entry.last_playback_event_s = os.time()
				else
					warn(`discarded malformed PlaybackStopped (user_id={principal.user_id})`)
					entry.last_playback_event_s = os.time()
				end
			end
		end
	end, function(err)
		warn(`socket handler error (user_id={principal.user_id}): {tostring(err)}`)
	end)

	task.cancel(reauth_thread)

	-- A concurrent reconnect may have replaced our slot; only clear it if
	-- it still points at our sender.
	if sessions[session_id] and sessions[session_id].sender == sender then
		syncplay.disconnect_entry(entry)
		sessions[session_id] = nil
	end
end, "public")

local function public_system_info<T>(extra: T): T & PublicSystemInfo
	local server_info = server.info()
	local public_address: string? = if server_info.published_url then `{server_info.published_url}{P}` else nil
	local info: T & PublicSystemInfo = extra
	info.ServerName = "Lyra"
	info.Version = SERVER_VERSION
	info.ProductName = "Lyra"
	info.Id = SERVER_ID
	info.LocalAddress = `http://{server_info.hostname}:{server_info.port}{P}`
	info.PublicAddress = public_address
	info.StartupWizardCompleted = true
	return info
end

local AUTH_PROVIDER_ID = "Lyra"

local function find_public_user_by_id(user_id: number): PublicUser?
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

local function root_folder_json(user_id: number): JsonObject
	local views = catalog.to_music_view_items()
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
	local options = catalog.with_user_query_options(catalog.coerce_items_query(nil), principal.user_id)
	options.include_item_types = { include_item_type }
	options.limit = 0
	options.is_favorite = is_favorite

	local ok, _items, total_count = pcall(catalog.list_items, options)
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

	local outcome = auth.login(credentials.username, credentials.password)
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

	return api.response.json(200, session.to_authentication_result(login_result))
end, "public", true)
api.get(P .. "/Users/Public", function(): ApiResponse
	return api.response.json(200, {})
end, "public", true)
api.get(P .. "/Users", function(ctx: ApiContext): ApiResponse
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	if not session.is_admin(principal) then
		return api.response.json(200, { session.to_jellyfin_principal_user(principal) })
	end

	local result: { JsonValue } = {}
	for _, user in users.list() do
		table.insert(
			result,
			session.to_jellyfin_user({
				user_id = user.user_id,
				username = user.username,
			})
		)
	end

	return api.response.json(200, result)
end, nil, true)
api.get(P .. "/Users/{id}", function(ctx: ApiContext): ApiResponse
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local params = if ctx then ctx.params else nil
	local raw_user_id = if params then params.id else nil
	local user_id = item_ids.decode_jellyfin_item_id(raw_user_id)
	if not user_id then
		return responses.not_found("user not found")
	end
	if user_id ~= principal.user_id and not session.is_admin(principal) then
		return session.unauthorized()
	end
	if user_id == principal.user_id then
		return api.response.json(200, session.to_jellyfin_principal_user(principal))
	end

	local user = find_public_user_by_id(user_id)
	if not user then
		return responses.not_found("user not found")
	end
	return api.response.json(200, session.to_jellyfin_user(user))
end, nil, true)
api.get(P .. "/QuickConnect/Enabled", function(): ApiResponse
	return api.response.json(200, false)
end, "public", true)
api.post(P .. "/Users/ForgotPassword", function(): ApiResponse
	return api.response.json(200, {
		Action = "ContactAdmin",
		PinFile = nil,
		PinExpirationDate = nil,
	})
end, "public", true)
api.post(P .. "/Users/ForgotPassword/Pin", function(): ApiResponse
	return api.response.json(200, {
		Success = false,
		UsersReset = {},
	})
end, "public", true)
api.post(P .. "/Sessions/Logout", function(ctx: ApiContext)
	local resolved_auth, token = session.require_jellyfin_auth(ctx)
	if not resolved_auth or not token then
		return session.unauthorized()
	end

	auth.logout_session(token)

	return api.response.empty(204)
end, nil, true)
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
end, nil, true)
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
end, nil, true)
api.get(P .. "/System/Info/Public", function(): ApiResponse
	return api.response.json(200, public_system_info({}))
end, "public", true)
api.get(P .. "/System/Info", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	return api.response.json(
		200,
		public_system_info({
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
end, nil, true)
api.get(P .. "/System/Ping", function(): ApiResponse
	return api.response.json(200, "Lyra")
end, "public", true)
api.post(P .. "/System/Ping", function(): ApiResponse
	return api.response.json(200, "Lyra")
end, "public", true)
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
end, nil, true)
local BITRATE_TEST_DEFAULT_SIZE = 102400
local BITRATE_TEST_MIN_SIZE = 1
local BITRATE_TEST_MAX_SIZE = 100000000
local BITRATE_TEST_CHUNK = string.rep("0", 65536)

local function parse_bitrate_test_size(ctx: ApiContext): (number?, ApiResponse?)
	local query = if ctx and ctx.request then ctx.request.query else nil
	local raw_size = requests.parse_query_string(query, { "Size", "size" }, nil)
	if raw_size == nil then
		return BITRATE_TEST_DEFAULT_SIZE, nil
	end

	local size = tonumber(requests.trim(raw_size) or "")
	if type(size) ~= "number" or size ~= size or math.floor(size) ~= size then
		return nil,
			responses.bad_request(
				"The requested size must be greater than or equal to 1 and less than or equal to 100000000"
			)
	end

	local parsed = size :: number
	if parsed < BITRATE_TEST_MIN_SIZE or parsed > BITRATE_TEST_MAX_SIZE then
		return nil,
			responses.bad_request(
				"The requested size must be greater than or equal to 1 and less than or equal to 100000000"
			)
	end

	return parsed, nil
end

local function build_bitrate_test_body(size: number): string
	if size <= #BITRATE_TEST_CHUNK then
		return string.sub(BITRATE_TEST_CHUNK, 1, size)
	end

	local remaining = size
	local chunks = {}
	while remaining > #BITRATE_TEST_CHUNK do
		table.insert(chunks, BITRATE_TEST_CHUNK)
		remaining -= #BITRATE_TEST_CHUNK
	end
	if remaining > 0 then
		table.insert(chunks, string.sub(BITRATE_TEST_CHUNK, 1, remaining))
	end

	return table.concat(chunks)
end

api.get(P .. "/Playback/BitrateTest", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local size, size_err = parse_bitrate_test_size(ctx)
	if size_err then
		return size_err
	end

	return api.response.bytes(200, build_bitrate_test_body(size :: number))
end, nil, true)
api.get(P .. "/Library/VirtualFolders", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local ok, libs = pcall(libraries.list, nil)
	if not ok or not libs then
		return api.response.json(200, {})
	end

	local folders: { JsonValue } = {}
	for _, lib in libs do
		local hex_id = item_ids.nanoid_to_hex(lib.id)
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
end, nil, true)
api.get(P .. "/Library/MediaFolders", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local views = catalog.to_music_view_items()
	return api.response.json(200, catalog.build_item_query_result(views, #views, 0))
end, nil, true)
api.get(P .. "/Channels", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	return api.response.json(200, empty_query_result())
end, nil, true)
api.get(P .. "/Devices", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	return api.response.json(200, {
		Items = {},
		TotalRecordCount = 0,
	})
end, nil, true)
api.get(P .. "/Users/Me", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	return api.response.json(200, session.to_jellyfin_principal_user(principal))
end, nil, true)
local function respond_with_user_views(ctx: ApiContext): ApiResponse
	local query = if ctx and ctx.request then ctx.request.query else nil
	local options = catalog.coerce_items_query(query)
	local views = catalog.to_music_view_items()
	views = catalog.apply_search_filter(views, options.search_term)
	catalog.apply_sort(views, options)
	local items, total_count, start_index = results.paginate_items(views, options.start_index, options.limit)
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

	return respond_with_user_views(ctx)
end, nil, true)
api.get(P .. "/UserViews", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	return respond_with_user_views(ctx)
end, nil, true)
api.get(P .. "/UserViews/GroupingOptions", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	return api.response.json(200, {})
end, nil, true)
api.get(P .. "/Users/{userId}/GroupingOptions", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	return api.response.json(200, {})
end, nil, true)
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

api.get(P .. "/Items/Root", handle_root_folder_route, nil, true)
api.get(P .. "/Users/{userId}/Items/Root", handle_root_folder_route, nil, true)
local function respond_with_items_listing(ctx: ApiContext, principal: session.Principal): ApiResponse
	local query = if ctx and ctx.request then ctx.request.query else nil
	local options = catalog.with_user_query_options(catalog.coerce_items_query(query), principal.user_id)

	-- Jellyfin returns the user root's direct children (library views) when ParentId/Ids are absent and Recursive isn't true.
	if
		(options.parent_id == nil or constants.is_root_item_id(options.parent_id))
		and #options.ids == 0
		and options.recursive == false
	then
		local views = catalog.to_music_view_items()
		views = catalog.apply_search_filter(views, options.search_term)
		catalog.apply_sort(views, options)
		local items, total_count, start_index = results.paginate_items(views, options.start_index, options.limit)
		return api.response.json(200, catalog.build_item_query_result(items, total_count, start_index))
	end

	local items, total_count, start_index = catalog.list_items(options)
	return api.response.json(200, catalog.build_item_query_result(items, total_count, start_index))
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
end, nil, true)
api.get(P .. "/Items", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	return respond_with_items_listing(ctx, principal)
end, nil, true)
local function handle_similar_items_route(ctx: ApiContext): ApiResponse
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

	local query = if ctx and ctx.request then ctx.request.query else nil
	local options = catalog.with_user_query_options(catalog.coerce_items_query(query), principal.user_id)
	local items, total_count, start_index, found = catalog.list_similar_items(item_id, options)
	if not found then
		return responses.not_found("item not found")
	end

	return api.response.json(200, catalog.build_item_query_result(items, total_count, start_index))
end

api.get(P .. "/Artists/{itemId}/Similar", handle_similar_items_route, nil, true)
api.get(P .. "/Items/{itemId}/Similar", handle_similar_items_route, nil, true)
api.get(P .. "/Albums/{itemId}/Similar", handle_similar_items_route, nil, true)
local function handle_instant_mix_route(ctx: ApiContext): ApiResponse
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

	local query = if ctx and ctx.request then ctx.request.query else nil
	local options = catalog.with_user_query_options(catalog.coerce_items_query(query), principal.user_id)
	local items, total_count, start_index, found = catalog.list_instant_mix_audio(item_id, options)
	if not found then
		return responses.not_found("item not found")
	end

	return api.response.json(200, catalog.build_item_query_result(items, total_count, start_index))
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
	local options = catalog.with_user_query_options(catalog.coerce_items_query(query), principal.user_id)
	local items, total_count, start_index, found = catalog.list_instant_mix_audio_from_genre_name(name, options)
	if not found then
		return responses.not_found("genre not found")
	end

	return api.response.json(200, catalog.build_item_query_result(items, total_count, start_index))
end

api.get(P .. "/Items/{itemId}/InstantMix", handle_instant_mix_route, nil, true)
api.get(P .. "/Songs/{itemId}/InstantMix", handle_instant_mix_route, nil, true)
api.get(P .. "/Albums/{itemId}/InstantMix", handle_instant_mix_route, nil, true)
api.get(P .. "/Artists/{itemId}/InstantMix", handle_instant_mix_route, nil, true)
api.get(P .. "/Playlists/{itemId}/InstantMix", handle_instant_mix_route, nil, true)
api.get(P .. "/MusicGenres/{name}/InstantMix", handle_instant_mix_genre_name_route, nil, true)
local function respond_with_latest_items(ctx: ApiContext, principal: session.Principal): ApiResponse
	local query = if ctx and ctx.request then ctx.request.query else nil
	local options = catalog.with_user_query_options(catalog.coerce_items_query(query), principal.user_id)
	local items = catalog.list_latest_items(options)
	return api.response.json(200, catalog.items_to_json(items))
end

api.get(P .. "/Users/{userId}/Items/Latest", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	return respond_with_latest_items(ctx, principal)
end, nil, true)
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

	local item = catalog.find_item_by_id(item_ids.decode_jellyfin_item_id(raw_item_id), principal.user_id)
	if not item then
		return responses.not_found("item not found")
	end

	return api.response.json(200, catalog.item_to_json(item))
end, nil, true)
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

	local item = catalog.find_item_by_id(item_ids.decode_jellyfin_item_id(raw_id), principal.user_id)
	if not item then
		return responses.not_found("item not found")
	end

	return api.response.json(200, catalog.item_to_json(item))
end, nil, true)

local function handle_item_user_data_route(ctx: ApiContext): ApiResponse
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	if not session.require_matching_user_scope(ctx, principal) then
		return responses.forbidden("user is not allowed to view this item user data")
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

	local item = catalog.find_item_by_id(item_id, principal.user_id)
	if not item then
		return responses.not_found("item not found")
	end

	local item_hex_id = item_ids.encode_jellyfin_item_id(item_id)
	local item_name = requests.trim(item.Name)
	local item_type = requests.trim(item.Type)
	local key = item_hex_id
	if item_type == "MusicArtist" then
		key = `Artist-{item_name or ""}`
	elseif item_type == "MusicAlbum" then
		key = `{requests.trim(item.AlbumArtist) or ""}-{item_name or ""}`
	elseif item_type == "Audio" then
		key = `{item.ParentIndexNumber or 0}-{item.IndexNumber or 0}{item_name or ""}`
	elseif item_type == "Playlist" then
		key = `Playlist-{item_name or ""}`
	elseif item_type == "MusicGenre" then
		key = `MusicGenre-{item_name or ""}`
	end

	local item_user_data = item.UserData
		or user_data.get_user_data({
			item_id = item_id,
			user_id = principal.user_id,
			item_hex_id = item_hex_id,
			key = key,
			include_favorite = true,
			include_play_count = true,
		})

	return api.response.json(200, user_data.user_data_to_json(item_user_data))
end

api.get(P .. "/UserItems/{itemId}/UserData", handle_item_user_data_route, nil, true)
api.get(P .. "/Users/{userId}/Items/{itemId}/UserData", handle_item_user_data_route, nil, true)

local function handle_favorite_item_route(ctx: ApiContext, should_favorite: boolean)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	local params = if ctx then ctx.params else nil
	local item_id = if params then params.itemId else nil
	if not item_id then
		return responses.bad_request("missing item id")
	end

	local target_id = item_ids.decode_jellyfin_item_id(item_id)
	if target_id == nil then
		return responses.not_found("item not found")
	end

	local item = catalog.find_item_by_id(target_id, principal.user_id)
	if not item then
		return responses.not_found("item not found")
	end

	local update_ok = if should_favorite
		then user_data.add_favorite(principal.user_id, target_id)
		else user_data.remove_favorite(principal.user_id, target_id)
	if not update_ok then
		return responses.bad_request(if should_favorite then "failed to add favorite" else "failed to remove favorite")
	end

	return api.response.json(
		200,
		user_data.user_data_to_json(user_data.get_user_data({
			item_id = target_id,
			user_id = principal.user_id,
			include_favorite = true,
			include_play_count = true,
		}))
	)
end

api.post(P .. "/UserFavoriteItems/{itemId}", function(ctx: ApiContext)
	return handle_favorite_item_route(ctx, true)
end, nil, true)
api.post(P .. "/Users/{userId}/FavoriteItems/{itemId}", function(ctx: ApiContext)
	return handle_favorite_item_route(ctx, true)
end, nil, true)
api.delete(P .. "/UserFavoriteItems/{itemId}", function(ctx: ApiContext)
	return handle_favorite_item_route(ctx, false)
end, nil, true)
api.delete(P .. "/Users/{userId}/FavoriteItems/{itemId}", function(ctx: ApiContext)
	return handle_favorite_item_route(ctx, false)
end, nil, true)
api.get(P .. "/Playlists/{playlistId}/Items", function(ctx: ApiContext)
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

	local query = if ctx and ctx.request then ctx.request.query else nil
	local options = catalog.with_user_query_options(catalog.coerce_items_query(query), principal.user_id)
	local items, total_count, start_index = catalog.list_playlist_track_items(playlist, options)
	return api.response.json(200, catalog.build_item_query_result(items, total_count, start_index))
end, nil, true)
api.post(P .. "/Playlists", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local request_payload, parse_err = session.parse_playlist_request_body(ctx)
	if not request_payload then
		return parse_err or responses.bad_request("invalid playlist request body")
	end
	if not session.ensure_user_scope_matches_principal(request_payload.user_id, principal) then
		return session.unauthorized()
	end

	local playlist_name = request_payload.name or "New Playlist"
	local create_request: PlaylistCreateRequest = {
		user_id = principal.user_id,
		name = playlist_name,
		is_public = if request_payload.is_public ~= nil then request_payload.is_public else false,
		created_at = session.current_unix_seconds(),
		updated_at = session.current_unix_seconds(),
	}

	local ok, playlist_id = pcall(playlists.create, create_request)
	if not ok then
		return responses.bad_request("failed to create playlist")
	end

	local resolved_playlist_id: number = playlist_id :: number
	for _, track_id in request_payload.ids do
		if playback.resolve_track_for_playback(track_id) then
			pcall(playlists.add_track, resolved_playlist_id, track_id)
		end
	end

	local jellyfin_playlist_id = item_ids.encode_jellyfin_item_id(playlist_id)
	if not jellyfin_playlist_id then
		return responses.bad_request("failed to encode playlist id")
	end
	return api.response.json(200, {
		Id = jellyfin_playlist_id,
	})
end, nil, true)
api.post(P .. "/Playlists/{playlistId}/Items", function(ctx: ApiContext)
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

	local can_edit, edit_err = session.ensure_playlist_editable(playlist, principal)
	if not can_edit then
		return edit_err or responses.bad_request("playlist is read-only")
	end

	local query = if ctx and ctx.request then ctx.request.query else nil
	local track_ids = session.parse_playlist_query_track_ids(query)
	if #track_ids == 0 then
		return responses.bad_request("missing ids query parameter")
	end

	for _, track_id in track_ids do
		if playback.resolve_track_for_playback(track_id) then
			local playlist_db_id = playlist.db_id
			if playlist_db_id ~= nil then
				pcall(playlists.add_track, playlist_db_id, track_id)
			end
		end
	end

	return api.response.empty(204)
end, nil, true)
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
	local entry_ids = session.parse_playlist_query_entry_ids(query)
	if #entry_ids == 0 then
		return responses.bad_request("missing entryIds query parameter")
	end

	for _, entry_id in entry_ids do
		pcall(playlists.remove_track, entry_id)
	end

	return api.response.empty(204)
end, nil, true)
api.post(P .. "/Playlists/{playlistId}", function(ctx: ApiContext)
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

	local request_payload, parse_err = session.parse_playlist_request_body(ctx)
	if not request_payload then
		return parse_err or responses.bad_request("invalid playlist request body")
	end
	if not session.ensure_user_scope_matches_principal(request_payload.user_id, principal) then
		return session.unauthorized()
	end
	local playlist_db_id = playlist.db_id
	if playlist_db_id == nil then
		return responses.bad_request("playlist not found")
	end

	local update_request: PlaylistUpdateRequest = {
		playlist_id = playlist_db_id,
		name = request_payload.name,
		is_public = request_payload.is_public,
		updated_at = session.current_unix_seconds(),
	}
	local ok = pcall(playlists.update, update_request)
	if not ok then
		return responses.bad_request("failed to update playlist")
	end

	return api.response.empty(204)
end, nil, true)
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
end, nil, true)
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

	local requester_can_edit = playlist_access.can_edit(playlist, principal.user_id)
	if item_ids.decode_jellyfin_item_id(requested_user_id) ~= principal.user_id and not requester_can_edit then
		return session.unauthorized()
	end

	return api.response.json(
		200,
		session.playlist_user_permission_to_json(
			session.to_playlist_user_permission(playlist, item_ids.decode_jellyfin_item_id(requested_user_id) or 0)
		)
	)
end, nil, true)
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
end, nil, true)
api.get(P .. "/Artists", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local query = if ctx and ctx.request then ctx.request.query else nil
	local options = catalog.with_user_query_options(catalog.coerce_items_query(query), principal.user_id)
	local items, total_count, start_index = catalog.list_artist_items(options)
	return api.response.json(200, catalog.build_item_query_result(items, total_count, start_index))
end, nil, true)
api.get(P .. "/Persons", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local query = if ctx and ctx.request then ctx.request.query else nil
	local options = catalog.with_user_query_options(catalog.coerce_items_query(query), principal.user_id)
	local items, total_count, start_index = catalog.list_person_items(options)
	return api.response.json(200, catalog.build_item_query_result(items, total_count, start_index))
end, nil, true)
api.get(P .. "/Artists/AlbumArtists", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	local query = if ctx and ctx.request then ctx.request.query else nil
	local options = catalog.with_user_query_options(catalog.coerce_items_query(query), principal.user_id)
	local items, total_count, start_index = catalog.list_artist_items(options)
	return api.response.json(200, catalog.build_item_query_result(items, total_count, start_index))
end, nil, true)
local function handle_genres_route(ctx: ApiContext): ApiResponse
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local query = if ctx and ctx.request then ctx.request.query else nil
	local options = catalog.with_user_query_options(catalog.coerce_items_query(query), principal.user_id)
	local items, total_count, start_index = catalog.list_genre_items(options)
	return api.response.json(200, catalog.build_item_query_result(items, total_count, start_index))
end

local function handle_studios_route(ctx: ApiContext): ApiResponse
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local query = if ctx and ctx.request then ctx.request.query else nil
	local options = catalog.with_user_query_options(catalog.coerce_items_query(query), principal.user_id)
	local empty_items = {}
	return api.response.json(200, catalog.build_item_query_result(empty_items, 0, options.start_index))
end

api.get(P .. "/Genres", function(ctx: ApiContext)
	return handle_genres_route(ctx)
end, nil, true)
api.get(P .. "/MusicGenres", function(ctx: ApiContext)
	return handle_genres_route(ctx)
end, nil, true)
api.get(P .. "/Studios", function(ctx: ApiContext)
	return handle_studios_route(ctx)
end, nil, true)
api.get(P .. "/Items/{imageId}/Images/Primary", function(ctx: ApiContext)
	return session.handle_primary_image_request(ctx)
end, nil, true)
api.head(P .. "/Items/{imageId}/Images/Primary", function(ctx: ApiContext)
	return session.handle_primary_image_request(ctx)
end, nil, true)
api.get(P .. "/Persons/{name}/Images/{imageType}", function(ctx: ApiContext)
	return session.handle_person_image_request(ctx)
end, nil, true)
api.head(P .. "/Persons/{name}/Images/{imageType}", function(ctx: ApiContext)
	return session.handle_person_image_request(ctx)
end, nil, true)
api.get(P .. "/Persons/{name}/Images/{imageType}/{imageIndex}", function(ctx: ApiContext)
	return session.handle_person_image_request(ctx)
end, nil, true)
api.head(P .. "/Persons/{name}/Images/{imageType}/{imageIndex}", function(ctx: ApiContext)
	return session.handle_person_image_request(ctx)
end, nil, true)
api.get(P .. "/Artists/{name}/Images/{imageType}/{imageIndex}", function(ctx: ApiContext)
	return session.handle_artist_image_request(ctx)
end, nil, true)
api.head(P .. "/Artists/{name}/Images/{imageType}/{imageIndex}", function(ctx: ApiContext)
	return session.handle_artist_image_request(ctx)
end, nil, true)
-- Merge rule: Full wins on collision, non-Full fills gaps. Without a live
-- WS entry there's nowhere to stash caps; 204 silently so non-WS clients
-- aren't broken.
local function handle_capabilities_post(ctx: ApiContext, body_is_object: boolean): ApiResponse
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local identity = session.resolve_client_identity(ctx)
	if not identity then
		return api.response.empty(204)
	end

	local session_id = session.build_session_id(principal, identity)
	local entry = get_owned_session(session_id, principal)
	if not entry then
		return api.response.empty(204)
	end

	if body_is_object then
		local request = if ctx then ctx.request else nil
		local request_json = if request then request.json else nil
		if request_json ~= nil and type(request_json) ~= "table" then
			return responses.bad_request("invalid capabilities request body")
		end
		local parsed = session.parse_client_capabilities(request_json)
		if parsed then
			entry.capabilities = parsed
			local full_keys: { [string]: boolean } = {}
			for key in parsed do
				full_keys[key] = true
			end
			entry.full_capability_keys = full_keys
		end
	else
		local request = if ctx then ctx.request else nil
		local query = if request then request.query else nil
		local partial = session.parse_client_capabilities_query(query)
		if partial and next(partial) ~= nil then
			local merged: JsonObject = if entry.capabilities then table.clone(entry.capabilities) else {}
			for key, value in partial do
				if not entry.full_capability_keys[key] then
					merged[key] = value
				end
			end
			entry.capabilities = merged
		end
	end

	return api.response.empty(204)
end

api.post(P .. "/Sessions/Capabilities", function(ctx: ApiContext)
	return handle_capabilities_post(ctx, false)
end, nil, true)
api.post(P .. "/Sessions/Capabilities/Full", function(ctx: ApiContext)
	return handle_capabilities_post(ctx, true)
end, nil, true)
local JELLYFIN_COMMAND_MAP: { [string]: string } = {
	play = "Play",
	pause = "Pause",
	unpause = "Unpause",
	stop = "Stop",
	seek = "Seek",
	next_track = "NextTrack",
	previous_track = "PreviousTrack",
	set_volume = "SetVolume",
}

local function to_jellyfin_supported_commands(commands: { string }): { string }
	local result = {}
	for _, cmd in commands do
		local mapped = JELLYFIN_COMMAND_MAP[cmd]
		if mapped then
			table.insert(result, mapped)
		end
	end
	return result
end

type Track = tracks.Track

local function resolve_track_name(track_id: number): string
	local ok, result = pcall(tracks.get_by_ids, { track_id })
	if not ok or not result then
		return ""
	end
	local track: Track? = result[track_id]
	if track then
		return track.track_title
	end
	return ""
end

local function handle_send_command_error(err: unknown): ApiResponse
	if string.find(tostring(err), "not found") then
		return responses.not_found("session not found")
	end
	if string.find(tostring(err), "not authorized") then
		return session.unauthorized()
	end
	return api.response.empty(500)
end

-- Remote-control commands aren't idempotent, so scope mismatch surfaces as
-- 401 and send failure as 404 rather than collapsing to 204.
local function resolve_ws_entry_for_dispatch(
	principal: session.Principal,
	session_id: string?
): (SessionEntry?, ApiResponse?)
	if not session_id then
		return nil, nil
	end
	local entry = sessions[session_id]
	if not entry then
		return nil, nil
	end
	if entry.sender:is_closed() then
		sessions[session_id] = nil
		return nil, nil
	end
	if not session.can_control_user(principal, entry.user_id) then
		warn(`dispatch scope mismatch: principal={principal.user_id}, entry={entry.user_id}, session_id={session_id}`)
		return nil, session.unauthorized()
	end
	return entry, nil
end

local function dispatch_to_ws(principal: session.Principal, session_id: string?, envelope_text: string): ApiResponse?
	local entry, resolve_err = resolve_ws_entry_for_dispatch(principal, session_id)
	if resolve_err then
		return resolve_err
	end
	if not entry then
		return nil
	end
	local target_session_id = session_id :: string
	local ok = pcall(function()
		entry.sender:send(envelope_text)
	end)
	if not ok then
		if sessions[target_session_id] == entry then
			sessions[target_session_id] = nil
		end
		return responses.not_found("session not found")
	end
	return api.response.empty(204)
end

local function build_now_playing(playback: playback_sessions.PlaybackInfo): (JsonObject, JsonObject)
	local track_db_id = ids.get_db_id(playback.track_public_id)
	local now_playing: JsonObject = {
		Id = item_ids.encode_jellyfin_item_id(playback.track_public_id),
		Name = if track_db_id then resolve_track_name(track_db_id) else "",
		Type = "Audio",
	}
	if playback.duration_ms then
		now_playing.RunTimeTicks = playback.duration_ms * 10000
	end
	local play_state: JsonObject = {
		PositionTicks = playback.position_ms * 10000,
		IsPaused = playback.state == "paused",
		CanSeek = true,
	}
	return now_playing, play_state
end

local function build_sessions_dto_list(
	user_id: number,
	username: string,
	connections: { playback_sessions.ConnectionInfo }
): { JsonValue }
	local conn_by_session_key: { [string]: playback_sessions.ConnectionInfo } = {}
	for _, conn in connections do
		conn_by_session_key[conn.session_key] = conn
	end

	local jellyfin_user_id = item_ids.encode_jellyfin_item_id(user_id)
	local result: { JsonValue } = {}
	local seen_session_keys: { [string]: boolean } = {}

	for _, entry in sessions do
		-- Skip foreign users and zombies (closed sender pre-cleanup).
		if entry.user_id ~= user_id or entry.sender:is_closed() then
			continue
		end
		local join_key = connection_join_key(entry)
		seen_session_keys[join_key] = true
		local dto: JsonObject = table.clone(entry.info)
		dto.LastActivityDate = entry.last_activity
		dto.LastPlaybackCheckIn = entry.last_activity
		if entry.capabilities then
			dto.Capabilities = entry.capabilities
		end
		local conn = conn_by_session_key[join_key]
		local conn_playback = if conn then conn.playback else nil
		if conn_playback then
			local now_playing, play_state = build_now_playing(conn_playback)
			dto.NowPlayingItem = now_playing
			dto.PlayState = play_state
		end
		table.insert(result, dto)
	end

	-- Non-WS clients (playback-only) still need to surface here.
	for _, conn in connections do
		if seen_session_keys[conn.session_key] then
			continue
		end
		local playable_types: { JsonValue } = { "Audio" }
		local legacy: JsonObject = {
			Id = conn.token,
			UserId = jellyfin_user_id,
			UserName = username,
			DeviceId = conn.session_key,
			DeviceName = conn.session_key,
			Client = "Lyra",
			ServerId = SERVER_ID,
			IsActive = true,
			SupportsRemoteControl = #conn.supported_commands > 0,
			PlayableMediaTypes = playable_types,
		}
		local conn_playback = conn.playback
		if conn_playback then
			local now_playing, play_state = build_now_playing(conn_playback)
			legacy.NowPlayingItem = now_playing
			legacy.PlayState = play_state
		end
		local supported_commands = to_jellyfin_supported_commands(conn.supported_commands)
		local cmd_list: { JsonValue } = {}
		for _, cmd in supported_commands do
			table.insert(cmd_list, cmd)
		end
		legacy.Capabilities = {
			SupportsMediaControl = #supported_commands > 0,
			SupportedCommands = cmd_list,
			PlayableMediaTypes = playable_types,
		}
		table.insert(result, legacy)
	end

	return result
end

local function list_all_user_identities(fallback: UserIdentity?): { UserIdentity }
	local result: { UserIdentity } = {}
	local seen: { [number]: boolean } = {}
	for _, user: PublicUser in users.list() do
		if not seen[user.user_id] then
			seen[user.user_id] = true
			table.insert(result, {
				user_id = user.user_id,
				username = user.username,
			})
		end
	end
	if fallback and not seen[fallback.user_id] then
		table.insert(result, fallback)
	end
	return result
end

local function visible_user_identities(principal: session.Principal): { UserIdentity }
	if session.is_admin(principal) then
		return list_all_user_identities({
			user_id = principal.user_id,
			username = principal.username,
		})
	end
	return {
		{
			user_id = principal.user_id,
			username = principal.username,
		},
	}
end

local function session_supports_remote_control(dto: JsonValue): boolean
	if type(dto) ~= "table" then
		return false
	end
	return (dto :: JsonObject).SupportsRemoteControl == true
end

local function append_sessions_for_user(
	result: { JsonValue },
	user_id: number,
	username: string,
	require_remote_control: boolean
)
	local connections = playback_sessions.list_connections(user_id)
	for _, dto in build_sessions_dto_list(user_id, username, connections) do
		if not require_remote_control or session_supports_remote_control(dto) then
			table.insert(result, dto)
		end
	end
end

local function list_all_sessions(fallback: UserIdentity?, require_remote_control: boolean): { JsonValue }
	local result: { JsonValue } = {}
	for _, user in list_all_user_identities(fallback) do
		append_sessions_for_user(result, user.user_id, user.username, require_remote_control)
	end
	return result
end

local function list_visible_sessions(principal: session.Principal, controllable_user_id: number?): { JsonValue }
	if controllable_user_id and not session.can_control_user(principal, controllable_user_id) then
		return {}
	end

	local require_remote_control = controllable_user_id ~= nil
	if session.is_admin(principal) then
		return list_all_sessions({
			user_id = principal.user_id,
			username = principal.username,
		}, require_remote_control)
	end

	local result: { JsonValue } = {}
	append_sessions_for_user(result, principal.user_id, principal.username, require_remote_control)
	return result
end

local function find_native_connection_for_user(
	user_id: number,
	session_id: string,
	session_key: string?
): NativeConnectionTarget?
	for _, conn in playback_sessions.list_connections(user_id) do
		if conn.token == session_id or (session_key ~= nil and conn.session_key == session_key) then
			return {
				user_id = user_id,
				connection = conn,
			}
		end
	end
	return nil
end

local function resolve_native_target(
	principal: session.Principal,
	session_id: string,
	ws_entry: SessionEntry?
): (NativeConnectionTarget?, ApiResponse?)
	if ws_entry then
		if not session.can_control_user(principal, ws_entry.user_id) then
			return nil, session.unauthorized()
		end
		return find_native_connection_for_user(ws_entry.user_id, session_id, connection_join_key(ws_entry)), nil
	end

	for _, user in visible_user_identities(principal) do
		local target = find_native_connection_for_user(user.user_id, session_id, nil)
		if target then
			return target, nil
		end
	end
	return nil, nil
end

local function dispatch_to_native(
	principal: session.Principal,
	session_id: string,
	action: string,
	position_ms: number?,
	level: number?
): ApiResponse
	local target, target_err = resolve_native_target(principal, session_id, nil)
	if target_err then
		return target_err
	end
	if not target then
		return responses.not_found("session not found")
	end

	local cmd_request: playback_sessions.SendCommandRequest = {
		user_id = target.user_id,
		target_token = target.connection.token,
		action = action,
	}
	if position_ms ~= nil then
		cmd_request.position_ms = position_ms
	end
	if level ~= nil then
		cmd_request.level = level
	end
	local ok, err = pcall(playback_sessions.send_command, cmd_request)
	if not ok then
		return handle_send_command_error(err)
	end
	return api.response.empty(204)
end

api.get(P .. "/Sessions", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local request = if ctx then ctx.request else nil
	local query = if request then request.query else nil
	local controllable = requests.parse_query_string(query, { "controllableByUserId", "ControllableByUserId" }, nil)
	local controllable_user_id = if controllable then item_ids.decode_jellyfin_item_id(controllable) else nil
	if controllable and not controllable_user_id then
		return api.response.json(200, {})
	end

	return api.response.json(200, list_visible_sessions(principal, controllable_user_id))
end, nil, true)

syncplay.register(sessions, get_owned_session)
-- Jellyfin clients handle Pause/Unpause, not toggle — resolve PlayPause
-- against last-known state. Nil means nothing to toggle; caller should 204.
local function resolve_play_pause(principal: session.Principal, session_id: string): (string?, ApiResponse?)
	local entry, entry_err = resolve_ws_entry_for_dispatch(principal, session_id)
	if entry_err then
		return nil, entry_err
	end
	local target, target_err = resolve_native_target(principal, session_id, entry)
	if target_err then
		return nil, target_err
	end
	local playback_info = if target then target.connection.playback else nil
	if playback_info then
		local resolved = if playback_info.state == "paused" then "Unpause" else "Pause"
		return resolved, nil
	end

	return nil, nil
end

api.post(P .. "/Sessions/{sessionId}/Playing/{command}", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local params = if ctx then ctx.params else nil
	local session_id = if params then params.sessionId else nil
	local command = if params then params.command else nil
	if not session_id or not command then
		return responses.bad_request("missing session id or command")
	end

	local request = if ctx then ctx.request else nil
	local query = if request then request.query else nil

	local resolved_command = command
	local seek_ticks: number? = nil
	if command == "PlayPause" then
		local resolved, resolve_err = resolve_play_pause(principal, session_id)
		if resolve_err then
			return resolve_err
		end
		if not resolved then
			-- Nothing to toggle; avoid sending a spurious Pause on a stopped player.
			return api.response.empty(204)
		end
		resolved_command = resolved
	elseif command == "Seek" then
		local raw_ticks = requests.parse_query_string(query, { "seekPositionTicks", "SeekPositionTicks" }, nil)
		seek_ticks = if raw_ticks then tonumber(raw_ticks) else nil
		if not seek_ticks then
			return responses.bad_request("seekPositionTicks is required for Seek")
		end
	end

	local data: JsonObject = { Command = resolved_command }
	if seek_ticks then
		data.SeekPositionTicks = seek_ticks
	end
	local ws_response = dispatch_to_ws(principal, session_id, websocket.encode("Playstate", data))
	if ws_response then
		return ws_response
	end

	local native_action_map: { [string]: string } = {
		Pause = "pause",
		Unpause = "unpause",
		Stop = "stop",
		NextTrack = "next_track",
		PreviousTrack = "previous_track",
		Seek = "seek",
	}
	local action = native_action_map[resolved_command]
	if not action then
		return api.response.empty(204)
	end

	local position_ms: number? = nil
	if seek_ticks then
		position_ms = math.floor(seek_ticks / 10000)
	end
	return dispatch_to_native(principal, session_id, action, position_ms, nil)
end, nil, true)
api.post(P .. "/Sessions/{sessionId}/Command/{command}", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local params = if ctx then ctx.params else nil
	local session_id = if params then params.sessionId else nil
	local command = if params then params.command else nil
	if not session_id or not command then
		return responses.bad_request("missing session id or command")
	end

	-- Route-param variant has no body, so Arguments is always empty here;
	-- parameterised commands like SetVolume need POST /Sessions/{id}/Command.
	local ws_response = dispatch_to_ws(
		principal,
		session_id,
		websocket.encode("GeneralCommand", {
			Name = command,
			Arguments = {} :: JsonObject,
			ControllingUserId = item_ids.encode_jellyfin_item_id(principal.user_id),
		})
	)
	if ws_response then
		return ws_response
	end
	return api.response.empty(204)
end, nil, true)
api.post(P .. "/Sessions/{sessionId}/Command", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local params = if ctx then ctx.params else nil
	local session_id = if params then params.sessionId else nil
	if not session_id then
		return responses.bad_request("missing session id")
	end

	local request = if ctx then ctx.request else nil
	local request_json = if request then request.json else nil
	if type(request_json) ~= "table" then
		return responses.bad_request("invalid command request body")
	end
	local body = request_json :: JsonObject

	local command_name = body.Name
	if type(command_name) ~= "string" then
		return responses.bad_request("missing command Name")
	end
	local arguments: JsonObject = {}
	local raw_arguments = body.Arguments
	if type(raw_arguments) == "table" then
		arguments = raw_arguments :: JsonObject
	end

	local ws_response = dispatch_to_ws(
		principal,
		session_id,
		websocket.encode("GeneralCommand", {
			Name = command_name,
			Arguments = arguments,
			ControllingUserId = item_ids.encode_jellyfin_item_id(principal.user_id),
		})
	)
	if ws_response then
		return ws_response
	end

	if command_name == "SetVolume" then
		local volume = tonumber(arguments.Volume)
		if volume then
			return dispatch_to_native(principal, session_id, "set_volume", nil, volume / 100)
		end
	end
	return api.response.empty(204)
end, nil, true)
api.post(P .. "/Sessions/{sessionId}/Playing", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local params = if ctx then ctx.params else nil
	local session_id = if params then params.sessionId else nil
	if not session_id then
		return responses.bad_request("missing session id")
	end

	local request = if ctx then ctx.request else nil
	local query = if request then request.query else nil
	local request_json = if request then request.json else nil
	local body: JsonObject = if type(request_json) == "table" then request_json :: JsonObject else {}

	local play_command = requests.parse_query_string(query, { "playCommand", "PlayCommand" }, nil)
		or (if type(body.PlayCommand) == "string" then body.PlayCommand :: string else "PlayNow")

	local raw_item_ids: JsonValue = body.ItemIds
	local item_ids: { JsonValue } = {}
	if type(raw_item_ids) == "table" then
		for _, value: JsonValue in raw_item_ids :: { JsonValue } do
			if type(value) == "string" then
				table.insert(item_ids, value)
			end
		end
	else
		local csv = requests.parse_query_string(query, { "itemIds", "ItemIds" }, nil)
		if csv then
			for _, part in string.split(csv, ",") do
				local trimmed = requests.trim(part)
				if trimmed then
					table.insert(item_ids, trimmed)
				end
			end
		end
	end
	if #item_ids == 0 then
		return responses.bad_request("missing ItemIds")
	end

	local data: JsonObject = {
		PlayCommand = play_command,
		ItemIds = item_ids,
	}
	local start_ticks = requests.parse_query_int(query, { "startPositionTicks", "StartPositionTicks" }, nil, 0, nil)
		or (if type(body.StartPositionTicks) == "number" then body.StartPositionTicks :: number else nil)
	if start_ticks then
		data.StartPositionTicks = start_ticks
	end
	local start_index = requests.parse_query_int(query, { "startIndex", "StartIndex" }, nil, 0, nil)
		or (if type(body.StartIndex) == "number" then body.StartIndex :: number else nil)
	if start_index then
		data.StartIndex = start_index
	end

	local ws_response = dispatch_to_ws(principal, session_id, websocket.encode("Play", data))
	if ws_response then
		return ws_response
	end
	return responses.not_found("session not found")
end, nil, true)
api.post(P .. "/Sessions/Playing", function(ctx: ApiContext)
	return session.handle_playback_report_route(ctx, "started")
end, nil, true)
api.post(P .. "/Sessions/Playing/Progress", function(ctx: ApiContext)
	return session.handle_playback_report_route(ctx, "progress")
end, nil, true)
api.post(P .. "/Sessions/Playing/Stopped", function(ctx: ApiContext)
	return session.handle_playback_report_route(ctx, "stopped")
end, nil, true)
api.get(P .. "/Items/{id}/PlaybackInfo", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	local item_id, track, resolve_err = playback.resolve_playback_info_item(ctx)
	if resolve_err then
		return resolve_err or responses.bad_request("invalid playback item")
	end

	local request = if ctx then ctx.request else nil
	local play_session_id = playback.resolve_requested_play_session_id(request, nil)
	local response = playback.build_playback_info(item_id, track, play_session_id, nil, nil)
	return api.response.json(200, response)
end, nil, true)
api.get(P .. "/Items/{itemId}/File", function(ctx: ApiContext)
	return session.handle_item_file_stream_request(ctx)
end, nil, true)
api.get(P .. "/Items/{itemId}/Download", function(ctx: ApiContext)
	return session.handle_item_file_stream_request(ctx)
end, nil, true)

-- Returns (value, err) so the handler can distinguish "absent" (value=nil, err=nil)
-- from "present but malformed/negative" (value=nil, err=string). Zero is returned
-- as-is so the server-side policy sees the value and can reject it.
audio_routes.register()
api.post(P .. "/Items/{id}/PlaybackInfo", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local request = if ctx then ctx.request else nil
	local request_json = if request then request.json else nil
	if request_json ~= nil and type(request_json) ~= "table" then
		return responses.bad_request("invalid playback info request body")
	end
	local request_object = if type(request_json) == "table" then request_json :: JsonObject else nil

	local requested_user_id = if request_object then request_object.UserId else nil
	if
		type(requested_user_id) == "string"
		and item_ids.decode_jellyfin_item_id(requested_user_id) ~= principal.user_id
	then
		return session.unauthorized()
	end

	local item_id, track, resolve_err = playback.resolve_playback_info_item(ctx)
	if resolve_err then
		return resolve_err or responses.bad_request("invalid playback item")
	end

	local playback_request = if request_object then request_object :: PlaybackPayload else nil
	local play_session_id = playback.resolve_requested_play_session_id(request, playback_request)
	local requested_media_source_id = playback.resolve_requested_media_source_id(request, request_object)
	local response =
		playback.build_playback_info(item_id, track, play_session_id, request_object, requested_media_source_id)
	return api.response.json(200, response)
end, nil, true)

api.get(P .. "/Items/Latest", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end
	return respond_with_latest_items(ctx, principal)
end, nil, true)
api.get(P .. "/Users/{userId}/Items/Resume", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	return api.response.json(200, empty_query_result())
end, nil, true)
api.get(P .. "/UserItems/Resume", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	return api.response.json(200, empty_query_result())
end, nil, true)
api.get(P .. "/Shows/NextUp", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	return api.response.json(200, empty_query_result())
end, nil, true)
api.get(P .. "/Items/Suggestions", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	return api.response.json(200, empty_query_result())
end, nil, true)
api.get(P .. "/Users/{userId}/Suggestions", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	return api.response.json(200, empty_query_result())
end, nil, true)
api.get(P .. "/Items/Counts", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	local query = if ctx and ctx.request then ctx.request.query else nil
	local options = catalog.coerce_items_query(query)
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
end, nil, true)
api.get(P .. "/Items/Filters", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	return api.response.json(200, {
		Genres = {},
		Tags = {},
		OfficialRatings = {},
		Years = {},
	})
end, nil, true)
api.get(P .. "/Items/Filters2", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	if not session.require_matching_user_scope(ctx, principal) then
		return session.unauthorized()
	end

	return api.response.json(200, {
		Genres = {},
		Tags = {},
	})
end, nil, true)
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
	local item_id = item_ids.decode_jellyfin_item_id(raw_id)
	if not item_id then
		return responses.not_found("item not found")
	end

	local chain, item_found = catalog.ancestors_chain(item_id, principal.user_id)
	if not item_found then
		return responses.not_found("item not found")
	end

	local result: { api.JsonValue } = {}
	for _, parent in chain do
		table.insert(result, catalog.item_to_json(parent))
	end
	return api.response.json(200, result)
end, nil, true)
api.get(P .. "/Items/{itemId}/Intros", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	return api.response.json(200, empty_query_result())
end, nil, true)
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

api.get(P .. "/Items/{itemId}/LocalTrailers", handle_empty_item_array_route, nil, true)
api.get(P .. "/Users/{userId}/Items/{itemId}/LocalTrailers", handle_empty_item_array_route, nil, true)
api.get(P .. "/Items/{itemId}/SpecialFeatures", handle_empty_item_array_route, nil, true)
api.get(P .. "/Users/{userId}/Items/{itemId}/SpecialFeatures", handle_empty_item_array_route, nil, true)
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

	local found = catalog.find_artist_by_name(name, principal.user_id)
	if not found then
		return responses.not_found("artist not found")
	end
	return api.response.json(200, catalog.item_to_json(found))
end, nil, true)
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

	local found = catalog.find_person_by_name(name, principal.user_id)
	if not found then
		return responses.not_found("person not found")
	end
	return api.response.json(200, catalog.item_to_json(found))
end, nil, true)
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

	local found = catalog.find_genre_by_name(name, principal.user_id)
	if not found then
		return responses.not_found("genre not found")
	end
	return api.response.json(200, catalog.item_to_json(found))
end

api.get(P .. "/Genres/{genreName}", handle_genre_by_name_route, nil, true)
api.get(P .. "/MusicGenres/{genreName}", handle_genre_by_name_route, nil, true)
api.get(P .. "/GetUtcTime", function(): ApiResponse
	return statics.utc_time_response()
end, "public", true)
api.get(P .. "/Localization/Cultures", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	return statics.cultures_response()
end, nil, true)
api.get(P .. "/Localization/Countries", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	return statics.countries_response()
end, nil, true)
api.get(P .. "/Localization/ParentalRatings", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	return statics.parental_ratings_response()
end, nil, true)
api.get(P .. "/Branding/Configuration", function(): ApiResponse
	return statics.branding_configuration_response()
end, "public", true)
api.get(P .. "/Branding/Css", function(): ApiResponse
	return statics.branding_css_response()
end, "public", true)
api.get(P .. "/Branding/Css.css", function(): ApiResponse
	return statics.branding_css_response()
end, "public", true)
api.get(P .. "/Devices/Info", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	return session.devices_info_response(ctx, principal.user_id)
end, nil, true)
api.get(P .. "/Devices/Options", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	return preferences.device_options_get(ctx, principal.user_id)
end, nil, true)
api.post(P .. "/Devices/Options", function(ctx: ApiContext)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	return preferences.device_options_set(ctx, principal.user_id)
end, nil, true)
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
end, nil, true)
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
end, nil, true)
local function handle_missing_image_route(ctx: ApiContext): ApiResponse
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
	api.get(P .. "/Items/{imageId}/Images/" .. image_type, handle_missing_image_route, nil, true)
	api.head(P .. "/Items/{imageId}/Images/" .. image_type, handle_missing_image_route, nil, true)
	api.get(P .. "/Items/{imageId}/Images/" .. image_type .. "/{imageIndex}", handle_missing_image_route, nil, true)
	api.head(P .. "/Items/{imageId}/Images/" .. image_type .. "/{imageIndex}", handle_missing_image_route, nil, true)
end

discovery.start(server_info)

local JELLYFIN_SESSIONS_BROADCAST_SECONDS = 2
local JELLYFIN_SESSIONS_BROADCAST_MAX_ERRORS = 5

-- One list per visibility bucket, shared across matching subscribers. Outer
-- xpcall covers task.wait; the consecutive-error counter stops a tight spin if
-- scheduling itself is the fault.
task.spawn(function()
	local consecutive_errors = 0
	while true do
		local tick_ok = xpcall(function()
			task.wait(JELLYFIN_SESSIONS_BROADCAST_SECONDS)
			local by_user: { [number]: { username: string, senders: { api.ApiWebSocketSender } } } = {}
			local admin_senders: { api.ApiWebSocketSender } = {}
			local admin_fallback: UserIdentity? = nil
			for _, entry in sessions do
				if not entry.subscribed_sessions or entry.sender:is_closed() then
					continue
				end
				if entry.is_admin then
					table.insert(admin_senders, entry.sender)
					if not admin_fallback then
						admin_fallback = {
							user_id = entry.user_id,
							username = tostring(entry.info.UserName or ""),
						}
					end
					continue
				end
				local bucket = by_user[entry.user_id]
				if not bucket then
					bucket = { username = tostring(entry.info.UserName or ""), senders = {} }
					by_user[entry.user_id] = bucket
				end
				table.insert(bucket.senders, entry.sender)
			end
			for user_id, bucket in by_user do
				-- One user's render failure must not strand the rest; only
				-- scheduler-level throws reach the outer counter.
				xpcall(function()
					local connections = playback_sessions.list_connections(user_id)
					local list = build_sessions_dto_list(user_id, bucket.username, connections)
					local text = websocket.encode("Sessions", list, nil)
					for _, sender in bucket.senders do
						pcall(function()
							sender:send(text)
						end)
					end
				end, function(err)
					warn(`sessions broadcast failed for user {user_id}: {tostring(err)}`)
				end)
			end
			if #admin_senders > 0 then
				xpcall(function()
					local list = list_all_sessions(admin_fallback, false)
					local text = websocket.encode("Sessions", list, nil)
					for _, sender in admin_senders do
						pcall(function()
							sender:send(text)
						end)
					end
				end, function(err)
					warn(`sessions broadcast failed for admins: {tostring(err)}`)
				end)
			end
		end, function(err)
			warn(`sessions broadcast tick failed: {tostring(err)}`)
		end)
		if tick_ok then
			consecutive_errors = 0
		else
			consecutive_errors += 1
			if consecutive_errors >= JELLYFIN_SESSIONS_BROADCAST_MAX_ERRORS then
				warn(`sessions broadcast disabled after {consecutive_errors} consecutive failures`)
				break
			end
		end
	end
end)
