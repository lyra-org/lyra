--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")
local auth = require("@lyra/auth")
local playlists = require("@lyra/playlists")
local server = require("@lyra/server")
local tracks = require("@lyra/tracks")

local crypt = require("@harmony/crypt")

local artwork = require("./items/images")
local catalog = require("./items/catalog")
local constants = require("../constants")
local item_ids = require("./items/identifiers")
local playback = require("./items/playback")
local playlist_access = require("./playlist_access")
local preferences = require("./preferences")
local requests = require("./items/requests")
local responses = require("./items/responses")

local SERVER_ID = constants.SERVER_ID
local module = {}

type ApiContext = api.ApiContext
type ApiQueryParams = api.ApiQueryParams
type ApiRequest = api.ApiRequest
type ApiResponse = api.ApiResponse
type CoverServeOptions = api.CoverServeOptions
type JsonValue = api.JsonValue
type JsonObject = { [string]: JsonValue }
type LoginResult = auth.LoginResult
export type Principal = auth.Principal
type ResolvedAuth = auth.ResolvedAuth
type Playlist = playlists.Playlist
type PlaylistEntry = playlists.PlaylistEntry
type PlaylistCreate = playlists.PlaylistCreate
type PlaylistFields = playlists.PlaylistFields
type Permission = auth.Permission
type UserIdentity = {
	id: string,
	username: string,
	permissions: { Permission },
}
type AuthenticateRequest = {
	username: string,
	password: string,
}
type PlaylistRouteRequest = {
	name: string?,
	requested_ids: (string | { string })?,
	has_invalid_ids: boolean,
	user_id: string?,
	is_public: boolean?,
}
type PlaylistUserPermission = {
	UserId: string,
	CanEdit: boolean,
}
type PlaylistResponseInfo = {
	OpenAccess: boolean,
	Shares: { PlaylistUserPermission },
	ItemIds: { string },
}
-- What a login saves on its session of the client that made it.
export type LoginIdentity = {
	client: string,
	device: string,
	device_id: string,
	version: string,
}
export type ClientIdentity = {
	client: string,
	device: string,
	device_id: string,
	version: string,
	-- The client as Lyra names it: the app, then the device when the client named one.
	label: string,
}

-- Mirrors the server: the admin permission grants every other permission.
function module.has_permission(user: { permissions: { Permission } }, permission_name: Permission): boolean
	for _, permission in user.permissions do
		if permission == permission_name or permission == "admin" then
			return true
		end
	end
	return false
end

function module.is_admin(principal: Principal): boolean
	return module.has_permission(principal, "admin")
end

function module.can_control_user(controller: Principal, target_user_id: string): boolean
	return controller.user_id == target_user_id or module.is_admin(controller)
end

local function json_text_value(value: JsonValue): string?
	if value == nil then
		return nil
	end

	return requests.trim(tostring(value))
end

local function jellyfin_ids_input(value: JsonValue): (string | { string })?
	if type(value) == "string" then
		return value
	end
	if type(value) ~= "table" then
		return nil
	end

	local ids: { string } = {}
	for _, item: JsonValue in value :: { JsonValue } do
		if type(item) == "string" then
			table.insert(ids, item)
		end
	end
	return ids
end

local function has_non_string_ids(value: JsonValue): boolean
	if value == nil or type(value) == "string" then
		return false
	end
	if type(value) ~= "table" then
		return true
	end
	for _, item: JsonValue in value :: { JsonValue } do
		if type(item) ~= "string" then
			return true
		end
	end
	return false
end

-- Decoding drops null array elements, so `[null]` would otherwise read as an empty list.
local function raw_ids_contain_null(body_raw: string?): boolean
	local raw_ids = if body_raw then string.match(body_raw, '"[Ii]ds"%s*:%s*(%b[])') else nil
	return raw_ids ~= nil and string.find(raw_ids, "%f[%w]null%f[%W]") ~= nil
end

-- Upstream's scheme is `MediaBrowser`; it accepts `Emby` only under legacy authorization.
local function parse_mediabrowser_params(header_value: string): { [string]: string? }?
	local scheme, remainder = string.match(header_value, "^%s*([^%s]+)%s+(.+)%s*$")
	local scheme_name = if scheme then string.lower(scheme) else nil
	local valid_scheme = scheme_name == "mediabrowser"
		or (scheme_name == "emby" and constants.ACCEPT_LEGACY_AUTH_HEADERS)
	if not valid_scheme or remainder == nil then
		return nil
	end

	local values: { [string]: string? } = {}
	for _, part: string in requests.split_csv_preserving_quotes(remainder) do
		local key, raw_value = string.match(part, "^%s*([^=]+)%s*=%s*(.-)%s*$")
		if key and raw_value then
			local normalized_key = string.lower(requests.trim(key) or "")
			if normalized_key ~= "" then
				local value = string.gsub(requests.unquote(raw_value) or "", "%+", " ")
				values[normalized_key] = string.gsub(value, "%%(%x%x)", function(hex)
					return string.char(tonumber(hex, 16) :: number)
				end)
			end
		end
	end
	return values
end

-- The authorization header, as upstream reads it: `X-Emby-Authorization` stands in for a missing
-- `Authorization` only under legacy authorization.
local function authorization_header(headers: { [string]: string }?): string?
	if not headers then
		return nil
	end
	local value: string? = headers.authorization
	if (value == nil or value == "") and constants.ACCEPT_LEGACY_AUTH_HEADERS then
		value = headers["x-emby-authorization"]
	end
	return if value == "" then nil else value
end

local function mediabrowser_header_values(headers: { [string]: string }?): { [string]: string? }
	local value = authorization_header(headers)
	return (if value then parse_mediabrowser_params(value) else nil) or {}
end

local function header_token(value: string?): string?
	if not value then
		return nil
	end
	local params = parse_mediabrowser_params(value)
	return if params then requests.trim(params.token) else nil
end

-- The token headers and `api_key` are legacy forms; `ApiKey` is always accepted.
local function legacy_header_token(headers: { [string]: string }?): string?
	if not constants.ACCEPT_LEGACY_AUTH_HEADERS or type(headers) ~= "table" then
		return nil
	end
	return requests.trim(headers["x-emby-token"]) or requests.trim(headers["x-mediabrowser-token"])
end

local function get_query_token(query: ApiQueryParams?): string?
	return requests.parse_query_string(query, { "ApiKey" }, nil)
		or (
			if constants.ACCEPT_LEGACY_AUTH_HEADERS then requests.parse_query_string(query, { "api_key" }, nil) else nil
		)
end

local function build_jellyfin_user_policy(user: UserIdentity): JsonObject
	local is_administrator = module.has_permission(user, "admin")
	return {
		IsAdministrator = is_administrator,
		IsHidden = false,
		IsDisabled = false,
		MaxParentalRating = nil,
		BlockedTags = {},
		EnableUserPreferenceAccess = false,
		AccessSchedules = {},
		BlockUnratedItems = {},
		EnableRemoteControlOfOtherUsers = is_administrator,
		EnableSharedDeviceControl = true,
		EnableRemoteAccess = true,
		EnableLiveTvManagement = false,
		EnableLiveTvAccess = false,
		EnableMediaPlayback = true,
		EnableAudioPlaybackTranscoding = true,
		EnableVideoPlaybackTranscoding = false,
		EnablePlaybackRemuxing = false,
		ForceRemoteSourceTranscoding = false,
		EnableContentDeletion = false,
		EnableContentDeletionFromFolders = {},
		EnableContentDownloading = module.has_permission(user, "download"),
		EnableSyncTranscoding = false,
		EnableMediaConversion = false,
		EnabledDevices = {},
		EnableAllDevices = true,
		EnabledChannels = {},
		EnableAllChannels = false,
		EnabledFolders = {},
		EnableAllFolders = true,
		InvalidLoginAttemptCount = 0,
		LoginAttemptsBeforeLockout = nil,
		MaxActiveSessions = nil,
		EnablePublicSharing = false,
		BlockedMediaFolders = {},
		BlockedChannels = {},
		RemoteClientBitrateLimit = 0,
		AuthenticationProviderId = nil,
		PasswordResetProviderId = nil,
		SyncPlayAccess = "CreateAndJoinGroups",
	}
end

function module.to_jellyfin_user(user: UserIdentity): JsonValue
	return {
		Id = item_ids.encode(user.id),
		Name = user.username,
		ServerId = SERVER_ID,
		HasPassword = true,
		HasConfiguredPassword = true,
		HasConfiguredEasyPassword = false,
		Configuration = preferences.user_configuration(user.id),
		Policy = build_jellyfin_user_policy(user),
	}
end

-- The caller, described as `to_jellyfin_user` describes a listed user.
function module.principal_to_jellyfin_user(principal: Principal): JsonValue
	return module.to_jellyfin_user({
		id = principal.user_id,
		username = principal.username,
		permissions = principal.permissions,
	})
end

function module.to_authentication_result(login_result: LoginResult): JsonValue
	local principal = login_result.principal
	return {
		User = module.principal_to_jellyfin_user(principal),
		AccessToken = tostring(login_result.token),
		ServerId = SERVER_ID,
		SessionInfo = nil,
	}
end

function module.parse_authenticate_request(ctx: ApiContext?): (AuthenticateRequest?, ApiResponse?)
	local request = if ctx then ctx.request else nil
	local request_json = if request then request.json else nil
	if type(request_json) ~= "table" then
		return nil, responses.bad_request("invalid authentication request body")
	end
	local request_object = request_json :: JsonObject

	local username = json_text_value(request_object.Username or request_object.username)
	if not username then
		return nil, responses.bad_request("missing Username")
	end

	local password_raw = request_object.Pw
	if password_raw == nil then
		password_raw = request_object.Password
	end
	local password = if password_raw == nil then "" else tostring(password_raw)

	return {
		username = username,
		password = password,
	}, nil
end

function module.unauthorized(): ApiResponse
	return api.response.json(401, {
		Message = "Unauthorized",
	})
end

-- Upstream's answer to a request naming a user other than the caller, from the `SecurityException`
-- its user check throws, as its exception middleware writes it outside development.
function module.forbidden_user(): ApiResponse
	return api.response.text(403, "Error processing request.")
end

local function resolve_jellyfin_auth(ctx: ApiContext?): (ResolvedAuth?, string?)
	local request = if ctx then ctx.request else nil
	local headers = if request then request.headers else nil
	local query = if request then request.query else nil

	local token = header_token(authorization_header(headers)) or legacy_header_token(headers) or get_query_token(query)
	local requested_user_id = requests.trim(mediabrowser_header_values(headers).userid)

	local resolved_auth = auth.resolve_auth(token)
	if not resolved_auth then
		return nil, token
	end
	local principal = resolved_auth.principal

	if requested_user_id then
		local decoded_user_id = item_ids.decode(requested_user_id)
		if decoded_user_id ~= principal.user_id then
			return nil, token
		end
	end

	return resolved_auth, token
end

-- For routes upstream leaves open to anyone: when the request's token resolves, the request acts
-- as its user, so the server counts what it does against that user. A missing, unknown or
-- failing token changes nothing.
function module.adopt_optional_auth(ctx: ApiContext?)
	pcall(resolve_jellyfin_auth, ctx)
end

-- The client, device, device id and version a request sends, from its Authorization header or,
-- for browser WebSockets, its query parameters.
local function request_identity(request: ApiRequest): (string?, string?, string?, string?)
	local query = request.query
	local header_values = mediabrowser_header_values(request.headers)

	local function take(header_key: string, query_keys: { string }): string?
		local header_value = requests.trim(header_values[header_key])
		if header_value then
			return header_value
		end
		return requests.parse_query_string(query, query_keys, nil)
	end

	return take("client", { "client", "Client" }),
		take("device", { "device", "Device" }),
		take("deviceid", { "deviceId", "DeviceId", "deviceid" }),
		take("version", { "version", "Version" })
end

-- As upstream lists a session by its device and app, Lyra's one label names both.
local function client_label(client: string, device: string?): string
	if device and device ~= client then
		return `{client} · {device}`
	end
	return client
end

-- Request identity overrides the login defaults without changing the saved binding.
-- Browser WebSockets supply these via query parameters instead of custom headers.
function module.resolve_client_identity(
	ctx: ApiContext?,
	resolved_auth: ResolvedAuth?,
	token: string?
): (ClientIdentity?, string?)
	local request = if ctx then ctx.request else nil
	if not request then
		return nil, "missing request"
	end

	local client: string?, device: string?, device_id: string?, version: string? = request_identity(request)

	if not resolved_auth or not token then
		local request_auth, request_token = module.require_jellyfin_auth(ctx)
		resolved_auth = resolved_auth or request_auth
		token = token or request_token
	end
	local session_token = if resolved_auth and resolved_auth.credential.kind == "session" then token else nil
	local saved = if session_token then auth.get_session_data(session_token) else nil
	if type(saved) == "table" then
		local fields = saved :: { [string]: auth.JsonValue }
		local saved_client = if type(fields.client) == "string" then fields.client else nil
		local saved_device_id = if type(fields.device_id) == "string" then fields.device_id else nil
		if saved_client and saved_device_id then
			client = client or saved_client
			device_id = device_id or saved_device_id
			device = device or (if type(fields.device) == "string" then fields.device else nil)
			version = version or (if type(fields.version) == "string" then fields.version else nil)
		end
	end
	local named_device = device
	if resolved_auth and resolved_auth.credential.kind == "api_key" then
		client = resolved_auth.credential.api_key_name
		device_id = device_id or SERVER_ID
		device = device or "Lyra"
		version = version or server.info().version
	end
	if not client or not device_id then
		return nil,
			"missing Client or DeviceId; send player identity in Authorization or client/deviceId query parameters"
	end

	local identity: ClientIdentity = {
		client = client,
		device = device or client,
		device_id = device_id :: string,
		version = version or "",
		label = client_label(client, named_device),
	}
	return identity, nil
end

-- The identity a request sends itself, or nil unless it names all four parts, as upstream's Quick
-- Connect wants before anything else.
function module.request_login_identity(ctx: ApiContext?): LoginIdentity?
	local request = if ctx then ctx.request else nil
	if not request then
		return nil
	end
	local client, device, device_id, version = request_identity(request)
	if not client or not device or not device_id or not version then
		return nil
	end
	return {
		client = client,
		device = device,
		device_id = device_id,
		version = version,
	}
end

-- The session metadata a login records, from the identity the login request sends.
function module.login_metadata(ctx: ApiContext): (string?, string?)
	local request = ctx.request
	local client, device = request_identity(request)
	local client_name = if client then client_label(client, device) else nil
	return requests.trim(request.headers["user-agent"]), client_name
end

-- `saved` speaks for a client whose own request has since ended, as a Quick Connect sign-in's does.
function module.remember_login_identity(
	ctx: ApiContext,
	resolved_auth: ResolvedAuth,
	token: string,
	saved: LoginIdentity?
)
	local identity: LoginIdentity? = saved or module.resolve_client_identity(ctx, resolved_auth, token)
	if identity then
		auth.set_session_data(token, {
			client = identity.client,
			device_id = identity.device_id,
			device = identity.device,
			version = identity.version,
		})
	end
end

function module.require_jellyfin_auth(ctx: ApiContext?): (ResolvedAuth?, string?)
	local resolved_auth, token = resolve_jellyfin_auth(ctx)
	if not resolved_auth then
		return nil, token
	end

	return resolved_auth, token
end

function module.require_jellyfin_principal(ctx: ApiContext?): Principal?
	local resolved_auth, _token = module.require_jellyfin_auth(ctx)
	if not resolved_auth then
		return nil
	end

	return resolved_auth.principal
end

local function report_client_playback(
	resolved_auth: ResolvedAuth,
	payload: playback.PlaybackPayload,
	event_name: string,
	identity: ClientIdentity
): (boolean, string?)
	local principal = resolved_auth.principal
	local session_id = module.build_session_id(principal, identity)
	return playback.report_playback_event(resolved_auth, payload, event_name, session_id, {
		session_info = module.build_session_info(session_id, principal, identity, module.current_iso_timestamp()),
		client_name = identity.label,
	})
end

-- WS counterpart to handle_playback_report_route; auth and identity come
-- from the socket handshake, not per-frame headers.
function module.handle_ws_playback_event(
	resolved_auth: ResolvedAuth,
	data: JsonValue,
	event_name: string,
	identity: ClientIdentity
): (boolean, string?)
	local payload = playback.as_playback_payload(data)
	if not payload then
		return false, "invalid playback report body"
	end
	if not playback.ensure_payload_user_matches(resolved_auth, payload) then
		return false, "playback report user does not match authenticated user"
	end
	return report_client_playback(resolved_auth, payload, event_name, identity)
end

function module.handle_playback_report_route(ctx: ApiContext?, event_name: string): ApiResponse
	local resolved_auth, token = module.require_jellyfin_auth(ctx)
	if not resolved_auth then
		return module.unauthorized()
	end

	local payload, parse_err = playback.parse_playback_report_body(ctx)
	if not payload then
		return parse_err or responses.bad_request("invalid playback report body")
	end
	if not playback.ensure_payload_user_matches(resolved_auth, payload) then
		return module.unauthorized()
	end

	local identity, identity_err = module.resolve_client_identity(ctx, resolved_auth, token)
	if not identity then
		return responses.bad_request(identity_err or "missing Client or DeviceId")
	end

	local ok, report_err = report_client_playback(resolved_auth, payload, event_name, identity)
	if not ok then
		return responses.bad_request(report_err or "failed to report playback event")
	end

	return api.response.empty(204)
end

-- Upstream only keeps the play session's transcode job alive, and Lyra runs none.
function module.handle_playback_ping_route(ctx: ApiContext?): ApiResponse
	if not module.require_jellyfin_principal(ctx) then
		return module.unauthorized()
	end

	local query = if ctx and ctx.request then ctx.request.query else nil
	if not requests.parse_query_string(query, { "playSessionId", "PlaySessionId" }, nil) then
		return responses.bad_request("missing playSessionId")
	end
	return api.response.empty(204)
end

-- Upstream kills a device's transcodes for a play session. Lyra's encodes end with the response
-- streaming them, so there is nothing to stop.
function module.handle_stop_encoding_route(ctx: ApiContext?): ApiResponse
	if not module.require_jellyfin_principal(ctx) then
		return module.unauthorized()
	end

	local query = if ctx and ctx.request then ctx.request.query else nil
	if not requests.parse_query_string(query, { "deviceId" }, nil) then
		return responses.bad_request("missing deviceId")
	end
	if not requests.parse_query_string(query, { "playSessionId" }, nil) then
		return responses.bad_request("missing playSessionId")
	end
	return api.response.empty(204)
end

function module.require_matching_user_scope(ctx: ApiContext?, principal: Principal): boolean
	local params = if ctx then ctx.params else nil
	local path_user_id = if params then requests.trim(params.userId) else nil
	if path_user_id and item_ids.decode(path_user_id) ~= principal.user_id then
		return false
	end

	local query = if ctx and ctx.request then ctx.request.query else nil
	local query_user_id = requests.parse_query_string(query, { "userId", "UserId", "userid" }, nil)
	if query_user_id and item_ids.decode(query_user_id) ~= principal.user_id then
		return false
	end

	return true
end

function module.require_matching_user_scope_from_id_param(ctx: ApiContext?, principal: Principal): boolean
	local params = if ctx then ctx.params else nil
	local path_user_id = if params then requests.trim(params.id) else nil
	if path_user_id and item_ids.decode(path_user_id) ~= principal.user_id then
		return false
	end

	return module.require_matching_user_scope(ctx, principal)
end

function module.current_unix_ms(): number
	return os.time() * 1000
end

function module.normalize_optional_user_id(value: JsonValue): string?
	if value == nil then
		return nil
	end

	return requests.trim(tostring(value))
end

function module.parse_playlist_request_body(ctx: ApiContext?): (PlaylistRouteRequest?, ApiResponse?)
	local request = if ctx then ctx.request else nil
	local request_json = if request then request.json else nil
	if type(request_json) ~= "table" then
		return nil, responses.bad_request("invalid playlist request body")
	end
	local request_object = request_json :: JsonObject

	local requested_user_id = module.normalize_optional_user_id(request_object.UserId or request_object.userId)
	local raw_is_public = request_object.IsPublic
	local is_public = nil
	if raw_is_public ~= nil then
		if type(raw_is_public) ~= "boolean" then
			return nil, responses.bad_request("invalid IsPublic value")
		end
		is_public = raw_is_public
	end

	local raw_ids = request_object.Ids or request_object.ids
	local requested_ids = jellyfin_ids_input(raw_ids)
	return {
		name = json_text_value(request_object.Name or request_object.name),
		requested_ids = requested_ids,
		has_invalid_ids = has_non_string_ids(raw_ids)
			or raw_ids_contain_null(if request then request.body_raw else nil),
		user_id = requested_user_id,
		is_public = is_public,
	},
		nil
end

-- Of these track ids, the ones the caller can see, in order.
local function visible_track_ids(track_ids: { string }): { string }
	if #track_ids == 0 then
		return {}
	end
	local found = tracks.get(track_ids)
	local result: { string } = {}
	for _, id in track_ids do
		if found[id] then
			table.insert(result, id)
		end
	end
	return result
end

function module.parse_playlist_query_track_ids(query: ApiQueryParams?): { string }
	local raw_ids = requests.parse_query_string(query, { "ids", "Ids" }, nil)
	return visible_track_ids(item_ids.decode_list(raw_ids))
end

function module.parse_playlist_query_entry_ids(query: ApiQueryParams?, playlist: Playlist): { string }
	local raw = requests.parse_query_string(query, { "entryIds", "EntryIds", "entryids" }, nil)
	local raw_ids: { string } = {}
	for _, value in if raw then string.split(raw, ",") else {} do
		local trimmed = requests.trim(value)
		if trimmed then
			table.insert(raw_ids, trimmed)
		end
	end
	return playlist_access.entries_of_items(playlist, raw_ids)
end

function module.ensure_user_scope_matches_principal(requested_user_id: string?, principal: Principal): boolean
	if requested_user_id and item_ids.decode(requested_user_id) ~= principal.user_id then
		return false
	end

	return true
end

function module.resolve_playlist_for_route(ctx: ApiContext?): (Playlist?, ApiResponse?)
	local params = if ctx then ctx.params else nil
	local playlist_id = if params then params.playlistId else nil
	if not playlist_id then
		return nil, responses.bad_request("missing playlist id")
	end

	local playlist = playlist_access.find(item_ids.decode(playlist_id))
	if not playlist then
		return nil, responses.not_found("playlist not found")
	end

	return playlist, nil
end

function module.ensure_playlist_visible(playlist: Playlist, principal: Principal): (boolean, ApiResponse?)
	if playlist_access.can_view(playlist, principal.user_id) then
		return true, nil
	end

	return false, responses.forbidden("playlist not accessible")
end

function module.ensure_playlist_editable(playlist: Playlist, principal: Principal): (boolean, ApiResponse?)
	if playlist_access.can_edit(playlist, principal.user_id) then
		return true, nil
	end

	return false, responses.forbidden("playlist is read-only")
end

-- Deletes a playlist as upstream's item delete does: one the caller can't see isn't found, one
-- it can see is deleted by its owner or an admin, and anyone else is unauthorized.
function module.handle_delete_item_route(ctx: ApiContext): ApiResponse
	local principal = module.require_jellyfin_principal(ctx)
	if not principal then
		return module.unauthorized()
	end
	if not module.require_matching_user_scope(ctx, principal) then
		return module.unauthorized()
	end

	local playlist = playlist_access.find(item_ids.decode(ctx.params.id))
	if not playlist or not playlist_access.can_view(playlist, principal.user_id) then
		return responses.not_found("item not found")
	end
	if not playlist_access.can_delete(playlist, principal.user_id, module.is_admin(principal)) then
		return module.unauthorized()
	end
	local ok, deleted = pcall(playlists.delete, playlist.id)
	if not ok then
		return responses.bad_request("failed to delete playlist")
	end
	if not deleted then
		return responses.not_found("item not found")
	end
	return api.response.empty(204)
end

function module.handle_create_playlist_route(ctx: ApiContext): ApiResponse
	local principal = module.require_jellyfin_principal(ctx)
	if not principal then
		return module.unauthorized()
	end

	local request_payload, parse_err = module.parse_playlist_request_body(ctx)
	if not request_payload then
		return parse_err or responses.bad_request("invalid playlist request body")
	end
	if not module.ensure_user_scope_matches_principal(request_payload.user_id, principal) then
		return module.unauthorized()
	end

	local playlist_name = request_payload.name or "New Playlist"
	local create_request: PlaylistCreate = {
		name = playlist_name,
		is_public = if request_payload.is_public ~= nil then request_payload.is_public else false,
		created_at_ms = module.current_unix_ms(),
		updated_at_ms = module.current_unix_ms(),
		track_ids = visible_track_ids(item_ids.decode_list(request_payload.requested_ids)),
	}

	local ok, created = pcall(playlists.create, create_request)
	if not ok then
		return responses.bad_request("failed to create playlist")
	end

	local jellyfin_playlist_id = item_ids.encode(created.id)
	if not jellyfin_playlist_id then
		return responses.bad_request("failed to encode playlist id")
	end
	return api.response.json(200, {
		Id = jellyfin_playlist_id,
	})
end
function module.handle_add_playlist_items_route(ctx: ApiContext): ApiResponse
	local principal = module.require_jellyfin_principal(ctx)
	if not principal then
		return module.unauthorized()
	end

	if not module.require_matching_user_scope(ctx, principal) then
		return module.unauthorized()
	end

	local playlist, playlist_err = module.resolve_playlist_for_route(ctx)
	if not playlist then
		return playlist_err or responses.bad_request("playlist not found")
	end

	local can_edit, edit_err = module.ensure_playlist_editable(playlist, principal)
	if not can_edit then
		return edit_err or responses.bad_request("playlist is read-only")
	end

	local query = if ctx and ctx.request then ctx.request.query else nil
	local track_ids = module.parse_playlist_query_track_ids(query)
	if #track_ids == 0 then
		return responses.bad_request("missing ids query parameter")
	end

	-- Upstream inserts at the 0-based `position`, from the start when it's negative, and appends
	-- without it.
	local position = requests.parse_query_int(query, { "position", "Position" }, nil, nil, nil)
	if position ~= nil then
		position = math.max(position, 0)
	end
	local added, entry_ids = pcall(playlists.add_tracks, playlist.id, track_ids, { position = position })
	if not added or entry_ids == nil then
		return responses.bad_request("failed to add track to playlist")
	end

	return api.response.empty(204)
end
function module.handle_update_playlist_route(ctx: ApiContext): ApiResponse
	local principal = module.require_jellyfin_principal(ctx)
	if not principal then
		return module.unauthorized()
	end

	local playlist, playlist_err = module.resolve_playlist_for_route(ctx)
	if not playlist then
		return playlist_err or responses.bad_request("playlist not found")
	end

	local can_edit, edit_err = module.ensure_playlist_editable(playlist, principal)
	if not can_edit then
		return edit_err or responses.bad_request("playlist is read-only")
	end

	local request_payload, parse_err = module.parse_playlist_request_body(ctx)
	if not request_payload then
		return parse_err or responses.bad_request("invalid playlist request body")
	end
	if not module.ensure_user_scope_matches_principal(request_payload.user_id, principal) then
		return module.unauthorized()
	end
	if request_payload.has_invalid_ids then
		return responses.bad_request("invalid playlist item ids")
	end
	local fields: PlaylistFields = {
		name = request_payload.name,
		is_public = request_payload.is_public,
		updated_at_ms = module.current_unix_ms(),
	}
	local ok, updated
	if request_payload.requested_ids ~= nil then
		local replacement_ids = item_ids.decode_list_exact(request_payload.requested_ids)
		if replacement_ids == nil then
			return responses.bad_request("invalid playlist item ids")
		end
		ok, updated = pcall(playlists.replace, playlist.id, { fields = fields, track_ids = replacement_ids })
	else
		ok, updated = pcall(playlists.update, playlist.id, fields)
	end
	if not ok or updated == nil then
		return responses.bad_request("failed to update playlist")
	end

	return api.response.empty(204)
end
local function playlist_item_ids(playlist: Playlist): { string }
	local encoded_ids: { string } = {}
	for _, track_ref: PlaylistEntry in playlist_access.track_refs(playlist) do
		local item_id = item_ids.encode(track_ref.track_id)
		if item_id then
			table.insert(encoded_ids, item_id)
		end
	end

	return encoded_ids
end

function module.to_playlist_info(playlist: Playlist): PlaylistResponseInfo
	local owner_id = playlist_access.owner_id(playlist)
	local shares: { PlaylistUserPermission } = {}
	if owner_id then
		table.insert(shares, {
			UserId = item_ids.encode(owner_id) or "",
			CanEdit = true,
		})
	end

	return {
		OpenAccess = playlist and playlist.is_public == true,
		Shares = shares,
		ItemIds = playlist_item_ids(playlist),
	}
end

function module.playlist_user_permission_to_json(permission: PlaylistUserPermission): JsonObject
	return {
		UserId = permission.UserId,
		CanEdit = permission.CanEdit,
	}
end

function module.playlist_users_to_json(users: { PlaylistUserPermission }): { JsonValue }
	local json_users: { JsonValue } = {}
	for _, permission: PlaylistUserPermission in users do
		table.insert(json_users, module.playlist_user_permission_to_json(permission))
	end
	return json_users
end

function module.playlist_info_to_json(info: PlaylistResponseInfo): JsonObject
	local shares = module.playlist_users_to_json(info.Shares)
	local item_ids: { JsonValue } = {}
	for _, item_id: string in info.ItemIds do
		table.insert(item_ids, item_id)
	end
	return {
		OpenAccess = info.OpenAccess,
		Shares = shares,
		ItemIds = item_ids,
	}
end

function module.to_playlist_user_permission(playlist: Playlist, user_id: string): PlaylistUserPermission
	local owner_id = playlist_access.owner_id(playlist)
	return {
		UserId = item_ids.encode(user_id) or "",
		CanEdit = owner_id ~= nil and owner_id == user_id,
	}
end

-- The bound on one side of an image: its explicit maximum, else the smallest size asked for.
local function image_bound(query: ApiQueryParams?, max_key: string, size_keys: { string }): number?
	local max = requests.parse_query_int(query, { max_key }, nil, 1, nil)
	if max then
		return max
	end
	local bound: number? = nil
	for _, key in size_keys do
		local size = requests.parse_query_int(query, { key }, nil, 1, nil)
		if size and (bound == nil or size < bound) then
			bound = size
		end
	end
	return bound
end

local function respond_with_primary_image(ctx: ApiContext?, cover_info: artwork.ResolvedCoverInfo): ApiResponse
	local query = if ctx and ctx.request then ctx.request.query else nil
	local format_query = requests.parse_query_string(query, { "format" }, nil)
	local requested_format = requests.normalize_image_format(format_query)
	if format_query ~= nil and requested_format == nil then
		return responses.bad_request("invalid image format")
	end
	local options: CoverServeOptions = {
		format = requested_format,
		quality = requests.parse_query_int(query, { "quality" }, nil, 0, 100),
		max_width = image_bound(query, "MaxWidth", { "fillWidth", "width" }),
		max_height = image_bound(query, "MaxHeight", { "fillHeight", "height" }),
	}

	-- Upstream's caching: a request naming the image `tag` gets an immutable response validated by
	-- that tag, and `Cache-Control: no-cache` turns caching off.
	local request_headers = if ctx and ctx.request then ctx.request.headers else nil
	local tag = requests.parse_query_string(query, { "tag" }, nil)
	local headers = {
		["vary"] = "Accept",
		["content-disposition"] = "attachment",
	}
	if request_headers and requests.trim(request_headers["cache-control"]) == "no-cache" then
		headers["cache-control"] = "no-cache, no-store, must-revalidate"
		headers["pragma"] = "no-cache, no-store, must-revalidate"
	elseif tag then
		headers["cache-control"] = "public, max-age=31536000, immutable"
		headers["etag"] = `"{tag}"`
		local if_none_match = if request_headers then request_headers["if-none-match"] else nil
		if if_none_match == `"{tag}"` or if_none_match == tag then
			return api.response.empty(304, headers)
		end
	else
		headers["cache-control"] = "public"
	end

	local response = api.response.cover(cover_info.id, options)
	response.headers = headers
	return response
end

-- Artwork routes are public, as upstream: an anonymous caller gets any item's image, and a caller
-- with valid credentials only the images of items it can see. Invalid credentials read as none.
function module.handle_primary_image_request(ctx: ApiContext?): ApiResponse
	local params = if ctx then ctx.params else nil
	local image_id = if params then params.imageId else nil
	if not image_id then
		return responses.bad_request("missing image id")
	end

	local item_id = item_ids.decode(image_id)
	if not item_id then
		return responses.not_found("image not found")
	end
	local signed_in = module.require_jellyfin_principal(ctx) ~= nil
	local cover_info = if signed_in then artwork.get_item_cover_file(item_id) else artwork.get_artwork_file(item_id)
	if not cover_info then
		return responses.not_found("image not found")
	end
	return respond_with_primary_image(ctx, cover_info)
end

local function serve_public_image_by_name(
	ctx: ApiContext?,
	find_cover: (string) -> artwork.ResolvedCoverInfo?
): ApiResponse
	local params = if ctx then ctx.params else nil
	local name = if params then params.name else nil
	if not name or name == "" then
		return responses.bad_request("missing name")
	end

	local image_type = if params then params.imageType else nil
	if type(image_type) ~= "string" or string.lower(image_type) ~= "primary" then
		return responses.not_found("image not found")
	end

	local raw_image_index = if params then params.imageIndex else nil
	local image_index = if type(raw_image_index) == "string" then tonumber(raw_image_index) else 0
	if image_index == nil or image_index ~= math.floor(image_index) or image_index < 0 then
		return responses.bad_request("invalid image index")
	end
	if image_index ~= 0 then
		return responses.not_found("image not found")
	end

	local cover_info = find_cover(name)
	if not cover_info then
		return responses.not_found("image not found")
	end
	return respond_with_primary_image(ctx, cover_info)
end

function module.handle_artist_image_request(ctx: ApiContext?): ApiResponse
	return serve_public_image_by_name(ctx, function(name)
		return artwork.get_artwork_file(catalog.find_artist_id_by_name(name))
	end)
end

function module.handle_person_image_request(ctx: ApiContext?): ApiResponse
	return serve_public_image_by_name(ctx, function(name)
		return artwork.get_artwork_file(catalog.find_artist_id_by_name(name))
	end)
end

function module.handle_genre_image_request(ctx: ApiContext?): ApiResponse
	return serve_public_image_by_name(ctx, function(name)
		return artwork.get_artwork_file(catalog.find_genre_id_by_name(name))
	end)
end

-- Whitelist of capability fields the plugin forwards to callers of
-- GET /Sessions. Anything outside this set (DeviceProfile, IconUrl, etc.) is
-- dropped to keep the stored object bounded.
local CAPABILITY_STRING_FIELDS = { "AppStoreUrl", "IconUrl" }
local CAPABILITY_BOOL_FIELDS = {
	"SupportsMediaControl",
	"SupportsPersistentIdentifier",
}
local CAPABILITY_STRING_ARRAY_FIELDS = { "PlayableMediaTypes", "SupportedCommands" }
-- What a declaration may hold, since each is stored. Upstream has about 50 general commands.
local MAX_CAPABILITY_LIST_LENGTH = 128
local MAX_CAPABILITY_STRING_LENGTH = 256
module.MAX_CAPABILITY_LIST_LENGTH = MAX_CAPABILITY_LIST_LENGTH
module.MAX_CAPABILITY_STRING_LENGTH = MAX_CAPABILITY_STRING_LENGTH

local function to_string_array(value: JsonValue): { JsonValue }?
	if type(value) ~= "table" then
		return nil
	end
	local result: { JsonValue } = {}
	for _, item: JsonValue in value :: { JsonValue } do
		if #result >= MAX_CAPABILITY_LIST_LENGTH then
			break
		end
		if type(item) == "string" and #item <= MAX_CAPABILITY_STRING_LENGTH then
			table.insert(result, item)
		end
	end
	return result
end

-- The fields upstream always serializes. A declaration without them would be an empty table,
-- which encodes as a JSON array.
local function with_capability_defaults(capabilities: JsonObject, persistent_identifier: boolean): JsonObject
	for _, field in CAPABILITY_STRING_ARRAY_FIELDS do
		if capabilities[field] == nil then
			capabilities[field] = {} :: { JsonValue }
		end
	end
	if capabilities.SupportsMediaControl == nil then
		capabilities.SupportsMediaControl = false
	end
	if capabilities.SupportsPersistentIdentifier == nil then
		capabilities.SupportsPersistentIdentifier = persistent_identifier
	end
	return capabilities
end

local function parse_bool_string(raw: string?): boolean?
	if type(raw) ~= "string" then
		return nil
	end
	local lower = string.lower(raw)
	if lower == "true" or lower == "1" then
		return true
	end
	if lower == "false" or lower == "0" then
		return false
	end
	return nil
end

local function parse_csv_array(raw: string?): { JsonValue }?
	if type(raw) ~= "string" or raw == "" then
		return nil
	end
	local result: { JsonValue } = {}
	for _, part in string.split(raw, ",") do
		local trimmed = requests.trim(part)
		if trimmed then
			table.insert(result, trimmed)
		end
	end
	return to_string_array(result)
end

-- The non-/Full declaration, from query params, with upstream's defaults for what it leaves out.
function module.parse_client_capabilities_query(query: ApiQueryParams?): JsonObject?
	if type(query) ~= "table" then
		return nil
	end
	local result: JsonObject = {}
	local commands =
		parse_csv_array(requests.parse_query_string(query, { "SupportedCommands", "supportedCommands" }, nil))
	if commands then
		result.SupportedCommands = commands
	end
	local media_types =
		parse_csv_array(requests.parse_query_string(query, { "PlayableMediaTypes", "playableMediaTypes" }, nil))
	if media_types then
		result.PlayableMediaTypes = media_types
	end
	for _, key in CAPABILITY_BOOL_FIELDS do
		local raw = requests.parse_query_string(query, { key }, nil)
		local parsed = parse_bool_string(raw)
		if parsed ~= nil then
			result[key] = parsed
		end
	end
	return with_capability_defaults(result, true)
end

function module.parse_client_capabilities(body: JsonValue): JsonObject?
	if type(body) ~= "table" then
		return nil
	end
	local raw = body :: JsonObject
	local caps: JsonObject = {}
	for _, field in CAPABILITY_STRING_FIELDS do
		local value = raw[field]
		if type(value) == "string" and #value <= MAX_CAPABILITY_STRING_LENGTH then
			caps[field] = value
		end
	end
	for _, field in CAPABILITY_BOOL_FIELDS do
		local value = raw[field]
		if type(value) == "boolean" then
			caps[field] = value
		end
	end
	for _, field in CAPABILITY_STRING_ARRAY_FIELDS do
		local parsed = to_string_array(raw[field])
		if parsed then
			caps[field] = parsed
		end
	end
	return with_capability_defaults(caps, false)
end

-- Length-prefixed material prevents cross-tuple collisions on arbitrary
-- client/device_id bytes. 8-4-4-4-12 format keeps Guid.Parse happy on the
-- client side.
function module.build_session_id(principal: Principal, identity: ClientIdentity): string
	local user_part = tostring(principal.user_id)
	local material = string.format(
		"%d:%s|%d:%s|%d:%s",
		#user_part,
		user_part,
		#identity.client,
		identity.client,
		#identity.device_id,
		identity.device_id
	)
	return crypt.hash("xxh3_128", material)
end

-- What upstream reports for a session that plays nothing.
function module.idle_play_state(): JsonObject
	return {
		CanSeek = false,
		IsPaused = false,
		IsMuted = false,
		RepeatMode = "RepeatNone",
		PlaybackOrder = "Default",
	}
end

function module.build_session_info(
	session_id: string,
	principal: Principal,
	identity: ClientIdentity,
	activity_iso: string
): JsonObject
	local custom_name = preferences.session_device_name(identity.device_id)
	return {
		Id = session_id,
		UserId = item_ids.encode(principal.user_id),
		UserName = principal.username,
		Client = identity.client,
		DeviceName = custom_name or identity.device,
		DeviceId = identity.device_id,
		ApplicationVersion = identity.version,
		ServerId = SERVER_ID,
		LastActivityDate = activity_iso,
		LastPlaybackCheckIn = activity_iso,
		IsActive = true,
		SupportsRemoteControl = false,
		SupportsMediaControl = false,
		HasCustomDeviceName = custom_name ~= nil,
		PlayableMediaTypes = {},
		SupportedCommands = {},
		AdditionalUsers = {},
		PlayState = module.idle_play_state(),
	}
end

local function iso_timestamp(unix_seconds: number): string
	return os.date("!%Y-%m-%dT%H:%M:%SZ", unix_seconds) :: string
end

function module.current_iso_timestamp(): string
	return iso_timestamp(os.time())
end

function module.iso_timestamp_ms(unix_ms: number): string
	return iso_timestamp(unix_ms // 1000)
end

local function saved_text(fields: { [string]: auth.JsonValue }, key: string): string?
	local value = fields[key]
	return if type(value) == "string" then value else nil
end

-- The identity remember_login_identity saved on a login's session, if it has a device.
local function login_identity(entry: auth.SessionInfo): ({ [string]: auth.JsonValue }?, string?)
	if type(entry.data) ~= "table" then
		return nil, nil
	end
	local fields = entry.data :: { [string]: auth.JsonValue }
	local device_id = saved_text(fields, "device_id")
	if not device_id then
		return nil, nil
	end
	return fields, device_id
end

local function to_device_info(entry: auth.SessionInfo): JsonObject?
	local fields, device_id = login_identity(entry)
	if not fields or not device_id then
		return nil
	end
	return {
		Id = device_id,
		Name = saved_text(fields, "device"),
		CustomName = preferences.device_custom_name(device_id),
		AppName = saved_text(fields, "client"),
		AppVersion = saved_text(fields, "version"),
		LastUserId = item_ids.encode(entry.user_id),
		LastUserName = entry.username,
		DateLastActivity = iso_timestamp(entry.last_seen_at_ms // 1000),
	}
end

type DeviceRow = { device: JsonObject, last_seen_at_ms: number }

-- One device per login across all users, most recently active first.
local function list_devices(): { JsonObject }
	local rows: { DeviceRow } = {}
	for _, entry in auth.list_sessions({ all_users = true }) do
		local device = to_device_info(entry)
		if device then
			table.insert(rows, { device = device, last_seen_at_ms = entry.last_seen_at_ms })
		end
	end
	table.sort(rows, function(a: DeviceRow, b: DeviceRow): boolean
		if a.last_seen_at_ms ~= b.last_seen_at_ms then
			return a.last_seen_at_ms > b.last_seen_at_ms
		end
		return (a.device.Id :: string) < (b.device.Id :: string)
	end)
	local devices: { JsonObject } = {}
	for _, row in rows do
		table.insert(devices, row.device)
	end
	return devices
end

-- A 401 or 403 response unless the caller is an administrator, as upstream's elevation policy.
function module.require_admin(ctx: ApiContext?): ApiResponse?
	local principal = module.require_jellyfin_principal(ctx)
	if not principal then
		return module.unauthorized()
	end
	if not module.is_admin(principal) then
		return responses.forbidden()
	end
	return nil
end

function module.devices_response(ctx: ApiContext?): ApiResponse
	local denied = module.require_admin(ctx)
	if denied then
		return denied
	end

	local devices = list_devices()
	return api.response.json(200, {
		Items = devices,
		TotalRecordCount = #devices,
		StartIndex = 0,
	})
end

function module.device_info_response(ctx: ApiContext?): ApiResponse
	local denied = module.require_admin(ctx)
	if denied then
		return denied
	end

	local query = if ctx and ctx.request then ctx.request.query else nil
	local device_id = requests.parse_query_string(query, { "id", "Id" }, nil)
	if not device_id then
		return responses.bad_request("missing id")
	end
	for _, device in list_devices() do
		if device.Id == device_id then
			return api.response.json(200, device)
		end
	end
	return responses.not_found("device not found")
end

-- Logs out every login of each device; any unknown device fails the whole request.
function module.delete_devices_response(ctx: ApiContext?): ApiResponse
	local denied = module.require_admin(ctx)
	if denied then
		return denied
	end

	local logins_by_device: { [string]: { string } } = {}
	for _, entry in auth.list_sessions({ all_users = true }) do
		local _, device_id = login_identity(entry)
		if device_id then
			local logins = logins_by_device[device_id] or {}
			table.insert(logins, entry.session_id)
			logins_by_device[device_id] = logins
		end
	end

	local query = if ctx and ctx.request then ctx.request.query else nil
	local device_ids: { string } = {}
	for key, values in query or {} do
		if string.lower(tostring(key)) == "id" then
			for _, value in values do
				local device_id = tostring(value)
				if not logins_by_device[device_id] then
					return responses.bad_request("device not found")
				end
				table.insert(device_ids, device_id)
			end
		end
	end

	for _, device_id in device_ids do
		for _, session_id in logins_by_device[device_id] do
			auth.revoke_session(session_id)
		end
	end
	return api.response.empty(204)
end

local function resolve_item_file_track_id(ctx: ApiContext?): (string?, ApiResponse?)
	local principal = module.require_jellyfin_principal(ctx)
	if not principal then
		return nil, module.unauthorized()
	end

	if not module.require_matching_user_scope(ctx, principal) then
		return nil, module.unauthorized()
	end

	local params = if ctx then ctx.params else nil
	local item_id = if params then params.itemId else nil
	if not item_id then
		return nil, responses.bad_request("missing item id")
	end

	local track = playback.resolve_track_for_playback(item_ids.decode(item_id))
	if not track then
		return nil, responses.not_found("item not found")
	end

	return track.id, nil
end

function module.handle_item_file_stream_request(ctx: ApiContext?): ApiResponse
	local track_id, err = resolve_item_file_track_id(ctx)
	if not track_id then
		return err or responses.not_found("item not found")
	end
	return api.response.stream_track(track_id)
end

function module.handle_item_download_request(ctx: ApiContext?): ApiResponse
	local track_id, err = resolve_item_file_track_id(ctx)
	if not track_id then
		return err or responses.not_found("item not found")
	end
	return api.response.download_track(track_id)
end

return module
