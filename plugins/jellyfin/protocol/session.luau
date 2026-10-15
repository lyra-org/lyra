--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")
local auth = require("@lyra/auth")
local playlists = require("@lyra/playlists")
local server = require("@lyra/server")

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
local ACCEPT_LEGACY_AUTH_HEADERS = constants.ACCEPT_LEGACY_AUTH_HEADERS
local module = {}

type ApiContext = api.ApiContext
type ApiQueryParams = api.ApiQueryParams
type ApiResponse = api.ApiResponse
type JsonValue = api.JsonValue
type JsonObject = { [string]: JsonValue }
type LoginResult = auth.LoginResult
export type Principal = auth.Principal
type ResolvedAuth = auth.ResolvedAuth
type PlaylistInfo = playlists.PlaylistInfo
type PlaylistTrackLink = playlists.PlaylistTrackLink
type UserIdentity = {
	user_id: string,
	username: string,
	permissions: { string },
}
type AuthenticateRequest = {
	username: string,
	password: string,
}
type PlaylistRouteRequest = {
	name: string?,
	ids: { number },
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
export type ClientIdentity = {
	client: string,
	device: string,
	device_id: string,
	version: string,
}

-- Mirrors the server: the admin permission grants every other permission.
function module.has_permission(user: { permissions: { string } }, permission_name: string): boolean
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

local function parse_bearer_authorization(header_value: string): string?
	local scheme, token = string.match(header_value, "^%s*([^%s]+)%s+(.+)%s*$")
	if not scheme or token == nil or string.lower(scheme) ~= "bearer" then
		return nil
	end

	return requests.trim(token)
end

local function parse_mediabrowser_params(header_value: string): { [string]: string? }?
	local scheme, remainder = string.match(header_value, "^%s*([^%s]+)%s+(.+)%s*$")
	if not scheme or remainder == nil or string.lower(scheme) ~= "mediabrowser" then
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

local function mediabrowser_header_values(headers: { [string]: string }?): { [string]: string? }
	if headers then
		for _, name in { "authorization", "x-emby-authorization" } do
			local value = headers[name]
			if value then
				local parsed = parse_mediabrowser_params(value)
				if parsed then
					return parsed
				end
			end
		end
	end
	return {}
end

local function header_token(value: string?): string?
	if not value then
		return nil
	end
	local params = parse_mediabrowser_params(value)
	return if params then requests.trim(params.token) else parse_bearer_authorization(value)
end

local function get_query_token(query: ApiQueryParams?): string?
	return requests.parse_query_string(query, { "ApiKey" }, nil)
		or requests.parse_query_string(query, { "api_key" }, nil)
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

-- X-Emby-Token / X-MediaBrowser-Token legacy headers, gated by accept_legacy_auth_headers.
local function get_legacy_header_token(headers: { [string]: string }?): string?
	if not ACCEPT_LEGACY_AUTH_HEADERS then
		return nil
	end
	if type(headers) ~= "table" then
		return nil
	end

	return requests.trim(headers["x-emby-token"]) or requests.trim(headers["x-mediabrowser-token"])
end

function module.to_jellyfin_user(user: UserIdentity): JsonValue
	local auth_enabled = auth.capabilities().enabled
	return {
		Id = item_ids.encode_jellyfin_item_id(user.user_id),
		Name = user.username,
		ServerId = SERVER_ID,
		HasPassword = auth_enabled,
		HasConfiguredPassword = auth_enabled,
		HasConfiguredEasyPassword = false,
		Configuration = preferences.user_configuration(user.user_id),
		Policy = build_jellyfin_user_policy(user),
	}
end

function module.to_authentication_result(login_result: LoginResult): JsonValue
	local principal = login_result.principal
	return {
		User = module.to_jellyfin_user(principal),
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

local function resolve_jellyfin_auth(ctx: ApiContext?): (ResolvedAuth?, string?)
	local request = if ctx then ctx.request else nil
	local headers = if request then request.headers else nil
	local query = if request then request.query else nil

	local token = if headers
		then header_token(headers.authorization) or header_token(headers["x-emby-authorization"])
		else nil
	local requested_user_id = requests.trim(mediabrowser_header_values(headers).userid)
	token = token or get_legacy_header_token(headers) or get_query_token(query)

	local resolved_auth = auth.resolve_auth(token)
	if not resolved_auth then
		return nil, token
	end
	local principal = resolved_auth.principal

	if requested_user_id then
		local decoded_user_id = item_ids.decode_jellyfin_user_id(requested_user_id)
		if decoded_user_id ~= principal.user_id then
			return nil, token
		end
	end

	return resolved_auth, token
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

	local headers = request.headers
	local query = request.query
	local header_values = mediabrowser_header_values(headers)

	local function take(header_key: string, query_keys: { string }): string?
		local header_value = requests.trim(header_values[header_key])
		if header_value then
			return header_value
		end
		return requests.parse_query_string(query, query_keys, nil)
	end

	local client: string? = take("client", { "client", "Client" })
	local device: string? = take("device", { "device", "Device" })
	local device_id: string? = take("deviceid", { "deviceId", "DeviceId", "deviceid" })
	local version: string? = take("version", { "version", "Version" })

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
	if resolved_auth and resolved_auth.credential.kind == "api_key" then
		client = resolved_auth.credential.api_key_name
		device_id = device_id or SERVER_ID
		device = device or "Lyra"
		version = version or server.info().version
	end
	if not client or not device_id then
		return nil,
			"missing Client or DeviceId; send player identity in X-Emby-Authorization or client/deviceId query parameters"
	end

	local identity: ClientIdentity = {
		client = client,
		device = device or client,
		device_id = device_id :: string,
		version = version or "",
	}
	return identity, nil
end

function module.remember_login_identity(ctx: ApiContext, resolved_auth: ResolvedAuth, token: string)
	local identity = module.resolve_client_identity(ctx, resolved_auth, token)
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
	identity: ClientIdentity,
	via_socket: boolean
): (boolean, string?)
	local principal = resolved_auth.principal
	local session_id = module.build_session_id(principal, identity)
	return playback.report_playback_event(resolved_auth, payload, event_name, session_id, {
		session_info = module.build_session_info(session_id, principal, identity, module.current_iso_timestamp()),
		via_socket = via_socket,
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
	return report_client_playback(resolved_auth, payload, event_name, identity, true)
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

	local ok, report_err = report_client_playback(resolved_auth, payload, event_name, identity, false)
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

function module.require_matching_user_scope(ctx: ApiContext?, principal: Principal): boolean
	local params = if ctx then ctx.params else nil
	local path_user_id = if params then requests.trim(params.userId) else nil
	if path_user_id and item_ids.decode_jellyfin_user_id(path_user_id) ~= principal.user_id then
		return false
	end

	local query = if ctx and ctx.request then ctx.request.query else nil
	local query_user_id = requests.parse_query_string(query, { "userId", "UserId", "userid" }, nil)
	if query_user_id and item_ids.decode_jellyfin_user_id(query_user_id) ~= principal.user_id then
		return false
	end

	return true
end

function module.require_matching_user_scope_from_id_param(ctx: ApiContext?, principal: Principal): boolean
	local params = if ctx then ctx.params else nil
	local path_user_id = if params then requests.trim(params.id) else nil
	if path_user_id and item_ids.decode_jellyfin_user_id(path_user_id) ~= principal.user_id then
		return false
	end

	return module.require_matching_user_scope(ctx, principal)
end

function module.current_unix_seconds(): number
	return os.time()
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
		ids = item_ids.parse_jellyfin_ids_array(requested_ids),
		requested_ids = requested_ids,
		has_invalid_ids = has_non_string_ids(raw_ids)
			or raw_ids_contain_null(if request then request.body_raw else nil),
		user_id = requested_user_id,
		is_public = is_public,
	},
		nil
end

function module.parse_playlist_query_track_ids(query: ApiQueryParams?): { number }
	local raw_ids = requests.parse_query_string(query, { "ids", "Ids" }, nil)
	return item_ids.parse_jellyfin_ids_csv(raw_ids)
end

function module.parse_playlist_query_entry_ids(query: ApiQueryParams?, playlist: PlaylistInfo): { string }?
	local raw = requests.parse_query_string(query, { "entryIds", "EntryIds", "entryids" }, nil)
	if not raw then
		return nil
	end
	local raw_ids: { string } = {}
	for _, value in string.split(raw, ",") do
		local trimmed = requests.trim(value)
		if not trimmed then
			return nil
		end
		table.insert(raw_ids, trimmed)
	end
	return playlist_access.resolve_entry_ids(playlist, raw_ids)
end

function module.ensure_user_scope_matches_principal(requested_user_id: string?, principal: Principal): boolean
	if requested_user_id and item_ids.decode_jellyfin_user_id(requested_user_id) ~= principal.user_id then
		return false
	end

	return true
end

function module.resolve_playlist_for_route(ctx: ApiContext?): (PlaylistInfo?, ApiResponse?)
	local params = if ctx then ctx.params else nil
	local playlist_id = if params then params.playlistId else nil
	if not playlist_id then
		return nil, responses.bad_request("missing playlist id")
	end

	local playlist = playlist_access.find(item_ids.decode_jellyfin_item_id(playlist_id))
	if not playlist then
		return nil, responses.not_found("playlist not found")
	end

	return playlist, nil
end

function module.ensure_playlist_visible(playlist: PlaylistInfo, principal: Principal): (boolean, ApiResponse?)
	if playlist_access.can_view(playlist, principal.user_id) then
		return true, nil
	end

	return false, responses.forbidden("playlist not accessible")
end

function module.ensure_playlist_editable(playlist: PlaylistInfo, principal: Principal): (boolean, ApiResponse?)
	if playlist_access.can_edit(playlist, principal.user_id) then
		return true, nil
	end

	return false, responses.forbidden("playlist is read-only")
end

local function playlist_item_ids(playlist: PlaylistInfo): { string }
	local encoded_ids: { string } = {}
	for _, track_ref: PlaylistTrackLink in playlist_access.track_refs(playlist) do
		local item_id = item_ids.encode_jellyfin_item_id(track_ref.track_id)
		if item_id then
			table.insert(encoded_ids, item_id)
		end
	end

	return encoded_ids
end

function module.to_playlist_info(playlist: PlaylistInfo): PlaylistResponseInfo
	local owner_id = playlist_access.owner_id(playlist)
	local shares: { PlaylistUserPermission } = {}
	if owner_id then
		table.insert(shares, {
			UserId = item_ids.encode_jellyfin_item_id(owner_id) or "",
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

function module.to_playlist_user_permission(playlist: PlaylistInfo, user_id: string): PlaylistUserPermission
	local owner_id = playlist_access.owner_id(playlist)
	return {
		UserId = item_ids.encode_jellyfin_item_id(user_id) or "",
		CanEdit = owner_id ~= nil and owner_id == user_id,
	}
end

local function respond_with_primary_image(ctx: ApiContext?, cover_info: artwork.ResolvedCoverInfo): ApiResponse
	local query = if ctx and ctx.request then ctx.request.query else nil
	local format_query = requests.parse_query_string(query, { "format" }, nil)
	local requested_format = requests.normalize_image_format(format_query)
	if format_query ~= nil and requested_format == nil then
		return responses.bad_request("invalid image format")
	end

	local requested_quality = requests.parse_query_int(query, { "quality" }, nil, 0, 100)
	local requested_max_width = requests.parse_query_int(query, { "MaxWidth" }, nil, 1, nil)
	local requested_max_height = requests.parse_query_int(query, { "MaxHeight" }, nil, 1, nil)

	local image_format = requested_format
	local image_quality = requested_quality
	local use_transforms = image_format ~= nil
		or image_quality ~= nil
		or requested_max_width ~= nil
		or requested_max_height ~= nil

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

	if not use_transforms then
		headers["content-type"] = cover_info.mime_type
		return api.response.file(200, cover_info.path, headers)
	end

	if image_format == nil then
		image_format = "jpg"
	end
	if image_format == "jpg" then
		image_quality = image_quality or 90
	end
	return api.response.file(200, cover_info.path, headers, {
		format = image_format,
		quality = image_quality,
		max_width = requested_max_width,
		max_height = requested_max_height,
	})
end

-- Artwork routes are public, as upstream: an anonymous caller gets any item's image, and a caller
-- with valid credentials only the images of items it can see. Invalid credentials read as none.
function module.handle_primary_image_request(ctx: ApiContext?): ApiResponse
	local params = if ctx then ctx.params else nil
	local image_id = if params then params.imageId else nil
	if not image_id then
		return responses.bad_request("missing image id")
	end

	local item_db_id = item_ids.decode_jellyfin_item_id(image_id)
	if not item_db_id then
		return responses.not_found("image not found")
	end
	local signed_in = module.require_jellyfin_principal(ctx) ~= nil
	local item_cover = if signed_in then artwork.get_cover_info(item_db_id) else artwork.get_artwork_info(item_db_id)
	local cover_info = item_cover
		or catalog.resolve_genre_cover(item_db_id)
		or catalog.resolve_playlist_cover(item_db_id, signed_in)
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
		return artwork.get_artwork_info(catalog.find_artist_db_id_by_name(name))
	end)
end

function module.handle_person_image_request(ctx: ApiContext?): ApiResponse
	return serve_public_image_by_name(ctx, function(name)
		return artwork.get_artwork_info(catalog.find_artist_db_id_by_name(name))
	end)
end

function module.handle_genre_image_request(ctx: ApiContext?): ApiResponse
	return serve_public_image_by_name(ctx, catalog.resolve_genre_cover_by_name)
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

local function to_string_array(value: JsonValue): { JsonValue }?
	if type(value) ~= "table" then
		return nil
	end
	local result: { JsonValue } = {}
	for _, item: JsonValue in value :: { JsonValue } do
		if type(item) == "string" then
			table.insert(result, item)
		end
	end
	return result
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
	return result
end

-- Non-/Full variant carries capability hints in query params. Only writes
-- keys it actually sees so the caller's merge logic never clobbers fields
-- the client didn't send.
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
	return result
end

function module.parse_client_capabilities(body: JsonValue): JsonObject?
	if type(body) ~= "table" then
		return nil
	end
	local raw = body :: JsonObject
	local caps: JsonObject = {}
	for _, field in CAPABILITY_STRING_FIELDS do
		local value = raw[field]
		if type(value) == "string" then
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
	return caps
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
	local hex = crypt.hash("xxh3_128", material)
	return string.format(
		"%s-%s-%s-%s-%s",
		hex:sub(1, 8),
		hex:sub(9, 12),
		hex:sub(13, 16),
		hex:sub(17, 20),
		hex:sub(21, 32)
	)
end

function module.build_session_info(
	session_id: string,
	principal: Principal,
	identity: ClientIdentity,
	activity_iso: string
): JsonObject
	return {
		Id = session_id,
		UserId = item_ids.encode_jellyfin_item_id(principal.user_id),
		UserName = principal.username,
		Client = identity.client,
		DeviceName = identity.device,
		DeviceId = identity.device_id,
		ApplicationVersion = identity.version,
		ServerId = SERVER_ID,
		LastActivityDate = activity_iso,
		LastPlaybackCheckIn = activity_iso,
		IsActive = true,
		SupportsRemoteControl = true,
		HasCustomDeviceName = false,
		PlayableMediaTypes = { "Audio" },
		SupportedCommands = {},
		AdditionalUsers = {},
	}
end

local function iso_timestamp(unix_seconds: number): string
	return os.date("!%Y-%m-%dT%H:%M:%SZ", unix_seconds) :: string
end

function module.current_iso_timestamp(): string
	return iso_timestamp(os.time())
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
		LastUserId = item_ids.encode_jellyfin_item_id(entry.user_id),
		LastUserName = entry.username,
		DateLastActivity = iso_timestamp(entry.last_seen_at),
	}
end

type DeviceRow = { device: JsonObject, last_seen_at: number }

-- One device per login across all users, most recently active first.
local function list_devices(): { JsonObject }
	local rows: { DeviceRow } = {}
	for _, entry in auth.list_sessions({ all_users = true }) do
		local device = to_device_info(entry)
		if device then
			table.insert(rows, { device = device, last_seen_at = entry.last_seen_at })
		end
	end
	table.sort(rows, function(a: DeviceRow, b: DeviceRow): boolean
		if a.last_seen_at ~= b.last_seen_at then
			return a.last_seen_at > b.last_seen_at
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

local function resolve_item_file_track_id(ctx: ApiContext?): (number?, ApiResponse?)
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

	local track = playback.resolve_track_for_playback(item_ids.decode_jellyfin_item_id(item_id))
	if not track then
		return nil, responses.not_found("item not found")
	end

	local track_id = track.db_id
	if track_id == nil or track_id <= 0 then
		return nil, responses.not_found("item not found")
	end

	return track_id, nil
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
