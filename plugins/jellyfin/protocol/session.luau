--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")
local auth = require("@lyra/auth")
local playback_sessions = require("@lyra/playback_sessions")
local playlists = require("@lyra/playlists")

local crypt = require("@harmony/crypt")

local base = require("./items/common")
local catalog = require("./items/catalog")
local constants = require("../constants")
local playback = require("./items/playback")

local SERVER_ID = constants.SERVER_ID
local AUTH_ENABLED = constants.AUTH_ENABLED
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
	user_id: number,
	username: string,
}
type AuthenticateRequest = {
	username: string,
	password: string,
}
type PlaylistRouteRequest = {
	name: string?,
	ids: { number },
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

function module.has_permission(principal: Principal, permission_name: string): boolean
	for _, permission in principal.permissions do
		if permission == permission_name then
			return true
		end
	end
	return false
end

function module.is_admin(principal: Principal): boolean
	return module.has_permission(principal, "admin")
end

function module.can_control_user(controller: Principal, target_user_id: number): boolean
	return controller.user_id == target_user_id or module.is_admin(controller)
end

local function json_text_value(value: JsonValue): string?
	if value == nil then
		return nil
	end

	return base.trim(tostring(value))
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

local function parse_bearer_authorization(header_value: string): string?
	local scheme, token = string.match(header_value, "^%s*([^%s]+)%s+(.+)%s*$")
	if not scheme or token == nil or string.lower(scheme) ~= "bearer" then
		return nil
	end

	return base.trim(token)
end

local function parse_mediabrowser_params(header_value: string): { [string]: string? }?
	local scheme, remainder = string.match(header_value, "^%s*([^%s]+)%s+(.+)%s*$")
	if not scheme or remainder == nil or string.lower(scheme) ~= "mediabrowser" then
		return nil
	end

	local values: { [string]: string? } = {}
	for _, part: string in base.split_csv_preserving_quotes(remainder) do
		local key, raw_value = string.match(part, "^%s*([^=]+)%s*=%s*(.-)%s*$")
		if key and raw_value then
			local normalized_key = string.lower(base.trim(key) or "")
			if normalized_key ~= "" then
				values[normalized_key] = base.unquote(raw_value)
			end
		end
	end
	return values
end

local function parse_mediabrowser_authorization(header_value: string): (string?, string?, string?)
	local values = parse_mediabrowser_params(header_value)
	if not values then
		return nil, nil, nil
	end
	return base.trim(values.token), base.trim(values.userid), base.trim(values.deviceid)
end

local function extract_header_identity(header_value: string?): (string?, string?)
	if type(header_value) ~= "string" then
		return nil, nil
	end

	local token, user_id = parse_mediabrowser_authorization(header_value)
	if token then
		return token, user_id
	end

	return parse_bearer_authorization(header_value), nil
end

local function extract_header_device_id(header_value: string?): string?
	if type(header_value) ~= "string" then
		return nil
	end

	local _, _, device_id = parse_mediabrowser_authorization(header_value)
	return base.trim(device_id)
end

local function get_query_token(query: ApiQueryParams?): string?
	if type(query) ~= "table" then
		return nil
	end

	return base.trim(query.ApiKey) or base.trim(query.api_key)
end

local function build_jellyfin_user_configuration(): JsonObject
	return {
		PlayDefaultAudioTrack = false,
		DisplayMissingEpisodes = false,
		GroupedFolders = {},
		SubtitleMode = "Default",
		DisplayCollectionsView = true,
		EnableLocalPassword = false,
		OrderedViews = {},
		LatestItemsExcludes = {},
		MyMediaExcludes = {},
		HidePlayedInLatest = false,
		RememberAudioSelections = false,
		RememberSubtitleSelections = false,
		EnableNextEpisodeAutoPlay = false,
	}
end

local function build_jellyfin_user_policy(is_administrator: boolean, enable_shared_device_control: boolean): JsonObject
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
		EnableSharedDeviceControl = enable_shared_device_control,
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
		EnableContentDownloading = true,
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

	local raw = headers["x-emby-token"] or headers["x-mediabrowser-token"]
	if type(raw) ~= "string" then
		return nil
	end

	return base.trim(raw)
end

function module.to_jellyfin_user(principal: UserIdentity): JsonValue
	return {
		Id = base.encode_jellyfin_item_id(principal.user_id),
		Name = principal.username,
		ServerId = SERVER_ID,
		HasPassword = AUTH_ENABLED,
		HasConfiguredPassword = AUTH_ENABLED,
		HasConfiguredEasyPassword = false,
		Configuration = build_jellyfin_user_configuration(),
		Policy = build_jellyfin_user_policy(false, false),
	}
end

function module.to_jellyfin_principal_user(principal: Principal): JsonValue
	local is_admin = module.is_admin(principal)
	return {
		Id = base.encode_jellyfin_item_id(principal.user_id),
		Name = principal.username,
		ServerId = SERVER_ID,
		HasPassword = AUTH_ENABLED,
		HasConfiguredPassword = AUTH_ENABLED,
		HasConfiguredEasyPassword = false,
		Configuration = build_jellyfin_user_configuration(),
		Policy = build_jellyfin_user_policy(is_admin, true),
	}
end

function module.to_authentication_result(login_result: LoginResult): JsonValue
	local principal = login_result.principal
	return {
		User = module.to_jellyfin_principal_user(principal),
		AccessToken = tostring(login_result.token),
		ServerId = SERVER_ID,
		SessionInfo = nil,
	}
end

function module.parse_authenticate_request(ctx: ApiContext?): (AuthenticateRequest?, ApiResponse?)
	local request = if ctx then ctx.request else nil
	local request_json = if request then request.json else nil
	if type(request_json) ~= "table" then
		return nil, base.bad_request("invalid authentication request body")
	end
	local request_object = request_json :: JsonObject

	local username = json_text_value(request_object.Username or request_object.username)
	if not username then
		return nil, base.bad_request("missing Username")
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

	local auth_header = if headers then (headers.authorization or headers.Authorization) else nil
	local token, requested_user_id = extract_header_identity(auth_header)
	token = token or get_legacy_header_token(headers) or get_query_token(query)

	local resolved_auth = auth.resolve_auth(token)
	if not resolved_auth then
		return nil, token
	end
	local principal = resolved_auth.principal

	if requested_user_id then
		local decoded_user_id = base.decode_jellyfin_item_id(requested_user_id)
		if decoded_user_id ~= principal.user_id then
			return nil, token
		end
	end

	return resolved_auth, token
end

-- Browser WebSocket API cannot set custom headers on upgrade, so clients ride
-- the `client` / `device` / `deviceId` / `version` query params instead.
function module.resolve_client_identity(ctx: ApiContext?): ClientIdentity?
	local request = if ctx then ctx.request else nil
	if not request then
		return nil
	end

	local headers = request.headers
	local query = request.query
	local header_values: { [string]: string? } = {}
	local auth_header = if headers then (headers.authorization or headers.Authorization) else nil
	if type(auth_header) == "string" then
		header_values = parse_mediabrowser_params(auth_header) or header_values
	end

	local function take(header_key: string, query_keys: { string }): string?
		local header_value = base.trim(header_values[header_key])
		if header_value then
			return header_value
		end
		return base.parse_query_string(query, query_keys, nil)
	end

	local client = take("client", { "client", "Client" })
	local device = take("device", { "device", "Device" })
	local device_id = take("deviceid", { "deviceId", "DeviceId", "deviceid" })
	local version = take("version", { "version", "Version" })

	if not client or not device_id then
		return nil
	end

	return {
		client = client,
		device = device or client,
		device_id = device_id,
		version = version or "",
	}
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

-- WS counterpart to handle_playback_report_route; auth and device_id come
-- from the socket handshake, not per-frame headers.
function module.handle_ws_playback_event(
	resolved_auth: ResolvedAuth,
	data: JsonValue,
	event_name: string,
	device_id: string?
): boolean
	local payload = playback.as_playback_payload(data)
	if not payload or not playback.ensure_payload_user_matches(resolved_auth, payload) then
		return false
	end
	local ok, _err = playback.report_playback_event(resolved_auth, payload, event_name, device_id)
	return ok
end

function module.handle_playback_report_route(ctx: ApiContext?, event_name: string): ApiResponse
	local resolved_auth, _token = module.require_jellyfin_auth(ctx)
	if not resolved_auth then
		return module.unauthorized()
	end

	local payload, parse_err = playback.parse_playback_report_body(ctx)
	if not payload then
		return parse_err or base.bad_request("invalid playback report body")
	end
	if not playback.ensure_payload_user_matches(resolved_auth, payload) then
		return module.unauthorized()
	end

	local request = if ctx then ctx.request else nil
	local headers = if request then request.headers else nil
	local auth_header = if headers then (headers.authorization or headers.Authorization) else nil
	local request_device_id = extract_header_device_id(auth_header)

	local ok, report_err = playback.report_playback_event(resolved_auth, payload, event_name, request_device_id)
	if not ok then
		return report_err or base.bad_request("failed to report playback event")
	end

	return api.response.empty(204)
end

function module.require_matching_user_scope(ctx: ApiContext?, principal: Principal): boolean
	local params = if ctx then ctx.params else nil
	local path_user_id = if params then base.trim(params.userId) else nil
	if path_user_id and base.decode_jellyfin_item_id(path_user_id) ~= principal.user_id then
		return false
	end

	local query = if ctx and ctx.request then ctx.request.query else nil
	local query_user_id = base.parse_query_string(query, { "userId", "UserId", "userid" }, nil)
	if query_user_id and base.decode_jellyfin_item_id(query_user_id) ~= principal.user_id then
		return false
	end

	return true
end

function module.require_matching_user_scope_from_id_param(ctx: ApiContext?, principal: Principal): boolean
	local params = if ctx then ctx.params else nil
	local path_user_id = if params then base.trim(params.id) else nil
	if path_user_id and base.decode_jellyfin_item_id(path_user_id) ~= principal.user_id then
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

	return base.trim(tostring(value))
end

function module.parse_playlist_request_body(ctx: ApiContext?): (PlaylistRouteRequest?, ApiResponse?)
	local request = if ctx then ctx.request else nil
	local request_json = if request then request.json else nil
	if type(request_json) ~= "table" then
		return nil, base.bad_request("invalid playlist request body")
	end
	local request_object = request_json :: JsonObject

	local requested_user_id = module.normalize_optional_user_id(request_object.UserId or request_object.userId)
	local raw_is_public = request_object.IsPublic
	local is_public = nil
	if raw_is_public ~= nil then
		if type(raw_is_public) ~= "boolean" then
			return nil, base.bad_request("invalid IsPublic value")
		end
		is_public = raw_is_public
	end

	return {
		name = json_text_value(request_object.Name or request_object.name),
		ids = base.parse_jellyfin_ids_array(jellyfin_ids_input(request_object.Ids or request_object.ids)),
		user_id = requested_user_id,
		is_public = is_public,
	},
		nil
end

function module.parse_playlist_query_track_ids(query: ApiQueryParams?): { number }
	local raw_ids = base.parse_query_string(query, { "ids", "Ids" }, nil)
	return base.parse_jellyfin_ids_csv(raw_ids)
end

function module.parse_playlist_query_entry_ids(query: ApiQueryParams?): { number }
	local raw_entry_ids = base.parse_query_string(query, { "entryIds", "EntryIds", "entryids" }, nil)
	return base.parse_jellyfin_ids_csv(raw_entry_ids)
end

function module.ensure_user_scope_matches_principal(requested_user_id: string?, principal: Principal): boolean
	if requested_user_id and base.decode_jellyfin_item_id(requested_user_id) ~= principal.user_id then
		return false
	end

	return true
end

function module.resolve_playlist_for_route(ctx: ApiContext?): (PlaylistInfo?, ApiResponse?)
	local params = if ctx then ctx.params else nil
	local playlist_id = if params then params.playlistId else nil
	if not playlist_id then
		return nil, base.bad_request("missing playlist id")
	end

	local playlist = catalog.find_playlist_by_id(base.decode_jellyfin_item_id(playlist_id))
	if not playlist then
		return nil, base.not_found("playlist not found")
	end

	return playlist, nil
end

function module.ensure_playlist_visible(playlist: PlaylistInfo, principal: Principal): (boolean, ApiResponse?)
	if catalog.playlist_can_view(playlist, principal.user_id) then
		return true, nil
	end

	return false, base.forbidden("playlist not accessible")
end

function module.ensure_playlist_editable(playlist: PlaylistInfo, principal: Principal): (boolean, ApiResponse?)
	if catalog.playlist_can_edit(playlist, principal.user_id) then
		return true, nil
	end

	return false, base.forbidden("playlist is read-only")
end

local function playlist_item_ids(playlist: PlaylistInfo): { string }
	local item_ids: { string } = {}
	for _, track_ref: PlaylistTrackLink in catalog.playlist_track_refs(playlist) do
		local item_id = base.encode_jellyfin_item_id(track_ref.track_id)
		if item_id then
			table.insert(item_ids, item_id)
		end
	end

	return item_ids
end

function module.to_playlist_info(playlist: PlaylistInfo): PlaylistResponseInfo
	local owner_id = catalog.playlist_owner_id(playlist)
	local shares: { PlaylistUserPermission } = {}
	if owner_id then
		table.insert(shares, {
			UserId = tostring(owner_id),
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

function module.to_playlist_user_permission(playlist: PlaylistInfo, user_id: number): PlaylistUserPermission
	local owner_id = catalog.playlist_owner_id(playlist)
	return {
		UserId = base.encode_jellyfin_item_id(user_id) or "",
		CanEdit = owner_id ~= nil and owner_id == user_id,
	}
end

local function resolve_primary_image_cover_info(item_db_id: number?): base.ResolvedCoverInfo?
	local cover_info = base.get_cover_info(item_db_id)
	if not cover_info and item_db_id then
		cover_info = catalog.resolve_genre_cover(item_db_id)
	end
	if not cover_info and item_db_id then
		cover_info = catalog.resolve_playlist_cover(item_db_id)
	end
	return cover_info
end

local function respond_with_primary_image(ctx: ApiContext?, cover_info: base.ResolvedCoverInfo): ApiResponse
	local query = if ctx and ctx.request then ctx.request.query else nil
	local format_query = base.parse_query_string(query, { "format" }, nil)
	local requested_format = base.normalize_image_format(format_query)
	if format_query ~= nil and requested_format == nil then
		return base.bad_request("invalid image format")
	end

	local requested_quality = base.parse_query_int(query, { "quality" }, nil, 0, 100)
	local requested_max_width = base.parse_query_int(query, { "MaxWidth" }, nil, 1, nil)
	local requested_max_height = base.parse_query_int(query, { "MaxHeight" }, nil, 1, nil)

	local image_format = requested_format
	local image_quality = requested_quality
	local use_transforms = image_format ~= nil
		or image_quality ~= nil
		or requested_max_width ~= nil
		or requested_max_height ~= nil

	local headers = {
		["etag"] = cover_info.hash,
	}

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

local function serve_primary_image_for_item(ctx: ApiContext?, item_db_id: number?): ApiResponse
	local cover_info = resolve_primary_image_cover_info(item_db_id)
	if not cover_info then
		return base.not_found("image not found")
	end
	return respond_with_primary_image(ctx, cover_info)
end

function module.handle_primary_image_request(ctx: ApiContext?): ApiResponse
	local principal = module.require_jellyfin_principal(ctx)
	if not principal then
		return module.unauthorized()
	end

	local params = if ctx then ctx.params else nil
	local image_id = if params then params.imageId else nil
	if not image_id then
		return base.bad_request("missing image id")
	end

	return serve_primary_image_for_item(ctx, base.decode_jellyfin_item_id(image_id))
end

function module.handle_artist_image_request(ctx: ApiContext?): ApiResponse
	local principal = module.require_jellyfin_principal(ctx)
	if not principal then
		return module.unauthorized()
	end

	local params = if ctx then ctx.params else nil
	local name = if params then params.name else nil
	if not name or name == "" then
		return base.bad_request("missing artist name")
	end

	local image_type = if params then params.imageType else nil
	if type(image_type) ~= "string" or string.lower(image_type) ~= "primary" then
		return base.not_found("image not found")
	end

	local raw_image_index = if params then params.imageIndex else nil
	local image_index = if type(raw_image_index) == "string" then tonumber(raw_image_index) else nil
	if image_index == nil or image_index ~= math.floor(image_index) or image_index < 0 then
		return base.bad_request("invalid image index")
	end
	if image_index ~= 0 then
		return base.not_found("image not found")
	end

	local artist = catalog.find_artist_by_name(name, principal.user_id)
	if not artist or not artist.Id then
		return base.not_found("artist not found")
	end

	return serve_primary_image_for_item(ctx, base.decode_jellyfin_item_id(artist.Id))
end

function module.handle_person_image_request(ctx: ApiContext?): ApiResponse
	local principal = module.require_jellyfin_principal(ctx)
	if not principal then
		return module.unauthorized()
	end

	local params = if ctx then ctx.params else nil
	local name = if params then params.name else nil
	if not name or name == "" then
		return base.bad_request("missing person name")
	end

	local image_type = if params then params.imageType else nil
	if type(image_type) ~= "string" or string.lower(image_type) ~= "primary" then
		return base.not_found("image not found")
	end

	local raw_image_index = if params then params.imageIndex else nil
	local image_index = if type(raw_image_index) == "string" then tonumber(raw_image_index) else 0
	if image_index == nil or image_index ~= math.floor(image_index) or image_index < 0 then
		return base.bad_request("invalid image index")
	end
	if image_index ~= 0 then
		return base.not_found("image not found")
	end

	local person = catalog.find_person_by_name(name, principal.user_id)
	if not person or not person.Id then
		return base.not_found("person not found")
	end

	return serve_primary_image_for_item(ctx, base.decode_jellyfin_item_id(person.Id))
end

local MAX_SESSION_KEY_DISPLAY_LENGTH = 64

-- Client-supplied; strip control/NUL to keep the value safe in displayable fields.
local function sanitize_session_key(raw: string): string
	local trimmed = if #raw <= MAX_SESSION_KEY_DISPLAY_LENGTH
		then raw
		else string.sub(raw, 1, MAX_SESSION_KEY_DISPLAY_LENGTH)
	return (string.gsub(trimmed, "[%c%z]", ""))
end

-- Whitelist of capability fields the plugin forwards to callers of
-- GET /Sessions. Anything outside this set (DeviceProfile, IconUrl, etc.) is
-- dropped to keep the stored object bounded.
local CAPABILITY_STRING_FIELDS = { "AppStoreUrl", "IconUrl" }
local CAPABILITY_BOOL_FIELDS = {
	"SupportsMediaControl",
	"SupportsPersistentIdentifier",
	"SupportsSync",
	"SupportsContentUploading",
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
		local trimmed = base.trim(part)
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
	local commands = parse_csv_array(base.parse_query_string(query, { "SupportedCommands", "supportedCommands" }, nil))
	if commands then
		result.SupportedCommands = commands
	end
	local media_types =
		parse_csv_array(base.parse_query_string(query, { "PlayableMediaTypes", "playableMediaTypes" }, nil))
	if media_types then
		result.PlayableMediaTypes = media_types
	end
	for _, key in { "SupportsMediaControl", "SupportsPersistentIdentifier", "SupportsSync" } do
		local raw = base.parse_query_string(query, { key }, nil)
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
		UserId = base.encode_jellyfin_item_id(principal.user_id),
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

function module.current_iso_timestamp(): string
	return os.date("!%Y-%m-%dT%H:%M:%SZ", os.time()) :: string
end

function module.devices_info_response(ctx: ApiContext?, user_id: number): ApiResponse
	local query = if ctx and ctx.request then ctx.request.query else nil
	local requested_device_id = base.parse_query_string(query, { "id", "Id" }, nil)

	local ok, connections = pcall(playback_sessions.list_connections, user_id)
	if not ok or type(connections) ~= "table" then
		return api.response.empty(500)
	end

	local user_encoded_id = base.encode_jellyfin_item_id(user_id)
	local activity_iso = os.date("!%Y-%m-%dT%H:%M:%SZ", os.time())

	local function build_entry(conn): JsonObject
		local short_id = sanitize_session_key(conn.session_key)
		return {
			Id = short_id,
			Name = "Lyra Client",
			LastUserId = user_encoded_id,
			AppName = "Lyra",
			DateLastActivity = activity_iso,
		}
	end

	if requested_device_id then
		for _, conn in connections do
			if conn.session_key == requested_device_id then
				return api.response.json(200, build_entry(conn))
			end
		end
		return base.not_found("device not found")
	end

	local items: { JsonValue } = {}
	for _, conn in connections do
		table.insert(items, build_entry(conn))
	end
	return api.response.json(200, {
		Items = items,
		TotalRecordCount = #items,
		StartIndex = 0,
	})
end

function module.handle_item_file_stream_request(ctx: ApiContext?): ApiResponse
	local principal = module.require_jellyfin_principal(ctx)
	if not principal then
		return module.unauthorized()
	end

	if not module.require_matching_user_scope(ctx, principal) then
		return module.unauthorized()
	end

	local params = if ctx then ctx.params else nil
	local item_id = if params then params.itemId else nil
	if not item_id then
		return base.bad_request("missing item id")
	end

	local track = playback.resolve_track_for_playback(base.decode_jellyfin_item_id(item_id))
	if not track then
		return base.not_found("item not found")
	end

	local track_id = track.db_id
	if track_id == nil or track_id <= 0 then
		return base.not_found("item not found")
	end

	return api.response.stream_track(track_id)
end

return module
