--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local plugins = require("@lyra/plugins")

local crypt = require("@harmony/crypt")

local item_ids = require("./protocol/items/identifiers")

type SettingsBuilder = plugins.SettingsBuilder
type SettingsConfig = plugins.SettingsConfig

local module = {
	SERVER_ID = "",
	JELLYFIN_PLUGIN_ID = "jellyfin",
	ROOT_ITEM_ID = "",
	PLAYLISTS_FOLDER_ID = "",
	DEFAULT_ITEMS_FIELDS = {
		"ChildCount",
		"DateCreated",
		"DateLastMediaAdded",
		"Etag",
		"Genres",
		"ParentId",
		"ProviderIds",
		"Tags",
	},
	PLAYBACK_COMPLETION_TOLERANCE_MS = 3000,
	RESOLVE_MERGE_TRACK_COUNT = false,
	ROUTE_PREFIX = "/jellyfin",
	DISCOVERY_ENABLED = true,
	DISCOVERY_PORT = 7359,
	ACCEPT_LEGACY_AUTH_HEADERS = true,
	QUICK_CONNECT_ENABLED = false,
}

function module.configure_root_item_id(value: string)
	local normalized = item_ids.normalize_guid(value)
	if normalized == nil then
		error("invalid Jellyfin root item id")
	end
	if normalized == item_ids.EMPTY_GUID then
		error("Jellyfin root item id cannot be the empty GUID")
	end
	module.ROOT_ITEM_ID = normalized
	-- The playlists view has no entity of its own; its id follows the root's.
	module.PLAYLISTS_FOLDER_ID = item_ids.normalize_guid(crypt.hash("xxh3_128", `{normalized}:playlists`)) or ""
end

function module.is_root_item_id(value: string?): boolean
	local normalized = item_ids.normalize_guid(value)
	return normalized == module.ROOT_ITEM_ID or normalized == item_ids.EMPTY_GUID
end

function module.is_playlists_folder_id(value: string?): boolean
	local normalized = item_ids.normalize_guid(value)
	return normalized ~= nil and normalized == module.PLAYLISTS_FOLDER_ID
end

function module.declare_settings(builder: SettingsBuilder)
	builder:group("routes", "Routes")
	builder:string("route_prefix", {
		label = "Route prefix",
		description = "URL path prefix for all Jellyfin-compatible endpoints.",
		default = "/jellyfin",
	})

	builder:group("discovery", "Discovery")
	builder:bool("discovery_enabled", {
		label = "Enable auto-discovery",
		description = "Listen for Jellyfin client discovery broadcasts on UDP port 7359.",
		default = true,
	})

	builder:group("playback", "Playback")
	builder:number("playback_completion_tolerance_ms", {
		label = "Playback completion tolerance (ms)",
		description = "How many milliseconds before the end of a track to consider playback complete.",
		default = 3000,
		min = 0,
	})
	builder:bool("resolve_merge_track_count", {
		label = "Resolve merged track count",
		description = "When counting listens, resolve across merged tracks.",
		default = false,
	})

	builder:group("auth", "Authentication")
	builder:bool("accept_legacy_auth_headers", {
		label = "Accept legacy authorization",
		description = "Accept the legacy credential forms Jellyfin gates behind EnableLegacyAuthorization: the X-Emby-Token and X-MediaBrowser-Token headers, the X-Emby-Authorization header, the api_key query parameter, and the Emby authorization scheme. The Authorization header with the MediaBrowser scheme and the ApiKey query parameter are always accepted.",
		default = true,
	})
	builder:bool("quick_connect_enabled", {
		label = "Quick Connect",
		description = "Let a client sign in by showing a six-digit code that a signed-in device approves. These codes are easier to guess than the six-letter codes of Lyra's own device login: someone with an account who guesses one signs the waiting client in to their own account. Takes effect when the plugin restarts.",
		default = false,
	})
end

function module.configure_server_id(server_id: string)
	module.SERVER_ID = item_ids.encode(server_id) or server_id
end

local function string_setting(config: SettingsConfig, key: string, default_value: string): string
	local value = config[key]
	return if type(value) == "string" then value else default_value
end

local function number_setting(config: SettingsConfig, key: string, default_value: number): number
	local value = config[key]
	return if type(value) == "number" then value else default_value
end

local function bool_setting(config: SettingsConfig, key: string, default_value: boolean): boolean
	local value = config[key]
	return if type(value) == "boolean" then value else default_value
end

function module.configure_settings(config: SettingsConfig)
	module.PLAYBACK_COMPLETION_TOLERANCE_MS =
		number_setting(config, "playback_completion_tolerance_ms", module.PLAYBACK_COMPLETION_TOLERANCE_MS)
	module.RESOLVE_MERGE_TRACK_COUNT =
		bool_setting(config, "resolve_merge_track_count", module.RESOLVE_MERGE_TRACK_COUNT)
	module.ROUTE_PREFIX = string_setting(config, "route_prefix", module.ROUTE_PREFIX)
	module.DISCOVERY_ENABLED = bool_setting(config, "discovery_enabled", module.DISCOVERY_ENABLED)
	module.ACCEPT_LEGACY_AUTH_HEADERS =
		bool_setting(config, "accept_legacy_auth_headers", module.ACCEPT_LEGACY_AUTH_HEADERS)
	module.QUICK_CONNECT_ENABLED = bool_setting(config, "quick_connect_enabled", module.QUICK_CONNECT_ENABLED)
end

return module
