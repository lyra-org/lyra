--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local plugins = require("@lyra/plugins")

local encoding = require("./encoding")

type SettingsBuilder = plugins.SettingsBuilder
type SettingsConfig = plugins.SettingsConfig

local module = {
	SERVER_ID = "",
	AUTH_ENABLED = false,
	JELLYFIN_PLUGIN_ID = "jellyfin",
	ITEM_ID_HEX_LENGTH = 32,
	ITEM_ID_HEX_PATTERN = "^[0-9a-f]+$",
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
}

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
		label = "Accept legacy auth headers",
		description = "Accept X-Emby-Token and X-MediaBrowser-Token in addition to the canonical Authorization header. Jellyfin itself gates these behind EnableLegacyAuthorization.",
		default = true,
	})
end

function module.configure_server_id(server_id: string)
	module.SERVER_ID = encoding.nanoid_to_hex(server_id) or server_id
end

function module.configure_auth_enabled(enabled: boolean)
	module.AUTH_ENABLED = enabled
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
end

return module
