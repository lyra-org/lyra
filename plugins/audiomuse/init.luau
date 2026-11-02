--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local http = require("@harmony/http")
local mix = require("@lyra/mix")
local plugins = require("@lyra/plugins")

local client = require("@self/client")
local jellyfin_hex = require("@self/jellyfin_hex")
local mixes = require("@self/mixes")
local routes = require("@self/routes")
local startup = require("@self/startup")

local config = plugins.declare_settings(function(builder)
	builder:group("connection", "Connection")
	builder:string("base_url", {
		label = "Base URL",
		description = "AudioMuse server base URL.",
		default = "",
	})
	builder:string("token", {
		label = "Token",
		description = "Authentication token for the AudioMuse API.",
		default = "",
	})
	builder:choice("media_server", {
		label = "Media server",
		description = "The API the AudioMuse server indexes Lyra through.",
		default = "jellyfin",
		options = {
			{ value = "jellyfin", label = "Jellyfin" },
			{ value = "opensubsonic", label = "OpenSubsonic" },
		},
	})
	builder:string("server", {
		label = "AudioMuse server",
		description = "The name or ID Lyra is registered under in AudioMuse. Empty uses AudioMuse's default server.",
		default = "",
	})
end)

local BASE_URL = config.base_url :: string
local MEDIA_SERVER = config.media_server :: string

if not BASE_URL or BASE_URL == "" then
	warn("base_url not configured; skipping initialization.")
	return
end

if plugins.get(MEDIA_SERVER) == nil then
	warn(`media server plugin {MEDIA_SERVER} is not installed; skipping initialization.`)
	return
end

client.configure({
	base_url = BASE_URL,
	token = config.token :: string,
	server = string.match(config.server :: string, "^%s*(.-)%s*$") :: string,
})

http.set_rate_limit({
	domain = BASE_URL,
	requests_per_second = 10,
})

-- The track ids AudioMuse stores: Jellyfin item ids are hex encodings of Lyra ids, while
-- OpenSubsonic song ids are Lyra ids.
local function passthrough_id(id: string): string?
	return id
end
local ids: mixes.TrackIds = if MEDIA_SERVER == "jellyfin"
	then { encode = jellyfin_hex.nanoid_to_hex, decode = jellyfin_hex.hex_to_nanoid }
	else { encode = passthrough_id, decode = passthrough_id }

local mixer = mix.Mixer.new("audiomuse", { display_name = "AudioMuse AI" })
mixes.register(mixer, { ids = ids, clap = startup.check() })
routes.register()
