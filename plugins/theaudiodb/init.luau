--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local http = require("@harmony/http")
local metadata = require("@lyra/metadata")
local plugins = require("@lyra/plugins")

local api = require("@self/api")
local handlers = require("@self/handlers")

local config = plugins.declare_settings(function(builder)
	builder:group("api", "API")
	builder:string("api_key", {
		label = "API key",
		description = "TheAudioDB v1 API key. Leave as 123 to use the public free tier.",
		default = api.DEFAULT_API_KEY,
	})
end)

api.configure((config.api_key or api.DEFAULT_API_KEY) :: string)

http.set_rate_limit({
	domain = "www.theaudiodb.com",
	requests_per_second = 0.5,
	retry_on = { 429, 503 },
	max_retries = 2,
	backoff_ms = 2500,
})

handlers.register(metadata.Provider.new("theaudiodb", { display_name = "TheAudioDB" }))
