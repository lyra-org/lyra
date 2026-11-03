--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")
local plugins = require("@lyra/plugins")

local dispatch = require("@self/dispatch")
local sonic = require("@self/endpoints/sonic")

local config = plugins.declare_settings(function(builder)
	builder:group("sonic", "Sonic similarity")
	builder:string("sonic_mixer", {
		label = "Mixer",
		description = "The ID of the mixer that finds sonically similar songs and paths between songs. Empty turns sonic similarity off.",
		default = "",
	})
end)
sonic.configure(config.sonic_mixer :: string?)

api.get("/rest/{method}", dispatch.dispatch, { auth = "public" })
api.post("/rest/{method}", dispatch.dispatch, { auth = "public" })
