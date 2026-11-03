--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")

local request = require("../protocol/request")
local response = require("../protocol/response")
local sonic = require("./sonic")

type ApiQueryParams = api.ApiQueryParams
type ApiResponse = api.ApiResponse
type Request = request.Request

local EXTENSIONS = {
	{ name = "apiKeyAuthentication", versions = { 1 } },
	{ name = "formPost", versions = { 1 } },
	{ name = "indexBasedQueue", versions = { 1 } },
	{ name = "songLyrics", versions = { 1 } },
	{ name = "topSongsByArtistId", versions = { 1 } },
	{ name = "transcodeOffset", versions = { 1 } },
}

local SONIC_SIMILARITY = { name = "sonicSimilarity", versions = { 1 } }

-- Methods served without authentication.
local public_methods: { [string]: (params: ApiQueryParams) -> ApiResponse } = {
	getOpenSubsonicExtensions = function(params: ApiQueryParams): ApiResponse
		local extensions = EXTENSIONS
		if sonic.available() then
			extensions = table.clone(EXTENSIONS)
			table.insert(extensions, SONIC_SIMILARITY)
		end
		return response.ok(params, { openSubsonicExtensions = extensions })
	end,
}

local module = {}

module.public_methods = public_methods

module.methods = {
	ping = function(req: Request): ApiResponse
		return response.ok(req.params)
	end,
	-- Lyra needs no license, so it is always valid and never expires.
	getLicense = function(req: Request): ApiResponse
		return response.ok(req.params, { license = { valid = true } })
	end,
	tokenInfo = function(req: Request): ApiResponse
		return response.ok(req.params, { tokenInfo = { username = req.auth.principal.username } })
	end,
}

return module
