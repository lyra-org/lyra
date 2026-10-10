--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")
local auth = require("@lyra/auth")

local auth_protocol = require("@self/protocol/auth")
local response = require("@self/protocol/response")

type ApiContext = api.ApiContext
type ApiResponse = api.ApiResponse
type ResolvedAuth = auth.ResolvedAuth

local EXTENSIONS = {
	{ name = "apiKeyAuthentication", versions = { 1 } },
}

local public_methods: { [string]: (ctx: ApiContext) -> ApiResponse } = {
	getOpenSubsonicExtensions = function(ctx)
		return response.ok(ctx.request.query, { openSubsonicExtensions = EXTENSIONS })
	end,
}

local methods: { [string]: (ctx: ApiContext, resolved: ResolvedAuth) -> ApiResponse } = {
	ping = function(ctx, _resolved)
		return response.ok(ctx.request.query)
	end,
	tokenInfo = function(ctx, resolved)
		return response.ok(ctx.request.query, { tokenInfo = { username = resolved.principal.username } })
	end,
}

local function dispatch(ctx: ApiContext): ApiResponse
	local query = ctx.request.query
	local name = string.gsub(ctx.params.method, "%.view$", "")

	local public_method = public_methods[name]
	if public_method then
		return public_method(ctx)
	end

	local method = methods[name]
	if not method then
		return response.failed(query, response.error(0, `Unknown method: {name}`), 404)
	end

	local resolved, err = auth_protocol.authenticate(query)
	if not resolved then
		return response.failed(query, err :: response.SubsonicError)
	end
	return method(ctx, resolved)
end

api.get("/rest/{method}", dispatch, "public")
api.post("/rest/{method}", dispatch, "public")
