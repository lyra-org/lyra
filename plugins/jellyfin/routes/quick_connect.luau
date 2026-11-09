--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

-- Upstream's Quick Connect, which Lyra doesn't offer: every route answers as upstream does with
-- Quick Connect turned off, after the checks upstream makes first.

local api = require("@lyra/api")

local constants = require("../constants")
local item_ids = require("../protocol/items/identifiers")
local requests = require("../protocol/items/requests")
local responses = require("../protocol/items/responses")
local session = require("../protocol/session")

local P = constants.ROUTE_PREFIX

type ApiContext = api.ApiContext
type ApiResponse = api.ApiResponse

local module = {}

-- Upstream's exception middleware, outside development.
local ERROR_PROCESSING_REQUEST = "Error processing request."

local function disabled(): ApiResponse
	return api.response.json(401, "Quick connect is disabled")
end

function module.handle_enabled(): ApiResponse
	return api.response.json(200, false)
end

-- Open to anyone, as upstream, which wants the client's identity before it looks at the setting.
function module.handle_initiate(ctx: ApiContext): ApiResponse
	local identity = session.resolve_client_identity(ctx)
	if not identity or identity.version == "" then
		return api.response.text(400, ERROR_PROCESSING_REQUEST)
	end
	return disabled()
end

-- Open to anyone, as upstream.
function module.handle_connect(ctx: ApiContext): ApiResponse
	local query = if ctx and ctx.request then ctx.request.query else nil
	if not requests.parse_query_string(query, { "secret" }, nil) then
		return responses.bad_request("missing secret")
	end
	return disabled()
end

-- Takes a caller, who as upstream may name another user only as an admin.
function module.handle_authorize(ctx: ApiContext): ApiResponse
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	local query = if ctx and ctx.request then ctx.request.query else nil
	if not requests.parse_query_string(query, { "code" }, nil) then
		return responses.bad_request("missing code")
	end
	local user_id = requests.parse_query_string(query, { "userId" }, nil)
	if user_id and item_ids.decode(user_id) ~= principal.user_id and not session.is_admin(principal) then
		return session.forbidden_user()
	end
	return disabled()
end

-- Open to anyone, as upstream, whose failure here is its exception middleware's.
function module.handle_authenticate(ctx: ApiContext): ApiResponse
	local body = if ctx and ctx.request then ctx.request.json else nil
	local secret: api.JsonValue = nil
	if type(body) == "table" then
		for key, value in body :: { [string]: api.JsonValue } do
			if string.lower(key) == "secret" then
				secret = value
			end
		end
	end
	if type(secret) ~= "string" then
		return responses.bad_request("missing Secret")
	end
	return api.response.text(401, ERROR_PROCESSING_REQUEST)
end

function module.register()
	local options: api.ApiRouteOptions = { auth = "public", case_insensitive = true }
	api.get(P .. "/QuickConnect/Enabled", module.handle_enabled, options)
	api.post(P .. "/QuickConnect/Initiate", module.handle_initiate, options)
	api.get(P .. "/QuickConnect/Connect", module.handle_connect, options)
	api.post(P .. "/QuickConnect/Authorize", module.handle_authorize, options)
	api.post(P .. "/Users/AuthenticateWithQuickConnect", module.handle_authenticate, options)
end

return module
