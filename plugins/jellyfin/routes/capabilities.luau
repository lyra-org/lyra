--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

-- A client declaring what its session can do. As upstream, each declaration replaces the last.

local api = require("@lyra/api")

local active_sessions = require("../protocol/active_sessions")
local responses = require("../protocol/items/responses")
local session = require("../protocol/session")

type ApiContext = api.ApiContext
type ApiResponse = api.ApiResponse

local module = {}

function module.handle(ctx: ApiContext, body_is_object: boolean): ApiResponse
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local identity = session.resolve_client_identity(ctx)
	if not identity then
		return api.response.empty(204)
	end

	local request = if ctx then ctx.request else nil
	local capabilities
	if body_is_object then
		local request_json = if request then request.json else nil
		if request_json ~= nil and type(request_json) ~= "table" then
			return responses.bad_request("invalid capabilities request body")
		end
		capabilities = session.parse_client_capabilities(request_json)
	else
		capabilities = session.parse_client_capabilities_query(if request then request.query else nil)
	end
	if capabilities then
		active_sessions.declare_capabilities(session.build_session_id(principal, identity), principal, capabilities)
	end
	return api.response.empty(204)
end

return module
