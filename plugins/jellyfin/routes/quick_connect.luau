--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

-- Upstream's Quick Connect on Lyra's device logins. With authentication off Quick Connect is off,
-- and every route answers as upstream's does with it turned off, after the checks upstream makes
-- first. Unlike upstream, the session is created when the client signs in, not when it is
-- authorized.

local api = require("@lyra/api")
local auth = require("@lyra/auth")
local crypt = require("@harmony/crypt")
local errors = require("@lyra/errors")

local constants = require("../constants")
local item_ids = require("../protocol/items/identifiers")
local requests = require("../protocol/items/requests")
local responses = require("../protocol/items/responses")
local session = require("../protocol/session")

local P = constants.ROUTE_PREFIX

type ApiContext = api.ApiContext
type ApiResponse = api.ApiResponse
type JsonValue = api.JsonValue
type LoginIdentity = session.LoginIdentity

local module = {}

-- Upstream's exception middleware, outside development.
local ERROR_PROCESSING_REQUEST = "Error processing request."

-- Upstream's codes: six digits with no leading zero.
local SMALLEST_CODE = 100000
local CODE_COUNT = 900000
local RANDOM_COUNT = 2 ^ 32
-- Draws at or past the last whole multiple of the count would favour the small codes.
local UNBIASED_RANDOM_COUNT = RANDOM_COUNT - RANDOM_COUNT % CODE_COUNT
-- A code another request holds is drawn again; live requests are a sliver of the codes.
local MAX_CODE_ATTEMPTS = 8

local function available(): boolean
	return auth.capabilities().enabled
end

local function generate_code(): string
	while true do
		local drawn = tonumber(crypt.random(4), 16) :: number
		if drawn < UNBIASED_RANDOM_COUNT then
			return tostring(SMALLEST_CODE + drawn % CODE_COUNT)
		end
	end
end

local function disabled(): ApiResponse
	return api.response.json(401, "Quick connect is disabled")
end

local function too_many_requests(retry_after_seconds: number?): ApiResponse
	local headers: { [string]: string } = {}
	if retry_after_seconds then
		headers["retry-after"] = tostring(retry_after_seconds)
	end
	return api.response.text(429, "Too Many Requests", headers)
end

-- The identity a device login began with, read back from its data.
local function saved_identity(saved: JsonValue?): LoginIdentity?
	if type(saved) ~= "table" then
		return nil
	end
	local fields = saved :: { [string]: JsonValue }
	local client, device, device_id, version = fields.client, fields.device, fields.device_id, fields.version
	if
		type(client) ~= "string"
		or type(device) ~= "string"
		or type(device_id) ~= "string"
		or type(version) ~= "string"
	then
		return nil
	end
	return {
		client = client,
		device = device,
		device_id = device_id,
		version = version,
	}
end

local function quick_connect_result(
	secret: string,
	code: string,
	created_at_ms: number,
	authenticated: boolean,
	identity: LoginIdentity?
): ApiResponse
	return api.response.json(200, {
		Authenticated = authenticated,
		Secret = secret,
		Code = code,
		DeviceId = if identity then identity.device_id else "",
		DeviceName = if identity then identity.device else "",
		AppName = if identity then identity.client else "",
		AppVersion = if identity then identity.version else "",
		DateAdded = session.iso_timestamp_ms(created_at_ms),
	})
end

function module.handle_enabled(): ApiResponse
	return api.response.json(200, available())
end

-- Open to anyone, as upstream, which wants the client's identity before it looks at the setting.
function module.handle_initiate(ctx: ApiContext): ApiResponse
	local identity = session.request_login_identity(ctx)
	if not identity then
		return api.response.text(400, ERROR_PROCESSING_REQUEST)
	end
	if not available() then
		return disabled()
	end
	local user_agent, client_name = session.login_metadata(ctx)
	for _ = 1, MAX_CODE_ATTEMPTS do
		local outcome = auth.begin_device_login({
			code = generate_code(),
			client_name = client_name,
			user_agent = user_agent,
			data = identity :: JsonValue,
		})
		local status = outcome.status
		if status == "ok" then
			local begun = outcome.result :: auth.BegunDeviceLogin
			return quick_connect_result(begun.secret, begun.code, begun.created_at_ms, false, identity)
		elseif status == "rate_limited" then
			return too_many_requests(outcome.retry_after_seconds)
		elseif status == "disabled" then
			return disabled()
		elseif status ~= "code_taken" then
			local _: never = status
			error("unreachable")
		end
	end
	error("no free Quick Connect code")
end

-- Open to anyone, as upstream. Only reads the request's state, so polling it signs in no one.
function module.handle_connect(ctx: ApiContext): ApiResponse
	local query = if ctx and ctx.request then ctx.request.query else nil
	local secret = requests.parse_query_string(query, { "secret" }, nil)
	if not secret then
		return responses.bad_request("missing secret")
	end
	if not available() then
		return disabled()
	end
	local outcome = auth.device_login_status(secret)
	local status = outcome.status
	if status == "pending" or status == "approved" then
		local request = outcome.result :: auth.DeviceLoginRequest
		return quick_connect_result(
			secret,
			request.code,
			request.created_at_ms,
			status == "approved",
			saved_identity(request.data)
		)
	elseif status == "not_found" then
		return api.response.json(404, "Unknown secret")
	elseif status == "disabled" then
		return disabled()
	else
		local _: never = status
		error("unreachable")
	end
end

-- Takes a caller, who as upstream may name another user only as an admin.
function module.handle_authorize(ctx: ApiContext): ApiResponse
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	local query = if ctx and ctx.request then ctx.request.query else nil
	local code = requests.parse_query_string(query, { "code" }, nil)
	if not code then
		return responses.bad_request("missing code")
	end
	local user_id = requests.parse_query_string(query, { "userId" }, nil)
	if user_id and item_ids.decode(user_id) ~= principal.user_id and not session.is_admin(principal) then
		return session.forbidden_user()
	end
	if not available() then
		return disabled()
	end
	local target = if user_id then item_ids.decode(user_id) else nil
	if user_id and not target then
		return responses.bad_request("invalid userId")
	end

	local approved, answer = pcall(auth.approve_device_login, code, { user_id = target })
	if not approved then
		-- An API key, which signs no one in, or a user the caller's permissions don't cover.
		if errors.kind(answer) == "forbidden" then
			return session.forbidden_user()
		end
		error(answer, 0)
	end
	local outcome = answer :: auth.ApproveDeviceLoginOutcome
	local status = outcome.status
	if status == "ok" then
		return api.response.json(200, true)
	elseif status == "not_found" then
		return api.response.text(404, ERROR_PROCESSING_REQUEST)
	elseif status == "already_approved" then
		return api.response.text(500, ERROR_PROCESSING_REQUEST)
	elseif status == "rate_limited" then
		return too_many_requests(outcome.retry_after_seconds)
	elseif status == "user_not_found" or status == "disabled" then
		-- Upstream's unknown user is the authentication failure this route answers as disabled.
		return disabled()
	else
		local _: never = status
		error("unreachable")
	end
end

-- Open to anyone, as upstream, whose failures here are its exception middleware's. Repeats sign the
-- client in with the same session, so a client that lost the answer can ask again.
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
	if not available() then
		return api.response.text(401, ERROR_PROCESSING_REQUEST)
	end

	local outcome = auth.claim_device_login(secret)
	local status = outcome.status
	if status == "ok" then
		local login_result = outcome.result :: auth.LoginResult
		local resolved_auth = auth.resolve_auth(login_result.token)
		if resolved_auth then
			session.remember_login_identity(ctx, resolved_auth, login_result.token, saved_identity(outcome.data))
		end
		return api.response.json(200, session.to_authentication_result(login_result))
	elseif status == "pending" or status == "not_found" then
		return api.response.text(404, ERROR_PROCESSING_REQUEST)
	elseif status == "disabled" then
		return api.response.text(401, ERROR_PROCESSING_REQUEST)
	else
		local _: never = status
		error("unreachable")
	end
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
