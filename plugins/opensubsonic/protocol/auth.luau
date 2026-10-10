--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")
local auth = require("@lyra/auth")

local response = require("./response")

type ApiQueryParams = api.ApiQueryParams
type ResolvedAuth = auth.ResolvedAuth
type SubsonicError = response.SubsonicError

local module = {
	HELP_URL = "https://git.lyra.pub/lyra/lyra/-/blob/main/plugins/opensubsonic/README.md#signing-in",
}

-- `enc:` passwords are hex-encoded.
local function decode_password(value: string): string?
	local hex = string.match(value, "^enc:(.*)$")
	if not hex then
		return value
	end
	if #hex % 2 ~= 0 or not string.match(hex, "^%x*$") then
		return nil
	end
	return (
		string.gsub(hex, "%x%x", function(pair: string): string
			return string.char(tonumber(pair, 16) :: number)
		end)
	)
end

local function resolve_api_key(key: string): ResolvedAuth?
	local resolved = auth.resolve_auth(key)
	return if resolved and resolved.credential.kind == "api_key" then resolved else nil
end

-- API keys are the only accepted secret, sent as `apiKey` or as the `p` of the key's owner.
-- Lyra can't verify `t`/`s`, which needs the password itself.
function module.authenticate(query: ApiQueryParams): (ResolvedAuth?, SubsonicError?)
	if not auth.capabilities().enabled then
		return auth.resolve_auth(nil), nil
	end

	local api_key = response.param(query, "apiKey")
	local username = response.param(query, "u")
	local password = response.param(query, "p")
	local token = response.param(query, "t")
	local salt = response.param(query, "s")
	local help_url = module.HELP_URL

	if api_key then
		if username or password or token or salt then
			return nil, response.error(43, "Multiple conflicting authentication mechanisms provided")
		end
		local resolved = resolve_api_key(api_key)
		if not resolved then
			return nil, response.error(44, "Invalid API key", help_url)
		end
		return resolved, nil
	end

	if password then
		if token or salt then
			return nil, response.error(43, "Multiple conflicting authentication mechanisms provided")
		end
		if not username then
			return nil, response.error(10, "Required parameter is missing: u")
		end
		local key = decode_password(password)
		local resolved = if key then resolve_api_key(key) else nil
		if not resolved or string.lower(resolved.principal.username) ~= string.lower(username) then
			return nil, response.error(40, "Wrong username or password; use an API key as the password", help_url)
		end
		return resolved, nil
	end

	if token or salt then
		return nil, response.error(41, "Token authentication is not supported; use an API key", help_url)
	end

	return nil, response.error(10, "Required parameter is missing: apiKey")
end

return module
