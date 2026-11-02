--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")
local auth = require("@lyra/auth")

local request = require("./request")
local response = require("./response")

type ApiQueryParams = api.ApiQueryParams
export type ResolvedAuth = auth.ResolvedAuth
type SubsonicError = response.SubsonicError

local module = {
	HELP_URL = "https://git.lyra.pub/lyra/lyra/-/blob/main/plugins/opensubsonic/README.md#signing-in",
}

-- `enc:` passwords are hex-encoded. Nil for anything that doesn't decode to ASCII, as no API key
-- does and the host takes only UTF-8.
local function decode_password(value: string): string?
	local hex = string.match(value, "^enc:(.*)$")
	if not hex then
		return value
	end
	if #hex % 2 ~= 0 or not string.match(hex, "^%x*$") then
		return nil
	end
	local decoded = string.gsub(hex, "%x%x", function(pair: string): string
		return string.char(tonumber(pair, 16) :: number)
	end)
	return if string.find(decoded, "[\128-\255]") then nil else decoded
end

local function resolve_api_key(key: string): ResolvedAuth?
	local resolved = auth.resolve_auth(key)
	return if resolved and resolved.credential.kind == "api_key" then resolved else nil
end

local function present(query: ApiQueryParams, name: string): boolean
	return query[name] ~= nil
end

-- API keys are the only accepted secret, sent as `apiKey` or as the `p` of the key's owner.
-- Lyra can't verify `t`/`s`, which needs the password itself. With Lyra's auth disabled, every
-- request acts as the default user, whatever credentials it sends.
function module.authenticate(query: ApiQueryParams): (ResolvedAuth?, SubsonicError?)
	if not auth.capabilities().enabled then
		local resolved = auth.resolve_auth(nil)
		if not resolved then
			return nil, response.error(0, "The default user is unavailable")
		end
		return resolved, nil
	end

	local help_url = module.HELP_URL
	local conflict = response.error(43, "Multiple conflicting authentication mechanisms provided")

	if present(query, "apiKey") then
		if present(query, "u") or present(query, "p") or present(query, "t") or present(query, "s") then
			return nil, conflict
		end
		local api_key = request.param(query, "apiKey")
		local resolved = if api_key then resolve_api_key(api_key) else nil
		if not resolved then
			return nil, response.error(44, "Invalid API key", help_url)
		end
		return resolved, nil
	end

	if present(query, "p") then
		if present(query, "t") or present(query, "s") then
			return nil, conflict
		end
		local username = request.param(query, "u")
		if not username then
			return nil, response.error(10, "Required parameter is missing: u")
		end
		local password = request.param_or_empty(query, "p") :: string
		local key = decode_password(password)
		local resolved = if key and key ~= "" then resolve_api_key(key) else nil
		if not resolved or string.lower(resolved.principal.username) ~= string.lower(username) then
			return nil, response.error(40, "Wrong username or password; use an API key as the password", help_url)
		end
		return resolved, nil
	end

	if present(query, "t") or present(query, "s") then
		return nil, response.error(41, "Token authentication is not supported; use an API key", help_url)
	end

	return nil, response.error(10, "Required parameter is missing: apiKey")
end

return module
