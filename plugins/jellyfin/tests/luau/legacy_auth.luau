local fixture = ...
local constants = require("../../constants")
local session = require("../../protocol/session")

-- Upstream accepts the canonical forms always and the legacy forms only under
-- EnableLegacyAuthorization, which `accept_legacy_auth_headers` stands for.

local token = fixture.api_keys.phone

local function accepted(headers: { [string]: string }, query: { [string]: { string } }?): boolean
	local principal = session.require_jellyfin_principal({
		plugin_id = "jellyfin",
		request = {
			method = "GET",
			path = "/",
			headers = headers,
			query = query or {},
			body_raw = "",
			json = nil,
		},
		params = {},
	} :: any)
	return principal ~= nil and principal.user_id == fixture.users.listener
end

local canonical = {
	["MediaBrowser scheme"] = function()
		return accepted({ authorization = `MediaBrowser Client="c", Token="{token}"` })
	end,
	["ApiKey query"] = function()
		return accepted({}, { ApiKey = { token } })
	end,
}

local legacy = {
	["Emby scheme"] = function()
		return accepted({ authorization = `Emby Client="c", Token="{token}"` })
	end,
	["X-Emby-Authorization"] = function()
		return accepted({ ["x-emby-authorization"] = `MediaBrowser Client="c", Token="{token}"` })
	end,
	["X-Emby-Token"] = function()
		return accepted({ ["x-emby-token"] = token })
	end,
	["X-MediaBrowser-Token"] = function()
		return accepted({ ["x-mediabrowser-token"] = token })
	end,
	["api_key query"] = function()
		return accepted({}, { api_key = { token } })
	end,
}

local default = constants.ACCEPT_LEGACY_AUTH_HEADERS
assert(default == true, "legacy authorization is accepted by default")

for _, enabled in { true, false } do
	constants.ACCEPT_LEGACY_AUTH_HEADERS = enabled
	for name, attempt in canonical do
		assert(attempt(), `{name} is accepted with legacy authorization {enabled}`)
	end
	for name, attempt in legacy do
		assert(attempt() == enabled, `{name} follows legacy authorization {enabled}`)
	end
end

assert(not accepted({ authorization = `Bearer {token}` }), "the Bearer scheme is not accepted")

-- X-Emby-Authorization stands in only for a missing Authorization header.
constants.ACCEPT_LEGACY_AUTH_HEADERS = true
assert(not accepted({
	authorization = `MediaBrowser Client="c"`,
	["x-emby-authorization"] = `MediaBrowser Client="c", Token="{token}"`,
}), "Authorization wins over X-Emby-Authorization")

constants.ACCEPT_LEGACY_AUTH_HEADERS = default
