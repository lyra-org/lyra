local fixture = ...
local auth = require("../../protocol/auth")
local call = require("./_call")

local function query_of(params: { [string]: string })
	local query = {}
	for name, value in params do
		query[name] = { value }
	end
	return query
end

local function authenticate(params: { [string]: string })
	return auth.authenticate(query_of(params))
end

local function assert_error(params: { [string]: string }, code: number)
	local resolved, err = authenticate(params)
	assert(resolved == nil, `{code} resolves no user`)
	assert(err ~= nil and err.code == code, `expected error {code}, got {err and err.code}`)
end

local function assert_user(params: { [string]: string }, key: string)
	local resolved, err = authenticate(params)
	assert(err == nil, `unexpected error {err and err.code}`)
	assert(resolved ~= nil and resolved.principal.user_id == fixture.users[key], `resolves {key}`)
end

local function hex(value: string): string
	return (
		string.gsub(value, ".", function(char: string): string
			return string.format("%02x", string.byte(char))
		end)
	)
end

local phone = fixture.api_keys.phone
local tablet = fixture.api_keys.tablet

-- `apiKey` stands alone.
assert_user({ apiKey = phone }, "owner")
assert_user({ apiKey = tablet }, "other")
assert_error({ apiKey = "not-a-key" }, 44)
assert_error({ apiKey = phone, u = "owner" }, 43)

-- `p` accepts its user's API key, in clear or `enc:` form.
assert_user({ u = "owner", p = phone }, "owner")
assert_user({ u = "OWNER", p = phone }, "owner")
assert_user({ u = "owner", p = `enc:{hex(phone)}` }, "owner")
assert_error({ u = "other", p = phone }, 40)
assert_error({ u = "owner", p = "password" }, 40)
assert_error({ u = "owner", p = "enc:zz" }, 40)
assert_error({ p = phone }, 10)

-- Tokens need the password itself.
assert_error({ u = "owner", t = "26719a1196d2a940705a59634eb18eab", s = "c19b2d" }, 41)
assert_error({ u = "owner", p = phone, t = "26719a1196d2a940705a59634eb18eab", s = "c19b2d" }, 43)

assert_error({}, 10)

-- A mechanism counts as given even with an empty value.
assert_error({ apiKey = phone, u = "" }, 43)
assert_error({ apiKey = phone, p = "" }, 43)
assert_error({ apiKey = phone, t = "" }, 43)
assert_error({ apiKey = phone, s = "" }, 43)
assert_error({ u = "owner", p = phone, t = "" }, 43)
assert_error({ u = "owner", p = phone, s = "" }, 43)
assert_error({ apiKey = "" }, 44)
assert_error({ u = "owner", p = "" }, 40)

-- `tokenInfo` checks the key it describes.
assert(call.error_code("tokenInfo", "not-a-key") == 44, "an invalid key")
assert(call.ok("tokenInfo", phone).tokenInfo.username ~= nil, "a valid key")

-- Errors a key would fix point at the plugin's sign-in docs.
local _, token_error = authenticate({ u = "owner", t = "26719a1196d2a940705a59634eb18eab", s = "c19b2d" })
assert(token_error ~= nil and token_error.helpUrl == auth.HELP_URL)

-- Every authenticated method needs the protocol version and the client's name, checked before the
-- credentials; the extensions list doesn't.
local serde = require("@harmony/serde")
local function failure(method: string, params: { [string]: string }): any
	local body = (serde.json.decode(call.raw(method, params, { bare = true }).body) :: any)["subsonic-response"]
	assert(body.status == "failed", `{method} succeeded`)
	return body.error
end
local missing_v = failure("ping", { apiKey = phone, f = "json", c = "test" })
assert(missing_v.code == 10 and missing_v.message == "Required parameter is missing: v", missing_v.message)
local missing_c = failure("ping", { apiKey = phone, f = "json", v = "1.16.1" })
assert(missing_c.code == 10 and missing_c.message == "Required parameter is missing: c", missing_c.message)
assert(failure("ping", { apiKey = phone, f = "json", v = "", c = "test" }).code == 10, "an empty v")
assert(failure("ping", { apiKey = "not-a-key", f = "json" }).code == 10, "before the credentials")
local bare_stream = call.raw("stream", { apiKey = phone, f = "json", c = "test" }, { bare = true })
assert(string.find(bare_stream.body, "Required parameter is missing: v", 1, true), bare_stream.body)
local extensions = call.raw("getOpenSubsonicExtensions", { f = "json" }, { bare = true })
assert((serde.json.decode(extensions.body) :: any)["subsonic-response"].status == "ok", "public without them")
