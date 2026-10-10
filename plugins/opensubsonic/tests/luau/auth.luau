local fixture = ...
local auth = require("../../protocol/auth")

local function authenticate(params: { [string]: string })
	local query = {}
	for name, value in params do
		query[name] = { value }
	end
	return auth.authenticate(query)
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

-- Errors a key would fix point at the plugin's sign-in docs.
local _, token_error = authenticate({ u = "owner", t = "26719a1196d2a940705a59634eb18eab", s = "c19b2d" })
assert(token_error ~= nil and token_error.helpUrl == auth.HELP_URL)
