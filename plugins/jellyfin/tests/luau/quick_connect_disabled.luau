local fixture, harness = ...
local item_ids = require("../../protocol/items/identifiers")
local quick_connect = require("../../routes/quick_connect")

-- With authentication off Quick Connect is off, so its routes answer as upstream's do with it
-- turned off, after the checks upstream makes before it looks at the setting.

local IDENTITY = 'Client="Finamp", Device="Pixel", DeviceId="pixel", Version="1"'

local function call(handler: (any) -> any, request: { authorization: string?, query: any?, json: any? }): any
	local query: { [string]: { string } } = {}
	for key, value in request.query or {} do
		query[key] = { value }
	end
	return harness.plugin:call(handler, {
		plugin_id = "jellyfin",
		request = {
			method = "POST",
			path = "/QuickConnect",
			headers = { authorization = request.authorization },
			query = query,
			body_raw = "",
			json = request.json,
		},
		params = {},
	})
end

local function token(key: string): string
	return `MediaBrowser {IDENTITY}, Token="{fixture.api_keys[key]}"`
end

local function is_disabled(response: any, why: string)
	assert(response.status == 401, `{why}: status {response.status}`)
	assert(response.body == "Quick connect is disabled", `{why}: body {tostring(response.body)}`)
end

local enabled = call(quick_connect.handle_enabled, {})
assert(enabled.status == 200 and enabled.body == false, "Quick Connect reads as off")

-- Initiate takes no caller, only a client that names itself.
is_disabled(call(quick_connect.handle_initiate, { authorization = `MediaBrowser {IDENTITY}` }), "initiate")
for _, partial in
	{
		"",
		'Client="Finamp", DeviceId="pixel", Version="1"',
		'Client="Finamp", Device="Pixel", DeviceId="pixel"',
	}
do
	local refused = call(quick_connect.handle_initiate, { authorization = `MediaBrowser {partial}` })
	assert(refused.status == 400, `initiate with an incomplete identity: {refused.status}`)
	assert(refused.body == "Error processing request.", `initiate: {tostring(refused.body)}`)
end

-- Connect takes no caller either, only a secret.
is_disabled(call(quick_connect.handle_connect, { query = { secret = "s" } }), "connect")
assert(call(quick_connect.handle_connect, {}).status == 400, "connect without a secret")

-- Authorize takes a code. Every caller is the default admin, who may name any user.
local other = item_ids.encode(fixture.users.other) :: string
assert(
	call(quick_connect.handle_authorize, { authorization = token("phone") }).status == 400,
	"authorize without a code"
)
local code = { code = "123456" }
is_disabled(call(quick_connect.handle_authorize, { query = code }), "authorize without a token")
is_disabled(call(quick_connect.handle_authorize, { authorization = token("phone"), query = code }), "authorize")
local for_other = { code = "123456", userId = other }
is_disabled(call(quick_connect.handle_authorize, { authorization = token("phone"), query = for_other }), "for another")

-- Signing in with a secret fails as upstream's unhandled authentication error does.
local sign_in = call(quick_connect.handle_authenticate, { json = { Secret = "s" } })
assert(sign_in.status == 401, `signing in: {sign_in.status}`)
assert(sign_in.body == "Error processing request.", `signing in: {tostring(sign_in.body)}`)
assert(call(quick_connect.handle_authenticate, { json = {} }).status == 400, "signing in without a secret")
assert(call(quick_connect.handle_authenticate, {}).status == 400, "signing in without a body")
