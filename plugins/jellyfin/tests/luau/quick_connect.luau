local fixture, harness = ...
local auth = require("@lyra/auth")
local constants = require("../../constants")
local item_ids = require("../../protocol/items/identifiers")
local quick_connect = require("../../routes/quick_connect")

local IDENTITY = 'Client="Finamp", Device="Pixel", DeviceId="pixel", Version="1"'
local ERROR_PROCESSING_REQUEST = "Error processing request."

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
	return `MediaBrowser {IDENTITY}, Token="{fixture.sessions[key]}"`
end

local function initiate(): any
	local response = call(quick_connect.handle_initiate, { authorization = `MediaBrowser {IDENTITY}` })
	assert(response.status == 200, `initiate: {response.status}`)
	return response.body
end

local function connect(secret: string): any
	return call(quick_connect.handle_connect, { query = { secret = secret } })
end

local function authorize(key: string, code: string, user_id: string?): any
	return call(quick_connect.handle_authorize, {
		authorization = token(key),
		query = { code = code, userId = user_id },
	})
end

local function authenticate(secret: string, authorization: string?): any
	return call(quick_connect.handle_authenticate, { authorization = authorization, json = { Secret = secret } })
end

local function failed(response: any, status: number, why: string)
	assert(response.status == status, `{why}: status {response.status}`)
	assert(response.body == ERROR_PROCESSING_REQUEST, `{why}: body {tostring(response.body)}`)
end

-- Quick Connect is off until its setting turns it on, and then begins nothing.
assert(constants.QUICK_CONNECT_ENABLED == false, "off by default")
assert(call(quick_connect.handle_enabled, {}).body == false, "reads as off by default")
local while_off = call(quick_connect.handle_initiate, { authorization = `MediaBrowser {IDENTITY}` })
assert(while_off.status == 401 and while_off.body == "Quick connect is disabled", "initiate while off")
assert(call(quick_connect.handle_connect, { query = { secret = "s" } }).status == 401, "connect while off")
failed(call(quick_connect.handle_authenticate, { json = { Secret = "s" } }), 401, "signing in while off")

constants.QUICK_CONNECT_ENABLED = true
local enabled = call(quick_connect.handle_enabled, {})
assert(enabled.status == 200 and enabled.body == true, "Quick Connect is on with its setting and authentication")

-- Initiate wants all four parts of the identity, from the request alone, and takes no caller.
for _, partial in
	{
		'Device="Pixel", DeviceId="pixel", Version="1"',
		'Client="Finamp", DeviceId="pixel", Version="1"',
		'Client="Finamp", Device="Pixel", Version="1"',
		'Client="Finamp", Device="Pixel", DeviceId="pixel"',
		'Client="Finamp", Device="", DeviceId="pixel", Version="1"',
	}
do
	failed(call(quick_connect.handle_initiate, { authorization = `MediaBrowser {partial}` }), 400, partial)
end
failed(call(quick_connect.handle_initiate, {}), 400, "initiate without an identity")

-- A code is six digits, and the request reads back as upstream's result.
local begun = initiate()
assert(string.match(begun.Code, "^[1-9]%d%d%d%d%d$"), `code {begun.Code}`)
assert(#begun.Secret == 64, "the secret is 32 bytes of hex")
assert(begun.Authenticated == false)
assert(begun.DeviceId == "pixel" and begun.DeviceName == "Pixel")
assert(begun.AppName == "Finamp" and begun.AppVersion == "1")
assert(type(begun.DateAdded) == "string")

local waiting = connect(begun.Secret)
assert(waiting.status == 200 and waiting.body.Authenticated == false, "still waiting")
assert(waiting.body.Code == begun.Code and waiting.body.Secret == begun.Secret)
assert(waiting.body.DeviceName == "Pixel" and waiting.body.AppName == "Finamp")

-- Signing in before anyone authorizes is the middleware's 404.
failed(authenticate(begun.Secret), 404, "signing in while pending")
failed(authenticate("no-such-secret"), 404, "signing in with an unknown secret")

-- Connect answers an unknown secret as upstream's controller does.
local unknown_secret = connect("no-such-secret")
assert(unknown_secret.status == 404 and unknown_secret.body == "Unknown secret", "connect, unknown secret")
assert(call(quick_connect.handle_connect, {}).status == 400, "connect without a secret")

-- Authorize takes a caller, a code, and no other user unless the caller is an admin.
local code = { code = begun.Code }
local unauthenticated = call(quick_connect.handle_authorize, { query = code })
assert(unauthenticated.status == 401, `authorize without a caller: {unauthenticated.status}`)
assert(
	call(quick_connect.handle_authorize, { authorization = token("phone") }).status == 400,
	"authorize without a code"
)
local other = item_ids.encode(fixture.users.other) :: string
assert(authorize("phone", begun.Code, other).status == 403, "authorizing for another user")
assert(connect(begun.Secret).body.Authenticated == false, "a refused authorization leaves the request waiting")
failed(authorize("phone", "000000"), 404, "authorizing an unknown code")

-- An API key signs no one in.
local by_key = call(quick_connect.handle_authorize, {
	authorization = `MediaBrowser {IDENTITY}, Token="{fixture.api_keys.script}"`,
	query = code,
})
failed(by_key, 403, "authorizing with an API key")
assert(connect(begun.Secret).body.Authenticated == false, "an API key authorizes nothing")

-- Polling after the code is authorized signs in no one, until the client asks.
local authorized = authorize("phone", begun.Code)
assert(authorized.status == 200 and authorized.body == true, `authorize: {authorized.status}`)
local connected = connect(begun.Secret)
assert(connected.status == 200 and connected.body.Authenticated == true, "connect sees the authorization")
failed(authorize("console", begun.Code), 500, "authorizing twice")

-- Signing in saves the identity the client began with, whatever it sends now, and retries answer
-- the same session.
local first = authenticate(begun.Secret)
assert(first.status == 200, `signing in: {first.status}`)
local access_token = first.body.AccessToken
assert(type(access_token) == "string" and access_token ~= "", "an access token")
assert(first.body.User.Id == item_ids.encode(fixture.users.listener), "signed in as the authorizing user")
assert(first.body.ServerId ~= nil, "an AuthenticationResult")
local again = authenticate(begun.Secret, `MediaBrowser Client="Other", Device="Tablet", DeviceId="tablet", Version="9"`)
assert(again.status == 200 and again.body.AccessToken == access_token, "a retry answers the same token")
local saved = auth.get_session_data(access_token) :: any
assert(saved.client == "Finamp" and saved.device == "Pixel", "the saved identity is the initiating client's")
assert(saved.device_id == "pixel" and saved.version == "1")
assert(connect(begun.Secret).body.Authenticated == true, "a claimed request still reads as authorized")

-- An admin may authorize for another user, and a garbled user is the client's mistake.
local for_other = initiate()
local by_admin = authorize("console", for_other.Code, other)
assert(by_admin.status == 200 and by_admin.body == true, `admin authorizing for another: {by_admin.status}`)
assert(authenticate(for_other.Secret).body.User.Id == other, "signed in as the named user")
local garbled = initiate()
assert(authorize("console", garbled.Code, "not-a-user-id").status == 400, "a garbled user id")
assert(connect(garbled.Secret).body.Authenticated == false, "a garbled user authorizes nothing")
-- A well-formed id of no account is the authentication failure upstream reports as disabled.
local nobody = authorize("console", garbled.Code, "00000000000000000000000000000001")
assert(nobody.status == 401 and nobody.body == "Quick connect is disabled", `no such user: {nobody.status}`)
