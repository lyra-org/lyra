local response = require("../../protocol/response")
local serde = require("@harmony/serde")

local function query(params: { [string]: string })
	local result = {}
	for name, value in params do
		result[name] = { value }
	end
	return result
end

-- JSON wraps the body in `subsonic-response`.
local ok = response.ok(query({ f = "json" }), { tokenInfo = { username = "owner" } }) :: any
assert(ok.status == 200)
assert(string.find(ok.headers["content-type"], "application/json", 1, true))
local body = (serde.json.decode(ok.body) :: any)["subsonic-response"]
assert(body.status == "ok" and body.version == "1.16.1" and body.openSubsonic == true)
assert(body.tokenInfo.username == "owner")

-- Failures keep HTTP 200 and carry the error.
local failed = response.failed(query({ f = "json" }), response.error(40, "Wrong", "https://lyra.test/keys")) :: any
assert(failed.status == 200)
local error_body = (serde.json.decode(failed.body) :: any)["subsonic-response"]
assert(error_body.status == "failed")
assert(error_body.error.code == 40 and error_body.error.helpUrl == "https://lyra.test/keys")

-- JSONP calls the named callback, and rejects anything else.
local jsonp = response.ok(query({ f = "jsonp", callback = "handle" })) :: any
assert(string.match(jsonp.body, "^handle%(.*%);$"))
local bad_callback = response.ok(query({ f = "jsonp", callback = "alert(1)//" })) :: any
local bad_body = (serde.json.decode(bad_callback.body) :: any)["subsonic-response"]
assert(bad_body.error.code == 10)

-- XML is the default, with scalars as attributes and lists as repeated elements.
local xml = response.ok(query({}), {
	openSubsonicExtensions = { { name = "apiKeyAuthentication", versions = { 1, 2 } } },
}) :: any
assert(string.find(xml.headers["content-type"], "application/xml", 1, true))
assert(string.find(xml.body, '^<%?xml version="1.0" encoding="UTF%-8"%?>'))
assert(string.find(xml.body, 'xmlns="http://subsonic.org/restapi"', 1, true))
assert(string.find(xml.body, 'status="ok"', 1, true))
assert(string.find(xml.body, '<openSubsonicExtensions name="apiKeyAuthentication">', 1, true))
assert(string.find(xml.body, "<versions>1</versions><versions>2</versions>", 1, true))
