local api = require("@lyra/api")

local function handler(): api.ApiResponse
	return api.response.empty()
end

local function refuses(label: string, expected: string, register: () -> ())
	local ok, err = pcall(register)
	assert(not ok, `{label} registered`)
	assert(string.find(tostring(err), expected, 1, true), `{label}: {tostring(err)}`)
end

assert(api.MAX_BODY_BYTES == 32 * 1024 * 1024, `MAX_BODY_BYTES {api.MAX_BODY_BYTES}`)

api.get("/api-routes/default", handler)
api.post("/api-routes/upload", handler, {
	auth = "public",
	case_insensitive = true,
	max_body_bytes = api.MAX_BODY_BYTES,
})
api.route("PUT", "/api-routes/small", handler, { max_body_bytes = 1 })
api.websocket("/api-routes/socket", function() end, { auth = "public" })

refuses("a body limit past the maximum", "max_body_bytes must be between 1 and MAX_BODY_BYTES", function()
	api.post("/api-routes/huge", handler, { max_body_bytes = api.MAX_BODY_BYTES + 1 })
end)
refuses("an empty body limit", "max_body_bytes must be between 1", function()
	api.post("/api-routes/empty", handler, { max_body_bytes = 0 })
end)
refuses("an unknown auth mode", `route options.auth must be one of "public", "required", got "open"`, function()
	api.get("/api-routes/open", handler, { auth = "open" } :: any)
end)
refuses(
	"an unknown option",
	"route options: unknown field 'public'; expected auth, case_insensitive, max_body_bytes",
	function()
		api.get("/api-routes/unknown", handler, { public = true } :: any)
	end
)
refuses("a body limit on a socket", "websocket options: unknown field 'max_body_bytes'; expected auth", function()
	api.websocket("/api-routes/socket-limit", function() end, { max_body_bytes = 1 } :: any)
end)
