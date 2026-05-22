local api = require("@lyra/api")

api.get("/demo/{id}", function(ctx)
	return api.response.json(200, { id = ctx.params.id })
end, "public", true)

api.route("post", "/demo", function()
	return api.response.empty(204)
end)

api.websocket("/socket", function() end, "public")

local response = api.response.json(201, { ok = true }, {
	["x-test"] = "1",
})
local query = {
	flag = { "yes" },
	count = { "12" },
	csv = { "a,b", "c" },
}

assert(type(api.get) == "function")
assert(type(api.response.stream_track) == "function")
assert(response.kind == "json")
assert(tostring(response.status) == "201")
assert(response.headers["content-type"] == "application/json")
assert(response.headers["x-test"] == "1")
assert(api.query.bool(query, "flag", false) == true)
assert(tostring(api.query.int(query, "count", nil, 1, 20)) == "12")
assert(table.concat(api.query.csv(query, "csv"), "|") == "a|b|c")
