local api = require("@lyra/api")

local response = api.response.json(201, { ok = true }, {
	["x-test"] = "1",
})
local query = {
	flag = { "yes" },
	count = { "12" },
	csv = { "a,b", "c" },
}

assert(response.kind == "json")
assert(tostring(response.status) == "201")
assert(response.headers["content-type"] == "application/json")
assert(response.headers["x-test"] == "1")

local typed = api.response.text(200, "body", { ["Content-Type"] = "text/csv" })
assert(typed.headers["Content-Type"] == "text/csv")
assert(typed.headers["content-type"] == nil, "a plugin's header replaces the default in any case")
assert(api.query.bool(query, "flag", false) == true)
assert(tostring(api.query.int(query, "count", nil, 1, 20)) == "12")
assert(table.concat(api.query.csv(query, "csv"), "|") == "a|b|c")
