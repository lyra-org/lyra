local api = require("@lyra/api")

assert(api.MAX_FILLER_BYTES == 100000000, `MAX_FILLER_BYTES {api.MAX_FILLER_BYTES}`)

local filler = api.response.filler(200, api.MAX_FILLER_BYTES, { ["x-test"] = "1" })
assert(filler.kind == "filler" and filler.status == 200)
assert(filler.size_bytes == api.MAX_FILLER_BYTES)
assert(filler.headers["content-type"] == "application/octet-stream")
assert(filler.headers["x-test"] == "1")
assert(api.response.filler(200, 0).size_bytes == 0, "an empty filler")
assert(
	api.response.filler(200, 1, { ["content-type"] = "text/plain" }).headers["content-type"] == "text/plain",
	"the content type is the plugin's"
)

for _, size in { api.MAX_FILLER_BYTES + 1, -1, 1.5 } do
	local ok, err = pcall(api.response.filler, 200, size)
	assert(not ok, `size {size} built`)
	assert(
		string.find(tostring(err), "filler size_bytes must be an integer from 0 to MAX_FILLER_BYTES", 1, true),
		tostring(err)
	)
end
