local http = require("@harmony/http")

assert(http.DEFAULT_MAX_BODY_BYTES == 16 * 1024 * 1024, `DEFAULT_MAX_BODY_BYTES {http.DEFAULT_MAX_BODY_BYTES}`)
assert(http.MAX_BODY_BYTES == 64 * 1024 * 1024, `MAX_BODY_BYTES {http.MAX_BODY_BYTES}`)

local function refuses(label: string, max_body_bytes: number)
	local ok, err = pcall(http.request, {
		url = "http://body-limit.test/",
		method = http.HttpMethod.Get,
		max_body_bytes = max_body_bytes,
	})
	assert(not ok, `{label} sent`)
	assert(
		string.find(tostring(err), "max_body_bytes must be between 1 and MAX_BODY_BYTES", 1, true),
		`{label}: {tostring(err)}`
	)
end

refuses("an empty body cap", 0)
refuses("a body cap past the maximum", http.MAX_BODY_BYTES + 1)
