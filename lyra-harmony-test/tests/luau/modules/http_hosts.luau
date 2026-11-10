local http = require("@harmony/http")

-- The plugin these tests run as lists no hosts, so the server refuses its every request, before
-- sending anything.
local response = http.request({
	url = "https://unlisted.test/path?key=secret",
	method = http.HttpMethod.Get,
	cache_ttl_ms = 1000,
})

assert(response.success == false, "a request to an unlisted host succeeded")
assert(response.error_kind == "host_not_allowed", `error_kind {tostring(response.error_kind)}`)
assert(response.status_code == nil, `status_code {tostring(response.status_code)}`)
assert(
	response.status_message == "host 'unlisted.test' is not among the plugin's hosts",
	`status_message {response.status_message}`
)
assert(response.body == "", "a refused request has a body")
assert(response.retries == 0 and response.rate_limited == false, "a refused request was retried")
