--!strict
-- Answers the plugin's TheAudioDB requests with scripted responses, and records their URLs.
local http = require("@harmony/http")
local serde = require("@harmony/serde")

local api = require("../../api")

local module = {}

-- A route answers each request whose URL contains its pattern with its body, encoded as JSON.
export type Route = { pattern: string, body: any }

module.routes = {} :: { Route }
module.requests = {} :: { string }

api.send = function(options: http.HttpRequestOptions): http.HttpResponse
	table.insert(module.requests, options.url)
	for _, route in module.routes do
		if string.find(options.url, route.pattern, 1, true) then
			return {
				success = true,
				status_code = 200,
				status_message = "",
				headers = {},
				cookies = {},
				body = serde.json.encode(route.body),
				retries = 0,
				rate_limited = false,
			}
		end
	end
	error(`unexpected request {options.url}`)
end

-- Answers with `routes` from now on, forgetting the requests made so far.
function module.serve(routes: { Route })
	module.routes = routes
	module.requests = {}
end

return module
