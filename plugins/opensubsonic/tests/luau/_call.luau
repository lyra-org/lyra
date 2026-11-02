local dispatch = require("../../dispatch")
local serde = require("@harmony/serde")

export type Params = { [string]: string | { string } }

local function query_of(params: Params): { [string]: { string } }
	local query = {}
	for name, value in params do
		query[name] = if type(value) == "table" then value else { value }
	end
	return query
end

-- The helpers, calling the plugin through the test's `harness` as the host dispatches a request, so
-- whom a call authenticates doesn't become the test's caller.
return function(harness: any)
	local module = {}

	-- The raw response to a method call, with the protocol version and client name unless the call
	-- gives its own or `bare` asks for neither.
	function module.raw(
		method: string,
		params: Params,
		options: { http_method: string?, headers: { [string]: string }?, body: string?, bare: boolean? }?
	): any
		if not (options and options.bare) then
			params = table.clone(params)
			params.v = params.v or "1.16.1"
			params.c = params.c or "test"
		end
		return harness.plugin:call(
			dispatch.dispatch,
			{
				plugin_id = "opensubsonic",
				request = {
					method = if options and options.http_method then options.http_method else "GET",
					path = `/rest/{method}`,
					headers = if options and options.headers then options.headers else {},
					query = query_of(params),
					body_raw = if options and options.body then options.body else "",
					json = nil,
				},
				params = { method = method },
			} :: any
		)
	end

	-- The decoded `subsonic-response` of a JSON call authenticated with `key`.
	function module.call(method: string, key: string, params: Params?): any
		local query: Params = { apiKey = key, f = "json" }
		for name, value in params or {} do
			query[name] = value
		end
		local response = module.raw(method, query)
		assert(response.kind == "text", `{method} answered {response.kind}`)
		return (serde.json.decode(response.body) :: any)["subsonic-response"]
	end

	function module.ok(method: string, key: string, params: Params?): any
		local body = module.call(method, key, params)
		assert(
			body.status == "ok",
			`{method} failed: {body.error and body.error.code} {body.error and body.error.message}`
		)
		return body
	end

	function module.error_code(method: string, key: string, params: Params?): number
		local body = module.call(method, key, params)
		assert(body.status == "failed", `{method} succeeded`)
		return body.error.code
	end

	function module.ids(items: { any }?): { string }
		local ids = {}
		for _, item in items or {} do
			table.insert(ids, item.id)
		end
		return ids
	end

	function module.same(actual: { any }, expected: { any }, label: string)
		local function show(list: { any }): string
			local parts = {}
			for _, value in list do
				table.insert(parts, tostring(value))
			end
			return table.concat(parts, ",")
		end
		assert(show(actual) == show(expected), `{label}: expected {show(expected)}, got {show(actual)}`)
	end

	return module
end
