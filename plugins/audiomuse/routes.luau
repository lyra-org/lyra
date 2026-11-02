--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

-- Operational routes, separate from the mixer.

local api = require("@lyra/api")
local serde = require("@harmony/serde")

local client = require("./client")

type ApiContext = api.ApiContext
type ApiResponse = api.ApiResponse
type JsonValue = serde.JsonValue
type JsonObject = { [string]: JsonValue }

local routes = {}

-- Starts an analysis, forwarding the client's body so callers can scope it (e.g.
-- num_recent_albums).
function routes.analysis_start(ctx: ApiContext): ApiResponse
	local raw = ctx.request.body_raw
	local given = type(raw) == "string" and string.match(raw, "%S") ~= nil
	-- Objects and arrays both decode to tables; only an object may scope the analysis.
	local request_body = if given then client.decode(raw :: string) else {}
	if type(request_body) ~= "table" or (given and string.match(raw :: string, "^%s*{") == nil) then
		return api.response.json(400, { error = "body must be a JSON object" })
	end

	local response = client.post("/api/analysis/start", request_body :: JsonObject)
	local body = client.decode(response.body)
	local status = response.status_code or 502
	-- 2xx with an `error` payload is a gateway failure.
	if status < 400 and type(body) == "table" and type((body :: JsonObject).error) == "string" then
		return api.response.json(502, body)
	end

	return api.response.json(status, body or {
		error = `analysis start failed: {response.status_code or 0}`,
	})
end

function routes.status(ctx: ApiContext): ApiResponse
	local task_id = ctx.params.task_id
	if not task_id or not string.match(task_id, "^[%w%-_]+$") then
		return api.response.json(400, { error = "invalid task_id" })
	end

	local response = client.get(`/api/status/{task_id}`)
	return api.response.json(response.status_code or 502, client.decode(response.body) or {})
end

function routes.health(): ApiResponse
	local response = client.get("/api/health")
	return api.response.json(response.status_code or 502, {
		healthy = client.ok(response),
		backend_url = client.base_url(),
	})
end

function routes.register()
	api.post("/audiomuse/analysis/start", routes.analysis_start)
	api.get("/audiomuse/status/{task_id}", routes.status)
	api.get("/audiomuse/health", routes.health)
end

return routes
