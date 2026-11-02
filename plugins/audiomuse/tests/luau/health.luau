local audiomuse = require("./_audiomuse")
local routes = require("../../routes")

local take = audiomuse.take

-- Health is AudioMuse's health route, unhealthy on an error status.
audiomuse.configure()
audiomuse.routes = { { pattern = "/api/health", body = { status = "ok" } } }
local response = routes.health() :: any
assert(response.status == 200 and response.body.healthy == true, "healthy")
audiomuse.routes = { { pattern = "/api/health", status = 500, body = { error = "boom" } } }
response = routes.health() :: any
assert(response.status == 500 and response.body.healthy == false, "unhealthy")

-- An analysis starts with the client's body, or an empty object without one.
audiomuse.routes = { { pattern = "/api/analysis/start", body = { task_id = "t-1", status = "queued" } } }
take()
local started = routes.analysis_start({ request = { body_raw = "" } } :: any) :: any
assert(started.status == 200 and started.body.task_id == "t-1", "started")
local sent = take()[1]
assert(sent.method == "POST" and sent.raw == "{}", `an empty object, got {sent.raw}`)
audiomuse.configure("Lyra Home")
routes.analysis_start({ request = { body_raw = '{"num_recent_albums":2}' } } :: any)
sent = take()[1]
assert(sent.body.num_recent_albums == 2 and sent.body.server == "Lyra Home", "the client's body, with the server")

-- A body that isn't a JSON object is refused before it reaches AudioMuse.
for _, raw in { "[]", "[1]", "3", "not json" } do
	local refused = routes.analysis_start({ request = { body_raw = raw } } :: any) :: any
	assert(refused.status == 400, `{raw}: status {refused.status}`)
end
assert(#take() == 0, "nothing sent")
