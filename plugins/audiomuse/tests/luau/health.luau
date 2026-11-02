local audiomuse = require("./_audiomuse")
local routes = require("../../routes")
local startup = require("../../startup")

local take, param = audiomuse.take, audiomuse.param

local MODELS = {
	server_id = "srv-1",
	models = {
		musicnn = { enabled = true, global = { count = 4, total = 5, percentage = 80 } },
		clap = { enabled = false, global = { count = 0, total = 5, percentage = 0 } },
	},
}

-- Health proves the token through AudioMuse's authenticated model coverage, for the configured
-- server.
audiomuse.configure("Lyra Home")
audiomuse.routes = { { pattern = "/api/models", body = MODELS } }
local response = routes.health() :: any
assert(response.status == 200, `status {response.status}`)
assert(response.body.healthy == true and response.body.server_id == "srv-1", "healthy")
assert(response.body.models.musicnn.global.percentage == 80, "the coverage")
local request = take()[1]
assert(param(request.url, "server") == "Lyra Home", "the configured server")

-- A rejected token or an unknown server is unhealthy, with AudioMuse's reason.
audiomuse.routes = {
	{
		pattern = "/api/models",
		status = 400,
		body = { error = "Unknown server 'Lyra Home'", error_message = "Invalid request. Unknown server 'Lyra Home'" },
	},
}
response = routes.health() :: any
assert(response.status == 502, `status {response.status}`)
assert(response.body.healthy == false and response.body.backend_status == 400, "unhealthy")
assert(string.find(response.body.error, "Unknown server 'Lyra Home'", 1, true), response.body.error)

-- Loading checks the server.
audiomuse.routes = { { pattern = "/api/models", body = MODELS } }
assert(startup.check() == true, "the check passes")
audiomuse.routes = { { pattern = "/api/models", status = 401, body = { error = "Unauthorized" } } }
assert(startup.check() == false, "a rejected token fails the check")

-- An analysis starts with the client's body, or an empty object without one.
audiomuse.configure()
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
