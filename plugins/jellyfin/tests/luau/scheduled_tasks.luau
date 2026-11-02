local fixture = ...
local libraries = require("@lyra/libraries")
local scheduled_tasks = require("../../routes/scheduled_tasks")
local task = require("@harmony/task")

-- Feishin lists the scheduled tasks and starts "Scan Media Library" by its upstream id, as
-- Jellyfin's dashboard does.

local TASK_ID = "7738148ffcd07979c7ceb148e06b3aed"
local music = fixture.libraries.music
local talks = fixture.libraries.talks

local function context(task_id: string?, query: { [string]: { string } }?, key: string?): any
	return {
		plugin_id = "jellyfin",
		request = {
			method = "GET",
			path = "/ScheduledTasks",
			headers = { authorization = `MediaBrowser Token="{key or fixture.api_keys.admin_key}"` },
			query = query or {},
			body_raw = "",
		},
		params = { taskId = task_id },
	}
end

local function info(): any
	local response = scheduled_tasks.handle_get(context(TASK_ID)) :: any
	assert(response.status == 200)
	return response.body
end

local function wait_for(done: () -> boolean, what: string)
	for _ = 1, 200 do
		if done() then
			return
		end
		task.wait(0.05)
	end
	error(`timed out waiting for {what}`)
end

-- Before any scan, the task is idle with no result.
local listed = scheduled_tasks.handle_list(context()) :: any
assert(listed.status == 200 and #listed.body == 1)
local idle = listed.body[1]
assert(idle.Id == TASK_ID and idle.Key == "RefreshLibrary" and idle.Name == "Scan Media Library")
assert(idle.Category == "Library" and idle.IsHidden == false and #idle.Triggers == 0)
assert(idle.State == "Idle" and idle.CurrentProgressPercentage == nil and idle.LastExecutionResult == nil)

-- Upstream's filters pass over a task that can be neither hidden nor disabled, so it is always
-- listed; they are still read as booleans.
assert(
	#(scheduled_tasks.handle_list(context(nil, { isHidden = { "false" }, isEnabled = { "true" } })) :: any).body == 1
)
assert(#(scheduled_tasks.handle_list(context(nil, { IsHidden = { "true" } })) :: any).body == 1)
assert(#(scheduled_tasks.handle_list(context(nil, { isEnabled = { "false" } })) :: any).body == 1)
assert(scheduled_tasks.handle_list(context(nil, { isHidden = { "maybe" } })).status == 400)
assert(scheduled_tasks.handle_list(context(nil, { isEnabled = { "1" } })).status == 400)

-- Ids match case-insensitively; other tasks are not found.
assert(scheduled_tasks.handle_get(context(string.upper(TASK_ID))).status == 200)
assert(scheduled_tasks.handle_get(context("not-a-task")).status == 404)
assert(scheduled_tasks.handle_start(context("not-a-task")).status == 404)
assert(scheduled_tasks.handle_stop(context("not-a-task")).status == 404)

-- Starting the task syncs every library; it reports the result once they end.
assert(scheduled_tasks.handle_start(context(TASK_ID)).status == 204)
local runs = libraries.sync_status({ music, talks })
assert(runs[music].kind == "library_sync" and runs[music].id ~= nil, "the music library syncs")
assert(runs[talks].kind == "library_sync" and runs[talks].id ~= nil, "the talks library syncs")
local started = info()
assert(started.State == "Running" or started.State == "Idle")
if started.State == "Running" then
	assert(type(started.CurrentProgressPercentage) == "number")
end
wait_for(function()
	return info().State == "Idle"
end, "the scan")
local result = info().LastExecutionResult
assert(result ~= nil, "the finished scan has a result")
assert(result.Id == TASK_ID and result.Key == "RefreshLibrary" and result.Name == "Scan Media Library")
assert(result.Status == "Completed" or result.Status == "Failed", `status {result.Status}`)
assert(type(result.StartTimeUtc) == "string" and type(result.EndTimeUtc) == "string")

-- A library refresh is no scan: the task stays idle with the scan's result.
local refresh = libraries.refresh(music)
assert(refresh ~= nil and refresh.started)
assert(info().State == "Idle")
wait_for(function()
	return libraries.sync_status({ music })[music].finished_at_ms ~= nil
end, "the refresh")
assert(info().LastExecutionResult.EndTimeUtc == result.EndTimeUtc, "the refresh leaves the result alone")

-- Stopping cancels whatever runs, and answers 204 when nothing does. The last result stays while
-- the new scan runs, and the progress while it is cancelled.
assert(scheduled_tasks.handle_start(context(TASK_ID)).status == 204)
assert(info().LastExecutionResult ~= nil)
assert(scheduled_tasks.handle_stop(context(TASK_ID)).status == 204)
local stopping = info()
if stopping.State ~= "Idle" then
	assert(type(stopping.CurrentProgressPercentage) == "number", "a cancelling scan keeps its progress")
end
wait_for(function()
	return info().State == "Idle"
end, "the cancelled scan")
assert(scheduled_tasks.handle_stop(context(TASK_ID)).status == 204)

-- `POST /Library/Refresh` scans every library as the task does.
local scanned = libraries.sync_status({ music, talks }, { kind = "library_sync", finished = true })
assert(scheduled_tasks.handle_library_refresh(context()).status == 204)
local rescanned = libraries.sync_status({ music, talks })
for _, library_id in { music, talks } do
	assert(rescanned[library_id].kind == "library_sync", "the library syncs")
	assert(rescanned[library_id].id ~= scanned[library_id].id, "a new sync started")
end
wait_for(function()
	return info().State == "Idle"
end, "the library refresh")

-- Like upstream, the tasks and the library refresh are an administrator's.
assert(scheduled_tasks.handle_library_refresh(context(nil, nil, "not-a-key")).status == 401)
assert(scheduled_tasks.handle_library_refresh(context(nil, nil, fixture.api_keys.listener_key)).status == 403)
assert(scheduled_tasks.handle_list(context(nil, nil, "not-a-key")).status == 401)
assert(scheduled_tasks.handle_start(context(TASK_ID, nil, fixture.api_keys.listener_key)).status == 403)
