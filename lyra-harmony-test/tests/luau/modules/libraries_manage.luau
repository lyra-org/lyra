local fixture = ...
local libraries = require("@lyra/libraries")
local task = require("@harmony/task")

type SyncRun = libraries.SyncRun

local shown = fixture.libraries.shown
local hidden = fixture.libraries.hidden

local function running(run: SyncRun): boolean
	local status = run.status
	if status == "queued" or status == "planning" or status == "running" or status == "cancelling" then
		return true
	elseif status == "idle" or status == "cancelled" or status == "succeeded" or status == "failed" then
		return false
	else
		local _: never = status
		return false
	end
end

-- Waits for the library's run to end and returns it.
local function ended(): SyncRun
	for _ = 1, 200 do
		local run = libraries.sync_status({ shown })[shown]
		if not running(run) then
			return run
		end
		task.wait(0.05)
	end
	error("the library's run did not end")
end

-- A library that never ran reports an idle run without an id; others are absent.
local statuses = libraries.sync_status({ shown, hidden, "missing" })
assert(statuses[hidden] == nil and statuses.missing == nil, "missing and hidden libraries are absent")
local idle = statuses[shown]
assert(idle.id == nil and idle.status == "idle" and idle.kind == "library_sync")
assert(idle.started_at_ms == nil and idle.finished_at_ms == nil)
assert(idle.total_units == 0 and idle.cancellation_requested == false)
local cancelled_idle = libraries.cancel_sync(shown)
assert(cancelled_idle ~= nil and cancelled_idle.status == "idle", "cancelling nothing returns the idle run")

-- A sync starts once; asking again while it runs returns the same run.
local first = libraries.sync(shown)
assert(first ~= nil and first.started, "the sync started")
assert(first.run.kind == "library_sync" and type(first.run.id) == "string")
assert(type(first.run.started_at_ms) == "number")
local again = libraries.sync(shown)
assert(again ~= nil)
if not again.started then
	assert(again.run.id == first.run.id, "the running sync is returned")
end
local synced = ended()
assert(synced.finished_at_ms ~= nil and synced.finished_at_ms >= (synced.started_at_ms :: number))
assert(synced.stage == nil, "a finished run has no stage")

-- A refresh is a run of its own kind; cancelling returns it.
local refresh = libraries.refresh(shown, { covers = "replace", bypass_cover_cache = true })
assert(refresh ~= nil and refresh.started and refresh.run.kind == "library_refresh")
local cancelled = libraries.cancel_sync(shown)
assert(cancelled ~= nil and cancelled.id == refresh.run.id, "the running refresh is cancelled")
local refreshed = ended()
assert(refreshed.id == refresh.run.id and refreshed.kind == "library_refresh")

-- With nothing running, cancelling returns the latest run.
local latest = libraries.cancel_sync(shown)
assert(latest ~= nil and latest.id == refresh.run.id and not running(latest))

-- The options narrow the runs a status reports.
assert(libraries.sync_status({ shown }, { kind = "library_sync" })[shown].id == first.run.id)
assert(libraries.sync_status({ shown }, { kind = "library_refresh", finished = true })[shown].id == refresh.run.id)
assert(libraries.sync_status({ shown }, { finished = true })[shown].id == refresh.run.id)
local never = libraries.sync_status({ shown, hidden }, { kind = "library_sync", finished = false })
assert(never[hidden] == nil and never[shown].kind == "library_sync")

-- A refresh without covers runs the providers alone.
local metadata_only = libraries.refresh(shown, { covers = "skip" })
assert(metadata_only ~= nil and metadata_only.started)
local finished = ended()
assert(finished.id == metadata_only.run.id)
local running_sync = libraries.sync(shown)
assert(running_sync ~= nil and running_sync.started)
local previous = libraries.sync_status({ shown }, { finished = true })[shown]
assert(not running(previous), "a finished read passes over the running sync")
ended()

-- Missing and hidden libraries are nil; bad options raise.
assert(libraries.sync(hidden) == nil)
assert(libraries.refresh("missing") == nil)
assert(libraries.cancel_sync(hidden) == nil)
assert(not pcall(libraries.refresh, shown, { replace = true } :: any), "an unknown option is refused")
assert(not pcall(libraries.refresh, shown, { bypass_cover_cache = 1 } :: any))
assert(not pcall(libraries.refresh, shown, { covers = false } :: any), "covers is a string")
assert(not pcall(libraries.refresh, shown, { covers = "all" } :: any), "an unknown covers value is refused")
assert(
	not pcall(libraries.refresh, shown, { covers = "skip", bypass_cover_cache = true }),
	"bypassing the cache needs covers"
)
assert(not pcall(libraries.sync_status, { shown }, { kind = "scan" } :: any), "an unknown kind is refused")
