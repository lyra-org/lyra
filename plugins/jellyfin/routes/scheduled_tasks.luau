--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")
local libraries = require("@lyra/libraries")

local constants = require("../constants")
local requests = require("../protocol/items/requests")
local responses = require("../protocol/items/responses")
local session = require("../protocol/session")

local P = constants.ROUTE_PREFIX

type ApiContext = api.ApiContext
type ApiResponse = api.ApiResponse
type JsonValue = api.JsonValue
type SyncRun = libraries.SyncRun

-- Upstream's "Scan Media Library" task, the one scheduled task Lyra has a counterpart for: a sync
-- of every library the caller can manage. Its id is upstream's, the MD5 of the task's .NET type
-- name, so clients that start it by id find it.
local TASK_ID = "7738148ffcd07979c7ceb148e06b3aed"
local TASK_KEY = "RefreshLibrary"
local TASK_NAME = "Scan Media Library"
local TASK_DESCRIPTION = "Scans your media library for new files and refreshes metadata."
local TASK_CATEGORY = "Library"

type TaskState = "Idle" | "Cancelling" | "Running"

local module = {}

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

-- Each library's running sync, else its latest, or with `finished` its latest that ended, for the
-- libraries the caller can manage, keyed by library id. A library refresh is no scan, so it
-- neither runs the task nor ends it.
local function library_syncs(finished: boolean): { [string]: SyncRun }
	local library_ids: { string } = {}
	for _, library in libraries.list() do
		table.insert(library_ids, library.id)
	end
	return libraries.sync_status(library_ids, { kind = "library_sync", finished = finished })
end

-- Upstream's TaskResult for the last scan: each library's latest finished sync, failed when any
-- failed, else cancelled when any was cancelled, else completed. Nil when no library synced yet.
local function last_execution_result(): JsonValue
	local started_at_ms: number?, finished_at_ms: number? = nil, nil
	local failed: SyncRun?, cancelled = nil, false
	for _, run in library_syncs(true) do
		if run.status == "idle" then
			continue
		elseif run.status == "failed" then
			failed = failed or run
		elseif run.status == "cancelled" then
			cancelled = true
		end
		local started, finished = run.started_at_ms, run.finished_at_ms
		if started and (started_at_ms == nil or started < started_at_ms) then
			started_at_ms = started
		end
		if finished and (finished_at_ms == nil or finished > finished_at_ms) then
			finished_at_ms = finished
		end
	end
	if finished_at_ms == nil then
		return nil
	end
	return {
		StartTimeUtc = session.iso_timestamp_ms(started_at_ms or finished_at_ms),
		EndTimeUtc = session.iso_timestamp_ms(finished_at_ms),
		Status = if failed then "Failed" elseif cancelled then "Cancelled" else "Completed",
		Name = TASK_NAME,
		Key = TASK_KEY,
		Id = TASK_ID,
		ErrorMessage = if failed then failed.error else nil,
	}
end

-- Upstream's TaskInfo for the task: running while any library's sync is, cancelling while every
-- running one is being cancelled, with the progress of the running syncs' work in either, as
-- upstream keeps it until a cancelled run ends. The last result stays while a new scan runs, as
-- upstream's does.
function module.task_info(): JsonValue
	local runs = library_syncs(false)
	local state: TaskState = "Idle"
	local settled_units, total_units = 0, 0
	for _, run in runs do
		if running(run) then
			if run.status == "cancelling" then
				if state == "Idle" then
					state = "Cancelling"
				end
			else
				state = "Running"
			end
			settled_units += run.completed_units + run.failed_units + run.skipped_units
			total_units += run.total_units
		end
	end
	local progress: number? = nil
	if state ~= "Idle" then
		progress = if total_units > 0 then math.min(100, settled_units / total_units * 100) else 0
	end
	return {
		Name = TASK_NAME,
		State = state,
		CurrentProgressPercentage = progress,
		Id = TASK_ID,
		LastExecutionResult = last_execution_result(),
		Triggers = {},
		Description = TASK_DESCRIPTION,
		Category = TASK_CATEGORY,
		IsHidden = false,
		Key = TASK_KEY,
	}
end

local function is_task(task_id: string?): boolean
	return task_id ~= nil and string.lower(task_id) == TASK_ID
end

local function query_flag(ctx: ApiContext, key: string): (boolean?, ApiResponse?)
	local value, invalid = requests.parse_query_bool(ctx.request.query, key)
	if invalid then
		return nil, responses.bad_request(`The value '{invalid}' is not valid for {key}.`)
	end
	return value, nil
end

-- Upstream filters by `isHidden` and `isEnabled` only the tasks that can be hidden or disabled,
-- which its library scan cannot, so the task is always listed; the filters are still read as
-- booleans.
function module.handle_list(ctx: ApiContext): ApiResponse
	local denied = session.require_admin(ctx)
	if denied then
		return denied
	end
	for _, key in { "isHidden", "isEnabled" } do
		local _, err = query_flag(ctx, key)
		if err then
			return err
		end
	end
	return api.response.json(200, { module.task_info() })
end

function module.handle_get(ctx: ApiContext): ApiResponse
	local denied = session.require_admin(ctx)
	if denied then
		return denied
	end
	if not is_task(ctx.params.taskId) then
		return responses.not_found()
	end
	return api.response.json(200, module.task_info())
end

-- Starts a sync of every library the caller can manage, with `options`; a library already running
-- keeps its run.
function module.start_scan(options: libraries.SyncOptions?)
	for _, library in libraries.list() do
		libraries.sync(library.id, options)
	end
end

function module.handle_start(ctx: ApiContext): ApiResponse
	local denied = session.require_admin(ctx)
	if denied then
		return denied
	end
	if not is_task(ctx.params.taskId) then
		return responses.not_found()
	end
	module.start_scan()
	return api.response.empty(204)
end

-- Upstream's `POST /Library/Refresh` scans every library as the task does. It answers once the scan
-- ends; this answers once the syncs started.
function module.handle_library_refresh(ctx: ApiContext): ApiResponse
	local denied = session.require_admin(ctx)
	if denied then
		return denied
	end
	module.start_scan()
	return api.response.empty(204)
end

-- Cancels every running sync of the libraries the caller can manage. With none running, upstream
-- fails with 500; this answers 204, as stopping a stopped task leaves it as asked.
function module.handle_stop(ctx: ApiContext): ApiResponse
	local denied = session.require_admin(ctx)
	if denied then
		return denied
	end
	if not is_task(ctx.params.taskId) then
		return responses.not_found()
	end
	for library_id, run in library_syncs(false) do
		if running(run) then
			libraries.cancel_sync(library_id)
		end
	end
	return api.response.empty(204)
end

function module.register()
	local options: api.ApiRouteOptions = { auth = "public", case_insensitive = true }
	api.get(P .. "/ScheduledTasks", module.handle_list, options)
	api.get(P .. "/ScheduledTasks/{taskId}", module.handle_get, options)
	api.post(P .. "/ScheduledTasks/Running/{taskId}", module.handle_start, options)
	api.delete(P .. "/ScheduledTasks/Running/{taskId}", module.handle_stop, options)
	api.post(P .. "/Library/Refresh", module.handle_library_refresh, options)
end

return module
