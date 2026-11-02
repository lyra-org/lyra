--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")
local libraries = require("@lyra/libraries")

local request = require("../protocol/request")
local response = require("../protocol/response")

type ApiResponse = api.ApiResponse
type Request = request.Request
type SyncRun = libraries.SyncRun

local function active(run: SyncRun): boolean
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

local function library_ids(): { string }
	local ids: { string } = {}
	for _, library in libraries.list() do
		table.insert(ids, library.id)
	end
	return ids
end

-- Scanning while a sync runs in any library the caller sees; a metadata refresh scans no files.
-- Lyra counts no scanned items, so the optional `count` is left out.
local function scan_status(req: Request): ApiResponse
	local scanning = false
	for _, run in libraries.sync_status(library_ids(), { kind = "library_sync" }) do
		scanning = scanning or active(run)
	end
	return response.ok(req.params, { scanStatus = { scanning = scanning } })
end

local module = {}

module.methods = {
	getScanStatus = scan_status,
	startScan = function(req: Request): ApiResponse
		local permissions = req.auth.principal.permissions
		if not (table.find(permissions, "admin") or table.find(permissions, "manage_libraries")) then
			return request.fail(50, "Only users who manage libraries can start a scan")
		end
		-- A library running a refresh starts no sync, and reports no scan.
		for _, library_id in library_ids() do
			libraries.sync(library_id)
		end
		return scan_status(req)
	end,
}

return module
