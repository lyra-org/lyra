local fixture, harness = ...
local call = require("./_call")(harness)
local libraries = require("@lyra/libraries")

local music = fixture.libraries.music
local locked = fixture.libraries.locked
local listener_key = fixture.api_keys.listener_key
local manager_key = fixture.api_keys.manager_key
local admin_key = fixture.api_keys.admin_key

local function scan_status(key: string): any
	return call.ok("getScanStatus", key).scanStatus
end

-- Waits for the scan the user sees to end and returns its status.
local function settled(key: string): any
	for _ = 1, 200 do
		local status = scan_status(key)
		if not status.scanning then
			return status
		end
		harness.task:wait(0.05)
	end
	error("the scan did not end")
end

-- The test reads as the admin, who sees both libraries.
local function synced(library_id: string): boolean
	return libraries.sync_status({ library_id }, { kind = "library_sync" })[library_id].id ~= nil
end

-- Before any scan nothing is scanning, and no count is given.
local idle = scan_status(listener_key)
assert(idle.scanning == false and idle.count == nil, `idle {idle.scanning} {idle.count}`)

-- Only a user who manages libraries starts a scan.
assert(call.error_code("startScan", listener_key) == 50, "a listener can't start a scan")
assert(not synced(music), "nothing started")

-- A scan syncs every library the manager sees and answers with the scan status.
local started = call.ok("startScan", manager_key).scanStatus
assert(type(started.scanning) == "boolean" and started.count == nil)
settled(manager_key)
assert(synced(music), "the manager's library synced")
assert(not synced(locked), "a library hidden from the manager didn't")
assert(scan_status(listener_key).scanning == false, "the listener sees the scan ended")

-- An admin's scan reaches every library.
call.ok("startScan", admin_key)
settled(admin_key)
assert(synced(locked), "the admin's scan synced the locked library")
