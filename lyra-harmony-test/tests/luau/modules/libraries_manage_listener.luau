local fixture = ...
local libraries = require("@lyra/libraries")

-- Seeing a library is not managing it: without manage_libraries every call but `sync_status`
-- raises.
local shown = fixture.libraries.shown
local function refused(f: (...any) -> ...any, ...: any)
	local ok, err = pcall(f, ...)
	assert(not ok, "the call is refused")
	assert(string.find(tostring(err), "ManageLibraries permission required", 1, true), tostring(err))
end

refused(libraries.sync, shown)
refused(libraries.refresh, shown)
refused(libraries.cancel_sync, shown)
refused(libraries.sync, "missing")

-- A listener reads the status of the libraries it sees, without their errors or subjects.
local statuses = libraries.sync_status({ shown, fixture.libraries.hidden, "missing" })
assert(statuses[fixture.libraries.hidden] == nil and statuses.missing == nil, "unseen libraries are absent")
local idle = statuses[shown]
assert(idle ~= nil and idle.id == nil and idle.status == "idle")
assert(idle.error == nil and idle.subject == nil)
