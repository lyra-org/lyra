local fixture = ...
local libraries = require("@lyra/libraries")

-- Seeing a library is not managing it: without manage_libraries every call raises.
local shown = fixture.libraries.shown
local function refused(f: (...any) -> ...any, ...: any)
	local ok, err = pcall(f, ...)
	assert(not ok, "the call is refused")
	assert(string.find(tostring(err), "ManageLibraries permission required", 1, true), tostring(err))
end

refused(libraries.sync, shown)
refused(libraries.refresh, shown)
refused(libraries.sync_status, { shown })
refused(libraries.cancel_sync, shown)
refused(libraries.sync, "missing")
