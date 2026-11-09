local errors = require("@lyra/errors")
local playback_sessions = require("@lyra/playback_sessions")

local function kind_of(f: (...any) -> ...any, ...: any): errors.ErrorKind?
	local ok, err = pcall(f, ...)
	assert(not ok, "expected an error")
	return errors.kind(err)
end

-- A host error keeps its kind however Luau passes it on.
local ok, err = pcall(playback_sessions.send_command, { connection_id = "missing", action = "pause" })
assert(not ok and errors.kind(err) == "not_found", `error {err}`)
assert(typeof(err) == "userdata", typeof(err))
assert(string.find(tostring(err), "target connection not found", 1, true), tostring(err))
assert(string.find(`{err}`, "target connection not found", 1, true), `{err}`)
local rethrown_ok, rethrown = pcall(function()
	error(err, 0)
end)
assert(not rethrown_ok and rawequal(rethrown, err) and errors.kind(rethrown) == "not_found")
assert(kind_of(coroutine.wrap(function()
	playback_sessions.send_command({ connection_id = "missing", action = "pause" })
end)) == "not_found", "through a coroutine")

assert(kind_of(playback_sessions.list_connections, { all_users = true }) == "forbidden")

assert(kind_of(playback_sessions.send_command, { connection_id = "missing", action = "jump" } :: any) == "invalid")

-- Everything else has no kind.
assert(kind_of(error, "not_found") == nil)
assert(kind_of(error, { kind = "not_found" }) == nil)
assert(errors.kind(nil) == nil)
assert(errors.kind("not_found") == nil)
assert(errors.kind(newproxy()) == nil)
