local _, harness = ...

local datastore = require("@lyra/datastore")

local HOUR_MS = 60 * 60 * 1000

local store = datastore.get_or_create("expiry")

-- Polls until `key` reads as absent, failing after five seconds.
local function wait_until_absent(key: string)
	local deadline = os.time() + 5
	while store:get(key) ~= nil do
		assert(os.time() < deadline, `{key} never expired`)
		harness.task:wait(0.01)
	end
end

-- Long-lived entries read back like persistent ones.
store:set("persistent", { n = 1 })
store:set("long", "kept", { ttl_ms = HOUR_MS })
store:set_many({
	batch_kept = { value = true },
	batch_long = { value = { 1, 2 }, ttl_ms = HOUR_MS },
})
assert((store:get("persistent") :: { n: number }).n == 1)
assert(store:get("long") == "kept")
local batch = store:get_many({ "batch_kept", "batch_long", "missing" })
assert(batch[1] == true)
assert((batch[2] :: { number })[2] == 2)
assert(batch[3] == nil)

-- Short-lived entries expire and read as absent.
store:set("short", "soon", { ttl_ms = 1 })
store:set_many({ batch_short = { value = "soon", ttl_ms = 1 } })
wait_until_absent("short")
wait_until_absent("batch_short")
assert(store:get_many({ "short", "batch_short" })[2] == nil)
assert(store:remove("short") == false, "an expired entry counts as absent")

-- A write without ttl_ms clears an earlier expiry; a later ttl_ms replaces it.
store:set("rewritten", "first", { ttl_ms = 1 })
store:set("rewritten", "second")
store:set("revived", "again", { ttl_ms = 1 })
store:set("revived", "again", { ttl_ms = HOUR_MS })
store:set("probe", "x", { ttl_ms = 1 })
wait_until_absent("probe")
assert(store:get("rewritten") == "second")
assert(store:get("revived") == "again")

local function raises(pattern: string, f: () -> ())
	local ok, err = pcall(f)
	assert(not ok, `expected an error matching {pattern}`)
	assert(string.find(tostring(err), pattern, 1, true), tostring(err))
end

raises("options.ttl_ms must be a positive finite number", function()
	store:set("bad", 1, { ttl_ms = 0 })
end)
raises("options.ttl_ms must be a positive finite number", function()
	store:set("bad", 1, { ttl_ms = -5 })
end)
raises("options.ttl_ms must be a positive finite number", function()
	store:set("bad", 1, { ttl_ms = math.huge })
end)
raises("options.ttl_ms must be a positive finite number", function()
	store:set("bad", 1, { ttl_ms = 0 / 0 })
end)
raises("options.ttl_ms must be a number", function()
	store:set("bad", 1, { ttl_ms = "10" } :: any)
end)
raises("unknown field 'ttl'", function()
	store:set("bad", 1, { ttl = 10 } :: any)
end)
raises("entries['bad'].ttl_ms must be a positive finite number", function()
	store:set_many({ good = { value = 1 }, bad = { value = 1, ttl_ms = 0 } })
end)
raises("entries['bad'] must be a record table", function()
	store:set_many({ bad = 1 } :: any)
end)
raises("entries['bad']: unknown field 'other'", function()
	store:set_many({ bad = { value = 1, other = 2 } } :: any)
end)
assert(store:get("bad") == nil)
assert(store:get("good") == nil)

-- persistent, long, batch_kept, batch_long, rewritten and revived are live.
assert(store:clear() == 6)
