local datastore = require("@lyra/datastore")

local state = require("../../state")

local HOUR_MS = 60 * 60 * 1000

-- Each miss waits twice as long as the one before, up to a day.
local waits: { number } = {}
for _ = 1, 7 do
	local loaded = state.load("track-1", "Song|Artist|Album|180")
	local wait_ms = state.miss_backoff_ms(loaded.prev_attempt)
	table.insert(waits, wait_ms)
	local now = state.now_ms()
	loaded.set_attempt("miss", now + wait_ms)
	loaded.flush()
end
local expected = { 1, 2, 4, 8, 16, 24, 24 }
for index, hours in expected do
	assert(waits[index] == hours * HOUR_MS, `miss {index} waits {waits[index]} ms, expected {hours} h`)
end

-- A hit records its id under the store's expiry and its attempt as persistent.
local hit = state.load("track-2", "Other|Artist||")
assert(hit.prev_attempt == nil and hit.cached_id == nil)
hit.set_cache("42")
hit.set_attempt("hit", state.now_ms())
hit.flush()

local reloaded = state.load("track-2", "Other|Artist||")
assert(reloaded.cached_id == "42", "the cached id reads back")
assert(reloaded.prev_attempt ~= nil and reloaded.prev_attempt.status == "hit")
assert(state.miss_backoff_ms(reloaded.prev_attempt) == HOUR_MS, "a miss after a hit starts over")

-- Cached ids from before stored expiry live under another prefix and are never read.
datastore.get_or_create("lrclib_state"):set("cache:id:Old|Artist||", { lrclib_id = "7", expires_at_ms = 0 })
assert(state.load("track-3", "Old|Artist||").cached_id == nil)
