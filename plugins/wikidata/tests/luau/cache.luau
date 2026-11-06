local _, harness = ...

local datastore = require("@lyra/datastore")

local shared = require("../../resolution/shared")

-- Entries are stored as the data itself, under the store's own expiry.
shared.cache_set("release:kept", { qid = "Q1", genres = 2 }, shared.HIT_CACHE_TTL_MS)

local kept = shared.cache_get("release:kept")
assert(kept ~= nil and kept.qid == "Q1" and kept.genres == 2, "a cached hit reads back")

local stored = datastore.get_or_create("cache"):get("release:kept") :: { [string]: any }
assert(stored.qid == "Q1" and stored.data == nil and stored.expires_at_ms == nil, "no wrapper is stored")

-- An expired entry reads as a miss.
shared.cache_set("release:short", { miss = true }, 1)
local deadline = os.time() + 5
while shared.cache_get("release:short") ~= nil do
	assert(os.time() < deadline, "a short-lived entry never expired")
	harness.task:wait(0.01)
end
assert(shared.cache_get("release:kept") ~= nil, "an unexpired entry stays")
