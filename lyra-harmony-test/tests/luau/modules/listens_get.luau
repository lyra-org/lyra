local fixture = ...

local listens = require("@lyra/listens")

local ids = fixture.ids

local stats = listens.get({ ids.first, ids.second, ids.album, ids.amy })
local function expect(id: string, count: number, last_played_at_ms: number, what: string)
	local entry = stats[id]
	assert(entry ~= nil, `{what} has listens`)
	assert(entry.count == count, `{what} count {entry.count}`)
	assert(entry.last_played_at_ms == last_played_at_ms, `{what} last played {entry.last_played_at_ms}`)
end
expect(ids.first, 2, 5000, "a track counts only the caller's listens")
expect(ids.second, 1, 3000, "another track")
expect(ids.album, 3, 5000, "a release totals its tracks")
expect(ids.amy, 3, 5000, "an artist totals the tracks it is credited on")

local absent = listens.get({ ids.unplayed, ids.hidden, ids.mine, "missing-track" })
assert(next(absent) == nil, "unlistened, hidden, other kinds and missing ids are absent")
assert(next(listens.get({})) == nil, "no ids, no stats")

-- Without a provider declaring unique track ids, merging counts each track alone.
local merged = listens.get({ ids.first, ids.album }, { merge_unique_external_ids = true })
assert(merged[ids.first].count == 2, `merged track count {merged[ids.first].count}`)
assert(merged[ids.album].count == 3, "merging leaves releases alone")

local ok, err = pcall(listens.get, { ids.first }, { merge_unique_external_ids = "yes" })
assert(not ok and string.find(tostring(err), "merge_unique_external_ids must be a boolean", 1, true), tostring(err))
