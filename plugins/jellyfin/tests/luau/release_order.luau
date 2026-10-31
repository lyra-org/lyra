local lookups = require("../../protocol/items/lookups")

-- A track on several releases shows the one added first: the earliest creation time, a release
-- without one after those with one, then the lowest id. Fixtures can't put a track on two releases
-- or set a release's creation time, so this checks the rule on records.

local function release(id: string, created_at_ms: number?): any
	return { id = id, release_title = id, created_at_ms = created_at_ms }
end

local function order(releases: { any }): string
	local ids = {}
	for _, item in lookups.releases_by_added(releases) do
		table.insert(ids, item.id)
	end
	return table.concat(ids, ",")
end

local releases = { release("d", nil), release("c", 200), release("b", 100), release("a", nil), release("e", 100) }
assert(order(releases) == "b,e,c,a,d", `by creation time, then id, untimed last, got "{order(releases)}"`)
assert(releases[1].id == "d", "the given list is left alone")
local first = lookups.first_added_release(releases)
assert(first and first.id == "b", `the first added, got {first and first.id}`)
assert(lookups.first_added_release({}) == nil, "none of no releases")
assert(lookups.first_added_release(nil) == nil, "none without releases")
