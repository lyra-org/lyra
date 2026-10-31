local fixture = ...

local artists = require("@lyra/artists")
local ratings = require("@lyra/ratings")
local releases = require("@lyra/releases")
local tracks = require("@lyra/tracks")

local ids = fixture.ids

local function rejects(f: (...any) -> ...any, message: string, ...: any)
	local ok, err = pcall(f, ...)
	assert(not ok, `rejected: {message}`)
	assert(string.find(tostring(err), message, 1, true), `error {err}`)
end

local function order(items: { any }): { string }
	local result = {}
	for _, item in items do
		table.insert(result, item.id)
	end
	return result
end

local function same(actual: { string }, expected: { string }, label: string)
	assert(#actual == #expected, `{label}: {#actual} items`)
	for index, id in expected do
		assert(actual[index] == id, `{label}: item {index} is {actual[index]}, expected {id}`)
	end
end

assert(ratings.set(ids.low, 2), "a visible track is ratable")
assert(ratings.set(ids.high, 3), "a visible track is ratable")
assert(ratings.set(ids.high, 5), "a rating can change")
assert(ratings.set(ids.album, 4), "a release is ratable")
assert(ratings.set(ids.bea, 1), "an artist is ratable")
assert(not ratings.set(ids.hidden, 4), "a hidden track is not ratable")
assert(not ratings.set(ids.mix, 4), "a playlist is not ratable")
assert(not ratings.set("missing-target", 4), "a missing target is not ratable")

rejects(ratings.set, "rating must be an integer from 1 to 5, got 0", ids.low, 0)
rejects(ratings.set, "rating must be an integer from 1 to 5, got 6", ids.low, 6)
rejects(ratings.set, "expected string", 1, 3)

local got = ratings.get({ ids.low, ids.high, ids.unrated, ids.hidden, ids.album, ids.bea, ids.amy, "missing-target" })
assert(got[ids.low] == 2, `low {got[ids.low]}`)
assert(got[ids.high] == 5, `high {got[ids.high]}`)
assert(got[ids.album] == 4, `album {got[ids.album]}`)
assert(got[ids.bea] == 1, `bea {got[ids.bea]}`)
assert(got[ids.unrated] == nil, "another user's rating is not the caller's")
assert(got[ids.hidden] == nil, "a hidden target's rating is absent")
assert(got[ids.amy] == nil and got["missing-target"] == nil, "unrated and missing targets are absent")
assert(next(ratings.get({})) == nil, "no ids, no ratings")

-- Averages across users.
local averages = ratings.average({ ids.low, ids.high, ids.unrated, ids.hidden, ids.amy, "missing-target" })
assert(averages[ids.low] == 2, `low average {averages[ids.low]}`)
assert(averages[ids.high] == 4.5, `high average {averages[ids.high]}`)
assert(averages[ids.unrated] == 5, "another user's rating counts toward the average")
assert(averages[ids.hidden] == nil, "a hidden target's average is absent")
assert(averages[ids.amy] == nil and averages["missing-target"] == nil, "unrated and missing targets are absent")
assert(next(ratings.average({})) == nil, "no ids, no averages")

-- Filters.
same(order(tracks.query({ min_rating = 3 }).items), { ids.high }, "min_rating")
same(order(tracks.query({ max_rating = 2 }).items), { ids.low }, "max_rating")
same(order(releases.query({ min_rating = 4, max_rating = 4 }).items), { ids.album }, "release rating")
same(order(artists.query({ max_rating = 1 }).items), { ids.bea }, "artist rating")
rejects(tracks.query, "min_rating must not exceed max_rating", { min_rating = 4, max_rating = 2 })
rejects(tracks.query, "min_rating must be an integer from 1 to 5, got 0", { min_rating = 0 })
rejects(releases.query, "max_rating must be an integer from 1 to 5, got 6", { max_rating = 6 })
local rated_tracks = tracks.query({
	release_ids = { ids.album },
	rated = true,
	sort = { { key = "rating", order = "descending" } },
})
same(order(rated_tracks.items), { ids.high, ids.low }, "rated")
same(order(tracks.query({ release_ids = { ids.album }, rated = false }).items), { ids.unrated }, "unrated tracks")
same(order(releases.query({ rated = false }).items), { ids.single }, "unrated releases")
same(order(artists.query({ rated = false }).items), { ids.amy }, "unrated artists")
rejects(
	tracks.query,
	"min_rating and max_rating cannot be combined with rated = false",
	{ rated = false, min_rating = 1 }
)

-- Sorting: unrated items sort last in both directions.
local function by_rating(direction: string): { string }
	return order(tracks.query({ release_ids = { ids.album }, sort = { { key = "rating", order = direction } } }).items)
end
same(by_rating("descending"), { ids.high, ids.low, ids.unrated }, "descending")
same(by_rating("ascending"), { ids.low, ids.high, ids.unrated }, "ascending")
same(
	order(releases.query({ sort = { { key = "rating", order = "ascending" } } }).items),
	{ ids.album, ids.single },
	"release sort"
)
same(
	order(artists.query({ sort = { { key = "rating", order = "descending" } } }).items),
	{ ids.bea, ids.amy },
	"artist sort"
)

-- Removing.
assert(ratings.remove(ids.low), "a rating can be removed")
assert(ratings.remove(ids.low), "removing again still names a ratable target")
assert(ratings.get({ ids.low })[ids.low] == nil, "a removed rating is gone")
assert(ratings.average({ ids.low })[ids.low] == nil, "a removed rating leaves the average")
assert(ratings.remove(ids.hidden), "a hidden target's rating can be cleared")
assert(not ratings.remove(ids.mix), "a playlist has no rating")
assert(not ratings.remove("missing-target"), "a missing target has no rating")
