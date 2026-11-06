local fixture = ...

local bookmarks = require("@lyra/bookmarks")
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

assert(bookmarks.MAX_COMMENT_CHARS == 1000, `cap {bookmarks.MAX_COMMENT_CHARS}`)

-- Setting: positions past a known duration clamp, unknown durations keep the position.
assert(bookmarks.set(ids.timed, 500000, { comment = "outro" }), "a visible track takes a bookmark")
assert(bookmarks.set(ids.untimed, 500000), "a track without a duration takes a bookmark")
assert(not bookmarks.set(ids.hidden, 0), "a hidden track takes no bookmark")
assert(not bookmarks.set(ids.album, 0), "a release takes no bookmark")
assert(not bookmarks.set(ids.amy, 0), "an artist takes no bookmark")
assert(not bookmarks.set(ids.mix, 0), "a playlist takes no bookmark")
assert(not bookmarks.set("missing-target", 0), "a missing target takes no bookmark")

rejects(bookmarks.set, "position_ms must be a non-negative integer within the exact numeric range", ids.timed, -1)
rejects(bookmarks.set, "position_ms must be a non-negative integer within the exact numeric range", ids.untimed, 2 ^ 53)
rejects(bookmarks.set, "comment exceeds 1000 characters", ids.timed, 0, { comment = string.rep("a", 1001) })
rejects(bookmarks.set, "unknown field", ids.timed, 0, { note = "x" })

local got = bookmarks.get({ ids.timed, ids.untimed, ids.early, ids.hidden, ids.album, "missing-target" })
local timed = assert(got[ids.timed], "timed bookmark")
assert(timed.position_ms == 180000, `clamped {timed.position_ms}`)
assert(timed.comment == "outro", `comment {timed.comment}`)
assert(timed.created_at_ms == timed.updated_at_ms, "a new bookmark was created when it was set")
local untimed = assert(got[ids.untimed], "untimed bookmark")
assert(untimed.position_ms == 500000, `kept {untimed.position_ms}`)
assert(untimed.comment == nil, "no comment")
local early = assert(got[ids.early], "seeded bookmark")
assert(early.position_ms == 5000 and early.created_at_ms == 1000, "seeded values")
assert(got[ids.hidden] == nil, "a hidden track's bookmark is absent")
assert(got[ids.album] == nil and got["missing-target"] == nil, "non-tracks are absent")
assert(next(bookmarks.get({})) == nil, "no ids, no bookmarks")

-- Updating keeps when the bookmark was created, and replaces the rest.
assert(bookmarks.set(ids.early, 6000, { comment = "" }), "a bookmark can change")
local changed = bookmarks.get({ ids.early })[ids.early]
assert(changed.position_ms == 6000, `moved {changed.position_ms}`)
assert(changed.comment == nil, "an empty comment stores none")
assert(changed.created_at_ms == 1000, `created {changed.created_at_ms}`)
assert(changed.updated_at_ms > 1000, `updated {changed.updated_at_ms}`)

-- Filtering by the caller's bookmarks.
same(order(tracks.query({ bookmarked = false }).items), {}, "not bookmarked")

-- Removing.
assert(bookmarks.remove(ids.timed), "a bookmark can be removed")
assert(bookmarks.remove(ids.timed), "removing again still names a track")
assert(bookmarks.get({ ids.timed })[ids.timed] == nil, "a removed bookmark is gone")
same(order(tracks.query({ bookmarked = false }).items), { ids.timed }, "removed bookmark")
assert(not bookmarks.remove(ids.hidden), "a hidden bookmark's removal answers as missing")
assert(not bookmarks.remove(ids.album), "a release has no bookmark")
assert(not bookmarks.remove("missing-target"), "a missing target has no bookmark")

-- Sorting by when each bookmark was last set: changing `early` moved it past the seeded `later`.
assert(bookmarks.remove(ids.untimed), "the other bookmark set just now can go")
same(
	order(tracks.query({ bookmarked = true, sort = { { key = "bookmarked_at", order = "ascending" } } }).items),
	{ ids.later, ids.early },
	"bookmarked_at"
)
