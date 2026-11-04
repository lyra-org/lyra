local fixture, harness = ...
local call = require("./_call")(harness)

local ids = fixture.ids
local key = fixture.api_keys.listener_key

local function bookmarks(api_key: string?): any
	return call.ok("getBookmarks", api_key or key).bookmarks
end

local function song(id: string): any
	return call.ok("getSong", key, { id = id }).song
end

-- A seeded bookmark carries its song, its owner and its times.
local seeded = bookmarks().bookmark
assert(#seeded == 1, `{#seeded} bookmarks`)
local bridge = seeded[1]
assert(bridge.entry.id == ids.s3 and bridge.entry.title == "S3", "the bookmarked song")
assert(bridge.entry.bookmarkPosition == 30000, `entry position {bridge.entry.bookmarkPosition}`)
assert(bridge.position == 30000, `position {bridge.position}`)
assert(bridge.username == "listener", bridge.username)
assert(bridge.comment == "Bridge", bridge.comment)
assert(bridge.created == "2023-11-14T22:13:20Z" and bridge.changed == bridge.created, "seeded times")
assert(song(ids.s3).bookmarkPosition == 30000, "a song shows its bookmark")
assert(song(ids.s1).bookmarkPosition == nil, "another user's bookmark isn't the caller's")

-- Creating clamps a position past a known end, keeps one past an unknown end, and lists the most
-- recently changed first.
call.ok("createBookmark", key, { id = ids.s1, position = "999000", comment = "Outro" })
call.ok("createBookmark", key, { id = ids.s2, position = "999000" })
local listed = bookmarks().bookmark
assert(#listed == 3 and listed[3].entry.id == ids.s3, "the older bookmark comes last")
local by_song = {}
for _, bookmark in listed do
	by_song[bookmark.entry.id] = bookmark
end
assert(by_song[ids.s2].position == 999000, `unknown end {by_song[ids.s2].position}`)
assert(by_song[ids.s2].comment == nil, "no comment")
assert(by_song[ids.s1].position == 180000, `clamped {by_song[ids.s1].position}`)
assert(by_song[ids.s1].comment == "Outro", by_song[ids.s1].comment)
assert(type(by_song[ids.s1].created) == "string" and type(by_song[ids.s1].changed) == "string", "times")

-- Updating replaces the position and comment and keeps when it was created.
call.ok("createBookmark", key, { id = ids.s3, position = "45000" })
local updated = bookmarks().bookmark[1]
assert(updated.entry.id == ids.s3, "the changed bookmark comes first")
assert(updated.position == 45000 and updated.entry.bookmarkPosition == 45000, "moved")
assert(updated.comment == nil, "the comment is replaced")
assert(updated.created == "2023-11-14T22:13:20Z" and updated.changed ~= updated.created, "times kept and changed")

-- Only songs the caller sees take bookmarks.
assert(call.error_code("createBookmark", key, { id = ids.solo, position = "0" }) == 70, "an album")
assert(call.error_code("createBookmark", key, { id = ids.amy, position = "0" }) == 70, "an artist")
assert(call.error_code("createBookmark", key, { id = ids.h1, position = "0" }) == 70, "a hidden song")
assert(call.error_code("createBookmark", key, { id = "missing", position = "0" }) == 70, "a missing song")
assert(call.error_code("createBookmark", key, { id = ids.s1 }) == 10, "a position is required")
assert(call.error_code("createBookmark", key, { position = "0" }) == 10, "an id is required")
assert(call.error_code("createBookmark", key, { id = ids.s1, position = "-1" }) == 0, "a negative position")
assert(call.error_code("createBookmark", key, { id = ids.s1, position = "1.5" }) == 0, "an integer position")
assert(
	call.error_code("createBookmark", key, { id = ids.s1, position = "0", comment = string.rep("a", 1001) }) == 0,
	"a comment past the cap"
)

-- Bookmarks are the caller's own.
local neighbours = bookmarks(fixture.api_keys.neighbour_key).bookmark
assert(#neighbours == 1 and neighbours[1].entry.id == ids.s1, "the neighbour's bookmark")
assert(neighbours[1].position == 5000 and neighbours[1].username == "neighbour", "the neighbour's position")

-- Deleting.
call.ok("deleteBookmark", key, { id = ids.s1 })
call.ok("deleteBookmark", key, { id = ids.s1 })
assert(song(ids.s1).bookmarkPosition == nil, "a deleted bookmark")
assert(call.error_code("deleteBookmark", key, { id = ids.solo }) == 70, "an album has no bookmark")
assert(call.error_code("deleteBookmark", key, { id = ids.h1 }) == 70, "a hidden song's bookmark")
assert(call.error_code("deleteBookmark", key, {}) == 10, "an id is required")
call.ok("deleteBookmark", key, { id = ids.s2 })
call.ok("deleteBookmark", key, { id = ids.s3 })
local empty = bookmarks()
assert(type(empty) == "table" and next(empty) == nil, "no bookmarks answer an empty object")
