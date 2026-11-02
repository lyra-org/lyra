local fixture, harness = ...
local call = require("./_call")(harness)
local folders = require("../../catalog/folders")

local ids = fixture.ids
local key = fixture.api_keys.listener_key

local function search(params: { [string]: string }): any
	return call.ok("search3", key, params).searchResult3
end

-- An empty query pages through every song, album and artist the caller sees.
local all = search({ query = "", songCount = "500" })
call.same(call.ids(all.song), { ids.o1, ids.s1, ids.s2 }, "every song")
call.same(call.ids(all.album), { ids.other, ids.solo }, "every album")
call.same(call.ids(all.artist), { ids.amy, ids.bob }, "every artist")
call.same(call.ids(search({ query = "", songCount = "2", songOffset = "2" }).song), { ids.s2 }, "a page of songs")
call.same(call.ids(search({ query = '""', songCount = "1" }).song), { ids.o1 }, "a quoted empty query")
assert(all.artist[1].albumCount == 1 and all.artist[1].name == "Amy", "artist album count")

-- A count of zero leaves its kind out; past the end is empty.
local songs_only = search({ query = "", albumCount = "0", artistCount = "0" })
assert(songs_only.album == nil and songs_only.artist == nil and #songs_only.song == 3)
local past_end = search({ query = "", songOffset = "10", albumCount = "0", artistCount = "0" })
assert(type(past_end) == "table" and past_end.song == nil, "an empty result is an empty object")
local past_end_xml =
	call.raw("search3", { apiKey = key, query = "", songOffset = "10", albumCount = "0", artistCount = "0" }).body
assert(string.find(past_end_xml, "<searchResult3/>", 1, true), past_end_xml)

-- A query matches names by word prefix, through related items too.
call.same(call.ids(search({ query = "sol", songCount = "0", artistCount = "0" }).album), { ids.solo }, "album search")
call.same(
	call.ids(search({ query = "solo*", albumCount = "0", artistCount = "0" }).song),
	{ ids.s1, ids.s2 },
	"a Lucene wildcard"
)
call.same(call.ids(search({ query = "bob", songCount = "0", albumCount = "0" }).artist), { ids.bob }, "artist search")
call.same(call.ids(search({ query = "secret" }).album), {}, "a hidden album")

local folder = tostring(folders.id(fixture.libraries.music))
assert(#search({ query = "", musicFolderId = folder }).song == 3, "in a folder")
assert(call.error_code("search3", key, { musicFolderId = "1", query = "" }) == 70, "an unknown folder")
assert(call.error_code("search3", key, {}) == 10, "search3 needs a query")
