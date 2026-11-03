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
	call.body(call.raw("search3", { apiKey = key, query = "", songOffset = "10", albumCount = "0", artistCount = "0" }))
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

-- `search2` searches as `search3` does, with artists as `Artist` and albums as directories under
-- their first artist.
local function search2(params: { [string]: string }): any
	return call.ok("search2", key, params).searchResult2
end

local all2 = search2({ query = "", songCount = "500" })
call.same(call.ids(all2.song), call.ids(all.song), "search2 songs")
call.same(call.ids(all2.album), call.ids(all.album), "search2 albums")
call.same(call.ids(all2.artist), call.ids(all.artist), "search2 artists")

local amy = all2.artist[1]
assert(amy.name == "Amy" and amy.albumCount == nil and amy.sortName == nil, "an Artist, not an ArtistID3")
assert(amy.userRating == 3 and amy.averageRating == 4, `artist ratings {amy.userRating} {amy.averageRating}`)
assert(amy.starred == "2023-11-14T22:13:20Z", `artist starred {amy.starred}`)
assert(all2.artist[2].userRating == nil and all2.artist[2].starred == nil, "an unrated, unstarred artist")

local solo = all2.album[2]
assert(solo.id == ids.solo and solo.isDir == true and solo.mediaType == "album", "an album directory")
assert(solo.title == "Solo" and solo.album == "Solo" and solo.name == nil, `album title {solo.title}`)
assert(solo.parent == ids.amy and solo.artistId == ids.amy and solo.artist == "Amy", "under its artist")
assert(solo.userRating == 5 and solo.averageRating == 4, `album ratings {solo.userRating} {solo.averageRating}`)
assert(solo.starred and solo.playCount == 2 and solo.genre == "Rock" and solo.duration == 380, "album fields")
assert(#solo.albumArtists == 1 and solo.albumArtists[1].id == ids.amy and solo.displayAlbumArtist == "Amy")
assert(all2.album[1].averageRating == nil, "an album nobody rated")

call.same(call.ids(search2({ query = "sol", songCount = "0", artistCount = "0" }).album), { ids.solo }, "search2 query")
local empty2_xml =
	call.body(call.raw("search2", { apiKey = key, query = "", songOffset = "10", albumCount = "0", artistCount = "0" }))
assert(string.find(empty2_xml, "<searchResult2/>", 1, true), empty2_xml)
assert(#search2({ query = "", musicFolderId = folder }).song == 3, "search2 in a folder")
assert(call.error_code("search2", key, { musicFolderId = "1", query = "" }) == 70, "search2 in an unknown folder")
assert(call.error_code("search2", key, {}) == 10, "search2 needs a query")
