local fixture, harness = ...
local call = require("./_call")(harness)
local folders = require("../../catalog/folders")

local ids = fixture.ids
local key = fixture.api_keys.listener_key

local function list(params: { [string]: string }): { string }
	local body = call.ok("getAlbumList2", key, params)
	return call.ids(body.albumList2.album)
end

-- Every list holds only the caller's albums.
call.same(list({ type = "newest" }), { ids.solo, ids.other }, "newest")
call.same(list({ type = "newest", size = "1", offset = "1" }), { ids.other }, "newest, paged")
call.same(list({ type = "newest", offset = "2" }), {}, "past the end")
call.same(list({ type = "alphabeticalByName" }), { ids.other, ids.solo }, "by name")
call.same(list({ type = "alphabeticalByArtist" }), { ids.solo, ids.other }, "by artist")
call.same(list({ type = "frequent" }), { ids.solo, ids.other }, "most played")
call.same(list({ type = "recent" }), { ids.solo, ids.other }, "recently played")
call.same(list({ type = "starred" }), { ids.solo }, "starred")
call.same(list({ type = "highest" }), { ids.solo }, "rated")
-- An album's year is its release date's, which fixture releases go without.
call.same(list({ type = "byYear", fromYear = "1990", toYear = "2010" }), {}, "undated albums")
call.same(list({ type = "byGenre", genre = "ROCK" }), { ids.solo }, "in a genre")
call.same(list({ type = "byGenre", genre = "Jazz" }), {}, "an unknown genre")
assert(#list({ type = "random" }) == 2, "random")

-- The host shuffles by the seed it is given, so each random list asks with its own: over 40 lists
-- of two albums, both orders come up.
local orders = {}
for _ = 1, 40 do
	orders[table.concat(list({ type = "random" }), ",")] = true
end
assert(orders[`{ids.solo},{ids.other}`] and orders[`{ids.other},{ids.solo}`], "random lists vary")

local empty_xml = call.raw("getAlbumList2", { apiKey = key, type = "byGenre", genre = "Jazz" }).body
assert(string.find(empty_xml, "<albumList2/>", 1, true), empty_xml)

local folder = tostring(folders.id(fixture.libraries.music))
call.same(list({ type = "newest", musicFolderId = folder }), { ids.solo, ids.other }, "in a folder")
assert(call.error_code("getAlbumList2", key, { type = "newest", musicFolderId = "1" }) == 70, "an unknown folder")
local hidden_folder = tostring(folders.id(fixture.libraries.hidden))
assert(
	call.error_code("getAlbumList2", key, { type = "newest", musicFolderId = hidden_folder }) == 70,
	"a hidden folder"
)

-- An empty list is an empty object.
local empty = call.ok("getAlbumList2", key, { type = "byGenre", genre = "Jazz" })
assert(type(empty.albumList2) == "table" and empty.albumList2.album == nil)

-- Albums carry the caller's listens.
local frequent = call.ok("getAlbumList2", key, { type = "frequent" }).albumList2.album
assert(frequent[1].playCount == 2 and frequent[2].playCount == 1, "play counts")

assert(call.error_code("getAlbumList2", key, {}) == 10, "a list needs a type")
assert(call.error_code("getAlbumList2", key, { type = "loudest" }) == 0, "an unknown type")
assert(call.error_code("getAlbumList2", key, { type = "byYear", fromYear = "2000" }) == 10, "byYear needs both years")
assert(call.error_code("getAlbumList2", key, { type = "byGenre" }) == 10, "byGenre needs a genre")
assert(call.error_code("getAlbumList2", key, { type = "newest", size = "ten" }) == 0, "a size is an integer")

-- Another user's listens, favorites and ratings don't count.
local curator = fixture.api_keys.curator_key
local function list_as_curator(list_type: string): { string }
	return call.ids(call.ok("getAlbumList2", curator, { type = list_type }).albumList2.album)
end
call.same(list_as_curator("frequent"), {}, "the curator's most played")
call.same(list_as_curator("recent"), {}, "the curator's recently played")
call.same(list_as_curator("starred"), {}, "the curator's starred")
call.same(list_as_curator("highest"), {}, "the curator's rated")

-- `getAlbumList` takes the same lists, its albums directories under their first artist.
local function folder_list(params: { [string]: string }): { any }
	local album_list = call.ok("getAlbumList", key, params).albumList
	assert(type(album_list) == "table", "an albumList")
	return album_list.album or {}
end
call.same(call.ids(folder_list({ type = "newest" })), { ids.solo, ids.other }, "folder newest")
call.same(call.ids(folder_list({ type = "newest", size = "1", offset = "1" })), { ids.other }, "folder newest, paged")
call.same(call.ids(folder_list({ type = "alphabeticalByName" })), { ids.other, ids.solo }, "folder by name")
call.same(call.ids(folder_list({ type = "starred" })), { ids.solo }, "folder starred")
call.same(call.ids(folder_list({ type = "byGenre", genre = "Rock" })), { ids.solo }, "folder in a genre")
call.same(
	call.ids(folder_list({ type = "newest", musicFolderId = folder })),
	{ ids.solo, ids.other },
	"folder in a folder"
)
local played = folder_list({ type = "frequent" })
call.same(call.ids(played), { ids.solo, ids.other }, "folder most played")
local solo = played[1]
assert(solo.isDir == true and solo.title == "Solo" and solo.album == "Solo", "a directory Child")
assert(solo.parent == ids.amy and solo.artist == "Amy" and solo.artistId == ids.amy, "under its artist")
assert(solo.playCount == 2 and solo.userRating == 5 and solo.genre == "Rock", "the caller's view")
assert(solo.songCount == nil and solo.name == nil, "no AlbumID3 fields")
call.same(call.ids(call.ok("getAlbumList", curator, { type = "frequent" }).albumList.album), {}, "the curator's")
assert(call.error_code("getAlbumList", key, {}) == 10, "a folder list needs a type")
assert(call.error_code("getAlbumList", key, { type = "loudest" }) == 0, "an unknown folder list type")
assert(call.error_code("getAlbumList", key, { type = "newest", musicFolderId = "1" }) == 70, "an unknown folder")
local empty_folder_xml = call.raw("getAlbumList", { apiKey = key, type = "byGenre", genre = "Jazz" }).body
assert(string.find(empty_folder_xml, "<albumList/>", 1, true), empty_folder_xml)
local folder_xml = call.raw("getAlbumList", { apiKey = key, type = "starred" }).body
assert(string.find(folder_xml, "<albumList><album ", 1, true), folder_xml)
assert(string.find(folder_xml, `id="{ids.solo}" isDir="true"`, 1, true), folder_xml)
