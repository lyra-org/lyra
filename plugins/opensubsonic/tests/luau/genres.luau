local fixture, harness = ...
local call = require("./_call")(harness)
local covers = require("@lyra/covers")
local folders = require("../../catalog/folders")

local ids = fixture.ids
local key = fixture.api_keys.listener_key
local music = tostring(folders.id(fixture.libraries.music))
local hidden = tostring(folders.id(fixture.libraries.hidden))

local function sorted(list: { string }): { string }
	local copy = table.clone(list)
	table.sort(copy)
	return copy
end

-- getGenres lists the genres of the albums the caller sees by name, counting only those albums
-- and their songs: Jazz is only on a hidden album, which Rock's counts leave out.
local genres = call.ok("getGenres", key).genres.genre
assert(#genres == 2, `two genres, got {#genres}`)
assert(genres[1].value == "Pop" and genres[1].albumCount == 1 and genres[1].songCount == 1, "Pop")
assert(genres[2].value == "Rock" and genres[2].albumCount == 2 and genres[2].songCount == 3, "Rock")
local genres_xml = call.raw("getGenres", { apiKey = key }).body
local rock_xml = string.match(genres_xml, "<genre [^>]*>Rock</genre>")
assert(rock_xml, genres_xml)
assert(string.find(rock_xml, 'songCount="3"', 1, true) and string.find(rock_xml, 'albumCount="2"', 1, true), rock_xml)

-- getSongsByGenre pages through a genre's songs by title, matching its name in any case.
local function by_genre(params: { [string]: string }): { string }
	return call.ids(call.ok("getSongsByGenre", key, params).songsByGenre.song)
end
call.same(by_genre({ genre = "ROCK" }), { ids.l1, ids.s1, ids.s2 }, "Rock songs")
call.same(by_genre({ genre = "Rock", count = "1", offset = "1" }), { ids.s1 }, "a page")
call.same(by_genre({ genre = "Rock", offset = "3" }), {}, "past the end")
call.same(by_genre({ genre = "Rock", count = "0" }), {}, "count 0")
call.same(by_genre({ genre = "Rock", musicFolderId = music }), { ids.s1, ids.s2 }, "Rock in Music")
call.same(by_genre({ genre = "Jazz" }), {}, "a genre only on hidden albums")
call.same(by_genre({ genre = "Blues" }), {}, "an unknown genre")
local empty = call.ok("getSongsByGenre", key, { genre = "Blues" })
assert(type(empty.songsByGenre) == "table" and next(empty.songsByGenre) == nil, "an empty object")
local empty_xml = call.raw("getSongsByGenre", { apiKey = key, genre = "Blues" }).body
assert(string.find(empty_xml, "<songsByGenre/>", 1, true), empty_xml)
local songs_xml = call.raw("getSongsByGenre", { apiKey = key, genre = "Rock" }).body
local _, song_elements = string.gsub(songs_xml, "<song ", "")
assert(song_elements == 3, `three song elements, got {song_elements}`)

-- Songs carry their album's coverArt, or none.
local solo_cover = covers.get({ ids.solo })[ids.solo]
local solo_art = `{solo_cover.owner_id}.{string.sub(solo_cover.hash, 1, 8)}`
local rock = call.ok("getSongsByGenre", key, { genre = "Rock" }).songsByGenre.song
assert(rock[1].coverArt == nil, "Live has no cover")
assert(rock[2].coverArt == solo_art and rock[3].coverArt == solo_art, `Solo's coverArt {rock[2].coverArt}`)
assert(rock[2].title == "S1" and rock[2].genre == "Rock" and rock[2].albumId == ids.solo, "a full song")

assert(call.error_code("getSongsByGenre", key, {}) == 10, "getSongsByGenre needs a genre")
assert(call.error_code("getSongsByGenre", key, { genre = "Rock", count = "x" }) == 0, "a count is an integer")
assert(call.error_code("getSongsByGenre", key, { genre = "Rock", offset = "x" }) == 0, "an offset too")
assert(call.error_code("getSongsByGenre", key, { genre = "Rock", musicFolderId = "1" }) == 70, "an unknown folder")
assert(call.error_code("getSongsByGenre", key, { genre = "Rock", musicFolderId = hidden }) == 70, "a hidden folder")

-- getRandomSongs picks among the songs the caller sees that pass every filter.
local function random(params: { [string]: string }?): { string }
	return sorted(call.ids(call.ok("getRandomSongs", key, params).randomSongs.song))
end
local visible = sorted({ ids.s1, ids.s2, ids.o1, ids.l1, ids.b1 })
call.same(random(), visible, "every song")
call.same(random({ genre = "rock" }), sorted({ ids.s1, ids.s2, ids.l1 }), "Rock songs")
call.same(random({ musicFolderId = music }), sorted({ ids.s1, ids.s2, ids.o1 }), "songs in Music")
call.same(random({ genre = "Rock", musicFolderId = music }), sorted({ ids.s1, ids.s2 }), "Rock in Music")
call.same(random({ genre = "Jazz" }), {}, "a genre only on hidden albums")
call.same(random({ genre = "Blues" }), {}, "an unknown genre")
assert(#random({ size = "2" }) == 2, "size")
call.same(random({ size = "0" }), {}, "size 0")

-- Years bound the songs' own years inclusively; undated songs fall outside any bound.
call.same(random({ fromYear = "2001" }), sorted({ ids.s2, ids.o1, ids.l1 }), "from 2001")
call.same(random({ toYear = "2001" }), sorted({ ids.s1, ids.s2, ids.l1 }), "to 2001")
call.same(random({ fromYear = "2000", toYear = "2004" }), sorted({ ids.s2, ids.l1 }), "2000 to 2004")
call.same(random({ fromYear = "1000", toYear = "3000" }), sorted({ ids.s1, ids.s2, ids.o1, ids.l1 }), "wide")
call.same(random({ fromYear = "2006" }), {}, "after every song")
call.same(random({ toYear = "1990" }), {}, "before every song")
call.same(random({ fromYear = "2004", toYear = "2000" }), {}, "a reversed range")
call.same(random({ fromYear = "-5" }), sorted({ ids.s1, ids.s2, ids.o1, ids.l1 }), "a negative fromYear")
call.same(random({ toYear = "-1" }), {}, "a negative toYear")
call.same(random({ fromYear = "99999999999" }), {}, "a fromYear past any year")
call.same(random({ toYear = "99999999999" }), sorted({ ids.s1, ids.s2, ids.o1, ids.l1 }), "a toYear past any year")
call.same(random({ genre = "Pop", toYear = "2001" }), {}, "Pop before 2001")
call.same(random({ musicFolderId = music, fromYear = "2001" }), sorted({ ids.s2, ids.o1 }), "Music from 2001")

-- Each request shuffles anew: over 40 requests for one of two songs, both come up.
local picked: { [string]: boolean } = {}
for _ = 1, 40 do
	picked[random({ genre = "Rock", musicFolderId = music, size = "1" })[1]] = true
end
assert(picked[ids.s1] and picked[ids.s2], "random songs vary")

local some = call.ok("getRandomSongs", key, { genre = "Rock", musicFolderId = music }).randomSongs.song
assert(some[1].coverArt == solo_art and some[2].coverArt == solo_art, "random songs carry coverArt")
local bare = call.ok("getRandomSongs", key, { toYear = "1000" })
assert(type(bare.randomSongs) == "table" and next(bare.randomSongs) == nil, "an empty object")
local bare_xml = call.raw("getRandomSongs", { apiKey = key, genre = "Blues" }).body
assert(string.find(bare_xml, "<randomSongs/>", 1, true), bare_xml)
local random_xml = call.raw("getRandomSongs", { apiKey = key }).body
local _, random_elements = string.gsub(random_xml, "<song ", "")
assert(random_elements == 5, `five song elements, got {random_elements}`)

assert(call.error_code("getRandomSongs", key, { size = "ten" }) == 0, "a size is an integer")
assert(call.error_code("getRandomSongs", key, { fromYear = "recent" }) == 0, "a year is an integer")
assert(call.error_code("getRandomSongs", key, { toYear = "1.5" }) == 0, "a whole one")
assert(call.error_code("getRandomSongs", key, { musicFolderId = "1" }) == 70, "an unknown folder")
assert(call.error_code("getRandomSongs", key, { musicFolderId = hidden }) == 70, "a hidden folder")
