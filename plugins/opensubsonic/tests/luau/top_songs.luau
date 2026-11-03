local fixture, harness = ...
local call = require("./_call")(harness)
local serde = require("@harmony/serde")

local ids = fixture.ids
local key = fixture.api_keys.listener_key
local friend = fixture.api_keys.friend_key

local function top(params: { [string]: string }, api_key: string?): { string }
	local body = call.ok("getTopSongs", api_key or key, params)
	assert(type(body.topSongs) == "table", "a topSongs list")
	return call.ids(body.topSongs.song)
end

-- An artist's songs the caller played, most played first, then most recently played; songs they
-- never played and songs they can't see stay out.
call.same(top({ id = ids.amy }), { ids.a1, ids.a2, ids.a3 }, "by id")
call.same(top({ id = ids.amy, count = "2" }), { ids.a1, ids.a2 }, "a count")
call.same(top({ id = ids.amy, count = "0" }), {}, "no songs")
call.same(top({ id = ids.bob }), { ids.b1 }, "another artist")
-- An artist credited on an album holds its songs.
call.same(top({ id = ids.cid }), { ids.b1 }, "an album artist")

-- A name finds every artist of that name, compared as names are.
call.same(top({ artist = "amy" }), { ids.a1, ids.n1, ids.a2, ids.a3 }, "by name")
call.same(top({ artist = "Bob" }), { ids.b1 }, "a single artist by name")
-- The id comes first.
call.same(top({ id = ids.namesake, artist = "Bob" }), { ids.n1 }, "id over name")

-- Unknown names and artists the caller never played have no top songs.
call.same(top({ artist = "Nobody" }), {}, "an unknown name")
call.same(top({ artist = "Hal" }), {}, "a hidden artist's name")
call.same(top({ id = ids.amy }, friend), { ids.a4 }, "the friend's own listens")
call.same(top({ id = ids.bob }, friend), {}, "an artist the friend never played")

-- Ids name a visible artist.
assert(call.error_code("getTopSongs", key, { id = ids.hal }) == 70, "a hidden artist")
assert(call.error_code("getTopSongs", key, { id = "missing" }) == 70, "an unknown id")
assert(call.error_code("getTopSongs", key, { id = ids.a1 }) == 70, "a song isn't an artist")
assert(call.error_code("getTopSongs", key, {}) == 10, "an artist or id is required")
assert(call.error_code("getTopSongs", key, { artist = "Amy", count = "lots" }) == 0, "a count is an integer")

local song = call.ok("getTopSongs", key, { id = ids.amy }).topSongs.song[1]
assert(song.title == "A1" and song.playCount == 3 and song.isDir == false, "each song is a Child")
local empty_xml = call.raw("getTopSongs", { apiKey = key, artist = "Nobody" }).body
assert(string.find(empty_xml, "<topSongs/>", 1, true), empty_xml)

-- The extension is advertised.
local body = call.raw("getOpenSubsonicExtensions", { f = "json" }, { bare = true }).body
local advertised = false
for _, extension in (serde.json.decode(body) :: any)["subsonic-response"].openSubsonicExtensions do
	if extension.name == "topSongsByArtistId" then
		advertised = #extension.versions == 1 and extension.versions[1] == 1
	end
end
assert(advertised, "topSongsByArtistId version 1")
