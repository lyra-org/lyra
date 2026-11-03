local fixture, harness = ...
local call = require("./_call")(harness)

local key = fixture.api_keys.listener_key

local function random(params: { [string]: string }?): number
	return #call.ok("getRandomSongs", key, params).randomSongs.song
end

local function by_genre(params: { [string]: string }): { any }
	return call.ok("getSongsByGenre", key, params).songsByGenre.song
end

-- Ten songs by default, and at most 500.
assert(random() == 10, "getRandomSongs gives 10 by default")
assert(random({ size = "1000" }) == 500, "and at most 500")
assert(#by_genre({ genre = "Rock" }) == 10, "getSongsByGenre gives 10 by default")
local most = by_genre({ genre = "Rock", count = "1000" })
assert(#most == 500 and most[500].title == "T500", `at most 500, got {#most}`)
local rest = by_genre({ genre = "Rock", count = "500", offset = "500" })
assert(#rest == 1 and rest[1].title == "T501", "the rest")
