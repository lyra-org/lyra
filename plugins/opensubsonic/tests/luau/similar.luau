local fixture, harness = ...
local call = require("./_call")(harness)
local mix = require("@lyra/mix")

local ids = fixture.ids
local key = fixture.api_keys.listener_key

local function similar(method: string, params: { [string]: string }): { string }
	local body = call.ok(method, key, params)
	local container = if method == "getSimilarSongs" then body.similarSongs else body.similarSongs2
	assert(type(container) == "table", `{method} answers its list`)
	return call.ids(container.song)
end

local function sorted(list: { string }): { string }
	local copy = table.clone(list)
	table.sort(copy)
	return copy
end

-- Without a mixer plugin, the built-in mixer blends songs of the seed's genres the caller sees.
local visible = sorted({ ids.s1, ids.s2, ids.o1 })
call.same(sorted(similar("getSimilarSongs", { id = ids.solo })), visible, "like an album")
call.same(sorted(similar("getSimilarSongs", { id = ids.amy })), visible, "like an artist")
call.same(sorted(similar("getSimilarSongs2", { id = ids.amy })), visible, "like an artist, by ID3")
-- A song leaves itself out.
call.same(sorted(similar("getSimilarSongs", { id = ids.s1 })), sorted({ ids.s2, ids.o1 }), "like a song")
assert(#similar("getSimilarSongs", { id = ids.solo, count = "2" }) == 2, "a count")
call.same(similar("getSimilarSongs", { id = ids.solo, count = "0" }), {}, "no songs")

local song = call.ok("getSimilarSongs", key, { id = ids.s1 }).similarSongs.song[1]
assert(song.isDir == false and song.type == "music" and song.album ~= nil, "each song is a Child")

-- Anything else, or a seed the caller can't see, isn't found.
local function missing(method: string, id: string, why: string)
	assert(call.error_code(method, key, { id = id }) == 70, why)
end
missing("getSimilarSongs", ids.h1, "a hidden song")
missing("getSimilarSongs", ids.secret, "a hidden album")
missing("getSimilarSongs", ids.hal, "an artist of hidden albums only")
missing("getSimilarSongs", "missing", "an unknown id")
missing("getSimilarSongs", ids.rock, "a genre")
missing("getSimilarSongs", ids.mix, "a playlist")
missing("getSimilarSongs2", ids.solo, "an album, by ID3")
missing("getSimilarSongs2", ids.s1, "a song, by ID3")
assert(call.error_code("getSimilarSongs", key, {}) == 10, "an id is required")
assert(call.error_code("getSimilarSongs2", key, {}) == 10, "an id is required, by ID3")
assert(call.error_code("getSimilarSongs", key, { id = ids.s1, count = "many" }) == 0, "a count is an integer")
local empty_xml = call.raw("getSimilarSongs2", { apiKey = key, id = ids.amy, count = "0" }).body
assert(string.find(empty_xml, "<similarSongs2/>", 1, true), empty_xml)

local mixer = mix.Mixer.new("similar")

-- A mixer plugin that blends songs serves them in its order, asked for 50 by default and one more
-- for a song seed, which it may return among its own.
local seen: any = nil
mixer:blend({ seeds = { mix.SeedType.Track, mix.SeedType.Artist } }, function(ctx: mix.MixBlendContext): mix.MixResult
	seen = ctx
	return {
		tracks = { { track_id = ids.o1 }, { track_id = ids.s1 }, { track_id = ids.h1 }, { track_id = ids.s2 } },
	}
end)
call.same(similar("getSimilarSongs", { id = ids.s1 }), { ids.o1, ids.s2 }, "the mixer's order, without the seed")
assert(seen.limit == 51 and seen.seeds[1].id == ids.s1, `asked for {seen.limit}`)
call.same(similar("getSimilarSongs", { id = ids.s1, count = "1" }), { ids.o1 }, "one song")
assert(seen.limit == 2, "one more than the count")
call.same(similar("getSimilarSongs", { id = ids.s1, count = "100000" }), { ids.o1, ids.s2 }, "a large count")
assert(seen.limit == mix.MAX_TRACKS, "capped to what a mix returns")

-- An artist seed asks for the count; its songs stay.
call.same(similar("getSimilarSongs2", { id = ids.amy }), { ids.o1, ids.s1, ids.s2 }, "like an artist, from the mixer")
assert(seen.limit == 50 and seen.seeds[1].id == ids.amy, `an artist asked for {seen.limit}`)
