local fixture, harness = ...
local call = require("./_call")(harness)
local mix = require("@lyra/mix")
local serde = require("@harmony/serde")
local sonic = require("../../endpoints/sonic")

local ids = fixture.ids
local key = fixture.api_keys.listener_key

local function advertised(): boolean
	local body = call.raw("getOpenSubsonicExtensions", { f = "json" }, { bare = true })
	for _, extension in (serde.json.decode(body.body) :: any)["subsonic-response"].openSubsonicExtensions do
		if extension.name == "sonicSimilarity" then
			assert(#extension.versions == 1 and extension.versions[1] == 1, "version 1")
			return true
		end
	end
	return false
end

local function result(entries: { { any } }): mix.MixResult
	local tracks: { mix.MixResultTrack } = {}
	for _, entry in entries do
		table.insert(tracks, { track_id = entry[1], score = entry[2] })
	end
	return { tracks = tracks }
end

-- The ids and similarities of a `sonicMatch` list, as `id=similarity`.
local function shown(found: { any }): string
	local parts = {}
	for _, match in found do
		table.insert(parts, `{match.entry.id}={match.similarity}`)
	end
	return table.concat(parts, ",")
end

local function expected(entries: { { any } }): string
	local parts = {}
	for _, entry in entries do
		table.insert(parts, `{entry[1]}={entry[2]}`)
	end
	return table.concat(parts, ",")
end

local function unknown(method: string, params: { [string]: string })
	local query: { [string]: string } = { apiKey = key, f = "json" }
	for name, value in params do
		query[name] = value
	end
	local raw = call.raw(method, query)
	local body = (serde.json.decode(raw.body) :: any)["subsonic-response"]
	assert(raw.status == 404 and body.error.code == 0, `{method} is unknown`)
	assert(body.error.message == `Unknown method: {method}`, body.error.message)
end

-- What the last call to each handler saw.
local seen: { [string]: any } = {}

-- "blender" blends from songs but walks no paths.
local blender = mix.Mixer.new("blender")
blender:blend({ seeds = { mix.SeedType.Track } }, function(): mix.MixResult
	return result({})
end)

-- Without a mixer configured, the methods are unknown and the extension unlisted.
assert(not advertised(), "off without a mixer")
unknown("getSonicSimilarTracks", { id = ids.s1 })
unknown("findSonicPath", { startSongId = ids.s1, endSongId = ids.s2 })

-- A mixer that isn't listed, or doesn't take song paths, leaves them off.
sonic.configure("absent")
assert(not advertised(), "off with an unknown mixer")
sonic.configure("blender")
assert(not advertised(), "off with a mixer that walks no paths")
unknown("getSonicSimilarTracks", { id = ids.s1 })

-- "sonic" scores its blends and paths; it finds nothing like S5.
local scorer = mix.Mixer.new("sonic")
scorer:blend({ seeds = { mix.SeedType.Track } }, function(ctx: mix.MixBlendContext): mix.MixResult
	seen.blend = ctx
	if ctx.seeds[1].id == ids.s5 then
		return result({})
	end
	return result({ { ids.s2, 0.25 }, { ids.s3 }, { ids.h1, 0.9 }, { ids.s4, 0.75 }, { ids.s5 } })
end)
scorer:path({ seeds = { mix.SeedType.Track } }, function(ctx: mix.MixPathContext): mix.MixResult
	seen.path = ctx
	if ctx.seeds[1].id == ids.s5 then
		return result({})
	end
	return result({
		{ ctx.seeds[1].id, 1 },
		{ ids.h1, 0.75 },
		{ ids.s4 },
		{ ids.s3, 0.5 },
		{ ctx.seeds[2].id, 0 },
	})
end)

-- The configured mixer is named without surrounding space.
sonic.configure(" sonic ")
assert(advertised(), "on with a capable mixer")

-- Similar songs come most similar first, hidden ones dropped, and those without a score last as
-- the mixer gave them.
local similar = call.ok("getSonicSimilarTracks", key, { id = ids.s1 }).sonicMatch
local similar_shown = shown(similar)
assert(
	similar_shown == expected({ { ids.s4, 0.75 }, { ids.s2, 0.25 }, { ids.s3, -1 }, { ids.s5, -1 } }),
	`similar: {similar_shown}`
)
assert(seen.blend.limit == 10 and #seen.blend.seeds == 1 and seen.blend.seeds[1].id == ids.s1, "10 by default")
-- Each entry is the song's Child.
local entry = similar[1].entry
assert(entry.title == "S4" and entry.isDir == false and entry.type == "music" and entry.album == "Solo", "a Child")

-- The count is passed on, and capped to what a mix returns.
call.ok("getSonicSimilarTracks", key, { id = ids.s1, count = "3" })
assert(seen.blend.limit == 3, "a count")
call.ok("getSonicSimilarTracks", key, { id = ids.s1, count = "100000" })
assert(seen.blend.limit == mix.MAX_TRACKS, `a count is capped to {mix.MAX_TRACKS}`)
seen.blend = nil
assert(#call.ok("getSonicSimilarTracks", key, { id = ids.s1, count = "0" }).sonicMatch == 0, "no songs")
assert(seen.blend == nil, "an empty count asks no mixer")
assert(call.error_code("getSonicSimilarTracks", key, { id = ids.s1, count = "ten" }) == 0, "a count is an integer")

assert(call.error_code("getSonicSimilarTracks", key, {}) == 10, "an id is required")
assert(call.error_code("getSonicSimilarTracks", key, { id = ids.h1 }) == 70, "a hidden song")
assert(call.error_code("getSonicSimilarTracks", key, { id = "missing" }) == 70, "an unknown song")
assert(call.error_code("getSonicSimilarTracks", key, { id = ids.solo }) == 70, "an album isn't a song")

-- Nothing similar is an empty list.
local nothing = call.raw("getSonicSimilarTracks", { apiKey = key, f = "json", id = ids.s5 })
assert(string.find(nothing.body, '"sonicMatch":[]', 1, true), nothing.body)

-- A path keeps the mixer's order, from the start to the end, scored against the start.
local path = call.ok("findSonicPath", key, { startSongId = ids.s1, endSongId = ids.s2 }).sonicMatch
local path_shown = shown(path)
assert(path_shown == expected({ { ids.s1, 1 }, { ids.s4, -1 }, { ids.s3, 0.5 }, { ids.s2, 0 } }), `path: {path_shown}`)
assert(seen.path.limit == 25, "25 steps by default")
assert(seen.path.seeds[1].id == ids.s1 and seen.path.seeds[2].id == ids.s2, "the start and end")
call.ok("findSonicPath", key, { startSongId = ids.s1, endSongId = ids.s2, count = "100000" })
assert(seen.path.limit == mix.MAX_TRACKS, "a path's count is capped")

assert(call.error_code("findSonicPath", key, { startSongId = ids.s1 }) == 10, "an end is required")
assert(call.error_code("findSonicPath", key, { endSongId = ids.s2 }) == 10, "a start is required")
assert(call.error_code("findSonicPath", key, { startSongId = ids.s1, endSongId = ids.h1 }) == 70, "a hidden end")
assert(call.error_code("findSonicPath", key, { startSongId = "missing", endSongId = ids.s2 }) == 70, "an unknown start")

local no_path = call.raw("findSonicPath", { apiKey = key, f = "json", startSongId = ids.s5, endSongId = ids.s1 })
assert(string.find(no_path.body, '"sonicMatch":[]', 1, true), no_path.body)

-- In XML, each match carries its similarity and holds its song as `entry`.
local xml = call.raw("getSonicSimilarTracks", { apiKey = key, id = ids.s1 }).body
assert(string.find(xml, '<sonicMatch similarity="0.75"><entry ', 1, true), xml)
assert(string.find(xml, `id="{ids.s4}"`, 1, true), xml)
assert(string.find(xml, '<sonicMatch similarity="-1"><entry ', 1, true), xml)
local empty_xml = call.raw("getSonicSimilarTracks", { apiKey = key, id = ids.s5 }).body
assert(string.find(empty_xml, 'status="ok"', 1, true) and not string.find(empty_xml, "<sonicMatch", 1, true), empty_xml)
