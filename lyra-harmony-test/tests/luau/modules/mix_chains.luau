local fixture = ...
local mix = require("@lyra/mix")
local releases = require("@lyra/releases")

local ids = fixture.ids
releases.replace_chains(ids.book, { { ids.part1, ids.part2 } })

local function titles(tracks: { mix.Track }?): string
	assert(tracks, "expected a mix")
	local names = {}
	for _, track in tracks do
		table.insert(names, track.track_title)
	end
	return table.concat(names, ",")
end

-- The mixer answers with the chain split and out of order.
local mixer = mix.Mixer.new("splitter")
local function split(ctx: mix.MixBlendContext): mix.MixResult
	local tracks = {}
	for _, id in { ctx.seeds[1].id, ids.part2, ids.song, ids.part1 } do
		table.insert(tracks, { track_id = id })
	end
	if ctx.seeds[2] then
		table.insert(tracks, { track_id = ctx.seeds[2].id })
	end
	return { tracks = tracks }
end
mixer:blend({ seeds = { mix.SeedType.Track } }, split)
mixer:path({ seeds = { mix.SeedType.Track } }, split)

-- A blend plays the chain together and in order, where its first track sat.
local blended = titles(mix.blend({ seeds = { ids.start } }))
assert(blended == "Start,Part I,Part II,Song", `blend: {blended}`)

-- A pinned blend leads with its seed and its chain's later tracks.
local pinned = titles(mix.blend({ seeds = { ids.part1 }, pin_seed = true }))
assert(pinned == "Part I,Part II,Song", `pinned: {pinned}`)

-- A path keeps the mixer's order.
local path = titles(mix.path({ seeds = { ids.start, ids.finish } }))
assert(path == "Start,Part II,Song,Part I,End", `path: {path}`)
