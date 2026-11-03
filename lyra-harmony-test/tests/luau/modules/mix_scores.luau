local fixture = ...
local mix = require("@lyra/mix")
local releases = require("@lyra/releases")

local ids = fixture.ids
releases.replace_chains(ids.book, { { ids.part1, ids.part2 } })

-- The mix's titles and scores, in order, with `-` for an unscored track.
local function scored(tracks: { mix.MixedTrack }?): string
	assert(tracks, "expected a mix")
	local entries = {}
	for _, entry in tracks do
		table.insert(entries, `{entry.track.track_title}={if entry.score then entry.score else "-"}`)
	end
	return table.concat(entries, ",")
end

local function result(entries: { { any } }): mix.MixResult
	local tracks: { mix.MixResultTrack } = {}
	for _, entry in entries do
		table.insert(tracks, { track_id = entry[1], score = entry[2] })
	end
	return { tracks = tracks }
end

-- "scorer" scores every track but Song, the chain split and a hidden track among them.
local scorer = mix.Mixer.new("scorer")
scorer:blend({ seeds = { mix.SeedType.Track } }, function(): mix.MixResult
	return result({ { ids.start, 0.5 }, { ids.part2, 0.875 }, { ids.h1, 1 }, { ids.song }, { ids.part1, 0.25 } })
end)
scorer:path({ seeds = { mix.SeedType.Track } }, function(ctx: mix.MixPathContext): mix.MixResult
	return result({
		{ ctx.seeds[1].id, 1 },
		{ ids.h1, 0.5 },
		{ ids.part2, 0.75 },
		{ ids.part1, 0.125 },
		{ ctx.seeds[2].id, 0 },
	})
end)

-- Scores stay with their tracks as the chain regroups and hidden tracks drop.
local blended = scored(mix.blend({ seeds = { ids.song }, mixer = "scorer" }))
assert(blended == "Start=0.5,Part I=0.25,Part II=0.875,Song=-", `blend: {blended}`)

-- A pinned seed the mixer didn't return has no score; one it did keeps its own.
local pinned = scored(mix.blend({ seeds = { ids.finish }, mixer = "scorer", pin_seed = true }))
assert(pinned == "End=-,Start=0.5,Part I=0.25,Part II=0.875,Song=-", `pinned: {pinned}`)
pinned = scored(mix.blend({ seeds = { ids.part1 }, mixer = "scorer", pin_seed = true, limit = 2 }))
assert(pinned == "Part I=0.25,Part II=0.875", `pinned part: {pinned}`)

-- A path keeps the mixer's order, and its scores.
local path = scored(mix.path({ seeds = { ids.start, ids.finish }, mixer = "scorer" }))
assert(path == "Start=1,Part II=0.75,Part I=0.125,End=0", `path: {path}`)

-- A track named twice keeps the score of its first entry, even an absent one.
local duplicates = mix.Mixer.new("duplicates")
duplicates:blend({ seeds = { mix.SeedType.Track } }, function(): mix.MixResult
	return result({ { ids.start }, { ids.song, 0.5 }, { ids.start, 0.75 }, { ids.song, 0.25 } })
end)
local repeated = scored(mix.blend({ seeds = { ids.finish }, mixer = "duplicates" }))
assert(repeated == "Start=-,Song=0.5,Start=-,Song=0.5", `duplicates: {repeated}`)
repeated = scored(mix.blend({ seeds = { ids.finish }, mixer = "duplicates", limit = 1 }))
assert(repeated == "Start=-", `truncated duplicates: {repeated}`)

-- The built-in mixer gives no scores.
for _, entry in assert(mix.blend({ seeds = { ids.book } })) do
	assert(entry.score == nil, "the built-in mixer's tracks are unscored")
end

-- A score that isn't a number from 0 to 1 fails the handler, so its mix comes up empty.
local bad_score: any
local broken = mix.Mixer.new("broken")
broken:blend({ seeds = { mix.SeedType.Track } }, function(): mix.MixResult
	return result({ { ids.start, 0.5 }, { ids.song, bad_score } })
end)
for _, score in { 1.5, -0.25, 0 / 0, "0.5" :: any } do
	bad_score = score
	assert(scored(mix.blend({ seeds = { ids.part1 }, mixer = "broken" })) == "", `score {score} fails the handler`)
end
