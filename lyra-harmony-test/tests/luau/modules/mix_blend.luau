local fixture = ...
local mix = require("@lyra/mix")

local ids = fixture.ids

local function titles(tracks: { mix.Track }?): string
	assert(tracks, "expected a mix")
	local names = {}
	for _, track in tracks do
		table.insert(names, track.track_title)
	end
	table.sort(names)
	return table.concat(names, ",")
end

local function fails(request: any, message: string)
	local ok, err = pcall(mix.blend, request)
	assert(not ok, `expected an error naming "{message}"`)
	assert(string.find(tostring(err), message, 1, true), `error {err}`)
end

-- The built-in mixer blends from one seed of any type, keeping to what the listener can see.
for _, seed in { ids.s1, ids.solo, ids.amy, ids.rock, ids.mix } do
	local blended = titles(mix.blend({ seeds = { seed } }))
	assert(blended == "S1,S2,S3", `blend of {seed}: {blended}`)
end
assert(titles(mix.blend({ seeds = { { id = ids.rock, exclude = false } } })) == "S1,S2,S3", "a seed entry")

-- Or from the listener's history alone.
assert(titles(mix.blend({ from_history = true })) == "S1,S2,S3", "history blend")

-- A pinned track seed leads.
local pinned = mix.blend({ seeds = { ids.s2 }, pin_seed = true, limit = 2 })
assert(pinned and #pinned == 2 and pinned[1].db_id == ids.s2, "pinned seed leads")
fails({ seeds = { ids.solo }, pin_seed = true }, "pin_seed needs exactly one seed, a track")

-- A seed the listener can't see, or one without an id, is not found, even one to push away from.
assert(mix.blend({ seeds = { ids.h1 } }) == nil, "hidden seed")
assert(mix.blend({ seeds = { ids.s1, { id = ids.h1, exclude = true } } }) == nil, "hidden excluded seed")
assert(mix.blend({ seeds = { 999999999 } }) == nil, "missing seed")

-- What no mixer serves fails, naming what is missing.
fails({}, "a mix needs a seed that isn't excluded, from_history, or q")
fails({ seeds = { { id = ids.s1, exclude = true } } }, "a mix needs a seed that isn't excluded")
fails({ q = "rain" }, "no mixer can serve this mix: query")
fails({ seeds = { ids.s1, ids.s2 } }, "no mixer can serve this mix: 2 seeds")
-- History fills only what the mixer takes beyond the explicit seeds; the built-in takes one.
assert(titles(mix.blend({ seeds = { ids.s2 }, from_history = true })) == "S1,S2,S3", "seed and history")
fails({ seeds = { ids.s2, { id = ids.s1, exclude = true } } }, "no mixer can serve this mix: 2 seeds, excluded seeds")
fails({ seeds = { ids.s1 }, mixer = "ghost" }, "unknown mixer 'ghost'")
local ok, err = pcall(mix.path, { seeds = { ids.s1, ids.s2 } })
assert(not ok and string.find(tostring(err), "no mixer can serve this mix: path", 1, true), `path: {err}`)

-- Malformed requests fail as well.
fails({ seeds = { ids.s1, ids.s1 } }, "is given twice")
fails({ seeds = { { id = ids.s1, weight = 2 } } }, "request:")
fails({ seeds = { { id = ids.s1, exclude = true }, ids.s2 }, pin_seed = true }, "pin_seed needs exactly one seed")
fails({ seeds = { ids.s1 }, kind = "track" }, "unknown field")
fails({ seeds = { ids.s1 }, limit = 2001 }, "limit must be <= 2000")
fails({ seeds = { ids.s1 }, options = { ["a.b"] = { 1 } } }, "option 'a.b' must be a boolean, number, or string")
fails({ seeds = { ids.s1 }, shape = "path" }, "unknown field `shape`")
fails({ seeds = { ids.s1 + 0.5 } }, "mix.blend: request:")
assert(titles(mix.blend({ seeds = { ids.s1 * 1.0 } })) == "S1,S2,S3", "a whole number is an id")
ok, err = pcall(mix.path, { seeds = { ids.s1 } })
assert(not ok and string.find(tostring(err), "exactly two seeds", 1, true), `one-seed path: {err}`)
ok, err = pcall(mix.path, { seeds = { ids.s1, { id = ids.s2, exclude = true } } })
assert(not ok and string.find(tostring(err), "a path can't exclude seeds", 1, true), `excluded path end: {err}`)
