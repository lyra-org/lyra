local fixture = ...
local mix = require("@lyra/mix")

local ids = fixture.ids
local SeedType = mix.SeedType

local function result(...: string): mix.MixResult
	local tracks = {}
	for _, id in { ... } do
		table.insert(tracks, { track_id = id })
	end
	return { tracks = tracks }
end

local function titles(tracks: { mix.Track }?): string
	assert(tracks, "expected a mix")
	local names = {}
	for _, track in tracks do
		table.insert(names, track.track_title)
	end
	return table.concat(names, ",")
end

local function fails(call: any, request: any, message: string)
	local ok, err = pcall(call, request)
	assert(not ok, `expected an error naming "{message}"`)
	assert(string.find(tostring(err), message, 1, true), `error {err}`)
end

-- What the last call to each handler saw.
local seen: { [string]: mix.MixBlendContext } = {}

-- "wide" blends up to three track seeds, with excluded seeds and a query. It has nothing for
-- S1 alone, and answers with a hidden track among visible ones.
local wide = mix.Mixer.new("wide", { display_name = "Wide" })
wide:declare_option({ name = "boost", label = "Boost", type = "boolean" })
wide:blend({
	seeds = { SeedType.Track },
	max_seeds = 3,
	exclude = true,
	query = true,
}, function(ctx: mix.MixBlendContext): mix.MixResult
	seen.wide = ctx
	if #ctx.seeds == 1 and ctx.seeds[1].id == ids.s1 then
		return result()
	end
	return result(ids.s5, ids.h1, ids.s4, ids.s3)
end)

wide:declare_option({
	name = "space",
	label = "Space",
	type = "choice",
	options = { { value = "audio", label = "Audio" }, { value = "lyrics", label = "Lyrics" } },
	default = "audio",
})

-- "narrow" blends a single track or playlist seed.
local narrow = mix.Mixer.new("narrow")
narrow:declare_option({ name = "boost", label = "Boost", type = "boolean" })
narrow:blend({ seeds = { SeedType.Track, SeedType.Playlist } }, function(ctx: mix.MixBlendContext): mix.MixResult
	seen.narrow = ctx
	return result(ids.s2)
end)

-- "route" walks from the start to the end through more steps than asked for.
local route = mix.Mixer.new("route")
route:path({ seeds = { SeedType.Track } }, function(ctx: mix.MixPathContext): mix.MixResult
	seen.route = ctx
	return result(ctx.seeds[1].id, ids.h1, ids.s3, ids.s4, ids.s5, ctx.seeds[2].id)
end)

-- Handlers get the seeds with their types and whether they are excluded, the query, the limit and
-- their own options.
local blended = mix.blend({
	seeds = { ids.s2, { id = ids.s3, exclude = true } },
	q = "calm",
	limit = 2,
	options = { ["wide.boost"] = true, ["narrow.boost"] = false, boost = false, ["wide.undeclared"] = 1 },
})
assert(titles(blended) == "S5,S4", `hidden tracks drop before the limit: {titles(blended)}`)
local ctx = seen.wide
assert(#ctx.seeds == 2, "both seeds")
assert(ctx.seeds[1].id == ids.s2 and ctx.seeds[1].type == SeedType.Track and not ctx.seeds[1].exclude)
assert(ctx.seeds[2].id == ids.s3 and ctx.seeds[2].exclude)
assert(ctx.q == "calm" and ctx.limit == 2, "query and limit")
assert(ctx.options and ctx.options.boost == true, "the mixer's own option")
local option_names = {}
for name in ctx.options :: { [string]: any } do
	table.insert(option_names, name)
end
assert(#option_names == 1, `only declared options, got {table.concat(option_names, ",")}`)

-- An option of another type than its mixer declares fails.
fails(
	mix.blend,
	{ seeds = { ids.s2 }, options = { ["narrow.boost"] = "yes" } },
	"option 'narrow.boost' must be a boolean"
)

-- A choice takes only its listed values.
mix.blend({ seeds = { ids.s2, ids.s3 }, options = { ["wide.space"] = "lyrics" } })
assert(seen.wide.options and seen.wide.options.space == "lyrics", "a listed choice")
fails(
	mix.blend,
	{ seeds = { ids.s2, ids.s3 }, options = { ["wide.space"] = "video" } },
	"option 'wide.space' must be one of 'audio', 'lyrics'"
)
fails(
	mix.blend,
	{ seeds = { ids.s2, ids.s3 }, options = { ["wide.space"] = 1 } },
	"option 'wide.space' must be a string"
)

-- A mixer that comes up empty passes the mix to the next one that takes it.
seen.narrow = nil
assert(titles(mix.blend({ seeds = { ids.s1 } })) == "S2", "falls through to narrow")
assert(seen.narrow, "narrow was asked")

-- A pinned mixer serves alone, with no fallback.
assert(titles(mix.blend({ seeds = { ids.s1 }, mixer = "wide" })) == "", "pinned wide comes up empty")
assert(titles(mix.blend({ seeds = { ids.s2 }, mixer = "narrow" })) == "S2", "pinned narrow")
fails(mix.blend, { seeds = { ids.s2, ids.s3 }, mixer = "narrow" }, "mixer 'narrow' can't serve this mix: 2 seeds")
fails(mix.path, { seeds = { ids.s1, ids.s2 }, mixer = "wide" }, "mixer 'wide' can't serve this mix: path")

-- Requests no handler's spec covers skip it: a genre seed goes to the built-in mixer.
seen.wide, seen.narrow = nil, nil
assert(titles(mix.blend({ seeds = { ids.rock } }, { limit = 10 })) ~= "", "built-in genre blend")
assert(seen.wide == nil and seen.narrow == nil, "no mixer takes a genre seed")
fails(mix.blend, { seeds = { ids.s1, ids.s2, ids.s3, ids.s4 } }, "no mixer can serve this mix: 4 seeds")
fails(mix.blend, { seeds = { ids.mix, ids.s2 } }, "no single mixer takes playlist seeds, track seeds, 2 seeds")
fails(mix.blend, { seeds = { ids.rock }, q = "loud" }, "no single mixer takes genre seeds, query")

-- History becomes track seeds after the explicit ones, filling what the mixer takes beyond them.
assert(titles(mix.blend({ seeds = { ids.s2 }, from_history = true })) ~= "", "history blend")
local history = {}
for _, seed in seen.wide.seeds do
	assert(seed.type == SeedType.Track and not seed.exclude, "history seeds are included tracks")
	table.insert(history, seed.id)
end
assert(#history == 3 and history[1] == ids.s2, "explicit seed first")
assert(table.find(history, ids.s1) and table.find(history, ids.s3), "then the listens")
assert(titles(mix.blend({ from_history = true, mixer = "narrow" })) == "S2", "history blend on narrow")
assert(#seen.narrow.seeds == 1, "narrow takes one recent listen")
local most_recent = seen.narrow.seeds[1].id
assert(most_recent == ids.s1 or most_recent == ids.s3, "a recent listen")

-- A path keeps the mixer's order and length, leaving out only what the listener can't see.
local path = mix.path({ seeds = { ids.s1, ids.s2 }, limit = 2 })
assert(titles(path) == "S1,S3,S4,S5,S2", `path: {titles(path)}`)
assert(seen.route.limit == 2, "the limit is a step hint")
assert(mix.path({ seeds = { ids.s1, ids.h1 } }) == nil, "a hidden end is not found")
fails(mix.path, { seeds = { ids.s1, ids.rock } }, "no mixer can serve this mix: genre seeds")

-- Specs name what a handler takes, and nothing else.
local function rejects_spec(register: (any) -> (), message: string)
	local ok, err = pcall(register)
	assert(not ok, `expected a spec error naming "{message}"`)
	assert(string.find(tostring(err), message, 1, true), `error {err}`)
end
local function noop(): mix.MixResult
	return result()
end
local spare = mix.Mixer.new("spare")
rejects_spec(function()
	spare:blend({}, noop)
end, "spec needs seeds or query")
rejects_spec(function()
	spare:blend({ seeds = { "track" } } :: any, noop)
end, "SeedType")
rejects_spec(function()
	spare:blend({ seeds = { SeedType.Track }, max_seed = 2 } :: any, noop)
end, "unknown field 'max_seed'")
rejects_spec(function()
	spare:blend({ seeds = { SeedType.Track }, max_seeds = 0 }, noop)
end, "max_seeds must be an integer >= 1")
rejects_spec(function()
	spare:blend({ seeds = { SeedType.Track }, timeout_ms = 15001 }, noop)
end, "timeout_ms must be <= 15000")
rejects_spec(function()
	spare:path({ seeds = { SeedType.Track }, max_seeds = 2 } :: any, noop)
end, "unknown field 'max_seeds'")
rejects_spec(function()
	spare:path({ query = true } :: any, noop)
end, "spec.seeds must name a SeedType")
spare:blend({ query = true, timeout_ms = 15000 }, noop)

-- A `.` separates a mixer's id from its option names, so neither may hold one.
rejects_spec(function()
	mix.Mixer.new("dotted.id")
end, "must be non-empty and contain no '.'")
rejects_spec(function()
	spare:declare_option({ name = "a.b", label = "A", type = "boolean" })
end, "must be non-empty and contain no '.'")
