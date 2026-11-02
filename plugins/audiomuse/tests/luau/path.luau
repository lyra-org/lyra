local fixture = ...
local audiomuse = require("./_audiomuse")
local mix = require("@lyra/mix")

local ids = fixture.ids
local titles, take, param, item = audiomuse.titles, audiomuse.take, audiomuse.param, audiomuse.item

audiomuse.register(false)

-- A path runs from the start track to the end track through AudioMuse's song path, in its order.
audiomuse.routes = {
	{
		pattern = "/api/find_path",
		body = { path = audiomuse.rows(ids.t1, ids.t4, ids.t3, ids.t5), total_distance = 1.5 },
	},
}
local path = mix.path({ seeds = { ids.t1, ids.t5 }, mixer = "audiomuse", limit = 4 })
assert(titles(path) == "T1,T4,T3,T5", `path: {titles(path)}`)
local request = take()[1]
assert(request.method == "GET", "a path is a GET")
assert(param(request.url, "start_song_id") == item(ids.t1), "the start")
assert(param(request.url, "end_song_id") == item(ids.t5), "the end")
assert(param(request.url, "max_steps") == "4", "the limit is the step count")
assert(param(request.url, "path_space") == nil, "the path space is left to AudioMuse unless set")
assert(param(request.url, "path_fix_size") == nil, "the size is left to AudioMuse unless set")

-- Options choose the space and fix the size.
mix.path({
	seeds = { ids.t1, ids.t5 },
	mixer = "audiomuse",
	options = { ["audiomuse.path_space"] = "lyrics", ["audiomuse.path_fix_size"] = true },
})
request = take()[1]
assert(param(request.url, "path_space") == "lyrics", "lyrics path")
assert(param(request.url, "path_fix_size") == "true", "fixed size")
assert(param(request.url, "max_steps") == nil, "without a limit, AudioMuse's default length")
audiomuse.fails(
	mix.path,
	{ seeds = { ids.t1, ids.t5 }, mixer = "audiomuse", options = { ["audiomuse.path_space"] = "video" } },
	"option 'audiomuse.path_space' must be one of 'audio', 'lyrics'"
)

-- Seeds outside the index asked for, or not analysed, give an empty path.
audiomuse.routes = {
	{
		pattern = "/api/find_path",
		status = 404,
		body = { error = "One or both selected songs are not in the Lyrics index" },
	},
}
assert(titles(mix.path({ seeds = { ids.t1, ids.t5 }, mixer = "audiomuse" })) == "", "404 is an empty path")

-- Paths run between tracks only.
audiomuse.fails(mix.path, { seeds = { ids.solo, ids.t5 }, mixer = "audiomuse" }, "can't serve this mix")
audiomuse.fails(mix.path, { seeds = { ids.amy, ids.t5 }, mixer = "audiomuse" }, "can't serve this mix")
