local fixture = ...
local audiomuse = require("./_audiomuse")
local mix = require("@lyra/mix")

local ids = fixture.ids
local titles, take, param, item = audiomuse.titles, audiomuse.take, audiomuse.param, audiomuse.item

audiomuse.register(false)

-- A single track seed mixes from its similar tracks, leaving out the seed.
audiomuse.routes = { { pattern = "/api/similar_tracks", body = audiomuse.rows(ids.t1, ids.t3, ids.t2) } }
assert(titles(mix.blend({ seeds = { ids.t1 }, mixer = "audiomuse", limit = 2 })) == "T3,T2", "similar tracks")
local request = take()[1]
assert(request.method == "GET", "similar tracks are a GET")
assert(param(request.url, "item_id") == item(ids.t1), "the seed's item id")
assert(param(request.url, "n") == "3", "one more than the limit, for the seed")
for _, name in { "eliminate_duplicates", "mood_similarity", "radius_similarity", "server" } do
	assert(param(request.url, name) == nil, `{name} is left to AudioMuse unless set`)
end

-- Options the request sets are passed on.
mix.blend({
	seeds = { ids.t1 },
	mixer = "audiomuse",
	options = {
		["audiomuse.eliminate_duplicates"] = false,
		["audiomuse.mood_similarity"] = true,
		["audiomuse.radius_similarity"] = false,
	},
})
request = take()[1]
assert(param(request.url, "eliminate_duplicates") == "false", "eliminate_duplicates")
assert(param(request.url, "mood_similarity") == "true", "mood_similarity")
assert(param(request.url, "radius_similarity") == "false", "radius_similarity")

-- Seeds AudioMuse hasn't analysed give an empty mix; so does a failing AudioMuse.
audiomuse.routes = { { pattern = "/api/similar_tracks", status = 404, body = { error = "not found" } } }
assert(titles(mix.blend({ seeds = { ids.t1 }, mixer = "audiomuse" })) == "", "404 is an empty mix")
audiomuse.routes = { { pattern = "/api/similar_tracks", status = 500, body = { error = "boom" } } }
assert(titles(mix.blend({ seeds = { ids.t1 }, mixer = "audiomuse" })) == "", "500 is an empty mix")
take()

-- Several seeds blend through alchemy: excluded seeds subtract, releases add their first tracks,
-- and the seeds themselves are left out of the mix.
audiomuse.routes = { { pattern = "/api/alchemy", body = { results = audiomuse.rows(ids.t4, ids.t2, ids.t5) } } }
local blended = mix.blend({
	seeds = { ids.t2, { id = ids.t3, exclude = true } },
	mixer = "audiomuse",
	limit = 10,
	options = { ["audiomuse.temperature"] = 0 },
})
assert(titles(blended) == "T4,T5", `alchemy: {titles(blended)}`)
request = take()[1]
assert(request.method == "POST" and string.find(request.url, "/api/alchemy", 1, true), "alchemy is a POST")
local body = request.body
assert(body.n == 10, "the limit")
assert(body.temperature == 0, "the temperature")
assert(body.subtract_distance == nil and body.server == nil, "nothing else unless set")
assert(#body.items == 2, "one item per seed")
assert(body.items[1].id == item(ids.t2) and body.items[1].op == "ADD" and body.items[1].type == "song")
assert(body.items[2].id == item(ids.t3) and body.items[2].op == "SUBTRACT")

mix.blend({ seeds = { ids.solo }, mixer = "audiomuse" })
body = take()[1].body
assert(#body.items == 5, "a release adds its first five tracks")
local added = {}
for _, entry in body.items do
	added[entry.id] = entry.op
end
for _, key in { "t1", "t2", "t3", "t4", "t5" } do
	assert(added[item(ids[key])] == "ADD", `{key} adds`)
end
assert(body.temperature == nil, "temperature is left to AudioMuse unless set")

-- Without CLAP, blends don't take a query.
audiomuse.fails(mix.blend, { q = "calm piano", mixer = "audiomuse" }, "mixer 'audiomuse' can't serve this mix: query")

-- A configured AudioMuse server is named on every request: in a GET's query, in a POST's body.
audiomuse.configure("Lyra Home")
audiomuse.routes = {
	{ pattern = "/api/similar_tracks", body = audiomuse.rows(ids.t2) },
	{ pattern = "/api/alchemy", body = { results = audiomuse.rows(ids.t4) } },
}
mix.blend({ seeds = { ids.t1 }, mixer = "audiomuse" })
mix.blend({ seeds = { ids.t1, ids.t2 }, mixer = "audiomuse" })
local requests = take()
assert(param(requests[1].url, "server") == "Lyra Home", "the server in the query")
assert(requests[2].body.server == "Lyra Home", "the server in the body")
