local fixture = ...
local audiomuse = require("./_audiomuse")
local mix = require("@lyra/mix")

local ids = fixture.ids
local titles, scores, take, item = audiomuse.titles, audiomuse.scores, audiomuse.take, audiomuse.item

audiomuse.register(true)

-- A query alone mixes from AudioMuse's CLAP text search.
audiomuse.routes = {
	{
		pattern = "/api/clap/search",
		body = { query = "calm piano", results = audiomuse.rows(ids.t3, ids.t1), count = 2 },
	},
	{ pattern = "/api/alchemy", body = { results = audiomuse.rows(ids.t1, ids.t4, ids.t5) } },
}
assert(titles(mix.blend({ q = "calm piano", mixer = "audiomuse", limit = 7 })) == "T3,T1", "text search")
local request = take()[1]
assert(request.method == "POST", "a search is a POST")
assert(request.body.query == "calm piano" and request.body.limit == 7, "the query and the limit")

-- Text search matches score their similarity to the query, kept within 0 to 1.
audiomuse.routes[1].body = {
	results = {
		{ item_id = item(ids.t3), similarity = 1.25 },
		{ item_id = item(ids.t1), similarity = 0.625 },
		{ item_id = item(ids.t2), similarity = -0.125 },
		{ item_id = item(ids.t4) },
	},
}
local matched = mix.blend({ q = "calm piano", mixer = "audiomuse" })
assert(scores(matched) == "1,0.625,0,-", `text search scores: {scores(matched)}`)
take()
audiomuse.routes[1].body = { query = "calm piano", results = audiomuse.rows(ids.t3, ids.t1), count = 2 }

-- A query with seeds adds its best matches to the alchemy blend, as many as the seeds add.
local blended =
	mix.blend({ seeds = { ids.t2, { id = ids.t4, exclude = true } }, q = "calm piano", mixer = "audiomuse" })
assert(titles(blended) == "T1,T5", `blend: {titles(blended)}`)
local requests = take()
assert(requests[1].body.query == "calm piano" and requests[1].body.limit == 3, "room for one match past the seeds")
local items = requests[2].body.items
assert(#items == 3, "the seeds and the match")
assert(items[1].id == item(ids.t2) and items[1].op == "ADD")
assert(items[2].id == item(ids.t4) and items[2].op == "SUBTRACT")
assert(items[3].id == item(ids.t3) and items[3].op == "ADD", "the best match adds")

-- Matches that are seeds are skipped, so an excluded seed never adds back.
audiomuse.routes[1].body = { results = audiomuse.rows(ids.t4, ids.t2, ids.t3) }
mix.blend({ seeds = { ids.t2, { id = ids.t4, exclude = true } }, q = "calm piano", mixer = "audiomuse" })
items = take()[2].body.items
assert(#items == 3 and items[3].id == item(ids.t3) and items[3].op == "ADD", "the first match that isn't a seed")
audiomuse.routes[1].body = { query = "calm piano", results = audiomuse.rows(ids.t3, ids.t1), count = 2 }

-- A single track seed with a query blends too, rather than taking only similar tracks.
mix.blend({ seeds = { ids.t2 }, q = "calm piano", mixer = "audiomuse" })
requests = take()
assert(#requests == 2 and string.find(requests[2].url, "/api/alchemy", 1, true), "an alchemy blend")

-- Text search that finds nothing, or fails, gives an empty mix.
audiomuse.routes = { { pattern = "/api/clap/search", status = 503, body = { error = "CLAP cache not loaded" } } }
assert(titles(mix.blend({ q = "calm piano", mixer = "audiomuse" })) == "", "no text search, no mix")
take()
assert(titles(mix.blend({ seeds = { ids.t2 }, q = "calm piano", mixer = "audiomuse" })) == "", "nor with seeds")
assert(#take() == 1, "no blend without the query")
