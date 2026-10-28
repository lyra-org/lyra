local fixture = ...
local releases = require("@lyra/releases")
local tracks = require("@lyra/tracks")

local ids = fixture.ids
local book = ids.book

local function describe(chains: { { number } }): string
	local parts = {}
	for _, chain in chains do
		local members = {}
		for _, track_id in chain do
			table.insert(members, tostring(track_id))
		end
		table.insert(parts, table.concat(members, ">"))
	end
	return table.concat(parts, " ")
end

local function track_ids(chains: { releases.ReleaseChain }): string
	local lists = {}
	for _, chain in chains do
		table.insert(lists, chain.track_ids)
	end
	return describe(lists)
end

-- Chains read back ordered by where their first track sits.
local stored = releases.replace_chains(book, {
	{ ids.worship, ids.idol },
	{ ids.awakening, ids.blessing },
})
local want = describe({ { ids.awakening, ids.blessing }, { ids.worship, ids.idol } })
assert(track_ids(stored) == want, track_ids(stored))
assert(track_ids(releases.chains({ book })[book]) == want)

-- Each chained track knows its chain and place; others are left out.
local places = tracks.chains({ ids.awakening, ids.idol, ids.hero })
assert(places[ids.awakening].id == stored[1].id and places[ids.awakening].position == 0)
assert(places[ids.idol].id == stored[2].id and places[ids.idol].position == 1)
assert(places[ids.hero] == nil)

-- A random sort keeps each chain together and in order.
for seed = 1, 8 do
	local page = tracks.query({ release_ids = { book }, sort = { { key = "random" } }, seed = seed })
	local order = {}
	for index, track in page.items do
		order[track.db_id] = index
	end
	assert(order[ids.blessing] == order[ids.awakening] + 1, `seed {seed}`)
	assert(order[ids.idol] == order[ids.worship] + 1, `seed {seed}`)
end

-- Invalid sets fail and keep what is stored.
for _, chains in
	{
		{ { ids.hero } },
		{ { ids.hero, ids.hero } },
		{ { ids.hero, ids.elsewhere } },
	}
do
	local ok = pcall(releases.replace_chains, book, chains)
	assert(not ok, "invalid chains are rejected")
end
assert(track_ids(releases.chains({ book })[book]) == want)

-- An empty set clears them.
assert(#releases.replace_chains(book, {}) == 0)
assert(#releases.chains({ book })[book] == 0)
assert(next(tracks.chains({ ids.awakening, ids.idol })) == nil)
