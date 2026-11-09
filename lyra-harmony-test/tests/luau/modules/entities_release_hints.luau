local fixture = ...

local entities = require("@lyra/entities")

local seen = fixture.ids.seen

-- The hidden library's path sorts first, but a listener's hints never read it.
local function check(release: any)
	for _, hints in { release.lookup_hints, release.includes.tracks[1].lookup_hints } do
		assert(hints.artist_name ~= "Private Artist", "an artist from a hidden path")
		assert(hints.release_title ~= "Private Album", "an album from a hidden path")
		assert(hints.year ~= 2016, "a year from a hidden path")
	end
end

check(entities.query_release({ id = seen, include = "tracks" }))
check(entities.query_many({ ids = { seen }, include = "tracks" })[seen])
