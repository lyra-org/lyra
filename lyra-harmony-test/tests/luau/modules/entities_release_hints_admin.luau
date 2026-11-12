local fixture = ...

local entities = require("@lyra/entities")

local seen = fixture.ids.seen

-- Seeing every library, hints still read the first path of all the track's files.
local function check(release: any)
	for _, hints in { release.lookup_hints, release.includes.tracks[1].lookup_hints } do
		assert(hints.artist_name == "Private Artist", `artist {hints.artist_name}`)
		assert(hints.release_title == "Private Album", `album {hints.release_title}`)
		assert(hints.year == 2016, `year {hints.year}`)
	end
end

check(entities.get({ seen }, { include = "tracks" })[seen])
