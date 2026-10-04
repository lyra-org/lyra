local fixture = ...
local playlists = require("@lyra/playlists")

-- The test runs as the fixture's `run_as` user, which owns what it creates.
local mine = playlists.create({ name = "Mine" })
assert(playlists.get_owner(mine) == fixture.users.owner)

-- Seeded entities keep their fixture keys, and playlists list their entries in order.
local shared = fixture.ids.shared
assert(playlists.get_owner(shared) == fixture.users.other)
local entries = playlists.get_tracks(shared)
assert(#entries == 2 and #fixture.entries.shared == 2)
for position, entry in entries do
	assert(entry.track_id == fixture.ids.song)
	assert(entry.entry_id == fixture.entries.shared[position])
end
