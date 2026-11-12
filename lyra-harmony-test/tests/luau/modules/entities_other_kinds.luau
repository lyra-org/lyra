local fixture = ...
local entities = require("@lyra/entities")

local ids = fixture.ids

-- Visible nodes that have no projection read as absent, like ids naming nothing.
local others = { ids.rock, ids.mix, fixture.libraries.music }
for _, id in others do
	assert(entities.get_type(id) ~= nil, "the caller sees the node")
end

local projected = entities.query_many({ ids = { ids.song, ids.album, ids.amy, table.unpack(others) } })
assert(projected[ids.song].entity_type == "track", "the track")
assert(projected[ids.album].entity_type == "release", "the release")
assert(projected[ids.amy].entity_type == "artist", "the artist")
for _, id in others do
	assert(projected[id] == nil, "a node without a projection")
end
