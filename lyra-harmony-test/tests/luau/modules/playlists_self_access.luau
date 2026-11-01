local playlists = require("@lyra/playlists")

-- Bindings act as the dispatch principal, which owns what it creates.
local created = playlists.create({ name = "Harness Playlist" })
local owner_id = created.owner_id
assert(type(owner_id) == "string" and #owner_id == 21)
assert(created.is_public == false)

local owned = playlists.query({ owner_id = owner_id }).items
assert(#owned == 1)
assert(owned[1].id == created.id and owned[1].owner_id == owner_id)

local updated = playlists.update(created.id, { name = "Renamed" })
assert(updated ~= nil and updated.name == "Renamed" and updated.owner_id == owner_id)

assert(playlists.delete(created.id) == true)
assert(playlists.query({ owner_id = owner_id }).total == 0)
