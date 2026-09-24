local playlists = require("@lyra/playlists")

-- Bindings act as the dispatch principal, which owns what it creates.
local playlist_id = playlists.create({ name = "Harness Playlist" })
local owner_id = playlists.get_owner(playlist_id)
assert(type(owner_id) == "string" and #owner_id == 21)

local owned = playlists.list_owned()
assert(#owned == 1)
assert(owned[1].db_id == playlist_id)

local updated = playlists.update({ playlist_id = playlist_id, name = "Renamed" })
assert(updated ~= nil and updated.name == "Renamed")
assert(playlists.get_owner(playlist_id) == owner_id)

assert(playlists.delete(playlist_id) == true)
assert(#playlists.list_owned() == 0)
