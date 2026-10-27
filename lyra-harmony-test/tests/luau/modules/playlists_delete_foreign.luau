local fixture = ...
local playlists = require("@lyra/playlists")

-- A user who isn't an admin deletes only its own playlists, not another user's public one.
assert(playlists.delete(fixture.ids.foreign) == false)
assert(playlists.get_by_id(fixture.ids.foreign) ~= nil)
