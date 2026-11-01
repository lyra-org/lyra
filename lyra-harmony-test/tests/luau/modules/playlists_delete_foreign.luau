local fixture = ...
local playlists = require("@lyra/playlists")

-- A user who isn't an admin deletes only its own playlists, not another user's public one.
local foreign = fixture.ids.foreign
assert(playlists.delete(foreign) == false)
assert(playlists.get({ foreign })[foreign] ~= nil)
