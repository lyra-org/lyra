local fixture = ...
local track_id: string = fixture.ids.track
local foreign_id: string = fixture.ids.foreign
local hidden_id: string = fixture.ids.hidden

local playlists = require("@lyra/playlists")
local playlist = playlists.create({ name = "Delete me" })
local id = playlist.id
local entries = playlists.add_tracks(id, { track_id })
assert(entries ~= nil and #entries == 1)
assert(playlists.delete(track_id) == false)
assert(playlists.delete("missing-playlist") == false)
assert(not pcall(playlists.delete, 1))
assert(playlists.delete(id) == true)
assert(playlists.get({ id })[id] == nil)
assert(playlists.delete(id) == false)
assert(playlists.remove_entries(id, entries) == false)
assert(playlists.delete(foreign_id) == true, "an admin deletes another user's public playlist")
assert(playlists.get({ foreign_id })[foreign_id] == nil)
assert(playlists.delete(hidden_id) == false, "but not a private one it cannot see")
assert(not pcall(playlists.delete, {}))
