local fixture = ...
local track_id: number = fixture.ids.track
local foreign_id: number = fixture.ids.foreign

local playlists = require("@lyra/playlists")
local id = playlists.create({ name = "Delete me" })
local entry = playlists.add_track(id, track_id)
local playlist = playlists.get_by_id(id)
assert(playlist ~= nil)
assert(playlists.delete(foreign_id) == false)
assert(playlists.delete(track_id) == false)
assert(playlists.delete("missing-playlist") == false)
assert(playlists.delete(999999999) == false)
assert(playlists.delete(playlist.id) == true)
assert(playlists.get_by_id(id) == nil)
assert(playlists.delete(id) == false)
assert(entry ~= nil and playlists.remove_track(id, entry) == false)
assert(playlists.get_by_id(foreign_id) ~= nil)
assert(not pcall(playlists.delete, {}))
