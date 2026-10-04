local fixture = ...
local track_id: number = fixture.ids.track
local foreign_id: number = fixture.ids.foreign
local foreign_entry: string = fixture.entries.foreign[1]

local playlists = require("@lyra/playlists")
local id = playlists.create({ name = "Mine" })
local updated = playlists.update({ playlist_id = id, name = "Renamed" })
assert(updated and updated.name == "Renamed")
for _, missing in { "missing-playlist", 999999999, foreign_id, track_id } do
	assert(playlists.update({ playlist_id = missing, name = "Rejected" }) == nil)
	assert(playlists.add_track(missing, track_id) == nil)
end
for _, missing in { "missing-track", 999999999, id } do
	assert(playlists.add_track(id, missing) == nil)
end
local first = playlists.add_track(id, track_id)
local duplicate = playlists.add_track(id, track_id)
assert(first ~= nil and duplicate ~= nil and first ~= duplicate)
local third = playlists.add_track(id, track_id)
assert(third ~= nil)
assert(playlists.remove_track(id, first) == true)
assert(playlists.remove_track(id, first) == false)
assert(playlists.remove_track(id, foreign_entry) == false)
assert(playlists.remove_track(foreign_id, foreign_entry) == false)
assert(playlists.remove_track(track_id, duplicate) == false)
assert(playlists.remove_track(id, "missing-entry") == false)
assert(playlists.move_track(id, foreign_entry, 0) == false)
assert(playlists.move_track(foreign_id, foreign_entry, 0) == false)
assert(playlists.move_track(id, third, 0) == true)
local remaining = playlists.get_tracks(id)
assert(#remaining == 2, `remaining count: {#remaining}`)
assert(remaining[1].entry_id == third, `expected {third}, got {remaining[1].entry_id}`)
assert(remaining[2].entry_id == duplicate, `expected {duplicate}, got {remaining[2].entry_id}`)
assert(#playlists.get_tracks(foreign_id) == 1, "foreign playlist changed")
assert(not pcall(playlists.add_track, {}, track_id))
assert(not pcall(playlists.remove_track, id, {}))
