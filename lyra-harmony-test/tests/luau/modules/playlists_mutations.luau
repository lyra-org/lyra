local fixture = ...
local track_id: string = fixture.ids.track
local foreign_id: string = fixture.ids.foreign
local foreign_entry: string = fixture.entries.foreign[1]

local playlists = require("@lyra/playlists")

local function entries(id: string): { playlists.PlaylistEntry }
	return playlists.entries({ id })[id]
end

local id = playlists.create({ name = "Mine", description = "notes" }).id
local updated = playlists.update(id, { name = "Renamed" })
assert(updated and updated.name == "Renamed" and updated.description == "notes")
local cleared = playlists.update(id, { description = "" })
assert(cleared and cleared.description == nil, "an empty description clears it")
for _, missing in { "missing-playlist", foreign_id, track_id } do
	assert(playlists.update(missing, { name = "Rejected" }) == nil)
	assert(playlists.add_tracks(missing, { track_id }) == nil)
end
for _, missing in { "missing-track", id } do
	assert(not pcall(playlists.add_tracks, id, { missing }))
end
local added = playlists.add_tracks(id, { track_id, track_id, track_id })
assert(added ~= nil and #added == 3)
local first, duplicate, third = added[1], added[2], added[3]
assert(first ~= duplicate)
assert(playlists.remove_entries(id, { first }) == true)
assert(playlists.remove_entries(id, { first }) == false)
assert(playlists.remove_entries(id, { foreign_entry }) == false)
assert(playlists.remove_entries(foreign_id, { foreign_entry }) == false)
assert(playlists.remove_entries(track_id, { duplicate }) == false)
assert(playlists.remove_entries(id, { "missing-entry" }) == false)
assert(not pcall(playlists.remove_entries, id, { duplicate, duplicate }), "a repeated entry raises")
assert(#entries(id) == 2, "a repeated entry removed something")
assert(playlists.move_entry(id, foreign_entry, 0) == false)
assert(playlists.move_entry(foreign_id, foreign_entry, 0) == false)
assert(playlists.move_entry(id, third, 0) == true)
local remaining = entries(id)
assert(#remaining == 2, `remaining count: {#remaining}`)
assert(remaining[1].entry_id == third, `expected {third}, got {remaining[1].entry_id}`)
assert(remaining[2].entry_id == duplicate, `expected {duplicate}, got {remaining[2].entry_id}`)
assert(#entries(foreign_id) == 1, "foreign playlist changed")
assert(not pcall(playlists.add_tracks, {}, { track_id }))
assert(not pcall(playlists.add_tracks, id, { 1 }))
assert(playlists.remove_entries(id, {}) == true, "no entries remove nothing")
assert(not pcall(playlists.remove_entries, id, { {} }))
assert(not pcall(playlists.move_entry, id, third, -1))

assert(playlists.MAX_TRACK_IDS == 10000)
local too_many = table.create(playlists.MAX_TRACK_IDS + 1, track_id)
assert(not pcall(playlists.add_tracks, id, too_many), "past the cap raises")
assert(not pcall(playlists.create, { name = "Too many", track_ids = too_many }))
assert(not pcall(playlists.remove_entries, id, table.create(playlists.MAX_TRACK_IDS + 1, "x")))
assert(#entries(id) == 2, "a capped add changed the playlist")
