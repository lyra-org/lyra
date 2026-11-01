local fixture = ...
local track_id: string = fixture.ids.track
local foreign_id: string = fixture.ids.foreign
local foreign_entry: string = fixture.entries.foreign[1]

local playlists = require("@lyra/playlists")
local created = playlists.create({ name = "Replace me" })
local id = created.id

local function entries_of(playlist_id: string): { playlists.PlaylistEntry }
	return playlists.entries({ playlist_id })[playlist_id]
end

local added = playlists.add_tracks(id, { track_id, track_id })
assert(added ~= nil and #added == 2 and added[1] ~= added[2])
assert(playlists.add_tracks(foreign_id, { track_id }) == nil)
assert(not pcall(playlists.add_tracks, id, { track_id, "missing-track" }))
assert(#entries_of(id) == 2, "a failed add changed the playlist")

local replaced = playlists.replace(id, {
	fields = { name = "Replaced", is_public = true },
	track_ids = { track_id, track_id, track_id },
})
assert(replaced ~= nil and replaced.name == "Replaced" and replaced.is_public == true)
local entries = entries_of(id)
assert(#entries == 3, `entry count: {#entries}`)
for position, entry in entries do
	assert(entry.track_id == track_id and entry.position == position - 1)
	assert(entry.entry_id ~= added[1] and entry.entry_id ~= added[2])
end

assert(not pcall(playlists.replace, id, { fields = { name = "Broken" }, track_ids = { "missing-track" } }))
local unchanged = playlists.get({ id })[id]
assert(unchanged ~= nil and unchanged.name == "Replaced")
assert(#entries_of(id) == 3, "a failed replace changed the playlist")

assert(playlists.replace(foreign_id, { track_ids = {} }) == nil)
assert(#entries_of(foreign_id) == 1, "foreign playlist changed")
local emptied = playlists.replace(id, { track_ids = {} })
assert(emptied ~= nil and emptied.name == "Replaced" and #entries_of(id) == 0)

local before = #entries_of(id)
for _, invalid in { { 1 }, { foo = "x" } } do
	assert(not pcall(playlists.replace, id, { track_ids = invalid :: any }))
	assert(not pcall(playlists.add_tracks, id, invalid :: any))
	assert(#entries_of(id) == before, "invalid ids changed the playlist")
end

local function owned_count(): number
	return playlists.query({ owner_id = created.owner_id }).total
end
local owned_before = owned_count()
assert(not pcall(playlists.create, { name = "Broken", track_ids = { track_id, "missing-track" } }))
assert(owned_count() == owned_before, "a failed create left a playlist")
local filled = playlists.create({ name = "Filled", track_ids = { track_id, track_id } }).id
local filled_entries = entries_of(filled)
assert(#filled_entries == 2, `filled count: {#filled_entries}`)

assert(playlists.remove_entries(filled, { filled_entries[1].entry_id, "missing-entry" }) == false)
assert(#entries_of(filled) == 2, "a failed removal changed the playlist")
assert(playlists.remove_entries(foreign_id, { foreign_entry }) == false)
assert(playlists.remove_entries(filled, { filled_entries[1].entry_id, filled_entries[2].entry_id }) == true)
assert(#entries_of(filled) == 0)
assert(#entries_of(foreign_id) == 1, "foreign playlist changed")
