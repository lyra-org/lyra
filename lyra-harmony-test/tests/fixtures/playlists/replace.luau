return function(fixture: { track_id: number, foreign_id: number, foreign_entry: string })
	local playlists = require("@lyra/playlists")
	local id = playlists.create({ name = "Replace me" })

	local added = playlists.add_tracks(id, { fixture.track_id, fixture.track_id })
	assert(added ~= nil and #added == 2 and added[1] ~= added[2])
	assert(playlists.add_tracks(fixture.foreign_id, { fixture.track_id }) == nil)
	assert(not pcall(playlists.add_tracks, id, { fixture.track_id, 999999999 }))
	assert(#playlists.get_tracks(id) == 2, "a failed add changed the playlist")

	local replaced = playlists.replace(id, {
		fields = { name = "Replaced", is_public = true },
		track_ids = { fixture.track_id, fixture.track_id, fixture.track_id },
	})
	assert(replaced ~= nil and replaced.name == "Replaced" and replaced.is_public == true)
	local entries = playlists.get_tracks(id)
	assert(#entries == 3, `entry count: {#entries}`)
	for position, entry in entries do
		assert(entry.track_id == fixture.track_id and entry.position == position - 1)
		assert(entry.entry_id ~= added[1] and entry.entry_id ~= added[2])
	end

	assert(not pcall(playlists.replace, id, { fields = { name = "Broken" }, track_ids = { 999999999 } }))
	local unchanged = playlists.get_by_id(id)
	assert(unchanged ~= nil and unchanged.name == "Replaced")
	assert(#playlists.get_tracks(id) == 3, "a failed replace changed the playlist")

	assert(playlists.replace(fixture.foreign_id, { track_ids = {} }) == nil)
	assert(#playlists.get_tracks(fixture.foreign_id) == 1, "foreign playlist changed")
	local emptied = playlists.replace(id, { track_ids = {} })
	assert(emptied ~= nil and emptied.name == "Replaced" and #playlists.get_tracks(id) == 0)

	local owned_before = #playlists.list_owned()
	assert(not pcall(playlists.create, { name = "Broken", track_ids = { fixture.track_id, 999999999 } }))
	assert(#playlists.list_owned() == owned_before, "a failed create left a playlist")
	local filled = playlists.create({ name = "Filled", track_ids = { fixture.track_id, fixture.track_id } })
	local filled_entries = playlists.get_tracks(filled)
	assert(#filled_entries == 2, `filled count: {#filled_entries}`)

	assert(playlists.remove_tracks(filled, { filled_entries[1].entry_id, "missing-entry" }) == false)
	assert(#playlists.get_tracks(filled) == 2, "a failed removal changed the playlist")
	assert(playlists.remove_tracks(fixture.foreign_id, { fixture.foreign_entry }) == false)
	assert(playlists.remove_tracks(filled, { filled_entries[1].entry_id, filled_entries[2].entry_id }) == true)
	assert(#playlists.get_tracks(filled) == 0)
	assert(#playlists.get_tracks(fixture.foreign_id) == 1, "foreign playlist changed")
end
