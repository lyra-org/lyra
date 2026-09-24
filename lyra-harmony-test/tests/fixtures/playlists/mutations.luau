return function(fixture: { track_id: number, foreign_id: number, foreign_entry: number })
	-- The Rust executor test supplies the database fixture and caller principal.
	local playlists = require("@lyra/playlists")
	local id = playlists.create({ name = "Mine" })
	local updated = playlists.update({ playlist_id = id, name = "Renamed" })
	assert(updated and updated.name == "Renamed")
	for _, missing in { "missing-playlist", 999999999, fixture.foreign_id, fixture.track_id } do
		assert(playlists.update({ playlist_id = missing, name = "Rejected" }) == nil)
		assert(playlists.add_track(missing, fixture.track_id) == nil)
	end
	for _, missing in { "missing-track", 999999999, id } do
		assert(playlists.add_track(id, missing) == nil)
	end
	local first = playlists.add_track(id, fixture.track_id)
	local duplicate = playlists.add_track(id, fixture.track_id)
	assert(first ~= nil and duplicate ~= nil and first ~= duplicate)
	assert(playlists.remove_track(first) == true)
	assert(playlists.remove_track(first) == false)
	assert(playlists.remove_track(fixture.foreign_entry) == false)
	assert(playlists.remove_track("missing-entry") == false)
	assert(playlists.remove_track(-999999999) == false)
	assert(playlists.remove_track(id) == false)
	local remaining = playlists.get_tracks(id)
	assert(#remaining == 1, `remaining count: {#remaining}`)
	assert(remaining[1].entry_id == duplicate, `expected {duplicate}, got {remaining[1].entry_id}`)
	assert(#playlists.get_tracks(fixture.foreign_id) == 1, "foreign playlist changed")
	assert(not pcall(playlists.add_track, {}, fixture.track_id))
	assert(not pcall(playlists.remove_track, 1.5))
end
