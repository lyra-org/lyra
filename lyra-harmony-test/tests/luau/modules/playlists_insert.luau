local fixture = ...
local a: string = fixture.ids.a
local b: string = fixture.ids.b
local c: string = fixture.ids.c
local mix: string = fixture.ids.mix

local playlists = require("@lyra/playlists")

local function order(): { string }
	local ids = {}
	for position, entry in playlists.entries({ mix })[mix] do
		assert(entry.position == position - 1, `position {entry.position} at index {position}`)
		table.insert(ids, entry.track_id)
	end
	return ids
end

local function assert_order(expected: { string })
	local actual = order()
	assert(
		table.concat(actual, ",") == table.concat(expected, ","),
		`expected {table.concat(expected, ",")}, got {table.concat(actual, ",")}`
	)
end

local inserted = playlists.add_tracks(mix, { c, c }, { position = 1 })
assert(inserted ~= nil and #inserted == 2)
assert_order({ a, c, c, b })
local entries = playlists.entries({ mix })[mix]
assert(entries[2].entry_id == inserted[1] and entries[3].entry_id == inserted[2])

playlists.add_tracks(mix, { b }, { position = 0 })
assert_order({ b, a, c, c, b })
playlists.add_tracks(mix, { a }, { position = 99 })
assert_order({ b, a, c, c, b, a })
playlists.add_tracks(mix, { c })
assert_order({ b, a, c, c, b, a, c })

for _, invalid in { -1, 1.5, "1" } do
	assert(not pcall(playlists.add_tracks, mix, { a }, { position = invalid :: any }))
end
assert(#playlists.entries({ mix })[mix] == 7, "an invalid position changed the playlist")

local none = playlists.add_tracks(mix, {})
assert(none ~= nil and #none == 0, "no tracks add nothing")
assert(#playlists.entries({ mix })[mix] == 7)
