local fixture = ...
local a: number = fixture.ids.a
local b: number = fixture.ids.b
local c: number = fixture.ids.c
local mix: number = fixture.ids.mix

local playlists = require("@lyra/playlists")

local function order(): { number }
	local ids = {}
	for position, entry in playlists.get_tracks(mix) do
		assert(entry.position == position - 1, `position {entry.position} at index {position}`)
		table.insert(ids, entry.track_id)
	end
	return ids
end

local function assert_order(expected: { number })
	local actual = order()
	assert(
		table.concat(actual, ",") == table.concat(expected, ","),
		`expected {table.concat(expected, ",")}, got {table.concat(actual, ",")}`
	)
end

local inserted = playlists.add_tracks(mix, { c, c }, 1)
assert(inserted ~= nil and #inserted == 2)
assert_order({ a, c, c, b })
local entries = playlists.get_tracks(mix)
assert(entries[2].entry_id == inserted[1] and entries[3].entry_id == inserted[2])

playlists.add_tracks(mix, { b }, 0)
assert_order({ b, a, c, c, b })
playlists.add_tracks(mix, { a }, 99)
assert_order({ b, a, c, c, b, a })
playlists.add_tracks(mix, { c })
assert_order({ b, a, c, c, b, a, c })

for _, invalid in { -1, 1.5, "1" } do
	assert(not pcall(playlists.add_tracks, mix, { a }, invalid :: any))
end
assert(#playlists.get_tracks(mix) == 7, "an invalid position changed the playlist")
