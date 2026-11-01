local fixture = ...
local ids = fixture.ids

local playlists = require("@lyra/playlists")

local function names(page: playlists.PlaylistPage): string
	local listed = {}
	for _, playlist in page.items do
		table.insert(listed, playlist.name)
	end
	return table.concat(listed, ",")
end

local function assert_names(query: playlists.PlaylistQuery?, expected: string)
	local actual = names(playlists.query(query))
	assert(actual == expected, `expected {expected}, got {actual}`)
end

-- Every role sees its own playlists and the public ones, by name.
assert_names(nil, "A Jazz Night,Jazz,Shared")
local all = playlists.query()
assert(all.total == 3 and all.offset == 0)
local found = playlists.get({ ids.jazz, ids.shared, ids.hidden, "missing" })
assert(found[ids.jazz].owner_id == fixture.users.owner)
assert(found[ids.shared].owner_id == fixture.users.other and found[ids.shared].is_public == true)
assert(found[ids.hidden] == nil, "an admin doesn't see another user's private playlist")
assert(found.missing == nil)
local entries = playlists.entries({ ids.jazz, ids.hidden, ids.night })
assert(#entries[ids.jazz] == 1 and entries[ids.jazz][1].track_id == ids.song)
assert(entries[ids.hidden] == nil)
assert(#entries[ids.night] == 0, "an empty playlist maps to no entries")

-- A search ranks by relevance unless sorted.
assert_names({ search = "jazz" }, "Jazz,A Jazz Night")
assert_names({ search = "jazz", sort = { { key = "name" } } }, "A Jazz Night,Jazz")
assert_names({ search = "jazz", sort = { { key = "relevance", order = "descending" } } }, "A Jazz Night,Jazz")
assert(not pcall(playlists.query, { sort = { { key = "relevance" } } }), "relevance needs a search")
assert(not pcall(playlists.query, { sort = { { key = "date_created" } } }), "an unknown key raises")
assert(not pcall(playlists.query, { sort = { { key = "name", order = "up" } } }), "an unknown order raises")

assert_names({ sort = { { key = "name", order = "descending" } } }, "Shared,Jazz,A Jazz Night")
assert_names({ sort = { { key = "track_count", order = "descending" }, { key = "name" } } }, "Jazz,A Jazz Night,Shared")

-- Owners, ids and favorites narrow the list before it is paged.
assert_names({ owner_id = fixture.users.owner }, "A Jazz Night,Jazz")
assert_names({ owner_id = fixture.users.other }, "Shared")
assert_names({ owner_id = "missing-user" }, "")
assert(playlists.query({ owner_id = ids.song }).total == 0, "a non-user owner gives an empty page")
assert_names({ ids = { ids.jazz, ids.hidden, "missing" } }, "Jazz")
assert_names({ ids = { "missing" } }, "")
assert_names({ exclude_ids = { ids.jazz, "missing" } }, "A Jazz Night,Shared")
assert_names({ favorite = true }, "Shared")
assert_names({ favorite = false }, "A Jazz Night,Jazz")
assert_names({ sort = { { key = "favorited_at" } } }, "Shared,A Jazz Night,Jazz")

local page = playlists.query({ offset = 1, limit = 1 })
assert(names(page) == "Jazz" and page.total == 3 and page.offset == 1)
assert(#playlists.query({ offset = 5 }).items == 0)
