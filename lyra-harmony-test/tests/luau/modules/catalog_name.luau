local artists = require("@lyra/artists")
local genres = require("@lyra/genres")
local releases = require("@lyra/releases")
local tracks = require("@lyra/tracks")

local fixture = ...
local ids = fixture.ids

local function names(module, field: string, query: { [string]: any }): string
	local found = {}
	for _, item in module.query(query).items do
		table.insert(found, item[field])
	end
	return table.concat(found, " | ")
end

local function expect(module, field: string, query: { [string]: any }, expected: string)
	local found = names(module, field, query)
	assert(found == expected, `{query.name}: {found}`)
end

-- `name` keeps exact names, folding case and accents beyond ASCII, and nothing that only starts
-- with the name.
expect(artists, "artist_name", { name = "BJÖRK" }, "BJÖRK | Björk")
expect(artists, "artist_name", { name = "bjork" }, "BJÖRK | Björk")
expect(tracks, "track_title", { name = "jóga" }, "JÓGA | Jóga")
expect(releases, "release_title", { name = "homogénic" }, "HOMOGENIC | Homogenic")
expect(tracks, "track_title", { name = "Jóga Live" }, "Jóga (Live)")
expect(tracks, "track_title", { name = "Jog" }, "")
expect(genres, "name", { name = "ÉLECTRONIQUE" }, "Électronique")
expect(genres, "name", { name = "electronique" }, "Électronique")
expect(genres, "name", { name = "trip hop" }, "Trip-Hop")

-- A name that folds away, as punctuation or spaces alone do, keeps nothing.
expect(artists, "artist_name", { name = "" }, "")
expect(artists, "artist_name", { name = "'" }, "")
expect(genres, "name", { name = "  " }, "")

-- It narrows the other filters and a search, and the total counts what it keeps.
expect(tracks, "track_title", { name = "Jóga", artist_ids = { ids.bjork } }, "Jóga")
expect(artists, "artist_name", { name = "Björk", search = "trio" }, "")
local page = tracks.query({ name = "JOGA", limit = 1 })
assert(page.total == 2 and #page.items == 1, `total {page.total}`)
