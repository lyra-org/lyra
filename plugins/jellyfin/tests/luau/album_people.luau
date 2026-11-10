local fixture = ...
local item_ids = require("../../protocol/items/identifiers")
local listing = require("../../protocol/items/listing")
local options = require("../../protocol/items/options")

local function people_of(id: string, fields: string): string
	local parsed = options.for_user(
		options.parse({ Ids = { item_ids.encode(id) }, Fields = { fields } } :: any),
		fixture.users.admin
	)
	local items = listing.list_items(parsed)
	local names = {}
	for _, person in items[1].People or {} do
		table.insert(names, `{person.Name}:{person.Type}`)
	end
	return table.concat(names, ",")
end

local expected = "Band:AlbumArtist,Often:Artist,Sometimes:Artist,Rare:Artist,Tied:Artist"
local actual = people_of(fixture.ids.album, "People")
assert(actual == expected, `expected "{expected}", got "{actual}"`)
assert(people_of(fixture.ids.album, "Genres") == "", "People only when asked for")

-- `AlbumArtists` falls back to a track's own artists where its release has none; its People don't.
local loose = people_of(fixture.ids.loose, "People")
assert(loose == "Solo:Artist", `a release without artists adds no album artist: {loose}`)
