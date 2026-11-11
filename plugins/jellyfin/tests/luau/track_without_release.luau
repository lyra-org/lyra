local fixture = ...
local catalog = require("../../protocol/items/catalog")
local item_ids = require("../../protocol/items/identifiers")
local options = require("../../protocol/items/options")
local tracks = require("@lyra/tracks")

local function parse(params: { [string]: string })
	local raw: { [string]: { string } } = {}
	for key, value in params do
		raw[key] = { value }
	end
	return options.for_user(options.parse(raw :: any), fixture.users.admin)
end

local loose = tracks.get({ fixture.ids.loose })[fixture.ids.loose]

-- With images on, as by default, and off: a track shows an album only if it has one.
for _, images in { "true", "false" } do
	local shown = parse({ Fields = "People", EnableImages = images })
	local item = catalog.realize(catalog.track_stubs({ loose }), shown)[1]
	assert(item.Name == "Loose", "the track is realized")
	assert(item.Album == nil and item.AlbumId == nil, "a track of no release shows no album")
	assert(item.AlbumPrimaryImageTag == nil, "nor an album's image")
	assert(#item.Artists == 1 and item.Artists[1] == "Amy", "its own artist")
	assert(item.AlbumArtist == "Amy", "who stands in as its album artist")
	assert(#item.AlbumArtists == 1 and item.AlbumArtists[1].Id == item_ids.encode(fixture.ids.amy), "by id too")
	assert(#item.People == 1 and item.People[1].Name == "Amy" and item.People[1].Type == "Artist", "and is its People")

	local people = catalog.people_of_stubs(catalog.track_stubs({ loose }), shown)[fixture.ids.loose]
	assert(#people == 1 and people[1].Name == "Amy", "the People a person filter reads")
end
