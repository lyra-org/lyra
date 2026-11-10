local fixture = ...
local item_ids = require("../../protocol/items/identifiers")
local listing = require("../../protocol/items/listing")
local options = require("../../protocol/items/options")

local ids = fixture.ids

local function parse(params: { [string]: string })
	local raw: { [string]: { string } } = {}
	for key, value in params do
		raw[key] = { value }
	end
	return options.for_user(options.parse(raw :: any), fixture.users.admin)
end

local function persons(params: { [string]: string }): string
	local items, total = listing.list_person_items(parse(params))
	local names = {}
	for _, item in items do
		table.insert(names, `{item.Name}:{item.Type}`)
	end
	return `{table.concat(names, ",")} ({total})`
end

local function people(id: string): string
	local items = listing.list_items(parse({ Ids = item_ids.encode(id), Fields = "People" }))
	local names = {}
	for _, person in items[1].People do
		table.insert(names, `{person.Name}:{person.Type}`)
	end
	return table.concat(names, ",")
end

local function check(actual: string, expected: string, why: string)
	assert(actual == expected, `{why}: expected "{expected}", got "{actual}"`)
end

-- Everyone credited is a person, artists and groups included, unless the request names types.
check(persons({}), "Band:Person,Comp:Person,Prod:Person,Singer:Person (4)", "every credited artist")
check(persons({ PersonTypes = "Artist" }), "Band:Person,Singer:Person (2)", "artist credits anywhere")
check(persons({ PersonTypes = "AlbumArtist" }), "Band:Person (1)", "AlbumArtist alone means release credits")
check(
	persons({ PersonTypes = "AlbumArtist,Composer" }),
	"Band:Person,Comp:Person (2)",
	"AlbumArtist among other types still means release credits"
)
check(
	persons({ PersonTypes = "Artist,Composer" }),
	"Band:Person,Comp:Person,Singer:Person (3)",
	"Artist among other types counts artist credits anywhere"
)
check(
	persons({ PersonTypes = "AlbumArtist,Composer", AppearsInItemId = item_ids.encode(ids.song) }),
	"Comp:Person (1)",
	"the mix narrows with the rest of the listing, and the track's own artist holds no release credit"
)
check(persons({ PersonTypes = "Composer" }), "Comp:Person (1)", "one credit type")
check(persons({ PersonTypes = "Director" }), " (0)", "a type no credit maps to")
check(persons({ PersonTypes = "Narrator" }), " (0)", "narrators, which no credit maps to")
check(
	persons({ ExcludePersonTypes = "Artist,AlbumArtist" }),
	"Comp:Person,Prod:Person (2)",
	"jellyfin-web's people search"
)
check(
	persons({ ExcludePersonTypes = "Composer" }),
	"Band:Person,Prod:Person,Singer:Person (3)",
	"an excluded type drops only those holding no other credit"
)
check(
	persons({ AppearsInItemId = item_ids.encode(ids.song) }),
	"Comp:Person,Singer:Person (2)",
	"people credited on a track"
)

-- An item's People include its artist credits, typed by where the credit sits.
check(people(ids.song), "Band:AlbumArtist,Singer:Artist,Comp:Composer", "a track opens with its album's artists")
check(people(ids.other), "Band:AlbumArtist,Prod:Producer", "a track without credits takes its release's, once")
check(people(ids.album), "Band:AlbumArtist,Singer:Artist,Band:Artist", "an album's artists and its tracks'")

-- `PersonIds` with `PersonTypes` reads the People a listing shows.
local catalog = require("../../protocol/items/catalog")
local tracks = require("@lyra/tracks")
local shown = parse({ Fields = "People" })
local song_track = tracks.get({ ids.song })[ids.song]
local realized = catalog.realize(catalog.track_stubs({ song_track }), shown)[1]
local read = catalog.people_of_stubs(catalog.track_stubs({ song_track }), parse({}))[ids.song]
assert(#read == #realized.People, "as many people as realizing")
for index, person in read do
	local other = realized.People[index]
	assert(person.Id == other.Id and person.Type == other.Type and person.Role == other.Role, "the same people")
end

-- `PersonIds` with `PersonTypes` keeps the items listing the person as one of those types.
local function items(params: { [string]: string }): string
	local listed = listing.list_items(parse(params))
	local names = {}
	for _, item in listed do
		table.insert(names, item.Name)
	end
	table.sort(names)
	return table.concat(names, ",")
end
local function ask(person: string, types: string?, include: string): { [string]: string }
	return {
		PersonIds = item_ids.encode(ids[person]),
		PersonTypes = types,
		IncludeItemTypes = include,
		Recursive = "true",
	}
end

check(items(ask("band", "AlbumArtist", "Audio")), "Other,Song", "a release's artist is its tracks' album artist")
check(items(ask("band", "Artist", "Audio")), "", "that artist isn't a track's artist")
check(items(ask("singer", "Artist", "Audio")), "Song", "a track's artist")
check(items(ask("singer", "AlbumArtist", "Audio")), "", "isn't its album artist")
check(items(ask("comp", "Composer", "Audio")), "Song", "a composer")
check(items(ask("comp", "Artist,Composer", "Audio")), "Song", "any of the types")
check(items(ask("band", nil, "Audio")), "Other,Song", "without types, any credit")
check(items(ask("band", "AlbumArtist", "MusicAlbum")), "Album", "an album's artist")
check(items(ask("singer", "Artist", "MusicAlbum")), "Album", "its tracks' artist is the album's Artist")
check(items(ask("singer", "AlbumArtist", "MusicAlbum")), "", "but not its album artist")
check(items(ask("prod", "Producer", "MusicAlbum")), "", "an album no longer lists its producers")

-- `/Persons/{name}` resolves an artist to the artist itself.
local band = listing.find_person_by_name("band", fixture.users.admin)
assert(band, "Band resolves by name")
check(`{band.Name}:{band.Type}`, "Band:Person", "an artist found by name")
check(band.Id, item_ids.encode(ids.band), "a person's id is the artist's")
