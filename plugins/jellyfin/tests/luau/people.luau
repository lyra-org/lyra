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

local function people(id: number): string
	local items = listing.list_items(parse({ Ids = item_ids.encode_jellyfin_item_id(id), Fields = "People" }))
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
	"Band:Person,Comp:Person,Singer:Person (3)",
	"AlbumArtist among other types counts artist credits anywhere"
)
check(persons({ PersonTypes = "Composer" }), "Comp:Person (1)", "one credit type")
check(persons({ PersonTypes = "Director" }), " (0)", "a type no credit maps to")
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
	persons({ AppearsInItemId = item_ids.encode_jellyfin_item_id(ids.song) }),
	"Comp:Person,Singer:Person (2)",
	"people credited on a track"
)

-- An item's People include its artist credits, typed by where the credit sits.
check(people(ids.song), "Singer:Artist,Comp:Composer", "a track's own credits")
check(people(ids.other), "Band:AlbumArtist,Prod:Producer", "a track without credits takes its release's")
check(people(ids.album), "Band:AlbumArtist,Prod:Producer", "a release's credits")

-- `/Persons/{name}` resolves an artist to the artist itself.
local band = listing.find_person_by_name("band", fixture.users.admin)
assert(band, "Band resolves by name")
check(`{band.Name}:{band.Type}`, "Band:Person", "an artist found by name")
check(band.Id, item_ids.encode_jellyfin_item_id(ids.band), "a person's id is the artist's")
