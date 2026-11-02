local fixture = ...
local artists = require("@lyra/artists")
local metadata = require("@lyra/metadata")

local ArtistType = metadata.ArtistType
local provider = metadata.Provider.new("ensure-artist-test")
provider:id({ id_type = "artist_id", entity = metadata.EntityType.Artist })

local function artist_type(id: string): string?
	return artists.get({ id })[id].artist_type
end

-- A seeded artist goes by its exact name, as a scanned one does.
local found = provider:ensure_artist({
	id_type = "artist_id",
	id_value = "amy",
	artist_name = "Amy",
	artist_type = ArtistType.Person,
})
assert(found == fixture.ids.amy, `expected the seeded artist, got {found}`)

-- The provider corrects the type it gave.
found = provider:ensure_artist({
	id_type = "artist_id",
	id_value = "amy",
	artist_name = "Amy",
	artist_type = ArtistType.Group,
})
assert(found == fixture.ids.amy, `expected the same artist, got {found}`)
assert(artist_type(found) == "group", `got {artist_type(found)}`)

-- A scanned character is not the person of the same name.
found = provider:ensure_artist({
	id_type = "artist_id",
	id_value = "chara",
	artist_name = "Chara",
	artist_type = ArtistType.Person,
})
assert(found ~= fixture.ids.chara, "a person does not match the scanned character")
assert(artist_type(fixture.ids.chara) == "character", `got {artist_type(fixture.ids.chara)}`)

-- A manual type does not keep a provider from matching by name.
found = provider:ensure_artist({
	id_type = "artist_id",
	id_value = "manny",
	artist_name = "Manny",
	artist_type = ArtistType.Person,
})
assert(found == fixture.ids.manny, `expected the manually typed artist, got {found}`)
assert(artist_type(found) == "character", `got {artist_type(found)}`)

-- Nor does a type another provider supplied.
local other = metadata.Provider.new("ensure-artist-other")
local other_layer = other:layer(fixture.ids.otto)
other_layer:set_field("artist_type", "group")
other_layer:save()
found = provider:ensure_artist({
	id_type = "artist_id",
	id_value = "otto",
	artist_name = "Otto",
	artist_type = ArtistType.Person,
})
assert(found == fixture.ids.otto, `expected the artist another provider typed, got {found}`)

-- A decomposed spelling finds the scanned, composed one.
found = provider:ensure_artist({ id_type = "artist_id", id_value = "beyonce", artist_name = "Beyonce\u{301}" })
assert(found == fixture.ids.beyonce, `expected the scanned artist, got {found}`)

-- A new artist takes the composed spelling, as a scan would.
found = provider:ensure_artist({ id_type = "artist_id", id_value = "zoe", artist_name = "Zoe\u{308}" })
local created = artists.get({ found })[found]
assert(created.scan_name == "Zo\u{eb}" and created.artist_name == "Zo\u{eb}", `got {created.scan_name}`)

-- A new artist takes what the request supplies.
found = provider:ensure_artist({
	id_type = "artist_id",
	id_value = "nina",
	artist_name = "Nina",
	sort_name = "Nina, The",
	artist_type = ArtistType.Group,
})
local nina = artists.get({ found })[found]
assert(nina.sort_name == "Nina, The" and nina.artist_type == "group", `got {nina.sort_name}, {nina.artist_type}`)

-- Ensuring it again with less, as a credit naming only the artist does, keeps
-- what the provider stored.
assert(provider:ensure_artist({ id_type = "artist_id", id_value = "nina", artist_name = "Nina" }) == found)
nina = artists.get({ found })[found]
assert(nina.sort_name == "Nina, The", `the sort name is kept, got {nina.sort_name}`)
assert(nina.artist_type == "group", `the type is kept, got {nina.artist_type}`)
