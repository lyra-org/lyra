local artists = require("@lyra/artists")
local releases = require("@lyra/releases")
local tracks = require("@lyra/tracks")

local function names(module, field: string, search: string): string
	local found = {}
	for _, item in module.query({ search = search }).items do
		table.insert(found, item[field])
	end
	return table.concat(found, " | ")
end

local function expect(module, field: string, search: string, expected: string)
	local found = names(module, field, search)
	assert(found == expected, `{search}: {found}`)
end

-- Accents fold whichever side carries them.
expect(artists, "artist_name", "beyonce", "Beyoncé")
expect(artists, "artist_name", "BEYONCÉ", "Beyoncé")
expect(artists, "artist_name", "sigur rós", "Sigur Ros")
expect(releases, "release_title", "agaetis", "Ágætis byrjun")

-- Punctuation and apostrophes fold, straight or curly.
expect(artists, "artist_name", "ac dc", "AC/DC")
expect(artists, "artist_name", "acdc", "AC/DC")
expect(artists, "artist_name", "guns n roses", "Guns N’ Roses")
expect(artists, "artist_name", "guns n' roses", "Guns N’ Roses")

-- No character is an operator.
expect(artists, "artist_name", "!!!", "!!!")
expect(artists, "artist_name", "$uicideboy$", "$uicideboy$")
expect(artists, "artist_name", "^^^", "")

-- Every word must start a word.
expect(tracks, "track_title", "hell bell", "Hells Bells")
expect(tracks, "track_title", "ells", "")

-- Tracks and releases also match through their artists and releases, below their own names.
expect(tracks, "track_title", "beyonce", "Beyonce Medley | Formation")
expect(tracks, "track_title", "!!!", "Beyonce Medley | Me and Giuliani Down by the Schoolyard")
expect(tracks, "track_title", "back black", "Beyonce Medley | Hells Bells")
expect(tracks, "track_title", "acdc bells", "Hells Bells")
expect(releases, "release_title", "acdc", "Back in Black")
expect(releases, "release_title", "lemonade", "Lemonade")
