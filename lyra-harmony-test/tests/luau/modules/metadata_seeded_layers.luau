local fixture = ...
local artists = require("@lyra/artists")
local genres = require("@lyra/genres")
local metadata = require("@lyra/metadata")
local tracks = require("@lyra/tracks")

local provider = metadata.Provider.new("seeded-layer-test")
local album = fixture.ids.album
local song = fixture.ids.song

local function names(list: { artists.Artist }?): string
	local found = {}
	for _, artist in list or {} do
		table.insert(found, artist.artist_name)
	end
	table.sort(found)
	return table.concat(found, ", ")
end

-- A provider layer that sets neither credits nor genres leaves the seeded ones.
local release_layer = provider:layer(album)
release_layer:set_field("release_title", "Provider Album")
release_layer:save()
local track_layer = provider:layer(song)
track_layer:set_field("track_title", "Provider Song")
track_layer:save()

local release_artists = names(artists.by_release({ album })[album])
assert(release_artists == "Singer", release_artists)
local release_genres = genres.by_release({ album })[album] or {}
assert(#release_genres == 1 and release_genres[1].name == "Rock", "the seeded genre stays")
local track_artists = names(artists.by_track({ song })[song])
assert(track_artists == "Singer, Writer", track_artists)

local track = tracks.get({ song })[song]
assert(track.track_title == "Provider Song", track.track_title)
assert(track.disc == 1 and track.track == 2 and track.year == 2001, "the seeded numbering stays")
