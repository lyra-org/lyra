--!strict
local matching = require("../../matching")

local LISA_MBID = "85d76093-9865-4605-97fa-8c910929d366"
local ALBUM = {
	idAlbum = "2365671",
	idArtist = "122120",
	strAlbum = "LEO-NiNE",
	strArtist = "LiSA",
	strMusicBrainzID = "8cacc1ab-799a-484c-af26-c1c1bf3af7c7",
	strMusicBrainzArtistID = LISA_MBID,
}

-- An album matches only by its exact title and artist, never as the first of the results.
assert(matching.best_album({ ALBUM }, "lisa", "leo-nine", nil) == ALBUM, "exact, normalized")
assert(matching.best_album({ ALBUM }, "LiSA", "LEO-NiNE (Deluxe)", nil) == nil, "another title")
assert(matching.best_album({ ALBUM }, "LiSA feat. PABLO", "LEO-NiNE", nil) == nil, "another artist")

-- Of several, the one released in the release's year.
local REISSUE = table.clone(ALBUM)
REISSUE.idAlbum = "9"
REISSUE.intYearReleased = "2022"
local ORIGINAL = table.clone(ALBUM)
ORIGINAL.intYearReleased = "2020"
assert(matching.best_album({ REISSUE, ORIGINAL }, "LiSA", "LEO-NiNE", "2020-10-14") == ORIGINAL, "the year")

-- A track's artist must be one of the names exactly.
assert(matching.artist_name_matches_any("LiSA", { "lisa" }), "exact")
assert(not matching.artist_name_matches_any("LiSA feat. PABLO", { "LiSA" }), "a partial name")

-- The album's artist id goes to the credited artist named as the album's artist, unless their
-- MusicBrainz id is another's.
local function artist(name: string, musicbrainz_id: string?): matching.CreditedArtist
	return { id = name, artist_name = name, musicbrainz_id = musicbrainz_id }
end
local credited = matching.album_artist({ artist("PABLO"), artist("LiSA", LISA_MBID) }, ALBUM)
assert(credited ~= nil and credited.id == "LiSA", "the album's artist")
assert(matching.album_artist({ artist("PABLO") }, ALBUM) == nil, "a sole artist of another name")
assert(
	matching.album_artist({ artist("Lisa", "fde67432-278f-4a41-9e2f-0d1b35d8a3dc") }, ALBUM) == nil,
	"a namesake by MusicBrainz id"
)
