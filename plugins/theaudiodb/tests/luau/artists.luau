--!strict
local audiodb = require("./_audiodb")
local matching = require("../../matching")

local LISA_MBID = "85d76093-9865-4605-97fa-8c910929d366"
local LISA = { idArtist = "122120", strArtist = "LiSA", strMusicBrainzID = LISA_MBID }
local NAMESAKE = {
	idArtist = "119641",
	strArtist = "Lisa",
	strMusicBrainzID = "fde67432-278f-4a41-9e2f-0d1b35d8a3dc",
}

-- The id of the record the refresh resolves `query` to.
local function resolved_id(query: matching.ArtistQuery): string?
	local artist = matching.refresh_artist(query)
	return if artist then artist.idArtist :: string else nil
end

-- The id of the record the cover lookup resolves `query` to.
local function cover_id(query: matching.ArtistQuery): string?
	local artist = matching.cover_artist(query)
	return if artist then artist.idArtist :: string else nil
end

-- A name search hit naming another MusicBrainz artist is no match, though its name is.
audiodb.serve({
	{ pattern = "artist-mb.php", body = { artists = nil } },
	{ pattern = "search.php?s=LiSA", body = { artists = { NAMESAKE } } },
})
assert(resolved_id({ musicbrainz_id = LISA_MBID, names = { "LiSA" } }) == nil, "the namesake is rejected")

-- The MusicBrainz id finds the artist before any search.
audiodb.serve({
	{ pattern = "artist-mb.php?i=" .. LISA_MBID, body = { artists = { LISA } } },
})
assert(resolved_id({ musicbrainz_id = LISA_MBID, names = { "LiSA" } }) == "122120", "by MBID")
assert(#audiodb.requests == 1, "no search")

-- A stored id whose record names another MusicBrainz artist gives way to the MusicBrainz id.
audiodb.serve({
	{ pattern = "artist.php?i=119641", body = { artists = { NAMESAKE } } },
	{ pattern = "artist-mb.php?i=" .. LISA_MBID, body = { artists = { LISA } } },
})
assert(
	resolved_id({ artist_id = "119641", musicbrainz_id = LISA_MBID, names = { "LiSA" } }) == "122120",
	"the wrong stored id is passed over"
)

-- The cover lookup takes a stored id as it is, while its record exists; the refresh before it
-- is what replaces a wrong one.
audiodb.serve({
	{ pattern = "artist.php?i=119641", body = { artists = { NAMESAKE } } },
})
assert(
	cover_id({ artist_id = "119641", musicbrainz_id = LISA_MBID, names = { "LiSA" } }) == "119641",
	"the cover lookup does not doubt the stored id"
)
audiodb.serve({
	{ pattern = "artist.php?i=119641", body = { artists = nil } },
	{ pattern = "artist-mb.php?i=" .. LISA_MBID, body = { artists = { LISA } } },
})
assert(
	cover_id({ artist_id = "119641", musicbrainz_id = LISA_MBID, names = { "LiSA" } }) == "122120",
	"a gone record falls through to the MusicBrainz id"
)

-- A stored id whose record names no MusicBrainz artist stands.
audiodb.serve({
	{ pattern = "artist.php?i=122120", body = { artists = { { idArtist = "122120", strArtist = "LiSA" } } } },
})
assert(
	resolved_id({ artist_id = "122120", musicbrainz_id = LISA_MBID, names = {} }) == "122120",
	"no MBID is no evidence"
)

-- A stored id that no longer resolves falls through to the search.
audiodb.serve({
	{ pattern = "artist.php?i=1", body = { artists = nil } },
	{ pattern = "search.php?s=LiSA", body = { artists = { LISA } } },
})
assert(resolved_id({ artist_id = "1", names = { "LiSA" } }) == "122120", "the search after a gone id")

-- Only an exact name or alternate name matches; nothing matching is no match, not the first hit.
audiodb.serve({
	{
		pattern = "search.php?s=Yorushika",
		body = { artists = { { idArtist = "1", strArtist = "Yorushika Tribute" } } },
	},
})
assert(resolved_id({ names = { "Yorushika" } }) == nil, "a partial name is no match")
audiodb.serve({
	{
		pattern = "search.php?s=",
		body = { artists = { { idArtist = "2", strArtist = "ヨルシカ", strArtistAlternate = "Yorushika" } } },
	},
})
assert(resolved_id({ names = { "yorushika" } }) == "2", "the alternate name")

-- The sort name is searched when the name finds nothing.
audiodb.serve({
	{
		pattern = "search.php?s=Beatles%2C%20The",
		body = { artists = { { idArtist = "3", strArtist = "Beatles, The" } } },
	},
	{ pattern = "search.php?s=The%20Beatles", body = { artists = nil } },
})
assert(resolved_id({ names = matching.lookup_names({ "The Beatles", "Beatles, The" }) }) == "3", "the sort name")
