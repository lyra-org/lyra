--!strict
local api = require("../../api")
local handlers = require("./_handlers")

local LISA_MBID = "85d76093-9865-4605-97fa-8c910929d366"
local NAMESAKE_MBID = "fde67432-278f-4a41-9e2f-0d1b35d8a3dc"
local LISA = {
	idArtist = "122120",
	strArtist = "LiSA",
	strMusicBrainzID = LISA_MBID,
	strArtistThumb = "https://example.test/lisa.jpg",
}
local NAMESAKE = {
	idArtist = "119641",
	strArtist = "Lisa",
	strMusicBrainzID = NAMESAKE_MBID,
	strArtistThumb = "https://example.test/lisa-namesake.jpg",
}
local ALBUM = {
	idAlbum = "2365671",
	idArtist = "122120",
	strAlbum = "LEO-NiNE",
	strArtist = "LiSA",
	intYearReleased = "2020",
	strMusicBrainzID = "8cacc1ab-799a-484c-af26-c1c1bf3af7c7",
	strMusicBrainzArtistID = LISA_MBID,
}

-- The free tier finds only the namesake by name: with LiSA's MusicBrainz id, no cover.
handlers.serve({
	{ pattern = "artist-mb.php", body = { artists = nil } },
	{ pattern = "search.php", body = { artists = { NAMESAKE } } },
})
assert(handlers.artist_cover({
	id = "lisa",
	artist_name = "LiSA",
	external_ids = { ["musicbrainz:artist"] = LISA_MBID },
}) == nil, "the namesake's photo is not taken")

-- The cover takes a stored id as it is; the refresh replaces it when its record names another
-- MusicBrainz artist.
handlers.serve({
	{ pattern = "artist.php?i=119641", body = { artists = { NAMESAKE } } },
	{ pattern = "artist-mb.php?i=" .. LISA_MBID, body = { artists = { LISA } } },
})
assert(handlers.artist_cover({
	id = "lisa",
	artist_name = "LiSA",
	external_ids = { ["theaudiodb:artist"] = "119641", ["musicbrainz:artist"] = LISA_MBID },
}) == NAMESAKE.strArtistThumb, "the stored id's cover")
handlers.refresh_artist({
	id = "lisa",
	artist_name = "LiSA",
	external_ids = { ["theaudiodb:artist"] = "119641", ["musicbrainz:artist"] = LISA_MBID },
})
assert(handlers.saved_ids.lisa and handlers.saved_ids.lisa.artist_id == "122120", "the stored id is replaced")

-- A refresh that resolves to the stored id writes nothing.
handlers.refresh_artist({
	id = "lisa",
	artist_name = "LiSA",
	external_ids = { ["theaudiodb:artist"] = "122120", ["musicbrainz:artist"] = LISA_MBID },
})
assert(next(handlers.saved_ids) == nil, "nothing new to save")

-- A search hit's context holds only the ids the hit carries.
assert(
	handlers.artist_cover({ external_ids = { ["theaudiodb:artist"] = "119641" } }) == NAMESAKE.strArtistThumb,
	"a search hit's cover"
)

-- Once a stored id's record is gone, the cover and the refresh go on by the MusicBrainz id.
handlers.serve({
	{ pattern = "artist.php?i=119641", body = { artists = nil } },
	{ pattern = "artist-mb.php?i=" .. LISA_MBID, body = { artists = { LISA } } },
})
assert(handlers.artist_cover({
	id = "lisa",
	artist_name = "LiSA",
	external_ids = { ["theaudiodb:artist"] = "119641", ["musicbrainz:artist"] = LISA_MBID },
}) == LISA.strArtistThumb, "a gone record falls through")
handlers.refresh_artist({
	id = "lisa",
	artist_name = "LiSA",
	external_ids = { ["theaudiodb:artist"] = "119641", ["musicbrainz:artist"] = LISA_MBID },
})
assert(handlers.saved_ids.lisa and handlers.saved_ids.lisa.artist_id == "122120", "the found id is written")

-- The release refresh links the album's artist id to the credited artist it names.
local function release(artists: { any }): any
	return { id = "leo-nine", release_title = "LEO-NiNE", release_date = "2020-10-14", artists = artists }
end
local function credited(id: string, name: string, musicbrainz_id: string?): any
	return {
		id = id,
		artist_name = name,
		external_ids = if musicbrainz_id then { ["musicbrainz:artist"] = musicbrainz_id } else {},
	}
end
handlers.serve({ { pattern = "searchalbum.php", body = { album = { ALBUM } } } })
handlers.refresh_release(release({ credited("lisa", "LiSA", LISA_MBID), credited("pablo", "PABLO") }))
assert(handlers.saved_ids["leo-nine"].album_id == "2365671", "the album")
assert(handlers.saved_ids.lisa and handlers.saved_ids.lisa.artist_id == "122120", "the album's artist")
assert(handlers.saved_ids.pablo == nil, "not the other credited artist")

handlers.refresh_release(release({ credited("lisa", "Lisa", NAMESAKE_MBID) }))
assert(handlers.saved_ids["leo-nine"].album_id == "2365671", "the album still")
assert(handlers.saved_ids.lisa == nil, "not a namesake by MusicBrainz id")

-- An album of another title is no match, though it is the only result.
handlers.serve({ { pattern = "searchalbum.php", body = { album = { ALBUM } } } })
local other = release({ credited("lisa", "LiSA", LISA_MBID) })
other.release_title = "LEO-NiNE (Deluxe)"
handlers.refresh_release(other)
assert(next(handlers.saved_ids) == nil, "nothing saved")

-- Lookups reuse recent responses, except for a cover lookup asked to bypass them.
handlers.serve({
	{ pattern = "artist.php?i=122120", body = { artists = { LISA } } },
})
local cover = {
	id = "lisa",
	artist_name = "LiSA",
	external_ids = { ["theaudiodb:artist"] = "122120" },
}
handlers.artist_cover(cover)
assert(handlers.cache_ttls()[1] == api.CACHE_TTL_MS, "a recent response answers")
handlers.serve({
	{ pattern = "artist.php?i=122120", body = { artists = { LISA } } },
})
cover.cover_options = { bypass_cache = true }
handlers.artist_cover(cover)
assert(handlers.cache_ttls()[1] == 0, "a bypassing lookup asks again")
