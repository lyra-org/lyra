local fixture = ...
local artists = require("@lyra/artists")
local genres = require("@lyra/genres")
local releases = require("@lyra/releases")
local tracks = require("@lyra/tracks")

local ids = fixture.ids

local function joined(values: { any }): string
	local listed = {}
	for _, value in values do
		table.insert(listed, tostring(value))
	end
	return table.concat(listed, ",")
end

local function check(actual: string, expected: string, why: string)
	assert(actual == expected, `{why}: expected "{expected}", got "{actual}"`)
end

local function titles(page: tracks.TrackPage): string
	local found = {}
	for _, track in page.items do
		table.insert(found, track.track_title)
	end
	return joined(found)
end

-- `name:release_count:track_count` for each genre a query finds.
local function genre_counts(query: genres.GenreQuery): string
	local found = {}
	for _, genre in genres.query(query).items do
		table.insert(found, `{genre.name}:{genre.release_count}:{genre.track_count}`)
	end
	return joined(found)
end

local function rejects(f: (...any) -> ...any, message: string, ...: any)
	local ok, err = pcall(f, ...)
	assert(not ok, `rejected: {message}`)
	assert(string.find(tostring(err), message, 1, true), `error {err}`)
end

-- Years are each item's own, ascending, from what the caller sees: a track without one lends none.
check(joined(tracks.years({})), "2001,2009", "track years")
check(joined(releases.years({})), "1999", "release years")
check(joined(tracks.years({ favorite = true })), "", "the favourite track has no year")
check(joined(tracks.years({ release_ids = { ids.solo } })), "2001", "years of one album")

-- Tags compare case-sensitively, and those on hidden items stay out.
check(joined(tracks.tags({})), "Vinyl,vinyl", "track tags")
check(joined(releases.tags({})), "Vinyl", "release tags")
check(joined(artists.tags({})), "Band", "artist tags")
check(joined(tracks.tags({ favorite = true })), "vinyl", "tags of the favourite track")
check(titles(tracks.query({ tags = { "Vinyl" } })), "S1", "one tag")
check(titles(tracks.query({ tags = { "Vinyl", "vinyl" } })), "S1,S2", "any of several tags")
check(titles(tracks.query({ tags = { "Lost" } })), "", "a tag on hidden items only")
check(titles(tracks.query({ tags = { "Nope" } })), "", "a tag the caller lacks")
check(tostring(#releases.query({ tags = { "Vinyl" } }).items), "1", "a release tag")
check(tostring(#artists.query({ tags = { "Band" } }).items), "1", "an artist tag")
rejects(tracks.query, "invalid tag name", { tags = { " " } })

-- A nested match narrows the genres and what their counts count.
check(genre_counts({ tracks = { favorite = true } }), "jazz:1:1,Rock:1:1", "genres of the favourite track")
check(genre_counts({ tracks = { ids = { ids.s1, ids.l1 } } }), "Blues:1:1,jazz:1:1,Rock:1:1", "genres of tracks")
check(genre_counts({}), "Blues:1:1,jazz:1:2,Rock:1:2", "every genre the caller sees")
check(
	genre_counts({ releases = { artist_ids = { ids.amy }, credit_role = "release" } }),
	"jazz:1:2,Rock:1:2",
	"genres of an artist's visible albums"
)
check(genre_counts({ releases = { ids = { ids.hidden } } }), "", "genres of a hidden album")
check(genre_counts({ tracks = { ids = { "NoSuchTrack" } } }), "", "ids naming nothing match nothing")
check(
	genre_counts({ releases = { ids = { ids.solo, ids.live } }, tracks = { years = { 2009 } } }),
	"Blues:1:1",
	"both nested matches hold"
)

-- Availability applies to the whole query, and a match neither orders nor pages.
rejects(genres.query, "availability applies to the whole query", { tracks = { available = true } })
check(
	genre_counts({ available = true, tracks = { available = true, ids = { ids.l1 } } }),
	"Blues:1:1",
	"a repeated availability"
)
rejects(tracks.years, "a match takes no limit", { limit = 1 })
rejects(genres.query, "a match takes no sort", { releases = { sort = { { key = "name" } } } })
