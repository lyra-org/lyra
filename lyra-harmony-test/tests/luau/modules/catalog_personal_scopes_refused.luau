local artists = require("@lyra/artists")
local genres = require("@lyra/genres")
local releases = require("@lyra/releases")
local tracks = require("@lyra/tracks")

-- The test goes without `lyra.bookmarks`, `lyra.favorites`, `lyra.listens`, `lyra.ratings` and
-- `lyra.tags`.
local BOOKMARKS = "reading the caller's bookmarks requires the lyra.bookmarks scope"
local FAVORITES = "reading the caller's favorites requires the lyra.favorites scope"
local LISTENS = "reading the caller's listens requires the lyra.listens scope"
local RATINGS = "reading the caller's ratings requires the lyra.ratings scope"
local TAGS = "reading the caller's tags requires the lyra.tags scope"

local function rejects(f: (...any) -> ...any, message: string, ...: any)
	local ok, err = pcall(f, ...)
	assert(not ok, `rejected: {message}`)
	assert(string.find(tostring(err), message, 1, true), `error {err}`)
end

rejects(tracks.query, BOOKMARKS, { bookmarked = true })
rejects(tracks.query, BOOKMARKS, { sort = { { key = "bookmarked_at", order = "descending" } } })

rejects(tracks.query, FAVORITES, { favorite = true })
rejects(releases.query, FAVORITES, { favorite = false })
rejects(artists.query, FAVORITES, { favorite = true })
rejects(tracks.query, FAVORITES, { sort = { { key = "favorited_at" } } })

rejects(tracks.query, LISTENS, { listened = false })
rejects(releases.query, LISTENS, { sort = { { key = "last_played_at" } } })
rejects(genres.query, LISTENS, { sort = { { key = "listen_count" } } })

rejects(tracks.query, RATINGS, { min_rating = 1 })
rejects(artists.query, RATINGS, { sort = { { key = "rating" } } })
rejects(tracks.query, RATINGS, { rated = false })
rejects(releases.query, RATINGS, { sort = { { key = "rated_at" } } })

rejects(tracks.query, TAGS, { tags = { "Vinyl" } })
rejects(releases.query, TAGS, { tags = { "Vinyl" } })
rejects(artists.query, TAGS, { tags = { "Vinyl" } })
rejects(tracks.tags, TAGS, {})
rejects(releases.tags, TAGS, {})
rejects(artists.tags, TAGS, {})

-- Nested and facet matches read what their filters read.
rejects(genres.query, FAVORITES, { tracks = { favorite = true } })
rejects(genres.query, RATINGS, { releases = { min_rating = 3 } })
rejects(genres.query, BOOKMARKS, { tracks = { bookmarked = false } })
rejects(genres.query, TAGS, { releases = { tags = { "Vinyl" } } })
rejects(genres.query, TAGS, { tracks = { tags = { "Vinyl" } } })
rejects(tracks.years, LISTENS, { listened = true })
rejects(releases.years, FAVORITES, { favorite = true })

local playlists = require("@lyra/playlists")
rejects(playlists.query, FAVORITES, { favorite = true })
rejects(playlists.query, FAVORITES, { sort = { { key = "favorited_at", order = "descending" } } })
