local artists = require("@lyra/artists")
local genres = require("@lyra/genres")
local releases = require("@lyra/releases")
local tracks = require("@lyra/tracks")

-- The test goes without `lyra.favorites`, `lyra.listens` and `lyra.ratings`.
local FAVORITES = "filtering or sorting by the caller's favorites requires the lyra.favorites scope"
local LISTENS = "filtering or sorting by the caller's listens requires the lyra.listens scope"
local RATINGS = "filtering or sorting by the caller's ratings requires the lyra.ratings scope"

local function rejects(f: (...any) -> ...any, message: string, ...: any)
	local ok, err = pcall(f, ...)
	assert(not ok, `rejected: {message}`)
	assert(string.find(tostring(err), message, 1, true), `error {err}`)
end

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
