local artists = require("@lyra/artists")
local genres = require("@lyra/genres")
local releases = require("@lyra/releases")
local tracks = require("@lyra/tracks")

-- The test plugin declares `lyra.bookmarks`, `lyra.favorites`, `lyra.listens` and `lyra.ratings`;
-- `catalog_personal_scopes_refused` covers queries refused without them.
assert(#tracks.query({ bookmarked = true }).items == 0, "bookmark filters need lyra.bookmarks")
assert(#tracks.query({ sort = { { key = "bookmarked_at" } } }).items == 1, "bookmark sorts need lyra.bookmarks")
assert(#tracks.query({ favorite = true }).items == 0, "favorite filters need lyra.favorites")
assert(#tracks.query({ listened = false }).items == 1, "listened needs lyra.listens")
assert(#tracks.query({ min_rating = 1 }).items == 0, "a rating filter needs lyra.ratings")
assert(#releases.query({ sort = { { key = "last_played_at" } } }).items == 1, "listen sorts need lyra.listens")
assert(#artists.query({ sort = { { key = "rating" } } }).items == 0, "rating sorts need lyra.ratings")
assert(#genres.query({ sort = { { key = "listen_count" } } }).items == 0, "genre listen sorts need lyra.listens")
