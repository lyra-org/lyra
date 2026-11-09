local fixture = ...

local bookmarks = require("@lyra/bookmarks")
local favorites = require("@lyra/favorites")
local ratings = require("@lyra/ratings")
local task = require("@harmony/task")

-- Every change to a favorite, rating or bookmark reaches its module's `on_update` once, in order,
-- and a call that changes nothing reaches none.

local ids = fixture.ids
local user = fixture.users.listener

local seen: { string } = {}
favorites.on_update(function(update: favorites.FavoriteUpdatePayload)
	assert(update.user_id == user, `favorite of {update.user_id}`)
	table.insert(seen, `favorite {update.event} {update.target_type} {update.target_id}`)
end)
ratings.on_update(function(update: ratings.RatingUpdatePayload)
	assert(update.user_id == user, `rating of {update.user_id}`)
	table.insert(seen, `rating {update.event} {update.target_type} {update.target_id} {update.rating}`)
end)
bookmarks.on_update(function(update: bookmarks.BookmarkUpdatePayload)
	assert(update.user_id == user, `bookmark of {update.user_id}`)
	table.insert(seen, `bookmark {update.event} {update.track_id} {update.position_ms}`)
end)

for _, on_update in { favorites.on_update, ratings.on_update, bookmarks.on_update } do
	assert(not pcall(on_update :: any, "not a function"), "on_update takes a function")
end

-- Nothing to announce: targets the caller can't see, and nothing to remove.
assert(not favorites.add(ids.hidden))
assert(not ratings.set(ids.hidden, 3))
assert(not bookmarks.set(ids.hidden, 1000))
assert(favorites.remove(ids.song))
assert(ratings.remove(ids.song))
assert(bookmarks.remove(ids.song))

assert(favorites.add(ids.song))
assert(favorites.add(ids.song), "favoriting again refreshes the favorite")
assert(favorites.add(ids.mix))
assert(favorites.remove(ids.song))
assert(ratings.set(ids.amy, 2))
assert(ratings.set(ids.amy, 5))
assert(ratings.set(ids.album, 1))
assert(ratings.remove(ids.amy))
assert(bookmarks.set(ids.song, 1000, { comment = "Chorus" }))
assert(bookmarks.set(ids.song, 90000), "a position past the end is stored as the duration")
assert(bookmarks.remove(ids.song))

task.wait(0.1)
local expected = {
	`favorite added track {ids.song}`,
	`favorite added track {ids.song}`,
	`favorite added playlist {ids.mix}`,
	`favorite removed track {ids.song}`,
	`rating set artist {ids.amy} 2`,
	`rating set artist {ids.amy} 5`,
	`rating set release {ids.album} 1`,
	`rating removed artist {ids.amy} nil`,
	`bookmark set {ids.song} 1000`,
	`bookmark set {ids.song} 60000`,
	`bookmark removed {ids.song} nil`,
}
local got = table.concat(seen, "\n")
assert(got == table.concat(expected, "\n"), `updates:\n{got}`)
