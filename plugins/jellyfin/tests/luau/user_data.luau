local fixture = ...
local bookmarks = require("@lyra/bookmarks")
local favorites = require("@lyra/favorites")
local listens = require("@lyra/listens")

local item_ids = require("../../protocol/items/identifiers")
local listing = require("../../protocol/items/listing")
local session = require("../../protocol/session")
local user_data = require("../../routes/user_data")

-- A track's PlaybackPositionTicks is the caller's bookmark of it, which only an explicit user data
-- update moves, as upstream: playback reports never keep a position for music.

local ids = fixture.ids
local TICKS_PER_MS = 10000
local AUTHORIZATION = `MediaBrowser Token="{fixture.api_keys.phone}"`

local function hex(id: string): string
	return item_ids.encode_jellyfin_item_id(id) :: string
end

local function context(method: string, params: { [string]: string }, body: any?): any
	return {
		plugin_id = "jellyfin",
		request = {
			method = method,
			path = "/UserItems/UserData",
			headers = { authorization = AUTHORIZATION },
			query = {},
			body_raw = "",
			json = body,
		},
		params = params,
	}
end

local function read(key: string): any
	local response = user_data.handle_get(context("GET", { itemId = hex(ids[key]) }))
	assert(response.status == 200, `reading {key}: {response.status}`)
	return response.body
end

local function update(key: string, body: any?): (number, any)
	local response = user_data.handle_post(context("POST", { itemId = hex(ids[key]) }, body))
	return response.status, response.body
end

local function position_ms(key: string): number?
	local bookmark = bookmarks.get({ ids[key] })[ids[key]]
	return if bookmark then bookmark.position_ms else nil
end

-- Reading: a track without a bookmark is at its start; a bookmarked one at the bookmark, with the
-- share of its run time that is.
local song = read("song")
assert(song.PlaybackPositionTicks == 0, `no bookmark, at {song.PlaybackPositionTicks}`)
assert(song.PlayedPercentage == nil, "no share played at the start")
local saved = read("saved")
assert(saved.PlaybackPositionTicks == 42000 * TICKS_PER_MS, `bookmarked at {saved.PlaybackPositionTicks}`)
assert(saved.PlayedPercentage == 21, `played {saved.PlayedPercentage}`)
assert(saved.ItemId == hex(ids.saved), `item id {saved.ItemId}`)

-- Setting answers with the updated user data, which every read of the item then shows.
local status, body = update("song", { PlaybackPositionTicks = 50000 * TICKS_PER_MS + 1234 })
assert(status == 200, `setting: {status}`)
assert(body.PlaybackPositionTicks == 50000 * TICKS_PER_MS, `set at {body.PlaybackPositionTicks}`)
assert(body.PlayedPercentage == 25, `played {body.PlayedPercentage}`)
assert(body.ItemId == hex(ids.song) and body.Key ~= nil, "the response names the item")
assert(position_ms("song") == 50000, `the bookmark is at {position_ms("song")}`)
assert(read("song").PlaybackPositionTicks == 50000 * TICKS_PER_MS, "user data reads show the position")
local item = listing.find_item_by_id(ids.song, ids.listener) :: any
assert(item.UserData.PlaybackPositionTicks == 50000 * TICKS_PER_MS, "item reads show the position")
assert(item.UserData.PlayedPercentage == 25, `item reads show the share played, {item.UserData.PlayedPercentage}`)

-- Field names match without case; a position past a known run time is kept as its end, and a
-- track without one keeps the position as given.
status = update("song", { playbackPositionTicks = 900000 * TICKS_PER_MS })
assert(status == 200 and position_ms("song") == 200000, `past the end: {position_ms("song")}`)
status = update("untimed", { PlaybackPositionTicks = 900000 * TICKS_PER_MS })
assert(status == 200 and position_ms("untimed") == 900000, `untimed: {position_ms("untimed")}`)
assert(read("untimed").PlayedPercentage == nil, "no share played without a run time")

-- Moving a bookmark keeps the comment another client left on it.
status = update("saved", { PlaybackPositionTicks = 60000 * TICKS_PER_MS })
local moved = bookmarks.get({ ids.saved })[ids.saved]
assert(status == 200 and moved.position_ms == 60000, "the bookmark moved")
assert(moved.comment == "Chorus", `the comment stays, got {moved.comment}`)

-- Zero clears the bookmark, as does a position under a millisecond; a body without the field
-- leaves it be.
status, body = update("song", { PlaybackPositionTicks = 0 })
assert(status == 200 and body.PlaybackPositionTicks == 0, `clearing: {status}`)
assert(position_ms("song") == nil, "the bookmark is gone")
assert(read("song").PlaybackPositionTicks == 0, "reads show the start")
status = update("untimed", { PlaybackPositionTicks = TICKS_PER_MS - 1 })
assert(status == 200 and position_ms("untimed") == nil, "under a millisecond clears")
status = update("saved", {})
assert(status == 200 and position_ms("saved") == 60000, "an empty body changes nothing")

-- IsFavorite goes through the caller's favorites.
status, body = update("song", { IsFavorite = true })
assert(status == 200 and body.IsFavorite == true, `favoriting: {status}`)
assert(favorites.get({ ids.song })[ids.song] ~= nil, "the track is a favorite")
status, body = update("song", { IsFavorite = false })
assert(status == 200 and body.IsFavorite == false, `unfavoriting: {status}`)
assert(favorites.get({ ids.song })[ids.song] == nil, "the track is no favorite")

-- Lyra's listens decide Played, PlayCount and LastPlayedDate; a body's are ignored.
local last_played = read("song").LastPlayedDate
assert(type(last_played) == "string", "the played track has a last play date")
status, body = update("song", { Played = false, PlayCount = 10, LastPlayedDate = "2020-01-01T00:00:00.0000000Z" })
assert(status == 200, `play state: {status}`)
assert(body.PlayCount == 1 and body.Played == true, `play state kept, {body.PlayCount} {body.Played}`)
assert(body.LastPlayedDate == last_played, `the last play date is kept, {body.LastPlayedDate}`)
local stats = listens.get({ ids.song })[ids.song]
assert(stats and stats.count == 1, "no listen is added or removed")

-- Items other than tracks hold no position; the rest of the update still applies.
status, body = update("album", { PlaybackPositionTicks = 50000 * TICKS_PER_MS, IsFavorite = true })
assert(status == 200 and body.PlaybackPositionTicks == 0 and body.IsFavorite == true, `album: {status}`)

-- A track the caller can't see reads as one that doesn't exist, and changes nothing.
status = update("secret", { PlaybackPositionTicks = 50000 * TICKS_PER_MS })
assert(status == 404, `hidden: {status}`)
assert(bookmarks.get({ ids.secret })[ids.secret] == nil, "no bookmark of a hidden track")
local unknown =
	user_data.handle_post(context("POST", { itemId = string.rep("a", 32) }, { PlaybackPositionTicks = TICKS_PER_MS }))
assert(unknown.status == 404, `unknown: {unknown.status}`)

-- A missing body or an invalid position is a client error.
assert(update("song", nil) == 400, "a missing body")
assert(update("song", { PlaybackPositionTicks = -1 }) == 400, "a negative position")
assert(update("song", { PlaybackPositionTicks = 1.5 }) == 400, "a fractional position")
local favorite_before = favorites.get({ ids.song })[ids.song] ~= nil
assert(
	update("song", { PlaybackPositionTicks = 1e20, IsFavorite = not favorite_before }) == 400,
	"a position past what a bookmark holds"
)
assert((favorites.get({ ids.song })[ids.song] ~= nil) == favorite_before, "a refused update changes nothing")
assert(update("song", { PlaybackPositionTicks = "10" }) == 400, "a string position")
assert(update("song", { IsFavorite = "yes" }) == 400, "a string favorite")

-- The routes answer for the caller only: another user's item user data and favorites are
-- forbidden, as upstream answers them, whether the legacy path or the query names the user.
local OTHER_USER = string.rep("b", 32)
local function expect_forbidden(response: any, label: string)
	assert(response.status == 403, `{label}: {response.status}`)
	assert(response.kind == "text" and response.body == "Error processing request.", `{label}: body {response.body}`)
end
local function by_query(ctx: any): any
	ctx.request.query = { userId = { OTHER_USER } }
	return ctx
end
local song_params = { userId = OTHER_USER, itemId = hex(ids.song) }
expect_forbidden(user_data.handle_get(context("GET", song_params)), "reading another user's")
expect_forbidden(
	user_data.handle_get(by_query(context("GET", { itemId = hex(ids.song) }))),
	"reading another user's by query"
)
expect_forbidden(
	user_data.handle_post(context("POST", song_params, { PlaybackPositionTicks = 0 })),
	"updating another user's"
)
expect_forbidden(
	user_data.handle_post(by_query(context("POST", { itemId = hex(ids.song) }, { PlaybackPositionTicks = 0 }))),
	"updating another user's by query"
)
local favorite_now = favorites.get({ ids.song })[ids.song] ~= nil
expect_forbidden(user_data.handle_favorite(context("POST", song_params)), "favoriting for another user")
expect_forbidden(user_data.handle_unfavorite(context("DELETE", song_params)), "unfavoriting for another user")
expect_forbidden(
	user_data.handle_favorite(by_query(context("POST", { itemId = hex(ids.song) }))),
	"favoriting for another user by query"
)
assert((favorites.get({ ids.song })[ids.song] ~= nil) == favorite_now, "a forbidden favorite changes nothing")

-- The caller's own favorites change, by either route.
local favorited = user_data.handle_favorite(context("POST", { itemId = hex(ids.song) }))
assert(favorited.status == 200 and favorited.body.IsFavorite == true, `favorited: {favorited.status}`)
local unfavorited =
	user_data.handle_unfavorite(context("DELETE", { userId = hex(ids.listener), itemId = hex(ids.song) }))
assert(unfavorited.status == 200 and unfavorited.body.IsFavorite == false, `unfavorited: {unfavorited.status}`)
if favorite_now then
	assert(user_data.handle_favorite(context("POST", { itemId = hex(ids.song) })).status == 200, "favorite restored")
end
local own = user_data.handle_post(
	context("POST", { userId = hex(ids.listener), itemId = hex(ids.song) }, { PlaybackPositionTicks = TICKS_PER_MS })
)
assert(own.status == 200 and position_ms("song") == 1, `own user: {own.status}`)
assert(update("song", { PlaybackPositionTicks = 0 }) == 200, "cleared again")

-- Playback reports, however far they get, keep no position.
local function report(path: string, event: string, ticks: number)
	local response = session.handle_playback_report_route(
		{
			plugin_id = "jellyfin",
			request = {
				method = "POST",
				path = path,
				headers = { authorization = AUTHORIZATION },
				query = {},
				body_raw = "",
				json = { ItemId = hex(ids.song), PlaySessionId = "play", PositionTicks = ticks },
			},
			params = {},
		} :: any,
		event
	)
	assert(response.status == 204, `{event}: {response.status}`)
end
report("/Sessions/Playing", "started", 0)
report("/Sessions/Playing/Progress", "progress", 90000 * TICKS_PER_MS)
assert(position_ms("song") == nil, "progress keeps no bookmark")
report("/Sessions/Playing/Stopped", "stopped", 100000 * TICKS_PER_MS)
assert(position_ms("song") == nil, "a stop keeps no bookmark")
assert(read("song").PlaybackPositionTicks == 0, "reads show the start after playback")
