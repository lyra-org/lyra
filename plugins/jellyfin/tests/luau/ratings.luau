local fixture = ...
local ratings = require("@lyra/ratings")

local item_ids = require("../../protocol/items/identifiers")
local listing = require("../../protocol/items/listing")
local options = require("../../protocol/items/options")
local protocol_user_data = require("../../protocol/items/user_data")
local user_data = require("../../routes/user_data")

-- An item's Rating is twice the caller's Lyra stars; likes and ratings sent in are rounded to stars.

local ids = fixture.ids
local AUTHORIZATION = `MediaBrowser Token="{fixture.api_keys.phone}"`

local function hex(id: string): string
	return item_ids.encode_jellyfin_item_id(id) :: string
end

local function context(method: string, key: string, query: { [string]: string }?, body: any?): any
	local raw_query: { [string]: { string } } = {}
	for name, value in query or {} do
		raw_query[name] = { value }
	end
	return {
		plugin_id = "jellyfin",
		request = {
			method = method,
			path = "/UserItems/Rating",
			headers = { authorization = AUTHORIZATION },
			query = raw_query,
			body_raw = "",
			json = body,
		},
		params = { itemId = hex(ids[key]) },
	}
end

local function stars(key: string): number?
	return ratings.get({ ids[key] })[ids[key]]
end

local function check(response: any, status: number, why: string): any
	assert(response.status == status, `{why}: expected {status}, got {response.status}`)
	return response.body
end

local function rate(key: string, query: { [string]: string }): any
	return user_data.handle_rate(context("POST", key, query))
end

local function update(key: string, body: any): any
	return user_data.handle_post(context("POST", key, nil, body))
end

-- Converting: the nearest star, 0 clears, and a like never rounds down to a dislike.
local function converts(rating: number, ok: boolean, expected: number?)
	local got_ok, got = protocol_user_data.stars_from_rating(rating)
	assert(got_ok == ok and got == expected, `{rating}: got {got_ok}, {got}`)
end
converts(0, true, nil)
converts(1, true, 1)
converts(3, true, 2)
converts(6.4, true, 3)
converts(6.5, true, 4)
converts(10, true, 5)
converts(-1, false, nil)
converts(10.5, false, nil)
converts(0 / 0, false, nil)

-- Reading: twice the stars, with Likes from upstream's 6.5 line; unrated items carry neither.
local liked = check(user_data.handle_get(context("GET", "liked")), 200, "reading")
assert(liked.Rating == 8 and liked.Likes == true, `liked: {liked.Rating}, {liked.Likes}`)
local song = check(user_data.handle_get(context("GET", "song")), 200, "reading")
assert(song.Rating == nil and song.Likes == nil, "unrated")

local listed =
	listing.list_items(options.for_user(options.parse({ ParentId = { hex(ids.album) } } :: any), ids.listener))
local listed_ratings = {}
for _, item in listed do
	listed_ratings[item.Name] = (item.UserData :: any).Rating
end
assert(listed_ratings.Liked == 8 and listed_ratings.Song == nil, "listings carry ratings")

-- `/Rating`: likes, a numeric rating, and no value at all, which clears as upstream.
local body = check(rate("song", { likes = "true" }), 200, "like")
assert(stars("song") == 5 and body.Rating == 10 and body.Likes == true, "a like is 5 stars")
body = check(rate("song", { Likes = "false" }), 200, "dislike")
assert(stars("song") == 1 and body.Rating == 2 and body.Likes == false, "a dislike is 1 star")
check(rate("song", { rating = "7" }), 200, "numeric rating")
assert(stars("song") == 4, `7 is 4 stars, got {stars("song")}`)
check(rate("song", { rating = "0" }), 200, "zero rating")
assert(stars("song") == nil, "0 clears")
check(rate("song", { likes = "true" }), 200, "like")
check(rate("song", {}), 200, "no likes")
assert(stars("song") == nil, "no likes clears")
check(rate("song", { likes = "maybe" }), 400, "invalid likes")
check(rate("song", { rating = "11" }), 400, "rating out of range")
check(rate("song", { rating = "high" }), 400, "rating not a number")
check(rate("mix", { likes = "true" }), 400, "a playlist")

check(rate("album", { likes = "true" }), 200, "an album")
assert(stars("album") == 5, "albums are rated")
check(user_data.handle_unrate(context("DELETE", "album")), 200, "unrating")
assert(stars("album") == nil, "DELETE clears")

-- UserData: Rating wins over Likes, and invalid values change nothing.
check(update("song", { Likes = false, Rating = 10 }), 200, "both")
assert(stars("song") == 5, "Rating wins")
check(update("song", { likes = false }), 200, "Likes")
assert(stars("song") == 1, "a dislike")
check(update("song", { Rating = 6.5 }), 200, "Rating")
assert(stars("song") == 4, "6.5 stays a like")
check(update("song", { Rating = "7" }), 400, "a string Rating")
check(update("song", { Rating = 12 }), 400, "Rating out of range")
check(update("song", { Likes = "yes" }), 400, "a string Likes")
assert(stars("song") == 4, "unchanged by invalid bodies")
check(update("song", { IsFavorite = true }), 200, "no rating fields")
assert(stars("song") == 4, "unchanged without rating fields")
check(update("song", { Rating = 0 }), 200, "zero")
assert(stars("song") == nil, "0 clears")
check(update("mix", { Rating = 10 }), 400, "a playlist")
