local fixture, harness = ...
local favorites = require("@lyra/favorites")
local listens = require("@lyra/listens")
local ratings = require("@lyra/ratings")
local serde = require("@harmony/serde")
local task = require("@harmony/task")

local active_sessions = require("../../protocol/active_sessions")
local item_ids = require("../../protocol/items/identifiers")
local listing = require("../../protocol/items/listing")
local user_data = require("../../routes/user_data")
local user_data_changes = require("../../protocol/user_data_changes")

-- As upstream, every socket of a user is told with `UserDataChanged` when the user's data of an
-- item changes, the socket of the client that changed it among them. The change may come through
-- this plugin's routes or from anywhere else in Lyra, and is told once either way.

local ids = fixture.ids
local AUTHORIZATION = `MediaBrowser Token="{fixture.api_keys.phone}"`

local function hex(id: string): string
	return item_ids.encode(id) :: string
end

-- A socket of the user, which records the messages it is sent.
local function socket(session_id: string, user_id: string, closed: boolean): (any, { any })
	local inbox: { any } = {}
	local entry: any = {
		session_id = session_id,
		user_id = user_id,
		evicted = false,
		changed_items = { queue = {}, queued = {} },
		sender = {
			is_closed = function()
				return closed
			end,
			send = function(_, message: string)
				table.insert(inbox, serde.json.decode(message))
			end,
		},
	}
	active_sessions.entries[session_id] = entry
	return entry, inbox
end

local phone_entry, phone = socket("phone", fixture.users.listener, false)
local laptop_entry, laptop = socket("laptop", fixture.users.listener, false)
local gone_entry, gone = socket("gone", fixture.users.listener, true)
local stranger_entry, stranger = socket("stranger", fixture.users.other, false)

user_data_changes.subscribe()

local function context(key: string, request: { query: any?, json: any? }?, authorization: string?): any
	local query: { [string]: { string } } = {}
	for name, value in (if request then request.query else nil) or {} do
		query[name] = { value }
	end
	return {
		plugin_id = "jellyfin",
		request = {
			method = "POST",
			path = "/UserItems",
			headers = { authorization = authorization or AUTHORIZATION },
			query = query,
			body_raw = "",
			json = if request then request.json else nil,
		},
		params = { itemId = hex(ids[key]) },
	}
end

local function call(handler: (any) -> any, key: string, request: { query: any?, json: any? }?): any
	return handler(context(key, request))
end

local function waiting(entry: any): string
	return table.concat(entry.changed_items.queue, ",")
end

-- Lets the changes made so far reach the plugin, then has each socket report what waits on it, as
-- its own thread does once a window.
local function report()
	harness.task:wait(0.1)
	for _, entry in { phone_entry, laptop_entry, gone_entry, stranger_entry } do
		user_data_changes.flush(entry)
	end
end

-- The one message the changes sent, which every open socket of the user got and no other.
local function pushed(why: string): any
	report()
	assert(#phone == 1, `{why}: the phone got {#phone} messages`)
	assert(#laptop == 1, `{why}: the laptop got {#laptop} messages`)
	assert(#gone == 0, `{why}: a closed socket is sent nothing`)
	assert(#stranger == 0, `{why}: another user's socket is sent nothing`)
	local message = table.remove(phone)
	table.remove(laptop)
	assert(message.MessageType == "UserDataChanged", `{why}: {message.MessageType}`)
	assert(type(message.MessageId) == "string", `{why}: the message has an id`)
	assert(message.Data.UserId == hex(fixture.users.listener), `{why}: user {message.Data.UserId}`)
	return message.Data.UserDataList
end

local function quiet(why: string)
	report()
	assert(#phone == 0 and #laptop == 0 and #gone == 0 and #stranger == 0, `{why}: nothing is sent`)
end

-- A change through the plugin's own route is told once. A track's message carries the track,
-- then its album.
local response = call(user_data.handle_favorite, "song")
assert(response.status == 200, `favoriting: {response.status}`)
local list = pushed("favoriting a track")
assert(#list == 2, `the track and its album, got {#list}`)
assert(list[1].ItemId == hex(ids.song) and list[1].IsFavorite == true, "the track, now a favorite")
assert(list[1].Key ~= nil and list[1].PlayCount == 0 and list[1].Played == false, "as a UserItemDataDto")
assert(list[1].PlaybackPositionTicks == 0, "with its position")
assert(list[2].ItemId == hex(ids.album) and list[2].IsFavorite == false, "then its album")
quiet("the change is told once")

assert(call(user_data.handle_unfavorite, "song").status == 200)
assert(pushed("unfavoriting")[1].IsFavorite == false, "no longer a favorite")

-- Other items go alone.
assert(call(user_data.handle_favorite, "album").status == 200)
list = pushed("favoriting an album")
assert(#list == 1 and list[1].ItemId == hex(ids.album) and list[1].IsFavorite == true, "the album alone")
assert(call(user_data.handle_favorite, "mix").status == 200)
list = pushed("favoriting a playlist")
assert(#list == 1 and list[1].ItemId == hex(ids.mix), "the playlist alone")

-- Ratings and explicit updates tell too.
assert(call(user_data.handle_rate, "song", { query = { likes = "true" } }).status == 200)
list = pushed("rating")
assert(list[1].Rating == 10 and list[1].Likes == true, `rated {tostring(list[1].Rating)}`)
assert(call(user_data.handle_unrate, "song").status == 200)
assert(pushed("clearing a rating")[1].Rating == nil, "the rating is cleared")
assert(call(user_data.handle_post, "song", { json = { PlaybackPositionTicks = 500000000 } }).status == 200)
assert(pushed("updating user data")[1].PlaybackPositionTicks == 500000000, "the position is set")

-- Reading, refused changes and changes that change nothing tell no one.
assert(call(user_data.handle_get, "song").status == 200)
assert(call(user_data.handle_post, "song", { json = { IsFavorite = "yes" } }).status == 400)
assert(call(user_data.handle_rate, "mix", { query = { likes = "true" } }).status == 400, "playlists take no rating")
assert(call(user_data.handle_unfavorite, "song").status == 200, "unfavoriting what is no favorite")
quiet("no change")

-- A change made outside the plugin's routes, as Lyra's own API or another plugin makes it, tells
-- the same. Changes within a window go in one message, each item once.
assert(favorites.add(ids.album))
assert(ratings.set(ids.album, 4))
assert(favorites.add(ids.mix))
list = pushed("changes made elsewhere")
assert(#list == 2, `the album once and the playlist, got {#list}`)
assert(list[1].ItemId == hex(ids.album) and list[1].IsFavorite == true and list[1].Rating == 8, "the album")
assert(list[2].ItemId == hex(ids.mix) and list[2].IsFavorite == true, "the playlist")

-- A play that counts as a listen tells of the track's new play count, with its album.
local outcomes = listens.submit({
	{ play_id = "user-data-changed-play-1", track_id = ids.song, started_at_ms = 1700000000000, listened = true },
})
assert(outcomes["user-data-changed-play-1"] == "recorded", "the listen is recorded")
list = pushed("a listen")
assert(#list == 2 and list[1].ItemId == hex(ids.song), "the track and its album")
assert(list[1].PlayCount == 1 and list[1].Played == true, `played {list[1].PlayCount} times`)
assert(list[1].LastPlayedDate ~= nil, "with when it last played")

-- Another user's change waits on that user's socket alone. The test acts for the listener, so the
-- stranger's socket can't report here as its own thread would; what waits on it is checked instead.
local theirs = harness.plugin:call(
	user_data.handle_favorite,
	context("song", nil, `MediaBrowser Token="{fixture.api_keys.tablet}"`)
)
assert(theirs.status == 200, `the other user favoriting: {theirs.status}`)
harness.task:wait(0.1)
assert(waiting(stranger_entry) == ids.song, `the stranger's socket waits on the track, got "{waiting(stranger_entry)}"`)
assert(waiting(phone_entry) == "" and waiting(laptop_entry) == "", "the listener's sockets wait on nothing")
assert(waiting(gone_entry) == "", "a closed socket waits on nothing")
stranger_entry.changed_items = { queue = {}, queued = {} }
quiet("another user's change")

-- A socket the cap closed, whose sender still reads as open, waits on nothing either.
laptop_entry.evicted = true
assert(favorites.add(ids.mix))
harness.task:wait(0.1)
assert(waiting(laptop_entry) == "", "an evicted socket waits on nothing")
laptop_entry.evicted = false
user_data_changes.flush(phone_entry)
assert(#table.remove(phone).Data.UserDataList == 1, "the open socket is told")

-- A change to an item the user can no longer see is left out: removing the favorite of a track in
-- a library since closed to the listener is announced, and reported to no client.
assert(not favorites.remove(ids.secret), "the hidden track answers as untargetable")
harness.task:wait(0.1)
assert(waiting(phone_entry) == ids.secret, "the change waits on the socket")
quiet("an item the user can't see")
assert(waiting(phone_entry) == "", "and waits no longer")

-- An item that fails to read is dropped alone: the ones around it are still told. A track whose
-- album fails to read goes without it.
do
	local find_item_by_id = listing.find_item_by_id
	local failing: { [string]: boolean } = { [ids.mix] = true, [ids.bulk_release_02] = true }
	listing.find_item_by_id = function(item_id: string?, ...: any): any
		if item_id and failing[item_id] then
			error(`cannot read {item_id}`)
		end
		return find_item_by_id(item_id, ...)
	end
	assert(ratings.set(ids.bulk_release_01, 3))
	assert(favorites.remove(ids.mix))
	assert(ratings.set(ids.bulk_track_02, 3))
	assert(ratings.set(ids.bulk_release_03, 3))
	list = pushed("changes around one that fails")
	listing.find_item_by_id = find_item_by_id
	local told = {}
	for _, dto in list do
		table.insert(told, dto.ItemId)
	end
	local expected = { hex(ids.bulk_release_01), hex(ids.bulk_track_02), hex(ids.bulk_release_03) }
	assert(table.concat(told, ",") == table.concat(expected, ","), `told of {table.concat(told, ",")}`)
	quiet("the failed item is not tried again")
end

-- A message carries at most fifty items, albums included; the rest follow in the next windows,
-- with nothing told twice.
do
	for index = 1, 26 do
		assert(favorites.add(ids[string.format("bulk_track_%02d", index)]))
	end
	local first = pushed("a burst of changes")
	assert(#first == 50, `the first message carries {#first} items`)
	assert(first[1].ItemId == hex(ids.bulk_track_01), "the oldest change first")
	assert(first[2].ItemId == hex(ids.bulk_release_01), "each track with its album")
	assert(waiting(phone_entry) == ids.bulk_track_26, `the last track still waits, got "{waiting(phone_entry)}"`)
	local second = pushed("the rest of the burst")
	assert(#second == 2 and second[1].ItemId == hex(ids.bulk_track_26), "the rest follow")
	assert(second[2].ItemId == hex(ids.bulk_release_26), "with its album")
	quiet("the burst is told once")
end

-- A socket's own thread reports what waits on it once a window.
local watching = user_data_changes.watch(phone_entry, 0.05)
assert(favorites.add(ids.mix))
harness.task:wait(0.5)
task.cancel(watching)
assert(#phone == 1 and phone[1].Data.UserDataList[1].ItemId == hex(ids.mix), "the socket's thread told it")
assert(phone[1].Data.UserDataList[1].IsFavorite == true, "of the playlist, a favorite again")
