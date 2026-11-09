--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

-- A user's data of an item as upstream's `UserItemDataDto`, and upstream's `UserDataChanged`
-- message, which tells every socket of a user that the user's data of an item changed.

local api = require("@lyra/api")
local bookmarks = require("@lyra/bookmarks")
local errors = require("@lyra/errors")
local favorites = require("@lyra/favorites")
local listens = require("@lyra/listens")
local ratings = require("@lyra/ratings")

local task = require("@harmony/task")

local active_sessions = require("./active_sessions")
local catalog = require("./items/catalog")
local item_ids = require("./items/identifiers")
local listing = require("./items/listing")
local requests = require("./items/requests")
local user_data = require("./items/user_data")
local websocket = require("./websocket")

type ItemInfo = catalog.ItemInfo
type JsonObject = { [string]: api.JsonValue }

local module = {}

local function user_data_key(item: ItemInfo, item_hex_id: string?): string?
	local item_name = requests.trim(item.Name)
	local item_type = requests.trim(item.Type)
	if item_type == "MusicArtist" then
		return `Artist-{item_name or ""}`
	elseif item_type == "MusicAlbum" then
		return `{requests.trim(item.AlbumArtist) or ""}-{item_name or ""}`
	elseif item_type == "Audio" then
		return `{item.ParentIndexNumber or 0}-{item.IndexNumber or 0}{item_name or ""}`
	elseif item_type == "Playlist" then
		return `Playlist-{item_name or ""}`
	elseif item_type == "MusicGenre" then
		return `MusicGenre-{item_name or ""}`
	end
	return item_hex_id
end

-- The user's `UserItemDataDto` of the item, read afresh.
function module.user_data_json(item_id: string, item: ItemInfo, user_id: string): JsonObject
	local item_hex_id = item_ids.encode(item_id)
	local is_track = requests.trim(item.Type) == "Audio"
	local last_played: { [string]: number } = {}
	if is_track then
		local stats = listens.get({ item_id })[item_id]
		if stats and stats.last_played_at_ms ~= nil then
			last_played[item_id] = stats.last_played_at_ms
		end
	end
	return user_data.user_data_to_json(user_data.get_user_data({
		item_id = item_id,
		user_id = user_id,
		item_hex_id = item_hex_id,
		key = user_data_key(item, item_hex_id),
		include_favorite = true,
		include_play_count = is_track,
		include_position = is_track,
		include_rating = true,
		last_played_lookup = last_played,
		run_time_ticks = item.RunTimeTicks,
	}))
end

-- As upstream, which gathers the changes of half a second into one message.
local WINDOW_SECONDS = 0.5

-- The most `UserItemDataDto`s one message carries, so a burst of changes, such as a submitted
-- batch of listens, is read a part at a time rather than holding the plugin runtime for all of it.
-- The rest follow in the next windows.
local MAX_MESSAGE_ITEMS = 50
-- Past this many changed items waiting on a socket, a few windows' worth, further ones are dropped.
local MAX_PENDING_ITEMS = 250

-- Notes that the user's data of the item changed, for each of the user's sockets to report.
local function changed(user_id: string, item_id: string)
	for _, entry in active_sessions.entries do
		local pending = entry.changed_items
		if
			entry.user_id == user_id
			and not entry.evicted
			and not entry.sender:is_closed()
			and not pending.queued[item_id]
			and #pending.queue < MAX_PENDING_ITEMS
		then
			pending.queued[item_id] = true
			table.insert(pending.queue, item_id)
		end
	end
end

-- Listens for the changes Lyra announces to a user's data of an item, wherever they were made:
-- through this plugin's routes, Lyra's own API or another plugin. Playback progress records
-- nothing, so as upstream it reports nothing; a play that counts as a listen does.
function module.subscribe()
	favorites.on_update(function(update: favorites.FavoriteUpdatePayload)
		changed(update.user_id, update.target_id)
	end)
	ratings.on_update(function(update: ratings.RatingUpdatePayload)
		changed(update.user_id, update.target_id)
	end)
	bookmarks.on_update(function(update: bookmarks.BookmarkUpdatePayload)
		changed(update.user_id, update.track_id)
	end)
	listens.on_update(function(update: listens.ListenUpdatePayload)
		for _, listen in update.listens do
			changed(update.user_id, listen.track_id)
		end
	end)
end

type Dto = { item_id: string, json: JsonObject }

-- Whether the socket's user no longer verifies, which closes the socket.
local function lost_user(entry: active_sessions.SessionEntry, err: any): boolean
	if errors.kind(err) ~= "unauthenticated" then
		return false
	end
	entry.sender:close()
	return true
end

-- The user's data of the item and, as upstream goes up one level for the indicators a parent
-- shows, of a track's album, leaving out what `listed` holds and what the user can't see. An album
-- that fails to read is left out alone. Raises when the item fails to read, or the user no longer
-- verifies.
local function dtos_of(item_id: string, user_id: string, listed: { [string]: boolean }): { Dto }
	local dtos: { Dto } = {}
	if listed[item_id] then
		return dtos
	end
	local item = listing.find_item_by_id(item_id, user_id)
	if not item then
		return dtos
	end
	table.insert(dtos, { item_id = item_id, json = module.user_data_json(item_id, item, user_id) })

	local album_id = if requests.trim(item.Type) == "Audio" then item_ids.decode(item.AlbumId) else nil
	if album_id and not listed[album_id] then
		local ok, album_json = pcall(function(): JsonObject?
			local album = listing.find_item_by_id(album_id, user_id)
			return if album then module.user_data_json(album_id, album, user_id) else nil
		end)
		if ok and album_json then
			table.insert(dtos, { item_id = album_id, json = album_json })
		elseif not ok then
			if errors.kind(album_json) == "unauthenticated" then
				error(album_json)
			end
			warn(`could not read user data of album {album_id} (user_id={user_id}): {album_json}`)
		end
	end
	return dtos
end

-- Sends the socket upstream's `UserDataChanged` for the items waiting on it, the oldest first and
-- each once, up to `MAX_MESSAGE_ITEMS`; the rest keep waiting. An item that fails to read is
-- dropped alone. Must run in the socket's own dispatch: the handlers that note a change act for
-- no user, and the data is the socket's user's to read.
function module.flush(entry: active_sessions.SessionEntry)
	local pending = entry.changed_items
	local queue = pending.queue
	if #queue == 0 then
		return
	end

	local user_id = entry.user_id
	local list: { api.JsonValue } = {}
	local listed: { [string]: boolean } = {}
	local taken = 0
	for _, item_id in queue do
		local ok, result = pcall(dtos_of, item_id, user_id, listed)
		local dtos: { Dto } = {}
		if ok then
			dtos = result :: { Dto }
		elseif lost_user(entry, result) then
			return
		else
			warn(`could not read user data of item {item_id} (user_id={user_id}): {result}`)
		end
		if #list > 0 and #list + #dtos > MAX_MESSAGE_ITEMS then
			break
		end
		taken += 1
		pending.queued[item_id] = nil
		for _, dto in dtos do
			listed[dto.item_id] = true
			table.insert(list, dto.json :: api.JsonValue)
		end
	end
	pending.queue = table.move(queue, taken + 1, #queue, 1, {})

	if #list == 0 then
		return
	end
	local message = websocket.encode("UserDataChanged", {
		UserId = item_ids.encode(user_id),
		UserDataList = list,
	}, nil)
	local ok, err = pcall(entry.sender.send, entry.sender, message)
	if not ok and not lost_user(entry, err) then
		warn(`could not send UserDataChanged (user_id={user_id}): {err}`)
	end
end

-- Reports the socket's changed items once a window, upstream's unless `window_seconds` gives
-- another, until it closes. Must be spawned from the socket's own dispatch, as `flush` must run
-- there.
function module.watch(entry: active_sessions.SessionEntry, window_seconds: number?): thread
	return task.spawn(function()
		while not entry.sender:is_closed() do
			task.wait(window_seconds or WINDOW_SECONDS)
			if not entry.sender:is_closed() then
				module.flush(entry)
			end
		end
	end)
end

return module
