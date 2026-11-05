--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")
local bookmarks = require("@lyra/bookmarks")
local playback_sessions = require("@lyra/playback_sessions")
local tracks = require("@lyra/tracks")

local dates = require("../protocol/dates")
local request = require("../protocol/request")
local response = require("../protocol/response")
local songs = require("../catalog/songs")

type ApiResponse = api.ApiResponse
type JsonObject = { [string]: any }
type Request = request.Request

-- How many songs before the current one a queue too long to save keeps.
local KEPT_BEFORE_CURRENT = 100

-- Stands in for the change time of a queue never saved, as `changed` is required.
local NEVER_CHANGED = "1970-01-01T00:00:00Z"

local module = {}

-- The caller's newest queue from any client, Lyra's own included, of the songs they can still see,
-- and the 1-based index of its current song among them, nil without songs. A song gone from view
-- hands over to the next one, from its start, or to the last one when none follows. Without a queue,
-- an empty one, as the queue is required.
local function latest_queue(req: Request): (JsonObject, number?)
	local username = req.auth.principal.username
	local latest = playback_sessions.latest_queue()
	if not latest then
		return { username = username, changed = NEVER_CHANGED, changedBy = "" }, nil
	end
	local found = tracks.get(latest.track_ids)
	local entries: { tracks.Track } = {}
	local current: number? = nil
	local position = latest.position_ms
	for index, id in latest.track_ids do
		local track = found[id]
		if track and track.available then
			table.insert(entries, track)
			if current == nil and index > latest.current_index then
				current = #entries
				if index ~= latest.current_index + 1 then
					position = 0
				end
			end
		end
	end
	if current == nil and #entries > 0 then
		current = #entries
		position = 0
	end
	local queue: JsonObject = {
		username = username,
		changed = dates.iso8601(latest.updated_at_ms) or NEVER_CHANGED,
		changedBy = latest.client_name or "",
		position = position,
		entry = songs.list(entries),
	}
	return queue, current
end

-- Saves the calling client's queue, the song `current_of` picks among its songs current, or deletes
-- it without any `id` parameter, when `current_of` picks none. A queue longer than Lyra keeps is
-- cut around the current song.
local function save_queue(req: Request, current_of: (ids: { string }) -> number?): ApiResponse
	local params = req.params
	local client = request.param(params, "c") or ""
	-- Only a request without any `id` empties the queue; an empty `id` names no song.
	local ids = params.id or {}
	if table.find(ids, "") then
		return request.not_found("Song ''")
	end
	local picked = current_of(ids)
	if #ids == 0 then
		playback_sessions.remove_queue(client)
		return response.ok(params)
	end
	local current = picked :: number
	local position_ms = math.max(request.int(params, "position") or 0, 0)

	local max = playback_sessions.MAX_QUEUE_TRACK_IDS
	if #ids > max then
		local first = math.clamp(current - KEPT_BEFORE_CURRENT, 1, #ids - max + 1)
		ids = table.move(ids, first, first + max - 1, 1, {})
		current -= first - 1
	end

	local saved, err = pcall(playback_sessions.save_queue, {
		client_name = client,
		track_ids = ids,
		current_index = current - 1,
		position_ms = position_ms,
	})
	if not saved then
		local found = tracks.get(ids)
		for _, id in ids do
			if not found[id] then
				return request.not_found(`Song {id}`)
			end
		end
		error(err, 0)
	end
	return response.ok(params)
end

-- Every bookmark of the caller's, of the songs they can see, most recently changed first.
local function bookmark_list(req: Request): { JsonObject }
	local found = tracks.query({
		bookmarked = true,
		available = true,
		sort = { { key = "bookmarked_at", order = "descending" } },
	}).items
	if #found == 0 then
		return {}
	end
	local ids: { string } = {}
	for _, track in found do
		table.insert(ids, track.id)
	end
	local saved = bookmarks.get(ids)
	local items: { JsonObject } = {}
	for index, entry in songs.list(found, saved) do
		local bookmark = saved[ids[index]]
		if bookmark then
			local item: JsonObject = {
				entry = entry,
				position = bookmark.position_ms,
				username = req.auth.principal.username,
				comment = bookmark.comment,
				created = dates.iso8601(bookmark.created_at_ms),
				changed = dates.iso8601(bookmark.updated_at_ms),
			}
			table.insert(items, item)
		end
	end
	return items
end

module.methods = {
	-- A position past the song's end is kept as its end.
	createBookmark = function(req: Request): ApiResponse
		local params = req.params
		local id = request.required(params, "id")
		local position = request.int(params, "position")
		if position == nil then
			return request.fail(10, "Required parameter is missing: position")
		elseif position < 0 then
			return request.fail(0, `Invalid position: {position}`)
		end
		local comment = request.param(params, "comment")
		-- Counts the bytes that start a UTF-8 character.
		local _, characters = string.gsub(comment or "", "[^\128-\191]", "")
		if characters > bookmarks.MAX_COMMENT_CHARS then
			return request.fail(0, `The comment must be at most {bookmarks.MAX_COMMENT_CHARS} characters`)
		end
		if not bookmarks.set(id, position, { comment = comment }) then
			return request.not_found(`Song {id}`)
		end
		return response.ok(params)
	end,

	deleteBookmark = function(req: Request): ApiResponse
		local params = req.params
		local id = request.required(params, "id")
		if not bookmarks.remove(id) then
			return request.not_found(`Song {id}`)
		end
		return response.ok(params)
	end,

	getBookmarks = function(req: Request): ApiResponse
		local items = bookmark_list(req)
		return response.ok(
			req.params,
			{ bookmarks = if #items > 0 then { bookmark = items } else response.EMPTY_OBJECT }
		)
	end,

	getPlayQueue = function(req: Request): ApiResponse
		local queue, current = latest_queue(req)
		if current then
			queue.current = queue.entry[current].id
		end
		return response.ok(req.params, { playQueue = queue })
	end,

	-- The same queue as `getPlayQueue`, its current song by its 0-based index.
	getPlayQueueByIndex = function(req: Request): ApiResponse
		local queue, current = latest_queue(req)
		if current then
			queue.currentIndex = current - 1
		end
		return response.ok(req.params, { playQueueByIndex = queue })
	end,

	-- `current` names the first of the queue's songs it repeats.
	savePlayQueue = function(req: Request): ApiResponse
		return save_queue(req, function(ids: { string }): number?
			if #ids == 0 then
				return nil
			end
			local current = table.find(ids, request.required(req.params, "current"))
			if not current then
				return request.fail(0, "The current song must be one of the queue's songs")
			end
			return current
		end)
	end,

	-- `currentIndex` is the 0-based index of the current song, which tells repeats apart. It must be
	-- absent to delete the queue, and fails with 10 when it isn't an index of the queue's songs.
	savePlayQueueByIndex = function(req: Request): ApiResponse
		return save_queue(req, function(ids: { string }): number?
			local raw = request.param(req.params, "currentIndex")
			if raw == nil then
				if #ids == 0 then
					return nil
				end
				return request.fail(10, "Required parameter is missing: currentIndex")
			end
			local index = if string.match(raw, "^%d+$") then tonumber(raw) else nil
			if index == nil or index >= #ids then
				if #ids == 0 then
					return request.fail(10, "currentIndex must not be set without songs")
				end
				return request.fail(10, `currentIndex must be an integer from 0 to {#ids - 1}`)
			end
			return index + 1
		end)
	end,
}

return module
