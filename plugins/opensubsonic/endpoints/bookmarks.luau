--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")
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

local module = {}

module.methods = {
	-- The caller's newest queue from any client, Lyra's own included, of the songs they can still
	-- see, or none. A song gone from view hands `current` to the next one, from its start.
	getPlayQueue = function(req: Request): ApiResponse
		local latest = playback_sessions.latest_queue()
		if not latest then
			return response.ok(req.params)
		end
		local found = tracks.get(latest.track_ids)
		local entries: { any } = {}
		for _, id in latest.track_ids do
			local track = found[id]
			if track then
				table.insert(entries, track)
			end
		end
		local queue: JsonObject = {
			username = req.auth.principal.username,
			changed = dates.iso8601(latest.updated_at_ms),
			changedBy = latest.client_name or "",
			current = latest.track_ids[latest.current_index + 1],
			position = latest.position_ms,
			entry = songs.list(entries),
		}
		return response.ok(req.params, { playQueue = queue })
	end,

	-- Saves the queue of the calling client, or deletes it without any `id` parameter. `current` names the
	-- first of its songs it repeats. A queue longer than Lyra keeps is cut around `current`.
	savePlayQueue = function(req: Request): ApiResponse
		local params = req.params
		local client = request.param(params, "c") or ""
		-- Only a request without any `id` empties the queue; an empty `id` names no song.
		local ids = params.id or {}
		if #ids == 0 then
			playback_sessions.remove_queue(client)
			return response.ok(params)
		end
		if table.find(ids, "") then
			return request.not_found("Song ''")
		end
		local current = table.find(ids, request.required(params, "current"))
		if not current then
			return request.fail(0, "The current song must be one of the queue's songs")
		end
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
	end,
}

return module
