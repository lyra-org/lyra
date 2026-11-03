--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")
local crypt = require("@harmony/crypt")
local favorites = require("@lyra/favorites")
local listens = require("@lyra/listens")
local ratings = require("@lyra/ratings")
local tracks = require("@lyra/tracks")

local request = require("../protocol/request")
local response = require("../protocol/response")

type ApiQueryParams = api.ApiQueryParams
type ApiResponse = api.ApiResponse
type Request = request.Request
type SubmittedPlay = listens.SubmittedPlay

local module = {}

-- Every id `star` and `unstar` take: Lyra ids name one item of any kind, so `id`, `albumId` and
-- `artistId` are alike.
local function annotated_ids(params: ApiQueryParams): { string }
	local ids: { string } = {}
	for _, name in { "id", "albumId", "artistId" } do
		for _, id in request.values(params, name) do
			table.insert(ids, id)
		end
	end
	if #ids == 0 then
		return request.fail(10, "Required parameter is missing: id")
	end
	return ids
end

-- Applies `change` to every id, skipping those the caller can't see.
local function annotate(params: ApiQueryParams, change: (id: string) -> boolean): ApiResponse
	for _, id in annotated_ids(params) do
		change(id)
	end
	return response.ok(params)
end

-- The largest time a Luau number holds exactly, well within the host's timestamps.
local MAX_TIME_MS = 2 ^ 53 - 1

-- A play reported with its start time keeps one id however often it is retried, `occurrence`
-- telling apart the same song and time repeated in one request; one without gets a fresh id each
-- time.
local function play_id(track_id: string, started_at_ms: number?, occurrence: number): string
	if started_at_ms then
		local key = `{track_id}@{started_at_ms}#{occurrence}`
		return "subsonic-" .. string.sub(crypt.hash("sha256", key), 1, 48)
	end
	return "subsonic-" .. crypt.random(24)
end

local function submit(plays: { SubmittedPlay })
	for first = 1, #plays, listens.MAX_PLAYS do
		listens.submit(table.move(plays, first, math.min(first + listens.MAX_PLAYS - 1, #plays), 1, {}))
	end
end

module.methods = {
	star = function(req: Request): ApiResponse
		return annotate(req.params, favorites.add)
	end,

	unstar = function(req: Request): ApiResponse
		return annotate(req.params, favorites.remove)
	end,

	setRating = function(req: Request): ApiResponse
		local params = req.params
		local id = request.required(params, "id")
		local rating = request.int(params, "rating")
		if rating == nil then
			return request.fail(10, "Required parameter is missing: rating")
		elseif rating < 0 or rating > 5 then
			return request.fail(0, `Invalid rating: {rating}`)
		end
		local found = if rating == 0 then ratings.remove(id) else ratings.set(id, rating)
		if not found then
			return request.not_found("Item")
		end
		return response.ok(params)
	end,

	-- Each submitted song is a listen the client already judged, played through from the `time` it
	-- was listened to, else ending now. "Now playing" notifications are accepted and dropped.
	scrobble = function(req: Request): ApiResponse
		local params = req.params
		local ids = request.values(params, "id")
		if #ids == 0 then
			return request.fail(10, "Required parameter is missing: id")
		end
		local times = request.ints(params, "time")
		if #times > 0 and #times ~= #ids then
			return request.fail(0, "Wrong number of timestamps")
		end
		for _, time in times do
			if time < 0 or time > MAX_TIME_MS then
				return request.fail(0, `Invalid time: {time}`)
			end
		end
		if request.bool(params, "submission") == false then
			return response.ok(params)
		end

		local found = tracks.get(ids)
		local now_ms = os.time() * 1000
		local plays: { SubmittedPlay } = {}
		local occurrences: { [string]: number } = {}
		for index, id in ids do
			local track = found[id]
			if track then
				local time = times[index]
				local pair = `{id}@{time}`
				local occurrence = (occurrences[pair] or 0) + 1
				occurrences[pair] = occurrence
				-- A client may send the time it scrobbled, at the end of the song, so the play
				-- starts no later than a whole song before now.
				local latest_start_ms = math.max(now_ms - (track.duration_ms or 0), 0)
				local play: SubmittedPlay = {
					play_id = play_id(id, time, occurrence),
					track_id = id,
					started_at_ms = if time then math.min(time, latest_start_ms) else latest_start_ms,
					listened = true,
				}
				table.insert(plays, play)
			end
		end
		submit(plays)
		return response.ok(params)
	end,
}

return module
