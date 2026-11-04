--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")
local crypt = require("@harmony/crypt")
local favorites = require("@lyra/favorites")
local listens = require("@lyra/listens")
local playback_sessions = require("@lyra/playback_sessions")
local ratings = require("@lyra/ratings")
local tracks = require("@lyra/tracks")

local request = require("../protocol/request")
local response = require("../protocol/response")

type ApiQueryParams = api.ApiQueryParams
type ApiResponse = api.ApiResponse
type PlaybackEvent = playback_sessions.PlaybackEvent
type PlaybackSessionReportRequest = playback_sessions.PlaybackSessionReportRequest
type PlaybackState = playback_sessions.PlaybackState
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

local PLUGIN_ID = "opensubsonic"

-- The host's playback rates, as multiples of normal speed.
local MIN_RATE = 0.25
local MAX_RATE = 4

-- How each `reportPlayback` state reports to the host. A starting player buffers, so its position
-- holds until it plays.
local REPORTED_STATES: { [string]: { event: PlaybackEvent, state: PlaybackState } } = {
	starting = { event = "started", state = "buffering" },
	playing = { event = "progress", state = "playing" },
	paused = { event = "progress", state = "paused" },
	stopped = { event = "stopped", state = "stopped" },
}

-- Fails the request with the host's reason when it refuses the report.
local function report(playback: PlaybackSessionReportRequest)
	local _, refused = playback_sessions.report_session(playback)
	if refused then
		request.fail(0, refused)
	end
end

-- Whether `err` is the host refusing to change whether a play counts as a listen.
local function changes_counting(err: unknown): boolean
	return string.find(tostring(err), "cannot change during a play", 1, true) ~= nil
end

-- The calling client's player: its client name and User-Agent, so devices running the same app
-- under one name are told apart where their User-Agents differ.
local function player_key(req: Request): string
	local client = request.param(req.params, "c") or ""
	local agent = request.header(req.ctx, "user-agent") or ""
	return `{client}#{string.sub(crypt.hash("sha256", agent), 1, 16)}`
end

-- The calling client's live play of `track_id`, if any.
local function live_play(req: Request, track_id: string): playback_sessions.ActivePlaybackInfo?
	local key = player_key(req)
	for _, playing in playback_sessions.list_playbacks() do
		if playing.plugin_id == PLUGIN_ID and playing.session_key == key and playing.track_id == track_id then
			return playing
		end
	end
	return nil
end

-- A report of the calling client's playback of `track`.
local function playback_of(req: Request, track: tracks.Track): PlaybackSessionReportRequest
	local client = request.param(req.params, "c") or ""
	return {
		session_key = player_key(req),
		client_name = client,
		track_id = track.id,
		duration_ms = track.duration_ms,
	}
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
	-- was listened to, else ending now. A "now playing" notification shows its first song playing,
	-- from its start, counting no listen, unless the client already reports that song's playback.
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
		local found = tracks.get(ids)
		if request.bool(params, "submission") == false then
			for _, id in ids do
				local track = found[id]
				if track then
					local live = live_play(req, id)
					if not (live and live.counts_as_listen) then
						local playback = playback_of(req, track)
						playback.event = "started"
						playback.state = "playing"
						playback.position_ms = 0
						playback.counts_as_listen = false
						report(playback)
					end
					break
				end
			end
			return response.ok(params)
		end

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

	-- Only songs play. Whether a play counts as a listen is set by the report that starts it: a
	-- starting report declaring otherwise stops the play and starts another, and any other report
	-- keeps the play's. A starting report of the song the client already plays, counted alike,
	-- arrived after the client's playing report and is dropped, so the play keeps moving.
	reportPlayback = function(req: Request): ApiResponse
		local params = req.params
		local id = request.required(params, "mediaId")
		local media_type = request.required(params, "mediaType")
		if media_type ~= "song" then
			return request.fail(0, `Unsupported mediaType: {media_type}`)
		end
		local position = request.int(params, "positionMs")
		if position == nil then
			return request.fail(10, "Required parameter is missing: positionMs")
		elseif position < 0 then
			return request.fail(0, `Invalid positionMs: {position}`)
		end
		local state = request.required(params, "state")
		local reported = REPORTED_STATES[state]
		if not reported then
			return request.fail(0, `Invalid state: {state}`)
		end
		local rate = request.number(params, "playbackRate")
		if rate and rate <= 0 then
			return request.fail(0, `Invalid playbackRate: {rate}`)
		end
		local counts_as_listen = request.bool(params, "ignoreScrobble") ~= true
		local track = tracks.get({ id })[id]
		if not track then
			return request.not_found(`Song {id}`)
		end

		if state == "starting" then
			local live = live_play(req, id)
			if live and live.counts_as_listen ~= counts_as_listen then
				local stop = playback_of(req, track)
				stop.event = "stopped"
				stop.state = "stopped"
				stop.position_ms = position
				report(stop)
			elseif live and live.state == "playing" then
				return response.ok(params)
			end
		end

		local playback = playback_of(req, track)
		playback.event = reported.event
		playback.state = reported.state
		playback.position_ms = position
		playback.rate = if rate then math.clamp(rate, MIN_RATE, MAX_RATE) else nil
		playback.counts_as_listen = counts_as_listen
		local ok, err = pcall(report, playback)
		if not ok then
			-- A play the list doesn't show, such as a stopped one this report resumes, keeps its own.
			if not changes_counting(err) then
				error(err, 0)
			end
			playback.counts_as_listen = nil
			report(playback)
		end
		return response.ok(params)
	end,
}

return module
