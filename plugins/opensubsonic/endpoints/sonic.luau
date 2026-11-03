--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")
local mix = require("@lyra/mix")
local tracks = require("@lyra/tracks")

local request = require("../protocol/request")
local response = require("../protocol/response")
local songs = require("../catalog/songs")

type ApiResponse = api.ApiResponse
type JsonObject = { [string]: any }
type MixedTrack = mix.MixedTrack
type MixHandlerListing = mix.MixHandlerListing
type Request = request.Request

local DEFAULT_SIMILAR_COUNT = 10
local DEFAULT_PATH_COUNT = 25
-- The similarity of a song its mixer gave no score.
local UNSCORED = -1

-- The mixer serving the sonic methods; nil leaves them off.
local mixer_id: string? = nil

local module = {}

function module.configure(id: string?)
	local trimmed = if id then string.match(id, "^%s*(.-)%s*$") else nil
	mixer_id = if trimmed and trimmed ~= "" then trimmed else nil
end

local function takes_tracks(handler: MixHandlerListing?): boolean
	return handler ~= nil and table.find(handler.seeds, "track" :: "track") ~= nil
end

-- Whether the configured mixer is enabled, and blends and walks paths from songs.
function module.available(): boolean
	if not mixer_id then
		return false
	end
	for _, mixer in mix.mixers().mixers do
		if mixer.mixer_id == mixer_id then
			return mixer.enabled and takes_tracks(mixer.blend) and takes_tracks(mixer.path)
		end
	end
	return false
end

local function require_songs(ids: { string })
	local found = tracks.get(ids)
	for _, id in ids do
		if not found[id] then
			request.not_found(`Song {id}`)
		end
	end
end

-- Runs a mix, failing as a missing song when a seed is gone and as unavailable when the mixer
-- fails, such as one turned off since the request began.
local function mixed(name: string, run: () -> { MixedTrack }?): { MixedTrack }
	local ok, result = pcall(run)
	if not ok then
		warn(`{name} mix failed: {tostring(result)}`)
		return request.fail(0, "Sonic similarity is unavailable")
	end
	if not result then
		return request.not_found("Song")
	end
	return result
end

local function matches(found: { MixedTrack }): { JsonObject }
	local track_list: { tracks.Track } = {}
	for index, entry in found do
		track_list[index] = entry.track
	end
	local entries = songs.list(track_list)
	local items: { JsonObject } = {}
	for index, entry in found do
		items[index] = { entry = entries[index], similarity = entry.score or UNSCORED }
	end
	return items
end

-- Most similar first; songs without a score follow in the mixer's order.
local function by_score(found: { MixedTrack }): { MixedTrack }
	local indices: { number } = {}
	for index in found do
		indices[index] = index
	end
	table.sort(indices, function(a: number, b: number): boolean
		local a_score, b_score = found[a].score or UNSCORED, found[b].score or UNSCORED
		if a_score ~= b_score then
			return a_score > b_score
		end
		return a < b
	end)
	local sorted: { MixedTrack } = {}
	for position, index in indices do
		sorted[position] = found[index]
	end
	return sorted
end

local function unavailable(req: Request, name: string): ApiResponse
	return response.unknown_method(req.params, name)
end

module.methods = {
	getSonicSimilarTracks = function(req: Request): ApiResponse
		if not module.available() then
			return unavailable(req, "getSonicSimilarTracks")
		end
		local params = req.params
		local id = request.required(params, "id")
		local count = request.count(params, "count", DEFAULT_SIMILAR_COUNT, mix.MAX_TRACKS)
		require_songs({ id })
		if count == 0 then
			return response.ok(params, { sonicMatch = {} })
		end
		local found = mixed("getSonicSimilarTracks", function()
			return mix.blend({ seeds = { id }, mixer = mixer_id, limit = count })
		end)
		return response.ok(params, { sonicMatch = matches(by_score(found)) })
	end,

	findSonicPath = function(req: Request): ApiResponse
		if not module.available() then
			return unavailable(req, "findSonicPath")
		end
		local params = req.params
		local start_id = request.required(params, "startSongId")
		local end_id = request.required(params, "endSongId")
		local count = request.count(params, "count", DEFAULT_PATH_COUNT, mix.MAX_TRACKS)
		require_songs({ start_id, end_id })
		if count == 0 then
			return response.ok(params, { sonicMatch = {} })
		end
		local found = mixed("findSonicPath", function()
			return mix.path({ seeds = { start_id, end_id }, mixer = mixer_id, limit = count })
		end)
		return response.ok(params, { sonicMatch = matches(found) })
	end,
}

return module
