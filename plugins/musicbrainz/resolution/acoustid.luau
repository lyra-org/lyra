--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local chromaprint = require("@lyra/chromaprint")
local entries = require("@lyra/entries")
local http = require("@harmony/http")
local json = require("@harmony/json")

local json_helpers = require("./json_helpers")
local release_search = require("./release_search")
local track_helpers = require("./track_helpers")

type JsonValue = json.JsonValue
type JsonObject = { [string]: JsonValue }
type HttpRequestOptions = http.HttpRequestOptions
type ReleaseContext = release_search.ReleaseContext
type LocalTrack = track_helpers.LocalTrack

local as_json_object = json_helpers.as_json_object
local as_json_array = json_helpers.as_json_array

local ACOUSTID_API = "https://api.acoustid.org/v2/lookup"
local ACOUSTID_CLIENT_ID = "hrpiiKRyqd"
local MIN_DURATION_MS = 30000
local MIN_SCORE = 0.7

local module = {}

type AcoustIdResult = {
	track_id: string?,
	recording_ids: { string },
	score: number,
}

local function get_entry_db_id_for_track(track_db_id: number): number?
	local track_entries = entries.get(track_db_id)
	if type(track_entries) ~= "table" then
		return nil
	end
	for _, entry in track_entries do
		if type(entry) == "table" and type(entry.db_id) == "number" then
			return entry.db_id
		end
	end
	return nil
end

function module.select_track(tracks: { LocalTrack }): LocalTrack?
	local best: LocalTrack? = nil
	local best_duration = -1
	for _, track in tracks do
		if track.db_id == nil then
			continue
		end
		local duration = track.duration_ms or 0
		if duration < MIN_DURATION_MS then
			continue
		end
		if duration > best_duration then
			best = track
			best_duration = duration
		end
	end
	if best then
		return best
	end
	-- fallback: pick the longest track with a db_id regardless of minimum
	for _, track in tracks do
		if track.db_id == nil then
			continue
		end
		local duration = track.duration_ms or 0
		if duration > best_duration then
			best = track
			best_duration = duration
		end
	end
	return best
end

function module.compute_fingerprint(track_db_id: number): (string?, number?)
	local entry_db_id = get_entry_db_id_for_track(track_db_id)
	if entry_db_id == nil then
		return nil, nil
	end
	local ok, result = pcall(chromaprint.compute, entry_db_id)
	if not ok or type(result) ~= "table" then
		return nil, nil
	end
	return result.fingerprint, result.duration
end

function module.lookup(fingerprint: string, duration: number): AcoustIdResult?
	local url =
		`{ACOUSTID_API}?client={http.encode_uri_component(ACOUSTID_CLIENT_ID)}&meta=recordings&fingerprint={http.encode_uri_component(
			fingerprint
		)}&duration={math.floor(duration)}`

	local request: HttpRequestOptions = {
		url = url,
		method = http.HttpMethod.Get,
		headers = {
			Accept = "application/json",
		},
	}

	local ok, response = pcall(http.request, request)
	if not ok or type(response) ~= "table" then
		return nil
	end
	if not response.success or response.status_code ~= 200 then
		return nil
	end

	local decoded_ok, data = pcall(json.decode, response.body :: string)
	if not decoded_ok then
		return nil
	end

	local root = as_json_object(data)
	if root == nil or root.status ~= "ok" then
		return nil
	end

	local results = as_json_array(root.results)
	if results == nil or #results == 0 then
		return nil
	end

	-- find the highest-scoring result (not guaranteed sorted)
	local best: JsonObject? = nil
	local best_score = -1
	for _, result_value in results do
		local candidate = as_json_object(result_value)
		if candidate == nil then
			continue
		end
		local candidate_score = candidate.score
		if type(candidate_score) == "number" and candidate_score > best_score then
			best = candidate
			best_score = candidate_score
		end
	end
	if best == nil or best_score < MIN_SCORE then
		return nil
	end

	local track_id = best.id
	if type(track_id) ~= "string" or track_id == "" then
		track_id = nil
	end

	local recording_ids: { string } = {}

	local recordings = as_json_array(best.recordings)
	if recordings then
		for _, recording_value in recordings do
			local recording = as_json_object(recording_value)
			if recording == nil then
				continue
			end
			local recording_id = recording.id
			if type(recording_id) == "string" and recording_id ~= "" then
				table.insert(recording_ids, recording_id)
			end
		end
	end

	return {
		track_id = track_id :: string?,
		recording_ids = recording_ids,
		score = best_score,
	}
end

export type ResolveDeps = {
	mb_get: (endpoint: string) -> JsonObject?,
	fetch_release_by_id: (release_id: string, release_context: ReleaseContext) -> boolean,
	json_string_field: (obj: JsonObject?, key: string) -> string?,
	json_number_field: (obj: JsonObject?, key: string) -> number?,
}

-- Tries track-count matching releases first, then falls back to others.
-- Each candidate goes through fetch_release_by_id for full validation.
function module.resolve_recording_to_release(
	recording_id: string,
	release_context: ReleaseContext,
	local_track_count: number,
	deps: ResolveDeps
): boolean
	-- validate recording ID format
	if not string.match(recording_id, "^[%x%-]+$") then
		return false
	end

	local recording_data = deps.mb_get(`/recording/{http.encode_uri_component(recording_id)}?inc=releases`)
	if not recording_data then
		return false
	end

	local releases = as_json_array(recording_data.releases)
	if not releases or #releases == 0 then
		return false
	end

	-- collect candidate release IDs, prioritizing track-count matches.
	-- fetch_release_by_id validates durations and applies via process_release,
	-- which handles language/country preference and edition scoring.
	local matching_ids: { string } = {}
	local other_ids: { string } = {}
	for _, release_value in releases do
		local release = as_json_object(release_value)
		if release == nil then
			continue
		end
		local release_id = deps.json_string_field(release, "id")
		if not release_id then
			continue
		end
		local track_count = deps.json_number_field(release, "track-count")
		if track_count and math.floor(track_count) == local_track_count then
			table.insert(matching_ids, release_id)
		else
			table.insert(other_ids, release_id)
		end
	end

	-- try track-count matches first, then others as fallback
	for _, release_id in matching_ids do
		if deps.fetch_release_by_id(release_id, release_context) then
			return true
		end
	end
	for _, release_id in other_ids do
		if deps.fetch_release_by_id(release_id, release_context) then
			return true
		end
	end

	return false
end

return module
