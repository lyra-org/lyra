--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local chromaprint = require("@lyra/chromaprint")
local entries = require("@lyra/entries")
local http = require("@harmony/http")
local serde = require("@harmony/serde")

local json_helpers = require("./json_helpers")
local local_release = require("./local_release")
local musicbrainz_api = require("./musicbrainz_api")
local resolve = require("./resolve")
local track_helpers = require("./track_helpers")

type JsonValue = serde.JsonValue
type JsonObject = { [string]: JsonValue }
type HttpRequestOptions = http.HttpRequestOptions
type LocalRelease = local_release.LocalRelease
type LocalTrack = track_helpers.LocalTrack

local as_json_object = json_helpers.as_json_object
local as_json_array = json_helpers.as_json_array

local ACOUSTID_API = "https://api.acoustid.org/v2/lookup"
local ACOUSTID_CLIENT_ID = "hrpiiKRyqd"
local MIN_DURATION_MS = 30000
local MIN_SCORE = 0.7
local MAX_RELEASE_TRIES = 5

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

function module.compute_fingerprint(track_db_id: number): string?
	local entry_db_id = get_entry_db_id_for_track(track_db_id)
	if entry_db_id == nil then
		return nil
	end
	local ok, result = pcall(chromaprint.compute, entry_db_id)
	if not ok or type(result) ~= "table" then
		return nil
	end
	return result.fingerprint
end

-- `duration_ms` is the track's scanned duration; AcoustID takes whole seconds.
function module.lookup(fingerprint: string, duration_ms: number): AcoustIdResult?
	local url =
		`{ACOUSTID_API}?client={http.encode_uri_component(ACOUSTID_CLIENT_ID)}&meta=recordings&fingerprint={http.encode_uri_component(
			fingerprint
		)}&duration={math.floor(duration_ms / 1000)}`

	local request: HttpRequestOptions = {
		url = url,
		method = http.HttpMethod.Get,
		headers = {
			Accept = "application/json",
		},
	}

	local root = musicbrainz_api.response_object(pcall(http.request, request))
	if root == nil then
		return nil
	end
	if root.status ~= "ok" then
		musicbrainz_api.note_request_failure()
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

-- Tries the recording's releases with a matching track count first, then the
-- rest. Each goes through acceptance, with the recording as evidence.
function module.resolve_recording_to_release(recording_id: string, local_release_info: LocalRelease): boolean
	local local_track_count = #local_release_info.context.tracks
	-- validate recording ID format
	if not string.match(recording_id, "^[%x%-]+$") then
		return false
	end

	local recording_data = musicbrainz_api.mb_get(`/recording/{http.encode_uri_component(recording_id)}?inc=releases`)
	if not recording_data then
		return false
	end

	local releases = as_json_array(recording_data.releases)
	if not releases or #releases == 0 then
		return false
	end

	local fingerprinted = local_release.with_verified_recordings(local_release_info, { [recording_id] = true })
	local matching_ids: { string } = {}
	local other_ids: { string } = {}
	for _, release_value in releases do
		local release = as_json_object(release_value)
		if release == nil then
			continue
		end
		local release_id = musicbrainz_api.json_string_field(release, "id")
		if not release_id then
			continue
		end
		local track_count = musicbrainz_api.json_number_field(release, "track-count")
		if track_count and math.floor(track_count) == local_track_count then
			table.insert(matching_ids, release_id)
		else
			table.insert(other_ids, release_id)
		end
	end

	-- A popular recording sits on dozens of releases, mostly compilations;
	-- each try fetches one, so stop after a few.
	local tries = 0
	for _, candidates in { matching_ids, other_ids } do
		for _, release_id in candidates do
			if tries >= MAX_RELEASE_TRIES then
				return false
			end
			tries += 1
			if resolve.try_release_id(release_id, fingerprinted) then
				return true
			end
		end
	end

	return false
end

return module
