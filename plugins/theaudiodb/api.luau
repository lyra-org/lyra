--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

-- Requests to the TheAudioDB v1 API, and the JSON helpers reading its answers.

local http = require("@harmony/http")
local serde = require("@harmony/serde")
local json = serde.json

local json_helpers = require("./resolution/json_helpers")

export type JsonValue = json_helpers.JsonValue
export type JsonObject = json_helpers.JsonObject
export type JsonArray = json_helpers.JsonArray
type HttpRequestOptions = http.HttpRequestOptions
type HttpResponse = http.HttpResponse

local BASE_URL = "https://www.theaudiodb.com"
local API_BASE = `{BASE_URL}/api/v1/json`

local api = {}

api.BASE_URL = BASE_URL
api.DEFAULT_API_KEY = "123"
-- How long a response answers the same request again. TheAudioDB's records change rarely, and a
-- sync asks for the same artist once per release.
api.CACHE_TTL_MS = 60 * 1000

-- Sends a request; tests replace it to answer with scripted responses.
api.send = http.request :: (HttpRequestOptions) -> HttpResponse

local api_key = api.DEFAULT_API_KEY

api.trim = json_helpers.trim
api.as_json_object = json_helpers.as_json_object
api.as_json_array = json_helpers.as_json_array
api.string_field = json_helpers.json_string_field
api.number_field = json_helpers.json_number_field
api.first_non_empty_field = json_helpers.first_non_empty_field

local trim = api.trim

-- Uses `key`, or the public free-tier key when it is blank.
function api.configure(key: string?)
	local trimmed = trim(key)
	api_key = if trimmed ~= "" then trimmed else api.DEFAULT_API_KEY
end

function api.object_list(root: JsonObject?, key: string): { JsonObject }
	local items = if root then api.as_json_array(root[key]) else nil
	if items == nil then
		return {}
	end

	local results: { JsonObject } = {}
	for _, item in items do
		local object = api.as_json_object(item)
		if object ~= nil then
			table.insert(results, object)
		end
	end
	return results
end

-- The decoded answer to `endpoint`, `fresh` asking TheAudioDB again instead of reusing a recent
-- response.
function api.get(endpoint: string, params: { [string]: string }?, fresh: boolean?): JsonObject?
	local url = `{API_BASE}/{api_key}/{endpoint}`
	if params ~= nil then
		local query_parts: { string } = {}
		for key, value in params do
			local trimmed = trim(value)
			if trimmed ~= "" then
				table.insert(query_parts, `{http.encode_uri_component(key)}={http.encode_uri_component(trimmed)}`)
			end
		end
		if #query_parts > 0 then
			table.sort(query_parts)
			url ..= `?{table.concat(query_parts, "&")}`
		end
	end

	local request: HttpRequestOptions = {
		url = url,
		method = http.HttpMethod.Get,
		headers = {
			Accept = "application/json",
		},
		cache_ttl_ms = if fresh then 0 else api.CACHE_TTL_MS,
	}

	local ok, response = pcall(api.send, request)
	if not ok or type(response) ~= "table" then
		return nil
	end
	if not response.success or response.status_code ~= 200 then
		return nil
	end
	if type(response.body) ~= "string" or response.body == "" then
		return nil
	end

	local decoded_ok, data = pcall(json.decode, response.body :: string)
	if not decoded_ok then
		return nil
	end
	return api.as_json_object(data)
end

function api.lookup_artist_by_id(artist_id: string, fresh: boolean?): JsonObject?
	return api.object_list(api.get("artist.php", { i = artist_id }, fresh), "artists")[1]
end

function api.lookup_artist_by_musicbrainz_id(artist_musicbrainz_id: string, fresh: boolean?): JsonObject?
	return api.object_list(api.get("artist-mb.php", { i = artist_musicbrainz_id }, fresh), "artists")[1]
end

function api.lookup_album_by_id(album_id: string, fresh: boolean?): JsonObject?
	return api.object_list(api.get("album.php", { i = album_id }, fresh), "album")[1]
end

function api.lookup_album_by_release_group_id(release_group_id: string, fresh: boolean?): JsonObject?
	return api.object_list(api.get("album-mb.php", { i = release_group_id }, fresh), "album")[1]
end

function api.lookup_track_by_musicbrainz_id(recording_id: string, fresh: boolean?): JsonObject?
	return api.object_list(api.get("track-mb.php", { i = recording_id }, fresh), "track")[1]
end

function api.search_artists(name: string, fresh: boolean?): { JsonObject }
	return api.object_list(api.get("search.php", { s = name }, fresh), "artists")
end

function api.search_albums(artist_query: string, album_query: string?, fresh: boolean?): { JsonObject }
	local params: { [string]: string } = { s = artist_query }
	if album_query ~= nil and trim(album_query) ~= "" then
		params.a = album_query
	end
	return api.object_list(api.get("searchalbum.php", params, fresh), "album")
end

function api.search_tracks(artist_query: string, track_query: string?): { JsonObject }
	local params: { [string]: string } = { s = artist_query }
	if track_query ~= nil and trim(track_query) ~= "" then
		params.t = track_query
	end
	return api.object_list(api.get("searchtrack.php", params), "track")
end

return api
