--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local http = require("@harmony/http")
local serde = require("@harmony/serde")
local json = serde.json

local track_helpers = require("./track_helpers")

type JsonValue = serde.JsonValue
type JsonObject = { [string]: JsonValue }
type HttpRequestOptions = http.HttpRequestOptions
type ReleaseArtist = track_helpers.ReleaseArtist
type ReleaseArtistCredit = track_helpers.ReleaseArtistCredit
type ReleaseGenre = track_helpers.ReleaseGenre
type ReleaseGroup = track_helpers.ReleaseGroup
type ReleaseRecording = track_helpers.ReleaseRecording
type ReleaseMediumTrack = track_helpers.ReleaseMediumTrack
type ReleaseMedium = track_helpers.ReleaseMedium
type Release = track_helpers.Release

export type Relation = track_helpers.Relation

export type Recording = {
	id: string?,
	title: string?,
	length: number?,
	releases: { Release }?,
	["artist-credit"]: { ReleaseArtistCredit }?,
}

export type ArtistSearchResult = {
	count: number,
	artists: { ReleaseArtist },
}

export type ReleaseSearchResult = {
	count: number,
	releases: { Release },
}

export type RecordingSearchResult = {
	count: number,
	recordings: { Recording },
}

export type RecordingResult = {
	relations: { Relation },
}

local compute_track_count = track_helpers.compute_track_count

local BASE_URL = "https://musicbrainz.org"
-- Everything a release needs to be checked and applied, including its
-- recordings' works and the works' own credits (composer, lyricist).
local RELEASE_INCLUDES =
	"recordings+artist-credits+genres+media+release-groups+recording-level-rels+work-rels+work-level-rels+artist-rels"
local API_BASE = `{BASE_URL}/ws/2`

local module = {}

module.BASE_URL = BASE_URL
module.RELEASE_INCLUDES = RELEASE_INCLUDES

local function json_string_field(object: JsonObject?, key: string): string?
	if object == nil then
		return nil
	end
	local value = object[key]
	return if type(value) == "string" then value else nil
end

local function json_number_field(object: JsonObject?, key: string): number?
	if object == nil then
		return nil
	end
	local value = object[key]
	return if type(value) == "number" then value else nil
end

local function json_object_field(object: JsonObject?, key: string): JsonObject?
	if object == nil then
		return nil
	end
	local value = object[key]
	if type(value) ~= "table" then
		return nil
	end
	return value :: JsonObject
end

local function json_array_field(object: JsonObject?, key: string): { JsonValue }?
	if object == nil then
		return nil
	end
	local value = object[key]
	if type(value) ~= "table" then
		return nil
	end
	return value :: { JsonValue }
end

local function normalize_release_artist(raw_artist: JsonValue): ReleaseArtist?
	if type(raw_artist) ~= "table" then
		return nil
	end
	local artist = raw_artist :: JsonObject
	return {
		id = json_string_field(artist, "id"),
		name = json_string_field(artist, "name"),
		["sort-name"] = json_string_field(artist, "sort-name"),
		country = json_string_field(artist, "country"),
		score = json_number_field(artist, "score"),
		["type"] = json_string_field(artist, "type"),
	}
end

local function normalize_release_artist_credits(raw_credits: { JsonValue }?): { ReleaseArtistCredit }
	local credits: { ReleaseArtistCredit } = {}
	if raw_credits == nil then
		return credits
	end
	for _, raw_credit: JsonValue in raw_credits do
		if type(raw_credit) == "table" then
			local credit = raw_credit :: JsonObject
			table.insert(credits, {
				name = json_string_field(credit, "name"),
				joinphrase = json_string_field(credit, "joinphrase"),
				artist = normalize_release_artist(json_object_field(credit, "artist")),
			})
		end
	end
	return credits
end

local function normalize_release_genres(raw_genres: { JsonValue }?): { ReleaseGenre }
	local genres: { ReleaseGenre } = {}
	if raw_genres == nil then
		return genres
	end
	for _, raw_genre: JsonValue in raw_genres do
		if type(raw_genre) == "table" then
			local genre = raw_genre :: JsonObject
			table.insert(genres, {
				id = json_string_field(genre, "id"),
				name = json_string_field(genre, "name"),
			})
		end
	end
	return genres
end

local function normalize_release_group(raw_group: JsonObject?): ReleaseGroup?
	if raw_group == nil then
		return nil
	end
	return {
		id = json_string_field(raw_group, "id"),
		["first-release-date"] = json_string_field(raw_group, "first-release-date"),
		["primary-type"] = json_string_field(raw_group, "primary-type"),
	}
end

local normalize_relations: ({ JsonValue }?) -> { Relation }

local function normalize_work(raw_work: JsonObject): { id: string?, relations: { Relation }? }
	local raw_relations = json_array_field(raw_work, "relations")
	return {
		id = json_string_field(raw_work, "id"),
		relations = if raw_relations ~= nil then normalize_relations(raw_relations) else nil,
	}
end

function normalize_relations(raw_relations: { JsonValue }?): { Relation }
	local relations: { Relation } = {}
	if raw_relations == nil then
		return relations
	end
	for _, raw_relation: JsonValue in raw_relations do
		if type(raw_relation) ~= "table" then
			continue
		end
		local relation = raw_relation :: JsonObject
		local raw_work = json_object_field(relation, "work")
		local raw_artist = json_object_field(relation, "artist")
		local raw_url = json_object_field(relation, "url")
		local raw_attributes = json_array_field(relation, "attributes")
		local attributes: { string }? = nil
		if raw_attributes ~= nil then
			local strings: { string } = {}
			for _, attr: JsonValue in raw_attributes do
				if type(attr) == "string" then
					table.insert(strings, attr)
				end
			end
			attributes = strings
		end
		table.insert(relations, {
			type = json_string_field(relation, "type"),
			["target-type"] = json_string_field(relation, "target-type"),
			direction = json_string_field(relation, "direction"),
			work = if raw_work ~= nil then normalize_work(raw_work) else nil,
			artist = if raw_artist ~= nil
				then {
					id = json_string_field(raw_artist, "id"),
					name = json_string_field(raw_artist, "name"),
					["sort-name"] = json_string_field(raw_artist, "sort-name"),
					["type"] = json_string_field(raw_artist, "type"),
				}
				else nil,
			url = if raw_url ~= nil then { resource = json_string_field(raw_url, "resource") } else nil,
			attributes = attributes,
		})
	end
	return relations
end

local function normalize_release_recording(raw_recording: JsonObject?): ReleaseRecording?
	if raw_recording == nil then
		return nil
	end
	local raw_relations = json_array_field(raw_recording, "relations")
	return {
		id = json_string_field(raw_recording, "id"),
		video = if type(raw_recording.video) == "boolean" then raw_recording.video else nil,
		relations = if raw_relations ~= nil then normalize_relations(raw_relations) else nil,
	}
end

local function normalize_release_medium_track(raw_track: JsonValue): ReleaseMediumTrack?
	if type(raw_track) ~= "table" then
		return nil
	end
	local track = raw_track :: JsonObject
	local title = json_string_field(track, "title")
	if title == nil then
		return nil
	end
	return {
		id = json_string_field(track, "id"),
		title = title,
		position = json_number_field(track, "position"),
		recording = normalize_release_recording(json_object_field(track, "recording")),
		length = json_number_field(track, "length"),
		["artist-credit"] = normalize_release_artist_credits(json_array_field(track, "artist-credit")),
	}
end

local function normalize_release_mediums(raw_media: { JsonValue }?): { ReleaseMedium }
	local media: { ReleaseMedium } = {}
	if raw_media == nil then
		return media
	end
	for _, raw_medium: JsonValue in raw_media do
		if type(raw_medium) == "table" then
			local medium = raw_medium :: JsonObject
			local tracks: { ReleaseMediumTrack } = {}
			local raw_tracks = json_array_field(medium, "tracks")
			if raw_tracks ~= nil then
				for _, raw_track: JsonValue in raw_tracks do
					local track = normalize_release_medium_track(raw_track)
					if track ~= nil then
						table.insert(tracks, track)
					end
				end
			end
			table.insert(media, {
				position = json_number_field(medium, "position"),
				format = json_string_field(medium, "format"),
				["track-count"] = json_number_field(medium, "track-count"),
				tracks = tracks,
			})
		end
	end
	return media
end

local function normalize_release(raw_release: JsonObject): Release
	local text_representation = json_object_field(raw_release, "text-representation")
	local cover_art_archive = json_object_field(raw_release, "cover-art-archive")
	return {
		id = json_string_field(raw_release, "id"),
		title = json_string_field(raw_release, "title"),
		status = json_string_field(raw_release, "status"),
		country = json_string_field(raw_release, "country"),
		date = json_string_field(raw_release, "date"),
		barcode = json_string_field(raw_release, "barcode"),
		packaging = json_string_field(raw_release, "packaging"),
		score = json_number_field(raw_release, "score"),
		media = normalize_release_mediums(json_array_field(raw_release, "media")),
		genres = normalize_release_genres(json_array_field(raw_release, "genres")),
		["track-count"] = json_number_field(raw_release, "track-count"),
		["artist-credit"] = normalize_release_artist_credits(json_array_field(raw_release, "artist-credit")),
		["release-group"] = normalize_release_group(json_object_field(raw_release, "release-group")),
		["text-representation"] = if text_representation ~= nil
			then {
				language = json_string_field(text_representation, "language"),
			}
			else nil,
		["cover-art-archive"] = if cover_art_archive ~= nil
			then {
				front = cover_art_archive["front"] == true,
			}
			else nil,
	}
end

local function normalize_release_list(raw_releases: { JsonValue }?): { Release }
	local releases: { Release } = {}
	if raw_releases == nil then
		return releases
	end
	for _, raw_release: JsonValue in raw_releases do
		if type(raw_release) == "table" then
			table.insert(releases, normalize_release(raw_release :: JsonObject))
		end
	end
	return releases
end

local function normalize_release_artist_list(raw_artists: { JsonValue }?): { ReleaseArtist }
	local artists: { ReleaseArtist } = {}
	if raw_artists == nil then
		return artists
	end
	for _, raw_artist: JsonValue in raw_artists do
		local artist = normalize_release_artist(raw_artist)
		if artist ~= nil then
			table.insert(artists, artist)
		end
	end
	return artists
end

local function normalize_recording(raw_recording: JsonValue): Recording?
	if type(raw_recording) ~= "table" then
		return nil
	end
	local recording = raw_recording :: JsonObject
	return {
		id = json_string_field(recording, "id"),
		title = json_string_field(recording, "title"),
		length = json_number_field(recording, "length"),
		releases = normalize_release_list(json_array_field(recording, "releases")),
		["artist-credit"] = normalize_release_artist_credits(json_array_field(recording, "artist-credit")),
	}
end

local function normalize_artist_search_result(raw_data: JsonObject): ArtistSearchResult
	return {
		count = json_number_field(raw_data, "count") or 0,
		artists = normalize_release_artist_list(json_array_field(raw_data, "artists")),
	}
end

local function normalize_recording_search_result(raw_data: JsonObject): RecordingSearchResult
	local recordings: { Recording } = {}
	local raw_recordings = json_array_field(raw_data, "recordings")
	if raw_recordings ~= nil then
		for _, raw_recording: JsonValue in raw_recordings do
			local recording = normalize_recording(raw_recording)
			if recording ~= nil then
				table.insert(recordings, recording)
			end
		end
	end
	return {
		count = json_number_field(raw_data, "count") or 0,
		recordings = recordings,
	}
end

local function normalize_release_search_result(raw_data: JsonObject): ReleaseSearchResult
	local releases: { Release } = {}
	local raw_releases = json_array_field(raw_data, "releases")
	if raw_releases ~= nil then
		for _, raw_release: JsonValue in raw_releases do
			if type(raw_release) == "table" then
				table.insert(releases, normalize_release(raw_release :: JsonObject))
			end
		end
	end
	return {
		count = json_number_field(raw_data, "count") or 0,
		releases = releases,
	}
end

local function normalize_recording_result(raw_data: JsonObject): RecordingResult
	return {
		relations = normalize_relations(json_array_field(raw_data, "relations")),
	}
end

-- Escape special Lucene query characters.
local LUCENE_SPECIAL = {
	["+"] = true,
	["-"] = true,
	["!"] = true,
	["("] = true,
	[")"] = true,
	["{"] = true,
	["}"] = true,
	["["] = true,
	["]"] = true,
	["^"] = true,
	['"'] = true,
	["~"] = true,
	["*"] = true,
	["?"] = true,
	[":"] = true,
	["\\"] = true,
	["/"] = true,
}

local function escape_lucene(str: string): string
	local out = {}
	for i = 1, #str do
		local ch = string.sub(str, i, i)
		if LUCENE_SPECIAL[ch] then
			table.insert(out, "\\")
		end
		table.insert(out, ch)
	end
	return table.concat(out)
end

-- Threads that saw a request fail. Each refresh handler runs on a thread of its
-- own, while concurrent refreshes share this module.
local failed_threads: { [thread]: boolean } = setmetatable({}, { __mode = "k" }) :: any

-- Records that a request failed, as opposed to answering that nothing matched.
local function note_request_failure()
	failed_threads[coroutine.running()] = true
end

-- Whether a request failed on the running refresh, so an empty result may not
-- mean that nothing matches.
local function request_failed(): boolean
	return failed_threads[coroutine.running()] == true
end

-- The JSON object a request returned, given `pcall(http.request, ...)`. A 404
-- is an answer, not a failure: the entity was removed. Any other failure is
-- recorded for the running refresh.
local function response_object(ok: boolean, response: any): JsonObject?
	if not ok or type(response) ~= "table" then
		note_request_failure()
		return nil
	end
	if not response.success or response.status_code ~= 200 then
		if response.status_code ~= 404 then
			note_request_failure()
		end
		return nil
	end
	local decoded_ok, data = pcall(json.decode, response.body :: string)
	if not decoded_ok or type(data) ~= "table" then
		note_request_failure()
		return nil
	end
	return data :: JsonObject
end

-- Helper for MusicBrainz API GET requests.
local function mb_get(endpoint: string): (JsonObject?, number)
	local request_options: HttpRequestOptions = {
		url = `{API_BASE}{endpoint}`,
		method = http.HttpMethod.Get,
		headers = {
			Accept = "application/json",
		},
	}
	local ok, response = pcall(http.request, request_options)
	local retries = if ok and type(response) == "table" then response.retries else 0
	return response_object(ok, response), retries
end

local function browse_release_group(release_group_id: string, fast_mode: boolean, track_count: number?): { Release }?
	local detailed_response = mb_get(`/release?release-group={release_group_id}&inc={RELEASE_INCLUDES}&limit=100`)
	local detailed_data = if detailed_response then normalize_release_search_result(detailed_response) else nil

	local all_data: ReleaseSearchResult? = nil
	local release_count = if detailed_data and detailed_data.releases then #detailed_data.releases else 0
	local detailed_count_source = if detailed_data then detailed_data.count else nil
	local detailed_count = tonumber(detailed_count_source) or release_count

	local needs_all_data = false
	if release_count == 0 then
		needs_all_data = true
	elseif not fast_mode then
		if detailed_count > release_count then
			needs_all_data = true
		elseif track_count ~= nil and track_count >= 100 then
			needs_all_data = true
		end
	end
	if needs_all_data then
		local all_response = mb_get(`/release?release-group={release_group_id}&limit=100`)
		all_data = if all_response then normalize_release_search_result(all_response) else nil
	end
	local releases: { Release } = {}
	local seen: { [string]: boolean } = {}
	local function add_release(release: Release)
		if not release or not release.id or seen[release.id] then
			return
		end
		compute_track_count(release) -- fills in "track-count" from the media
		table.insert(releases, release)
		seen[release.id] = true
	end
	if detailed_data then
		for _, release: Release in detailed_data.releases do
			add_release(release)
		end
	end
	if all_data then
		for _, release: Release in all_data.releases do
			add_release(release)
		end
	end
	if #releases == 0 then
		return nil
	end
	return releases
end

local function fetch_release_details(release_id: string): Release?
	local release_response = mb_get(`/release/{release_id}?inc={RELEASE_INCLUDES}`)
	local release = if release_response then normalize_release(release_response) else nil
	if release then
		compute_track_count(release)
	end
	return release
end

module.note_request_failure = note_request_failure
module.response_object = response_object
module.request_failed = request_failed
module.json_string_field = json_string_field
module.json_number_field = json_number_field
module.json_object_field = json_object_field
module.json_array_field = json_array_field
module.normalize_release_artist = normalize_release_artist
module.normalize_artist_search_result = normalize_artist_search_result
module.normalize_release_search_result = normalize_release_search_result
module.normalize_recording_search_result = normalize_recording_search_result
module.normalize_recording_result = normalize_recording_result
module.normalize_relations = normalize_relations
module.escape_lucene = escape_lucene
module.mb_get = mb_get
module.browse_release_group = browse_release_group
module.fetch_release_details = fetch_release_details

return module
