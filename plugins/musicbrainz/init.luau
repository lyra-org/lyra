--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local datastore = require("@lyra/datastore")
local http = require("@harmony/http")
local json = require("@harmony/json")
local metadata = require("@lyra/metadata")

local acoustid = require("@self/resolution/acoustid")
local json_helpers = require("@self/resolution/json_helpers")
local lookup = require("@self/resolution/lookup")
local release_search = require("@self/resolution/release_search")
local text_helpers = require("@self/resolution/text_helpers")
local track_helpers = require("@self/resolution/track_helpers")

http.set_rate_limit({ domain = "musicbrainz.org" })
http.set_rate_limit({
	domain = "api.acoustid.org",
	requests_per_second = 3,
	retry_on = { 429, 503 },
	max_retries = 2,
	backoff_ms = 2000,
})

local ET = metadata.EntityType
local provider = metadata.Provider.new("musicbrainz")
local BASE_URL = lookup.get_base_url()
local API_BASE = `{BASE_URL}/ws/2`
local COVER_CACHE_STORE_NAME = "musicbrainz"
local COVER_CACHE_KEY_PREFIX = "cover:release:"
local COVER_CACHE_HIT_TTL = 30 * 24 * 60 * 60
local COVER_CACHE_MISS_TTL = 7 * 24 * 60 * 60

type JsonValue = json.JsonValue
type JsonObject = { [string]: JsonValue }
type JsonArray = { JsonValue }
type HttpRequestOptions = http.HttpRequestOptions
type DataStore = datastore.DataStore
type DataStoreJsonValue = datastore.JsonValue
type ProviderCoverContext = metadata.ProviderCoverContext
type LookupHintLibrary = text_helpers.LookupHintLibrary
type LocalArtist = track_helpers.LocalArtist
type LocalTrack = track_helpers.LocalTrack
type ReleaseIds = release_search.ReleaseIds
type ReleaseContext = release_search.ReleaseContext
type CoverCacheContext = typeof(lookup.cover_cache_context(nil))

type CoverCacheEntry = {
	context: CoverCacheContext,
	resolved_release_id: string | false,
	expires_at: number,
}

type ProviderIdSpec = {
	id_type: string,
	entity: string,
	unique: boolean?,
}

type SearchResultItem = { [string]: JsonValue }
type SearchMapFn = (JsonObject) -> SearchResultItem

local cover_store: DataStore? = nil

local as_json_object = json_helpers.as_json_object
local as_json_array = json_helpers.as_json_array

local function json_string_field(value: JsonObject?, key: string): string?
	local field = if value then value[key] else nil
	if type(field) == "string" and field ~= "" then
		return field
	end
	return nil
end

local function json_number_field(value: JsonObject?, key: string): number?
	local field = if value then value[key] else nil
	if type(field) == "number" then
		return field
	end
	return nil
end

local function json_boolean_field(value: JsonObject?, key: string): boolean?
	local field = if value then value[key] else nil
	if type(field) == "boolean" then
		return field
	end
	return nil
end

local function json_object_field(value: JsonObject?, key: string): JsonObject?
	return as_json_object(if value then value[key] else nil)
end

local function json_array_field(value: JsonObject?, key: string): JsonArray?
	return as_json_array(if value then value[key] else nil)
end

local function normalize_cover_cache_context(value: DataStoreJsonValue): CoverCacheContext?
	local object = as_json_object(value)
	if object == nil then
		return nil
	end

	local track_count = json_number_field(object, "track_count")
	return {
		release_id = json_string_field(object, "release_id"),
		release_group_id = json_string_field(object, "release_group_id"),
		track_count = if track_count ~= nil then math.floor(track_count) else nil,
		preferred_language = json_string_field(object, "preferred_language"),
		preferred_country = json_string_field(object, "preferred_country"),
		search_title = json_string_field(object, "search_title"),
	} :: CoverCacheContext
end

local function normalize_cover_cache_entry(value: DataStoreJsonValue): CoverCacheEntry?
	local object = as_json_object(value)
	if object == nil then
		return nil
	end

	local context = normalize_cover_cache_context(object.context)
	local expires_at = json_number_field(object, "expires_at")
	if context == nil or expires_at == nil then
		return nil
	end

	local raw_release_id = object.resolved_release_id
	local resolved_release_id: string | false
	if type(raw_release_id) == "string" and raw_release_id ~= "" then
		resolved_release_id = raw_release_id
	elseif raw_release_id == false then
		resolved_release_id = false
	else
		return nil
	end

	return {
		context = context,
		resolved_release_id = resolved_release_id,
		expires_at = math.floor(expires_at),
	}
end

local function normalize_local_artists(artists: { metadata.ProviderCoverArtist }?): { LocalArtist }?
	if artists == nil then
		return nil
	end
	local normalized_artists: { LocalArtist } = {}
	for _, artist in artists do
		table.insert(normalized_artists, {
			db_id = artist.db_id,
			artist_name = artist.artist_name,
		})
	end
	return normalized_artists
end

local function normalize_local_tracks(tracks: { metadata.ProviderCoverTrack }?): { LocalTrack }
	local normalized_tracks: { LocalTrack } = {}
	if tracks == nil then
		return normalized_tracks
	end
	for _, track in tracks do
		table.insert(normalized_tracks, {
			db_id = track.db_id,
			track_title = track.track_title,
			artists = nil,
			disc_total = nil,
			disc = track.disc,
			track = track.track,
			track_total = track.track_total,
			duration_ms = track.duration_ms,
			external_ids = nil,
		})
	end
	return normalized_tracks
end

local function normalize_lookup_library(library: metadata.ProviderCoverLibrary?): LookupHintLibrary?
	if library == nil then
		return nil
	end
	return {
		language = library.language,
		country = library.country,
	}
end

local function normalize_lookup_ids(ids: { [string]: string }?): ReleaseIds
	return {
		release_id = if ids then ids.release_id else nil,
		release_group_id = if ids then ids.release_group_id else nil,
	}
end

local function normalize_release_context(ctx: ProviderCoverContext?): ReleaseContext
	return {
		db_id = if ctx then ctx.db_id else nil,
		release_title = if ctx then ctx.release_title else nil,
		lookup_hints = nil,
		artists = normalize_local_artists(if ctx then ctx.artists else nil),
		tracks = normalize_local_tracks(if ctx then ctx.tracks else nil),
		library = normalize_lookup_library(if ctx then ctx.library else nil),
		library_id = if ctx and ctx.library then ctx.library.db_id else nil,
		ids = normalize_lookup_ids(if ctx then ctx.ids else nil),
		external_ids = nil,
	}
end

local function search_mb_get(endpoint: string): JsonObject?
	local request: HttpRequestOptions = {
		url = `{API_BASE}{endpoint}`,
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
	return as_json_object(data)
end

local function get_cover_store(): DataStore?
	if cover_store ~= nil then
		return cover_store
	end

	local ok, store = pcall(datastore.get_or_create, COVER_CACHE_STORE_NAME)
	if not ok then
		return nil
	end
	cover_store = store
	return store
end

local function cover_cache_key(ctx: ProviderCoverContext?): string?
	local release_db_id = if ctx then ctx.db_id else nil
	if type(release_db_id) == "number" then
		return `{COVER_CACHE_KEY_PREFIX}{release_db_id}`
	end
	return nil
end

local function cover_cache_context_matches(stored: CoverCacheContext?, current: CoverCacheContext?): boolean
	return type(stored) == "table"
		and type(current) == "table"
		and stored.release_id == current.release_id
		and stored.release_group_id == current.release_group_id
		and stored.track_count == current.track_count
		and stored.preferred_language == current.preferred_language
		and stored.preferred_country == current.preferred_country
		and stored.search_title == current.search_title
end

local function read_cached_cover_release_id(ctx: ProviderCoverContext?): (boolean, string | false | nil)
	local cover_options = if ctx and type(ctx.cover_options) == "table" then ctx.cover_options else nil
	if cover_options and cover_options.force_refresh == true then
		return false, nil
	end

	local key = cover_cache_key(ctx)
	if not key then
		return false, nil
	end

	local store = get_cover_store()
	if not store then
		return false, nil
	end

	local ok, raw_entry = pcall(store.get, store, key)
	if not ok then
		return false, nil
	end
	local entry = normalize_cover_cache_entry(raw_entry)
	if entry == nil then
		return false, nil
	end
	if type(entry.expires_at) ~= "number" or entry.expires_at <= os.time() then
		return false, nil
	end

	local release_context = normalize_release_context(ctx)
	local current_context: CoverCacheContext = lookup.cover_cache_context(release_context)
	if not cover_cache_context_matches(entry.context, current_context) then
		return false, nil
	end

	local cached_release_id = entry.resolved_release_id
	if type(cached_release_id) == "string" and cached_release_id ~= "" then
		return true, cached_release_id
	end
	if cached_release_id == false then
		return true, false
	end
	return false, nil
end

local function write_cover_release_cache(ctx: ProviderCoverContext?, release_id: string?)
	local key = cover_cache_key(ctx)
	if not key then
		return
	end

	local store = get_cover_store()
	if not store then
		return
	end

	local release_context = normalize_release_context(ctx)
	local cache_context: CoverCacheContext = lookup.cover_cache_context(release_context)
	local resolved_release_id = if type(release_id) == "string" and release_id ~= "" then release_id else false
	local ttl = if resolved_release_id ~= false then COVER_CACHE_HIT_TTL else COVER_CACHE_MISS_TTL
	pcall(
		store.set,
		store,
		key,
		{
			context = cache_context :: JsonValue,
			resolved_release_id = resolved_release_id :: JsonValue,
			expires_at = os.time() + ttl,
		} :: JsonValue
	)
end

local function single_cover_candidates_payload(url: string): JsonValue
	local candidate: JsonObject = {
		url = url,
	}
	local candidates: JsonArray = { candidate }
	local payload: JsonObject = {
		candidates = candidates,
		selected_index = 1,
	}
	return payload
end

local id_specs: { ProviderIdSpec } = {
	{ id_type = "release", entity = "release", unique = true },
	{ id_type = "release-group", entity = "release" },
	{ id_type = "artist", entity = "artist" },
	{ id_type = "recording", entity = "track", unique = true },
}

for _, id_spec in id_specs do
	local field_name = string.gsub(id_spec.id_type, "-", "_") .. "_id"
	provider:id({
		id = field_name,
		entity = id_spec.entity,
		unique = id_spec.unique,
	}, function(id)
		return `{BASE_URL}/{id_spec.id_type}/{id}`
	end)
end

local function release_cover_handler(ctx: ProviderCoverContext): JsonValue
	local cache_hit, cached_release_id = read_cached_cover_release_id(ctx)
	if cache_hit then
		if type(cached_release_id) == "string" and cached_release_id ~= "" then
			return single_cover_candidates_payload(`https://coverartarchive.org/release/{cached_release_id}/front`)
		end
		return nil
	end

	local release_context = normalize_release_context(ctx)
	local release_id = lookup.resolve_cover_release_id(release_context)
	write_cover_release_cache(ctx, release_id)
	if type(release_id) == "string" and release_id ~= "" then
		return single_cover_candidates_payload(`https://coverartarchive.org/release/{release_id}/front`)
	end
	return nil
end

provider:cover(ET.Release, {
	priority = 100,
	require = {
		any_of = { "ids.release_id", "ids.release_group_id" },
	},
}, release_cover_handler)

local function first_credit_name(credits: JsonValue): string?
	local credits_array = as_json_array(credits)
	local first_credit = if credits_array then as_json_object(credits_array[1]) else nil
	return json_string_field(first_credit, "name")
end

local function redirect_url(entity: string, id: string?): string
	if id then
		return `{BASE_URL}/{entity}/{id}`
	end
	return ""
end

local function search_handler(
	endpoint: string,
	list_key: string,
	map_item: SearchMapFn
): (string) -> { SearchResultItem }
	return function(query: string): { SearchResultItem }
		local data = search_mb_get(`/{endpoint}?query={http.encode_uri_component(query)}`)
		if not data then
			return {}
		end
		local results: { SearchResultItem } = {}
		local items = json_array_field(data, list_key)
		if items == nil then
			return results
		end
		for _, item in items do
			local item_object = as_json_object(item)
			if item_object then
				table.insert(results, map_item(item_object))
			end
		end
		return results
	end
end

provider:search(
	ET.Release,
	search_handler("release", "releases", function(release: JsonObject): SearchResultItem
		local cover_archive = json_object_field(release, "cover-art-archive")
		local release_group = json_object_field(release, "release-group")
		local release_id = json_string_field(release, "id")
		local release_group_id = json_string_field(release_group, "id")
		local release_title = json_string_field(release, "title")
		local release_date = json_string_field(release, "date")
		local cover_url: string? = nil
		if json_boolean_field(cover_archive, "front") == true and release_id then
			cover_url = `https://coverartarchive.org/release/{release_id}/front`
		end
		return {
			title = release_title,
			release_date = release_date,
			artist_name = first_credit_name(release["artist-credit"]),
			cover_url = cover_url,
			redirect_url = redirect_url("release", release_id),
			ids = {
				release_id = release_id,
				release_group_id = release_group_id,
			},
		}
	end)
)

provider:search(
	ET.Artist,
	search_handler("artist", "artists", function(artist: JsonObject): SearchResultItem
		local artist_id = json_string_field(artist, "id")
		local artist_name = json_string_field(artist, "name")
		return {
			title = artist_name,
			artist_name = artist_name,
			sort_name = json_string_field(artist, "sort-name"),
			redirect_url = redirect_url("artist", artist_id),
			ids = { artist_id = artist_id },
		}
	end)
)

provider:search(
	ET.Track,
	search_handler("recording", "recordings", function(recording: JsonObject): SearchResultItem
		local recording_id = json_string_field(recording, "id")
		local releases = json_array_field(recording, "releases")
		local release = if releases then as_json_object(releases[1]) else nil
		return {
			title = json_string_field(recording, "title"),
			artist_name = first_credit_name(recording["artist-credit"]),
			release_title = json_string_field(release, "title"),
			duration_ms = json_number_field(recording, "length"),
			redirect_url = redirect_url("recording", recording_id),
			ids = { recording_id = recording_id },
		}
	end)
)

local acoustid_deps: acoustid.ResolveDeps = {
	mb_get = search_mb_get,
	fetch_release_by_id = lookup.fetch_release_by_id,
	json_string_field = json_string_field,
	json_number_field = json_number_field,
}

provider:id({
	id = "acoustid_track_id",
	entity = "track",
}, function(id)
	return `https://acoustid.org/track/{id}`
end)

provider:declare_option({
	name = "acoustid_fingerprint",
	label = "AcoustID Fingerprint Lookup",
	type = "boolean",
	default = false,
})

lookup.register_refresh(provider, {
	deps = acoustid_deps,
	module = acoustid,
})
