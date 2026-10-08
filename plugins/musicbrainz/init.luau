--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local datastore = require("@lyra/datastore")
local http = require("@harmony/http")
local metadata = require("@lyra/metadata")
local serde = require("@harmony/serde")

local json_helpers = require("@self/resolution/json_helpers")
local local_release = require("@self/resolution/local_release")
local lookup = require("@self/resolution/lookup")
local musicbrainz_api = require("@self/resolution/musicbrainz_api")
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
local provider = metadata.Provider.new("musicbrainz", { display_name = "MusicBrainz" })
local BASE_URL = musicbrainz_api.BASE_URL
local COVER_CACHE_STORE_NAME = "musicbrainz"
local COVER_CACHE_KEY_PREFIX = "cover:release:"
local COVER_CACHE_HIT_TTL = 30 * 24 * 60 * 60
local COVER_CACHE_MISS_TTL = 7 * 24 * 60 * 60

type JsonValue = serde.JsonValue
type JsonObject = { [string]: JsonValue }
type DataStore = datastore.DataStore
type DataStoreJsonValue = datastore.JsonValue
type ProviderCoverCandidate = metadata.ProviderCoverCandidate
type ProviderCoverContext = metadata.ProviderCoverContext
type ProviderCoverResult = metadata.ProviderCoverResult
type SimilarReleaseContext = metadata.SimilarReleaseContext
type SimilarReleasesResult = metadata.SimilarReleasesResult
type LookupHintLibrary = text_helpers.LookupHintLibrary
type LocalArtist = track_helpers.LocalArtist
type LocalTrack = track_helpers.LocalTrack
type ReleaseArtistCredit = track_helpers.ReleaseArtistCredit
type ReleaseIds = local_release.ReleaseIds
type ReleaseContext = local_release.ReleaseContext
type CoverCacheContext = lookup.CoverCacheContext

type CoverCacheEntry = {
	context: CoverCacheContext,
	resolved_release_id: string | false,
	expires_at: number,
}

type ProviderIdSpec = {
	id_type: string,
	field: string?,
	entity: metadata.EntityType,
	unique: boolean?,
}

type SearchResultItem = { [string]: JsonValue }

local cover_store: DataStore? = nil

local as_json_object = json_helpers.as_json_object

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

local function single_cover_candidates_payload(url: string): ProviderCoverResult
	local candidate: ProviderCoverCandidate = {
		url = url,
	}
	local payload = {
		candidates = { candidate },
		selected_index = 1,
	}
	return payload :: ProviderCoverResult
end

local id_specs: { ProviderIdSpec } = {
	{ id_type = "release", entity = ET.Release, unique = true },
	{ id_type = "release-group", entity = ET.Release },
	{ id_type = "artist", entity = ET.Artist },
	{ id_type = "recording", entity = ET.Track, unique = true },
	{ id_type = "track", field = "release_track_id", entity = ET.Track, unique = true },
	{ id_type = "work", entity = ET.Track },
}

for _, id_spec in id_specs do
	local field_name = id_spec.field or string.gsub(id_spec.id_type, "-", "_") .. "_id"
	provider:id({
		id_type = field_name,
		entity = id_spec.entity,
		unique = id_spec.unique,
		scheme = "musicbrainz:" .. id_spec.id_type,
	}, BASE_URL .. "/" .. id_spec.id_type .. "/{id}")
end

provider:similar_releases({
	require = {
		all_of = { "ids.release_group_id" },
	},
}, function(ctx: SimilarReleaseContext): SimilarReleasesResult
	return {
		candidates = {
			{
				external_id = {
					provider_id = "musicbrainz",
					id_type = "release_group_id",
					id_value = ctx.ids.release_group_id,
				},
			},
		},
	}
end)

local function release_cover_handler(ctx: ProviderCoverContext): ProviderCoverResult?
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

local function nonempty(value: string?): string?
	if value == "" then
		return nil
	end
	return value
end

local function first_credit_name(credits: { ReleaseArtistCredit }?): string?
	local first_credit = if credits then credits[1] else nil
	return nonempty(if first_credit then first_credit.name else nil)
end

local function redirect_url(entity: string, id: string?): string
	if id then
		return `{BASE_URL}/{entity}/{id}`
	end
	return ""
end

local function search_ids(ids: { [string]: string? }): JsonValue
	local result: { [string]: JsonValue } = {}
	for id_type, id_value in ids do
		result[id_type] = id_value
	end
	return result
end

local function search_musicbrainz(endpoint: string, query: string): JsonObject?
	return (musicbrainz_api.mb_get(`/{endpoint}?query={http.encode_uri_component(query)}`))
end

provider:search(ET.Release, function(query: string): { SearchResultItem }
	local data = search_musicbrainz("release", query)
	local results: { SearchResultItem } = {}
	if not data then
		return results
	end
	for _, release in musicbrainz_api.normalize_release_search_result(data).releases do
		local release_id = nonempty(release.id)
		local release_group = release["release-group"]
		local cover_archive = release["cover-art-archive"]
		local cover_url: string? = nil
		if cover_archive and cover_archive.front == true and release_id then
			cover_url = `https://coverartarchive.org/release/{release_id}/front`
		end
		local item: SearchResultItem = {
			title = nonempty(release.title),
			release_date = nonempty(release.date),
			artist_name = first_credit_name(release["artist-credit"]),
			cover_url = cover_url,
			redirect_url = redirect_url("release", release_id),
			ids = search_ids({
				release_id = release_id,
				release_group_id = nonempty(if release_group then release_group.id else nil),
			}),
		}
		table.insert(results, item)
	end
	return results
end)

provider:search(ET.Artist, function(query: string): { SearchResultItem }
	local data = search_musicbrainz("artist", query)
	local results: { SearchResultItem } = {}
	if not data then
		return results
	end
	for _, artist in musicbrainz_api.normalize_artist_search_result(data).artists do
		local artist_id = nonempty(artist.id)
		local artist_name = nonempty(artist.name)
		local item: SearchResultItem = {
			title = artist_name,
			artist_name = artist_name,
			sort_name = nonempty(artist["sort-name"]),
			redirect_url = redirect_url("artist", artist_id),
			ids = search_ids({ artist_id = artist_id }),
		}
		table.insert(results, item)
	end
	return results
end)

provider:search(ET.Track, function(query: string): { SearchResultItem }
	local data = search_musicbrainz("recording", query)
	local results: { SearchResultItem } = {}
	if not data then
		return results
	end
	for _, recording in musicbrainz_api.normalize_recording_search_result(data).recordings do
		local recording_id = nonempty(recording.id)
		local releases = recording.releases
		local release = if releases then releases[1] else nil
		local item: SearchResultItem = {
			title = nonempty(recording.title),
			artist_name = first_credit_name(recording["artist-credit"]),
			release_title = nonempty(if release then release.title else nil),
			duration_ms = recording.length,
			redirect_url = redirect_url("recording", recording_id),
			ids = search_ids({ recording_id = recording_id }),
		}
		table.insert(results, item)
	end
	return results
end)

provider:id({
	id_type = "wikidata_qid",
	entity = ET.Release,
	scheme = "wikidata:item",
	label = "Wikidata",
}, "https://www.wikidata.org/wiki/{id}")

provider:id({
	id_type = "acoustid_track_id",
	entity = ET.Track,
	scheme = "acoustid:track",
	label = "AcoustID",
}, "https://acoustid.org/track/{id}")

provider:declare_option({
	name = "acoustid_fingerprint",
	label = "AcoustID Fingerprint Lookup",
	type = "boolean",
	default = false,
})

lookup.register_refresh(provider)
