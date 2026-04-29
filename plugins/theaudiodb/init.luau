--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local http = require("@harmony/http")
local json = require("@harmony/json")
local metadata = require("@lyra/metadata")
local plugins = require("@lyra/plugins")

local ET = metadata.EntityType
local provider = metadata.Provider.new("theaudiodb")

local BASE_URL = "https://www.theaudiodb.com"
local API_BASE = `{BASE_URL}/api/v1/json`
local DEFAULT_API_KEY = "123"

type JsonValue = json.JsonValue
type JsonObject = { [string]: JsonValue }
type JsonArray = { JsonValue }
type HttpRequestOptions = http.HttpRequestOptions
type SearchResultItem = { [string]: JsonValue }
type ProviderExternalIdMap = metadata.ProviderExternalIdMap
type ExternalIdsByProvider = metadata.ExternalIdsByProvider
type RefreshOptions = { [string]: boolean | string | number }
type ArtistCoverContext = {
	artist_name: string?,
	sort_name: string?,
	ids: ProviderExternalIdMap?,
}
type ArtistRefreshContext = {
	db_id: number?,
	artist_name: string?,
	sort_name: string?,
	external_ids: ExternalIdsByProvider?,
	options: RefreshOptions?,
}
type SearchCandidatesFn = (string, string?) -> { JsonObject }

local function trim(value: string?): string
	if value == nil then
		return ""
	end
	local trimmed = string.gsub(value, "^%s+", "")
	trimmed = string.gsub(trimmed, "%s+$", "")
	return trimmed
end

local config = plugins.declare_settings(function(builder)
	builder:group("api", "API")
	builder:string("api_key", {
		label = "API key",
		description = "TheAudioDB v1 API key. Leave as 123 to use the public free tier.",
		default = DEFAULT_API_KEY,
	})
end)

local RAW_API_KEY = trim((config.api_key or DEFAULT_API_KEY) :: string)
local API_KEY = if RAW_API_KEY ~= "" then RAW_API_KEY else DEFAULT_API_KEY

http.set_rate_limit({
	domain = "www.theaudiodb.com",
	requests_per_second = 0.5,
	retry_on = { 429, 503 },
	max_retries = 2,
	backoff_ms = 2500,
})

local function as_json_object(value: JsonValue?): JsonObject?
	if type(value) ~= "table" then
		return nil
	end
	local encoded = json.encode(value)
	if string.sub(encoded, 1, 1) ~= "{" then
		return nil
	end
	return value :: JsonObject
end

local function as_json_array(value: JsonValue?): JsonArray?
	if type(value) ~= "table" then
		return nil
	end
	local encoded = json.encode(value)
	if string.sub(encoded, 1, 1) ~= "[" then
		return nil
	end
	return value :: JsonArray
end

local function json_string_field(value: JsonObject?, key: string): string?
	local field = if value then value[key] else nil
	if type(field) == "string" then
		local trimmed = trim(field)
		if trimmed ~= "" then
			return trimmed
		end
	end
	return nil
end

local function json_number_field(value: JsonObject?, key: string): number?
	local field = if value then value[key] else nil
	if type(field) == "number" then
		return field
	end
	if type(field) == "string" then
		local parsed = tonumber(trim(field))
		if parsed ~= nil then
			return parsed
		end
	end
	return nil
end

local function first_non_empty_field(value: JsonObject?, keys: { string }): string?
	for _, key in keys do
		local field = json_string_field(value, key)
		if field ~= nil then
			return field
		end
	end
	return nil
end

local function json_object_list(root: JsonObject?, key: string): { JsonObject }
	local items = if root then as_json_array(root[key]) else nil
	if items == nil then
		return {}
	end

	local results: { JsonObject } = {}
	for _, item in items do
		local object = as_json_object(item)
		if object ~= nil then
			table.insert(results, object)
		end
	end
	return results
end

local function normalize_text(value: string?): string
	local normalized = string.lower(trim(value))
	normalized = string.gsub(normalized, "%s+", " ")
	return normalized
end

local function year_string(value: string?): string?
	local match = if value then string.match(value, "(%d%d%d%d)") else nil
	if match ~= nil and match ~= "" then
		return match
	end
	return nil
end

local QUERY_SEPARATORS = { " - ", " – ", " — ", ": " }
local ARTIST_COVER_KEYS = {
	"strArtistThumb",
	"strArtistWideThumb",
	"strArtistFanart",
	"strArtistFanart2",
	"strArtistFanart3",
	"strArtistFanart4",
	"strArtistBanner",
}
local ALBUM_COVER_KEYS = {
	"strAlbumThumbHQ",
	"strAlbumThumb",
	"strAlbum3DThumb",
	"strAlbum3DCase",
	"strAlbum3DFace",
	"strAlbum3DFlat",
}
local ALBUM_ID_KEYS = { "idAlbum" }
local TRACK_ID_KEYS = { "idTrack" }

local function split_compound_query(query: string): (string?, string?)
	for _, separator in QUERY_SEPARATORS do
		local start_index = string.find(query, separator, 1, true)
		if start_index ~= nil then
			local left = trim(string.sub(query, 1, start_index - 1))
			local right = trim(string.sub(query, start_index + #separator))
			if left ~= "" and right ~= "" then
				return left, right
			end
		end
	end
	return nil, nil
end

local function audiodb_url(path: string): string
	return `{BASE_URL}/{path}`
end

local function build_redirect_url(entity: string, id: string?): string
	if id ~= nil and id ~= "" then
		return audiodb_url(`{entity}/{id}`)
	end
	return BASE_URL
end

local function redirect_url_generator(entity: string): (...unknown) -> string
	return function(...: unknown): string
		local id = select(1, ...)
		if type(id) == "string" then
			return build_redirect_url(entity, id)
		end
		return build_redirect_url(entity, nil)
	end
end

local function single_id_map(key: string, value: string?): JsonObject?
	if value == nil or value == "" then
		return nil
	end
	return {
		[key] = value,
	}
end
local provider_external_ids = metadata.ids.for_provider

local function audiodb_get(endpoint: string, params: { [string]: string }?): JsonObject?
	local url = `{API_BASE}/{API_KEY}/{endpoint}`
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
	}

	local ok, response = pcall(http.request, request)
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
	return as_json_object(data)
end

local function lookup_artist_by_id(artist_id: string): JsonObject?
	local root = audiodb_get("artist.php", { i = artist_id })
	return json_object_list(root, "artists")[1]
end

local function lookup_artist_by_musicbrainz_id(artist_musicbrainz_id: string): JsonObject?
	local root = audiodb_get("artist-mb.php", { i = artist_musicbrainz_id })
	return json_object_list(root, "artists")[1]
end

local function lookup_album_by_id(album_id: string): JsonObject?
	local root = audiodb_get("album.php", { i = album_id })
	return json_object_list(root, "album")[1]
end

local function lookup_album_by_release_group_id(release_group_id: string): JsonObject?
	local root = audiodb_get("album-mb.php", { i = release_group_id })
	return json_object_list(root, "album")[1]
end

local function lookup_track_by_musicbrainz_id(recording_id: string): JsonObject?
	local root = audiodb_get("track-mb.php", { i = recording_id })
	return json_object_list(root, "track")[1]
end

local function search_artists_by_name(name: string): { JsonObject }
	local root = audiodb_get("search.php", { s = name })
	return json_object_list(root, "artists")
end

local function search_albums(artist_query: string, album_query: string?): { JsonObject }
	local params: { [string]: string } = { s = artist_query }
	if album_query ~= nil and trim(album_query) ~= "" then
		params.a = album_query
	end
	local root = audiodb_get("searchalbum.php", params)
	return json_object_list(root, "album")
end

local function search_tracks(artist_query: string, track_query: string?): { JsonObject }
	local params: { [string]: string } = { s = artist_query }
	if track_query ~= nil and trim(track_query) ~= "" then
		params.t = track_query
	end
	local root = audiodb_get("searchtrack.php", params)
	return json_object_list(root, "track")
end

local function score_exactish_match(candidate: string?, target: string?): number
	local normalized_candidate = normalize_text(candidate)
	local normalized_target = normalize_text(target)
	if normalized_candidate == "" or normalized_target == "" then
		return 0
	end
	if normalized_candidate == normalized_target then
		return 100
	end
	if string.find(normalized_candidate, normalized_target, 1, true) ~= nil then
		return 50
	end
	if string.find(normalized_target, normalized_candidate, 1, true) ~= nil then
		return 35
	end
	return 0
end

local function best_artist_match(artists: { JsonObject }, target_name: string?): JsonObject?
	if #artists == 0 then
		return nil
	end
	if target_name == nil or trim(target_name) == "" then
		return artists[1]
	end

	local best = artists[1]
	local best_score = -1
	for _, artist in artists do
		local score = score_exactish_match(json_string_field(artist, "strArtist"), target_name)
		score += score_exactish_match(json_string_field(artist, "strArtistAlternate"), target_name)
		if score > best_score then
			best = artist
			best_score = score
		end
	end
	return best
end

local function best_album_match(
	albums: { JsonObject },
	artist_name: string?,
	album_title: string?,
	release_year: string?
): JsonObject?
	if #albums == 0 then
		return nil
	end

	local best = albums[1]
	local best_score = -1
	local target_year = year_string(release_year)
	for _, album in albums do
		local score = 0
		score += score_exactish_match(json_string_field(album, "strAlbum"), album_title)
		score += score_exactish_match(json_string_field(album, "strArtist"), artist_name)

		local candidate_year = year_string(json_string_field(album, "intYearReleased"))
		if target_year ~= nil and candidate_year ~= nil and target_year == candidate_year then
			score += 20
		end

		if score > best_score then
			best = album
			best_score = score
		end
	end
	return best
end

local function artist_name_matches_any(candidate_name: string?, names: { string }): boolean
	if #names == 0 then
		return true
	end
	for _, name in names do
		if score_exactish_match(candidate_name, name) > 0 then
			return true
		end
	end
	return false
end

local function push_unique_string(results: { string }, seen: { [string]: boolean }, value: string?)
	if value == nil or value == "" or seen[value] == true then
		return
	end
	seen[value] = true
	table.insert(results, value)
end

local function push_unique_object(
	results: { JsonObject },
	seen: { [string]: boolean },
	value: JsonObject,
	id_keys: { string }
)
	local dedupe_key = ""
	for _, key in id_keys do
		local value_id = json_string_field(value, key)
		if value_id ~= nil then
			dedupe_key = `{key}:{value_id}`
			break
		end
	end
	if dedupe_key == "" then
		dedupe_key = json.encode(value)
	end
	if seen[dedupe_key] == true then
		return
	end
	seen[dedupe_key] = true
	table.insert(results, value)
end

local function genre_values(genre_name: string?): JsonArray?
	local genre = trim(genre_name)
	if genre == "" then
		return nil
	end
	return { genre :: JsonValue }
end

local function preferred_cover_url(item: JsonObject, keys: { string }): string?
	return first_non_empty_field(item, keys)
end

local function preferred_artist_cover_url(artist: JsonObject): string?
	return preferred_cover_url(artist, ARTIST_COVER_KEYS)
end

local function preferred_album_cover_url(album: JsonObject): string?
	return preferred_cover_url(album, ALBUM_COVER_KEYS)
end

local function cover_payload(item: JsonObject, keys: { string }): JsonValue?
	local urls: { string } = {}
	local seen: { [string]: boolean } = {}
	for _, key in keys do
		push_unique_string(urls, seen, json_string_field(item, key))
	end
	if #urls == 0 then
		return nil
	end

	local candidates: { JsonValue } = {}
	for _, url in urls do
		table.insert(candidates, { url = url } :: JsonValue)
	end
	return {
		candidates = candidates,
		selected_index = 1,
	} :: JsonValue
end

local function artist_cover_payload(artist: JsonObject): JsonValue?
	return cover_payload(artist, ARTIST_COVER_KEYS)
end

local function album_cover_payload(album: JsonObject): JsonValue?
	return cover_payload(album, ALBUM_COVER_KEYS)
end

local function resolve_artist_cover_match(ctx: ArtistCoverContext): JsonObject?
	local ids = ctx.ids
	local artist_id = if ids then ids.artist_id else nil
	if artist_id ~= nil and artist_id ~= "" then
		local artist = lookup_artist_by_id(artist_id)
		if artist ~= nil then
			return artist
		end
	end

	local primary_name = ctx.artist_name
	local backup_name = ctx.sort_name
	if primary_name ~= nil and trim(primary_name) ~= "" then
		local artist = best_artist_match(search_artists_by_name(primary_name), primary_name)
		if artist ~= nil then
			return artist
		end
	end
	if
		backup_name ~= nil
		and trim(backup_name) ~= ""
		and normalize_text(backup_name) ~= normalize_text(primary_name)
	then
		return best_artist_match(search_artists_by_name(backup_name), backup_name)
	end
	return nil
end

local function first_artist_name(ctx: metadata.ProviderCoverContext): string?
	local names = ctx.artist_names
	if names ~= nil then
		for _, name in names do
			if trim(name) ~= "" then
				return name
			end
		end
	end
	local artists = ctx.artists
	if artists ~= nil then
		for _, artist in artists do
			if artist.artist_name ~= nil and trim(artist.artist_name) ~= "" then
				return artist.artist_name
			end
		end
	end
	return nil
end

local function resolve_release_cover_match(ctx: metadata.ProviderCoverContext): JsonObject?
	local ids = ctx.ids
	local album_id = if ids then ids.album_id else nil
	if album_id ~= nil and album_id ~= "" then
		local album = lookup_album_by_id(album_id)
		if album ~= nil then
			return album
		end
	end

	local release_title = if ctx.release_title then trim(ctx.release_title) else ""
	local artist_name = first_artist_name(ctx)
	if release_title == "" or artist_name == nil or trim(artist_name) == "" then
		return nil
	end

	local albums = search_albums(artist_name, release_title)
	return best_album_match(albums, artist_name, release_title, ctx.release_date)
end

local function artist_search_results(query: string): { SearchResultItem }
	local results: { SearchResultItem } = {}
	for _, artist in search_artists_by_name(query) do
		local artist_id = json_string_field(artist, "idArtist")
		local artist_name = json_string_field(artist, "strArtist")
		if artist_name == nil then
			continue
		end

		local item: SearchResultItem = {
			title = artist_name,
			artist_name = artist_name,
			redirect_url = build_redirect_url("artist", artist_id),
		}

		local sort_name = first_non_empty_field(artist, { "strArtistStripped", "strArtist" })
		if sort_name ~= nil then
			item.sort_name = sort_name
		end
		local description = first_non_empty_field(artist, { "strBiography", "strBiographyEN" })
		if description ~= nil then
			item.description = description
		end
		local cover_url = preferred_artist_cover_url(artist)
		if cover_url ~= nil then
			item.cover_url = cover_url
		end
		local ids = single_id_map("artist_id", artist_id)
		if ids ~= nil then
			item.ids = ids
		end

		table.insert(results, item)
	end
	return results
end

local function search_candidates(query: string, searcher: SearchCandidatesFn, id_keys: { string }): { JsonObject }
	local results: { JsonObject } = {}
	local seen: { [string]: boolean } = {}

	local left, right = split_compound_query(query)
	if left ~= nil and right ~= nil then
		for _, item in searcher(left, right) do
			push_unique_object(results, seen, item, id_keys)
		end
		for _, item in searcher(right, left) do
			push_unique_object(results, seen, item, id_keys)
		end
	end

	for _, item in searcher(query, nil) do
		push_unique_object(results, seen, item, id_keys)
	end

	return results
end

local function release_search_candidates(query: string): { JsonObject }
	return search_candidates(query, search_albums, ALBUM_ID_KEYS)
end

local function release_search_results(query: string): { SearchResultItem }
	local results: { SearchResultItem } = {}
	for _, album in release_search_candidates(query) do
		local album_id = json_string_field(album, "idAlbum")
		local title = json_string_field(album, "strAlbum")
		if title == nil then
			continue
		end

		local item: SearchResultItem = {
			title = title,
			redirect_url = build_redirect_url("album", album_id),
		}

		local artist_name = json_string_field(album, "strArtist")
		if artist_name ~= nil then
			item.artist_name = artist_name
		end
		local release_date = year_string(json_string_field(album, "intYearReleased"))
		if release_date ~= nil then
			item.release_date = release_date
		end
		local genres = genre_values(json_string_field(album, "strGenre"))
		if genres ~= nil then
			item.genres = genres
		end
		local description = first_non_empty_field(album, { "strDescription", "strDescriptionEN", "strReview" })
		if description ~= nil then
			item.description = description
		end
		local cover_url = preferred_album_cover_url(album)
		if cover_url ~= nil then
			item.cover_url = cover_url
		end
		local ids = single_id_map("album_id", album_id)
		if ids ~= nil then
			item.ids = ids
		end

		table.insert(results, item)
	end
	return results
end

local function track_search_candidates(query: string): { JsonObject }
	return search_candidates(query, search_tracks, TRACK_ID_KEYS)
end

local function track_search_results(query: string): { SearchResultItem }
	local results: { SearchResultItem } = {}
	for _, track in track_search_candidates(query) do
		local track_id = json_string_field(track, "idTrack")
		local title = json_string_field(track, "strTrack")
		if title == nil then
			continue
		end

		local item: SearchResultItem = {
			title = title,
			redirect_url = build_redirect_url("track", track_id),
		}

		local artist_name = json_string_field(track, "strArtist")
		if artist_name ~= nil then
			item.artist_name = artist_name
		end
		local release_title = json_string_field(track, "strAlbum")
		if release_title ~= nil then
			item.release_title = release_title
		end
		local description = first_non_empty_field(track, { "strDescriptionEN", "strTrackLyrics" })
		if description ~= nil then
			item.description = description
		end
		local duration_ms = json_number_field(track, "intDuration")
		if duration_ms ~= nil then
			item.duration_ms = duration_ms
		end
		local disc = json_number_field(track, "intCD")
		if disc ~= nil then
			item.disc = disc
		end
		local track_number = json_number_field(track, "intTrackNumber")
		if track_number ~= nil then
			item.track = track_number
		end
		local cover_url = json_string_field(track, "strTrackThumb")
		if cover_url ~= nil then
			item.cover_url = cover_url
		end
		local ids = single_id_map("track_id", track_id)
		if ids ~= nil then
			item.ids = ids
		end

		table.insert(results, item)
	end
	return results
end

provider:id({
	id = "artist_id",
	entity = "artist",
	unique = true,
}, redirect_url_generator("artist"))

provider:id({
	id = "album_id",
	entity = "release",
	unique = true,
}, redirect_url_generator("album"))

provider:id({
	id = "track_id",
	entity = "track",
	unique = true,
}, redirect_url_generator("track"))

provider:search(ET.Artist, function(query: string): { SearchResultItem }
	return artist_search_results(query)
end)

provider:search(ET.Release, function(query: string): { SearchResultItem }
	return release_search_results(query)
end)

provider:search(ET.Track, function(query: string): { SearchResultItem }
	return track_search_results(query)
end)

provider:cover(ET.Artist, {
	priority = 80,
	require = {
		any_of = { "ids.artist_id", "artist_name", "sort_name" },
	},
}, function(ctx): JsonValue?
	local artist = resolve_artist_cover_match(ctx :: ArtistCoverContext)
	if artist == nil then
		return nil
	end
	return artist_cover_payload(artist)
end)

provider:cover(ET.Release, {
	priority = 60,
	require = {
		all_of = { "release_title" },
		any_of = { "ids.album_id", "artist_names" },
	},
}, function(ctx: metadata.ProviderCoverContext): JsonValue?
	local album = resolve_release_cover_match(ctx)
	if album == nil then
		return nil
	end
	return album_cover_payload(album)
end)

provider:declare_option({
	name = "refresh_existing",
	label = "Refresh Existing Match",
	type = "boolean",
	default = false,
})

local function refresh_artist_names(ctx: metadata.ReleaseRefreshContext): { string }
	local names: { string } = {}
	local seen: { [string]: boolean } = {}

	if ctx.artists ~= nil then
		for _, artist in ctx.artists do
			local name = trim(artist.artist_name)
			if name ~= "" and seen[name] ~= true then
				seen[name] = true
				table.insert(names, name)
			end
		end
	end

	if ctx.lookup_hints ~= nil and ctx.lookup_hints.artist_name ~= nil then
		local hint_name = trim(ctx.lookup_hints.artist_name)
		if hint_name ~= "" and seen[hint_name] ~= true then
			seen[hint_name] = true
			table.insert(names, hint_name)
		end
	end

	return names
end

local function artist_lookup_names(ctx: ArtistRefreshContext): { string }
	local names: { string } = {}
	local seen: { [string]: boolean } = {}

	local primary_name = trim(ctx.artist_name)
	if primary_name ~= "" and seen[primary_name] ~= true then
		seen[primary_name] = true
		table.insert(names, primary_name)
	end

	local sort_name = trim(ctx.sort_name)
	if sort_name ~= "" and seen[sort_name] ~= true then
		seen[sort_name] = true
		table.insert(names, sort_name)
	end

	return names
end

local function should_refresh_existing(ctx: metadata.ReleaseRefreshContext): boolean
	return ctx.options ~= nil and ctx.options.refresh_existing == true
end

local function refresh_artist_match(ctx: ArtistRefreshContext): JsonObject?
	local theaudiodb_ids = provider_external_ids(ctx.external_ids, "theaudiodb")
	if theaudiodb_ids ~= nil then
		local existing_artist_id = theaudiodb_ids.artist_id
		if existing_artist_id ~= nil and existing_artist_id ~= "" then
			return lookup_artist_by_id(existing_artist_id)
		end
	end

	local musicbrainz_ids = provider_external_ids(ctx.external_ids, "musicbrainz")
	local artist_musicbrainz_id = if musicbrainz_ids then musicbrainz_ids.artist_id else nil
	if artist_musicbrainz_id ~= nil and artist_musicbrainz_id ~= "" then
		local artist = lookup_artist_by_musicbrainz_id(artist_musicbrainz_id)
		if artist ~= nil then
			return artist
		end
	end

	local names = artist_lookup_names(ctx)
	for _, name in names do
		local artist = best_artist_match(search_artists_by_name(name), name)
		if artist ~= nil then
			return artist
		end
	end

	return nil
end

local function refresh_album_match_from_recording_ids(
	ctx: metadata.ReleaseRefreshContext,
	artist_names: { string },
	release_title: string
): JsonObject?
	if ctx.tracks == nil then
		return nil
	end

	for _, track in ctx.tracks do
		local musicbrainz_ids = provider_external_ids(track.external_ids, "musicbrainz")
		local recording_id = if musicbrainz_ids then musicbrainz_ids.recording_id else nil
		if recording_id == nil or recording_id == "" then
			continue
		end

		local matched_track = lookup_track_by_musicbrainz_id(recording_id)
		local album_id = json_string_field(matched_track, "idAlbum")
		if album_id == nil then
			continue
		end
		if score_exactish_match(json_string_field(matched_track, "strAlbum"), release_title) < 100 then
			continue
		end
		if not artist_name_matches_any(json_string_field(matched_track, "strArtist"), artist_names) then
			continue
		end

		local album = lookup_album_by_id(album_id)
		if album ~= nil then
			return album
		end
	end

	return nil
end

local function refresh_album_match(ctx: metadata.ReleaseRefreshContext): JsonObject?
	local theaudiodb_ids = provider_external_ids(ctx.external_ids, "theaudiodb")
	if theaudiodb_ids ~= nil then
		local existing_album_id = theaudiodb_ids.album_id
		if existing_album_id ~= nil and existing_album_id ~= "" then
			return lookup_album_by_id(existing_album_id)
		end
	end

	local musicbrainz_ids = provider_external_ids(ctx.external_ids, "musicbrainz")
	local release_group_id = if musicbrainz_ids then musicbrainz_ids.release_group_id else nil
	if release_group_id ~= nil and release_group_id ~= "" then
		local album = lookup_album_by_release_group_id(release_group_id)
		if album ~= nil then
			return album
		end
	end

	local artist_names = refresh_artist_names(ctx)
	local release_title = trim(ctx.release_title)
	if release_title == "" then
		return nil
	end

	local album_from_track = refresh_album_match_from_recording_ids(ctx, artist_names, release_title)
	if album_from_track ~= nil then
		return album_from_track
	end

	local release_year = if ctx.lookup_hints ~= nil and ctx.lookup_hints.year ~= nil
		then tostring(ctx.lookup_hints.year)
		else ctx.release_date
	for _, artist_name in artist_names do
		local matched =
			best_album_match(search_albums(artist_name, release_title), artist_name, release_title, release_year)
		if matched ~= nil then
			return matched
		end
	end

	return nil
end

local function matching_artist_db_id(artists: { metadata.ReleaseRefreshArtist }?, album_artist_name: string?): number?
	if artists == nil or #artists == 0 then
		return nil
	end
	if #artists == 1 then
		return artists[1].db_id
	end
	local target = normalize_text(album_artist_name)
	if target == "" then
		return nil
	end
	for _, artist in artists do
		if normalize_text(artist.artist_name) == target then
			return artist.db_id
		end
	end
	return nil
end

provider:refresh(ET.Artist, function(ctx: metadata.ReleaseRefreshContext)
	local artist_ctx = ctx :: ArtistRefreshContext
	local artist_db_id = artist_ctx.db_id
	if artist_db_id == nil then
		return nil
	end

	local artist = refresh_artist_match(artist_ctx)
	if artist == nil then
		return nil
	end

	local artist_id = json_string_field(artist, "idArtist")
	if artist_id == nil then
		return nil
	end

	local artist_layer = provider:layer(artist_db_id)
	artist_layer:set_id("artist_id", artist_id)
	artist_layer:save()
	return nil
end, function(ctx: metadata.ReleaseRefreshContext): boolean
	local artist_ctx = ctx :: ArtistRefreshContext
	if should_refresh_existing(ctx) then
		return #artist_lookup_names(artist_ctx) > 0 or provider_external_ids(ctx.external_ids, "musicbrainz") ~= nil
	end

	local theaudiodb_ids = provider_external_ids(ctx.external_ids, "theaudiodb")
	if theaudiodb_ids ~= nil and theaudiodb_ids.artist_id ~= nil and theaudiodb_ids.artist_id ~= "" then
		return false
	end

	local musicbrainz_ids = provider_external_ids(ctx.external_ids, "musicbrainz")
	return musicbrainz_ids ~= nil and musicbrainz_ids.artist_id ~= nil and musicbrainz_ids.artist_id ~= ""
end)

provider:refresh(ET.Release, function(ctx: metadata.ReleaseRefreshContext)
	local release_db_id = ctx.db_id
	if release_db_id == nil then
		return nil
	end

	local album = refresh_album_match(ctx)
	if album == nil then
		return nil
	end

	local album_id = json_string_field(album, "idAlbum")
	if album_id == nil then
		return nil
	end

	local release_layer = provider:layer(release_db_id)
	release_layer:set_id("album_id", album_id)

	local genres = genre_values(json_string_field(album, "strGenre"))
	if genres ~= nil then
		release_layer:set_field("genres", genres)
	end

	release_layer:save()

	local artist_db_id = matching_artist_db_id(ctx.artists, json_string_field(album, "strArtist"))
	local audiodb_artist_id = json_string_field(album, "idArtist")
	if artist_db_id ~= nil and audiodb_artist_id ~= nil then
		local artist_layer = provider:layer(artist_db_id)
		artist_layer:set_id("artist_id", audiodb_artist_id)
		artist_layer:save()
	end

	return nil
end, function(ctx: metadata.ReleaseRefreshContext): boolean
	if trim(ctx.release_title) == "" then
		return false
	end
	if should_refresh_existing(ctx) then
		return true
	end
	local theaudiodb_ids = provider_external_ids(ctx.external_ids, "theaudiodb")
	if theaudiodb_ids ~= nil and theaudiodb_ids.album_id ~= nil and theaudiodb_ids.album_id ~= "" then
		return false
	end
	local musicbrainz_ids = provider_external_ids(ctx.external_ids, "musicbrainz")
	if
		musicbrainz_ids ~= nil
		and musicbrainz_ids.release_group_id ~= nil
		and musicbrainz_ids.release_group_id ~= ""
	then
		return true
	end
	return #refresh_artist_names(ctx) > 0
end)
