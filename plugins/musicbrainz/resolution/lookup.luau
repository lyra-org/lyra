--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local genres = require("@lyra/genres")
local http = require("@harmony/http")
local json = require("@harmony/json")
local libraries = require("@lyra/libraries")
local metadata = require("@lyra/metadata")

local acoustid = require("./acoustid")
local release_search = require("./release_search")
local text_helpers = require("./text_helpers")
local track_helpers = require("./track_helpers")

local BASE_URL = "https://musicbrainz.org"
local API_BASE = `{BASE_URL}/ws/2`

type JsonValue = json.JsonValue
type JsonObject = { [string]: JsonValue }
type HttpRequestOptions = http.HttpRequestOptions
type LookupHints = text_helpers.LookupHints
type LookupArtist = text_helpers.LookupHintArtist
type LookupTrack = text_helpers.LookupHintTrack
type LookupLibrary = text_helpers.LookupHintLibrary
type LookupContext = text_helpers.LookupHintReleaseContext
type LocalArtist = track_helpers.LocalArtist
type LocalTrack = track_helpers.LocalTrack
type ReleaseArtist = track_helpers.ReleaseArtist
type ReleaseArtistCredit = track_helpers.ReleaseArtistCredit
type ReleaseGroup = track_helpers.ReleaseGroup
type ReleaseTextRepresentation = track_helpers.ReleaseTextRepresentation
type ReleaseCoverArchive = track_helpers.ReleaseCoverArchive
type ReleaseRecording = track_helpers.ReleaseRecording
type ReleaseGenre = track_helpers.ReleaseGenre
type ReleaseMediumTrack = track_helpers.ReleaseMediumTrack
type ReleaseMedium = track_helpers.ReleaseMedium
type Release = track_helpers.Release
type ArtistIdSet = release_search.ArtistIdSet
type ReleaseIds = release_search.ReleaseIds
type ExternalIdsByProvider = metadata.ExternalIdsByProvider
type ReleaseContext = release_search.ReleaseContext
type ProviderLayer = release_search.ProviderLayer
type ProviderHandle = release_search.ProviderHandle
type SearchDeps = release_search.SearchDeps
type ReleaseSearchResult = {
	count: number,
	releases: { Release },
}
type RelationWork = {
	id: string?,
}
type RelationArtist = {
	id: string?,
	name: string?,
	["sort-name"]: string?,
}
type Relation = {
	type: string?,
	["target-type"]: string?,
	work: RelationWork?,
	artist: RelationArtist?,
	attributes: { string }?,
}
type RecordingResult = {
	relations: { Relation },
}
type Layer = metadata.Layer
type EnsureArtistRequest = metadata.EnsureArtistRequest
type Provider = metadata.Provider

local provider: Provider? = nil
local module = {}

local normalize_title = text_helpers.normalize_title
local normalize_overlap_title = text_helpers.normalize_overlap_title
local clean_release_title = text_helpers.clean_release_title
local trim_text = text_helpers.trim_text
local release_has_barcode = text_helpers.release_has_barcode
local is_blank_text = text_helpers.is_blank_text
local is_placeholder_text = text_helpers.is_placeholder_text
local infer_lookup_hints = text_helpers.infer_lookup_hints
local is_generic_artist = text_helpers.is_generic_artist
local contains_multibyte = text_helpers.contains_multibyte
local normalize_artist_name_for_lookup = text_helpers.normalize_artist_name_for_lookup
local artist_name_matches = text_helpers.artist_name_matches

local max_declared_track_total = track_helpers.max_declared_track_total
local artist_name_from_tracks = track_helpers.artist_name_from_tracks
local is_compilation = track_helpers.is_compilation
local sort_tracks_for_refresh = track_helpers.sort_tracks_for_refresh
local flatten_release_tracks = track_helpers.flatten_release_tracks
local assign_mb_tracks_to_local_tracks = track_helpers.assign_mb_tracks_to_local_tracks
local count_track_title_overlap = track_helpers.count_track_title_overlap
local release_has_track_title = track_helpers.release_has_track_title
local validate_release_durations = track_helpers.validate_release_durations
local should_preserve_local_track_positions = track_helpers.should_preserve_local_track_positions
local single_track_duration_diff = track_helpers.single_track_duration_diff

local function musicbrainz_external_ids(external_ids: ExternalIdsByProvider?): ReleaseIds?
	local ids = metadata.ids.for_provider(external_ids, "musicbrainz")
	if ids == nil then
		return nil
	end
	return ids :: ReleaseIds
end

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
					if type(raw_track) == "table" then
						local track = raw_track :: JsonObject
						local title = json_string_field(track, "title")
						if title ~= nil then
							local raw_recording = json_object_field(track, "recording")
							local recording_relations: { Relation }? = if raw_recording
								then json_array_field(raw_recording, "relations") :: { Relation }?
								else nil
							table.insert(tracks, {
								title = title,
								position = json_number_field(track, "position"),
								recording = {
									id = json_string_field(raw_recording, "id"),
									relations = recording_relations,
								},
								length = json_number_field(track, "length"),
								["artist-credit"] = normalize_release_artist_credits(
									json_array_field(track, "artist-credit")
								),
							})
						end
					end
				end
			end
			table.insert(media, {
				position = json_number_field(medium, "position"),
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
	local relations: { Relation } = {}
	local raw_relations = json_array_field(raw_data, "relations")
	if raw_relations ~= nil then
		for _, raw_relation: JsonValue in raw_relations do
			if type(raw_relation) == "table" then
				local relation = raw_relation :: JsonObject
				local raw_artist = json_object_field(relation, "artist")
				local raw_attributes = json_array_field(relation, "attributes")
				local attributes: { string }? = nil
				if raw_attributes ~= nil then
					attributes = {}
					for _, attr: JsonValue in raw_attributes do
						if type(attr) == "string" then
							table.insert(attributes :: { string }, attr :: string)
						end
					end
				end
				table.insert(relations, {
					type = json_string_field(relation, "type"),
					["target-type"] = json_string_field(relation, "target-type"),
					work = if json_object_field(relation, "work") ~= nil
						then {
							id = json_string_field(json_object_field(relation, "work"), "id"),
						}
						else nil,
					artist = if raw_artist ~= nil
						then {
							id = json_string_field(raw_artist, "id"),
							name = json_string_field(raw_artist, "name"),
							["sort-name"] = json_string_field(raw_artist, "sort-name"),
						}
						else nil,
					attributes = attributes,
				})
			end
		end
	end
	return {
		relations = relations,
	}
end

function module.get_base_url()
	return BASE_URL
end

local function normalize_preferred_language(language_code: string?): string?
	local normalized = string.lower(trim_text(language_code or ""))
	if normalized == "" then
		return nil
	end
	return normalized
end

local function normalize_preferred_country(country: string?): string?
	local normalized = string.upper(trim_text(country or ""))
	if normalized == "" then
		return nil
	end
	return normalized
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

	if not ok or type(response) ~= "table" then
		return nil, 0
	end
	if not response.success or response.status_code ~= 200 then
		return nil, response.retries
	end
	local decoded_ok, data = pcall(json.decode, response.body :: string)
	if not decoded_ok then
		return nil, response.retries
	end
	if type(data) ~= "table" then
		return nil, response.retries
	end
	return data :: JsonObject, response.retries
end

local function compute_track_count(release: Release): number
	local release_track_count = release["track-count"]
	if type(release_track_count) == "number" then
		return release_track_count
	end

	local total = 0
	local media = release.media
	if media == nil then
		return total
	end
	for _, medium: ReleaseMedium in media do
		local medium_count = medium["track-count"]
		if not medium_count and medium.tracks then
			medium_count = #medium.tracks
		end
		total += (medium_count or 0)
	end
	if total > 0 then
		release["track-count"] = total
	end
	return total
end

local function medium_has_track_count(release: Release, expected_count: number): boolean
	local release_track_count = release["track-count"]
	if type(release_track_count) == "number" and release_track_count == expected_count then
		return true
	end
	local summed_track_count = 0
	local media = release.media
	if media == nil then
		return false
	end
	for _, medium: ReleaseMedium in media do
		local medium_count = medium["track-count"]
		if not medium_count and medium.tracks then
			medium_count = #medium.tracks
		end
		if medium_count == expected_count then
			return true
		end
		if type(medium_count) == "number" then
			summed_track_count += medium_count
		end
	end
	if summed_track_count > 0 and summed_track_count == expected_count then
		return true
	end
	return false
end

local function browse_release_group(release_group_id: string, fast_mode: boolean, track_count: number?): { Release }?
	local detailed_response = mb_get(
		`/release?release-group={release_group_id}&inc=recordings+artist-credits+genres+media+release-groups+recording-level-rels+work-rels+artist-rels&limit=100`
	)
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
		if not release["track-count"] then
			release["track-count"] = compute_track_count(release)
		end
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

local function release_context_track_count(release_context: ReleaseContext?): number
	local track_list = if release_context then release_context.tracks else nil
	local track_count = if track_list then #track_list else 0
	if track_list then
		for _, track: LocalTrack in track_list do
			if
				track.track_total
				and track.track_total > track_count
				and track_count >= math.ceil(track.track_total * 0.5)
			then
				track_count = track.track_total
				break
			end
		end
	end
	return track_count
end

local function lookup_context_artists(artists: { LocalArtist }?): { LookupArtist }?
	if artists == nil then
		return nil
	end
	local lookup_artists: { LookupArtist } = {}
	for _, artist: LocalArtist in artists do
		local lookup_artist: LookupArtist = {
			artist_name = artist.artist_name,
		}
		table.insert(lookup_artists, lookup_artist)
	end
	return lookup_artists
end

local function lookup_context_tracks(tracks: { LocalTrack }?): { LookupTrack }?
	if tracks == nil then
		return nil
	end
	local lookup_tracks: { LookupTrack } = {}
	for _, track: LocalTrack in tracks do
		local lookup_track: LookupTrack = {
			track_title = track.track_title,
			track_total = track.track_total,
			artists = lookup_context_artists(track.artists),
			disc = track.disc,
			track = track.track,
			duration_ms = track.duration_ms,
		}
		table.insert(lookup_tracks, lookup_track)
	end
	return lookup_tracks
end

local function normalize_lookup_context(release_context: ReleaseContext?): LookupContext?
	if release_context == nil then
		return nil
	end
	return {
		lookup_hints = release_context.lookup_hints,
		release_title = release_context.release_title,
		artists = lookup_context_artists(release_context.artists),
		tracks = lookup_context_tracks(release_context.tracks),
		library = release_context.library,
		library_id = release_context.library_id,
	}
end

local function release_lookup_inputs(
	release_context: ReleaseContext?,
	preferred_language: string?,
	preferred_country: string?,
	fallback_title: string?
)
	local lookup_context = normalize_lookup_context(release_context)
	local inferred_artist_hint, inferred_release_hint = infer_lookup_hints(lookup_context)
	local release_title = if lookup_context then lookup_context.release_title else nil
	if (is_blank_text(release_title) or is_placeholder_text(release_title)) and inferred_release_hint then
		release_title = inferred_release_hint
	end
	if
		(is_blank_text(release_title) or is_placeholder_text(release_title))
		and type(fallback_title) == "string"
		and fallback_title ~= ""
	then
		release_title = fallback_title
	end

	local artist_name = lookup_context
		and lookup_context.artists
		and lookup_context.artists[1]
		and lookup_context.artists[1].artist_name
	if is_blank_text(artist_name) or is_generic_artist(artist_name) then
		artist_name = nil
	end
	local local_tracks = if release_context then release_context.tracks else nil
	local compilation_release = is_compilation(local_tracks)
	if not artist_name then
		if compilation_release then
			artist_name = nil -- signals title-only search
		else
			artist_name = artist_name_from_tracks(local_tracks)
		end
	end
	if is_blank_text(artist_name) or is_generic_artist(artist_name) then
		artist_name = nil
	end
	if not artist_name and inferred_artist_hint and not is_generic_artist(inferred_artist_hint) then
		artist_name = inferred_artist_hint
	end

	local library = if lookup_context then lookup_context.library else nil
	local library_id = if lookup_context then lookup_context.library_id else nil
	if not library and library_id then
		local lib_results = libraries.list(library_id)
		local first_library = if lib_results then lib_results[1] else nil
		if first_library then
			library = {
				language = first_library.language,
				country = first_library.country,
			}
		end
	end
	if library then
		if preferred_language == nil then
			preferred_language = normalize_preferred_language(library.language)
		end
		if preferred_country == nil then
			preferred_country = normalize_preferred_country(library.country)
		end
	end

	return {
		artist_name = artist_name,
		release_title = release_title,
		track_count = release_context_track_count(release_context),
		preferred_language = preferred_language,
		preferred_country = preferred_country,
		compilation_release = compilation_release,
	}
end

local function cover_cache_context(release_context: ReleaseContext?)
	local ids = if release_context
		then release_context.ids or musicbrainz_external_ids(release_context.external_ids)
		else nil
	local lookup_input = release_lookup_inputs(release_context)
	local search_title = clean_release_title(lookup_input.release_title or "")
	if search_title == "" and release_context and release_context.tracks and #release_context.tracks > 0 then
		local first_track = release_context.tracks[1]
		search_title = if first_track then first_track.track_title or "" else ""
	end

	return {
		release_id = if ids and type(ids.release_id) == "string" and ids.release_id ~= "" then ids.release_id else nil,
		release_group_id = if ids
				and type(ids.release_group_id) == "string"
				and ids.release_group_id ~= ""
			then ids.release_group_id
			else nil,
		track_count = if lookup_input.track_count > 0 then lookup_input.track_count else nil,
		preferred_language = lookup_input.preferred_language,
		preferred_country = lookup_input.preferred_country,
		search_title = if search_title ~= "" then search_title else nil,
	}
end

function module.cover_cache_context(release_context: ReleaseContext?)
	return cover_cache_context(release_context)
end

local function release_artist_ids(release: Release?): ArtistIdSet?
	local artist_ids: ArtistIdSet? = nil
	local artist_credits = if release then release["artist-credit"] else nil
	if artist_credits == nil then
		return nil
	end
	for _, credit: ReleaseArtistCredit in artist_credits do
		local artist = credit.artist
		if artist and artist.id then
			local resolved_artist_ids: ArtistIdSet = if artist_ids ~= nil then artist_ids else {}
			artist_ids = resolved_artist_ids
			resolved_artist_ids[artist.id] = true
		end
	end
	return artist_ids
end

local function release_has_front_cover(release: Release?): boolean
	local cover_archive = release and release["cover-art-archive"]
	return release ~= nil and release.id ~= nil and cover_archive ~= nil and cover_archive.front == true
end

local function max_track_count_diff(track_count, is_edition_mode)
	if track_count == nil then
		return 0
	end
	if not is_edition_mode then
		return 1
	end
	if track_count <= 1 then
		return 6
	end
	if track_count <= 4 then
		return 3
	end
	return math.max(2, math.floor(track_count * 0.2))
end

local function should_prefer_release_on_tie(
	candidate: Release,
	current: Release,
	preferred_country: string?,
	track_count: number?
): boolean
	if track_count ~= nil then
		local candidate_count = compute_track_count(candidate)
		local current_count = compute_track_count(current)
		local candidate_diff = if candidate_count > 0 then math.abs(candidate_count - track_count) else math.huge
		local current_diff = if current_count > 0 then math.abs(current_count - track_count) else math.huge

		if candidate_diff ~= current_diff then
			return candidate_diff < current_diff
		end
	end
	local candidate_country = preferred_country ~= nil and candidate.country == preferred_country
	local current_country = preferred_country ~= nil and current.country == preferred_country
	if candidate_country ~= current_country then
		return candidate_country
	end
	local candidate_official = candidate.status == "Official"
	local current_official = current.status == "Official"
	if candidate_official ~= current_official then
		return candidate_official
	end
	local candidate_packaging = candidate.packaging and candidate.packaging ~= "" and candidate.packaging ~= "None"
	local current_packaging = current.packaging and current.packaging ~= "" and current.packaging ~= "None"
	if candidate_packaging ~= current_packaging then
		return not candidate_packaging
	end
	local candidate_date = candidate.date or ""
	local current_date = current.date or ""
	if candidate_date ~= current_date then
		return candidate_date < current_date
	end
	return false
end

local function release_matches_artist_ids(release: Release?, artist_ids: ArtistIdSet?): boolean
	if not release or not artist_ids then
		return false
	end
	local artist_credits = release["artist-credit"]
	if artist_credits == nil then
		return false
	end
	for _, credit: ReleaseArtistCredit in artist_credits do
		local artist = credit.artist
		local artist_id = artist and artist.id
		if artist_id and artist_ids[artist_id] then
			return true
		end
	end
	return false
end

-- Release scoring:
-- artist MBID > title > track-count fit > status/cover/language/country.
local function select_best_release(
	releases: { Release },
	track_count: number?,
	preferred_language: string?,
	preferred_country: string?,
	expected_title: string?,
	artist_ids: ArtistIdSet?,
	local_tracks: { LocalTrack }?,
	artist_mbid_weight: number?
): Release?
	local best: Release? = nil
	local best_score = -1
	local best_title_matched = false
	local mbid_weight = artist_mbid_weight or 16
	local is_edition_mode = artist_mbid_weight ~= nil
	local norm_expected = if expected_title then normalize_title(expected_title) else nil
	local local_track_total_hint = max_declared_track_total(local_tracks)
	local local_disc_hint: number? = nil
	if local_tracks and #local_tracks > 0 then
		local max_disc = 0
		local max_disc_total = 0
		local seen_discs = {}
		local unique_discs = 0
		for _, track in local_tracks do
			local disc = track.disc
			if type(disc) == "number" and disc > 0 then
				if disc > max_disc then
					max_disc = disc
				end
				if not seen_discs[disc] then
					seen_discs[disc] = true
					unique_discs += 1
				end
			end
			local disc_total = track.disc_total
			if type(disc_total) == "number" and disc_total > max_disc_total then
				max_disc_total = disc_total
			end
		end
		local has_explicit_multidisc_evidence = max_disc > 1 or unique_discs > 1
		if has_explicit_multidisc_evidence then
			local inferred_disc_hint = math.max(max_disc, max_disc_total, unique_discs)
			if inferred_disc_hint > 1 then
				local_disc_hint = inferred_disc_hint
			end
		end
	end

	for _, release in releases do
		local release_track_count: number? = compute_track_count(release)
		if release_track_count == 0 then
			release_track_count = nil
		end
		local track_diff: number? = nil
		if track_count ~= nil and release_track_count then
			track_diff = math.abs(release_track_count - track_count)
			if track_diff > max_track_count_diff(track_count, is_edition_mode) then
				continue
			end
		end
		local score = 0
		local text_rep = release["text-representation"]
		local country = release.country
		local lang_matches = preferred_language ~= nil and text_rep ~= nil and text_rep.language == preferred_language
		local release_media_count = 0
		local medium_matches_local_track_total = false
		local media = release.media
		if media ~= nil then
			for _, medium: ReleaseMedium in media do
				release_media_count += 1
				local medium_track_count = medium["track-count"]
				if not medium_track_count and medium.tracks then
					medium_track_count = #medium.tracks
				end
				if
					local_track_total_hint ~= nil
					and local_track_total_hint > 1
					and medium_track_count == local_track_total_hint
				then
					medium_matches_local_track_total = true
				end
			end
		end
		if track_diff ~= nil then
			if track_diff == 0 then
				score += 6
			elseif track_diff == 1 then
				score += 3
			else
				score -= track_diff
			end
		elseif track_count ~= nil then
			score -= 2
		end
		if release.status == "Official" then
			score += 4
		elseif release.status == "Promotion" then
			score -= if is_edition_mode then 8 else 3
		elseif release.status == "Pseudo-Release" then
			score -= if is_edition_mode then 4 else 3
		end
		if preferred_country and country == preferred_country then
			score += 2
		end
		if lang_matches and contains_multibyte(release.title) then
			score += 3
		end
		if local_disc_hint and release_media_count > 0 then
			if release_media_count == local_disc_hint then
				score += 10
			elseif release_media_count == 1 then
				score -= 6
			else
				score -= math.min(math.abs(release_media_count - local_disc_hint) * 2, 6)
			end
		end
		if medium_matches_local_track_total then
			score += if local_disc_hint then 4 else 2
		end
		if artist_ids and release_matches_artist_ids(release, artist_ids) then
			score += mbid_weight
		end
		local release_title_matched = false
		if norm_expected then
			local title_matched = false
			if expected_title == release.title then
				score += 4
				title_matched = true
			end
			local norm_release = normalize_title(release.title)
			local exact_title_bonus = 8
			local prefix_title_bonus = 6
			if is_edition_mode and track_count == 1 and release.status ~= "Official" then
				exact_title_bonus = 2
				prefix_title_bonus = 1
				score -= 7
			end
			if norm_release == norm_expected then
				score += exact_title_bonus
				title_matched = true
			elseif #norm_release >= 3 and #norm_expected >= 3 then
				if string.sub(norm_expected, 1, #norm_release) == norm_release then
					score += prefix_title_bonus
					title_matched = true
				elseif string.sub(norm_release, 1, #norm_expected) == norm_expected then
					local remainder = string.sub(norm_release, #norm_expected + 1)
					if string.match(remainder, "^%s*%(") or string.match(remainder, "^%s*%[") then
						score += 1
					else
						score += prefix_title_bonus
					end
					title_matched = true
				end
			end
			if not title_matched and not is_edition_mode then
				score -= if artist_ids then 6 else 3
			end
			release_title_matched = title_matched
		end
		local cover_archive = release["cover-art-archive"]
		if cover_archive and cover_archive.front then
			score += if is_edition_mode then 1 else 4
		end
		if local_tracks and #local_tracks > 0 and release.media then
			local mb_titles: { [string]: boolean } = {}
			for _, medium: ReleaseMedium in release.media do
				local medium_tracks = medium.tracks
				if medium_tracks == nil then
					continue
				end
				for _, track: ReleaseMediumTrack in medium_tracks do
					local norm = normalize_overlap_title(track.title)
					if norm ~= "" then
						mb_titles[norm] = true
					end
				end
			end
			local matches = 0
			for _, lt in local_tracks do
				local norm = normalize_overlap_title(lt.track_title)
				if norm ~= "" and mb_titles[norm] then
					matches += 1
				end
			end
			local track_score = matches * 2
			if is_edition_mode then
				local unmatched = #local_tracks - matches
				if unmatched > 1 then
					track_score = 0
				else
					track_score = (math.min(matches, 4) * 2) - (unmatched * 2)
					if track_score < 0 then
						track_score = 0
					end
				end
			end
			score += track_score
		end
		local choose_release = false
		if score > best_score then
			choose_release = true
		elseif score == best_score and best then
			local tie_handled = false
			if is_edition_mode and norm_expected then
				local norm_release = normalize_title(release.title)
				local norm_best = normalize_title(best.title)
				if norm_release == norm_expected and norm_best == norm_expected then
					local release_date = release.date
					local best_date = best.date
					if release_date and best_date and release_date ~= best_date then
						local release_year = tonumber(string.sub(release_date, 1, 4))
						local best_year = tonumber(string.sub(best_date, 1, 4))
						if release_year and best_year and math.abs(release_year - best_year) >= 5 then
							tie_handled = true
							choose_release = release_date > best_date
						end
					end
				end
			end
			if not tie_handled and should_prefer_release_on_tie(release, best, preferred_country, track_count) then
				choose_release = true
			end
		end
		if choose_release then
			best_score = score
			best = release
			best_title_matched = release_title_matched
		end
	end
	local min_score = if is_edition_mode then 4 else 8
	if best_score < min_score then
		return nil
	end
	if not best_title_matched and not is_edition_mode and norm_expected and #releases > 0 then
		local fallback: Release? = nil
		local fallback_score = -1
		for _, release in releases do
			if artist_ids and not release_matches_artist_ids(release, artist_ids) then
				continue
			end
			local norm_release = normalize_title(release.title)
			local has_title_prefix = norm_release ~= ""
				and #norm_release >= 3
				and (norm_release == norm_expected or string.sub(norm_expected, 1, #norm_release) == norm_release)
			if not has_title_prefix then
				continue
			end
			local score = 0
			if artist_ids then
				score += mbid_weight
			end
			if norm_release == norm_expected then
				score += 8
			else
				score += 6
			end
			if release.status == "Official" then
				score += 4
			end
			if score > fallback_score then
				fallback_score = score
				fallback = release
			end
		end
		if fallback and fallback_score > best_score then
			return fallback
		end
	end
	return best
end

local function set_artist_metadata(db_id: number?, mb_artist: ReleaseArtist?): boolean
	local current_provider = provider
	if not db_id or mb_artist == nil or not mb_artist.id or current_provider == nil then
		return false
	end
	local artist_layer = current_provider:layer(db_id)
	artist_layer:set_field("artist_name", mb_artist.name)
	if mb_artist["sort-name"] then
		artist_layer:set_field("sort_name", mb_artist["sort-name"])
	end
	if mb_artist["type"] then
		artist_layer:set_field("artist_type", string.lower(mb_artist["type"]))
	end
	artist_layer:set_id("artist_id", mb_artist.id)
	artist_layer:save()
	return true
end

local function ensure_artist_entity(mb_artist: ReleaseArtist?): number?
	local current_provider = provider
	if mb_artist == nil or not mb_artist.id or current_provider == nil then
		return nil
	end
	local artist_name = trim_text(mb_artist.name)
	if artist_name == "" or is_generic_artist(artist_name) then
		return nil
	end
	local request: EnsureArtistRequest = {
		id_type = "artist_id",
		id_value = mb_artist.id,
		artist_name = artist_name,
	}
	if mb_artist["sort-name"] then
		request.sort_name = mb_artist["sort-name"]
	end
	if mb_artist["type"] then
		request.artist_type = string.lower(mb_artist["type"])
	end
	local ok, artist_id = pcall(current_provider.ensure_artist, current_provider, request)
	if not ok or not artist_id then
		return nil
	end
	return artist_id
end

type ArtistRelationType = metadata.ArtistRelationType
local ArtistRelationType = metadata.ArtistRelationType

local MB_ARTIST_RELATION_MAP: { [string]: ArtistRelationType } = {
	["voice actor"] = ArtistRelationType.VoiceActor,
	["member of band"] = ArtistRelationType.MemberOf,
}

local function fetch_and_link_artist_relations(
	from_artist_db_id: number,
	mb_artist_id: string,
	mb_artist_type: string?
): boolean
	local current_provider = provider
	if current_provider == nil then
		return false
	end

	local normalized_type = if mb_artist_type then string.lower(mb_artist_type) else nil
	if normalized_type ~= "character" and normalized_type ~= "group" then
		return false
	end

	local response = mb_get(`/artist/{mb_artist_id}?inc=artist-rels`)
	if response == nil then
		return false
	end

	local relations = response.relations
	if type(relations) ~= "table" then
		return false
	end

	local linked_count = 0
	for _, relation in relations :: { any } do
		if type(relation) ~= "table" then
			continue
		end
		local target_type = relation["target-type"]
		if target_type ~= "artist" then
			continue
		end

		local relation_type_str = relation["type"]
		if type(relation_type_str) ~= "string" then
			continue
		end
		local relation_type = MB_ARTIST_RELATION_MAP[string.lower(relation_type_str)]
		if relation_type == nil then
			continue
		end

		local related_artist = relation.artist
		if type(related_artist) ~= "table" or not related_artist.id or not related_artist.name then
			continue
		end

		local related_artist_db_id = ensure_artist_entity(related_artist :: ReleaseArtist)
		if related_artist_db_id == nil then
			continue
		end

		local attributes = relation.attributes
		local attributes_str: string? = nil
		if type(attributes) == "table" and #attributes > 0 then
			local attr_parts: { string } = {}
			for _, attr in attributes :: { any } do
				if type(attr) == "string" then
					table.insert(attr_parts, attr)
				end
			end
			if #attr_parts > 0 then
				attributes_str = table.concat(attr_parts, ", ")
			end
		end

		local link_ok = pcall(
			current_provider.link_artist_relation,
			current_provider,
			related_artist_db_id,
			from_artist_db_id,
			relation_type,
			attributes_str
		)
		if link_ok then
			linked_count += 1
		end
	end

	return linked_count > 0
end

local function extract_track_work_id_from_relations(relations: { Relation }?): string?
	local fallback: string? = nil
	if relations == nil then
		return nil
	end
	for _, relation: Relation in relations do
		if relation["target-type"] ~= "work" then
			continue
		end
		local work = relation.work
		if work == nil or type(work.id) ~= "string" or work.id == "" then
			continue
		end
		local relation_type = string.lower(relation.type or "")
		if relation_type == "performance" then
			return work.id
		end
		if not fallback then
			fallback = work.id
		end
	end
	return fallback
end

type RecordingCache = { [string]: RecordingResult | false }

local function fetch_recording_result(recording_id: string?, recording_cache: RecordingCache?): RecordingResult?
	if type(recording_id) ~= "string" or recording_id == "" then
		return nil
	end
	if recording_cache and recording_cache[recording_id] ~= nil then
		local cached = recording_cache[recording_id]
		if cached == false then
			return nil
		end
		return cached :: RecordingResult
	end
	local recording_response = mb_get(`/recording/{recording_id}?inc=work-rels+artist-rels`)
	local result = if recording_response then normalize_recording_result(recording_response) else nil
	if recording_cache then
		recording_cache[recording_id] = result or false
	end
	return result
end

type WorkCache = { [string]: { Relation } | false }

local function fetch_work_relations(work_id: string?, work_cache: WorkCache?): { Relation }?
	if type(work_id) ~= "string" or work_id == "" then
		return nil
	end
	if work_cache and work_cache[work_id] ~= nil then
		local cached = work_cache[work_id]
		if cached == false then
			return nil
		end
		return cached :: { Relation }
	end
	local work_response = mb_get(`/work/{work_id}?inc=artist-rels`)
	local relations: { Relation }? = nil
	if work_response then
		local raw_relations = json_array_field(work_response, "relations")
		if raw_relations then
			relations = {}
			for _, raw_relation: JsonValue in raw_relations do
				if type(raw_relation) == "table" then
					local relation = raw_relation :: JsonObject
					local raw_artist = json_object_field(relation, "artist")
					local raw_attributes = json_array_field(relation, "attributes")
					local attributes: { string }? = nil
					if raw_attributes ~= nil then
						attributes = {}
						for _, attr: JsonValue in raw_attributes do
							if type(attr) == "string" then
								table.insert(attributes :: { string }, attr :: string)
							end
						end
					end
					if raw_artist ~= nil then
						local parsed_relation: Relation = {
							type = json_string_field(relation, "type"),
							["target-type"] = json_string_field(relation, "target-type"),
							artist = {
								id = json_string_field(raw_artist, "id"),
								name = json_string_field(raw_artist, "name"),
								["sort-name"] = json_string_field(raw_artist, "sort-name"),
							},
							attributes = attributes,
						}
						table.insert(relations :: { Relation }, parsed_relation)
					end
				end
			end
		end
	end
	if work_cache then
		work_cache[work_id] = relations or false
	end
	return relations
end

type CreditType = metadata.CreditType
local CreditType = metadata.CreditType

local MB_RELATION_TO_CREDIT: { [string]: { type: CreditType, detail: string? } } = {
	-- performance
	vocal = { type = CreditType.Vocalist },
	instrument = { type = CreditType.Instrumentalist },
	conductor = { type = CreditType.Conductor },
	-- composition
	arranger = { type = CreditType.Arranger },
	composer = { type = CreditType.Composer },
	lyricist = { type = CreditType.Lyricist },
	writer = { type = CreditType.Writer },
	-- production
	producer = { type = CreditType.Producer },
	engineer = { type = CreditType.Engineer },
	["audio engineer"] = { type = CreditType.Engineer, detail = "audio" },
	["sound engineer"] = { type = CreditType.Engineer, detail = "sound" },
	mix = { type = CreditType.Mixer },
	["recording engineer"] = { type = CreditType.Engineer, detail = "recording" },
	["balance engineer"] = { type = CreditType.Engineer, detail = "balance" },
	editor = { type = CreditType.Engineer, detail = "editor" },
	["instrument arranger"] = { type = CreditType.Arranger, detail = "instrument" },
	-- remix
	remixer = { type = CreditType.Remixer },
	["dj-mixer"] = { type = CreditType.Mixer, detail = "DJ mix" },
}

local function link_artist_credits_from_relations(track_db_id: number, relations: { Relation })
	local current_provider = provider
	if current_provider == nil then
		return
	end
	for _, relation: Relation in relations do
		if relation["target-type"] ~= "artist" then
			continue
		end
		local relation_type = string.lower(relation.type or "")
		local mapping = MB_RELATION_TO_CREDIT[relation_type]
		if mapping == nil then
			continue
		end
		local artist = relation.artist
		if artist == nil or not artist.id or not artist.name then
			continue
		end

		local artist_id = ensure_artist_entity({
			id = artist.id,
			name = artist.name,
			["sort-name"] = artist["sort-name"],
		} :: ReleaseArtist)
		if not artist_id then
			continue
		end

		local detail = mapping.detail
		local attributes = relation.attributes
		if attributes and #attributes > 0 then
			detail = table.concat(attributes, ", ")
		end

		pcall(current_provider.link_credit, current_provider, track_db_id, artist_id, mapping.type, detail)
	end
end

local function titles_match_search(candidate_title: string?, search_title: string?): boolean
	local norm_candidate = normalize_title(candidate_title)
	local norm_search = normalize_title(search_title)
	if norm_candidate == norm_search then
		return true
	end
	if #norm_candidate < 3 or #norm_search < 3 then
		return false
	end
	return string.sub(norm_search, 1, #norm_candidate) == norm_candidate
		or string.sub(norm_candidate, 1, #norm_search) == norm_search
end

local function for_each_local_artist(release_context: ReleaseContext, visit: (LocalArtist) -> ())
	if release_context.artists then
		for _, artist: LocalArtist in release_context.artists do
			visit(artist)
		end
	end
	for _, track: LocalTrack in release_context.tracks do
		local artists = track.artists
		if artists == nil then
			continue
		end
		for _, artist: LocalArtist in artists do
			visit(artist)
		end
	end
end

local function maybe_reuse_release_media(selected_release: Release?, edition_release: Release?): Release?
	if not selected_release or not edition_release then
		return edition_release
	end
	if edition_release.media and #edition_release.media > 0 then
		return edition_release
	end
	if not selected_release.media or #selected_release.media == 0 then
		return edition_release
	end
	local selected_group = selected_release["release-group"]
	local edition_group = edition_release["release-group"]
	local selected_group_id = selected_group and selected_group.id
	local edition_group_id = edition_group and edition_group.id
	if selected_group_id and edition_group_id and selected_group_id ~= edition_group_id then
		return edition_release
	end
	local selected_count = compute_track_count(selected_release)
	local edition_count = compute_track_count(edition_release)
	if selected_count > 0 and edition_count > 0 and math.abs(selected_count - edition_count) > 1 then
		return edition_release
	end
	local merged: Release = {
		id = edition_release.id,
		title = edition_release.title,
		status = edition_release.status,
		country = edition_release.country,
		date = edition_release.date,
		barcode = edition_release.barcode,
		packaging = edition_release.packaging,
		score = edition_release.score,
		media = selected_release.media,
		genres = edition_release.genres,
		["track-count"] = if edition_release["track-count"] ~= nil and edition_release["track-count"] ~= 0
			then edition_release["track-count"]
			else selected_count,
		["artist-credit"] = edition_release["artist-credit"],
		["release-group"] = edition_release["release-group"],
		["text-representation"] = edition_release["text-representation"],
		["cover-art-archive"] = edition_release["cover-art-archive"],
	}
	return merged
end

local function build_credit_lookup(credits: { ReleaseArtistCredit }): { [string]: ReleaseArtist }
	local by_name: { [string]: ReleaseArtist } = {}
	for _, credit: ReleaseArtistCredit in credits do
		local mb_artist = credit.artist
		if mb_artist and credit.name then
			by_name[string.lower(credit.name)] = mb_artist
		end
	end
	return by_name
end

local function match_credit(
	local_name_lower: string,
	credits: { ReleaseArtistCredit },
	by_name: { [string]: ReleaseArtist }
): ReleaseArtist?
	local exact = by_name[local_name_lower]
	if exact then
		return exact
	end
	for _, credit: ReleaseArtistCredit in credits do
		if not credit.artist or not credit.name then
			continue
		end
		local jp = credit.joinphrase or ""
		if not string.find(jp, "(", 1, true) and not string.find(jp, "（", 1, true) then
			continue
		end
		local prefix = string.lower(credit.name)
		if #local_name_lower > #prefix and string.sub(local_name_lower, 1, #prefix) == prefix then
			local rest = string.gsub(string.sub(local_name_lower, #prefix + 1), "^%s+", "")
			if string.sub(rest, 1, 1) == "(" or string.sub(rest, 1, 3) == "（" then
				return credit.artist
			end
		end
	end
	if #local_name_lower >= 2 then
		for _, credit: ReleaseArtistCredit in credits do
			if not credit.artist or not credit.name then
				continue
			end
			local credit_name = string.lower(credit.name)
			if
				string.find(credit_name, local_name_lower, 1, true)
				or string.find(local_name_lower, credit_name, 1, true)
			then
				return credit.artist
			end
		end
	end
	return nil
end

local function process_release(
	release: Release,
	release_context: ReleaseContext,
	release_group_id: string?,
	release_group_info: ReleaseGroup?
): boolean
	local current_provider = provider
	if current_provider == nil then
		return false
	end
	local release_db_id = release_context.db_id
	local release_id = release.id
	if release_db_id == nil or release_id == nil then
		return false
	end

	local rg_info = release_group_info or release["release-group"]
	local release_layer = current_provider:layer(release_db_id)
	local release_title = release.title
	local release_track_count = compute_track_count(release)

	if #release_context.tracks == 1 and release_track_count > 1 then
		local local_release_title = release_context.release_title
		local local_track_title = release_context.tracks[1] and release_context.tracks[1].track_title
		local norm_local_release = normalize_title(local_release_title)
		local norm_local_track = normalize_title(local_track_title)
		if
			norm_local_release ~= ""
			and norm_local_release == norm_local_track
			and normalize_title(release.title) ~= norm_local_release
		then
			release_title = local_release_title
		end
	end

	release_layer:set_field("release_title", release_title)

	local date_str = release.date or (rg_info and rg_info["first-release-date"])
	local rg_url_data: JsonObject? = nil
	if not date_str then
		local rg_id_for_date = release_group_id or (rg_info and rg_info.id)
		if rg_id_for_date then
			-- Include url-rels to extract WikiData QID in the same request.
			rg_url_data = mb_get(`/release-group/{rg_id_for_date}?inc=url-rels`)
			if rg_url_data then
				date_str = json_string_field(rg_url_data, "first-release-date") or date_str
			end
		end
	end
	if date_str then
		release_layer:set_field("release_date", date_str)
	end

	local genre_names: { JsonValue } = {}
	local release_genres = release.genres
	if release_genres ~= nil then
		for _, genre: ReleaseGenre in release_genres do
			if genre.name then
				table.insert(genre_names, genre.name)
				genres.add(release_db_id, {
					name = genre.name,
					external_id = if genre.id
						then {
							provider_id = "musicbrainz",
							id_type = "genre_id",
							id = genre.id,
						}
						else nil,
				})
			end
		end
	end
	if #genre_names > 0 then
		release_layer:set_field("genres", genre_names)
	end

	local primary_type: string? = if rg_info then rg_info["primary-type"] else nil
	if not primary_type and rg_url_data then
		primary_type = json_string_field(rg_url_data, "primary-type")
	end
	if primary_type then
		release_layer:set_field("release_type", string.lower(primary_type))
	end

	release_layer:set_id("release_id", release_id)
	local rg_id = release_group_id or (rg_info and rg_info.id)
	if rg_id then
		release_layer:set_id("release_group_id", rg_id)

		-- Cooperation: if we already fetched release-group data (date fallback),
		-- extract WikiData QID from its URL relationships at zero extra cost.
		-- No dedicated fetch — the WikiData plugin handles releases MB doesn't cover.
		if rg_url_data then
			local url_relations = json_array_field(rg_url_data, "relations")
			if url_relations then
				for _, rel in url_relations do
					if type(rel) ~= "table" then
						continue
					end
					local rel_obj = rel :: JsonObject
					local rel_type = json_string_field(rel_obj, "type")
					if rel_type == "wikidata" then
						local url_obj = json_object_field(rel_obj, "url")
						local resource = json_string_field(url_obj, "resource")
						if resource then
							local qid = string.match(resource, "(Q%d+)$")
							if qid then
								release_layer:set_id("wikidata_qid", qid)
								break
							end
						end
					end
				end
			end
		end
	end
	release_layer:save()

	local artist_credits = release["artist-credit"]
	local tagged_artist_ids: { [number]: boolean } = {}
	local tagged_artist_mb_ids: { [number]: string } = {}
	local fetched_relations_for: { [string]: boolean } = {}

	local function maybe_fetch_artist_relations(db_id: number, mb_artist: ReleaseArtist)
		if mb_artist.id == nil or fetched_relations_for[mb_artist.id] then
			return
		end
		fetched_relations_for[mb_artist.id] = true
		fetch_and_link_artist_relations(db_id, mb_artist.id, mb_artist["type"])
	end

	local function tag_local_artist(db_id: number?, mb_artist: ReleaseArtist?): boolean
		if not db_id or mb_artist == nil or mb_artist.id == nil then
			return false
		end
		ensure_artist_entity(mb_artist)
		if set_artist_metadata(db_id, mb_artist) then
			tagged_artist_ids[db_id] = true
			tagged_artist_mb_ids[db_id] = mb_artist.id
			maybe_fetch_artist_relations(db_id, mb_artist)
			return true
		end
		return false
	end

	if artist_credits and release_context.artists then
		local matched_release_mb_artist_ids: { [string]: boolean } = {}
		local mb_release_artists_by_name = build_credit_lookup(artist_credits)

		for _, local_artist: LocalArtist in release_context.artists do
			if local_artist.db_id and not tagged_artist_ids[local_artist.db_id] then
				local local_name = string.lower(local_artist.artist_name or "")
				local matched = match_credit(local_name, artist_credits, mb_release_artists_by_name)
				if matched and matched.id and tag_local_artist(local_artist.db_id, matched) then
					matched_release_mb_artist_ids[matched.id] = true
				end
			end
		end

		local untagged_ids: { [number]: boolean } = {}
		local untagged_first: LocalArtist? = nil
		local unique_untagged = 0
		for _, local_artist: LocalArtist in release_context.artists do
			local local_artist_id = local_artist.db_id
			if local_artist_id and not tagged_artist_ids[local_artist_id] and not untagged_ids[local_artist_id] then
				untagged_ids[local_artist_id] = true
				unique_untagged += 1
				if untagged_first == nil then
					untagged_first = local_artist
				end
			end
		end

		local first_mb_credit = artist_credits[1]
		local first_mb_artist = if first_mb_credit then first_mb_credit.artist else nil
		if
			unique_untagged == 1
			and #artist_credits == 1
			and untagged_first ~= nil
			and first_mb_artist ~= nil
			and first_mb_artist.id ~= nil
			and not matched_release_mb_artist_ids[first_mb_artist.id]
			and tag_local_artist(untagged_first.db_id, first_mb_artist)
		then
			matched_release_mb_artist_ids[first_mb_artist.id] = true
		end

		for _, mb_credit: ReleaseArtistCredit in artist_credits do
			local mb_artist = mb_credit.artist
			if mb_artist and mb_artist.id and not matched_release_mb_artist_ids[mb_artist.id] then
				local ensured_artist_id = ensure_artist_entity(mb_artist)
				if ensured_artist_id then
					current_provider:link_credit(release_db_id, ensured_artist_id, nil, nil)
					tagged_artist_ids[ensured_artist_id] = true
					tagged_artist_mb_ids[ensured_artist_id] = mb_artist.id
					matched_release_mb_artist_ids[mb_artist.id] = true
					maybe_fetch_artist_relations(ensured_artist_id, mb_artist)
				end
			end
		end
	end

	if #release_context.tracks > 0 then
		local mb_tracks = flatten_release_tracks(release)
		local local_tracks: { LocalTrack } = {}
		for _, track: LocalTrack in release_context.tracks do
			table.insert(local_tracks, track)
		end
		sort_tracks_for_refresh(local_tracks)

		local assignments = assign_mb_tracks_to_local_tracks(local_tracks, mb_tracks)
		local release_media = release.media
		local release_disc_total = if release_media ~= nil then #release_media else 0
		local inferred_disc_total = 0
		for i: number, local_track: LocalTrack in local_tracks do
			local mb_track = assignments[i]
			if mb_track and local_track.db_id then
				local assigned_disc = mb_track.disc
				if assigned_disc and assigned_disc > inferred_disc_total then
					inferred_disc_total = assigned_disc
				end
			end
		end

		local resolved_disc_total = if release_disc_total > 0 then release_disc_total else inferred_disc_total
		local first_local_track = local_tracks[1]
		local first_track_total = if first_local_track ~= nil then first_local_track.track_total else nil
		local preserve_single_track_position = #local_tracks == 1
			and first_track_total ~= nil
			and first_track_total <= 1
			and first_local_track ~= nil
			and first_local_track.track ~= nil
		local preserve_local_multidisc_position = should_preserve_local_track_positions(local_tracks, release)
		local preserve_local_position = preserve_single_track_position or preserve_local_multidisc_position
		local should_lookup_track_work = #local_tracks == 1
		local recording_cache: RecordingCache = {}
		local work_cache: WorkCache = {}
		local track_artist_count = 0

		for i: number, local_track: LocalTrack in local_tracks do
			local mb_track = assignments[i]
			if mb_track and local_track.db_id then
				local track_layer = current_provider:layer(local_track.db_id)
				track_layer:set_field("track_title", mb_track.title)
				if preserve_local_position then
					if local_track.disc ~= nil then
						track_layer:set_field("disc", local_track.disc)
					else
						track_layer:set_field("disc", mb_track.disc)
					end
					if local_track.track ~= nil then
						track_layer:set_field("track", local_track.track)
					else
						track_layer:set_field("track", mb_track.track)
					end
				else
					track_layer:set_field("disc", mb_track.disc)
					track_layer:set_field("track", mb_track.track)
				end
				if resolved_disc_total > 0 then
					track_layer:set_field("disc_total", resolved_disc_total)
				end
				local recording_relations: { Relation }? = nil
				if mb_track.recording_id then
					track_layer:set_id("recording_id", mb_track.recording_id)
					local inline_relations = mb_track.recording_relations
					if inline_relations then
						recording_relations = inline_relations :: { Relation }?
					end
					local work_id = extract_track_work_id_from_relations(recording_relations)
					if should_lookup_track_work and work_id == nil and recording_relations == nil then
						local result = fetch_recording_result(mb_track.recording_id, recording_cache)
						if result then
							work_id = extract_track_work_id_from_relations(result.relations)
							recording_relations = result.relations
						end
					end
					if work_id then
						track_layer:set_field("work_id", work_id)
						local work_relations = fetch_work_relations(work_id, work_cache)
						if work_relations and local_track.db_id then
							link_artist_credits_from_relations(local_track.db_id, work_relations)
						end
					end
				end
				track_layer:save()

				if recording_relations and local_track.db_id then
					link_artist_credits_from_relations(local_track.db_id, recording_relations)
				end

				if mb_track.artist_credits and local_track.artists then
					local mb_credit_list = mb_track.artist_credits
					local mb_artist_by_name = build_credit_lookup(mb_credit_list)
					local matched_mb_artist_ids: { [string]: boolean } = {}
					local unmatched_local_artists: { LocalArtist } = {}

					for _, track_artist: LocalArtist in local_track.artists do
						local track_artist_id = track_artist.db_id
						if track_artist_id ~= nil then
							local previously_tagged_mb_id = tagged_artist_mb_ids[track_artist_id]
							if previously_tagged_mb_id then
								matched_mb_artist_ids[previously_tagged_mb_id] = true
							elseif not tagged_artist_ids[track_artist_id] then
								local local_name = string.lower(track_artist.artist_name or "")
								local matched_mb_artist = match_credit(local_name, mb_credit_list, mb_artist_by_name)
								if matched_mb_artist and matched_mb_artist.id then
									matched_mb_artist_ids[matched_mb_artist.id] = true
									if tag_local_artist(track_artist_id, matched_mb_artist) then
										track_artist_count += 1
									end
								else
									table.insert(unmatched_local_artists, track_artist)
								end
							end
						end
					end

					local unmatched_mb_artists: { ReleaseArtist } = {}
					for _, mb_credit: ReleaseArtistCredit in mb_credit_list do
						local mb_artist = mb_credit.artist
						if mb_artist and mb_artist.id and not matched_mb_artist_ids[mb_artist.id] then
							table.insert(unmatched_mb_artists, mb_artist)
						end
					end

					local can_assign_by_order = #unmatched_local_artists > 0
						and #unmatched_local_artists == #unmatched_mb_artists
						and #unmatched_local_artists <= 4
					if can_assign_by_order then
						for idx: number, track_artist: LocalArtist in unmatched_local_artists do
							local mb_artist = unmatched_mb_artists[idx]
							if
								track_artist.db_id
								and mb_artist ~= nil
								and mb_artist.id ~= nil
								and not tagged_artist_ids[track_artist.db_id]
								and tag_local_artist(track_artist.db_id, mb_artist)
							then
								matched_mb_artist_ids[mb_artist.id] = true
								track_artist_count += 1
							end
						end
					end

					for _, mb_artist: ReleaseArtist in unmatched_mb_artists do
						if local_track.db_id and mb_artist.id and not matched_mb_artist_ids[mb_artist.id] then
							local ensured_artist_id = ensure_artist_entity(mb_artist)
							if ensured_artist_id then
								current_provider:link_credit(local_track.db_id, ensured_artist_id, nil, nil)
								tagged_artist_ids[ensured_artist_id] = true
								tagged_artist_mb_ids[ensured_artist_id] = mb_artist.id
								matched_mb_artist_ids[mb_artist.id] = true
								track_artist_count += 1
								maybe_fetch_artist_relations(ensured_artist_id, mb_artist)
							end
						end
					end
				end
			end
		end

		local release_primary = artist_credits and artist_credits[1] and artist_credits[1].artist
		local should_override_with_track_artist = #local_tracks == 1
			and release_track_count > 1
			and release_primary ~= nil
			and is_generic_artist(release_primary.name)
		if should_override_with_track_artist then
			local assigned = assignments[1]
			local mb_credit_list = assigned and assigned.artist_credits
			local mb_track_artist = mb_credit_list and mb_credit_list[1] and mb_credit_list[1].artist
			if mb_track_artist and mb_track_artist.id then
				local updated_ids: { [number]: boolean } = {}
				local function apply_track_artist(entity: LocalArtist)
					if entity.db_id == nil or updated_ids[entity.db_id] then
						return
					end
					updated_ids[entity.db_id] = true
					if tag_local_artist(entity.db_id, mb_track_artist) then
						track_artist_count += 1
					end
				end

				if release_context.artists then
					for _, artist: LocalArtist in release_context.artists do
						apply_track_artist(artist)
					end
				end
				local first_track_artists = local_tracks[1].artists
				if first_track_artists then
					for _, artist: LocalArtist in first_track_artists do
						apply_track_artist(artist)
					end
				end
			end
		end
	end

	return true
end

local function fetch_release_details(release_id: string): Release?
	local release_response = mb_get(
		`/release/{release_id}?inc=recordings+genres+artist-credits+media+release-groups+recording-level-rels+work-rels+artist-rels`
	)
	local release = if release_response then normalize_release(release_response) else nil
	if release and not release["track-count"] then
		release["track-count"] = compute_track_count(release)
	end
	return release
end

local function fetch_release_by_id(release_id, release_context)
	local release = fetch_release_details(release_id)
	if not release then
		return false
	end
	if
		release_context.tracks
		and #release_context.tracks > 0
		and not validate_release_durations(release_context.tracks, release)
	then
		return false
	end
	return process_release(release, release_context)
end

local function localize_edition(
	best: Release?,
	candidates: { Release },
	track_count: number?,
	preferred_language: string?,
	preferred_country: string?
): Release?
	if not best or not preferred_language then
		return best
	end

	local function should_prefer_on_tie(candidate: Release, current: Release?): boolean
		if not current then
			return true
		end
		local candidate_country = preferred_country ~= nil and candidate.country == preferred_country
		local current_country = preferred_country ~= nil and current.country == preferred_country
		if candidate_country ~= current_country then
			return candidate_country
		end
		local candidate_barcode = release_has_barcode(candidate)
		local current_barcode = release_has_barcode(current)
		if candidate_barcode ~= current_barcode then
			return candidate_barcode
		end
		local candidate_date = candidate.date or ""
		local current_date = current.date or ""
		if candidate_date ~= current_date then
			return candidate_date < current_date
		end
		return false
	end

	local best_has_mb = contains_multibyte(best.title)
	local best_text = best["text-representation"]
	local best_lang = best_text and best_text.language
	if best_text and best_text.language == preferred_language and best_has_mb then
		return best
	end

	local preferred_language_localization = best_lang ~= nil and best_lang ~= preferred_language
	local best_media_count = 0
	local best_recordings: { [string]: boolean } = {}
	local recording_count = 0
	local best_media = best.media
	if best_media ~= nil then
		best_media_count = #best_media
		for _, medium: ReleaseMedium in best_media do
			local medium_tracks = medium.tracks
			if medium_tracks == nil then
				continue
			end
			for _, track: ReleaseMediumTrack in medium_tracks do
				if track.recording and track.recording.id then
					best_recordings[track.recording.id] = true
					recording_count += 1
				end
			end
		end
	end
	if recording_count == 0 then
		return best
	end

	local best_localized: Release? = nil
	local best_loc_score = -1
	local fallback_localized: Release? = nil
	local fallback_loc_score = -1
	for _, candidate: Release in candidates do
		if candidate.id == best.id then
			continue
		end
		local candidate_track_count = compute_track_count(candidate)
		if track_count and candidate_track_count > 0 and math.abs(candidate_track_count - track_count) > 1 then
			continue
		end
		local c_text = candidate["text-representation"]
		if not c_text or c_text.language ~= preferred_language then
			continue
		end

		local is_preferred_language_localization = preferred_language_localization == true
			and best_has_mb
			and contains_multibyte(candidate.title)
		local is_lang_upgrade = best_lang ~= preferred_language
		if
			contains_multibyte(candidate.title) == best_has_mb
			and not is_preferred_language_localization
			and not is_lang_upgrade
		then
			continue
		end

		local c_media_count = if candidate.media ~= nil then #candidate.media else 0
		if c_media_count > 0 and c_media_count ~= best_media_count then
			continue
		end

		local score = 0
		if preferred_country and candidate.country == preferred_country then
			score += 10000
		elseif
			preferred_country
			and candidate.country
			and candidate.country ~= preferred_country
			and candidate.country ~= "XW"
			and candidate.country ~= "XE"
		then
			score -= 5000
		end
		if candidate.status == "Official" then
			score += 100
		elseif candidate.status == "Pseudo-Release" then
			score += 25
		end

		local c_matches = 0
		local c_total = 0
		if candidate.media ~= nil then
			for _, medium: ReleaseMedium in candidate.media do
				local medium_tracks = medium.tracks
				if medium_tracks == nil then
					continue
				end
				for _, track: ReleaseMediumTrack in medium_tracks do
					if track.recording and track.recording.id then
						c_total += 1
						if best_recordings[track.recording.id] then
							c_matches += 1
						end
					end
				end
			end
		end

		if c_total > 0 and c_matches >= math.ceil(math.min(recording_count, c_total) * 0.8) then
			score += c_matches
			if
				score > best_loc_score or (score == best_loc_score and should_prefer_on_tie(candidate, best_localized))
			then
				best_loc_score = score
				best_localized = candidate
			end
		elseif
			is_preferred_language_localization
			and (
				score > fallback_loc_score
				or (score == fallback_loc_score and should_prefer_on_tie(candidate, fallback_localized))
			)
		then
			fallback_loc_score = score
			fallback_localized = candidate
		end
	end

	return best_localized or fallback_localized or best
end

local function should_keep_selected_release(
	selected_release: Release?,
	candidate_release: Release?,
	search_title: string?,
	preferred_language: string?,
	local_tracks: { LocalTrack }?,
	track_count: number?
): boolean
	if not selected_release or not candidate_release then
		return false
	end

	if track_count == 1 and local_tracks and #local_tracks == 1 then
		local selected_count = compute_track_count(selected_release)
		local candidate_count = compute_track_count(candidate_release)
		if selected_count == 1 and candidate_count > 1 then
			local selected_diff = single_track_duration_diff(local_tracks, selected_release)
			local candidate_diff = single_track_duration_diff(local_tracks, candidate_release)
			if selected_diff ~= nil and candidate_diff ~= nil and selected_diff + 500 < candidate_diff then
				return true
			end
		end
	end

	local norm_search = normalize_title(search_title)
	local norm_selected = normalize_title(selected_release.title)
	if track_count ~= nil and track_count > 1 then
		local selected_count = compute_track_count(selected_release)
		local candidate_count = compute_track_count(candidate_release)
		if
			selected_count >= track_count
			and candidate_count > 0
			and candidate_count < track_count
			and norm_selected == norm_search
		then
			return true
		end
	end

	if norm_selected ~= norm_search then
		return false
	end
	if normalize_title(candidate_release.title) == norm_search then
		return false
	end

	local selected_text = selected_release["text-representation"]
	local candidate_text = candidate_release["text-representation"]
	local selected_lang = selected_text and selected_text.language
	local candidate_lang = candidate_text and candidate_text.language
	if
		(selected_release.status == "Pseudo-Release" or selected_release.status == "Promotion")
		and candidate_release.status == "Official"
	then
		if
			preferred_language
			and selected_lang == preferred_language
			and candidate_lang
			and candidate_lang ~= preferred_language
		then
			return true
		end
		return false
	end
	if preferred_language then
		if candidate_lang == preferred_language and selected_lang ~= preferred_language then
			if
				(candidate_release.status == "Pseudo-Release" or candidate_release.status == "Promotion")
				and selected_lang
				and selected_lang ~= preferred_language
			then
				return false
			end
			if not contains_multibyte(search_title) then
				return false
			end
		end
	end

	if
		contains_multibyte(candidate_release.title)
		and not contains_multibyte(selected_release.title)
		and release_has_barcode(candidate_release)
		and not release_has_barcode(selected_release)
		and selected_lang
		and candidate_lang
		and selected_lang == candidate_lang
	then
		return false
	end
	return true
end

local function prefer_official_single_track_edition(
	best: Release?,
	candidates: { Release },
	local_tracks: { LocalTrack }?,
	track_count: number?,
	preferred_country: string?
): Release?
	if not best or best.status ~= "Promotion" then
		return best
	end
	if not local_tracks or #local_tracks ~= 1 then
		return best
	end
	local local_title = local_tracks[1] and local_tracks[1].track_title
	if not local_title or local_title == "" then
		return best
	end
	local preferred: Release? = nil
	local preferred_score = -1
	for _, candidate: Release in candidates do
		if candidate.status ~= "Official" then
			continue
		end
		if not release_has_track_title(candidate, local_title) then
			continue
		end
		local score = 0
		local candidate_count = compute_track_count(candidate)
		if track_count ~= nil and candidate_count > 0 then
			local diff = math.abs(candidate_count - track_count)
			if diff <= max_track_count_diff(track_count, true) then
				score += 100 - diff
			end
		else
			score += 50
		end
		local cover = candidate["cover-art-archive"]
		if cover and cover.front then
			score += 5
		end
		if score > preferred_score then
			preferred_score = score
			preferred = candidate
		end
	end
	return preferred or best
end

local function release_from_candidates(candidates: { Release }?, release_id: string?, fallback: Release?): Release?
	if candidates == nil or release_id == nil then
		return fallback
	end
	for _, candidate: Release in candidates do
		if candidate.id == release_id then
			return candidate
		end
	end
	return fallback
end

local function select_release_group_candidate(
	candidates: { Release }?,
	track_count: number?,
	preferred_language: string?,
	preferred_country: string?,
	search_title: string?,
	artist_ids: ArtistIdSet?,
	local_tracks: { LocalTrack }?,
	selected_release: Release?,
	keep_selected: boolean
): Release?
	if not candidates or #candidates == 0 then
		return nil
	end
	local best_edition = select_best_release(
		candidates,
		track_count,
		preferred_language,
		preferred_country,
		search_title,
		artist_ids,
		local_tracks,
		1
	)
	if not best_edition and track_count ~= nil and track_count > 1 then
		local medium_candidates: { Release } = {}
		for _, candidate: Release in candidates do
			if medium_has_track_count(candidate, track_count) then
				table.insert(medium_candidates, candidate)
			end
		end
		if #medium_candidates > 0 then
			best_edition = select_best_release(
				medium_candidates,
				nil,
				preferred_language,
				preferred_country,
				search_title,
				artist_ids,
				local_tracks,
				1
			)
			if not best_edition and #medium_candidates == 1 then
				best_edition = medium_candidates[1]
			end
		end
	end
	if not best_edition then
		return nil
	end
	best_edition = localize_edition(best_edition, candidates, track_count, preferred_language, preferred_country)
	if not best_edition then
		return nil
	end
	best_edition =
		prefer_official_single_track_edition(best_edition, candidates, local_tracks, track_count, preferred_country)
	if not best_edition then
		return nil
	end
	if keep_selected and selected_release then
		local selected_for_keep = release_from_candidates(candidates, selected_release.id, selected_release)
		if
			should_keep_selected_release(
				selected_for_keep,
				best_edition,
				search_title,
				preferred_language,
				local_tracks,
				track_count
			)
		then
			best_edition = selected_for_keep
		end
	end
	return best_edition
end

local function process_release_via_group(
	selected_release: Release,
	track_count: number?,
	preferred_language: string?,
	preferred_country: string?,
	search_title: string?,
	artist_ids: ArtistIdSet?,
	release_context: ReleaseContext,
	keep_selected: boolean,
	hard_fail_on_duration: boolean
): (boolean | "skip")?
	local release_group = selected_release["release-group"]
	local release_group_id = release_group and release_group.id
	if not release_group_id then
		return nil
	end
	local candidates = browse_release_group(release_group_id, track_count == 1, track_count)
	if not candidates or #candidates == 0 then
		return nil
	end
	local best_edition: Release? = select_release_group_candidate(
		candidates,
		track_count,
		preferred_language,
		preferred_country,
		search_title,
		artist_ids,
		release_context.tracks,
		selected_release,
		keep_selected
	)
	if not best_edition then
		return nil
	end
	if best_edition ~= nil and (best_edition.media == nil or #best_edition.media == 0) then
		best_edition = maybe_reuse_release_media(selected_release, best_edition)
	end
	if best_edition ~= nil and (best_edition.media == nil or #best_edition.media == 0) then
		local best_edition_id = best_edition.id
		if best_edition_id ~= nil then
			best_edition = fetch_release_details(best_edition_id)
		end
	end
	if not best_edition or not best_edition.media or #best_edition.media == 0 then
		if hard_fail_on_duration then
			return false
		end
		return "skip"
	end
	if release_context.tracks and #release_context.tracks > 0 then
		if not validate_release_durations(release_context.tracks, best_edition) then
			if hard_fail_on_duration then
				return false
			end
			return "skip"
		end
	end
	local best_rg_info = release_group
	local edition_rg_info = best_edition["release-group"]
	if edition_rg_info and (not best_rg_info or not best_rg_info["first-release-date"]) then
		best_rg_info = edition_rg_info
	end
	if best_rg_info and not best_rg_info["first-release-date"] then
		for _, candidate: Release in candidates do
			local candidate_rg = candidate["release-group"]
			if candidate_rg and candidate_rg["first-release-date"] then
				best_rg_info = {
					id = best_rg_info.id or (candidate_rg and candidate_rg.id),
					["first-release-date"] = candidate_rg["first-release-date"],
				}
				break
			end
		end
	end
	return process_release(best_edition, release_context, release_group_id, best_rg_info)
end

function module.resolve_cover_release_id(release_context: ReleaseContext): string?
	local ids = if release_context
		then release_context.ids or musicbrainz_external_ids(release_context.external_ids)
		else nil
	if not ids then
		return nil
	end

	local cache_context = cover_cache_context(release_context)
	local lookup_input = release_lookup_inputs(release_context)
	local track_count = cache_context.track_count
	local release_group_id = cache_context.release_group_id
	local selected_release: Release? = nil
	local candidates: { Release }? = nil
	local release_id = cache_context.release_id
	if type(release_group_id) == "string" and release_group_id ~= "" then
		candidates = browse_release_group(release_group_id, track_count == 1, track_count)
		if candidates and #candidates > 0 and type(release_id) == "string" and release_id ~= "" then
			selected_release = release_from_candidates(candidates, release_id, nil)
			if selected_release ~= nil and release_has_front_cover(selected_release) then
				local selected_release_id = selected_release.id
				if selected_release_id ~= nil then
					return selected_release_id
				end
			end
		end
	elseif type(release_id) == "string" and release_id ~= "" then
		selected_release = fetch_release_details(release_id)
		if selected_release ~= nil and release_has_front_cover(selected_release) then
			local selected_release_id = selected_release.id
			if selected_release_id ~= nil then
				return selected_release_id
			end
		end
		local release_group = selected_release and selected_release["release-group"]
		release_group_id = release_group and release_group.id
		if type(release_group_id) ~= "string" or release_group_id == "" then
			return nil
		end
		candidates = browse_release_group(release_group_id, track_count == 1, track_count)
		if candidates and #candidates > 0 and selected_release then
			selected_release = release_from_candidates(candidates, selected_release.id, selected_release)
		end
	else
		return nil
	end

	if not candidates or #candidates == 0 then
		return nil
	end

	local preferred_language = lookup_input.preferred_language
	local preferred_country = lookup_input.preferred_country
	local search_title = cache_context.search_title or ""
	if search_title == "" and selected_release and selected_release.title then
		search_title = selected_release.title
	end
	local artist_ids = release_artist_ids(selected_release)
	local local_tracks = release_context.tracks

	local cover_candidates: { Release } = {}
	for _, candidate: Release in candidates do
		if release_has_front_cover(candidate) then
			table.insert(cover_candidates, candidate)
		end
	end
	if #cover_candidates == 0 then
		return nil
	end

	local best_cover_release = select_release_group_candidate(
		cover_candidates,
		track_count,
		preferred_language,
		preferred_country,
		search_title,
		artist_ids,
		local_tracks,
		nil,
		false
	)
	return if best_cover_release then best_cover_release.id else nil
end

local function score_artist_candidate_match(
	local_name: string?,
	candidate: ReleaseArtist,
	preferred_country: string?
): number
	if type(local_name) ~= "string" or type(candidate.name) ~= "string" then
		return -1
	end
	local local_lower = string.lower(local_name)
	local candidate_lower = string.lower(candidate.name)
	local norm_local = normalize_artist_name_for_lookup(local_name)
	local norm_candidate = normalize_artist_name_for_lookup(candidate.name)
	local score = -1
	if local_lower == candidate_lower then
		score = 1200
	elseif norm_local ~= "" and norm_local == norm_candidate then
		score = 1100
	elseif norm_local ~= "" and norm_candidate ~= "" then
		if #norm_candidate >= 3 and string.find(norm_local, norm_candidate, 1, true) then
			score = 700 + math.min(#norm_candidate, 30)
		elseif #norm_local >= 3 and string.find(norm_candidate, norm_local, 1, true) then
			score = 650 + math.min(#norm_local, 30)
		elseif artist_name_matches(local_name, candidate.name) then
			score = 500
		end
	elseif artist_name_matches(local_name, candidate.name) then
		score = 500
	end
	if score < 0 then
		return -1
	end
	if candidate.score then
		score += candidate.score
	end
	if preferred_country and candidate.country == preferred_country then
		score += 75
	end
	return score
end

local function select_artist_country_hint(local_name: string, candidates: { ReleaseArtist }): (string?, number)
	if type(local_name) ~= "string" then
		return nil, -1
	end
	if #candidates == 0 then
		return nil, -1
	end
	local best_country: string? = nil
	local best_score = -1
	for _, candidate: ReleaseArtist in candidates do
		local score = score_artist_candidate_match(local_name, candidate, nil)
		if score < 0 then
			if candidate.score and candidate.score >= 99 then
				score = candidate.score
			end
		end
		if score > best_score then
			local country = trim_text(candidate.country)
			best_score = score
			if country ~= "" then
				best_country = string.upper(country)
			else
				best_country = nil
			end
		end
	end
	return best_country, best_score
end

local function apply_artist_fallback_updates(
	release_context: ReleaseContext,
	candidates: { ReleaseArtist },
	preferred_country: string?
): number
	if not release_context or not candidates or #candidates == 0 then
		return 0
	end
	local artists_to_check: { LocalArtist } = {}
	local seen_artist_ids: { [number]: boolean } = {}
	local fetched_relations_for: { [string]: boolean } = {}
	local function queue_local_artist(artist: LocalArtist)
		if not artist or not artist.db_id then
			return
		end
		if seen_artist_ids[artist.db_id] then
			return
		end
		if is_generic_artist(artist.artist_name) then
			return
		end
		seen_artist_ids[artist.db_id] = true
		table.insert(artists_to_check, artist)
	end
	for_each_local_artist(release_context, queue_local_artist)
	local updated = 0
	for _, local_artist in artists_to_check do
		local name = local_artist.artist_name
		if name == nil or name == "" then
			continue
		end
		local matched: ReleaseArtist? = nil
		local matched_score = -1
		for _, candidate: ReleaseArtist in candidates do
			local score = score_artist_candidate_match(name, candidate, preferred_country)
			if score > matched_score then
				matched = candidate
				matched_score = score
			end
		end
		if not matched and contains_multibyte(name) then
			local best_score = -1
			for _, candidate: ReleaseArtist in candidates do
				if candidate.id and candidate.score and candidate.score >= 99 then
					local score = candidate.score
					if preferred_country and candidate.country == preferred_country then
						score += 25
					end
					if score > best_score then
						best_score = score
						matched = candidate
					end
				end
			end
		end
		if matched and matched.id then
			local local_artist_db_id = local_artist.db_id
			if local_artist_db_id and set_artist_metadata(local_artist_db_id, matched) then
				if not fetched_relations_for[matched.id] then
					fetched_relations_for[matched.id] = true
					fetch_and_link_artist_relations(local_artist_db_id, matched.id, matched["type"])
				end
				updated += 1
			end
		end
	end
	return updated
end

local function search_and_fetch_release(
	artist_name: string?,
	release_title: string?,
	track_count: number,
	release_context: ReleaseContext,
	preferred_language: string?,
	preferred_country: string?
): boolean
	local current_provider = provider
	if current_provider == nil then
		return false
	end

	local provider_handle: ProviderHandle = {
		layer = function(_, db_id: number): ProviderLayer
			local layer = current_provider:layer(db_id)
			return {
				set_id = function(_, key: string, value: string)
					layer:set_id(key, value)
				end,
				save = function(_)
					layer:save()
				end,
			}
		end,
	}

	local deps: SearchDeps = {
		is_blank_text = is_blank_text,
		is_generic_artist = is_generic_artist,
		clean_release_title = clean_release_title,
		apply_artist_fallback_updates = apply_artist_fallback_updates,
		contains_multibyte = contains_multibyte,
		infer_lookup_hints = infer_lookup_hints,
		sort_tracks_for_refresh = sort_tracks_for_refresh,
		normalize_title = normalize_title,
		normalize_overlap_title = normalize_overlap_title,
		normalize_artist_name_for_lookup = normalize_artist_name_for_lookup,
		artist_name_matches = artist_name_matches,
		select_artist_country_hint = select_artist_country_hint,
		mb_get = mb_get,
		http = http,
		escape_lucene = escape_lucene,
		trim_text = trim_text,
		titles_match_search = titles_match_search,
		compute_track_count = compute_track_count,
		fetch_release_by_id = fetch_release_by_id,
		select_best_release = select_best_release,
		release_matches_artist_ids = release_matches_artist_ids,
		process_release_via_group = process_release_via_group,
		release_has_track_title = release_has_track_title,
		count_track_title_overlap = count_track_title_overlap,
		validate_release_durations = validate_release_durations,
		process_release = process_release,
		max_declared_track_total = max_declared_track_total,
		single_track_duration_diff = single_track_duration_diff,
		provider = provider_handle,
		for_each_local_artist = for_each_local_artist,
	}

	return release_search.search_and_fetch_release(
		deps,
		artist_name,
		release_title,
		track_count,
		release_context,
		preferred_language,
		preferred_country
	)
end

type AcoustIdConfig = {
	deps: acoustid.ResolveDeps,
	module: typeof(acoustid),
}

function module.register_refresh(provider_instance: Provider, acoustid_config: AcoustIdConfig?): ()
	provider = provider_instance
	provider_instance:refresh(metadata.EntityType.Release, function(ctx: metadata.ReleaseRefreshContext)
		local release: ReleaseContext = ctx :: ReleaseContext
		local lookup_input = release_lookup_inputs(release)
		local musicbrainz_ids = musicbrainz_external_ids(release.external_ids)
		if musicbrainz_ids and musicbrainz_ids.release_id then
			if fetch_release_by_id(musicbrainz_ids.release_id, release) then
				return nil
			end
		end

		if not lookup_input.artist_name and not lookup_input.compilation_release then
			return nil
		end

		local found = search_and_fetch_release(
			lookup_input.artist_name,
			lookup_input.release_title,
			lookup_input.track_count,
			release,
			lookup_input.preferred_language,
			lookup_input.preferred_country
		)
		if not found then
			-- Try AcoustID fingerprint lookup if the option is enabled
			if acoustid_config and ctx.options and ctx.options.acoustid_fingerprint == true then
				local acoustid_mod = acoustid_config.module
				local tracks = release.tracks
				if tracks and #tracks > 0 then
					local track = acoustid_mod.select_track(tracks)
					if track and track.db_id then
						local fingerprint, duration = acoustid_mod.compute_fingerprint(track.db_id)
						if fingerprint and duration then
							local result = acoustid_mod.lookup(fingerprint, duration)
							if result then
								local recording_id = result.recording_ids[1]
								if recording_id then
									local applied = acoustid_mod.resolve_recording_to_release(
										recording_id,
										release,
										#tracks,
										acoustid_config.deps
									)
									if applied and result.track_id and track.db_id then
										local track_layer = provider_instance:layer(track.db_id :: number)
										track_layer:set_id("acoustid_track_id", result.track_id :: string)
										track_layer:save()
									end
									if applied then
										return nil
									end
								end
							end
						end
					end
				end
			end

			local release_db_id = release.db_id
			if release_db_id == nil then
				return nil
			end

			local release_layer = provider_instance:layer(release_db_id)
			release_layer:set_id("release_id", "")
			release_layer:set_id("release_group_id", "")
			release_layer:save()
		end

		return nil
	end, function(ctx: metadata.ReleaseRefreshContext): boolean
		local release: ReleaseContext = ctx :: ReleaseContext
		local musicbrainz_ids = musicbrainz_external_ids(release.external_ids)
		if musicbrainz_ids and musicbrainz_ids.release_id then
			return false
		end
		if not release.tracks or #release.tracks == 0 then
			return false
		end
		return true
	end)
end

module.fetch_release_by_id = fetch_release_by_id

return module
