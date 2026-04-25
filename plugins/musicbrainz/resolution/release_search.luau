--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local json = require("@harmony/json")

local text_helpers = require("./text_helpers")
local track_helpers = require("./track_helpers")

local module = {}

type JsonValue = json.JsonValue
type JsonObject = { [string]: JsonValue }
type LookupHints = text_helpers.LookupHints
type LookupArtist = text_helpers.LookupHintArtist
type LookupTrack = text_helpers.LookupHintTrack
type LookupHintLibrary = text_helpers.LookupHintLibrary
type LookupHintReleaseContext = text_helpers.LookupHintReleaseContext
type LocalArtist = track_helpers.LocalArtist
type LocalTrack = track_helpers.LocalTrack
type ReleaseArtist = track_helpers.ReleaseArtist
type ReleaseArtistCredit = track_helpers.ReleaseArtistCredit
type ReleaseGroup = track_helpers.ReleaseGroup
type RecordingRelation = track_helpers.RecordingRelation
type ReleaseRecording = track_helpers.ReleaseRecording
type ReleaseMediumTrack = track_helpers.ReleaseMediumTrack
type ReleaseMedium = track_helpers.ReleaseMedium
type Release = track_helpers.Release

export type ArtistIdSet = { [string]: boolean }

export type ReleaseIds = {
	release_id: string?,
	release_group_id: string?,
	[string]: JsonValue,
}

type Recording = {
	id: string?,
	title: string?,
	length: number?,
	releases: { Release }?,
	["artist-credit"]: { ReleaseArtistCredit }?,
}

type RelationWork = {
	id: string?,
}

type Relation = {
	type: string?,
	["target-type"]: string?,
	work: RelationWork?,
}

type ArtistSearchResult = {
	count: number,
	artists: { ReleaseArtist },
}

type ReleaseSearchResult = {
	count: number,
	releases: { Release },
}

type RecordingSearchResult = {
	count: number,
	recordings: { Recording },
}

export type ReleaseContext = {
	db_id: number?,
	lookup_hints: LookupHints?,
	release_title: string?,
	artists: { LocalArtist }?,
	tracks: { LocalTrack },
	library: LookupHintLibrary?,
	library_id: number?,
	ids: ReleaseIds?,
	external_ids: ReleaseIds?,
	[string]: JsonValue,
}

export type ProviderLayer = {
	set_id: (ProviderLayer, string, string) -> (),
	save: (ProviderLayer) -> (),
}

export type ProviderHandle = {
	layer: (ProviderHandle, number) -> ProviderLayer,
}

export type SearchDeps = {
	is_blank_text: (string?) -> boolean,
	is_generic_artist: (string?) -> boolean,
	clean_release_title: (string?) -> string,
	apply_artist_fallback_updates: (ReleaseContext, { ReleaseArtist }, string?) -> number,
	contains_multibyte: (string?) -> boolean,
	infer_lookup_hints: (LookupHintReleaseContext?) -> (string?, string?, number?),
	sort_tracks_for_refresh: ({ LocalTrack }) -> (),
	normalize_title: (string?) -> string,
	normalize_overlap_title: (string?) -> string,
	normalize_artist_name_for_lookup: (string?) -> string,
	artist_name_matches: (string?, string?) -> boolean,
	select_artist_country_hint: (string, { ReleaseArtist }) -> (string?, number),
	mb_get: (string) -> (JsonObject?, number),
	http: {
		encode_uri_component: (string) -> string,
	},
	escape_lucene: (string) -> string,
	trim_text: (string?) -> string,
	titles_match_search: (string?, string?) -> boolean,
	compute_track_count: (Release) -> number,
	fetch_release_by_id: (string, ReleaseContext) -> boolean,
	select_best_release: (
		{ Release },
		number?,
		string?,
		string?,
		string?,
		ArtistIdSet?,
		{ LocalTrack }?,
		number?
	) -> Release?,
	release_matches_artist_ids: (Release, ArtistIdSet?) -> boolean,
	process_release_via_group: (
		Release,
		number?,
		string?,
		string?,
		string?,
		ArtistIdSet?,
		ReleaseContext,
		boolean,
		boolean
	) -> (boolean | "skip")?,
	release_has_track_title: (Release, string?) -> boolean,
	count_track_title_overlap: ({ LocalTrack }?, Release) -> number,
	validate_release_durations: ({ LocalTrack }, Release) -> boolean,
	process_release: (Release, ReleaseContext, string?, ReleaseGroup?) -> boolean,
	max_declared_track_total: ({ LocalTrack }?) -> number,
	single_track_duration_diff: ({ LocalTrack }?, Release?) -> number?,
	provider: ProviderHandle,
	for_each_local_artist: (ReleaseContext, (LocalArtist) -> ()) -> (),
}

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

local function normalize_artist_credit(raw_credit: JsonValue): ReleaseArtistCredit?
	if type(raw_credit) ~= "table" then
		return nil
	end
	local credit = raw_credit :: JsonObject
	return {
		name = json_string_field(credit, "name"),
		joinphrase = json_string_field(credit, "joinphrase"),
		artist = normalize_release_artist(json_object_field(credit, "artist")),
	}
end

local function normalize_artist_credit_list(raw_credits: { JsonValue }?): { ReleaseArtistCredit }
	local credits: { ReleaseArtistCredit } = {}
	if raw_credits == nil then
		return credits
	end
	for _, raw_credit: JsonValue in raw_credits do
		local credit = normalize_artist_credit(raw_credit)
		if credit ~= nil then
			table.insert(credits, credit)
		end
	end
	return credits
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

local function normalize_release_recording(raw_recording: JsonObject?): ReleaseRecording?
	if raw_recording == nil then
		return nil
	end
	return {
		id = json_string_field(raw_recording, "id"),
		relations = json_array_field(raw_recording, "relations") :: { RecordingRelation }?,
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
		title = title,
		position = json_number_field(track, "position"),
		recording = normalize_release_recording(json_object_field(track, "recording")),
		length = json_number_field(track, "length"),
		["artist-credit"] = normalize_artist_credit_list(json_array_field(track, "artist-credit")),
	}
end

local function normalize_release_medium_list(raw_media: { JsonValue }?): { ReleaseMedium }
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
				["track-count"] = json_number_field(medium, "track-count"),
				tracks = tracks,
			})
		end
	end
	return media
end

local function normalize_release(raw_release: JsonValue): Release?
	if type(raw_release) ~= "table" then
		return nil
	end
	local release = raw_release :: JsonObject
	local text_representation = json_object_field(release, "text-representation")
	local cover_art_archive = json_object_field(release, "cover-art-archive")
	return {
		id = json_string_field(release, "id"),
		title = json_string_field(release, "title"),
		status = json_string_field(release, "status"),
		country = json_string_field(release, "country"),
		date = json_string_field(release, "date"),
		barcode = json_string_field(release, "barcode"),
		packaging = json_string_field(release, "packaging"),
		score = json_number_field(release, "score"),
		media = normalize_release_medium_list(json_array_field(release, "media")),
		["track-count"] = json_number_field(release, "track-count"),
		["artist-credit"] = normalize_artist_credit_list(json_array_field(release, "artist-credit")),
		["release-group"] = normalize_release_group(json_object_field(release, "release-group")),
		["text-representation"] = if text_representation ~= nil
			then {
				language = json_string_field(text_representation, "language"),
			}
			else nil,
		["cover-art-archive"] = if cover_art_archive ~= nil
			then {
				front = if cover_art_archive ~= nil then cover_art_archive["front"] == true else nil,
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
		local release = normalize_release(raw_release)
		if release ~= nil then
			table.insert(releases, release)
		end
	end
	return releases
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
		["artist-credit"] = normalize_artist_credit_list(json_array_field(recording, "artist-credit")),
	}
end

local function normalize_artist_search_result(raw_data: JsonObject): ArtistSearchResult
	return {
		count = json_number_field(raw_data, "count") or 0,
		artists = normalize_release_artist_list(json_array_field(raw_data, "artists")),
	}
end

local function normalize_release_search_result(raw_data: JsonObject): ReleaseSearchResult
	return {
		count = json_number_field(raw_data, "count") or 0,
		releases = normalize_release_list(json_array_field(raw_data, "releases")),
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

local function normalize_lookup_artists(artists: { LocalArtist }?): { LookupArtist }?
	if artists == nil then
		return nil
	end
	local normalized_artists: { LookupArtist } = {}
	for _, artist: LocalArtist in artists do
		local normalized_artist: LookupArtist = {
			artist_name = artist.artist_name,
		}
		table.insert(normalized_artists, normalized_artist)
	end
	return normalized_artists
end

local function normalize_lookup_tracks(tracks: { LocalTrack }): { LookupTrack }
	local normalized_tracks: { LookupTrack } = {}
	for _, track: LocalTrack in tracks do
		local normalized_track: LookupTrack = {
			lookup_hints = nil,
			track_title = track.track_title,
			track_total = track.track_total,
			artists = normalize_lookup_artists(track.artists),
			disc = track.disc,
			track = track.track,
			duration_ms = track.duration_ms,
		}
		table.insert(normalized_tracks, normalized_track)
	end
	return normalized_tracks
end

local function normalize_lookup_context(release_context: ReleaseContext): LookupHintReleaseContext
	return {
		lookup_hints = release_context.lookup_hints,
		release_title = release_context.release_title,
		artists = normalize_lookup_artists(release_context.artists),
		tracks = normalize_lookup_tracks(release_context.tracks),
		library = release_context.library,
		library_id = release_context.library_id,
	}
end

local function search_and_fetch_release(
	deps: SearchDeps,
	artist_name: string?,
	release_title: string?,
	track_count: number,
	release_context: ReleaseContext,
	preferred_language: string?,
	preferred_country: string?
)
	local is_blank_text = deps.is_blank_text
	local is_generic_artist = deps.is_generic_artist
	local clean_release_title = deps.clean_release_title
	local apply_artist_fallback_updates = deps.apply_artist_fallback_updates
	local contains_multibyte = deps.contains_multibyte
	local infer_lookup_hints = deps.infer_lookup_hints
	local sort_tracks_for_refresh = deps.sort_tracks_for_refresh
	local normalize_title = deps.normalize_title
	local normalize_overlap_title = deps.normalize_overlap_title
	local normalize_artist_name_for_lookup = deps.normalize_artist_name_for_lookup
	local artist_name_matches = deps.artist_name_matches
	local select_artist_country_hint = deps.select_artist_country_hint
	local mb_get = deps.mb_get
	local http = deps.http
	local escape_lucene = deps.escape_lucene
	local trim_text = deps.trim_text
	local titles_match_search = deps.titles_match_search
	local compute_track_count = deps.compute_track_count
	local fetch_release_by_id = deps.fetch_release_by_id
	local select_best_release = deps.select_best_release
	local release_matches_artist_ids = deps.release_matches_artist_ids
	local process_release_via_group = deps.process_release_via_group
	local release_has_track_title = deps.release_has_track_title
	local count_track_title_overlap = deps.count_track_title_overlap
	local validate_release_durations = deps.validate_release_durations
	local process_release = deps.process_release
	local max_declared_track_total = deps.max_declared_track_total
	local single_track_duration_diff = deps.single_track_duration_diff
	local provider = deps.provider
	local for_each_local_artist = deps.for_each_local_artist

	if is_blank_text(artist_name) or is_generic_artist(artist_name) then
		artist_name = nil
	end

	local artist_candidate_pool: { ReleaseArtist } = {}
	local search_title = clean_release_title(release_title)
	if is_blank_text(search_title) and #release_context.tracks > 0 then
		local first_track = release_context.tracks[1]
		search_title = clean_release_title(if first_track then first_track.track_title else "")
	end
	if is_blank_text(search_title) then
		apply_artist_fallback_updates(release_context, artist_candidate_pool, preferred_country)
		return false
	end
	local search_title_has_multibyte = contains_multibyte(search_title)
	local _, inferred_release_hint, inferred_year = infer_lookup_hints(normalize_lookup_context(release_context))
	local function representative_track_title(): string?
		local tracks = release_context.tracks
		if #tracks == 0 then
			return nil
		end
		local ordered: { LocalTrack } = {}
		for _, track: LocalTrack in tracks do
			table.insert(ordered, track)
		end
		sort_tracks_for_refresh(ordered)
		local norm_search = normalize_title(search_title)
		if norm_search ~= "" then
			for _, track in ordered do
				if normalize_title(track.track_title) == norm_search then
					return track.track_title
				end
			end
		end
		local first_track = ordered[1]
		return if first_track then first_track.track_title else nil
	end
	local artist_ids: ArtistIdSet? = nil
	local top_artist_id: string? = nil
	local top_artist_ids: ArtistIdSet? = nil
	local has_artist_name_match = false
	local inferred_artist_country: string? = nil
	local inferred_artist_country_score = -1
	local function is_strong_artist_result(name: string, artist: ReleaseArtist, top_score: number?)
		if type(name) ~= "string" or type(artist) ~= "table" or type(artist.name) ~= "string" then
			return false
		end
		if artist_name_matches(name, artist.name) then
			return true
		end
		local norm_name = normalize_artist_name_for_lookup(name)
		local norm_artist = normalize_artist_name_for_lookup(artist.name)
		if norm_name ~= "" and norm_artist ~= "" then
			if #norm_artist >= 3 and string.find(norm_name, norm_artist, 1, true) then
				return true
			end
			if #norm_name >= 3 and string.find(norm_artist, norm_name, 1, true) then
				return true
			end
		end
		if contains_multibyte(name) then
			if artist.score and artist.score >= 99 then
				return true
			end
		end
		local minimum_score = if type(top_score) == "number" then math.max(65, top_score - 20) else 80
		if artist.score and artist.score >= minimum_score then
			return true
		end
		return false
	end
	if artist_name then
		local names_to_search: { string } = {}
		local seen_names: { [string]: boolean } = {}
		local function queue_artist(name: string?)
			if type(name) ~= "string" or is_blank_text(name) or is_generic_artist(name) then
				return
			end
			local resolved_name = name
			local key = string.lower(resolved_name)
			if seen_names[key] then
				return
			end
			seen_names[key] = true
			table.insert(names_to_search, resolved_name)
		end
		queue_artist(artist_name)
		if release_context.artists then
			for _, a: LocalArtist in release_context.artists do
				queue_artist(a.artist_name)
			end
		end
		local function has_more_specific_name_variant(name: string)
			local norm_name = normalize_artist_name_for_lookup(name)
			if norm_name == "" then
				return false
			end
			for _, candidate_name: string in names_to_search do
				local norm_candidate = normalize_artist_name_for_lookup(candidate_name)
				if #norm_candidate > #norm_name + 3 and string.sub(norm_candidate, 1, #norm_name) == norm_name then
					return true
				end
			end
			return false
		end
		for _, name: string in names_to_search do
			local artists_response = mb_get(`/artist?query={http.encode_uri_component(name)}`)
			local artists_data = if artists_response then normalize_artist_search_result(artists_response) else nil
			if artists_data and artists_data.count > 0 then
				if not preferred_country then
					local inferred_country, inferred_score = select_artist_country_hint(name, artists_data.artists)
					if inferred_country and inferred_score > inferred_artist_country_score then
						inferred_artist_country = inferred_country
						inferred_artist_country_score = inferred_score
					end
				end
				local top_score = if artists_data.artists[1] then artists_data.artists[1].score else nil
				local query_top_artist_id: string? = nil
				local query_has_text_match = false
				for i: number, artist: ReleaseArtist in artists_data.artists do
					if artist.name and artist_name_matches(name, artist.name) then
						query_has_text_match = true
					end
					local strong_match = is_strong_artist_result(name, artist, top_score)
					if strong_match then
						local resolved_artist_ids: ArtistIdSet = if artist_ids ~= nil then artist_ids else {}
						artist_ids = resolved_artist_ids
						if artist.id then
							resolved_artist_ids[artist.id] = true
							if not query_top_artist_id then
								query_top_artist_id = artist.id
							end
						end
					end
					if i <= 5 then
						table.insert(artist_candidate_pool, artist)
					end
				end
				if not query_top_artist_id and artists_data.artists[1] then
					query_top_artist_id = artists_data.artists[1].id
				end
				if not top_artist_id then
					top_artist_id = query_top_artist_id
				end
				if query_has_text_match then
					has_artist_name_match = true
				end
			end
			if top_artist_id and has_artist_name_match and not has_more_specific_name_variant(name) then
				break
			end
		end
		if not preferred_country and inferred_artist_country then
			preferred_country = inferred_artist_country
		end
		if not artist_ids and top_artist_id then
			artist_ids = { [top_artist_id] = true }
		end
		if not artist_ids then
			apply_artist_fallback_updates(release_context, artist_candidate_pool, preferred_country)
			return false
		end
		if top_artist_id then
			top_artist_ids = { [top_artist_id] = true }
		end
	end
	local function normalize_release_query_title(raw_title)
		local normalized_title = raw_title
		local paren_normalized = string.gsub(raw_title, "（", "(")
		paren_normalized = string.gsub(paren_normalized, "）", ")")
		local paren_match = string.match(paren_normalized, "%s*(%b())$")
		if paren_match then
			local inner = string.lower(string.sub(paren_match, 2, -2))
			local keep_paren = string.find(inner, "^feat")
				or string.find(inner, "^ft%.")
				or string.find(inner, "^with ")
				or string.find(inner, "ver", 1, true)
				or string.find(inner, "version", 1, true)
				or string.find(inner, "mix", 1, true)
				or string.find(inner, "remix", 1, true)
				or string.find(inner, "live", 1, true)
				or string.find(inner, "acoustic", 1, true)
			if not keep_paren then
				local stripped = string.gsub(paren_normalized, "%s*%b()$", "")
				if stripped ~= "" then
					normalized_title = stripped
				end
			end
		end
		return normalized_title
	end

	local query_title = normalize_release_query_title(search_title)
	local lucene_query = `release:{escape_lucene(query_title)}`
	local hint_search_title: string? = nil
	if inferred_release_hint then
		local cleaned_hint = clean_release_title(inferred_release_hint)
		if not is_blank_text(cleaned_hint) and normalize_title(cleaned_hint) ~= normalize_title(search_title) then
			hint_search_title = cleaned_hint
		end
	end
	local first_track_title = representative_track_title()
	local local_track_count = #release_context.tracks
	local declared_track_total = max_declared_track_total(release_context.tracks)
	local hint_search_title_has_multibyte = hint_search_title ~= nil and contains_multibyte(hint_search_title)
	local first_track_title_is_non_multibyte = first_track_title
		and first_track_title ~= ""
		and not contains_multibyte(first_track_title)
	local releases_data: ReleaseSearchResult? = nil
	if
		top_artist_id ~= nil
		and hint_search_title ~= nil
		and hint_search_title_has_multibyte
		and first_track_title_is_non_multibyte
		and not search_title_has_multibyte
		and track_count > 1
	then
		local resolved_top_artist_id = top_artist_id
		local resolved_hint_search_title = hint_search_title
		local hint_query_title = normalize_release_query_title(resolved_hint_search_title)
		local hint_lucene_query = `release:{escape_lucene(hint_query_title)}`
		local hint_primary_data: ReleaseSearchResult? = nil
		local hint_primary_arid_query: string
		if first_track_title and first_track_title ~= "" then
			hint_primary_arid_query = `arid:{resolved_top_artist_id} AND (release:{escape_lucene(
				resolved_hint_search_title
			)} OR recording:{escape_lucene(first_track_title)})`
		else
			hint_primary_arid_query =
				`arid:{resolved_top_artist_id} AND release:{escape_lucene(resolved_hint_search_title)}`
		end
		local hint_primary_response =
			mb_get(`/release?query={http.encode_uri_component(hint_primary_arid_query)}&limit=100`)
		hint_primary_data = if hint_primary_response
			then normalize_release_search_result(hint_primary_response)
			else nil
		if not hint_primary_data or hint_primary_data.count == 0 then
			local hint_lucene_response =
				mb_get(`/release?query={http.encode_uri_component(hint_lucene_query)}&limit=100`)
			hint_primary_data = if hint_lucene_response
				then normalize_release_search_result(hint_lucene_response)
				else nil
		end
		if hint_primary_data and hint_primary_data.count > 0 then
			releases_data = hint_primary_data
			search_title = resolved_hint_search_title
			search_title_has_multibyte = true
			query_title = hint_query_title
			lucene_query = hint_lucene_query
		end
	end
	local title_has_ascii_upper = string.find(search_title, "%u") ~= nil
	local title_has_ascii_letters = string.find(search_title, "%a") ~= nil
	local normalized_query_title = normalize_title(query_title)
	local title_is_low_information_single = normalized_query_title ~= ""
		and not title_has_ascii_letters
		and #normalized_query_title <= 4
	local attempted_arid_primary = false
	local used_arid_primary_results = false
	if
		top_artist_id ~= nil
		and track_count == 1
		and not search_title_has_multibyte
		and (title_has_ascii_upper or title_is_low_information_single)
	then
		local resolved_top_artist_id = top_artist_id
		local arid_query: string
		if first_track_title and first_track_title ~= "" then
			arid_query =
				`arid:{resolved_top_artist_id} AND (release:{escape_lucene(search_title)} OR recording:{escape_lucene(
					first_track_title
				)})`
		else
			arid_query = `arid:{resolved_top_artist_id} AND release:{escape_lucene(search_title)}`
		end
		attempted_arid_primary = true
		local arid_response = mb_get(`/release?query={http.encode_uri_component(arid_query)}&limit=100`)
		releases_data = if arid_response then normalize_release_search_result(arid_response) else nil
		used_arid_primary_results = releases_data ~= nil and releases_data.count > 0
	end
	if not releases_data or releases_data.count == 0 then
		local lucene_response = mb_get(`/release?query={http.encode_uri_component(lucene_query)}&limit=100`)
		releases_data = if lucene_response then normalize_release_search_result(lucene_response) else nil
	end
	if not releases_data or releases_data.count == 0 then
		local simplified = string.gsub(query_title, "（[^）]*）%s*$", "")
		simplified = string.gsub(simplified, "%s*%b()%s*$", "")
		simplified = trim_text(simplified)
		if simplified ~= "" and simplified ~= query_title then
			local simplified_query = `release:{escape_lucene(simplified)}`
			local simplified_response = mb_get(`/release?query={http.encode_uri_component(simplified_query)}&limit=100`)
			releases_data = if simplified_response then normalize_release_search_result(simplified_response) else nil
		end
	end
	if (not releases_data or releases_data.count == 0) and hint_search_title then
		local hint_query_title = normalize_release_query_title(hint_search_title)
		local hint_lucene_query = `release:{escape_lucene(hint_query_title)}`
		local hint_releases_data: ReleaseSearchResult? = nil
		if top_artist_id then
			local hint_arid_query: string
			if first_track_title and first_track_title ~= "" then
				hint_arid_query =
					`arid:{top_artist_id} AND (release:{escape_lucene(hint_search_title)} OR recording:{escape_lucene(
						first_track_title
					)})`
			else
				hint_arid_query = `arid:{top_artist_id} AND release:{escape_lucene(hint_search_title)}`
			end
			local hint_arid_response = mb_get(`/release?query={http.encode_uri_component(hint_arid_query)}&limit=100`)
			hint_releases_data = if hint_arid_response then normalize_release_search_result(hint_arid_response) else nil
		end
		if not hint_releases_data or hint_releases_data.count == 0 then
			local hint_lucene_response =
				mb_get(`/release?query={http.encode_uri_component(hint_lucene_query)}&limit=100`)
			hint_releases_data = if hint_lucene_response
				then normalize_release_search_result(hint_lucene_response)
				else nil
		end
		if not hint_releases_data or hint_releases_data.count == 0 then
			local hint_simplified = string.gsub(hint_query_title, "（[^）]*）%s*$", "")
			hint_simplified = string.gsub(hint_simplified, "%s*%b()%s*$", "")
			hint_simplified = trim_text(hint_simplified)
			if hint_simplified ~= "" and hint_simplified ~= hint_query_title then
				local hint_simplified_query = `release:{escape_lucene(hint_simplified)}`
				local hint_simplified_response =
					mb_get(`/release?query={http.encode_uri_component(hint_simplified_query)}&limit=100`)
				hint_releases_data = if hint_simplified_response
					then normalize_release_search_result(hint_simplified_response)
					else nil
			end
		end
		if hint_releases_data and hint_releases_data.count > 0 then
			if track_count > 0 then
				for _, candidate: Release in hint_releases_data.releases do
					if
						titles_match_search(candidate.title, hint_search_title)
						and compute_track_count(candidate) == track_count
					then
						local artist_ok = not top_artist_id or release_matches_artist_ids(candidate, top_artist_ids)
						if artist_ok and candidate.id and fetch_release_by_id(candidate.id, release_context) then
							return true
						end
					end
				end
			end
			releases_data = hint_releases_data
			search_title = hint_search_title
			search_title_has_multibyte = contains_multibyte(search_title)
			query_title = hint_query_title
			lucene_query = hint_lucene_query
		end
	end
	if not releases_data or releases_data.count == 0 then
		apply_artist_fallback_updates(release_context, artist_candidate_pool, preferred_country)
		return false
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

	local function medium_count(release: Release): number
		local count = 0
		local media = release.media
		if media == nil then
			return count
		end
		for _ in media do
			count += 1
		end
		return count
	end

	local function matches_top_artist(release: Release): boolean
		if not top_artist_id then
			return true
		end
		return release_matches_artist_ids(release, top_artist_ids)
	end

	local disc_split_title: string? = nil
	do
		local stripped = string.gsub(search_title, "%s*[（(]%s*[dD][iI][sS][cC]%s*%d+%s*[)）]%s*$", "")
		stripped = trim_text(stripped)
		if stripped ~= "" and stripped ~= search_title then
			disc_split_title = stripped
		end
	end

	if disc_split_title and track_count ~= nil and track_count > 1 and releases_data.releases then
		local seed_release: Release? = nil
		local seed_score = -math.huge
		local norm_disc_title = normalize_title(disc_split_title)
		local function token_overlap_score(candidate_title: string?): number
			local expected_tokens = {}
			local expected_count = 0
			for token in string.gmatch(norm_disc_title, "[a-z0-9]+") do
				if not expected_tokens[token] then
					expected_tokens[token] = true
					expected_count += 1
				end
			end
			if expected_count == 0 then
				return 0
			end
			local overlap = 0
			local seen = {}
			for token in string.gmatch(normalize_title(candidate_title), "[a-z0-9]+") do
				if not seen[token] then
					seen[token] = true
					if expected_tokens[token] then
						overlap += 1
					end
				end
			end
			return overlap
		end
		for _, candidate: Release in releases_data.releases do
			local release_group = candidate["release-group"]
			if release_group and release_group.id and medium_has_track_count(candidate, track_count) then
				local title_overlap = token_overlap_score(candidate.title)
				if title_overlap <= 0 then
					continue
				end
				local score = 0
				score += title_overlap * 10
				if matches_top_artist(candidate) then
					score += 6
				else
					score -= 2
				end
				if candidate.status == "Official" then
					score += 2
				elseif candidate.status == "Promotion" then
					score += 1
				elseif candidate.status == "Pseudo-Release" then
					score -= 8
				end
				if trim_text(candidate.barcode) ~= "" then
					score += 1
				end
				if score > seed_score then
					seed_score = score
					seed_release = candidate
				elseif score == seed_score and seed_release then
					local candidate_date = candidate.date or ""
					local seed_date = seed_release.date or ""
					if candidate_date ~= "" and seed_date ~= "" and candidate_date < seed_date then
						seed_release = candidate
					end
				end
			end
		end

		if seed_release and seed_release["release-group"] and seed_release["release-group"].id then
			local release_group_id = seed_release["release-group"].id
			local group_response = mb_get(
				`/release?release-group={release_group_id}&inc=recordings+artist-credits+genres+media+release-groups+recording-level-rels+work-rels&limit=100`
			)
			local group_data = if group_response then normalize_release_search_result(group_response) else nil
			local group_releases = if group_data then group_data.releases else nil
			if group_releases and #group_releases > 0 then
				local best_group_release: Release? = nil
				local best_group_score = -math.huge
				for _, candidate: Release in group_releases do
					if medium_has_track_count(candidate, track_count) then
						local score = 0
						if matches_top_artist(candidate) then
							score += 8
						else
							score -= 3
						end
						if candidate.status == "Official" then
							score += 4
						elseif candidate.status == "Promotion" then
							score += 3
						elseif candidate.status == "Pseudo-Release" then
							score -= 10
						end
						local candidate_media_count = medium_count(candidate)
						if candidate_media_count > 2 then
							score -= (candidate_media_count - 2) * 8
						end
						if trim_text(candidate.barcode) ~= "" then
							score += 6
						end
						if score > best_group_score then
							best_group_score = score
							best_group_release = candidate
						elseif score == best_group_score and best_group_release then
							local candidate_date = candidate.date or ""
							local best_date = best_group_release.date or ""
							if candidate_date ~= "" and best_date ~= "" and candidate_date < best_date then
								best_group_release = candidate
							end
						end
					end
				end
				local is_strong_country_seed = best_group_release
					and preferred_country
					and best_group_release.country == preferred_country
					and trim_text(best_group_release.barcode) ~= ""
				if
					best_group_release
					and (validate_release_durations(release_context.tracks, best_group_release) or is_strong_country_seed)
					and process_release(
						best_group_release,
						release_context,
						release_group_id,
						best_group_release["release-group"]
					)
				then
					return true
				end
			end
		end
	end

	local function should_expand_release_search(candidate: Release?): boolean
		local norm_title = normalize_title(search_title)
		if not candidate then
			return true
		end
		if artist_ids and not release_matches_artist_ids(candidate, artist_ids) then
			if track_count == 1 and not search_title_has_multibyte then
				return false
			end
			return true
		elseif #norm_title <= 4 then
			return true
		end
		local candidate_title = normalize_title(candidate.title)
		if string.find(candidate_title, "remix", 1, true) and not string.find(norm_title, "remix", 1, true) then
			return true
		end
		return false
	end
	local selected_release = select_best_release(
		releases_data.releases,
		track_count,
		preferred_language,
		preferred_country,
		search_title,
		artist_ids,
		release_context.tracks
	)
	if should_expand_release_search(selected_release) and not top_artist_id then
		local expanded_response = mb_get(
			`/release?query={http.encode_uri_component(lucene_query)}&limit=100&inc=recordings+artist-credits+genres+media+release-groups`
		)
		local expanded_data = if expanded_response then normalize_release_search_result(expanded_response) else nil
		if expanded_data and #expanded_data.releases > 0 then
			local expanded_release = select_best_release(
				expanded_data.releases,
				track_count,
				preferred_language,
				preferred_country,
				search_title,
				artist_ids,
				release_context.tracks
			)
			if expanded_release then
				selected_release = expanded_release
			end
		end
	end
	local deferred_release: Release? = nil
	if selected_release and artist_ids then
		local should_defer_for_artist_mismatch = not used_arid_primary_results
			and not release_matches_artist_ids(selected_release, artist_ids)
		if should_defer_for_artist_mismatch then
			deferred_release = selected_release
			selected_release = nil
		end
	end
	local selected_release_was_group_skipped = false
	if selected_release then
		local group_result = process_release_via_group(
			selected_release,
			track_count,
			preferred_language,
			preferred_country,
			search_title,
			artist_ids,
			release_context,
			true,
			false
		)
		if group_result == true then
			return true
		end
		if group_result == "skip" then
			selected_release_was_group_skipped = true
			selected_release = nil
		end
		if selected_release and selected_release.id and fetch_release_by_id(selected_release.id, release_context) then
			return true
		end
	end
	local skip_arid_fallback = top_artist_id
		and not selected_release_was_group_skipped
		and track_count > 1
		and (
			(not deferred_release and not search_title_has_multibyte and contains_multibyte(first_track_title))
			or (deferred_release and string.find(search_title, "/", 1, true) ~= nil)
		)
	if top_artist_id ~= nil and not skip_arid_fallback and not attempted_arid_primary then
		local resolved_top_artist_id = top_artist_id
		local fallback_query: string
		if first_track_title and first_track_title ~= "" then
			fallback_query =
				`arid:{resolved_top_artist_id} AND (release:{escape_lucene(search_title)} OR recording:{escape_lucene(
					first_track_title
				)})`
		else
			fallback_query = `arid:{resolved_top_artist_id} AND release:{escape_lucene(search_title)}`
		end
		local fallback_response = mb_get(`/release?query={http.encode_uri_component(fallback_query)}`)
		local fallback_data = if fallback_response then normalize_release_search_result(fallback_response) else nil
		if fallback_data and fallback_data.count > 0 then
			local fallback_ids = { [resolved_top_artist_id] = true }
			local fallback_candidates = fallback_data.releases
			local artist_filtered_fallback: { Release } = {}
			for _, candidate: Release in fallback_candidates do
				if release_matches_artist_ids(candidate, fallback_ids) then
					table.insert(artist_filtered_fallback, candidate)
				end
			end
			if #artist_filtered_fallback > 0 then
				fallback_candidates = artist_filtered_fallback
			end
			if track_count > 1 then
				local medium_count_candidates: { Release } = {}
				for _, candidate: Release in fallback_candidates do
					local has_matching_medium = medium_has_track_count(candidate, track_count)
					if has_matching_medium then
						table.insert(medium_count_candidates, candidate)
					end
				end
				if #medium_count_candidates > 0 then
					fallback_candidates = medium_count_candidates
				end
			end
			local fallback_release = select_best_release(
				fallback_candidates,
				track_count,
				preferred_language,
				preferred_country,
				search_title,
				fallback_ids,
				release_context.tracks
			)
			if not fallback_release and #fallback_candidates > 0 then
				fallback_release = select_best_release(
					fallback_candidates,
					nil,
					preferred_language,
					preferred_country,
					search_title,
					fallback_ids,
					release_context.tracks
				)
			end
			if not fallback_release and #fallback_candidates == 1 then
				fallback_release = fallback_candidates[1]
			end
			if fallback_release then
				local title_ok = titles_match_search(fallback_release.title, search_title)
				if not title_ok and first_track_title then
					title_ok = normalize_title(search_title) == normalize_title(first_track_title)
						and (not fallback_release.media or release_has_track_title(fallback_release, first_track_title))
				end
				if
					not title_ok
					and track_count == 1
					and not search_title_has_multibyte
					and fallback_data.releases
					and #fallback_data.releases == 1
				then
					title_ok = true
				end
				if title_ok then
					local group_result = process_release_via_group(
						fallback_release,
						track_count,
						preferred_language,
						preferred_country,
						search_title,
						fallback_ids,
						release_context,
						true,
						true
					)
					if group_result == true then
						return true
					end
					if group_result == false then
						return false
					end
					if fallback_release.id then
						return fetch_release_by_id(fallback_release.id, release_context)
					end
				end
			end
		end
	end
	local function deferred_title_is_strong(release: Release?): boolean
		if not release or type(release.title) ~= "string" then
			return false
		end
		if normalize_title(release.title) == "" or normalize_title(search_title) == "" then
			return false
		end
		return titles_match_search(release.title, search_title)
	end
	local defer_to_deferred_release = deferred_release
		and deferred_title_is_strong(deferred_release)
		and (
			search_title_has_multibyte
			or (local_track_count >= 2 and string.find(search_title, "/", 1, true) == nil)
			or declared_track_total > local_track_count
			or (inferred_year ~= nil and string.find(search_title, "%s") ~= nil)
		)
	local skip_recording_fallbacks = defer_to_deferred_release
		or (
			deferred_release
			and local_track_count >= 2
			and search_title_has_multibyte
			and string.find(search_title, "/", 1, true) == nil
		)
	local function try_artist_year_fallback()
		if
			not top_artist_id
			or not artist_name
			or not has_artist_name_match
			or not inferred_year
			or #release_context.tracks ~= 1
			or search_title_has_multibyte
			or string.find(search_title, "%s") == nil
		then
			return false
		end
		local year_query = `arid:{top_artist_id} AND date:{inferred_year}`
		local year_response = mb_get(
			`/release?query={http.encode_uri_component(year_query)}&limit=100&inc=recordings+genres+artist-credits+media+release-groups`
		)
		local year_data = if year_response then normalize_release_search_result(year_response) else nil
		local year_releases = if year_data then year_data.releases else nil
		if not year_releases or #year_releases == 0 then
			return false
		end
		local fallback_candidates: { Release } = {}
		for _, candidate: Release in year_releases do
			if compute_track_count(candidate) == 1 then
				if not artist_ids or release_matches_artist_ids(candidate, artist_ids) then
					table.insert(fallback_candidates, candidate)
				end
			end
		end
		if #fallback_candidates == 0 then
			return false
		end
		table.sort(fallback_candidates, function(a: Release, b: Release): boolean
			local a_country = preferred_country ~= nil and a.country == preferred_country
			local b_country = preferred_country ~= nil and b.country == preferred_country
			if a_country ~= b_country then
				return a_country
			end
			if not search_title_has_multibyte then
				local a_multibyte = contains_multibyte(a.title)
				local b_multibyte = contains_multibyte(b.title)
				if a_multibyte ~= b_multibyte then
					return a_multibyte
				end
			end
			local a_score = a.score or -math.huge
			local b_score = b.score or -math.huge
			if a_score ~= b_score then
				return a_score > b_score
			end
			return (a.date or "") < (b.date or "")
		end)
		local best_release: Release? = fallback_candidates[1]
		local current_best_release = best_release
		if
			current_best_release
			and (not current_best_release.media or #current_best_release.media == 0)
			and current_best_release.id
		then
			local best_release_response = mb_get(
				`/release/{current_best_release.id}?inc=recordings+genres+artist-credits+media+release-groups+recording-level-rels+work-rels`
			)
			best_release = if best_release_response then normalize_release(best_release_response) else nil
		end
		if not best_release or compute_track_count(best_release) ~= 1 then
			return false
		end
		local duration_diff = single_track_duration_diff(release_context.tracks, best_release)
		if duration_diff ~= nil and duration_diff > 15000 then
			return false
		end
		return process_release(best_release, release_context)
	end
	if defer_to_deferred_release and try_artist_year_fallback() then
		return true
	end
	local skip_alias_search = top_artist_id
		and inferred_year
		and #release_context.tracks == 1
		and not search_title_has_multibyte
		and string.find(search_title, "%s") ~= nil
	if top_artist_id and not skip_alias_search and not skip_recording_fallbacks then
		local normalized_artist_name = normalize_artist_name_for_lookup(artist_name)
		local use_broad_alias_query = track_count > 1 and #normalized_artist_name > 0 and #normalized_artist_name <= 4
		local alias_query = `alias:{escape_lucene(search_title)} AND arid:{top_artist_id}`
		if use_broad_alias_query then
			alias_query = `alias:{escape_lucene(search_title)}`
		end
		local can_use_inline_alias_release = local_track_count == 1 and track_count == 1
		local local_track_title_set: { [string]: boolean }? = nil
		if track_count > 1 then
			local resolved_title_set: { [string]: boolean } = {}
			local_track_title_set = resolved_title_set
			for _, local_track: LocalTrack in release_context.tracks do
				local norm_local = normalize_overlap_title(local_track.track_title)
				if norm_local ~= "" then
					resolved_title_set[norm_local] = true
				end
			end
		end
		local recording_response = mb_get(`/recording?query={http.encode_uri_component(alias_query)}`)
		local recording_data = if recording_response then normalize_recording_search_result(recording_response) else nil
		if recording_data then
			for _, recording: Recording in recording_data.recordings do
				local recording_releases = recording.releases
				if recording_releases == nil then
					continue
				end
				for _, release: Release in recording_releases do
					if release["track-count"] == track_count then
						if local_track_title_set then
							local title_matches = titles_match_search(release.title, search_title)
							local recording_matches = false
							local norm_recording = normalize_overlap_title(recording.title)
							if norm_recording ~= "" and local_track_title_set[norm_recording] then
								recording_matches = true
							end
							if not title_matches and not recording_matches then
								continue
							end
						end
						if can_use_inline_alias_release and recording.id and recording.title then
							local release_stub: Release = {
								id = release.id,
								title = release.title or recording.title,
								status = release.status,
								country = release.country,
								date = if release.date == nil and inferred_year
									then `{inferred_year}-01-01`
									else release.date,
								barcode = release.barcode,
								packaging = release.packaging,
								score = release.score,
								media = {
									{
										position = 1,
										["track-count"] = 1,
										tracks = {
											{
												title = recording.title,
												position = 1,
												length = recording.length,
												recording = { id = recording.id, relations = {} },
												["artist-credit"] = recording["artist-credit"],
											},
										},
									},
								},
								["track-count"] = 1,
								["artist-credit"] = if release["artist-credit"]
										and #release["artist-credit"] > 0
									then release["artist-credit"]
									else recording["artist-credit"],
								["release-group"] = release["release-group"],
								["text-representation"] = release["text-representation"],
								["cover-art-archive"] = release["cover-art-archive"],
							}
							local release_group = release_stub["release-group"]
							local release_group_id = release_group and release_group.id
							if process_release(release_stub, release_context, release_group_id, release_group) then
								return true
							end
						end
						if release.id and fetch_release_by_id(release.id, release_context) then
							return true
						end
					end
				end
			end
		end
	end
	if
		not skip_recording_fallbacks
		and top_artist_id
		and #release_context.tracks == 1
		and not search_title_has_multibyte
	then
		local translit_first_track_title = representative_track_title()
		if translit_first_track_title and translit_first_track_title ~= "" then
			local translit_query = `arid:{top_artist_id} AND recording:{escape_lucene(translit_first_track_title)}`
			local translit_response = mb_get(
				`/release?query={http.encode_uri_component(translit_query)}&limit=100&inc=recordings+artist-credits+genres+media+release-groups`
			)
			local translit_data = if translit_response then normalize_release_search_result(translit_response) else nil
			if translit_data and #translit_data.releases > 0 then
				local translit_ids = { [top_artist_id] = true }
				local translit_release = select_best_release(
					translit_data.releases,
					track_count,
					preferred_language,
					preferred_country,
					search_title,
					translit_ids,
					release_context.tracks
				)
				if not translit_release then
					translit_release = translit_data.releases[1]
				end
				if translit_release then
					local group_result = process_release_via_group(
						translit_release,
						track_count,
						preferred_language,
						preferred_country,
						search_title,
						translit_ids,
						release_context,
						false,
						true
					)
					if group_result == true then
						return true
					end
					if group_result == false then
						return false
					end
					if translit_release.id and fetch_release_by_id(translit_release.id, release_context) then
						return true
					end
				end
			end
		end
	end
	if not defer_to_deferred_release and try_artist_year_fallback() then
		return true
	end
	if deferred_release then
		local deferred_track_count = #release_context.tracks
		if deferred_track_count >= 2 and deferred_release.id then
			local deferred_response = mb_get(
				`/release/{deferred_release.id}?inc=recordings+genres+artist-credits+media+release-groups+recording-level-rels+work-rels`
			)
			local deferred_data = if deferred_response then normalize_release(deferred_response) else nil
			if deferred_data then
				local artist_ok = true
				if artist_ids and has_artist_name_match then
					artist_ok = release_matches_artist_ids(deferred_data, artist_ids)
				end
				local overlap = count_track_title_overlap(release_context.tracks, deferred_data)
				local allow_partial_release_mismatch = declared_track_total > deferred_track_count
				local artist_mismatch = not artist_ok and not allow_partial_release_mismatch
				local required_overlap_for_artist_mismatch = math.max(2, math.floor(deferred_track_count / 2))
				local rejected_for_artist = artist_mismatch
					and (deferred_track_count <= 2 or overlap < required_overlap_for_artist_mismatch)
				if
					not rejected_for_artist
					and overlap >= 1
					and validate_release_durations(release_context.tracks, deferred_data)
				then
					return process_release(deferred_data, release_context)
				end
			end
		elseif deferred_track_count == 1 and deferred_release.id then
			local norm_title = normalize_title(search_title)
			local declared_total = max_declared_track_total(release_context.tracks)
			local deferred_response = mb_get(
				`/release/{deferred_release.id}?inc=recordings+genres+artist-credits+media+release-groups+recording-level-rels+work-rels`
			)
			local deferred_data = if deferred_response then normalize_release(deferred_response) else nil
			if deferred_data then
				local allow_partial_release_mismatch = declared_total > deferred_track_count
				local artist_mismatch = artist_ids and not release_matches_artist_ids(deferred_data, artist_ids)
				local strict_artist_guard = has_artist_name_match or not search_title_has_multibyte
				local rejected_for_artist = artist_mismatch
					and not allow_partial_release_mismatch
					and strict_artist_guard
				if
					not rejected_for_artist
					and count_track_title_overlap(release_context.tracks, deferred_data) >= 1
					and #norm_title >= 6
				then
					return process_release(deferred_data, release_context)
				end
			end
		elseif deferred_release.id then
			return fetch_release_by_id(deferred_release.id, release_context)
		end
	end
	local fallback_updates = apply_artist_fallback_updates(release_context, artist_candidate_pool, preferred_country)
	if fallback_updates == 0 then
		local cleared_artist_ids: { [number]: boolean } = {}
		local function clear_artist_id(artist: LocalArtist)
			if artist.db_id and not cleared_artist_ids[artist.db_id] then
				cleared_artist_ids[artist.db_id] = true
				local artist_layer = provider:layer(artist.db_id)
				artist_layer:set_id("artist_id", "")
				artist_layer:save()
			end
		end
		for_each_local_artist(release_context, clear_artist_id)
		for _, track: LocalTrack in release_context.tracks do
			if track.db_id then
				local track_layer = provider:layer(track.db_id)
				track_layer:set_id("recording_id", "")
				track_layer:save()
			end
		end
	end
	return false
end

module.search_and_fetch_release = search_and_fetch_release

return module
