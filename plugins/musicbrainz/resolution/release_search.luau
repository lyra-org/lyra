--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local apply = require("./apply")
local local_release = require("./local_release")
local musicbrainz_api = require("./musicbrainz_api")
local ranking = require("./ranking")
local resolve = require("./resolve")
local text_helpers = require("./text_helpers")
local track_helpers = require("./track_helpers")

local http = require("@harmony/http")

type ArtistIdSet = local_release.ArtistIdSet
type LocalRelease = local_release.LocalRelease
type LocalArtist = track_helpers.LocalArtist
type LocalTrack = track_helpers.LocalTrack
type ReleaseArtist = track_helpers.ReleaseArtist
type Release = track_helpers.Release
type ReleaseSearchResult = musicbrainz_api.ReleaseSearchResult
type Recording = musicbrainz_api.Recording

local is_blank_text = text_helpers.is_blank_text
local is_generic_artist = text_helpers.is_generic_artist
local clean_release_title = text_helpers.clean_release_title
local contains_multibyte = text_helpers.contains_multibyte
local infer_lookup_hints = text_helpers.infer_lookup_hints
local normalize_title = text_helpers.normalize_title
local normalize_overlap_title = text_helpers.normalize_overlap_title
local normalize_artist_name_for_lookup = text_helpers.normalize_artist_name_for_lookup
local artist_name_matches = text_helpers.artist_name_matches
local trim_text = text_helpers.trim_text
local titles_match_search = text_helpers.titles_match_search
local sort_tracks_for_refresh = track_helpers.sort_tracks_for_refresh
local compute_track_count = track_helpers.compute_track_count
local medium_has_track_count = track_helpers.medium_has_track_count
local max_declared_track_total = track_helpers.max_declared_track_total
local mb_get = musicbrainz_api.mb_get
local escape_lucene = musicbrainz_api.escape_lucene
local normalize_artist_search_result = musicbrainz_api.normalize_artist_search_result
local normalize_release_search_result = musicbrainz_api.normalize_release_search_result
local normalize_recording_search_result = musicbrainz_api.normalize_recording_search_result
local select_best_release = ranking.select_best_release
local release_matches_artist_ids = ranking.release_matches_artist_ids
local try_release = resolve.try_release
local try_release_id = resolve.try_release_id
local try_release_group = resolve.try_release_group
local for_each_local_artist = local_release.for_each_local_artist
local normalize_lookup_context = local_release.normalize_lookup_context

-- Alias search can list many releases; fetching each to check it is slow.
local MAX_ALIAS_RELEASE_FETCHES = 5

local module = {}

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

function module.search_and_fetch_release(local_release_info: LocalRelease): boolean
	local release_context = local_release_info.context
	local artist_name = local_release_info.artist_name
	local release_title = local_release_info.release_title
	local track_count = local_release_info.track_count
	local preferred_language = local_release_info.preferred_language
	local preferred_country = local_release_info.preferred_country

	local marked_artist_ids: { [number]: boolean } = {}
	local function mark_artist_unmatched(artist: LocalArtist)
		if artist.db_id and not marked_artist_ids[artist.db_id] then
			marked_artist_ids[artist.db_id] = true
			apply.mark_unmatched(artist.db_id, { "artist_id" })
		end
	end
	local function mark_lookup_ids_unmatched(): boolean
		for_each_local_artist(release_context, mark_artist_unmatched)
		for _, track: LocalTrack in release_context.tracks do
			if track.db_id then
				apply.mark_unmatched(track.db_id, { "recording_id", "release_track_id", "work_id" })
			end
		end
		return false
	end

	if is_blank_text(artist_name) or is_generic_artist(artist_name) then
		artist_name = nil
	end

	local search_title = clean_release_title(release_title)
	if is_blank_text(search_title) and #release_context.tracks > 0 then
		local first_track = release_context.tracks[1]
		search_title = clean_release_title(if first_track then first_track.track_title else "")
	end
	if is_blank_text(search_title) then
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
			local_release_info = local_release.with_preferred_country(local_release_info, preferred_country)
		end
		if not artist_ids and top_artist_id then
			artist_ids = { [top_artist_id] = true }
		end
		if not artist_ids then
			return mark_lookup_ids_unmatched()
		end
		if top_artist_id then
			top_artist_ids = { [top_artist_id] = true }
		end
		local_release_info = local_release.with_artist_search(local_release_info, artist_ids, not has_artist_name_match)
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
						and candidate.id
						and try_release_id(candidate.id, local_release_info)
					then
						return true
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
		return mark_lookup_ids_unmatched()
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
			local group_response =
				mb_get(`/release?release-group={release_group_id}&inc={musicbrainz_api.RELEASE_INCLUDES}&limit=100`)
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
				if
					best_group_release
					and try_release(
						best_group_release,
						local_release_info,
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
	local selected_release_rejected = false
	if selected_release then
		if try_release_group(selected_release, local_release_info, search_title, artist_ids, true) then
			return true
		end
		if selected_release.id and try_release_id(selected_release.id, local_release_info) then
			return true
		end
		selected_release_rejected = true
	end
	local skip_arid_fallback = top_artist_id
		and not selected_release_rejected
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
				if try_release_group(fallback_release, local_release_info, search_title, fallback_ids, true) then
					return true
				end
				if fallback_release.id and try_release_id(fallback_release.id, local_release_info) then
					return true
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
		return try_release(fallback_candidates[1], local_release_info)
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
		-- For several tracks, a release must also carry the title or a local
		-- track as the recording; one track is identified by the alias itself.
		local local_track_titles: { [string]: boolean } = {}
		for _, local_track: LocalTrack in release_context.tracks do
			local_track_titles[normalize_overlap_title(local_track.track_title)] = true
		end
		local recording_response = mb_get(`/recording?query={http.encode_uri_component(alias_query)}`)
		local recording_data = if recording_response then normalize_recording_search_result(recording_response) else nil
		local fetches = 0
		if recording_data then
			for _, recording: Recording in recording_data.recordings do
				local recording_releases = recording.releases
				if recording_releases == nil then
					continue
				end
				local recording_is_local_track = local_track_titles[normalize_overlap_title(recording.title)] == true
				for _, release: Release in recording_releases do
					local release_id = release.id
					if
						not release_id
						or release["track-count"] ~= track_count
						or (
							track_count > 1
							and not recording_is_local_track
							and not titles_match_search(release.title, search_title)
						)
					then
						continue
					end
					if fetches >= MAX_ALIAS_RELEASE_FETCHES then
						break
					end
					fetches += 1
					if try_release_id(release_id, local_release_info) then
						return true
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
					if try_release_group(translit_release, local_release_info, search_title, translit_ids, false) then
						return true
					end
					if translit_release.id and try_release_id(translit_release.id, local_release_info) then
						return true
					end
				end
			end
		end
	end
	if not defer_to_deferred_release and try_artist_year_fallback() then
		return true
	end
	if deferred_release and deferred_release.id and try_release_id(deferred_release.id, local_release_info) then
		return true
	end
	return mark_lookup_ids_unmatched()
end

return module
