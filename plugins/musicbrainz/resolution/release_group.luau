--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local acceptance = require("./acceptance")
local local_release = require("./local_release")
local ranking = require("./ranking")
local text_helpers = require("./text_helpers")
local track_helpers = require("./track_helpers")

type ArtistIdSet = local_release.ArtistIdSet
type LocalRelease = local_release.LocalRelease
type EditionHints = track_helpers.EditionHints
type LocalTrack = track_helpers.LocalTrack
type ReleaseMedium = track_helpers.ReleaseMedium
type ReleaseMediumTrack = track_helpers.ReleaseMediumTrack
type Release = track_helpers.Release

local normalize_title = text_helpers.normalize_title
local contains_multibyte = text_helpers.contains_multibyte
local release_has_barcode = text_helpers.release_has_barcode
local titles_match_search = text_helpers.titles_match_search
local compute_track_count = track_helpers.compute_track_count
local medium_has_track_count = track_helpers.medium_has_track_count
local max_declared_track_total = track_helpers.max_declared_track_total
local count_track_title_overlap = track_helpers.count_track_title_overlap
local release_has_track_title = track_helpers.release_has_track_title
local release_lists_tracks = track_helpers.release_lists_tracks
local validate_release_durations = track_helpers.validate_release_durations
local single_track_duration_diff = track_helpers.single_track_duration_diff
local max_track_count_diff = ranking.max_track_count_diff
local release_matches_artist_ids = ranking.release_matches_artist_ids
local select_best_release = ranking.select_best_release

local module = {}

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
	local best_media_count = if best.media ~= nil then #best.media else 0

	local best_localized: Release? = nil
	local best_loc_score = -1
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

		if not acceptance.localization_keeps_recordings(candidate, best) then
			continue
		end
		score += acceptance.shared_recording_count(candidate, best)
		if score > best_loc_score or (score == best_loc_score and should_prefer_on_tie(candidate, best_localized)) then
			best_loc_score = score
			best_localized = candidate
		end
	end

	return best_localized or best
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
	if local_tracks ~= nil and max_declared_track_total(local_tracks) > #local_tracks then
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

local function select_partial_release_candidate(
	candidates: { Release },
	local_tracks: { LocalTrack }?,
	track_count: number?,
	search_title: string?,
	artist_ids: ArtistIdSet?,
	selected_release: Release?,
	preferred_country: string?
): Release?
	if local_tracks == nil or #local_tracks == 0 then
		return nil
	end
	local local_track_count = #local_tracks
	local declared_track_total = max_declared_track_total(local_tracks)
	if declared_track_total <= local_track_count then
		return nil
	end
	if track_count ~= nil and track_count ~= local_track_count then
		return nil
	end

	local best: Release? = nil
	local best_score = -math.huge
	for _, candidate: Release in candidates do
		if candidate.status ~= "Promotion" then
			continue
		end
		if compute_track_count(candidate) ~= local_track_count then
			continue
		end
		if
			search_title
			and normalize_title(search_title) ~= ""
			and not titles_match_search(candidate.title, search_title)
		then
			continue
		end
		if artist_ids and not release_matches_artist_ids(candidate, artist_ids) then
			continue
		end
		if count_track_title_overlap(local_tracks, candidate) < local_track_count then
			continue
		end
		if not validate_release_durations(local_tracks, candidate) then
			continue
		end

		local score = 0
		if selected_release and candidate.id == selected_release.id then
			score += 20
		end
		if preferred_country and candidate.country == preferred_country then
			score += 5
		end
		if normalize_title(candidate.title) == normalize_title(search_title) then
			score += 5
		end
		if release_has_barcode(candidate) then
			score += 1
		end
		if score > best_score then
			best_score = score
			best = candidate
		elseif score == best_score and best then
			local candidate_date = candidate.date or ""
			local best_date = best.date or ""
			if candidate_date ~= "" and best_date ~= "" and candidate_date < best_date then
				best = candidate
			end
		end
	end

	return best
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

-- Picks an edition from a release group's `candidates` for the local release.
-- `anchor` is the group's release a search matched, if any, taken from
-- `candidates`. Returns the edition to apply and the edition that must pass
-- acceptance for it: they differ when a localized edition stands in.
local function select_release_group_candidate(
	candidates: { Release },
	local_release_info: LocalRelease,
	search_title: string?,
	artist_ids: ArtistIdSet?,
	anchor: Release?,
	keep_selected: boolean
): (Release?, Release?)
	if #candidates == 0 then
		return nil, nil
	end
	local local_tracks = local_release_info.context.tracks
	local track_count = if local_release_info.track_count > 0 then local_release_info.track_count else nil
	local preferred_language = local_release_info.preferred_language
	local preferred_country = local_release_info.preferred_country
	local edition_hints = local_release_info.edition_hints
	local selected_release = anchor
	-- Only editions that can pass acceptance's edition rule compete. Editions
	-- browsed without a track listing can't be checked yet and are kept.
	local fits_anchor = acceptance.edition_filter(anchor, local_tracks)
	local fitting_candidates: { Release } = {}
	for _, candidate: Release in candidates do
		if not release_lists_tracks(candidate) or fits_anchor(candidate) then
			table.insert(fitting_candidates, candidate)
		end
	end
	candidates = fitting_candidates
	local partial_release = select_partial_release_candidate(
		candidates,
		local_tracks,
		track_count,
		search_title,
		artist_ids,
		selected_release,
		preferred_country
	)
	if partial_release then
		return partial_release, partial_release
	end
	local best_edition = select_best_release(
		candidates,
		track_count,
		preferred_language,
		preferred_country,
		search_title,
		artist_ids,
		local_tracks,
		1,
		edition_hints
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
				1,
				edition_hints
			)
			if not best_edition and #medium_candidates == 1 then
				best_edition = medium_candidates[1]
			end
		end
	end
	if not best_edition then
		return nil, nil
	end
	-- `identity` is the edition that must pass acceptance; a localized edition
	-- only stands in for it.
	local identity: Release = best_edition
	best_edition = localize_edition(identity, candidates, track_count, preferred_language, preferred_country)
	if not best_edition then
		return nil, nil
	end
	local localized = best_edition
	best_edition =
		prefer_official_single_track_edition(best_edition, candidates, local_tracks, track_count, preferred_country)
	if not best_edition then
		return nil, nil
	end
	if best_edition ~= localized then
		identity = best_edition
	end
	if
		keep_selected
		and selected_release
		and should_keep_selected_release(
			selected_release,
			best_edition,
			search_title,
			preferred_language,
			local_tracks,
			track_count
		)
	then
		best_edition = selected_release
		identity = selected_release
	end
	return best_edition, identity
end

module.release_from_candidates = release_from_candidates
module.select_release_group_candidate = select_release_group_candidate

return module
