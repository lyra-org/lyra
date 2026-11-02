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
local compute_track_count = track_helpers.compute_track_count
local medium_has_track_count = track_helpers.medium_has_track_count
local release_lists_tracks = track_helpers.release_lists_tracks
local single_track_duration_diff = track_helpers.single_track_duration_diff
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

		-- A candidate browsed without its track list can't be checked here; the
		-- caller fetches it and checks it before using it.
		if release_lists_tracks(candidate) then
			if not acceptance.localization_keeps_recordings(candidate, best) then
				continue
			end
			score += acceptance.shared_recording_count(candidate, best)
		end
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
	keep_selected: boolean,
	localization_pool: { Release }?
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
		if
			not release_lists_tracks(candidate)
			or (fits_anchor(candidate) and acceptance.carries_verified_recording(candidate, local_release_info))
		then
			table.insert(fitting_candidates, candidate)
		end
	end
	candidates = fitting_candidates
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
	-- The edition standing in for `identity` may be any of the group's.
	local pool = localization_pool or candidates
	best_edition = localize_edition(identity, pool, track_count, preferred_language, preferred_country)
	if not best_edition then
		return nil, nil
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

-- An edition to try, and the localized edition that may stand in for it.
export type EditionPick = { identity: Release, localized: Release? }

-- Up to `limit` editions of the group to try, best first: each pick the one
-- `select_release_group_candidate` makes among the identities not yet picked,
-- with its stand-in from the whole group. The anchor is kept, when
-- `keep_selected` asks it to be, only until it has been picked, even if the
-- browse left it out.
local function ranked_release_group_candidates(
	candidates: { Release },
	local_release_info: LocalRelease,
	search_title: string?,
	artist_ids: ArtistIdSet?,
	anchor: Release?,
	keep_selected: boolean,
	limit: number
): { EditionPick }
	local picks: { EditionPick } = {}
	local remaining = candidates
	local anchor_picked = false
	while #picks < limit and #remaining > 0 do
		local edition, identity = select_release_group_candidate(
			remaining,
			local_release_info,
			search_title,
			artist_ids,
			anchor,
			keep_selected and not anchor_picked,
			candidates
		)
		if edition == nil or identity == nil then
			break
		end
		table.insert(picks, { identity = identity, localized = if edition ~= identity then edition else nil })
		local picked_anchor = anchor ~= nil and identity.id == anchor.id
		anchor_picked = anchor_picked or picked_anchor
		local rest: { Release } = {}
		for _, candidate in remaining do
			if candidate ~= identity and candidate.id ~= identity.id then
				table.insert(rest, candidate)
			end
		end
		-- Only the anchor, absent from the browse, may be picked without
		-- leaving the pool smaller.
		if #rest == #remaining and not picked_anchor then
			break
		end
		remaining = rest
	end
	return picks
end

module.release_from_candidates = release_from_candidates
module.ranked_release_group_candidates = ranked_release_group_candidates

return module
