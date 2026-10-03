--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local editions = require("@lyra/editions")

local local_release = require("./local_release")
local text_helpers = require("./text_helpers")
local track_helpers = require("./track_helpers")

type ArtistIdSet = local_release.ArtistIdSet
type EditionHints = track_helpers.EditionHints
type LocalTrack = track_helpers.LocalTrack
type ReleaseArtistCredit = track_helpers.ReleaseArtistCredit
type ReleaseMedium = track_helpers.ReleaseMedium
type ReleaseMediumTrack = track_helpers.ReleaseMediumTrack
type Release = track_helpers.Release

local normalize_title = text_helpers.normalize_title
local normalize_overlap_title = text_helpers.normalize_overlap_title
local contains_multibyte = text_helpers.contains_multibyte
local compute_track_count = track_helpers.compute_track_count
local max_declared_track_total = track_helpers.max_declared_track_total

local module = {}

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

local function edition_hint_score(release: Release, hints: EditionHints?): number
	if not hints then
		return 0
	end
	local score = 0
	if hints.barcode and editions.barcodes_match(hints.barcode, release.barcode) then
		score += 12
	end
	local local_formats = hints.media_formats
	if local_formats and #local_formats > 0 and release.media then
		local raw_formats: { string } = {}
		for _, medium: ReleaseMedium in release.media do
			if medium.format then
				table.insert(raw_formats, medium.format)
			end
		end
		local release_formats = editions.media_formats(raw_formats)
		if #release_formats > 0 then
			score += if editions.media_formats_match(local_formats, release_formats) then 3 else -3
		end
	end
	return score
end

local function select_best_release(
	releases: { Release },
	track_count: number?,
	preferred_language: string?,
	preferred_country: string?,
	expected_title: string?,
	artist_ids: ArtistIdSet?,
	local_tracks: { LocalTrack }?,
	artist_mbid_weight: number?,
	edition_hints: EditionHints?
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
		score += edition_hint_score(release, edition_hints)
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

module.max_track_count_diff = max_track_count_diff
module.release_matches_artist_ids = release_matches_artist_ids
module.select_best_release = select_best_release

return module
