--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

-- Every candidate, from any path, is checked by acceptance against its full
-- track list before it is applied. A rejected candidate returns false and the
-- caller moves on to its next candidate. Only a release the user set manually
-- skips the check.

local acceptance = require("./acceptance")
local apply = require("./apply")
local local_release = require("./local_release")
local musicbrainz_api = require("./musicbrainz_api")
local release_group = require("./release_group")
local track_helpers = require("./track_helpers")

type ArtistIdSet = local_release.ArtistIdSet
type LocalRelease = local_release.LocalRelease
type Release = track_helpers.Release
type ReleaseGroup = track_helpers.ReleaseGroup
type EditionPick = release_group.EditionPick

local browse_release_group = musicbrainz_api.browse_release_group
local fetch_release_details = musicbrainz_api.fetch_release_details
local release_from_candidates = release_group.release_from_candidates
local ranked_release_group_candidates = release_group.ranked_release_group_candidates
local release_lists_tracks = track_helpers.release_lists_tracks

local module = {}

-- Rejections are remembered per local release, so a later path that reaches
-- the same release skips it instead of fetching and checking it again. A
-- rejection against another release's anchor only holds for that anchor.
local function remembered_rejection(release_id: string?, local_release_info: LocalRelease, anchor: Release?): boolean
	return release_id ~= nil
		and (anchor == nil or anchor.id == release_id)
		and local_release_info.rejected_release_ids[release_id] == true
end

-- `release` with every include applying it needs, fetching it unless it is
-- complete already.
local function complete_release(release: Release): Release?
	if release.complete then
		return release
	end
	local release_id = release.id
	if release_id == nil then
		return nil
	end
	return fetch_release_details(release_id)
end

-- `releases` with `release` in place of any listing of it.
local function with_release(releases: { Release }, release: Release): { Release }
	local result: { Release } = {}
	for _, listed in releases do
		if listed.id ~= release.id then
			table.insert(result, listed)
		end
	end
	table.insert(result, release)
	return result
end

local function check(release: Release, local_release_info: LocalRelease, anchor: Release?): boolean
	-- After a failed request nothing is applied, so nothing is worth rejecting.
	if musicbrainz_api.request_failed() then
		return false
	end
	local accepted, reason = acceptance.accepts(release, local_release_info, anchor)
	if not accepted then
		warn(`musicbrainz: rejected release {release.id} "{release.title}": {reason}`)
		local release_id = release.id
		if release_id ~= nil and (anchor == nil or anchor.id == release_id) then
			local_release_info.rejected_release_ids[release_id] = true
		end
	end
	return accepted
end

function module.try_release(
	release: Release,
	local_release_info: LocalRelease,
	release_group_id: string?,
	release_group_info: ReleaseGroup?
): boolean
	if remembered_rejection(release.id, local_release_info, nil) then
		return false
	end
	local full_release = complete_release(release)
	if not full_release or not check(full_release, local_release_info, nil) then
		return false
	end
	return apply.process_release(full_release, local_release_info, release_group_id, release_group_info)
end

function module.try_release_id(release_id: string, local_release_info: LocalRelease): boolean
	return module.try_release({ id = release_id }, local_release_info)
end

-- Applies a release the user chose without checking it: acceptance exists to
-- catch wrong guesses, and this one is not a guess.
function module.apply_release_id(release_id: string, local_release_info: LocalRelease): boolean
	local release = fetch_release_details(release_id)
	return release ~= nil and apply.process_release(release, local_release_info)
end

-- Editions of a group tried per match, best first. Each may need a full
-- fetch at the MusicBrainz rate of a request a second, on top of the searches
-- and browse a refresh already spends of its 60 seconds.
local MAX_EDITION_TRIES = 3

-- The edition `pick` applies, if its identity passes acceptance against
-- `anchor`: its localized stand-in when that keeps the identity's recordings
-- and passes too, else the identity.
local function try_pick(pick: EditionPick, local_release_info: LocalRelease, anchor: Release): Release?
	local identity_edition = pick.identity
	if remembered_rejection(identity_edition.id, local_release_info, anchor) then
		return nil
	end
	local identity = complete_release(identity_edition)
	if not identity or not check(identity, local_release_info, anchor) then
		return nil
	end
	local stand_in = pick.localized
	if stand_in == nil or stand_in.id == identity.id then
		return identity
	end
	local localized = complete_release(stand_in)
	local verified_info = local_release.with_verified_recordings(
		local_release_info,
		acceptance.assigned_recording_ids(identity, local_release_info.context.tracks)
	)
	if
		localized
		and acceptance.localization_keeps_recordings(localized, identity)
		and acceptance.carries_verified_recording(localized, local_release_info)
		and check(localized, verified_info, identity)
	then
		return localized
	end
	return identity
end

-- Applies the best edition from `selected_release`'s release group. The edition
-- must pass acceptance with `selected_release` as its anchor. A localized
-- edition stands in for it only if, with its full track list, it keeps the
-- edition's recordings and passes acceptance against it. Only when the group
-- offers no edition at all is `selected_release` tried on its own; a selected
-- edition that is rejected is not replaced by it. A browse lists at most a
-- page of the group, so `selected_release`, when it lists its tracks, competes
-- even if the browse left it out.
function module.try_release_group(
	selected_release: Release,
	local_release_info: LocalRelease,
	search_title: string?,
	artist_ids: ArtistIdSet?,
	keep_selected: boolean
): boolean
	local track_count = local_release_info.track_count
	local release_group = selected_release["release-group"]
	local release_group_id = release_group and release_group.id
	-- The group's own copy of the release, browsed with every include it needs,
	-- spares fetching it; a search result's copy may list tracks without them.
	local function try_selected_release(group_copy: Release?): boolean
		if group_copy then
			return module.try_release(group_copy, local_release_info)
		end
		local release_id = selected_release.id
		return release_id ~= nil and module.try_release_id(release_id, local_release_info)
	end
	if not release_group_id then
		return try_selected_release(nil)
	end
	local listed: { Release } = browse_release_group(release_group_id, track_count == 1, track_count) or {}
	local group_copy = release_from_candidates(listed, selected_release.id, nil)
	local candidates = listed
	if release_lists_tracks(selected_release) and not (group_copy and group_copy.complete) then
		candidates = with_release(listed, selected_release)
	end
	if #candidates == 0 then
		return try_selected_release(group_copy)
	end
	local edition_anchor = group_copy or selected_release
	local picks = ranked_release_group_candidates(
		candidates,
		local_release_info,
		search_title,
		artist_ids,
		edition_anchor,
		keep_selected,
		MAX_EDITION_TRIES
	)
	if #picks == 0 then
		return try_selected_release(group_copy)
	end
	-- The best edition that passes acceptance, as the next best after one the
	-- local files reject. A failed request ends the walk: nothing is applied.
	local edition: Release? = nil
	for _, pick in picks do
		if musicbrainz_api.request_failed() then
			return false
		end
		edition = try_pick(pick, local_release_info, edition_anchor)
		if edition then
			break
		end
	end
	if edition == nil then
		return false
	end
	local best_rg_info = release_group
	local edition_rg_info = edition["release-group"]
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
	return apply.process_release(edition, local_release_info, release_group_id, best_rg_info)
end

return module
