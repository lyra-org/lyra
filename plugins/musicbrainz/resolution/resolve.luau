--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

-- The only way a release gets applied: every candidate, from any path, is
-- checked by acceptance against its full track list first. A rejected
-- candidate returns false and the caller moves on to its next candidate.

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

local browse_release_group = musicbrainz_api.browse_release_group
local fetch_release_details = musicbrainz_api.fetch_release_details
local release_from_candidates = release_group.release_from_candidates
local select_release_group_candidate = release_group.select_release_group_candidate
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

-- `release` with its full track list, fetching it when the listing lacks one.
local function with_track_list(release: Release): Release?
	if release_lists_tracks(release) then
		return release
	end
	local release_id = release.id
	if release_id == nil then
		return nil
	end
	return fetch_release_details(release_id)
end

local function check(release: Release, local_release_info: LocalRelease, anchor: Release?): boolean
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
	local full_release = with_track_list(release)
	if not full_release or not check(full_release, local_release_info, nil) then
		return false
	end
	return apply.process_release(full_release, local_release_info, release_group_id, release_group_info)
end

function module.try_release_id(release_id: string, local_release_info: LocalRelease): boolean
	if remembered_rejection(release_id, local_release_info, nil) then
		return false
	end
	local release = fetch_release_details(release_id)
	if not release or not check(release, local_release_info, nil) then
		return false
	end
	return apply.process_release(release, local_release_info)
end

-- Applies the best edition from `selected_release`'s release group. The edition
-- must pass acceptance with `selected_release` as its anchor. A localized
-- edition stands in for it only if it keeps the edition's recordings and
-- passes acceptance against it.
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
	if not release_group_id then
		return false
	end
	local candidates = browse_release_group(release_group_id, track_count == 1, track_count)
	if not candidates or #candidates == 0 then
		return false
	end
	local anchor = release_from_candidates(candidates, selected_release.id, selected_release)
	local selected_edition, identity_edition =
		select_release_group_candidate(candidates, local_release_info, search_title, artist_ids, anchor, keep_selected)
	if not selected_edition or not identity_edition then
		return false
	end
	if remembered_rejection(identity_edition.id, local_release_info, anchor) then
		return false
	end
	local identity = with_track_list(identity_edition)
	if not identity or not check(identity, local_release_info, anchor) then
		return false
	end
	local edition = identity
	if selected_edition.id ~= identity.id then
		local localized = selected_edition
		local verified_info = local_release.with_verified_recordings(
			local_release_info,
			acceptance.assigned_recording_ids(identity, local_release_info.context.tracks)
		)
		if
			acceptance.localization_keeps_recordings(localized, identity)
			and check(localized, verified_info, identity)
		then
			edition = localized
		end
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
