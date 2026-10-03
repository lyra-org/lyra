--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

-- Decides whether a MusicBrainz release plausibly is the local release. Every
-- path that applies a release asks this one question; paths only find and
-- rank candidates.

local local_release = require("./local_release")
local text_helpers = require("./text_helpers")
local track_helpers = require("./track_helpers")

type LocalRelease = local_release.LocalRelease
type LocalTrack = track_helpers.LocalTrack
type ReleaseArtistCredit = track_helpers.ReleaseArtistCredit
type Release = track_helpers.Release

-- Provisional: how far a track's length may be from the local track's and
-- still count as the same recording. Applies to one-track releases, and to
-- every track when lengths are the only evidence beyond the artist.
local TRACK_DURATION_TOLERANCE_MS = 5000
-- Share of local track titles a larger release must contain to match a subset of it.
local SUBSET_TITLE_OVERLAP_RATIO = 0.8
-- Share of recordings a localized edition must keep from the edition it replaces.
local LOCALIZATION_RECORDING_SHARE = 0.8

local normalize_title = text_helpers.normalize_title
local clean_release_title = text_helpers.clean_release_title
local titles_match_search = text_helpers.titles_match_search
local artist_names_agree = text_helpers.artist_names_agree
local is_blank_text = text_helpers.is_blank_text
local is_generic_artist = text_helpers.is_generic_artist
local is_placeholder_text = text_helpers.is_placeholder_text
local compute_track_count = track_helpers.compute_track_count
local medium_has_track_count = track_helpers.medium_has_track_count
local max_declared_track_total = track_helpers.max_declared_track_total
local count_track_title_overlap = track_helpers.count_track_title_overlap
local validate_release_durations = track_helpers.validate_release_durations
local single_track_duration_diff = track_helpers.single_track_duration_diff
local max_assigned_duration_diff = track_helpers.max_assigned_duration_diff
local release_lists_tracks = track_helpers.release_lists_tracks
local release_has_any_recording = track_helpers.release_has_any_recording
local flatten_release_tracks = track_helpers.flatten_release_tracks
local assign_release_tracks = track_helpers.assign_release_tracks

local module = {}

local function local_artist_names(local_release_info: LocalRelease): { string }
	local names: { string } = {}
	local function add(name: string?)
		if
			type(name) == "string"
			and not is_blank_text(name)
			and not is_generic_artist(name)
			and not is_placeholder_text(name)
		then
			table.insert(names, name)
		end
	end
	add(local_release_info.artist_name)
	local_release.for_each_local_artist(local_release_info.context, function(artist)
		add(artist.artist_name)
	end)
	return names
end

local function for_each_credit(release: Release, visit: (ReleaseArtistCredit) -> ())
	local release_credits = release["artist-credit"]
	if release_credits then
		for _, credit in release_credits do
			visit(credit)
		end
	end
	for _, mb_track in flatten_release_tracks(release) do
		local track_credits = mb_track.artist_credits
		if track_credits then
			for _, credit in track_credits do
				visit(credit)
			end
		end
	end
end

-- Whether the release credits the local artist, on the release or any track. A
-- local release without a usable artist name has nothing to disagree with.
local function artist_agrees(release: Release, local_release_info: LocalRelease): boolean
	if local_release_info.compilation_release then
		return true
	end
	local names = local_artist_names(local_release_info)
	if #names == 0 then
		return true
	end
	local artist_ids = local_release_info.artist_ids
	local agrees = false
	for_each_credit(release, function(credit)
		if agrees then
			return
		end
		local artist = credit.artist
		if artist_ids and artist and artist.id and artist_ids[artist.id] then
			agrees = true
			return
		end
		local credit_names = { credit.name, artist and artist.name, artist and artist["sort-name"] }
		for _, credit_name in credit_names do
			for _, name in names do
				if artist_names_agree(name, credit_name) then
					agrees = true
					return
				end
			end
		end
	end)
	return agrees
end

-- Whether a release crediting another artist may still be the local release:
-- only on an exact title, with the local track titles on it. For one or two
-- tracks, also only when the artist search found no artist named like the
-- local one (MusicBrainz credits them under another name); a same-titled cover
-- or original by an artist MusicBrainz knows is not the local release.
local function other_artist_is_plausible(
	local_release_info: LocalRelease,
	title_exact: boolean,
	title_overlap: number,
	local_count: number
): boolean
	if not title_exact then
		return false
	end
	if local_count <= 2 then
		return local_release_info.artist_missing_from_musicbrainz and title_overlap == local_count
	end
	return title_overlap >= math.max(2, math.ceil(local_count / 2))
end

-- Whether the release's track count fits the local tracks: equal, a partial
-- rip of it, one of its discs, or a subset it carries by title.
local function shape_fits(
	release: Release,
	local_tracks: { LocalTrack },
	title_agrees: boolean,
	title_exact: boolean,
	title_overlap: number,
	verified_recording: boolean,
	album_unnamed: boolean
): boolean
	local local_count = #local_tracks
	local release_count = compute_track_count(release)
	if local_count == 0 or release_count == local_count then
		return true
	end
	if release_count < local_count then
		return false
	end
	if max_declared_track_total(local_tracks) > local_count then
		return true
	end
	if local_count == 1 then
		-- A single cut from a larger release, like a single with its
		-- instrumental: the release must carry the local title exactly. A
		-- verified recording places a track whose album tag names nothing.
		return (title_exact and title_overlap == 1) or (album_unnamed and verified_recording)
	end
	if medium_has_track_count(release, local_count) then
		return true
	end
	return title_agrees and title_overlap >= math.ceil(local_count * SUBSET_TITLE_OVERLAP_RATIO)
end

-- Whether track lengths alone single out the release, as they must when its
-- titles are transliterated or translated: at least half the local tracks
-- compare, all within tolerance. Missing lengths are no evidence.
local function lengths_identify(release: Release, local_tracks: { LocalTrack }): boolean
	local compared, max_diff = max_assigned_duration_diff(local_tracks, release)
	return compared > 0 and compared >= math.ceil(#local_tracks / 2) and max_diff <= TRACK_DURATION_TOLERANCE_MS
end

local function has_verified_recording(release: Release, local_release_info: LocalRelease): boolean
	local recording_ids = local_release_info.verified_recording_ids
	return recording_ids ~= nil and release_has_any_recording(release, recording_ids)
end

-- Recordings of `release` that stand for local tracks.
function module.assigned_recording_ids(release: Release, local_tracks: { LocalTrack }): { [string]: boolean }
	local recording_ids: { [string]: boolean } = {}
	if #local_tracks == 0 then
		return recording_ids
	end
	local _, assignments = assign_release_tracks(local_tracks, release)
	for _, mb_track in assignments do
		if mb_track.recording_id then
			recording_ids[mb_track.recording_id] = true
		end
	end
	return recording_ids
end

-- A predicate for editions of the matched release's group: an edition must
-- hold a local track, by title or by a recording `anchor` assigns to one. A
-- release group can hold another song's release, such as a single split from a
-- double A-side. Without anchor recordings there is nothing to check.
function module.edition_filter(anchor: Release?, local_tracks: { LocalTrack }): (Release) -> boolean
	local recording_ids = if anchor then module.assigned_recording_ids(anchor, local_tracks) else {}
	return function(edition: Release): boolean
		if anchor == nil or edition.id == anchor.id or next(recording_ids) == nil then
			return true
		end
		return count_track_title_overlap(local_tracks, edition) > 0 or release_has_any_recording(edition, recording_ids)
	end
end

-- Whether `release` is plausibly the local release. `anchor` is the release a
-- search matched when `release` is another edition from its group. The reason
-- names the first rule that failed.
function module.accepts(release: Release, local_release_info: LocalRelease, anchor: Release?): (boolean, string?)
	if not release_lists_tracks(release) then
		return false, "no track list"
	end
	local local_tracks = local_release_info.context.tracks
	local local_count = #local_tracks

	local local_title = clean_release_title(local_release_info.release_title)
	local has_local_title = not is_blank_text(local_title)
	local title_agrees = has_local_title and titles_match_search(release.title, local_title)
	local title_exact = has_local_title and normalize_title(release.title) == normalize_title(local_title)
	local title_overlap = count_track_title_overlap(local_tracks, release)
	local verified_recording = has_verified_recording(release, local_release_info)
	-- The files' own album tag, before a hint from their paths replaces it.
	local album_tag = local_release_info.context.release_title
	local album_unnamed = is_blank_text(album_tag) or is_placeholder_text(album_tag)

	if
		not shape_fits(
			release,
			local_tracks,
			title_agrees,
			title_exact,
			title_overlap,
			verified_recording,
			album_unnamed
		)
	then
		return false, `track count {compute_track_count(release)} does not fit {local_count} local tracks`
	end

	if local_count == 1 then
		local diff = single_track_duration_diff(local_tracks, release)
		if diff ~= nil and diff > TRACK_DURATION_TOLERANCE_MS then
			return false, `duration differs by {diff} ms`
		end
	elseif local_count > 1 and not validate_release_durations(local_tracks, release) then
		return false, "durations disagree"
	end

	local artist_ok = artist_agrees(release, local_release_info)
	if not artist_ok and not other_artist_is_plausible(local_release_info, title_exact, title_overlap, local_count) then
		return false, "artist disagrees"
	end

	-- A title prefix ("Love" and "Love Song") only counts next to the artist.
	local identified = title_exact
		or title_overlap > 0
		or verified_recording
		or (artist_ok and (title_agrees or lengths_identify(release, local_tracks)))
	if not identified then
		return false, "no title, track, or recording agreement, and lengths don't identify it"
	end

	if not module.edition_filter(anchor, local_tracks)(release) then
		return false, "edition shares no local track with the matched release"
	end

	return true, nil
end

local function recording_ids_of(release: Release): { [string]: boolean }
	local recording_ids: { [string]: boolean } = {}
	for _, mb_track in flatten_release_tracks(release) do
		if mb_track.recording_id then
			recording_ids[mb_track.recording_id] = true
		end
	end
	return recording_ids
end

-- How many of `identity`'s recordings `edition` also has.
function module.shared_recording_count(edition: Release, identity: Release): number
	local edition_ids = recording_ids_of(edition)
	local shared = 0
	for recording_id in recording_ids_of(identity) do
		if edition_ids[recording_id] then
			shared += 1
		end
	end
	return shared
end

-- Whether `localized` keeps enough of `identity`'s recordings to stand in for it.
function module.localization_keeps_recordings(localized: Release, identity: Release): boolean
	local identity_count = 0
	for _ in recording_ids_of(identity) do
		identity_count += 1
	end
	return identity_count > 0
		and module.shared_recording_count(localized, identity)
			>= math.ceil(identity_count * LOCALIZATION_RECORDING_SHARE)
end

return module
