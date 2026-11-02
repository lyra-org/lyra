--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local metadata = require("@lyra/metadata")
local text_helpers = require("./text_helpers")

local normalize_title = text_helpers.normalize_title
local normalize_overlap_title = text_helpers.normalize_overlap_title

local module = {}

-- A scanned length under a second is too short to be a song: a truncated or
-- misread length rather than the track's own, it counts as no length at all.
local MIN_PLAUSIBLE_LENGTH_MS = 1000

export type LocalArtist = {
	id: string?,
	artist_name: string?,
	created_at_ms: number?,
}

export type LocalTrack = {
	id: string?,
	track_title: string,
	artists: { LocalArtist }?,
	disc: number?,
	disc_total: number?,
	track: number?,
	track_total: number?,
	duration_ms: number?,
	external_ids: metadata.ExternalIdsByProvider?,
	created_at_ms: number?,
}

export type ReleaseArtist = {
	id: string?,
	name: string?,
	["sort-name"]: string?,
	country: string?,
	score: number?,
	["type"]: string?,
}

export type ReleaseArtistCredit = {
	name: string?,
	joinphrase: string?,
	artist: ReleaseArtist?,
}

export type ReleaseGroup = {
	id: string?,
	["first-release-date"]: string?,
	["primary-type"]: string?,
}

export type ReleaseTextRepresentation = {
	language: string?,
}

export type ReleaseCoverArchive = {
	front: boolean?,
}

-- A MusicBrainz relationship, from any entity's `relations`.
export type Relation = {
	type: string?,
	["target-type"]: string?,
	direction: string?,
	-- With work-level-rels, the work's own relationships, such as its composer.
	work: { id: string?, relations: { Relation }? }?,
	artist: { id: string?, name: string?, ["sort-name"]: string?, ["type"]: string? }?,
	url: { resource: string? }?,
	attributes: { string }?,
}

export type ReleaseRecording = {
	id: string?,
	video: boolean?,
	relations: { Relation }?,
}

export type ReleaseGenre = {
	id: string?,
	name: string?,
}

export type ReleaseMediumTrack = {
	id: string?,
	title: string,
	position: number?,
	recording: ReleaseRecording?,
	length: number?,
	["artist-credit"]: { ReleaseArtistCredit }?,
}

export type EditionHints = {
	media_formats: { string }?,
	barcode: string?,
}

export type ReleaseMedium = {
	position: number?,
	format: string?,
	["track-count"]: number?,
	tracks: { ReleaseMediumTrack }?,
}

export type Release = {
	id: string?,
	title: string?,
	status: string?,
	country: string?,
	date: string?,
	barcode: string?,
	packaging: string?,
	score: number?,
	media: { ReleaseMedium }?,
	genres: { ReleaseGenre }?,
	["track-count"]: number?,
	["artist-credit"]: { ReleaseArtistCredit }?,
	["release-group"]: ReleaseGroup?,
	["text-representation"]: ReleaseTextRepresentation?,
	["cover-art-archive"]: ReleaseCoverArchive?,
}

type FlattenedMbTrack = {
	title: string,
	disc: number?,
	track: number?,
	release_track_id: string?,
	recording_id: string?,
	recording_relations: { Relation }?,
	duration_ms: number?,
	artist_credits: { ReleaseArtistCredit }?,
}

type DiscTitleInfo = {
	count: number,
	titles: { [string]: boolean },
}

type DiscRemap = {
	local_disc: number,
	mb_disc: number,
}

type TrackAssignments = {
	[number]: FlattenedMbTrack,
}

local function local_recording_id(local_track: LocalTrack): string?
	local musicbrainz_ids = local_track.external_ids and local_track.external_ids.musicbrainz
	if musicbrainz_ids == nil then
		return nil
	end
	return musicbrainz_ids.recording_id
end

function module.max_declared_track_total(tracks: { LocalTrack }?): number
	local max_total = 0
	if tracks == nil then
		return max_total
	end
	for _, track in tracks do
		if type(track.track_total) == "number" and track.track_total > max_total then
			max_total = track.track_total
		end
	end
	return max_total
end

function module.artist_name_from_tracks(tracks: { LocalTrack }?): string?
	if tracks == nil then
		return nil
	end
	for _, track in tracks do
		local artist_list = track.artists
		if artist_list == nil then
			continue
		end
		for _, artist in artist_list do
			local name = artist.artist_name
			if name ~= nil and name ~= "" then
				return name
			end
		end
	end
	return nil
end

function module.is_compilation(tracks: { LocalTrack }?): boolean
	if not tracks or #tracks == 0 then
		return false
	end
	local seen: { [string]: boolean } = {}
	local unique = 0
	for _, track in tracks do
		if track.artists then
			for _, artist in track.artists do
				local name = artist.artist_name
				if name ~= nil and name ~= "" then
					local key = string.lower(name)
					if not seen[key] then
						seen[key] = true
						unique += 1
					end
				end
			end
		end
	end
	return unique > #tracks / 2
end

-- Whether entity `a` orders before `b` when nothing else tells them apart:
-- the one created first, then by id. One without a creation time or id goes
-- after one with it.
function module.created_before(
	a_created_at_ms: number?,
	a_id: string?,
	b_created_at_ms: number?,
	b_id: string?
): boolean
	if a_created_at_ms ~= b_created_at_ms then
		if a_created_at_ms == nil or b_created_at_ms == nil then
			return a_created_at_ms ~= nil
		end
		return a_created_at_ms < b_created_at_ms
	end
	if a_id == nil or b_id == nil then
		return a_id ~= nil
	end
	return a_id < b_id
end

function module.sort_tracks_for_refresh(track_list: { LocalTrack })
	table.sort(track_list, function(a: LocalTrack, b: LocalTrack): boolean
		local a_disc = a.disc or 1
		local b_disc = b.disc or 1
		if a_disc ~= b_disc then
			return a_disc < b_disc
		end
		local a_track = a.track or math.huge
		local b_track = b.track or math.huge
		if a_track ~= b_track then
			return a_track < b_track
		end
		local a_title = normalize_title(a.track_title)
		local b_title = normalize_title(b.track_title)
		if a_title ~= b_title then
			return a_title < b_title
		end
		return module.created_before(a.created_at_ms, a.id, b.created_at_ms, b.id)
	end)
end

function module.flatten_release_tracks(release: Release): { FlattenedMbTrack }
	local mb_tracks: { FlattenedMbTrack } = {}
	local media_list = release.media
	if media_list == nil then
		return mb_tracks
	end
	for _, medium in media_list do
		local medium_tracks = medium.tracks
		if medium_tracks == nil then
			continue
		end
		for _, track in medium_tracks do
			local recording = track.recording
			table.insert(mb_tracks, {
				title = track.title,
				disc = medium.position,
				track = track.position,
				release_track_id = track.id,
				recording_id = recording and recording.id,
				recording_relations = recording and recording.relations,
				duration_ms = track.length,
				artist_credits = track["artist-credit"],
			})
		end
	end
	return mb_tracks
end

local function find_unique_unmatched_mb_track(
	mb_tracks: { FlattenedMbTrack },
	used_mb_indices: { [number]: boolean },
	predicate: (FlattenedMbTrack) -> boolean
): (number?, FlattenedMbTrack?)
	local match_index: number? = nil
	local match_track: FlattenedMbTrack? = nil
	for index, mb_track in mb_tracks do
		if not used_mb_indices[index] and predicate(mb_track) then
			if match_index ~= nil then
				return nil, nil
			end
			match_index = index
			match_track = mb_track
		end
	end
	return match_index, match_track
end

local function infer_single_disc_remap(local_tracks: { LocalTrack }?, mb_tracks: { FlattenedMbTrack }): DiscRemap?
	if not local_tracks or #local_tracks < 2 then
		return nil
	end
	local local_disc = local_tracks[1] and local_tracks[1].disc
	if local_disc == nil then
		return nil
	end
	for _, local_track in local_tracks do
		if local_track.disc ~= local_disc then
			return nil
		end
	end

	local local_count = #local_tracks
	local medium_info_by_disc: { [number]: DiscTitleInfo } = {}
	for _, mb_track in mb_tracks do
		local disc = mb_track.disc
		if disc ~= nil then
			local info = medium_info_by_disc[disc]
			if info == nil then
				info = { count = 0, titles = {} }
				medium_info_by_disc[disc] = info
			end
			info.count += 1
			local norm_title = normalize_overlap_title(mb_track.title)
			if norm_title ~= "" then
				info.titles[norm_title] = true
			end
		end
	end

	local best_disc: number? = nil
	local best_overlap = -1
	local best_disc_count = 0
	for disc, info in medium_info_by_disc do
		if info.count == local_count then
			local overlap = 0
			for _, local_track in local_tracks do
				local norm_local_title = normalize_overlap_title(local_track.track_title)
				if norm_local_title ~= "" and info.titles[norm_local_title] then
					overlap += 1
				end
			end
			if overlap > best_overlap then
				best_overlap = overlap
				best_disc = disc
				best_disc_count = 1
			elseif overlap == best_overlap then
				best_disc_count += 1
			end
		end
	end

	if best_disc == nil or best_disc == local_disc then
		return nil
	end
	if best_overlap <= 0 then
		return nil
	end
	local min_overlap = math.max(2, math.ceil(local_count * 0.20))
	if best_overlap < min_overlap then
		return nil
	end
	if best_disc_count > 1 then
		return nil
	end
	return {
		local_disc = local_disc,
		mb_disc = best_disc,
	}
end

-- Whether local disc and track numbers can place tracks: not when two local
-- tracks share a position, as discs ripped to folders without disc numbers do,
-- or when the release has several discs and a local track names none.
local function positions_reliable(local_tracks: { LocalTrack }, mb_tracks: { FlattenedMbTrack }): boolean
	local seen: { [string]: boolean } = {}
	for _, local_track in local_tracks do
		if local_track.track ~= nil then
			local key = `{local_track.disc or 0}:{local_track.track}`
			if seen[key] then
				return false
			end
			seen[key] = true
		end
	end
	local first_disc = mb_tracks[1] and mb_tracks[1].disc
	for _, mb_track in mb_tracks do
		if mb_track.disc ~= first_disc then
			for _, local_track in local_tracks do
				if local_track.disc == nil then
					return false
				end
			end
			break
		end
	end
	return true
end

function module.assign_mb_tracks_to_local_tracks(
	local_tracks: { LocalTrack },
	mb_tracks: { FlattenedMbTrack }
): TrackAssignments
	local assignments: TrackAssignments = {}
	local used_mb_indices: { [number]: boolean } = {}
	local disc_remap = infer_single_disc_remap(local_tracks, mb_tracks)
	local function assign(local_index: number, mb_index: number, mb_track: FlattenedMbTrack)
		assignments[local_index] = mb_track
		used_mb_indices[mb_index] = true
	end

	for local_index, local_track in local_tracks do
		local recording_id = local_recording_id(local_track)
		if recording_id then
			local mb_index, mb_track = find_unique_unmatched_mb_track(mb_tracks, used_mb_indices, function(candidate)
				return candidate.recording_id == recording_id
			end)
			if mb_index ~= nil and mb_track ~= nil then
				assign(local_index, mb_index, mb_track)
			end
		end
	end

	local function pass_match(find_match: (LocalTrack, FlattenedMbTrack) -> boolean)
		for local_index, local_track in local_tracks do
			if assignments[local_index] == nil then
				local mb_index, mb_track = find_unique_unmatched_mb_track(
					mb_tracks,
					used_mb_indices,
					function(candidate)
						return find_match(local_track, candidate)
					end
				)
				if mb_index ~= nil and mb_track ~= nil then
					assign(local_index, mb_index, mb_track)
				end
			end
		end
	end

	local function pass_disc_track()
		pass_match(function(local_track, candidate)
			if local_track.disc == nil or local_track.track == nil then
				return false
			end
			local expected_disc = local_track.disc
			if disc_remap and local_track.disc == disc_remap.local_disc then
				expected_disc = disc_remap.mb_disc
			end
			return candidate.disc == expected_disc and candidate.track == local_track.track
		end)
	end

	-- A title places a track; among tracks of the same title, its track
	-- number when positions can be trusted, then the length closest to the
	-- file's. Files with a length to go by are placed first, so one without
	-- can't take the track another's length singles out.
	local function pass_title_by_length(trust_numbers: boolean)
		local function place(with_length: boolean)
			for local_index, local_track in local_tracks do
				local title = normalize_title(local_track.track_title)
				local has_length = local_track.duration_ms ~= nil and local_track.duration_ms >= MIN_PLAUSIBLE_LENGTH_MS
				if assignments[local_index] ~= nil or title == "" or has_length ~= with_length then
					continue
				end
				local best_index: number? = nil
				local best_diff = math.huge
				local best_same_number = false
				for index, candidate in mb_tracks do
					if used_mb_indices[index] or normalize_title(candidate.title) ~= title then
						continue
					end
					local local_ms, mb_ms = module.comparable_lengths(local_track, candidate)
					local diff = if local_ms and mb_ms then math.abs(local_ms - mb_ms) else math.huge
					local same_number = trust_numbers and candidate.track == local_track.track
					if
						best_index == nil
						or (same_number and not best_same_number)
						or (same_number == best_same_number and diff < best_diff)
					then
						best_index = index
						best_diff = diff
						best_same_number = same_number
					end
				end
				if best_index ~= nil then
					assign(local_index, best_index, mb_tracks[best_index])
				end
			end
		end
		place(true)
		place(false)
	end

	if not positions_reliable(local_tracks, mb_tracks) then
		pass_title_by_length(false)
	elseif #local_tracks == 1 then
		pass_title_by_length(true)
		pass_disc_track()
	else
		pass_disc_track()
		pass_title_by_length(true)
	end

	local next_mb = 1
	for local_index, _ in local_tracks do
		if assignments[local_index] == nil then
			while mb_tracks[next_mb] and used_mb_indices[next_mb] do
				next_mb += 1
			end
			local next_mb_track = mb_tracks[next_mb]
			if next_mb_track then
				assign(local_index, next_mb, next_mb_track)
				next_mb += 1
			end
		end
	end
	return assignments
end

function module.count_track_title_overlap(local_tracks: { LocalTrack }?, release: Release): number
	if not local_tracks or #local_tracks == 0 then
		return 0
	end
	local mb_titles: { [string]: boolean } = {}
	local media_list = release.media
	if media_list == nil then
		return 0
	end
	for _, medium in media_list do
		local medium_tracks = medium.tracks
		if medium_tracks == nil then
			continue
		end
		for _, track in medium_tracks do
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
	return matches
end

-- How many of the release's tracks are video recordings, such as music videos.
function module.video_recording_count(release: Release): number
	local count = 0
	for _, medium in release.media or {} do
		for _, track in medium.tracks or {} do
			if track.recording and track.recording.video == true then
				count += 1
			end
		end
	end
	return count
end

function module.release_lists_tracks(release: Release): boolean
	local media_list = release.media
	if media_list == nil then
		return false
	end
	for _, medium in media_list do
		local medium_tracks = medium.tracks
		if medium_tracks ~= nil and #medium_tracks > 0 then
			return true
		end
	end
	return false
end

function module.release_has_any_recording(release: Release, recording_ids: { [string]: boolean }): boolean
	local media_list = release.media
	if media_list == nil then
		return false
	end
	for _, medium in media_list do
		local medium_tracks = medium.tracks
		if medium_tracks == nil then
			continue
		end
		for _, track in medium_tracks do
			local recording = track.recording
			if recording and recording.id and recording_ids[recording.id] then
				return true
			end
		end
	end
	return false
end

function module.release_has_track_title(release: Release, title: string?): boolean
	local norm_title = normalize_overlap_title(title)
	if norm_title == "" then
		return false
	end
	local media_list = release.media
	if media_list == nil then
		return false
	end
	for _, medium in media_list do
		local medium_tracks = medium.tracks
		if medium_tracks == nil then
			continue
		end
		for _, track in medium_tracks do
			if normalize_overlap_title(track.title) == norm_title then
				return true
			end
		end
	end
	return false
end

-- The local tracks in refresh order, and the release track assigned to each.
function module.assign_release_tracks(
	local_tracks: { LocalTrack },
	release: Release
): ({ LocalTrack }, TrackAssignments)
	local sorted_local: { LocalTrack } = table.clone(local_tracks)
	module.sort_tracks_for_refresh(sorted_local)
	return sorted_local, module.assign_mb_tracks_to_local_tracks(sorted_local, module.flatten_release_tracks(release))
end

-- The local and MusicBrainz lengths of a paired track, if both are known and
-- the local one can be the track's real length.
function module.comparable_lengths(local_track: LocalTrack, mb_track: FlattenedMbTrack): (number?, number?)
	local local_ms = local_track.duration_ms
	local mb_ms = mb_track.duration_ms
	if local_ms == nil or mb_ms == nil or local_ms < MIN_PLAUSIBLE_LENGTH_MS then
		return nil, nil
	end
	return local_ms, mb_ms
end

function module.validate_release_durations(local_tracks: { LocalTrack }, release: Release): boolean
	if not module.release_lists_tracks(release) then
		return true
	end
	local sorted_local, assignments = module.assign_release_tracks(local_tracks, release)
	local compared = 0
	local failed = 0
	for i, local_track in sorted_local do
		local mb_track = assignments[i]
		if mb_track and mb_track.duration_ms and local_track.duration_ms then
			compared += 1
			local diff = math.abs(mb_track.duration_ms - local_track.duration_ms)
			local max_dur = math.max(mb_track.duration_ms, local_track.duration_ms)
			local relative = if max_dur > 0 then diff / max_dur else 0
			if relative > 0.30 and diff > 15000 then
				failed += 1
			end
		end
	end
	if compared < 2 then
		return true
	end
	return failed <= compared / 2
end

-- How many local tracks have a length to compare with their assigned
-- MusicBrainz track, and the largest difference among them.
function module.max_assigned_duration_diff(local_tracks: { LocalTrack }, release: Release): (number, number)
	local sorted_local, assignments = module.assign_release_tracks(local_tracks, release)
	local compared = 0
	local max_diff = 0
	for i, local_track in sorted_local do
		local mb_track = assignments[i]
		if mb_track and mb_track.duration_ms and local_track.duration_ms then
			compared += 1
			max_diff = math.max(max_diff, math.abs(mb_track.duration_ms - local_track.duration_ms))
		end
	end
	return compared, max_diff
end

function module.should_preserve_local_track_positions(local_tracks: { LocalTrack }?, release: Release?): boolean
	if not local_tracks or #local_tracks == 0 or not release then
		return false
	end
	local local_max_disc = 1
	local local_declared_disc_total = 1
	local has_local_disc_hint = false
	for _, local_track in local_tracks do
		if local_track.disc ~= nil and local_track.disc > 0 then
			has_local_disc_hint = true
			if local_track.disc > local_max_disc then
				local_max_disc = local_track.disc
			end
		end
		if local_track.disc_total ~= nil and local_track.disc_total > local_declared_disc_total then
			has_local_disc_hint = true
			local_declared_disc_total = local_track.disc_total
		end
	end
	if not has_local_disc_hint then
		return false
	end
	local release_media_count = 0
	local media_list = release.media
	if media_list == nil then
		return false
	end
	for _ in media_list do
		release_media_count += 1
	end
	return release_media_count == 1 and (local_max_disc > 1 or local_declared_disc_total > 1)
end

function module.single_track_duration_diff(local_tracks: { LocalTrack }?, release: Release?): number?
	if not local_tracks or #local_tracks ~= 1 or not release then
		return nil
	end
	local local_track = local_tracks[1]
	if local_track.duration_ms == nil then
		return nil
	end
	local _, assignments = module.assign_release_tracks({ local_track }, release)
	local matched = assignments[1]
	if not matched or matched.duration_ms == nil then
		return nil
	end
	return math.abs(matched.duration_ms - local_track.duration_ms)
end

function module.compute_track_count(release: Release): number
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

function module.medium_has_track_count(release: Release, expected_count: number): boolean
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

return module
