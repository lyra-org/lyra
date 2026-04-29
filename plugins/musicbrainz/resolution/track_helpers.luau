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

export type LocalArtist = {
	db_id: number?,
	artist_name: string?,
}

export type LocalTrack = {
	db_id: number?,
	track_title: string,
	artists: { LocalArtist }?,
	disc: number?,
	disc_total: number?,
	track: number?,
	track_total: number?,
	duration_ms: number?,
	external_ids: metadata.ExternalIdsByProvider?,
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

export type RecordingRelation = {
	type: string?,
	["target-type"]: string?,
	work: { id: string? }?,
	artist: { id: string?, name: string?, ["sort-name"]: string? }?,
	attributes: { string }?,
}

export type ReleaseRecording = {
	id: string?,
	relations: { RecordingRelation }?,
}

export type ReleaseGenre = {
	id: string?,
	name: string?,
}

export type ReleaseMediumTrack = {
	title: string,
	position: number?,
	recording: ReleaseRecording?,
	length: number?,
	["artist-credit"]: { ReleaseArtistCredit }?,
}

export type ReleaseMedium = {
	position: number?,
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
	recording_id: string?,
	recording_relations: { RecordingRelation }?,
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
	local musicbrainz_ids = metadata.ids.for_provider(local_track.external_ids, "musicbrainz")
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
		return (a.db_id or math.huge) < (b.db_id or math.huge)
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

	local function pass_title_exact()
		pass_match(function(local_track, candidate)
			local title = normalize_title(local_track.track_title)
			return title ~= "" and normalize_title(candidate.title) == title
		end)
	end

	if #local_tracks == 1 then
		pass_title_exact()
		pass_disc_track()
	else
		pass_disc_track()
		pass_title_exact()
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

function module.validate_release_durations(local_tracks: { LocalTrack }, release: Release): boolean
	local mb_tracks = module.flatten_release_tracks(release)
	if #mb_tracks == 0 then
		return true
	end
	local sorted_local: { LocalTrack } = {}
	for _, track in local_tracks do
		table.insert(sorted_local, track)
	end
	module.sort_tracks_for_refresh(sorted_local)
	local assignments = module.assign_mb_tracks_to_local_tracks(sorted_local, mb_tracks)
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
	local mb_tracks = module.flatten_release_tracks(release)
	if #mb_tracks == 0 then
		return nil
	end
	local single_track_list: { LocalTrack } = { local_track }
	local assignments = module.assign_mb_tracks_to_local_tracks(single_track_list, mb_tracks)
	local matched = assignments[1]
	if not matched or matched.duration_ms == nil then
		return nil
	end
	return math.abs(matched.duration_ms - local_track.duration_ms)
end

return module
