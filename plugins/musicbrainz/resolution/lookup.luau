--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local metadata = require("@lyra/metadata")

local acoustid = require("./acoustid")
local apply = require("./apply")
local local_release = require("./local_release")
local musicbrainz_api = require("./musicbrainz_api")
local release_group = require("./release_group")
local release_search = require("./release_search")
local resolve = require("./resolve")
local text_helpers = require("./text_helpers")
local track_helpers = require("./track_helpers")

type ArtistIdSet = local_release.ArtistIdSet
type ReleaseIds = local_release.ReleaseIds
type LocalRelease = local_release.LocalRelease
type ReleaseContext = local_release.ReleaseContext
type ReleaseArtistCredit = track_helpers.ReleaseArtistCredit
type Release = track_helpers.Release
type ExternalIdsByProvider = metadata.ExternalIdsByProvider
type Provider = metadata.Provider
type ArtistRefreshContext = metadata.ArtistRefreshContext

local clean_release_title = text_helpers.clean_release_title
local release_lists_tracks = track_helpers.release_lists_tracks
local browse_release_group = musicbrainz_api.browse_release_group
local fetch_release_details = musicbrainz_api.fetch_release_details
local release_from_candidates = release_group.release_from_candidates
local select_release_group_candidate = release_group.select_release_group_candidate

export type CoverCacheContext = {
	release_id: string?,
	release_group_id: string?,
	track_count: number?,
	preferred_language: string?,
	preferred_country: string?,
	search_title: string?,
}

local module = {}

local function musicbrainz_external_ids(external_ids: ExternalIdsByProvider?): ReleaseIds?
	return if external_ids then external_ids.musicbrainz :: ReleaseIds? else nil
end

local function musicbrainz_artist_id(external_ids: ExternalIdsByProvider?): string?
	local ids = external_ids and external_ids.musicbrainz
	if ids == nil then
		return nil
	end
	local artist_id = ids.artist_id
	return if type(artist_id) == "string" and artist_id ~= "" then artist_id else nil
end

local function cover_cache_context_for(local_release_info: LocalRelease): CoverCacheContext
	local release_context = local_release_info.context
	local ids = release_context.ids or musicbrainz_external_ids(release_context.external_ids)
	local search_title = clean_release_title(local_release_info.release_title or "")
	if search_title == "" and #release_context.tracks > 0 then
		local first_track = release_context.tracks[1]
		search_title = if first_track then first_track.track_title or "" else ""
	end

	return {
		release_id = if ids and type(ids.release_id) == "string" and ids.release_id ~= "" then ids.release_id else nil,
		release_group_id = if ids
				and type(ids.release_group_id) == "string"
				and ids.release_group_id ~= ""
			then ids.release_group_id
			else nil,
		track_count = if local_release_info.track_count > 0 then local_release_info.track_count else nil,
		preferred_language = local_release_info.preferred_language,
		preferred_country = local_release_info.preferred_country,
		search_title = if search_title ~= "" then search_title else nil,
	}
end

function module.cover_cache_context(release_context: ReleaseContext): CoverCacheContext
	return cover_cache_context_for(local_release.from_context(release_context))
end

local function release_artist_ids(release: Release?): ArtistIdSet?
	local artist_ids: ArtistIdSet? = nil
	local artist_credits = if release then release["artist-credit"] else nil
	if artist_credits == nil then
		return nil
	end
	for _, credit: ReleaseArtistCredit in artist_credits do
		local artist = credit.artist
		if artist and artist.id then
			local resolved_artist_ids: ArtistIdSet = if artist_ids ~= nil then artist_ids else {}
			artist_ids = resolved_artist_ids
			resolved_artist_ids[artist.id] = true
		end
	end
	return artist_ids
end

local function release_has_front_cover(release: Release?): boolean
	local cover_archive = release and release["cover-art-archive"]
	return release ~= nil and release.id ~= nil and cover_archive ~= nil and cover_archive.front == true
end

function module.resolve_cover_release_id(release_context: ReleaseContext): string?
	local ids = if release_context
		then release_context.ids or musicbrainz_external_ids(release_context.external_ids)
		else nil
	if not ids then
		return nil
	end

	local local_release_info = local_release.from_context(release_context)
	local cache_context = cover_cache_context_for(local_release_info)
	local track_count = cache_context.track_count
	local release_group_id = cache_context.release_group_id
	local selected_release: Release? = nil
	local candidates: { Release }? = nil
	local release_id = cache_context.release_id
	if type(release_group_id) == "string" and release_group_id ~= "" then
		candidates = browse_release_group(release_group_id, track_count == 1, track_count)
		if candidates and #candidates > 0 and type(release_id) == "string" and release_id ~= "" then
			selected_release = release_from_candidates(candidates, release_id, nil)
			if selected_release ~= nil and release_has_front_cover(selected_release) then
				local selected_release_id = selected_release.id
				if selected_release_id ~= nil then
					return selected_release_id
				end
			end
		end
	elseif type(release_id) == "string" and release_id ~= "" then
		selected_release = fetch_release_details(release_id)
		if selected_release ~= nil and release_has_front_cover(selected_release) then
			local selected_release_id = selected_release.id
			if selected_release_id ~= nil then
				return selected_release_id
			end
		end
		local release_group = selected_release and selected_release["release-group"]
		release_group_id = release_group and release_group.id
		if type(release_group_id) ~= "string" or release_group_id == "" then
			return nil
		end
		candidates = browse_release_group(release_group_id, track_count == 1, track_count)
		if candidates and #candidates > 0 and selected_release then
			selected_release = release_from_candidates(candidates, selected_release.id, selected_release)
		end
	else
		return nil
	end

	if not candidates or #candidates == 0 then
		return nil
	end

	local search_title = cache_context.search_title or ""
	if search_title == "" and selected_release and selected_release.title then
		search_title = selected_release.title
	end
	local artist_ids = release_artist_ids(selected_release)

	local cover_candidates: { Release } = {}
	for _, candidate: Release in candidates do
		if release_has_front_cover(candidate) then
			table.insert(cover_candidates, candidate)
		end
	end
	if #cover_candidates == 0 then
		return nil
	end

	local best_cover_release, identity_release = select_release_group_candidate(
		cover_candidates,
		local_release_info,
		search_title,
		artist_ids,
		selected_release,
		false
	)
	-- A localized edition browsed without its track list is unverified; use the
	-- edition it would stand in for.
	if best_cover_release and not release_lists_tracks(best_cover_release) then
		best_cover_release = identity_release
	end
	return if best_cover_release then best_cover_release.id else nil
end

-- Matches the local release through the AcoustID fingerprint of its longest
-- track, recording the AcoustID track ID when a release is applied.
local function try_acoustid(local_release_info: LocalRelease): boolean
	local tracks = local_release_info.context.tracks
	local track = acoustid.select_track(tracks)
	local track_db_id = track and track.db_id
	if not track or not track_db_id or not track.duration_ms then
		return false
	end
	local fingerprint = acoustid.compute_fingerprint(track_db_id)
	if not fingerprint then
		return false
	end
	local result = acoustid.lookup(fingerprint, track.duration_ms)
	local recording_id = result and result.recording_ids[1]
	if not result or not recording_id then
		return false
	end
	if not acoustid.resolve_recording_to_release(recording_id, local_release_info) then
		return false
	end
	local acoustid_track_id = result.track_id
	if acoustid_track_id then
		apply.set_track_id(track_db_id, "acoustid_track_id", acoustid_track_id)
	end
	return true
end

function module.register_refresh(provider_instance: Provider): ()
	apply.set_provider(provider_instance)
	provider_instance:refresh(metadata.EntityType.Artist, function(ctx: ArtistRefreshContext)
		local artist_db_id = ctx.db_id
		if artist_db_id == nil then
			return nil
		end

		local artist_id = musicbrainz_artist_id(ctx.external_ids)
		if artist_id == nil then
			return nil
		end

		apply.refresh_artist_by_id(artist_db_id, artist_id)
		return nil
	end, function(ctx: ArtistRefreshContext): boolean
		return musicbrainz_artist_id(ctx.external_ids) ~= nil
	end)

	provider_instance:refresh(metadata.EntityType.Release, function(ctx: metadata.ReleaseRefreshContext)
		local release: ReleaseContext = ctx :: ReleaseContext
		local local_release_info = local_release.from_context(release)
		local musicbrainz_ids = musicbrainz_external_ids(release.external_ids)
		if musicbrainz_ids and musicbrainz_ids.release_id then
			if resolve.try_release_id(musicbrainz_ids.release_id, local_release_info) then
				return nil
			end
		end

		local searchable = local_release_info.artist_name ~= nil or local_release_info.compilation_release
		if searchable and release_search.search_and_fetch_release(local_release_info) then
			return nil
		end
		if ctx.options and ctx.options.acoustid_fingerprint == true and try_acoustid(local_release_info) then
			return nil
		end
		local release_db_id = release.db_id
		if release_db_id ~= nil then
			apply.mark_unmatched(release_db_id, { "release_id", "release_group_id" })
		end
		return nil
	end, function(ctx: metadata.ReleaseRefreshContext): boolean
		local release: ReleaseContext = ctx :: ReleaseContext
		local musicbrainz_ids = musicbrainz_external_ids(release.external_ids)
		if musicbrainz_ids and musicbrainz_ids.release_id then
			return false
		end
		if not release.tracks or #release.tracks == 0 then
			return false
		end
		return true
	end)
end

return module
