--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

-- Catalog lookups by scope and by relation, as the caller sees them.

local artists_module = require("@lyra/artists")
local entities = require("@lyra/entities")
local releases = require("@lyra/releases")
local tracks = require("@lyra/tracks")

type Artist = artists_module.Artist
type ArtistQuery = artists_module.ArtistQuery
type CreditRole = releases.CreditRole
type Release = releases.Release
type ReleasePage = releases.ReleasePage
type ReleaseQuery = releases.ReleaseQuery
type Track = tracks.Track
type TrackQuery = tracks.TrackQuery

local module = {}

function module.scope_entity_type(scope: number?): string?
	if scope == nil or scope <= 0 then
		return nil
	end
	local ok, element_type = pcall(entities.get_type, scope)
	return if ok and type(element_type) == "string" then element_type else nil
end

function module.tracks_of_release(release_id: number?): { Track }
	if release_id == nil then
		return {}
	end
	return tracks.by_release({ release_id })[release_id] or {}
end

function module.tracks_of_artist(artist_id: number?): { Track }
	if artist_id == nil then
		return {}
	end
	local query: TrackQuery = {}
	query.artist_ids = { artist_id }
	return tracks.query(query).items
end

function module.tracks_of_artists(artist_ids: { number }): { [number]: { Track } }
	local tracks_by_artist: { [number]: { Track } } = {}
	for _, artist_id in artist_ids do
		tracks_by_artist[artist_id] = module.tracks_of_artist(artist_id)
	end
	return tracks_by_artist
end

function module.releases_credited_to(
	artist_id: number,
	role: CreditRole?,
	excluded: CreditRole?,
	limit: number?
): ReleasePage
	local query: ReleaseQuery = {}
	query.artist_ids = { artist_id }
	query.credit_role = role
	query.exclude_credit_role = excluded
	query.limit = limit
	return releases.query(query)
end

-- Releases with the artist's tracks that don't credit the artist themselves.
function module.appearances_of_artist(artist_id: number): { Release }
	return module.releases_credited_to(artist_id, "track", "release").items
end

function module.releases_of_track(track_id: number?): { Release }
	if track_id == nil then
		return {}
	end
	return releases.by_track({ track_id })[track_id] or {}
end

function module.tracks_in_scope(scope: number?): { Track }
	local scope_type = module.scope_entity_type(scope)
	local query: TrackQuery = {}
	if scope_type == "Release" then
		return module.tracks_of_release(scope)
	elseif scope_type == "Artist" then
		return module.tracks_of_artist(scope)
	elseif scope_type == "Library" then
		query.library_id = scope
	elseif scope ~= nil then
		return {}
	end
	return tracks.query(query).items
end

function module.releases_in_scope(scope: number?): { Release }
	local scope_type = module.scope_entity_type(scope)
	local query: ReleaseQuery = {}
	if scope_type == "Genre" and scope ~= nil then
		return releases.by_genre({ scope })[scope] or {}
	elseif scope_type == "Track" then
		return module.releases_of_track(scope)
	elseif scope_type == "Artist" and scope ~= nil then
		return module.releases_credited_to(scope).items
	elseif scope_type == "Library" then
		query.library_id = scope
	elseif scope ~= nil then
		return {}
	end
	return releases.query(query).items
end

function module.artists_in_scope(scope: number?): { Artist }
	local scope_type = module.scope_entity_type(scope)
	local query: ArtistQuery = {}
	if scope_type == "Release" and scope ~= nil then
		return artists_module.by_release({ scope })[scope] or {}
	elseif scope_type == "Track" and scope ~= nil then
		return artists_module.by_track({ scope })[scope] or {}
	elseif scope_type == "Library" then
		query.library_id = scope
	elseif scope ~= nil then
		return {}
	end
	return artists_module.query(query).items
end

return module
