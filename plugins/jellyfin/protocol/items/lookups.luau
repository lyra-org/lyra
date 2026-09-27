--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

-- Catalog lookups by scope and by relation, as the caller sees them.

local entities = require("@lyra/entities")
local releases = require("@lyra/releases")
local tracks = require("@lyra/tracks")

local item_ids = require("./identifiers")

type CreditRole = releases.CreditRole
type Release = releases.Release
type ReleasePage = releases.ReleasePage
type ReleaseQuery = releases.ReleaseQuery
type Track = tracks.Track
type TrackQuery = tracks.TrackQuery

-- An item a listing is scoped to, and what kind of entity it is.
export type Scope = { db_id: number?, entity_type: string? }

local module = {}

function module.scope_entity_type(scope: number?): string?
	if scope == nil or scope <= 0 then
		return nil
	end
	local ok, element_type = pcall(entities.get_type, scope)
	return if ok and type(element_type) == "string" then element_type else nil
end

-- The scope a Jellyfin item id names; empty for no id or an unknown one.
function module.scope(item_id: string?): Scope
	local db_id = if item_id then item_ids.decode_jellyfin_item_id(item_id) else nil
	return { db_id = db_id, entity_type = module.scope_entity_type(db_id) }
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

function module.releases_credited_to(artist_id: number, role: CreditRole?, limit: number?): ReleasePage
	local query: ReleaseQuery = {}
	query.artist_ids = { artist_id }
	query.credit_role = role
	query.limit = limit
	return releases.query(query)
end

function module.releases_of_track(track_id: number?): { Release }
	if track_id == nil then
		return {}
	end
	return releases.by_track({ track_id })[track_id] or {}
end

return module
