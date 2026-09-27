--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

-- Catalog lookups by scope and by relation, as the caller sees them.

local artists = require("@lyra/artists")
local entities = require("@lyra/entities")
local genres = require("@lyra/genres")
local releases = require("@lyra/releases")
local tracks = require("@lyra/tracks")

local item_ids = require("./identifiers")
local playlist_access = require("../playlist_access")

type Release = releases.Release
type Track = tracks.Track

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

-- The kind of entity `item_id` is, when the caller can see it.
function module.visible_kind(item_id: number): string?
	local kind = module.scope_entity_type(item_id)
	local visible: boolean
	if kind == "Release" then
		visible = releases.get({ item_id })[item_id] ~= nil
	elseif kind == "Track" then
		visible = tracks.get({ item_id })[item_id] ~= nil
	elseif kind == "Artist" then
		visible = artists.get({ item_id })[item_id] ~= nil
	elseif kind == "Genre" then
		visible = genres.get({ item_id })[item_id] ~= nil
	elseif kind == "Playlist" then
		visible = playlist_access.find(item_id) ~= nil
	else
		visible = kind ~= nil
	end
	return if visible then kind else nil
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

function module.releases_of_track(track_id: number?): { Release }
	if track_id == nil then
		return {}
	end
	return releases.by_track({ track_id })[track_id] or {}
end

return module
