--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

-- Catalog lookups by scope and by relation, as the caller sees them.

local entities = require("@lyra/entities")
local releases = require("@lyra/releases")
local tracks = require("@lyra/tracks")

local constants = require("../../constants")
local item_ids = require("./identifiers")
local playlist_access = require("../playlist_access")

type Release = releases.Release
type Track = tracks.Track

-- An item a listing is scoped to, and what kind of entity it is.
export type Scope = { id: string?, entity_type: string? }

local module = {}

function module.scope_entity_type(scope: string?): string?
	if scope == nil then
		return nil
	end
	local ok, element_type = pcall(entities.get_type, scope)
	return if ok and type(element_type) == "string" then element_type else nil
end

-- The kind of entity `item_id` is, when the caller can see it. A playlist is visible as upstream
-- shows it, to its owner or as a public one, whatever the caller's role.
function module.visible_kind(item_id: string, user_id: string?): string?
	local kind = module.scope_entity_type(item_id)
	if kind == "Playlist" and not playlist_access.can_view(playlist_access.find(item_id), user_id) then
		return nil
	end
	return kind
end

-- The scope a Jellyfin item id names: empty for no id or a synthetic folder of the plugin's own
-- (the root and the playlists view, which name no entity), and without an entity type for an id
-- naming nothing the caller can see, which queries scoped to it treat as matching nothing.
function module.scope(item_id: string?): Scope
	if constants.is_root_item_id(item_id) or constants.is_playlists_folder_id(item_id) then
		return {}
	end
	local id = if item_id then item_ids.decode_jellyfin_item_id(item_id) else nil
	return { id = id, entity_type = module.scope_entity_type(id) }
end

function module.tracks_of_release(release_id: string?): { Track }
	if release_id == nil then
		return {}
	end
	return tracks.by_release({ release_id })[release_id] or {}
end

function module.releases_of_track(track_id: string?): { Release }
	if track_id == nil then
		return {}
	end
	return releases.by_track({ track_id })[track_id] or {}
end

return module
