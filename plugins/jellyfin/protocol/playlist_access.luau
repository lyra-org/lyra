--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local constants = require("../constants")
local item_ids = require("./items/identifiers")
local playlists = require("@lyra/playlists")

type PlaylistInfo = playlists.PlaylistInfo
type PlaylistTrackLink = playlists.PlaylistTrackLink

local module = {}

function module.find(playlist_id: number?): PlaylistInfo?
	local numeric_id = if playlist_id then tonumber(playlist_id) else nil
	if not numeric_id or numeric_id <= 0 then
		return nil
	end

	local ok, result = pcall(playlists.get_by_id, numeric_id)
	if not ok then
		return nil
	end

	return result
end

-- The owner's public id.
function module.owner_id(playlist: PlaylistInfo?): string?
	local raw_playlist_id = if playlist then playlist.db_id else nil
	if raw_playlist_id == nil or raw_playlist_id <= 0 then
		return nil
	end

	local ok, owner_id = pcall(playlists.get_owner, raw_playlist_id)
	if not ok or type(owner_id) ~= "string" then
		return nil
	end

	return owner_id
end

function module.can_edit(playlist: PlaylistInfo?, user_id: string?): boolean
	local owner_id = module.owner_id(playlist)
	return owner_id ~= nil and owner_id == user_id
end

function module.can_view(playlist: PlaylistInfo?, user_id: string?): boolean
	if module.can_edit(playlist, user_id) then
		return true
	end

	return playlist ~= nil and playlist.is_public == true
end

-- Playlists owned by the dispatch principal.
function module.list_owned(): { PlaylistInfo }
	local ok, playlist_items = pcall(playlists.list_owned)
	if not ok or type(playlist_items) ~= "table" then
		return {}
	end
	return playlist_items
end

function module.track_refs(playlist: PlaylistInfo?): { PlaylistTrackLink }
	local playlist_id = if playlist then playlist.db_id else nil
	if playlist_id == nil or playlist_id <= 0 then
		return {}
	end
	local ok, track_links = pcall(playlists.get_tracks, playlist_id)
	if not ok or type(track_links) ~= "table" then
		return {}
	end
	return track_links
end

function module.track_count(playlist: PlaylistInfo?): number
	return #module.track_refs(playlist)
end

function module.track_ids(playlist: PlaylistInfo): { number }
	local track_ids: { number } = {}
	for _, link: PlaylistTrackLink in module.track_refs(playlist) do
		if link.track_id and link.track_id > 0 then
			table.insert(track_ids, link.track_id)
		end
	end
	return track_ids
end

function module.resolve_entry_ids(playlist: PlaylistInfo, raw_ids: { string }): { string }?
	local by_hex: { [string]: string } = {}
	for _, entry in module.track_refs(playlist) do
		local hex_id = item_ids.encode_jellyfin_item_id(entry.entry_id)
		if hex_id then
			by_hex[hex_id] = entry.entry_id
		end
	end
	local resolved: { string } = {}
	local seen: { [string]: boolean } = {}
	for _, raw_id in raw_ids do
		local hex_id = constants.normalize_item_id(raw_id)
		local entry_id = if hex_id then by_hex[hex_id] else nil
		if not hex_id or entry_id == nil then
			return nil
		end
		if not seen[hex_id] then
			seen[hex_id] = true
			table.insert(resolved, entry_id)
		end
	end
	return resolved
end

return module
