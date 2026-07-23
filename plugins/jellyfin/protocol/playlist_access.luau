--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local playlists = require("@lyra/playlists")

type PlaylistInfo = playlists.PlaylistInfo
type PlaylistTrackLink = playlists.PlaylistTrackLink

local module = {}

function module.find(playlist_id: number?): PlaylistInfo?
	if not playlist_id or playlist_id <= 0 then
		return nil
	end

	local ok, result = pcall(playlists.get_by_id, playlist_id)
	if not ok then
		return nil
	end

	return result
end

function module.owner_id(playlist: PlaylistInfo?): number?
	local raw_playlist_id = if playlist then playlist.db_id else nil
	if raw_playlist_id == nil or raw_playlist_id <= 0 then
		return nil
	end

	local ok, owner_id = pcall(playlists.get_owner, raw_playlist_id)
	if not ok then
		return nil
	end

	if owner_id == nil or type(owner_id) ~= "number" or owner_id <= 0 then
		return nil
	end

	return owner_id
end

function module.can_edit(playlist: PlaylistInfo?, user_id: number?): boolean
	local owner_id = module.owner_id(playlist)
	return owner_id ~= nil and user_id ~= nil and owner_id == user_id
end

function module.can_view(playlist: PlaylistInfo?, user_id: number?): boolean
	if module.can_edit(playlist, user_id) then
		return true
	end

	return playlist ~= nil and playlist.is_public == true
end

function module.list_for_user(user_id: number?): { PlaylistInfo }
	if user_id == nil or user_id <= 0 then
		return {}
	end
	local ok, playlist_items = pcall(playlists.get_by_user, user_id)
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

return module
