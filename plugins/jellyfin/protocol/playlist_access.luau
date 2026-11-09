--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local item_ids = require("./items/identifiers")
local playlists = require("@lyra/playlists")

type Playlist = playlists.Playlist
type PlaylistEntry = playlists.PlaylistEntry

local module = {}

function module.find(playlist_id: string?): Playlist?
	if not playlist_id then
		return nil
	end

	local ok, result = pcall(playlists.get, { playlist_id })
	if not ok then
		return nil
	end

	return result[playlist_id]
end

-- The playlists with these ids the dispatch principal can see, in the order given.
function module.find_many(playlist_ids: { string }): { Playlist }
	if #playlist_ids == 0 then
		return {}
	end
	local ok, result = pcall(playlists.get, playlist_ids)
	if not ok then
		return {}
	end
	local found: { Playlist } = {}
	for _, playlist_id in playlist_ids do
		local playlist = result[playlist_id]
		if playlist then
			table.insert(found, playlist)
		end
	end
	return found
end

function module.owner_id(playlist: Playlist?): string?
	return if playlist then playlist.owner_id else nil
end

function module.can_edit(playlist: Playlist?, user_id: string?): boolean
	return playlist ~= nil and playlist.owner_id == user_id
end

-- As upstream, a playlist is deleted by its owner or an admin.
function module.can_delete(playlist: Playlist?, user_id: string?, is_admin: boolean): boolean
	return playlist ~= nil and (playlist.owner_id == user_id or is_admin)
end

function module.can_view(playlist: Playlist?, user_id: string?): boolean
	return playlist ~= nil and (playlist.owner_id == user_id or playlist.is_public)
end

-- Playlists the dispatch principal sees as upstream shows them: its own and the public ones,
-- whatever its role.
function module.list_visible(): { Playlist }
	local ok, page = pcall(playlists.query, nil)
	if not ok then
		return {}
	end
	return page.items
end

function module.count_visible(): number
	local count_only: playlists.PlaylistQuery = { limit = 0 }
	local ok, page = pcall(playlists.query, count_only)
	if not ok then
		return 0
	end
	return page.total
end

function module.load_track_refs(playlist: Playlist?): { PlaylistEntry }?
	if not playlist then
		return nil
	end
	local ok, entries = pcall(playlists.entries, { playlist.id })
	if not ok then
		return nil
	end
	return entries[playlist.id]
end

function module.track_refs(playlist: Playlist?): { PlaylistEntry }
	return module.load_track_refs(playlist) or {}
end

function module.track_count(playlist: Playlist?): number
	return #module.track_refs(playlist)
end

function module.track_ids(playlist: Playlist): { string }
	local track_ids: { string } = {}
	for _, link: PlaylistEntry in module.track_refs(playlist) do
		table.insert(track_ids, link.track_id)
	end
	return track_ids
end

-- Upstream names a playlist entry by its item: `PlaylistItemId` is the track's id, and requests
-- that name one match every entry of that track, in playlist order. Ids that match none are
-- ignored, as upstream.
function module.entries_of_items(playlist: Playlist, raw_ids: { string }): { string }
	local wanted: { [string]: boolean } = {}
	for _, raw_id in raw_ids do
		local track_id = item_ids.decode(raw_id)
		if track_id then
			wanted[track_id] = true
		end
	end
	local entry_ids: { string } = {}
	for _, entry in module.track_refs(playlist) do
		if wanted[entry.track_id] then
			table.insert(entry_ids, entry.entry_id)
		end
	end
	return entry_ids
end

return module
