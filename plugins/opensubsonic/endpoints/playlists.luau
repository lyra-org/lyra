--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")
local playlists = require("@lyra/playlists")
local tracks = require("@lyra/tracks")
local users = require("@lyra/users")

local dates = require("../protocol/dates")
local request = require("../protocol/request")
local response = require("../protocol/response")
local songs = require("../catalog/songs")

type ApiQueryParams = api.ApiQueryParams
type ApiResponse = api.ApiResponse
type JsonObject = { [string]: any }
type PlaylistInfo = playlists.PlaylistInfo
type PlaylistTrackLink = playlists.PlaylistTrackLink
type Request = request.Request
type Track = tracks.Track

-- A playlist with its owner's id, which listings and `readonly` need.
type Owned = { playlist: PlaylistInfo, owner_id: string? }

local module = {}

local function is_admin(req: Request): boolean
	return table.find(req.auth.principal.permissions, "admin") ~= nil
end

-- The playlists a user can play: its own, then everyone else's public ones, whatever its role.
local function playable_by(user_id: string, caller_id: string): { Owned }
	local playable: { Owned } = {}
	local listed: { [string]: boolean } = {}
	if user_id == caller_id then
		for _, playlist in playlists.list_owned() do
			listed[playlist.id] = true
			table.insert(playable, { playlist = playlist, owner_id = caller_id })
		end
	end
	for _, playlist in playlists.list() do
		if listed[playlist.id] or (user_id == caller_id and playlist.is_public ~= true) then
			continue
		end
		local owner_id = playlists.get_owner(playlist.id)
		if owner_id == user_id or playlist.is_public == true then
			table.insert(playable, { playlist = playlist, owner_id = owner_id })
		end
	end
	return playable
end

-- The `playlist` of each playlist, in order, from its visible entries, and each one's entry
-- tracks by playlist id, in order and with repeats.
local function describe(
	owned: { Owned },
	links_by_playlist: { [string]: { PlaylistTrackLink } },
	caller_id: string
): ({ JsonObject }, { [string]: { Track } })
	local track_ids: { string } = {}
	local seen: { [string]: boolean } = {}
	for _, links in links_by_playlist do
		for _, link in links do
			if not seen[link.track_id] then
				seen[link.track_id] = true
				table.insert(track_ids, link.track_id)
			end
		end
	end
	local found: { [string]: Track } = if #track_ids > 0 then tracks.get(track_ids) else {}
	local usernames: { [string]: string } = {}
	for _, user in users.list() do
		usernames[user.user_id] = user.username
	end

	local items: { JsonObject } = {}
	local entries_by_playlist: { [string]: { Track } } = {}
	for _, item in owned do
		local playlist = item.playlist
		local entry_tracks: { Track } = {}
		local duration_ms = 0
		for _, link in links_by_playlist[playlist.id] or {} do
			local track = found[link.track_id]
			if track then
				table.insert(entry_tracks, track)
				duration_ms += track.duration_ms or 0
			end
		end
		entries_by_playlist[playlist.id] = entry_tracks
		local created_at_ms = playlist.created_at_ms or 0
		local described: JsonObject = {
			id = playlist.id,
			name = playlist.name,
			comment = playlist.description,
			owner = if item.owner_id then usernames[item.owner_id] else nil,
			public = playlist.is_public == true,
			songCount = #entry_tracks,
			duration = dates.seconds(duration_ms),
			created = dates.iso8601(created_at_ms),
			changed = dates.iso8601(playlist.updated_at_ms or created_at_ms),
			readonly = item.owner_id ~= caller_id,
		}
		table.insert(items, described)
	end
	return items, entries_by_playlist
end

-- The songs these ids name, in order. Fails with 70 when one is missing or hidden.
local function song_ids(params: ApiQueryParams, name: string): { string }
	local ids = request.values(params, name)
	if #ids == 0 then
		return ids
	end
	local found = tracks.get(ids)
	for _, id in ids do
		if not found[id] then
			return request.not_found(`Song {id}`)
		end
	end
	return ids
end

local function visible_playlist(id: string): PlaylistInfo
	local playlist = playlists.get_by_id(id)
	if not playlist then
		return request.not_found("Playlist")
	end
	return playlist
end

-- The playlist parameter `name` names, which the caller must own.
local function owned_playlist(req: Request, name: string): PlaylistInfo
	local playlist = visible_playlist(request.required(req.params, name))
	if playlists.get_owner(playlist.id) ~= req.auth.principal.user_id then
		return request.fail(50, "Only the owner can change the playlist")
	end
	return playlist
end

local function playlist_response(req: Request, playlist: PlaylistInfo): ApiResponse
	local caller_id = req.auth.principal.user_id
	local owned = { { playlist = playlist, owner_id = playlists.get_owner(playlist.id) } }
	local items, entries = describe(owned, { [playlist.id] = playlists.get_tracks(playlist.id) }, caller_id)
	local item = items[1]
	local entry_tracks = entries[playlist.id]
	if #entry_tracks > 0 then
		item.entry = songs.list(entry_tracks)
	end
	return response.ok(req.params, { playlist = item })
end

module.methods = {
	getPlaylists = function(req: Request): ApiResponse
		local caller_id = req.auth.principal.user_id
		local user_id = caller_id
		local username = request.param(req.params, "username")
		if username and string.lower(username) ~= string.lower(req.auth.principal.username) then
			if not is_admin(req) then
				return request.fail(50, "Only admins can list another user's playlists")
			end
			local wanted = string.lower(username)
			local found: string? = nil
			for _, user in users.list() do
				if string.lower(user.username) == wanted then
					found = user.user_id
					break
				end
			end
			if not found then
				return request.not_found("User")
			end
			user_id = found
		end

		local owned = playable_by(user_id, caller_id)
		local ids: { string } = {}
		for _, item in owned do
			table.insert(ids, item.playlist.id)
		end
		local links_by_playlist = if #ids > 0 then playlists.get_tracks_many(ids) else {}
		local items = describe(owned, links_by_playlist, caller_id)
		return response.ok(
			req.params,
			{ playlists = if #items > 0 then { playlist = items } else response.EMPTY_OBJECT }
		)
	end,

	getPlaylist = function(req: Request): ApiResponse
		return playlist_response(req, visible_playlist(request.required(req.params, "id")))
	end,

	-- Creates a playlist of `songId`s, or with `playlistId` makes them an owned playlist's songs.
	createPlaylist = function(req: Request): ApiResponse
		local params = req.params
		if request.param(params, "playlistId") then
			local playlist = owned_playlist(req, "playlistId")
			local ids = song_ids(params, "songId")
			local name = request.param(params, "name")
			local replaced =
				playlists.replace(playlist.id, { fields = if name then { name = name } else nil, track_ids = ids })
			if not replaced then
				return request.not_found("Playlist")
			end
			return playlist_response(req, replaced)
		end

		local name = request.required(params, "name")
		local playlist_id = playlists.create({ name = name, track_ids = song_ids(params, "songId") })
		return playlist_response(req, visible_playlist(playlist_id))
	end,

	-- Removes the entries at `songIndexToRemove` from the playlist as the caller sees it, then
	-- appends `songIdToAdd`.
	updatePlaylist = function(req: Request): ApiResponse
		local params = req.params
		local playlist = owned_playlist(req, "playlistId")
		local name = request.param(params, "name")
		local comment = request.param_or_empty(params, "comment")
		local is_public = request.bool(params, "public")
		local indices = request.ints(params, "songIndexToRemove")
		local add_ids = song_ids(params, "songIdToAdd")

		local remove_entries: { string } = {}
		if #indices > 0 then
			local links = playlists.get_tracks(playlist.id)
			local removing: { [string]: boolean } = {}
			for _, index in indices do
				local link = links[index + 1]
				if not link then
					return request.not_found(`Playlist entry {index}`)
				end
				if not removing[link.entry_id] then
					removing[link.entry_id] = true
					table.insert(remove_entries, link.entry_id)
				end
			end
		end

		if name or comment or is_public ~= nil then
			playlists.update({
				playlist_id = playlist.id,
				name = name,
				description = comment,
				is_public = is_public,
			})
		end
		if #remove_entries > 0 and not playlists.remove_tracks(playlist.id, remove_entries) then
			return request.fail(0, "The playlist changed while it was being updated")
		end
		if #add_ids > 0 then
			playlists.add_tracks(playlist.id, add_ids)
		end
		return response.ok(params)
	end,

	deletePlaylist = function(req: Request): ApiResponse
		local playlist = visible_playlist(request.required(req.params, "id"))
		if not playlists.delete(playlist.id) then
			return request.fail(50, "Not allowed to delete the playlist")
		end
		return response.ok(req.params)
	end,
}

return module
