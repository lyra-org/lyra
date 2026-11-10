--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")
local playlists = require("@lyra/playlists")
local tracks = require("@lyra/tracks")
local users = require("@lyra/users")

local cover_art = require("../catalog/cover_art")
local dates = require("../protocol/dates")
local request = require("../protocol/request")
local response = require("../protocol/response")
local songs = require("../catalog/songs")

type ApiQueryParams = api.ApiQueryParams
type ApiResponse = api.ApiResponse
type JsonObject = { [string]: any }
type Playlist = playlists.Playlist
type PlaylistEntry = playlists.PlaylistEntry
type Request = request.Request
type Track = tracks.Track

local module = {}

-- The playlists listed for a user: for the caller, its own and everyone's public ones; for
-- another user, that user's public ones.
local function listed_for(user_id: string, caller_id: string): { Playlist }
	local query: playlists.PlaylistQuery? = if user_id == caller_id then nil else { owner_id = user_id }
	return playlists.query(query).items
end

-- The `playlist` of a playlist showing `song_count` songs lasting `duration_ms`.
local function playlist_fields(
	playlist: Playlist,
	owners: { [string]: users.User },
	cover_art_ids: { [string]: string },
	caller_id: string,
	song_count: number,
	duration_ms: number
): JsonObject
	local created_at_ms = playlist.created_at_ms or 0
	return {
		id = playlist.id,
		name = playlist.name,
		comment = playlist.description,
		coverArt = cover_art_ids[playlist.id],
		owner = if owners[playlist.owner_id] then owners[playlist.owner_id].username else nil,
		public = playlist.is_public == true,
		songCount = song_count,
		duration = dates.seconds(duration_ms),
		created = dates.iso8601(created_at_ms),
		changed = dates.iso8601(playlist.updated_at_ms or created_at_ms),
		readonly = playlist.owner_id ~= caller_id,
	}
end

-- The `playlist` of each playlist, in order, from its visible entries.
local function describe(
	listed: { Playlist },
	links_by_playlist: { [string]: { PlaylistEntry } },
	caller_id: string
): { JsonObject }
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
	local owner_ids: { string } = {}
	local playlist_ids: { string } = {}
	for _, playlist in listed do
		table.insert(owner_ids, playlist.owner_id)
		table.insert(playlist_ids, playlist.id)
	end
	local owners = users.get(owner_ids)
	local cover_art_ids = cover_art.ids(playlist_ids)

	local items: { JsonObject } = {}
	for _, playlist in listed do
		local song_count, duration_ms = 0, 0
		for _, link in links_by_playlist[playlist.id] or {} do
			local track = found[link.track_id]
			if track and track.available then
				song_count += 1
				duration_ms += track.duration_ms or 0
			end
		end
		table.insert(items, playlist_fields(playlist, owners, cover_art_ids, caller_id, song_count, duration_ms))
	end
	return items
end

local function check_count(count: number, what: string)
	if count > playlists.MAX_TRACK_IDS then
		request.fail(0, `A playlist change takes at most {playlists.MAX_TRACK_IDS} {what}`)
	end
end

-- The songs these ids name, in order. Fails with 70 when one is missing or hidden, and with 0
-- past the playlist cap.
local function song_ids(params: ApiQueryParams, name: string): { string }
	local ids = request.values(params, name)
	check_count(#ids, "songs")
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

-- The entries of `links` the caller is shown, in order: those of songs they see that can play.
local function shown_entries(links: { PlaylistEntry }): { PlaylistEntry }
	local ids: { string } = {}
	for _, link in links do
		table.insert(ids, link.track_id)
	end
	local found = if #ids > 0 then tracks.get(ids) else {}
	local shown: { PlaylistEntry } = {}
	for _, link in links do
		local track = found[link.track_id]
		if track and track.available then
			table.insert(shown, link)
		end
	end
	return shown
end

local function visible_playlist(id: string): Playlist
	local playlist = playlists.get({ id })[id]
	if not playlist then
		return request.not_found("Playlist")
	end
	return playlist
end

-- The playlist parameter `name` names, which the caller must own.
local function owned_playlist(req: Request, name: string): Playlist
	local playlist = visible_playlist(request.required(req.params, name))
	if playlist.owner_id ~= req.auth.principal.user_id then
		return request.fail(50, "Only the owner can change the playlist")
	end
	return playlist
end

-- The tracks these ids name that the caller sees and can play, in order and with repeats.
local function playable_tracks(ids: { string }): { Track }
	local found = tracks.get(ids)
	local shown: { Track } = {}
	for _, id in ids do
		local track = found[id]
		if track and track.available then
			table.insert(shown, track)
		end
	end
	return shown
end

-- Only the entries' ids stay in memory: the tracks are read a batch at a time, for the song count
-- and the duration and again for the songs.
local function playlist_response(req: Request, playlist: Playlist): ApiResponse
	local links: { PlaylistEntry } = playlists.entries({ playlist.id })[playlist.id] or {}
	local entry_ids: { string } = {}
	for _, link in links do
		table.insert(entry_ids, link.track_id)
	end
	local song_count, duration_ms = 0, 0
	response.in_batches(entry_ids, function(batch)
		for _, track in playable_tracks(batch) do
			song_count += 1
			duration_ms += track.duration_ms or 0
		end
	end)
	local fields = playlist_fields(
		playlist,
		users.get({ playlist.owner_id }),
		cover_art.ids({ playlist.id }),
		req.auth.principal.user_id,
		song_count,
		duration_ms
	)
	return response.ok_stream(req.params, "playlist", {
		response.batched("entry", entry_ids, function(batch)
			return songs.list(playable_tracks(batch))
		end),
	}, fields :: any)
end

module.methods = {
	getPlaylists = function(req: Request): ApiResponse
		local caller_id = req.auth.principal.user_id
		local user_id = caller_id
		local username = request.param(req.params, "username")
		if username then
			if not request.holds(req.auth.principal.permissions, "admin") then
				return request.fail(50, "Only admins can name a user")
			end
			local named = users.list({ username = username })[1]
			if not named then
				return request.not_found("User")
			end
			user_id = named.id
		end

		local listed = listed_for(user_id, caller_id)
		local ids: { string } = {}
		for _, playlist in listed do
			table.insert(ids, playlist.id)
		end
		local links_by_playlist = if #ids > 0 then playlists.entries(ids) else {}
		local items = describe(listed, links_by_playlist, caller_id)
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
		return playlist_response(req, playlists.create({ name = name, track_ids = song_ids(params, "songId") }))
	end,

	-- Sets the fields, then removes the entries at `songIndexToRemove` from the playlist as the
	-- caller sees it, then appends `songIdToAdd`. The fields go first because the host validates
	-- them; every other parameter is checked before then, so only a concurrent change can stop it
	-- part way.
	updatePlaylist = function(req: Request): ApiResponse
		local params = req.params
		local playlist = owned_playlist(req, "playlistId")
		local name = request.param(params, "name")
		local comment = request.param_or_empty(params, "comment")
		local is_public = request.bool(params, "public")
		local indices = request.ints(params, "songIndexToRemove")
		check_count(#indices, "removals")
		local add_ids = song_ids(params, "songIdToAdd")

		local remove_entries: { string } = {}
		if #indices > 0 then
			local links = shown_entries(playlists.entries({ playlist.id })[playlist.id] or {})
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

		if
			(name or comment or is_public ~= nil)
			and not playlists.update(playlist.id, {
				name = name,
				description = comment,
				is_public = is_public,
			})
		then
			return request.fail(0, "The playlist changed while it was being updated")
		end
		if #remove_entries > 0 and not playlists.remove_entries(playlist.id, remove_entries) then
			return request.fail(0, "The playlist changed while it was being updated")
		end
		if #add_ids > 0 and not playlists.add_tracks(playlist.id, add_ids) then
			return request.fail(0, "The playlist changed while it was being updated")
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
