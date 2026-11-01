--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")
local releases = require("@lyra/releases")
local tracks = require("@lyra/tracks")

local albums = require("../catalog/albums")
local folders = require("../catalog/folders")
local request = require("../protocol/request")
local response = require("../protocol/response")
local songs = require("../catalog/songs")

type ApiResponse = api.ApiResponse
type Request = request.Request

local module = {}

module.methods = {
	getMusicFolders = function(req: Request): ApiResponse
		return response.ok(req.params, { musicFolders = { musicFolder = folders.list() } })
	end,

	getAlbum = function(req: Request): ApiResponse
		local id = request.required(req.params, "id")
		local summary = releases.query({ ids = { id }, limit = 1 }).items[1]
		if not summary then
			return request.not_found("Album")
		end
		local album = albums.list({ summary })[1]
		local album_tracks = tracks.query({ release_ids = { id } }).items
		if #album_tracks > 0 then
			album.song = songs.list(album_tracks)
		end
		return response.ok(req.params, { album = album })
	end,

	getSong = function(req: Request): ApiResponse
		local id = request.required(req.params, "id")
		local track = tracks.get({ id })[id]
		if not track then
			return request.not_found("Song")
		end
		return response.ok(req.params, { song = songs.list({ track })[1] })
	end,
}

return module
