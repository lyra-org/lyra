--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")
local artists = require("@lyra/artists")
local entities = require("@lyra/entities")
local genres = require("@lyra/genres")
local mix = require("@lyra/mix")
local releases = require("@lyra/releases")
local tracks = require("@lyra/tracks")

local albums = require("../catalog/albums")
local artist_items = require("../catalog/artists")
local credits = require("../catalog/credits")
local folders = require("../catalog/folders")
local request = require("../protocol/request")
local response = require("../protocol/response")
local songs = require("../catalog/songs")

type ApiResponse = api.ApiResponse
type JsonObject = { [string]: any }
type Request = request.Request

-- Lyra orders artists by their sort names, which tags give, rather than by dropping articles.
local IGNORED_ARTICLES = ""

local MUSICBRAINZ_ARTIST = "musicbrainz:artist"

local DEFAULT_SIMILAR_COUNT = 50

-- The artist an artist, album or song id names: the artist itself, or the first artist credited
-- on the album or song. Fails with 70 for anything else, or an album or song without an artist.
local function artist_projection(id: string): any
	local projection = entities.query_many({ ids = { id }, include = { "external_ids" } })[id] :: any
	if projection and projection.entity_type ~= "artist" then
		local refs = credits.artists(credits.projections({ id }, { "credits" })[id])
		local artist_id = if refs then refs[1].id else nil
		projection = if artist_id
			then entities.query_many({ ids = { artist_id }, include = { "external_ids" } })[artist_id]
			else nil
	end
	if not projection then
		return request.not_found("Artist")
	end
	return projection
end

-- Up to `count` songs like the seed `id` names, `what` when it is none of `seed_types`, from
-- whichever mixer serves a blend of it, the built-in one at least. A song leaves itself out.
local function similar_songs(req: Request, container: string, seed_types: { string }, what: string): ApiResponse
	local params = req.params
	local id = request.required(params, "id")
	local count = request.count(params, "count", DEFAULT_SIMILAR_COUNT, mix.MAX_TRACKS)
	local seed_type = entities.get_type(id)
	if not seed_type or not table.find(seed_types, seed_type) then
		return request.not_found(what)
	end
	local found: { tracks.Track } = {}
	if count > 0 then
		local is_song = seed_type == "Track"
		local mixed = mix.blend({
			seeds = { id },
			limit = if is_song then math.min(count + 1, mix.MAX_TRACKS) else count,
		})
		if not mixed then
			return request.not_found(what)
		end
		for _, entry in mixed do
			if #found == count then
				break
			end
			if not (is_song and entry.track.id == id) then
				table.insert(found, entry.track)
			end
		end
	end
	local items = songs.list(found)
	return response.ok(params, { [container] = if #items > 0 then { song = items } else response.EMPTY_OBJECT })
end

local module = {}

module.methods = {
	getMusicFolders = function(req: Request): ApiResponse
		return response.ok(req.params, { musicFolders = { musicFolder = folders.list() } })
	end,

	-- Every artist credited on an album the caller can see, as Navidrome and gonic list album
	-- artists only: artists credited only on songs have no albums to show.
	getArtists = function(req: Request): ApiResponse
		local found = artists.query({
			library_id = folders.library_id(req.params),
			credit_role = "release",
			sort = { { key = "sort_name" } },
		}).items
		local result: JsonObject = { ignoredArticles = IGNORED_ARTICLES }
		if #found > 0 then
			result.index = artist_items.indexes(artist_items.list(found))
		end
		return response.ok(req.params, { artists = result })
	end,

	getArtist = function(req: Request): ApiResponse
		local id = request.required(req.params, "id")
		local summary = artists.query({ ids = { id }, limit = 1 }).items[1]
		if not summary then
			return request.not_found("Artist")
		end
		local artist = artist_items.list({ summary })[1]
		-- The albums `albumCount` counts: those crediting the artist themselves, oldest first.
		local artist_albums = releases.query({
			artist_ids = { id },
			credit_role = "release",
			sort = { { key = "year" }, { key = "sort_name" } },
		}).items
		if #artist_albums > 0 then
			artist.album = albums.list(artist_albums)
		end
		return response.ok(req.params, { artist = artist })
	end,

	-- What Lyra keeps of an artist: its description and MusicBrainz id. Lyra keeps no similar
	-- artists, so `count` and `includeNotPresent` change nothing.
	getArtistInfo2 = function(req: Request): ApiResponse
		local id = request.required(req.params, "id")
		request.int(req.params, "count")
		request.bool(req.params, "includeNotPresent")
		local projection = artist_projection(id)
		local description: string? = projection.entity.description
		local external_ids = projection.includes.external_ids or {}
		local info: JsonObject = {
			biography = if description and description ~= "" then description else nil,
			musicBrainzId = external_ids[MUSICBRAINZ_ARTIST],
		}
		return response.ok(req.params, { artistInfo2 = if next(info) ~= nil then info else response.EMPTY_OBJECT })
	end,

	-- Songs like an artist, album or song.
	getSimilarSongs = function(req: Request): ApiResponse
		return similar_songs(req, "similarSongs", { "Artist", "Release", "Track" }, "Artist, album or song")
	end,

	getSimilarSongs2 = function(req: Request): ApiResponse
		return similar_songs(req, "similarSongs2", { "Artist" }, "Artist")
	end,

	getGenres = function(req: Request): ApiResponse
		local items: { JsonObject } = {}
		for _, genre in genres.query({ sort = { { key = "name" } } }).items do
			local item: JsonObject =
				{ value = genre.name, songCount = genre.track_count, albumCount = genre.release_count }
			table.insert(items, item)
		end
		return response.ok(req.params, { genres = if #items > 0 then { genre = items } else response.EMPTY_OBJECT })
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
