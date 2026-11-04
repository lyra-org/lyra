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
local MUSICBRAINZ_RELEASE = "musicbrainz:release"

local DEFAULT_SIMILAR_COUNT = 50
local DEFAULT_TOP_COUNT = 50
local MAX_TOP_COUNT = 500

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

-- The artists `getTopSongs` asks for: the one `id` names, else every artist named `artist`, as
-- a name can't tell namesakes apart.
local function top_artist_ids(params: api.ApiQueryParams): { string }
	local id = request.param(params, "id")
	if id then
		if not artists.query({ ids = { id }, limit = 1 }).items[1] then
			return request.not_found("Artist")
		end
		return { id }
	end
	local ids: { string } = {}
	for _, artist in artists.query({ name = request.required(params, "artist") }).items do
		table.insert(ids, artist.id)
	end
	return ids
end

-- The album an album or song id names, the song's own; fails with 70 for anything else, or a song
-- without an album.
local function album_projection(id: string): any
	local projection = entities.query_many({ ids = { id }, include = { "external_ids" } })[id] :: any
	if projection and projection.entity_type == "track" then
		local song = entities.query_many({ ids = { id }, include = { "releases" } })[id] :: any
		local album = if song then (song.includes.releases or {})[1] else nil
		projection = if album
			then entities.query_many({ ids = { album.id }, include = { "external_ids" } })[album.id]
			else nil
	end
	if not projection or projection.entity_type ~= "release" then
		return request.not_found("Album")
	end
	return projection
end

-- What Lyra keeps of an album: its description and MusicBrainz id. Image URLs would need the
-- caller's credentials to load, so they stay out.
local function album_info(req: Request): ApiResponse
	local projection = album_projection(request.required(req.params, "id"))
	local description: string? = projection.entity.description
	local external_ids = projection.includes.external_ids or {}
	local info: JsonObject = {
		notes = if description and description ~= "" then description else nil,
		musicBrainzId = external_ids[MUSICBRAINZ_RELEASE],
	}
	return response.ok(req.params, { albumInfo = if next(info) ~= nil then info else response.EMPTY_OBJECT })
end

-- Every artist credited on an album the caller can see, by sort name, as Navidrome and gonic list
-- album artists only: artists credited only on songs have no albums to show.
local function album_artists(params: api.ApiQueryParams): { artists.ArtistSummary }
	return artists.query({
		library_id = folders.library_id(params),
		credit_role = "release",
		sort = { { key = "sort_name" } },
	}).items
end

-- The albums an artist's `albumCount` counts: those crediting the artist themselves, oldest first.
local function artist_albums(artist_id: string): { releases.ReleaseSummary }
	return releases.query({
		artist_ids = { artist_id },
		credit_role = "release",
		sort = { { key = "year" }, { key = "sort_name" } },
	}).items
end

-- An album's songs, in disc and track order: the host's order for a query of one release.
local function album_songs(release_id: string): { JsonObject }
	return songs.list(tracks.query({ release_ids = { release_id } }).items)
end

local module = {}

module.methods = {
	getMusicFolders = function(req: Request): ApiResponse
		return response.ok(req.params, { musicFolders = { musicFolder = folders.list() } })
	end,

	getArtists = function(req: Request): ApiResponse
		local found = album_artists(req.params)
		local result: JsonObject = { ignoredArticles = IGNORED_ARTICLES }
		if #found > 0 then
			result.index = artist_items.indexes(found, artist_items.list(found))
		end
		return response.ok(req.params, { artists = result })
	end,

	-- The artists of `getArtists`, as the top directories of the folder tree: artists, their
	-- albums, the albums' songs. Lyra keeps no time the catalog last changed, so `lastModified` is
	-- 0 and `ifModifiedSince` changes nothing, as on gonic.
	getIndexes = function(req: Request): ApiResponse
		local found = album_artists(req.params)
		local result: JsonObject = { ignoredArticles = IGNORED_ARTICLES, lastModified = 0 }
		if #found > 0 then
			result.index = artist_items.indexes(found, artist_items.directories(found))
		end
		return response.ok(req.params, { indexes = result })
	end,

	-- An artist's directory holds its albums, an album's its songs, as `getArtist` and `getAlbum`
	-- list them. Only artists and albums are directories.
	getMusicDirectory = function(req: Request): ApiResponse
		local id = request.required(req.params, "id")
		local kind = entities.get_type(id)
		local directory: JsonObject? = nil
		if kind == "Artist" then
			local summary = artists.query({ ids = { id }, limit = 1 }).items[1]
			if summary then
				local artist = artist_items.directories({ summary })[1]
				directory = {
					id = id,
					name = artist.name,
					starred = artist.starred,
					userRating = artist.userRating,
					averageRating = artist.averageRating,
				}
				local children = albums.children(artist_albums(id))
				if #children > 0 then
					directory.child = children
				end
			end
		elseif kind == "Release" then
			local summary = releases.query({ ids = { id }, limit = 1 }).items[1]
			if summary then
				local album = albums.children({ summary })[1]
				directory = {
					id = id,
					parent = album.parent,
					name = album.title,
					starred = album.starred,
					userRating = album.userRating,
					averageRating = album.averageRating,
					playCount = album.playCount,
				}
				local children = album_songs(id)
				if #children > 0 then
					directory.child = children
				end
			end
		end
		if not directory then
			return request.not_found("Directory")
		end
		return response.ok(req.params, { directory = directory })
	end,

	getArtist = function(req: Request): ApiResponse
		local id = request.required(req.params, "id")
		local summary = artists.query({ ids = { id }, limit = 1 }).items[1]
		if not summary then
			return request.not_found("Artist")
		end
		local artist = artist_items.list({ summary })[1]
		local found = artist_albums(id)
		if #found > 0 then
			artist.album = albums.list(found)
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

	-- The caller's own most played songs of an artist; Lyra keeps no popularity beyond listens.
	getTopSongs = function(req: Request): ApiResponse
		local params = req.params
		local artist_ids = top_artist_ids(params)
		local count = request.count(params, "count", DEFAULT_TOP_COUNT, MAX_TOP_COUNT)
		local items: { JsonObject } = {}
		if #artist_ids > 0 and count > 0 then
			items = songs.list(tracks.query({
				artist_ids = artist_ids,
				listened = true,
				sort = {
					{ key = "listen_count", order = "descending" },
					{ key = "last_played_at", order = "descending" },
				},
				limit = count,
			}).items)
		end
		return response.ok(params, { topSongs = if #items > 0 then { song = items } else response.EMPTY_OBJECT })
	end,

	getAlbumInfo2 = album_info,
	-- Albums are directories of the same ids, so their info is the same.
	getAlbumInfo = album_info,

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
		local album_song_items = album_songs(id)
		if #album_song_items > 0 then
			album.song = album_song_items
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
