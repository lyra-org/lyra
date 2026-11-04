--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")
local bookmarks = require("@lyra/bookmarks")
local entities = require("@lyra/entities")
local favorites = require("@lyra/favorites")
local genres = require("@lyra/genres")
local listens = require("@lyra/listens")
local playback_sources = require("@lyra/playback_sources")
local ratings = require("@lyra/ratings")
local tracks = require("@lyra/tracks")

local albums = require("./albums")
local cover_art = require("./cover_art")
local credits = require("./credits")
local dates = require("../protocol/dates")
local paths = require("./paths")
local response = require("../protocol/response")

type JsonObject = { [string]: any }
type EntryInfo = playback_sources.EntryInfo
type PlaybackSourceInfo = playback_sources.PlaybackSourceInfo
type Release = entities.Release
type Track = tracks.Track

local MUSICBRAINZ_RECORDING = "musicbrainz:recording"

local CONTENT_TYPES: { [string]: string } = {
	aac = "audio/aac",
	aiff = "audio/aiff",
	ape = "audio/x-ape",
	flac = "audio/flac",
	m4a = "audio/mp4",
	mp3 = "audio/mpeg",
	mpc = "audio/x-musepack",
	ogg = "audio/ogg",
	opus = "audio/ogg",
	wav = "audio/wav",
	wavpack = "audio/x-wavpack",
	wv = "audio/x-wavpack",
}

local module = {}

local function unique_ids(items: { { id: string } }): { string }
	local ids: { string } = {}
	local seen: { [string]: boolean } = {}
	for _, item in items do
		if not seen[item.id] then
			seen[item.id] = true
			table.insert(ids, item.id)
		end
	end
	return ids
end

local function suffix_of(name: string?): string?
	local extension = if name then string.match(name, "%.([^%./\\]+)$") else nil
	return if extension then string.lower(extension) else nil
end

-- Always present, an empty object without any gain.
local function replay_gain(track: Track): any
	if track.track_gain_db == nil and track.album_gain_db == nil then
		return response.EMPTY_OBJECT
	end
	return { trackGain = track.track_gain_db, albumGain = track.album_gain_db }
end

-- The `Child` of each track, in order, with every field the caller's view of it fills.
-- `saved_bookmarks` are the caller's bookmarks of the tracks when already read.
function module.list(track_list: { Track }, saved_bookmarks: { [string]: bookmarks.Bookmark }?): { JsonObject }
	if #track_list == 0 then
		return {}
	end
	local ids = unique_ids(track_list :: any)

	local projections = credits.projections(ids, { "credits", "releases", "external_ids" })
	local album_by_track: { [string]: Release } = {}
	local release_ids: { string } = {}
	local release_seen: { [string]: boolean } = {}
	for _, id in ids do
		local projection = projections[id] :: any
		local releases: { Release }? = if projection then projection.includes.releases else nil
		local album = if releases then releases[1] else nil
		if album then
			album_by_track[id] = album
			if not release_seen[album.id] then
				release_seen[album.id] = true
				table.insert(release_ids, album.id)
			end
		end
	end
	local release_projections = credits.projections(release_ids, { "credits" })
	local genres_by_release = if #release_ids > 0 then genres.by_release(release_ids) else {}

	local sources = playback_sources.get_many(ids, true)
	local relative_files: { EntryInfo } = {}
	for _, source in sources do
		if source.entry and source.entry.full_path == nil then
			table.insert(relative_files, source.entry)
		end
	end
	local relative_paths = if #relative_files > 0 then paths.relative(relative_files) else {}

	local listen_stats = listens.get(ids)
	local user_ratings = ratings.get(ids)
	local average_ratings = ratings.average(ids)
	local starred = favorites.get(ids)
	local bookmarks_by_id = saved_bookmarks or bookmarks.get(ids)
	local cover_art_ids = cover_art.ids(ids)

	local songs: { JsonObject } = {}
	for _, track in track_list do
		local id = track.id
		local album = album_by_track[id]
		local projection = projections[id] :: any
		local external_ids = if projection then projection.includes.external_ids or {} else {}
		local artists, display_artist = credits.artists(projections[id])
		local album_artists, display_album_artist =
			credits.artists(if album then release_projections[album.id] else nil)
		local contributors, display_composer = credits.contributors(projections[id])
		local genre_items, genre = albums.genres(if album then genres_by_release[album.id] else nil)
		local stats = listen_stats[id]
		local bookmark = bookmarks_by_id[id]

		local source: PlaybackSourceInfo? = sources[id]
		local entry = if source then source.entry else nil
		local suffix = if entry then suffix_of(entry.name) else nil
		local container = if source then source.container else nil
		local codec = if source then source.codec else nil
		local content_type = (if codec then CONTENT_TYPES[codec] else nil)
			or (if container then CONTENT_TYPES[container] else nil)
			or (if suffix then CONTENT_TYPES[suffix] else nil)
		local bitrate_bps = if source then source.bitrate_bps else nil

		local item: JsonObject = {
			id = id,
			parent = if album then album.id else nil,
			isDir = false,
			title = track.track_title,
			album = if album then album.release_title else nil,
			coverArt = cover_art_ids[id],
			artist = display_artist,
			track = track.track,
			year = track.year or (if album then dates.year(album.release_date) else nil),
			genre = genre,
			size = if entry then entry.size else nil,
			contentType = content_type,
			suffix = suffix or container,
			duration = dates.seconds(track.duration_ms),
			bitRate = if bitrate_bps then math.floor(bitrate_bps / 1000 + 0.5) else nil,
			bitDepth = if source then source.bit_depth or 0 else 0,
			samplingRate = if source then source.sample_rate_hz or 0 else 0,
			channelCount = if source then source.channel_count or 0 else 0,
			path = if entry then entry.full_path or relative_paths[entry.id] else nil,
			isVideo = false,
			userRating = user_ratings[id],
			averageRating = average_ratings[id],
			playCount = if stats then stats.count else 0,
			-- Absent rather than empty when unplayed: "" isn't an ISO 8601 date. Navidrome does the same.
			played = dates.iso8601(if stats then stats.last_played_at_ms else nil),
			discNumber = track.disc,
			created = dates.iso8601(track.file_created_at_ms or track.created_at_ms),
			starred = dates.iso8601(starred[id]),
			-- In milliseconds, as clients and the spec's example read it, though its schema says seconds.
			bookmarkPosition = if bookmark then bookmark.position_ms else nil,
			albumId = if album then album.id else nil,
			artistId = if artists then artists[1].id else nil,
			type = "music",
			mediaType = "song",
			sortName = track.sort_title or "",
			musicBrainzId = external_ids[MUSICBRAINZ_RECORDING],
			genres = genre_items or {},
			artists = artists or {},
			displayArtist = display_artist or "",
			albumArtists = album_artists or {},
			displayAlbumArtist = display_album_artist or "",
			contributors = contributors or {},
			displayComposer = display_composer or "",
			replayGain = replay_gain(track),
		}
		table.insert(songs, item)
	end
	return songs
end

return module
