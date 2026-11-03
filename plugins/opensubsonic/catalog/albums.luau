--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")
local favorites = require("@lyra/favorites")
local genres = require("@lyra/genres")
local listens = require("@lyra/listens")
local ratings = require("@lyra/ratings")
local releases = require("@lyra/releases")
local tracks = require("@lyra/tracks")

local cover_art = require("./cover_art")
local credits = require("./credits")
local dates = require("../protocol/dates")
local response = require("../protocol/response")

type JsonObject = { [string]: any }
type ReleaseSummary = releases.ReleaseSummary
type ReleaseType = releases.ReleaseType

local module = {}

-- The `ItemGenre` list of an album's genres, and the first one's name. Songs take their album's,
-- as Lyra keeps no genres of their own.
function module.genres(release_genres: { genres.GenreInfo }?): ({ any }?, string?)
	if not release_genres or #release_genres == 0 then
		return nil, nil
	end
	local items: { any } = {}
	for _, genre in release_genres do
		table.insert(items, { name = genre.name })
	end
	return items, release_genres[1].name
end

local function release_type_name(release_type: ReleaseType?): string?
	if release_type == nil or release_type == "unknown" then
		return nil
	elseif release_type == "album" then
		return "Album"
	elseif release_type == "single" then
		return "Single"
	elseif release_type == "ep" then
		return "EP"
	elseif release_type == "compilation" then
		return "Compilation"
	elseif release_type == "soundtrack" then
		return "Soundtrack"
	elseif release_type == "live" then
		return "Live"
	elseif release_type == "remix" then
		return "Remix"
	elseif release_type == "broadcast" then
		return "Broadcast"
	elseif release_type == "other" then
		return "Other"
	else
		local _: never = release_type
		return nil
	end
end

-- Stands in for a creation time when neither the release nor any of its tracks has one.
local UNKNOWN_CREATED = "1970-01-01T00:00:00Z"

local function created_ms(item: { created_at_ms: number?, file_created_at_ms: number? }): number?
	return item.file_created_at_ms or item.created_at_ms
end

-- Each release's creation time, else its earliest track's, as `AlbumID3.created` is required.
local function created_times(summaries: { ReleaseSummary }): { [string]: string }
	local times: { [string]: string } = {}
	local missing: { string } = {}
	for _, summary in summaries do
		local time = dates.iso8601(created_ms(summary))
		if time then
			times[summary.id] = time
		else
			table.insert(missing, summary.id)
		end
	end
	if #missing > 0 then
		for release_id, release_tracks in tracks.by_release(missing) do
			local earliest: number? = nil
			for _, track in release_tracks do
				local time = created_ms(track)
				if time and (earliest == nil or time < earliest) then
					earliest = time
				end
			end
			times[release_id] = dates.iso8601(earliest)
		end
	end
	return times
end

-- The `AlbumID3` of each release, in order.
function module.list(summaries: { ReleaseSummary }): { JsonObject }
	if #summaries == 0 then
		return {}
	end
	local ids: { string } = {}
	for _, summary in summaries do
		table.insert(ids, summary.id)
	end

	local projections = credits.projections(ids, { "credits" })
	local genres_by_release = genres.by_release(ids)
	local listen_stats = listens.get(ids)
	local user_ratings = ratings.get(ids)
	local starred = favorites.get(ids)
	local created = created_times(summaries)
	local cover_art_ids = cover_art.ids(ids)

	local albums: { JsonObject } = {}
	for _, summary in summaries do
		local id = summary.id
		local artists, display_artist = credits.artists(projections[id])
		local genre_items, genre = module.genres(genres_by_release[id])
		local stats = listen_stats[id]
		local release_type = release_type_name(summary.release_type)
		local item: JsonObject = {
			id = id,
			name = summary.release_title,
			artist = display_artist,
			artistId = if artists then artists[1].id else nil,
			coverArt = cover_art_ids[id],
			songCount = summary.track_count,
			duration = dates.seconds(summary.total_duration),
			playCount = if stats then stats.count else 0,
			-- Absent rather than empty when unplayed: "" isn't an ISO 8601 date. Navidrome does the same.
			played = dates.iso8601(if stats then stats.last_played_at_ms else nil),
			created = created[id] or UNKNOWN_CREATED,
			starred = dates.iso8601(starred[id]),
			year = dates.year(summary.release_date),
			genre = genre,
			-- Absent rather than 0 when unrated: ratings are 1-5, and 0 is how `setRating` clears one.
			userRating = user_ratings[id],
			genres = genre_items or {},
			artists = artists or {},
			displayArtist = display_artist or "",
			releaseTypes = if release_type then { release_type } else {},
			sortName = summary.sort_title or "",
			releaseDate = dates.item_date(summary.release_date) or response.EMPTY_OBJECT,
			isCompilation = summary.release_type == "compilation",
		}
		table.insert(albums, item)
	end
	return albums
end

-- The `Child` of each release, in order, as directory-based methods such as `search2` list albums:
-- a directory whose parent is its first artist.
function module.children(summaries: { ReleaseSummary }): { JsonObject }
	if #summaries == 0 then
		return {}
	end
	local items = module.list(summaries)
	local ids: { string } = {}
	for _, summary in summaries do
		table.insert(ids, summary.id)
	end
	local average_ratings = ratings.average(ids)

	local children: { JsonObject } = {}
	for _, album in items do
		local child: JsonObject = {
			id = album.id,
			parent = album.artistId,
			isDir = true,
			title = album.name,
			album = album.name,
			artist = album.artist,
			year = album.year,
			genre = album.genre,
			coverArt = album.coverArt,
			duration = album.duration,
			playCount = album.playCount,
			played = album.played,
			created = album.created,
			starred = album.starred,
			userRating = album.userRating,
			averageRating = average_ratings[album.id],
			artistId = album.artistId,
			mediaType = "album",
			sortName = album.sortName,
			genres = album.genres,
			artists = album.artists,
			displayArtist = album.displayArtist,
			albumArtists = album.artists,
			displayAlbumArtist = album.displayArtist,
		}
		table.insert(children, child)
	end
	return children
end

return module
