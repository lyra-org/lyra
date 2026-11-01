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

local credits = require("./credits")
local dates = require("../protocol/dates")

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
			songCount = summary.track_count,
			duration = dates.seconds(summary.total_duration),
			playCount = if stats then stats.count else 0,
			played = dates.iso8601(if stats then stats.last_played_at_ms else nil),
			created = dates.iso8601(summary.file_created_at_ms or summary.created_at_ms),
			starred = dates.iso8601(starred[id]),
			year = dates.year(summary.release_date),
			genre = genre,
			userRating = user_ratings[id],
			genres = genre_items,
			artists = artists,
			displayArtist = display_artist,
			releaseTypes = if release_type then { release_type } else nil,
			sortName = summary.sort_title,
			releaseDate = dates.item_date(summary.release_date),
			isCompilation = summary.release_type == "compilation",
		}
		table.insert(albums, item)
	end
	return albums
end

return module
