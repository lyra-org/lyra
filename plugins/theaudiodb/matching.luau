--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

-- Matching local artists and releases to TheAudioDB records.

local api = require("./api")

type JsonObject = api.JsonObject

-- What is known locally about an artist to look it up by.
export type ArtistQuery = {
	-- The TheAudioDB artist id stored on the artist.
	artist_id: string?,
	musicbrainz_id: string?,
	-- Names to search by, in order.
	names: { string },
	-- Asks TheAudioDB again instead of reusing recent responses.
	fresh: boolean?,
}

-- A credited artist of a local release.
export type CreditedArtist = {
	id: string,
	artist_name: string,
	musicbrainz_id: string?,
}

local matching = {}

local trim = api.trim
local string_field = api.string_field

function matching.normalize(value: string?): string
	local normalized = string.lower(trim(value))
	normalized = string.gsub(normalized, "%s+", " ")
	return normalized
end

local normalize = matching.normalize

-- The first four-digit year in `value`.
function matching.year(value: string?): string?
	return if value then string.match(value, "(%d%d%d%d)") else nil
end

function matching.names_equal(candidate: string?, target: string?): boolean
	local normalized = normalize(candidate)
	return normalized ~= "" and normalized == normalize(target)
end

local names_equal = matching.names_equal

-- Whether the record's MusicBrainz id names another artist. A record without one tells nothing.
local function musicbrainz_conflict(record_musicbrainz_id: string?, musicbrainz_id: string?): boolean
	if record_musicbrainz_id == nil or musicbrainz_id == nil or trim(musicbrainz_id) == "" then
		return false
	end
	return string.lower(record_musicbrainz_id) ~= string.lower(trim(musicbrainz_id))
end

local function artist_conflicts(artist: JsonObject, musicbrainz_id: string?): boolean
	return musicbrainz_conflict(string_field(artist, "strMusicBrainzID"), musicbrainz_id)
end

local function artist_name_matches(artist: JsonObject, name: string): boolean
	return names_equal(string_field(artist, "strArtist"), name)
		or names_equal(string_field(artist, "strArtistAlternate"), name)
end

-- Unique non-blank names, in order.
function matching.lookup_names(names: { string? }): { string }
	local results: { string } = {}
	local seen: { [string]: boolean } = {}
	for _, name in names do
		local trimmed = trim(name)
		local key = normalize(trimmed)
		if key ~= "" and not seen[key] then
			seen[key] = true
			table.insert(results, trimmed)
		end
	end
	return results
end

-- The artist's record: by its stored id while TheAudioDB has a record for it, unless
-- `check_stored` and the record names another MusicBrainz artist; then by its MusicBrainz id;
-- then by a search hit named exactly as one of its names and not naming another MusicBrainz
-- artist. Without a local MusicBrainz id, a namesake can still match.
local function resolve_artist(query: ArtistQuery, check_stored: boolean): JsonObject?
	local artist_id = trim(query.artist_id)
	if artist_id ~= "" then
		local artist = api.lookup_artist_by_id(artist_id, query.fresh)
		if artist ~= nil and not (check_stored and artist_conflicts(artist, query.musicbrainz_id)) then
			return artist
		end
	end

	local musicbrainz_id = trim(query.musicbrainz_id)
	if musicbrainz_id ~= "" then
		local artist = api.lookup_artist_by_musicbrainz_id(musicbrainz_id, query.fresh)
		if artist ~= nil then
			return artist
		end
	end

	for _, name in query.names do
		for _, artist in api.search_artists(name, query.fresh) do
			if artist_name_matches(artist, name) and not artist_conflicts(artist, query.musicbrainz_id) then
				return artist
			end
		end
	end
	return nil
end

-- The artist's record for its cover. The stored id is taken as it is: the refresh that runs
-- before every cover fetch is what replaces a wrong one.
function matching.cover_artist(query: ArtistQuery): JsonObject?
	return resolve_artist(query, false)
end

-- The artist's record for a refresh, which passes over a stored id whose record names another
-- MusicBrainz artist.
function matching.refresh_artist(query: ArtistQuery): JsonObject?
	return resolve_artist(query, true)
end

-- The album titled exactly `album_title` by an artist named exactly `artist_name`, preferring
-- one released in `release_year`.
function matching.best_album(
	albums: { JsonObject },
	artist_name: string?,
	album_title: string?,
	release_year: string?
): JsonObject?
	local target_year = matching.year(release_year)
	local best: JsonObject? = nil
	for _, album in albums do
		if
			not names_equal(string_field(album, "strAlbum"), album_title)
			or not names_equal(string_field(album, "strArtist"), artist_name)
		then
			continue
		end
		local candidate_year = matching.year(string_field(album, "intYearReleased"))
		if target_year ~= nil and candidate_year == target_year then
			return album
		end
		if best == nil then
			best = album
		end
	end
	return best
end

-- Whether `candidate_name` is exactly one of `names`; any name passes when there are none.
function matching.artist_name_matches_any(candidate_name: string?, names: { string }): boolean
	if #names == 0 then
		return true
	end
	for _, name in names do
		if names_equal(candidate_name, name) then
			return true
		end
	end
	return false
end

-- The credited artist the album's TheAudioDB artist is: named exactly as the album's artist and
-- not naming another MusicBrainz artist.
function matching.album_artist(artists: { CreditedArtist }, album: JsonObject): CreditedArtist?
	local album_artist_name = string_field(album, "strArtist")
	local album_musicbrainz_id = string_field(album, "strMusicBrainzArtistID")
	for _, artist in artists do
		if
			names_equal(artist.artist_name, album_artist_name)
			and not musicbrainz_conflict(album_musicbrainz_id, artist.musicbrainz_id)
		then
			return artist
		end
	end
	return nil
end

return matching
