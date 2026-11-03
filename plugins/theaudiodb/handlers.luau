--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

-- TheAudioDB's ids, searches, covers and refreshes, registered on a metadata provider.

local serde = require("@harmony/serde")
local json = serde.json
local metadata = require("@lyra/metadata")

local albums = require("./resolution/albums")
local api = require("./api")
local matching = require("./matching")

local ET = metadata.EntityType

local BASE_URL = api.BASE_URL

type Provider = metadata.Provider
type JsonValue = api.JsonValue
type JsonObject = api.JsonObject
type SearchResultItem = { [string]: JsonValue }
type ProviderCoverCandidate = metadata.ProviderCoverCandidate
type ProviderCoverResult = metadata.ProviderCoverResult
type ExternalIdsByScheme = metadata.ExternalIdsByScheme
type ArtistQuery = matching.ArtistQuery
type CreditedArtist = matching.CreditedArtist
type SearchCandidatesFn = (string, string?) -> { JsonObject }

local trim = api.trim
local json_string_field = api.string_field
local json_number_field = api.number_field
local first_non_empty_field = api.first_non_empty_field

local handlers = {}

local QUERY_SEPARATORS = { " - ", " – ", " — ", ": " }
local ARTIST_COVER_KEYS = {
	"strArtistThumb",
	"strArtistWideThumb",
	"strArtistFanart",
	"strArtistFanart2",
	"strArtistFanart3",
	"strArtistFanart4",
	"strArtistBanner",
}
local ALBUM_COVER_KEYS = {
	"strAlbumThumbHQ",
	"strAlbumThumb",
	"strAlbum3DThumb",
	"strAlbum3DCase",
	"strAlbum3DFace",
	"strAlbum3DFlat",
}
local ALBUM_ID_KEYS = { "idAlbum" }
local TRACK_ID_KEYS = { "idTrack" }

local function split_compound_query(query: string): (string?, string?)
	for _, separator in QUERY_SEPARATORS do
		local start_index = string.find(query, separator, 1, true)
		if start_index ~= nil then
			local left = trim(string.sub(query, 1, start_index - 1))
			local right = trim(string.sub(query, start_index + #separator))
			if left ~= "" and right ~= "" then
				return left, right
			end
		end
	end
	return nil, nil
end

local function audiodb_url(path: string): string
	return `{BASE_URL}/{path}`
end

local function build_redirect_url(entity: string, id: string?): string
	if id ~= nil and id ~= "" then
		return audiodb_url(`{entity}/{id}`)
	end
	return BASE_URL
end

local function single_id_map(key: string, value: string?): JsonObject?
	if value == nil or value == "" then
		return nil
	end
	return {
		[key] = value,
	}
end
local ARTIST_SCHEME = "theaudiodb:artist"
local ALBUM_SCHEME = "theaudiodb:album"

local function identifier(external_ids: ExternalIdsByScheme?, scheme: string): string?
	return if external_ids then external_ids[scheme] else nil
end

local search_artists_by_name = api.search_artists
local search_albums = api.search_albums
local search_tracks = api.search_tracks

local function push_unique_string(results: { string }, seen: { [string]: boolean }, value: string?)
	if value == nil or value == "" or seen[value] == true then
		return
	end
	seen[value] = true
	table.insert(results, value)
end

local function push_unique_object(
	results: { JsonObject },
	seen: { [string]: boolean },
	value: JsonObject,
	id_keys: { string }
)
	local dedupe_key = ""
	for _, key in id_keys do
		local value_id = json_string_field(value, key)
		if value_id ~= nil then
			dedupe_key = `{key}:{value_id}`
			break
		end
	end
	if dedupe_key == "" then
		dedupe_key = json.encode(value)
	end
	if seen[dedupe_key] == true then
		return
	end
	seen[dedupe_key] = true
	table.insert(results, value)
end

local function preferred_cover_url(item: JsonObject, keys: { string }): string?
	return first_non_empty_field(item, keys)
end

local function preferred_artist_cover_url(artist: JsonObject): string?
	return preferred_cover_url(artist, ARTIST_COVER_KEYS)
end

local function preferred_album_cover_url(album: JsonObject): string?
	return preferred_cover_url(album, ALBUM_COVER_KEYS)
end

local function cover_payload(item: JsonObject, keys: { string }): ProviderCoverResult?
	local urls: { string } = {}
	local seen: { [string]: boolean } = {}
	for _, key in keys do
		push_unique_string(urls, seen, json_string_field(item, key))
	end
	if #urls == 0 then
		return nil
	end

	local candidates: { ProviderCoverCandidate } = {}
	for _, url in urls do
		table.insert(candidates, { url = url } :: ProviderCoverCandidate)
	end
	return {
		candidates = candidates,
		selected_index = 1,
	} :: ProviderCoverResult
end

local function artist_cover_payload(artist: JsonObject): ProviderCoverResult?
	return cover_payload(artist, ARTIST_COVER_KEYS)
end

local function album_cover_payload(album: JsonObject): ProviderCoverResult?
	return cover_payload(album, ALBUM_COVER_KEYS)
end

local function cover_artist_query(ctx: metadata.ArtistCoverContext): ArtistQuery
	local artist_id = identifier(ctx.external_ids, ARTIST_SCHEME)
	return {
		artist_id = artist_id,
		musicbrainz_id = identifier(ctx.external_ids, "musicbrainz:artist"),
		names = matching.lookup_names({ ctx.artist_name, ctx.sort_name }),
	}
end

local function first_artist_name(ctx: metadata.ReleaseCoverContext): string?
	local names = ctx.artist_names
	if names ~= nil then
		for _, name in names do
			if trim(name) ~= "" then
				return name
			end
		end
	end
	local artists = ctx.artists
	if artists ~= nil then
		for _, artist in artists do
			if artist.artist_name ~= nil and trim(artist.artist_name) ~= "" then
				return artist.artist_name
			end
		end
	end
	return nil
end

local function resolve_release_cover_match(ctx: metadata.ReleaseCoverContext): JsonObject?
	local album_id = identifier(ctx.external_ids, ALBUM_SCHEME)
	if album_id ~= nil and album_id ~= "" then
		local album = api.lookup_album_by_id(album_id)
		if album ~= nil then
			return album
		end
	end

	local release_title = if ctx.release_title then trim(ctx.release_title) else ""
	local artist_name = first_artist_name(ctx)
	if release_title == "" or artist_name == nil or trim(artist_name) == "" then
		return nil
	end

	local albums = search_albums(artist_name, release_title)
	return matching.best_album(albums, artist_name, release_title, ctx.release_date)
end

local function artist_search_results(query: string): { SearchResultItem }
	local results: { SearchResultItem } = {}
	for _, artist in search_artists_by_name(query) do
		local artist_id = json_string_field(artist, "idArtist")
		local artist_name = json_string_field(artist, "strArtist")
		if artist_name == nil then
			continue
		end

		local item: SearchResultItem = {
			title = artist_name,
			artist_name = artist_name,
			redirect_url = build_redirect_url("artist", artist_id),
		}

		local sort_name = first_non_empty_field(artist, { "strArtistStripped", "strArtist" })
		if sort_name ~= nil then
			item.sort_name = sort_name
		end
		local description = first_non_empty_field(artist, { "strBiography", "strBiographyEN" })
		if description ~= nil then
			item.description = description
		end
		local cover_url = preferred_artist_cover_url(artist)
		if cover_url ~= nil then
			item.cover_url = cover_url
		end
		local ids = single_id_map("artist_id", artist_id)
		if ids ~= nil then
			item.ids = ids
		end

		table.insert(results, item)
	end
	return results
end

local function search_candidates(query: string, searcher: SearchCandidatesFn, id_keys: { string }): { JsonObject }
	local results: { JsonObject } = {}
	local seen: { [string]: boolean } = {}

	local left, right = split_compound_query(query)
	if left ~= nil and right ~= nil then
		for _, item in searcher(left, right) do
			push_unique_object(results, seen, item, id_keys)
		end
		for _, item in searcher(right, left) do
			push_unique_object(results, seen, item, id_keys)
		end
	end

	for _, item in searcher(query, nil) do
		push_unique_object(results, seen, item, id_keys)
	end

	return results
end

local function release_search_candidates(query: string): { JsonObject }
	return search_candidates(query, search_albums, ALBUM_ID_KEYS)
end

local function release_search_results(query: string): { SearchResultItem }
	local results: { SearchResultItem } = {}
	for _, album in release_search_candidates(query) do
		local album_id = json_string_field(album, "idAlbum")
		local title = json_string_field(album, "strAlbum")
		if title == nil then
			continue
		end

		local item: SearchResultItem = {
			title = title,
			redirect_url = build_redirect_url("album", album_id),
		}

		local artist_name = json_string_field(album, "strArtist")
		if artist_name ~= nil then
			item.artist_name = artist_name
		end
		local release_date = matching.year(json_string_field(album, "intYearReleased"))
		if release_date ~= nil then
			item.release_date = release_date
		end
		local genres = albums.genre_values(json_string_field(album, "strGenre"))
		if genres ~= nil then
			item.genres = genres
		end
		local description = albums.description(album)
		if description then
			item.description = description.text
		end
		local cover_url = preferred_album_cover_url(album)
		if cover_url ~= nil then
			item.cover_url = cover_url
		end
		local ids = single_id_map("album_id", album_id)
		if ids ~= nil then
			item.ids = ids
		end

		table.insert(results, item)
	end
	return results
end

local function track_search_candidates(query: string): { JsonObject }
	return search_candidates(query, search_tracks, TRACK_ID_KEYS)
end

local function track_search_results(query: string): { SearchResultItem }
	local results: { SearchResultItem } = {}
	for _, track in track_search_candidates(query) do
		local track_id = json_string_field(track, "idTrack")
		local title = json_string_field(track, "strTrack")
		if title == nil then
			continue
		end

		local item: SearchResultItem = {
			title = title,
			redirect_url = build_redirect_url("track", track_id),
		}

		local artist_name = json_string_field(track, "strArtist")
		if artist_name ~= nil then
			item.artist_name = artist_name
		end
		local release_title = json_string_field(track, "strAlbum")
		if release_title ~= nil then
			item.release_title = release_title
		end
		local description = first_non_empty_field(track, { "strDescriptionEN", "strTrackLyrics" })
		if description ~= nil then
			item.description = description
		end
		local duration_ms = json_number_field(track, "intDuration")
		if duration_ms ~= nil then
			item.duration_ms = duration_ms
		end
		local disc = json_number_field(track, "intCD")
		if disc ~= nil then
			item.disc = disc
		end
		local track_number = json_number_field(track, "intTrackNumber")
		if track_number ~= nil then
			item.track = track_number
		end
		local cover_url = json_string_field(track, "strTrackThumb")
		if cover_url ~= nil then
			item.cover_url = cover_url
		end
		local ids = single_id_map("track_id", track_id)
		if ids ~= nil then
			item.ids = ids
		end

		table.insert(results, item)
	end
	return results
end

local function refresh_artist_names(ctx: metadata.ReleaseRefreshContext): { string }
	local names: { string? } = {}
	for _, artist in ctx.artists or {} do
		local name: string? = artist.artist_name
		table.insert(names, name)
	end
	local hint: string? = if ctx.lookup_hints then ctx.lookup_hints.artist_name else nil
	table.insert(names, hint)
	return matching.lookup_names(names)
end

local function should_refresh_existing(options: { [string]: metadata.OptionValue }?): boolean
	return options ~= nil and options.refresh_existing == true
end

local function refresh_artist_query(ctx: metadata.ArtistRefreshContext): ArtistQuery
	local artist_id = identifier(ctx.external_ids, ARTIST_SCHEME)
	return {
		artist_id = artist_id,
		musicbrainz_id = identifier(ctx.external_ids, "musicbrainz:artist"),
		names = matching.lookup_names({ ctx.artist_name, ctx.sort_name }),
	}
end

local function refresh_album_match_from_recording_ids(
	ctx: metadata.ReleaseRefreshContext,
	artist_names: { string },
	release_title: string
): JsonObject?
	if ctx.tracks == nil then
		return nil
	end

	for _, track in ctx.tracks do
		local recording_id = identifier(track.external_ids, "musicbrainz:recording")
		if recording_id == nil then
			continue
		end

		local matched_track = api.lookup_track_by_musicbrainz_id(recording_id)
		local album_id = json_string_field(matched_track, "idAlbum")
		if album_id == nil then
			continue
		end
		if not matching.names_equal(json_string_field(matched_track, "strAlbum"), release_title) then
			continue
		end
		if not matching.artist_name_matches_any(json_string_field(matched_track, "strArtist"), artist_names) then
			continue
		end

		local album = api.lookup_album_by_id(album_id)
		if album ~= nil then
			return album
		end
	end

	return nil
end

local function refresh_album_match(ctx: metadata.ReleaseRefreshContext): JsonObject?
	local existing_album_id = identifier(ctx.external_ids, ALBUM_SCHEME)
	if existing_album_id ~= nil then
		return api.lookup_album_by_id(existing_album_id)
	end

	local release_group_id = identifier(ctx.external_ids, "musicbrainz:release-group")
	if release_group_id ~= nil then
		local album = api.lookup_album_by_release_group_id(release_group_id)
		if album ~= nil then
			return album
		end
	end

	local artist_names = refresh_artist_names(ctx)
	local release_title = trim(ctx.release_title)
	if release_title == "" then
		return nil
	end

	local album_from_track = refresh_album_match_from_recording_ids(ctx, artist_names, release_title)
	if album_from_track ~= nil then
		return album_from_track
	end

	local release_year = if ctx.lookup_hints ~= nil and ctx.lookup_hints.year ~= nil
		then tostring(ctx.lookup_hints.year)
		else ctx.release_date
	for _, artist_name in artist_names do
		local matched =
			matching.best_album(search_albums(artist_name, release_title), artist_name, release_title, release_year)
		if matched ~= nil then
			return matched
		end
	end

	return nil
end

local function credited_artists(artists: { metadata.ReleaseRefreshArtist }?): { CreditedArtist }
	local results: { CreditedArtist } = {}
	for _, artist in artists or {} do
		local credited: CreditedArtist = {
			id = artist.id,
			artist_name = artist.artist_name,
			musicbrainz_id = identifier(artist.external_ids, "musicbrainz:artist"),
		}
		table.insert(results, credited)
	end
	return results
end

function handlers.register(provider: Provider)
	provider:id({
		id_type = "artist_id",
		entity = ET.Artist,
		unique = true,
		scheme = ARTIST_SCHEME,
	}, audiodb_url("artist/{id}"))

	provider:id({
		id_type = "album_id",
		entity = ET.Release,
		unique = true,
		scheme = ALBUM_SCHEME,
	}, audiodb_url("album/{id}"))

	provider:id({
		id_type = "track_id",
		entity = ET.Track,
		unique = true,
		scheme = "theaudiodb:track",
	}, audiodb_url("track/{id}"))

	provider:search(ET.Artist, function(query: string): { SearchResultItem }
		return artist_search_results(query)
	end)

	provider:search(ET.Release, function(query: string): { SearchResultItem }
		return release_search_results(query)
	end)

	provider:search(ET.Track, function(query: string): { SearchResultItem }
		return track_search_results(query)
	end)

	provider:cover(ET.Artist, {
		require = {
			any_of = { "external_ids.theaudiodb:artist", "artist_name", "sort_name" },
		},
	}, function(ctx: metadata.ArtistCoverContext): ProviderCoverResult?
		local artist = matching.cover_artist(cover_artist_query(ctx))
		if artist == nil then
			return nil
		end
		return artist_cover_payload(artist)
	end)

	provider:cover(ET.Release, {
		require = {
			all_of = { "release_title" },
			any_of = { "external_ids.theaudiodb:album", "artist_names" },
		},
	}, function(ctx: metadata.ReleaseCoverContext): ProviderCoverResult?
		local album = resolve_release_cover_match(ctx)
		if album == nil then
			return nil
		end
		return album_cover_payload(album)
	end)

	provider:declare_option({
		name = "refresh_existing",
		label = "Refresh Existing Match",
		type = "boolean",
		default = false,
	})

	provider:refresh(ET.Artist, function(ctx: metadata.ArtistRefreshContext)
		local artist_id = ctx.id
		if artist_id == nil then
			return nil
		end

		local query = refresh_artist_query(ctx)
		local artist = matching.refresh_artist(query)
		local audiodb_artist_id = json_string_field(artist, "idArtist")
		if audiodb_artist_id == nil or audiodb_artist_id == query.artist_id then
			return nil
		end

		local artist_layer = provider:layer(artist_id)
		artist_layer:set_id("artist_id", audiodb_artist_id)
		artist_layer:save()
		return nil
	end, function(ctx: metadata.ArtistRefreshContext): boolean
		local query = refresh_artist_query(ctx)
		if should_refresh_existing(ctx.options) then
			return #query.names > 0 or query.musicbrainz_id ~= nil
		end
		-- Only a MusicBrainz id can show a stored id to be wrong.
		return query.musicbrainz_id ~= nil
	end)

	provider:refresh(ET.Release, function(ctx: metadata.ReleaseRefreshContext)
		local release_id = ctx.id
		if release_id == nil then
			return nil
		end

		local album = refresh_album_match(ctx)
		if album == nil then
			return nil
		end

		local album_id = json_string_field(album, "idAlbum")
		if album_id == nil then
			return nil
		end

		albums.save_release_layer(provider, release_id, album_id, album, albums.library_language(release_id))

		local artist = matching.album_artist(credited_artists(ctx.artists), album)
		local audiodb_artist_id = json_string_field(album, "idArtist")
		if artist ~= nil and audiodb_artist_id ~= nil then
			local artist_layer = provider:layer(artist.id)
			artist_layer:set_id("artist_id", audiodb_artist_id)
			artist_layer:save()
		end

		return nil
	end, function(ctx: metadata.ReleaseRefreshContext): boolean
		if trim(ctx.release_title) == "" then
			return false
		end
		if should_refresh_existing(ctx.options) then
			return true
		end
		if identifier(ctx.external_ids, ALBUM_SCHEME) ~= nil then
			return false
		end
		if identifier(ctx.external_ids, "musicbrainz:release-group") ~= nil then
			return true
		end
		return #refresh_artist_names(ctx) > 0
	end)
end

return handlers
