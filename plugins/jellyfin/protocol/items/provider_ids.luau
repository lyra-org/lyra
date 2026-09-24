--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local artists = require("@lyra/artists")
local entities = require("@lyra/entities")

local encoding = require("../../encoding")
local item_types = require("./types")

type Artist = artists.Artist
type EntityProjectionInfo = entities.EntityProjectionInfo
type EntityQueryManyRequest = entities.EntityQueryManyRequest
type IdentifiersByScheme = { [string]: string }
type ItemInfo = item_types.SerializableItemInfo

type ProviderIdSource = "self" | "album" | "album_artist" | "artist"

type ProviderIdKey = {
	key: string,
	source: ProviderIdSource,
	scheme: string,
}

-- Jellyfin ProviderIds keys per item type, read from the item or a related item.
local PROVIDER_ID_KEYS: { [string]: { ProviderIdKey } } = {
	Audio = {
		{ key = "MusicBrainzTrack", source = "self", scheme = "musicbrainz:track" },
		{ key = "MusicBrainzRecording", source = "self", scheme = "musicbrainz:recording" },
		{ key = "MusicBrainzAlbum", source = "album", scheme = "musicbrainz:release" },
		{ key = "MusicBrainzReleaseGroup", source = "album", scheme = "musicbrainz:release-group" },
		{ key = "MusicBrainzAlbumArtist", source = "album_artist", scheme = "musicbrainz:artist" },
		{ key = "MusicBrainzArtist", source = "artist", scheme = "musicbrainz:artist" },
		{ key = "AudioDbAlbum", source = "album", scheme = "theaudiodb:album" },
	},
	MusicAlbum = {
		{ key = "MusicBrainzAlbum", source = "self", scheme = "musicbrainz:release" },
		{ key = "MusicBrainzReleaseGroup", source = "self", scheme = "musicbrainz:release-group" },
		{ key = "MusicBrainzAlbumArtist", source = "album_artist", scheme = "musicbrainz:artist" },
		{ key = "AudioDbAlbum", source = "self", scheme = "theaudiodb:album" },
		-- Jellyfin's AudioDB album provider also stores the album artist here.
		{ key = "AudioDbArtist", source = "album_artist", scheme = "theaudiodb:artist" },
	},
	MusicArtist = {
		{ key = "MusicBrainzArtist", source = "self", scheme = "musicbrainz:artist" },
		{ key = "AudioDbArtist", source = "self", scheme = "theaudiodb:artist" },
	},
}

-- Per Jellyfin item id.
export type ItemProviderIds = {
	identifiers: { [string]: IdentifiersByScheme },
}

export type PageEntityIds = {
	release_ids: { number },
	track_ids: { number },
	artist_ids: { number },
}

local module = {}

local function append_unique(id: number?, ids: { number | string }, seen: { [number]: boolean })
	if id ~= nil and id > 0 and not seen[id] then
		seen[id] = true
		table.insert(ids, id)
	end
end

-- Page entities plus the artists credited on its releases and tracks.
local function page_entity_ids(page: PageEntityIds, artists_by_owner_id: { [number]: { Artist }? }): { number | string }
	local ids: { number | string } = {}
	local seen: { [number]: boolean } = {}
	for _, owner_ids: { number } in { page.release_ids, page.track_ids } do
		for _, owner_id: number in owner_ids do
			append_unique(owner_id, ids, seen)
			local owner_artists = artists_by_owner_id[owner_id]
			if owner_artists ~= nil then
				for _, artist: Artist in owner_artists do
					append_unique(artist.db_id, ids, seen)
				end
			end
		end
	end
	for _, artist_id: number in page.artist_ids do
		append_unique(artist_id, ids, seen)
	end
	return ids
end

-- One batch query for the page.
function module.fetch(page: PageEntityIds, artists_by_owner_id: { [number]: { Artist }? }): ItemProviderIds?
	local ids = page_entity_ids(page, artists_by_owner_id)
	if #ids == 0 then
		return nil
	end

	local index: ItemProviderIds = { identifiers = {} }
	local request: EntityQueryManyRequest = { ids = ids, include = { "identifiers" } }
	local ok, projections = pcall(entities.query_many, request)
	if not ok or type(projections) ~= "table" then
		return index
	end

	for _, projection: EntityProjectionInfo in projections do
		local item_id = encoding.nanoid_to_hex(projection.entity.id)
		if item_id == nil then
			continue
		end
		local identifiers = projection.includes.identifiers
		if identifiers then
			index.identifiers[item_id] = identifiers
		end
	end
	return index
end

local function source_item_id(item: ItemInfo, source: ProviderIdSource): string?
	if source == "self" then
		return item.Id
	elseif source == "album" then
		return item.AlbumId
	end

	local related = if source == "album_artist" then item.AlbumArtists else item.ArtistItems
	local first = if related then related[1] else nil
	return if first then first.Id else nil
end

-- Nil for item types without Jellyfin provider id keys.
local function provider_ids(
	item: ItemInfo,
	identifiers_by_item_id: { [string]: IdentifiersByScheme }
): { [string]: string }?
	local keys = PROVIDER_ID_KEYS[item.Type or ""]
	if keys == nil then
		return nil
	end

	local provider_ids: { [string]: string } = {}
	for _, spec: ProviderIdKey in keys do
		local item_id = source_item_id(item, spec.source)
		local identifiers = if item_id then identifiers_by_item_id[item_id] else nil
		local value = if identifiers then identifiers[spec.scheme] else nil
		if value then
			provider_ids[spec.key] = value
		end
	end
	return provider_ids
end

function module.apply(item: ItemInfo, index: ItemProviderIds)
	item.ProviderIds = provider_ids(item, index.identifiers)
end

return module
