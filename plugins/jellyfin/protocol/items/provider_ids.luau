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
type ExternalUrlInfo = item_types.ExternalUrlInfo
type IdLink = entities.IdLink
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

-- Per Jellyfin item id; a map is present only when its field was requested.
export type ItemProviderIds = {
	identifiers: { [string]: IdentifiersByScheme }?,
	links: { [string]: { IdLink } }?,
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

-- One batch query for the page. Credited artists are only needed for ProviderIds.
function module.fetch(
	page: PageEntityIds,
	artists_by_owner_id: { [number]: { Artist }? },
	include_provider_ids: boolean,
	include_external_urls: boolean,
	library_id: number?
): ItemProviderIds?
	local ids = page_entity_ids(page, if include_provider_ids then artists_by_owner_id else {})
	if #ids == 0 then
		return nil
	end

	local include: { string } = {}
	local index: ItemProviderIds = {}
	if include_provider_ids then
		table.insert(include, "identifiers")
		index.identifiers = {}
	end
	if include_external_urls then
		table.insert(include, "links")
		index.links = {}
	end

	local request: EntityQueryManyRequest = { ids = ids, include = include, library_id = library_id }
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
		if index.identifiers and identifiers then
			index.identifiers[item_id] = identifiers
		end
		local links = projection.includes.links
		if index.links and links then
			index.links[item_id] = links
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

-- Links of the item's own entity, first occurrence of each URL.
local function external_urls(item: ItemInfo, links_by_item_id: { [string]: { IdLink } }): { ExternalUrlInfo }?
	if PROVIDER_ID_KEYS[item.Type or ""] == nil then
		return nil
	end

	local urls: { ExternalUrlInfo } = {}
	local seen: { [string]: boolean } = {}
	for _, link: IdLink in (if item.Id then links_by_item_id[item.Id] else nil) or {} do
		if not seen[link.url] then
			seen[link.url] = true
			local url: ExternalUrlInfo = { Name = link.label, Url = link.url }
			table.insert(urls, url)
		end
	end
	return urls
end

function module.apply(item: ItemInfo, index: ItemProviderIds)
	if index.identifiers then
		item.ProviderIds = provider_ids(item, index.identifiers)
	end
	if index.links then
		local urls = external_urls(item, index.links)
		if urls then
			item.ExternalUrls = urls
		end
	end
end

return module
