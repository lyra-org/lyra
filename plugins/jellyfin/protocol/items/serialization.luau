--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")
local json = require("@harmony/json")

local base = require("./common")
local constants = require("../../constants")
local item_types = require("./types")

local SERVER_ID = constants.SERVER_ID

type JsonValue = api.JsonValue
type JsonObject = { [string]: JsonValue }
type UserData = item_types.UserData
type ArtistProjectionItem = item_types.ArtistProjectionItem
type ImageBlurHashes = item_types.ImageBlurHashes
type PersonItem = item_types.PersonItem
export type ItemInfo = item_types.SerializableItemInfo

local module = {}

local function string_array_to_json(values: { string }?): { JsonValue }?
	if values == nil then
		return nil
	end
	local output: { JsonValue } = {}
	for _, value: string in values do
		table.insert(output, value)
	end
	return output
end

local function string_map_to_json(values: { [string]: string }?): JsonObject?
	if values == nil then
		return nil
	end
	local output: JsonObject = {}
	for key: string, value: string in values do
		output[key] = value
	end
	return output
end

local function artist_projection_item_to_json(item: ArtistProjectionItem): JsonObject
	return {
		Id = item.Id,
		Name = item.Name,
	}
end

local function artist_projection_items_to_json(items: { ArtistProjectionItem }?): { JsonValue }?
	if items == nil then
		return nil
	end
	local output: { JsonValue } = {}
	for _, item: ArtistProjectionItem in items do
		table.insert(output, artist_projection_item_to_json(item))
	end
	return output
end

local function user_data_to_json(user_data: UserData?): JsonObject?
	if user_data == nil then
		return nil
	end
	return base.user_data_to_json(user_data)
end

local function image_blur_hashes_to_json(blur_hashes: ImageBlurHashes?): JsonObject?
	if blur_hashes == nil then
		return nil
	end
	local primary = string_map_to_json(blur_hashes.Primary)
	if primary == nil then
		return nil
	end
	return {
		Primary = primary,
	}
end

local function person_item_to_json(item: PersonItem): JsonObject
	local output: JsonObject = {
		Name = item.Name,
		Id = item.Id,
		Type = item.Type,
	}
	if item.Role ~= nil then
		output.Role = item.Role
	end
	if item.PrimaryImageTag ~= nil then
		output.PrimaryImageTag = item.PrimaryImageTag
	end
	local image_blur_hashes = image_blur_hashes_to_json(item.ImageBlurHashes)
	if image_blur_hashes ~= nil then
		output.ImageBlurHashes = image_blur_hashes
	end
	return output
end

local function person_items_to_json(items: { PersonItem }?): { JsonValue }?
	if items == nil then
		return nil
	end
	local output: { JsonValue } = {}
	for _, item: PersonItem in items do
		table.insert(output, person_item_to_json(item))
	end
	return output
end

local function json_object_array(values: { JsonObject }?): { JsonValue }?
	if values == nil then
		return nil
	end
	local output: { JsonValue } = {}
	for _, value: JsonObject in values do
		table.insert(output, value)
	end
	return output
end

function module.item_to_json(item: ItemInfo): JsonObject
	local output: JsonObject = {}
	output.ServerId = item.ServerId or SERVER_ID
	if item.Id ~= nil then
		output.Id = item.Id
	end
	if item.Name ~= nil then
		output.Name = item.Name
	end
	if item.Type ~= nil then
		output.Type = item.Type
	end
	if item.CollectionType ~= nil then
		output.CollectionType = item.CollectionType
	end
	if item.IsFolder ~= nil then
		output.IsFolder = item.IsFolder
	end
	if item.SortName ~= nil then
		output.SortName = item.SortName
	end
	if item.ChildCount ~= nil then
		output.ChildCount = item.ChildCount
	end
	if item.AlbumCount ~= nil then
		output.AlbumCount = item.AlbumCount
	end
	if item.SongCount ~= nil then
		output.SongCount = item.SongCount
	end
	if item.ArtistCount ~= nil then
		output.ArtistCount = item.ArtistCount
	end
	if item.DateCreated ~= nil then
		output.DateCreated = item.DateCreated
	end
	local image_tags = string_map_to_json(item.ImageTags)
	output.ImageTags = if image_tags ~= nil then image_tags else json.empty_object()
	if item.LocationType ~= nil then
		output.LocationType = item.LocationType
	end
	if item.MediaType ~= nil then
		output.MediaType = item.MediaType
	end
	local user_data = user_data_to_json(item.UserData)
	if user_data ~= nil then
		output.UserData = user_data
	end
	if item.ProductionYear ~= nil then
		output.ProductionYear = item.ProductionYear
	end
	if item.PremiereDate ~= nil then
		output.PremiereDate = item.PremiereDate
	end
	local genres = string_array_to_json(item.Genres)
	if genres ~= nil then
		output.Genres = genres
	end
	local genre_items = artist_projection_items_to_json(item.GenreItems)
	if genre_items ~= nil then
		output.GenreItems = genre_items
	end
	if item.AlbumArtist ~= nil then
		output.AlbumArtist = item.AlbumArtist
	end
	local release_artists = artist_projection_items_to_json(item.AlbumArtists)
	if release_artists ~= nil then
		output.AlbumArtists = release_artists
	end
	local artists = string_array_to_json(item.Artists)
	if artists ~= nil then
		output.Artists = artists
	end
	local artist_items = artist_projection_items_to_json(item.ArtistItems)
	if artist_items ~= nil then
		output.ArtistItems = artist_items
	end
	local people = person_items_to_json(item.People)
	if people ~= nil then
		output.People = people
	end
	if item.RunTimeTicks ~= nil then
		output.RunTimeTicks = item.RunTimeTicks
	end
	if item.ParentPrimaryImageItemId ~= nil then
		output.ParentPrimaryImageItemId = item.ParentPrimaryImageItemId
	end
	if item.ParentPrimaryImageTag ~= nil then
		output.ParentPrimaryImageTag = item.ParentPrimaryImageTag
	end
	if item.AlbumPrimaryImageTag ~= nil then
		output.AlbumPrimaryImageTag = item.AlbumPrimaryImageTag
	end
	local image_blur_hashes = image_blur_hashes_to_json(item.ImageBlurHashes)
	output.ImageBlurHashes = if image_blur_hashes ~= nil then image_blur_hashes else json.empty_object()
	if item.Container ~= nil then
		output.Container = item.Container
	end
	if item.IndexNumber ~= nil then
		output.IndexNumber = item.IndexNumber
	end
	if item.ParentIndexNumber ~= nil then
		output.ParentIndexNumber = item.ParentIndexNumber
	end
	if item.Album ~= nil then
		output.Album = item.Album
	end
	if item.AlbumId ~= nil then
		output.AlbumId = item.AlbumId
	end
	if item.ParentId ~= nil then
		output.ParentId = item.ParentId
	end
	if item.Artist ~= nil then
		output.Artist = item.Artist
	end
	if item.HasLyrics ~= nil then
		output.HasLyrics = item.HasLyrics
	end
	if item.NormalizationGain ~= nil then
		output.NormalizationGain = item.NormalizationGain
	end
	local media_sources = json_object_array(item.MediaSources)
	if media_sources ~= nil then
		output.MediaSources = media_sources
	end
	local media_streams = json_object_array(item.MediaStreams)
	if media_streams ~= nil then
		output.MediaStreams = media_streams
	end
	if item.CommunityRating ~= nil then
		output.CommunityRating = item.CommunityRating
	end
	if item.PlaylistItemId ~= nil then
		output.PlaylistItemId = item.PlaylistItemId
	end
	if item.Etag ~= nil then
		output.Etag = item.Etag
	end
	local tags_array = string_array_to_json(item.Tags)
	if tags_array ~= nil then
		output.Tags = tags_array
	end
	if item.ProviderIds ~= nil then
		output.ProviderIds = string_map_to_json(item.ProviderIds)
	end
	return output
end

function module.items_to_json(items: { ItemInfo }): { JsonValue }
	local output: { JsonValue } = {}
	for _, item: ItemInfo in items do
		table.insert(output, module.item_to_json(item))
	end
	return output
end

function module.build_item_query_result(items: { ItemInfo }, total_count: number, start_index: number): JsonValue
	return base.build_query_result(module.items_to_json(items), total_count, start_index)
end

return module
