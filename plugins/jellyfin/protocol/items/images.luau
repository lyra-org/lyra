--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local covers = require("@lyra/covers")
local entities = require("@lyra/entities")
local genres = require("@lyra/genres")

local identifiers = require("./identifiers")
local playlist_access = require("../playlist_access")
local requests = require("./requests")

export type ResolvedCoverInfo = {
	id: string,
	hash: string,
	blurhash: string?,
	owner_id: string?,
}

local module = {}

function module.normalize_cover_info(raw_cover: covers.Cover?): ResolvedCoverInfo?
	if raw_cover == nil then
		return nil
	end

	local id = requests.trim(raw_cover.id)
	local hash = requests.trim(raw_cover.hash)
	if not id or not hash then
		return nil
	end

	return {
		id = id,
		hash = hash,
		blurhash = requests.trim(raw_cover.blurhash),
		owner_id = identifiers.encode_jellyfin_item_id(raw_cover.owner_id),
	}
end

local function normalize_all(raw_covers: { [string]: covers.Cover }): { [string]: ResolvedCoverInfo }
	local resolved: { [string]: ResolvedCoverInfo } = {}
	for id, raw_cover in raw_covers do
		resolved[id] = module.normalize_cover_info(raw_cover)
	end
	return resolved
end

-- The covers of the releases, artists and tracks the caller can see, keyed by id. Listings read
-- them without looking at the files.
function module.get_cover_infos(item_ids: { string }): { [string]: ResolvedCoverInfo }
	if #item_ids == 0 then
		return {}
	end
	return normalize_all(covers.get(item_ids))
end

-- For anonymous requests to public artwork routes, which resolve covers without a caller, and
-- for the covers of playlists and genres the caller can see. Image requests carry no credentials,
-- so a playlist's or genre's cover is always the one the system picks from the instance's
-- listens: the tag a listing advertises then names the image every request serves.
function module.get_artwork_infos(item_ids: { string }): { [string]: ResolvedCoverInfo }
	if #item_ids == 0 then
		return {}
	end
	return normalize_all(covers.artwork(item_ids))
end

function module.get_cover_info(item_id: string?): ResolvedCoverInfo?
	if item_id == nil then
		return nil
	end
	return module.get_cover_infos({ item_id })[item_id]
end

function module.get_artwork_info(item_id: string?): ResolvedCoverInfo?
	if item_id == nil then
		return nil
	end
	return module.get_artwork_infos({ item_id })[item_id]
end

-- The artwork to serve for an item: as `get_artwork_info`, but only when its file is there. A
-- cover whose file is gone is forgotten, so the next listing advertises the next pick.
function module.get_artwork_file(item_id: string?): ResolvedCoverInfo?
	if item_id == nil then
		return nil
	end
	return module.normalize_cover_info(covers.artwork({ item_id }, { require_file = true })[item_id])
end

-- Any item's cover to serve, if the caller can see the item and its file is there.
function module.get_item_cover_file(item_id: string): ResolvedCoverInfo?
	local element_type = entities.get_type(item_id)
	if element_type == "Playlist" then
		return if playlist_access.find(item_id) then module.get_artwork_file(item_id) else nil
	elseif element_type == "Genre" then
		return if genres.get({ item_id })[item_id] then module.get_artwork_file(item_id) else nil
	end
	return module.normalize_cover_info(covers.get({ item_id }, { require_file = true })[item_id])
end

return module
