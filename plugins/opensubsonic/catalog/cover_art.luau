--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local covers = require("@lyra/covers")

type Cover = covers.Cover

-- A coverArt id is the public id of the item the cover belongs to, then `.` and the start of the
-- image's hash: items sharing a cover share an id, and an id changes with its image, so clients
-- caching by id fetch a new one. Public ids never hold a `.`.
local HASH_LENGTH = 8

local module = {}

local function id_of(cover: Cover): string
	return `{cover.owner_id}.{string.sub(cover.hash, 1, HASH_LENGTH)}`
end

-- The coverArt id of each item the caller can see that has a cover, keyed by item id. Reads the
-- stored records only, so it never touches the disk.
function module.ids(item_ids: { string }): { [string]: string }
	if #item_ids == 0 then
		return {}
	end
	local ids: { [string]: string } = {}
	for item_id, cover in covers.get(item_ids) do
		ids[item_id] = id_of(cover)
	end
	return ids
end

-- The cover a coverArt id names, if the caller can see its item and the image file is there. An
-- id without the hash, a bare item id, names the item's current cover too.
function module.find(cover_art_id: string): Cover?
	local item_id = string.match(cover_art_id, "^[^%.]+")
	if not item_id then
		return nil
	end
	return covers.get({ item_id }, { require_file = true })[item_id]
end

-- Whether `cover_art_id` is the id listings give `cover`, which names its image for good. A bare
-- item id, or one with an older image's hash, names whatever image the item shows.
function module.names_current(cover_art_id: string, cover: Cover): boolean
	return cover_art_id == id_of(cover)
end

return module
